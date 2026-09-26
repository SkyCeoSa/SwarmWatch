/**
 * Partition Reconciliation & Conflict Lattice Engine (Seam 57 / ADR 0068).
 * Safely rejoins partitioned gossip mesh clusters and shard replicas without silent safety trip clearance.
 * Enforces:
 * 1. Safety Trip Monotonicity Invariant: TRIPPED state is an absorbing barrier across partitions.
 * 2. Explicit Conflict Lattice: Concurrent spans (V_A || V_B) preserve provenance metadata rather than silently dropping.
 * 3. Monotonic Epoch Fencing: Reconciled authority epoch = max(E_local, E_remote) + (1 if conflict).
 * 4. Zero external LLM API calls (<25us local execution).
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export const LatticeVerdict = {
  DOMINATES: "DOMINATES",
  DOMINATED: "DOMINATED",
  IDENTICAL: "IDENTICAL",
  CONCURRENT: "CONCURRENT",
} as const;

export type LatticeVerdictEnum = (typeof LatticeVerdict)[keyof typeof LatticeVerdict];

export function compareVectorClocks(
  vcA: Record<string, number>,
  vcB: Record<string, number>,
): LatticeVerdictEnum {
  const allKeys = new Set([...Object.keys(vcA || {}), ...Object.keys(vcB || {})]);
  let aGreater = false;
  let bGreater = false;

  for (const k of allKeys) {
    const va = vcA?.[k] ?? 0;
    const vb = vcB?.[k] ?? 0;
    if (va > vb) {
      aGreater = true;
    } else if (vb > va) {
      bGreater = true;
    }
  }

  if (aGreater && !bGreater) {
    return LatticeVerdict.DOMINATES;
  } else if (bGreater && !aGreater) {
    return LatticeVerdict.DOMINATED;
  } else if (!aGreater && !bGreater) {
    return LatticeVerdict.IDENTICAL;
  } else {
    return LatticeVerdict.CONCURRENT;
  }
}

export function mergeVectorClocks(
  vcA: Record<string, number>,
  vcB: Record<string, number>,
): Record<string, number> {
  const allKeys = Array.from(new Set([...Object.keys(vcA || {}), ...Object.keys(vcB || {})])).sort();
  const merged: Record<string, number> = {};
  for (const k of allKeys) {
    merged[k] = Math.max(vcA?.[k] ?? 0, vcB?.[k] ?? 0);
  }
  return merged;
}

export interface ReconciliationPlan {
  plan_id: string;
  tenant_id: string;
  verdict: LatticeVerdictEnum;
  divergent_keys: string[];
  missing_local_events: number;
  missing_remote_events: number;
  conflict_detected: boolean;
  merged_vector_clock: Record<string, number>;
  action_items: string[];
  created_at: number;
  plan_hash?: string;
}

export interface MergeResult {
  merged_spans: Record<string, any>[];
  total_spans: number;
  deduplicated_count: number;
  concurrent_conflicts_retained: number;
  merged_vector_clock: Record<string, number>;
  convergence_hash: string;
  execution_latency_us: number;
}

export interface AuthorityReconciliationResult {
  tenant_id: string;
  converged_state: string;
  trip_monotonicity_enforced: boolean;
  previous_local_epoch: number;
  previous_remote_epoch: number;
  reconciled_epoch: number;
  active_permits_revoked: number;
  reason: string;
  reconciled_at: number;
  reconciliation_hash?: string;
}

export class MeshReconciler {
  private _reconciliations: AuthorityReconciliationResult[] = [];
  private _tripsPreservedCount: number = 0;
  private _concurrentSpansRetained: number = 0;
  private _activeConflicts: number = 0;

  public diff(
    localState: Record<string, any>,
    remoteState: Record<string, any>,
    tenantId: string = "default",
  ): ReconciliationPlan {
    const vcLocal = localState.vector_clock || {};
    const vcRemote = remoteState.vector_clock || {};

    const verdict = compareVectorClocks(vcLocal, vcRemote);
    const mergedVc = mergeVectorClocks(vcLocal, vcRemote);

    // Discover divergent keys
    const getKeys = (st: Record<string, any>): Set<string> => {
      if (st.keys && typeof st.keys === "object" && !Array.isArray(st.keys)) {
        return new Set(Object.keys(st.keys));
      }
      return new Set(Object.keys(st));
    };

    const keysLocal = getKeys(localState);
    const keysRemote = getKeys(remoteState);

    const divergentKeys: string[] = [];
    for (const k of keysLocal) {
      if (!keysRemote.has(k)) divergentKeys.push(k);
    }
    for (const k of keysRemote) {
      if (!keysLocal.has(k)) divergentKeys.push(k);
    }
    divergentKeys.sort();

    const maxSeqLocal = Number(localState.max_sequence || 0);
    const maxSeqRemote = Number(remoteState.max_sequence || 0);
    const missingLocal = Math.max(0, maxSeqRemote - maxSeqLocal);
    const missingRemote = Math.max(0, maxSeqLocal - maxSeqRemote);

    const conflict = verdict === LatticeVerdict.CONCURRENT || divergentKeys.length > 0;

    const actionItems: string[] = [];
    if (verdict === LatticeVerdict.DOMINATES) {
      actionItems.push("PULL_REMOTE_FROM_LOCAL");
    } else if (verdict === LatticeVerdict.DOMINATED) {
      actionItems.push("FAST_FORWARD_LOCAL_TO_REMOTE");
    } else if (verdict === LatticeVerdict.CONCURRENT) {
      actionItems.push("RESOLVE_CONCURRENT_LATTICE_PRESERVE_PROVENANCE");
      this._activeConflicts++;
    } else {
      actionItems.push("NO_OP_IN_SYNC");
    }

    const planId = `plan-${crypto.randomBytes(6).toString("hex")}`;
    const planHash = CanonicalCodec.hash(
      {
        conflict_detected: conflict,
        divergent_keys: divergentKeys,
        merged_vector_clock: mergedVc,
        missing_local_events: missingLocal,
        missing_remote_events: missingRemote,
        tenant_id: tenantId,
        verdict: verdict,
      },
      CanonicalDomainTag.RECONCILER,
    );

    return {
      plan_id: planId,
      tenant_id: tenantId,
      verdict,
      divergent_keys: divergentKeys,
      missing_local_events: missingLocal,
      missing_remote_events: missingRemote,
      conflict_detected: conflict,
      merged_vector_clock: mergedVc,
      action_items: actionItems,
      created_at: Date.now() / 1000,
      plan_hash: planHash,
    };
  }

  public mergeEvidence(
    localEvents: Record<string, any>[],
    remoteEvents: Record<string, any>[],
    tenantId: string = "default",
  ): MergeResult {
    const startHr = process.hrtime.bigint();
    const seenHashes = new Set<string>();
    const merged: Record<string, any>[] = [];
    let dedupCount = 0;
    let concurrentRetained = 0;
    let cumulativeVc: Record<string, number> = {};

    const allRaw = [...(localEvents || []), ...(remoteEvents || [])];

    for (const evt of allRaw) {
      const evtId = evt.span_id || evt.id || crypto.randomBytes(4).toString("hex");
      const turn = evt.turn_idx ?? evt.turn ?? 0;
      const agent = evt.agent_id || evt.agent || "unknown";
      const payload = evt.payload || {};
      const vc = evt.vector_clock || {};

      if (vc && typeof vc === "object") {
        cumulativeVc = mergeVectorClocks(cumulativeVc, vc);
      }

      const evtDigest =
        evt.digest ||
        CanonicalCodec.hash(
          {
            agent,
            id: evtId,
            payload,
            turn,
          },
          CanonicalDomainTag.RECONCILER,
        );

      if (seenHashes.has(evtDigest)) {
        dedupCount++;
        continue;
      }

      seenHashes.add(evtDigest);

      const isConcurrent = Boolean(evt.is_concurrent_fork);
      const originPeer = evt.origin_peer || "peer_local";

      if (isConcurrent) {
        concurrentRetained++;
        this._concurrentSpansRetained++;
      }

      const retainedEvt = {
        ...evt,
        digest: evtDigest,
        provenance_peer: originPeer,
        is_concurrent_fork: isConcurrent,
      };
      merged.push(retainedEvt);
    }

    // Sort deterministically by turn_idx, then timestamp, then digest
    merged.sort((a, b) => {
      const turnA = a.turn_idx ?? a.turn ?? 0;
      const turnB = b.turn_idx ?? b.turn ?? 0;
      if (turnA !== turnB) return turnA - turnB;

      const timeA = Number(a.timestamp || 0);
      const timeB = Number(b.timestamp || 0);
      if (timeA !== timeB) return timeA - timeB;

      const digA = String(a.digest || "");
      const digB = String(b.digest || "");
      return digA.localeCompare(digB);
    });

    const convergenceHash = CanonicalCodec.hash(
      {
        count: merged.length,
        cumulative_vc: cumulativeVc,
        first_digest: merged.length > 0 ? merged[0].digest : "",
        last_digest: merged.length > 0 ? merged[merged.length - 1].digest : "",
        tenant_id: tenantId,
      },
      CanonicalDomainTag.RECONCILER,
    );

    const endHr = process.hrtime.bigint();
    const elapsedUs = Number(endHr - startHr) / 1000.0;

    return {
      merged_spans: merged,
      total_spans: merged.length,
      deduplicated_count: dedupCount,
      concurrent_conflicts_retained: concurrentRetained,
      merged_vector_clock: cumulativeVc,
      convergence_hash: convergenceHash,
      execution_latency_us: elapsedUs,
    };
  }

  public reconcileAuthority(
    localAuthority: Record<string, any>,
    remoteAuthority: Record<string, any>,
    tenantId: string = "default",
  ): AuthorityReconciliationResult {
    const localState = String(localAuthority.state || "NOMINAL").toUpperCase();
    const remoteState = String(remoteAuthority.state || "NOMINAL").toUpperCase();

    const epochL = Number(localAuthority.epoch ?? 1);
    const epochR = Number(remoteAuthority.epoch ?? 1);

    let tripEnforced = false;
    let convergedState = "NOMINAL";
    let reason = "Authority states in agreement";

    // Trip Monotonicity Invariant: TRIPPED dominates all other states
    if (localState === "TRIPPED" || remoteState === "TRIPPED") {
      convergedState = "TRIPPED";
      tripEnforced = true;
      this._tripsPreservedCount++;
      if (localState !== remoteState) {
        reason = `Trip monotonicity enforced: preserved TRIPPED state across partition divergence (Local=${localState}, Remote=${remoteState})`;
      } else {
        reason = "Both partitions agreed on TRIPPED state";
      }
    } else if (localState === "DEGRADED" || remoteState === "DEGRADED") {
      convergedState = "DEGRADED";
      reason = "Partition converged to DEGRADED state";
    } else {
      convergedState = "NOMINAL";
    }

    const statesDiffer = localState !== remoteState;
    const reconciledEpoch = Math.max(epochL, epochR) + (statesDiffer ? 1 : 0);

    let revokedCount = 0;
    if (statesDiffer) {
      revokedCount = Number(localAuthority.active_permits || 0) + Number(remoteAuthority.active_permits || 0);
    }

    const hashPayload = {
      active_permits_revoked: revokedCount,
      converged_state: convergedState,
      previous_local_epoch: epochL,
      previous_remote_epoch: epochR,
      reconciled_epoch: reconciledEpoch,
      tenant_id: tenantId,
      trip_monotonicity_enforced: tripEnforced,
    };

    const reconciliationHash = CanonicalCodec.hash(hashPayload, CanonicalDomainTag.RECONCILER);

    const result: AuthorityReconciliationResult = {
      tenant_id: tenantId,
      converged_state: convergedState,
      trip_monotonicity_enforced: tripEnforced,
      previous_local_epoch: epochL,
      previous_remote_epoch: epochR,
      reconciled_epoch: reconciledEpoch,
      active_permits_revoked: revokedCount,
      reason,
      reconciled_at: Date.now() / 1000,
      reconciliation_hash: reconciliationHash,
    };

    this._reconciliations.push(result);
    return result;
  }

  public status(tenantId?: string): Record<string, any> {
    const list = tenantId
      ? this._reconciliations.filter((r) => r.tenant_id === tenantId)
      : this._reconciliations;

    return {
      total_reconciliations: list.length,
      trips_preserved_count: this._tripsPreservedCount,
      concurrent_spans_retained: this._concurrentSpansRetained,
      active_conflicts: this._activeConflicts,
      last_reconciliation: list.length > 0 ? list[list.length - 1] : null,
      status: this._activeConflicts === 0 ? "CONVERGED" : "RESOLVING_CONFLICTS",
    };
  }

  public reset(): void {
    this._reconciliations = [];
    this._tripsPreservedCount = 0;
    this._concurrentSpansRetained = 0;
    this._activeConflicts = 0;
  }
}

export const globalMeshReconciler = new MeshReconciler();
