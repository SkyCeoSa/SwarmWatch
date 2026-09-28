/**
 * Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072 - TypeScript Edition).
 * Replicated state machine binding execution permits to majority quorum consensus (Q >= floor(N/2) + 1).
 * Enforces fail-closed rejection on network partition, minority isolation, or leader split-brain.
 */

import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";
import { AuthorityState, SafetyAuthority, SafetyPermit, TenantQuota } from "./safety_authority.ts";

export const ConsensusPermitStatus = {
  ACCEPTED: "ACCEPTED",
  REJECTED: "REJECTED",
  QUORUM_LOSS: "QUORUM_LOSS",
  TRIPPED: "TRIPPED",
  EXPIRED: "EXPIRED",
} as const;

export type ConsensusPermitStatusEnum = typeof ConsensusPermitStatus[keyof typeof ConsensusPermitStatus];

export interface ConsensusLogEntryOptions {
  index: number;
  epoch: number;
  term: number;
  opType: string;
  payload: Record<string, unknown>;
  entryHash?: string;
  committed?: boolean;
  ackNodes?: string[];
  timestamp?: number;
}

export class ConsensusLogEntry {
  public readonly index: number;
  public readonly epoch: number;
  public readonly term: number;
  public readonly opType: string;
  public readonly payload: Record<string, unknown>;
  public readonly timestamp: number;
  public committed: boolean;
  public ackNodes: Set<string>;
  public readonly entryHash: string;

  constructor(opts: ConsensusLogEntryOptions) {
    this.index = opts.index;
    this.epoch = opts.epoch;
    this.term = opts.term;
    this.opType = opts.opType;
    this.payload = { ...opts.payload };
    this.timestamp = opts.timestamp !== undefined ? opts.timestamp : Date.now() / 1000.0;
    this.committed = Boolean(opts.committed);
    this.ackNodes = new Set(opts.ackNodes || []);
    this.entryHash = opts.entryHash || this.computeHash();
  }

  public computeHash(): string {
    const hashPayload = {
      epoch: this.epoch,
      index: this.index,
      op_type: this.opType,
      payload: this.payload,
      term: this.term,
      timestamp: Number(this.timestamp.toFixed(6)),
    };
    return CanonicalCodec.hash(hashPayload, CanonicalDomainTag.PERMIT);
  }

  public toDict(): Record<string, unknown> {
    return {
      index: this.index,
      epoch: this.epoch,
      term: this.term,
      op_type: this.opType,
      payload: this.payload,
      timestamp: this.timestamp,
      committed: this.committed,
      ack_nodes: Array.from(this.ackNodes).sort(),
      entry_hash: this.entryHash,
    };
  }

  public static fromDict(data: Record<string, unknown>): ConsensusLogEntry {
    return new ConsensusLogEntry({
      index: Number(data.index),
      epoch: Number(data.epoch),
      term: Number(data.term),
      opType: String(data.op_type),
      payload: (data.payload as Record<string, unknown>) || {},
      entryHash: data.entry_hash as string | undefined,
      committed: Boolean(data.committed),
      ackNodes: (data.ack_nodes as string[]) || [],
      timestamp: data.timestamp !== undefined ? Number(data.timestamp) : undefined,
    });
  }
}

export interface ConsensusPermitResultOptions {
  permitId: string;
  status: ConsensusPermitStatusEnum;
  epoch: number;
  logIndex: number;
  quorumAchieved: boolean;
  committedCost?: number;
  committedTokens?: number;
  rejectionReason?: string | null;
  permitHash?: string | null;
  nodeId?: string;
  ackCount?: number;
  quorumRequired?: number;
}

export class ConsensusPermitResult {
  public readonly permitId: string;
  public readonly status: ConsensusPermitStatusEnum;
  public readonly epoch: number;
  public readonly logIndex: number;
  public readonly quorumAchieved: boolean;
  public readonly committedCost: number;
  public readonly committedTokens: number;
  public readonly rejectionReason: string | null;
  public readonly permitHash: string | null;
  public readonly nodeId: string;
  public readonly ackCount: number;
  public readonly quorumRequired: number;

  constructor(opts: ConsensusPermitResultOptions) {
    this.permitId = opts.permitId;
    this.status = opts.status;
    this.epoch = opts.epoch;
    this.logIndex = opts.logIndex;
    this.quorumAchieved = opts.quorumAchieved;
    this.committedCost = opts.committedCost !== undefined ? opts.committedCost : 0.0;
    this.committedTokens = opts.committedTokens !== undefined ? opts.committedTokens : 0;
    this.rejectionReason = opts.rejectionReason || null;
    this.permitHash = opts.permitHash || null;
    this.nodeId = opts.nodeId || "node-1";
    this.ackCount = opts.ackCount !== undefined ? opts.ackCount : 1;
    this.quorumRequired = opts.quorumRequired !== undefined ? opts.quorumRequired : 1;
  }

  public get isAccepted(): boolean {
    return this.status === ConsensusPermitStatus.ACCEPTED && this.quorumAchieved;
  }

  public toDict(): Record<string, unknown> {
    return {
      permit_id: this.permitId,
      status: this.status,
      epoch: this.epoch,
      log_index: this.logIndex,
      quorum_achieved: this.quorumAchieved,
      committed_cost: this.committedCost,
      committed_tokens: this.committedTokens,
      rejection_reason: this.rejectionReason,
      permit_hash: this.permitHash,
      node_id: this.nodeId,
      ack_count: this.ackCount,
      quorum_required: this.quorumRequired,
    };
  }
}

export interface CommittedTenantStateOptions {
  tenantId: string;
  epoch: number;
  state: string;
  maxCost: number;
  maxTokens: number;
  maxTurns: number;
  spentCost: number;
  reservedCost: number;
  spentTokens: number;
  reservedTokens: number;
  spentTurns: number;
  lastCommittedIndex: number;
  tripReason?: string | null;
}

export class CommittedTenantState {
  public readonly tenantId: string;
  public readonly epoch: number;
  public readonly state: string;
  public readonly maxCost: number;
  public readonly maxTokens: number;
  public readonly maxTurns: number;
  public readonly spentCost: number;
  public readonly reservedCost: number;
  public readonly availableCost: number;
  public readonly spentTokens: number;
  public readonly reservedTokens: number;
  public readonly availableTokens: number;
  public readonly spentTurns: number;
  public readonly lastCommittedIndex: number;
  public readonly tripReason: string | null;

  constructor(opts: CommittedTenantStateOptions) {
    this.tenantId = opts.tenantId;
    this.epoch = opts.epoch;
    this.state = opts.state;
    this.maxCost = opts.maxCost;
    this.maxTokens = opts.maxTokens;
    this.maxTurns = opts.maxTurns;
    this.spentCost = opts.spentCost;
    this.reservedCost = opts.reservedCost;
    this.availableCost = Math.max(0, this.maxCost - (this.spentCost + this.reservedCost));
    this.spentTokens = opts.spentTokens;
    this.reservedTokens = opts.reservedTokens;
    this.availableTokens = Math.max(0, this.maxTokens - (this.spentTokens + this.reservedTokens));
    this.spentTurns = opts.spentTurns;
    this.lastCommittedIndex = opts.lastCommittedIndex;
    this.tripReason = opts.tripReason || null;
  }

  public toDict(): Record<string, unknown> {
    return {
      tenant_id: this.tenantId,
      epoch: this.epoch,
      state: this.state,
      max_cost: this.maxCost,
      max_tokens: this.maxTokens,
      max_turns: this.maxTurns,
      spent_cost: this.spentCost,
      reserved_cost: this.reservedCost,
      available_cost: this.availableCost,
      spent_tokens: this.spentTokens,
      reserved_tokens: this.reservedTokens,
      available_tokens: this.availableTokens,
      spent_turns: this.spentTurns,
      last_committed_index: this.lastCommittedIndex,
      trip_reason: this.tripReason,
    };
  }
}

export class ConsensusTripResult {
  public readonly tenantId: string;
  public readonly tripped: boolean;
  public readonly epoch: number;
  public readonly logIndex: number;
  public readonly quorumAchieved: boolean;
  public readonly reason: string;

  constructor(tenantId: string, tripped: boolean, epoch: number, logIndex: number, quorumAchieved: boolean, reason: string) {
    this.tenantId = tenantId;
    this.tripped = tripped;
    this.epoch = epoch;
    this.logIndex = logIndex;
    this.quorumAchieved = quorumAchieved;
    this.reason = reason;
  }

  public toDict(): Record<string, unknown> {
    return {
      tenant_id: this.tenantId,
      tripped: this.tripped,
      epoch: this.epoch,
      log_index: this.logIndex,
      quorum_achieved: this.quorumAchieved,
      reason: this.reason,
    };
  }
}

export class ConsensusRearmResult {
  public readonly tenantId: string;
  public readonly rearmed: boolean;
  public readonly oldEpoch: number;
  public readonly newEpoch: number;
  public readonly logIndex: number;
  public readonly quorumAchieved: boolean;

  constructor(tenantId: string, rearmed: boolean, oldEpoch: number, newEpoch: number, logIndex: number, quorumAchieved: boolean) {
    this.tenantId = tenantId;
    this.rearmed = rearmed;
    this.oldEpoch = oldEpoch;
    this.newEpoch = newEpoch;
    this.logIndex = logIndex;
    this.quorumAchieved = quorumAchieved;
  }

  public toDict(): Record<string, unknown> {
    return {
      tenant_id: this.tenantId,
      rearmed: this.rearmed,
      old_epoch: this.oldEpoch,
      new_epoch: this.newEpoch,
      log_index: this.logIndex,
      quorum_achieved: this.quorumAchieved,
    };
  }
}

export class ConsensusAuthorityBinding {
  public readonly nodeId: string;
  public clusterNodes: string[];
  private _epoch: number;
  private _term: number;
  private _committedIndex: number;
  private _log: ConsensusLogEntry[];
  private _isolatedNodes: Set<string>;
  private _authority: SafetyAuthority;
  private _pendingReservations: Map<string, { permitId: string; tenantId: string; costEstimate: number; tokensEstimate: number; logIndex: number }>;

  constructor(
    nodeId: string = "node-1",
    clusterNodes?: string[],
    initialEpoch: number = 1,
    underlyingAuthority?: SafetyAuthority,
  ) {
    this.nodeId = nodeId;
    const initialList = clusterNodes && clusterNodes.length > 0 ? [...clusterNodes] : [this.nodeId];
    if (!initialList.includes(this.nodeId)) {
      initialList.push(this.nodeId);
    }
    initialList.sort();
    this.clusterNodes = initialList;

    this._epoch = initialEpoch;
    this._term = 1;
    this._committedIndex = 0;
    this._log = [];
    this._isolatedNodes = new Set();
    this._authority = underlyingAuthority || new SafetyAuthority(this._epoch);
    this._pendingReservations = new Map();
  }

  public get quorumRequired(): number {
    const n = this.clusterNodes.length;
    return Math.floor(n / 2) + 1;
  }

  public get reachableNodes(): string[] {
    if (this._isolatedNodes.has(this.nodeId)) {
      return [this.nodeId];
    }
    return this.clusterNodes.filter((n) => !this._isolatedNodes.has(n));
  }

  public get hasQuorum(): boolean {
    return this.reachableNodes.length >= this.quorumRequired;
  }

  public get epoch(): number {
    return this._epoch;
  }

  public get term(): number {
    return this._term;
  }

  public get committedIndex(): number {
    return this._committedIndex;
  }

  public setClusterMembership(nodes: string[]): void {
    const newSet = new Set(nodes);
    newSet.add(this.nodeId);
    this.clusterNodes = Array.from(newSet).sort();
  }

  public simulatePartition(isolatedNodes: string[]): void {
    this._isolatedNodes = new Set(isolatedNodes);
  }

  public restorePartition(): void {
    this._isolatedNodes.clear();
  }

  public configureTenant(
    tenantId: string,
    maxCost: number = 100.0,
    maxTokens: number = 1_000_000,
    maxTurns: number = 1000,
  ): CommittedTenantState {
    this._authority.configureTenant(tenantId, maxCost, maxTokens, maxTurns);
    return this.readCommitted(tenantId);
  }

  public reserve(
    tenantId: string,
    action: string = "agent_turn",
    costEstimate: number = 0.0,
    tokensEstimate: number = 0,
    timeoutMs: number = 5000.0,
  ): ConsensusPermitResult {
    const permitId = `permit-${crypto.randomBytes(6).toString("hex")}`;
    const quorumNeeded = this.quorumRequired;
    const reachable = this.reachableNodes;

    // Minority Partition Check (Fail-Closed)
    if (reachable.length < quorumNeeded) {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.QUORUM_LOSS,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: false,
        rejectionReason: `Quorum loss: reachable nodes ${reachable.length} < required ${quorumNeeded}`,
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    }

    const quota = this._authority.getOrCreateQuota(tenantId);
    if (quota.state === AuthorityState.TRIPPED) {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.TRIPPED,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: true,
        rejectionReason: `Tenant circuit tripped: ${quota.tripReason}`,
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    }

    if (costEstimate > quota.availableCost()) {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.REJECTED,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: true,
        rejectionReason: `Insufficient cost budget: ${costEstimate.toFixed(4)} > ${quota.availableCost().toFixed(4)}`,
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    }

    if (tokensEstimate > quota.availableTokens()) {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.REJECTED,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: true,
        rejectionReason: `Insufficient token budget: ${tokensEstimate} > ${quota.availableTokens()}`,
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    }

    // Propose Log Entry
    const newIndex = this._log.length + 1;
    const payload = {
      action,
      cost_estimate: costEstimate,
      permit_id: permitId,
      tenant_id: tenantId,
      timeout_ms: timeoutMs,
      tokens_estimate: tokensEstimate,
    };

    const entry = new ConsensusLogEntry({
      index: newIndex,
      epoch: this._epoch,
      term: this._term,
      opType: "RESERVE",
      payload,
      ackNodes: reachable,
    });

    if (entry.ackNodes.size >= quorumNeeded) {
      entry.committed = true;
      this._log.push(entry);
      this._committedIndex = newIndex;

      const permit = this._authority.reserve(tenantId, action, costEstimate, tokensEstimate, timeoutMs, permitId);
      this._pendingReservations.set(permit.permitId, {
        permitId: permit.permitId,
        tenantId,
        costEstimate,
        tokensEstimate,
        logIndex: newIndex,
      });

      return new ConsensusPermitResult({
        permitId: permit.permitId,
        status: ConsensusPermitStatus.ACCEPTED,
        epoch: this._epoch,
        logIndex: newIndex,
        quorumAchieved: true,
        committedCost: costEstimate,
        committedTokens: tokensEstimate,
        permitHash: permit.permitHash,
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    } else {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.QUORUM_LOSS,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: false,
        rejectionReason: "Failed to achieve quorum consensus during replication",
        nodeId: this.nodeId,
        ackCount: entry.ackNodes.size,
        quorumRequired: quorumNeeded,
      });
    }
  }

  public commit(permitId: string, actualCost?: number, actualTokens?: number): ConsensusPermitResult {
    const reachable = this.reachableNodes;
    const quorumNeeded = this.quorumRequired;

    if (reachable.length < quorumNeeded) {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.QUORUM_LOSS,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: false,
        rejectionReason: "Quorum loss during permit commitment",
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    }

    const reservation = this._pendingReservations.get(permitId);
    const tenantId = reservation ? reservation.tenantId : "default";
    const cost = actualCost !== undefined ? actualCost : (reservation ? reservation.costEstimate : 0.0);
    const tokens = actualTokens !== undefined ? actualTokens : (reservation ? reservation.tokensEstimate : 0);

    const newIndex = this._log.length + 1;
    const entry = new ConsensusLogEntry({
      index: newIndex,
      epoch: this._epoch,
      term: this._term,
      opType: "COMMIT",
      payload: {
        permit_id: permitId,
        tenant_id: tenantId,
        actual_cost: cost,
        actual_tokens: tokens,
      },
      ackNodes: reachable,
    });

    entry.committed = true;
    this._log.push(entry);
    this._committedIndex = newIndex;

    this._authority.commit(permitId, cost, tokens);
    this._pendingReservations.delete(permitId);

    return new ConsensusPermitResult({
      permitId,
      status: ConsensusPermitStatus.ACCEPTED,
      epoch: this._epoch,
      logIndex: newIndex,
      quorumAchieved: true,
      committedCost: cost,
      committedTokens: tokens,
      nodeId: this.nodeId,
      ackCount: reachable.length,
      quorumRequired: quorumNeeded,
    });
  }

  public release(permitId: string, reason: string = "voluntary_release"): ConsensusPermitResult {
    const reachable = this.reachableNodes;
    const quorumNeeded = this.quorumRequired;

    if (reachable.length < quorumNeeded) {
      return new ConsensusPermitResult({
        permitId,
        status: ConsensusPermitStatus.QUORUM_LOSS,
        epoch: this._epoch,
        logIndex: this._committedIndex,
        quorumAchieved: false,
        rejectionReason: "Quorum loss during permit release",
        nodeId: this.nodeId,
        ackCount: reachable.length,
        quorumRequired: quorumNeeded,
      });
    }

    const reservation = this._pendingReservations.get(permitId);
    const tenantId = reservation ? reservation.tenantId : "default";

    const newIndex = this._log.length + 1;
    const entry = new ConsensusLogEntry({
      index: newIndex,
      epoch: this._epoch,
      term: this._term,
      opType: "RELEASE",
      payload: { permit_id: permitId, tenant_id: tenantId, reason },
      ackNodes: reachable,
    });

    entry.committed = true;
    this._log.push(entry);
    this._committedIndex = newIndex;

    this._authority.release(permitId, reason);
    this._pendingReservations.delete(permitId);

    return new ConsensusPermitResult({
      permitId,
      status: ConsensusPermitStatus.ACCEPTED,
      epoch: this._epoch,
      logIndex: newIndex,
      quorumAchieved: true,
      nodeId: this.nodeId,
      ackCount: reachable.length,
      quorumRequired: quorumNeeded,
    });
  }

  public trip(tenantId: string, reason: string): ConsensusTripResult {
    const reachable = this.reachableNodes;
    const quorumNeeded = this.quorumRequired;

    if (reachable.length < quorumNeeded) {
      return new ConsensusTripResult(
        tenantId,
        false,
        this._epoch,
        this._committedIndex,
        false,
        "Quorum loss: cannot commit circuit trip across minority partition",
      );
    }

    const newIndex = this._log.length + 1;
    const entry = new ConsensusLogEntry({
      index: newIndex,
      epoch: this._epoch,
      term: this._term,
      opType: "TRIP",
      payload: { tenant_id: tenantId, reason },
      ackNodes: reachable,
    });

    entry.committed = true;
    this._log.push(entry);
    this._committedIndex = newIndex;

    this._authority.trip(reason, tenantId);

    return new ConsensusTripResult(tenantId, true, this._epoch, newIndex, true, reason);
  }

  public rearm(tenantId: string, newEpoch?: number): ConsensusRearmResult {
    const reachable = this.reachableNodes;
    const quorumNeeded = this.quorumRequired;
    const oldEp = this._epoch;

    if (reachable.length < quorumNeeded) {
      return new ConsensusRearmResult(tenantId, false, oldEp, oldEp, this._committedIndex, false);
    }

    let targetEpoch = newEpoch !== undefined ? newEpoch : oldEp + 1;
    if (targetEpoch <= oldEp) {
      targetEpoch = oldEp + 1;
    }

    const newIndex = this._log.length + 1;
    const entry = new ConsensusLogEntry({
      index: newIndex,
      epoch: targetEpoch,
      term: this._term + 1,
      opType: "REARM",
      payload: { tenant_id: tenantId, old_epoch: oldEp, new_epoch: targetEpoch },
      ackNodes: reachable,
    });

    entry.committed = true;
    this._log.push(entry);
    this._committedIndex = newIndex;
    this._epoch = targetEpoch;
    this._term += 1;

    this._authority.rearm(tenantId, targetEpoch);

    return new ConsensusRearmResult(tenantId, true, oldEp, targetEpoch, newIndex, true);
  }

  public readCommitted(tenantId: string): CommittedTenantState {
    const quota = this._authority.getOrCreateQuota(tenantId);
    return new CommittedTenantState({
      tenantId,
      epoch: this._epoch,
      state: quota.state,
      maxCost: quota.maxCost,
      maxTokens: quota.maxTokens,
      maxTurns: quota.maxTurns,
      spentCost: quota.spentCost,
      reservedCost: quota.reservedCost,
      spentTokens: quota.spentTokens,
      reservedTokens: quota.reservedTokens,
      spentTurns: quota.spentTurns,
      lastCommittedIndex: this._committedIndex,
      tripReason: quota.tripReason,
    });
  }

  public replicateToPeer(peer: ConsensusAuthorityBinding): number {
    let applied = 0;
    for (const entry of this._log) {
      if (entry.committed && entry.index > peer.committedIndex) {
        peer.applyReplicatedEntry(entry);
        applied += 1;
      }
    }
    return applied;
  }

  public applyReplicatedEntry(entry: ConsensusLogEntry): boolean {
    if (entry.index <= this._committedIndex) {
      return true;
    }

    if (entry.entryHash !== entry.computeHash()) {
      return false;
    }

    this._log.push(entry);
    this._committedIndex = entry.index;
    if (entry.epoch > this._epoch) {
      this._epoch = entry.epoch;
    }
    if (entry.term > this._term) {
      this._term = entry.term;
    }

    const op = entry.opType;
    const p = entry.payload;
    const tid = (p.tenant_id as string) || "default";

    if (op === "RESERVE") {
      const pid = p.permit_id as string | undefined;
      const cost = Number(p.cost_estimate || 0.0);
      const tokens = Number(p.tokens_estimate || 0);
      const timeout = Number(p.timeout_ms || 5000.0);
      const action = (p.action as string) || "agent_turn";
      this._authority.reserve(tid, action, cost, tokens, timeout, pid);
      if (pid) {
        this._pendingReservations.set(pid, {
          permitId: pid,
          tenantId: tid,
          costEstimate: cost,
          tokensEstimate: tokens,
          logIndex: entry.index,
        });
      }
    } else if (op === "COMMIT") {
      const pid = (p.permit_id as string) || "";
      const cost = p.actual_cost !== undefined ? Number(p.actual_cost) : undefined;
      const tokens = p.actual_tokens !== undefined ? Number(p.actual_tokens) : undefined;
      this._authority.commit(pid, cost, tokens);
      this._pendingReservations.delete(pid);
    } else if (op === "RELEASE") {
      const pid = (p.permit_id as string) || "";
      const reason = (p.reason as string) || "replicated_release";
      this._authority.release(pid, reason);
      this._pendingReservations.delete(pid);
    } else if (op === "TRIP") {
      const reason = (p.reason as string) || "replicated_trip";
      this._authority.trip(reason, tid);
    } else if (op === "REARM") {
      const newEp = p.new_epoch !== undefined ? Number(p.new_epoch) : undefined;
      this._authority.rearm(tid, newEp);
    }

    return true;
  }

  public exportStatus(): Record<string, unknown> {
    return {
      node_id: this.nodeId,
      cluster_nodes: this.clusterNodes,
      reachable_nodes: this.reachableNodes,
      quorum_required: this.quorumRequired,
      has_quorum: this.hasQuorum,
      epoch: this._epoch,
      term: this._term,
      committed_index: this._committedIndex,
      total_log_entries: this._log.length,
      isolated_nodes: Array.from(this._isolatedNodes).sort(),
    };
  }
}

export const globalConsensusAuthority = new ConsensusAuthorityBinding();
