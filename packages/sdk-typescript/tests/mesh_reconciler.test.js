/**
 * Unit and integration tests for Partition Reconciliation & Conflict Lattice Engine (Seam 57 / ADR 0068).
 * Follows Node 22 native test runner conventions and strict type-stripping rules (no 'as ...' type casts).
 */

import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  LatticeVerdict,
  compareVectorClocks,
  mergeVectorClocks,
  MeshReconciler,
  globalMeshReconciler,
} from "../src/mesh_reconciler.ts";

describe("MeshReconciler (Seam 57 / ADR 0068)", () => {
  let reconciler;

  beforeEach(() => {
    reconciler = new MeshReconciler();
    globalMeshReconciler.reset();
  });

  test("compareVectorClocks computes correct lattice verdicts", () => {
    // Identical
    assert.equal(
      compareVectorClocks({ nodeA: 2, nodeB: 3 }, { nodeA: 2, nodeB: 3 }),
      LatticeVerdict.IDENTICAL,
    );

    // Dominates
    assert.equal(
      compareVectorClocks({ nodeA: 3, nodeB: 3 }, { nodeA: 2, nodeB: 3 }),
      LatticeVerdict.DOMINATES,
    );
    assert.equal(
      compareVectorClocks({ nodeA: 2, nodeB: 3, nodeC: 1 }, { nodeA: 2, nodeB: 3 }),
      LatticeVerdict.DOMINATES,
    );

    // Dominated
    assert.equal(
      compareVectorClocks({ nodeA: 2, nodeB: 2 }, { nodeA: 2, nodeB: 3 }),
      LatticeVerdict.DOMINATED,
    );

    // Concurrent (Split-Brain)
    assert.equal(
      compareVectorClocks({ nodeA: 3, nodeB: 1 }, { nodeA: 2, nodeB: 2 }),
      LatticeVerdict.CONCURRENT,
    );
    assert.equal(
      compareVectorClocks({ nodeA: 1 }, { nodeB: 1 }),
      LatticeVerdict.CONCURRENT,
    );
  });

  test("mergeVectorClocks computes least upper bound (join)", () => {
    const vcA = { nodeA: 5, nodeB: 2, nodeC: 0 };
    const vcB = { nodeA: 3, nodeB: 4, nodeD: 1 };
    const merged = mergeVectorClocks(vcA, vcB);
    assert.deepEqual(merged, { nodeA: 5, nodeB: 4, nodeC: 0, nodeD: 1 });
  });

  test("diff generates valid ReconciliationPlan with dominant and concurrent states", () => {
    // Local dominates remote
    const localState = {
      vector_clock: { nodeA: 3, nodeB: 2 },
      keys: { k1: 1, k2: 2 },
      max_sequence: 100,
    };
    const remoteState = {
      vector_clock: { nodeA: 2, nodeB: 2 },
      keys: { k1: 1 },
      max_sequence: 90,
    };

    const plan = reconciler.diff(localState, remoteState, "tenant-alpha");
    assert.equal(plan.verdict, LatticeVerdict.DOMINATES);
    assert.ok(plan.divergent_keys.includes("k2"));
    assert.equal(plan.missing_remote_events, 10);
    assert.equal(plan.missing_local_events, 0);
    assert.ok(plan.action_items.includes("PULL_REMOTE_FROM_LOCAL"));
    assert.ok(plan.plan_hash);

    // Concurrent divergence
    const localConc = {
      vector_clock: { nodeA: 3, nodeB: 1 },
      keys: { k_loc: 1 },
      max_sequence: 50,
    };
    const remoteConc = {
      vector_clock: { nodeA: 1, nodeB: 3 },
      keys: { k_rem: 1 },
      max_sequence: 55,
    };

    const planConc = reconciler.diff(localConc, remoteConc, "tenant-alpha");
    assert.equal(planConc.verdict, LatticeVerdict.CONCURRENT);
    assert.equal(planConc.conflict_detected, true);
    assert.ok(planConc.action_items.includes("RESOLVE_CONCURRENT_LATTICE_PRESERVE_PROVENANCE"));
  });

  test("mergeEvidence deduplicates identical events and preserves concurrent fork provenance", () => {
    const sharedEvt = {
      span_id: "span-shared-1",
      turn_idx: 1,
      agent_id: "agent-lead",
      payload: { action: "plan" },
      vector_clock: { nodeA: 1 },
      timestamp: 100.0,
    };

    const localEvt = {
      span_id: "span-loc-1",
      turn_idx: 2,
      agent_id: "agent-worker-1",
      payload: { action: "compute" },
      vector_clock: { nodeA: 2 },
      timestamp: 105.0,
      is_concurrent_fork: false,
      origin_peer: "peer_A",
    };

    const remoteEvt = {
      span_id: "span-rem-fork-1",
      turn_idx: 2,
      agent_id: "agent-worker-2",
      payload: { action: "compute_alt" },
      vector_clock: { nodeB: 2 },
      timestamp: 106.0,
      is_concurrent_fork: true,
      origin_peer: "peer_B",
    };

    const result = reconciler.mergeEvidence(
      [sharedEvt, localEvt],
      [sharedEvt, remoteEvt],
      "tenant-prod",
    );

    assert.equal(result.total_spans, 3);
    assert.equal(result.deduplicated_count, 1);
    assert.equal(result.concurrent_conflicts_retained, 1);
    assert.deepEqual(result.merged_vector_clock, { nodeA: 2, nodeB: 2 });
    assert.ok(result.convergence_hash);
    assert.ok(result.execution_latency_us >= 0);

    const forkRetained = result.merged_spans.find((s) => s.span_id === "span-rem-fork-1");
    assert.ok(forkRetained);
    assert.equal(forkRetained.is_concurrent_fork, true);
    assert.equal(forkRetained.provenance_peer, "peer_B");
  });

  test("reconcileAuthority strictly enforces Trip Monotonicity invariant", () => {
    // Scenario A: Local TRIPPED, Remote NOMINAL -> TRIPPED dominates
    const resA = reconciler.reconcileAuthority(
      { state: "TRIPPED", epoch: 3, active_permits: 2 },
      { state: "NOMINAL", epoch: 4, active_permits: 5 },
      "tenant-prod",
    );
    assert.equal(resA.converged_state, "TRIPPED");
    assert.equal(resA.trip_monotonicity_enforced, true);
    assert.equal(resA.reconciled_epoch, 5); // max(3, 4) + 1
    assert.equal(resA.active_permits_revoked, 7);

    // Scenario B: Local NOMINAL, Remote TRIPPED -> TRIPPED dominates
    const resB = reconciler.reconcileAuthority(
      { state: "NOMINAL", epoch: 10, active_permits: 1 },
      { state: "TRIPPED", epoch: 8, active_permits: 0 },
      "tenant-prod",
    );
    assert.equal(resB.converged_state, "TRIPPED");
    assert.equal(resB.trip_monotonicity_enforced, true);
    assert.equal(resB.reconciled_epoch, 11);

    // Scenario C: Both NOMINAL and equal epoch -> NOMINAL preserved
    const resC = reconciler.reconcileAuthority(
      { state: "NOMINAL", epoch: 12, active_permits: 3 },
      { state: "NOMINAL", epoch: 12, active_permits: 3 },
      "tenant-prod",
    );
    assert.equal(resC.converged_state, "NOMINAL");
    assert.equal(resC.trip_monotonicity_enforced, false);
    assert.equal(resC.reconciled_epoch, 12);
    assert.equal(resC.active_permits_revoked, 0);
  });

  test("status reports diagnostics and reset clears state", () => {
    reconciler.reconcileAuthority(
      { state: "TRIPPED", epoch: 1 },
      { state: "NOMINAL", epoch: 1 },
      "tenant-beta",
    );

    const st = reconciler.status("tenant-beta");
    assert.equal(st.total_reconciliations, 1);
    assert.equal(st.trips_preserved_count, 1);
    assert.ok(st.last_reconciliation);

    reconciler.reset();
    const stReset = reconciler.status("tenant-beta");
    assert.equal(stReset.total_reconciliations, 0);
    assert.equal(stReset.trips_preserved_count, 0);
  });
});
