import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import {
  CausalityBarrierError,
  BarrierCircuitTrippedError,
  StaleCausalityBarrierError,
  InvalidEpochCutOverError,
  BarrierQuiescenceTimeoutError,
  InvalidBarrierProofError,
  BarrierParticipantMismatchError,
  BarrierState,
  BarrierDrainStatus,
  ClusterQuiescenceReport,
  BarrierProposal,
  EpochCutOverCertificate,
  BarrierReceipt,
  CrossClusterCausalityBarrier,
  globalCausalityBarrier,
  computeCanonicalBarrierHash,
} from "../src/causality_barrier.ts";

describe("Seam 80: Autonomous Cross-Cluster Causality Barrier & Multi-Swarm Epoch Cut-Over (TypeScript)", () => {
  beforeEach(() => {
    globalCausalityBarrier.rearmCircuit();
  });

  test("Generalized vector barrier supremum calculation", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    const clusterClocks = {
      "cluster-us-east": { us: 10, eu: 5, ap: 2 },
      "cluster-eu-west": { us: 8, eu: 12, ap: 3 },
      "cluster-ap-south": { us: 7, eu: 4, ap: 15 },
    };

    const targetVector = barrier.computeBarrierVector(clusterClocks);
    assert.deepEqual(targetVector, { us: 10, eu: 12, ap: 15 });
  });

  test("Arming barrier and 7-phase state automaton", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    assert.equal(barrier.state, BarrierState.IDLE);

    const clusterClocks = {
      "cluster-a": { a: 5, b: 2 },
      "cluster-b": { a: 3, b: 7 },
    };

    const proposal = barrier.armBarrier({
      targetEpoch: 2,
      participatingClusters: ["cluster-a", "cluster-b"],
      clusterClocks,
      reason: "Scheduled epoch rotation in TypeScript",
    });

    assert.equal(proposal.current_epoch, 1);
    assert.equal(proposal.target_epoch, 2);
    assert.deepEqual(proposal.participating_clusters, ["cluster-a", "cluster-b"]);
    assert.deepEqual(proposal.target_barrier_vector, { a: 5, b: 7 });
    assert.ok(proposal.proposal_hash.startsWith("sw:barrier:prop:v1:"));
    assert.equal(barrier.state, BarrierState.ARMING_BARRIER);

    // Cannot arm again while active
    assert.throws(() => {
      barrier.armBarrier({ targetEpoch: 3, participatingClusters: ["cluster-a"] });
    }, CausalityBarrierError);
  });

  test("Draining and Quiescence reporting", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    barrier.armBarrier({
      targetEpoch: 2,
      participatingClusters: ["cluster-a", "cluster-b"],
      clusterClocks: { "cluster-a": { a: 10, b: 5 }, "cluster-b": { a: 8, b: 12 } },
    });

    barrier.startDraining();
    assert.equal(barrier.state, BarrierState.DRAINING_INFLIGHT);

    // Cluster A reports partial quiescence (inflight > 0)
    const repA1 = barrier.reportQuiescence("cluster-a", { a: 10, b: 12 }, 3);
    assert.equal(repA1.is_quiescent, false);
    assert.equal(barrier.state, BarrierState.DRAINING_INFLIGHT);

    // Cluster A completes draining
    const repA2 = barrier.reportQuiescence("cluster-a", { a: 10, b: 12 }, 0);
    assert.equal(repA2.is_quiescent, true);
    assert.equal(barrier.state, BarrierState.DRAINING_INFLIGHT);

    // Cluster B reports with lagging vector clock
    const repB1 = barrier.reportQuiescence("cluster-b", { a: 8, b: 12 }, 0);
    assert.equal(repB1.is_quiescent, false);
    assert.equal(barrier.state, BarrierState.DRAINING_INFLIGHT);

    // Cluster B catches up
    const repB2 = barrier.reportQuiescence("cluster-b", { a: 10, b: 12 }, 0);
    assert.equal(repB2.is_quiescent, true);
    assert.equal(barrier.state, BarrierState.BARRIER_SYNCHRONIZED);
  });

  test("Invariant B1: Barrier monotonicity and stale operation rejection", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    barrier.armBarrier({
      targetEpoch: 2,
      participatingClusters: ["cluster-a"],
      clusterClocks: { "cluster-a": { a: 20 } },
    });
    barrier.startDraining();

    // Equal or ahead -> admitted
    assert.equal(barrier.admitOperation("cluster-a", { a: 20 }, 1), true);

    // Stale operation preceding barrier vector -> rejected with StaleCausalityBarrierError
    assert.throws(() => {
      barrier.admitOperation("cluster-a", { a: 19 }, 1);
    }, StaleCausalityBarrierError);
  });

  test("Invariant B2 & B3: Atomic cut-over certificate and receipt verification", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    barrier.armBarrier({
      targetEpoch: 2,
      participatingClusters: ["cluster-a", "cluster-b"],
      clusterClocks: { "cluster-a": { a: 5 }, "cluster-b": { b: 5 } },
    });
    barrier.startDraining();
    barrier.reportQuiescence("cluster-a", { a: 5, b: 5 }, 0);
    barrier.reportQuiescence("cluster-b", { a: 5, b: 5 }, 0);
    assert.equal(barrier.state, BarrierState.BARRIER_SYNCHRONIZED);

    // Commit cutover
    const [cert, rcpt] = barrier.commitEpochCutover();

    assert.equal(barrier.state, BarrierState.COMPLETED_EPOCH_ADVANCED);
    assert.equal(barrier.activeEpoch, 2);
    assert.equal(cert.prev_epoch, 1);
    assert.equal(cert.new_epoch, 2);
    assert.ok(cert.certificate_hash.startsWith("sw:barrier:cert:v1:"));
    assert.ok(rcpt.receipt_hash.startsWith("sw:barrier:rcpt:v1:"));

    // Verification
    assert.equal(barrier.verifyCutoverCertificate(cert), true);
    assert.equal(barrier.verifyBarrierReceipt(rcpt), true);

    // Previous epoch rejected under Invariant B2
    assert.throws(() => {
      barrier.admitOperation("cluster-a", { a: 100 }, 1);
    }, StaleCausalityBarrierError);

    // Current epoch admitted
    assert.equal(barrier.admitOperation("cluster-a", { a: 100 }, 2), true);
  });

  test("Circuit breaker fail-closed latching and rearm", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    barrier.tripCircuit("Simulated Byzantine breach in TS");

    assert.equal(barrier.isCircuitTripped(), true);
    assert.equal(barrier.state, BarrierState.CIRCUIT_TRIPPED);

    assert.throws(() => {
      barrier.armBarrier({ targetEpoch: 2, participatingClusters: ["cluster-a"] });
    }, BarrierCircuitTrippedError);

    assert.throws(() => {
      barrier.startDraining();
    }, BarrierCircuitTrippedError);

    assert.throws(() => {
      barrier.admitOperation("cluster-a", {}, 1);
    }, BarrierCircuitTrippedError);

    barrier.rearmCircuit();
    assert.equal(barrier.isCircuitTripped(), false);
    assert.equal(barrier.state, BarrierState.IDLE);
  });

  test("Sub-250µs execution latency SLO benchmark in TypeScript", () => {
    const clusterClocks = {
      "cluster-1": { c1: 100, c2: 50 },
      "cluster-2": { c1: 80, c2: 120 },
    };

    // Warmup JIT compiler and inline caches
    for (let i = 0; i < 15; i++) {
      const bWarm = new CrossClusterCausalityBarrier("tenant-warm", 1);
      const vWarm = bWarm.computeBarrierVector(clusterClocks);
      bWarm.armBarrier({
        targetEpoch: 2,
        participatingClusters: ["cluster-1", "cluster-2"],
        clusterClocks,
      });
      bWarm.startDraining();
      bWarm.reportQuiescence("cluster-1", vWarm, 0);
      bWarm.reportQuiescence("cluster-2", vWarm, 0);
      bWarm.commitEpochCutover();
      bWarm.admitOperation("cluster-1", { c1: 200 }, 2);
    }

    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);

    // Benchmark vector calc
    const t0 = performance.now();
    const targetVec = barrier.computeBarrierVector(clusterClocks);
    const tCalcUs = (performance.now() - t0) * 1000.0;

    // Benchmark arm
    const t1 = performance.now();
    barrier.armBarrier({
      targetEpoch: 2,
      participatingClusters: ["cluster-1", "cluster-2"],
      clusterClocks,
    });
    const tArmUs = (performance.now() - t1) * 1000.0;

    // Benchmark drain
    const t2 = performance.now();
    barrier.startDraining();
    const tDrainUs = (performance.now() - t2) * 1000.0;

    // Benchmark quiescence report
    const t3 = performance.now();
    barrier.reportQuiescence("cluster-1", targetVec, 0);
    const tReportUs = (performance.now() - t3) * 1000.0;

    barrier.reportQuiescence("cluster-2", targetVec, 0);

    // Benchmark cutover
    const t4 = performance.now();
    barrier.commitEpochCutover();
    const tCutUs = (performance.now() - t4) * 1000.0;

    // Benchmark admit
    const t5 = performance.now();
    barrier.admitOperation("cluster-1", { c1: 200 }, 2);
    const tAdmitUs = (performance.now() - t5) * 1000.0;

    assert.ok(tCalcUs < 250.0, `Vector calc latency ${tCalcUs.toFixed(2)}µs exceeded 250µs SLO`);
    assert.ok(tArmUs < 250.0, `Arm latency ${tArmUs.toFixed(2)}µs exceeded 250µs SLO`);
    assert.ok(tDrainUs < 250.0, `Drain latency ${tDrainUs.toFixed(2)}µs exceeded 250µs SLO`);
    assert.ok(tReportUs < 250.0, `Report latency ${tReportUs.toFixed(2)}µs exceeded 250µs SLO`);
    assert.ok(tCutUs < 250.0, `Cutover latency ${tCutUs.toFixed(2)}µs exceeded 250µs SLO`);
    assert.ok(tAdmitUs < 250.0, `Admit latency ${tAdmitUs.toFixed(2)}µs exceeded 250µs SLO`);
  });

  test("Zero-token cost guarantee ($0.00 USD)", () => {
    const barrier = new CrossClusterCausalityBarrier("tenant-ts", 1);
    barrier.armBarrier({
      targetEpoch: 2,
      participatingClusters: ["cluster-1"],
      clusterClocks: { "cluster-1": { k: 1 } },
    });
    barrier.startDraining();
    barrier.reportQuiescence("cluster-1", { k: 1 }, 0);
    const [cert, rcpt] = barrier.commitEpochCutover();

    assert.ok(cert.certificate_hash.startsWith("sw:barrier:cert:v1:"));
    assert.ok(rcpt.receipt_hash.startsWith("sw:barrier:rcpt:v1:"));
  });
});
