/**
 * Test Suite for Seam 80 (ADR 0091) Autonomous Cross-Cluster Causality Barrier &
 * Multi-Swarm Epoch Cut-Over Synchronization Live Simulation in TypeScript.
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import {
  CausalityBarrierError,
  BarrierCircuitTrippedError,
  StaleCausalityBarrierError,
  InvalidEpochCutOverError,
  BarrierState,
  BarrierDrainStatus,
  ClusterQuiescenceReport,
  BarrierProposal,
  EpochCutOverCertificate,
  BarrierReceipt,
  CrossClusterCausalityBarrier,
} from "../src/causality_barrier.ts";

test("Seam 80 Live Simulation: Certify all 8 operational invariants in TypeScript", () => {
  const tSimStart = performance.now();
  const barrier = new CrossClusterCausalityBarrier("tenant-prod-ts", 1);

  // Phase 0
  const st0 = barrier.status();
  assert.equal(st0.active_epoch, 1);
  assert.equal(st0.state, BarrierState.IDLE);
  assert.equal(st0.circuit_tripped, false);

  // Phase 1: Arming & Supremum
  const clusters = ["cluster-us-east", "cluster-eu-west", "cluster-ap-south"];
  const clusterClocks = {
    "cluster-us-east": { us: 120, eu: 95, ap: 40 },
    "cluster-eu-west": { us: 115, eu: 130, ap: 45 },
    "cluster-ap-south": { us: 110, eu: 100, ap: 80 },
  };

  const t0 = performance.now();
  const targetVector = barrier.computeBarrierVector(clusterClocks);
  const calcUs = (performance.now() - t0) * 1000.0;
  assert.deepEqual(targetVector, { us: 120, eu: 130, ap: 80 });

  const t1 = performance.now();
  const proposal = barrier.armBarrier({
    targetEpoch: 2,
    participatingClusters: clusters,
    clusterClocks,
    reason: "Scheduled epoch advancement and Byzantine committee rotation in TS",
  });
  const armUs = (performance.now() - t1) * 1000.0;
  assert.equal(proposal.target_epoch, 2);
  assert.ok(proposal.proposal_hash.startsWith("sw:barrier:prop:v1:"));
  assert.equal(barrier.state, BarrierState.ARMING_BARRIER);

  // Phase 2: Draining & Invariant B1
  barrier.startDraining();
  assert.equal(barrier.state, BarrierState.DRAINING_INFLIGHT);

  const repUs1 = barrier.reportQuiescence("cluster-us-east", targetVector, 5);
  assert.equal(repUs1.is_quiescent, false);

  // Pre-barrier operation rejected
  assert.throws(() => {
    barrier.admitOperation("cluster-us-east", { us: 110, eu: 100, ap: 40 }, 1);
  }, StaleCausalityBarrierError);

  const repUs2 = barrier.reportQuiescence("cluster-us-east", targetVector, 0);
  assert.equal(repUs2.is_quiescent, true);

  const repEu = barrier.reportQuiescence("cluster-eu-west", targetVector, 0);
  assert.equal(repEu.is_quiescent, true);

  const t2 = performance.now();
  const repAp = barrier.reportQuiescence("cluster-ap-south", targetVector, 0);
  const syncUs = (performance.now() - t2) * 1000.0;
  assert.equal(repAp.is_quiescent, true);
  assert.equal(barrier.state, BarrierState.BARRIER_SYNCHRONIZED);

  // Phase 3 & 4: Committing Cut-Over (Invariants B2 & B3)
  const t3 = performance.now();
  const [cert, rcpt] = barrier.commitEpochCutover();
  const cutUs = (performance.now() - t3) * 1000.0;

  assert.equal(barrier.activeEpoch, 2);
  assert.equal(cert.prev_epoch, 1);
  assert.equal(cert.new_epoch, 2);
  assert.ok(cert.certificate_hash.startsWith("sw:barrier:cert:v1:"));
  assert.ok(rcpt.receipt_hash.startsWith("sw:barrier:rcpt:v1:"));

  assert.equal(barrier.verifyCutoverCertificate(cert), true);
  assert.equal(barrier.verifyBarrierReceipt(rcpt), true);

  // Old epoch rejected
  assert.throws(() => {
    barrier.admitOperation("cluster-us-east", { us: 200 }, 1);
  }, StaleCausalityBarrierError);

  // Active epoch admitted
  assert.equal(barrier.admitOperation("cluster-us-east", { us: 200 }, 2), true);

  // Circuit breaker fail-closed
  barrier.tripCircuit("Manual Byzantine Anomaly Simulation in TS");
  assert.equal(barrier.isCircuitTripped(), true);
  assert.throws(() => {
    barrier.armBarrier({ targetEpoch: 3, participatingClusters: ["cluster-us-east"] });
  }, BarrierCircuitTrippedError);

  barrier.rearmCircuit();
  assert.equal(barrier.isCircuitTripped(), false);
  assert.equal(barrier.state, BarrierState.IDLE);

  const durationMs = performance.now() - tSimStart;
  assert.ok(durationMs > 0);
  assert.equal(barrier.activeEpoch, 2);
});
