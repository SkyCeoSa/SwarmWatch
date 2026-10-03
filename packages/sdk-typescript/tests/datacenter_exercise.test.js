/**
 * Unit and integration test suite for Datacenter Failure Exercise & Region Loss Fault Injection (ADR 0081 / Seam 70).
 * Plain ES module for Node.js native execution.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  DatacenterExerciseError,
  MinorityPartitionBlockedError,
  SplitBrainViolationError,
  SafetyMonotonicityViolationError,
  FailureScenarioType,
  ExercisePhase,
  NodeRole,
  ClusterNode,
  NetworkMatrix,
  PermitAttemptResult,
  FailureExerciseEvent,
  FailureExerciseReport,
  DatacenterFailureOrchestrator,
  globalDatacenterOrchestrator,
} from "../src/datacenter_exercise.ts";

test("ClusterNode and NetworkMatrix reachability and severance", () => {
  const node = new ClusterNode("n1", "us-east-1", NodeRole.LEADER);
  const d = node.toDict();
  assert.equal(d.node_id, "n1");
  assert.equal(d.role, "LEADER");

  const reconstructed = ClusterNode.fromDict(d);
  assert.equal(reconstructed.region_id, "us-east-1");
  assert.equal(reconstructed.role, NodeRole.LEADER);

  const matrix = new NetworkMatrix(["us-east-1", "us-west-2", "eu-central-1"]);
  assert.equal(matrix.isReachable("us-east-1", "us-west-2"), true);
  assert.equal(matrix.isReachable("us-east-1", "eu-central-1"), true);

  matrix.severRegion("eu-central-1");
  assert.equal(matrix.isReachable("us-east-1", "us-west-2"), true);
  assert.equal(matrix.isReachable("us-east-1", "eu-central-1"), false);
  assert.equal(matrix.isReachable("eu-central-1", "us-east-1"), false);

  matrix.restoreAll();
  assert.equal(matrix.isReachable("eu-central-1", "us-east-1"), true);
});

test("Majority quorum calculation", () => {
  const nodes = [
    new ClusterNode("n1", "us-east-1"),
    new ClusterNode("n2", "us-east-1"),
    new ClusterNode("n3", "us-west-2"),
    new ClusterNode("n4", "us-west-2"),
    new ClusterNode("n5", "eu-central-1"),
  ];
  const orchestrator = new DatacenterFailureOrchestrator(nodes);

  // In nominal state, all 5 nodes have majority quorum
  for (const n of ["n1", "n2", "n3", "n4", "n5"]) {
    assert.equal(orchestrator.hasMajorityQuorum(n), true);
  }

  // Sever eu-central-1 (1 node severed out of 5)
  orchestrator.injectPartition(["eu-central-1"]);

  // us-east-1 (2) + us-west-2 (2) = 4 reachable nodes >= 3 (majority)
  assert.equal(orchestrator.hasMajorityQuorum("n1"), true);
  assert.equal(orchestrator.hasMajorityQuorum("n3"), true);

  // eu-central-1 (1 node) has only 1 reachable node < 3 -> minority partition
  assert.equal(orchestrator.hasMajorityQuorum("n5"), false);
});

test("Nominal permit grant", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  const res = orchestrator.requestPermit("node-us-east-1", "tenant-alpha", 100);
  assert.equal(res.granted, true);
  assert.equal(res.node_id, "node-us-east-1");
  assert.notEqual(res.permit_id, null);
  assert.match(res.reason, /Majority quorum verified/);
});

test("Minority partition blocked fail-closed", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  orchestrator.injectPartition(["eu-central-1"]);

  // Attempting permit on isolated node MUST throw MinorityPartitionBlockedError
  assert.throws(
    () => {
      orchestrator.requestPermit("node-eu-cent-1", "tenant-alpha", 50);
    },
    (err) => err instanceof MinorityPartitionBlockedError
  );
});

test("Majority continues during minority severance", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  orchestrator.injectPartition(["eu-central-1"]);

  // Nodes in us-east-1 and us-west-2 continue issuing permits smoothly
  const res1 = orchestrator.requestPermit("node-us-east-1", "tenant-alpha", 25);
  assert.equal(res1.granted, true);

  const res2 = orchestrator.requestPermit("node-us-west-1", "tenant-alpha", 30);
  assert.equal(res2.granted, true);
});

test("Safety trip monotonicity across partitions", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  orchestrator.injectPartition(["eu-central-1"]);

  // Trigger trip from a majority node
  const tripRes = orchestrator.broadcastTrip("node-us-east-1", "Excessive entropy drop");
  assert.equal(tripRes.cluster_globally_tripped, true);

  // Reconnect network
  orchestrator.healPartition();

  // Invariant: Cluster remains tripped monotonically!
  const st = orchestrator.status();
  assert.equal(st.cluster_globally_tripped, true);

  // Post-heal permit attempt must fail closed
  const res = orchestrator.requestPermit("node-us-east-1", "tenant-alpha", 10);
  assert.equal(res.granted, false);
  assert.match(res.reason, /Cluster safety trip latched/);
});

test("Trip from minority node latches upon heal", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  orchestrator.injectPartition(["eu-central-1"]);

  // Trigger trip from minority node
  orchestrator.broadcastTrip("node-eu-cent-1", "Minority node detected local corruption");

  // Heal partition
  orchestrator.healPartition();

  // Invariant: Trip from minority node propagates to global cluster upon heal
  const st = orchestrator.status();
  assert.equal(st.cluster_globally_tripped, true);
});

test("Asymmetric network partition", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  // us-west cannot reach us-east
  orchestrator.injectPartition([], [["us-west-2", "us-east-1"]]);

  // us-east-1 can reach us-east-1 (2) + us-west-2 (2) + eu-central-1 (1) = 5 nodes >= 3
  assert.equal(orchestrator.hasMajorityQuorum("node-us-east-1"), true);

  // us-west-2 can reach us-west-2 (2) + eu-central-1 (1) = 3 nodes >= 3
  assert.equal(orchestrator.hasMajorityQuorum("node-us-west-1"), true);
});

test("End-to-end runExercise nominal", () => {
  const orchestrator = new DatacenterFailureOrchestrator();
  const report = orchestrator.runExercise(
    FailureScenarioType.REGION_SEVERED,
    ["eu-central-1"]
  );

  assert.equal(report.scenario, FailureScenarioType.REGION_SEVERED);
  assert.equal(report.majority_partition_operational, true);
  assert.equal(report.minority_partition_isolated, true);
  assert.equal(report.split_brain_prevented, true);
  assert.equal(report.safety_monotonicity_verified, true);
  assert.equal(report.invariants_passed, true);
  assert.equal(report.exercise_hash.length, 64);
  assert.ok(report.events.length >= 5);

  const d = report.toDict();
  assert.equal(d.invariants_passed, true);
  assert.equal(d.exercise_hash.includes("sw:fail:v1"), false); // raw hex
});

test("Sub-250us orchestrator SLO", () => {
  const orchestrator = new DatacenterFailureOrchestrator();

  // Warmup
  for (let i = 0; i < 10; i++) {
    orchestrator.hasMajorityQuorum("node-us-east-1");
  }

  // Benchmark: 500 iterations of quorum and reachability checks
  const iterations = 500;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    orchestrator.hasMajorityQuorum("node-us-east-1");
    orchestrator.getReachableNodes("node-us-east-1");
  }
  const t1 = performance.now();

  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250.0, `Datacenter quorum check took ${avgUs.toFixed(2)}µs, exceeding 250µs SLO`);
});

test("Global singleton status and reset", () => {
  const st = globalDatacenterOrchestrator.status();
  assert.ok("total_nodes" in st);
  assert.equal(st.total_nodes, 5);
  assert.equal(st.total_regions, 3);

  globalDatacenterOrchestrator.injectPartition(["eu-central-1"]);
  assert.equal(globalDatacenterOrchestrator.status().active_phase, "REGION_ISOLATION");

  globalDatacenterOrchestrator.reset();
  assert.equal(globalDatacenterOrchestrator.status().active_phase, "NOMINAL");
  assert.equal(globalDatacenterOrchestrator.status().cluster_globally_tripped, false);
});
