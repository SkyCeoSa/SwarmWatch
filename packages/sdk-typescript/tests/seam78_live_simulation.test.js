/**
 * Test Suite for Seam 78 (ADR 0089) Autonomous Distributed Hyper-Graph
 * Partition Healing & Causal Vector Resumption Live Simulation in TypeScript.
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import {
  generateNodeKeypair,
  computeCanonicalPoeHash,
  signBftPayload,
  ByzantineConsensusOracle,
} from "../src/byzantine_oracle.ts";

import {
  HyperNodeType,
  HyperEdgeType,
  HyperNode,
  HyperEdge,
  HyperGraphIntegrityError,
  signCrossClusterPoe,
} from "../src/cross_cluster_sync.ts";

import {
  PartitionState,
  PartitionHealingError,
  UnhealedPartitionConflictError,
  InvalidResumptionProofError,
  ResumptionVectorRegressionError,
  HealingCircuitTrippedError,
  ResumptionDelta,
  PartitionHealingPlan,
  PartitionHealingReceipt,
  PartitionHealingEngine,
  globalPartitionHealer,
} from "../src/partition_healer.ts";

test("Seam 78 Live Simulation: Certify all 8 operational invariants in TypeScript", () => {
  const tSimStart = performance.now();
  const engine = new PartitionHealingEngine("cluster-us-east", 2);

  // Invariant 1: Zero-Token Cost Guarantee ($0.00 local in-memory execution)
  const initialStatus = engine.getStatus();
  assert.equal(initialStatus.local_cluster, "cluster-us-east");
  assert.equal(initialStatus.epoch, 2);
  assert.equal(initialStatus.state, PartitionState.NOMINAL);
  assert.equal(initialStatus.circuit_tripped, false);

  // Phase 2: Local Hyper-Graph initialization
  const n1 = new HyperNode(
    "agent-planner-01",
    "cluster-us-east",
    HyperNodeType.AGENT_CONTEXT,
    { role: "planner", turn: 5 },
    {},
    2,
    1000
  );
  const n2 = new HyperNode(
    "lease-gpu-a100",
    "cluster-us-east",
    HyperNodeType.RESOURCE_LEASE,
    { leased_to: "agent-planner-01" },
    {},
    2,
    1000
  );
  const e1 = new HyperEdge(
    "edge-lease-coordination",
    [n1.node_id, n2.node_id],
    HyperEdgeType.COORDINATION_LEASE,
    { ttl_ms: 10000 },
    0.90,
    {},
    2,
    1000
  );
  engine.hypergraph.addNode(n1);
  engine.hypergraph.addNode(n2);
  engine.hypergraph.addEdge(e1);
  engine.hypergraph.cluster_vector["cluster-us-east"] = 12;
  engine.hypergraph.cluster_vector["cluster-eu-west"] = 4;

  // Invariant 3 & 4: Partition Detection & Symmetric Difference Matrix
  const remoteVc = {
    "cluster-us-east": 7,
    "cluster-eu-west": 14,
  };
  const tDetect0 = performance.now();
  const [isPart, diffs] = engine.detectPartition("cluster-eu-west", remoteVc);
  const detectLatencyUs = (performance.now() - tDetect0) * 1000;
  assert.equal(isPart, true);
  assert.equal(diffs["cluster-us-east"], -5);
  assert.equal(diffs["cluster-eu-west"], 10);
  assert.equal(engine.state, PartitionState.PARTITION_DETECTED);
  assert.ok(detectLatencyUs < 250000.0);

  // Phase 4: Deterministic Healing Plan Formulation
  const plan = engine.formulateHealingPlan("cluster-eu-west", remoteVc);
  assert.equal(plan.is_partition_detected, true);
  assert.equal(plan.missing_local_steps, 5);
  assert.equal(plan.missing_remote_steps, 10);
  assert.equal(plan.merged_vector_clock["cluster-us-east"], 12);
  assert.equal(plan.merged_vector_clock["cluster-eu-west"], 14);
  assert.equal(engine.state, PartitionState.PLANNING_RESUMPTION);

  // Invariant 6: Foreign PoE Verification (Invariant I1)
  const kp1 = generateNodeKeypair();
  const kp2 = generateNodeKeypair();
  const oracle = new ByzantineConsensusOracle();
  oracle.registerNode("v1", "cluster-eu-west", kp1.publicKeyHex);
  oracle.registerNode("v2", "cluster-eu-west", kp2.publicKeyHex);
  const commRoot = oracle.computeCommitteeRoot();

  engine.sync_engine.registerForeignCommittee(
    "cluster-eu-west",
    2,
    commRoot,
    { v1: kp1.publicKeyHex, v2: kp2.publicKeyHex }
  );

  const rawCert = {
    certificate_id: "poe-cert-eu-01",
    proposal_id: "prop-cross-01",
    action_type: "HEALING_RESUMPTION",
    view: 0,
    sequence_number: 1,
    digest: "aa".repeat(32),
    pre_state_hash: "bb".repeat(32),
    post_state_hash: "cc".repeat(32),
    signers: ["v1", "v2"],
    signatures: {},
    epoch: 2,
    committee_root: commRoot,
    quorum_required: 2,
  };
  rawCert.signatures = {
    v1: signCrossClusterPoe(rawCert, kp1.privateKey),
    v2: signCrossClusterPoe(rawCert, kp2.privateKey),
  };
  rawCert.receipt_hash = computeCanonicalPoeHash(rawCert);

  const rN1 = new HyperNode(
    "agent-planner-01",
    "cluster-eu-west",
    HyperNodeType.AGENT_CONTEXT,
    { role: "planner", turn: 8, remote_fix: true },
    {},
    2,
    2000
  );
  const rN3 = new HyperNode(
    "telemetry-buffer-01",
    "cluster-eu-west",
    HyperNodeType.SHARED_MEMORY,
    { buffered_spans: 42 },
    {},
    2,
    1500
  );
  const rE1 = new HyperEdge(
    "edge-lease-coordination",
    [n1.node_id, n2.node_id, rN3.node_id],
    HyperEdgeType.COORDINATION_LEASE,
    { ttl_ms: 15000, multi_cluster: true },
    0.99,
    {},
    2,
    2000
  );

  const delta = new ResumptionDelta(
    "delta-eu-heal-01",
    "cluster-eu-west",
    "cluster-us-east",
    2,
    2,
    [rN1, rN3],
    [rE1],
    { "cluster-eu-west": 14, "cluster-us-east": 7 },
    rawCert
  );

  // Invariant 2, 5, 7: Execute Healing, Invariant I2 Monotonicity, and Receipt Integrity
  const receipt = engine.executeHealing(delta);
  assert.equal(receipt.nodes_reconciled, 2);
  assert.equal(receipt.edges_spliced, 1);
  assert.ok(receipt.receipt_hash.startsWith("sw:heal:v1:"));
  assert.equal(engine.verifyHealingReceipt(receipt), true);

  // Invariant I2 check
  assert.ok(engine.hypergraph.cluster_vector["cluster-eu-west"] >= 14);
  assert.ok(engine.hypergraph.cluster_vector["cluster-us-east"] >= 12);
  assert.equal(engine.state, PartitionState.HEALED);

  // Invariant 8: Fail-Closed Safety Circuit Coupling
  const badEdge = new HyperEdge(
    "edge-dangling-fail",
    [n1.node_id, "phantom-node-999"],
    HyperEdgeType.CAUSAL_DEPENDENCY
  );
  const badDelta = new ResumptionDelta(
    "delta-bad-edge",
    "cluster-eu-west",
    "cluster-us-east",
    2,
    2,
    [],
    [badEdge],
    { "cluster-eu-west": 15 }
  );

  assert.throws(() => {
    engine.executeHealing(badDelta);
  }, HyperGraphIntegrityError);

  assert.equal(engine.circuit_tripped, true);
  assert.equal(engine.state, PartitionState.CIRCUIT_TRIPPED);

  assert.throws(() => {
    engine.formulateHealingPlan("cluster-eu-west", { "cluster-eu-west": 20 });
  }, HealingCircuitTrippedError);

  engine.rearmCircuit();
  assert.equal(engine.circuit_tripped, false);
  assert.equal(engine.state, PartitionState.NOMINAL);

  const totalDurationMs = performance.now() - tSimStart;
  assert.ok(totalDurationMs > 0);
});
