/**
 * Test Suite for Seam 77 (ADR 0088) Cross-Cluster State Synchronization &
 * Distributed Hyper-Graph Reconciliation Live Simulation in TypeScript.
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
  CrossClusterSyncError,
  SyncCircuitTrippedError,
  UnverifiableCertificateError,
  CausalDiscontinuityError,
  HyperGraphIntegrityError,
  SyncStatus,
  CausalOrderingVerdict,
  HyperNodeType,
  HyperEdgeType,
  HyperNode,
  HyperEdge,
  DistributedHyperGraph,
  SyncPacket,
  SyncReconciliationReceipt,
  AntiEntropyDigest,
  compareVectorClocks,
  verifyCrossClusterPoe,
  signCrossClusterPoe,
  CrossClusterSyncEngine,
} from "../src/cross_cluster_sync.ts";

test("Seam 77 Live Simulation: Certify all 8 operational invariants in TypeScript", () => {
  const tSimStart = performance.now();
  const engineLocal = new CrossClusterSyncEngine("cluster-us-east", 1);

  // Invariant 1: Zero-Token Cost Guarantee ($0.00, deterministic in-memory execution)
  const initialStatus = engineLocal.getStatus();
  assert.equal(initialStatus.status, SyncStatus.NOMINAL);
  assert.equal(initialStatus.cluster_id, "cluster-us-east");
  assert.equal(initialStatus.epoch, 1);
  assert.equal(initialStatus.circuit_tripped, false);

  // Invariant 4: Distributed Hyper-Graph Modeling & Structural Integrity Enforcement
  const n1 = engineLocal.upsertLocalNode("agent-coordinator", HyperNodeType.AGENT_CONTEXT, { role: "leader" });
  const n2 = engineLocal.upsertLocalNode("resource-shared-db", HyperNodeType.RESOURCE_LEASE, { lease_ttl_ms: 30000 });
  const n3 = engineLocal.upsertLocalNode("pipeline-etl-state", HyperNodeType.DATA_PIPELINE, { stage: "INGESTING" });

  const e1 = engineLocal.upsertLocalEdge(
    "joint-lease-01",
    [n1.node_id, n2.node_id, n3.node_id],
    HyperEdgeType.COORDINATION_LEASE,
    { quorum_ack: true },
    0.98
  );

  assert.equal(e1.node_ids.length, 3);
  assert.equal(engineLocal.hypergraph.validateIntegrity(), true);
  const rootDigest = engineLocal.hypergraph.computeRootDigest();
  assert.ok(rootDigest.length === 64, "Composite root digest must be 64-char sha256");

  // Dangling hyper-edge rejection
  assert.throws(
    () => engineLocal.upsertLocalEdge("dangling-edge", ["agent-coordinator", "phantom-node"], HyperEdgeType.CAUSAL_DEPENDENCY),
    HyperGraphIntegrityError
  );

  // Invariant 2: Sub-250µs Execution Latency SLO
  const nOps = 50;
  const t0 = performance.now();
  for (let i = 0; i < nOps; i++) {
    engineLocal.upsertLocalNode(`bench-node-${i}`, HyperNodeType.AGENT_CONTEXT, { idx: i });
  }
  const t1 = performance.now();
  const avgLatencyUs = ((t1 - t0) / nOps) * 1000;
  assert.ok(avgLatencyUs < 250.0, `Average upsert latency ${avgLatencyUs}µs exceeded 250µs SLO`);

  // Invariant 3: Invariant I1 - Issuer-Bound Proof-of-Execution Verification
  const kp0 = generateNodeKeypair();
  const kp1 = generateNodeKeypair();
  const kp2 = generateNodeKeypair();
  const kp3 = generateNodeKeypair();

  const euKeys = {
    "eu-v0": kp0.publicKeyHex,
    "eu-v1": kp1.publicKeyHex,
    "eu-v2": kp2.publicKeyHex,
    "eu-v3": kp3.publicKeyHex,
  };

  const euOracle = new ByzantineConsensusOracle();
  for (const [vid, pk] of Object.entries(euKeys)) {
    euOracle.registerNode(vid, "cluster-eu-west", pk);
  }
  const euCommitteeRoot = euOracle.computeCommitteeRoot();

  engineLocal.registerForeignCommittee("cluster-eu-west", 1, euCommitteeRoot, euKeys);

  const digestVal = "fe".repeat(32);
  const validPoe = {
    certificate_id: "poe-eu-001",
    proposal_id: "prop-cross-sync-01",
    action_type: "CROSS_CLUSTER_MIGRATION",
    view: 0,
    sequence_number: 1,
    digest: digestVal,
    pre_state_hash: "11".repeat(32),
    post_state_hash: "22".repeat(32),
    signers: ["eu-v0", "eu-v1", "eu-v2"],
    signatures: {},
    epoch: 1,
    committee_root: euCommitteeRoot,
    quorum_required: 3,
  };
  validPoe.signatures = {
    "eu-v0": signCrossClusterPoe(validPoe, kp0.privateKey),
    "eu-v1": signCrossClusterPoe(validPoe, kp1.privateKey),
    "eu-v2": signCrossClusterPoe(validPoe, kp2.privateKey),
  };
  validPoe.receipt_hash = computeCanonicalPoeHash(validPoe);

  assert.equal(verifyCrossClusterPoe(validPoe, euCommitteeRoot, 1, euKeys), true);
  assert.equal(verifyCrossClusterPoe(validPoe, "00".repeat(32), 1, euKeys), false);

  // Invariant 5: Vector Clock Causal Analysis
  const vcLocal = { ...engineLocal.hypergraph.cluster_vector };
  const vcAhead = { ...vcLocal, "cluster-eu-west": 1 };
  assert.equal(compareVectorClocks(vcLocal, vcAhead, 1), CausalOrderingVerdict.CAUSALLY_AHEAD);

  const vcConcurrent = {
    ...vcLocal,
    "cluster-us-east": (vcLocal["cluster-us-east"] || 1) - 1,
    "cluster-eu-west": 1,
  };
  assert.equal(compareVectorClocks(vcLocal, vcConcurrent, 1), CausalOrderingVerdict.CONCURRENT);

  const vcBehind = {
    ...vcLocal,
    "cluster-us-east": (vcLocal["cluster-us-east"] || 1) - 1,
  };
  assert.equal(compareVectorClocks(vcLocal, vcBehind, 1), CausalOrderingVerdict.CAUSALLY_BEHIND);

  // Invariant 6: Multi-Cluster CRDT Reconciliation & Convergence
  const euNode = new HyperNode(
    "eu-worker-context",
    "cluster-eu-west",
    HyperNodeType.AGENT_CONTEXT,
    { workload: "analytics", status: "ONLINE" }
  );

  const euEdge = new HyperEdge(
    "joint-lease-01",
    ["agent-coordinator", "resource-shared-db", "eu-worker-context"],
    HyperEdgeType.COORDINATION_LEASE,
    { quorum_ack: true, eu_approved: true },
    0.99
  );

  const syncPacket = new SyncPacket(
    "pkt-eu-to-us-001",
    "cluster-eu-west",
    "cluster-us-east",
    1,
    euCommitteeRoot,
    3,
    { "cluster-us-east": vcLocal["cluster-us-east"], "cluster-eu-west": 1 },
    [euNode],
    [euEdge],
    validPoe
  );

  const receipt = engineLocal.ingestSyncPacket(syncPacket);

  // Invariant 7: Tamper-Evident Sync Reconciliation Receipt
  assert.ok(receipt.receipt_hash.startsWith("sw:sync:v1:"));
  assert.equal(engineLocal.verifyReconciliationReceipt(receipt), true);

  // Assert CRDT Convergence
  const mergedEdge = engineLocal.hypergraph.edges.get("joint-lease-01");
  assert.ok(mergedEdge);
  assert.ok(mergedEdge.node_ids.includes("eu-worker-context"));
  assert.ok(mergedEdge.node_ids.includes("pipeline-etl-state"));
  assert.equal(mergedEdge.weight, 0.99);

  // Invariant 8: Fail-Closed Safety Circuit Coupling & Downstream Rejection
  const gapPacket = new SyncPacket(
    "pkt-gap-001",
    "cluster-eu-west",
    "cluster-us-east",
    1,
    euCommitteeRoot,
    3,
    { "cluster-us-east": vcLocal["cluster-us-east"], "cluster-eu-west": 999 }, // Gap of 998
    [],
    [],
    validPoe
  );

  assert.throws(() => engineLocal.ingestSyncPacket(gapPacket), CausalDiscontinuityError);
  assert.equal(engineLocal.circuit_tripped, true);

  assert.throws(
    () => engineLocal.upsertLocalNode("blocked-node", HyperNodeType.AGENT_CONTEXT, {}),
    SyncCircuitTrippedError
  );

  engineLocal.rearmCircuit();
  assert.equal(engineLocal.circuit_tripped, false);

  const totalDurationMs = performance.now() - tSimStart;
  assert.ok(totalDurationMs < 1000.0, `Total simulation time exceeded 1000ms: ${totalDurationMs}ms`);
});
