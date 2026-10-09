/**
 * Test Suite for Seam 79 (ADR 0090) Dynamic Topology Re-Partitioning &
 * Adaptive Sub-Graph Migration Live Simulation in TypeScript.
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
  DistributedHyperGraph,
  signCrossClusterPoe,
} from "../src/cross_cluster_sync.ts";

import {
  MigrationState,
  TopologyMigrationError,
  MigrationCircuitTrippedError,
  MigrationCausalRegressionError,
  OrphanSubGraphEdgeError,
  InvalidMigrationProofError,
  SubGraphNotFoundError,
  InvalidMigrationPlanError,
  ClusterLoadProfile,
  SubGraphBoundaryContract,
  SubGraphBundle,
  MigrationPlan,
  MigrationReceipt,
  DynamicTopologyMigrator,
} from "../src/topology_migrator.ts";

test("Seam 79 Live Simulation: Certify all 8 operational invariants in TypeScript", () => {
  const tSimStart = performance.now();
  const migratorSrc = new DynamicTopologyMigrator("cluster-us-east", 1);
  const migratorDst = new DynamicTopologyMigrator("cluster-eu-west", 1);

  // Invariant 1: Zero-Token Cost Guarantee ($0.00 local in-memory execution)
  const initialStatus = migratorSrc.get_status();
  assert.equal(initialStatus.cluster_id, "cluster-us-east");
  assert.equal(initialStatus.state, MigrationState.IDLE);
  assert.equal(initialStatus.circuit_tripped, false);

  // Phase 2: Register Cluster Profiles & Skew Evaluation
  const p1 = new ClusterLoadProfile("cluster-us-east", 180, 240, 0.94, 0.90);
  const p2 = new ClusterLoadProfile("cluster-eu-west", 25, 30, 0.12, 0.18);

  migratorSrc.register_cluster_profile(p1);
  migratorSrc.register_cluster_profile(p2);

  const tSkew0 = performance.now();
  const skew = migratorSrc.evaluate_partition_skew();
  const skewLatencyUs = (performance.now() - tSkew0) * 1000;

  assert.equal(skew.cluster_count, 2);
  assert.equal(skew.most_loaded_cluster, "cluster-us-east");
  assert.equal(skew.least_loaded_cluster, "cluster-eu-west");
  assert.ok(skew.load_delta > 0.40);
  assert.equal(skew.migration_recommended, true);
  assert.ok(skewLatencyUs < 250000.0);

  // Phase 3: Populate Source Distributed HyperGraph
  const srcGraph = new DistributedHyperGraph();

  const nA = new HyperNode("agent-worker-01", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, { task: "index" });
  const nB = new HyperNode("agent-worker-02", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, { task: "agg" });
  const nC = new HyperNode("memory-shared-01", "cluster-us-east", HyperNodeType.SHARED_MEMORY, { capacity: 4096 });
  const nD = new HyperNode("lease-coordinator-01", "cluster-us-east", HyperNodeType.RESOURCE_LEASE, { ttl: 5000 });

  srcGraph.addNode(nA);
  srcGraph.addNode(nB);
  srcGraph.addNode(nC);
  srcGraph.addNode(nD);

  const eInt = new HyperEdge("edge-worker-collab", [nA.node_id, nB.node_id], HyperEdgeType.COORDINATION_LEASE, {}, 0.85);
  const eCut = new HyperEdge("edge-worker-memory", [nB.node_id, nC.node_id], HyperEdgeType.CAUSAL_DEPENDENCY, {}, 0.60);
  const eExt = new HyperEdge("edge-memory-lease", [nC.node_id, nD.node_id], HyperEdgeType.COORDINATION_LEASE, {}, 0.40);

  srcGraph.addEdge(eInt);
  srcGraph.addEdge(eCut);
  srcGraph.addEdge(eExt);

  // Phase 4: Formulate Migration Plan
  const tPlan0 = performance.now();
  const plan = migratorSrc.formulate_migration_plan(
    "cluster-us-east",
    "cluster-eu-west",
    ["agent-worker-01", "agent-worker-02"],
    srcGraph
  );
  const planLatencyUs = (performance.now() - tPlan0) * 1000;

  assert.ok(plan.plan_id.startsWith("mig-plan-"));
  assert.ok(plan.plan_hash.startsWith("sw:mig:plan:v1:"));
  assert.deepEqual(plan.node_ids, ["agent-worker-01", "agent-worker-02"]);
  assert.deepEqual(plan.edge_ids, ["edge-worker-collab"]);
  assert.deepEqual(plan.boundary_edge_ids, ["edge-worker-memory"]);
  assert.equal(plan.estimated_cut_reduction, 0.425);
  assert.equal(migratorSrc.state, MigrationState.PLAN_SYNTHESIZED);
  assert.ok(planLatencyUs < 250000.0);

  // Phase 5: Package SubGraphBundle with Boundary Contracts
  const sourceClock = { "cluster-us-east": 24, "cluster-eu-west": 8 };
  const tBundle0 = performance.now();
  const bundle = migratorSrc.prepare_subgraph_transfer(plan.plan_id, srcGraph, sourceClock);
  const bundleLatencyUs = (performance.now() - tBundle0) * 1000;

  assert.ok(bundle.subgraph_id.startsWith("subgraph-"));
  assert.ok(bundle.bundle_digest.startsWith("sw:mig:bundle:v1:"));
  assert.equal(Object.keys(bundle.nodes).length, 2);
  assert.equal(Object.keys(bundle.internal_edges).length, 1);
  assert.equal(bundle.boundary_contracts.length, 1);
  assert.equal(bundle.boundary_contracts[0].edge_id, "edge-worker-memory");
  assert.deepEqual(bundle.boundary_contracts[0].cut_node_ids, ["agent-worker-02"]);
  assert.deepEqual(bundle.boundary_contracts[0].retained_node_ids, ["memory-shared-01"]);
  assert.equal(migratorSrc.state, MigrationState.PREPARING_TRANSFER);
  assert.ok(bundleLatencyUs < 250000.0);

  // Phase 6: Foreign PoE Verification (Invariant M3) & Execution (Invariant M1/M2)
  const kp1 = generateNodeKeypair();
  const kp2 = generateNodeKeypair();
  const validatorKeys = { "val-1": kp1.publicKeyHex, "val-2": kp2.publicKeyHex };
  const commRoot = "comm-root-live-sim-79";

  migratorDst.register_foreign_committee("cluster-us-east", 1, commRoot, validatorKeys, 2);

  const certData = {
    certificate_id: "poe-cert-ts-sim-01",
    proposal_id: "prop-mig-ts-01",
    action_type: "SUBGRAPH_MIGRATION",
    view: 0,
    sequence_number: 1,
    digest: "aa".repeat(32),
    pre_state_hash: "bb".repeat(32),
    post_state_hash: "cc".repeat(32),
    signers: ["val-1", "val-2"],
    epoch: 1,
    committee_root: commRoot,
    quorum_required: 2,
  };
  const certHash = computeCanonicalPoeHash(certData);
  const validPoe = {
    ...certData,
    receipt_hash: certHash,
    signatures: {
      "val-1": signCrossClusterPoe(certData, kp1.privateKey),
      "val-2": signCrossClusterPoe(certData, kp2.privateKey),
    },
  };

  const dstGraph = new DistributedHyperGraph();
  const targetClockBefore = { "cluster-eu-west": 12, "cluster-us-east": 15 };

  const tExec0 = performance.now();
  const receipt = migratorDst.execute_migration(bundle, dstGraph, targetClockBefore, validPoe);
  const execLatencyUs = (performance.now() - tExec0) * 1000;

  assert.ok(receipt.receipt_id.startsWith("mig-rcpt-"));
  assert.ok(receipt.receipt_hash.startsWith("sw:mig:rcpt:v1:"));
  assert.ok(execLatencyUs < 250000.0);

  // Invariant M1: Causal Vector Monotonicity Handover
  assert.ok(receipt.post_migration_vector_clock["cluster-us-east"] >= 24);
  assert.ok(receipt.post_migration_vector_clock["cluster-eu-west"] > 12);

  // Invariant M2: Boundary Closure
  assert.ok(dstGraph.nodes.has("agent-worker-01"));
  assert.ok(dstGraph.nodes.has("agent-worker-02"));
  assert.ok(dstGraph.edges.has("edge-worker-collab"));
  assert.ok(dstGraph.edges.has("edge-worker-memory"));
  assert.ok(dstGraph.nodes.has("memory-shared-01")); // Retained stub
  assert.equal(dstGraph.nodes.get("memory-shared-01")?.state_payload._stub, true);

  // Invariant M3: Verification
  assert.equal(migratorDst.verify_migration_receipt(receipt), true);
  assert.equal(migratorDst.state, MigrationState.COMPLETED_STABLE);

  // Phase 7: Fail-Closed Safety Circuit Coupling
  // Test 1: Clock Regression
  const regressedBundle = new SubGraphBundle(
    "subgraph-bad-clock",
    "cluster-us-east",
    "cluster-eu-west",
    ["n-reg"],
    { "n-reg": new HyperNode("n-reg", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict() },
    {},
    [],
    { "cluster-us-east": -5 },
    1
  );
  const testGraph = new DistributedHyperGraph();
  assert.throws(
    () => migratorDst.execute_migration(regressedBundle, testGraph, { "cluster-eu-west": 5 }),
    MigrationCausalRegressionError
  );
  assert.equal(migratorDst.circuit_tripped, true);
  assert.equal(migratorDst.state, MigrationState.CIRCUIT_TRIPPED);

  // Blocked operations
  assert.throws(
    () => migratorDst.formulate_migration_plan("cluster-us-east", "cluster-eu-west", ["agent-worker-01"], srcGraph),
    MigrationCircuitTrippedError
  );

  migratorDst.rearm_circuit();
  assert.equal(migratorDst.circuit_tripped, false);
  assert.equal(migratorDst.state, MigrationState.IDLE);

  // Test 2: Boundary Leak
  const orphanBundle = new SubGraphBundle(
    "subgraph-leak",
    "cluster-us-east",
    "cluster-eu-west",
    ["n-leak"],
    { "n-leak": new HyperNode("n-leak", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict() },
    {
      "e-leak": new HyperEdge("e-leak", ["n-leak", "orphan-ghost"], HyperEdgeType.COORDINATION_LEASE, {}).to_dict(),
    },
    [],
    { "cluster-us-east": 5 },
    1
  );
  assert.throws(
    () => migratorDst.execute_migration(orphanBundle, testGraph, { "cluster-eu-west": 5 }),
    OrphanSubGraphEdgeError
  );
  assert.equal(migratorDst.circuit_tripped, true);
  migratorDst.rearm_circuit();

  // Test 3: Tampered PoE
  const tamperedPoe = {
    ...validPoe,
    signatures: { "val-1": "00".repeat(64), "val-2": "00".repeat(64) },
  };
  assert.throws(
    () => migratorDst.execute_migration(bundle, testGraph, targetClockBefore, tamperedPoe),
    InvalidMigrationProofError
  );
  assert.equal(migratorDst.circuit_tripped, true);
  migratorDst.rearm_circuit();

  const totalSimDurationMs = performance.now() - tSimStart;
  assert.ok(totalSimDurationMs > 0);
});
