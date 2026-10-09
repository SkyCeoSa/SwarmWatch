import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  generateNodeKeypair,
  computeCanonicalPoeHash,
  signBftPayload,
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
  TopologyMigrationError,
  MigrationCircuitTrippedError,
  MigrationCausalRegressionError,
  OrphanSubGraphEdgeError,
  InvalidMigrationProofError,
  SubGraphNotFoundError,
  InvalidMigrationPlanError,
  MigrationState,
  ClusterLoadProfile,
  SubGraphBoundaryContract,
  SubGraphBundle,
  MigrationPlan,
  MigrationReceipt,
  DynamicTopologyMigrator,
  globalTopologyMigrator,
} from "../src/topology_migrator.ts";

describe("Seam 79: Dynamic Topology Re-Partitioning & Adaptive Sub-Graph Migration (TypeScript)", () => {
  beforeEach(() => {
    globalTopologyMigrator.rearm_circuit();
  });

  test("Load profile registration and hyper-graph cut skew evaluation", () => {
    const engine = new DynamicTopologyMigrator("cluster-us-east");

    const p1 = new ClusterLoadProfile("cluster-us-east", 120, 180, 0.92, 0.88);
    const p2 = new ClusterLoadProfile("cluster-eu-west", 15, 20, 0.10, 0.15);

    engine.register_cluster_profile(p1);
    engine.register_cluster_profile(p2);

    const skew = engine.evaluate_partition_skew();
    assert.equal(skew.cluster_count, 2);
    assert.equal(skew.most_loaded_cluster, "cluster-us-east");
    assert.equal(skew.least_loaded_cluster, "cluster-eu-west");
    assert.ok(skew.load_delta > 0.40);
    assert.equal(skew.migration_recommended, true);
  });

  test("Deterministic migration plan formulation: internal vs boundary cut edges", () => {
    const engine = new DynamicTopologyMigrator("cluster-us-east");

    const srcGraph = new DistributedHyperGraph();
    srcGraph.addNode(new HyperNode("node-A", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));
    srcGraph.addNode(new HyperNode("node-B", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));
    srcGraph.addNode(new HyperNode("node-C", "cluster-us-east", HyperNodeType.SHARED_MEMORY, {}));
    srcGraph.addNode(new HyperNode("node-D", "cluster-us-east", HyperNodeType.RESOURCE_LEASE, {}));

    srcGraph.addEdge(new HyperEdge("edge-int-1", ["node-A", "node-B"], HyperEdgeType.COORDINATION_LEASE, {}, 0.8));
    srcGraph.addEdge(new HyperEdge("edge-cut-1", ["node-B", "node-C"], HyperEdgeType.CAUSAL_DEPENDENCY, {}, 0.6));
    srcGraph.addEdge(new HyperEdge("edge-other", ["node-C", "node-D"], HyperEdgeType.COORDINATION_LEASE, {}, 0.4));

    const plan = engine.formulate_migration_plan(
      "cluster-us-east",
      "cluster-eu-west",
      ["node-A", "node-B"],
      srcGraph
    );

    assert.ok(plan.plan_id.startsWith("mig-plan-"));
    assert.ok(plan.plan_hash.startsWith("sw:mig:plan:v1:"));
    assert.deepEqual(plan.node_ids, ["node-A", "node-B"]);
    assert.deepEqual(plan.edge_ids, ["edge-int-1"]);
    assert.deepEqual(plan.boundary_edge_ids, ["edge-cut-1"]);
    assert.equal(plan.estimated_cut_reduction, 0.4);
    assert.equal(engine.state, MigrationState.PLAN_SYNTHESIZED);
  });

  test("SubGraphBundle preparation with boundary contracts", () => {
    const engine = new DynamicTopologyMigrator("cluster-us-east");

    const srcGraph = new DistributedHyperGraph();
    srcGraph.addNode(new HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));
    srcGraph.addNode(new HyperNode("n2", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));
    srcGraph.addNode(new HyperNode("n3", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));

    srcGraph.addEdge(new HyperEdge("e-int", ["n1", "n2"], HyperEdgeType.COORDINATION_LEASE, {}, 0.9));
    srcGraph.addEdge(new HyperEdge("e-cut", ["n2", "n3"], HyperEdgeType.COORDINATION_LEASE, {}, 0.7));

    const plan = engine.formulate_migration_plan("cluster-us-east", "cluster-eu-west", ["n1", "n2"], srcGraph);
    const sourceClock = { "cluster-us-east": 18, "cluster-eu-west": 5 };
    const bundle = engine.prepare_subgraph_transfer(plan.plan_id, srcGraph, sourceClock);

    assert.equal(bundle.subgraph_id, plan.subgraph_id);
    assert.ok(bundle.bundle_digest.startsWith("sw:mig:bundle:v1:"));
    assert.ok(bundle.nodes["n1"] && bundle.nodes["n2"]);
    assert.ok(bundle.internal_edges["e-int"]);
    assert.equal(bundle.boundary_contracts.length, 1);

    const bc = bundle.boundary_contracts[0];
    assert.equal(bc.edge_id, "e-cut");
    assert.deepEqual(bc.cut_node_ids, ["n2"]);
    assert.deepEqual(bc.retained_node_ids, ["n3"]);
  });

  test("Atomic sub-graph migration execution and receipt verification", () => {
    const engineSrc = new DynamicTopologyMigrator("cluster-us-east");
    const engineDst = new DynamicTopologyMigrator("cluster-eu-west");

    const srcGraph = new DistributedHyperGraph();
    srcGraph.addNode(new HyperNode("agent-1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));
    srcGraph.addNode(new HyperNode("agent-2", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}));
    srcGraph.addEdge(new HyperEdge("edge-a1a2", ["agent-1", "agent-2"], HyperEdgeType.COORDINATION_LEASE, {}, 0.95));

    const plan = engineSrc.formulate_migration_plan("cluster-us-east", "cluster-eu-west", ["agent-1", "agent-2"], srcGraph);
    const bundle = engineSrc.prepare_subgraph_transfer(plan.plan_id, srcGraph, { "cluster-us-east": 25 });

    const targetGraph = new DistributedHyperGraph();
    const targetClock = { "cluster-eu-west": 8 };

    const receipt = engineDst.execute_migration(bundle, targetGraph, targetClock);

    assert.ok(receipt.receipt_hash.startsWith("sw:mig:rcpt:v1:"));
    assert.equal(receipt.migrated_nodes_count, 2);
    assert.equal(receipt.migrated_edges_count, 1);
    assert.ok(targetGraph.nodes.has("agent-1") || targetGraph.nodes["agent-1"]);
    assert.ok(targetGraph.nodes.has("agent-2") || targetGraph.nodes["agent-2"]);

    // Invariant M1: Clock handover monotonicity
    assert.ok(receipt.post_migration_vector_clock["cluster-us-east"] >= 25);
    assert.equal(receipt.post_migration_vector_clock["cluster-eu-west"], 9);
    assert.equal(engineDst.verify_migration_receipt(receipt), true);
  });

  test("Invariant M1: Negative clock counter rejection and regression defense", () => {
    const engine = new DynamicTopologyMigrator("cluster-eu-west");

    const bundle = new SubGraphBundle(
      "subgraph-bad-clock",
      "cluster-us-east",
      "cluster-eu-west",
      ["n1"],
      { n1: new HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict() },
      {},
      [],
      { "cluster-us-east": -10 },
      1
    );

    const targetGraph = new DistributedHyperGraph();
    assert.throws(
      () => engine.execute_migration(bundle, targetGraph, { "cluster-eu-west": 5 }),
      MigrationCausalRegressionError
    );
    assert.equal(engine.circuit_tripped, true);
    assert.equal(engine.state, MigrationState.CIRCUIT_TRIPPED);
  });

  test("Invariant M2: Boundary closure failure and dangling edge defense", () => {
    const engine = new DynamicTopologyMigrator("cluster-eu-west");

    const bundle = new SubGraphBundle(
      "subgraph-orphan-edge",
      "cluster-us-east",
      "cluster-eu-west",
      ["n1"],
      { n1: new HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict() },
      {
        "dangling-e": new HyperEdge("dangling-e", ["n1", "missing-orphan"], HyperEdgeType.COORDINATION_LEASE, {}).to_dict(),
      },
      [],
      { "cluster-us-east": 1 },
      1
    );

    const targetGraph = new DistributedHyperGraph();
    assert.throws(
      () => engine.execute_migration(bundle, targetGraph, { "cluster-eu-west": 1 }),
      OrphanSubGraphEdgeError
    );
    assert.equal(engine.circuit_tripped, true);
  });

  test("Invariant M3: Foreign Proof-of-Execution verification and tamper detection", () => {
    const engine = new DynamicTopologyMigrator("cluster-eu-west", 1);

    const kp1 = generateNodeKeypair();
    const kp2 = generateNodeKeypair();
    const validatorKeys = {
      v1: kp1.publicKeyHex,
      v2: kp2.publicKeyHex,
    };
    const commRoot = "comm-root-ts-999";

    engine.register_foreign_committee("cluster-us-east", 1, commRoot, validatorKeys, 2);

    const certData = {
      certificate_id: "poe-ts-1",
      proposal_id: "prop-ts-1",
      action_type: "SUBGRAPH_MIGRATION",
      view: 0,
      sequence_number: 1,
      digest: "digest-ts-1",
      pre_state_hash: "pre-ts-1",
      post_state_hash: "post-ts-1",
      signers: ["v1", "v2"],
      epoch: 1,
      committee_root: commRoot,
      quorum_required: 2,
    };
    const certHash = computeCanonicalPoeHash(certData);
    const validCert = {
      ...certData,
      receipt_hash: certHash,
      signatures: {
        v1: signCrossClusterPoe(certData, kp1.privateKey),
        v2: signCrossClusterPoe(certData, kp2.privateKey),
      },
    };

    const bundle = new SubGraphBundle(
      "subgraph-poe-ts",
      "cluster-us-east",
      "cluster-eu-west",
      ["n1"],
      { n1: new HyperNode("n1", "cluster-us-east", HyperNodeType.AGENT_CONTEXT, {}).to_dict() },
      {},
      [],
      { "cluster-us-east": 1 },
      1
    );

    const targetGraph = new DistributedHyperGraph();
    const rcpt = engine.execute_migration(bundle, targetGraph, { "cluster-eu-west": 1 }, validCert);
    assert.ok(rcpt.receipt_hash.startsWith("sw:mig:rcpt:v1:"));

    // Tampered certificate
    const tamperedCert = {
      ...validCert,
      signatures: { v1: "00".repeat(64), v2: "00".repeat(64) },
    };
    assert.throws(
      () => engine.execute_migration(bundle, targetGraph, { "cluster-eu-west": 2 }, tamperedCert),
      InvalidMigrationProofError
    );
    assert.equal(engine.circuit_tripped, true);
  });
});
