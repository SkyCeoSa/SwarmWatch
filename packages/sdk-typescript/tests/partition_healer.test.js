import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

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

describe("Seam 78: Autonomous Distributed Hyper-Graph Partition Healing & Causal Vector Resumption", () => {
  beforeEach(() => {
    globalPartitionHealer.reset();
  });

  test("Partition detection via symmetric difference matrix", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 2);
    engine.hypergraph.cluster_vector["cluster-us-east"] = 10;
    engine.hypergraph.cluster_vector["cluster-eu-west"] = 4;

    // Concurrent divergence: remote is ahead on eu-west, behind on us-east
    const remoteVc = {
      "cluster-us-east": 6,
      "cluster-eu-west": 12,
    };
    const [isPartition, diffs] = engine.detectPartition("cluster-eu-west", remoteVc);
    assert.equal(isPartition, true);
    assert.equal(diffs["cluster-us-east"], -4);
    assert.equal(diffs["cluster-eu-west"], 8);
    assert.equal(engine.state, PartitionState.PARTITION_DETECTED);

    // Nominal progression without concurrent deviation
    engine.reset();
    engine.hypergraph.cluster_vector["cluster-us-east"] = 5;
    const [isNominal, diffsNominal] = engine.detectPartition(
      "cluster-eu-west",
      { "cluster-us-east": 5, "cluster-eu-west": 2 },
      5
    );
    assert.equal(isNominal, false);
  });

  test("Deterministic healing plan formulation", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 3);
    engine.hypergraph.cluster_vector["cluster-us-east"] = 20;
    engine.hypergraph.cluster_vector["cluster-eu-west"] = 5;

    const remoteVc = {
      "cluster-us-east": 15,
      "cluster-eu-west": 25,
    };
    const plan = engine.formulateHealingPlan("cluster-eu-west", remoteVc);
    assert.equal(plan.is_partition_detected, true);
    assert.equal(plan.local_cluster, "cluster-us-east");
    assert.equal(plan.remote_cluster, "cluster-eu-west");
    assert.ok(plan.divergent_clusters.includes("cluster-us-east"));
    assert.ok(plan.divergent_clusters.includes("cluster-eu-west"));
    assert.equal(plan.missing_local_steps, 5);
    assert.equal(plan.missing_remote_steps, 20);
    assert.equal(plan.merged_vector_clock["cluster-us-east"], 20);
    assert.equal(plan.merged_vector_clock["cluster-eu-west"], 25);
    assert.equal(engine.state, PartitionState.PLANNING_RESUMPTION);
  });

  test("Resumption delta creation and backlog extraction", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 1);
    const n1 = new HyperNode(
      "node-alpha",
      "cluster-us-east",
      HyperNodeType.AGENT_CONTEXT,
      { plan: "step-1" }
    );
    const n2 = new HyperNode(
      "node-beta",
      "cluster-us-east",
      HyperNodeType.RESOURCE_LEASE,
      { resource: "gpu-01" }
    );
    const e1 = new HyperEdge(
      "edge-1",
      ["node-alpha", "node-beta"],
      HyperEdgeType.COORDINATION_LEASE,
      {},
      1.0
    );
    engine.hypergraph.addNode(n1);
    engine.hypergraph.addNode(n2);
    engine.hypergraph.addEdge(e1);
    engine.hypergraph.cluster_vector["cluster-us-east"] = 7;

    const targetVc = { "cluster-us-east": 0 };
    const delta = engine.createResumptionDelta("cluster-eu-west", targetVc);
    assert.equal(delta.source_cluster, "cluster-us-east");
    assert.equal(delta.target_cluster, "cluster-eu-west");
    assert.equal(delta.nodes_delta.length, 2);
    assert.equal(delta.edges_delta.length, 1);
    assert.equal(delta.delta_hash.length, 64);
    assert.ok(engine.resumption_deltas.has(delta.delta_id));
  });

  test("CRDT hyper-graph splicing and receipt issuance", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 1);
    const n1 = new HyperNode(
      "node-1",
      "cluster-us-east",
      HyperNodeType.AGENT_CONTEXT,
      { turn: 1 },
      {},
      1,
      1000
    );
    const n2 = new HyperNode(
      "node-2",
      "cluster-us-east",
      HyperNodeType.RESOURCE_LEASE,
      { lease: "active" },
      {},
      1,
      1000
    );
    engine.hypergraph.addNode(n1);
    engine.hypergraph.addNode(n2);
    engine.hypergraph.cluster_vector["cluster-us-east"] = 5;

    // Remote side has updated node-1, added node-3, and created edge-123
    const rN1 = new HyperNode(
      "node-1",
      "cluster-eu-west",
      HyperNodeType.AGENT_CONTEXT,
      { turn: 2, updated_by: "eu" },
      {},
      1,
      2000
    );
    const rN3 = new HyperNode(
      "node-3",
      "cluster-eu-west",
      HyperNodeType.POLICY_STATE,
      { quota: 100 },
      {},
      1,
      1500
    );
    const rE1 = new HyperEdge(
      "edge-123",
      ["node-1", "node-2", "node-3"],
      HyperEdgeType.COORDINATION_LEASE,
      { status: "reconciled" },
      0.85
    );

    const delta = new ResumptionDelta(
      "delta-remote-001",
      "cluster-eu-west",
      "cluster-us-east",
      1,
      1,
      [rN1, rN3],
      [rE1],
      { "cluster-eu-west": 8, "cluster-us-east": 2 }
    );

    const receipt = engine.executeHealing(delta);
    assert.ok(receipt.receipt_hash.startsWith("sw:heal:v1:"));
    assert.equal(receipt.nodes_reconciled, 2);
    assert.equal(receipt.edges_spliced, 1);
    assert.equal(engine.verifyHealingReceipt(receipt), true);

    // Merged hypergraph state
    assert.equal(engine.hypergraph.nodes.size, 3);
    assert.equal(engine.hypergraph.nodes.get("node-1").state_payload.turn, 2);
    assert.ok(engine.hypergraph.edges.has("edge-123"));
    assert.equal(engine.hypergraph.edges.get("edge-123").weight, 0.85);

    // Invariant I2: Monotonicity
    assert.equal(engine.hypergraph.cluster_vector["cluster-eu-west"], 8);
    assert.equal(engine.hypergraph.cluster_vector["cluster-us-east"], 6); // 5 + 1
    assert.equal(engine.state, PartitionState.HEALED);
  });

  test("Dangling edge trips safety circuit fail-closed", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 1);
    const n1 = new HyperNode(
      "node-exist-1",
      "cluster-us-east",
      HyperNodeType.AGENT_CONTEXT,
      { ok: true }
    );
    engine.hypergraph.addNode(n1);

    const badEdge = new HyperEdge(
      "edge-broken",
      ["node-exist-1", "node-missing-99"],
      HyperEdgeType.COORDINATION_LEASE
    );
    const delta = new ResumptionDelta(
      "delta-bad",
      "cluster-eu-west",
      "cluster-us-east",
      1,
      1,
      [],
      [badEdge],
      { "cluster-eu-west": 5 }
    );

    assert.throws(() => {
      engine.executeHealing(delta);
    }, HyperGraphIntegrityError);

    assert.equal(engine.circuit_tripped, true);
    assert.equal(engine.state, PartitionState.CIRCUIT_TRIPPED);
    assert.ok(engine.trip_reason.includes("Dangling edge"));

    // Operations blocked while circuit tripped
    assert.throws(() => {
      engine.formulateHealingPlan("cluster-eu-west", { "cluster-eu-west": 6 });
    }, HealingCircuitTrippedError);

    engine.rearmCircuit();
    assert.equal(engine.circuit_tripped, false);
    assert.equal(engine.state, PartitionState.NOMINAL);
  });

  test("Invariant I2: Negative counter clock regression is rejected", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 1);
    engine.hypergraph.cluster_vector["cluster-us-east"] = 10;

    const badDelta = new ResumptionDelta(
      "delta-malicious",
      "cluster-eu-west",
      "cluster-us-east",
      1,
      1,
      [],
      [],
      { "cluster-us-east": -1, "cluster-eu-west": 5 }
    );

    assert.throws(() => {
      engine.executeHealing(badDelta);
    }, ResumptionVectorRegressionError);

    assert.equal(engine.circuit_tripped, true);
    assert.equal(engine.state, PartitionState.CIRCUIT_TRIPPED);
  });

  test("Invariant I1: Foreign PoE certificate verification and tamper resistance", () => {
    const engine = new PartitionHealingEngine("cluster-us-east", 1);
    const kp1 = generateNodeKeypair();
    const kp2 = generateNodeKeypair();
    const priv1 = kp1.privateKey;
    const pub1 = kp1.publicKeyHex;
    const priv2 = kp2.privateKey;
    const pub2 = kp2.publicKeyHex;

    const oracle = new ByzantineConsensusOracle();
    oracle.registerNode("v1", "cluster-eu-west", pub1);
    oracle.registerNode("v2", "cluster-eu-west", pub2);
    const committeeRoot = oracle.computeCommitteeRoot();

    engine.sync_engine.registerForeignCommittee(
      "cluster-eu-west",
      1,
      committeeRoot,
      { v1: pub1, v2: pub2 }
    );

    const rawCert = {
      certificate_id: "poe-cert-cross-01",
      proposal_id: "prop-cross-01",
      action_type: "HEALING_RESUMPTION",
      view: 0,
      sequence_number: 1,
      digest: "aa".repeat(32),
      pre_state_hash: "bb".repeat(32),
      post_state_hash: "cc".repeat(32),
      signers: ["v1", "v2"],
      signatures: {},
      epoch: 1,
      committee_root: committeeRoot,
      quorum_required: 2,
    };
    rawCert.signatures = {
      v1: signCrossClusterPoe(rawCert, priv1),
      v2: signCrossClusterPoe(rawCert, priv2),
    };
    rawCert.receipt_hash = computeCanonicalPoeHash(rawCert);

    const n1 = new HyperNode(
      "node-poe-1",
      "cluster-eu-west",
      HyperNodeType.AGENT_CONTEXT,
      { verified: true }
    );
    const deltaValid = new ResumptionDelta(
      "delta-poe-valid",
      "cluster-eu-west",
      "cluster-us-east",
      1,
      1,
      [n1],
      [],
      { "cluster-eu-west": 1 },
      rawCert
    );

    const receipt = engine.executeHealing(deltaValid);
    assert.equal(receipt.nodes_reconciled, 1);

    // Tampered PoE signature must be rejected and trip circuit
    engine.reset();
    engine.sync_engine.registerForeignCommittee(
      "cluster-eu-west",
      1,
      committeeRoot,
      { v1: pub1, v2: pub2 }
    );
    const tamperedPoe = {
      ...rawCert,
      signatures: {
        v1: "00".repeat(64),
        v2: "00".repeat(64),
      },
    };
    const deltaTampered = new ResumptionDelta(
      "delta-poe-tampered",
      "cluster-eu-west",
      "cluster-us-east",
      1,
      1,
      [n1],
      [],
      { "cluster-eu-west": 2 },
      tamperedPoe
    );

    assert.throws(() => {
      engine.executeHealing(deltaTampered);
    }, InvalidResumptionProofError);
    assert.equal(engine.circuit_tripped, true);
  });
});
