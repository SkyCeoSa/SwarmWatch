import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

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
  compareVectorClocks,
  verifyCrossClusterPoe,
  signCrossClusterPoe,
  CrossClusterSyncEngine,
  globalCrossClusterSyncEngine,
} from "../src/cross_cluster_sync.ts";

describe("Seam 77: Autonomous Cross-Cluster State Synchronization & Distributed Hyper-Graph Reconciliation", () => {
  beforeEach(() => {
    globalCrossClusterSyncEngine.reset();
  });

  test("Hyper-Graph entities and integrity enforcement", () => {
    const hg = new DistributedHyperGraph({ "cluster-a": 1 });

    const n1 = new HyperNode(
      "n1",
      "cluster-a",
      HyperNodeType.AGENT_CONTEXT,
      { role: "planner", turn: 4 }
    );
    const n2 = new HyperNode(
      "n2",
      "cluster-a",
      HyperNodeType.RESOURCE_LEASE,
      { resource_id: "res-99", leased_to: "agent-1" }
    );
    hg.addNode(n1);
    hg.addNode(n2);

    const e1 = new HyperEdge(
      "e1",
      ["n1", "n2"],
      HyperEdgeType.COORDINATION_LEASE,
      { timeout_ms: 5000 },
      0.95
    );
    hg.addEdge(e1);

    assert.equal(hg.validateIntegrity(), true);
    assert.equal(hg.computeRootDigest().length, 64);

    // HyperEdge with < 2 nodes must throw
    assert.throws(
      () => {
        new HyperEdge(
          "e_invalid",
          ["n1"],
          HyperEdgeType.COORDINATION_LEASE
        );
      },
      HyperGraphIntegrityError
    );

    // Adding edge referencing missing node must throw
    const eDangling = new HyperEdge(
      "e_dangling",
      ["n1", "ghost-node"],
      HyperEdgeType.CAUSAL_DEPENDENCY
    );
    assert.throws(() => {
      hg.addEdge(eDangling);
    }, HyperGraphIntegrityError);
  });

  test("Vector clock causal ordering classification", () => {
    const vcLocal = { c1: 2, c2: 3 };

    // 1. Ahead
    assert.equal(
      compareVectorClocks(vcLocal, { c1: 2, c2: 4 }, 1),
      CausalOrderingVerdict.CAUSALLY_AHEAD
    );

    // 2. Behind / stale
    assert.equal(
      compareVectorClocks(vcLocal, { c1: 1, c2: 3 }, 1),
      CausalOrderingVerdict.CAUSALLY_BEHIND
    );

    // 3. Identical
    assert.equal(
      compareVectorClocks(vcLocal, { ...vcLocal }, 1),
      CausalOrderingVerdict.CAUSALLY_BEHIND
    );

    // 4. Concurrent
    assert.equal(
      compareVectorClocks(vcLocal, { c1: 3, c2: 2 }, 1),
      CausalOrderingVerdict.CONCURRENT
    );

    // 5. Gap detected
    assert.equal(
      compareVectorClocks(vcLocal, { c1: 2, c2: 6 }, 1),
      CausalOrderingVerdict.CAUSAL_GAP_DETECTED
    );
  });

  test("Invariant I1: Issuer-bound Proof-of-Execution certificate verification", () => {
    const kp0 = generateNodeKeypair();
    const kp1 = generateNodeKeypair();
    const kp2 = generateNodeKeypair();
    const kp3 = generateNodeKeypair();

    const knownKeys = {
      v0: kp0.publicKeyHex,
      v1: kp1.publicKeyHex,
      v2: kp2.publicKeyHex,
      v3: kp3.publicKeyHex,
    };

    const oracle = new ByzantineConsensusOracle();
    for (const [vid, pk] of Object.entries(knownKeys)) {
      oracle.registerNode(vid, "cluster-remote", pk);
    }
    const committeeRoot = oracle.computeCommitteeRoot();

    const digest = "aa".repeat(32);
    const rawCert = {
      action_type: "DEPLOY_PIPELINE",
      certificate_id: "poe-cert-cross-01",
      committee_root: committeeRoot,
      digest: digest,
      epoch: 2,
      post_state_hash: "cc".repeat(32),
      pre_state_hash: "bb".repeat(32),
      proposal_id: "prop-cross-01",
      quorum_required: 3,
      sequence_number: 1,
      signatures: {},
      signers: ["v0", "v1", "v2"],
      view: 0,
      receipt_hash: "",
    };
    rawCert.signatures = {
      v0: signCrossClusterPoe(rawCert, kp0.privateKey),
      v1: signCrossClusterPoe(rawCert, kp1.privateKey),
      v2: signCrossClusterPoe(rawCert, kp2.privateKey),
    };
    rawCert.receipt_hash = computeCanonicalPoeHash(rawCert);

    // 1. Authentic passes Invariant I1
    assert.equal(
      verifyCrossClusterPoe(rawCert, committeeRoot, 2, knownKeys),
      true
    );

    // 2. Wrong epoch fails closed
    assert.equal(
      verifyCrossClusterPoe(rawCert, committeeRoot, 3, knownKeys),
      false
    );

    // 3. Wrong committee root fails closed
    assert.equal(
      verifyCrossClusterPoe(rawCert, "ff".repeat(32), 2, knownKeys),
      false
    );

    // 4. Insufficient quorum fails closed
    const underQuorumCert = {
      ...rawCert,
      signers: ["v0", "v1"],
    };
    underQuorumCert.receipt_hash = computeCanonicalPoeHash(underQuorumCert);
    assert.equal(
      verifyCrossClusterPoe(underQuorumCert, committeeRoot, 2, knownKeys),
      false
    );

    // 5. Tampered receipt hash fails closed
    const tamperedCert = {
      ...rawCert,
      receipt_hash: "sw:oracle:poe:v1:forged0000000000000000",
    };
    assert.equal(
      verifyCrossClusterPoe(tamperedCert, committeeRoot, 2, knownKeys),
      false
    );
  });

  test("Cross-cluster CRDT reconciliation and convergence", () => {
    const engineA = new CrossClusterSyncEngine("cluster-a", 1, "00".repeat(32), false);
    const engineB = new CrossClusterSyncEngine("cluster-b", 1, "00".repeat(32), false);
    engineA.registerForeignCommittee("cluster-b", 1, "00".repeat(32));
    engineB.registerForeignCommittee("cluster-a", 1, "00".repeat(32));

    // Cluster A creates node and edge
    engineA.upsertLocalNode("shared-node-1", HyperNodeType.AGENT_CONTEXT, { version: 1 });
    engineA.upsertLocalNode("shared-node-2", HyperNodeType.RESOURCE_LEASE, { status: "ACTIVE" });
    engineA.upsertLocalEdge("shared-edge-1", ["shared-node-1", "shared-node-2"], HyperEdgeType.COORDINATION_LEASE, {}, 0.8);

    // Cluster B creates node and edge concurrently
    engineB.upsertLocalNode("shared-node-1", HyperNodeType.AGENT_CONTEXT, { version: 2 });
    engineB.upsertLocalNode("shared-node-3", HyperNodeType.SHARED_MEMORY, { memory: "bootstrapped" });
    engineB.upsertLocalEdge("shared-edge-1", ["shared-node-1", "shared-node-3"], HyperEdgeType.COORDINATION_LEASE, {}, 0.95);

    // Sync A -> B
    const pktAtoB = engineA.createSyncPacket("cluster-b");
    const receiptB = engineB.ingestSyncPacket(pktAtoB);
    assert.equal(receiptB.causal_verdict, CausalOrderingVerdict.CONCURRENT);
    assert.equal(receiptB.receipt_hash.startsWith("sw:sync:v1:"), true);
    assert.equal(engineB.verifyReconciliationReceipt(receiptB), true);

    // Sync B -> A
    const pktBtoA = engineB.createSyncPacket("cluster-a");
    const receiptA = engineA.ingestSyncPacket(pktBtoA);
    assert.equal(receiptA.receipt_hash.startsWith("sw:sync:v1:"), true);

    // Convergence
    assert.equal(engineA.hypergraph.nodes.size, 3);
    assert.equal(engineB.hypergraph.nodes.size, 3);

    const edgeA = engineA.hypergraph.edges.get("shared-edge-1");
    const edgeB = engineB.hypergraph.edges.get("shared-edge-1");
    assert.deepEqual(edgeA.node_ids, ["shared-node-1", "shared-node-2", "shared-node-3"]);
    assert.deepEqual(edgeB.node_ids, ["shared-node-1", "shared-node-2", "shared-node-3"]);
    assert.equal(edgeA.weight, 0.95);
    assert.equal(edgeB.weight, 0.95);
  });

  test("Fail-closed circuit coupling on invariant violations", () => {
    const engine = new CrossClusterSyncEngine("cluster-c", 1);

    // Unregistered foreign committee root throws and trips circuit
    const badCertPkt = new SyncPacket(
      "pkt-bad",
      "cluster-unregistered",
      "cluster-c",
      5,
      "ee".repeat(32),
      3,
      { "cluster-unregistered": 1 },
      [],
      [],
      { epoch: 5, committee_root: "ee".repeat(32), quorum_required: 3, signers: ["u1", "u2", "u3"] }
    );

    assert.throws(() => {
      engine.ingestSyncPacket(badCertPkt);
    }, UnverifiableCertificateError);

    assert.equal(engine.circuit_tripped, true);
    assert.equal(engine.getStatus().status, SyncStatus.CIRCUIT_TRIPPED);

    // Subsequent operation rejected
    assert.throws(() => {
      engine.upsertLocalNode("n-fail", HyperNodeType.AGENT_CONTEXT, {});
    }, SyncCircuitTrippedError);

    // Rearm
    engine.rearmCircuit();
    assert.equal(engine.circuit_tripped, false);

    // Causal gap trips circuit
    engine.registerForeignCommittee("cluster-remote", 1, "00".repeat(32));
    engine.require_foreign_certificate = false;
    engine.upsertLocalNode("n0", HyperNodeType.AGENT_CONTEXT, {});
    const gapPkt = new SyncPacket(
      "pkt-gap",
      "cluster-remote",
      "cluster-c",
      1,
      "",
      0,
      { "cluster-c": 1, "cluster-remote": 10 },
      [],
      []
    );

    assert.throws(() => {
      engine.ingestSyncPacket(gapPkt);
    }, CausalDiscontinuityError);

    assert.equal(engine.circuit_tripped, true);
  });

  test("Adversarial: P0 #2 audit vulnerabilities neutralized in TS", () => {
    // 1. Repeated signer cannot satisfy quorum
    const kp0 = generateNodeKeypair();
    const knownKeys = {
      v0: kp0.publicKeyHex,
      v1: "01".repeat(32),
      v2: "02".repeat(32),
      v3: "03".repeat(32),
    };
    const rawCert = {
      certificate_id: "poe-fake-repeat",
      proposal_id: "p1",
      action_type: "EXECUTE",
      view: 0,
      sequence_number: 1,
      digest: "aa".repeat(32),
      pre_state_hash: "bb".repeat(32),
      post_state_hash: "cc".repeat(32),
      signers: ["v0", "v0", "v0"],
      signatures: { v0: signBftPayload(kp0.privateKey, "aa".repeat(32)) },
      epoch: 1,
      committee_root: "00".repeat(32),
      quorum_required: 3,
      receipt_hash: "",
    };
    rawCert.receipt_hash = computeCanonicalPoeHash(rawCert);
    assert.equal(verifyCrossClusterPoe(rawCert, "00".repeat(32), 1, knownKeys), false);

    // 2. Unpinned keys fails closed
    assert.equal(verifyCrossClusterPoe(rawCert, "00".repeat(32), 1, undefined), false);
    assert.equal(verifyCrossClusterPoe(rawCert, "00".repeat(32), 1, {}), false);

    // 3. Unregistered foreign cluster rejected even without certificate
    const engine = new CrossClusterSyncEngine("cluster-c", 1);
    const unregPkt = new SyncPacket(
      "pkt-unreg",
      "cluster-never-registered",
      "cluster-c",
      1,
      "00".repeat(32),
      0,
      { "cluster-c": 0 },
      [],
      [],
      null
    );
    assert.throws(() => {
      engine.ingestSyncPacket(unregPkt);
    }, UnverifiableCertificateError);
    assert.equal(engine.circuit_tripped, true);
  });
});

