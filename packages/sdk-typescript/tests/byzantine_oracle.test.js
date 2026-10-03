/**
 * Test Suite for SwarmWatch Cross-Cluster Byzantine Consensus & Proof-of-Execution Oracle (Seam 74 / ADR 0085).
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  ByzantineConsensusOracle,
  ProposalPayload,
  BftMessage,
  BftPhase,
  NodeRole,
  NodeStatus,
  EquivocationDetectedError,
  OracleCircuitTrippedError,
  ProofOfExecutionCertificate,
  generateNodeKeypair,
  signBftPayload,
} from "../src/byzantine_oracle.ts";

test("Node registration and PBFT quorum formula: N >= 3f + 1, Q = 2f + 1", () => {
  const oracle = new ByzantineConsensusOracle();

  // 1 node
  const kp1 = generateNodeKeypair();
  oracle.registerNode({
    node_id: "node-1",
    cluster_id: "cluster-us-east",
    public_key_hex: kp1.publicKeyHex,
    secret_key: kp1.privateKey,
    role: NodeRole.PRIMARY,
  });

  assert.equal(oracle.totalNodesCount, 1);
  assert.equal(oracle.activeNodesCount, 1);
  assert.equal(oracle.faultToleranceF, 0);
  assert.equal(oracle.quorumSizeQ, 1);

  // Add 3 more nodes (total 4 -> f = 1, Q = 3)
  for (let i = 2; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node-${i}`,
      cluster_id: `cluster-${i}`,
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
      role: NodeRole.REPLICA,
    });
  }

  assert.equal(oracle.totalNodesCount, 4);
  assert.equal(oracle.activeNodesCount, 4);
  assert.equal(oracle.faultToleranceF, 1);
  assert.equal(oracle.quorumSizeQ, 3);

  // Add 3 more nodes (total 7 -> f = 2, Q = 5)
  for (let i = 5; i <= 7; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node-${i}`,
      cluster_id: `cluster-${i}`,
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
      role: NodeRole.REPLICA,
    });
  }

  assert.equal(oracle.totalNodesCount, 7);
  assert.equal(oracle.faultToleranceF, 2);
  assert.equal(oracle.quorumSizeQ, 5);
});

test("Complete PBFT consensus lifecycle and PoE certificate generation", () => {
  const oracle = new ByzantineConsensusOracle();

  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `val-node-${i}`,
      cluster_id: `cluster-${i}`,
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
      role: i === 1 ? NodeRole.PRIMARY : NodeRole.REPLICA,
    });
  }

  // 1. Propose
  const proposal = new ProposalPayload({
    proposal_id: "prop-finance-901",
    action_type: "DATABASE_MIGRATION",
    target_cluster: "cluster-1",
    tenant_id: "tenant-acme",
    pre_state_hash: "pre-sha256-0001",
    post_state_hash: "post-sha256-0002",
    payload_data: { table: "ledger_entries", shards: 64 },
  });

  const prePrepareMsg = oracle.propose(proposal);
  assert.equal(prePrepareMsg.phase, BftPhase.PRE_PREPARE);
  assert.equal(prePrepareMsg.view, 0);
  assert.equal(prePrepareMsg.sequence_number, 1);
  assert.equal(prePrepareMsg.signature.length, 64);

  // 2. Prepare Phase
  const prep2 = oracle.prepare(prePrepareMsg, "val-node-2");
  const prep3 = oracle.prepare(prePrepareMsg, "val-node-3");
  assert.equal(prep2.phase, BftPhase.PREPARE);
  assert.equal(prep3.phase, BftPhase.PREPARE);

  // 3. Commit Phase
  oracle.commit(prep2, "val-node-1");
  oracle.commit(prep2, "val-node-2");
  oracle.commit(prep2, "val-node-3");

  // 4. Decided / Certify Execution
  const cert = oracle.executeAndCertify(proposal.proposal_id);
  assert.equal(cert.proposal_id, "prop-finance-901");
  assert.equal(cert.action_type, "DATABASE_MIGRATION");
  assert.ok(cert.signers.length >= 3);
  assert.ok(cert.receipt_hash.startsWith("sw:oracle:poe:v1:"));
  assert.ok(cert.duration_us >= 0.0);

  // 5. Independent Verification
  assert.equal(oracle.verifyPoeCertificate(cert), true);
});

test("PoE certificate tamper detection", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node-${i}`,
      cluster_id: "cluster-us",
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
    });
  }

  const proposal = new ProposalPayload({
    proposal_id: "prop-tamper-01",
    action_type: "PAYMENT_TRANSFER",
    target_cluster: "cluster-us",
    pre_state_hash: "state-1",
    post_state_hash: "state-2",
  });

  const prePrep = oracle.propose(proposal);
  oracle.prepare(prePrep, "node-2");
  oracle.prepare(prePrep, "node-3");
  oracle.commit(prePrep, "node-1");
  oracle.commit(prePrep, "node-2");
  oracle.commit(prePrep, "node-3");

  const cert = oracle.executeAndCertify(proposal.proposal_id);
  assert.equal(oracle.verifyPoeCertificate(cert), true);

  // Tamper with post-state hash
  const tamperedCert = new ProofOfExecutionCertificate({
    certificate_id: cert.certificate_id,
    proposal_id: cert.proposal_id,
    action_type: cert.action_type,
    view: cert.view,
    sequence_number: cert.sequence_number,
    digest: cert.digest,
    pre_state_hash: cert.pre_state_hash,
    post_state_hash: "forged-malicious-post-state",
    signers: cert.signers,
    signatures: cert.signatures,
    timestamp_ns: cert.timestamp_ns,
    receipt_hash: cert.receipt_hash,
  });

  assert.equal(oracle.verifyPoeCertificate(tamperedCert), false);
});

test("Byzantine equivocation detection and node slashing", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node-${i}`,
      cluster_id: "cluster-a",
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
    });
  }

  const prop1 = new ProposalPayload({
    proposal_id: "prop-1",
    action_type: "ACTION_A",
    target_cluster: "cluster-a",
  });
  const prop2 = new ProposalPayload({
    proposal_id: "prop-2",
    action_type: "ACTION_B",
    target_cluster: "cluster-a",
  });

  const prePrep1 = oracle.propose(prop1);
  oracle.prepare(prePrep1, "node-2");

  // Rogue pre-prepare with different digest signed by node-1
  const roguePrePrep = new BftMessage({
    msg_id: "rogue-msg",
    phase: BftPhase.PRE_PREPARE,
    view: prePrep1.view,
    sequence_number: prePrep1.sequence_number,
    proposal_id: prop2.proposal_id,
    digest: "conflicting-fake-digest-9999",
    node_id: "node-1",
    cluster_id: "cluster-a",
  });

  const node1 = oracle.getNode("node-1");
  roguePrePrep.signature = signBftPayload(node1.secret_key, roguePrePrep.signingContent());

  assert.throws(
    () => oracle.prepare(roguePrePrep, "node-2"),
    (err) => err instanceof EquivocationDetectedError
  );

  const slashedNode = oracle.getNode("node-2");
  assert.ok(slashedNode);
  assert.equal(slashedNode.status, NodeStatus.SLASHED);
  assert.ok(slashedNode.fault_score >= 100);
  assert.equal(oracle.getEquivocationEvents().length, 1);
});

test("Fail-closed safety circuit coupling", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node-${i}`,
      cluster_id: "cluster-a",
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
    });
  }

  const proposal = new ProposalPayload({
    proposal_id: "prop-circuit-01",
    action_type: "SAFE_ACTION",
    target_cluster: "cluster-a",
  });
  const prePrep = oracle.propose(proposal);

  oracle.tripCircuit();
  assert.equal(oracle.isCircuitTripped(), true);

  assert.throws(
    () => oracle.prepare(prePrep, "node-2"),
    (err) => err instanceof OracleCircuitTrippedError
  );
  assert.throws(
    () => oracle.commit(prePrep, "node-2"),
    (err) => err instanceof OracleCircuitTrippedError
  );
  assert.throws(
    () => oracle.executeAndCertify(proposal.proposal_id),
    (err) => err instanceof OracleCircuitTrippedError
  );

  oracle.resetCircuit();
  assert.equal(oracle.isCircuitTripped(), false);
});

test("View change recovery and primary rotation", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node-${i}`,
      cluster_id: "cluster-a",
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
    });
  }

  const primaryV0 = oracle.getPrimaryNode();
  assert.ok(primaryV0);

  const newView = oracle.triggerViewChange("Primary timeout");
  assert.equal(newView, 1);

  const primaryV1 = oracle.getPrimaryNode();
  assert.ok(primaryV1);
  assert.ok(primaryV1.node_id !== primaryV0.node_id || oracle.totalNodesCount === 1);
});

test("Sub-250µs execution benchmark", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `b-node-${i}`,
      cluster_id: "cluster-bench",
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
    });
  }

  const iterations = 200;
  const t0 = performance.now();

  for (let k = 0; k < iterations; k++) {
    const prop = new ProposalPayload({
      proposal_id: `prop-bench-${k}`,
      action_type: "BENCH_ACTION",
      target_cluster: "cluster-bench",
    });
    const pre = oracle.propose(prop);
    oracle.prepare(pre, "b-node-2");
    oracle.prepare(pre, "b-node-3");
    oracle.commit(pre, "b-node-1");
    oracle.commit(pre, "b-node-2");
    oracle.commit(pre, "b-node-3");
    const cert = oracle.executeAndCertify(prop.proposal_id);
    assert.ok(cert.receipt_hash.startsWith("sw:oracle:poe:v1:"));
  }

  const totalMs = performance.now() - t0;
  const avgUs = (totalMs / iterations) * 1000.0;
  const perOpUs = avgUs / 7.0;

  assert.ok(perOpUs < 250.0, `Consensus per-operation latency ${perOpUs.toFixed(2)}µs exceeded 250µs SLO`);
});
