/**
 * Automated Test Harness for Seam 74 Live Byzantine Consensus & Proof-of-Execution Oracle Simulation (Node.js Native ESM).
 * Validates dual-SDK parity for all 8 operational phases.
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
  generateNodeKeypair,
  signBftPayload,
} from "../src/byzantine_oracle.ts";

test("Seam 74 Live Byzantine Consensus & Proof-of-Execution Oracle Full Lifecycle Simulation", () => {
  const oracle = new ByzantineConsensusOracle("tenant-quantum-fintech");

  // --------------------------------------------------------------------------
  // Phase 1: Multi-Cluster Validator Provisioning & Quorum Math
  // --------------------------------------------------------------------------
  const clusters = ["us-east-aws", "eu-central-gcp", "ap-south-azure", "us-west-onprem"];
  for (let i = 1; i <= clusters.length; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `val-node-${i}`,
      cluster_id: clusters[i - 1],
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
      role: i === 1 ? NodeRole.PRIMARY : NodeRole.REPLICA,
    });
  }

  assert.equal(oracle.totalNodesCount, 4);
  assert.equal(oracle.faultToleranceF, 1);
  assert.equal(oracle.quorumSizeQ, 3);
  const primary = oracle.getPrimaryNode();
  assert.ok(primary);
  assert.equal(primary.node_id, "val-node-1");

  // --------------------------------------------------------------------------
  // Phase 2: Proposal Ingestion & Cryptographic Pre-Prepare
  // --------------------------------------------------------------------------
  const proposal = new ProposalPayload({
    proposal_id: "prop-audit-9901",
    action_type: "HIGH_VALUE_SETTLEMENT",
    target_cluster: "eu-central-gcp",
    tenant_id: "tenant-quantum-fintech",
    pre_state_hash: "state-pre-sha256-48c90b",
    post_state_hash: "state-post-sha256-59d01c",
    payload_data: { transfer_amount_usd: 5000000.0, source: "acc-99", dest: "acc-104" },
  });

  const prePrepare = oracle.propose(proposal);
  assert.equal(prePrepare.phase, BftPhase.PRE_PREPARE);
  assert.equal(prePrepare.view, 0);
  assert.equal(prePrepare.sequence_number, 1);
  assert.equal(prePrepare.signature.length, 64);

  // --------------------------------------------------------------------------
  // Phase 3: Multi-Cluster Prepare Phase & Quorum Assembly
  // --------------------------------------------------------------------------
  const prepMsg2 = oracle.prepare(prePrepare, "val-node-2");
  const prepMsg3 = oracle.prepare(prePrepare, "val-node-3");
  assert.equal(prepMsg2.phase, BftPhase.PREPARE);
  assert.equal(prepMsg3.phase, BftPhase.PREPARE);

  // --------------------------------------------------------------------------
  // Phase 4: Threshold Commit Phase & Quorum Signatures
  // --------------------------------------------------------------------------
  const com1 = oracle.commit(prepMsg2, "val-node-1");
  const com2 = oracle.commit(prepMsg2, "val-node-2");
  const com3 = oracle.commit(prepMsg2, "val-node-3");
  assert.equal(com1.phase, BftPhase.COMMIT);
  assert.equal(com2.phase, BftPhase.COMMIT);
  assert.equal(com3.phase, BftPhase.COMMIT);

  // --------------------------------------------------------------------------
  // Phase 5: Proof-of-Execution Certificate & Receipt Hash
  // --------------------------------------------------------------------------
  const cert = oracle.executeAndCertify(proposal.proposal_id);
  assert.ok(cert.receipt_hash.startsWith("sw:oracle:poe:v1:"));
  assert.equal(cert.signers.length, 3);
  assert.deepEqual(new Set(cert.signers), new Set(["val-node-1", "val-node-2", "val-node-3"]));

  const verified = oracle.verifyPoeCertificate(cert);
  assert.equal(verified, true);

  // --------------------------------------------------------------------------
  // Phase 6: Byzantine Equivocation Detection & Node Slashing
  // --------------------------------------------------------------------------
  const rogueProposal = new ProposalPayload({
    proposal_id: "prop-rogue-double-spend",
    action_type: "UNAUTHORIZED_EXFILTRATION",
    target_cluster: "us-west-onprem",
  });
  const rogueMsg = new BftMessage({
    msg_id: "rogue-msg-double-sign",
    phase: BftPhase.PRE_PREPARE,
    view: prePrepare.view,
    sequence_number: prePrepare.sequence_number,
    proposal_id: rogueProposal.proposal_id,
    digest: "conflicting-malicious-digest-cafebabe",
    node_id: "val-node-1",
    cluster_id: "us-east-aws",
  });
  const node1 = oracle.getNode("val-node-1");
  rogueMsg.signature = signBftPayload(node1.secret_key, rogueMsg.signingContent());

  assert.throws(
    () => oracle.prepare(rogueMsg, "val-node-2"),
    (err) => err instanceof EquivocationDetectedError
  );

  const slashedNode = oracle.getNode("val-node-2");
  assert.ok(slashedNode);
  assert.equal(slashedNode.status, NodeStatus.SLASHED);
  assert.ok(slashedNode.fault_score >= 100);

  // --------------------------------------------------------------------------
  // Phase 7: Fail-Closed Circuit Breaker Coupling
  // --------------------------------------------------------------------------
  oracle.tripCircuit();
  assert.equal(oracle.isCircuitTripped(), true);

  assert.throws(
    () => oracle.propose(new ProposalPayload({ proposal_id: "prop-blocked", action_type: "ACTION", target_cluster: "cluster-1" })),
    (err) => err instanceof OracleCircuitTrippedError
  );

  oracle.resetCircuit();
  assert.equal(oracle.isCircuitTripped(), false);

  // --------------------------------------------------------------------------
  // Phase 8: Microsecond Precision Latency Benchmark & Operations
  // --------------------------------------------------------------------------
  const benchOracle = new ByzantineConsensusOracle();
  for (let i = 1; i <= 4; i++) {
    const kp = generateNodeKeypair();
    benchOracle.registerNode({
      node_id: `bench-node-${i}`,
      cluster_id: `cloud-${i}`,
      public_key_hex: kp.publicKeyHex,
      secret_key: kp.privateKey,
    });
  }

  const iterations = 500;
  const t0 = performance.now();
  for (let k = 0; k < iterations; k++) {
    const p = new ProposalPayload({
      proposal_id: `prop-bench-${k}`,
      action_type: "SWARM_TRANSFER",
      target_cluster: "cloud-1",
    });
    const pre = benchOracle.propose(p);
    benchOracle.prepare(pre, "bench-node-2");
    benchOracle.prepare(pre, "bench-node-3");
    benchOracle.commit(pre, "bench-node-1");
    benchOracle.commit(pre, "bench-node-2");
    benchOracle.commit(pre, "bench-node-3");
    const c = benchOracle.executeAndCertify(p.proposal_id);
    assert.ok(c.receipt_hash.startsWith("sw:oracle:poe:v1:"));
  }
  const totalMs = performance.now() - t0;
  const avgUs = (totalMs / iterations) * 1000.0;
  const perOpUs = avgUs / 7.0;

  assert.ok(perOpUs < 250.0, `Consensus per-operation latency ${perOpUs.toFixed(2)}µs exceeded 250µs SLO`);

  const st = benchOracle.status();
  assert.equal(st.total_nodes, 4);
  assert.equal(st.total_certificates, iterations);
  assert.equal(st.circuit_tripped, false);
});
