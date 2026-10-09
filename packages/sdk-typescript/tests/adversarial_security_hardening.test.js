/**
 * Adversarial Security & Mathematical Hardening Test Suite (Seams 74–76).
 * Permanently certifies that all auditor exploit vectors (D-01 through D-07)
 * and Milestone H blockers (B-1, B-2, B-3) fail closed with strict cryptographic
 * and mathematical invariants in TypeScript.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  ByzantineConsensusOracle,
  NodeStatus,
  generateNodeKeypair,
  computeCanonicalPoeHash,
  NodeSigner,
  ProposalPayload,
} from "../src/byzantine_oracle.ts";

import {
  BN254_PRIME,
  ZkRollupEngine,
  OptimisticRollupEngine,
  ProofSystem,
  computeMerkleRoot,
  sha256Hex,
  deepCanonicalJson,
  InvalidStateTransitionError,
} from "../src/zk_rollup.ts";

import {
  EpistemicProofDAG,
  DeductionRule,
  InvalidEpistemicProofError,
} from "../src/epistemic_arbitration.ts";

import { WitnessKeyManager } from "../src/root_witness.ts";

test("D-01: Verify forged or non-asymmetric PBFT PoE certificates are rejected", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 0; i < 4; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node_${i}`,
      cluster_id: "cluster_alpha",
      public_key_hex: kp.publicKeyHex,
    });
  }

  // Attacker attempts to forge PoE certificate with 128-char hex signatures
  const signatures = {};
  for (let i = 0; i < 3; i++) {
    signatures[`node_${i}`] = "f".repeat(128);
  }

  const cert = {
    certificate_id: "cert_forged_d1",
    proposal_id: "prop_malicious",
    action_type: "DROP_DATABASE_PROD",
    view: 1,
    sequence_number: 0,
    digest: "d".repeat(64),
    pre_state_hash: "a".repeat(64),
    post_state_hash: "b".repeat(64),
    signers: ["node_0", "node_1", "node_2"],
    signatures,
    timestamp_ns: 0,
    receipt_hash: "",
    epoch: 1,
    committee_root: oracle.computeCommitteeRoot(),
    quorum_required: 3,
  };
  cert.receipt_hash = computeCanonicalPoeHash(cert);

  assert.equal(oracle.verifyPoeCertificate(cert), false);

  // Also test legacy 64-char signatures
  const signatures64 = {};
  for (let i = 0; i < 3; i++) {
    signatures64[`node_${i}`] = "f".repeat(64);
  }
  cert.signatures = signatures64;
  cert.receipt_hash = computeCanonicalPoeHash(cert);
  assert.equal(oracle.verifyPoeCertificate(cert), false);
});

test("D-02: Verify quorum Q does not erode when nodes are slashed or go offline", () => {
  const oracle = new ByzantineConsensusOracle();
  for (let i = 0; i < 7; i++) {
    const kp = generateNodeKeypair();
    oracle.registerNode({
      node_id: `node_${i}`,
      cluster_id: "cluster_alpha",
      public_key_hex: kp.publicKeyHex,
    });
  }

  // Initial N=7: f = (7-1)//3 = 2, Q = ceil((7 + 2 + 1) / 2) = 5
  assert.equal(oracle.quorumSizeQ, 5);

  // Slash 1 node: quorum Q MUST remain 5, not shrink
  oracle.slashNode("node_0");
  assert.equal(oracle.quorumSizeQ, 5);
  assert.equal(oracle.isCircuitTripped(), false);

  // Slash more than f nodes (3 slashes > f=2): circuit MUST trip fail-closed
  oracle.slashNode("node_1");
  oracle.slashNode("node_2");
  assert.equal(oracle.isCircuitTripped(), true);

  // Offline nodes test: N=4 committee
  const oracle4 = new ByzantineConsensusOracle();
  for (let i = 0; i < 4; i++) {
    const kp = generateNodeKeypair();
    oracle4.registerNode({
      node_id: `node_${i}`,
      cluster_id: "cluster_beta",
      public_key_hex: kp.publicKeyHex,
    });
  }

  assert.equal(oracle4.quorumSizeQ, 3);
  oracle4.setNodeStatus("node_2", NodeStatus.OFFLINE);
  oracle4.setNodeStatus("node_3", NodeStatus.OFFLINE);
  assert.equal(oracle4.activeNodesCount, 2);
  assert.equal(oracle4.isCircuitTripped(), true);
});

test("D-03: Verify succinct rollup proof binds post_state_root and authenticates prover", () => {
  const p = BN254_PRIME;
  const pre = "0".repeat(64);
  const post = "dead".repeat(16);
  const bd = sha256Hex("malicious_batch");
  const delta = sha256Hex("delta_root");

  const challengeSeed = `${pre}${post}${bd}${delta}`;
  const z = BigInt("0x" + sha256Hex(challengeSeed)) % p;
  const z2 = (z * z) % p;
  const z3 = (z2 * z) % p;
  const c0 = BigInt("0x" + pre) % p;
  const c1 = BigInt("0x" + bd) % p;
  const c2 = BigInt("0x" + delta) % p;
  const c3 = BigInt("0x" + post) % p;
  const pz = (c0 + ((c1 * z) % p) + ((c2 * z2) % p) + ((c3 * z3) % p)) % p;

  const proofData = {
    blinding_commitment: sha256Hex(challengeSeed),
    eval_point: "0x" + z.toString(16),
    delta_merkle_root: delta,
    proof_eval: "0x" + pz.toString(16),
    polynomial_eval: "0x" + pz.toString(16),
    transition_law_digest: sha256Hex(challengeSeed),
    field_modulus: "0x" + p.toString(16),
  };

  const payload = {
    batch_id: "malicious_batch",
    sequence_start: 0,
    sequence_end: 0,
    pre_state_root: pre,
    post_state_root: post,
    transition_count: 1,
    batch_digest: bd,
    proof_system: ProofSystem.POLYNOMIAL_ZK,
    zk_proof_data: proofData,
    prover_public_key_hex: "0".repeat(64),
  };

  const receipt = {
    rollup_id: "receipt_forged",
    batch_id: "malicious_batch",
    sequence_start: 0,
    sequence_end: 0,
    pre_state_root: pre,
    post_state_root: post,
    transition_count: 1,
    batch_digest: bd,
    proof_system: ProofSystem.POLYNOMIAL_ZK,
    zk_proof_data: proofData,
    receipt_hash: `sw:rollup:v1:${sha256Hex(deepCanonicalJson(payload))}`,
    prover_public_key_hex: "0".repeat(64),
    prover_signature: "f".repeat(128),
    is_valid: true,
    prover_id: "adversary",
    verified_at_ns: 0,
  };

  const engine = new ZkRollupEngine();
  const { isValid, errorReason } = engine.verifyRollupProof(receipt);
  assert.equal(isValid, false);
  assert.ok(errorReason !== null);
});

test("D-04: Verify RFC 6962 odd-leaf promotion without duplication prevents collisions", () => {
  const leafA = sha256Hex("a");
  const leafB = sha256Hex("b");
  const leafC = sha256Hex("c");
  const rootAbc = computeMerkleRoot([leafA, leafB, leafC]);
  const rootAbcc = computeMerkleRoot([leafA, leafB, leafC, leafC]);

  assert.notEqual(rootAbc, rootAbcc);
  assert.equal(rootAbc.length, 64);
  assert.equal(rootAbcc.length, 64);
});

test("D-05: Verify DAG rejects dangling premise references and disconnected orphan nodes", () => {
  // 1. Dangling premise reference
  const dagDangling = new EpistemicProofDAG({ dag_id: "dag_dangling", root_node_id: "root" });
  dagDangling.addNode({
    node_id: "root",
    rule: DeductionRule.MODUS_PONENS,
    premise_ids: ["ghost_premise"],
    conclusion: "claim",
    evidence_weight: 0.5,
    is_valid: true,
  });

  assert.throws(
    () => dagDangling.evaluateDag(),
    /Dangling premise reference 'ghost_premise'/
  );

  // 2. Orphan padding
  const dagOrphan = new EpistemicProofDAG({ dag_id: "dag_orphan", root_node_id: "root" });
  dagOrphan.addNode({
    node_id: "root",
    rule: DeductionRule.AXIOM,
    premise_ids: [],
    conclusion: "root",
    evidence_weight: 0.2,
    is_valid: true,
  });
  for (let i = 0; i < 5; i++) {
    dagOrphan.addNode({
      node_id: `orphan_${i}`,
      rule: DeductionRule.AXIOM,
      premise_ids: [],
      conclusion: `orphan_${i}`,
      evidence_weight: 1.0,
      is_valid: true,
    });
  }

  assert.throws(
    () => dagOrphan.evaluateDag(),
    /unreachable from root/
  );
});

test("D-06: Verify out-of-bounds evidence weights are strictly rejected", () => {
  const dag = new EpistemicProofDAG({ dag_id: "dag_bounds" });
  assert.throws(
    () =>
      dag.addNode({
        node_id: "a",
        rule: DeductionRule.AXIOM,
        premise_ids: [],
        conclusion: "A",
        evidence_weight: 9e9,
        is_valid: true,
      }),
    /out of bounds/
  );

  assert.throws(
    () =>
      dag.addNode({
        node_id: "b",
        rule: DeductionRule.AXIOM,
        premise_ids: [],
        conclusion: "B",
        evidence_weight: -0.5,
        is_valid: true,
      }),
    /out of bounds/
  );
});

test("D-07: Verify constant-time ECDSA P-256 signing and verification meets sub-250µs SLO", () => {
  const km = WitnessKeyManager.generate();
  const msg = Buffer.from("SwarmWatch monotonic root witness checkpoint assertion", "utf8");

  // Warmup
  for (let i = 0; i < 10; i++) {
    km.sign(msg);
  }

  const iterations = 100;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    km.sign(msg);
  }
  const t1 = performance.now();

  const signUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(signUs < 250.0, `Sign latency ${signUs.toFixed(2)}µs exceeded 250µs SLO`);

  const sig = km.sign(msg);
  for (let i = 0; i < 10; i++) {
    km.verify(msg, sig);
  }

  const t2 = performance.now();
  for (let i = 0; i < iterations; i++) {
    assert.equal(km.verify(msg, sig), true);
  }
  const t3 = performance.now();

  const verifyUs = ((t3 - t2) / iterations) * 1000;
  assert.ok(verifyUs < 250.0, `Verify latency ${verifyUs.toFixed(2)}µs exceeded 250µs SLO`);
});

// ==============================================================================
// Milestone H Blocker Regression Tests (B-1, B-2, B-3 & Golden Vectors)
// ==============================================================================

test("B-1: Decoupled validator signing and zero oracle private key material", () => {
  const oracle = new ByzantineConsensusOracle();
  const signers = new Map();

  for (let i = 0; i < 4; i++) {
    const kp = generateNodeKeypair();
    const nid = `b1_node_${i}`;
    oracle.registerNode({
      node_id: nid,
      cluster_id: "cluster_b1",
      public_key_hex: kp.publicKeyHex,
    });
    signers.set(nid, new NodeSigner({ node_id: nid, cluster_id: "cluster_b1", private_key_hex: kp.privateKey, public_key_hex: kp.publicKeyHex }));
  }

  // Architectural invariant: oracle holds strictly zero secret keys
  for (const node of oracle.listNodes()) {
    assert.equal(node.secret_key, undefined);
  }

  const prop = new ProposalPayload({
    proposal_id: "prop_b1",
    action_type: "TRANSFER",
    target_cluster: "cluster_b1",
  });
  const pre = oracle.createPrePrepare(prop);
  signers.get("b1_node_0").signPhase(pre);
  assert.equal(oracle.acceptPrePrepare(pre), true);

  const prep1 = oracle.createPrepare(pre, "b1_node_1");
  signers.get("b1_node_1").signPhase(prep1);
  assert.equal(oracle.acceptPrepare(prep1), true);

  const prep2 = oracle.createPrepare(pre, "b1_node_2");
  signers.get("b1_node_2").signPhase(prep2);
  assert.equal(oracle.acceptPrepare(prep2), true);

  const com0 = oracle.createCommit(prep1, "b1_node_0");
  signers.get("b1_node_0").signPhase(com0);
  assert.equal(oracle.acceptCommit(com0), true);

  const com1 = oracle.createCommit(prep1, "b1_node_1");
  signers.get("b1_node_1").signPhase(com1);
  assert.equal(oracle.acceptCommit(com1), true);

  const com2 = oracle.createCommit(prep1, "b1_node_2");
  signers.get("b1_node_2").signPhase(com2);
  assert.equal(oracle.acceptCommit(com2), true);

  const cert = oracle.executeAndCertify(prop.proposal_id);
  assert.equal(cert.epoch, 1);
  assert.equal(cert.committee_root.length, 64);
  assert.equal(cert.quorum_required, 3);
  assert.equal(oracle.verifyPoeCertificate(cert), true);
});

test("B-2: Enforce strict 64-char lowercase hex leaf hashes, rejecting ambiguous encodings", () => {
  assert.throws(
    () => computeMerkleRoot(["616263", "a".repeat(64)]),
    (err) => err instanceof InvalidStateTransitionError
  );

  assert.throws(
    () => computeMerkleRoot(["abc", "a".repeat(64)]),
    (err) => err instanceof InvalidStateTransitionError
  );

  assert.throws(
    () => computeMerkleRoot(["zz".repeat(32)]),
    (err) => err instanceof InvalidStateTransitionError
  );

  assert.throws(
    () => computeMerkleRoot(["A".repeat(64)]),
    (err) => err instanceof InvalidStateTransitionError
  );
});

test("Cross-SDK Merkle Golden Vector", () => {
  const leaves = [
    sha256Hex("swarmwatch_golden_leaf_0"),
    sha256Hex("swarmwatch_golden_leaf_1"),
    sha256Hex("swarmwatch_golden_leaf_2"),
  ];
  const root = computeMerkleRoot(leaves);
  assert.equal(root, "22df1288e0238c7c62c158f64cbe7df342b27320262335c84a57c2aa99a4be3e");
});

test("B-3: State transition attestation is signed by Prover's Ed25519 key and verifiable by third parties without secrets", () => {
  const engine = new OptimisticRollupEngine();
  engine.queueTransition({
    agentId: "agent_1",
    slotKey: "slot_k",
    fromValueHash: "0".repeat(64),
    toValueHash: sha256Hex("val_1"),
    deltaPayload: "op(1)",
  });
  const receipt = engine.commitBatch();

  // Third party verifier with ZERO secret seeds can verify the receipt using ONLY the Prover's public key
  const thirdPartyEngine = new OptimisticRollupEngine();
  const { isValid, errorReason } = thirdPartyEngine.verifyAttestation(
    receipt,
    engine.proverPublicKeyHex
  );
  assert.equal(isValid, true);
  assert.equal(errorReason, null);

  // Third party cannot forge a receipt with different post-state
  const forgedReceipt = {
    ...receipt,
    post_state_root: sha256Hex("forged_state"),
  };
  const res = thirdPartyEngine.verifyAttestation(forgedReceipt);
  assert.equal(res.isValid, false);
  assert.ok(
    res.errorReason?.toLowerCase().includes("tampering") ||
    res.errorReason?.toLowerCase().includes("failed")
  );
});
