import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  ProofCommitmentType,
  SpanCommitmentLeaf,
  MerkleAuditProof,
  CausalAuditReceipt,
  CausalProofAccumulator,
  ZkProofEngine,
  globalZkProofEngine,
  sha256Digest,
} from "../src/zk_proof.ts";

describe("Seam 49: Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine", () => {
  test("SpanCommitmentLeaf creation, deterministic hashing, sensitivity, and serialization", () => {
    const leaf1 = new SpanCommitmentLeaf({
      spanId: "span-ts-01",
      turnIndex: 1,
      agentId: "agent-planner",
      payloadHash: sha256Digest("Plan execution steps"),
      parentSpanIds: ["span-root"],
      circuitTripped: false,
    });

    assert.ok(leaf1.leafHash);
    assert.equal(leaf1.leafHash.length, 64);

    const dict = leaf1.toDict();
    assert.equal(dict.span_id, "span-ts-01");
    assert.equal(dict.agent_id, "agent-planner");
    assert.equal(dict.circuit_tripped, false);

    const leaf2 = SpanCommitmentLeaf.fromDict(dict);
    assert.equal(leaf2.spanId, leaf1.spanId);
    assert.equal(leaf2.leafHash, leaf1.leafHash);

    // Sensitivity: changing turnIndex alters leaf hash
    const leafTampered = new SpanCommitmentLeaf({
      spanId: "span-ts-01",
      turnIndex: 2,
      agentId: "agent-planner",
      payloadHash: sha256Digest("Plan execution steps"),
      parentSpanIds: ["span-root"],
      circuitTripped: false,
    });
    assert.notEqual(leafTampered.leafHash, leaf1.leafHash);
  });

  test("CausalProofAccumulator empty root and singleton inclusion proof", () => {
    const acc = new CausalProofAccumulator("swarm-ts-empty");
    assert.equal(acc.totalLeaves, 0);
    const emptyRoot = acc.getMerkleRoot();
    assert.equal(emptyRoot, sha256Digest("empty_swarm_session"));

    const leaf = acc.commitSpan({
      spanId: "span-singleton",
      turnIndex: 0,
      agentId: "agent-single",
      payloadContent: "singleton payload",
    });

    assert.equal(acc.totalLeaves, 1);
    const root = acc.getMerkleRoot();
    assert.equal(root, leaf.leafHash);

    const proof = acc.generateInclusionProof("span-singleton");
    assert.equal(proof.leafHash, leaf.leafHash);
    assert.equal(proof.leafIndex, 0);
    assert.equal(proof.siblingHashes.length, 0);
    assert.equal(CausalProofAccumulator.verifyInclusionProof(proof), true);
  });

  test("CausalProofAccumulator balanced Merkle tree over odd/even leaf counts and proof verification", () => {
    const acc = new CausalProofAccumulator("swarm-ts-odd");
    const spanIds = ["span-0", "span-1", "span-2", "span-3", "span-4"];

    for (let i = 0; i < spanIds.length; i++) {
      acc.commitSpan({
        spanId: spanIds[i],
        turnIndex: i,
        agentId: `agent-${i % 2}`,
        payloadContent: `Turn message payload #${i}`,
        parentSpanIds: i > 0 ? [spanIds[i - 1]] : [],
      });
    }

    assert.equal(acc.totalLeaves, 5);
    const root = acc.getMerkleRoot();
    assert.equal(root.length, 64);

    for (let i = 0; i < spanIds.length; i++) {
      const proof = acc.generateInclusionProof(spanIds[i]);
      assert.equal(proof.leafIndex, i);
      assert.equal(proof.totalLeaves, 5);
      assert.equal(proof.merkleRoot, root);

      const isValid = CausalProofAccumulator.verifyInclusionProof(proof);
      assert.equal(isValid, true);

      // Index-based lookup
      const proofByIdx = acc.generateInclusionProof(i);
      assert.equal(proofByIdx.leafHash, proof.leafHash);
      assert.equal(CausalProofAccumulator.verifyInclusionProof(proofByIdx), true);
    }
  });

  test("CausalAuditReceipt generation, HMAC provenance signing, and verification", () => {
    const acc = new CausalProofAccumulator("swarm-ts-receipt");
    for (let i = 0; i < 4; i++) {
      acc.commitSpan({
        spanId: `step-${i}`,
        turnIndex: i,
        agentId: "agent-coder",
        payloadContent: `Code output part ${i}`,
      });
    }

    const receipt = acc.generateAuditReceipt("step-2", ProofCommitmentType.TURN_INCLUSION);
    assert.ok(receipt.receiptId.startsWith("rcpt_"));
    assert.equal(receipt.swarmId, "swarm-ts-receipt");
    assert.equal(receipt.commitmentType, ProofCommitmentType.TURN_INCLUSION);
    assert.equal(receipt.verified, true);
    assert.equal(receipt.provenanceSignature.length, 64);

    const { isValid, error } = acc.verifyAuditReceipt(receipt);
    assert.equal(isValid, true);
    assert.equal(error, null);

    // Serialization roundtrip
    const dict = receipt.toDict();
    const restored = CausalAuditReceipt.fromDict(dict);
    assert.equal(restored.receiptId, receipt.receiptId);

    const { isValid: isValidRestored } = acc.verifyAuditReceipt(restored);
    assert.equal(isValidRestored, true);
  });

  test("Tamper-detection on span attributes, Merkle sibling hashes, and HMAC signatures", () => {
    const acc = new CausalProofAccumulator("swarm-ts-tamper");
    acc.commitSpan({
      spanId: "valid-span-01",
      turnIndex: 0,
      agentId: "agent-verifier",
      payloadContent: "Critical compliance text",
    });

    const receipt = acc.generateAuditReceipt("valid-span-01");

    // 1. Tamper span attributes
    const tamperedLeaf = new SpanCommitmentLeaf({
      spanId: "valid-span-01",
      turnIndex: 0,
      agentId: "agent-verifier",
      payloadHash: sha256Digest("Altered text"),
      parentSpanIds: [],
      leafHash: receipt.spanCommitment.leafHash,
    });
    const badReceipt1 = new CausalAuditReceipt({
      receiptId: receipt.receiptId,
      swarmId: receipt.swarmId,
      commitmentType: receipt.commitmentType,
      spanCommitment: tamperedLeaf,
      merkleProof: receipt.merkleProof,
      sessionMerkleRoot: receipt.sessionMerkleRoot,
      provenanceSignature: receipt.provenanceSignature,
    });
    const res1 = acc.verifyAuditReceipt(badReceipt1);
    assert.equal(res1.isValid, false);
    assert.ok(res1.error?.includes("Leaf hash does not match"));

    // 2. Tamper provenance HMAC signature
    const badReceipt2 = new CausalAuditReceipt({
      receiptId: receipt.receiptId,
      swarmId: receipt.swarmId,
      commitmentType: receipt.commitmentType,
      spanCommitment: receipt.spanCommitment,
      merkleProof: receipt.merkleProof,
      sessionMerkleRoot: receipt.sessionMerkleRoot,
      provenanceSignature: "deadbeef0123456789abcdefdeadbeef0123456789abcdefdeadbeef0123456789",
    });
    const res2 = acc.verifyAuditReceipt(badReceipt2);
    assert.equal(res2.isValid, false);
    assert.ok(res2.error?.includes("Invalid provenance HMAC"));

    // 3. Wrong secret key
    const res3 = acc.verifyAuditReceipt(receipt, "wrong_secret_key");
    assert.equal(res3.isValid, false);
    assert.ok(res3.error?.includes("Invalid provenance HMAC"));
  });

  test("Circuit breaker trip commitment and verification", () => {
    const acc = new CausalProofAccumulator("swarm-ts-circuit");
    acc.commitSpan({
      spanId: "normal-step",
      turnIndex: 0,
      agentId: "agent-worker",
      payloadContent: "Normal execution step",
    });
    acc.commitSpan({
      spanId: "tripped-step",
      turnIndex: 1,
      agentId: "agent-worker",
      payloadContent: "Recursion collapse detected",
      circuitTripped: true,
      tripReason: "Entropy collapse trip",
    });

    const receipt = acc.generateAuditReceipt("tripped-step", ProofCommitmentType.CIRCUIT_TRIP);
    assert.equal(receipt.commitmentType, ProofCommitmentType.CIRCUIT_TRIP);
    assert.equal(receipt.spanCommitment.circuitTripped, true);
    assert.ok(receipt.spanCommitment.tripReasonHash);

    const { isValid, error } = acc.verifyAuditReceipt(receipt);
    assert.equal(isValid, true);
    assert.equal(error, null);
  });

  test("ZkProofEngine multi-swarm partition isolation and reset", () => {
    const engine = new ZkProofEngine();
    engine.commitSpan("swarm-a", "span-a1", 0, "agent-1", "data a");
    engine.commitSpan("swarm-b", "span-b1", 0, "agent-2", "data b");

    const rootA = engine.getRoot("swarm-a");
    const rootB = engine.getRoot("swarm-b");
    assert.notEqual(rootA, rootB);

    const receiptA = engine.generateReceipt("swarm-a", "span-a1");
    const { isValid } = engine.verifyReceipt(receiptA);
    assert.equal(isValid, true);

    const manifestA = engine.getManifest("swarm-a");
    assert.equal(manifestA.total_leaves, 1);
    assert.equal(manifestA.swarm_id, "swarm-a");

    // Reset swarm-a only
    engine.reset("swarm-a");
    assert.equal(engine.getManifest("swarm-a").total_leaves, 0);
    assert.equal(engine.getManifest("swarm-b").total_leaves, 1);
  });
});
