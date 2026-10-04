/**
 * Test Suite for SwarmWatch Zero-Knowledge State Transition Rollup & Succinct Proof Verification (Seam 75 / ADR 0086).
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  BN254_PRIME,
  StateMerkleTree,
  ZkRollupEngine,
  RollupStatus,
  ProofSystem,
  RollupCircuitTrippedError,
  SequenceDiscontinuityError,
  FraudulentStateTransitionError,
  RollupVerificationFailedError,
  ZkRollupError,
  sha256Hex,
} from "../src/zk_rollup.ts";

test("StateMerkleTree: Sparse Merkle tree deterministic roots and updates", () => {
  const tree = new StateMerkleTree();
  const initialRoot = tree.getRoot();
  assert.equal(initialRoot, "0".repeat(64));

  const val1 = sha256Hex("agent_alpha_state_v1");
  tree.setSlot("agent_alpha", val1);
  const root1 = tree.getRoot();
  assert.notEqual(root1, initialRoot);
  assert.equal(root1.length, 64);

  const val2 = sha256Hex("agent_beta_state_v1");
  tree.setSlot("agent_beta", val2);
  const root2 = tree.getRoot();
  assert.notEqual(root2, root1);

  // Clone isolation
  const clone = tree.clone();
  assert.equal(clone.getRoot(), root2);
  clone.setSlot("agent_gamma", sha256Hex("gamma_v1"));
  assert.notEqual(clone.getRoot(), tree.getRoot());
  assert.equal(tree.getSlot("agent_gamma"), "0".repeat(64));
});

test("ZkRollupEngine: Strict sequence monotonicity and atomic queuing", () => {
  const engine = new ZkRollupEngine();
  const v0 = "0".repeat(64);
  const v1 = sha256Hex("state_1");
  const v2 = sha256Hex("state_2");

  const t1 = engine.queueTransition({
    agentId: "agent_1",
    slotKey: "slot_main",
    fromValueHash: v0,
    toValueHash: v1,
    deltaPayload: "increment(1)",
  });
  assert.equal(t1.sequence, 1);
  assert.equal(t1.slot_key, "slot_main");
  assert.equal(t1.to_value_hash, v1);

  const t2 = engine.queueTransition({
    agentId: "agent_1",
    slotKey: "slot_main",
    fromValueHash: v1,
    toValueHash: v2,
    deltaPayload: "increment(2)",
  });
  assert.equal(t2.sequence, 2);
  assert.equal(t2.to_value_hash, v2);

  // Attempt gapped sequence (seq 4 instead of 3)
  assert.throws(
    () => {
      engine.queueTransition({
        agentId: "agent_1",
        slotKey: "slot_main",
        fromValueHash: v2,
        toValueHash: sha256Hex("state_3"),
        deltaPayload: "increment(3)",
        sequence: 4,
      });
    },
    (err) => err instanceof SequenceDiscontinuityError
  );

  assert.equal(engine.getStatus().circuit_tripped, true);
});

test("ZkRollupEngine: Fraudulent pre-state rejection and circuit trip", () => {
  const engine = new ZkRollupEngine();
  const v0 = "0".repeat(64);
  const v1 = sha256Hex("state_1");

  engine.queueTransition({
    agentId: "agent_1",
    slotKey: "slot_account",
    fromValueHash: v0,
    toValueHash: v1,
    deltaPayload: "init()",
  });

  const forgedPreState = sha256Hex("forged_pre_state");
  assert.throws(
    () => {
      engine.queueTransition({
        agentId: "agent_1",
        slotKey: "slot_account",
        fromValueHash: forgedPreState,
        toValueHash: sha256Hex("state_2"),
        deltaPayload: "mutate()",
      });
    },
    (err) => err instanceof FraudulentStateTransitionError
  );

  assert.equal(engine.getStatus().circuit_tripped, true);
});

test("ZkRollupEngine: Batch commitment and succinct ZK proof synthesis", () => {
  const engine = new ZkRollupEngine();

  const currSlots = {};
  for (let i = 0; i < 5; i++) {
    currSlots[`slot_${i}`] = "0".repeat(64);
  }

  for (let i = 0; i < 5; i++) {
    const nextVal = sha256Hex(`value_${i}`);
    engine.queueTransition({
      agentId: `agent_${i}`,
      slotKey: `slot_${i}`,
      fromValueHash: currSlots[`slot_${i}`],
      toValueHash: nextVal,
      deltaPayload: `write_${i}`,
      privateScratchpad: `secret_scratchpad_${i}`,
    });
    currSlots[`slot_${i}`] = nextVal;
  }

  const receipt = engine.commitBatch("batch_alpha_001");
  assert.equal(receipt.batch_id, "batch_alpha_001");
  assert.equal(receipt.transition_count, 5);
  assert.equal(receipt.sequence_start, 1);
  assert.equal(receipt.sequence_end, 5);
  assert.ok(receipt.receipt_hash.startsWith("sw:rollup:v1:"));
  assert.equal(receipt.is_valid, true);

  const zkData = receipt.zk_proof_data;
  assert.ok("eval_point" in zkData);
  assert.ok("proof_eval" in zkData);
  assert.ok("blinding_commitment" in zkData);
  assert.ok("delta_merkle_root" in zkData);

  const verification = engine.verifyRollupProof(receipt);
  assert.equal(verification.isValid, true);
  assert.equal(verification.errorReason, null);
});

test("ZkRollupEngine: Proof verification fails on payload tampering", () => {
  const engine = new ZkRollupEngine();
  engine.queueTransition({
    agentId: "agent_x",
    slotKey: "x_slot",
    fromValueHash: "0".repeat(64),
    toValueHash: sha256Hex("x_val"),
    deltaPayload: "set_x",
  });
  const receipt = engine.commitBatch();

  // 1. Tamper with proof_eval
  const tamperedReceipt = {
    ...receipt,
    zk_proof_data: {
      ...receipt.zk_proof_data,
      proof_eval: "0x123456789",
    },
  };

  const res1 = engine.verifyRollupProof(tamperedReceipt);
  assert.equal(res1.isValid, false);
  assert.ok(res1.errorReason.includes("Receipt hash mismatch"));

  // 2. Tamper with receipt hash prefix
  const invalidTagReceipt = {
    ...receipt,
    receipt_hash: "invalid_prefix:12345",
  };
  const res2 = engine.verifyRollupProof(invalidTagReceipt);
  assert.equal(res2.isValid, false);
  assert.ok(res2.errorReason.includes("Invalid receipt hash domain prefix"));
});

test("ZkRollupEngine: Fraud challenge and batch invalidation", () => {
  const engine = new ZkRollupEngine();
  engine.queueTransition({
    agentId: "agent_y",
    slotKey: "y_slot",
    fromValueHash: "0".repeat(64),
    toValueHash: sha256Hex("y_val"),
    deltaPayload: "set_y",
  });
  const receipt = engine.commitBatch("batch_fraud_test");

  const fraudDetected = engine.challengeFraud({
    batchId: "batch_fraud_test",
    challengeType: "PRE_STATE_CORRUPTION",
    evidence: { pre_state_root: "f".repeat(64) },
  });
  assert.equal(fraudDetected, true);

  const batch = engine.getBatch("batch_fraud_test");
  assert.equal(batch.status, RollupStatus.CHALLENGED);
  assert.equal(receipt.is_valid, false);
  assert.equal(engine.getStatus().circuit_tripped, true);
});

test("ZkRollupEngine: Fail-closed circuit breaker lockout", () => {
  const engine = new ZkRollupEngine();
  engine.tripCircuit("Manual operator emergency trip");

  assert.throws(
    () => {
      engine.queueTransition({
        agentId: "agent_z",
        slotKey: "z_slot",
        fromValueHash: "0".repeat(64),
        toValueHash: sha256Hex("z_val"),
        deltaPayload: "op",
      });
    },
    (err) => err instanceof RollupCircuitTrippedError
  );

  assert.throws(
    () => {
      engine.commitBatch();
    },
    (err) => err instanceof RollupCircuitTrippedError
  );
});

test("ZkRollupEngine: Zero-Knowledge privacy and scratchpad masking", () => {
  const engine = new ZkRollupEngine();
  const secretScratchpad = "Confidential prompt reasoning: transfer $10M to vault";

  const t = engine.queueTransition({
    agentId: "agent_secret",
    slotKey: "vault_slot",
    fromValueHash: "0".repeat(64),
    toValueHash: sha256Hex("vault_funded"),
    deltaPayload: "fund_vault()",
    privateScratchpad: secretScratchpad,
  });

  assert.equal(typeof t.private_payload_mask, "string");
  assert.notEqual(t.private_payload_mask, secretScratchpad);
  assert.equal(t.private_payload_mask.length, 64);
});
