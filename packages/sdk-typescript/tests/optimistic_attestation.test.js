import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";

import {
  OptimisticAttestationEngine,
  executeTransitionStep,
  UnauthorizedProverError,
  PrematureFinalizationError,
  BatchChallengedError,
} from "../src/optimistic_attestation.ts";

test("Seam 88: Optimistic attestation and clean finalization (TypeScript)", () => {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
  const pubHex = publicKey.export({ type: "spki", format: "der" }).toString("hex");

  const engine = new OptimisticAttestationEngine();
  engine.registerProverKey(pubHex);

  const s0Pre = "00".repeat(32);
  const s0Post = executeTransitionStep(s0Pre, "action_create_user");
  const s1Pre = s0Post;
  const s1Post = executeTransitionStep(s1Pre, "action_assign_role");
  const s2Pre = s1Post;
  const s2Post = executeTransitionStep(s2Pre, "action_issue_token");

  const steps = [
    { step_index: 0, pre_state_hash: s0Pre, action: "action_create_user", post_state_hash: s0Post },
    { step_index: 1, pre_state_hash: s1Pre, action: "action_assign_role", post_state_hash: s1Post },
    { step_index: 2, pre_state_hash: s2Pre, action: "action_issue_token", post_state_hash: s2Post },
  ];

  const startNs = process.hrtime.bigint();
  const attestation = engine.attestBatch(steps, "prover-primary", privateKey, 500);
  const durationUs = Number(process.hrtime.bigint() - startNs) / 1000;
  assert.ok(durationUs < 10000, `Execution latency ${durationUs}us must be fast`);

  assert.equal(attestation.step_count, 3);
  assert.equal(attestation.final_state_hash, s2Post);

  // Premature finalization rejected
  assert.throws(() => {
    engine.finalize(attestation, Date.now() * 1_000_000);
  }, PrematureFinalizationError);

  // Finalize after challenge window passes
  const futureTime = attestation.challenge_deadline_ns + 1_000_000;
  const receipt = engine.finalize(attestation, futureTime);

  assert.equal(receipt.batch_id, attestation.batch_id);
  assert.equal(receipt.final_state_hash, s2Post);
});

test("Seam 88: Fraud challenge detects invalid transition (TypeScript)", () => {
  const { privateKey } = crypto.generateKeyPairSync("ed25519");
  const engine = new OptimisticAttestationEngine();

  const s0Pre = "aa".repeat(32);
  const s0Post = executeTransitionStep(s0Pre, "action_credit_balance");
  const s1Pre = s0Post;
  const s1FraudPost = "ff".repeat(32); // Forged post-state

  const steps = [
    { step_index: 0, pre_state_hash: s0Pre, action: "action_credit_balance", post_state_hash: s0Post },
    { step_index: 1, pre_state_hash: s1Pre, action: "action_transfer_funds", post_state_hash: s1FraudPost },
  ];

  const attestation = engine.attestBatch(steps, "malicious-prover", privateKey, 5000);

  // Invariant F1: Challenger proves fraud
  const proof = engine.challengeStep(attestation, 1);
  assert.notEqual(proof, null);
  assert.equal(proof.failing_step_index, 1);
  assert.equal(proof.claimed_post_state, s1FraudPost);
  assert.notEqual(proof.recomputed_post_state, s1FraudPost);

  // Finalization blocked
  const futureTime = attestation.challenge_deadline_ns + 10_000_000;
  assert.throws(() => {
    engine.finalize(attestation, futureTime);
  }, BatchChallengedError);
});

test("Seam 88: Unpinned prover key rejected (TypeScript)", () => {
  const { publicKey: authPub } = crypto.generateKeyPairSync("ed25519");
  const authPubHex = authPub.export({ type: "spki", format: "der" }).toString("hex");

  const engine = new OptimisticAttestationEngine(new Set([authPubHex]));

  const { privateKey: attackerKey } = crypto.generateKeyPairSync("ed25519");
  const steps = [{ step_index: 0, pre_state_hash: "00".repeat(32), action: "act", post_state_hash: executeTransitionStep("00".repeat(32), "act") }];

  assert.throws(() => {
    engine.attestBatch(steps, "attacker", attackerKey);
  }, UnauthorizedProverError);
});
