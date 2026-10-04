/**
 * Live simulation test suite for Seam 75 (ADR 0086):
 * Zero-Knowledge State Transition Rollups & Succinct Proof Verification.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  ZkRollupEngine,
  SequenceDiscontinuityError,
  sha256Hex,
} from "../src/zk_rollup.ts";

test("Seam 75: Live Simulation of Zero-Knowledge Rollups and 8 Invariants", () => {
  const engine = new ZkRollupEngine({ maxBatchSize: 100 });

  // Phase 1: Sparse State Merkle Tree
  const initialRoot = engine.getStateRoot();
  assert.equal(initialRoot, "0".repeat(64));

  // Phase 2: Ingest 50 Atomic Transitions
  const batchSize = 50;
  const currentSlots = {};
  for (let i = 0; i < 10; i++) {
    currentSlots[`agent_slot_${i}`] = "0".repeat(64);
  }

  // Warmup JIT & crypto buffers
  for (let w = 0; w < 5; w++) {
    sha256Hex(`warmup_${w}`);
  }

  const t0 = performance.now();
  for (let i = 0; i < batchSize; i++) {
    const slot = `agent_slot_${i % 10}`;
    const fromVal = currentSlots[slot];
    const toVal = sha256Hex(`state_val_${i}`);
    engine.queueTransition({
      agentId: `worker_agent_${i % 5}`,
      slotKey: slot,
      fromValueHash: fromVal,
      toValueHash: toVal,
      deltaPayload: `mutation_step_${i}(args=${i * 10})`,
      privateScratchpad: `Confidential reasoning chain #${i}: secret token ${i * 999}`,
    });
    currentSlots[slot] = toVal;
  }
  const t1 = performance.now();
  const queueDurationUs = ((t1 - t0) / batchSize) * 1000;
  assert.ok(queueDurationUs < 250.0, `Queue duration ${queueDurationUs}us exceeded 250us`);

  // Phase 3: Rollup Batch Commitment
  const receipt = engine.commitBatch("batch_sim_alpha");
  assert.equal(receipt.transition_count, batchSize);
  assert.ok(receipt.receipt_hash.startsWith("sw:rollup:v1:"));

  // Phase 4: O(1) Succinct Proof Verification
  const verifyIterations = 50;
  const v0 = performance.now();
  for (let v = 0; v < verifyIterations; v++) {
    const verification = engine.verifyRollupProof(receipt);
    assert.equal(verification.isValid, true);
    assert.equal(verification.errorReason, null);
  }
  const v1 = performance.now();
  const verifyLatencyUs = ((v1 - v0) / verifyIterations) * 1000;
  assert.ok(verifyLatencyUs < 250.0, `Verify latency ${verifyLatencyUs}us exceeded 250us`);

  // Phase 5: Adversarial Discontinuity Defense
  const expectedSeq = engine.getStatus().next_sequence;
  assert.throws(
    () => {
      engine.queueTransition({
        agentId: "adversary_agent",
        slotKey: "agent_slot_0",
        fromValueHash: currentSlots["agent_slot_0"],
        toValueHash: sha256Hex("adversarial_state"),
        deltaPayload: "exploit()",
        sequence: expectedSeq + 10,
      });
    },
    (err) => err instanceof SequenceDiscontinuityError
  );

  assert.equal(engine.getStatus().circuit_tripped, true);
});
