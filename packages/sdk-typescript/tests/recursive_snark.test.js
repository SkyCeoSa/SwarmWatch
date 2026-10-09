import test from "node:test";
import assert from "node:assert/strict";

import { SafetyCircuitBus } from "../src/circuit_bus.ts";
import {
  RecursiveSnarkGateway,
  EmptyProofBatchError,
  RecursiveProofSoundnessBreachError,
  RecursiveVerificationFailedError,
} from "../src/recursive_snark.ts";

function generateProof(proofId, epoch = 1, isValid = true) {
  return {
    proof_id: proofId,
    batch_epoch: epoch,
    state_digest: `digest-${proofId}`.repeat(4),
    proof_witness: `witness-${proofId}`.repeat(4),
    is_valid: isValid,
  };
}

test("Seam 94: Binary tree folding and succinct compression RZK1 RZK3 (TypeScript)", () => {
  const gateway = new RecursiveSnarkGateway("gateway-test");

  for (const count of [1, 2, 4, 7]) {
    const batch = Array.from({ length: count }, (_, i) => generateProof(`p-${count}-${i}`));
    const receipt = gateway.foldBatch(batch);

    assert.equal(receipt.constituent_count, count);
    assert.equal(receipt.aggregate_root.length, 64);
    // Constant O(1) size: exactly 64 hex characters (32 bytes)
    assert.equal(receipt.recursive_proof_bytes.length, 64);
    assert.equal(receipt.verified, true);

    // Deterministic tree derivation check
    const repeatReceipt = gateway.foldBatch(batch);
    assert.equal(receipt.aggregate_root, repeatReceipt.aggregate_root);
    assert.equal(receipt.recursive_proof_bytes, repeatReceipt.recursive_proof_bytes);
  }
});

test("Seam 94: Recursive soundness rejection RZK2 (TypeScript)", () => {
  const gateway = new RecursiveSnarkGateway("gateway-test");

  // Empty batch rejected
  assert.throws(() => gateway.foldBatch([]), EmptyProofBatchError);

  // Tainted proof rejected fail-closed
  const good1 = generateProof("good-1");
  const bad = generateProof("bad-proof", 1, false);
  const good2 = generateProof("good-2");

  assert.throws(
    () => gateway.foldBatch([good1, bad, good2]),
    RecursiveProofSoundnessBreachError
  );

  // Missing witness rejected
  const emptyWit = { proof_id: "empty-wit", batch_epoch: 1, state_digest: "digest", proof_witness: "" };
  assert.throws(
    () => gateway.foldBatch([good1, emptyWit]),
    RecursiveProofSoundnessBreachError
  );
});

test("Seam 94: Constant-time aggregate verification RZK1 (TypeScript)", () => {
  const gateway = new RecursiveSnarkGateway("gateway-test");
  const batch = Array.from({ length: 8 }, (_, i) => generateProof(`v-proof-${i}`));
  const receipt = gateway.foldBatch(batch);

  assert.equal(gateway.verifyAggregateReceipt(receipt), true);
});

test("Seam 94: Forged proof trips SafetyCircuitBus RZK4 (TypeScript)", () => {
  const circuitBus = new SafetyCircuitBus();
  const gateway = new RecursiveSnarkGateway("gateway-test", circuitBus);

  const batch = Array.from({ length: 4 }, (_, i) => generateProof(`proof-${i}`));
  const receipt = gateway.foldBatch(batch);

  assert.equal(circuitBus.isTripped(), false);

  // Tamper with recursive proof bytes (forgery attack)
  const tamperedReceipt = {
    ...receipt,
    recursive_proof_bytes: "00".repeat(32),
  };

  assert.throws(
    () => gateway.verifyAggregateReceipt(tamperedReceipt),
    RecursiveVerificationFailedError
  );

  assert.equal(circuitBus.isTripped(), true);
  const status = circuitBus.getStatus();
  assert.equal(status.last_trip_event.reason, "RECURSIVE_SNARK_VERIFICATION_FAILURE");
});

test("Seam 94: Sub-250 microsecond execution latency SLO (TypeScript)", () => {
  const gateway = new RecursiveSnarkGateway("gateway-bench");
  const batch = Array.from({ length: 4 }, (_, i) => generateProof(`bench-p-${i}`));

  const start = performance.now();
  const iterations = 200;
  for (let i = 0; i < iterations; i++) {
    const rcpt = gateway.foldBatch(batch);
    gateway.verifyAggregateReceipt(rcpt);
  }
  const elapsedMs = performance.now() - start;
  const avgLatencyUs = (elapsedMs / iterations) * 1000;
  assert.ok(avgLatencyUs < 250.0, `Average latency ${avgLatencyUs.toFixed(2)}µs exceeds 250µs SLO`);
});
