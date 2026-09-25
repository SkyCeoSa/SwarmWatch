import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  ClaimStatus,
  EffectReceipt,
  EffectRecord,
  EffectLedger,
  globalEffectLedger,
} from "../src/effect_ledger.ts";

describe("Seam 52: Fenced Effect Ledger & Monotonic Idempotency Barrier (ADR 0063)", () => {
  beforeEach(() => {
    globalEffectLedger.reset();
  });

  test("EffectReceipt creation and canonical hash verification", () => {
    const receipt = new EffectReceipt({
      receiptId: "rcpt-eff-ts-001",
      effectKey: "db_write:user_123",
      idempotencyKey: "idemp-alpha",
      epoch: 1,
      tenantId: "tenant-alpha",
      committedAt: 1000.0,
      resultDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    });

    assert.equal(receipt.receiptId, "rcpt-eff-ts-001");
    assert.equal(receipt.receiptHash.length, 64);

    // Dict round-trip
    const dict = receipt.toDict();
    assert.equal(dict.receipt_id, "rcpt-eff-ts-001");
    const reconstructed = EffectReceipt.fromDict(dict);
    assert.equal(reconstructed.receiptId, receipt.receiptId);
    assert.equal(reconstructed.receiptHash, receipt.receiptHash);
  });

  test("CAS claim, authorize, and complete lifecycle", () => {
    const ledger = new EffectLedger(1);

    // 1. Claim effect
    const claimRes = ledger.claim(
      "payment:invoice_999",
      "tenant-corp",
      1,
      "idemp-inv-999",
      "stripe_charge",
      5000.0,
    );
    assert.equal(claimRes.status, "ACCEPTED");
    assert.ok(claimRes.claimId?.startsWith("clm-"));
    const claimId = claimRes.claimId;

    // 2. Authorize effect with permit hash
    const fakePermitHash = "a".repeat(64);
    const authRes = ledger.authorize(claimId, fakePermitHash);
    assert.equal(authRes.success, true);
    assert.ok(authRes.message.includes("authorized"));

    // 3. Complete effect
    const executionResult = { charge_id: "ch_12345", amount_cents: 5000 };
    const compRes = ledger.complete(claimId, executionResult);
    assert.equal(compRes.success, true);
    assert.ok(compRes.receipt !== null);
    assert.equal(compRes.receipt.effectKey, "payment:invoice_999");
    assert.equal(compRes.receipt.receiptHash.length, 64);

    // Status check
    const status = ledger.status("tenant-corp");
    assert.equal(status.committed_effects_count, 1);
    assert.equal(status.active_claims_count, 0);
  });

  test("Idempotent commit barrier prevents duplicate external effects", () => {
    const ledger = new EffectLedger(1);

    // First claim & complete
    const res1 = ledger.claim("email:welcome_user_1", "default", 1, "idemp-key-1");
    assert.equal(res1.status, "ACCEPTED");
    const comp1 = ledger.complete(res1.claimId, { sent: true });
    assert.equal(comp1.success, true);
    const receipt1 = comp1.receipt;

    // Second claim with SAME idempotency key returns IDEMPOTENT_COMMITTED
    const res2 = ledger.claim("email:welcome_user_1", "default", 1, "idemp-key-1");
    assert.equal(res2.status, "IDEMPOTENT_COMMITTED");
    assert.equal(res2.receipt.receiptId, receipt1.receiptId);
    assert.equal(res2.receipt.receiptHash, receipt1.receiptHash);

    // Third claim with DIFFERENT idempotency key is REJECTED
    const res3 = ledger.claim("email:welcome_user_1", "default", 1, "different-key");
    assert.equal(res3.status, "REJECTED");
    assert.ok(res3.reason.includes("already committed under different idempotency key"));
  });

  test("Monotonic fencing epoch invalidation and stale leader rejection", () => {
    const ledger = new EffectLedger(1);

    // Claim under epoch 1
    const res1 = ledger.claim("deploy:service_a", "default", 1);
    assert.equal(res1.status, "ACCEPTED");
    const claimId = res1.claimId;

    // Epoch bump
    const newEpoch = ledger.tripEpoch("leader_failover");
    assert.equal(newEpoch, 2);

    // Attempting to complete old claim fails fail-closed
    const compFail = ledger.complete(claimId, { status: "deployed" });
    assert.equal(compFail.success, false);
    assert.equal(compFail.receipt, null);
    assert.ok(compFail.message.includes("epoch transition"));

    // Stale epoch claim is immediately rejected
    const staleClaim = ledger.claim("deploy:service_b", "default", 1);
    assert.equal(staleClaim.status, "REJECTED");
    assert.ok(staleClaim.reason.includes("STALE_EPOCH"));
  });

  test("Lease timeout and CAS takeover", async () => {
    const ledger = new EffectLedger(1);

    // Short timeout claim
    const res1 = ledger.claim("cron:sync_catalog", "default", 1, null, "tool", 100.0);
    assert.equal(res1.status, "ACCEPTED");

    // Concurrent claim rejected while active
    const res2 = ledger.claim("cron:sync_catalog", "default", 1, null, "tool", 100.0);
    assert.equal(res2.status, "REJECTED");
    assert.ok(res2.reason.includes("actively locked"));

    // Wait past expiration
    await new Promise((r) => setTimeout(r, 150));

    // Claim after lease expiry succeeds
    const res3 = ledger.claim("cron:sync_catalog", "default", 1, null, "tool", 5000.0);
    assert.equal(res3.status, "ACCEPTED");
    assert.notEqual(res3.claimId, res1.claimId);
  });

  test("Abort and reconcile lifecycle", () => {
    const ledger = new EffectLedger(1);

    // Claim and abort
    const res = ledger.claim("file:delete_temp");
    assert.equal(res.status, "ACCEPTED");
    const abortRes = ledger.abort(res.claimId, "aborted_by_user");
    assert.equal(abortRes.success, true);

    // Commit under epoch 1 and epoch 2
    const resEp1 = ledger.claim("task:one", "default", 1);
    ledger.complete(resEp1.claimId, "done_1");

    ledger.tripEpoch(); // now 2
    const resEp2 = ledger.claim("task:two", "default", 2);
    ledger.complete(resEp2.claimId, "done_2");

    // Reconcile all
    const allRecords = ledger.reconcile(1);
    assert.equal(allRecords.length, 2);

    // Reconcile since epoch 2
    const ep2Records = ledger.reconcile(2);
    assert.equal(ep2Records.length, 1);
    assert.equal(ep2Records[0].effect_key, "task:two");
  });

  test("Global singleton instance behavior", () => {
    assert.equal(globalEffectLedger.currentEpoch, 1);
    const res = globalEffectLedger.claim("global:test_effect");
    assert.equal(res.status, "ACCEPTED");
    const comp = globalEffectLedger.complete(res.claimId, { test: true });
    assert.equal(comp.success, true);
  });
});
