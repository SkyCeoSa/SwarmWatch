import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  PermitStatus,
  AuthorityState,
  SafetyPermit,
  TenantQuota,
  SafetyAuthority,
  globalSafetyAuthority,
} from "../src/safety_authority.ts";

describe("Seam 51: Unified Safety Authority & Linearizable Execution Permit Engine (ADR 0062)", () => {
  beforeEach(() => {
    globalSafetyAuthority.reset();
  });

  test("SafetyPermit creation and canonical hash verification", () => {
    const permit = new SafetyPermit({
      permitId: "pmt-ts-001",
      tenantId: "tenant-alpha",
      epoch: 1,
      action: "web_search",
      costEstimate: 0.05,
      tokensEstimate: 500,
      status: PermitStatus.ACCEPTED,
      issuedAt: 1000.0,
      expiresAt: 1005.0,
    });

    assert.equal(permit.permitId, "pmt-ts-001");
    assert.equal(permit.status, PermitStatus.ACCEPTED);
    assert.equal(permit.epoch, 1);
    assert.equal(permit.permitHash.length, 64);

    // Dict round-trip
    const dict = permit.toDict();
    assert.equal(dict.permit_id, "pmt-ts-001");
    assert.equal(dict.epoch, 1);

    const reconstructed = SafetyPermit.fromDict(dict);
    assert.equal(reconstructed.permitId, permit.permitId);
    assert.equal(reconstructed.permitHash, permit.permitHash);

    // Validity check
    assert.equal(permit.isValid(1, 1002.0), true);
    assert.equal(permit.isValid(2, 1002.0), false); // Epoch mismatch
    assert.equal(permit.isValid(1, 1006.0), false); // Expired
  });

  test("Atomic reserve, commit settlement, and release lifecycle", () => {
    const auth = new SafetyAuthority(1);
    auth.configureTenant("tenant-1", 10.0, 100_000, 50);

    // 1. Reserve permit
    const permit = auth.reserve("tenant-1", "llm_generate", 2.5, 5000, 5000.0);
    assert.equal(permit.status, PermitStatus.ACCEPTED);
    assert.equal(permit.epoch, 1);

    const status1 = auth.status("tenant-1");
    assert.equal(status1.active_permits_count, 1);
    assert.equal(status1.tenant.reserved_cost, 2.5);
    assert.equal(status1.tenant.available_cost, 7.5);

    // 2. Commit permit
    const commitRes = auth.commit(permit.permitId, 2.0, 4500);
    assert.equal(commitRes.success, true);
    assert.ok(commitRes.message.includes("successfully"));

    const status2 = auth.status("tenant-1");
    assert.equal(status2.active_permits_count, 0);
    assert.equal(status2.tenant.reserved_cost, 0.0);
    assert.equal(status2.tenant.spent_cost, 2.0);
    assert.equal(status2.tenant.available_cost, 8.0);
    assert.equal(status2.tenant.spent_turns, 1);

    // 3. Release permit
    const permit2 = auth.reserve("tenant-1", "analysis", 1.0, 2000);
    const status3 = auth.status("tenant-1");
    assert.equal(status3.tenant.reserved_cost, 1.0);

    const relRes = auth.release(permit2.permitId, "task_canceled");
    assert.equal(relRes.success, true);
    const status4 = auth.status("tenant-1");
    assert.equal(status4.tenant.reserved_cost, 0.0);
    assert.equal(status4.tenant.spent_cost, 2.0); // Spend unchanged
  });

  test("Budget exhaustion rejection for cost and tokens", () => {
    const auth = new SafetyAuthority(1);
    auth.configureTenant("tenant-budget", 1.0, 1000);

    // Exceed cost
    const permitCostFail = auth.reserve("tenant-budget", "action", 5.0);
    assert.equal(permitCostFail.status, PermitStatus.REJECTED);
    assert.ok(permitCostFail.rejectionReason?.includes("Insufficient cost budget"));

    // Exceed tokens
    const permitTokFail = auth.reserve("tenant-budget", "action", 0.5, 5000);
    assert.equal(permitTokFail.status, PermitStatus.REJECTED);
    assert.ok(permitTokFail.rejectionReason?.includes("Insufficient token budget"));
  });

  test("Fencing epoch invalidation and trip / rearm transitions", () => {
    const auth = new SafetyAuthority(1);
    auth.configureTenant("tenant-fenced", 50.0);

    // Reserve under epoch 1
    const permit = auth.reserve("tenant-fenced", "action", 1.0);
    assert.equal(permit.epoch, 1);

    // Trip globally
    const newEpoch = auth.trip("entropy_collapse_detected");
    assert.equal(newEpoch, 2);
    assert.equal(auth.epoch, 2);
    assert.equal(auth.globalState, AuthorityState.TRIPPED);

    // Attempting to commit old permit fails
    const failCommit = auth.commit(permit.permitId, 1.0);
    assert.equal(failCommit.success, false);

    // New reservations while tripped are rejected
    const blockedPermit = auth.reserve("tenant-fenced", "action", 0.5);
    assert.equal(blockedPermit.status, PermitStatus.REJECTED);
    assert.ok(blockedPermit.rejectionReason?.includes("tripped at epoch 2"));

    // Re-arm increments epoch again and restores nominal
    const epochRearm = auth.rearm();
    assert.equal(epochRearm, 3);
    assert.equal(auth.globalState, AuthorityState.NOMINAL);

    // New permit succeeds under epoch 3
    const newPermit = auth.reserve("tenant-fenced", "action", 0.5);
    assert.equal(newPermit.status, PermitStatus.ACCEPTED);
    assert.equal(newPermit.epoch, 3);
  });

  test("Lease timeout expiration frees reserved budget", async () => {
    const auth = new SafetyAuthority(1);
    auth.configureTenant("tenant-expire", 20.0);

    // Permit with 100ms timeout
    const permit = auth.reserve("tenant-expire", "action", 5.0, 0, 100.0);
    assert.equal(permit.status, PermitStatus.ACCEPTED);

    // Wait past expiration
    await new Promise((r) => setTimeout(r, 150));

    // Commit must fail due to expiration
    const commitRes = auth.commit(permit.permitId, 5.0);
    assert.equal(commitRes.success, false);
    assert.ok(commitRes.message.includes("expired before commit"));

    // Reserved budget is freed
    const status = auth.status("tenant-expire");
    assert.equal(status.tenant.reserved_cost, 0.0);
  });

  test("Fail-closed partition mode behavior", () => {
    const auth = new SafetyAuthority(1);
    auth.configureTenant("tenant-partition", 10.0);

    // Simulate partition
    auth.setPartitionSimulation(true);
    const permitPartition = auth.reserve("tenant-partition", "action", 1.0);
    assert.equal(permitPartition.status, PermitStatus.INDETERMINATE);
    assert.ok(permitPartition.rejectionReason?.includes("fail-closed"));

    // Recover partition
    auth.setPartitionSimulation(false);
    const permitRecovered = auth.reserve("tenant-partition", "action", 1.0);
    assert.equal(permitRecovered.status, PermitStatus.ACCEPTED);
  });

  test("Global singleton instance behavior", () => {
    assert.equal(globalSafetyAuthority.epoch, 1);
    const permit = globalSafetyAuthority.reserve("default", "test_turn", 0.01);
    assert.equal(permit.status, PermitStatus.ACCEPTED);
    const res = globalSafetyAuthority.commit(permit.permitId, 0.01);
    assert.equal(res.success, true);
  });
});
