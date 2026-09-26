/**
 * Unit and integration tests for Privacy Release Ledger & Differential Privacy Accountant (Seam 58 / ADR 0069).
 * Follows Node 22 native test runner conventions and strict type-stripping rules (no 'as ...' type casts).
 */

import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  PrivacyBudgetExhaustedError,
  ReservationStatus,
  sampleLaplace,
  clipValue,
  CompositionAccountant,
  PrivacyRelease,
  globalPrivacyRelease,
} from "../src/privacy_ledger.ts";

describe("PrivacyRelease & Differential Privacy Accountant (Seam 58 / ADR 0069)", () => {
  let ledger;

  beforeEach(() => {
    ledger = new PrivacyRelease();
    globalPrivacyRelease.reset();
  });

  test("sampleLaplace respects scale and deterministic seed reproducibility", () => {
    assert.equal(sampleLaplace(0.0), 0.0);
    assert.equal(sampleLaplace(-1.0), 0.0);

    const val1 = sampleLaplace(1.0, 42);
    const val2 = sampleLaplace(1.0, 42);
    assert.equal(val1, val2, "Identical seeds must yield identical Laplace noise");

    const val3 = sampleLaplace(1.0, 9999);
    assert.notEqual(val1, val3, "Different seeds should yield different noise");
  });

  test("clipValue clamps values strictly within bounds", () => {
    assert.equal(clipValue(5.0, 0.0, 10.0), 5.0);
    assert.equal(clipValue(-2.0, 0.0, 10.0), 0.0);
    assert.equal(clipValue(15.0, 0.0, 10.0), 10.0);
    assert.equal(clipValue(42.0), 42.0);
  });

  test("CompositionAccountant tracks linear and advanced DP composition", () => {
    const accountant = new CompositionAccountant();
    assert.equal(accountant.count(), 0);

    accountant.record(0.5, 1e-6);
    accountant.record(0.5, 1e-6);
    accountant.record(0.5, 1e-6);
    assert.equal(accountant.count(), 3);

    const [linEps, linDelta] = accountant.linearTotal();
    assert.ok(Math.abs(linEps - 1.5) < 1e-9);
    assert.ok(Math.abs(linDelta - 3e-6) < 1e-9);

    const [advEps, advDelta] = accountant.advancedComposition(1e-5);
    assert.ok(advEps <= linEps, "Advanced composition epsilon must be <= linear composition");
    assert.ok(advDelta > 0.0);
  });

  test("PrivacyRelease.reserve creates reservations and fails closed on budget exhaustion", () => {
    ledger.setTenantBudget("tenant-alpha", 2.0, 1e-4);

    const r1 = ledger.reserve("tenant-alpha", 1.0, 1e-5, 1.0, "q-101");
    assert.ok(r1.reservation_id.startsWith("prev-"));
    assert.equal(r1.status, ReservationStatus.PENDING);
    assert.equal(r1.epsilon, 1.0);
    assert.ok(r1.reservation_hash.length === 64);

    // Reserve second chunk
    const r2 = ledger.reserve("tenant-alpha", 0.8, 1e-5, 1.0, "q-102");
    assert.equal(r2.status, ReservationStatus.PENDING);

    // Third attempt exceeds 2.0 total epsilon -> throws PrivacyBudgetExhaustedError
    assert.throws(
      () => ledger.reserve("tenant-alpha", 0.5, 1e-5, 1.0, "q-103"),
      (err) => err instanceof PrivacyBudgetExhaustedError,
    );
  });

  test("PrivacyRelease.release perturbs scalar and handles deterministic seed", () => {
    ledger.setTenantBudget("tenant-beta", 5.0, 1e-3);
    const r1 = ledger.reserve("tenant-beta", 0.5, 1e-5, 1.0, "q-201");

    const res1 = ledger.release(r1.reservation_id, 100.0, 1234, 0.0, 200.0);
    assert.ok(res1.release_id.startsWith("prel-"));
    assert.equal(res1.reservation_id, r1.reservation_id);
    assert.equal(res1.raw_clipped, 100.0);
    assert.equal(res1.epsilon_spent, 0.5);
    assert.ok(typeof res1.perturbed_value === "number");
    assert.notEqual(res1.perturbed_value, 100.0);

    // Second release on same reservation should fail closed
    assert.throws(
      () => ledger.release(r1.reservation_id, 100.0),
      /has invalid status: RELEASED/,
    );
  });

  test("PrivacyRelease.release handles lists and dictionaries", () => {
    ledger.setTenantBudget("tenant-gamma", 5.0, 1e-3);

    // List release
    const rList = ledger.reserve("tenant-gamma", 0.2, 0.0, 1.0, "q-list");
    const listRes = ledger.release(rList.reservation_id, [10.0, 20.0, 30.0], 42, 0.0, 50.0);
    assert.ok(Array.isArray(listRes.perturbed_value));
    assert.equal(listRes.perturbed_value.length, 3);

    // Dict release
    const rDict = ledger.reserve("tenant-gamma", 0.2, 0.0, 1.0, "q-dict");
    const dictRes = ledger.release(
      rDict.reservation_id,
      { cpu: 45.0, mem: 80.0 },
      42,
      0.0,
      100.0,
    );
    assert.ok(typeof dictRes.perturbed_value === "object");
    assert.ok("cpu" in dictRes.perturbed_value);
    assert.ok("mem" in dictRes.perturbed_value);
  });

  test("PrivacyRelease audit reproducibility with identical seeds", () => {
    ledger.setTenantBudget("tenant-audit", 10.0, 1e-3);

    const rA = ledger.reserve("tenant-audit", 0.5, 0.0, 1.0, "q-audit-a");
    const relA = ledger.release(rA.reservation_id, 50.0, 777, 0.0, 100.0);

    const rB = ledger.reserve("tenant-audit", 0.5, 0.0, 1.0, "q-audit-b");
    const relB = ledger.release(rB.reservation_id, 50.0, 777, 0.0, 100.0);

    assert.equal(relA.perturbed_value, relB.perturbed_value);
    assert.equal(relA.noise_added, relB.noise_added);
  });

  test("PrivacyRelease remaining and status queries", () => {
    ledger.setTenantBudget("tenant-delta", 4.0, 1e-4);
    const r = ledger.reserve("tenant-delta", 1.0, 1e-5, 1.0, "q-stat");

    const remBefore = ledger.remaining("tenant-delta");
    assert.equal(remBefore.total_epsilon, 4.0);
    assert.equal(remBefore.pending_epsilon, 1.0);
    assert.equal(remBefore.remaining_epsilon, 3.0);

    ledger.release(r.reservation_id, 10.0, 101);

    const remAfter = ledger.remaining("tenant-delta");
    assert.equal(remAfter.spent_epsilon, 1.0);
    assert.equal(remAfter.pending_epsilon, 0.0);
    assert.equal(remAfter.remaining_epsilon, 3.0);

    const stat = ledger.status("tenant-delta");
    assert.equal(stat.active_tenants, 1);
    assert.equal(stat.total_releases, 1);
    assert.equal(stat.status, "CONVERGED");

    ledger.reset();
    assert.equal(ledger.status().active_tenants, 0);
  });
});
