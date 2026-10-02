/**
 * Tests for Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger (Seam 67 / ADR 0078).
 */

import test from "node:test";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import {
  DatasetPrivacyLedger,
  DatasetPrivacyBudgetExhaustedError,
  EntityPrivacyBudgetExhaustedError,
  DatasetReservationStatus,
  DatasetPrivacyReservation,
  DatasetPrivacyReleaseResult,
  globalDatasetPrivacyLedger,
} from "../src/dataset_privacy.ts";

test("Dataset registration and initial balances", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({
    datasetId: "ds_telemetry",
    tenantId: "tenant_alpha",
    maxEpsilon: 10.0,
    maxDelta: 1e-4,
    entityEpsilonLimit: 2.0,
    entityDeltaLimit: 1e-5,
  });

  const rem = ledger.remaining({
    tenantId: "tenant_alpha",
    datasetId: "ds_telemetry",
    entityId: "user_1",
  });

  assert.equal(rem.dataset_remaining_epsilon, 10.0);
  assert.equal(rem.dataset_remaining_delta, 1e-4);
  assert.equal(rem.entity_remaining_epsilon, 2.0);
  assert.equal(rem.entity_remaining_delta, 1e-5);
});

test("Atomic reservation admission and entity isolation", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({
    datasetId: "ds_clicks",
    tenantId: "tenant_alpha",
    maxEpsilon: 10.0,
    maxDelta: 1e-4,
    entityEpsilonLimit: 2.0,
    entityDeltaLimit: 1e-5,
  });

  // 1. Reserve 0.5 for entity user_1
  const res1 = ledger.reserve({
    tenantId: "tenant_alpha",
    datasetId: "ds_clicks",
    entityId: "user_1",
    epsilon: 0.5,
    delta: 1e-6,
    sensitivity: 1.0,
    queryId: "q1",
  });

  assert.ok(res1 instanceof DatasetPrivacyReservation);
  assert.equal(res1.status, DatasetReservationStatus.PENDING);
  assert.equal(res1.receiptHash.length, 64);

  // Check user_1 remaining
  const rem1 = ledger.remaining({
    tenantId: "tenant_alpha",
    datasetId: "ds_clicks",
    entityId: "user_1",
  });
  assert.equal(rem1.dataset_remaining_epsilon, 9.5);
  assert.equal(rem1.entity_remaining_epsilon, 1.5);

  // Check user_2 remaining: entity cap is intact (2.0), but dataset is 9.5
  const rem2 = ledger.remaining({
    tenantId: "tenant_alpha",
    datasetId: "ds_clicks",
    entityId: "user_2",
  });
  assert.equal(rem2.dataset_remaining_epsilon, 9.5);
  assert.equal(rem2.entity_remaining_epsilon, 2.0);
});

test("Dataset budget exhaustion halts fail-closed", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({
    datasetId: "ds_small",
    tenantId: "t1",
    maxEpsilon: 1.0,
    maxDelta: 1e-4,
    entityEpsilonLimit: 1.0,
  });

  ledger.reserve({
    tenantId: "t1",
    datasetId: "ds_small",
    entityId: "e1",
    epsilon: 0.8,
    delta: 0.0,
  });

  assert.throws(
    () => {
      ledger.reserve({
        tenantId: "t1",
        datasetId: "ds_small",
        entityId: "e2",
        epsilon: 0.3,
        delta: 0.0,
      });
    },
    (err) => err instanceof DatasetPrivacyBudgetExhaustedError
  );
});

test("Entity budget exhaustion halts fail-closed", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({
    datasetId: "ds_large",
    tenantId: "t1",
    maxEpsilon: 50.0,
    maxDelta: 1e-3,
    entityEpsilonLimit: 1.0,
    entityDeltaLimit: 1e-5,
  });

  // Entity e1 spends 0.6
  ledger.reserve({
    tenantId: "t1",
    datasetId: "ds_large",
    entityId: "e1",
    epsilon: 0.6,
    delta: 0.0,
  });

  // Entity e1 attempts to spend 0.5 (0.6 + 0.5 = 1.1 > 1.0 limit) -> must fail closed
  assert.throws(
    () => {
      ledger.reserve({
        tenantId: "t1",
        datasetId: "ds_large",
        entityId: "e1",
        epsilon: 0.5,
        delta: 0.0,
      });
    },
    (err) => err instanceof EntityPrivacyBudgetExhaustedError
  );

  // Another entity e2 can still reserve budget without issue
  const resE2 = ledger.reserve({
    tenantId: "t1",
    datasetId: "ds_large",
    entityId: "e2",
    epsilon: 0.5,
    delta: 0.0,
  });
  assert.equal(resE2.status, DatasetReservationStatus.PENDING);
});

test("Release with Laplace noise perturbation", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({
    datasetId: "ds_metrics",
    tenantId: "t1",
    maxEpsilon: 5.0,
    maxDelta: 1e-4,
    entityEpsilonLimit: 2.0,
  });

  const res = ledger.reserve({
    tenantId: "t1",
    datasetId: "ds_metrics",
    entityId: "e10",
    epsilon: 1.0,
    delta: 0.0,
    sensitivity: 5.0,
  });

  const releaseResult = ledger.commitRelease({
    reservationId: res.reservationId,
    rawValue: 100.0,
    noiseSeed: 12345,
    minBound: 0.0,
    maxBound: 200.0,
  });

  assert.ok(releaseResult instanceof DatasetPrivacyReleaseResult);
  assert.equal(releaseResult.reservationId, res.reservationId);
  assert.equal(releaseResult.committedEpsilon, 1.0);
  assert.ok(releaseResult.perturbedValue >= 0.0 && releaseResult.perturbedValue <= 200.0);
  assert.notEqual(releaseResult.perturbedValue, 100.0);

  // Verify reservation status transitioned to COMMITTED
  const updatedRes = ledger.getReservation(res.reservationId);
  assert.ok(updatedRes);
  assert.equal(updatedRes.status, DatasetReservationStatus.COMMITTED);
});

test("Cancellation of pending reservation restores budget", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({
    datasetId: "ds_cancel",
    tenantId: "t1",
    maxEpsilon: 2.0,
    entityEpsilonLimit: 1.0,
  });

  const res = ledger.reserve({
    tenantId: "t1",
    datasetId: "ds_cancel",
    entityId: "e1",
    epsilon: 0.8,
    delta: 0.0,
  });

  const remBefore = ledger.remaining({ tenantId: "t1", datasetId: "ds_cancel", entityId: "e1" });
  assert.ok(Math.abs(remBefore.entity_remaining_epsilon - 0.2) < 1e-6);

  const cancelled = ledger.cancelReservation(res.reservationId);
  assert.equal(cancelled, true);

  const remAfter = ledger.remaining({ tenantId: "t1", datasetId: "ds_cancel", entityId: "e1" });
  assert.ok(Math.abs(remAfter.entity_remaining_epsilon - 1.0) < 1e-6);
  assert.ok(Math.abs(remAfter.dataset_remaining_epsilon - 2.0) < 1e-6);
});

test("Monotone suprema reconciliation prevents double spending", () => {
  const ledgerA = new DatasetPrivacyLedger();
  ledgerA.registerDataset({ datasetId: "ds_dist", tenantId: "t1", maxEpsilon: 10.0, entityEpsilonLimit: 5.0 });

  const ledgerB = new DatasetPrivacyLedger();
  ledgerB.registerDataset({ datasetId: "ds_dist", tenantId: "t1", maxEpsilon: 10.0, entityEpsilonLimit: 5.0 });

  // Partition: Replica A spends 2.0 on entity X
  const resA = ledgerA.reserve({ tenantId: "t1", datasetId: "ds_dist", entityId: "user_x", epsilon: 2.0, delta: 0.0 });
  ledgerA.commitRelease({ reservationId: resA.reservationId, rawValue: 50.0 });

  // Partition: Replica B spends 3.0 on entity X and 1.0 on entity Y
  const resB1 = ledgerB.reserve({ tenantId: "t1", datasetId: "ds_dist", entityId: "user_x", epsilon: 3.0, delta: 0.0 });
  ledgerB.commitRelease({ reservationId: resB1.reservationId, rawValue: 60.0 });
  const resB2 = ledgerB.reserve({ tenantId: "t1", datasetId: "ds_dist", entityId: "user_y", epsilon: 1.0, delta: 0.0 });
  ledgerB.commitRelease({ reservationId: resB2.reservationId, rawValue: 70.0 });

  // Reconcile B into A
  const snapB = ledgerB.exportSnapshot("t1", "ds_dist");
  const reconciliation = ledgerA.reconcile(snapB);

  assert.ok(reconciliation.reconciled_entities >= 2);
  const remAX = ledgerA.remaining({ tenantId: "t1", datasetId: "ds_dist", entityId: "user_x" });
  // Monotone join: max(2.0, 3.0) = 3.0 spent => remaining = 5.0 - 3.0 = 2.0
  assert.ok(Math.abs(remAX.entity_remaining_epsilon - 2.0) < 1e-6);

  const remAY = ledgerA.remaining({ tenantId: "t1", datasetId: "ds_dist", entityId: "user_y" });
  // Monotone join: max(0.0, 1.0) = 1.0 spent => remaining = 5.0 - 1.0 = 4.0
  assert.ok(Math.abs(remAY.entity_remaining_epsilon - 4.0) < 1e-6);
});

test("Sub-250us dataset privacy SLO micro-benchmark", () => {
  const ledger = new DatasetPrivacyLedger();
  ledger.registerDataset({ datasetId: "ds_perf", tenantId: "t1", maxEpsilon: 1000.0, entityEpsilonLimit: 100.0 });

  // Warmup
  for (let i = 0; i < 10; i++) {
    const r = ledger.reserve({ tenantId: "t1", datasetId: "ds_perf", entityId: `e_${i}`, epsilon: 0.01, delta: 0.0 });
    ledger.commitRelease({ reservationId: r.reservationId, rawValue: 10.0, noiseSeed: i });
  }

  // Benchmark: 500 iterations of reserve + commitRelease
  const iterations = 500;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const r = ledger.reserve({ tenantId: "t1", datasetId: "ds_perf", entityId: `entity_${i % 20}`, epsilon: 0.01, delta: 0.0 });
    ledger.commitRelease({ reservationId: r.reservationId, rawValue: 42.0, noiseSeed: i });
  }
  const t1 = performance.now();

  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250.0, `Dataset privacy operations took ${avgUs.toFixed(2)}µs, exceeding 250µs SLO`);
});

test("Global singleton and status reporting", () => {
  const st = globalDatasetPrivacyLedger.status();
  assert.ok("total_registered_datasets" in st);
  assert.ok("total_active_reservations" in st);
  assert.ok("total_entities_tracked" in st);
});
