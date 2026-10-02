# -*- coding: utf-8 -*-
"""
Tests for Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger (Seam 67 / ADR 0078).
Verifies:
1. Dataset registration and hierarchical (dataset & entity) budget limits.
2. Atomic reservation admission and fail-closed exhaustion.
3. Entity contribution isolation and independent entity tracking.
4. Release with calibrated Laplace perturbation and bounding.
5. Reservation cancellation and budget recovery.
6. Monotone suprema partition reconciliation preventing double-spending.
7. Cryptographic audit receipt hashes.
8. Sub-250 microsecond execution SLO.
9. Control plane REST API integration.
"""

from __future__ import annotations

import time
import pytest

from swarmwatch.dataset_privacy import (
    DatasetPrivacyLedger,
    DatasetPrivacyBudgetExhaustedError,
    EntityPrivacyBudgetExhaustedError,
    DatasetReservationStatus,
    DatasetPrivacyReservation,
    DatasetPrivacyReleaseResult,
    DatasetPrivacyConfig,
    DatasetLedgerSnapshot,
    global_dataset_privacy_ledger,
)


def test_dataset_registration_and_initial_balances() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset(
        dataset_id="ds_telemetry",
        tenant_id="tenant_alpha",
        max_epsilon=10.0,
        max_delta=1e-4,
        entity_epsilon_limit=2.0,
        entity_delta_limit=1e-5,
    )

    rem = ledger.remaining(tenant_id="tenant_alpha", dataset_id="ds_telemetry", entity_id="user_1")
    assert rem["dataset_remaining_epsilon"] == pytest.approx(10.0)
    assert rem["dataset_remaining_delta"] == pytest.approx(1e-4)
    assert rem["entity_remaining_epsilon"] == pytest.approx(2.0)
    assert rem["entity_remaining_delta"] == pytest.approx(1e-5)


def test_atomic_reservation_admission_and_isolation() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset(
        dataset_id="ds_clicks",
        tenant_id="tenant_alpha",
        max_epsilon=10.0,
        max_delta=1e-4,
        entity_epsilon_limit=2.0,
        entity_delta_limit=1e-5,
    )

    # 1. Reserve 0.5 for entity user_1
    res1 = ledger.reserve(
        tenant_id="tenant_alpha",
        dataset_id="ds_clicks",
        entity_id="user_1",
        epsilon=0.5,
        delta=1e-6,
        sensitivity=1.0,
        query_id="q1",
    )
    assert isinstance(res1, DatasetPrivacyReservation)
    assert res1.status == DatasetReservationStatus.PENDING
    assert len(res1.receipt_hash) == 64

    # Check user_1 remaining
    rem1 = ledger.remaining(tenant_id="tenant_alpha", dataset_id="ds_clicks", entity_id="user_1")
    assert rem1["dataset_remaining_epsilon"] == pytest.approx(9.5)
    assert rem1["entity_remaining_epsilon"] == pytest.approx(1.5)

    # Check user_2 remaining: entity cap is intact (2.0), but dataset is 9.5
    rem2 = ledger.remaining(tenant_id="tenant_alpha", dataset_id="ds_clicks", entity_id="user_2")
    assert rem2["dataset_remaining_epsilon"] == pytest.approx(9.5)
    assert rem2["entity_remaining_epsilon"] == pytest.approx(2.0)


def test_dataset_budget_exhaustion_halts_fail_closed() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset(
        dataset_id="ds_small",
        tenant_id="t1",
        max_epsilon=1.0,
        max_delta=1e-4,
        entity_epsilon_limit=1.0,
    )

    ledger.reserve(
        tenant_id="t1",
        dataset_id="ds_small",
        entity_id="e1",
        epsilon=0.8,
        delta=0.0,
    )

    # Requesting 0.3 should fail because dataset only has 0.2 remaining
    with pytest.raises(DatasetPrivacyBudgetExhaustedError, match="Dataset privacy budget exhausted"):
        ledger.reserve(
            tenant_id="t1",
            dataset_id="ds_small",
            entity_id="e2",
            epsilon=0.3,
            delta=0.0,
        )


def test_entity_budget_exhaustion_halts_fail_closed() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset(
        dataset_id="ds_large",
        tenant_id="t1",
        max_epsilon=50.0,
        max_delta=1e-3,
        entity_epsilon_limit=1.0,
        entity_delta_limit=1e-5,
    )

    # Entity e1 spends 0.6
    ledger.reserve(tenant_id="t1", dataset_id="ds_large", entity_id="e1", epsilon=0.6, delta=0.0)

    # Entity e1 attempts to spend 0.5 (0.6 + 0.5 = 1.1 > 1.0 limit) -> must fail closed
    with pytest.raises(EntityPrivacyBudgetExhaustedError, match="Entity privacy budget exhausted"):
        ledger.reserve(tenant_id="t1", dataset_id="ds_large", entity_id="e1", epsilon=0.5, delta=0.0)

    # Another entity e2 can still reserve budget without issue
    res_e2 = ledger.reserve(tenant_id="t1", dataset_id="ds_large", entity_id="e2", epsilon=0.5, delta=0.0)
    assert res_e2.status == DatasetReservationStatus.PENDING


def test_release_with_laplace_perturbation() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset(
        dataset_id="ds_metrics",
        tenant_id="t1",
        max_epsilon=5.0,
        max_delta=1e-4,
        entity_epsilon_limit=2.0,
    )

    res = ledger.reserve(
        tenant_id="t1",
        dataset_id="ds_metrics",
        entity_id="e10",
        epsilon=1.0,
        delta=0.0,
        sensitivity=5.0,
    )

    release_result = ledger.commit_release(
        reservation_id=res.reservation_id,
        raw_value=100.0,
        noise_seed=12345,
        min_bound=0.0,
        max_bound=200.0,
    )

    assert isinstance(release_result, DatasetPrivacyReleaseResult)
    assert release_result.reservation_id == res.reservation_id
    assert release_result.committed_epsilon == 1.0
    assert 0.0 <= release_result.perturbed_value <= 200.0
    assert release_result.perturbed_value != 100.0  # Noise was added

    # Verify reservation status transitioned to COMMITTED
    updated_res = ledger.get_reservation(res.reservation_id)
    assert updated_res is not None
    assert updated_res.status == DatasetReservationStatus.COMMITTED


def test_cancellation_of_pending_reservation_restores_budget() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset(
        dataset_id="ds_cancel",
        tenant_id="t1",
        max_epsilon=2.0,
        entity_epsilon_limit=1.0,
    )

    res = ledger.reserve(tenant_id="t1", dataset_id="ds_cancel", entity_id="e1", epsilon=0.8, delta=0.0)
    rem_before = ledger.remaining(tenant_id="t1", dataset_id="ds_cancel", entity_id="e1")
    assert rem_before["entity_remaining_epsilon"] == pytest.approx(0.2)

    cancelled = ledger.cancel_reservation(res.reservation_id)
    assert cancelled is True

    rem_after = ledger.remaining(tenant_id="t1", dataset_id="ds_cancel", entity_id="e1")
    assert rem_after["entity_remaining_epsilon"] == pytest.approx(1.0)
    assert rem_after["dataset_remaining_epsilon"] == pytest.approx(2.0)


def test_monotone_suprema_reconciliation_prevents_double_spending() -> None:
    ledger_a = DatasetPrivacyLedger()
    ledger_a.register_dataset("ds_dist", "t1", max_epsilon=10.0, entity_epsilon_limit=5.0)

    ledger_b = DatasetPrivacyLedger()
    ledger_b.register_dataset("ds_dist", "t1", max_epsilon=10.0, entity_epsilon_limit=5.0)

    # Partition: Replica A spends 2.0 on entity X
    res_a = ledger_a.reserve("t1", "ds_dist", "user_x", epsilon=2.0, delta=0.0)
    ledger_a.commit_release(res_a.reservation_id, raw_value=50.0)

    # Partition: Replica B spends 3.0 on entity X and 1.0 on entity Y
    res_b1 = ledger_b.reserve("t1", "ds_dist", "user_x", epsilon=3.0, delta=0.0)
    ledger_b.commit_release(res_b1.reservation_id, raw_value=60.0)
    res_b2 = ledger_b.reserve("t1", "ds_dist", "user_y", epsilon=1.0, delta=0.0)
    ledger_b.commit_release(res_b2.reservation_id, raw_value=70.0)

    # Reconcile B into A
    snap_b = ledger_b.export_snapshot("t1", "ds_dist")
    reconciliation = ledger_a.reconcile(snap_b)

    assert reconciliation["reconciled_entities"] >= 2
    rem_a_x = ledger_a.remaining("t1", "ds_dist", "user_x")
    # Monotone join: max(2.0, 3.0) = 3.0 spent => remaining = 5.0 - 3.0 = 2.0
    assert rem_a_x["entity_remaining_epsilon"] == pytest.approx(2.0)

    rem_a_y = ledger_a.remaining("t1", "ds_dist", "user_y")
    # Monotone join: max(0.0, 1.0) = 1.0 spent => remaining = 5.0 - 1.0 = 4.0
    assert rem_a_y["entity_remaining_epsilon"] == pytest.approx(4.0)


def test_sub_250us_dataset_privacy_slo() -> None:
    ledger = DatasetPrivacyLedger()
    ledger.register_dataset("ds_perf", "t1", max_epsilon=1000.0, entity_epsilon_limit=100.0)

    # Warmup
    for i in range(10):
        r = ledger.reserve("t1", "ds_perf", f"e_{i}", epsilon=0.01, delta=0.0)
        ledger.commit_release(r.reservation_id, raw_value=10.0, noise_seed=i)

    # Benchmark: 500 iterations of reserve + commit_release
    iterations = 500
    t0 = time.perf_counter()
    for i in range(iterations):
        r = ledger.reserve("t1", "ds_perf", f"entity_{i % 20}", epsilon=0.01, delta=0.0)
        ledger.commit_release(r.reservation_id, raw_value=42.0, noise_seed=i)
    t1 = time.perf_counter()

    avg_us = ((t1 - t0) / iterations) * 1_000_000
    assert avg_us < 250.0, f"Dataset privacy operations took {avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_singleton_and_status() -> None:
    st = global_dataset_privacy_ledger.status()
    assert "total_registered_datasets" in st
    assert "total_active_reservations" in st
    assert "total_entities_tracked" in st


def test_dataset_privacy_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    app = create_app(storage=SQLiteStorageAdapter(":memory:"))
    client = TestClient(app)

    # 1. Register dataset
    res_reg = client.post(
        "/api/v1/dataset-privacy/register",
        json={
            "dataset_id": "ds_rest_test",
            "tenant_id": "t_rest",
            "max_epsilon": 5.0,
            "max_delta": 1e-4,
            "entity_epsilon_limit": 1.0,
            "entity_delta_limit": 1e-5,
            "description": "REST test dataset",
        }
    )
    assert res_reg.status_code == 200, res_reg.text
    d_reg = res_reg.json()
    assert d_reg["dataset_id"] == "ds_rest_test"
    assert d_reg["max_epsilon"] == 5.0

    # 2. Reserve privacy budget
    res_res = client.post(
        "/api/v1/dataset-privacy/reserve",
        json={
            "tenant_id": "t_rest",
            "dataset_id": "ds_rest_test",
            "entity_id": "user_42",
            "epsilon": 0.4,
            "delta": 1e-6,
            "sensitivity": 1.0,
        }
    )
    assert res_res.status_code == 200, res_res.text
    d_res = res_res.json()
    assert d_res["status"] == "PENDING"
    assert d_res["epsilon"] == 0.4
    reservation_id = d_res["reservation_id"]
    assert reservation_id.startswith("res-dp-")

    # 3. Check remaining budget
    res_rem = client.get(
        "/api/v1/dataset-privacy/remaining",
        params={"tenant_id": "t_rest", "dataset_id": "ds_rest_test", "entity_id": "user_42"}
    )
    assert res_rem.status_code == 200, res_rem.text
    d_rem = res_rem.json()
    assert d_rem["entity_remaining_epsilon"] == pytest.approx(0.6)
    assert d_rem["dataset_remaining_epsilon"] == pytest.approx(4.6)

    # 4. Commit Laplace release
    res_rel = client.post(
        "/api/v1/dataset-privacy/release",
        json={
            "reservation_id": reservation_id,
            "raw_value": 100.0,
            "noise_seed": 12345,
            "min_bound": 0.0,
            "max_bound": 200.0,
        }
    )
    assert res_rel.status_code == 200, res_rel.text
    d_rel = res_rel.json()
    assert d_rel["reservation_id"] == reservation_id
    assert d_rel["raw_value"] == 100.0
    assert "perturbed_value" in d_rel
    assert "release_hash" in d_rel

    # 5. Fail-closed budget exhaustion (attempt reserve 0.8 when only 0.6 remains for user_42)
    res_fail = client.post(
        "/api/v1/dataset-privacy/reserve",
        json={
            "tenant_id": "t_rest",
            "dataset_id": "ds_rest_test",
            "entity_id": "user_42",
            "epsilon": 0.8,
            "delta": 0.0,
        }
    )
    assert res_fail.status_code == 429, res_fail.text
    assert "exhausted" in res_fail.json()["detail"].lower()

    # 6. Cancel a reservation
    res_cancel_prep = client.post(
        "/api/v1/dataset-privacy/reserve",
        json={
            "tenant_id": "t_rest",
            "dataset_id": "ds_rest_test",
            "entity_id": "user_cancel",
            "epsilon": 0.5,
            "delta": 0.0,
        }
    )
    assert res_cancel_prep.status_code == 200
    res_cancel_id = res_cancel_prep.json()["reservation_id"]

    res_canc = client.post(
        "/api/v1/dataset-privacy/cancel",
        json={"reservation_id": res_cancel_id}
    )
    assert res_canc.status_code == 200
    assert res_canc.json()["status"] == "CANCELLED"

    # 7. Snapshot & Reconcile
    res_snap = client.get(
        "/api/v1/dataset-privacy/snapshot",
        params={"tenant_id": "t_rest", "dataset_id": "ds_rest_test"}
    )
    assert res_snap.status_code == 200
    snap_data = res_snap.json()
    assert snap_data["dataset_id"] == "ds_rest_test"

    res_rec = client.post(
        "/api/v1/dataset-privacy/reconcile",
        json={"snapshot": snap_data}
    )
    assert res_rec.status_code == 200
    assert res_rec.json()["dataset_id"] == "ds_rest_test"

    # 8. Operational status endpoint
    res_stat = client.get("/api/v1/dataset-privacy/status")
    assert res_stat.status_code == 200
    d_stat = res_stat.json()
    assert d_stat["total_registered_datasets"] >= 1
    assert d_stat["total_releases_committed"] >= 1

