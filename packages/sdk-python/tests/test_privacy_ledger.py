# -*- coding: utf-8 -*-
"""
Tests for Privacy Release Ledger & Differential Privacy Accountant (Seam 58 / ADR 0069).
Verifies:
1. Calibrated Laplace noise generation and deterministic seeded reproducibility.
2. Sensitivity bounding and input clipping.
3. Linear and advanced composition accounting.
4. Privacy budget reservation lifecycle and fail-closed budget exhaustion.
5. Multi-type data release (scalar, list, dict) with canonical hashing.
6. REST endpoints and CLI subcommands.
"""

from __future__ import annotations

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag
from swarmwatch.cli import main as cli_main
from swarmwatch.privacy_ledger import (
    CompositionAccountant,
    PrivacyBudgetExhaustedError,
    PrivacyRelease,
    PrivacyReleaseResult,
    PrivacyReservation,
    ReservationStatus,
    clip_value,
    global_privacy_release,
    sample_laplace,
)
from swarmwatch.server import create_app


@pytest.fixture(autouse=True)
def reset_privacy():
    global_privacy_release.reset()
    yield
    global_privacy_release.reset()


def test_laplace_sampling_and_deterministic_seed():
    # Scale <= 0 returns 0.0
    assert sample_laplace(0.0) == 0.0
    assert sample_laplace(-1.0) == 0.0

    # Deterministic seed reproducibility
    s1 = sample_laplace(scale=2.0, seed=42)
    s2 = sample_laplace(scale=2.0, seed=42)
    assert s1 == s2
    assert s1 != 0.0

    # CSPRNG non-zero
    rand_sample = sample_laplace(scale=1.5)
    assert isinstance(rand_sample, float)


def test_clip_value_bounds():
    assert clip_value(15.0, min_val=0.0, max_val=10.0) == 10.0
    assert clip_value(-5.0, min_val=0.0, max_val=10.0) == 0.0
    assert clip_value(5.0, min_val=0.0, max_val=10.0) == 5.0
    assert clip_value(100.0, min_val=None, max_val=None) == 100.0


def test_composition_accountant():
    accountant = CompositionAccountant()
    accountant.record(epsilon=0.5, delta=1e-5)
    accountant.record(epsilon=0.5, delta=1e-5)

    tot_eps, tot_delta = accountant.linear_total()
    assert tot_eps == 1.0
    assert tot_delta == 2e-5

    adv_eps, adv_delta = accountant.advanced_composition(delta_prime=1e-5)
    assert adv_eps <= tot_eps
    assert adv_delta == tot_delta + 1e-5


def test_privacy_reserve_lifecycle_and_fail_closed():
    pr = PrivacyRelease(default_epsilon=2.0)

    # Valid reservation
    res1 = pr.reserve(tenant_id="tenant-alpha", epsilon=1.2, delta=1e-5, sensitivity=1.0)
    assert res1.reservation_id.startswith("prev-")
    assert res1.status == ReservationStatus.PENDING
    assert res1.reservation_hash is not None

    # Pending reservation reduces available budget
    rem = pr.remaining("tenant-alpha")
    assert pytest.approx(rem["remaining_epsilon"], 0.001) == 0.8

    # Over-budget request fails closed
    with pytest.raises(PrivacyBudgetExhaustedError):
        pr.reserve(tenant_id="tenant-alpha", epsilon=1.0)


def test_privacy_release_scalar_list_dict():
    pr = PrivacyRelease(default_epsilon=5.0)

    # 1. Scalar release
    res_scalar = pr.reserve(tenant_id="tenant-1", epsilon=1.0, sensitivity=1.0)
    rel_scalar = pr.release(
        reservation_id=res_scalar.reservation_id,
        raw_value=12.5,
        noise_seed=100,
        min_bound=0.0,
        max_bound=20.0,
    )
    assert rel_scalar.raw_clipped == 12.5
    assert rel_scalar.perturbed_value != 12.5
    assert rel_scalar.epsilon_spent == 1.0
    assert rel_scalar.remaining_epsilon == 4.0
    assert rel_scalar.release_hash is not None

    # Released reservation cannot be reused
    with pytest.raises(ValueError):
        pr.release(res_scalar.reservation_id, raw_value=10.0)

    # 2. List release
    res_list = pr.reserve(tenant_id="tenant-1", epsilon=1.0, sensitivity=2.0)
    rel_list = pr.release(
        reservation_id=res_list.reservation_id,
        raw_value=[5.0, 10.0, 15.0],
        noise_seed=200,
        min_bound=0.0,
        max_bound=10.0,
    )
    assert len(rel_list.raw_clipped) == 3
    assert rel_list.raw_clipped[2] == 10.0  # Clipped from 15.0 to 10.0
    assert len(rel_list.perturbed_value) == 3

    # 3. Dict release
    res_dict = pr.reserve(tenant_id="tenant-1", epsilon=1.0, sensitivity=1.0)
    rel_dict = pr.release(
        reservation_id=res_dict.reservation_id,
        raw_value={"tokens": 500.0, "latency": 120.0},
        noise_seed=300,
    )
    assert set(rel_dict.perturbed_value.keys()) == {"tokens", "latency"}


def test_deterministic_seeded_release_audit():
    pr = PrivacyRelease(default_epsilon=5.0)

    res1 = pr.reserve(tenant_id="t-audit", epsilon=1.0, sensitivity=1.0)
    rel1 = pr.release(res1.reservation_id, raw_value=42.0, noise_seed=999)

    # Second release with fresh ledger
    pr2 = PrivacyRelease(default_epsilon=5.0)
    res2 = pr2.reserve(tenant_id="t-audit", epsilon=1.0, sensitivity=1.0)
    rel2 = pr2.release(res2.reservation_id, raw_value=42.0, noise_seed=999)

    assert rel1.perturbed_value == rel2.perturbed_value
    assert rel1.noise_added == rel2.noise_added


def test_status_and_reset():
    pr = PrivacyRelease(default_epsilon=1.0)
    res = pr.reserve("t-stat", epsilon=1.0)
    pr.release(res.reservation_id, raw_value=1.0, noise_seed=1)

    st = pr.status("t-stat")
    assert st["total_releases"] == 1
    assert st["total_reservations"] == 1
    assert st["budget_constrained_count"] == 1
    assert st["status"] == "BUDGET_CONSTRAINED"

    pr.reset()
    st_empty = pr.status("t-stat")
    assert st_empty["total_releases"] == 0
    assert st_empty["total_reservations"] == 0


def test_server_privacy_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Reserve endpoint
    res_resp = client.post(
        "/api/v1/privacy-ledger/reserve",
        json={
            "tenant_id": "tenant-api-test",
            "epsilon": 2.0,
            "delta": 1e-5,
            "sensitivity": 1.0,
            "query_id": "q-101",
        },
    )
    assert res_resp.status_code == 200
    res_data = res_resp.json()
    assert res_data["epsilon"] == 2.0
    resid = res_data["reservation_id"]

    # 2. Release endpoint
    rel_resp = client.post(
        "/api/v1/privacy-ledger/release",
        json={
            "reservation_id": resid,
            "raw_value": 50.0,
            "noise_seed": 1234,
            "min_bound": 0.0,
            "max_bound": 100.0,
        },
    )
    assert rel_resp.status_code == 200
    rel_data = rel_resp.json()
    assert rel_data["epsilon_spent"] == 2.0
    assert rel_data["perturbed_value"] != 50.0

    # 3. Remaining endpoint
    rem_resp = client.get("/api/v1/privacy-ledger/remaining?tenant_id=tenant-api-test")
    assert rem_resp.status_code == 200
    rem_data = rem_resp.json()
    assert rem_data["spent_epsilon"] == 2.0

    # 4. Status endpoint
    stat_resp = client.get("/api/v1/privacy-ledger/status?tenant_id=tenant-api-test")
    assert stat_resp.status_code == 200
    stat_data = stat_resp.json()
    assert stat_data["total_releases"] == 1


def test_cli_privacy_subcommands(capsys):
    # Reserve via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main([
            "privacy-ledger", "reserve",
            "--tenant", "tenant-cli",
            "--epsilon", "1.5",
            "--sensitivity", "1.0",
            "--format", "json",
        ])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    res_data = json.loads(captured.out)
    assert res_data["status"] == "PENDING"
    resid = res_data["reservation_id"]

    # Release via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main([
            "privacy-ledger", "release",
            "--reservation-id", resid,
            "--value", "75.0",
            "--noise-seed", "777",
            "--format", "json",
        ])
    assert exc.value.code == 0
    captured_rel = capsys.readouterr()
    rel_data = json.loads(captured_rel.out)
    assert rel_data["raw_clipped"] == 75.0
    assert rel_data["perturbed_value"] != 75.0

    # Remaining via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["privacy-ledger", "remaining", "--tenant", "tenant-cli", "--format", "json"])
    assert exc.value.code == 0
    captured_rem = capsys.readouterr()
    rem_data = json.loads(captured_rem.out)
    assert rem_data["spent_epsilon"] == 1.5

    # Status via CLI
    with pytest.raises(SystemExit) as exc:
        cli_main(["privacy-ledger", "status", "--format", "json"])
    assert exc.value.code == 0
    captured_stat = capsys.readouterr()
    stat_data = json.loads(captured_stat.out)
    assert stat_data["total_releases"] >= 1

