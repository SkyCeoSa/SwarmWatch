# -*- coding: utf-8 -*-
"""
Tests for Unified Safety Authority & Linearizable Execution Permit Engine (Seam 51 / ADR 0062).
Validates atomic permit reservation, commit settlements, monotonic fencing epochs,
fail-closed partition behavior, REST endpoints, and CLI interface.
"""

import json
import time
import pytest
from fastapi.testclient import TestClient

from swarmwatch.safety_authority import (
    PermitStatus,
    AuthorityState,
    SafetyPermit,
    SafetyAuthority,
    global_safety_authority,
)
from swarmwatch.canonical import CanonicalDomainTag
from swarmwatch.server import create_app
from swarmwatch.cli import main


@pytest.fixture(autouse=True)
def reset_authority():
    """Reset global authority before each test."""
    global_safety_authority.reset()
    yield
    global_safety_authority.reset()


def test_permit_creation_and_canonical_hash():
    permit = SafetyPermit(
        permit_id="pmt-test-001",
        tenant_id="tenant-alpha",
        epoch=1,
        action="web_search",
        cost_estimate=0.05,
        tokens_estimate=500,
        status=PermitStatus.ACCEPTED,
        issued_at=1000.0,
        expires_at=1005.0,
    )
    assert permit.permit_id == "pmt-test-001"
    assert permit.status == PermitStatus.ACCEPTED
    assert len(permit.permit_hash) == 64

    # Dict round-trip
    d = permit.to_dict()
    assert d["permit_id"] == "pmt-test-001"
    assert d["epoch"] == 1

    reconstructed = SafetyPermit.from_dict(d)
    assert reconstructed.permit_id == permit.permit_id
    assert reconstructed.permit_hash == permit.permit_hash

    # Validity check
    assert permit.is_valid(current_epoch=1, current_time=1002.0) is True
    assert permit.is_valid(current_epoch=2, current_time=1002.0) is False  # Fencing epoch mismatch
    assert permit.is_valid(current_epoch=1, current_time=1006.0) is False  # Expired


def test_atomic_reserve_commit_and_release():
    auth = SafetyAuthority(initial_epoch=1)
    auth.configure_tenant("tenant-1", max_cost=10.0, max_tokens=100_000, max_turns=50)

    # 1. Reserve permit
    permit = auth.reserve(
        tenant_id="tenant-1",
        action="llm_generate",
        cost_estimate=2.5,
        tokens_estimate=5000,
        timeout_ms=5000.0,
    )
    assert permit.status == PermitStatus.ACCEPTED
    assert permit.epoch == 1
    assert auth.status("tenant-1")["active_permits_count"] == 1
    assert auth.status("tenant-1")["tenant"]["reserved_cost"] == 2.5
    assert auth.status("tenant-1")["tenant"]["available_cost"] == 7.5

    # 2. Commit permit
    ok, msg = auth.commit(permit.permit_id, actual_cost=2.0, actual_tokens=4500)
    assert ok is True
    assert "successfully" in msg
    assert auth.status("tenant-1")["active_permits_count"] == 0
    assert auth.status("tenant-1")["tenant"]["reserved_cost"] == 0.0
    assert auth.status("tenant-1")["tenant"]["spent_cost"] == 2.0
    assert auth.status("tenant-1")["tenant"]["available_cost"] == 8.0
    assert auth.status("tenant-1")["tenant"]["spent_turns"] == 1

    # 3. Test Release
    permit2 = auth.reserve("tenant-1", cost_estimate=1.0, tokens_estimate=2000)
    assert auth.status("tenant-1")["tenant"]["reserved_cost"] == 1.0
    ok_rel, msg_rel = auth.release(permit2.permit_id, reason="task_canceled")
    assert ok_rel is True
    assert auth.status("tenant-1")["tenant"]["reserved_cost"] == 0.0
    assert auth.status("tenant-1")["tenant"]["spent_cost"] == 2.0  # Spend unchanged


def test_budget_exhaustion_rejection():
    auth = SafetyAuthority(initial_epoch=1)
    auth.configure_tenant("tenant-budget", max_cost=1.0, max_tokens=1000)

    # Request exceeding cost budget
    permit_fail = auth.reserve("tenant-budget", cost_estimate=5.0)
    assert permit_fail.status == PermitStatus.REJECTED
    assert "Insufficient cost budget" in permit_fail.rejection_reason

    # Request exceeding token budget
    permit_fail_tok = auth.reserve("tenant-budget", cost_estimate=0.5, tokens_estimate=5000)
    assert permit_fail_tok.status == PermitStatus.REJECTED
    assert "Insufficient token budget" in permit_fail_tok.rejection_reason


def test_fencing_epoch_and_trip_invalidation():
    auth = SafetyAuthority(initial_epoch=1)
    auth.configure_tenant("tenant-fenced", max_cost=50.0)

    # Issue permit under epoch 1
    permit = auth.reserve("tenant-fenced", cost_estimate=1.0)
    assert permit.epoch == 1

    # Trip the safety authority globally
    new_epoch = auth.trip(reason="entropy_collapse_detected")
    assert new_epoch == 2
    assert auth.epoch == 2
    assert auth.global_state == AuthorityState.TRIPPED

    # Attempting to commit the old permit MUST fail because epoch changed
    ok, msg = auth.commit(permit.permit_id, actual_cost=1.0)
    assert ok is False
    assert "not found in active permits" in msg or "invalidated" in msg

    # Any new reserve requests while tripped must be REJECTED
    permit_blocked = auth.reserve("tenant-fenced", cost_estimate=0.5)
    assert permit_blocked.status == PermitStatus.REJECTED
    assert "tripped at epoch 2" in permit_blocked.rejection_reason

    # Re-arm increments epoch again and restores NOMINAL
    epoch_rearm = auth.rearm()
    assert epoch_rearm == 3
    assert auth.global_state == AuthorityState.NOMINAL

    # New permit succeeds under epoch 3
    permit_new = auth.reserve("tenant-fenced", cost_estimate=0.5)
    assert permit_new.status == PermitStatus.ACCEPTED
    assert permit_new.epoch == 3


def test_lease_timeout_expiration():
    auth = SafetyAuthority(initial_epoch=1)
    auth.configure_tenant("tenant-expire", max_cost=20.0)

    # Issue permit with very short timeout (100ms)
    permit = auth.reserve("tenant-expire", cost_estimate=5.0, timeout_ms=100.0)
    assert permit.status == PermitStatus.ACCEPTED

    # Sleep past expiration
    time.sleep(0.15)

    # Commit must fail due to expiration
    ok, msg = auth.commit(permit.permit_id, actual_cost=5.0)
    assert ok is False
    assert "expired before commit" in msg

    # Reserved budget should have been safely freed
    assert auth.status("tenant-expire")["tenant"]["reserved_cost"] == 0.0


def test_fail_closed_partition_mode():
    auth = SafetyAuthority(initial_epoch=1)
    auth.configure_tenant("tenant-partition", max_cost=10.0)

    # Simulate network partition
    auth.set_partition_simulation(True)

    permit = auth.reserve("tenant-partition", cost_estimate=1.0)
    assert permit.status == PermitStatus.INDETERMINATE
    assert "fail-closed" in permit.rejection_reason

    # Restore partition
    auth.set_partition_simulation(False)
    permit_ok = auth.reserve("tenant-partition", cost_estimate=1.0)
    assert permit_ok.status == PermitStatus.ACCEPTED


def test_rest_api_safety_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Reserve permit
    res = client.post(
        "/api/v1/safety/reserve",
        json={
            "tenant_id": "test-rest-tenant",
            "action": "deep_research",
            "cost_estimate": 0.5,
            "tokens_estimate": 1200,
            "timeout_ms": 10000.0,
        },
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "ACCEPTED"
    permit_id = data["permit"]["permit_id"]
    assert permit_id.startswith("pmt-")

    # 2. Check status
    res_status = client.get("/api/v1/safety/status?tenant_id=test-rest-tenant")
    assert res_status.status_code == 200
    status_data = res_status.json()
    assert status_data["active_permits_count"] == 1
    assert status_data["epoch"] == 1

    # 3. Commit permit
    res_commit = client.post(
        "/api/v1/safety/commit",
        json={"permit_id": permit_id, "actual_cost": 0.45, "actual_tokens": 1150},
    )
    assert res_commit.status_code == 200
    assert res_commit.json()["status"] == "committed"

    # 4. Trip safety authority
    res_trip = client.post("/api/v1/safety/trip", json={"reason": "simulated_test_trip"})
    assert res_trip.status_code == 200
    assert res_trip.json()["status"] == "tripped"
    assert res_trip.json()["new_epoch"] == 2

    # 5. Rearm safety authority
    res_rearm = client.post("/api/v1/safety/rearm", json={})
    assert res_rearm.status_code == 200
    assert res_rearm.json()["status"] == "rearmed"
    assert res_rearm.json()["new_epoch"] == 3


def test_cli_safety_commands(capsys):
    # Test reserve via CLI
    with pytest.raises(SystemExit) as exc:
        main([
            "safety", "reserve",
            "--tenant", "cli-tenant",
            "--action-name", "code_eval",
            "--cost", "0.25",
            "--tokens", "800",
            "--format", "json",
        ])
    assert exc.value.code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "ACCEPTED"
    permit_id = out["permit_id"]

    # Test status via CLI
    with pytest.raises(SystemExit) as exc2:
        main(["safety", "status", "--tenant", "cli-tenant", "--format", "json"])
    assert exc2.value.code == 0
    status_out = json.loads(capsys.readouterr().out)
    assert status_out["active_permits_count"] == 1

    # Test commit via CLI
    with pytest.raises(SystemExit) as exc3:
        main([
            "safety", "commit",
            "--permit-id", permit_id,
            "--cost", "0.20",
            "--tokens", "750",
            "--format", "json",
        ])
    assert exc3.value.code == 0
    commit_out = json.loads(capsys.readouterr().out)
    assert commit_out["status"] == "committed"
