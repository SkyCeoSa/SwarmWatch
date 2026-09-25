# -*- coding: utf-8 -*-
"""
Tests for Fenced Effect Ledger & Monotonic Idempotency Barrier (Seam 52 / ADR 0063).
Validates CAS claim transitions, monotonic fencing epochs, idempotency barrier,
lease expiry takeovers, REST endpoints, and CLI interface.
"""

import json
import time
import pytest
from fastapi.testclient import TestClient

from swarmwatch.effect_ledger import (
    ClaimStatus,
    EffectReceipt,
    EffectRecord,
    EffectLedger,
    global_effect_ledger,
)
from swarmwatch.canonical import CanonicalDomainTag
from swarmwatch.server import create_app
from swarmwatch.cli import main


@pytest.fixture(autouse=True)
def reset_ledger():
    """Reset global effect ledger before each test."""
    global_effect_ledger.reset()
    yield
    global_effect_ledger.reset()


def test_effect_receipt_canonical_hash():
    receipt = EffectReceipt(
        receipt_id="rcpt-eff-001",
        effect_key="db_write:user_123",
        idempotency_key="idemp-alpha",
        epoch=1,
        tenant_id="tenant-alpha",
        committed_at=1000.0,
        result_digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    )
    assert receipt.receipt_id == "rcpt-eff-001"
    assert len(receipt.receipt_hash) == 64

    # Dict round-trip
    d = receipt.to_dict()
    assert d["receipt_id"] == "rcpt-eff-001"
    reconstructed = EffectReceipt.from_dict(d)
    assert reconstructed.receipt_id == receipt.receipt_id
    assert reconstructed.receipt_hash == receipt.receipt_hash


def test_cas_claim_authorize_and_complete_lifecycle():
    ledger = EffectLedger(initial_epoch=1)

    # 1. Claim effect
    claim_res = ledger.claim(
        effect_key="payment:invoice_999",
        tenant_id="tenant-corp",
        epoch=1,
        idempotency_key="idemp-inv-999",
        action="stripe_charge",
        timeout_ms=5000.0,
    )
    assert claim_res.status == "ACCEPTED"
    assert claim_res.claim_id.startswith("clm-")
    claim_id = claim_res.claim_id

    # 2. Authorize effect with permit hash
    fake_permit_hash = "a" * 64
    ok_auth, msg_auth = ledger.authorize(claim_id, fake_permit_hash)
    assert ok_auth is True
    assert "authorized" in msg_auth.lower()

    # 3. Complete effect
    execution_result = {"charge_id": "ch_12345", "amount_cents": 5000}
    ok_comp, receipt, msg_comp = ledger.complete(claim_id, execution_result=execution_result)
    assert ok_comp is True
    assert receipt is not None
    assert receipt.effect_key == "payment:invoice_999"
    assert len(receipt.receipt_hash) == 64

    # Status check
    status = ledger.status("tenant-corp")
    assert status["committed_effects_count"] == 1
    assert status["active_claims_count"] == 0


def test_idempotent_commit_barrier():
    ledger = EffectLedger(initial_epoch=1)

    # First claim & complete
    res1 = ledger.claim("email:welcome_user_1", idempotency_key="idemp-key-1")
    assert res1.status == "ACCEPTED"
    ok, receipt1, _ = ledger.complete(res1.claim_id, execution_result={"sent": True})
    assert ok is True

    # Second claim with SAME idempotency key returns IDEMPOTENT_COMMITTED
    res2 = ledger.claim("email:welcome_user_1", idempotency_key="idemp-key-1")
    assert res2.status == "IDEMPOTENT_COMMITTED"
    assert res2.receipt.receipt_id == receipt1.receipt_id
    assert res2.receipt.receipt_hash == receipt1.receipt_hash

    # Third claim with DIFFERENT idempotency key is REJECTED
    res3 = ledger.claim("email:welcome_user_1", idempotency_key="different-key")
    assert res3.status == "REJECTED"
    assert "already committed under different idempotency key" in res3.reason


def test_monotonic_fencing_epoch_and_stale_leader_rejection():
    ledger = EffectLedger(initial_epoch=1)

    # Claim under epoch 1
    res1 = ledger.claim("deploy:service_a", epoch=1)
    assert res1.status == "ACCEPTED"
    claim_id = res1.claim_id

    # Monotonic epoch bump (e.g. failover or trip)
    new_epoch = ledger.trip_epoch(reason="leader_election_failover")
    assert new_epoch == 2

    # Attempting to complete the old epoch claim MUST fail fail-closed
    ok, receipt, msg = ledger.complete(claim_id, execution_result={"status": "deployed"})
    assert ok is False
    assert receipt is None
    assert "epoch transition" in msg

    # Any new claim presenting stale epoch 1 MUST be rejected immediately
    res_stale = ledger.claim("deploy:service_b", epoch=1)
    assert res_stale.status == "REJECTED"
    assert "STALE_EPOCH" in res_stale.reason


def test_lease_timeout_and_cas_takeover():
    ledger = EffectLedger(initial_epoch=1)

    # Worker 1 claims key with short timeout (100ms)
    res1 = ledger.claim("cron:sync_catalog", timeout_ms=100.0)
    assert res1.status == "ACCEPTED"

    # Worker 2 attempts immediate concurrent claim -> REJECTED
    res2 = ledger.claim("cron:sync_catalog", timeout_ms=100.0)
    assert res2.status == "REJECTED"
    assert "actively locked" in res2.reason

    # Sleep past expiration
    time.sleep(0.15)

    # Worker 3 attempts claim after lease expiry -> CAS TAKEOVER ACCEPTED
    res3 = ledger.claim("cron:sync_catalog", timeout_ms=5000.0)
    assert res3.status == "ACCEPTED"
    assert res3.claim_id != res1.claim_id


def test_abort_and_reconcile():
    ledger = EffectLedger(initial_epoch=1)

    # Claim and abort
    res = ledger.claim("file:delete_temp")
    assert res.status == "ACCEPTED"
    ok, msg = ledger.abort(res.claim_id, reason="aborted_by_user")
    assert ok is True

    # Commit effects under epoch 1 and epoch 2
    res_ep1 = ledger.claim("task:one", epoch=1)
    ledger.complete(res_ep1.claim_id, execution_result="done_1")

    ledger.trip_epoch()  # epoch is now 2
    res_ep2 = ledger.claim("task:two", epoch=2)
    ledger.complete(res_ep2.claim_id, execution_result="done_2")

    # Reconcile all
    all_records = ledger.reconcile(since_epoch=1)
    assert len(all_records) == 2

    # Reconcile since epoch 2
    ep2_records = ledger.reconcile(since_epoch=2)
    assert len(ep2_records) == 1
    assert ep2_records[0]["effect_key"] == "task:two"


def test_rest_api_effects_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Claim
    res_claim = client.post(
        "/api/v1/effects/claim",
        json={
            "effect_key": "api_test:action_01",
            "tenant_id": "test-tenant",
            "epoch": 1,
            "action": "webhook_post",
        },
    )
    assert res_claim.status_code == 200
    claim_data = res_claim.json()
    assert claim_data["status"] == "ACCEPTED"
    claim_id = claim_data["claim_id"]

    # 2. Authorize
    res_auth = client.post(
        "/api/v1/effects/authorize",
        json={"claim_id": claim_id, "permit_hash": "b" * 64},
    )
    assert res_auth.status_code == 200
    assert res_auth.json()["status"] == "authorized"

    # 3. Complete
    res_comp = client.post(
        "/api/v1/effects/complete",
        json={"claim_id": claim_id, "execution_result": {"status": 200}},
    )
    assert res_comp.status_code == 200
    comp_data = res_comp.json()
    assert comp_data["status"] == "committed"
    assert comp_data["receipt"]["effect_key"] == "api_test:action_01"

    # 4. Status
    res_status = client.get("/api/v1/effects/status?tenant_id=test-tenant")
    assert res_status.status_code == 200
    assert res_status.json()["committed_effects_count"] == 1

    # 5. Reconcile
    res_rec = client.get("/api/v1/effects/reconcile?since_epoch=1")
    assert res_rec.status_code == 200
    assert res_rec.json()["count"] == 1


def test_cli_effects_commands(capsys):
    # Test claim via CLI
    with pytest.raises(SystemExit) as exc:
        main([
            "effects", "claim",
            "--effect-key", "cli:task_001",
            "--tenant", "cli-tenant",
            "--format", "json",
        ])
    assert exc.value.code == 0
    claim_out = json.loads(capsys.readouterr().out)
    assert claim_out["status"] == "ACCEPTED"
    claim_id = claim_out["claim_id"]

    # Test authorize via CLI
    with pytest.raises(SystemExit) as exc2:
        main([
            "effects", "authorize",
            "--claim-id", claim_id,
            "--permit-hash", "c" * 64,
            "--format", "json",
        ])
    assert exc2.value.code == 0
    auth_out = json.loads(capsys.readouterr().out)
    assert auth_out["status"] == "authorized"

    # Test complete via CLI
    with pytest.raises(SystemExit) as exc3:
        main([
            "effects", "complete",
            "--claim-id", claim_id,
            "--format", "json",
        ])
    assert exc3.value.code == 0
    comp_out = json.loads(capsys.readouterr().out)
    assert comp_out["status"] == "committed"

    # Test status via CLI
    with pytest.raises(SystemExit) as exc4:
        main(["effects", "status", "--tenant", "cli-tenant", "--format", "json"])
    assert exc4.value.code == 0
    status_out = json.loads(capsys.readouterr().out)
    assert status_out["committed_effects_count"] == 1
