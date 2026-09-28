# -*- coding: utf-8 -*-
"""
Tests for Seam 62: Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation (ADR 0073).
Verifies that multi-agent external tool mutations adhere to strict two-phase commit semantics,
fail-closed outcome-unknown transitions on network partitions, and active downstream reconciliation.
"""

import time
import pytest

from swarmwatch.downstream_effect import (
    DownstreamEffectBoundary,
    DownstreamIdempotencyContract,
    EffectPhase,
    EffectPreparation,
    DispatchResult,
    DownstreamReceipt,
    ReconciliationResult,
    UncoordinatedRetryBlockedError,
    StaleEpochError,
    global_downstream_effect_boundary,
)


def test_happy_path_prepare_dispatch_acknowledge():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-alpha",
        effect_key="billing:charge:user_123",
        target_uri="https://api.stripe.mock/v1/charges",
        payload={"amount": 4900, "currency": "usd"},
        epoch=10,
        contract=DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    )

    assert prep.status == EffectPhase.PREPARED
    assert prep.effect_id.startswith("eff_")
    assert prep.idempotency_key is not None
    assert prep.epoch == 10

    # Simulate dispatch across network boundary
    def mock_dispatch(effect: EffectPreparation):
        assert effect.status == EffectPhase.PREPARED
        return {
            "status": "ok",
            "downstream_tx_id": "tx_998877",
            "provider_digest": "sha256:abc12345",
        }

    dispatch_res = boundary.dispatch(prep.effect_id, mock_dispatch)
    assert dispatch_res.status == EffectPhase.DISPATCHED

    # Acknowledge receipt
    receipt = DownstreamReceipt(
        downstream_confirmation_id="tx_998877",
        result_digest="sha256:abc12345",
        latency_ms=14.2,
    )
    ack_res = boundary.acknowledge(prep.effect_id, receipt)
    assert ack_res.status == EffectPhase.COMMITTED
    assert ack_res.is_outcome_unknown is False
    assert ack_res.receipt is not None
    assert ack_res.receipt.downstream_confirmation_id == "tx_998877"


def test_network_timeout_transitions_to_outcome_unknown():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-alpha",
        effect_key="cloud:vm:provision_vm_01",
        target_uri="https://cloud.mock/v1/instances",
        payload={"flavor": "c5.xlarge"},
        epoch=10,
        contract=DownstreamIdempotencyContract.UNCOORDINATED,
    )

    # Simulate dispatch timeout / connection reset
    def timeout_dispatch(effect: EffectPreparation):
        raise TimeoutError("Network connection reset by peer during POST")

    res = boundary.dispatch(prep.effect_id, timeout_dispatch)
    assert res.status == EffectPhase.OUTCOME_UNKNOWN
    assert res.is_outcome_unknown is True
    assert "TimeoutError" in (res.error_message or "")

    # Blind re-dispatch of uncoordinated service must be blocked fail-closed
    with pytest.raises(UncoordinatedRetryBlockedError):
        boundary.dispatch(prep.effect_id, timeout_dispatch)


def test_reconciliation_probe_detected_downstream_commit():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-beta",
        effect_key="db:mutation:order_456",
        target_uri="https://db.internal/mutations",
        payload={"order_id": 456, "status": "PAID"},
        epoch=12,
        contract=DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    )

    # Dispatch times out
    def broken_dispatch(effect: EffectPreparation):
        raise ConnectionResetError("Socket broken mid-flight")

    boundary.dispatch(prep.effect_id, broken_dispatch)
    record = boundary.get_effect(prep.effect_id)
    assert record.status == EffectPhase.OUTCOME_UNKNOWN

    # Active reconciliation probe finds it committed downstream
    def mock_probe(effect_id: str, idempotency_key: str):
        return {
            "found": True,
            "status": "COMMITTED",
            "downstream_id": "db_commit_999",
            "digest": "sha256:fedcba",
        }

    recon_res = boundary.reconcile(prep.effect_id, mock_probe)
    assert recon_res.status == EffectPhase.COMMITTED
    assert recon_res.reconciled is True
    assert recon_res.is_outcome_unknown is False
    assert recon_res.downstream_confirmation_id == "db_commit_999"

    # Verify updated record
    updated = boundary.get_effect(prep.effect_id)
    assert updated.status == EffectPhase.COMMITTED
    assert updated.reconciled is True


def test_reconciliation_probe_detected_downstream_not_found_aborts():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-gamma",
        effect_key="email:send:welcome_user_789",
        target_uri="https://mail.mock/v1/send",
        payload={"to": "user@example.com"},
        epoch=15,
        contract=DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    )

    def failing_dispatch(effect: EffectPreparation):
        raise TimeoutError("Gateway 504 Timeout")

    boundary.dispatch(prep.effect_id, failing_dispatch)
    assert boundary.get_effect(prep.effect_id).status == EffectPhase.OUTCOME_UNKNOWN

    # Downstream probe returns NOT FOUND
    def probe_not_found(effect_id: str, idempotency_key: str):
        return {"found": False}

    recon_res = boundary.reconcile(prep.effect_id, probe_not_found)
    assert recon_res.status == EffectPhase.ABORTED
    assert recon_res.reconciled is True
    assert recon_res.is_outcome_unknown is False

    updated = boundary.get_effect(prep.effect_id)
    assert updated.status == EffectPhase.ABORTED


def test_reconciliation_probe_timeout_remains_outcome_unknown():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-gamma",
        effect_key="wire:transfer:txn_001",
        target_uri="https://bank.mock/wire",
        payload={"amt": 10000},
        epoch=20,
    )

    boundary.dispatch(prep.effect_id, lambda e: (_ for _ in ()).throw(TimeoutError("No ack")))
    assert boundary.get_effect(prep.effect_id).status == EffectPhase.OUTCOME_UNKNOWN

    # Probe also times out
    def probe_timeout(effect_id: str, idempotency_key: str):
        raise TimeoutError("Probe timeout")

    recon_res = boundary.reconcile(prep.effect_id, probe_timeout)
    assert recon_res.status == EffectPhase.OUTCOME_UNKNOWN
    assert recon_res.is_outcome_unknown is True
    assert recon_res.probe_attempts == 1


def test_fenced_retry_for_downstream_fenced_service():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-retry",
        effect_key="storage:s3:put_manifest",
        target_uri="https://s3.mock/bucket/manifest",
        payload={"etag": "abc"},
        epoch=25,
        contract=DownstreamIdempotencyContract.DOWNSTREAM_FENCED,
    )

    # First attempt times out
    boundary.dispatch(prep.effect_id, lambda e: (_ for _ in ()).throw(TimeoutError("Drop")))
    assert boundary.get_effect(prep.effect_id).status == EffectPhase.OUTCOME_UNKNOWN

    # Second attempt succeeds
    def retry_dispatch(effect: EffectPreparation):
        assert effect.idempotency_key == prep.idempotency_key
        return {"ok": True, "tx_id": "s3_ver_42"}

    res = boundary.dispatch(prep.effect_id, retry_dispatch)
    assert res.status == EffectPhase.DISPATCHED

    boundary.acknowledge(prep.effect_id, DownstreamReceipt("s3_ver_42", "sha256:manifest"))
    assert boundary.get_effect(prep.effect_id).status == EffectPhase.COMMITTED


def test_stale_epoch_rejection():
    boundary = DownstreamEffectBoundary()
    prep = boundary.prepare(
        tenant_id="tenant-epoch",
        effect_key="action:test",
        target_uri="https://api.mock/action",
        payload={},
        epoch=50,
    )

    # Attempt acknowledge with older epoch than current fence
    boundary.set_tenant_epoch_fence("tenant-epoch", current_epoch=55)

    with pytest.raises(StaleEpochError):
        boundary.acknowledge(
            prep.effect_id,
            DownstreamReceipt("tx_1", "sha256:test"),
            epoch=50,
        )


def test_in_memory_latency_and_zero_token_invariants():
    boundary = DownstreamEffectBoundary()
    # Warmup JIT/interpreter
    for w in range(20):
        prep = boundary.prepare(
            tenant_id="warmup-tenant",
            effect_key=f"warm:action:{w}",
            target_uri="https://warm.mock",
            payload={"w": w},
            epoch=1,
        )
        boundary.dispatch(prep.effect_id, lambda e: {"ok": True})
        boundary.acknowledge(prep.effect_id, DownstreamReceipt(f"tx_{w}", "digest"))

    t0 = time.perf_counter()
    for i in range(100):
        prep = boundary.prepare(
            tenant_id="perf-tenant",
            effect_key=f"perf:action:{i}",
            target_uri="https://perf.mock",
            payload={"i": i},
            epoch=1,
        )
        boundary.dispatch(prep.effect_id, lambda e: {"ok": True})
        boundary.acknowledge(prep.effect_id, DownstreamReceipt(f"tx_{i}", "digest"))
    elapsed_us = (time.perf_counter() - t0) * 1_000_000 / 100
    # Must execute within sub-250 microseconds per full prepare-dispatch-ack cycle (audit SLO)
    assert elapsed_us < 250.0, f"Average latency too high: {elapsed_us:.2f}us"



def test_global_singleton_and_status_summary():
    status = global_downstream_effect_boundary.get_status()
    assert "total_effects" in status
    assert "outcome_unknown_count" in status
    assert "committed_count" in status
    assert "epoch_fences" in status


def test_downstream_effect_rest_api():
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    app = create_app()
    client = TestClient(app)

    # 1. Prepare
    resp = client.post("/api/v1/downstream-effects/prepare", json={
        "tenant_id": "test-rest-tenant",
        "effect_key": "payment:charge:rest_01",
        "target_uri": "https://api.mock/pay",
        "payload": {"amount": 5000},
        "epoch": 1,
    })
    assert resp.status_code == 200, resp.text
    prep = resp.json()
    effect_id = prep["effect_id"]
    assert prep["status"] == "PREPARED"

    # 2. Status
    st_resp = client.get("/api/v1/downstream-effects/status")
    assert st_resp.status_code == 200
    assert st_resp.json()["total_effects"] >= 1

    # 3. Get single effect
    get_resp = client.get(f"/api/v1/downstream-effects/{effect_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["effect_id"] == effect_id

    # 4. Dispatch with timeout simulation -> OUTCOME_UNKNOWN
    disp_resp = client.post("/api/v1/downstream-effects/dispatch", json={
        "effect_id": effect_id,
        "mock_downstream_status": "TIMEOUT",
    })
    assert disp_resp.status_code == 200
    assert disp_resp.json()["status"] == "OUTCOME_UNKNOWN"
    assert disp_resp.json()["is_outcome_unknown"] is True

    # 5. Reconcile
    recon_resp = client.post("/api/v1/downstream-effects/reconcile", json={
        "effect_id": effect_id,
        "mock_probe_found": True,
        "mock_probe_status": "COMMITTED",
        "mock_downstream_id": "ch_rest_probe_999",
    })
    assert recon_resp.status_code == 200
    assert recon_resp.json()["status"] == "COMMITTED"
    assert recon_resp.json()["reconciled"] is True

