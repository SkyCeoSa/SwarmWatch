# -*- coding: utf-8 -*-
"""
Unit and integration tests for Autonomous Action Gateway (Seam 71 / ADR 0082).
"""

import time
import pytest

from swarmwatch.action_gateway import (
    ActionGatewayError,
    UnauthorizedActionError,
    ActionQuorumPendingError,
    ActionCircuitBlockedError,
    ActionExecutionFailedError,
    ActionRiskLevel,
    ActionApprovalStatus,
    ActionRequest,
    ActionApproval,
    ActionExecutionReceipt,
    ActionGateway,
    global_action_gateway,
)


def test_low_risk_auto_approved_execution() -> None:
    gateway = ActionGateway()
    req = gateway.request_action(
        tenant_id="tenant-alpha",
        agent_id="agent-analyst",
        action_name="db:query",
        target_resource="pg://analytics/metrics",
        parameters={"sql": "SELECT 1"},
        estimated_cost_usd=0.05,
    )

    assert req.risk_level == ActionRiskLevel.LOW
    assert req.status == ActionApprovalStatus.APPROVED
    assert req.required_quorum_count == 0

    receipt = gateway.execute_action(req.request_id)
    assert receipt.status == "SUCCESS"
    assert receipt.result_summary["rows_returned"] == 42
    assert len(receipt.receipt_hash) == 64
    assert gateway.get_receipt(receipt.receipt_id) is not None


def test_medium_risk_single_approval_quorum() -> None:
    gateway = ActionGateway()
    req = gateway.request_action(
        tenant_id="tenant-beta",
        agent_id="agent-worker",
        action_name="api:webhook_call",
        target_resource="https://api.partner.com/notify",
        parameters={"url": "https://api.partner.com/notify"},
        estimated_cost_usd=5.0,
    )

    assert req.risk_level == ActionRiskLevel.MEDIUM
    assert req.status == ActionApprovalStatus.PENDING_APPROVAL
    assert req.required_quorum_count == 1

    # Cannot execute while pending approval
    with pytest.raises(ActionQuorumPendingError):
        gateway.execute_action(req.request_id)

    # Approve by operator
    gateway.approve_action(
        request_id=req.request_id,
        approver_principal="operator-alice",
        decision="APPROVED",
        comment="Verified webhook destination",
    )

    updated_req = gateway.get_request(req.request_id)
    assert updated_req is not None
    assert updated_req.status == ActionApprovalStatus.APPROVED

    receipt = gateway.execute_action(req.request_id)
    assert receipt.status == "SUCCESS"
    assert receipt.result_summary["delivered"] is True


def test_critical_risk_multi_party_quorum() -> None:
    gateway = ActionGateway()
    req = gateway.request_action(
        tenant_id="tenant-corp",
        agent_id="agent-dba",
        action_name="db:drop_table",
        target_resource="pg://prod/legacy_users",
        parameters={"table": "legacy_users"},
        estimated_cost_usd=0.0,
        reversibility_class="IRREVERSIBLE",
    )

    assert req.risk_level == ActionRiskLevel.CRITICAL
    assert req.status == ActionApprovalStatus.PENDING_APPROVAL
    assert req.required_quorum_count >= 2

    # First approval (still 1/2)
    gateway.approve_action(
        request_id=req.request_id,
        approver_principal="secops-lead",
        decision="APPROVED",
    )
    with pytest.raises(ActionQuorumPendingError):
        gateway.execute_action(req.request_id)

    # Second approval (reaches 2/2 quorum)
    gateway.approve_action(
        request_id=req.request_id,
        approver_principal="infrastructure-vp",
        decision="APPROVED",
    )

    receipt = gateway.execute_action(req.request_id)
    assert receipt.status == "SUCCESS"
    assert receipt.result_summary["dropped"] is True


def test_quorum_rejection_halts_execution() -> None:
    gateway = ActionGateway()
    req = gateway.request_action(
        tenant_id="tenant-corp",
        agent_id="agent-worker",
        action_name="finance:refund",
        target_resource="stripe://acct_01",
        parameters={"amount": 500},
        estimated_cost_usd=500.0,
    )

    # Reject request
    gateway.approve_action(
        request_id=req.request_id,
        approver_principal="risk-officer",
        decision="REJECTED",
        comment="Fraud alert flagged on account",
    )

    assert req.status == ActionApprovalStatus.REJECTED

    with pytest.raises(ActionGatewayError, match="was rejected"):
        gateway.execute_action(req.request_id)


def test_circuit_tripped_halts_gateway_fail_closed() -> None:
    gateway = ActionGateway()
    gateway.set_circuit_tripped(True)

    # Request rejected fail-closed
    with pytest.raises(ActionCircuitBlockedError):
        gateway.request_action(
            tenant_id="tenant-test",
            agent_id="agent-test",
            action_name="db:query",
            target_resource="pg://metrics",
            parameters={},
        )


def test_capability_token_enforcement() -> None:
    gateway = ActionGateway()

    # Configure mock capability validator requiring tokens
    def mock_validator(token: str, action: str, resource: str, tenant_id: str) -> bool:
        return token == "valid-token-alpha" and tenant_id == "tenant-alpha"

    gateway.set_capability_validator(mock_validator, require_tokens=True)

    # 1. Missing token raises UnauthorizedActionError
    with pytest.raises(UnauthorizedActionError, match="requires capability token"):
        gateway.request_action(
            tenant_id="tenant-alpha",
            agent_id="agent-01",
            action_name="db:query",
            target_resource="pg://analytics",
            parameters={},
        )

    # 2. Invalid token raises UnauthorizedActionError
    with pytest.raises(UnauthorizedActionError, match="rejected"):
        gateway.request_action(
            tenant_id="tenant-alpha",
            agent_id="agent-01",
            action_name="db:query",
            target_resource="pg://analytics",
            parameters={},
            capability_token="invalid-token",
        )

    # 3. Valid token succeeds
    req = gateway.request_action(
        tenant_id="tenant-alpha",
        agent_id="agent-01",
        action_name="db:query",
        target_resource="pg://analytics",
        parameters={},
        capability_token="valid-token-alpha",
    )
    assert req.status == ActionApprovalStatus.APPROVED


def test_downstream_handler_failure_produces_failed_receipt() -> None:
    gateway = ActionGateway()
    gateway.register_executor(
        action_name="faulty:action",
        handler=lambda params: 1 / 0,
        default_risk=ActionRiskLevel.LOW,
        required_quorum_count=0,
    )

    req = gateway.request_action(
        tenant_id="tenant-err",
        agent_id="agent-err",
        action_name="faulty:action",
        target_resource="test://err",
        parameters={},
    )

    with pytest.raises(ActionExecutionFailedError):
        gateway.execute_action(req.request_id)

    # Verify failed receipt was minted and recorded
    receipts = gateway.get_receipts("tenant-err")
    assert len(receipts) == 1
    assert receipts[0].status == "FAILED"
    assert "ZeroDivisionError" in str(receipts[0].result_summary)


def test_receipt_canonical_hash_integrity() -> None:
    gateway = ActionGateway()
    req = gateway.request_action(
        tenant_id="tenant-hash",
        agent_id="agent-hash",
        action_name="db:query",
        target_resource="pg://test",
        parameters={"limit": 10},
    )
    receipt = gateway.execute_action(req.request_id)

    assert receipt.receipt_hash.startswith("sw:act:v1:") is False  # raw hex
    assert len(receipt.receipt_hash) == 64

    # Dict roundtrip
    d = receipt.to_dict()
    reconstructed = ActionExecutionReceipt.from_dict(d)
    assert reconstructed.receipt_hash == receipt.receipt_hash
    assert reconstructed.duration_us == receipt.duration_us


def test_pending_approvals_filtering() -> None:
    gateway = ActionGateway()
    gateway.request_action("t1", "a1", "api:webhook_call", "res1", {})
    gateway.request_action("t2", "a2", "api:webhook_call", "res2", {})

    all_pending = gateway.get_pending_approvals()
    assert len(all_pending) == 2

    t1_pending = gateway.get_pending_approvals(tenant_id="t1")
    assert len(t1_pending) == 1
    assert t1_pending[0].tenant_id == "t1"


def test_sub_250us_action_gateway_slo() -> None:
    gateway = ActionGateway()

    # Warmup
    for _ in range(10):
        req = gateway.request_action("tenant-bench", "agent-bench", "db:query", "pg://bench", {})
        gateway.execute_action(req.request_id)

    # Benchmark: 500 complete request -> execute cycles
    iterations = 500
    t0 = time.perf_counter()
    for _ in range(iterations):
        req = gateway.request_action("tenant-bench", "agent-bench", "db:query", "pg://bench", {})
        gateway.execute_action(req.request_id)
    t1 = time.perf_counter()

    avg_us = ((t1 - t0) / iterations) * 1_000_000
    assert avg_us < 250.0, f"Action Gateway roundtrip took {avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_singleton_status_and_reset() -> None:
    st = global_action_gateway.status()
    assert "registered_executors_count" in st
    assert st["registered_executors_count"] >= 4

    global_action_gateway.request_action("t-singleton", "agent-s", "db:query", "pg://s", {})
    assert global_action_gateway.status()["total_requests"] >= 1

    global_action_gateway.reset()
    assert global_action_gateway.status()["total_requests"] == 0
    assert global_action_gateway.status()["circuit_tripped"] is False


def test_action_gateway_rest_api(tmp_path) -> None:
    from starlette.testclient import TestClient
    from swarmwatch.storage import SQLiteStorageAdapter
    from swarmwatch.server import create_app

    global_action_gateway.reset()

    db_path = str(tmp_path / "gateway_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Status endpoint
    status_res = client.get("/api/v1/gateway/status")
    assert status_res.status_code == 200
    status_data = status_res.json()
    assert "registered_executors_count" in status_data
    assert status_data["circuit_tripped"] is False

    # 2. Request auto-approved action
    req_payload = {
        "tenant_id": "tenant-rest",
        "agent_id": "agent-rest",
        "action_name": "db:query",
        "target_resource": "pg://warehouse/analytics",
        "parameters": {"sql": "SELECT COUNT(*) FROM events"},
        "estimated_cost_usd": 0.05,
    }
    req_res = client.post("/api/v1/gateway/request", json=req_payload)
    assert req_res.status_code == 200
    req_data = req_res.json()
    assert req_data["status"] == "APPROVED"
    req_id = req_data["request_id"]

    # Execute auto-approved action
    exec_res = client.post("/api/v1/gateway/execute", json={"request_id": req_id})
    assert exec_res.status_code == 200
    receipt_data = exec_res.json()
    assert receipt_data["status"] == "SUCCESS"
    receipt_id = receipt_data["receipt_id"]

    # 3. High-risk action requiring quorum
    high_req_payload = {
        "tenant_id": "tenant-rest",
        "agent_id": "agent-rest",
        "action_name": "db:drop_table",
        "target_resource": "pg://warehouse/customers",
        "parameters": {"table": "customers"},
        "reversibility_class": "IRREVERSIBLE",
        "estimated_cost_usd": 150.0,
    }
    high_req_res = client.post("/api/v1/gateway/request", json=high_req_payload)
    assert high_req_res.status_code == 200
    high_req_data = high_req_res.json()
    assert high_req_data["status"] == "PENDING_APPROVAL"
    assert high_req_data["required_quorum_count"] >= 2
    high_req_id = high_req_data["request_id"]

    # Premature execute returns 428 Precondition Required
    premature_res = client.post("/api/v1/gateway/execute", json={"request_id": high_req_id})
    assert premature_res.status_code == 428
    assert "requires 2 approvals" in premature_res.json()["detail"]

    # Check pending list
    pending_res = client.get("/api/v1/gateway/pending?tenant_id=tenant-rest")
    assert pending_res.status_code == 200
    pending_list = pending_res.json()
    assert any(p["request_id"] == high_req_id for p in pending_list)

    # Approve by operator 1
    appr1_res = client.post("/api/v1/gateway/approve", json={
        "request_id": high_req_id,
        "approver_principal": "secops-lead",
        "decision": "APPROVED",
        "comment": "Scheduled maintenance window verified",
    })
    assert appr1_res.status_code == 200
    assert appr1_res.json()["status"] == "PENDING_APPROVAL"

    # Approve by operator 2
    appr2_res = client.post("/api/v1/gateway/approve", json={
        "request_id": high_req_id,
        "approver_principal": "compliance-officer",
        "decision": "APPROVED",
        "comment": "Data retention signoff acquired",
    })
    assert appr2_res.status_code == 200
    assert appr2_res.json()["status"] == "APPROVED"

    # Execute after quorum acquired
    exec_high_res = client.post("/api/v1/gateway/execute", json={"request_id": high_req_id})
    assert exec_high_res.status_code == 200
    high_receipt = exec_high_res.json()
    assert high_receipt["status"] == "SUCCESS"

    # Receipts list and lookup
    receipts_res = client.get("/api/v1/gateway/receipts?tenant_id=tenant-rest")
    assert receipts_res.status_code == 200
    assert len(receipts_res.json()) >= 2

    lookup_res = client.get(f"/api/v1/gateway/receipts/{receipt_id}")
    assert lookup_res.status_code == 200
    assert lookup_res.json()["receipt_id"] == receipt_id

    # Reset clean
    global_action_gateway.reset()

