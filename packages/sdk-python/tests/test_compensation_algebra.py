# -*- coding: utf-8 -*-
"""
Tests for Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66 / ADR 0077).
Verifies action classification, lattice join composition, automated reversible rollbacks,
semantic forward compensations, fail-closed irreversible halting, and human escalation workflows.
"""

from __future__ import annotations

import time
import pytest

from swarmwatch.compensation_algebra import (
    ReversibilityClass,
    ActionContract,
    ActionExecution,
    CompositeClassification,
    CompensationStep,
    CompensationPlan,
    CompensationResult,
    HumanEscalationReceipt,
    EscalationResolution,
    CompensationStatus,
    CompensationError,
    IrreversibleActionError,
    UnregisteredActionError,
    EscalationPendingError,
    CompensationEngine,
    global_compensation_engine,
)


def test_action_classification_and_lattice_ordering() -> None:
    # Strict total order: REVERSIBLE (0) < COMPENSATABLE (1) < IRREVERSIBLE (2)
    assert ReversibilityClass.REVERSIBLE < ReversibilityClass.COMPENSATABLE
    assert ReversibilityClass.COMPENSATABLE < ReversibilityClass.IRREVERSIBLE
    assert ReversibilityClass.REVERSIBLE < ReversibilityClass.IRREVERSIBLE

    # Lattice Join (Least Upper Bound / max)
    assert ReversibilityClass.join([ReversibilityClass.REVERSIBLE]) == ReversibilityClass.REVERSIBLE
    assert ReversibilityClass.join([ReversibilityClass.REVERSIBLE, ReversibilityClass.COMPENSATABLE]) == ReversibilityClass.COMPENSATABLE
    assert ReversibilityClass.join([ReversibilityClass.REVERSIBLE, ReversibilityClass.IRREVERSIBLE]) == ReversibilityClass.IRREVERSIBLE
    assert ReversibilityClass.join([ReversibilityClass.COMPENSATABLE, ReversibilityClass.IRREVERSIBLE]) == ReversibilityClass.IRREVERSIBLE


def test_unregistered_action_defaults_to_irreversible_fail_closed() -> None:
    engine = CompensationEngine()
    # Unknown action type defaults to IRREVERSIBLE
    action = ActionExecution(
        action_id="act-001",
        action_type="unknown.external.mutation",
        tenant_id="tenant-1",
        principal="agent-0",
    )
    classification = engine.classify([action])
    assert classification.reversibility_class == ReversibilityClass.IRREVERSIBLE
    assert classification.is_fully_reversible is False
    assert classification.is_compensatable is False
    assert classification.has_irreversible_steps is True
    assert "act-001" in classification.irreversible_action_ids


def test_composite_classification_all_reversible() -> None:
    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="memory.set_scratch_key",
        reversibility_class=ReversibilityClass.REVERSIBLE,
        description="Sets an in-memory scratch key",
    ))
    engine.register_contract(ActionContract(
        action_type="db.start_transaction",
        reversibility_class=ReversibilityClass.REVERSIBLE,
        description="Starts a local database transaction",
    ))

    actions = [
        ActionExecution(action_id="act-1", action_type="memory.set_scratch_key", tenant_id="t1", principal="p1"),
        ActionExecution(action_id="act-2", action_type="db.start_transaction", tenant_id="t1", principal="p1"),
    ]

    res = engine.classify(actions)
    assert res.reversibility_class == ReversibilityClass.REVERSIBLE
    assert res.is_fully_reversible is True
    assert res.is_compensatable is True
    assert res.has_irreversible_steps is False
    assert len(res.irreversible_action_ids) == 0


def test_composite_classification_with_compensatable() -> None:
    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="memory.set_scratch_key",
        reversibility_class=ReversibilityClass.REVERSIBLE,
    ))
    engine.register_contract(ActionContract(
        action_type="payment.charge_card",
        reversibility_class=ReversibilityClass.COMPENSATABLE,
    ))

    actions = [
        ActionExecution(action_id="act-1", action_type="memory.set_scratch_key", tenant_id="t1", principal="p1"),
        ActionExecution(action_id="act-2", action_type="payment.charge_card", tenant_id="t1", principal="p1"),
    ]

    res = engine.classify(actions)
    assert res.reversibility_class == ReversibilityClass.COMPENSATABLE
    assert res.is_fully_reversible is False
    assert res.is_compensatable is True
    assert res.has_irreversible_steps is False


def test_composite_classification_with_irreversible() -> None:
    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="memory.set_scratch_key",
        reversibility_class=ReversibilityClass.REVERSIBLE,
    ))
    engine.register_contract(ActionContract(
        action_type="payment.charge_card",
        reversibility_class=ReversibilityClass.COMPENSATABLE,
    ))
    engine.register_contract(ActionContract(
        action_type="webhook.dispatch_public",
        reversibility_class=ReversibilityClass.IRREVERSIBLE,
    ))

    actions = [
        ActionExecution(action_id="act-1", action_type="memory.set_scratch_key", tenant_id="t1", principal="p1"),
        ActionExecution(action_id="act-2", action_type="payment.charge_card", tenant_id="t1", principal="p1"),
        ActionExecution(action_id="act-3", action_type="webhook.dispatch_public", tenant_id="t1", principal="p1"),
    ]

    res = engine.classify(actions)
    assert res.reversibility_class == ReversibilityClass.IRREVERSIBLE
    assert res.is_fully_reversible is False
    assert res.is_compensatable is False
    assert res.has_irreversible_steps is True
    assert res.irreversible_action_ids == ["act-3"]


def test_compensation_plan_generation_reverse_order() -> None:
    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="step.1",
        reversibility_class=ReversibilityClass.REVERSIBLE,
    ))
    engine.register_contract(ActionContract(
        action_type="step.2",
        reversibility_class=ReversibilityClass.COMPENSATABLE,
    ))
    engine.register_contract(ActionContract(
        action_type="step.3",
        reversibility_class=ReversibilityClass.REVERSIBLE,
    ))

    actions = [
        ActionExecution(action_id="a1", action_type="step.1", tenant_id="t1", principal="p1"),
        ActionExecution(action_id="a2", action_type="step.2", tenant_id="t1", principal="p1"),
        ActionExecution(action_id="a3", action_type="step.3", tenant_id="t1", principal="p1"),
    ]

    plan = engine.build_compensation_plan(actions)
    assert isinstance(plan, CompensationPlan)
    assert plan.composite_class == ReversibilityClass.COMPENSATABLE
    assert len(plan.steps) == 3
    # Compensation execution must proceed in strictly REVERSE order: a3 -> a2 -> a1
    assert [s.target_action_id for s in plan.steps] == ["a3", "a2", "a1"]


def test_execute_compensation_reversible_workflow() -> None:
    state = {"val": 100}

    def inverse_add(action: ActionExecution) -> bool:
        state["val"] -= action.payload.get("amount", 0)
        return True

    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="math.add",
        reversibility_class=ReversibilityClass.REVERSIBLE,
        inverse_handler=inverse_add,
    ))

    actions = [
        ActionExecution(action_id="a1", action_type="math.add", tenant_id="t1", principal="p1", payload={"amount": 10}),
        ActionExecution(action_id="a2", action_type="math.add", tenant_id="t1", principal="p1", payload={"amount": 25}),
    ]

    # Pre-compensation state with additions
    state["val"] += 10 + 25  # val = 135

    plan = engine.build_compensation_plan(actions)
    result = engine.execute_compensation(plan)

    assert result.success is True
    assert result.steps_executed == 2
    assert result.steps_failed == 0
    # Verified state returned to 100
    assert state["val"] == 100


def test_execute_compensation_compensatable_workflow() -> None:
    ledger = []

    def compensate_charge(action: ActionExecution) -> bool:
        ledger.append(f"REFUND:{action.payload.get('charge_id')}")
        return True

    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="billing.charge",
        reversibility_class=ReversibilityClass.COMPENSATABLE,
        compensation_handler=compensate_charge,
    ))

    actions = [
        ActionExecution(action_id="c1", action_type="billing.charge", tenant_id="t1", principal="p1", payload={"charge_id": "ch_999"}),
    ]

    plan = engine.build_compensation_plan(actions)
    result = engine.execute_compensation(plan)

    assert result.success is True
    assert result.steps_executed == 1
    assert ledger == ["REFUND:ch_999"]


def test_execute_compensation_irreversible_halts_fail_closed() -> None:
    engine = CompensationEngine()
    engine.register_contract(ActionContract(
        action_type="email.send_customer_email",
        reversibility_class=ReversibilityClass.IRREVERSIBLE,
    ))

    actions = [
        ActionExecution(action_id="e1", action_type="email.send_customer_email", tenant_id="t1", principal="p1"),
    ]

    plan = engine.build_compensation_plan(actions)
    assert plan.composite_class == ReversibilityClass.IRREVERSIBLE

    # Attempting automated compensation on an unapproved irreversible plan must raise IrreversibleActionError
    with pytest.raises(IrreversibleActionError, match="Automated rollback prohibited for irreversible action"):
        engine.execute_compensation(plan)


def test_human_escalation_lifecycle() -> None:
    engine = CompensationEngine()
    action = ActionExecution(
        action_id="irr-01",
        action_type="sns.broadcast_public_alert",
        tenant_id="tenant-alpha",
        principal="agent-root",
        payload={"topic": "market-outage", "msg": "Critical alert"},
    )

    # 1. Escalate irreversible action
    receipt = engine.escalate(
        action=action,
        reason="Action produced irreversible external SNS broadcast; manual remediation required",
    )
    assert isinstance(receipt, HumanEscalationReceipt)
    assert receipt.action_id == "irr-01"
    assert receipt.status == "PENDING"
    assert len(receipt.escalation_hash) == 64

    # 2. Cannot execute unapproved plan containing escalated action
    plan = engine.build_compensation_plan([action])
    with pytest.raises(IrreversibleActionError):
        engine.execute_compensation(plan)

    # 3. Human Operator Authorizes Manual Remediation
    resolution = engine.authorize_escalation(
        escalation_id=receipt.escalation_id,
        authorizer_principal="secops-lead",
        decision="MANUAL_REMEDIATED",
        remediation_notes="Verified corrective broadcast dispatched manually via console",
    )
    assert isinstance(resolution, EscalationResolution)
    assert resolution.decision == "MANUAL_REMEDIATED"
    assert resolution.authorizer_principal == "secops-lead"

    # 4. Plan can now complete with human authorization bypass
    result = engine.execute_compensation(plan, allow_authorized_escalations=True)
    assert result.success is True
    assert result.escalations_resolved == 1


def test_sub_250us_classification_slo() -> None:
    engine = CompensationEngine()
    for i in range(10):
        engine.register_contract(ActionContract(
            action_type=f"test.action.{i}",
            reversibility_class=ReversibilityClass.REVERSIBLE if i % 2 == 0 else ReversibilityClass.COMPENSATABLE,
        ))

    actions = [
        ActionExecution(action_id=f"a_{i}", action_type=f"test.action.{i}", tenant_id="t", principal="p")
        for i in range(10)
    ]

    # Warmup
    for _ in range(10):
        engine.classify(actions)

    # Benchmark: best of 3 runs of 500 iterations
    best_avg_us = float("inf")
    iterations = 500

    for _ in range(3):
        t0 = time.perf_counter()
        for _ in range(iterations):
            engine.classify(actions)
        t1 = time.perf_counter()
        avg_us = ((t1 - t0) / iterations) * 1_000_000
        if avg_us < best_avg_us:
            best_avg_us = avg_us

    # Assert under the 250 microsecond SLO (typically ~10-25 µs)
    assert best_avg_us < 250.0, f"Action classification took {best_avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_instance_and_status() -> None:
    status = global_compensation_engine.status()
    assert isinstance(status, CompensationStatus)
    assert status.total_registered_contracts >= 0
    assert status.total_plans_generated >= 0
    d = status.to_dict()
    assert "total_registered_contracts" in d
    assert "total_escalations_pending" in d


def test_compensation_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app

    # Register contracts on global engine for the test
    global_compensation_engine.register_contract(ActionContract(
        action_type="rest.cache.write",
        reversibility_class=ReversibilityClass.REVERSIBLE,
        inverse_handler=lambda a: True,
    ))
    global_compensation_engine.register_contract(ActionContract(
        action_type="rest.balance.debit",
        reversibility_class=ReversibilityClass.COMPENSATABLE,
        compensation_handler=lambda a: True,
    ))

    app = create_app()
    client = TestClient(app)

    actions = [
        {"action_id": "r1", "action_type": "rest.cache.write", "tenant_id": "t1", "principal": "p1", "payload": {}},
        {"action_id": "r2", "action_type": "rest.balance.debit", "tenant_id": "t1", "principal": "p1", "payload": {"amount": 50}},
    ]

    # 1. Classify sequence
    res_cls = client.post("/api/v1/compensation/classify", json={"actions": actions})
    assert res_cls.status_code == 200, res_cls.text
    d_cls = res_cls.json()
    assert d_cls["reversibility_class"] == "COMPENSATABLE"
    assert d_cls["total_actions"] == 2
    assert d_cls["reversible_count"] == 1
    assert d_cls["compensatable_count"] == 1

    # 2. Plan sequence
    res_plan = client.post("/api/v1/compensation/plan", json={"actions": actions})
    assert res_plan.status_code == 200, res_plan.text
    d_plan = res_plan.json()
    assert len(d_plan["steps"]) == 2
    # Verify strict reverse execution order: r2 first, then r1
    assert d_plan["steps"][0]["target_action_id"] == "r2"
    assert d_plan["steps"][1]["target_action_id"] == "r1"

    # 3. Execute plan
    res_exec = client.post("/api/v1/compensation/execute", json={"plan": d_plan, "actions": actions})
    assert res_exec.status_code == 200, res_exec.text
    d_exec = res_exec.json()
    assert d_exec["success"] is True
    assert d_exec["steps_executed"] == 2

    # 4. Escalate an irreversible action
    res_esc = client.post(
        "/api/v1/compensation/escalate",
        json={
            "action_id": "r3-irr",
            "action_type": "rest.wire.transfer",
            "tenant_id": "t1",
            "principal": "p1",
            "reason": "Wire transfer executed external irrevocably",
        }
    )
    assert res_esc.status_code == 200, res_esc.text
    d_esc = res_esc.json()
    assert d_esc["status"] == "PENDING"
    assert len(d_esc["escalation_hash"]) == 64
    escalation_id = d_esc["escalation_id"]

    # 5. Authorize escalation
    res_auth = client.post(
        "/api/v1/compensation/authorize-escalation",
        json={
            "escalation_id": escalation_id,
            "authorizer_principal": "compliance-officer",
            "decision": "APPROVED_BYPASS",
            "remediation_notes": "Authorized wire bypass following KYC verification",
        }
    )
    assert res_auth.status_code == 200, res_auth.text
    d_auth = res_auth.json()
    assert d_auth["decision"] == "APPROVED_BYPASS"
    assert d_auth["authorizer_principal"] == "compliance-officer"

    # 6. Status check
    res_st = client.get("/api/v1/compensation/status")
    assert res_st.status_code == 200, res_st.text
    d_st = res_st.json()
    assert d_st["total_registered_contracts"] >= 2
    assert d_st["total_plans_generated"] >= 1
    assert d_st["total_compensations_executed"] >= 1
    assert d_st["total_escalations_resolved"] >= 1
