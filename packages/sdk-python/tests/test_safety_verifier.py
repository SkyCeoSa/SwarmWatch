# -*- coding: utf-8 -*-
"""
Unit and integration tests for Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55 / ADR 0066).
Verifies BOUNDED_SAFE, UNBOUNDED_PROVEN, INDETERMINATE, VIOLATED, and VACUOUS verdicts,
state space coverage metrics, and independent trace replay validation.
"""

import time
import pytest
from fastapi.testclient import TestClient

from swarmwatch.model_checker import AgentTransitionModel
from swarmwatch.safety_verifier import (
    BoundedVerdict,
    VerificationInvariant,
    VerificationProperty,
    VerificationLimits,
    VerificationReport,
    SafetyVerifier,
    global_safety_verifier,
)
from swarmwatch.server import create_app


@pytest.fixture(autouse=True)
def reset_verifier():
    global_safety_verifier.reset()
    yield
    global_safety_verifier.reset()


def test_vacuous_verification():
    verifier = SafetyVerifier()
    empty_model = {
        "initial_agent": "Planner",
        "final_agents": [],
        "transitions": [],
    }
    report = verifier.verify(empty_model, model_id="empty_test")
    assert report.verdict == BoundedVerdict.VACUOUS.value
    assert report.total_states_explored == 0
    assert report.frontier_states_remaining == 0
    assert report.state_coverage_ratio == 1.0
    assert "vacuously safe" in report.diagnostic_reason


def test_unbounded_proven_finite_closed_pipeline():
    verifier = SafetyVerifier()
    model = {
        "initial_agent": "Planner",
        "final_agents": ["Publisher"],
        "transitions": [
            {"from_agent": "Planner", "to_agent": "Coder", "action_type": "DELEGATE", "token_cost": 200},
            {"from_agent": "Coder", "to_agent": "Publisher", "action_type": "SUBMIT", "token_cost": 150},
        ],
        "max_token_budget": 1000,
    }
    report = verifier.verify(model, limits=VerificationLimits(max_depth_k=10), model_id="pipeline_model")
    assert report.verdict == BoundedVerdict.UNBOUNDED_PROVEN.value
    assert report.depth_reached == 2
    assert report.total_states_explored == 3
    assert report.frontier_states_remaining == 0
    assert report.state_coverage_ratio == 1.0
    assert report.violated_property is None
    assert len(report.counterexample_trace) == 0


def test_bounded_safe_with_unclosed_frontier():
    verifier = SafetyVerifier()
    # Cyclic loop between A and B that could continue beyond depth k=3
    # Without exceeding max_repeat=3 yet
    model = {
        "initial_agent": "AgentA",
        "final_agents": ["TargetEnd"],
        "transitions": [
            {"from_agent": "AgentA", "to_agent": "AgentB", "action_type": "STEP_AB", "token_cost": 10},
            {"from_agent": "AgentB", "to_agent": "AgentA", "action_type": "STEP_BA", "token_cost": 10},
        ],
        "max_token_budget": 50000,
    }
    # Bound k=2: AgentA(0) -> AgentB(1) -> AgentA(2). At depth 2, AgentA has an outgoing transition to AgentB
    # which is not expanded because depth >= 2.
    limits = VerificationLimits(max_depth_k=2, max_states=100)
    props = [
        VerificationProperty("p_deadlock", VerificationInvariant.NO_DEADLOCK),
        VerificationProperty("p_budget", VerificationInvariant.BOUNDED_BUDGET, {"max_tokens": 50000}),
    ]
    report = verifier.verify(model, properties=props, limits=limits, model_id="unclosed_model")
    assert report.verdict == BoundedVerdict.BOUNDED_SAFE.value
    assert report.bound_k == 2
    assert report.frontier_states_remaining > 0
    assert report.state_coverage_ratio < 1.0
    assert report.state_coverage_ratio > 0.0
    assert "remains unclosed beyond k" in report.diagnostic_reason


def test_indeterminate_on_max_states_limit():
    verifier = SafetyVerifier()
    # Binary branching tree that creates many states
    transitions = []
    for i in range(10):
        transitions.append({"from_agent": f"N{i}", "to_agent": f"N{i+1}A", "action_type": "BRANCH_A", "token_cost": 10})
        transitions.append({"from_agent": f"N{i}", "to_agent": f"N{i+1}B", "action_type": "BRANCH_B", "token_cost": 10})
        transitions.append({"from_agent": f"N{i+1}A", "to_agent": f"N{i+1}", "action_type": "JOIN_A", "token_cost": 10})
        transitions.append({"from_agent": f"N{i+1}B", "to_agent": f"N{i+1}", "action_type": "JOIN_B", "token_cost": 10})

    model = {
        "initial_agent": "N0",
        "final_agents": ["N10"],
        "transitions": transitions,
    }
    limits = VerificationLimits(max_depth_k=20, max_states=5)
    report = verifier.verify(model, limits=limits, model_id="branch_model")
    assert report.verdict == BoundedVerdict.INDETERMINATE.value
    assert report.total_states_explored >= 5
    assert "state cap of 5 reached" in report.diagnostic_reason


def test_deadlock_violation_and_counterexample():
    verifier = SafetyVerifier()
    model = {
        "initial_agent": "Coordinator",
        "final_agents": ["Done"],
        "transitions": [
            {"from_agent": "Coordinator", "to_agent": "Worker", "action_type": "DELEGATE", "token_cost": 100},
            {"from_agent": "Worker", "to_agent": "StuckAgent", "action_type": "HANDOFF", "token_cost": 100},
            # StuckAgent has no transitions and is not in final_agents!
        ],
    }
    report = verifier.verify(model, limits=VerificationLimits(max_depth_k=5), model_id="deadlock_model")
    assert report.verdict == BoundedVerdict.VIOLATED.value
    assert report.violated_property == "prop_no_deadlock"
    assert len(report.counterexample_trace) == 2
    assert report.counterexample_trace[0]["from_agent"] == "Coordinator"
    assert report.counterexample_trace[0]["to_agent"] == "Worker"
    assert report.counterexample_trace[1]["from_agent"] == "Worker"
    assert report.counterexample_trace[1]["to_agent"] == "StuckAgent"
    assert "Conversational deadlock" in report.diagnostic_reason


def test_verify_trace_soundness_and_fake_rejection():
    verifier = SafetyVerifier()
    model = {
        "initial_agent": "Coordinator",
        "final_agents": ["Done"],
        "transitions": [
            {"from_agent": "Coordinator", "to_agent": "Worker", "action_type": "DELEGATE", "token_cost": 100},
            {"from_agent": "Worker", "to_agent": "StuckAgent", "action_type": "HANDOFF", "token_cost": 100},
        ],
    }
    valid_trace = [
        {
            "step_index": 1,
            "from_agent": "Coordinator",
            "to_agent": "Worker",
            "action_type": "DELEGATE",
            "token_cost": 100,
            "cumulative_tokens": 100,
            "state_flags": [],
        },
        {
            "step_index": 2,
            "from_agent": "Worker",
            "to_agent": "StuckAgent",
            "action_type": "HANDOFF",
            "token_cost": 100,
            "cumulative_tokens": 200,
            "state_flags": [],
        },
    ]

    # 1. Authentic trace passes validation
    is_sound, diagnostic = verifier.verify_trace(model, property="NO_DEADLOCK", trace=valid_trace)
    assert is_sound is True
    assert diagnostic is None

    # 2. Tampered token count is caught and rejected
    tampered_tokens_trace = [dict(s) for s in valid_trace]
    tampered_tokens_trace[1]["cumulative_tokens"] = 999
    is_sound, diagnostic = verifier.verify_trace(model, property="NO_DEADLOCK", trace=tampered_tokens_trace)
    assert is_sound is False
    assert "cumulative tokens" in diagnostic

    # 3. Fabricated non-existent transition is caught and rejected
    fabricated_trace = [dict(s) for s in valid_trace]
    fabricated_trace[0]["action_type"] = "NON_EXISTENT_ACTION"
    is_sound, diagnostic = verifier.verify_trace(model, property="NO_DEADLOCK", trace=fabricated_trace)
    assert is_sound is False
    assert "does not exist in model" in diagnostic

    # 4. Trace that ends in a valid final agent cannot be claimed as deadlock
    model_safe = dict(model)
    model_safe["final_agents"] = ["StuckAgent"]
    is_sound, diagnostic = verifier.verify_trace(model_safe, property="NO_DEADLOCK", trace=valid_trace)
    assert is_sound is False
    assert "declared final state; not deadlocked" in diagnostic


def test_budget_and_mutex_violations():
    verifier = SafetyVerifier()
    model = {
        "initial_agent": "AgentA",
        "final_agents": ["AgentD"],
        "transitions": [
            {"from_agent": "AgentA", "to_agent": "AgentB", "action_type": "CALL_B", "token_cost": 600},
            {"from_agent": "AgentB", "to_agent": "AgentC", "action_type": "CALL_C", "token_cost": 600},
            {"from_agent": "AgentC", "to_agent": "AgentD", "action_type": "CALL_D", "token_cost": 100},
        ],
        "mutual_exclusion_pairs": [["AgentA", "AgentC"]],
        "max_token_budget": 1000,
    }
    # Test Budget Violation
    report_budget = verifier.verify(
        model,
        properties=[VerificationProperty("p_budget", VerificationInvariant.BOUNDED_BUDGET, {"max_tokens": 1000})],
        limits=VerificationLimits(max_depth_k=5),
    )
    assert report_budget.verdict == BoundedVerdict.VIOLATED.value
    assert report_budget.violated_property == "p_budget"
    assert "exceeded token budget" in report_budget.diagnostic_reason
    is_sound, diag = verifier.verify_trace(
        model,
        property=VerificationProperty("p_budget", VerificationInvariant.BOUNDED_BUDGET, {"max_tokens": 1000}),
        trace=report_budget.counterexample_trace,
    )
    assert is_sound is True

    # Test Mutex Violation
    report_mutex = verifier.verify(
        model,
        properties=[VerificationProperty("p_mutex", VerificationInvariant.MUTUAL_EXCLUSION)],
        limits=VerificationLimits(max_depth_k=5),
    )
    assert report_mutex.verdict == BoundedVerdict.VIOLATED.value
    assert report_mutex.violated_property == "p_mutex"
    assert "Mutual exclusion violated" in report_mutex.diagnostic_reason
    is_sound, diag = verifier.verify_trace(
        model,
        property=VerificationProperty("p_mutex", VerificationInvariant.MUTUAL_EXCLUSION),
        trace=report_mutex.counterexample_trace,
    )
    assert is_sound is True


def test_rest_api_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Verify endpoint
    model_payload = {
        "initial_agent": "Init",
        "final_agents": ["Done"],
        "transitions": [
            {"from_agent": "Init", "to_agent": "Done", "action_type": "COMPLETE", "token_cost": 120}
        ],
    }
    resp = client.post("/api/v1/verifier/verify", json={
        "model": model_payload,
        "limits": {"max_depth_k": 5, "max_states": 100, "timeout_ms": 500.0},
        "model_id": "test_api_model",
        "tenant_id": "tenant_xyz",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["verdict"] == BoundedVerdict.UNBOUNDED_PROVEN.value
    assert data["model_id"] == "test_api_model"
    assert data["state_coverage_ratio"] == 1.0

    # 2. Check-trace endpoint
    trace_payload = [
        {
            "step_index": 1,
            "from_agent": "Init",
            "to_agent": "Done",
            "action_type": "COMPLETE",
            "token_cost": 120,
            "cumulative_tokens": 120,
            "state_flags": [],
        }
    ]
    resp2 = client.post("/api/v1/verifier/check-trace", json={
        "model": model_payload,
        "property": "NO_DEADLOCK",
        "trace": trace_payload,
    })
    assert resp2.status_code == 200
    data2 = resp2.json()
    # It ends at Done, which is in final_agents, so not deadlocked
    assert data2["is_sound"] is False
    assert "declared final state" in data2["diagnostic"]

    # 3. Status endpoint
    resp3 = client.get("/api/v1/verifier/status?tenant_id=tenant_xyz")
    assert resp3.status_code == 200
    data3 = resp3.json()
    assert data3["total_verifications"] == 1
    assert data3["verdicts_breakdown"][BoundedVerdict.UNBOUNDED_PROVEN.value] == 1
