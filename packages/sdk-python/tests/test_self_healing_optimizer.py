# -*- coding: utf-8 -*-
"""
Unit and integration test suite for SwarmWatch Autonomous Counterfactual
Swarm Optimization & Self-Healing Policy Controller (Seam 31 / ADR 0042).
"""

import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.schema import SwarmSpan
from swarmwatch.policy import PolicyEngine, GuardrailPolicy, RuleConfig
from swarmwatch.selfhealing import (
    AnomalyType,
    SelfHealingActionType,
    CounterfactualEvaluation,
    SelfHealingAction,
    SelfHealingPolicyConfig,
    SwarmSelfHealingController,
    global_self_healing_controller,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.cli import main as cli_main


@pytest.fixture
def clean_self_healing():
    """Resets global_self_healing_controller state before and after each test."""
    ctrl = global_self_healing_controller
    with ctrl._lock:
        ctrl.config = SelfHealingPolicyConfig()
        ctrl._consecutive_tool_failures.clear()
        ctrl._recent_entropy_history.clear()
        ctrl._agent_token_totals.clear()
        ctrl._agent_turn_counts.clear()
        ctrl._active_model_routes.clear()
        ctrl._active_backoffs.clear()
        ctrl._actions.clear()
        ctrl._cumulative_tokens_saved = 0
        ctrl._cumulative_cost_saved = 0.0
        ctrl.policy_engine = PolicyEngine()
    yield ctrl
    with ctrl._lock:
        ctrl.config = SelfHealingPolicyConfig()
        ctrl._consecutive_tool_failures.clear()
        ctrl._recent_entropy_history.clear()
        ctrl._agent_token_totals.clear()
        ctrl._agent_turn_counts.clear()
        ctrl._active_model_routes.clear()
        ctrl._active_backoffs.clear()
        ctrl._actions.clear()
        ctrl._cumulative_tokens_saved = 0
        ctrl._cumulative_cost_saved = 0.0


def test_tool_failure_anomaly_detection_and_blacklisting(clean_self_healing):
    ctrl = clean_self_healing
    engine = PolicyEngine()
    ctrl.set_policy_engine(engine)

    span1 = SwarmSpan(
        trace_id="trace-01",
        span_id="span-01",
        swarm_id="swarm-test-01",
        source_agent="coder_agent",
        target_agent="query_database",
        span_type="tool",
        status="error",
        output_payload={"error": "DatabaseConnectionError: timeout"},
    )
    span2 = SwarmSpan(
        trace_id="trace-01",
        span_id="span-02",
        swarm_id="swarm-test-01",
        source_agent="coder_agent",
        target_agent="query_database",
        span_type="tool",
        status="error",
        output_payload={"error": "DatabaseConnectionError: timeout"},
    )
    span3 = SwarmSpan(
        trace_id="trace-01",
        span_id="span-03",
        swarm_id="swarm-test-01",
        source_agent="coder_agent",
        target_agent="query_database",
        span_type="tool",
        status="error",
        output_payload={"error": "DatabaseConnectionError: timeout"},
    )

    # First two failures do not trigger remediation yet (threshold = 3)
    a1 = ctrl.observe_span(span1)
    assert a1 is None
    a2 = ctrl.observe_span(span2)
    assert a2 is None

    # Third failure triggers tool blacklisting
    a3 = ctrl.observe_span(span3)
    assert a3 is not None
    assert a3.action_type == SelfHealingActionType.BLACKLIST_TOOL
    assert a3.anomaly_detected == AnomalyType.REPEATED_TOOL_FAILURES
    assert a3.target == "query_database"
    assert a3.counterfactual.tokens_saved == 2500
    assert a3.counterfactual.cost_saved > 0

    # Verify tool is now registered in PolicyEngine prohibited_tools
    effective = engine.get_effective_config("coder_agent")
    assert "query_database" in effective.prohibited_tools

    # A subsequent successful tool execution resets the counter for that pair
    ok_span = SwarmSpan(
        trace_id="trace-01",
        span_id="span-04",
        swarm_id="swarm-test-01",
        source_agent="coder_agent",
        target_agent="safe_tool",
        span_type="tool",
        status="ok",
    )
    ctrl.observe_span(ok_span)
    assert ctrl._consecutive_tool_failures.get(("coder_agent", "safe_tool")) == 0


def test_entropy_volatility_model_fallback_routing(clean_self_healing):
    ctrl = clean_self_healing

    # Low entropy span indicating repetitive degeneration
    degenerate_span = SwarmSpan(
        trace_id="trace-02",
        span_id="span-05",
        swarm_id="swarm-test-02",
        source_agent="summarizer_agent",
        span_type="turn",
        status="ok",
        entropy_score=0.18,
        input_payload={"model": "gpt-4o"},
    )

    action = ctrl.observe_span(degenerate_span)
    assert action is not None
    assert action.action_type == SelfHealingActionType.ROUTE_MODEL_FALLBACK
    assert action.anomaly_detected == AnomalyType.ENTROPY_VOLATILITY
    assert action.target == "gpt-4o-mini"
    assert action.counterfactual.tokens_saved == 4000
    assert action.counterfactual.cost_saved == 0.12

    # Resolved model route should now return fallback model
    routed = ctrl.get_model_route("summarizer_agent", requested_model="gpt-4o")
    assert routed == "gpt-4o-mini"


def test_quota_clamping_on_budget_acceleration(clean_self_healing):
    ctrl = clean_self_healing
    engine = PolicyEngine()
    engine.policy.agents["heavy_agent"] = RuleConfig(token_budget=10000, max_turns=20)
    ctrl.set_policy_engine(engine)

    # Agent consumes 8500 tokens in its first turn (85% of budget in < 50% turns)
    accel_span = SwarmSpan(
        trace_id="trace-03",
        span_id="span-06",
        swarm_id="swarm-test-03",
        source_agent="heavy_agent",
        span_type="turn",
        token_count=8500,
        status="ok",
    )

    action = ctrl.observe_span(accel_span)
    assert action is not None
    assert action.action_type == SelfHealingActionType.CLAMP_QUOTA
    assert action.anomaly_detected == AnomalyType.BUDGET_RUNAWAY
    assert action.counterfactual.tokens_saved == 1500

    # Policy engine max_turns was clamped from 20 down to 15 (75% or turns+3)
    effective = engine.get_effective_config("heavy_agent")
    assert effective.max_turns < 20


def test_action_rollback_and_state_restoration(clean_self_healing):
    ctrl = clean_self_healing
    engine = PolicyEngine()
    engine.policy.agents["revert_agent"] = RuleConfig(max_turns=30, prohibited_tools=["old_bad_tool"])
    ctrl.set_policy_engine(engine)

    # 1. Enact programmatic tool blacklist
    action = ctrl.remediate(
        swarm_id="swarm-revert",
        agent_name="revert_agent",
        action_type=SelfHealingActionType.BLACKLIST_TOOL,
        target="transient_tool",
    )
    assert action.status == "active"
    assert "transient_tool" in engine.policy.agents["revert_agent"].prohibited_tools

    # 2. Rollback the blacklist action
    reverted = ctrl.rollback_action(action.id)
    assert reverted is True
    assert action.status == "rolled_back"
    assert "transient_tool" not in engine.policy.agents["revert_agent"].prohibited_tools
    assert "old_bad_tool" in engine.policy.agents["revert_agent"].prohibited_tools

    # 3. Model fallback and rollback
    ctrl.remediate(
        swarm_id="swarm-revert",
        agent_name="revert_agent",
        action_type=SelfHealingActionType.ROUTE_MODEL_FALLBACK,
        target="gpt-4o-mini",
    )
    assert ctrl.get_model_route("revert_agent") == "gpt-4o-mini"
    ctrl.rollback_all(swarm_id="swarm-revert")
    # After rollback_all, model route should be reverted back
    assert ctrl.get_model_route("revert_agent", requested_model="gpt-4o") == "gpt-4o"


def test_self_healing_fastapi_endpoints(clean_self_healing, tmp_path):
    db_file = str(tmp_path / "self_healing_test.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. GET /api/v1/self-healing/status
    res = client.get("/api/v1/self-healing/status")
    assert res.status_code == 200
    st = res.json()
    assert st["enabled"] is True
    assert "active_actions_count" in st
    assert "cumulative_tokens_saved" in st

    # 2. POST /api/v1/self-healing/remediate
    rem_payload = {
        "swarm_id": "swarm-fastapi-heal",
        "agent_name": "api_agent",
        "action_type": SelfHealingActionType.BLACKLIST_TOOL,
        "target": "unsafe_eval",
        "reason": "Test endpoint blacklist",
    }
    rem_res = client.post("/api/v1/self-healing/remediate", json=rem_payload)
    assert rem_res.status_code == 200
    action_data = rem_res.json()
    action_id = action_data["id"]
    assert action_data["target"] == "unsafe_eval"

    # 3. GET /api/v1/self-healing/actions
    actions_res = client.get("/api/v1/self-healing/actions")
    assert actions_res.status_code == 200
    all_actions = actions_res.json()["actions"]
    assert len(all_actions) >= 1
    assert any(a["id"] == action_id for a in all_actions)

    # 4. POST /api/v1/self-healing/rollback
    rb_res = client.post(f"/api/v1/self-healing/rollback?action_id={action_id}")
    assert rb_res.status_code == 200
    assert rb_res.json()["status"] == "rolled_back"

    # 5. POST /api/v1/self-healing/toggle
    tog_res = client.post("/api/v1/self-healing/toggle?enabled=false")
    assert tog_res.status_code == 200
    assert tog_res.json()["enabled"] is False

    st_after = client.get("/api/v1/self-healing/status").json()
    assert st_after["enabled"] is False

    # Re-enable
    client.post("/api/v1/self-healing/toggle?enabled=true")


def test_self_healing_cli_commands(clean_self_healing, capsys):
    # Test CLI heal status (json format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["heal", "status", "--format", "json"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "active_actions_count" in out

    # Test CLI heal status (terminal format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["heal", "status"])
    assert exc.value.code == 0
    term_out = capsys.readouterr().out
    assert "SWARMWATCH AUTONOMOUS SELF-HEALING" in term_out

    # Test CLI heal trigger
    with pytest.raises(SystemExit) as exc:
        cli_main([
            "heal", "trigger",
            "--agent", "cli_agent",
            "--strategy", "blacklist_tool",
            "--target", "shell_exec",
            "--format", "json"
        ])
    assert exc.value.code == 0
    trigger_out = capsys.readouterr().out
    assert "shell_exec" in trigger_out

    # Test CLI heal actions
    with pytest.raises(SystemExit) as exc:
        cli_main(["heal", "actions", "--format", "json"])
    assert exc.value.code == 0
    actions_out = capsys.readouterr().out
    assert "actions" in actions_out

    # Test CLI heal rollback
    with pytest.raises(SystemExit) as exc:
        cli_main(["heal", "rollback", "--format", "json"])
    assert exc.value.code == 0
    rb_out = capsys.readouterr().out
    assert "rolled_back_all" in rb_out
