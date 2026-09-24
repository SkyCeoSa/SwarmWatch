import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.cli import main
from swarmwatch.model_checker import (
    VerificationInvariant,
    VerificationVerdict,
    AgentTransition,
    AgentTransitionModel,
    CounterexampleStep,
    VerificationResult,
    BoundedModelChecker,
    global_model_checker,
)
from swarmwatch.server import create_app


def test_safe_acyclic_topology_verification():
    checker = BoundedModelChecker()
    model = AgentTransitionModel(
        initial_agent="agent-planner",
        final_agents={"agent-deployer"},
        transitions=[
            AgentTransition(from_agent="agent-planner", to_agent="agent-coder", action_type="GENERATE_CODE", token_cost=200),
            AgentTransition(from_agent="agent-coder", to_agent="agent-tester", action_type="RUN_TESTS", token_cost=300),
            AgentTransition(from_agent="agent-tester", to_agent="agent-deployer", action_type="DEPLOY", token_cost=150),
        ],
        max_token_budget=10000,
    )

    res = checker.verify(model, bound_k=5, swarm_id="swarm-safe-01")
    assert res.verdict == VerificationVerdict.VERIFIED
    assert res.violated_invariant is None
    assert res.total_states_explored >= 3
    assert res.execution_time_ms < 50.0  # Ultra-fast local execution
    assert len(res.counterexample) == 0
    assert len(res.invariants_checked) == 4


def test_deadlock_detection_with_counterexample():
    checker = BoundedModelChecker()
    # agent-deadlock has no transitions and is NOT in final_agents
    model = AgentTransitionModel(
        initial_agent="agent-router",
        final_agents={"agent-exit"},
        transitions=[
            AgentTransition(from_agent="agent-router", to_agent="agent-deadlock", action_type="ROUTE_UNKNOWN", token_cost=100),
        ],
        max_token_budget=5000,
    )

    res = checker.verify(model, bound_k=5, invariants=[VerificationInvariant.NO_DEADLOCK], swarm_id="swarm-deadlock-01")
    assert res.verdict == VerificationVerdict.VIOLATED
    assert res.violated_invariant == VerificationInvariant.NO_DEADLOCK
    assert len(res.counterexample) >= 1
    assert "deadlock" in res.summary.lower()
    first_step = res.counterexample[0]
    assert first_step["from_agent"] == "agent-router"
    assert first_step["to_agent"] == "agent-deadlock"


def test_unbounded_cycle_detection():
    checker = BoundedModelChecker()
    # Cycle between agent-A and agent-B without progress
    model = AgentTransitionModel(
        initial_agent="agent-A",
        final_agents={"agent-done"},
        transitions=[
            AgentTransition(from_agent="agent-A", to_agent="agent-B", action_type="PING", token_cost=50),
            AgentTransition(from_agent="agent-B", to_agent="agent-A", action_type="PONG", token_cost=50),
        ],
        max_token_budget=100000,
    )

    res = checker.verify(model, bound_k=15, invariants=[VerificationInvariant.NO_UNBOUNDED_CYCLES], swarm_id="swarm-cycle-01")
    assert res.verdict == VerificationVerdict.VIOLATED
    assert res.violated_invariant == VerificationInvariant.NO_UNBOUNDED_CYCLES
    assert len(res.counterexample) >= 3
    assert "unbounded delegation cycle" in res.summary.lower()


def test_budget_exhaustion_detection():
    checker = BoundedModelChecker()
    # Path exceeds max_token_budget of 500
    model = AgentTransitionModel(
        initial_agent="agent-root",
        final_agents={"agent-complete"},
        transitions=[
            AgentTransition(from_agent="agent-root", to_agent="agent-heavy-1", action_type="EXPENSIVE_OP", token_cost=300),
            AgentTransition(from_agent="agent-heavy-1", to_agent="agent-complete", action_type="EXPENSIVE_OP_2", token_cost=400),
        ],
        max_token_budget=500,
    )

    res = checker.verify(model, bound_k=5, invariants=[VerificationInvariant.BOUNDED_BUDGET], swarm_id="swarm-budget-01")
    assert res.verdict == VerificationVerdict.VIOLATED
    assert res.violated_invariant == VerificationInvariant.BOUNDED_BUDGET
    assert "token budget" in res.summary.lower()
    # Total tokens at breach should be 700 > 500
    assert res.counterexample[-1]["cumulative_tokens"] == 700


def test_mutual_exclusion_violation():
    checker = BoundedModelChecker()
    # agent-executor and agent-auditor cannot be in the same path execution
    model = AgentTransitionModel(
        initial_agent="agent-orchestrator",
        final_agents={"agent-end"},
        transitions=[
            AgentTransition(from_agent="agent-orchestrator", to_agent="agent-executor", action_type="EXECUTE", token_cost=100),
            AgentTransition(from_agent="agent-executor", to_agent="agent-auditor", action_type="SELF_AUDIT", token_cost=100),
            AgentTransition(from_agent="agent-auditor", to_agent="agent-end", action_type="FINISH", token_cost=50),
        ],
        mutual_exclusion_pairs=[("agent-executor", "agent-auditor")],
    )

    res = checker.verify(model, bound_k=5, invariants=[VerificationInvariant.MUTUAL_EXCLUSION], swarm_id="swarm-mutex-01")
    assert res.verdict == VerificationVerdict.VIOLATED
    assert res.violated_invariant == VerificationInvariant.MUTUAL_EXCLUSION
    assert "mutual exclusion violated" in res.summary.lower()
    assert len(res.counterexample) == 2


def test_state_flags_and_vacuous_verification():
    checker = BoundedModelChecker()

    # Vacuous on empty transitions
    empty_model = AgentTransitionModel(initial_agent="agent-alone", transitions=[])
    res_vac = checker.verify(empty_model, swarm_id="swarm-vacuous")
    assert res_vac.verdict == VerificationVerdict.VACUOUS
    assert res_vac.total_states_explored == 0

    # Flags gating transitions
    flag_model = AgentTransitionModel(
        initial_agent="agent-auth",
        final_agents={"agent-vault"},
        transitions=[
            AgentTransition(
                from_agent="agent-auth",
                to_agent="agent-privileged",
                action_type="AUTHENTICATE",
                token_cost=50,
                sets_flags=["AUTH_VALID"],
            ),
            AgentTransition(
                from_agent="agent-privileged",
                to_agent="agent-vault",
                action_type="ACCESS_VAULT",
                token_cost=100,
                required_flags=["AUTH_VALID"],
            ),
        ],
    )
    res_flag = checker.verify(flag_model, bound_k=5, swarm_id="swarm-flags")
    assert res_flag.verdict == VerificationVerdict.VERIFIED
    assert res_flag.total_states_explored == 3


def test_empirical_model_extraction_from_spans():
    checker = BoundedModelChecker()
    spans = [
        {"source_agent": "agent-planner", "target_agent": "agent-worker", "action_type": "DELEGATE", "token_count": 120},
        {"source_agent": "agent-worker", "target_agent": "agent-reviewer", "action_type": "SUBMIT_PR", "token_count": 180},
        {"source_agent": "agent-reviewer", "target_agent": "FINAL", "action_type": "APPROVE", "token_count": 50},
    ]

    extracted = checker.extract_model_from_spans(spans)
    assert extracted.initial_agent == "agent-planner"
    assert "agent-reviewer" in extracted.final_agents
    assert len(extracted.transitions) == 3

    res = checker.verify(extracted, bound_k=5, swarm_id="swarm-empirical")
    assert res.verdict == VerificationVerdict.VERIFIED


def test_server_model_check_endpoints():
    app = create_app()
    client = TestClient(app)
    swarm_id = "swarm-api-verify-01"

    # Status before verification runs (returns default verification)
    res_status = client.get(f"/api/v1/swarms/{swarm_id}/model-check/status")
    assert res_status.status_code == 200
    status_data = res_status.json()
    assert status_data["verdict"] == "VERIFIED"
    assert status_data["swarm_id"] == swarm_id

    # Verify custom model with deadlock
    verify_payload = {
        "bound_k": 5,
        "invariants": ["NO_DEADLOCK"],
        "initial_agent": "agent-start",
        "final_agents": ["agent-final"],
        "transitions": [
            {"from_agent": "agent-start", "to_agent": "agent-stuck", "action_type": "CALL", "token_cost": 50}
        ],
    }
    res_post = client.post(f"/api/v1/swarms/{swarm_id}/model-check/verify", json=verify_payload)
    assert res_post.status_code == 200
    post_data = res_post.json()
    assert post_data["status"] == "completed"
    assert post_data["verification"]["verdict"] == "VIOLATED"
    assert post_data["verification"]["violated_invariant"] == "NO_DEADLOCK"

    # Reset
    res_reset = client.post(f"/api/v1/swarms/{swarm_id}/model-check/reset")
    assert res_reset.status_code == 200
    assert res_reset.json()["status"] == "reset"


def test_cli_verify_subcommands(capsys):
    global_model_checker.reset("swarm-cli-verify")

    # CLI check with JSON format
    with pytest.raises(SystemExit) as exc:
        main(["verify", "check", "--swarm-id", "swarm-cli-verify", "--bound-k", "5", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["verdict"] == "VERIFIED"
    assert data["max_depth_bound"] == 5

    # CLI status with Terminal format
    with pytest.raises(SystemExit) as exc:
        main(["verify", "status", "--swarm-id", "swarm-cli-verify", "--format", "terminal"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "BOUNDED MODEL CHECKING STATUS" in captured.out
    assert "VERIFIED" in captured.out

    # CLI reset with JSON format
    with pytest.raises(SystemExit) as exc:
        main(["verify", "reset", "--swarm-id", "swarm-cli-verify", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["status"] == "reset"
