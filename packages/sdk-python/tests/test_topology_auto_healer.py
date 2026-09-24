import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.cli import main
from swarmwatch.model_checker import (
    VerificationInvariant,
    VerificationVerdict,
    AgentTransition,
    AgentTransitionModel,
)
from swarmwatch.topology_healer import (
    PatchAction,
    TopologyPatch,
    HealedModelReport,
    TopologyAutoHealer,
    global_topology_healer,
)
from swarmwatch.server import create_app


def test_auto_heal_deadlock():
    healer = TopologyAutoHealer()
    swarm_id = "swarm-heal-deadlock-01"

    # Deadlocked topology
    model = AgentTransitionModel(
        initial_agent="agent-planner",
        final_agents={"agent-complete"},
        transitions=[
            AgentTransition(from_agent="agent-planner", to_agent="agent-deadlock", action_type="ROUTE", token_cost=100),
        ],
        max_token_budget=5000,
    )

    report = healer.heal_and_verify(model, bound_k=5, swarm_id=swarm_id)
    assert report.original_verdict == VerificationVerdict.VIOLATED
    assert report.violated_invariant == VerificationInvariant.NO_DEADLOCK
    assert report.applied_patch is not None
    assert report.applied_patch.action == PatchAction.FALLBACK_REDIRECT
    assert report.applied_patch.target_agent == "agent-deadlock"
    assert report.healed_verdict == VerificationVerdict.VERIFIED
    assert len(healer.get_active_patches(swarm_id)) == 1
    assert report.execution_time_ms < 50.0


def test_auto_heal_unbounded_cycle():
    healer = TopologyAutoHealer()
    swarm_id = "swarm-heal-cycle-01"

    # Infinite ping-pong cycle
    model = AgentTransitionModel(
        initial_agent="agent-ping",
        final_agents={"agent-exit"},
        transitions=[
            AgentTransition(from_agent="agent-ping", to_agent="agent-pong", action_type="PING", token_cost=50),
            AgentTransition(from_agent="agent-pong", to_agent="agent-ping", action_type="PONG", token_cost=50),
        ],
        max_token_budget=100000,
    )

    report = healer.heal_and_verify(model, bound_k=10, swarm_id=swarm_id)
    assert report.original_verdict == VerificationVerdict.VIOLATED
    assert report.violated_invariant == VerificationInvariant.NO_UNBOUNDED_CYCLES
    assert report.applied_patch is not None
    assert report.applied_patch.action == PatchAction.CYCLE_GATE_BREAK
    assert report.healed_verdict == VerificationVerdict.VERIFIED


def test_auto_heal_budget_exhaustion():
    healer = TopologyAutoHealer()
    swarm_id = "swarm-heal-budget-01"

    # Greedily breaches max_token_budget of 600
    model = AgentTransitionModel(
        initial_agent="agent-source",
        final_agents={"agent-target"},
        transitions=[
            AgentTransition(from_agent="agent-source", to_agent="agent-mid", action_type="EXPENSIVE_STEP", token_cost=500),
            AgentTransition(from_agent="agent-mid", to_agent="agent-target", action_type="EXPENSIVE_STEP_2", token_cost=400),
        ],
        max_token_budget=600,
    )

    report = healer.heal_and_verify(model, bound_k=5, swarm_id=swarm_id)
    assert report.original_verdict == VerificationVerdict.VIOLATED
    assert report.violated_invariant == VerificationInvariant.BOUNDED_BUDGET
    assert report.applied_patch is not None
    assert report.applied_patch.action == PatchAction.BUDGET_REWIRE
    assert report.healed_verdict == VerificationVerdict.VERIFIED


def test_auto_heal_mutual_exclusion():
    healer = TopologyAutoHealer()
    swarm_id = "swarm-heal-mutex-01"

    # Conflicting roles execution
    model = AgentTransitionModel(
        initial_agent="agent-orchestrator",
        final_agents={"agent-done"},
        transitions=[
            AgentTransition(from_agent="agent-orchestrator", to_agent="agent-exec", action_type="RUN", token_cost=50),
            AgentTransition(from_agent="agent-exec", to_agent="agent-audit", action_type="AUDIT", token_cost=50),
            AgentTransition(from_agent="agent-audit", to_agent="agent-done", action_type="COMPLETE", token_cost=50),
        ],
        mutual_exclusion_pairs=[("agent-exec", "agent-audit")],
    )

    report = healer.heal_and_verify(model, bound_k=5, swarm_id=swarm_id)
    assert report.original_verdict == VerificationVerdict.VIOLATED
    assert report.violated_invariant == VerificationInvariant.MUTUAL_EXCLUSION
    assert report.applied_patch is not None
    assert report.applied_patch.action == PatchAction.MUTEX_MEDIATE
    assert report.healed_verdict == VerificationVerdict.VERIFIED


def test_healer_already_compliant_model():
    healer = TopologyAutoHealer()
    swarm_id = "swarm-heal-safe-01"

    model = AgentTransitionModel(
        initial_agent="agent-start",
        final_agents={"agent-end"},
        transitions=[
            AgentTransition(from_agent="agent-start", to_agent="agent-end", action_type="FINISH", token_cost=100),
        ],
        max_token_budget=5000,
    )

    report = healer.heal_and_verify(model, bound_k=5, swarm_id=swarm_id)
    assert report.original_verdict == VerificationVerdict.VERIFIED
    assert report.applied_patch is None
    assert report.healed_verdict == VerificationVerdict.VERIFIED


def test_revert_patch_lifecycle():
    healer = TopologyAutoHealer()
    swarm_id = "swarm-heal-revert-01"

    model = AgentTransitionModel(
        initial_agent="agent-stuck",
        final_agents={"agent-exit"},
        transitions=[],
    )

    # Heal vacuous/deadlock
    report = healer.heal_and_verify(model, bound_k=5, swarm_id=swarm_id)
    patches = healer.get_active_patches(swarm_id)
    if patches:
        assert len(patches) >= 1
        healer.revert_patch(swarm_id)
        assert len(healer.get_active_patches(swarm_id)) == 0


def test_server_topology_heal_endpoints():
    app = create_app()
    client = TestClient(app)
    swarm_id = "swarm-api-heal-01"

    # Status before heal
    res_status = client.get(f"/api/v1/swarms/{swarm_id}/topology-heal/status")
    assert res_status.status_code == 200
    assert res_status.json()["status"] in ("NOMINAL", "PATCHED")

    # Apply heal with failing deadlock model
    heal_payload = {
        "bound_k": 5,
        "initial_agent": "agent-start",
        "final_agents": ["agent-final"],
        "transitions": [
            {"from_agent": "agent-start", "to_agent": "agent-deadlocked", "action_type": "CALL", "token_cost": 50}
        ],
    }
    res_apply = client.post(f"/api/v1/swarms/{swarm_id}/topology-heal/apply", json=heal_payload)
    assert res_apply.status_code == 200
    apply_data = res_apply.json()
    assert apply_data["status"] == "applied"
    assert apply_data["report"]["healed_verdict"] == "VERIFIED"

    # Status after heal
    res_status_after = client.get(f"/api/v1/swarms/{swarm_id}/topology-heal/status")
    assert res_status_after.status_code == 200
    assert res_status_after.json()["active_patches_count"] >= 1

    # Revert
    res_revert = client.post(f"/api/v1/swarms/{swarm_id}/topology-heal/revert")
    assert res_revert.status_code == 200
    assert res_revert.json()["status"] == "reverted"


def test_cli_heal_subcommands(capsys):
    global_topology_healer.reset("swarm-cli-heal")

    # CLI apply with JSON format
    with pytest.raises(SystemExit) as exc:
        main(["patch", "apply", "--swarm-id", "swarm-cli-heal", "--bound-k", "5", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["healed_verdict"] == "VERIFIED"

    # CLI status with Terminal format
    with pytest.raises(SystemExit) as exc:
        main(["patch", "status", "--swarm-id", "swarm-cli-heal", "--format", "terminal"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "TOPOLOGY AUTO-HEAL STATUS" in captured.out
    assert "PATCHED" in captured.out

    # CLI revert with JSON format
    with pytest.raises(SystemExit) as exc:
        main(["patch", "revert", "--swarm-id", "swarm-cli-heal", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["status"] == "reverted"
