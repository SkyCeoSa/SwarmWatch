# -*- coding: utf-8 -*-
"""
Test Suite for Adaptive Dynamic Quota Re-balancing (Seam 35 / ADR 0046).
Verifies in-process mathematical utility evaluation, operational tier transitions,
quota reclamation from throttled/quarantined agents, surge grants for converging agents,
REST control plane endpoints, and CLI commands.
"""

import json
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient

from swarmwatch.rebalancer import (
    AdaptiveQuotaRebalancer,
    AgentTier,
    RebalancePolicy,
    global_rebalancer,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_rebalancer():
    """Resets the global rebalancer between tests."""
    global_rebalancer.reset("test-swarm-01")
    global_rebalancer.reset("swarm-live-01")
    yield
    global_rebalancer.reset("test-swarm-01")
    global_rebalancer.reset("swarm-live-01")


def test_evaluate_utility_score():
    """Verifies the mathematical correctness and weighting of U_a."""
    policy = RebalancePolicy(
        weight_drift=0.45,
        weight_vcr=0.25,
        weight_entropy=0.20,
        weight_fault=0.10,
        target_entropy=0.75,
    )
    rebalancer = AdaptiveQuotaRebalancer(policy=policy)

    # Ideal agent: zero drift (drift_inv=1.0), high VCR (1.0), optimal entropy (0.75), 0 faults
    ideal_u = rebalancer.evaluate_utility(drift_score=0.0, vcr=1.0, entropy=0.75, fault_rate=0.0)
    assert abs(ideal_u - (0.45*1.0 + 0.25*1.0 + 0.20*1.0 - 0.0)) < 1e-4
    assert ideal_u >= 0.89

    # Degraded/Divergent agent: high drift (0.8), low VCR (0.15), collapsed entropy (0.2), 0.5 fault rate
    bad_u = rebalancer.evaluate_utility(drift_score=0.8, vcr=0.15, entropy=0.2, fault_rate=0.5)
    # drift_inv = 0.2 -> 0.45 * 0.2 = 0.09
    # vcr = 0.15 -> 0.25 * 0.15 = 0.0375
    # diff = 0.55 -> entropy_term = max(0, 1 - 0.55/0.75) = 0.2667 -> 0.2 * 0.2667 = 0.0533
    # fault = 0.5 -> 0.1 * 0.5 = 0.05
    # sum = 0.09 + 0.0375 + 0.0533 - 0.05 = 0.1308
    assert bad_u < 0.20


def test_tier_classification():
    """Verifies that agents transition correctly between operational tiers."""
    rebalancer = AdaptiveQuotaRebalancer()
    assert rebalancer._classify_tier(0.85) == AgentTier.BOOSTED
    assert rebalancer._classify_tier(0.70) == AgentTier.BOOSTED
    assert rebalancer._classify_tier(0.69) == AgentTier.NOMINAL
    assert rebalancer._classify_tier(0.40) == AgentTier.NOMINAL
    assert rebalancer._classify_tier(0.39) == AgentTier.THROTTLED
    assert rebalancer._classify_tier(0.20) == AgentTier.THROTTLED
    assert rebalancer._classify_tier(0.19) == AgentTier.QUARANTINED
    assert rebalancer._classify_tier(0.0) == AgentTier.QUARANTINED


def test_agent_registration_and_turn_recording():
    """Verifies registering agents and tracking cumulative turns and token metrics."""
    rebalancer = AdaptiveQuotaRebalancer()
    swarm_id = "test-swarm-01"
    rebalancer.set_fleet_quota(swarm_id, 50_000)

    alloc1 = rebalancer.register_agent(swarm_id, "agent-researcher", initial_tokens=5000)
    assert alloc1.agent_id == "agent-researcher"
    assert alloc1.allocated_tokens == 5000
    assert alloc1.consumed_tokens == 0
    assert alloc1.remaining_tokens == 5000

    # Record turn with good metrics
    alloc2 = rebalancer.record_turn(
        swarm_id=swarm_id,
        agent_id="agent-researcher",
        tokens=1200,
        drift_score=0.05,
        vcr=0.90,
        entropy=0.74,
    )
    assert alloc2.consumed_tokens == 1200
    assert alloc2.remaining_tokens == 3800
    assert alloc2.tier == AgentTier.BOOSTED
    assert alloc2.utility_score >= 0.70


def test_dynamic_rebalance_reclaims_and_boosts():
    """
    Verifies that compute_rebalance:
    1. Reclaims unspent tokens from throttled/quarantined agents.
    2. Directs surplus budget to high-utility agents based on non-linear utility weights.
    3. Preserves safety floor for active agents.
    """
    rebalancer = AdaptiveQuotaRebalancer(
        policy=RebalancePolicy(
            safety_floor_tokens=500,
            sensitivity_exponent=1.5,
        )
    )
    swarm_id = "test-swarm-01"
    rebalancer.set_fleet_quota(swarm_id, 30_000)

    # Agent A: High performance, converging
    rebalancer.register_agent(swarm_id, "agent-converging", initial_tokens=5000)
    rebalancer.record_turn(swarm_id, "agent-converging", tokens=2000, drift_score=0.02, vcr=0.95, entropy=0.75)

    # Agent B: Nominal performance
    rebalancer.register_agent(swarm_id, "agent-nominal", initial_tokens=5000)
    rebalancer.record_turn(swarm_id, "agent-nominal", tokens=1000, drift_score=0.35, vcr=0.60, entropy=0.70)

    # Agent C: Rogue agent, severe semantic drift and echo chamber collapse
    rebalancer.register_agent(swarm_id, "agent-rogue", initial_tokens=5000)
    rebalancer.record_turn(swarm_id, "agent-rogue", tokens=1000, drift_score=0.90, vcr=0.10, entropy=0.20, is_fault=True)

    # Execute rebalance
    plan = rebalancer.compute_rebalance(swarm_id, justification="Test epoch")
    assert plan.swarm_id == swarm_id
    assert plan.reclaimed_buffer_tokens >= 0

    allocs = plan.allocations
    assert allocs["agent-rogue"].tier in (AgentTier.THROTTLED, AgentTier.QUARANTINED)
    assert allocs["agent-converging"].tier == AgentTier.BOOSTED

    # Converging agent should have received higher allocation than nominal
    assert allocs["agent-converging"].allocated_tokens > allocs["agent-nominal"].allocated_tokens

    # Rogue agent should have had excess reclaimed (not allowed to hold 5000)
    assert allocs["agent-rogue"].allocated_tokens <= (1000 + 500)


def test_surge_grant_for_boosted_agent():
    """Verifies that request_surge extracts from reclaimed buffer for converging agents."""
    rebalancer = AdaptiveQuotaRebalancer(
        policy=RebalancePolicy(safety_floor_tokens=500, surge_threshold_ratio=0.80)
    )
    swarm_id = "test-swarm-01"
    rebalancer.set_fleet_quota(swarm_id, 20_000)

    # Artificially populate reclaimed buffer
    rebalancer._reclaimed_buffers[swarm_id] = 4000

    rebalancer.register_agent(swarm_id, "agent-star", initial_tokens=2000)
    # Record 500 tokens (500 / 2000 = 25% < 80%, so auto-surge does not trigger yet)
    rebalancer.record_turn(swarm_id, "agent-star", tokens=500, drift_score=0.0, vcr=0.95, entropy=0.75)

    alloc_before = rebalancer.get_agent_quota(swarm_id, "agent-star")
    assert alloc_before.tier == AgentTier.BOOSTED
    assert alloc_before.surge_granted_tokens == 0

    # Manual request surge
    granted = rebalancer.request_surge(swarm_id, "agent-star", requested_tokens=1500)
    assert granted == 1500

    alloc_after = rebalancer.get_agent_quota(swarm_id, "agent-star")
    assert alloc_after.allocated_tokens == alloc_before.allocated_tokens + 1500
    assert alloc_after.surge_granted_tokens == 1500
    assert rebalancer._reclaimed_buffers[swarm_id] == 4000 - 1500


def test_server_rebalancer_endpoints():
    """Tests the REST API control plane endpoints for the rebalancer."""
    client = TestClient(create_app())
    swarm_id = "test-swarm-01"

    # Set quota
    resp = client.post(
        f"/api/v1/swarms/{swarm_id}/rebalancer/quota",
        json={"total_tokens": 45000},
    )
    assert resp.status_code == 200
    assert resp.json()["fleet_quota_tokens"] == 45000

    # Record turns in global_rebalancer
    global_rebalancer.record_turn(swarm_id, "agent-alpha", tokens=1500, drift_score=0.05, vcr=0.85)
    global_rebalancer.record_turn(swarm_id, "agent-beta", tokens=2500, drift_score=0.80, vcr=0.20)

    # Get status
    resp = client.get(f"/api/v1/swarms/{swarm_id}/rebalancer")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_agents"] == 2
    assert "agent-alpha" in data["allocations"]
    assert "agent-beta" in data["allocations"]

    # Trigger rebalance
    resp = client.post(
        f"/api/v1/swarms/{swarm_id}/rebalancer/rebalance",
        json={"justification": "API triggered rebalance"},
    )
    assert resp.status_code == 200
    plan_data = resp.json()
    assert plan_data["swarm_id"] == swarm_id
    assert "agent-alpha" in plan_data["allocations"]

    # Update policy
    resp = client.post(
        f"/api/v1/swarms/{swarm_id}/rebalancer/policy",
        json={"safety_floor_tokens": 750, "sensitivity_exponent": 2.0},
    )
    assert resp.status_code == 200
    assert resp.json()["policy"]["safety_floor_tokens"] == 750

    # Reset
    resp = client.post(f"/api/v1/swarms/{swarm_id}/rebalancer/reset")
    assert resp.status_code == 200
    assert resp.json()["status"] == "reset"


def test_cli_rebalance_commands(capsys):
    """Tests the CLI subcommands `swarmwatch rebalance status|run|quota`."""
    swarm_id = "swarm-live-01"
    global_rebalancer.set_fleet_quota(swarm_id, 60_000)
    global_rebalancer.record_turn(swarm_id, "worker-01", tokens=1200, drift_score=0.1, vcr=0.88)
    global_rebalancer.record_turn(swarm_id, "worker-02", tokens=800, drift_score=0.75, vcr=0.25)

    # CLI status (terminal format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["rebalance", "status", "--swarm-id", swarm_id])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "SWARMWATCH ADAPTIVE DYNAMIC QUOTA REBALANCER" in captured.out
    assert "worker-01" in captured.out
    assert "worker-02" in captured.out

    # CLI status (JSON format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["rebalance", "status", "--swarm-id", swarm_id, "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["swarm_id"] == swarm_id
    assert "allocations" in parsed

    # CLI run
    with pytest.raises(SystemExit) as exc:
        cli_main(["rebalance", "run", "--swarm-id", swarm_id])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "[Rebalance Epoch Completed]" in captured.out

    # CLI quota
    with pytest.raises(SystemExit) as exc:
        cli_main(["rebalance", "quota", "--swarm-id", swarm_id, "--tokens", "80000"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "[Fleet Quota Set]" in captured.out
