# -*- coding: utf-8 -*-
"""
TDD Test Suite for Distributed Fleet Circuit Coordinator (Phase 6).
"""
import pytest
from swarmwatch.circuit import CircuitPolicy, SwarmCircuitBreakException
from swarmwatch.fleet import FleetCircuitCoordinator, FleetMetrics, FleetEvent


def test_fleet_worker_registration_and_metrics():
    coordinator = FleetCircuitCoordinator(policy=CircuitPolicy())
    coordinator.register_worker("worker-1", "swarm-fleet-01")
    coordinator.register_worker("worker-2", "swarm-fleet-01")

    m1 = coordinator.record_turn("worker-1", "swarm-fleet-01", tokens=120, turns=1)
    assert m1.total_tokens == 120
    assert m1.total_turns == 1

    m2 = coordinator.record_turn("worker-2", "swarm-fleet-01", tokens=180, turns=1)
    assert m2.total_tokens == 300
    assert m2.total_turns == 2
    assert "worker-1" in m2.active_workers
    assert "worker-2" in m2.active_workers


def test_fleet_max_turns_distributed_halt():
    policy = CircuitPolicy(max_turns=3)
    coordinator = FleetCircuitCoordinator(policy=policy)

    # Worker 1 executes 2 turns
    coordinator.record_turn("worker-1", "swarm-turns", tokens=50, turns=2)
    assert coordinator.is_tripped("swarm-turns") is False

    # Worker 2 executes 1 turn (reaches 3)
    with pytest.raises(SwarmCircuitBreakException, match="max turns quota reached"):
        coordinator.record_turn("worker-2", "swarm-turns", tokens=50, turns=1)

    assert coordinator.is_tripped("swarm-turns") is True

    # Any worker attempting a turn now gets halted immediately
    with pytest.raises(SwarmCircuitBreakException, match="TRIPPED for swarm"):
        coordinator.record_turn("worker-1", "swarm-turns", tokens=10, turns=1)


def test_fleet_token_budget_distributed_halt():
    policy = CircuitPolicy(token_budget=500)
    coordinator = FleetCircuitCoordinator(policy=policy)

    # Worker 1 burns 300 tokens
    coordinator.record_turn("worker-1", "swarm-tokens", tokens=300, turns=1)
    assert coordinator.is_tripped("swarm-tokens") is False

    # Worker 2 burns 250 tokens (total 550 > 500)
    with pytest.raises(SwarmCircuitBreakException, match="token budget exceeded"):
        coordinator.record_turn("worker-2", "swarm-tokens", tokens=250, turns=1)

    assert coordinator.is_tripped("swarm-tokens") is True


def test_fleet_event_subscribers():
    coordinator = FleetCircuitCoordinator()
    captured_events = []

    def subscriber(event: FleetEvent):
        captured_events.append(event)

    coordinator.subscribe(subscriber)
    coordinator.register_worker("worker-test", "swarm-events")
    coordinator.record_turn("worker-test", "swarm-events", tokens=100, turns=1)
    coordinator.trip("swarm-events", "Emergency halt", "worker-test")

    assert len(captured_events) == 3
    assert captured_events[0].event_type == "worker_joined"
    assert captured_events[1].event_type == "metric_update"
    assert captured_events[2].event_type == "trip_broadcast"
