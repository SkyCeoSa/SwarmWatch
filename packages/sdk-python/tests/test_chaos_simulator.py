# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Interactive Chaos & Loop Simulator (ADR 0016).
Validates all 4 failure scenarios, circuit breaker interception, storage persistence,
and broadcast events.
"""

import pytest
from swarmwatch.chaos import ChaosSimulator, ChaosScenario
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.serializer import TimeMachine


@pytest.fixture
async def test_env(tmp_path):
    db_path = str(tmp_path / "test_chaos.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    await storage.initialize()
    time_machine = TimeMachine(storage=storage)
    yield storage, time_machine
    await storage.close()


def test_available_scenarios_catalog():
    scenarios = ChaosSimulator.get_available_scenarios()
    assert len(scenarios) == 4
    ids = [s["id"] for s in scenarios]
    assert "entropy_collapse" in ids
    assert "infinite_tool_recursion" in ids
    assert "budget_quota_exhaustion" in ids
    assert "conversational_deadlock" in ids


def test_invalid_scenario_raises():
    with pytest.raises(ValueError, match="Unknown chaos scenario"):
        ChaosSimulator.get_scenario_config("non_existent_mode")


@pytest.mark.anyio
async def test_entropy_collapse_scenario(test_env):
    storage, time_machine = test_env
    broadcasts = []

    async def mock_broadcast(swarm_id, msg):
        broadcasts.append(msg)

    result = await ChaosSimulator.run_scenario(
        scenario=ChaosScenario.ENTROPY_COLLAPSE,
        storage=storage,
        time_machine=time_machine,
        swarm_id="test-swarm-ec",
        step_delay=0.0,
        broadcast_callback=mock_broadcast,
    )

    assert result.status == "simulation_halted"
    assert result.scenario == "entropy_collapse"
    assert result.circuit_breaker["tripped"] is True
    assert "entropy" in result.circuit_breaker["reason"].lower() or "shannon" in result.circuit_breaker["reason"].lower()
    assert result.spans_count >= 5

    # Verify spans persisted in storage
    spans = await storage.get_swarm_spans("test-swarm-ec")
    assert len(spans) == result.spans_count

    # Verify broadcasts
    span_updates = [b for b in broadcasts if b["type"] == "span_update"]
    trip_updates = [b for b in broadcasts if b["type"] == "circuit_breaker_tripped"]
    assert len(span_updates) == result.spans_count
    assert len(trip_updates) == 1
    assert trip_updates[0]["scenario"] == "entropy_collapse"


@pytest.mark.anyio
async def test_tool_recursion_scenario(test_env):
    storage, time_machine = test_env
    broadcasts = []

    async def mock_broadcast(swarm_id, msg):
        broadcasts.append(msg)

    result = await ChaosSimulator.run_scenario(
        scenario="infinite_tool_recursion",
        storage=storage,
        time_machine=time_machine,
        swarm_id="test-swarm-tr",
        step_delay=0.0,
        broadcast_callback=mock_broadcast,
    )

    assert result.status == "simulation_halted"
    assert result.scenario == "infinite_tool_recursion"
    assert result.circuit_breaker["tripped"] is True
    assert result.spans_count >= 4

    # Verify tool call span types
    tool_calls = [s for s in result.spans if s.get("span_type") == "tool_call"]
    assert len(tool_calls) >= 2


@pytest.mark.anyio
async def test_budget_quota_exhaustion_scenario(test_env):
    storage, time_machine = test_env

    result = await ChaosSimulator.run_scenario(
        scenario=ChaosScenario.BUDGET_QUOTA_EXHAUSTION,
        storage=storage,
        time_machine=time_machine,
        swarm_id="test-swarm-bq",
        step_delay=0.0,
    )

    assert result.status == "simulation_halted"
    assert result.scenario == "budget_quota_exhaustion"
    assert result.circuit_breaker["tripped"] is True
    assert "budget exceeded" in result.circuit_breaker["reason"].lower()
    assert result.circuit_breaker["tokens_saved"] > 0
    assert result.spans_count == 2


@pytest.mark.anyio
async def test_conversational_deadlock_scenario(test_env):
    storage, time_machine = test_env

    result = await ChaosSimulator.run_scenario(
        scenario=ChaosScenario.CONVERSATIONAL_DEADLOCK,
        storage=storage,
        time_machine=time_machine,
        swarm_id="test-swarm-cd",
        step_delay=0.0,
    )

    assert result.status == "simulation_halted"
    assert result.scenario == "conversational_deadlock"
    assert result.circuit_breaker["tripped"] is True
    assert "similarity" in result.circuit_breaker["reason"].lower() or "repetition" in result.circuit_breaker["reason"].lower()
    assert result.spans_count >= 3


def test_api_chaos_scenarios_and_inject(tmp_path):
    from fastapi.testclient import TestClient
    from swarmwatch.server import create_app

    db_path = str(tmp_path / "test_api_chaos.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Test GET /api/v1/chaos/scenarios
    scenarios_res = client.get("/api/v1/chaos/scenarios")
    assert scenarios_res.status_code == 200
    catalog = scenarios_res.json()
    assert len(catalog) == 4
    assert any(s["id"] == "entropy_collapse" for s in catalog)

    # 2. Test POST /api/v1/chaos/inject with valid scenario
    inject_res = client.post(
        "/api/v1/chaos/inject?scenario=entropy_collapse&swarm_id=test-api-swarm&step_delay=0"
    )
    assert inject_res.status_code == 200
    body = inject_res.json()
    assert body["status"] == "simulation_halted"
    assert body["scenario"] == "entropy_collapse"
    assert body["circuit_breaker"]["tripped"] is True
    assert body["spans_count"] >= 5

    # 3. Test POST /api/v1/chaos/inject with unknown scenario (400 Bad Request)
    bad_res = client.post("/api/v1/chaos/inject?scenario=invalid_scenario")
    assert bad_res.status_code == 400
    assert "Unknown chaos scenario" in bad_res.json()["detail"]

