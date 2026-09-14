"""
TDD Tests for Ticket 06: Real-time Ingestion Server, WebSocket Control Plane & Live Swimlane Dashboard
"""
import asyncio
import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.schema import SwarmSpan
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app


def test_server_health_and_spans_rest_api(tmp_path):
    db_path = str(tmp_path / "server_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Health check
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"

    # 2. Ingest Spans via POST
    span_payload = [{
        "id": "span-rest-01",
        "swarm_id": "swarm-rest-01",
        "branch_id": "main",
        "source_agent": "planner",
        "target_agent": "worker",
        "span_type": "agent_call",
        "input_payload": {"cmd": "build"},
        "output_payload": {"status": "success"},
        "token_count": 50,
        "entropy_score": 0.92,
        "start_time": "2026-09-13T10:00:00Z",
        "end_time": "2026-09-13T10:00:01Z",
        "duration_ms": 1000.0
    }]
    post_res = client.post("/api/v1/spans", json=span_payload)
    assert post_res.status_code == 200
    assert post_res.json()["received"] == 1

    # 3. Query Spans
    get_res = client.get("/api/v1/swarms/swarm-rest-01/spans?branch_id=main")
    assert get_res.status_code == 200
    spans = get_res.json()
    assert len(spans) == 1
    assert spans[0]["id"] == "span-rest-01"


def test_server_dashboard_ui_endpoint(tmp_path):
    db_path = str(tmp_path / "ui_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    res = client.get("/")
    assert res.status_code == 200
    assert "<!DOCTYPE html>" in res.text
    assert "SwarmWatch" in res.text
    assert "canvas" in res.text.lower()


def test_websocket_broadcast_and_command_handling(tmp_path):
    db_path = str(tmp_path / "ws_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    with client.websocket_connect("/ws/swarm/swarm-live-01") as websocket:
        # 1. Send client command over WebSocket
        websocket.send_json({"command": "CMD_PAUSE", "reason": "user_debugger"})
        response = websocket.receive_json()
        assert response["type"] == "command_ack"
        assert response["command"] == "CMD_PAUSE"

        # 2. Ingest a span via REST and verify it streams over WebSocket
        span_payload = [{
            "id": "span-stream-01",
            "swarm_id": "swarm-live-01",
            "branch_id": "main",
            "source_agent": "agent_alpha",
            "target_agent": "agent_beta",
            "span_type": "agent_call",
            "input_payload": {"msg": "hello"},
            "output_payload": {"msg": "world"},
            "token_count": 25,
            "entropy_score": 0.88,
            "start_time": "2026-09-13T10:00:00Z",
            "end_time": "2026-09-13T10:00:01Z",
            "duration_ms": 250.0
        }]
        client.post("/api/v1/spans", json=span_payload)

        # 3. Receive broadcasted span
        broadcast = websocket.receive_json()
        assert broadcast["type"] == "span_update"
        assert broadcast["span"]["id"] == "span-stream-01"


def test_server_simulation_endpoint_and_circuit_breaker(tmp_path):
    db_path = str(tmp_path / "sim_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # Trigger simulation via POST
    res = client.post("/api/v1/simulate?swarm_id=swarm-sim-test")
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "simulation_complete"
    assert data["spans_count"] == 5
    assert data["circuit_breaker"]["tripped"] is True
    assert data["circuit_breaker"]["dollars_saved"] == 4.83
    assert data["circuit_breaker"]["tokens_saved"] == 241500

    # Verify spans persisted in storage
    get_res = client.get("/api/v1/swarms/swarm-sim-test/spans")
    assert get_res.status_code == 200
    spans = get_res.json()
    assert len(spans) == 5
    assert spans[0]["source_agent"] == "ProductArchitect"
    assert spans[1]["source_agent"] == "BackendEngineer"
    assert spans[2]["source_agent"] == "CodeReviewer"
    assert spans[3]["source_agent"] == "SecurityAuditor"


def test_server_simulation_broadcasts_over_websocket(tmp_path):
    db_path = str(tmp_path / "sim_ws_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    with client.websocket_connect("/ws/swarm/swarm-sim-ws") as websocket:
        # Trigger simulation
        res = client.post("/api/v1/simulate?swarm_id=swarm-sim-ws")
        assert res.status_code == 200

        # Receive 5 span updates + 1 circuit breaker tripped notification
        received_types = []
        for _ in range(6):
            msg = websocket.receive_json()
            received_types.append(msg.get("type"))

        assert received_types.count("span_update") == 5
        assert "circuit_breaker_tripped" in received_types


def test_server_fork_branch_rest_endpoint(tmp_path):
    db_path = str(tmp_path / "fork_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # First run simulation to populate spans and snapshots
    client.post("/api/v1/simulate?swarm_id=swarm-fork-test")

    # Fork from span_02
    fork_payload = {
        "swarm_id": "swarm-fork-test",
        "target_span_id": "span_02",
        "new_branch_id": "branch_fork_02"
    }
    res = client.post("/api/v1/branches/fork", json=fork_payload)
    assert res.status_code == 200
    fork_data = res.json()
    assert fork_data["status"] == "forked"
    assert fork_data["new_branch_id"] == "branch_fork_02"
    assert "state" in fork_data


def test_server_dynamic_policy_and_budget_endpoints(tmp_path):
    db_path = str(tmp_path / "policy_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Get default policy
    res = client.get("/api/v1/policy")
    assert res.status_code == 200
    policy = res.json()
    assert "similarity_threshold" in policy
    assert "entropy_threshold" in policy
    assert policy["entropy_threshold"] == 0.40

    # 2. Update policy dynamically
    update_res = client.post("/api/v1/policy", json={
        "similarity_threshold": 0.75,
        "entropy_threshold": 0.50,
        "window_size": 4,
        "max_warnings": 2,
        "max_turns": 40,
        "token_budget": 20000,
        "cost_budget": 0.50,
    })
    assert update_res.status_code == 200
    updated = update_res.json()["policy"]
    assert updated["entropy_threshold"] == 0.50
    assert updated["max_turns"] == 40
    assert updated["token_budget"] == 20000

    # 3. Verify budget endpoint
    budget_res = client.get("/api/v1/budget")
    assert budget_res.status_code == 200
    budget_data = budget_res.json()
    assert budget_data["max_turns"] == 40
    assert budget_data["token_budget"] == 20000
    assert "dollars_saved" in budget_data
