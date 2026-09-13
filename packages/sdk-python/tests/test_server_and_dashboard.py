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
