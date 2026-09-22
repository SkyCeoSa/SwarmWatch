# -*- coding: utf-8 -*-
"""
TDD Test Suite for Distributed Redis Fleet & Cluster Monitor Widget (Seam 23 / ADR 0034).
Validates dashboard.html DOM elements, CSS styles, server REST metadata, and live WebSocket broadcasts.
"""

from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from swarmwatch.circuit import CircuitPolicy
from swarmwatch.redis_circuit import MockRedisClient, RedisCircuitCoordinator
from swarmwatch.server import create_app


DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "swarmwatch" / "dashboard.html"


def test_dashboard_html_contains_redis_fleet_widget_elements():
    """Validates that dashboard.html includes all required Redis fleet DOM elements and CSS rules."""
    assert DASHBOARD_PATH.exists()
    content = DASHBOARD_PATH.read_text(encoding="utf-8")

    # CSS class definitions
    assert ".fleet-bar" in content
    assert ".fleet-badge" in content
    assert ".fleet-workers-container" in content
    assert ".fleet-worker-pill" in content
    assert ".fleet-meters-container" in content
    assert ".fleet-progress-bg" in content
    assert ".fleet-progress-fill" in content
    assert ".fleet-trip-alert-banner" in content
    assert ".btn-fleet-trip" in content
    assert ".btn-fleet-reset" in content

    # DOM elements
    assert 'id="fleetBar"' in content
    assert 'id="btnFleetToggle"' in content
    assert 'id="fleetClusterBadge"' in content
    assert 'id="fleetBackendTag"' in content
    assert 'id="fleetWorkersList"' in content
    assert 'id="fleetTurnsVal"' in content
    assert 'id="fleetTokensVal"' in content
    assert 'id="fleetCostVal"' in content
    assert 'id="btnFleetTrip"' in content
    assert 'id="btnFleetReset"' in content
    assert 'id="fleetTripAlert"' in content
    assert 'id="fleetTripReason"' in content
    assert 'id="btnFleetAlertReset"' in content

    # JavaScript controller functions
    assert "function updateFleetUI(" in content
    assert "function renderFleetTripAlert(" in content
    assert "function clearFleetTripAlert(" in content
    assert "function fetchFleetMetrics(" in content
    assert "function triggerEmergencyFleetTrip(" in content
    assert "function triggerFleetReset(" in content
    assert "msg.type === 'fleet_metric_update'" in content
    assert "msg.type === 'fleet_trip_broadcast'" in content
    assert "msg.type === 'fleet_state_reset'" in content


def test_server_fleet_metrics_metadata_and_policy():
    """Validates that GET /api/v1/fleet/redis/metrics returns backend_type and active policy ceilings."""
    mock_redis = MockRedisClient()
    policy = CircuitPolicy(max_turns=25, token_budget=50000, cost_budget=2.50)
    coordinator = RedisCircuitCoordinator(policy=policy, redis_client=mock_redis)
    coordinator.register_worker("worker-k8s-01", "swarm-widget-01")
    coordinator.record_turn("worker-k8s-01", "swarm-widget-01", tokens=1200, cost=0.12, turns=2)

    app = create_app(redis_coordinator=coordinator)
    client = TestClient(app)

    res = client.get("/api/v1/fleet/redis/metrics?swarm_id=swarm-widget-01")
    assert res.status_code == 200
    data = res.json()

    assert data["swarm_id"] == "swarm-widget-01"
    assert data["total_tokens"] == 1200
    assert data["total_turns"] == 2
    assert pytest.approx(data["total_cost"], rel=1e-4) == 0.12
    assert "worker-k8s-01" in data["active_workers"]
    assert data["backend_type"] == "MockRedisClient"
    assert data["policy"]["max_turns"] == 25
    assert data["policy"]["token_budget"] == 50000
    assert pytest.approx(data["policy"]["cost_budget"], rel=1e-4) == 2.50


def test_server_websocket_fleet_trip_and_reset_broadcasts():
    """Validates that control plane trip and reset endpoints broadcast over WebSocket."""
    mock_redis = MockRedisClient()
    coordinator = RedisCircuitCoordinator(redis_client=mock_redis)
    coordinator.register_worker("worker-01", "swarm-ws-test")

    app = create_app(redis_coordinator=coordinator)
    client = TestClient(app)

    with client.websocket_connect("/ws/swarm/swarm-ws-test") as ws:
        # Trigger fleet trip via POST
        res_trip = client.post("/api/v1/fleet/redis/trip", json={
            "swarm_id": "swarm-ws-test",
            "reason": "Anomalous tool loop detected across cluster",
            "tripped_by": "sec-guardrail",
            "metric_val": 42.0
        })
        assert res_trip.status_code == 200

        # Receive WebSocket message
        trip_msg = ws.receive_json()
        assert trip_msg["type"] == "fleet_trip_broadcast"
        assert trip_msg["swarm_id"] == "swarm-ws-test"
        assert trip_msg["reason"] == "Anomalous tool loop detected across cluster"
        assert trip_msg["tripped_by"] == "sec-guardrail"

        # Trigger fleet reset via POST
        res_reset = client.post("/api/v1/fleet/redis/reset", json={
            "swarm_id": "swarm-ws-test"
        })
        assert res_reset.status_code == 200

        # Receive WebSocket reset message
        reset_msg = ws.receive_json()
        assert reset_msg["type"] == "fleet_state_reset"
        assert reset_msg["swarm_id"] == "swarm-ws-test"
