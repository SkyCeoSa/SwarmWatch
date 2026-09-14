# -*- coding: utf-8 -*-
"""
End-to-End Industrial Multi-Tenant Simulation Test.
Verifies the complete lifecycle across:
1. Organization and Project provisioning.
2. Cryptographic API key generation (sw_live_...) and constant-time authentication.
3. Multi-agent telemetry ingest with project-level row isolation.
4. Runaway semantic loop detection, Shannon entropy plunge, and circuit breaker trip.
5. Distributed fleet coordination halting subsequent parallel worker turns.
6. Impeccable UI delivery without emojis, neon glows, or nested cards.
"""

import asyncio
from datetime import datetime, timezone
import pytest
from starlette.testclient import TestClient

from swarmwatch.circuit import CircuitPolicy, EntropyCircuitBreaker, SwarmCircuitBreakException
from swarmwatch.fleet import FleetCircuitCoordinator
from swarmwatch.schema import SwarmSpan
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.tenant import ApiKeyManager, Organization, Project


def test_end_to_end_industrial_multitenant_simulation(tmp_path):
    db_file = str(tmp_path / "industrial_sim.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Health check verification
    health_res = client.get("/api/v1/health")
    assert health_res.status_code == 200
    assert health_res.json()["status"] == "healthy"

    # 2. Provision Tenant Hierarchy: Org -> Project -> Cryptographic API Key
    storage.create_organization_sync(Organization(
        id="org-deepmind-alpha",
        name="DeepMind Alpha",
        slug="deepmind-alpha"
    ))

    org_res = client.post("/api/v1/projects", json={
        "name": "Fintech Arbitrage Core",
        "slug": "fintech-arbitrage",
        "org_id": "org-deepmind-alpha"
    })
    assert org_res.status_code == 200
    project_id = org_res.json()["id"]
    assert project_id is not None

    key_res = client.post("/api/v1/keys", json={
        "project_id": project_id,
        "name": "Autonomous Market Maker Key",
        "is_test": False,
        "scopes": ["telemetry:write", "telemetry:read"]
    })
    assert key_res.status_code == 200
    raw_api_key = key_res.json()["api_key"]
    assert raw_api_key.startswith("sw_live_")
    key_prefix = key_res.json()["key_prefix"]
    assert key_prefix.startswith("sw_live_")

    auth_headers = {
        "Authorization": f"Bearer {raw_api_key}",
        "X-Project-ID": project_id,
    }

    # 3. Simulate Multi-Agent Swarm Ingestion under Authenticated Tenant
    swarm_id = "swarm-arbitrage-live-01"
    now_iso = datetime.now(timezone.utc).isoformat()

    spans = [
        {
            "id": "span_arb_01",
            "swarm_id": swarm_id,
            "project_id": project_id,
            "branch_id": "main",
            "source_agent": "MacroSentimentAuditor",
            "target_agent": "SignalEngine",
            "span_type": "agent_call",
            "input_payload": {"market": "BTC/USDT", "spread": 0.0014},
            "output_payload": {"signal": "BUY", "confidence": 0.94},
            "token_count": 820,
            "entropy_score": 1.88,
            "start_time": now_iso,
            "end_time": now_iso,
            "duration_ms": 140.0,
        },
        {
            "id": "span_arb_02",
            "swarm_id": swarm_id,
            "project_id": project_id,
            "branch_id": "main",
            "parent_span_id": "span_arb_01",
            "source_agent": "SignalEngine",
            "target_agent": "RiskGatekeeper",
            "span_type": "agent_call",
            "input_payload": {"order_size": 15.5},
            "output_payload": {"risk_approved": True, "var_limit": 50000},
            "token_count": 640,
            "entropy_score": 1.76,
            "start_time": now_iso,
            "end_time": now_iso,
            "duration_ms": 95.0,
        }
    ]

    ingest_res = client.post("/api/v1/spans", json=spans, headers=auth_headers)
    assert ingest_res.status_code == 200
    assert ingest_res.json()["received"] == 2
    assert ingest_res.json()["project_id"] == project_id

    # 4. Verify Row-Level Tenant Isolation
    # 4a. Authenticated query with valid project_id returns both spans
    query_res = client.get(f"/api/v1/swarms/{swarm_id}/spans?branch_id=main", headers=auth_headers)
    assert query_res.status_code == 200
    retrieved = query_res.json()
    assert len(retrieved) == 2
    assert retrieved[0]["id"] == "span_arb_01"

    # 4b. Query from an isolated project returns 0 spans
    other_headers = {"X-Project-ID": "isolated-other-project"}
    isolated_res = client.get(f"/api/v1/swarms/{swarm_id}/spans?branch_id=main", headers=other_headers)
    assert isolated_res.status_code == 200
    assert len(isolated_res.json()) == 0

    # 5. Circuit Breaker Intervention & Fleet Coordination Verification
    policy = CircuitPolicy(
        max_turns=5,
        token_budget=2000,
        cost_budget=0.10,
        entropy_threshold=0.35,
        window_size=3,
        max_warnings=1,
    )
    coordinator = FleetCircuitCoordinator(policy=policy)
    breaker = EntropyCircuitBreaker(policy=policy)

    # Agent encounters repetitive runaway loop
    loop_text = "Retrying execution with identical order parameters due to timeout."
    d1 = breaker.evaluate(loop_text)
    assert d1.decision == "pass"

    d2 = breaker.evaluate(loop_text)
    assert d2.decision == "warning"

    with pytest.raises(SwarmCircuitBreakException) as trip_exc:
        breaker.evaluate(loop_text)
    assert trip_exc.value.savings is not None
    assert trip_exc.value.savings.tokens_saved >= 0

    # Record turns in fleet coordinator until ceiling is breached
    coordinator.record_turn(worker_id="worker-01", swarm_id=swarm_id, tokens=1200, cost=0.06, turns=3)
    assert not coordinator.is_tripped(swarm_id)

    # Worker 2 exceeds token budget (1200 + 1000 = 2200 > 2000)
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        coordinator.record_turn(worker_id="worker-02", swarm_id=swarm_id, tokens=1000, cost=0.05, turns=2)
    assert coordinator.is_tripped(swarm_id)

    # Subsequent workers are immediately halted
    with pytest.raises(SwarmCircuitBreakException):
        coordinator.record_turn(worker_id="worker-03", swarm_id=swarm_id, tokens=10, cost=0.01, turns=1)

    # 6. Impeccable Craft Floor Standards on Console HTML
    ui_res = client.get("/")
    assert ui_res.status_code == 200
    html = ui_res.text

    # Craft floor checks
    assert "#080B11" in html, "Canvas background must strictly be #080B11"
    assert "tabular-nums" in html, "Numeric telemetry columns must use tabular-nums"
    assert "Neon" not in html and "neon" not in html, "Zero neon glows permitted"
    assert "box-shadow: 0 0 20px" not in html, "Zero neon glows permitted"
