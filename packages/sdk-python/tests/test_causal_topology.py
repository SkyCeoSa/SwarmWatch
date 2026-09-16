"""
Tests for Causal Topology Graph and Loop Cycle Detection (ADR 0017 / Seam 9).
Verifies deterministic extraction of agent nodes, directed causal edges,
edge weight metrics, and Tarjan/DFS cycle detection from SwarmSpans.
"""

from datetime import datetime, timezone
import pytest
from swarmwatch.schema import SwarmSpan
from swarmwatch.topology import (
    AgentNode,
    CausalEdge,
    CausalTopology,
    build_topology_from_spans,
)


def _make_span(span_id: str, source: str, target: str, tokens: int = 100, duration_ms: float = 200.0) -> SwarmSpan:
    now = datetime.now(timezone.utc)
    return SwarmSpan(
        id=span_id,
        swarm_id="swarm-topo-test",
        source_agent=source,
        target_agent=target,
        input_payload={"message": f"Turn from {source} to {target}"},
        output_payload={"result": "ok"},
        token_count=tokens,
        duration_ms=duration_ms,
        entropy_score=1.85,
        start_time=now,
        end_time=now,
    )


def test_empty_spans_returns_empty_topology():
    topo = build_topology_from_spans([])
    assert len(topo.nodes) == 0
    assert len(topo.edges) == 0
    assert len(topo.cycles) == 0
    assert topo.total_turns == 0


def test_linear_dag_topology():
    spans = [
        _make_span("s1", "ProductArchitect", "BackendEngineer", tokens=1000, duration_ms=300.0),
        _make_span("s2", "BackendEngineer", "CodeReviewer", tokens=1500, duration_ms=450.0),
        _make_span("s3", "CodeReviewer", "SecurityAuditor", tokens=800, duration_ms=250.0),
    ]
    topo = build_topology_from_spans(spans)
    assert len(topo.nodes) == 4
    assert len(topo.edges) == 3
    assert len(topo.cycles) == 0
    assert topo.total_turns == 3

    # Check node attributes
    arch = next(n for n in topo.nodes if n.id == "ProductArchitect")
    assert arch.out_degree == 1
    assert arch.in_degree == 0

    backend = next(n for n in topo.nodes if n.id == "BackendEngineer")
    assert backend.in_degree == 1
    assert backend.out_degree == 1

    # Check edge attributes
    edge = next(e for e in topo.edges if e.source == "ProductArchitect" and e.target == "BackendEngineer")
    assert edge.turn_count == 1
    assert edge.total_tokens == 1000
    assert edge.mean_latency_ms == 300.0
    assert not edge.is_cyclic


def test_bidirectional_cycle_detection():
    # Ping-pong loop between BackendEngineer and SecurityAuditor
    spans = [
        _make_span("s1", "ProductArchitect", "BackendEngineer", tokens=500),
        _make_span("s2", "BackendEngineer", "SecurityAuditor", tokens=600),
        _make_span("s3", "SecurityAuditor", "BackendEngineer", tokens=600),
        _make_span("s4", "BackendEngineer", "SecurityAuditor", tokens=600),
    ]
    topo = build_topology_from_spans(spans)
    assert len(topo.cycles) >= 1
    cycle_nodes = set()
    for c in topo.cycles:
        cycle_nodes.update(c)
    assert "BackendEngineer" in cycle_nodes
    assert "SecurityAuditor" in cycle_nodes

    # The edges in the cycle should be flagged as cyclic
    edge_bs = next(e for e in topo.edges if e.source == "BackendEngineer" and e.target == "SecurityAuditor")
    edge_sb = next(e for e in topo.edges if e.source == "SecurityAuditor" and e.target == "BackendEngineer")
    assert edge_bs.is_cyclic
    assert edge_sb.is_cyclic
    assert edge_bs.turn_count == 2


def test_multi_agent_cycle_detection():
    # Triangular cycle: AgentA -> AgentB -> AgentC -> AgentA
    spans = [
        _make_span("s1", "AgentA", "AgentB", tokens=100),
        _make_span("s2", "AgentB", "AgentC", tokens=100),
        _make_span("s3", "AgentC", "AgentA", tokens=100),
    ]
    topo = build_topology_from_spans(spans)
    assert len(topo.cycles) >= 1
    
    edge_ab = next(e for e in topo.edges if e.source == "AgentA" and e.target == "AgentB")
    edge_bc = next(e for e in topo.edges if e.source == "AgentB" and e.target == "AgentC")
    edge_ca = next(e for e in topo.edges if e.source == "AgentC" and e.target == "AgentA")

    assert edge_ab.is_cyclic
    assert edge_bc.is_cyclic
    assert edge_ca.is_cyclic


def test_topology_json_serialization():
    spans = [
        _make_span("s1", "Architect", "Developer", tokens=500, duration_ms=200.0),
        _make_span("s2", "Developer", "Architect", tokens=400, duration_ms=180.0),
    ]
    topo = build_topology_from_spans(spans)
    data = topo.to_dict()

    assert "nodes" in data
    assert "edges" in data
    assert "cycles" in data
    assert "total_turns" in data
    assert data["total_turns"] == 2
    assert len(data["nodes"]) == 2
    assert len(data["edges"]) == 2


@pytest.mark.anyio
async def test_topology_endpoint(tmp_path):
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    db_path = str(tmp_path / "topo_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    await storage.initialize()

    # Preload a span
    span_item = _make_span("s_topo_1", "ProductArchitect", "BackendEngineer")
    await storage.save_spans([span_item])

    app = create_app(storage=storage)
    client = TestClient(app)

    res = client.get("/api/v1/topology?swarm_id=swarm-topo-test")
    assert res.status_code == 200
    payload = res.json()
    assert "nodes" in payload
    assert "edges" in payload
    assert "cycles" in payload
    assert payload["total_turns"] == 1
    assert len(payload["nodes"]) == 2
    assert len(payload["edges"]) == 1

    await storage.close()

