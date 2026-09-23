# -*- coding: utf-8 -*-
"""
Unit and integration test suite for SwarmWatch Distributed Multi-Agent
Root-Cause Causal Inference Engine (Seam 32 / ADR 0043).
"""

from datetime import datetime, timezone
import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.schema import SwarmSpan
from swarmwatch.causal_inference import (
    CausalPathNode,
    ContextPoisoningMetric,
    RootCauseAttribution,
    CausalInferenceResult,
    CausalInferenceEngine,
    global_causal_inference_engine,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.cli import main as cli_main


@pytest.fixture
def inference_engine():
    return CausalInferenceEngine()


def _make_span(
    span_id: str,
    swarm_id: str,
    source: str,
    target: str,
    span_type: str = "turn",
    status: str = "ok",
    entropy: float = 1.85,
    output: dict = None,
    parent_id: str = None,
) -> SwarmSpan:
    now = datetime.now(timezone.utc)
    return SwarmSpan(
        id=span_id,
        swarm_id=swarm_id,
        source_agent=source,
        target_agent=target,
        span_type=span_type,
        status=status,
        entropy_score=entropy,
        parent_span_id=parent_id,
        output_payload=output or {"result": "ok"},
        start_time=now,
        end_time=now,
    )


def test_nominal_execution_no_anomaly(inference_engine):
    spans = [
        _make_span("s1", "swarm-nom", "Architect", "Developer", "turn", "ok", 1.85),
        _make_span("s2", "swarm-nom", "Developer", "Tester", "turn", "ok", 1.90),
    ]
    res = inference_engine.infer_root_cause(spans)
    assert res.has_anomaly is False
    assert res.attribution is None
    assert len(res.context_poisoning) == 0


def test_multi_hop_failure_root_cause_attribution(inference_engine):
    # Turn 1: Upstream Researcher fails database query tool
    # Turn 2: Intermediary Planner acts on missing data
    # Turn 3: Downstream Coder trips when executing invalid code
    spans = [
        _make_span(
            "s1", "swarm-fail", "Researcher", "query_db", "tool", "error", 1.5,
            output={"error": "DatabaseConnectionError: timeout"},
        ),
        _make_span(
            "s2", "swarm-fail", "Planner", "Coder", "turn", "ok", 1.4,
            parent_id="s1",
        ),
        _make_span(
            "s3", "swarm-fail", "Coder", "execute_sql", "tool", "error", 1.2,
            output={"error": "ExecutionFailed: missing relation"},
            parent_id="s2",
        ),
    ]

    res = inference_engine.infer_root_cause(spans)
    assert res.has_anomaly is True
    assert res.attribution is not None

    attr = res.attribution
    assert attr.root_cause_agent == "Researcher"
    assert attr.root_cause_turn == 1
    assert attr.symptom_agent == "Coder"
    assert attr.symptom_turn == 3
    assert attr.propagation_depth == 2
    assert attr.recommended_fork_turn == 1
    assert attr.confidence_score >= 0.5
    assert len(attr.critical_path) == 3
    assert attr.critical_path[0].is_fault_origin is True


def test_context_poisoning_contagion_calculation(inference_engine):
    spans = [
        _make_span("s10", "swarm-poison", "Architect", "tool_fetch", "tool", "error", output={"error": "InvalidSchemaException"}),
        _make_span("s11", "swarm-poison", "DeveloperA", "DeveloperB", "turn", "ok"),
        _make_span("s12", "swarm-poison", "DeveloperB", "Tester", "turn", "ok"),
        _make_span("s13", "swarm-poison", "Tester", "Deployer", "turn", "error", output={"error": "TestRunFailed"}),
    ]

    metrics = inference_engine.compute_context_poisoning(spans)
    assert len(metrics) == 1
    cp = metrics[0]
    assert cp.origin_agent == "Architect"
    assert cp.poison_turn == 1
    assert "DeveloperA" in cp.tainted_agents
    assert "DeveloperB" in cp.tainted_agents
    assert "Tester" in cp.tainted_agents
    assert cp.taint_score > 0.0


def test_entropy_collapse_root_cause_identification(inference_engine):
    spans = [
        _make_span("ent-1", "swarm-ent", "Leader", "Writer", "turn", "ok", 1.8),
        _make_span("ent-2", "swarm-ent", "Writer", "Reviewer", "turn", "ok", 0.12),  # Severe collapse
        _make_span("ent-3", "swarm-ent", "Reviewer", "Leader", "turn", "error", 0.15, output={"error": "RepetitionLoop"}),
    ]

    res = inference_engine.infer_root_cause(spans)
    assert res.has_anomaly is True
    assert res.attribution is not None
    assert res.attribution.root_cause_agent == "Writer"
    assert res.attribution.root_cause_turn == 2
    assert res.attribution.recommended_fork_turn == 1


def test_root_cause_fastapi_endpoints(tmp_path):
    db_file = str(tmp_path / "root_cause_test.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # Ingest a multi-hop failure chain
    spans_payload = [
        {
            "id": "rc-span-01",
            "swarm_id": "swarm-api-rc",
            "source_agent": "Researcher",
            "target_agent": "fetch_api",
            "span_type": "tool",
            "status": "error",
            "output_payload": {"error": "Api404NotFound"},
            "entropy_score": 1.6,
        },
        {
            "id": "rc-span-02",
            "swarm_id": "swarm-api-rc",
            "source_agent": "Coder",
            "target_agent": "Interpreter",
            "span_type": "turn",
            "status": "error",
            "output_payload": {"error": "TypeError: null reference"},
            "entropy_score": 1.2,
        },
    ]
    ing_res = client.post("/api/v1/spans", json=spans_payload)
    assert ing_res.status_code == 200

    # 1. GET /api/v1/swarms/{swarm_id}/root-cause
    rc_res = client.get("/api/v1/swarms/swarm-api-rc/root-cause")
    assert rc_res.status_code == 200
    rc_data = rc_res.json()
    assert rc_data["has_anomaly"] is True
    assert rc_data["attribution"]["root_cause_agent"] == "Researcher"

    # 2. GET /api/v1/swarms/{swarm_id}/causal-path
    path_res = client.get("/api/v1/swarms/swarm-api-rc/causal-path")
    assert path_res.status_code == 200
    path_data = path_res.json()
    assert len(path_data["critical_path"]) >= 2

    # 3. GET /api/v1/swarms/{swarm_id}/context-poisoning
    cp_res = client.get("/api/v1/swarms/swarm-api-rc/context-poisoning")
    assert cp_res.status_code == 200
    cp_data = cp_res.json()
    assert len(cp_data["context_poisoning"]) >= 1


def test_root_cause_cli_command(capsys):
    # Test CLI rootcause json format
    with pytest.raises(SystemExit) as exc:
        cli_main(["rootcause", "--swarm-id", "swarm-test", "--format", "json"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "has_anomaly" in out

    # Test CLI rootcause terminal format
    with pytest.raises(SystemExit) as exc:
        cli_main(["rootcause", "--swarm-id", "swarm-test"])
    assert exc.value.code == 0
    term_out = capsys.readouterr().out
    assert "SWARMWATCH ROOT-CAUSE CAUSAL INFERENCE" in term_out
