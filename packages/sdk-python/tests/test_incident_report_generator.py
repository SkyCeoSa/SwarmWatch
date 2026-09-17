# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Incident Post-Mortem & Financial ROI Report Generator (Seam 11 / ADR 0020).
"""

from datetime import datetime, timezone
import json
import os
import tempfile
import pytest
from starlette.testclient import TestClient

from swarmwatch.schema import (
    CircuitEvent,
    CounterfactualSavings,
    SwarmSpan,
)
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app
from swarmwatch.report import (
    IncidentReportGenerator,
    IncidentPostMortem,
    FinancialRoiMetrics,
    RootCauseDiagnosis,
)


@pytest.fixture
def temp_db():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_report.db")
    storage = SQLiteStorageAdapter(db_path)
    yield storage
    try:
        storage.close_sync()
    except Exception:
        pass


def test_generate_report_completed_healthy_swarm(temp_db):
    """Verifies report generation for a healthy swarm that completed without incident."""
    swarm_id = "swarm-healthy-100"
    now_iso = datetime.now(timezone.utc).isoformat()

    spans = [
        SwarmSpan(
            span_id=f"span-{i}",
            swarm_id=swarm_id,
            source_agent="Planner",
            target_agent="Executor",
            step_index=i,
            entropy_score=0.88 - (i * 0.02),
            similarity_score=0.15,
            tokens_in=100,
            tokens_out=200,
            total_tokens=300,
            start_time=now_iso,
            input_payload={"prompt": f"Step {i} task"},
            output_payload={"result": f"Execution result {i}"},
        )
        for i in range(3)
    ]
    temp_db.save_spans_sync(spans)

    generator = IncidentReportGenerator(storage=temp_db)
    post_mortem = generator.analyze(swarm_id)

    assert isinstance(post_mortem, IncidentPostMortem)
    assert post_mortem.swarm_id == swarm_id
    assert post_mortem.status == "COMPLETED_SUCCESSFULLY"
    assert post_mortem.total_spans == 3
    assert post_mortem.circuit_tripped is False
    assert len(post_mortem.entropy_trajectory) == 3

    # Formats
    md = generator.generate_report(swarm_id, format="markdown")
    assert f"# SwarmWatch Incident Post-Mortem: {swarm_id}" in md
    assert "COMPLETED_SUCCESSFULLY" in md
    assert "Entropy Trajectory" in md

    data = generator.generate_report(swarm_id, format="json")
    assert isinstance(data, dict)
    assert data["status"] == "COMPLETED_SUCCESSFULLY"

    term = generator.generate_report(swarm_id, format="terminal")
    assert "SWARMWATCH INCIDENT POST-MORTEM" in term


def test_generate_report_circuit_tripped_with_roi(temp_db):
    """Verifies forensic RCA and financial ROI metrics when circuit breaker trips."""
    swarm_id = "swarm-loop-trip-200"
    now_iso = datetime.now(timezone.utc).isoformat()

    spans = [
        SwarmSpan(
            span_id="span-1",
            swarm_id=swarm_id,
            source_agent="BackendEngineer",
            target_agent="SecurityAuditor",
            step_index=1,
            entropy_score=0.75,
            similarity_score=0.20,
            tokens_in=250,
            tokens_out=500,
            total_tokens=750,
            start_time=now_iso,
            input_payload={"code": "def auth(): return True"},
            output_payload={"review": "Please add password validation"},
        ),
        SwarmSpan(
            span_id="span-2",
            swarm_id=swarm_id,
            source_agent="SecurityAuditor",
            target_agent="BackendEngineer",
            step_index=2,
            entropy_score=0.18,
            similarity_score=0.92,
            tokens_in=500,
            tokens_out=500,
            total_tokens=1000,
            start_time=now_iso,
            input_payload={"prompt": "Same review again and again"},
            output_payload={"review": "Same review again and again"},
        ),
    ]
    temp_db.save_spans_sync(spans)

    # Save Circuit Event
    circuit_event = CircuitEvent(
        event_id="evt-101",
        swarm_id=swarm_id,
        span_id="span-2",
        event_type="TRIP",
        reason="Shannon entropy collapsed to 0.180 (threshold: 0.350)",
        entropy_score=0.18,
        similarity_score=0.92,
        tokens_used=1750,
        counterfactual_savings=CounterfactualSavings(
            tokens_saved=241500,
            dollars_saved=4.83,
            turns_prevented=48,
        ),
        created_at=now_iso,
    )
    temp_db.record_circuit_event_sync(circuit_event)

    generator = IncidentReportGenerator(storage=temp_db)
    post_mortem = generator.analyze(swarm_id)

    assert post_mortem.circuit_tripped is True
    assert post_mortem.status == "HALTED_BY_CIRCUIT_BREAKER"
    assert post_mortem.roi.dollars_saved == pytest.approx(4.83, 0.01)
    assert post_mortem.roi.tokens_saved == 241500
    assert post_mortem.roi.turns_prevented == 48

    # Root Cause Diagnosis
    assert post_mortem.root_cause is not None
    assert post_mortem.root_cause.source_agent == "SecurityAuditor"
    assert post_mortem.root_cause.target_agent == "BackendEngineer"
    assert post_mortem.root_cause.trigger_span_id == "span-2"
    assert "Shannon entropy collapsed" in post_mortem.root_cause.trigger_reason

    # Markdown check
    md = generator.generate_report(swarm_id, format="markdown")
    assert "$4.83" in md
    assert "241,500" in md
    assert "HALTED_BY_CIRCUIT_BREAKER" in md
    assert "Root Cause Analysis (RCA)" in md
    assert "SecurityAuditor -> BackendEngineer" in md or "SecurityAuditor" in md


def test_security_redaction_audit_tally(temp_db):
    """Verifies that the report generator tallies redacted secrets across payloads."""
    swarm_id = "swarm-security-tally-300"
    now_iso = datetime.now(timezone.utc).isoformat()

    spans = [
        SwarmSpan(
            span_id="span-sec-1",
            swarm_id=swarm_id,
            source_agent="DevOps",
            target_agent="Database",
            step_index=1,
            entropy_score=0.85,
            total_tokens=200,
            start_time=now_iso,
            input_payload={
                "db": "[REDACTED_DB_DSN]",
                "auth": "Bearer [REDACTED_JWT]",
            },
            output_payload={
                "result": "Config with [REDACTED_AWS_KEY] and [REDACTED_API_KEY]"
            },
        ),
        SwarmSpan(
            span_id="span-sec-2",
            swarm_id=swarm_id,
            source_agent="DevOps",
            target_agent="Mail",
            step_index=2,
            entropy_score=0.82,
            total_tokens=150,
            start_time=now_iso,
            input_payload={"email": "[REDACTED_EMAIL]"},
            output_payload={"status": "sent to [REDACTED_EMAIL]"},
        ),
    ]
    temp_db.save_spans_sync(spans)

    generator = IncidentReportGenerator(storage=temp_db)
    post_mortem = generator.analyze(swarm_id)

    assert post_mortem.security_summary.total_redacted >= 5
    assert post_mortem.security_summary.redacted_categories.get("DB_DSN", 0) >= 1
    assert post_mortem.security_summary.redacted_categories.get("JWT", 0) >= 1
    assert post_mortem.security_summary.redacted_categories.get("AWS_KEY", 0) >= 1
    assert post_mortem.security_summary.redacted_categories.get("API_KEY", 0) >= 1
    assert post_mortem.security_summary.redacted_categories.get("EMAIL", 0) >= 2

    md = generator.generate_report(swarm_id, format="markdown")
    assert "Security & Credential Neutralization Audit" in md
    assert "DB_DSN" in md
    assert "AWS_KEY" in md


def test_rest_api_report_endpoint(temp_db):
    """Verifies GET /api/v1/swarms/{id}/report in both JSON and Markdown formats."""
    swarm_id = "swarm-api-endpoint-400"
    now_iso = datetime.now(timezone.utc).isoformat()

    spans = [
        SwarmSpan(
            span_id="span-api-1",
            swarm_id=swarm_id,
            source_agent="Agent1",
            target_agent="Agent2",
            step_index=1,
            entropy_score=0.90,
            total_tokens=100,
            start_time=now_iso,
            input_payload={"task": "init"},
            output_payload={"status": "ok"},
        )
    ]
    temp_db.save_spans_sync(spans)

    app = create_app(temp_db)
    client = TestClient(app)

    # Test format=json
    res_json = client.get(f"/api/v1/swarms/{swarm_id}/report?format=json")
    assert res_json.status_code == 200
    data = res_json.json()
    assert data["swarm_id"] == swarm_id
    assert data["total_spans"] == 1

    # Test format=markdown
    res_md = client.get(f"/api/v1/swarms/{swarm_id}/report?format=markdown")
    assert res_md.status_code == 200
    assert res_md.headers["content-type"].startswith("text/markdown")
    assert f"# SwarmWatch Incident Post-Mortem: {swarm_id}" in res_md.text

    # Non-existent swarm
    res_404 = client.get("/api/v1/swarms/non-existent-9999/report")
    assert res_404.status_code == 404


def test_cli_report_subcommand(temp_db, monkeypatch, capsys):
    """Verifies that 'swarmwatch report' CLI prints post-mortem output."""
    swarm_id = "swarm-cli-test-500"
    span = SwarmSpan(
        id="span-cli-1",
        swarm_id=swarm_id,
        source_agent="AgentA",
        target_agent="AgentB",
        step_index=1,
        total_tokens=150,
    )
    temp_db.save_spans_sync([span])

    from swarmwatch.cli import main
    monkeypatch.setattr(
        "sys.argv",
        ["swarmwatch", "report", "--swarm-id", swarm_id, "--format", "terminal", "--db", temp_db.db_path]
    )
    main()
    captured = capsys.readouterr()
    assert "SWARMWATCH INCIDENT POST-MORTEM" in captured.out
    assert swarm_id in captured.out
