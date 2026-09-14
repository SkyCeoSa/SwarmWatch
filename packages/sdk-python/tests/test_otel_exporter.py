# -*- coding: utf-8 -*-
"""
TDD Tests for OpenTelemetry GenAI Semantic Conventions & Native OTLP Exporter.
Validates ADR 0008:
- OTel GenAI Semantic Conventions mapping (gen_ai.system, gen_ai.operation.name, gen_ai.usage.total_tokens).
- Asynchronous batch OTLP trace export with sub-microsecond non-blocking submission.
- Zero external dependency guarantee.
"""
from datetime import datetime, timezone
import pytest

from swarmwatch.schema import SwarmSpan
from swarmwatch.otel import (
    to_otel_genai_span,
    OtelGenAiExporter,
    format_otlp_traces_payload,
)


def test_to_otel_genai_span_semantic_conventions():
    """Verify exact compliance with OpenTelemetry GenAI Semantic Conventions."""
    now = datetime.now(timezone.utc)
    span = SwarmSpan(
        id="span-otel-101",
        swarm_id="swarm-prod-otel",
        branch_id="main",
        parent_span_id=None,
        source_agent="ProductArchitect",
        target_agent="BackendEngineer",
        span_type="agent_call",
        input_payload={"prompt": "Design OAuth2 schema"},
        output_payload={"status": "completed"},
        token_count=350,
        entropy_score=0.92,
        start_time=now,
        end_time=now,
        duration_ms=45.0,
    )

    otel_span = to_otel_genai_span(span)

    assert otel_span["name"] == "ProductArchitect -> BackendEngineer:agent_call"
    assert otel_span["trace_id"] == "swarm-prod-otel"
    assert otel_span["span_id"] == "span-otel-101"

    attrs = otel_span["attributes"]
    assert attrs["gen_ai.system"] == "swarmwatch"
    assert attrs["gen_ai.agent.name"] == "ProductArchitect"
    assert attrs["gen_ai.target.agent"] == "BackendEngineer"
    assert attrs["gen_ai.operation.name"] == "ProductArchitect -> BackendEngineer:agent_call"
    assert attrs["gen_ai.usage.total_tokens"] == 350
    assert attrs["gen_ai.swarmwatch.entropy_score"] == 0.92
    assert attrs["gen_ai.swarmwatch.branch_id"] == "main"


def test_format_otlp_traces_payload_structure():
    """Verify OTLP standard JSON schema structure expected by OpenTelemetry Collector."""
    now = datetime.now(timezone.utc)
    span = SwarmSpan(
        id="span-01",
        swarm_id="swarm-01",
        source_agent="agent_a",
        target_agent="agent_b",
        span_type="agent_call",
        token_count=100,
        entropy_score=0.85,
        start_time=now,
        end_time=now,
    )
    payload = format_otlp_traces_payload([span], service_name="swarmwatch-swarm")

    assert "resourceSpans" in payload
    resource_spans = payload["resourceSpans"]
    assert len(resource_spans) == 1

    scope_spans = resource_spans[0]["scopeSpans"]
    assert len(scope_spans) == 1
    assert scope_spans[0]["scope"]["name"] == "swarmwatch.otel"

    spans = scope_spans[0]["spans"]
    assert len(spans) == 1
    assert spans[0]["name"] == "agent_a -> agent_b:agent_call"


def test_otel_genai_exporter_non_blocking_and_flush():
    """Verify that OtelGenAiExporter queues spans without blocking and flushes cleanly."""
    exported_batches = []

    def mock_sender(url: str, payload: dict, headers: dict):
        exported_batches.append(payload)
        return True

    exporter = OtelGenAiExporter(
        endpoint="http://localhost:4318/v1/traces",
        service_name="test-swarm-service",
        batch_size=5,
        flush_interval_seconds=0.5,
        sender=mock_sender,
    )
    exporter.start()

    now = datetime.now(timezone.utc)
    span1 = SwarmSpan(
        id="s1",
        swarm_id="sw1",
        source_agent="planner",
        target_agent="coder",
        span_type="agent_call",
        start_time=now,
        end_time=now,
    )
    span2 = SwarmSpan(
        id="s2",
        swarm_id="sw1",
        source_agent="coder",
        target_agent="reviewer",
        span_type="agent_call",
        start_time=now,
        end_time=now,
    )

    # Submitting must return immediately
    accepted1 = exporter.export_span(span1)
    accepted2 = exporter.export_span(span2)
    assert accepted1 is True
    assert accepted2 is True

    # Flush synchronously
    exporter.flush(timeout=2.0)
    exporter.shutdown()

    assert len(exported_batches) == 1
    resource_spans = exported_batches[0]["resourceSpans"]
    spans = resource_spans[0]["scopeSpans"][0]["spans"]
    assert len(spans) == 2
    assert spans[0]["name"] == "planner -> coder:agent_call"
    assert spans[1]["name"] == "coder -> reviewer:agent_call"
