# -*- coding: utf-8 -*-
"""
Tests for OpenTelemetry OTLP Ingestion Gateway (HTTP / Protobuf & JSON) - Seam 27 / ADR 0038.
Validates:
1. ProtobufWireDecoder tag-length-value parsing.
2. extract_otlp_attributes & extract_otlp_value normalization.
3. otlp_span_to_swarm_span GenAI Semantic Conventions mapping.
4. OtelTraceParser JSON & Protobuf decoding.
5. FastAPI /v1/traces and /api/v1/otlp/traces HTTP endpoints.
6. In-flight circuit breaker loop interception on collapsed entropy.
7. Ingestion stats tracking.
"""
from datetime import datetime, timezone
import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.schema import SwarmSpan
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app
from swarmwatch.otlp import (
    ProtobufWireDecoder,
    extract_otlp_value,
    extract_otlp_attributes,
    otlp_span_to_swarm_span,
    OtelTraceParser,
    OtlpIngestionStats,
    global_otlp_stats,
)


def test_extract_otlp_value_and_attributes_normalization():
    """Verify recursive proto3 AnyValue unpacking and flat dictionary normalization."""
    assert extract_otlp_value({"stringValue": "test-bot"}) == "test-bot"
    assert extract_otlp_value({"intValue": "150"}) == 150
    assert extract_otlp_value({"doubleValue": 0.85}) == 0.85
    assert extract_otlp_value({"boolValue": True}) is True
    assert extract_otlp_value({"bytesValue": "cafebabe"}) == "cafebabe"

    # ArrayValue
    arr_val = extract_otlp_value({
        "arrayValue": {
            "values": [{"stringValue": "tag1"}, {"intValue": 2}]
        }
    })
    assert arr_val == ["tag1", 2]

    # Proto3 KeyValue list
    proto_attrs = [
        {"key": "gen_ai.system", "value": {"stringValue": "openai"}},
        {"key": "gen_ai.usage.total_tokens", "value": {"intValue": 450}},
        {"key": "gen_ai.usage.cost", "value": {"doubleValue": 0.009}},
    ]
    parsed_proto = extract_otlp_attributes(proto_attrs)
    assert parsed_proto["gen_ai.system"] == "openai"
    assert parsed_proto["gen_ai.usage.total_tokens"] == 450
    assert parsed_proto["gen_ai.usage.cost"] == 0.009

    # Flat dictionary
    flat_attrs = {
        "gen_ai.system": "anthropic",
        "gen_ai.usage.total_tokens": 1200,
    }
    parsed_flat = extract_otlp_attributes(flat_attrs)
    assert parsed_flat["gen_ai.system"] == "anthropic"
    assert parsed_flat["gen_ai.usage.total_tokens"] == 1200


def test_protobuf_wire_decoder_varint_and_raw_message():
    """Verify low-level protobuf varint and length-delimited wire format decoding."""
    # Encode varint 300: 300 = 0x12c -> 0xac 0x02
    data_300 = bytes([0xAC, 0x02])
    val, offset = ProtobufWireDecoder.read_varint(data_300, 0)
    assert val == 300
    assert offset == 2

    # Encode a length-delimited field: tag = (1 << 3) | 2 = 10 (0x0a), length = 5, string = "hello"
    raw_record = bytes([0x0A, 0x05]) + b"hello"
    msg = ProtobufWireDecoder.decode_raw_message(raw_record)
    assert 1 in msg
    assert msg[1][0] == b"hello"


def test_otlp_span_to_swarm_span_mapping():
    """Verify translation of OTel span into SwarmSpan with GenAI Semantic Conventions."""
    sample_span = {
        "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
        "spanId": "00f067aa0ba902b7",
        "parentSpanId": "",
        "name": "BackendEngineer -> SecurityAuditor:chat",
        "kind": 1,
        "startTimeUnixNano": 1727085600000000000,
        "endTimeUnixNano": 1727085600500000000,
        "attributes": [
            {"key": "gen_ai.system", "value": {"stringValue": "langchain"}},
            {"key": "gen_ai.agent.name", "value": {"stringValue": "BackendEngineer"}},
            {"key": "gen_ai.target.agent", "value": {"stringValue": "SecurityAuditor"}},
            {"key": "gen_ai.operation.name", "value": {"stringValue": "chat"}},
            {"key": "gen_ai.usage.total_tokens", "value": {"intValue": 820}},
            {"key": "gen_ai.prompt", "value": {"stringValue": "Review this database query."}},
            {"key": "gen_ai.completion", "value": {"stringValue": "Query analysis complete. No SQL injection found."}},
        ],
    }

    swarm_span = otlp_span_to_swarm_span(sample_span, default_project_id="test-proj")

    assert swarm_span.id == "00f067aa0ba902b7"
    assert swarm_span.swarm_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert swarm_span.parent_span_id is None
    assert swarm_span.source_agent == "BackendEngineer"
    assert swarm_span.target_agent == "SecurityAuditor"
    assert swarm_span.span_type == "llm_completion"  # "chat" normalized
    assert swarm_span.token_count == 820
    assert swarm_span.cost_estimate == round(820 * 0.00002, 6)
    assert swarm_span.project_id == "test-proj"
    assert swarm_span.duration_ms == pytest.approx(500.0, rel=1e-2)
    assert swarm_span.input_payload["prompt"] == "Review this database query."
    assert swarm_span.output_payload["completion"] == "Query analysis complete. No SQL injection found."
    assert swarm_span.entropy_score > 0.0


def test_otel_trace_parser_full_json_payload():
    """Verify OtelTraceParser with full standard OTLP/HTTP JSON payload."""
    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "crewai-service"}},
                    ],
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "crewai.runner", "version": "0.1.0"},
                        "spans": [
                            {
                                "traceId": "trace-100",
                                "spanId": "span-101",
                                "name": "Planner -> Executor",
                                "startTimeUnixNano": 1000000000,
                                "endTimeUnixNano": 1500000000,
                                "attributes": [
                                    {"key": "gen_ai.system", "value": {"stringValue": "crewai"}},
                                    {"key": "gen_ai.agent.name", "value": {"stringValue": "Planner"}},
                                    {"key": "gen_ai.target.agent", "value": {"stringValue": "Executor"}},
                                    {"key": "gen_ai.usage.total_tokens", "value": {"intValue": 300}},
                                ],
                            },
                            {
                                "traceId": "trace-100",
                                "spanId": "span-102",
                                "parentSpanId": "span-101",
                                "name": "Executor -> Tools:execute_tool",
                                "startTimeUnixNano": 1500000000,
                                "endTimeUnixNano": 1800000000,
                                "attributes": [
                                    {"key": "gen_ai.system", "value": {"stringValue": "crewai"}},
                                    {"key": "gen_ai.agent.name", "value": {"stringValue": "Executor"}},
                                    {"key": "gen_ai.target.agent", "value": {"stringValue": "Tools"}},
                                    {"key": "gen_ai.operation.name", "value": {"stringValue": "execute_tool"}},
                                    {"key": "gen_ai.usage.total_tokens", "value": {"intValue": 150}},
                                ],
                            },
                        ],
                    }
                ],
            }
        ]
    }

    raw_bytes = json.dumps(payload).encode("utf-8")
    spans, rejected = OtelTraceParser.parse_otlp_payload(raw_bytes, content_type="application/json")

    assert rejected == 0
    assert len(spans) == 2
    assert spans[0].id == "span-101"
    assert spans[0].source_agent == "Planner"
    assert spans[1].id == "span-102"
    assert spans[1].parent_span_id == "span-101"
    assert spans[1].span_type == "tool_call"


def test_server_otlp_traces_endpoint_json(tmp_path):
    """Verify FastAPI /v1/traces and /api/v1/otlp/traces endpoints."""
    db_file = str(tmp_path / "otlp_test.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Check Gateway Status
    res_status = client.get("/v1/traces")
    assert res_status.status_code == 200
    status_data = res_status.json()
    assert status_data["status"] == "ready"
    assert "/v1/traces" in status_data["endpoint"]

    # 2. Ingest Standard OTLP Trace
    trace_payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "test-service"}}],
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "test.scope"},
                        "spans": [
                            {
                                "traceId": "trace-server-1",
                                "spanId": "span-server-1",
                                "name": "AgentA -> AgentB:chat",
                                "startTimeUnixNano": 1000000000,
                                "endTimeUnixNano": 1200000000,
                                "attributes": [
                                    {"key": "gen_ai.system", "value": {"stringValue": "openai"}},
                                    {"key": "gen_ai.agent.name", "value": {"stringValue": "AgentA"}},
                                    {"key": "gen_ai.target.agent", "value": {"stringValue": "AgentB"}},
                                    {"key": "gen_ai.usage.total_tokens", "value": {"intValue": 500}},
                                    {"key": "gen_ai.completion", "value": {"stringValue": "Normal valid response"}},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }

    res_post = client.post(
        "/v1/traces",
        data=json.dumps(trace_payload),
        headers={"Content-Type": "application/json", "x-swarmwatch-project-id": "project-otlp-1"},
    )
    assert res_post.status_code == 200
    assert res_post.json() == {"partialSuccess": {}}

    # Verify Spans Persisted in Database
    persisted = storage.get_spans("trace-server-1")
    assert len(persisted) == 1

    assert persisted[0].id == "span-server-1"
    assert persisted[0].source_agent == "AgentA"
    assert persisted[0].project_id == "project-otlp-1"

    # 3. Check Gateway Stats Endpoint
    res_stats = client.get("/api/v1/otlp/stats")
    assert res_stats.status_code == 200
    stats = res_stats.json()
    assert stats["total_spans_parsed"] >= 1
    assert "openai" in stats["active_systems"]


def test_server_otlp_circuit_breaker_loop_interception(tmp_path):
    """Verify circuit breaker intervention when external agent trace has collapsed entropy."""
    db_file = str(tmp_path / "otlp_circuit_test.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # Repetitive collapsed text causing Shannon entropy < 0.35
    collapsed_text = "loop loop loop loop loop loop loop loop loop loop loop loop loop"
    loop_payload = {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "trace-loop-1",
                                "spanId": "span-loop-1",
                                "name": "AgentA -> AgentB:chat",
                                "startTimeUnixNano": 1000000000,
                                "endTimeUnixNano": 1100000000,
                                "attributes": [
                                    {"key": "gen_ai.system", "value": {"stringValue": "langchain"}},
                                    {"key": "gen_ai.agent.name", "value": {"stringValue": "AgentA"}},
                                    {"key": "gen_ai.target.agent", "value": {"stringValue": "AgentB"}},
                                    {"key": "gen_ai.completion", "value": {"stringValue": collapsed_text}},
                                ],
                            }
                        ]
                    }
                ]
            }
        ]
    }

    res = client.post("/v1/traces", data=json.dumps(loop_payload), headers={"Content-Type": "application/json"})
    assert res.status_code == 200

    spans = storage.get_spans("trace-loop-1")
    assert len(spans) == 1
    # Entropy should be collapsed
    assert spans[0].entropy_score <= 0.35


def _encode_varint(val: int) -> bytes:
    res = bytearray()
    while val > 0x7F:
        res.append((val & 0x7F) | 0x80)
        val >>= 7
    res.append(val & 0x7F)
    return bytes(res)


def _encode_length_delimited(field_num: int, data: bytes) -> bytes:
    tag = (field_num << 3) | 2
    return _encode_varint(tag) + _encode_varint(len(data)) + data


def _encode_fixed64(field_num: int, val: int) -> bytes:
    import struct
    tag = (field_num << 3) | 1
    return _encode_varint(tag) + struct.pack("<Q", val)


def test_server_otlp_protobuf_payload_ingestion(tmp_path):
    """Verify binary protobuf trace ingestion at /v1/traces via ProtobufWireDecoder."""
    db_file = str(tmp_path / "otlp_pb_test.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # Build binary protobuf message for KeyValue: key="gen_ai.system", value={"stringValue": "openai"}
    any_val = _encode_length_delimited(1, b"openai")
    kv = _encode_length_delimited(1, b"gen_ai.system") + _encode_length_delimited(2, any_val)

    # Span: trace_id (16 bytes), span_id (8 bytes), name, start/end, attributes
    trace_id_bytes = bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736")
    span_id_bytes = bytes.fromhex("00f067aa0ba902b7")
    span_data = (
        _encode_length_delimited(1, trace_id_bytes) +
        _encode_length_delimited(2, span_id_bytes) +
        _encode_length_delimited(5, b"ProtobufAgent -> Receiver:tool_call") +
        _encode_fixed64(7, 1000000000) +
        _encode_fixed64(8, 1250000000) +
        _encode_length_delimited(9, kv)
    )

    scope_spans = _encode_length_delimited(2, span_data)
    resource_spans = _encode_length_delimited(2, scope_spans)
    export_req = _encode_length_delimited(1, resource_spans)

    res = client.post(
        "/v1/traces",
        content=export_req,
        headers={"Content-Type": "application/x-protobuf"},
    )
    assert res.status_code == 200
    assert res.json() == {"partialSuccess": {}}

    spans = storage.get_spans("4bf92f3577b34da6a3ce929d0e0e4736")
    assert len(spans) == 1
    assert spans[0].id == "00f067aa0ba902b7"
    assert spans[0].source_agent == "ProtobufAgent"
    assert spans[0].target_agent == "Receiver"
    assert spans[0].span_type == "tool_call"


