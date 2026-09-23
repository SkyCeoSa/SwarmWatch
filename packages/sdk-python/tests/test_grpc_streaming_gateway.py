# -*- coding: utf-8 -*-
"""
Tests for Ultra-High-Throughput gRPC Streaming Ingestion Gateway (Seam 33 / ADR 0044).
"""

import json
import struct
import pytest
from starlette.testclient import TestClient

from swarmwatch.schema import SwarmSpan
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main
from swarmwatch.grpc_gateway import (
    GrpcStatusCode,
    GrpcMessageFramer,
    SwarmProtobufCodec,
    GrpcGatewayStats,
    GrpcStreamingGateway,
    global_grpc_gateway,
    global_grpc_stats,
    GrpcCircuitBreakException,
)


def test_grpc_message_framer_encode_decode():
    """Verify 5-byte gRPC framing, chunk fragmentation, and multi-frame decoding."""
    raw_payload_1 = b'{"msg": "span 1"}'
    raw_payload_2 = b'{"msg": "span 2"}'

    # 1. Single frame encode
    frame_1 = GrpcMessageFramer.encode_frame(raw_payload_1, compressed=False)
    assert len(frame_1) == 5 + len(raw_payload_1)
    flag, length = struct.unpack(">BI", frame_1[:5])
    assert flag == 0
    assert length == len(raw_payload_1)
    assert frame_1[5:] == raw_payload_1

    # 2. Multi-frame stream decode
    frame_2 = GrpcMessageFramer.encode_frame(raw_payload_2, compressed=False)
    stream_buffer = frame_1 + frame_2

    frames, leftover = GrpcMessageFramer.decode_frames(stream_buffer)
    assert len(frames) == 2
    assert frames[0] == raw_payload_1
    assert frames[1] == raw_payload_2
    assert leftover == b""

    # 3. Chunk fragmentation reassembly
    # Split the buffer halfway through the second frame
    split_idx = len(frame_1) + 3
    chunk_a = stream_buffer[:split_idx]
    chunk_b = stream_buffer[split_idx:]

    frames_a, leftover_a = GrpcMessageFramer.decode_frames(chunk_a)
    assert len(frames_a) == 1
    assert frames_a[0] == raw_payload_1
    assert len(leftover_a) == 3

    # Reassemble with chunk_b
    frames_b, leftover_b = GrpcMessageFramer.decode_frames(leftover_a + chunk_b)
    assert len(frames_b) == 1
    assert frames_b[0] == raw_payload_2
    assert leftover_b == b""


def test_swarm_protobuf_codec_span_parsing():
    """Verify decoding of both JSON-over-gRPC and binary TLV protobuf spans."""
    # 1. JSON payload inside gRPC frame
    json_span = {
        "id": "span-grpc-01",
        "trace_id": "trace-grpc-01",
        "swarm_id": "swarm-grpc-01",
        "source_agent": "planner_agent",
        "target_agent": "coder_agent",
        "span_type": "turn",
        "token_count": 250,
        "entropy_score": 0.82,
        "status": "ok",
    }
    payload_json = json.dumps([json_span]).encode("utf-8")
    spans = SwarmProtobufCodec.decode_spans(payload_json, default_project_id="test-proj")
    assert len(spans) == 1
    assert spans[0].id == "span-grpc-01"
    assert spans[0].source_agent == "planner_agent"
    assert spans[0].entropy_score == 0.82

    # 2. Binary Protobuf TLV field construction
    # Field 1 (id, string): tag = 1 << 3 | 2 = 10
    # Field 5 (source_agent, string): tag = 5 << 3 | 2 = 42
    # Field 8 (token_count, varint): tag = 8 << 3 | 0 = 64
    proto_buf = bytearray()
    # Field 1
    id_bytes = b"span-proto-01"
    proto_buf.extend([10, len(id_bytes)])
    proto_buf.extend(id_bytes)
    # Field 5
    src_bytes = b"lead_architect"
    proto_buf.extend([42, len(src_bytes)])
    proto_buf.extend(src_bytes)
    # Field 8: token_count = 150 (varint: 0x96, 0x01)
    proto_buf.extend([64, 0x96, 0x01])

    proto_spans = SwarmProtobufCodec.decode_spans(bytes(proto_buf), default_project_id="proto-proj")
    assert len(proto_spans) == 1
    assert proto_spans[0].id == "span-proto-01"
    assert proto_spans[0].source_agent == "lead_architect"
    assert proto_spans[0].token_count == 150


@pytest.mark.asyncio
async def test_grpc_streaming_gateway_telemetry_service(tmp_path):
    """Verify GrpcStreamingGateway in-process stream ingestion and metrics."""
    db_file = str(tmp_path / "grpc_gw_test.db")
    storage = SQLiteStorageAdapter(db_file)
    stats = GrpcGatewayStats()
    gw = GrpcStreamingGateway(stats=stats)
    gw.set_storage(storage)

    span_data = [{
        "id": "span-gw-01",
        "trace_id": "trace-gw-01",
        "swarm_id": "swarm-gw-01",
        "source_agent": "orchestrator",
        "span_type": "turn",
        "token_count": 100,
        "entropy_score": 0.95,
        "status": "ok",
    }]
    framed = GrpcMessageFramer.encode_frame(json.dumps(span_data).encode("utf-8"))

    responses, status_code, err = await gw.handle_grpc_stream_chunk(
        chunk=framed,
        service_path="/swarmwatch.v1.TelemetryService/StreamSpans",
        project_id="gw-proj-1",
    )

    assert status_code == GrpcStatusCode.OK
    assert err is None
    assert len(responses) == 1

    # Verify persisted in storage
    spans = await storage.get_swarm_spans("swarm-gw-01")
    assert len(spans) == 1
    assert spans[0].source_agent == "orchestrator"

    # Verify stats
    st = gw.status()
    assert st["total_spans_received"] >= 1
    assert st["total_bytes_received"] > 0
    assert "/swarmwatch.v1.TelemetryService/StreamSpans" in st["active_services"]


@pytest.mark.asyncio
async def test_grpc_streaming_gateway_circuit_breaker_interception(tmp_path):
    """Verify in-flight circuit tripping when streaming spans with collapsed entropy."""
    db_file = str(tmp_path / "grpc_circuit_test.db")
    storage = SQLiteStorageAdapter(db_file)
    stats = GrpcGatewayStats()
    gw = GrpcStreamingGateway(stats=stats)
    gw.set_storage(storage)

    # Low entropy span: 0.15 <= 0.35
    collapsed_span = [{
        "id": "span-collapsed-01",
        "trace_id": "trace-collapsed-01",
        "swarm_id": "swarm-collapse-01",
        "source_agent": "looping_agent",
        "span_type": "turn",
        "token_count": 500,
        "entropy_score": 0.15,
        "status": "ok",
    }]
    framed = GrpcMessageFramer.encode_frame(json.dumps(collapsed_span).encode("utf-8"))

    responses, status_code, err = await gw.handle_grpc_stream_chunk(
        chunk=framed,
        service_path="/swarmwatch.v1.TelemetryService/StreamSpans",
        project_id="proj-circuit",
    )

    assert status_code == GrpcStatusCode.RESOURCE_EXHAUSTED
    assert "Runaway Loop Detected via gRPC Gateway" in str(err)
    assert stats.trips_triggered >= 1


def test_grpc_fastapi_endpoints(tmp_path):
    """Verify FastAPI gRPC streaming endpoints, status, and simulation."""
    db_file = str(tmp_path / "grpc_api_test.db")
    storage = SQLiteStorageAdapter(db_file)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Ingest via gRPC endpoint
    span_payload = [{
        "id": "span-api-grpc-1",
        "trace_id": "trace-api-grpc-1",
        "swarm_id": "swarm-api-grpc-1",
        "source_agent": "api_agent",
        "span_type": "turn",
        "token_count": 80,
        "entropy_score": 0.90,
        "status": "ok",
    }]
    framed = GrpcMessageFramer.encode_frame(json.dumps(span_payload).encode("utf-8"))

    res = client.post(
        "/swarmwatch.v1.TelemetryService/StreamSpans",
        content=framed,
        headers={"Content-Type": "application/grpc"},
    )
    assert res.status_code == 200
    assert res.headers.get("grpc-status") == "0"
    assert res.headers.get("content-type") == "application/grpc"

    # 2. Check Gateway Status
    res_status = client.get("/api/v1/grpc/status")
    assert res_status.status_code == 200
    status_data = res_status.json()
    assert "total_spans_received" in status_data
    assert "status" in status_data

    # 3. Simulate gRPC stream
    res_sim = client.post("/api/v1/grpc/simulate?spans_count=5&induce_loop=false")
    assert res_sim.status_code == 200
    sim_data = res_sim.json()
    assert sim_data["spans_sent"] == 5
    assert sim_data["grpc_status"] == "OK"

    # 4. Simulate loop trip
    res_loop = client.post("/api/v1/grpc/simulate?spans_count=5&induce_loop=true")
    assert res_loop.status_code == 200
    loop_data = res_loop.json()
    assert loop_data["grpc_status"] == "RESOURCE_EXHAUSTED"


def test_grpc_cli_commands(capsys):
    """Verify CLI subcommands for gRPC gateway status and simulation."""
    # 1. Status command (JSON format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["grpc", "status", "--format", "json"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "total_spans_received" in out

    # 2. Status command (Terminal format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["grpc", "status"])
    assert exc.value.code == 0
    term_out = capsys.readouterr().out
    assert "SWARMWATCH ULTRA-HIGH-THROUGHPUT gRPC GATEWAY" in term_out

    # 3. Simulate command (Terminal format)
    with pytest.raises(SystemExit) as exc:
        cli_main(["grpc", "simulate", "--spans", "3"])
    assert exc.value.code == 0
    sim_out = capsys.readouterr().out
    assert "gRPC Stream Simulation Result" in sim_out
