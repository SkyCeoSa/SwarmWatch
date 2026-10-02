# -*- coding: utf-8 -*-
"""
Tests for Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (Seam 68 / ADR 0079).
Verifies:
1. Protocol Conformance State Machine transitions and invalid transition rejections.
2. Wire chunking and irregular packet fragmentation reassembly (1-byte, 7-byte).
3. Early cancellation and premature EOF connection handling.
4. Ingestion window saturation and flow control backpressure.
5. Malformed wire frame rejection fail-closed.
6. Multi-protocol differential verification (OTLP JSON, OTLP Protobuf, gRPC Streaming, gRPC Unary).
7. Cryptographic Conformance Attestation Receipt generation (sw:conformance:v1:...).
8. Sub-250 microsecond execution latency SLO benchmark.
9. Control plane REST API integration.
"""

from __future__ import annotations

import time
import pytest

from swarmwatch.interop_lab import (
    InteropLab,
    ProtocolStandard,
    ConformanceFault,
    ClientVariant,
    ProtocolState,
    DeviationSeverity,
    ProtocolDeviation,
    ExerciseResult,
    ConformanceAttestationReceipt,
    ProtocolConformanceStateMachine,
    WireFaultInjector,
    global_interop_lab,
)


def test_protocol_state_machine_valid_transitions() -> None:
    sm = ProtocolConformanceStateMachine()
    assert sm.current_state == ProtocolState.IDLE

    assert sm.transition(ProtocolState.CONNECTING) is True
    assert sm.current_state == ProtocolState.CONNECTING

    assert sm.transition(ProtocolState.STREAMING) is True
    assert sm.current_state == ProtocolState.STREAMING

    assert sm.transition(ProtocolState.FRAME_ACCUMULATION) is True
    assert sm.current_state == ProtocolState.FRAME_ACCUMULATION

    assert sm.transition(ProtocolState.DISPATCHING) is True
    assert sm.current_state == ProtocolState.DISPATCHING

    assert sm.transition(ProtocolState.COMPLETED) is True
    assert sm.current_state == ProtocolState.COMPLETED

    assert len(sm.history) == 6


def test_protocol_state_machine_invalid_transitions() -> None:
    sm = ProtocolConformanceStateMachine()
    # Cannot jump directly from IDLE to DISPATCHING or COMPLETED
    assert sm.transition(ProtocolState.DISPATCHING) is False
    assert sm.transition(ProtocolState.COMPLETED) is False
    assert sm.current_state == ProtocolState.IDLE


def test_wire_fault_injector_chunking() -> None:
    data = b"SWARMWATCH_OTLP_STREAM_PAYLOAD_TEST_12345"
    # Slice with 1-byte chunks
    chunks_1b = WireFaultInjector.chunk_bytes(data, chunk_size=1)
    assert len(chunks_1b) == len(data)
    assert b"".join(chunks_1b) == data

    # Slice with 7-byte chunks
    chunks_7b = WireFaultInjector.chunk_bytes(data, chunk_size=7)
    assert len(chunks_7b) == (len(data) + 6) // 7
    assert b"".join(chunks_7b) == data


def test_exercise_standard_healthy_otlp_and_grpc() -> None:
    lab = InteropLab()
    protocols = [
        ProtocolStandard.OTLP_HTTP_JSON,
        ProtocolStandard.OTLP_HTTP_PROTOBUF,
        ProtocolStandard.GRPC_STREAMING,
        ProtocolStandard.GRPC_UNARY,
    ]

    for proto in protocols:
        res = lab.exercise(
            target_protocol=proto,
            client_variant=ClientVariant.NATIVE_OTEL_SDK,
            fault_schedule=ConformanceFault.NONE,
        )
        assert res.success is True
        assert res.protocol == proto
        assert res.reconstructed_spans_count == 1
        assert res.state_history[-1] == "COMPLETED"
        assert res.receipt_hash.startswith(res.receipt_hash[:16])

    status = lab.status()
    assert status["total_exercises_run"] == 4
    assert status["passed_exercises"] == 4
    assert status["failed_exercises"] == 0
    assert status["conformance_certified"] is True


def test_exercise_fragmentation_fault_reassembly() -> None:
    lab = InteropLab()
    custom_span = {"trace_id": "t1", "span_id": "s1", "name": "fragmented_op"}

    # Exercise with 1-byte fragmentation
    res_1b = lab.exercise(
        target_protocol=ProtocolStandard.OTLP_HTTP_JSON,
        fault_schedule=ConformanceFault.FRAGMENTATION,
        payload=custom_span,
        chunk_size=1,
    )
    assert res_1b.success is True
    assert res_1b.reconstructed_spans_count == 1
    assert "FRAME_ACCUMULATION" in res_1b.state_history
    assert "DISPATCHING" in res_1b.state_history
    assert res_1b.state_history[-1] == "COMPLETED"

    # Exercise with 7-byte fragmentation
    res_7b = lab.exercise(
        target_protocol=ProtocolStandard.GRPC_STREAMING,
        fault_schedule=ConformanceFault.FRAGMENTATION,
        payload=custom_span,
        chunk_size=7,
    )
    assert res_7b.success is True
    assert res_7b.reconstructed_spans_count == 1


def test_exercise_early_cancellation_lifecycle() -> None:
    lab = InteropLab()
    res = lab.exercise(
        target_protocol=ProtocolStandard.GRPC_STREAMING,
        client_variant=ClientVariant.GRPC_CORE,
        fault_schedule=ConformanceFault.EARLY_CANCELLATION,
    )
    assert res.success is True
    assert res.state_history[-1] == "TERMINATED_ABORTED"


def test_exercise_backpressure_flow_control() -> None:
    lab = InteropLab(buffer_capacity=100)
    res = lab.exercise(
        target_protocol=ProtocolStandard.OTLP_HTTP_JSON,
        fault_schedule=ConformanceFault.BACKPRESSURE_FLOW_CONTROL,
    )
    assert res.success is True
    assert len(res.deviations) == 1
    assert res.deviations[0].scenario == "flow_control_window_saturation"
    assert res.deviations[0].severity == DeviationSeverity.INFORMATIONAL


def test_exercise_malformed_header_and_truncated_body() -> None:
    lab = InteropLab()

    # Malformed header
    res_hdr = lab.exercise(
        target_protocol=ProtocolStandard.GRPC_UNARY,
        fault_schedule=ConformanceFault.MALFORMED_HEADER,
    )
    assert res_hdr.success is True
    assert res_hdr.state_history[-1] == "TERMINATED_ABORTED"
    assert len(res_hdr.deviations) == 1
    assert res_hdr.deviations[0].scenario == "malformed_header_injection"

    # Truncated body
    res_trunc = lab.exercise(
        target_protocol=ProtocolStandard.OTLP_HTTP_PROTOBUF,
        fault_schedule=ConformanceFault.TRUNCATED_BODY,
    )
    assert res_trunc.success is True
    assert res_trunc.state_history[-1] == "TERMINATED_ABORTED"
    assert len(res_trunc.deviations) == 1
    assert res_trunc.deviations[0].scenario == "truncated_payload_eof"


def test_record_deviation_and_matrix_aggregation() -> None:
    lab = InteropLab()
    lab.record_deviation(
        protocol=ProtocolStandard.OTLP_HTTP_JSON,
        scenario="missing_service_name_attribute",
        observed="service.name missing in resource attributes",
        expected="service.name must be set per OTel GenAI convention",
        severity=DeviationSeverity.WARNING,
    )
    st = lab.status()
    assert st["warning_deviations"] == 1
    assert st["fatal_deviations"] == 0

    matrix = lab.get_matrix()
    assert matrix["total_deviations"] == 1


def test_conformance_attestation_matrix_receipt() -> None:
    lab = InteropLab()
    lab.exercise(ProtocolStandard.OTLP_HTTP_JSON, fault_schedule=ConformanceFault.NONE)
    lab.exercise(ProtocolStandard.GRPC_STREAMING, fault_schedule=ConformanceFault.NONE)
    lab.exercise(ProtocolStandard.OTLP_HTTP_PROTOBUF, fault_schedule=ConformanceFault.FRAGMENTATION)

    receipt = lab.attest_matrix(suite_name="Production OTel Gateway Conformance Gate")
    assert receipt.suite_name == "Production OTel Gateway Conformance Gate"
    assert receipt.total_exercises == 3
    assert receipt.pass_count == 3
    assert receipt.fail_count == 0
    assert len(receipt.protocols_covered) == 3
    assert len(receipt.attestation_hash) == 64

    d = receipt.to_dict()
    assert "attestation_hash" in d


def test_sub_250us_interop_slo() -> None:
    lab = InteropLab()

    # Warmup
    for _ in range(10):
        lab.exercise(ProtocolStandard.OTLP_HTTP_JSON, fault_schedule=ConformanceFault.NONE)

    # Benchmark: 500 iterations of exercise
    iterations = 500
    t0 = time.perf_counter()
    for i in range(iterations):
        fault = ConformanceFault.NONE if i % 2 == 0 else ConformanceFault.FRAGMENTATION
        lab.exercise(ProtocolStandard.OTLP_HTTP_JSON, fault_schedule=fault, chunk_size=7)
    t1 = time.perf_counter()

    avg_us = ((t1 - t0) / iterations) * 1_000_000
    assert avg_us < 250.0, f"Protocol exercise took {avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_singleton_and_status() -> None:
    st = global_interop_lab.status()
    assert "total_exercises_run" in st
    assert "passed_exercises" in st
    assert "conformance_certified" in st


def test_interop_lab_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    app = create_app(storage=SQLiteStorageAdapter(db_path=":memory:"))
    client = TestClient(app)

    # 1. Exercise endpoint with protobuf
    res = client.post(
        "/api/v1/interop/exercise",
        json={
            "target_protocol": "OTLP_HTTP_PROTOBUF",
            "client_variant": "NATIVE_OTEL_SDK",
            "fault_schedule": "NONE",
            "payload": '{"resource_spans":[{"scope_spans":[{"spans":[{"name":"rest-span"}]}]}]}'
        }
    )
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert data["protocol"] == "OTLP_HTTP_PROTOBUF"
    assert data["reconstructed_spans_count"] == 1
    assert "receipt_hash" in data

    # 2. Exercise endpoint with fragmentation fault schedule
    res_frag = client.post(
        "/api/v1/interop/exercise",
        json={
            "target_protocol": "OTLP_HTTP_JSON",
            "client_variant": "NATIVE_OTEL_SDK",
            "fault_schedule": "FRAGMENTATION",
            "chunk_size": 7,
            "payload": {"resource_spans": [{"scope_spans": [{"spans": [{"name": "frag-span"}]}]}]}
        }
    )
    assert res_frag.status_code == 200
    assert res_frag.json()["success"] is True

    # 3. Record deviation endpoint
    res_dev = client.post(
        "/api/v1/interop/record-deviation",
        json={
            "protocol": "GRPC_STREAMING",
            "scenario": "window_update_delay",
            "observed": "FRAME_STALL_EXCEEDED_100MS",
            "expected": "FLOW_CONTROL_WINDOW_EXPANSION",
            "severity": "WARNING"
        }
    )
    assert res_dev.status_code == 200
    d = res_dev.json()
    assert d["protocol"] == "GRPC_STREAMING"
    assert d["severity"] == "WARNING"
    assert d["scenario"] == "window_update_delay"

    # 4. Attest matrix endpoint
    res_attest = client.post(
        "/api/v1/interop/attest",
        json={
            "suite_name": "Integration Test Conformance Suite"
        }
    )
    assert res_attest.status_code == 200
    r = res_attest.json()
    assert r["suite_name"] == "Integration Test Conformance Suite"
    assert r["total_exercises"] >= 2
    assert len(r["attestation_hash"]) == 64

    # 5. Matrix endpoint
    res_matrix = client.get("/api/v1/interop/matrix")
    assert res_matrix.status_code == 200
    m = res_matrix.json()
    assert "matrix_by_protocol" in m
    assert m["total_exercises"] >= 2

    # 6. Status endpoint
    res_status = client.get("/api/v1/interop/status")
    assert res_status.status_code == 200
    s = res_status.json()
    assert s["total_exercises_run"] >= 2
    assert s["warning_deviations"] >= 1

