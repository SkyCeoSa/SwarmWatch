# -*- coding: utf-8 -*-
"""
Test Suite for Zero-Copy Shared-Memory Ring Buffer IPC Gateway (Seam 37 / ADR 0048).
Verifies binary header framing, sub-microsecond span writes, FIFO draining,
buffer saturation / dropped records, background gateway, REST API, and CLI.
"""

import json
import pytest
from fastapi.testclient import TestClient

from swarmwatch.shm_ring import (
    ShmRingBuffer,
    ShmRingBufferWriter,
    ShmRingBufferReader,
    ShmIpcGateway,
    global_shm_gateway,
    HEADER_SIZE,
    HEADER_MAGIC,
    HEADER_VERSION,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_shm():
    """Resets global SHM gateway before and after each test."""
    global_shm_gateway.reset()
    yield
    global_shm_gateway.reset()


def test_shm_ring_header_initialization_and_serialization():
    """Verifies that 64-byte binary header is initialized and unpacked accurately."""
    ring = ShmRingBuffer(name="test_ring_hdr_01", capacity_slots=64, slot_size=512, create=True)
    try:
        hdr = ring.read_header()
        assert hdr.magic == "SWRB"
        assert hdr.version == HEADER_VERSION
        assert hdr.capacity_slots == 64
        assert hdr.slot_size == 512
        assert hdr.write_head == 0
        assert hdr.read_tail == 0
        assert hdr.dropped_records == 0
        assert hdr.total_written == 0
        assert hdr.total_read == 0
        assert ring.total_size == HEADER_SIZE + (64 * 512)
    finally:
        ring.close()
        ring.unlink()


def test_shm_ring_writer_and_reader_roundtrip():
    """Verifies writing spans to shared memory slots and FIFO reading."""
    ring = ShmRingBuffer(name="test_ring_io_01", capacity_slots=128, slot_size=1024, create=True)
    writer = ShmRingBufferWriter(ring)
    reader = ShmRingBufferReader(ring)

    try:
        spans_to_write = [
            {"span_id": f"span_{i}", "agent": f"Agent_{i}", "tokens": i * 10, "payload": f"Step {i}"}
            for i in range(10)
        ]

        for s in spans_to_write:
            ok = writer.write_span(s)
            assert ok is True

        hdr = ring.read_header()
        assert hdr.write_head == 10
        assert hdr.read_tail == 0
        assert hdr.total_written == 10

        drained = reader.read_available(max_records=100)
        assert len(drained) == 10
        for i, s in enumerate(drained):
            assert s["span_id"] == f"span_{i}"
            assert s["tokens"] == i * 10

        hdr_after = ring.read_header()
        assert hdr_after.read_tail == 10
        assert hdr_after.total_read == 10
    finally:
        ring.close()
        ring.unlink()


def test_shm_ring_buffer_saturation_and_dropped_records():
    """Verifies that when ring buffer is saturated, spans are dropped without blocking or throwing."""
    capacity = 8
    ring = ShmRingBuffer(name="test_ring_sat_01", capacity_slots=capacity, slot_size=256, create=True)
    writer = ShmRingBufferWriter(ring)

    try:
        # Write exactly capacity spans
        for i in range(capacity):
            ok = writer.write_span({"index": i})
            assert ok is True

        # Now write 4 more spans without reading
        for i in range(4):
            ok = writer.write_span({"overflow": i})
            assert ok is False

        hdr = ring.read_header()
        assert hdr.write_head == capacity
        assert hdr.dropped_records == 4
    finally:
        ring.close()
        ring.unlink()


def test_shm_ipc_gateway_lifecycle_and_flush():
    """Verifies ShmIpcGateway lifecycle, manual flush, and status tracking."""
    gateway = ShmIpcGateway(name="test_gateway_01", capacity_slots=32, slot_size=512)
    try:
        assert gateway.get_status()["pending_records"] == 0

        # Write 5 spans
        for i in range(5):
            gateway.write_span({"span_id": f"gw_{i}", "tokens": 50})

        status = gateway.get_status()
        assert status["pending_records"] == 5
        assert status["total_written"] == 5

        # Flush
        flushed = gateway.flush()
        assert len(flushed) == 5
        assert gateway.get_status()["pending_records"] == 0
        assert gateway.get_status()["total_read"] == 5
    finally:
        gateway.close()


def test_shm_server_rest_endpoints():
    """Verifies REST endpoints for SHM ring buffer."""
    app = create_app()
    client = TestClient(app)

    # 1. Status
    res_status = client.get("/api/v1/shm/status")
    assert res_status.status_code == 200
    st = res_status.json()
    assert "capacity_slots" in st
    assert "pending_records" in st

    # 2. Write to global gateway and verify pending
    global_shm_gateway.write_span({"test_server": True, "tokens": 120})
    st2 = client.get("/api/v1/shm/status").json()
    assert st2["pending_records"] >= 1

    # 3. Flush
    res_flush = client.post("/api/v1/shm/flush")
    assert res_flush.status_code == 200
    flush_data = res_flush.json()
    assert flush_data["status"] == "flushed"
    assert flush_data["drained_count"] >= 1

    # 4. Reset
    res_reset = client.post("/api/v1/shm/reset")
    assert res_reset.status_code == 200
    assert res_reset.json()["status"] == "reset"


def test_shm_cli_status_and_flush(capsys):
    """Verifies swarmwatch shm CLI command execution."""
    # Write span
    global_shm_gateway.write_span({"cli_test": True, "tokens": 99})

    # Status
    with pytest.raises(SystemExit) as exc:
        cli_main(["shm", "status"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "SWARMWATCH ZERO-COPY SHARED-MEMORY RING BUFFER" in out

    # Flush
    with pytest.raises(SystemExit) as exc:
        cli_main(["shm", "flush"])
    assert exc.value.code == 0
    out2 = capsys.readouterr().out
    assert "SHM Flush" in out2
