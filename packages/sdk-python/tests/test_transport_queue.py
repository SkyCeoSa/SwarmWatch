"""
TDD Tests for Ticket 02: In-Process SDK Transport & Bounded Queue Worker
"""
import time
import uuid
import pytest
from unittest.mock import MagicMock
from swarmwatch.schema import SwarmSpan
from swarmwatch.transport import TelemetryTransport, TransportConfig
from swarmwatch.storage import SQLiteStorageAdapter


def create_dummy_span(i: int, swarm_id: str = "test-swarm") -> SwarmSpan:
    return SwarmSpan(
        id=str(uuid.uuid4()),
        swarm_id=swarm_id,
        source_agent="planner_agent",
        target_agent="coder_agent",
        span_type="agent_call",
        input_payload={"prompt_index": i},
        output_payload={"response_index": i * 2},
        token_count=150,
        entropy_score=0.85,
    )


def test_transport_non_blocking_enqueue():
    mock_storage = MagicMock()
    config = TransportConfig(
        batch_size=50,
        flush_interval_seconds=0.1,
        max_queue_size=1000,
    )
    transport = TelemetryTransport(storage=mock_storage, config=config)
    transport.start()

    try:
        # Measure enqueue time for 500 spans
        start_time = time.perf_counter()
        for i in range(500):
            transport.emit_span(create_dummy_span(i))
        elapsed = time.perf_counter() - start_time

        # Ensure average enqueue time is less than 50 microseconds (non-blocking)
        avg_micros = (elapsed / 500) * 1_000_000
        assert avg_micros < 50, f"Average enqueue took {avg_micros} microseconds, expected < 50us"

        # Wait for background flusher or trigger explicit flush
        time.sleep(0.3)
        transport.flush()

        assert mock_storage.save_spans.called
        # Verify total spans received by storage equals 500
        total_flushed = sum(len(call.args[0]) for call in mock_storage.save_spans.call_args_list)
        assert total_flushed == 500
    finally:
        transport.shutdown()


def test_transport_bounded_queue_drop():
    mock_storage = MagicMock()
    # Tiny queue to test overflow behavior without blocking
    config = TransportConfig(
        batch_size=100,
        flush_interval_seconds=10.0,  # don't flush automatically
        max_queue_size=5,
    )
    # Don't start background worker yet so queue fills up
    transport = TelemetryTransport(storage=mock_storage, config=config)

    # Put 10 spans into queue of size 5
    for i in range(10):
        transport.emit_span(create_dummy_span(i))

    assert transport.dropped_count > 0, "Expected dropped spans when queue is full"
    assert transport.queue_size <= 5


def test_transport_with_real_sqlite_storage(tmp_path):
    db_path = str(tmp_path / "transport_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    config = TransportConfig(
        batch_size=10,
        flush_interval_seconds=0.05,
    )
    transport = TelemetryTransport(storage=storage, config=config)
    transport.start()

    try:
        swarm_id = "sqlite-live-swarm"
        for i in range(25):
            transport.emit_span(create_dummy_span(i, swarm_id=swarm_id))

        # Explicit flush to drain queue
        transport.flush()

        spans = storage.query_spans(swarm_id=swarm_id)
        assert len(spans) == 25
    finally:
        transport.shutdown()
        storage.close_sync()
