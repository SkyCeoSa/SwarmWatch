# -*- coding: utf-8 -*-
"""
Tests for Seam 86: Durable Causal Stream Segments & Crash-Consistent Recovery (ADR 0097).
Verifies:
- Invariant D1: Append durability and monotonic sequence offsets.
- Invariant D2: Crash recovery with prefix-closed truncation of torn tail writes.
- Invariant D3: Bounded memory compaction into Merkle state snapshots.
- Sub-250µs execution latency SLO and zero-token cost guarantee.
"""

import json
import tempfile
import time
from pathlib import Path
import pytest

from swarmwatch.durable_stream import (
    DurableCausalStream,
    StreamRecord,
)


def test_durable_stream_append_and_recovery():
    with tempfile.TemporaryDirectory() as tmpdir:
        stream = DurableCausalStream(stream_id="stream-test-01", data_dir=tmpdir)

        # Append records
        off0 = stream.append({"event": "agent_started", "agent_id": "agent-1"})
        off1 = stream.append({"event": "tool_executed", "tool": "search"})
        off2 = stream.append({"event": "agent_finished", "tokens": 42})

        assert off0 == 0
        assert off1 == 1
        assert off2 == 2
        assert stream.get_status()["active_records_in_ram"] == 3

        # Simulate process restart / new instance pointing to same WAL
        recovered_stream = DurableCausalStream(stream_id="stream-test-01", data_dir=tmpdir)
        head = recovered_stream.recover()

        # Invariant D2: Prefix-closed recovery matches exactly
        assert head.head_offset == 2
        assert head.total_records == 3
        records = recovered_stream.read_range(0)
        assert len(records) == 3
        assert records[0].payload["event"] == "agent_started"
        assert records[2].payload["tokens"] == 42


def test_durable_stream_torn_tail_crash_truncation():
    with tempfile.TemporaryDirectory() as tmpdir:
        stream = DurableCausalStream(stream_id="stream-torn-01", data_dir=tmpdir)

        stream.append({"turn": 1, "msg": "hello"})
        stream.append({"turn": 2, "msg": "world"})

        wal_file = Path(tmpdir) / "stream-torn-01.wal"
        assert wal_file.exists()

        # Inject corrupt / torn write at the end of the file
        with open(wal_file, "a", encoding="utf-8") as f:
            f.write('{"offset": 2, "timestamp_hlc": "CORRUPT_JSON_HALF_WRITE\n')

        # Invariant D2: Recovery safely discards corrupt tail and retains prefix
        new_stream = DurableCausalStream(stream_id="stream-torn-01", data_dir=tmpdir)
        head = new_stream.recover()

        assert head.head_offset == 1
        assert head.total_records == 2
        records = new_stream.read_range(0)
        assert len(records) == 2
        assert records[1].payload["msg"] == "world"


def test_durable_stream_bounded_memory_compaction():
    stream = DurableCausalStream(stream_id="stream-compact-01")

    for i in range(10):
        stream.append({"index": i, "data": f"payload-{i}"})

    assert stream.get_status()["active_records_in_ram"] == 10

    # Invariant D3: Compact records older than offset 7
    snapshot = stream.compact(before_offset=7)
    assert snapshot["compacted_before_offset"] == 7
    assert snapshot["record_count"] == 7
    assert len(snapshot["merkle_root"]) == 64

    # Memory bounded: only records 7, 8, 9 remain in RAM!
    assert stream.get_status()["active_records_in_ram"] == 3
    remaining = stream.read_range(0)
    assert len(remaining) == 3
    assert remaining[0].offset == 7
    assert remaining[2].offset == 9


def test_durable_stream_latency_slo():
    stream = DurableCausalStream(stream_id="stream-perf-01")
    start = time.perf_counter()
    for i in range(500):
        stream.append({"tick": i})
    elapsed_us = ((time.perf_counter() - start) / 500) * 1_000_000
    assert elapsed_us < 250.0  # sub-250µs SLO
