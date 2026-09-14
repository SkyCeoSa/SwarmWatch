# -*- coding: utf-8 -*-
"""
Integration Tests for Cross-Language Simulation and Performance Benchmarks.
Ensures end-to-end execution, DAG branch creation, and microsecond latency limits.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from benchmarks.benchmark_python import run_benchmark
from examples.simulate import main as run_simulation
from swarmwatch.storage import SQLiteStorageAdapter


def test_python_benchmark_metrics():
    """Verifies that the performance benchmark runs and meets latency constraints."""
    results = run_benchmark(n_spans=500, n_entropy_ops=1000)

    em = results["emission"]
    assert em["count"] == 500
    assert em["p50_us"] < 25.0  # Median latency must be under 25 microseconds

    ent = results["entropy"]
    assert ent["ops"] == 1000
    assert ent["ops_per_sec"] > 5000  # Shannon entropy math throughput > 5,000 ops/sec

    ser = results["serializer"]
    assert ser["ops_per_sec"] > 1000  # Serializer & SHA-256 throughput > 1,000 ops/sec


def test_simulation_end_to_end():
    """Verifies that the cross-language simulation runs, trips circuit on loop, and forks recovery branch."""
    exit_code = run_simulation()
    assert exit_code == 0

    # Verify SQLite database contains the simulation spans and recovery branch
    db_path = "swarmwatch.db"
    assert os.path.exists(db_path)

    import asyncio
    storage = SQLiteStorageAdapter(db_path=db_path)
    branches = asyncio.run(storage.list_branches("cross-language-swarm-01"))
    assert "main" in branches
    assert "recovery-passkeys-v2" in branches

    spans = storage.query_spans(swarm_id="cross-language-swarm-01", branch_id="main")
    assert len(spans) >= 1
    storage.close_sync()
