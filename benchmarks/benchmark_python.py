# -*- coding: utf-8 -*-
"""
SwarmWatch Python Performance Benchmark.
Rigorously measures in-process telemetry emission latency (<1 microsecond),
Shannon entropy throughput (ops/sec), and state snapshotting hashing performance.
"""

from datetime import datetime, timezone
import json
import os
import sys
import time
from typing import Dict, List
import uuid

# Ensure UTF-8 output
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Add SDK to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../packages/sdk-python")))

from swarmwatch.circuit import compute_shannon_entropy, compute_lexical_similarity
from swarmwatch.schema import SwarmSpan
from swarmwatch.serializer import SafeStateSerializer
from swarmwatch.storage import StorageAdapter
from swarmwatch.transport import TelemetryTransport, TransportConfig


class NullStorageAdapter(StorageAdapter):
    """Zero-overhead sink for pure transport queue benchmarking."""

    async def initialize(self) -> None:
        pass

    async def save_spans(self, spans: List[SwarmSpan]) -> None:
        pass

    async def get_swarm_spans(self, swarm_id: str, branch_id: str = "main") -> List[SwarmSpan]:
        return []

    async def save_snapshot(self, snapshot) -> None:
        pass

    async def get_snapshot(self, span_id: str):
        return None

    async def record_circuit_event(self, event) -> None:
        pass

    async def list_branches(self, swarm_id: str) -> List[str]:
        return ["main"]

    async def close(self) -> None:
        pass

    def save_spans_sync(self, spans: List[SwarmSpan]) -> None:
        pass

    def close_sync(self) -> None:
        pass


def percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_data) - 1)
    d = k - f
    return sorted_data[f] + d * (sorted_data[c] - sorted_data[f])


def run_benchmark(n_spans: int = 5000, n_entropy_ops: int = 10000) -> dict:
    results = {}

    # -------------------------------------------------------------------------
    # 1. Telemetry Queue Emission Latency Benchmark
    # -------------------------------------------------------------------------
    storage = NullStorageAdapter()
    config = TransportConfig(batch_size=500, max_queue_size=n_spans + 1000)
    transport = TelemetryTransport(storage=storage, config=config)
    transport.start()

    now_iso = datetime.now(timezone.utc)
    base_span = SwarmSpan(
        id=str(uuid.uuid4()),
        swarm_id="bench-swarm",
        source_agent="AgentBenchmark",
        target_agent="Database",
        span_type="agent_call",
        start_time=now_iso,
        end_time=now_iso,
    )

    latencies_us: List[float] = []

    # Warm-up (1000 iterations)
    for _ in range(1000):
        transport.emit_span(base_span)

    # Drained
    transport.flush()

    # Timed run
    for _ in range(n_spans):
        t0 = time.perf_counter_ns()
        transport.emit_span(base_span)
        t1 = time.perf_counter_ns()
        latencies_us.append((t1 - t0) / 1000.0)  # nanoseconds to microseconds

    transport.shutdown()

    results["emission"] = {
        "count": n_spans,
        "mean_us": sum(latencies_us) / len(latencies_us),
        "p50_us": percentile(latencies_us, 50),
        "p95_us": percentile(latencies_us, 95),
        "p99_us": percentile(latencies_us, 99),
        "max_us": max(latencies_us),
    }

    # -------------------------------------------------------------------------
    # 2. Normalized Shannon Entropy Throughput Benchmark
    # -------------------------------------------------------------------------
    sample_text = (
        "Agent received customer support ticket regarding API gateway rate limiting. "
        "Analyzing request payload, headers, and authentication tokens before routing to microservice."
    )

    # Warm-up
    for _ in range(1000):
        compute_shannon_entropy(sample_text)

    t_start = time.perf_counter()
    for _ in range(n_entropy_ops):
        compute_shannon_entropy(sample_text)
    t_end = time.perf_counter()

    entropy_duration = max(0.0001, t_end - t_start)
    entropy_ops_sec = int(n_entropy_ops / entropy_duration)
    results["entropy"] = {
        "ops": n_entropy_ops,
        "duration_sec": round(entropy_duration, 4),
        "ops_per_sec": entropy_ops_sec,
    }

    # -------------------------------------------------------------------------
    # 3. Lexical Jaccard Similarity Throughput Benchmark
    # -------------------------------------------------------------------------
    text_a = "Agent received customer support ticket regarding API gateway rate limiting."
    text_b = "Agent analyzing customer support ticket for API gateway rate limits."

    t_start = time.perf_counter()
    for _ in range(n_entropy_ops):
        compute_lexical_similarity(text_a, text_b)
    t_end = time.perf_counter()

    jaccard_duration = max(0.0001, t_end - t_start)
    jaccard_ops_sec = int(n_entropy_ops / jaccard_duration)
    results["jaccard"] = {
        "ops": n_entropy_ops,
        "duration_sec": round(jaccard_duration, 4),
        "ops_per_sec": jaccard_ops_sec,
    }

    # -------------------------------------------------------------------------
    # 4. Safe State Serializer & SHA-256 Hashing Benchmark
    # -------------------------------------------------------------------------
    serializer = SafeStateSerializer()
    sample_state = {
        "user_id": "usr_9921",
        "api_key": "sk-proj-super-secret-key-12345",
        "nested": {"tokens": 450, "authorized": True},
        "tags": ["prod", "us-east-1", "copilot"],
    }

    n_serializer_ops = 10000
    t_start = time.perf_counter()
    for _ in range(n_serializer_ops):
        sanitized = serializer.sanitize(sample_state)
        serializer.compute_hash(sanitized)
    t_end = time.perf_counter()

    serializer_duration = max(0.0001, t_end - t_start)
    serializer_ops_sec = int(n_serializer_ops / serializer_duration)
    results["serializer"] = {
        "ops": n_serializer_ops,
        "duration_sec": round(serializer_duration, 4),
        "ops_per_sec": serializer_ops_sec,
    }

    return results


def main():
    results = run_benchmark()
    if "--json" in sys.argv:
        print(json.dumps(results))
        return

    width = 72
    print("\n" + "=" * width)
    print("  SwarmWatch Python SDK Performance Benchmark Report")
    print("=" * width)
    em = results["emission"]
    print(f"\n1. In-Process Telemetry Ring Buffer Latency ({em['count']:,} spans):")
    print(f"   Mean Latency        : {em['mean_us']:>8.3f} us")
    print(f"   Median (p50) Latency: {em['p50_us']:>8.3f} us")
    print(f"   95th Percentile     : {em['p95_us']:>8.3f} us")
    print(f"   99th Percentile     : {em['p99_us']:>8.3f} us")
    print(f"   Max Latency         : {em['max_us']:>8.3f} us")

    ent = results["entropy"]
    print(f"\n2. Shannon Entropy Math Engine ({ent['ops']:,} iterations):")
    print(f"   Duration            : {ent['duration_sec']:>8.4f} sec")
    print(f"   Throughput          : {ent['ops_per_sec']:>8,d} ops/sec")

    jac = results["jaccard"]
    print(f"\n3. Jaccard Lexical Similarity Engine ({jac['ops']:,} iterations):")
    print(f"   Duration            : {jac['duration_sec']:>8.4f} sec")
    print(f"   Throughput          : {jac['ops_per_sec']:>8,d} ops/sec")

    ser = results["serializer"]
    print(f"\n4. Safe State Serializer & SHA-256 Hashing ({ser['ops']:,} iterations):")
    print(f"   Duration            : {ser['duration_sec']:>8.4f} sec")
    print(f"   Throughput          : {ser['ops_per_sec']:>8,d} ops/sec")
    print("\n" + "=" * width + "\n")


if __name__ == "__main__":
    main()
