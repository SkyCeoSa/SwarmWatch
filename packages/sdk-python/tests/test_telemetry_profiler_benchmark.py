# -*- coding: utf-8 -*-
"""
Tests for High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite (Seam 19 / ADR 0030).
"""

import json
import os
import subprocess
import sys
import tempfile
import pytest

from swarmwatch.profiler import (
    TelemetryProfiler,
    IngestionBenchmarkConfig,
    IngestionBenchmarkReport,
    LatencyPercentiles,
    BenchmarkMemoryStats,
)
from swarmwatch.schema import SwarmSpan


class TestTelemetryProfilerBenchmark:
    """Test suite for TelemetryProfiler and ingestion micro-benchmarks."""

    def test_synthetic_span_generation_complexities(self):
        """Verify synthetic span generation across light, medium, and heavy complexities."""
        profiler = TelemetryProfiler()

        for complexity in ["light", "medium", "heavy"]:
            spans = profiler._generate_synthetic_spans(50, complexity)
            assert len(spans) == 50
            for s in spans:
                assert isinstance(s, SwarmSpan)
                assert s.swarm_id.startswith("swarm-bench-")
                assert s.source_agent.startswith("agent-")
                assert s.token_count > 0
                assert "query" in s.input_payload
                assert "response" in s.output_payload

            if complexity == "heavy":
                assert "raw_context" in spans[0].input_payload
                assert "structured_artifacts" in spans[0].output_payload

    def test_ingestion_benchmark_single_thread(self):
        """Verify single-threaded ingestion throughput, latency percentiles, and zero drops."""
        profiler = TelemetryProfiler()
        config = IngestionBenchmarkConfig(
            total_spans=1500,
            concurrency=1,
            batch_size=50,
            payload_complexity="light",
            generate_flamegraph=True,
            track_memory=True,
        )

        report = profiler.run(config)

        assert report.total_spans == 1500
        assert report.successful_spans == 1500
        assert report.dropped_spans == 0
        assert report.drop_rate_pct == 0.0
        assert report.duration_seconds > 0
        assert report.throughput_spans_per_sec > 1000.0  # In-process queue enqueue is blazing fast

        # Latency verification
        p = report.latency_percentiles
        assert isinstance(p, LatencyPercentiles)
        assert p.p50_us > 0.0
        assert p.p90_us >= p.p50_us
        assert p.p99_us >= p.p90_us
        assert p.max_us >= p.p99_us
        assert p.min_us <= p.p50_us
        # Enqueue should be sub-millisecond (typically < 50µs)
        assert p.p50_us < 1000.0

    def test_ingestion_benchmark_concurrent_workers(self):
        """Verify multi-threaded concurrent ingestion across parallel worker threads."""
        profiler = TelemetryProfiler()
        config = IngestionBenchmarkConfig(
            total_spans=2000,
            concurrency=4,
            batch_size=50,
            payload_complexity="medium",
            generate_flamegraph=False,
            track_memory=True,
        )

        report = profiler.run(config)

        assert report.total_spans == 2000
        assert report.successful_spans == 2000
        assert report.dropped_spans == 0
        assert report.concurrency == 4
        assert report.throughput_spans_per_sec > 1000.0

    def test_memory_profiling_and_leak_verification(self):
        """Verify memory tracking and zero-leak verification across sustained workloads."""
        profiler = TelemetryProfiler()
        config = IngestionBenchmarkConfig(
            total_spans=1000,
            concurrency=2,
            batch_size=50,
            payload_complexity="medium",
            track_memory=True,
        )

        report = profiler.run(config)

        assert report.memory_stats is not None
        mem = report.memory_stats
        assert isinstance(mem, BenchmarkMemoryStats)
        assert mem.peak_heap_mb >= mem.initial_heap_mb
        # Retained delta after flush and garbage collection must be bounded (< 5MB)
        assert mem.retained_delta_mb < 5.0
        assert mem.zero_leak_verified is True

    def test_collapsed_stack_flamegraph_generation(self):
        """Verify collapsed stack flamegraph generation compatible with Speedscope."""
        profiler = TelemetryProfiler()
        config = IngestionBenchmarkConfig(
            total_spans=1000,
            concurrency=2,
            batch_size=50,
            payload_complexity="medium",
            generate_flamegraph=True,
            track_memory=False,
        )

        report = profiler.run(config)

        assert report.collapsed_stack is not None
        lines = report.collapsed_stack.strip().split("\n")
        assert len(lines) >= 4
        for line in lines:
            parts = line.strip().rsplit(" ", 1)
            assert len(parts) == 2, f"Invalid flamegraph line format: {line}"
            stack, count_str = parts
            assert ";" in stack
            assert count_str.isdigit()
            assert int(count_str) > 0
            assert "swarmwatch" in stack

    def test_report_formatting_terminal_table_and_dict(self):
        """Verify report serialization to dictionary and ASCII terminal table."""
        profiler = TelemetryProfiler()
        config = IngestionBenchmarkConfig(
            total_spans=500,
            concurrency=2,
            batch_size=50,
            payload_complexity="light",
            generate_flamegraph=True,
            track_memory=True,
        )

        report = profiler.run(config)

        # Dictionary format
        data = report.to_dict()
        assert isinstance(data, dict)
        assert data["total_spans"] == 500
        assert "throughput_spans_per_sec" in data
        assert "latency_percentiles" in data
        assert "memory_stats" in data
        assert "collapsed_stack" in data

        # Terminal table formatting
        table_output = report.to_terminal_table()
        assert "SWARMWATCH HIGH-FREQUENCY INGESTION BENCHMARK" in table_output
        assert "THROUGHPUT & CONCURRENCY" in table_output
        assert "HIGH-RESOLUTION ENQUEUE LATENCY (MICROSECONDS)" in table_output
        assert "P50 Latency" in table_output
        assert "P99 Latency" in table_output
        assert "MEMORY PROFILE & HEAP INTEGRITY" in table_output
        assert "COLLAPSED STACK FLAMEGRAPH TRACE" in table_output

    def test_cli_benchmark_subcommand_json_and_flamegraph(self):
        """Verify CLI benchmark command execution with JSON output and flamegraph export."""
        with tempfile.TemporaryDirectory() as tmpdir:
            flame_out = os.path.join(tmpdir, "flamegraph.txt")
            report_out = os.path.join(tmpdir, "benchmark_report.json")

            cmd = [
                sys.executable,
                "-m",
                "swarmwatch.cli",
                "benchmark",
                "--spans",
                "600",
                "--concurrency",
                "2",
                "--payload",
                "light",
                "--flamegraph",
                "--flamegraph-out",
                flame_out,
                "--format",
                "json",
                "--output",
                report_out,
            ]

            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert proc.returncode == 0, f"CLI command failed with stderr: {proc.stderr}"
            assert os.path.exists(flame_out)
            assert os.path.exists(report_out)

            with open(flame_out, "r", encoding="utf-8") as f:
                flame_content = f.read()
                assert "swarmwatch" in flame_content

            with open(report_out, "r", encoding="utf-8") as f:
                report_data = json.load(f)
                assert report_data["total_spans"] == 600
                assert report_data["successful_spans"] == 600
                assert report_data["drop_rate_pct"] == 0.0
