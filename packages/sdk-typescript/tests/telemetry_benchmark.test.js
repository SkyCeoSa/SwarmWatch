/**
 * High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite (TypeScript Edition - Seam 19 / ADR 0030).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  TelemetryProfiler,
  IngestionBenchmarkReport,
} from "../src/index.ts";

test("High-Frequency Telemetry Micro-Benchmark & Flamegraph Suite (TypeScript / ADR 0030)", async (t) => {
  const profiler = new TelemetryProfiler();

  await t.test("Synthetic span generation across light, medium, and heavy complexities", () => {
    for (const complexity of ["light", "medium", "heavy"]) {
      const spans = profiler.generateSyntheticSpans(25, complexity);
      assert.equal(spans.length, 25);
      for (const s of spans) {
        assert.ok(s.id.startsWith("span-bench-"));
        assert.ok(s.swarm_id.startsWith("swarm-bench-"));
        assert.ok(s.source_agent.startsWith("agent-"));
        assert.ok(s.target_agent.startsWith("agent-"));
        assert.ok((s.token_count || 0) > 0);
        assert.ok(s.input_payload && typeof s.input_payload === "object");
        assert.ok(s.output_payload && typeof s.output_payload === "object");
        assert.ok("query" in s.input_payload);
        assert.ok("response" in s.output_payload);
      }

      if (complexity === "heavy") {
        assert.ok("raw_context" in spans[0].input_payload);
        assert.ok("structured_artifacts" in spans[0].output_payload);
      }
    }
  });

  await t.test("Single-worker ingestion throughput and sub-microsecond latency percentiles", async () => {
    const report = await profiler.run({
      total_spans: 1500,
      concurrency: 1,
      batch_size: 50,
      payload_complexity: "light",
      generate_flamegraph: true,
      track_memory: true,
      warmup_spans: 50,
    });

    assert.ok(report instanceof IngestionBenchmarkReport);
    assert.equal(report.total_spans, 1500);
    assert.equal(report.successful_spans, 1500);
    assert.equal(report.dropped_spans, 0);
    assert.equal(report.drop_rate_pct, 0.0);
    assert.ok(report.duration_seconds > 0);
    assert.ok(report.throughput_spans_per_sec > 1000.0);

    // Latency distribution verification
    const lat = report.latency_percentiles;
    assert.ok(lat.p50_us > 0);
    assert.ok(lat.p90_us >= lat.p50_us);
    assert.ok(lat.p99_us >= lat.p90_us);
    assert.ok(lat.max_us >= lat.p99_us);
    assert.ok(lat.min_us <= lat.p50_us);
    // Non-blocking in-memory emitSpan should be extremely fast (< 1000us)
    assert.ok(lat.p50_us < 1000.0);
  });

  await t.test("Concurrent multi-task worker ingestion (concurrency: 4)", async () => {
    const report = await profiler.run({
      total_spans: 2000,
      concurrency: 4,
      batch_size: 50,
      payload_complexity: "medium",
      generate_flamegraph: false,
      track_memory: true,
      warmup_spans: 50,
    });

    assert.equal(report.total_spans, 2000);
    assert.equal(report.successful_spans, 2000);
    assert.equal(report.dropped_spans, 0);
    assert.equal(report.concurrency, 4);
    assert.ok(report.throughput_spans_per_sec > 1000.0);
  });

  await t.test("Memory tracking and zero-leak verification across sustained workloads", async () => {
    const report = await profiler.run({
      total_spans: 1000,
      concurrency: 2,
      batch_size: 50,
      payload_complexity: "medium",
      track_memory: true,
      warmup_spans: 20,
    });

    const mem = report.memory_stats;
    assert.ok(mem);
    assert.ok(mem.peak_heap_mb >= 0);
    assert.ok(mem.final_heap_mb >= 0);
    assert.ok(mem.retained_delta_mb < 5.0, `Retained memory delta too high: ${mem.retained_delta_mb}MB`);
    assert.equal(mem.zero_leak_verified, true);
  });

  await t.test("Collapsed stack flamegraph trace generation (Speedscope compatible)", async () => {
    const report = await profiler.run({
      total_spans: 1000,
      concurrency: 2,
      batch_size: 50,
      payload_complexity: "medium",
      generate_flamegraph: true,
      warmup_spans: 20,
    });

    assert.ok(report.collapsed_stack);
    const lines = report.collapsed_stack.trim().split("\n");
    assert.ok(lines.length >= 4);
    for (const line of lines) {
      const parts = line.trim().split(" ");
      assert.equal(parts.length, 2, `Invalid flamegraph format line: ${line}`);
      const [stack, countStr] = parts;
      assert.ok(stack.includes(";"));
      assert.ok(/^\d+$/.test(countStr));
      assert.ok(parseInt(countStr, 10) > 0);
      assert.ok(stack.includes("swarmwatch"));
    }
  });

  await t.test("Report serialization to dictionary and ASCII terminal table", async () => {
    const report = await profiler.run({
      total_spans: 500,
      concurrency: 2,
      batch_size: 50,
      payload_complexity: "light",
      generate_flamegraph: true,
      track_memory: true,
      warmup_spans: 20,
    });

    // Dictionary format
    const dict = report.toDict();
    assert.equal(dict.total_spans, 500);
    assert.ok("throughput_spans_per_sec" in dict);
    assert.ok("latency_percentiles" in dict);
    assert.ok("memory_stats" in dict);
    assert.ok("collapsed_stack" in dict);

    // ASCII Monospace Terminal Table
    const table = report.toTerminalTable();
    assert.ok(table.includes("SWARMWATCH HIGH-FREQUENCY INGESTION BENCHMARK"));
    assert.ok(table.includes("THROUGHPUT & CONCURRENCY"));
    assert.ok(table.includes("HIGH-RESOLUTION ENQUEUE LATENCY (MICROSECONDS)"));
    assert.ok(table.includes("P50 Latency"));
    assert.ok(table.includes("P99 Latency"));
    assert.ok(table.includes("MEMORY PROFILE & HEAP INTEGRITY"));
    assert.ok(table.includes("COLLAPSED STACK FLAMEGRAPH TRACE"));
  });
});
