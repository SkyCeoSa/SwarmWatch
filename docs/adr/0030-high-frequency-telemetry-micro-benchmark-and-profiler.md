# ADR 0030: High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite

## Status
Accepted (Seam 19)

## Context
SwarmWatch is designed to operate as a high-throughput, non-blocking telemetry substrate for multi-agent LLM systems. When large swarms (dozens of concurrent agents executing reasoning turns, tool calls, and state transitions) run in parallel, high-frequency span ingestion occurs at bursts of thousands of spans per second.

In production environments, SREs and enterprise architects must be certain that:
1. Telemetry ingestion imposes sub-microsecond latency on host agent worker threads ($<1.0\mu s$ median return time).
2. The in-process non-blocking ring buffer handles sustained ingestion rates of over 10,000 spans per second without thread starvation or unbounded queue growth.
3. Ingestion exhibits zero memory leaks across sustained long-running workloads, with bounded heap allocation and low memory overhead per span.
4. Developers can profile internal allocation hotspots and inspect call stacks via standard collapsed stack flamegraph formats without installing heavy, platform-dependent native profilers (like `py-spy` or C-extensions).

## Decision
We implement the **High-Frequency Telemetry Ingestion Micro-Benchmark & Flamegraph Profile Suite (Seam 19)**:

1. **`TelemetryProfiler` (Deep Module in `swarmwatch/profiler.py`)**:
   - Zero-dependency benchmarking engine utilizing Python standard library modules (`time.perf_counter_ns()`, `tracemalloc`, `threading`, `concurrent.futures`).
   - Configurable parameters (`IngestionBenchmarkConfig`):
     - `total_spans`: Target span count (e.g. 10,000 to 50,000).
     - `concurrency`: Number of concurrent simulated agent threads (e.g. 1, 4, 8, 16).
     - `batch_size`: TelemetryTransport flush batch size.
     - `payload_complexity`: Payload size and structure (`light`, `medium`, `heavy`).
     - `track_memory`: Heap tracking via `tracemalloc`.
     - `generate_flamegraph`: Collapsed stack trace generation.
   - Comprehensive Metrics (`IngestionBenchmarkReport`):
     - Throughput: Spans per second.
     - Latency Distribution: Percentiles P50, P90, P99, Min, Max, Mean in microseconds ($\mu s$).
     - Memory Profiling: Baseline RSS, peak RSS, final RSS, retained byte overhead per span, and zero-leak verification.
     - Flamegraph Collapsed Stack: Formats call stacks into standard `frame1;frame2;frame3 count` syntax directly consumable by Speedscope, flamegraph.pl, or d3-flame-graph.

2. **CLI Subcommand (`swarmwatch benchmark`)**:
   - Integrated terminal runner accepting flags `--spans`, `--concurrency`, `--payload`, `--flamegraph`, `--output`, `--format`.
   - Renders an austere, monospace ASCII table conforming strictly to SwarmWatch high-density craft standard standards (tabular numerals, zero emojis, zero neon glows).

3. **Dual-SDK Parity in TypeScript (`@swarmwatch/sdk`)**:
   - `packages/sdk-typescript/src/profiler.ts`: Implements `TelemetryProfiler` in TypeScript with Node.js high-resolution timers (`performance.now()`) and heap tracking (`process.memoryUsage()`).
   - Test suite `packages/sdk-typescript/tests/telemetry_benchmark.test.js`: Verifies >10,000 spans/sec throughput and sub-microsecond enqueue latency in Node.js ESM.

4. **Automated TDD Test Suite (`test_telemetry_profiler_benchmark.py`)**:
   - Automated tests verifying throughput thresholds, percentile accuracy, concurrent multi-threading safety, zero memory leak detection, and flamegraph syntax validity.

## Consequences
- **Positive**: Platform engineers have empirical proof that SwarmWatch ingestion adds $<1\mu s$ overhead and scales to >10,000 spans/sec with zero memory leaks.
- **Positive**: Zero external dependencies: 100% pure standard library implementation in both Python and TypeScript runtimes.
- **Positive**: Direct interoperability with standard flamegraph visualizers without third-party agent profilers.
