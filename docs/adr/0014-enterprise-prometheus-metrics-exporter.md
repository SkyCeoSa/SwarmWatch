# ADR 0014: Enterprise Prometheus Metrics Exporter & Operational Observability

## Status
Accepted

## Date
2026-09-16

## Context
As SwarmWatch transitions from local multi-agent debugging into production Kubernetes and Docker multi-tenant clusters, operations and SRE teams require real-time telemetry on system health, span throughput, circuit breaker activations, and counterfactual savings.

Standard observability systems in enterprise infrastructure (Prometheus, Grafana Agent, Datadog, VictoriaMetrics) scrape metrics via HTTP endpoints exposing the Prometheus text-based exposition format (v0.0.4) or OpenMetrics (v1.0.0).

However, introducing third-party monitoring libraries such as the official prometheus_client introduces non-trivial external dependencies, potential version conflicts, and memory footprint overhead that violates SwarmWatch\'s Core Invariant: **Lightweight, zero-token, and minimal external dependencies**.

## Decision
We implement a dedicated **Enterprise Prometheus Metrics Exporter** (packages/sdk-python/swarmwatch/metrics.py) as a **deep module** (Seam 8) using only the Python standard library (threading.Lock, atomic dictionary structures, and string formatters):

1. **Zero External Heavy Dependencies**:
   The metrics registry, counters, gauges, and histograms are built natively in pure Python without requiring prometheus_client. The output format strictly adheres to Prometheus 0.0.4 / OpenMetrics 1.0.0 specification.
2. **Sub-Microsecond Latency Guarantee**:
   All counter increments, gauge updates, and histogram observations utilize thread-safe atomic data structures designed for <0.5us execution on high-throughput agent paths.
3. **Multi-Tenant Label Partitioning**:
   Every operational metric includes structured label dimensions, primarily project_id, enabling fine-grained filtering and billing attribution in multi-tenant environments.
4. **Exposition Endpoint (GET /metrics)**:
   The FastAPI ingestion server exposes GET /metrics with Content-Type: text/plain; version=0.0.4; charset=utf-8.
5. **Catalog of Core Operational Metrics**:
   - swarmwatch_spans_ingested_total{project_id, agent_type, status}: Counter
   - swarmwatch_circuit_trips_total{project_id, reason}: Counter
   - swarmwatch_tokens_saved_total{project_id}: Counter
   - swarmwatch_dollars_saved_total{project_id}: Counter
   - swarmwatch_state_snapshots_total{project_id}: Counter
   - swarmwatch_time_machine_forks_total{project_id}: Counter
   - swarmwatch_active_swarms_gauge: Gauge
   - swarmwatch_connected_websockets_gauge: Gauge
   - swarmwatch_telemetry_queue_depth_gauge: Gauge
   - swarmwatch_span_latency_seconds: Histogram (le buckets: 0.0001 to 1.0)
   - swarmwatch_entropy_calculation_seconds: Histogram

## Consequences

### Positive
- **Production Kubernetes Ready**: Direct integration with Prometheus ServiceMonitor and Helm charts.
- **Zero Token Cost & Zero External Dependencies**: 100% standard library implementation preserving fast installs and small package size.
- **Enterprise Attribution**: Multi-tenant metrics allow Prometheus and Grafana to graph tenant-level resource savings and swarm activity.
- **Thread Safety**: High concurrency agent emissions cause zero race conditions or dropped counts.

### Negative / Tradeoffs
- Complex metric types (e.g. exponential summaries) are omitted in favor of simple, performant cumulative histograms.
