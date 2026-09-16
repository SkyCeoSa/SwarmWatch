# -*- coding: utf-8 -*-
"""
Test suite for Enterprise Prometheus Metrics Exporter (Seam 8).
Verifies atomic thread-safe counters, gauges, cumulative histograms,
OpenMetrics 0.0.4 format compliance, and FastAPI /metrics endpoint integration.
"""

import threading
import time
import pytest
from starlette.testclient import TestClient

from swarmwatch.metrics import (
    PrometheusMetricsRegistry,
    Counter,
    Gauge,
    Histogram,
    record_span_ingested,
    record_circuit_trip,
    record_state_snapshot,
    record_branch_fork,
    set_active_swarms,
    set_active_websockets,
    set_telemetry_queue_depth,
)
from swarmwatch.server import create_app


def test_counter_increments_and_labels():
    registry = PrometheusMetricsRegistry()
    cnt = registry.counter(
        name="test_requests_total",
        help_text="Total requests received",
        label_names=("method", "status"),
    )

    cnt.inc(labels={"method": "GET", "status": "200"})
    cnt.inc(labels={"method": "GET", "status": "200"}, value=4)
    cnt.inc(labels={"method": "POST", "status": "500"}, value=2)

    assert cnt.get(labels={"method": "GET", "status": "200"}) == 5.0
    assert cnt.get(labels={"method": "POST", "status": "500"}) == 2.0
    assert cnt.get(labels={"method": "DELETE", "status": "404"}) == 0.0


def test_gauge_operations():
    registry = PrometheusMetricsRegistry()
    g = registry.gauge(
        name="test_active_workers",
        help_text="Current active workers",
        label_names=("pool",),
    )

    g.set(10.0, labels={"pool": "compute"})
    assert g.get(labels={"pool": "compute"}) == 10.0

    g.inc(labels={"pool": "compute"}, value=2.0)
    assert g.get(labels={"pool": "compute"}) == 12.0

    g.dec(labels={"pool": "compute"}, value=5.0)
    assert g.get(labels={"pool": "compute"}) == 7.0


def test_histogram_cumulative_buckets_and_sums():
    registry = PrometheusMetricsRegistry()
    h = registry.histogram(
        name="test_latency_seconds",
        help_text="Request latency in seconds",
        label_names=("handler",),
        buckets=(0.01, 0.05, 0.1),
    )

    # Observe 4 values: 0.005 (<=0.01), 0.02 (<=0.05), 0.08 (<=0.1), 0.5 (>0.1)
    h.observe(0.005, labels={"handler": "query"})
    h.observe(0.02, labels={"handler": "query"})
    h.observe(0.08, labels={"handler": "query"})
    h.observe(0.5, labels={"handler": "query"})

    snapshot = h.get_snapshot(labels={"handler": "query"})
    assert snapshot["count"] == 4
    assert abs(snapshot["sum"] - 0.605) < 1e-6

    buckets = snapshot["buckets"]
    assert buckets[0.01] == 1  # 0.005
    assert buckets[0.05] == 2  # 0.005, 0.02
    assert buckets[0.1] == 3   # 0.005, 0.02, 0.08
    assert buckets[float("inf")] == 4


def test_openmetrics_text_format_compliance():
    registry = PrometheusMetricsRegistry()
    c = registry.counter("http_requests_total", "Total HTTP requests", ("service",))
    g = registry.gauge("memory_usage_bytes", "Process memory in bytes")
    h = registry.histogram("rpc_duration_seconds", "RPC duration", buckets=(0.1, 0.5))

    c.inc(labels={"service": "swarm"}, value=3)
    g.set(1048576)
    h.observe(0.2)

    text = registry.export_openmetrics()

    # Verify Prometheus headers and declarations
    assert "# HELP http_requests_total Total HTTP requests" in text
    assert "# TYPE http_requests_total counter" in text
    assert 'http_requests_total{service="swarm"} 3' in text

    assert "# HELP memory_usage_bytes Process memory in bytes" in text
    assert "# TYPE memory_usage_bytes gauge" in text
    assert "memory_usage_bytes 1048576" in text

    assert "# HELP rpc_duration_seconds RPC duration" in text
    assert "# TYPE rpc_duration_seconds histogram" in text
    assert 'rpc_duration_seconds_bucket{le="0.1"} 0' in text
    assert 'rpc_duration_seconds_bucket{le="0.5"} 1' in text
    assert 'rpc_duration_seconds_bucket{le="+Inf"} 1' in text
    assert "rpc_duration_seconds_count 1" in text
    assert "rpc_duration_seconds_sum 0.2" in text


def test_high_concurrency_multithreaded_safety():
    registry = PrometheusMetricsRegistry()
    cnt = registry.counter("concurrent_hits_total", "Hits under concurrency", ("worker",))

    def worker(worker_id: str):
        for _ in range(200):
            cnt.inc(labels={"worker": worker_id})

    threads = [threading.Thread(target=worker, args=(f"w-{i % 5}",)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 10 threads * 200 increments = 2000 total increments distributed across 5 workers
    total = sum(cnt.get(labels={"worker": f"w-{i}"}) for i in range(5))
    assert total == 2000.0


def test_fastapi_metrics_scrape_endpoint():
    app = create_app()
    client = TestClient(app)

    # Initial scrape
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "version=0.0.4" in response.headers["content-type"]

    body = response.text
    assert "# HELP swarmwatch_spans_ingested_total" in body
    assert "# HELP swarmwatch_circuit_trips_total" in body
    assert "# HELP swarmwatch_active_swarms_gauge" in body
    assert "# HELP swarmwatch_telemetry_queue_depth_gauge" in body


def test_metrics_facade_helpers():
    registry = PrometheusMetricsRegistry.get_default()

    record_span_ingested(project_id="proj-alpha", agent_type="llm", status="ok", duration_seconds=0.002)
    record_circuit_trip(project_id="proj-alpha", reason="entropy_collapse", tokens_saved=5000, dollars_saved=0.10)
    record_state_snapshot(project_id="proj-alpha")
    record_branch_fork(project_id="proj-alpha")
    set_active_swarms(3)
    set_active_websockets(12)
    set_telemetry_queue_depth(42)

    text = registry.export_openmetrics()
    assert 'swarmwatch_spans_ingested_total{agent_type="llm",project_id="proj-alpha",status="ok"} 1' in text
    assert 'swarmwatch_circuit_trips_total{project_id="proj-alpha",reason="entropy_collapse"} 1' in text
    assert 'swarmwatch_tokens_saved_total{project_id="proj-alpha"} 5000' in text
    assert 'swarmwatch_dollars_saved_total{project_id="proj-alpha"} 0.1' in text
    assert 'swarmwatch_state_snapshots_total{project_id="proj-alpha"} 1' in text
    assert 'swarmwatch_time_machine_forks_total{project_id="proj-alpha"} 1' in text
    assert "swarmwatch_active_swarms_gauge 3" in text
    assert "swarmwatch_connected_websockets_gauge 12" in text
    assert "swarmwatch_telemetry_queue_depth_gauge 42" in text
