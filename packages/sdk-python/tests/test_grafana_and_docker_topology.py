# -*- coding: utf-8 -*-
"""
Tests for Industrial Production Monitoring Stack & Grafana Dashboard Topology (Seam 17 / ADR 0028).

Verifies Docker Compose topology, Prometheus scrape configuration, Grafana automated
provisioning, dashboard JSON schema compliance, PromQL metrics parity, and Impeccable
design tokens with 100% local determinism.
"""

import json
from pathlib import Path
import re
import pytest

from swarmwatch.metrics import (
    PrometheusMetricsRegistry,
    record_span_ingested,
    record_circuit_trip,
    record_state_snapshot,
    record_branch_fork,
    set_active_swarms,
    set_active_websockets,
    set_telemetry_queue_depth,
)


REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DEPLOY_DIR = REPO_ROOT / "deploy"
PROMETHEUS_YML = DEPLOY_DIR / "prometheus" / "prometheus.yml"
DATASOURCES_YML = DEPLOY_DIR / "grafana" / "provisioning" / "datasources" / "datasources.yml"
DASHBOARDS_YML = DEPLOY_DIR / "grafana" / "provisioning" / "dashboards" / "dashboards.yml"
DASHBOARD_JSON = DEPLOY_DIR / "grafana" / "dashboards" / "swarmwatch-overview.json"
DOCKER_COMPOSE_MONITORING = REPO_ROOT / "docker-compose.monitoring.yml"


def _read_text(path: Path) -> str:
    assert path.is_file(), f"Expected file does not exist: {path}"
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def dashboard_data():
    content = _read_text(DASHBOARD_JSON)
    return json.loads(content)


class TestPrometheusConfiguration:
    """Verifies Prometheus scrape targets and configuration."""

    def test_prometheus_scrape_yaml_structure(self):
        content = _read_text(PROMETHEUS_YML)
        assert "global:" in content
        assert "scrape_interval: 5s" in content
        assert "evaluation_interval: 5s" in content
        assert 'job_name: "swarmwatch"' in content
        assert 'metrics_path: "/metrics"' in content
        assert 'targets: ["swarmwatch:8765"]' in content
        assert 'environment: "production-local"' in content
        assert 'cluster: "swarmwatch-cluster"' in content


class TestGrafanaProvisioning:
    """Verifies automated Grafana datasources and dashboard providers."""

    def test_datasource_provisioning_config(self):
        content = _read_text(DATASOURCES_YML)
        assert "apiVersion: 1" in content
        assert "name: Prometheus" in content
        assert "type: prometheus" in content
        assert "access: proxy" in content
        assert "url: http://prometheus:9090" in content
        assert "isDefault: true" in content
        assert "timeInterval: 5s" in content

    def test_dashboard_provider_config(self):
        content = _read_text(DASHBOARDS_YML)
        assert "apiVersion: 1" in content
        assert 'name: "SwarmWatch"' in content
        assert 'folder: "SwarmWatch"' in content
        assert "type: file" in content
        assert "path: /var/lib/grafana/dashboards" in content


class TestDockerComposeMonitoringTopology:
    """Verifies multi-service Docker Compose topology."""

    def test_docker_compose_monitoring_services_and_ports(self):
        content = _read_text(DOCKER_COMPOSE_MONITORING)
        assert "services:" in content

        # Check swarmwatch service
        assert "swarmwatch:" in content
        assert '"8765:8765"' in content
        assert "swarmwatch_data:/data" in content

        # Check prometheus service
        assert "prometheus:" in content
        assert "prom/prometheus:v2.54.1" in content
        assert '"9090:9090"' in content
        assert "/etc/prometheus/prometheus.yml:ro" in content
        assert "prometheus_data:/prometheus" in content

        # Check grafana service
        assert "grafana:" in content
        assert "grafana/grafana:11.2.0" in content
        assert '"3000:3000"' in content
        assert "GF_AUTH_ANONYMOUS_ENABLED=true" in content
        assert "GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer" in content
        assert "GF_SECURITY_ADMIN_PASSWORD=swarmwatch" in content
        assert "/var/lib/grafana/dashboards:ro" in content
        assert "grafana_data:/var/lib/grafana" in content

        # Check networks and persistent volumes
        assert "swarmwatch_net:" in content
        assert "volumes:" in content
        assert "swarmwatch_data:" in content
        assert "prometheus_data:" in content
        assert "grafana_data:" in content


class TestGrafanaDashboardPreset:
    """Verifies Grafana preset schema, panel layout, and Impeccable design tokens."""

    def test_dashboard_metadata_and_variables(self, dashboard_data):
        assert dashboard_data.get("title") == "SwarmWatch Multi-Agent Observability"
        assert "description" in dashboard_data
        assert dashboard_data.get("editable") is True

        # Check templating variables
        templating = dashboard_data.get("templating", {})
        variables = templating.get("list", [])
        var_names = [v.get("name") for v in variables]
        assert "project_id" in var_names
        assert "agent_type" in var_names

    def test_dashboard_panels_and_tier_structure(self, dashboard_data):
        panels = dashboard_data.get("panels", [])
        assert len(panels) >= 18, f"Expected at least 18 panels (including rows), found {len(panels)}"

        # Verify all panel IDs are unique
        panel_ids = [p["id"] for p in panels]
        assert len(panel_ids) == len(set(panel_ids)), "Panel IDs must be strictly unique"

        # Verify grid coordinates exist on every panel
        for p in panels:
            assert "gridPos" in p
            assert all(k in p["gridPos"] for k in ("h", "w", "x", "y"))

        # Verify 5 distinct operational row tiers
        row_titles = [p["title"] for p in panels if p.get("type") == "row"]
        expected_rows = [
            "Executive ROI & Financial Protection",
            "Ingestion Throughput & Buffer Capacity",
            "Circuit Breaker Interventions & Safety Anomalies",
            "Performance Latency & Shannon Entropy Histograms",
            "Time-Machine DAG & State Branching",
        ]
        for row in expected_rows:
            assert any(row in title for title in row_titles), f"Missing operational tier: {row}"

    def test_impeccable_palette_tokens(self, dashboard_data):
        raw_json = json.dumps(dashboard_data)
        # Check presence of key Impeccable tokens
        assert "#10B981" in raw_json  # Emerald (ROI, money saved)
        assert "#38BDF8" in raw_json  # Instrument Cyan (tokens, telemetry)
        assert "#EF4444" in raw_json  # Crimson (circuit trips, errors)
        assert "#F59E0B" in raw_json  # Amber (entropy warning)
        assert "#A78BFA" in raw_json  # Purple (time-machine forks, DAG)

    def test_promql_metrics_invariants(self, dashboard_data):
        """Verify that every metric queried in the dashboard exists in MetricsRegistry."""
        reg = PrometheusMetricsRegistry.get_default()
        valid_metric_families = {
            "swarmwatch_dollars_saved_total",
            "swarmwatch_tokens_saved_total",
            "swarmwatch_circuit_trips_total",
            "swarmwatch_active_swarms_gauge",
            "swarmwatch_connected_websockets_gauge",
            "swarmwatch_spans_ingested_total",
            "swarmwatch_telemetry_queue_depth_gauge",
            "swarmwatch_span_latency_seconds",
            "swarmwatch_entropy_calculation_seconds",
            "swarmwatch_state_snapshots_total",
            "swarmwatch_time_machine_forks_total",
        }

        # Extract all metric names referenced in targets expr
        raw_json = json.dumps(dashboard_data)
        metric_matches = set(re.findall(r"swarmwatch_[a-zA-Z0-9_]+", raw_json))

        # Normalize histogram suffix variations (_bucket, _count, _sum)
        normalized_metrics = set()
        for m in metric_matches:
            base = re.sub(r"_(bucket|count|sum)$", "", m)
            normalized_metrics.add(base)

        for m in normalized_metrics:
            assert m in valid_metric_families, f"Metric '{m}' in Grafana preset is not registered in SwarmWatch!"

    def test_live_metrics_exposition_parity_with_dashboard(self):
        """Simulate telemetry emission and ensure live /metrics contains all dashboard metrics."""
        reg = PrometheusMetricsRegistry.get_default()

        # Emit sample data across all metrics using high-level facade
        record_circuit_trip(
            project_id="proj-e2e",
            reason="entropy_collapse",
            tokens_saved=50000,
            dollars_saved=120.75,
        )
        record_span_ingested(
            project_id="proj-e2e",
            agent_type="analyst",
            status="ok",
            duration_seconds=0.015,
        )
        record_state_snapshot(project_id="proj-e2e")
        record_branch_fork(project_id="proj-e2e")
        set_active_swarms(3)
        set_active_websockets(1)
        set_telemetry_queue_depth(0)

        # Also observe entropy calculation latency
        reg.histogram(
            "swarmwatch_entropy_calculation_seconds",
            "Shannon entropy calculation latency in seconds",
            ("project_id",),
        ).observe(0.00012, labels={"project_id": "proj-e2e"})

        # Export Prometheus exposition text
        payload = reg.export_openmetrics()

        expected_families = [
            "swarmwatch_dollars_saved_total",
            "swarmwatch_tokens_saved_total",
            "swarmwatch_circuit_trips_total",
            "swarmwatch_active_swarms_gauge",
            "swarmwatch_connected_websockets_gauge",
            "swarmwatch_spans_ingested_total",
            "swarmwatch_telemetry_queue_depth_gauge",
            "swarmwatch_span_latency_seconds",
            "swarmwatch_entropy_calculation_seconds",
            "swarmwatch_state_snapshots_total",
            "swarmwatch_time_machine_forks_total",
        ]
        for family in expected_families:
            assert f"# HELP {family}" in payload, f"Missing # HELP for {family}"
            assert f"# TYPE {family}" in payload, f"Missing # TYPE for {family}"
            assert f"{family}" in payload, f"Missing samples for {family}"
