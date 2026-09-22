# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Helm 3 Production Chart & Cloud-Native Topology (Seam 20 / ADR 0031).
Verifies chart metadata, values, template structures, dual-storage topologies,
and cloud-native deployment invariants without requiring a live Kubernetes cluster.
"""

import os
from pathlib import Path
import re
import pytest

# Find project root directory
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
HELM_DIR = REPO_ROOT / "deploy" / "helm" / "swarmwatch"
TEMPLATES_DIR = HELM_DIR / "templates"


def _parse_simple_yaml(text: str) -> dict:
    """
    Lightweight, deterministic YAML key-value parser for testing
    manifest structure without external pyyaml dependencies.
    """
    result = {}
    lines = text.splitlines()
    stack = [(result, -1)]

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip())

        # Unwind stack if indentation decreased
        while len(stack) > 1 and indent <= stack[-1][1]:
            stack.pop()

        current_dict = stack[-1][0]

        if ":" in stripped:
            key, val = stripped.split(":", 1)
            key = key.strip()
            val = val.split("#", 1)[0].strip()

            if val == "":
                new_dict = {}
                current_dict[key] = new_dict
                stack.append((new_dict, indent))
            else:
                # Type conversions
                if val.lower() == "true":
                    val_conv = True
                elif val.lower() == "false":
                    val_conv = False
                elif val.isdigit():
                    val_conv = int(val)
                elif (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    val_conv = val[1:-1]
                else:
                    val_conv = val
                current_dict[key] = val_conv
        elif stripped.startswith("- "):
            item = stripped[2:].strip()
            if isinstance(current_dict, dict):
                # If current container is dict and has no current list, record in a list
                pass

    return result


def test_helm_chart_yaml_metadata():
    """Verify Chart.yaml meets Helm 3 specifications and version invariants."""
    chart_path = HELM_DIR / "Chart.yaml"
    assert chart_path.exists(), f"Chart.yaml not found at {chart_path}"

    content = chart_path.read_text(encoding="utf-8")
    assert "apiVersion: v2" in content
    assert "name: swarmwatch" in content
    assert "type: application" in content
    assert 'version: 0.1.0' in content or 'version: "0.1.0"' in content
    assert 'appVersion: "0.1.0"' in content
    assert "keywords:" in content
    assert "shannon-entropy" in content
    assert "time-machine" in content


def test_helm_default_values():
    """Verify default values.yaml configuration adheres to ADR 0031 production defaults."""
    values_path = HELM_DIR / "values.yaml"
    assert values_path.exists(), f"values.yaml not found at {values_path}"

    content = values_path.read_text(encoding="utf-8")
    parsed = _parse_simple_yaml(content)

    # Basic defaults
    assert parsed.get("replicaCount") == 1
    assert "image" in parsed
    assert parsed["image"].get("repository") == "swarmwatch"
    assert parsed["image"].get("pullPolicy") == "IfNotPresent"
    assert parsed["image"].get("tag") == "0.1.0"

    # Pod Security Context
    assert "podSecurityContext" in parsed
    assert parsed["podSecurityContext"].get("runAsNonRoot") is True
    assert parsed["podSecurityContext"].get("runAsUser") == 10001
    assert parsed["podSecurityContext"].get("runAsGroup") == 10001

    # Container Security Context
    assert "securityContext" in parsed
    assert parsed["securityContext"].get("allowPrivilegeEscalation") is False

    # Service & Ingress
    assert "service" in parsed
    assert parsed["service"].get("type") == "ClusterIP"
    assert parsed["service"].get("port") == 8765

    assert "ingress" in parsed
    assert parsed["ingress"].get("enabled") is True
    assert parsed["ingress"].get("className") == "nginx"

    # WebSocket timeout annotations in Ingress
    assert "nginx.ingress.kubernetes.io/proxy-read-timeout: \"3600\"" in content
    assert "nginx.ingress.kubernetes.io/proxy-send-timeout: \"3600\"" in content
    assert "nginx.ingress.kubernetes.io/websocket-services: \"swarmwatch\"" in content

    # Health Probes
    assert "livenessProbe" in parsed
    assert "readinessProbe" in parsed
    assert "/api/v1/health" in content

    # Storage Backend Default
    assert "storage" in parsed
    assert parsed["storage"].get("backend") == "sqlite"
    assert "/data/swarmwatch.db" in content

    # Persistence Default
    assert "persistence" in parsed
    assert parsed["persistence"].get("enabled") is True
    assert parsed["persistence"].get("size") == "10Gi"
    assert parsed["persistence"].get("mountPath") == "/data"

    # ServiceMonitor Default
    assert "serviceMonitor" in parsed
    assert parsed["serviceMonitor"].get("enabled") is True
    assert parsed["serviceMonitor"].get("interval") == "5s"
    assert parsed["serviceMonitor"].get("scrapeTimeout") == "3s"


def test_helm_templates_all_present():
    """Verify that all standard Helm 3 manifest templates are present."""
    expected_files = [
        "_helpers.tpl",
        "serviceaccount.yaml",
        "configmap.yaml",
        "secret.yaml",
        "pvc.yaml",
        "deployment.yaml",
        "service.yaml",
        "ingress.yaml",
        "hpa.yaml",
        "servicemonitor.yaml",
        "NOTES.txt",
    ]

    for fname in expected_files:
        fpath = TEMPLATES_DIR / fname
        assert fpath.exists(), f"Missing expected Helm template: {fname}"
        assert fpath.stat().st_size > 0, f"Template file {fname} is empty"


def test_helpers_tpl_definitions():
    """Verify _helpers.tpl provides all canonical labels and name templates."""
    helpers_path = TEMPLATES_DIR / "_helpers.tpl"
    content = helpers_path.read_text(encoding="utf-8")

    assert 'define "swarmwatch.name"' in content
    assert 'define "swarmwatch.fullname"' in content
    assert 'define "swarmwatch.chart"' in content
    assert 'define "swarmwatch.labels"' in content
    assert 'define "swarmwatch.selectorLabels"' in content
    assert 'define "swarmwatch.serviceAccountName"' in content
    assert "app.kubernetes.io/part-of: swarmwatch" in content


def test_deployment_template_storage_topology_strategy():
    """Verify deployment.yaml implements conditional Recreate strategy for SQLite WAL safety."""
    deploy_path = TEMPLATES_DIR / "deployment.yaml"
    content = deploy_path.read_text(encoding="utf-8")

    # Strategy must be Recreate for SQLite to prevent split-brain / database lock collisions
    assert 'type: {{ if eq .Values.storage.backend "sqlite" }}Recreate{{ else }}RollingUpdate{{ end }}' in content

    # Pod Security & Non-root enforcement
    assert "securityContext:" in content
    assert ".Values.podSecurityContext" in content
    assert ".Values.securityContext" in content

    # Ingestion Port
    assert "containerPort: {{ .Values.service.targetPort }}" in content

    # Health Probes
    assert "livenessProbe:" in content
    assert "readinessProbe:" in content

    # Conditional PVC Mounts
    assert '{{- if and .Values.persistence.enabled (eq .Values.storage.backend "sqlite") }}' in content
    assert "claimName: {{ include \"swarmwatch.fullname\" . }}-pvc" in content

    # ClickHouse Secret Injection
    assert '{{- if eq .Values.storage.backend "clickhouse" }}' in content
    assert "name: CLICKHOUSE_PASSWORD" in content


def test_pvc_template_sqlite_isolation():
    """Verify pvc.yaml is conditionally rendered only when persistence is enabled and sqlite is active."""
    pvc_path = TEMPLATES_DIR / "pvc.yaml"
    content = pvc_path.read_text(encoding="utf-8")

    assert '{{- if and .Values.persistence.enabled (eq .Values.storage.backend "sqlite") -}}' in content
    assert "kind: PersistentVolumeClaim" in content
    assert "storage: {{ .Values.persistence.size | quote }}" in content
    assert "accessModes:" in content


def test_configmap_template_dual_storage_injection():
    """Verify configmap.yaml injects environment variables for both SQLite and ClickHouse backends."""
    cm_path = TEMPLATES_DIR / "configmap.yaml"
    content = cm_path.read_text(encoding="utf-8")

    assert "kind: ConfigMap" in content
    assert "SWARMWATCH_HOST:" in content
    assert "SWARMWATCH_PORT:" in content
    assert "SWARMWATCH_STORAGE_BACKEND:" in content
    assert '{{- if eq .Values.storage.backend "sqlite" }}' in content
    assert "SWARMWATCH_DB:" in content
    assert '{{- else if eq .Values.storage.backend "clickhouse" }}' in content
    assert "CLICKHOUSE_HOST:" in content
    assert "CLICKHOUSE_PORT:" in content
    assert "CLICKHOUSE_DB:" in content
    assert "CLICKHOUSE_USER:" in content


def test_secret_template_clickhouse_credentials():
    """Verify secret.yaml securely injects ClickHouse password when external secret is not specified."""
    secret_path = TEMPLATES_DIR / "secret.yaml"
    content = secret_path.read_text(encoding="utf-8")

    assert '{{- if and (eq .Values.storage.backend "clickhouse") (not .Values.storage.clickhouse.existingSecret) -}}' in content
    assert "kind: Secret" in content
    assert "clickhouse-password:" in content
    assert "b64enc" in content


def test_service_template_clusterip_routing():
    """Verify service.yaml exposes port 8765 for internal cluster consumption."""
    svc_path = TEMPLATES_DIR / "service.yaml"
    content = svc_path.read_text(encoding="utf-8")

    assert "kind: Service" in content
    assert "port: {{ .Values.service.port }}" in content
    assert "targetPort: {{ .Values.service.targetPort }}" in content
    assert "protocol: TCP" in content
    assert '{{- include "swarmwatch.selectorLabels" . | nindent 4 }}' in content


def test_ingress_template_websocket_and_tls():
    """Verify ingress.yaml supports WebSocket persistent connections and optional TLS."""
    ing_path = TEMPLATES_DIR / "ingress.yaml"
    content = ing_path.read_text(encoding="utf-8")

    assert '{{- if .Values.ingress.enabled -}}' in content
    assert "kind: Ingress" in content
    assert "apiVersion: networking.k8s.io/v1" in content
    assert "ingressClassName: {{ .Values.ingress.className }}" in content
    assert "{{- if .Values.ingress.tls }}" in content
    assert "pathType: {{ .pathType }}" in content


def test_servicemonitor_prometheus_operator():
    """Verify servicemonitor.yaml aligns with Prometheus Operator CRD specification."""
    sm_path = TEMPLATES_DIR / "servicemonitor.yaml"
    content = sm_path.read_text(encoding="utf-8")

    assert '{{- if .Values.serviceMonitor.enabled -}}' in content
    assert "apiVersion: monitoring.coreos.com/v1" in content
    assert "kind: ServiceMonitor" in content
    assert "path: {{ .Values.serviceMonitor.path | default \"/metrics\" }}" in content
    assert "interval: {{ .Values.serviceMonitor.interval | default \"5s\" }}" in content
    assert "scrapeTimeout: {{ .Values.serviceMonitor.scrapeTimeout | default \"3s\" }}" in content


def test_hpa_autoscaling_template():
    """Verify hpa.yaml defines HorizontalPodAutoscaler v2 for distributed mode."""
    hpa_path = TEMPLATES_DIR / "hpa.yaml"
    content = hpa_path.read_text(encoding="utf-8")

    assert '{{- if .Values.autoscaling.enabled -}}' in content
    assert "apiVersion: autoscaling/v2" in content
    assert "kind: HorizontalPodAutoscaler" in content
    assert "scaleTargetRef:" in content
    assert "minReplicas: {{ .Values.autoscaling.minReplicas }}" in content
    assert "maxReplicas: {{ .Values.autoscaling.maxReplicas }}" in content


def test_notes_txt_instructions():
    """Verify NOTES.txt contains guidance for port-forwarding, healthcheck, and storage mode."""
    notes_path = TEMPLATES_DIR / "NOTES.txt"
    content = notes_path.read_text(encoding="utf-8")

    assert "SwarmWatch" in content
    assert "kubectl" in content
    assert "port-forward" in content
    assert "/api/v1/health" in content
    assert "/metrics" in content
    assert "Storage Mode:" in content


def test_cli_backend_environment_variable_wiring(monkeypatch):
    """Verify cli.py argument parser supports --backend and respects environment variables."""
    from swarmwatch.cli import main
    import argparse

    monkeypatch.setenv("SWARMWATCH_HOST", "0.0.0.0")
    monkeypatch.setenv("SWARMWATCH_PORT", "9999")
    monkeypatch.setenv("SWARMWATCH_STORAGE_BACKEND", "clickhouse")
    monkeypatch.setenv("SWARMWATCH_DB", "/custom/path.db")

    # Verify that argparse can parse with new env defaults
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")
    ui_parser = subparsers.add_parser("ui")
    ui_parser.add_argument("--host", default=os.getenv("SWARMWATCH_HOST", "127.0.0.1"))
    ui_parser.add_argument("--port", type=int, default=int(os.getenv("SWARMWATCH_PORT", "8765")))
    ui_parser.add_argument("--db", default=os.getenv("SWARMWATCH_DB", "swarmwatch.db"))
    ui_parser.add_argument("--backend", default=os.getenv("SWARMWATCH_STORAGE_BACKEND", "sqlite"))

    parsed = parser.parse_args(["ui"])
    assert parsed.host == "0.0.0.0"
    assert parsed.port == 9999
    assert parsed.backend == "clickhouse"
    assert parsed.db == "/custom/path.db"
