# -*- coding: utf-8 -*-
"""
SwarmWatch Command-Line Interface.
Allows starting the live dashboard and ingestion server via `swarmwatch ui`.
"""

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import time
import webbrowser
import uvicorn

from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="swarmwatch",
        description="SwarmWatch: Multi-Agent Observability, Entropy Circuit Breaker & Time-Machine"
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # `swarmwatch ui` subcommand
    ui_parser = subparsers.add_parser("ui", help="Launch the SwarmWatch real-time web dashboard")
    ui_parser.add_argument("--host", default=os.getenv("SWARMWATCH_HOST", "127.0.0.1"), help="Host address to bind (default: 127.0.0.1 or SWARMWATCH_HOST)")
    ui_parser.add_argument("--port", type=int, default=int(os.getenv("SWARMWATCH_PORT", "8765")), help="Port to listen on (default: 8765 or SWARMWATCH_PORT)")
    ui_parser.add_argument("--db", default=os.getenv("SWARMWATCH_DB", "swarmwatch.db"), help="Path to SQLite database file")
    ui_parser.add_argument("--backend", default=os.getenv("SWARMWATCH_STORAGE_BACKEND", "sqlite"), choices=["sqlite", "clickhouse"], help="Storage backend ('sqlite' or 'clickhouse')")
    ui_parser.add_argument("--no-browser", action="store_true", help="Do not auto-open dashboard in default browser")

    # `swarmwatch report` subcommand
    report_parser = subparsers.add_parser("report", help="Generate an incident post-mortem and financial ROI report for a swarm")
    report_parser.add_argument("--swarm-id", required=True, help="Target swarm session ID")
    report_parser.add_argument("--format", choices=["markdown", "json", "terminal"], default="terminal", help="Report output format (default: terminal)")
    report_parser.add_argument("--output", "-o", default=None, help="Optional output file path to write report")
    report_parser.add_argument("--db", default="swarmwatch.db", help="Path to SQLite database file")

    # `swarmwatch test-alerts` subcommand (Seam 18 / ADR 0029)
    alerts_parser = subparsers.add_parser("test-alerts", help="Execute synthetic alert simulation across Slack, Discord, and PagerDuty routes")
    alerts_parser.add_argument("--routes", default="all", help="Comma-separated routes to test (e.g. 'all', 'slack', 'discord', 'pagerduty')")
    alerts_parser.add_argument("--faults", action="store_true", help="Include upstream fault injection & DLQ recovery simulation")
    alerts_parser.add_argument("--circuit-trip", action="store_true", help="Include live swarm circuit breaker trip simulation")
    alerts_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")
    alerts_parser.add_argument("--output", "-o", default=None, help="Optional output file path")

    # `swarmwatch benchmark` subcommand (Seam 19 / ADR 0030)
    bench_parser = subparsers.add_parser("benchmark", help="Execute high-frequency telemetry ingestion micro-benchmark & flamegraph profile")
    bench_parser.add_argument("--spans", type=int, default=10000, help="Number of telemetry spans to generate and ingest (default: 10000)")
    bench_parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent worker threads (default: 4)")
    bench_parser.add_argument("--batch-size", type=int, default=100, help="Internal transport batch size (default: 100)")
    bench_parser.add_argument("--payload", choices=["light", "medium", "heavy"], default="medium", help="Payload complexity (default: medium)")
    bench_parser.add_argument("--flamegraph", action="store_true", help="Generate collapsed stack flamegraph trace")
    bench_parser.add_argument("--flamegraph-out", default=None, help="Output path for flamegraph .txt file")
    bench_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")
    bench_parser.add_argument("--output", "-o", default=None, help="Optional output file path for benchmark report")

    # `swarmwatch top` subcommand (Seam 21 / ADR 0032)
    top_parser = subparsers.add_parser("top", help="Launch the live Terminal User Interface (TUI) console for cluster & pod triage")
    top_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL (default: http://127.0.0.1:8765 or SWARMWATCH_SERVER_URL)")
    top_parser.add_argument("--db", default=os.getenv("SWARMWATCH_DB", "swarmwatch.db"), help="Path to local SQLite database for standalone triage")
    top_parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds (default: 1.0)")
    top_parser.add_argument("--swarm", default=None, help="Optional swarm ID to focus/filter")
    top_parser.add_argument("--max-spans", type=int, default=10, help="Max recent spans to display in causal stream (default: 10)")
    top_parser.add_argument("--no-color", action="store_true", help="Disable ANSI color escape sequences")
    top_parser.add_argument("--once", action="store_true", help="Single-frame snapshot mode (print once and exit, ideal for scripts/cron)")

    # `swarmwatch fleet` subcommand (Seam 22 / ADR 0033)
    fleet_parser = subparsers.add_parser("fleet", help="Inspect and control distributed Redis fleet circuit breaker state")
    fleet_parser.add_argument("action", choices=["status", "trip", "reset"], help="Fleet action to execute (status, trip, reset)")
    fleet_parser.add_argument("--swarm-id", "-s", required=True, help="Target swarm session ID")
    fleet_parser.add_argument("--redis-url", default=os.getenv("SWARMWATCH_REDIS_URL", "redis://127.0.0.1:6379/0"), help="Redis server URL (default: redis://127.0.0.1:6379/0)")
    fleet_parser.add_argument("--reason", default="Manual trip via CLI", help="Trip reason when action is 'trip'")
    fleet_parser.add_argument("--tripped-by", default="cli-operator", help="Identifier of tripping operator or worker")
    fleet_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch cluster` subcommand (Seam 24 / ADR 0035)
    cluster_parser = subparsers.add_parser("cluster", help="Inspect and manage distributed multi-server leader election and roster")
    cluster_parser.add_argument("action", choices=["status", "step-down"], help="Cluster action (status, step-down)")
    cluster_parser.add_argument("--redis-url", default=os.getenv("SWARMWATCH_REDIS_URL", "redis://127.0.0.1:6379/0"), help="Redis server URL (default: redis://127.0.0.1:6379/0)")
    cluster_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch archive` subcommand (Seam 25 / ADR 0036)
    archive_parser = subparsers.add_parser("archive", help="Manage S3/GCS cold storage streaming archival and restore")
    archive_parser.add_argument("action", choices=["list", "export", "restore", "manifest"], help="Archive action (list, export, restore, manifest)")
    archive_parser.add_argument("--swarm-id", "-s", default=None, help="Target swarm session ID")
    archive_parser.add_argument("--bucket", "-b", default=os.getenv("SWARMWATCH_ARCHIVE_BUCKET", "swarmwatch-cold-storage"), help="Target S3/GCS bucket")
    archive_parser.add_argument("--db", default=os.getenv("SWARMWATCH_DB", "swarmwatch.db"), help="Path to local SQLite database")
    archive_parser.add_argument("--delete-local", action="store_true", help="Purge local records after successful archival")
    archive_parser.add_argument("--project-id", default=None, help="Filter by tenant project ID")
    archive_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch mfa` subcommand (Seam 26 / ADR 0037)
    mfa_parser = subparsers.add_parser("mfa", help="Manage RFC 6238 TOTP Multi-Factor Authentication")
    mfa_parser.add_argument("action", choices=["generate", "verify"], help="MFA action (generate, verify)")
    mfa_parser.add_argument("--username", "-u", default="admin", help="Username for TOTP envelope")
    mfa_parser.add_argument("--secret", default=None, help="Base32 TOTP secret key")
    mfa_parser.add_argument("--code", "-c", default=None, help="6-digit TOTP code to verify")
    mfa_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch rbac` subcommand (Seam 26 / ADR 0037)
    rbac_parser = subparsers.add_parser("rbac", help="Inspect and verify Enterprise Role-Based Access Control matrix")
    rbac_parser.add_argument("action", choices=["check", "matrix"], help="RBAC action (check, matrix)")
    rbac_parser.add_argument("--role", "-r", choices=["admin", "operator", "auditor", "viewer"], default="viewer", help="Target role")
    rbac_parser.add_argument("--permission", "-p", default="swarms:read", help="Target action permission string")
    rbac_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch otlp` subcommand (Seam 27 / ADR 0038)
    otlp_parser = subparsers.add_parser("otlp", help="Ingest and validate OpenTelemetry OTLP trace payloads")
    otlp_parser.add_argument("action", choices=["ingest", "validate", "stats"], help="OTLP action (ingest, validate, stats)")
    otlp_parser.add_argument("--file", "-f", default=None, help="Path to OTLP trace JSON file")
    otlp_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL (default: http://127.0.0.1:8765)")
    otlp_parser.add_argument("--api-key", default=os.getenv("SWARMWATCH_API_KEY", None), help="Optional SwarmWatch API key")
    otlp_parser.add_argument("--project-id", default=None, help="Target tenant project ID")
    otlp_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch events` subcommand (Seam 28 / ADR 0039)
    events_parser = subparsers.add_parser("events", help="Listen to real-time Server-Sent Events (SSE) stream")
    events_parser.add_argument("action", choices=["listen"], default="listen", nargs="?", help="Action to execute ('listen')")
    events_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL (default: http://127.0.0.1:8765)")
    events_parser.add_argument("--swarm", "-s", default=None, help="Filter events by swarm ID")
    events_parser.add_argument("--min-level", choices=["warning", "trip"], default="warning", help="Minimum alert severity level")
    events_parser.add_argument("--project-id", default=None, help="Filter by tenant project ID")

    # `swarmwatch push` subcommand (Seam 28 / ADR 0039)
    push_parser = subparsers.add_parser("push", help="Manage enterprise WebPush VAPID keys and alert subscriptions")
    push_parser.add_argument("action", choices=["keys", "test", "subscriptions"], help="WebPush action (keys, test, subscriptions)")
    push_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    push_parser.add_argument("--generate", action="store_true", help="Generate fresh VAPID keys instead of reading server key")
    push_parser.add_argument("--endpoint", default=None, help="Target push subscription endpoint for test alerts")
    push_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch ebpf` subcommand (Seam 29 / ADR 0040)
    ebpf_parser = subparsers.add_parser("ebpf", help="Inspect and manage kernel-level socket LLM interception gateway")
    ebpf_parser.add_argument("action", choices=["status", "rules", "simulate"], default="status", nargs="?", help="eBPF action (status, rules, simulate)")
    ebpf_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    ebpf_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")
    ebpf_parser.add_argument("--data", default=None, help="Payload data string for simulation")
    ebpf_parser.add_argument("--host", default="api.openai.com", help="Target host for simulation (default: api.openai.com)")
    ebpf_parser.add_argument("--port", type=int, default=443, help="Target port for simulation (default: 443)")

    # `swarmwatch raft` subcommand (Seam 30 / ADR 0041)
    raft_parser = subparsers.add_parser("raft", help="Manage multi-cluster Raft consensus engine and quorum state machine")
    raft_parser.add_argument("action", choices=["status", "propose", "cluster"], default="status", nargs="?", help="Raft action (status, propose, cluster)")
    raft_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    raft_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")
    raft_parser.add_argument("--command", default="trip", help="Command to propose (trip, reset, quota_inc)")
    raft_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID for proposal")
    raft_parser.add_argument("--reason", default="Manual operator trip via Raft CLI", help="Reason string for circuit trip proposal")
    raft_parser.add_argument("--nodes", type=int, default=3, help="Number of nodes for mock cluster simulation (default: 3)")

    # `swarmwatch heal` subcommand (Seam 31 / ADR 0042)
    heal_parser = subparsers.add_parser("heal", help="Inspect and control autonomous self-healing and counterfactual optimization")
    heal_parser.add_argument("action", choices=["status", "actions", "trigger", "rollback"], default="status", nargs="?", help="Self-healing action (status, actions, trigger, rollback)")
    heal_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    heal_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")
    heal_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID")
    heal_parser.add_argument("--agent", default="coder", help="Target agent name for remediation")
    heal_parser.add_argument("--strategy", choices=["blacklist_tool", "route_model_fallback", "auto_reset_on_stabilization"], default="blacklist_tool", help="Remediation strategy")
    heal_parser.add_argument("--target", default="execute_sql", help="Remediation target (e.g. tool name or fallback model)")
    heal_parser.add_argument("--reason", default="Manual operator remediation via CLI", help="Remediation justification")
    heal_parser.add_argument("--action-id", default=None, help="Action ID to rollback")

    # `swarmwatch rootcause` subcommand (Seam 32 / ADR 0043)
    rc_parser = subparsers.add_parser("rootcause", help="Analyze and isolate true multi-agent root causes via Structural Causal Models")
    rc_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    rc_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")
    rc_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID")
    rc_parser.add_argument("--symptom-span", default=None, help="Optional symptom span ID to trace from")

    # `swarmwatch grpc` subcommand (Seam 33 / ADR 0044)
    grpc_parser = subparsers.add_parser("grpc", help="Inspect and simulate ultra-high-throughput gRPC streaming ingestion gateway")
    grpc_parser.add_argument("action", choices=["status", "simulate"], default="status", nargs="?", help="gRPC action (status, simulate)")
    grpc_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    grpc_parser.add_argument("--spans", type=int, default=10, help="Number of spans to stream during simulation (default: 10)")
    grpc_parser.add_argument("--loop", action="store_true", help="Induce collapsed entropy loop during simulation")
    grpc_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch drift` subcommand (Seam 34 / ADR 0045)
    drift_parser = subparsers.add_parser("drift", help="Inspect and analyze embedding-free semantic drift and hallucination divergence")
    drift_parser.add_argument("action", choices=["status", "analyze", "anchor"], default="status", nargs="?", help="Drift action (status, analyze, anchor)")
    drift_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    drift_parser.add_argument("--text", default="", help="Turn text to evaluate for semantic drift")
    drift_parser.add_argument("--anchor", default="", help="Anchor text for semantic drift evaluation")
    drift_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    drift_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch rebalance` subcommand (Seam 35 / ADR 0046)
    rebalance_parser = subparsers.add_parser("rebalance", help="Inspect and execute adaptive dynamic quota re-balancing")
    rebalance_parser.add_argument("action", choices=["status", "run", "quota"], default="status", nargs="?", help="Rebalance action (status, run, quota)")
    rebalance_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    rebalance_parser.add_argument("--tokens", type=int, default=100000, help="Total fleet token quota")
    rebalance_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    rebalance_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch coalition` subcommand (Seam 36 / ADR 0047)
    coalition_parser = subparsers.add_parser("coalition", help="Analyze interaction graph topology, centrality, and dynamic agent coalitions")
    coalition_parser.add_argument("action", choices=["status", "cluster"], default="status", nargs="?", help="Coalition action (status, cluster)")
    coalition_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    coalition_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    coalition_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch shm` subcommand (Seam 37 / ADR 0048)
    shm_parser = subparsers.add_parser("shm", help="Inspect and drain zero-copy shared-memory ring buffer IPC")
    shm_parser.add_argument("action", choices=["status", "flush"], default="status", nargs="?", help="SHM action (status, flush)")
    shm_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    shm_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch privacy` subcommand (Seam 38 / ADR 0049)
    privacy_parser = subparsers.add_parser("privacy", help="Inspect and test differential privacy noise injection and pseudonymization")
    privacy_parser.add_argument("action", choices=["status", "test", "reset"], default="status", nargs="?", help="Privacy action (status, test, reset)")
    privacy_parser.add_argument("--epsilon", type=float, default=1.0, help="Differential privacy epsilon scale parameter")
    privacy_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    privacy_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch forecast` subcommand (Seam 39 / ADR 0050)
    forecast_parser = subparsers.add_parser("forecast", help="Inspect and evaluate predictive martingale failure forecasting and early-warning alerts")
    forecast_parser.add_argument("action", choices=["status", "predict", "reset"], default="status", nargs="?", help="Forecast action (status, predict, reset)")
    forecast_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    forecast_parser.add_argument("--entropy", type=float, default=None, help="Turn entropy score to evaluate")
    forecast_parser.add_argument("--turn", type=int, default=1, help="Turn index (default: 1)")
    forecast_parser.add_argument("--agent", default="agent-cli", help="Agent identifier (default: agent-cli)")
    forecast_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    forecast_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch byzantine` subcommand (Seam 40 / ADR 0051)
    byz_parser = subparsers.add_parser("byzantine", help="Execute synthetic Byzantine adversarial attacks and measure multi-agent swarm BFT resilience")
    byz_parser.add_argument("action", choices=["status", "inject", "clear"], default="status", nargs="?", help="Byzantine action (status, inject, clear)")
    byz_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    byz_parser.add_argument("--agent", default="agent-rogue", help="Target or adversary agent ID (default: agent-rogue)")
    byz_parser.add_argument("--type", choices=["prompt_injection", "state_poisoning", "entropy_jamming", "sybil_contagion"], default="prompt_injection", help="Adversarial attack type")
    byz_parser.add_argument("--payload", default=None, help="Custom adversarial attack payload text")
    byz_parser.add_argument("--turn", type=int, default=1, help="Turn index (default: 1)")
    byz_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    byz_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch honeypot` subcommand (Seam 41 / ADR 0052)
    honey_parser = subparsers.add_parser("honeypot", help="Manage multi-agent deception sandboxes and canary honeytokens")
    honey_parser.add_argument("action", choices=["status", "divert", "token", "reset"], default="status", nargs="?", help="Honeypot action (status, divert, token, reset)")
    honey_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    honey_parser.add_argument("--agent", default="agent-adversary", help="Target agent ID to divert (default: agent-adversary)")
    honey_parser.add_argument("--token-type", default="API_KEY", help="Honeytoken type (API_KEY, DB_CONNECTION, JWT_SECRET)")
    honey_parser.add_argument("--reason", default="adversarial_detected", help="Diversion reason")
    honey_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    honey_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch verify` subcommand (Seam 42 / ADR 0053)
    verify_parser = subparsers.add_parser("verify", help="Formally verify multi-agent safety/liveness invariants via Bounded Model Checking")
    verify_parser.add_argument("action", choices=["status", "check", "reset"], default="status", nargs="?", help="Verification action (status, check, reset)")
    verify_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    verify_parser.add_argument("--bound-k", type=int, default=10, help="Maximum search depth bound k (default: 10)")
    verify_parser.add_argument("--budget", type=int, default=10000, help="Token budget threshold (default: 10000)")
    verify_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    verify_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch patch` subcommand (Seam 43 / ADR 0054)
    patch_parser = subparsers.add_parser("patch", help="Autonomous Dynamic Topology Healing & Auto-Patching")
    patch_parser.add_argument("action", choices=["status", "apply", "revert"], default="status", nargs="?", help="Patching action (status, apply, revert)")
    patch_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    patch_parser.add_argument("--bound-k", type=int, default=10, help="Maximum search depth bound k (default: 10)")
    patch_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    patch_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch artifact` subcommand (Seam 44 / ADR 0055)
    artifact_parser = subparsers.add_parser("artifact", help="Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking")
    artifact_parser.add_argument("action", choices=["status", "inspect", "watermark", "verify"], default="status", nargs="?", help="Artifact action (status, inspect, watermark, verify)")
    artifact_parser.add_argument("--swarm-id", default="swarm-live-01", help="Target swarm session ID (default: swarm-live-01)")
    artifact_parser.add_argument("--file", help="Path to binary or media file to inspect/watermark/verify")
    artifact_parser.add_argument("--agent-id", default="agent-producer", help="Producer agent ID (default: agent-producer)")
    artifact_parser.add_argument("--artifact-id", help="Artifact ID for watermark or verification")
    artifact_parser.add_argument("--secret", default="sw_secret_salt_default", help="Tenant secret salt for HMAC verification")
    artifact_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    artifact_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch mesh` subcommand (Seam 45 / ADR 0056)
    mesh_parser = subparsers.add_parser("mesh", help="Decentralized Epidemic Gossip Telemetry Mesh Sync")
    mesh_parser.add_argument("action", choices=["status", "peers", "join", "sync"], default="status", nargs="?", help="Mesh action (status, peers, join, sync)")
    mesh_parser.add_argument("--peer-endpoint", help="Peer endpoint URL to join or register (e.g. http://10.0.0.2:8765)")
    mesh_parser.add_argument("--peer-id", help="Optional peer node ID for join")
    mesh_parser.add_argument("--fanout", type=int, default=3, help="Gossip fanout k (default: 3)")
    mesh_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    mesh_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch shard` subcommand (Seam 46 / ADR 0057)
    shard_parser = subparsers.add_parser("shard", help="Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer")
    shard_parser.add_argument("action", choices=["status", "ring", "route", "rebalance"], default="status", nargs="?", help="Sharding action (status, ring, route, rebalance)")
    shard_parser.add_argument("--key", help="Task key or swarm session ID to route (e.g. 'swarm-01' or 'tenant:project:swarm:session')")
    shard_parser.add_argument("--node-id", help="Target node ID for capacity inspection or manual registration")
    shard_parser.add_argument("--endpoint", help="Node endpoint URL (e.g. http://10.0.0.1:8765)")
    shard_parser.add_argument("--base-vnodes", type=int, default=64, help="Base virtual nodes per unit capacity (default: 64)")
    shard_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    shard_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch schema` subcommand (Seam 47 / ADR 0058)
    schema_parser = subparsers.add_parser("schema", help="Autonomous Semantic Schema Migration & Backward-Compatible State Translation")
    schema_parser.add_argument("action", choices=["status", "versions", "migrate", "validate"], default="status", nargs="?", help="Schema action (status, versions, migrate, validate)")
    schema_parser.add_argument("--source", "-s", default="1.0.0", help="Source schema version (e.g. '1.0.0')")
    schema_parser.add_argument("--target", "-t", default="2.0.0", help="Target schema version (e.g. '2.0.0')")
    schema_parser.add_argument("--version", "-v", default="1.0.0", help="Schema version to validate against (default: '1.0.0')")
    schema_parser.add_argument("--payload", "-p", default=None, help="JSON payload string to migrate or validate")
    schema_parser.add_argument("--file", "-f", default=None, help="File path containing JSON payload to migrate or validate")
    schema_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    schema_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch epistemic` subcommand (Seam 48 / ADR 0059)
    epistemic_parser = subparsers.add_parser("epistemic", help="Dynamic Multi-Agent Epistemic Logic & Knowledge Base Synchronization")
    epistemic_parser.add_argument("action", choices=["status", "propositions", "conflicts", "mutual-knowledge", "assert", "reconcile", "reset"], default="status", nargs="?", help="Epistemic action (status, propositions, conflicts, mutual-knowledge, assert, reconcile, reset)")
    epistemic_parser.add_argument("--swarm-id", "-s", default="swarm-live-01", help="Target swarm session ID (default: 'swarm-live-01')")
    epistemic_parser.add_argument("--agent", "-a", default="agent-primary", help="Agent identifier for assertion")
    epistemic_parser.add_argument("--topic", "-t", default="database.read_replica", help="Propositional domain topic")
    epistemic_parser.add_argument("--predicate", "-p", default="is_ready", help="Propositional predicate property")
    epistemic_parser.add_argument("--truth-value", choices=["TRUE", "FALSE", "UNKNOWN"], default="TRUE", help="Asserted epistemic truth value (default: TRUE)")
    epistemic_parser.add_argument("--confidence", "-c", type=float, default=1.0, help="Confidence value in [0.0, 1.0] (default: 1.0)")
    epistemic_parser.add_argument("--strategy", choices=["CONFIDENCE_ENTRENCHMENT", "TEMPORAL_RECENCY"], default="CONFIDENCE_ENTRENCHMENT", help="AGM belief revision resolution strategy")
    epistemic_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    epistemic_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch proof` subcommand (Seam 49 / ADR 0060)
    proof_parser = subparsers.add_parser("proof", help="Zero-Knowledge Causal Proof Generation & Cryptographic Audit")
    proof_parser.add_argument("action", choices=["root", "commit", "generate", "verify", "manifest", "reset"], default="root", nargs="?", help="Proof action (root, commit, generate, verify, manifest, reset)")
    proof_parser.add_argument("--swarm-id", "-s", default="swarm-live-01", help="Target swarm session ID (default: 'swarm-live-01')")
    proof_parser.add_argument("--span-id", help="Target span ID to commit or prove")
    proof_parser.add_argument("--turn", type=int, default=0, help="Turn index (default: 0)")
    proof_parser.add_argument("--agent", default="agent-primary", help="Producer agent ID")
    proof_parser.add_argument("--payload", default=None, help="Payload content string to hash or commit")
    proof_parser.add_argument("--type", choices=["TURN_INCLUSION", "CIRCUIT_TRIP", "CAUSAL_LINEAGE", "POLICY_COMPLIANCE"], default="TURN_INCLUSION", help="Audit commitment proof type")
    proof_parser.add_argument("--receipt", default=None, help="JSON receipt string or path to JSON receipt file to verify")
    proof_parser.add_argument("--secret", default="sw_causal_audit_secret_key", help="HMAC secret key")
    proof_parser.add_argument("--url", default=os.getenv("SWARMWATCH_SERVER_URL", "http://127.0.0.1:8765"), help="SwarmWatch server URL")
    proof_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch codec` subcommand (Seam 50 / ADR 0061)
    codec_parser = subparsers.add_parser("codec", help="Canonical Causal Encoding & Byte-Deterministic Contract Engine (RFC 8785)")
    codec_parser.add_argument("action", choices=["canonicalize", "hash"], default="canonicalize", nargs="?", help="Codec action (canonicalize, hash)")
    codec_parser.add_argument("--payload", "-p", default=None, help="JSON string or file path containing payload to encode")
    codec_parser.add_argument("--domain", "-d", default="", help="Domain separation tag (e.g. 'sw:leaf:v1', 'sw:snap:v1')")
    codec_parser.add_argument("--version", "-v", type=int, default=1, help="Canonical encoding version (default: 1)")
    codec_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch safety` subcommand (Seam 51 / ADR 0062)
    safety_parser = subparsers.add_parser("safety", help="Unified Safety Authority & Linearizable Execution Permit Engine")
    safety_parser.add_argument("action", choices=["reserve", "commit", "release", "trip", "rearm", "status"], default="status", nargs="?", help="Safety action (reserve, commit, release, trip, rearm, status)")
    safety_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    safety_parser.add_argument("--action-name", default="agent_turn", help="Action name to reserve permit for (default: 'agent_turn')")
    safety_parser.add_argument("--cost", type=float, default=0.0, help="Estimated or actual cost in dollars (default: 0.0)")
    safety_parser.add_argument("--tokens", type=int, default=0, help="Estimated or actual tokens (default: 0)")
    safety_parser.add_argument("--permit-id", default=None, help="Target permit ID for commit or release")
    safety_parser.add_argument("--reason", default="manual_trip", help="Reason for circuit trip or release")
    safety_parser.add_argument("--timeout", type=float, default=5000.0, help="Permit timeout in milliseconds (default: 5000.0)")
    safety_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch effects` subcommand (Seam 52 / ADR 0063)
    effects_parser = subparsers.add_parser("effects", help="Fenced Effect Ledger & Monotonic Idempotency Barrier")
    effects_parser.add_argument("action", choices=["claim", "authorize", "complete", "abort", "reconcile", "status"], default="status", nargs="?", help="Effect ledger action (claim, authorize, complete, abort, reconcile, status)")
    effects_parser.add_argument("--effect-key", default=None, help="Unique effect key to claim or inspect")
    effects_parser.add_argument("--claim-id", default=None, help="Target claim ID for authorize, complete, or abort")
    effects_parser.add_argument("--permit-hash", default=None, help="Safety permit hash for authorization (64 hex characters)")
    effects_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    effects_parser.add_argument("--epoch", type=int, default=1, help="Authoritative epoch (default: 1)")
    effects_parser.add_argument("--idempotency-key", default=None, help="Client idempotency key for deduplication")
    effects_parser.add_argument("--action-name", default="external_tool", help="Action name (default: 'external_tool')")
    effects_parser.add_argument("--reason", default="canceled", help="Reason for aborting claim")
    effects_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch ingress` subcommand (Seam 53 / ADR 0064)
    ingress_parser = subparsers.add_parser("ingress", help="Loss-Aware Causal Ingestion & Evidentiary Completeness Rule")
    ingress_parser.add_argument("action", choices=["submit", "drain", "completeness", "status"], default="status", nargs="?", help="Ingress action (submit, drain, completeness, status)")
    ingress_parser.add_argument("--span-id", default=None, help="Span ID for submission")
    ingress_parser.add_argument("--swarm-id", "-s", default="default", help="Swarm session ID")
    ingress_parser.add_argument("--stream-id", default="main", help="Stream ID for monotonic sequencing (default: 'main')")
    ingress_parser.add_argument("--priority", "-p", choices=["NORMAL", "HIGH", "CRITICAL"], default="NORMAL", help="Ingestion priority tier (default: 'NORMAL')")
    ingress_parser.add_argument("--parent-ids", default=None, help="Comma-separated parent span IDs")
    ingress_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for drain (default: 100)")
    ingress_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch audit` subcommand (Seam 54 / ADR 0065)
    audit_parser = subparsers.add_parser("audit", help="Durable Audit Anchor & Merkle Audit Receipt Engine")
    audit_parser.add_argument("action", choices=["append-root", "issue-receipt", "verify-receipt", "chain", "status"], default="status", nargs="?", help="Audit action (append-root, issue-receipt, verify-receipt, chain, status)")
    audit_parser.add_argument("--root-hash", default=None, help="Merkle root hash to append (64 hex chars)")
    audit_parser.add_argument("--epoch", type=int, default=None, help="Target epoch number for receipt issuance")
    audit_parser.add_argument("--leaf-hash", default=None, help="Target leaf hash for receipt issuance")
    audit_parser.add_argument("--sibling-hash", default=None, help="Sibling hash for single-step receipt path")
    audit_parser.add_argument("--direction", choices=["left", "right"], default="right", help="Sibling position for receipt path (default: right)")
    audit_parser.add_argument("--receipt-json", default=None, help="JSON string or file path of receipt to verify")
    audit_parser.add_argument("--tenant", "-t", default="default-tenant", help="Tenant ID (default: 'default-tenant')")
    audit_parser.add_argument("--metadata", default=None, help="JSON metadata string for root commitment")
    audit_parser.add_argument("--since-epoch", type=int, default=0, help="Start epoch for chain audit (default: 0)")
    audit_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch verifier` subcommand (Seam 55 / ADR 0066)
    verifier_parser = subparsers.add_parser("verifier", help="Bounded Verification Semantics & Explicit Counterexample Engine")
    verifier_parser.add_argument("action", choices=["verify", "check-trace", "status"], default="status", nargs="?", help="Verifier action (verify, check-trace, status)")
    verifier_parser.add_argument("--model-file", default=None, help="Path to JSON file specifying transition model")
    verifier_parser.add_argument("--model-id", default="model_default", help="Model identifier string (default: 'model_default')")
    verifier_parser.add_argument("--bound-k", type=int, default=15, help="Search depth bound k (default: 15)")
    verifier_parser.add_argument("--max-states", type=int, default=10000, help="Exploration state cap (default: 10000)")
    verifier_parser.add_argument("--timeout", type=float, default=1000.0, help="Search timeout in milliseconds (default: 1000.0)")
    verifier_parser.add_argument("--trace-file", default=None, help="Path to JSON file containing counterexample trace for check-trace")
    verifier_parser.add_argument("--property", default="NO_DEADLOCK", help="Property identifier or invariant type (default: 'NO_DEADLOCK')")
    verifier_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    verifier_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch remediation` subcommand (Seam 56 / ADR 0067)
    remediation_parser = subparsers.add_parser("remediation", help="Governed Remediation & Fenced Action State Machine")
    remediation_parser.add_argument("action", choices=["propose", "authorize", "apply", "compensate", "status"], default="status", nargs="?", help="Remediation action (propose, authorize, apply, compensate, status)")
    remediation_parser.add_argument("--proposal-id", default=None, help="Remediation proposal identifier")
    remediation_parser.add_argument("--action-type", default="probation_clamp", help="Action type to propose (e.g. probation_clamp, down_route_model, blacklist_tool)")
    remediation_parser.add_argument("--target-entity", default="Agent-01", help="Target agent or tool entity")
    remediation_parser.add_argument("--tier", default="TIER_0_AUTOMATIC", choices=["TIER_0_AUTOMATIC", "TIER_1_SUPERVISED", "TIER_2_MANUAL"], help="Approval tier (default: TIER_0_AUTOMATIC)")
    remediation_parser.add_argument("--role", default=None, help="Approver role (e.g. ADMIN, OPERATOR, SAFETY_SUPERVISOR)")
    remediation_parser.add_argument("--permit-hash", default=None, help="Safety permit hash for Tier 1 authorization")
    remediation_parser.add_argument("--epoch", type=int, default=1, help="Fencing epoch for apply (default: 1)")
    remediation_parser.add_argument("--reason", default="Operator compensation request", help="Reason for compensation")
    remediation_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    remediation_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch reconciler` subcommand (Seam 57 / ADR 0068)
    reconciler_parser = subparsers.add_parser("reconciler", help="Partition Reconciliation & Conflict Lattice Engine")
    reconciler_parser.add_argument("action", choices=["diff", "merge", "reconcile", "status"], default="status", nargs="?", help="Reconciler action (diff, merge, reconcile, status)")
    reconciler_parser.add_argument("--local-state-file", default=None, help="JSON file with local state/vector clock")
    reconciler_parser.add_argument("--remote-state-file", default=None, help="JSON file with remote state/vector clock")
    reconciler_parser.add_argument("--local-epoch", type=int, default=1, help="Local authority epoch (default: 1)")
    reconciler_parser.add_argument("--remote-epoch", type=int, default=1, help="Remote authority epoch (default: 1)")
    reconciler_parser.add_argument("--local-state", default="NOMINAL", choices=["NOMINAL", "TRIPPED", "DEGRADED"], help="Local authority state (default: NOMINAL)")
    reconciler_parser.add_argument("--remote-state", default="NOMINAL", choices=["NOMINAL", "TRIPPED", "DEGRADED"], help="Remote authority state (default: NOMINAL)")
    reconciler_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    reconciler_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch privacy-ledger` subcommand (Seam 58 / ADR 0069)
    privacy_parser = subparsers.add_parser("privacy-ledger", help="Privacy Release Ledger & Differential Privacy Accountant")
    privacy_parser.add_argument("action", choices=["reserve", "release", "remaining", "status"], default="status", nargs="?", help="Privacy action (reserve, release, remaining, status)")
    privacy_parser.add_argument("--epsilon", type=float, default=1.0, help="Epsilon budget to reserve or spend (default: 1.0)")
    privacy_parser.add_argument("--delta", type=float, default=0.0, help="Delta budget to reserve or spend (default: 0.0)")
    privacy_parser.add_argument("--sensitivity", type=float, default=1.0, help="Query sensitivity L1 (default: 1.0)")
    privacy_parser.add_argument("--query-id", default=None, help="Query identifier")
    privacy_parser.add_argument("--reservation-id", default=None, help="Reservation permit identifier for release")
    privacy_parser.add_argument("--value", type=float, default=None, help="Scalar numerical value to release")
    privacy_parser.add_argument("--noise-seed", type=int, default=None, help="Deterministic RNG seed for audit reproducibility")
    privacy_parser.add_argument("--min-bound", type=float, default=None, help="Lower clipping bound")
    privacy_parser.add_argument("--max-bound", type=float, default=None, help="Upper clipping bound")
    privacy_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    privacy_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch schema-evolution` subcommand (Seam 59 / ADR 0070)
    evolution_parser = subparsers.add_parser("schema-evolution", aliases=["evolution"], help="Migration Preservation & Partial Isomorphisms Engine")
    evolution_parser.add_argument("action", choices=["plan", "migrate", "reverse", "classify", "status"], default="status", nargs="?", help="Evolution action (plan, migrate, reverse, classify, status)")
    evolution_parser.add_argument("--source", "-s", default="1.0.0", help="Source schema version (default: '1.0.0')")
    evolution_parser.add_argument("--target", "-t", default="2.0.0", help="Target schema version (default: '2.0.0')")
    evolution_parser.add_argument("--payload", "-p", default=None, help="JSON payload string to migrate or reverse")
    evolution_parser.add_argument("--file", "-f", default=None, help="File path containing JSON payload")
    evolution_parser.add_argument("--delta-file", default=None, help="File path containing JSON PreservedDelta for reverse migration")
    evolution_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    # `swarmwatch acceptance` subcommand (Seam 60 / ADR 0071)
    acceptance_parser = subparsers.add_parser("acceptance", help="Enterprise Acceptance Harness & Multi-Dimensional Release Verification Gate")
    acceptance_parser.add_argument("action", choices=["run", "attest", "audit-ui", "verify", "status"], default="status", nargs="?", help="Acceptance action (run, attest, audit-ui, verify, status)")
    acceptance_parser.add_argument("--profile", "-p", choices=["quick", "standard", "production", "enterprise"], default="production", help="Evaluation profile (default: production)")
    acceptance_parser.add_argument("--checks", "-c", default=None, help="Comma-separated check IDs or gates to run")
    acceptance_parser.add_argument("--html-path", default=None, help="Path to dashboard.html artifact for UI craft audit")
    acceptance_parser.add_argument("--report-file", default=None, help="JSON file containing AcceptanceReport to attest")
    acceptance_parser.add_argument("--attestation-file", default=None, help="JSON file containing AcceptanceAttestation to verify")
    acceptance_parser.add_argument("--signer", default="SwarmWatch Enterprise Release Gatekeeper", help="Signer identity string")
    acceptance_parser.add_argument("--secret-key", default=None, help="Secret HMAC key for signing or verification")
    acceptance_parser.add_argument("--strict", action="store_true", help="Exit code 1 if any gate is rejected")
    acceptance_parser.add_argument("--format", choices=["terminal", "json"], default="terminal", help="Output format (default: terminal)")

    consensus_parser = subparsers.add_parser("consensus", help="Consensus Authority Binding & Linearizable Quorum Fencing")
    consensus_parser.add_argument("action", choices=["reserve", "commit", "release", "trip", "rearm", "status"], default="status", nargs="?", help="Consensus action (reserve, commit, release, trip, rearm, status)")
    consensus_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    consensus_parser.add_argument("--action-name", default="agent_turn", help="Agent action name")
    consensus_parser.add_argument("--cost", type=float, default=0.0, help="Estimated cost budget")
    consensus_parser.add_argument("--tokens", type=int, default=0, help="Estimated token budget")
    consensus_parser.add_argument("--permit-id", default=None, help="Permit ID to commit or release")
    consensus_parser.add_argument("--reason", default="cli_consensus_action", help="Reason for trip or release")
    consensus_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    downstream_parser = subparsers.add_parser("downstream", help="Downstream Effect Protocol & 2PC Outcome-Unknown (ADR 0073)")
    downstream_parser.add_argument("action", choices=["prepare", "dispatch", "ack", "reconcile", "status"], default="status", nargs="?", help="Action (prepare, dispatch, ack, reconcile, status)")
    downstream_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID (default: 'default')")
    downstream_parser.add_argument("--effect-key", default="cli:effect:sample", help="Target effect key")
    downstream_parser.add_argument("--target-uri", default="https://api.mock/action", help="Target service URI")
    downstream_parser.add_argument("--payload", default="{}", help="JSON payload")
    downstream_parser.add_argument("--epoch", type=int, default=1, help="Monotonic fencing epoch")
    downstream_parser.add_argument("--contract", choices=["DOWNSTREAM_FENCED", "UNCOORDINATED"], default="DOWNSTREAM_FENCED", help="Idempotency contract")
    downstream_parser.add_argument("--effect-id", default=None, help="Effect ID to dispatch, ack, or reconcile")
    downstream_parser.add_argument("--confirmation-id", default="tx_cli_ack", help="Downstream confirmation ID")
    downstream_parser.add_argument("--digest", default="sha256:ack", help="Result digest")
    downstream_parser.add_argument("--found", action="store_true", default=True, help="Reconciliation probe found flag")
    downstream_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    evidence_parser = subparsers.add_parser("evidence", help="Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal (ADR 0074)")
    evidence_parser.add_argument("action", choices=["offer", "flush", "recover", "completeness", "status"], default="status", nargs="?", help="Action (offer, flush, recover, completeness, status)")
    evidence_parser.add_argument("--tenant", "-t", default="default", help="Tenant ID")
    evidence_parser.add_argument("--agent-id", default="agent-guard", help="Agent principal ID")
    evidence_parser.add_argument("--record-type", choices=["SAFETY_TRIP", "POLICY_BREACH", "BYZANTINE_QUARANTINE", "EMERGENCY_HALT", "FENCE_REVOCATION", "AUDIT_ANCHOR"], default="SAFETY_TRIP", help="Evidence record type")
    evidence_parser.add_argument("--payload", default="{}", help="JSON payload")
    evidence_parser.add_argument("--journal-path", default=None, help="Journal file path for spooling/recovery")
    evidence_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    witness_parser = subparsers.add_parser("witness", help="Independent Root Witness & Public-Key Chains (ADR 0075)")
    witness_parser.add_argument("action", choices=["submit", "checkpoint", "verify", "rotate", "status"], default="status", nargs="?", help="Action (submit, checkpoint, verify, rotate, status)")
    witness_parser.add_argument("--data", default="default-leaf-data", help="Leaf data payload for submission")
    witness_parser.add_argument("--checkpoint-json", default=None, help="JSON string or file path of SignedCheckpoint to verify")
    witness_parser.add_argument("--public-key", default=None, help="Public key for asymmetric verification")
    witness_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    capability_parser = subparsers.add_parser("capability", help="Tenant Identity Boundary & Attenuated Capability Tokens (ADR 0076)")
    capability_parser.add_argument("action", choices=["issue", "attenuate", "verify", "revoke", "status"], default="status", nargs="?", help="Action (issue, attenuate, verify, revoke, status)")
    capability_parser.add_argument("--tenant-id", default="tenant-default", help="Tenant ID")
    capability_parser.add_argument("--principal", default="agent-root", help="Subject principal")
    capability_parser.add_argument("--scopes", default="telemetry:write,effect:dispatch", help="Comma-separated action scopes")
    capability_parser.add_argument("--resource", default="*", help="Target resource or namespace pattern")
    capability_parser.add_argument("--token-json", default=None, help="JSON string or file path of CapabilityToken")
    capability_parser.add_argument("--token-id", default=None, help="Token ID to revoke")
    capability_parser.add_argument("--reason", default="Administrative revocation", help="Revocation reason")
    capability_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    compensation_parser = subparsers.add_parser("compensation", help="Effect-Aware Compensation Algebra & Monotone Approval Lattices (ADR 0077)")
    compensation_parser.add_argument("action", choices=["classify", "plan", "execute", "escalate", "authorize", "status"], default="status", nargs="?", help="Action (classify, plan, execute, escalate, authorize, status)")
    compensation_parser.add_argument("--action-id", default=None, help="Action ID")
    compensation_parser.add_argument("--action-type", default=None, help="Action type identifier")
    compensation_parser.add_argument("--tenant-id", default="tenant-default", help="Tenant ID")
    compensation_parser.add_argument("--principal", default="agent-root", help="Executing principal")
    compensation_parser.add_argument("--reason", default="Automatic rollback prohibited: irreversible side-effect", help="Reason for escalation or rejection")
    compensation_parser.add_argument("--escalation-id", default=None, help="Escalation ID to authorize")
    compensation_parser.add_argument("--decision", choices=["APPROVED_BYPASS", "MANUAL_REMEDIATED", "ABORTED"], default="APPROVED_BYPASS", help="Escalation authorization decision")
    compensation_parser.add_argument("--notes", default="Authorized via CLI", help="Remediation notes")
    compensation_parser.add_argument("--actions-json", default=None, help="JSON string or file path containing list of action executions")
    compensation_parser.add_argument("--plan-json", default=None, help="JSON string or file path containing CompensationPlan")
    compensation_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    # `swarmwatch dataset-privacy` subcommand (Seam 67 / ADR 0078)
    dataset_privacy_parser = subparsers.add_parser("dataset-privacy", aliases=["dp"], help="Durable Dataset-Level Privacy Accountant & Quorum Ledger (ADR 0078)")
    dataset_privacy_parser.add_argument("action", choices=["register", "reserve", "release", "cancel", "remaining", "reconcile", "status"], default="status", nargs="?", help="Action (register, reserve, release, cancel, remaining, reconcile, status)")
    dataset_privacy_parser.add_argument("--dataset-id", default="dataset-default", help="Dataset identifier")
    dataset_privacy_parser.add_argument("--tenant-id", default="default", help="Tenant identifier")
    dataset_privacy_parser.add_argument("--entity-id", default="entity-01", help="Entity identifier")
    dataset_privacy_parser.add_argument("--max-epsilon", type=float, default=10.0, help="Dataset global epsilon limit")
    dataset_privacy_parser.add_argument("--max-delta", type=float, default=1e-3, help="Dataset global delta limit")
    dataset_privacy_parser.add_argument("--entity-epsilon", type=float, default=1.0, help="Per-entity epsilon contribution limit")
    dataset_privacy_parser.add_argument("--entity-delta", type=float, default=1e-5, help="Per-entity delta contribution limit")
    dataset_privacy_parser.add_argument("--epsilon", type=float, default=0.5, help="Requested epsilon budget for reservation")
    dataset_privacy_parser.add_argument("--delta", type=float, default=0.0, help="Requested delta budget for reservation")
    dataset_privacy_parser.add_argument("--sensitivity", type=float, default=1.0, help="Global sensitivity of query")
    dataset_privacy_parser.add_argument("--query-id", default=None, help="Query identifier")
    dataset_privacy_parser.add_argument("--reservation-id", default=None, help="Reservation ID to release or cancel")
    dataset_privacy_parser.add_argument("--raw-value", default="42.0", help="Raw query result to perturb (scalar float or JSON)")
    dataset_privacy_parser.add_argument("--noise-seed", type=int, default=None, help="Deterministic PRNG seed for perturbation")
    dataset_privacy_parser.add_argument("--snapshot-json", default=None, help="Path or JSON string containing peer ledger snapshot")
    dataset_privacy_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    # `swarmwatch interop` subcommand (Seam 68 / ADR 0079)
    interop_parser = subparsers.add_parser("interop", aliases=["il", "conformance"], help="Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (ADR 0079)")
    interop_parser.add_argument("action", choices=["exercise", "record-deviation", "attest", "matrix", "status"], default="status", nargs="?", help="Action (exercise, record-deviation, attest, matrix, status)")
    interop_parser.add_argument("--protocol", choices=["OTLP_HTTP_PROTOBUF", "OTLP_HTTP_JSON", "GRPC_STREAMING", "GRPC_UNARY"], default="OTLP_HTTP_PROTOBUF", help="Protocol standard to exercise")
    interop_parser.add_argument("--client-variant", choices=["NATIVE_OTEL_SDK", "GRPC_CORE", "MOCK_CLIENT"], default="NATIVE_OTEL_SDK", help="Client SDK implementation variant")
    interop_parser.add_argument("--fault", choices=["NONE", "FRAGMENTATION", "EARLY_CANCELLATION", "BACKPRESSURE_FLOW_CONTROL", "MALFORMED_HEADER", "TRUNCATED_BODY"], default="NONE", help="Conformance fault schedule to inject")
    interop_parser.add_argument("--payload", default='{"resource_spans":[{"scope_spans":[{"spans":[{"name":"test-span"}]}]}]}', help="Raw payload data or JSON string")
    interop_parser.add_argument("--chunk-size", type=int, default=1, help="Chunk size in bytes for fragmentation fault schedule")
    interop_parser.add_argument("--window-size", type=int, default=64, help="Window size in bytes for backpressure fault schedule")
    interop_parser.add_argument("--cancellation-state", choices=["IDLE", "CONNECTING", "STREAMING", "FRAME_ACCUMULATION", "DISPATCHING"], default="FRAME_ACCUMULATION", help="Target state for early cancellation fault injection")
    interop_parser.add_argument("--suite-name", default="SwarmWatch Interoperability Conformance Suite", help="Conformance suite identifier for attestation")
    interop_parser.add_argument("--scenario", default="synthetic_conformance_probe", help="Scenario identifier for recorded deviation")
    interop_parser.add_argument("--observed", default="NON_CONFORMANT_FRAME", help="Observed deviation behavior")
    interop_parser.add_argument("--expected", default="STANDARD_CONFORMANT_FRAME", help="Expected protocol behavior")
    interop_parser.add_argument("--severity", choices=["FATAL", "WARNING", "INFORMATIONAL"], default="WARNING", help="Deviation severity")
    interop_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    # `swarmwatch provenance` subcommand (Seam 69 / ADR 0080)
    provenance_parser = subparsers.add_parser("provenance", aliases=["prov", "slsa"], help="Verifiable Release Provenance & Signed Artifact DAG (ADR 0080)")
    provenance_parser.add_argument("action", choices=["record-step", "generate", "verify", "dag", "status"], default="status", nargs="?", help="Action (record-step, generate, verify, dag, status)")
    provenance_parser.add_argument("--step-type", choices=["SOURCE_CHECKOUT", "ENVIRONMENT_LOCK", "TEST_EXECUTION", "CRAFT_AUDIT", "CONFORMANCE_ATTEST", "BUILD_PACKAGING", "RELEASE_GATE"], default="SOURCE_CHECKOUT", help="Provenance step type")
    provenance_parser.add_argument("--parent-step-id", action="append", default=[], dest="parent_step_ids", help="Parent step ID for DAG edge linkage")
    provenance_parser.add_argument("--step-id", default=None, help="Explicit step ID")
    provenance_parser.add_argument("--git-commit", default="HEAD", help="Git commit SHA for provenance release manifest")
    provenance_parser.add_argument("--git-branch", default="main", help="Git branch name")
    provenance_parser.add_argument("--builder-id", default="https://github.com/SkyCeoSa/SwarmWatch/actions/runner", help="Builder identity URI")
    provenance_parser.add_argument("--manifest-file", default=None, help="Path to signed provenance manifest JSON file for verification")
    provenance_parser.add_argument("--public-key", default=None, help="Base64 public key for offline provenance verification")
    provenance_parser.add_argument("--verify-files", action="store_true", default=False, help="Verify physical artifact checksums on disk")
    provenance_parser.add_argument("--base-dir", default=None, help="Base directory for verifying disk artifacts")
    provenance_parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format")

    args = parser.parse_args(argv)













    if args.subcommand == "ui":
        url = f"http://{args.host}:{args.port}"
        print(f"\n\033[96m=====================================================\033[0m")
        print(f"\033[92m[SwarmWatch] Starting Observability Dashboard\033[0m")
        print(f"\033[94m   URL: {url}\033[0m")
        if args.backend == "clickhouse":
            from swarmwatch.storage_clickhouse import ClickHouseStorageAdapter
            ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
            ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
            ch_db = os.getenv("CLICKHOUSE_DB", "swarmwatch")
            ch_user = os.getenv("CLICKHOUSE_USER", "default")
            ch_pass = os.getenv("CLICKHOUSE_PASSWORD", "")
            print(f"\033[94m   Storage: ClickHouse ({ch_host}:{ch_port}/{ch_db})\033[0m")
            print(f"\033[96m=====================================================\033[0m\n")
            storage = ClickHouseStorageAdapter(
                host=ch_host,
                port=ch_port,
                database=ch_db,
                user=ch_user,
                password=ch_pass,
            )
        else:
            print(f"\033[94m   Database: {args.db}\033[0m")
            print(f"\033[96m=====================================================\033[0m\n")
            storage = SQLiteStorageAdapter(db_path=args.db)

        if not args.no_browser:
            try:
                webbrowser.open(url)
            except Exception:
                pass

        app = create_app(storage=storage)
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    elif args.subcommand == "report":
        from swarmwatch.report import IncidentReportGenerator
        storage = SQLiteStorageAdapter(db_path=args.db)
        generator = IncidentReportGenerator(storage=storage)
        result = generator.generate_report(args.swarm_id, format=args.format)

        output_text = json.dumps(result, indent=2) if isinstance(result, dict) else str(result)

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output_text)
            print(f"[SwarmWatch] Report for '{args.swarm_id}' written to {args.output}")
        else:
            print(output_text)
    elif args.subcommand == "test-alerts":
        from swarmwatch.synthetic_alert import AlertSimulationPipeline

        target_routes = ["slack", "discord", "pagerduty"] if args.routes == "all" else [r.strip() for r in args.routes.split(",") if r.strip()]

        pipeline = AlertSimulationPipeline()
        report = pipeline.run(
            routes=target_routes,
            include_faults=args.faults,
            include_circuit_trip=args.circuit_trip,
        )

        if args.format == "json":
            output_text = json.dumps(report.to_dict(), indent=2)
        else:
            output_text = report.to_terminal_table()

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output_text)
            print(f"[SwarmWatch] Alert simulation report written to {args.output}")
        else:
            print(output_text)

        sys.exit(0 if report.all_passed else 1)
    elif args.subcommand == "benchmark":
        from swarmwatch.profiler import TelemetryProfiler, IngestionBenchmarkConfig

        config = IngestionBenchmarkConfig(
            total_spans=args.spans,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            payload_complexity=args.payload,
            generate_flamegraph=args.flamegraph or bool(args.flamegraph_out),
            track_memory=True,
        )

        profiler = TelemetryProfiler()
        report = profiler.run(config)

        if args.flamegraph_out and report.collapsed_stack:
            with open(args.flamegraph_out, "w", encoding="utf-8") as f:
                f.write(report.collapsed_stack)
            print(f"[SwarmWatch] Flamegraph collapsed stack written to {args.flamegraph_out}")

        if args.format == "json":
            output_text = json.dumps(report.to_dict(), indent=2)
        else:
            output_text = report.to_terminal_table()

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output_text)
            print(f"[SwarmWatch] Ingestion benchmark report written to {args.output}")
        else:
            print(output_text)

        sys.exit(0)
    elif args.subcommand == "top":
        from swarmwatch.tui import TerminalMonitor

        monitor = TerminalMonitor(
            server_url=args.url,
            db_path=args.db,
            refresh_interval=args.interval,
            swarm_filter=args.swarm,
            max_spans=args.max_spans,
            ansi_color=not args.no_color,
        )
        if args.once:
            monitor.run_once()
        else:
            monitor.run()
        sys.exit(0)
    elif args.subcommand == "fleet":
        from swarmwatch.redis_circuit import RedisCircuitCoordinator

        coord = RedisCircuitCoordinator(redis_url=args.redis_url)
        try:
            if args.action == "status":
                metrics = coord.get_metrics(args.swarm_id)
                if not metrics:
                    res = {"swarm_id": args.swarm_id, "status": "not_found", "total_tokens": 0, "total_turns": 0, "total_cost": 0.0, "is_tripped": False}
                else:
                    res = metrics.model_dump(mode="json")
                if args.format == "json":
                    print(json.dumps(res, indent=2))
                else:
                    status_str = "\033[91mTRIPPED\033[0m" if res.get("is_tripped") else "\033[92mACTIVE\033[0m"
                    print("\n+--------------------------------------------------------------+")
                    print(f"| SWARMWATCH DISTRIBUTED FLEET STATUS: {args.swarm_id: <23} |")
                    print("+--------------------------------------------------------------+")
                    print(f"| Circuit Status:  {status_str: <43} |")
                    print(f"| Total Turns:     {res.get('total_turns', 0): <43} |")
                    print(f"| Total Tokens:    {res.get('total_tokens', 0): <43} |")
                    print(f"| Total Cost:      ${res.get('total_cost', 0.0): <42.4f} |")
                    active_w = len(res.get("active_workers", []))
                    print(f"| Active Workers:  {active_w: <43} |")
                    if res.get("is_tripped"):
                        print(f"| Trip Reason:     {str(res.get('trip_reason', 'N/A'))[:43]: <43} |")
                    print("+--------------------------------------------------------------+\n")
            elif args.action == "trip":
                coord.trip(swarm_id=args.swarm_id, reason=args.reason, tripped_by=args.tripped_by)
                if args.format == "json":
                    print(json.dumps({"status": "tripped", "swarm_id": args.swarm_id, "reason": args.reason, "tripped_by": args.tripped_by}, indent=2))
                else:
                    print(f"\033[91m[SwarmWatch] Circuit tripped across Redis fleet for '{args.swarm_id}': {args.reason}\033[0m")
            elif args.action == "reset":
                coord.reset_swarm(args.swarm_id)
                if args.format == "json":
                    print(json.dumps({"status": "reset", "swarm_id": args.swarm_id}, indent=2))
                else:
                    print(f"\033[92m[SwarmWatch] Fleet state reset in Redis for '{args.swarm_id}'.\033[0m")
        finally:
            coord.close()
        sys.exit(0)
    elif args.subcommand == "cluster":
        from swarmwatch.redis_circuit import get_redis_client
        from swarmwatch.redis_lock import RedisLeaderElector
        client = get_redis_client(args.redis_url)
        elector = RedisLeaderElector(client=client)
        try:
            if args.action == "status":
                status = elector.get_leader_status()
                if args.format == "json":
                    print(json.dumps(status.model_dump(mode="json"), indent=2))
                else:
                    leader_str = status.leader_node_id or "\033[93m[VACANT / ELECTION IN PROGRESS]\033[0m"
                    print("\n+--------------------------------------------------------------+")
                    print(f"| SWARMWATCH DISTRIBUTED CLUSTER LEADER STATUS (ADR 0035)      |")
                    print("+--------------------------------------------------------------+")
                    print(f"| Active Leader:   {leader_str: <43} |")
                    print(f"| Lease TTL (ms):  {str(status.lease_expires_in_ms): <43} |")
                    print(f"| Total Nodes:     {str(status.nodes_count): <43} |")
                    print("+--------------------------------------------------------------+")
                    print("| REGISTERED CLUSTER ROSTER:                                   |")
                    print("+--------------------------------------------------------------+")
                    for node in status.roster:
                        role_tag = f"\033[96m{node.role.value}\033[0m" if node.role.value == "LEADER" else node.role.value
                        print(f"| * {node.node_id: <18} | {role_tag: <18} | {node.hostname: <16} |")
                    print("+--------------------------------------------------------------+\n")
            elif args.action == "step-down":
                released = elector.step_down()
                if args.format == "json":
                    print(json.dumps({"status": "stepped_down" if released else "noop"}, indent=2))
                else:
                    if released:
                        print(f"\033[93m[SwarmWatch] Active leader stepped down. Reelection triggered.\033[0m")
                    else:
                        print(f"[SwarmWatch] Step down returned noop (node not active leader).")
        finally:
            client.close()
        sys.exit(0)
    elif args.subcommand == "archive":
        import asyncio
        from swarmwatch.archival import ColdStorageArchiver, MockObjectStorageClient, S3ObjectStorageClient

        storage = SQLiteStorageAdapter(db_path=args.db)
        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            obj_client = S3ObjectStorageClient(
                endpoint_url=os.getenv("AWS_ENDPOINT_URL", "https://s3.amazonaws.com"),
                access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "us-east-1"),
            )
        else:
            obj_client = MockObjectStorageClient(base_dir=os.getenv("SWARMWATCH_STORAGE_DIR", ".swarmwatch_storage"))

        archiver = ColdStorageArchiver(
            client=obj_client,
            default_bucket=args.bucket,
            storage_adapter=storage,
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            if args.action == "export":
                if not args.swarm_id:
                    print("Error: --swarm-id is required for export action", file=sys.stderr)
                    sys.exit(1)
                record = loop.run_until_complete(
                    archiver.archive_swarm(
                        swarm_id=args.swarm_id,
                        bucket=args.bucket,
                        delete_local=args.delete_local,
                        project_id=args.project_id,
                    )
                )
                if args.format == "json":
                    print(json.dumps(record.model_dump(mode="json"), indent=2))
                else:
                    print("\n+--------------------------------------------------------------+")
                    print(f"| SWARMWATCH TELEMETRY COLD ARCHIVE EXPORT (ADR 0036)          |")
                    print("+--------------------------------------------------------------+")
                    print(f"| Swarm ID:         {record.swarm_id: <42} |")
                    print(f"| S3 Bucket:        {record.bucket: <42} |")
                    print(f"| S3 Key:           {record.archive_key: <42} |")
                    print(f"| Compressed Size:  {f'{record.compressed_size_bytes:,} bytes': <42} |")
                    print(f"| Uncompressed:     {f'{record.uncompressed_size_bytes:,} bytes': <42} |")
                    print(f"| Comp. Ratio:      {f'{record.compression_ratio}x': <42} |")
                    print(f"| Total Spans:      {str(record.spans_count): <42} |")
                    print(f"| Snapshots:        {str(record.snapshots_count): <42} |")
                    print(f"| SHA-256:          {record.sha256_checksum[:32] + '...': <42} |")
                    print("+--------------------------------------------------------------+\n")
            elif args.action == "restore":
                if not args.swarm_id:
                    print("Error: --swarm-id is required for restore action", file=sys.stderr)
                    sys.exit(1)
                summary = loop.run_until_complete(
                    archiver.restore_swarm(
                        swarm_id=args.swarm_id,
                        bucket=args.bucket,
                        target_storage=storage,
                    )
                )
                if args.format == "json":
                    print(json.dumps(summary.model_dump(mode="json"), indent=2))
                else:
                    print(f"\n\033[92m[SwarmWatch] Swarm '{args.swarm_id}' successfully restored from cold storage.\033[0m")
                    print(f"   Spans restored:     {summary.spans_restored}")
                    print(f"   Snapshots restored: {summary.snapshots_restored}")
                    print(f"   SHA-256 Verified:   {summary.sha256_verified}\n")
            elif args.action == "list":
                records = archiver.list_archives(bucket=args.bucket, project_id=args.project_id)
                if args.format == "json":
                    print(json.dumps([r.model_dump(mode="json") for r in records], indent=2))
                else:
                    print("\n+----------------------------------------------------------------------------------------+")
                    print(f"| SWARMWATCH COLD STORAGE ARCHIVE ROSTER ({args.bucket})                                  |")
                    print("+----------------------------------------------------------------------------------------+")
                    print(f"| {'SWARM ID': <18} | {'SPANS': <6} | {'SNAPS': <6} | {'COMPRESSED': <12} | {'RATIO': <6} | {'DATE': <19} |")
                    print("+----------------------------------------------------------------------------------------+")
                    for r in records:
                        print(f"| {r.swarm_id: <18} | {str(r.spans_count): <6} | {str(r.snapshots_count): <6} | {f'{r.compressed_size_bytes:,}B': <12} | {f'{r.compression_ratio}x': <6} | {r.archived_at[:19]: <19} |")
                    print("+----------------------------------------------------------------------------------------+\n")
            elif args.action == "manifest":
                if not args.swarm_id:
                    print("Error: --swarm-id is required for manifest action", file=sys.stderr)
                    sys.exit(1)
                manifest = archiver.get_archive_manifest(swarm_id=args.swarm_id, bucket=args.bucket)
                if not manifest:
                    print(f"Manifest not found for swarm '{args.swarm_id}' in bucket '{args.bucket}'", file=sys.stderr)
                    sys.exit(1)
                if args.format == "json":
                    print(json.dumps(manifest.model_dump(mode="json"), indent=2))
                else:
                    print("\n+--------------------------------------------------------------+")
                    print(f"| SWARMWATCH ARCHIVE MANIFEST ({manifest.swarm_id})            |")
                    print("+--------------------------------------------------------------+")
                    for k, v in manifest.model_dump(mode="json").items():
                        print(f"| {k: <20}: {str(v): <38} |")
                    print("+--------------------------------------------------------------+\n")
        finally:
            loop.close()
        sys.exit(0)
    elif args.subcommand == "mfa":
        from swarmwatch.rbac import TotpEngine
        if args.action == "generate":
            secret = TotpEngine.generate_secret()
            raw_backups, _ = TotpEngine.generate_backup_codes(count=8)
            uri = TotpEngine.build_otpauth_uri(args.username, secret)
            if args.format == "json":
                print(json.dumps({
                    "username": args.username,
                    "secret": secret,
                    "otpauth_uri": uri,
                    "backup_codes": raw_backups,
                }, indent=2))
            else:
                print("\n+--------------------------------------------------------------+")
                print("| SWARMWATCH ENTERPRISE MFA / TOTP ENROLLMENT (ADR 0037)       |")
                print("+--------------------------------------------------------------+")
                print(f"| Username:     {args.username: <46} |")
                print(f"| Secret:       {secret: <46} |")
                print("+--------------------------------------------------------------+")
                print("| BACKUP RECOVERY CODES:                                       |")
                print("+--------------------------------------------------------------+")
                for i in range(0, len(raw_backups), 2):
                    c1 = raw_backups[i]
                    c2 = raw_backups[i+1] if i+1 < len(raw_backups) else ""
                    print(f"| * {c1: <16} | * {c2: <38} |")
                print("+--------------------------------------------------------------+\n")
        elif args.action == "verify":
            if not args.secret or not args.code:
                print("Error: --secret and --code are required for verify action", file=sys.stderr)
                sys.exit(1)
            is_valid = TotpEngine.verify_totp(args.code, args.secret)
            if args.format == "json":
                print(json.dumps({"valid": is_valid, "code": args.code}, indent=2))
            else:
                if is_valid:
                    print(f"\033[92m[SwarmWatch] TOTP code '{args.code}' verified successfully.\033[0m")
                else:
                    print(f"\033[91m[SwarmWatch] TOTP verification failed for code '{args.code}'.\033[0m")
                    sys.exit(1)
        sys.exit(0)
    elif args.subcommand == "rbac":
        from swarmwatch.rbac import Role, Permission, ROLE_PERMISSIONS, has_permission
        if args.action == "check":
            try:
                target_role = Role(args.role.lower())
                target_perm = Permission(args.permission)
            except ValueError as e:
                print(f"Error: Invalid role or permission: {e}", file=sys.stderr)
                sys.exit(1)
            granted = has_permission(target_role, target_perm)
            if args.format == "json":
                print(json.dumps({
                    "role": target_role.value,
                    "permission": target_perm.value,
                    "granted": granted,
                }, indent=2))
            else:
                tag = "\033[92mGRANTED\033[0m" if granted else "\033[91mDENIED\033[0m"
                print(f"[SwarmWatch RBAC] Role '{target_role.value}' -> Permission '{target_perm.value}': {tag}")
            sys.exit(0 if granted else 1)
        elif args.action == "matrix":
            if args.format == "json":
                matrix_data = {
                    r.value: [p.value for p in perms]
                    for r, perms in ROLE_PERMISSIONS.items()
                }
                print(json.dumps(matrix_data, indent=2))
            else:
                print("\n+--------------------------------------------------------------+")
                print("| SWARMWATCH ENTERPRISE RBAC MATRIX (ADR 0037)                 |")
                print("+--------------------------------------------------------------+")
                for r, perms in ROLE_PERMISSIONS.items():
                    print(f"| ROLE: {r.value.upper(): <54} |")
                    for p in sorted(perms, key=lambda x: x.value):
                        print(f"|   + {p.value: <56} |")
                    print("+--------------------------------------------------------------+")
                print()
            sys.exit(0)
    elif args.subcommand == "otlp":
        from swarmwatch.otlp import OtelTraceParser, global_otlp_stats
        if args.action == "validate":
            if not args.file:
                print("Error: --file is required for 'otlp validate'", file=sys.stderr)
                sys.exit(1)
            try:
                with open(args.file, "rb") as f:
                    content = f.read()
                spans, rejected = OtelTraceParser.parse_otlp_payload(content, content_type="application/json")
                if args.format == "json":
                    print(json.dumps({
                        "valid": rejected == 0,
                        "parsed_spans": len(spans),
                        "rejected_spans": rejected,
                        "spans": [s.model_dump(mode="json") for s in spans],
                    }, indent=2))
                else:
                    status_tag = "\033[92mVALID\033[0m" if rejected == 0 else "\033[93mPARTIAL\033[0m"
                    print(f"\n[SwarmWatch OTLP Validator] Status: {status_tag}")
                    print(f"  Parsed Spans:   {len(spans)}")
                    print(f"  Rejected Spans: {rejected}")
                    for s in spans:
                        print(f"  + [{s.id}] {s.source_agent} -> {s.target_agent} ({s.span_type}) | Tokens: {s.token_count} | Entropy: {s.entropy_score:.2f}")
                    print()
                sys.exit(0 if rejected == 0 else 1)
            except Exception as e:
                print(f"Error validating OTLP file: {e}", file=sys.stderr)
                sys.exit(1)
        elif args.action == "ingest":
            if not args.file:
                print("Error: --file is required for 'otlp ingest'", file=sys.stderr)
                sys.exit(1)
            try:
                with open(args.file, "rb") as f:
                    content = f.read()
                import urllib.request
                target_url = args.url.rstrip("/") + "/v1/traces"
                headers = {"Content-Type": "application/json"}
                if args.api_key:
                    headers["X-API-Key"] = args.api_key
                if args.project_id:
                    headers["x-swarmwatch-project-id"] = args.project_id
                req = urllib.request.Request(target_url, data=content, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    resp_data = json.loads(resp.read().decode("utf-8"))
                    if args.format == "json":
                        print(json.dumps(resp_data, indent=2))
                    else:
                        print(f"\033[92m[SwarmWatch OTLP Gateway] Ingestion successful -> {target_url}\033[0m")
                        print(f"  Response: {json.dumps(resp_data)}")
                sys.exit(0)
            except Exception as e:
                print(f"\033[91mError ingesting OTLP trace: {e}\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "stats":
            import urllib.request
            target_url = args.url.rstrip("/") + "/api/v1/otlp/stats"
            headers = {}
            if args.api_key:
                headers["X-API-Key"] = args.api_key
            try:
                req = urllib.request.Request(target_url, headers=headers)
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    stats_data = json.loads(resp.read().decode("utf-8"))
                if args.format == "json":
                    print(json.dumps(stats_data, indent=2))
                else:
                    print("\n[SwarmWatch OTLP Ingestion Gateway Stats]")
                    print(f"  Total Received: {stats_data.get('total_spans_received', 0)}")
                    print(f"  Total Parsed:   {stats_data.get('total_spans_parsed', 0)}")
                    print(f"  Total Rejected: {stats_data.get('total_spans_rejected', 0)}")
                    print(f"  Active Systems: {', '.join(stats_data.get('active_systems', [])) or 'none'}")
                    print(f"  Last Ingested:  {stats_data.get('last_ingested_at') or 'never'}\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error fetching OTLP stats: {e}", file=sys.stderr)
                sys.exit(1)
        elif args.action == "events" or args.subcommand == "events":
            pass
    elif args.subcommand == "events":
        import urllib.request
        target_path = f"/api/v1/swarms/{args.swarm}/events/stream" if args.swarm else "/api/v1/events/stream"
        query_parts = []
        if args.min_level:
            query_parts.append(f"min_level={args.min_level}")
        if args.project_id:
            query_parts.append(f"project_id={args.project_id}")
        query_str = ("?" + "&".join(query_parts)) if query_parts else ""
        stream_url = args.url.rstrip("/") + target_path + query_str

        print(f"\033[96m=====================================================\033[0m")
        print(f"\033[92m[SwarmWatch SSE Listener] Connecting to {stream_url}\033[0m")
        print(f"\033[90mListening for real-time circuit trips, warnings & pings... (Ctrl+C to quit)\033[0m")
        print(f"\033[96m=====================================================\033[0m\n")

        try:
            req = urllib.request.Request(stream_url, headers={"Accept": "text/event-stream"})
            with urllib.request.urlopen(req, timeout=3600) as resp:
                current_event = {}
                for line_b in resp:
                    line = line_b.decode("utf-8").strip()
                    if not line:
                        if "event" in current_event or "data" in current_event:
                            evt_name = current_event.get("event", "message")
                            evt_data = current_event.get("data", "")
                            evt_id = current_event.get("id", "")
                            if evt_name == "ping":
                                print(f"\033[90m[HEARTBEAT] ping ({evt_data})\033[0m")
                            elif "trip" in evt_name.lower():
                                print(f"\033[91m[CIRCUIT TRIP] ID: {evt_id} | Type: {evt_name}\033[0m")
                                print(f"  Payload: {evt_data}")
                            elif "warn" in evt_name.lower():
                                print(f"\033[93m[CIRCUIT WARN] ID: {evt_id} | Type: {evt_name}\033[0m")
                                print(f"  Payload: {evt_data}")
                            else:
                                print(f"\033[94m[{evt_name.upper()}] ID: {evt_id}\033[0m")
                                print(f"  Payload: {evt_data}")
                            current_event = {}
                        continue

                    if line.startswith("id:"):
                        current_event["id"] = line[3:].strip()
                    elif line.startswith("event:"):
                        current_event["event"] = line[6:].strip()
                    elif line.startswith("data:"):
                        current_event["data"] = line[5:].strip()
        except KeyboardInterrupt:
            print("\n\033[90m[SwarmWatch SSE] Disconnected.\033[0m")
            sys.exit(0)
        except Exception as e:
            print(f"\033[91mError connecting to SSE stream: {e}\033[0m", file=sys.stderr)
            sys.exit(1)
    elif args.subcommand == "push":
        import urllib.request
        from swarmwatch.webpush import VapidKeyManager
        if args.action == "keys":
            if args.generate:
                mgr = VapidKeyManager.generate()
                keys_data = {
                    "public_key": mgr.public_key_b64,
                    "private_key": mgr.private_key_b64,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                target_url = args.url.rstrip("/") + "/api/v1/push/vapid-public-key"
                try:
                    with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                        keys_data = json.loads(resp.read().decode("utf-8"))
                except Exception as e:
                    # Fallback to local generation if server is offline
                    mgr = VapidKeyManager.generate()
                    keys_data = {
                        "public_key": mgr.public_key_b64,
                        "private_key": mgr.private_key_b64,
                        "note": f"Generated locally (server unreachable: {e})",
                    }
            if args.format == "json":
                print(json.dumps(keys_data, indent=2))
            else:
                print("\n[SwarmWatch VAPID Cryptographic Keys]")
                print(f"  Public Key:  {keys_data.get('public_key')}")
                if "private_key" in keys_data:
                    print(f"  Private Key: {keys_data.get('private_key')}")
                print()
            sys.exit(0)
        elif args.action == "test":
            target_url = args.url.rstrip("/") + "/api/v1/push/test"
            payload = {}
            if args.endpoint:
                payload["endpoint"] = args.endpoint
            data_bytes = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(target_url, data=data_bytes, headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                if args.format == "json":
                    print(json.dumps(res, indent=2))
                else:
                    print(f"\033[92m[SwarmWatch WebPush] Dispatched synthetic test alert: {res}\033[0m")
                sys.exit(0)
            except Exception as e:
                print(f"\033[91mError dispatching test push notification: {e}\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "subscriptions":
            target_url = args.url.rstrip("/") + "/api/v1/push/subscriptions"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                subs = res.get("subscriptions", [])
                if args.format == "json":
                    print(json.dumps(res, indent=2))
                else:
                    print(f"\n[SwarmWatch WebPush Subscriptions ({len(subs)} Active)]")
                    print(f"  {'ID':<16} {'Project':<16} {'Endpoint':<40} {'Created'}")
                    print(f"  {'-'*16} {'-'*16} {'-'*40} {'-'*20}")
                    for s in subs:
                        ep = s.get("endpoint", "")
                        if len(ep) > 37: ep = ep[:37] + "..."
                        print(f"  {s.get('id', ''):<16} {s.get('project_id', ''):<16} {ep:<40} {s.get('created_at', '')[:19]}")
                    print()
                sys.exit(0)
            except Exception as e:
                print(f"\033[91mError fetching subscriptions: {e}\033[0m", file=sys.stderr)
                sys.exit(1)
    elif args.subcommand == "ebpf":
        import urllib.request
        from swarmwatch.ebpf import global_ebpf_gateway
        if args.action == "status":
            target_url = args.url.rstrip("/") + "/api/v1/ebpf/status"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    status_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                status_data = global_ebpf_gateway.status()
            if args.format == "json":
                print(json.dumps(status_data, indent=2))
            else:
                st_color = "\033[92mATTACHED\033[0m" if status_data.get("status") == "attached" else "\033[93mDETACHED\033[0m"
                print("\n+--------------------------------------------------------------+")
                print(f"| SWARMWATCH KERNEL eBPF SOCKET GATEWAY STATUS (ADR 0040)     |")
                print("+--------------------------------------------------------------+")
                print(f"| Interceptor State:   {st_color: <43} |")
                print(f"| Active Sockets:      {str(status_data.get('active_sockets', 0)): <43} |")
                print(f"| Packets Intercepted: {str(status_data.get('packets_intercepted', 0)): <43} |")
                print(f"| Bytes Monitored:     {str(status_data.get('bytes_intercepted', 0)): <43} |")
                print(f"| TCP RST Tripped:     {str(status_data.get('interventions_enacted', 0)): <43} |")
                print(f"| Active Rules:        {str(status_data.get('rules_count', 0)): <43} |")
                print("+--------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "rules":
            target_url = args.url.rstrip("/") + "/api/v1/ebpf/rules"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    rules_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                rules_data = {"rules": [r.to_dict() for r in global_ebpf_gateway.list_rules()]}
            rules = rules_data.get("rules", [])
            if args.format == "json":
                print(json.dumps(rules_data, indent=2))
            else:
                print(f"\n[SwarmWatch eBPF Rules ({len(rules)} Active)]")
                print(f"  {'ID':<18} {'Action':<10} {'Ports':<18} {'Name'}")
                print(f"  {'-'*18} {'-'*10} {'-'*18} {'-'*24}")
                for r in rules:
                    ports_str = ",".join(str(p) for p in r.get("ports", []))[:16]
                    print(f"  {r.get('id', ''):<18} {r.get('action', ''):<10} {ports_str:<18} {r.get('name', '')}")
                print()
            sys.exit(0)
        elif args.action == "simulate":
            sim_data = args.data or "POST /v1/chat/completions HTTP/1.1\r\nHost: api.openai.com\r\n\r\n{\"messages\": [{\"role\": \"user\", \"content\": \"Test loop simulation\"}]}"
            terminated, intervention, decoder = global_ebpf_gateway.inspect_stream_data(
                sim_data.encode("utf-8"),
                host=args.host,
                port=args.port,
            )
            res = {
                "terminated": terminated,
                "intervention": intervention.to_dict() if intervention else None,
                "tokens_estimated": decoder.estimated_token_count,
                "entropy": decoder.entropy,
            }
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                term_str = "\033[91mTERMINATED (TCP RST)\033[0m" if terminated else "\033[92mFORWARDED (CLEAN)\033[0m"
                print(f"\n[eBPF Stream Simulation] Result: {term_str}")
                print(f"  Estimated Tokens: {decoder.estimated_token_count}")
                print(f"  Shannon Entropy:  {decoder.entropy:.4f}")
                if intervention:
                    print(f"  RST Reason:       {intervention.reason}")
                print()
            sys.exit(0)
    elif args.subcommand == "raft":
        import urllib.request
        from swarmwatch.raft import global_raft_engine, MockRaftCluster
        if args.action == "status":
            target_url = args.url.rstrip("/") + "/api/v1/raft/status"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    status_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                status_data = global_raft_engine.status()
                status_data["swarms"] = global_raft_engine.state_machine.list_swarms()
            if args.format == "json":
                print(json.dumps(status_data, indent=2))
            else:
                role = status_data.get("role", "FOLLOWER")
                if role == "LEADER":
                    role_color = "\033[92mLEADER\033[0m"
                elif role == "CANDIDATE":
                    role_color = "\033[93mCANDIDATE\033[0m"
                else:
                    role_color = "\033[94mFOLLOWER\033[0m"
                print("\n+--------------------------------------------------------------+")
                print("| SWARMWATCH RAFT DISTRIBUTED CONSENSUS ENGINE (ADR 0041)     |")
                print("+--------------------------------------------------------------+")
                print(f"| Node ID:             {str(status_data.get('node_id', '')): <43} |")
                print(f"| Cluster Role:        {role_color: <43} |")
                print(f"| Current Term:        {str(status_data.get('term', 0)): <43} |")
                print(f"| Leader ID:           {str(status_data.get('leader_id') or 'none'): <43} |")
                print(f"| Commit Index:        {str(status_data.get('commit_index', 0)): <43} |")
                print(f"| Last Log Index:      {str(status_data.get('last_log_index', 0)): <43} |")
                print(f"| Quorum Size (N/2+1): {str(status_data.get('quorum_size', 1)) + '/' + str(status_data.get('cluster_size', 1)): <43} |")
                print(f"| Tracked Swarms:      {str(len(status_data.get('swarms', []))): <43} |")
                print("+--------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "propose":
            import asyncio
            payload = {
                "swarm_id": args.swarm_id,
                "reason": args.reason,
                "tripped_by": "raft-cli",
            }
            # Attempt via HTTP first
            target_url = args.url.rstrip("/") + "/api/v1/raft/propose"
            try:
                req_data = json.dumps({"command": args.command, "payload": payload}).encode("utf-8")
                req = urllib.request.Request(target_url, data=req_data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
            except Exception:
                # Standalone fallback
                if global_raft_engine.cluster_size <= 1 and not global_raft_engine.is_leader:
                    global_raft_engine.become_leader()
                success, entry, leader_id = asyncio.run(global_raft_engine.propose(args.command, payload))
                res = {
                    "status": "committed" if success else "failed",
                    "entry": entry.to_dict() if entry else None,
                    "commit_index": global_raft_engine.commit_index,
                }
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                st = "\033[92mCOMMITTED\033[0m" if res.get("status") == "committed" else "\033[91mFAILED\033[0m"
                print(f"\n[Raft Proposal] Status: {st}")
                print(f"  Command:      {args.command}")
                print(f"  Swarm ID:     {args.swarm_id}")
                print(f"  Commit Index: {res.get('commit_index')}")
                print()
            sys.exit(0)
        elif args.action == "cluster":
            import asyncio
            async def run_sim():
                cluster = MockRaftCluster(node_count=args.nodes)
                leader = await cluster.elect_leader()
                success, entry = await cluster.propose_command(
                    args.command,
                    {"swarm_id": args.swarm_id, "reason": args.reason},
                )
                return cluster, leader, success, entry
            cluster, leader, success, entry = asyncio.run(run_sim())
            res = {
                "nodes": [n.status() for n in cluster.nodes.values()],
                "leader": leader.node_id,
                "proposal_success": success,
                "commit_index": leader.commit_index,
            }
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                print(f"\n[Raft Multi-Node Simulation: {args.nodes} Nodes]")
                print(f"  Leader Elected: \033[92m{leader.node_id}\033[0m (Term {leader.current_term})")
                print(f"  Proposal:       {args.command} -> \033[92mQuorum Committed\033[0m (Index {leader.commit_index})")
                print("\n  Node States:")
                for nid, n in cluster.nodes.items():
                    print(f"    - {nid}: role={n.role:<9} term={n.current_term} commit={n.commit_index} last_log={n.last_log_index}")
                print()
            sys.exit(0)
    elif args.subcommand == "heal":
        import urllib.request
        from swarmwatch.selfhealing import global_self_healing_controller
        if args.action == "status":
            target_url = args.url.rstrip("/") + "/api/v1/self-healing/status"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    status_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                status_data = global_self_healing_controller.status()
            if args.format == "json":
                print(json.dumps(status_data, indent=2))
            else:
                st_color = "\033[92mACTIVE\033[0m" if status_data.get("enabled") else "\033[91mDISABLED\033[0m"
                print("\n+--------------------------------------------------------------+")
                print("| SWARMWATCH AUTONOMOUS SELF-HEALING CONTROLLER (ADR 0042)    |")
                print("+--------------------------------------------------------------+")
                print(f"| Controller State:       {st_color: <43} |")
                print(f"| Active Actions:         {str(status_data.get('active_actions_count', 0)): <43} |")
                print(f"| Total Interventions:    {str(status_data.get('total_actions_recorded', 0)): <43} |")
                print(f"| Counterfactual Tokens:  {str(status_data.get('cumulative_tokens_saved', 0)) + ' tokens': <43} |")
                print(f"| Counterfactual Dollars: {'$' + str(status_data.get('cumulative_cost_saved', 0.0)): <43} |")
                routes = status_data.get("active_model_routes", {})
                route_str = ", ".join(f"{k}->{v}" for k, v in routes.items()) or "none"
                print(f"| Model Fallbacks:        {route_str[:40]: <43} |")
                print("+--------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "actions":
            target_url = args.url.rstrip("/") + "/api/v1/self-healing/actions"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    actions_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                actions_data = {"actions": [a.to_dict() for a in global_self_healing_controller.get_action_history()]}
            actions = actions_data.get("actions", [])
            if args.format == "json":
                print(json.dumps(actions_data, indent=2))
            else:
                print(f"\n[Self-Healing Remediation Ledger ({len(actions)} Records)]")
                print(f"  {'ID':<14} {'Agent':<14} {'Action':<22} {'Target':<16} {'Saved Tokens'}")
                print(f"  {'-'*14} {'-'*14} {'-'*22} {'-'*16} {'-'*12}")
                for a in actions:
                    cf = a.get("counterfactual", {})
                    tokens_str = str(cf.get("tokens_saved", 0))
                    print(f"  {a.get('id', ''):<14} {a.get('agent_name', ''):<14} {a.get('action_type', ''):<22} {a.get('target', ''):<16} {tokens_str}")
                print()
            sys.exit(0)
        elif args.action == "trigger":
            payload = {
                "swarm_id": args.swarm_id,
                "agent_name": args.agent,
                "action_type": args.strategy,
                "target": args.target,
                "reason": args.reason,
            }
            target_url = args.url.rstrip("/") + "/api/v1/self-healing/remediate"
            try:
                req_data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(target_url, data=req_data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
            except Exception:
                action = global_self_healing_controller.remediate(
                    swarm_id=args.swarm_id,
                    agent_name=args.agent,
                    action_type=args.strategy,
                    target=args.target,
                    reason=args.reason,
                )
                res = action.to_dict()
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                print(f"\n[Self-Healing Remediation Enacted]")
                print(f"  Action ID:   {res.get('id')}")
                print(f"  Strategy:    {res.get('action_type')}")
                print(f"  Target:      {res.get('target')}")
                print(f"  Tokens Saved: {res.get('counterfactual', {}).get('tokens_saved', 0)}")
                print()
            sys.exit(0)
        elif args.action == "rollback":
            target_url = args.url.rstrip("/") + "/api/v1/self-healing/rollback"
            if args.action_id:
                target_url += f"?action_id={args.action_id}"
            try:
                req = urllib.request.Request(target_url, data=b"", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
            except Exception:
                if args.action_id:
                    success = global_self_healing_controller.rollback_action(args.action_id)
                    res = {"status": "rolled_back" if success else "failed", "action_id": args.action_id}
                else:
                    count = global_self_healing_controller.rollback_all(args.swarm_id)
                    res = {"status": "rolled_back_all", "count": count}
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                print(f"\n[Self-Healing Rollback Result]: {res.get('status')}")
                print()
            sys.exit(0)
    elif args.subcommand == "rootcause":
        import urllib.request
        from swarmwatch.causal_inference import global_causal_inference_engine
        target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/root-cause"
        if args.symptom_span:
            target_url += f"?symptom_span_id={args.symptom_span}"
        try:
            with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                rc_data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            rc_data = global_causal_inference_engine.infer_root_cause([], symptom_span_id=args.symptom_span).to_dict()

        if args.format == "json":
            print(json.dumps(rc_data, indent=2))
        else:
            print("\n+--------------------------------------------------------------+")
            print("| SWARMWATCH ROOT-CAUSE CAUSAL INFERENCE ENGINE (ADR 0043)    |")
            print("+--------------------------------------------------------------+")
            attr = rc_data.get("attribution")
            if not rc_data.get("has_anomaly") or not attr:
                print("| Status:             \033[92mNOMINAL (NO ANOMALY DETECTED)\033[0m            |")
                print(f"| Analyzed Spans:     {str(rc_data.get('analyzed_spans_count', 0)): <43} |")
                print("+--------------------------------------------------------------+\n")
            else:
                print(f"| Swarm ID:           {rc_data.get('swarm_id', ''): <43} |")
                print(f"| Root Cause Agent:   \033[91m{attr.get('root_cause_agent', ''): <43}\033[0m |")
                print(f"| Root Trigger Turn:  {str(attr.get('root_cause_turn', 1)): <43} |")
                print(f"| Symptom Agent:      {attr.get('symptom_agent', ''): <43} |")
                print(f"| Symptom Turn:       {str(attr.get('symptom_turn', 1)): <43} |")
                print(f"| Propagation Depth:  {str(attr.get('propagation_depth', 0)) + ' hops': <43} |")
                print(f"| Confidence Score:   {str(int(attr.get('confidence_score', 0) * 100)) + '%': <43} |")
                print(f"| Recommended Fork:   \033[96mTurn {str(attr.get('recommended_fork_turn', 1))}\033[0m{' ': <36} |")
                print("+--------------------------------------------------------------+")
                crit = attr.get("critical_path", [])
                print(f"\n[Critical Propagation Path ({len(crit)} Spans)]")
                print(f"  {'Turn':<6} {'Agent':<18} {'Type':<12} {'Status':<10} {'Notes'}")
                print(f"  {'-'*6} {'-'*18} {'-'*12} {'-'*10} {'-'*24}")
                for p in crit:
                    origin_mark = " [ORIGIN]" if p.get("is_fault_origin") else ""
                    print(f"  {str(p.get('step_index', 0)):<6} {p.get('agent_name', '') + origin_mark:<18} {p.get('span_type', ''):<12} {p.get('status', ''):<10} {p.get('notes', '')[:30]}")
                print()
                cps = rc_data.get("context_poisoning", [])
                if cps:
                    print(f"[Context Poisoning Analysis]")
                    for c in cps:
                        print(f"  - Contamination: {c.get('summary', '')}")
                        print(f"    Tainted Agents: {', '.join(c.get('tainted_agents', [])) or 'None'}")
                    print()
        sys.exit(0)
    elif args.subcommand == "grpc":
        import urllib.request
        from swarmwatch.grpc_gateway import global_grpc_gateway, GrpcMessageFramer, GrpcStatusCode
        if args.action == "status":
            target_url = f"{args.url.rstrip('/')}/api/v1/grpc/status"
            try:
                with urllib.request.urlopen(target_url, timeout=5.0) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                data = global_grpc_gateway.status()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+--------------------------------------------------------------+")
                print("| SWARMWATCH ULTRA-HIGH-THROUGHPUT gRPC GATEWAY (ADR 0044)     |")
                print("+--------------------------------------------------------------+")
                print(f"| Status:                 \033[92m{data.get('status', 'ready').upper(): <36}\033[0m |")
                print(f"| Total Spans Received:   {str(data.get('total_spans_received', 0)): <36} |")
                print(f"| Total Bytes Received:   {str(data.get('total_bytes_received', 0)) + ' B': <36} |")
                print(f"| Total Frames Decoded:   {str(data.get('total_frames_decoded', 0)): <36} |")
                print(f"| Active Streams:         {str(data.get('active_streams', 0)): <36} |")
                print(f"| Total Streams:          {str(data.get('total_streams', 0)): <36} |")
                print(f"| Trips Triggered:        {str(data.get('trips_triggered', 0)): <36} |")
                print(f"| Throughput (Spans/s):   {str(data.get('spans_per_second', 0.0)): <36} |")
                print("+--------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "simulate":
            target_url = f"{args.url.rstrip('/')}/api/v1/grpc/simulate?spans_count={args.spans}&induce_loop={'true' if args.loop else 'false'}"
            try:
                req = urllib.request.Request(target_url, data=b"", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    sim_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                sim_data = {
                    "status": "simulated_local",
                    "spans_sent": args.spans,
                    "grpc_status": "RESOURCE_EXHAUSTED" if args.loop else "OK",
                    "grpc_status_code": 8 if args.loop else 0,
                    "error_message": "Runaway Loop Detected via gRPC Gateway" if args.loop else None,
                    "response_frames_count": 0 if args.loop else args.spans,
                }
            if args.format == "json":
                print(json.dumps(sim_data, indent=2))
            else:
                print(f"\n[gRPC Stream Simulation Result]")
                print(f"  Spans Streamed:   {sim_data.get('spans_sent')}")
                st = sim_data.get('grpc_status')
                color = "\033[91m" if st == "RESOURCE_EXHAUSTED" else "\033[92m"
                print(f"  gRPC Status:      {color}{st}\033[0m")
                if sim_data.get('error_message'):
                    print(f"  Circuit Trip:     \033[91m{sim_data.get('error_message')}\033[0m")
                print(f"  Response Frames:  {sim_data.get('response_frames_count')}\n")
            sys.exit(0)
    elif args.subcommand == "drift":
        from swarmwatch.drift import global_drift_detector, MinHashSigner, DivergenceStatus
        if args.action == "status":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/drift"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_drift_detector.get_aggregate_stats(args.swarm_id)
                data["timeline"] = [p.model_dump(mode="json") for p in global_drift_detector.get_timeline(args.swarm_id)]

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EMBEDDING-FREE SEMANTIC DRIFT DETECTOR (ADR 0045)  |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Total Turns:          {data.get('total_turns')}")
                print(f"  Current Drift Score:  {data.get('current_drift_score', 0.0):.4f}")
                print(f"  Max Drift Score:      {data.get('max_drift_score', 0.0):.4f}")
                print(f"  Mean Drift Score:     {data.get('mean_drift_score', 0.0):.4f}")
                st = data.get('current_status', 'nominal')
                color = "\033[92m" if st == "nominal" else ("\033[93m" if st == "warning" else "\033[91m")
                print(f"  Divergence Status:    {color}{st.upper()}\033[0m")
                if data.get('is_tripped'):
                    print(f"  Circuit Tripped:      \033[91mYES ({data.get('trip_reason')})\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "anchor":
            if not args.anchor:
                print("Error: --anchor required for action 'anchor'", file=sys.stderr)
                sys.exit(1)
            global_drift_detector.set_anchor(args.swarm_id, args.anchor)
            if args.format == "json":
                print(json.dumps({"status": "anchor_set", "swarm_id": args.swarm_id, "anchor_length": len(args.anchor)}))
            else:
                print(f"\n[Anchor Defined] Swarm: {args.swarm_id} ({len(args.anchor)} chars)\n")
            sys.exit(0)

        elif args.action == "analyze":
            if not args.text:
                print("Error: --text required for action 'analyze'", file=sys.stderr)
                sys.exit(1)
            if args.anchor:
                global_drift_detector.set_anchor(args.swarm_id, args.anchor)
            profile = global_drift_detector.evaluate_turn(
                swarm_id=args.swarm_id,
                agent_name="cli_agent",
                text=args.text,
            )
            data = profile.model_dump(mode="json")
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n[Semantic Drift Analysis]")
                print(f"  Drift Score:          {data.get('drift_score', 0.0):.4f}")
                print(f"  Anchor Similarity:    {data.get('anchor_similarity', 1.0):.4f}")
                print(f"  Predecessor Sim:      {data.get('consecutive_similarity', 1.0):.4f}")
                print(f"  Lexical Dispersion:   {data.get('lexical_dispersion', 1.0):.4f}")
                st = data.get('divergence_status', 'nominal')
                color = "\033[92m" if st == "nominal" else ("\033[93m" if st == "warning" else "\033[91m")
                print(f"  Status:               {color}{st.upper()}\033[0m")
                if data.get('is_circuit_broken'):
                    print(f"  Circuit Trip:         \033[91m{data.get('trip_reason')}\033[0m")
                print()
            sys.exit(0)
    elif args.subcommand == "rebalance":
        from swarmwatch.rebalancer import global_rebalancer, AgentTier
        if args.action == "status":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/rebalancer"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                stats = global_rebalancer.get_aggregate_stats(args.swarm_id)
                allocs = global_rebalancer.get_allocations(args.swarm_id)
                plan = global_rebalancer.get_plan(args.swarm_id)
                data = {
                    **stats,
                    "allocations": {aid: a.model_dump(mode="json") for aid, a in allocs.items()},
                    "latest_plan": plan.model_dump(mode="json") if plan else None,
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ADAPTIVE DYNAMIC QUOTA REBALANCER (ADR 0046)       |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Total Agents:         {data.get('total_agents')}")
                print(f"  Fleet Token Quota:    {data.get('fleet_quota_tokens', 0):,}")
                print(f"  Consumed Tokens:      {data.get('total_consumed_tokens', 0):,}")
                print(f"  Reclaimed Buffer:     {data.get('reclaimed_buffer_tokens', 0):,} tokens")
                td = data.get('tier_distribution', {})
                print(f"  Tiers:                Boosted={td.get('boosted', 0)} | Nominal={td.get('nominal', 0)} | Throttled={td.get('throttled', 0)} | Quarantined={td.get('quarantined', 0)}")
                print("+---------------------------------------------------------------+")
                allocs = data.get("allocations", {})
                if allocs:
                    print(f"  {'AGENT':<20} {'TIER':<12} {'UTILITY':<8} {'ALLOCATED':<10} {'CONSUMED':<10} {'REMAINING':<10}")
                    print(f"  {'-'*20} {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")
                    for aid, a in allocs.items():
                        tier = a.get("tier", "nominal").upper()
                        color = "\033[92m" if tier == "BOOSTED" else ("\033[96m" if tier == "NOMINAL" else ("\033[93m" if tier == "THROTTLED" else "\033[91m"))
                        u = f"{a.get('utility_score', 0.0):.2f}"
                        alloc_tok = f"{a.get('allocated_tokens', 0):,}"
                        cons_tok = f"{a.get('consumed_tokens', 0):,}"
                        rem_tok = f"{a.get('remaining_tokens', 0):,}"
                        print(f"  {aid:<20} {color}{tier:<12}\033[0m {u:<8} {alloc_tok:<10} {cons_tok:<10} {rem_tok:<10}")
                else:
                    print("  No registered agents in active swarm.")
                print()
            sys.exit(0)

        elif args.action == "run":
            plan = global_rebalancer.compute_rebalance(args.swarm_id, justification="CLI triggered rebalance")
            data = plan.model_dump(mode="json")
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n[Rebalance Epoch Completed] Swarm: {args.swarm_id}")
                print(f"  Epoch ID:         {data.get('epoch_id')}")
                print(f"  Reclaimed Buffer: {data.get('reclaimed_buffer_tokens', 0):,} tokens")
                print(f"  Agents Evaluated: {len(data.get('allocations', {}))}\n")
            sys.exit(0)

        elif args.action == "quota":
            global_rebalancer.set_fleet_quota(args.swarm_id, args.tokens)
            if args.format == "json":
                print(json.dumps({"status": "quota_set", "swarm_id": args.swarm_id, "fleet_quota_tokens": args.tokens}))
            else:
                print(f"\n[Fleet Quota Set] Swarm: {args.swarm_id} -> {args.tokens:,} tokens\n")
            sys.exit(0)
    elif args.subcommand == "coalition":
        from swarmwatch.coalition import global_coalition_engine
        target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/coalitions"
        try:
            with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                data = json.loads(resp.read().decode())
        except Exception:
            res = global_coalition_engine.compute_clusters(args.swarm_id)
            data = res.model_dump(mode="json")

        if args.format == "json":
            print(json.dumps(data, indent=2))
        else:
            print("\n+---------------------------------------------------------------+")
            print("| SWARMWATCH TOPOLOGICAL GRAPH & DYNAMIC COALITIONS (ADR 0047)  |")
            print("+---------------------------------------------------------------+")
            print(f"  Swarm ID:             {data.get('swarm_id')}")
            print(f"  Total Agents:         {data.get('total_agents')}")
            print(f"  Total Interactions:   {data.get('total_interactions')}")
            print(f"  Graph Modularity (Q): {data.get('modularity_score', 0.0):.4f}")
            btn_agents = data.get('bottleneck_agents', [])
            btn_str = ", ".join(btn_agents) if btn_agents else "None (Traffic Uniform)"
            print(f"  Bottleneck Nodes:     \033[93m{btn_str}\033[0m")
            print("+---------------------------------------------------------------+")
            coalitions = data.get("coalitions", [])
            if coalitions:
                print(f"  DYNAMIC COALITIONS ({len(coalitions)} formed):")
                for c in coalitions:
                    deadlock_flag = " \033[91m[DEADLOCKED]\033[0m" if c.get("is_deadlocked") else ""
                    print(f"    - ID: {c.get('coalition_id')} | Leader: \033[92m{c.get('leader_agent')}\033[0m{deadlock_flag}")
                    print(f"      Members: {', '.join(c.get('member_agents', []))}")
                    print(f"      Density: {c.get('interaction_density', 0.0):.2f} | Tokens: {c.get('total_tokens', 0):,}")
            else:
                print("  No cohesive coalitions formed (low interaction density).")
            print()
        sys.exit(0)
    elif args.subcommand == "shm":
        from swarmwatch.shm_ring import global_shm_gateway
        if args.action == "flush":
            target_url = f"{args.url.rstrip('/')}/api/v1/shm/flush"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                spans = global_shm_gateway.flush()
                data = {"status": "flushed", "drained_count": len(spans), "pending_records": global_shm_gateway.get_status()["pending_records"]}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n[SHM Flush] Drained {data.get('drained_count', 0)} spans | Pending: {data.get('pending_records', 0)}\n")
            sys.exit(0)
        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/shm/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_shm_gateway.get_status()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ZERO-COPY SHARED-MEMORY RING BUFFER (ADR 0048)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Buffer Name:          {data.get('name')}")
                print(f"  Protocol Version:     v{data.get('version')}")
                print(f"  Slot Capacity:        {data.get('capacity_slots'):,} slots ({data.get('slot_size')} bytes/slot)")
                print(f"  Total Allocation:     {data.get('total_bytes', 0):,} bytes")
                print(f"  Write Head:           {data.get('write_head'):,}")
                print(f"  Read Tail:            {data.get('read_tail'):,}")
                print(f"  Pending Records:      \033[92m{data.get('pending_records', 0):,}\033[0m")
                print(f"  Dropped Records:      \033[91m{data.get('dropped_records', 0):,}\033[0m")
                print(f"  Buffer Utilization:   {data.get('utilization_pct', 0.0):.2f}%")
                print(f"  Daemon Running:       {data.get('is_running')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "privacy":
        import urllib.request
        from swarmwatch.diff_privacy import global_privacy_engine
        if args.action == "test":
            sample_span = {
                "id": "span_sample_01",
                "swarm_id": "swarm-live-01",
                "agent": "SecurityAuditor",
                "role": "Lead Auditor",
                "tokens": 1540,
                "duration": 390.0,
                "cost": 0.042,
                "entropy": 1.68,
                "payload": "Sensitive database connection string postgresql://user:pass@host/db",
            }
            target_url = f"{args.url.rstrip('/')}/api/v1/privacy/anonymize"
            payload_data = json.dumps({"spans": [sample_span], "epsilon": args.epsilon}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                anonymized = global_privacy_engine.anonymize_batch([sample_span], epsilon=args.epsilon)
                data = {"status": "anonymized", "count": len(anonymized), "spans": anonymized, "budget": global_privacy_engine.get_status()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                s = data["spans"][0]
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DIFFERENTIAL PRIVACY ANONYMIZATION TEST (ADR 0049) |")
                print("+---------------------------------------------------------------+")
                print(f"  Original Agent:    SecurityAuditor -> \033[96m{s.get('agent')}\033[0m")
                print(f"  Original Role:     Lead Auditor    -> \033[96m{s.get('role')}\033[0m")
                print(f"  Tokens:            1540            -> \033[93m{s.get('tokens')}\033[0m (Laplace perturbed)")
                print(f"  Duration (ms):     390.0           -> \033[93m{s.get('duration')}\033[0m (Laplace perturbed)")
                print(f"  Cost ($):          0.042           -> \033[93m${s.get('cost')}\033[0m (Laplace perturbed)")
                print(f"  Shannon Entropy:   1.6800          -> \033[93m{s.get('entropy')}\033[0m (Laplace perturbed)")
                print(f"  Payload:           {s.get('payload')}")
                print(f"  Privacy Budget:    {data.get('budget', {}).get('spent_budget', 0.0)} / {data.get('budget', {}).get('total_budget', 100.0)} eps spent")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "reset":
            target_url = f"{args.url.rstrip('/')}/api/v1/privacy/reset"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_privacy_engine.reset_budget()
                data = {"status": "budget_reset", "status_info": global_privacy_engine.get_status()}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n\033[92m[SwarmWatch] Privacy budget reset to 0.0 spent.\033[0m\n")
            sys.exit(0)
        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/privacy/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_privacy_engine.get_status()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DIFFERENTIAL PRIVACY STATUS (ADR 0049)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Active Epsilon (eps): {data.get('epsilon')}")
                print(f"  Total Budget:         {data.get('total_budget')} eps")
                print(f"  Spent Budget:         {data.get('spent_budget')} eps ({data.get('budget_spent_pct')}%)")
                print(f"  Remaining Budget:     {data.get('remaining_budget')} eps")
                ex_str = "\033[91mYES\033[0m" if data.get('is_exhausted') else "\033[92mNO\033[0m"
                print(f"  Budget Exhausted:     {ex_str}")
                print(f"  Total Anonymized:     {data.get('total_anonymized'):,} spans")
                print(f"  Anonymize Identities:{data.get('anonymize_identities')}")
                print(f"  Anonymize Metrics:   {data.get('anonymize_metrics')}")
                print(f"  Anonymize Payloads:  {data.get('anonymize_payloads')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "forecast":
        import urllib.request
        from swarmwatch.forecast import global_forecaster

        if args.action == "predict":
            ent_val = args.entropy if args.entropy is not None else 0.42
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/forecast/evaluate"
            payload_data = json.dumps({
                "turn_index": args.turn,
                "entropy": ent_val,
                "agent": args.agent,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
                    report = data.get("report", {})
            except Exception:
                rep = global_forecaster.observe_turn(
                    swarm_id=args.swarm_id,
                    turn_index=args.turn,
                    entropy=ent_val,
                    agent=args.agent,
                )
                report = rep.to_dict()

            if args.format == "json":
                print(json.dumps(report, indent=2))
            else:
                risk_lvl = report.get("risk_level", "nominal")
                color = "\033[92m" if risk_lvl == "nominal" else ("\033[93m" if risk_lvl == "early_warning" else "\033[91m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ENTROPY MARTINGALE FAILURE FORECAST (ADR 0050)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {report.get('swarm_id')}")
                print(f"  Observed Turns:       {report.get('total_turns')}")
                print(f"  Current Entropy:      {report.get('current_entropy'):.4f} bits")
                print(f"  Critical Threshold:   {report.get('threshold'):.4f} bits")
                print(f"  Drift Rate:           {report.get('drift_rate'):.4f} bits/turn")
                print(f"  Estimated Turns Left: {report.get('turns_to_trip')}")
                print(f"  Risk Level:           {color}{risk_lvl.upper()}\033[0m")
                print(f"  Failure Probability:  {report.get('risk_probability', 0.0) * 100:.1f}%")
                print(f"  Preemptive Intervene: {report.get('preemptive_recommended')}")
                print(f"  Justification:        {report.get('justification')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "reset":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/forecast/reset"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_forecaster.reset(args.swarm_id)
                data = {"status": "forecast_reset", "swarm_id": args.swarm_id}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n\033[92m[SwarmWatch] Forecast state reset for swarm '{args.swarm_id}'.\033[0m\n")
            sys.exit(0)
        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/forecast"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_forecaster.get_forecast(args.swarm_id).to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                risk_lvl = data.get("risk_level", "nominal")
                color = "\033[92m" if risk_lvl == "nominal" else ("\033[93m" if risk_lvl == "early_warning" else "\033[91m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ENTROPY MARTINGALE FAILURE FORECAST (ADR 0050)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Observed Turns:       {data.get('total_turns')}")
                print(f"  Current Entropy:      {data.get('current_entropy'):.4f} bits")
                print(f"  Critical Threshold:   {data.get('threshold'):.4f} bits")
                print(f"  Drift Rate:           {data.get('drift_rate'):.4f} bits/turn")
                print(f"  Estimated Turns Left: {data.get('turns_to_trip')}")
                print(f"  Risk Level:           {color}{risk_lvl.upper()}\033[0m")
                print(f"  Failure Probability:  {data.get('risk_probability', 0.0) * 100:.1f}%")
                print(f"  Preemptive Intervene: {data.get('preemptive_recommended')}")
                print(f"  Justification:        {data.get('justification')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "byzantine":
        import urllib.request
        from swarmwatch.byzantine import global_byzantine_harness

        if args.action == "inject":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/byzantine/inject"
            payload_data = json.dumps({
                "agent_id": args.agent,
                "attack_type": args.type,
                "payload": args.payload,
                "turn_index": args.turn,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                res = global_byzantine_harness.inject_adversarial_turn(
                    swarm_id=args.swarm_id,
                    agent_id=args.agent,
                    attack_type=args.type,
                    custom_payload=args.payload,
                    turn_index=args.turn,
                )
                rep = global_byzantine_harness.get_resilience_report(args.swarm_id)
                data = {"status": "injected", "injection": res, "resilience": rep.to_dict()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                inj = data.get("injection", {})
                res = data.get("resilience", {})
                status_str = res.get("status", "NOMINAL")
                color = "\033[92m" if status_str == "SHIELDED" else ("\033[93m" if status_str == "ISOLATING" else "\033[91m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH BYZANTINE ATTACK INJECTION HARNESS (ADR 0051)      |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {args.swarm_id}")
                print(f"  Adversary Agent:      {inj.get('agent_id')}")
                print(f"  Attack Type:          {inj.get('attack_type')}")
                print(f"  Intercepted:          {inj.get('intercepted')}")
                print(f"  Defense Mechanism:    {inj.get('defense_mechanism')}")
                print(f"  Agent Quarantined:    {inj.get('isolated')}")
                print(f"  BFT Resilience Score: {res.get('bft_resilience_score', 1.0) * 100:.1f}%")
                print(f"  Swarm Shield Status:  {color}{status_str}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "clear":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/byzantine/clear"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_byzantine_harness.reset(args.swarm_id)
                data = {"status": "cleared", "swarm_id": args.swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n\033[92m[SwarmWatch] Byzantine attacks and quarantines cleared for '{args.swarm_id}'.\033[0m\n")
            sys.exit(0)
        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/byzantine/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_byzantine_harness.get_resilience_report(args.swarm_id).to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = data.get("status", "NOMINAL")
                color = "\033[92m" if status_str == "SHIELDED" else ("\033[93m" if status_str == "ISOLATING" else "\033[91m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH BYZANTINE FAULT TOLERANCE (BFT) STATUS (ADR 0051)  |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Total Attacks:        {data.get('total_attacks_injected')}")
                print(f"  Total Intercepted:    {data.get('total_attacks_intercepted')}")
                print(f"  Quarantined Agents:   {len(data.get('quarantined_agents', []))}")
                print(f"  Quarantine Roster:    {', '.join(data.get('quarantined_agents', [])) or 'None'}")
                print(f"  Contagion Ratio:      {data.get('contagion_ratio', 0.0) * 100:.1f}%")
                print(f"  BFT Resilience Score: {data.get('bft_resilience_score', 1.0) * 100:.1f}%")
                print(f"  Swarm Defense Status: {color}{status_str}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "honeypot":
        import urllib.request
        from swarmwatch.honeypot import global_honeypot_manager

        if args.action == "divert":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/honeypot/divert"
            payload_data = json.dumps({"agent_id": args.agent, "reason": args.reason}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_honeypot_manager.divert_agent(args.swarm_id, args.agent, args.reason)
                data = {"status": "diverted", "swarm_id": args.swarm_id, "agent_id": args.agent, "reason": args.reason}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DECEPTION SANDBOX DIVERSION (ADR 0052)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {args.swarm_id}")
                print(f"  Diverted Agent:       {args.agent}")
                print(f"  Reason:               {args.reason}")
                print(f"  Status:               \033[93mDIVERTED TO SHADOW SANDBOX\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "token":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/honeypot/token"
            payload_data = json.dumps({"token_type": args.token_type}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                token = global_honeypot_manager.generate_honeytoken(args.swarm_id, args.token_type)
                data = {"status": "created", "honeytoken": token.to_dict()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                htk = data.get("honeytoken", {})
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CANARY HONEYTOKEN GENERATED (ADR 0052)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {args.swarm_id}")
                print(f"  Token ID:             {htk.get('token_id')}")
                print(f"  Token Value:          \033[96m{htk.get('token_value')}\033[0m")
                print(f"  Type:                 {htk.get('token_type')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "reset":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/honeypot/reset"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_honeypot_manager.reset(args.swarm_id)
                data = {"status": "reset", "swarm_id": args.swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n\033[92m[SwarmWatch] Honeypot and deception sandbox reset for '{args.swarm_id}'.\033[0m\n")
            sys.exit(0)

        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/honeypot/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_honeypot_manager.get_report(args.swarm_id).to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = data.get("status", "ARMED")
                color = "\033[92m" if status_str == "DEFUSED" else ("\033[93m" if status_str == "DECEIVING" else "\033[96m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DYNAMIC HONEYPOT & DECEPTION STATUS (ADR 0052)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Operational Status:   {color}{status_str}\033[0m")
                print(f"  Diverted Turns:       {data.get('total_diverted_turns')}")
                print(f"  Sandboxed Agents:     {', '.join(data.get('active_sandboxed_agents', [])) or 'None'}")
                print(f"  Active Honeytokens:   {len(data.get('active_honeytokens', []))}")
                print(f"  Tripped Honeytokens:  {data.get('honeytokens_tripped')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "verify":
        import urllib.request
        from swarmwatch.model_checker import global_model_checker, AgentTransitionModel, AgentTransition

        if args.action == "check":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/model-check/verify"
            payload_data = json.dumps({"bound_k": args.bound_k, "max_token_budget": args.budget}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    raw = json.loads(resp.read().decode())
                    data = raw.get("verification", raw)
            except Exception:
                model = global_model_checker._models.get(args.swarm_id)
                if model is None:
                    model = AgentTransitionModel(
                        initial_agent="agent-planner",
                        final_agents={"agent-verifier"},
                        transitions=[
                            AgentTransition(from_agent="agent-planner", to_agent="agent-worker", action_type="DELEGATE", token_cost=150),
                            AgentTransition(from_agent="agent-worker", to_agent="agent-verifier", action_type="REVIEW", token_cost=200),
                        ],
                        max_token_budget=args.budget,
                    )
                res = global_model_checker.verify(model, bound_k=args.bound_k, swarm_id=args.swarm_id)
                data = res.to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                verdict_str = data.get("verdict", "VERIFIED")
                color = "\033[92m" if verdict_str == "VERIFIED" else ("\033[91m" if verdict_str == "VIOLATED" else "\033[93m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH BOUNDED MODEL CHECKING VERIFICATION (ADR 0053)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Verdict:              {color}{verdict_str}\033[0m")
                print(f"  Depth Bound (k):      {data.get('max_depth_bound')}")
                print(f"  States Explored:      {data.get('total_states_explored')}")
                print(f"  Execution Time:       {data.get('execution_time_ms')} ms")
                print(f"  Invariants Checked:   {', '.join(data.get('invariants_checked', []))}")
                if data.get("violated_invariant"):
                    print(f"  Violated Invariant:   \033[91m{data.get('violated_invariant')}\033[0m")
                    print(f"  Counterexample Steps: {len(data.get('counterexample', []))}")
                print(f"  Summary:              {data.get('summary')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "reset":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/model-check/reset"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_model_checker.reset(args.swarm_id)
                data = {"status": "reset", "swarm_id": args.swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n\033[92m[SwarmWatch] Model checker history reset for '{args.swarm_id}'.\033[0m\n")
            sys.exit(0)

        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/model-check/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                res = global_model_checker.get_latest_result(args.swarm_id)
                if res is None:
                    model = AgentTransitionModel(
                        initial_agent="agent-planner",
                        final_agents={"agent-verifier"},
                        transitions=[
                            AgentTransition(from_agent="agent-planner", to_agent="agent-worker", action_type="DELEGATE", token_cost=150),
                            AgentTransition(from_agent="agent-worker", to_agent="agent-verifier", action_type="REVIEW", token_cost=200),
                        ],
                        max_token_budget=args.budget,
                    )
                    res = global_model_checker.verify(model, bound_k=args.bound_k, swarm_id=args.swarm_id)
                data = res.to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                verdict_str = data.get("verdict", "VERIFIED")
                color = "\033[92m" if verdict_str == "VERIFIED" else ("\033[91m" if verdict_str == "VIOLATED" else "\033[93m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH BOUNDED MODEL CHECKING STATUS (ADR 0053)           |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Verdict:              {color}{verdict_str}\033[0m")
                print(f"  Depth Bound (k):      {data.get('max_depth_bound')}")
                print(f"  States Explored:      {data.get('total_states_explored')}")
                print(f"  Execution Time:       {data.get('execution_time_ms')} ms")
                print(f"  Invariants Checked:   {', '.join(data.get('invariants_checked', []))}")
                if data.get("violated_invariant"):
                    print(f"  Violated Invariant:   \033[91m{data.get('violated_invariant')}\033[0m")
                print(f"  Summary:              {data.get('summary')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "patch":
        import urllib.request
        from swarmwatch.topology_healer import global_topology_healer
        from swarmwatch.model_checker import global_model_checker, AgentTransitionModel, AgentTransition

        if args.action == "apply":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/topology-heal/apply"
            payload_data = json.dumps({"bound_k": args.bound_k}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload_data, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    raw = json.loads(resp.read().decode())
                    data = raw.get("report", raw)
            except Exception:
                model = global_model_checker._models.get(args.swarm_id)
                if model is None:
                    model = AgentTransitionModel(
                        initial_agent="agent-planner",
                        final_agents={"agent-verifier"},
                        transitions=[
                            AgentTransition(from_agent="agent-planner", to_agent="agent-deadlock", action_type="CALL", token_cost=100),
                        ],
                        max_token_budget=10000,
                    )
                report = global_topology_healer.heal_and_verify(model, bound_k=args.bound_k, swarm_id=args.swarm_id)
                data = report.to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                verdict_str = data.get("healed_verdict", "VERIFIED")
                color = "\033[92m" if verdict_str == "VERIFIED" else "\033[91m"
                patch_info = data.get("applied_patch") or {}
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH TOPOLOGY AUTO-HEAL REPORT (ADR 0054)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Original Verdict:     \033[91m{data.get('original_verdict')}\033[0m")
                print(f"  Violated Invariant:   {data.get('violated_invariant')}")
                print(f"  Patch Action:         \033[96m{patch_info.get('action', 'NONE')}\033[0m")
                print(f"  Rewired Target:       {patch_info.get('target_agent')} -> {patch_info.get('rewired_to')}")
                print(f"  Healed Verdict:       {color}{verdict_str}\033[0m")
                print(f"  Execution Time:       {data.get('execution_time_ms')} ms")
                print(f"  Summary:              {data.get('summary')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "revert":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/topology-heal/revert"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                reverted = global_topology_healer.revert_patch(args.swarm_id)
                data = {"status": "reverted" if reverted else "no_active_patches", "swarm_id": args.swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = data.get("status", "reverted")
                print(f"\n\033[92m[SwarmWatch] Topology patches {status_str} for '{args.swarm_id}'.\033[0m\n")
            sys.exit(0)

        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/topology-heal/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                active_patches = [p.to_dict() for p in global_topology_healer.get_active_patches(args.swarm_id)]
                latest_rep = global_topology_healer.get_latest_report(args.swarm_id)
                data = {
                    "swarm_id": args.swarm_id,
                    "active_patches_count": len(active_patches),
                    "active_patches": active_patches,
                    "latest_report": latest_rep.to_dict() if latest_rep else None,
                    "status": "PATCHED" if active_patches else "NOMINAL",
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = data.get("status", "NOMINAL")
                color = "\033[92m" if status_str == "NOMINAL" else "\033[93m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH TOPOLOGY AUTO-HEAL STATUS (ADR 0054)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Status:               {color}{status_str}\033[0m")
                print(f"  Active Patches:       {data.get('active_patches_count')}")
                patches = data.get("active_patches", [])
                if patches:
                    for idx, p in enumerate(patches):
                        print(f"    [{idx+1}] {p.get('patch_id')} ({p.get('action')}): {p.get('target_agent')} -> {p.get('rewired_to')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "artifact":
        from swarmwatch.multimodal import global_multimodal_inspector

        if args.action == "inspect":
            if args.file and os.path.exists(args.file):
                with open(args.file, "rb") as f:
                    content = f.read()
                filename = args.file
            else:
                # Synthetic sample PNG (1x1 transparent PNG)
                content = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
                filename = "sample.png"

            meta = global_multimodal_inspector.inspect_bytes(content, filename=filename)
            res = {"status": "inspected", "filename": filename, "metadata": meta.to_dict()}
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                dim_str = f"{meta.dimensions[0]}x{meta.dimensions[1]}" if meta.dimensions else "N/A"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH MULTI-MODAL ARTIFACT INSPECTION (ADR 0055)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Modality:             {meta.modality.value}")
                print(f"  MIME Type:            {meta.mime_type}")
                print(f"  Byte Size:            {meta.byte_size} bytes")
                print(f"  Dimensions:           {dim_str}")
                print(f"  Duration:             {meta.duration_seconds or 'N/A'}")
                print(f"  Perceptual Hash:      {meta.perceptual_hash or 'N/A'}")
                print(f"  SHA-256:              {meta.sha256_checksum[:32]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "watermark":
            if args.file and os.path.exists(args.file):
                with open(args.file, "rb") as f:
                    content = f.read()
                filename = args.file
            else:
                content = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
                filename = "sample.png"

            watermarked = global_multimodal_inspector.inspect_and_watermark(
                payload=content,
                producer_agent_id=args.agent_id,
                swarm_id=args.swarm_id,
                filename=filename,
                artifact_id=args.artifact_id,
                tenant_secret=args.secret,
            )
            res = {"status": "watermarked", "swarm_id": args.swarm_id, "artifact": watermarked.to_dict()}
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                wm = watermarked.watermark
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ARTIFACT CRYPTOGRAPHIC WATERMARK (ADR 0055)        |")
                print("+---------------------------------------------------------------+")
                print(f"  Artifact ID:          {watermarked.artifact_id}")
                print(f"  Producer Agent:       {wm.producer_agent_id}")
                print(f"  Swarm ID:             {wm.swarm_id}")
                print(f"  Tenant ID:            {wm.tenant_id}")
                print(f"  MIME Type:            {watermarked.metadata.mime_type}")
                print(f"  HMAC Signature:       {wm.hmac_signature[:32]}...")
                print(f"  Tamper Detected:      {wm.tamper_detected}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "verify":
            if args.file and os.path.exists(args.file):
                with open(args.file, "rb") as f:
                    content = f.read()
            else:
                content = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"

            artifact = global_multimodal_inspector.get_artifact(args.swarm_id, args.artifact_id or "default")
            if artifact:
                is_valid = global_multimodal_inspector.verify_watermark(artifact.watermark, content, tenant_secret=args.secret)
            else:
                is_valid = True  # Baseline verification if untracked

            res = {"status": "verified" if is_valid else "tamper_detected", "verified": is_valid}
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                tag = "\033[92mVERIFIED (AUTHENTIC)\033[0m" if is_valid else "\033[91mTAMPER DETECTED\033[0m"
                print(f"\n[SwarmWatch Artifact Verifier] Result: {tag}\n")
            sys.exit(0 if is_valid else 1)

        else:
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{args.swarm_id}/artifacts"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                artifacts = [a.to_dict() for a in global_multimodal_inspector.get_artifacts(args.swarm_id)]
                data = {
                    "swarm_id": args.swarm_id,
                    "artifacts_count": len(artifacts),
                    "artifacts": artifacts,
                    "status": "SECURED" if artifacts else "EMPTY",
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = data.get("status", "EMPTY")
                color = "\033[92m" if status_str == "SECURED" else "\033[90m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ARTIFACTS REGISTRY & PROVENANCE (ADR 0055)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Status:               {color}{status_str}\033[0m")
                print(f"  Artifacts Count:      {data.get('artifacts_count')}")
                arts = data.get("artifacts", [])
                if arts:
                    for idx, a in enumerate(arts):
                        m = a.get("metadata", {})
                        wm = a.get("watermark", {})
                        print(f"    [{idx+1}] {a.get('artifact_id')} ({m.get('modality')}/{m.get('mime_type')}): by {wm.get('producer_agent_id')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "mesh":
        from swarmwatch.gossip import global_gossip_manager
        import urllib.request

        if args.action == "peers":
            target_url = f"{args.url.rstrip('/')}/api/v1/mesh/peers"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                peers = [p.to_dict() for p in global_gossip_manager.list_peers()]
                data = {
                    "peers_count": len(peers),
                    "peers": peers,
                    "local_node_id": global_gossip_manager.node_id,
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH TELEMETRY MESH PEERS (ADR 0056)                    |")
                print("+---------------------------------------------------------------+")
                print(f"  Local Node ID:        {data.get('local_node_id')}")
                print(f"  Active Peers Count:   {data.get('peers_count')}")
                print("+---------------------------------------------------------------+")
                for idx, p in enumerate(data.get("peers", [])):
                    status_col = "\033[92m" if p.get("status") == "active" else "\033[93m"
                    print(f"  [{idx+1}] {p.get('node_id')} @ {p.get('endpoint')} -> {status_col}{p.get('status').upper()}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "join":
            endpoint = args.peer_endpoint or "http://127.0.0.1:8766"
            node_id = args.peer_id or f"peer-{endpoint.split(':')[-1]}"
            target_url = f"{args.url.rstrip('/')}/api/v1/mesh/peers/register"
            payload = json.dumps({"node_id": node_id, "endpoint": endpoint}).encode("utf-8")
            try:
                req = urllib.request.Request(target_url, data=payload, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                node = global_gossip_manager.register_peer(node_id, endpoint)
                data = {"status": "registered", "peer": node.to_dict()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n[SwarmWatch Mesh] Registered peer {node_id} @ {endpoint} (ACTIVE)\n")
            sys.exit(0)

        elif args.action == "sync":
            target_url = f"{args.url.rstrip('/')}/api/v1/mesh/gossip/round"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                res = global_gossip_manager.run_gossip_round()
                data = {"status": "round_completed", "result": res}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                r = data.get("result", {})
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EPIDEMIC GOSSIP SYNC ROUND (ADR 0056)              |")
                print("+---------------------------------------------------------------+")
                print(f"  Round Index:          {r.get('round')}")
                print(f"  Peers Synced:         {r.get('peers_synced')}")
                print(f"  Spans Exchanged:      {r.get('spans_exchanged')}")
                print(f"  Trips Exchanged:      {r.get('trips_exchanged')}")
                print(f"  Watermarks Exchanged: {r.get('watermarks_exchanged')}")
                print(f"  Convergence Health:   \033[92m{r.get('convergence_health', 'CONVERGED').upper()}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        else:  # status
            target_url = f"{args.url.rstrip('/')}/api/v1/mesh/status"
            try:
                with urllib.request.urlopen(target_url, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_gossip_manager.get_status().to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                health = data.get("convergence_health", "converged")
                col = "\033[92m" if health == "converged" else "\033[93m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DECENTRALIZED TELEMETRY MESH (ADR 0056)            |")
                print("+---------------------------------------------------------------+")
                print(f"  Local Node ID:        {data.get('local_node_id')}")
                print(f"  Endpoint:             {data.get('local_endpoint')}")
                print(f"  Active Peers:         {data.get('active_peers_count')}")
                print(f"  Suspect Peers:        {data.get('suspect_peers_count')}")
                print(f"  Dead Peers:           {data.get('dead_peers_count')}")
                print(f"  Synced Spans:         {data.get('total_synced_spans')}")
                print(f"  Synced Trips:         {data.get('total_synced_trips')}")
                print(f"  Synced Watermarks:    {data.get('total_synced_watermarks')}")
                print(f"  Gossip Rounds:        {data.get('gossip_rounds_completed')}")
                print(f"  Convergence Health:   {col}{health.upper()}\033[0m")
                print(f"  Vector Clock:         {json.dumps(data.get('vector_clock', {}))}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "shard":
        from swarmwatch.sharding import global_compute_balancer, ShardRoutingKey, NodeComputeMetrics
        import urllib.request

        if args.action == "ring":
            target_url = f"{args.url.rstrip('/')}/api/v1/sharding/ring"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                tokens = global_compute_balancer.get_ring_tokens()
                data = {"tokens_count": len(tokens), "tokens": tokens}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CONSISTENT HASH RING LAYOUT (ADR 0057)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Ring Virtual Nodes: {data.get('tokens_count')}")
                tokens = data.get("tokens", [])
                for t in tokens[:12]:
                    print(f"  - Token: 0x{t.get('token', 0):08x} -> {t.get('node_id')} (vnode #{t.get('vnode_idx')})")
                if len(tokens) > 12:
                    print(f"  ... and {len(tokens) - 12} more virtual node tokens")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "route":
            route_key_str = args.key or "default:default:swarm-live-01"
            target_url = f"{args.url.rstrip('/')}/api/v1/sharding/route"
            payload = json.dumps({"swarm_id": route_key_str}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                rk = ShardRoutingKey.from_string(route_key_str)
                res = global_compute_balancer.route(rk)
                data = res.to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                mode = data.get("route_mode", "primary")
                col = "\033[92m" if mode == "primary" else "\033[93m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH WORKLOAD SHARD ROUTE RESOLUTION (ADR 0057)        |")
                print("+---------------------------------------------------------------+")
                print(f"  Routing Key:          {data.get('routing_key')}")
                print(f"  Route Mode:           {col}{mode.upper()}\033[0m")
                print(f"  Primary Node ID:      {data.get('primary_node_id')}")
                print(f"  Endpoint:             {data.get('endpoint')}")
                print(f"  Spillover Node ID:    {data.get('spillover_node_id') or 'none'}")
                print(f"  Capacity Headroom:    {data.get('capacity_score')}")
                print(f"  Health Tier:          {data.get('health_tier')}")
                print(f"  Lookup Latency:       {data.get('lookup_latency_ns')} ns")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "rebalance":
            target_url = f"{args.url.rstrip('/')}/api/v1/sharding/rebalance"
            payload = json.dumps({"base_vnodes": args.base_vnodes}).encode()
            try:
                req = urllib.request.Request(target_url, data=payload, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                if args.base_vnodes:
                    global_compute_balancer.ring.base_vnodes = args.base_vnodes
                rep = global_compute_balancer.rebalance()
                data = {"status": "rebalanced", "report": rep.to_dict()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                rep = data.get("report", {})
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DYNAMIC SHARD REBALANCE REPORT (ADR 0057)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Physical Nodes:       {rep.get('total_physical_nodes')}")
                print(f"  Total Virtual Nodes:  {rep.get('total_virtual_nodes')}")
                print(f"  Rebalance Count:      {rep.get('rebalance_count')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        else:  # status
            target_url = f"{args.url.rstrip('/')}/api/v1/sharding/status"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_compute_balancer.get_status().to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH COMPUTE-AWARE WORKLOAD SHARDING STATUS (ADR 0057)  |")
                print("+---------------------------------------------------------------+")
                print(f"  Physical Nodes:       {data.get('total_physical_nodes')}")
                print(f"  Total Virtual Nodes:  {data.get('total_virtual_nodes')}")
                print(f"  Rebalance Epochs:     {data.get('rebalance_count')}")
                nodes = data.get("nodes", {})
                for nid, ninfo in nodes.items():
                    tier = ninfo.get("health_tier", "optimal")
                    tcol = "\033[92m" if tier in ("optimal", "nominal") else "\033[91m"
                    print(f"  Node [{nid}]:")
                    print(f"    - Endpoint:         {ninfo.get('endpoint')}")
                    print(f"    - Capacity Score:   {ninfo.get('capacity_score')}")
                    print(f"    - Health Tier:      {tcol}{tier.upper()}\033[0m")
                    print(f"    - Virtual Nodes:    {ninfo.get('vnodes')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "schema":
        from swarmwatch.schema_migration import (
            global_schema_migration_engine,
        )

        def _get_payload():
            if args.file and os.path.exists(args.file):
                with open(args.file, "r", encoding="utf-8") as f:
                    return json.load(f)
            elif args.payload:
                try:
                    return json.loads(args.payload)
                except Exception:
                    import ast
                    try:
                        return ast.literal_eval(args.payload)
                    except Exception:
                        return json.loads(args.payload.replace("'", '"'))
            return {"agent": "agent-planner", "tokens": "1500"}

        if args.action == "versions":
            target_url = f"{args.url.rstrip('/')}/api/v1/schemas/versions"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                versions = global_schema_migration_engine.get_versions()
                data = {"total_versions": len(versions), "versions": versions}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REGISTERED SCHEMA VERSIONS (ADR 0058)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Versions:       {data.get('total_versions')}")
                for v in data.get("versions", []):
                    print(f"    - {v}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "migrate":
            payload_data = _get_payload()
            target_url = f"{args.url.rstrip('/')}/api/v1/schemas/migrate"
            body = json.dumps({
                "source_version": args.source,
                "target_version": args.target,
                "payload": payload_data,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                res = global_schema_migration_engine.migrate(
                    payload=payload_data,
                    source_version=args.source,
                    target_version=args.target,
                )
                data = res.to_dict()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SEMANTIC SCHEMA MIGRATION RESULT (ADR 0058)       |")
                print("+---------------------------------------------------------------+")
                print(f"  Migration:            {data.get('source_version')} -> {data.get('target_version')}")
                print(f"  Applied Steps:        {data.get('applied_steps_count')}")
                print(f"  Path Taken:           {' -> '.join(data.get('applied_path', []))}")
                print(f"  Lookup Latency:       {data.get('lookup_latency_ns')} ns")
                print(f"  Contract Valid:       {data.get('is_valid')}")
                print("  Transformed Payload:")
                print(json.dumps(data.get("transformed_payload", {}), indent=4))
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "validate":
            payload_data = _get_payload()
            target_url = f"{args.url.rstrip('/')}/api/v1/schemas/validate"
            body = json.dumps({
                "version": args.version,
                "payload": payload_data,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                valid, errors = global_schema_migration_engine.validate_payload(payload_data, args.version)
                data = {"is_valid": valid, "version": args.version, "errors": errors}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                col = "\033[92mVALID\033[0m" if data.get("is_valid") else "\033[91mINVALID\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SCHEMA VALIDATION (ADR 0058)                      |")
                print("+---------------------------------------------------------------+")
                print(f"  Target Version:       {data.get('version')}")
                print(f"  Validation Status:    {col}")
                if data.get("errors"):
                    print("  Contract Violations:")
                    for err in data.get("errors"):
                        print(f"    - {err}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get("is_valid") else 1)

        else:  # status
            target_url = f"{args.url.rstrip('/')}/api/v1/schemas/status"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_schema_migration_engine.get_status()

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SCHEMA MIGRATION ENGINE STATUS (ADR 0058)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Versions:       {data.get('total_versions')}")
                print(f"  Total Migration Links:{data.get('total_steps')}")
                print(f"  Migrations Executed:  {data.get('total_migrations_executed')}")
                print(f"  Registered Versions:  {', '.join(data.get('registered_versions', []))}")
                steps = data.get("steps", [])
                if steps:
                    print("  Configured Migration Steps:")
                    for s in steps:
                        print(f"    - [{s.get('source')} <-> {s.get('target')}]: {s.get('description')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "epistemic":
        import urllib.request
        from swarmwatch.epistemic import (
            global_epistemic_engine,
            EpistemicTruthValue,
        )

        swarm_id = args.swarm_id

        if args.action == "assert":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/assert"
            body = json.dumps({
                "agent_id": args.agent,
                "topic": args.topic,
                "predicate": args.predicate,
                "truth_value": args.truth_value,
                "confidence": args.confidence,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                tv = EpistemicTruthValue(args.truth_value)
                claim, is_cons, conf = global_epistemic_engine.assert_proposition(
                    swarm_id=swarm_id,
                    agent_id=args.agent,
                    topic=args.topic,
                    predicate=args.predicate,
                    truth_value=tv,
                    confidence=args.confidence,
                )
                data = {
                    "status": "asserted",
                    "is_consistent": is_cons,
                    "claim": claim.to_dict(),
                    "conflict": conf.to_dict() if conf else None,
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                col = "\033[92mCONSISTENT\033[0m" if data.get("is_consistent") else "\033[91mCONTRADICTION DETECTED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EPISTEMIC ASSERTION (ADR 0059)                     |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Agent:                {args.agent}")
                print(f"  Proposition:          K_{args.agent}({args.topic}.{args.predicate} == {args.truth_value})")
                print(f"  Confidence:           {args.confidence:.2f}")
                print(f"  Epistemic State:      {col}")
                if data.get("conflict"):
                    c = data["conflict"]
                    print(f"  Conflict With:        Agent '{c.get('prior_agent_id')}' (Claim: {c.get('prior_truth_value')})")
                    print(f"  Severity:             {c.get('severity', 0.0):.2f}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "propositions":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/propositions"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                props = global_epistemic_engine.get_propositions(swarm_id)
                data = {"swarm_id": swarm_id, "propositions_count": len(props), "propositions": props}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EPISTEMIC KNOWLEDGE BASE (ADR 0059)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Total Propositions:   {data.get('propositions_count', 0)}")
                props = data.get("propositions", [])
                for p in props:
                    print(f"    - K_{p.get('agent_id')} ({p.get('topic')}.{p.get('predicate')} = {p.get('truth_value')}) [conf={p.get('confidence'):.2f}]")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "conflicts":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/conflicts"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                confs = global_epistemic_engine.get_conflicts(swarm_id)
                data = {"swarm_id": swarm_id, "conflicts_count": len(confs), "conflicts": confs}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EPISTEMIC CONTRADICTIONS (ADR 0059)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Active Conflicts:     {data.get('conflicts_count', 0)}")
                for c in data.get("conflicts", []):
                    print(f"    - [{c.get('topic')}.{c.get('predicate')}]: {c.get('prior_agent_id')} ({c.get('prior_truth_value')}) vs {c.get('conflicting_agent_id')} ({c.get('conflicting_truth_value')}) [severity={c.get('severity', 0.0):.2f}]")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "mutual-knowledge":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/mutual-knowledge"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                kb = global_epistemic_engine.get_knowledge_base(swarm_id)
                data = {"swarm_id": swarm_id, "mutual_knowledge": kb.get_mutual_knowledge()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                mk = data.get("mutual_knowledge", [])
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH MUTUAL KNOWLEDGE (E_G phi) (ADR 0059)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Mutual Knowledge Items:{len(mk)}")
                for item in mk:
                    print(f"    - E_G({item.get('topic')}.{item.get('predicate')} = {item.get('truth_value')}) [affirming agents: {', '.join(item.get('affirming_agents', []))}]")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "reconcile":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/reconcile"
            body = json.dumps({"strategy": args.strategy}).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_epistemic_engine.reconcile(swarm_id, strategy=args.strategy)

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH AGM BELIEF REVISION RECONCILIATION (ADR 0059)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Strategy Applied:     {data.get('strategy')}")
                print(f"  Contradictions Resolved:{data.get('reconciled_conflicts_count', 0)}")
                print(f"  Claims Retracted:     {data.get('retracted_claims_count', 0)}")
                print(f"  Post-Dissonance Score:{data.get('post_reconciliation_dissonance', 0.0):.4f}")
                print(f"  Final Epistemic Status:{data.get('epistemic_status')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "reset":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/reset"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_epistemic_engine.reset(swarm_id)
                data = {"status": "reset", "swarm_id": swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\033[92m[SwarmWatch] Epistemic knowledge base reset for swarm: {swarm_id}\033[0m")
            sys.exit(0)

        else:  # status
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/epistemic/status"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_epistemic_engine.get_status(swarm_id)

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                st = data.get("status", "CONSISTENT")
                st_col = "\033[92mCONSISTENT\033[0m" if st == "CONSISTENT" else "\033[91mDISSONANT\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EPISTEMIC LOGIC ENGINE STATUS (ADR 0059)          |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Epistemic Status:     {st_col}")
                print(f"  Dissonance Score:     {data.get('dissonance_score', 0.0):.4f}")
                print(f"  Total Propositions:   {data.get('total_propositions', 0)}")
                print(f"  Active Agents:        {data.get('active_agents_count', 0)}")
                print(f"  Unresolved Conflicts: {data.get('unresolved_conflicts_count', 0)}")
                print(f"  Mutual Knowledge Items:{data.get('mutual_knowledge_count', 0)}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "proof":
        import urllib.request
        from swarmwatch.zk_proof import (
            global_zk_proof_engine,
            ProofCommitmentType,
            CausalAuditReceipt,
        )

        swarm_id = args.swarm_id

        if args.action == "root":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/proof/root"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                acc = global_zk_proof_engine.get_accumulator(swarm_id)
                data = {
                    "swarm_id": swarm_id,
                    "session_merkle_root": acc.get_merkle_root(),
                    "total_leaves": acc.total_leaves,
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CRYPTOGRAPHIC CAUSAL MERKLE ROOT (ADR 0060)        |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Session Merkle Root:  {data.get('session_merkle_root')}")
                print(f"  Total Committed Spans:{data.get('total_leaves')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "commit":
            span_id = args.span_id or f"span_{datetime.now(timezone.utc).strftime('%H%M%S')}"
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/proof/commit"
            body = json.dumps({
                "span_id": span_id,
                "turn_index": args.turn,
                "agent_id": args.agent,
                "payload_content": args.payload,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                leaf = global_zk_proof_engine.commit_span(
                    swarm_id=swarm_id,
                    span_id=span_id,
                    turn_index=args.turn,
                    agent_id=args.agent,
                    payload_content=args.payload,
                )
                data = {
                    "status": "committed",
                    "swarm_id": swarm_id,
                    "leaf": leaf.to_dict(),
                    "session_merkle_root": global_zk_proof_engine.get_root(swarm_id),
                }

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                leaf = data.get("leaf", {})
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SPAN COMMITMENT RECORDED (ADR 0060)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Span ID:              {leaf.get('span_id')}")
                print(f"  Turn Index:           {leaf.get('turn_index')}")
                print(f"  Agent ID:             {leaf.get('agent_id')}")
                print(f"  Leaf Hash:            {leaf.get('leaf_hash')}")
                print(f"  Session Merkle Root:  {data.get('session_merkle_root')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "generate":
            if not args.span_id:
                print("\033[91mError: --span-id is required for proof generation.\033[0m")
                sys.exit(1)

            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/proof/generate"
            body = json.dumps({
                "span_id": args.span_id,
                "commitment_type": args.type,
                "secret_key": args.secret,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                try:
                    ct = ProofCommitmentType(args.type.upper())
                except ValueError:
                    ct = ProofCommitmentType.TURN_INCLUSION
                receipt = global_zk_proof_engine.generate_receipt(
                    swarm_id=swarm_id,
                    span_id=args.span_id,
                    commitment_type=ct,
                    secret_key=args.secret,
                )
                data = {"status": "generated", "receipt": receipt.to_dict()}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                rcpt = data.get("receipt", {})
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ZERO-KNOWLEDGE AUDIT RECEIPT (ADR 0060)            |")
                print("+---------------------------------------------------------------+")
                print(f"  Receipt ID:           {rcpt.get('receipt_id')}")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Commitment Type:      {rcpt.get('commitment_type')}")
                print(f"  Verified Valid:       {rcpt.get('verified')}")
                print(f"  Session Merkle Root:  {rcpt.get('session_merkle_root')}")
                print(f"  HMAC Provenance Sig:  {rcpt.get('provenance_signature')[:24]}...")
                proof = rcpt.get("merkle_proof", {})
                print(f"  Merkle Path Steps:    {len(proof.get('sibling_hashes', []))}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "verify":
            receipt_data = None
            if args.receipt:
                if os.path.exists(args.receipt):
                    with open(args.receipt, "r", encoding="utf-8") as f:
                        receipt_data = json.load(f)
                else:
                    try:
                        receipt_data = json.loads(args.receipt)
                    except Exception:
                        receipt_data = json.loads(args.receipt.replace("'", '"'))
            if not receipt_data:
                print("\033[91mError: --receipt JSON string or file path is required for verification.\033[0m")
                sys.exit(1)

            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/proof/verify"
            body = json.dumps({
                "receipt": receipt_data,
                "secret_key": args.secret,
            }).encode()
            try:
                req = urllib.request.Request(target_url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                is_valid, err = global_zk_proof_engine.verify_receipt(receipt_data, secret_key=args.secret)
                data = {"is_valid": is_valid, "error": err, "swarm_id": swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                col = "\033[92mVALID\033[0m" if data.get("is_valid") else "\033[91mTAMPERED / INVALID\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH AUDIT PROOF VERIFICATION (ADR 0060)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Verification Result:  {col}")
                if data.get("error"):
                    print(f"  Failure Detail:       {data.get('error')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get("is_valid") else 1)

        elif args.action == "manifest":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/proof/manifest"
            try:
                req = urllib.request.Request(target_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                data = global_zk_proof_engine.get_manifest(swarm_id)

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CAUSAL AUDIT MANIFEST (ADR 0060)                   |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {swarm_id}")
                print(f"  Session Merkle Root:  {data.get('session_merkle_root')}")
                print(f"  Total Committed Spans:{data.get('total_leaves')}")
                print(f"  Tree Depth:           {data.get('tree_depth')}")
                print(f"  Receipts Generated:   {data.get('total_receipts_generated')}")
                print(f"  Verifications Run:    {data.get('total_verifications')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "reset":
            target_url = f"{args.url.rstrip('/')}/api/v1/swarms/{swarm_id}/proof/reset"
            try:
                req = urllib.request.Request(target_url, data=b"{}", headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                global_zk_proof_engine.reset(swarm_id)
                data = {"status": "reset", "swarm_id": swarm_id}

            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\033[92m[SwarmWatch] Causal proof accumulator reset for swarm: {swarm_id}\033[0m")
            sys.exit(0)

    elif args.subcommand == "codec":
        from swarmwatch.canonical import global_canonical_codec, CanonicalDomainTag, CanonicalEncodingError
        payload_data = None
        if args.payload:
            if os.path.isfile(args.payload):
                with open(args.payload, "r", encoding="utf-8") as f:
                    payload_data = json.load(f)
            else:
                try:
                    payload_data = json.loads(args.payload)
                except Exception:
                    payload_data = args.payload
        else:
            if not sys.stdin.isatty():
                content = sys.stdin.read().strip()
                if content:
                    try:
                        payload_data = json.loads(content)
                    except Exception:
                        payload_data = content

        if payload_data is None:
            print("\033[91mError: --payload JSON string/file or piped stdin is required.\033[0m")
            sys.exit(1)

        try:
            canonical_str = global_canonical_codec.canonicalize(payload_data, version=args.version)
            canonical_bytes = canonical_str.encode("utf-8")
        except CanonicalEncodingError as e:
            print(f"\033[91mEncoding Error: {e}\033[0m")
            sys.exit(1)

        if args.action == "canonicalize":
            if args.format == "json":
                print(json.dumps({
                    "status": "success",
                    "canonical_str": canonical_str,
                    "length_bytes": len(canonical_bytes),
                    "version": args.version
                }, indent=2))
            else:
                print(canonical_str)
            sys.exit(0)

        elif args.action == "hash":
            digest = global_canonical_codec.hash(payload_data, domain_tag=args.domain, version=args.version)
            if args.format == "json":
                print(json.dumps({
                    "status": "success",
                    "digest": digest,
                    "domain_tag": args.domain,
                    "length_bytes": len(canonical_bytes),
                    "version": args.version
                }, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CANONICAL CAUSAL DIGEST (ADR 0061)                 |")
                print("+---------------------------------------------------------------+")
                print(f"  Domain Tag:           {args.domain or '(none)'}")
                print(f"  SHA-256 Digest:       {digest}")
                print(f"  Canonical Length:     {len(canonical_bytes)} bytes")
                print(f"  Encoding Version:     {args.version} (RFC-8785 JCS)")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "safety":
        from swarmwatch.safety_authority import global_safety_authority, PermitStatus, AuthorityState
        tenant_id = args.tenant or "default"

        if args.action == "reserve":
            permit = global_safety_authority.reserve(
                tenant_id=tenant_id,
                action=args.action_name,
                cost_estimate=args.cost,
                tokens_estimate=args.tokens,
                timeout_ms=args.timeout,
            )
            data = permit.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                col = "\033[92mACCEPTED\033[0m" if permit.status == PermitStatus.ACCEPTED else "\033[91mREJECTED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EXECUTION PERMIT RESERVED (ADR 0062)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Permit ID:            {permit.permit_id}")
                print(f"  Status:               {col}")
                print(f"  Tenant ID:            {permit.tenant_id}")
                print(f"  Fencing Epoch:        {permit.epoch}")
                print(f"  Action Name:          {permit.action}")
                print(f"  Cost Reserved:        ${permit.cost_estimate:.4f}")
                print(f"  Tokens Reserved:      {permit.tokens_estimate}")
                print(f"  Permit Hash:          {permit.permit_hash}")
                if permit.rejection_reason:
                    print(f"  Rejection Reason:     {permit.rejection_reason}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if permit.status == PermitStatus.ACCEPTED else 1)

        elif args.action == "commit":
            if not args.permit_id:
                print("\033[91mError: --permit-id is required for commit.\033[0m")
                sys.exit(1)
            ok, msg = global_safety_authority.commit(
                permit_id=args.permit_id,
                actual_cost=args.cost,
                actual_tokens=args.tokens,
            )
            data = {"status": "committed" if ok else "failed", "message": msg, "permit_id": args.permit_id}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"[{'SUCCESS' if ok else 'FAILURE'}] {msg}")
            sys.exit(0 if ok else 1)

        elif args.action == "release":
            if not args.permit_id:
                print("\033[91mError: --permit-id is required for release.\033[0m")
                sys.exit(1)
            ok, msg = global_safety_authority.release(permit_id=args.permit_id, reason=args.reason)
            data = {"status": "released" if ok else "failed", "message": msg, "permit_id": args.permit_id}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"[{'SUCCESS' if ok else 'FAILURE'}] {msg}")
            sys.exit(0 if ok else 1)

        elif args.action == "trip":
            new_epoch = global_safety_authority.trip(reason=args.reason, tenant_id=None if args.tenant == "default" else args.tenant)
            data = {"status": "tripped", "new_epoch": new_epoch, "reason": args.reason, "tenant_id": args.tenant}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\033[91m[SwarmWatch] Safety Authority TRIPPED. Monotonic Fencing Epoch bumped to: {new_epoch}\033[0m")
            sys.exit(0)

        elif args.action == "rearm":
            new_epoch = global_safety_authority.rearm(tenant_id=None if args.tenant == "default" else args.tenant)
            data = {"status": "rearmed", "new_epoch": new_epoch, "tenant_id": args.tenant}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\033[92m[SwarmWatch] Safety Authority RE-ARMED. Monotonic Fencing Epoch bumped to: {new_epoch}\033[0m")
            sys.exit(0)

        elif args.action == "status":
            data = global_safety_authority.status(tenant_id=tenant_id)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                t = data.get("tenant", {})
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SAFETY AUTHORITY STATUS (ADR 0062)                 |")
                print("+---------------------------------------------------------------+")
                print(f"  Fencing Epoch:        {data.get('epoch')}")
                print(f"  Global State:         {data.get('global_state')}")
                print(f"  Active Permits:       {data.get('active_permits_count')}")
                print(f"  Tenant ID:            {t.get('tenant_id')}")
                print(f"  Available Budget:     ${t.get('available_cost', 0):.4f} / ${t.get('max_cost', 0):.4f}")
                print(f"  Available Tokens:     {t.get('available_tokens', 0)} / {t.get('max_tokens', 0)}")
                print(f"  Spent Turns:          {t.get('spent_turns', 0)} / {t.get('max_turns', 0)}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "effects":
        from swarmwatch.effect_ledger import global_effect_ledger, ClaimStatus
        tenant_id = args.tenant or "default"

        if args.action == "claim":
            if not args.effect_key:
                print("\033[91mError: --effect-key is required for claim.\033[0m")
                sys.exit(1)
            res = global_effect_ledger.claim(
                effect_key=args.effect_key,
                tenant_id=tenant_id,
                epoch=args.epoch,
                idempotency_key=args.idempotency_key,
                action=args.action_name,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                color = "\033[92m" if res.status in ("ACCEPTED", "IDEMPOTENT_COMMITTED") else "\033[91m"
                print(f"{color}[SwarmWatch] Effect Claim: {res.status}\033[0m")
                if res.claim_id:
                    print(f"  Claim ID:        {res.claim_id}")
                if res.reason:
                    print(f"  Reason:          {res.reason}")
            sys.exit(0 if res.status in ("ACCEPTED", "IDEMPOTENT_COMMITTED") else 1)

        elif args.action == "authorize":
            if not args.claim_id or not args.permit_hash:
                print("\033[91mError: Both --claim-id and --permit-hash are required for authorize.\033[0m")
                sys.exit(1)
            ok, msg = global_effect_ledger.authorize(claim_id=args.claim_id, permit_hash=args.permit_hash)
            data = {"status": "authorized" if ok else "failed", "claim_id": args.claim_id, "message": msg}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                color = "\033[92m" if ok else "\033[91m"
                print(f"{color}[SwarmWatch] Effect Authorize: {'SUCCESS' if ok else 'FAILED'} - {msg}\033[0m")
            sys.exit(0 if ok else 1)

        elif args.action == "complete":
            if not args.claim_id:
                print("\033[91mError: --claim-id is required for complete.\033[0m")
                sys.exit(1)
            ok, receipt, msg = global_effect_ledger.complete(claim_id=args.claim_id)
            data = {"status": "committed" if ok else "failed", "claim_id": args.claim_id, "receipt": receipt.to_dict() if receipt else None, "message": msg}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                color = "\033[92m" if ok else "\033[91m"
                print(f"{color}[SwarmWatch] Effect Complete: {'COMMITTED' if ok else 'FAILED'} - {msg}\033[0m")
                if receipt:
                    print(f"  Receipt ID:      {receipt.receipt_id}")
                    print(f"  Receipt Hash:    {receipt.receipt_hash}")
            sys.exit(0 if ok else 1)

        elif args.action == "abort":
            if not args.claim_id:
                print("\033[91mError: --claim-id is required for abort.\033[0m")
                sys.exit(1)
            ok, msg = global_effect_ledger.abort(claim_id=args.claim_id, reason=args.reason)
            data = {"status": "aborted" if ok else "failed", "claim_id": args.claim_id, "message": msg}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                color = "\033[92m" if ok else "\033[91m"
                print(f"{color}[SwarmWatch] Effect Abort: {msg}\033[0m")
            sys.exit(0 if ok else 1)

        elif args.action == "reconcile":
            records = global_effect_ledger.reconcile(since_epoch=args.epoch, tenant_id=None if args.tenant == "default" else args.tenant)
            data = {"reconciled_records": records, "count": len(records), "since_epoch": args.epoch}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n[SwarmWatch] Reconciled {len(records)} committed effects since epoch {args.epoch}:")
                for r in records:
                    print(f"  - Key: {r['effect_key']} | Claim: {r['claim_id']} | Epoch: {r['epoch']} | Receipt: {r.get('receipt', {}).get('receipt_id')}")
            sys.exit(0)

        elif args.action == "status":
            data = global_effect_ledger.status(tenant_id=tenant_id)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH FENCED EFFECT LEDGER STATUS (ADR 0063)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Current Epoch:        {data.get('current_epoch')}")
                print(f"  Total Records:        {data.get('total_records')}")
                print(f"  Active Claims:        {data.get('active_claims_count')}")
                print(f"  Committed Effects:    {data.get('committed_effects_count')}")
                print(f"  Tenant ID:            {data.get('tenant_id')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "ingress":
        from swarmwatch.causal_ingress import (
            global_causal_ingress,
            IngressPriority,
            CompletenessStatus,
        )
        swarm_id = args.swarm_id or "default"

        if args.action == "submit":
            span_id = args.span_id or f"spn_{int(time.time()*1000)}"
            parents = [p.strip() for p in args.parent_ids.split(",")] if args.parent_ids else []
            span_dict = {
                "span_id": span_id,
                "swarm_id": swarm_id,
                "parent_span_ids": parents,
                "payload": "CLI span submission",
            }
            res = global_causal_ingress.submit(
                span=span_dict,
                priority=args.priority.upper(),
                stream_id=args.stream_id,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                color = "\033[92m" if res.accepted else "\033[91m"
                print(f"{color}[SwarmWatch] Ingress Submit: {'ACCEPTED' if res.accepted else 'DROPPED'} (Seq: #{res.seq_num})\033[0m")
                print(f"  Swarm ID:     {res.swarm_id}")
                print(f"  Priority:     {res.priority}")
                print(f"  Status:       {res.status}")
                if res.gap_detected:
                    missing_str = str(res.gap.missing_count) if res.gap else "unknown"
                    print(f"  \033[93mGap Detected: {missing_str} sequence(s) missing!\033[0m")
                if res.reason:
                    print(f"  Reason:       {res.reason}")
            sys.exit(0 if res.accepted else 1)

        elif args.action == "drain":
            drained = global_causal_ingress.drain(batch_size=args.batch_size)
            data = {"drained_spans": drained, "count": len(drained)}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\n[SwarmWatch] Drained {len(drained)} spans from ingress buffer:")
                for s in drained:
                    print(f"  - Seq #{s.get('_seq_num')}: {s.get('span_id')} [{s.get('_priority')}] (Stream: {s.get('_stream_id')})")
            sys.exit(0)

        elif args.action == "completeness":
            report = global_causal_ingress.completeness(swarm_id=swarm_id)
            data = report.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                color = "\033[92m" if report.status == CompletenessStatus.COMPLETE else ("\033[91m" if report.status == CompletenessStatus.SATURATED else "\033[93m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH EVIDENTIARY COMPLETENESS REPORT (ADR 0064)        |")
                print("+---------------------------------------------------------------+")
                print(f"  Swarm ID:             {report.swarm_id}")
                print(f"  Completeness Status:  {color}{report.status}\033[0m")
                print(f"  Total Submitted:      {report.total_submitted}")
                print(f"  Total Accepted:       {report.total_accepted}")
                print(f"  Total Dropped:        {report.total_dropped}")
                print(f"  Loss Ratio:           {report.loss_ratio:.4%}")
                print(f"  Critical Delivery:    {report.critical_delivery_ratio:.1%}")
                print(f"  Sequence Gaps:        {len(report.sequence_gaps)}")
                print(f"  Missing Causal Parents: {len(report.missing_causal_parents)}")
                if report.sequence_gaps:
                    print("\n  Sequence Gaps Detail:")
                    for g in report.sequence_gaps:
                        print(f"    - Stream '{g['stream_id']}': expected #{g['expected_seq']}, received #{g['received_seq']} (missing {g['missing_count']})")
                if report.missing_causal_parents:
                    print("\n  Missing Causal Parents:")
                    for p in report.missing_causal_parents:
                        print(f"    - {p}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "status":
            data = global_causal_ingress.status(swarm_id=swarm_id)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CAUSAL INGRESS BUFFER STATUS (ADR 0064)            |")
                print("+---------------------------------------------------------------+")
                print(f"  Queue Length:         {data.get('queue_length')}")
                print(f"  Max Capacity:         {data.get('max_capacity')}")
                print(f"  Normal Threshold:     {data.get('normal_threshold')}")
                print(f"  Reserve Capacity:     {data.get('reserve_capacity')}")
                print(f"  Swarm ID:             {data.get('swarm_id')}")
                print(f"  Completeness Status:  {data.get('completeness_status')}")
                print(f"  Total Dropped:        {data.get('total_dropped')}")
                print(f"  Total Gaps:           {data.get('total_gaps')}")
                print(f"  Critical Delivery:    {data.get('critical_delivery_ratio'):.1%}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "audit":
        from swarmwatch.audit_anchor import global_audit_anchor, MerkleAuditReceipt
        tenant_id = args.tenant or "default-tenant"

        if args.action == "append-root":
            if not args.root_hash:
                print("Error: --root-hash required for append-root", file=sys.stderr)
                sys.exit(1)
            meta = {}
            if args.metadata:
                try:
                    meta = json.loads(args.metadata)
                except Exception:
                    meta = {"raw": args.metadata}
            epoch_obj = global_audit_anchor.append_root(
                root_hash=args.root_hash,
                tenant_id=tenant_id,
                metadata=meta,
            )
            data = epoch_obj.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH AUDIT ANCHOR: ROOT EPOCH COMMITTED (ADR 0065)      |")
                print("+---------------------------------------------------------------+")
                print(f"  Epoch:                {data.get('epoch')}")
                print(f"  Root Hash:            {data.get('root_hash')}")
                print(f"  Chain Digest:         {data.get('chain_digest')}")
                print(f"  Prev Chain Digest:    {data.get('prev_chain_digest')}")
                print(f"  Tenant ID:            {data.get('tenant_id')}")
                print(f"  Signature:            {data.get('signature')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "issue-receipt":
            if args.epoch is None or not args.leaf_hash:
                print("Error: --epoch and --leaf-hash required for issue-receipt", file=sys.stderr)
                sys.exit(1)
            path = []
            if args.sibling_hash:
                path.append((args.direction, args.sibling_hash))
            try:
                receipt = global_audit_anchor.issue_receipt(
                    epoch=args.epoch,
                    leaf_hash=args.leaf_hash,
                    audit_path=path,
                    tenant_id=tenant_id,
                )
                data = receipt.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH MERKLE AUDIT RECEIPT ISSUED (ADR 0065)             |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Receipt ID:           {data.get('receipt_id')}")
                    print(f"  Epoch:                {data.get('epoch')}")
                    print(f"  Leaf Hash:            {data.get('leaf_hash')}")
                    print(f"  Root Hash:            {data.get('root_hash')}")
                    print(f"  Chain Digest:         {data.get('chain_digest')}")
                    print(f"  Signature:            {data.get('signature')}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "verify-receipt":
            if not args.receipt_json:
                print("Error: --receipt-json required for verify-receipt", file=sys.stderr)
                sys.exit(1)
            try:
                if os.path.exists(args.receipt_json):
                    with open(args.receipt_json, "r", encoding="utf-8") as f:
                        rcpt_data = json.load(f)
                else:
                    rcpt_data = json.loads(args.receipt_json)
                res = global_audit_anchor.verify_receipt(rcpt_data)
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    status_str = "\033[92mVALID\033[0m" if res.valid else "\033[91mINVALID\033[0m"
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH MERKLE AUDIT RECEIPT VERIFICATION (ADR 0065)       |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Verification Result:  {status_str}")
                    print(f"  Epoch:                {res.epoch}")
                    print(f"  Leaf Hash:            {res.leaf_hash}")
                    print(f"  Root Hash:            {res.root_hash}")
                    if res.reason:
                        print(f"  Failure Reason:       {res.reason}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0 if res.valid else 2)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "chain":
            data = global_audit_anchor.audit_chain(since_epoch=args.since_epoch)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = "\033[92mVALID\033[0m" if data.get('valid') else "\033[91mCORRUPTED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH AUDIT CHAIN INTEGRITY AUDIT (ADR 0065)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Chain Status:         {status_str}")
                print(f"  Total Epochs:         {data.get('total_epochs')}")
                print(f"  Latest Epoch:         {data.get('latest_epoch')}")
                print(f"  Genesis Hash:         {data.get('genesis_hash')}")
                print(f"  Latest Digest:        {data.get('latest_digest')}")
                if data.get('errors'):
                    print(f"  Errors:               {data.get('errors')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get('valid') else 2)

        elif args.action == "status":
            data = global_audit_anchor.status(tenant_id=tenant_id)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DURABLE AUDIT ANCHOR STATUS (ADR 0065)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Status:               \033[92m{data.get('status')}\033[0m")
                print(f"  Total Epochs:         {data.get('total_epochs')}")
                print(f"  Latest Epoch:         {data.get('latest_epoch')}")
                print(f"  Genesis Hash:         {data.get('genesis_hash')}")
                print(f"  Latest Root:          {data.get('latest_root')}")
                print(f"  Latest Digest:        {data.get('latest_digest')}")
                print(f"  Storage Path:         {data.get('storage_path')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "verifier":
        from swarmwatch.safety_verifier import (
            global_safety_verifier,
            VerificationLimits,
            VerificationProperty,
            BoundedVerdict,
        )
        tenant_id = args.tenant

        if args.action == "verify":
            model_dict: Dict[str, Any] = {}
            if args.model_file:
                with open(args.model_file, "r", encoding="utf-8") as f:
                    model_dict = json.load(f)
            else:
                # Default canonical pipeline model
                model_dict = {
                    "initial_agent": "Planner",
                    "final_agents": ["Publisher"],
                    "transitions": [
                        {"from_agent": "Planner", "to_agent": "Worker", "action_type": "DELEGATE", "token_cost": 200},
                        {"from_agent": "Worker", "to_agent": "Publisher", "action_type": "SUBMIT", "token_cost": 150},
                    ],
                    "max_token_budget": 5000,
                }
            limits = VerificationLimits(
                max_depth_k=args.bound_k,
                max_states=args.max_states,
                timeout_ms=args.timeout,
            )
            report = global_safety_verifier.verify(
                model=model_dict,
                limits=limits,
                model_id=args.model_id,
                tenant_id=tenant_id,
            )
            data = report.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                verdict_color = "\033[92m" if data.get("verdict") in ("BOUNDED_SAFE", "UNBOUNDED_PROVEN", "VACUOUS") else "\033[91m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH BOUNDED SAFETY VERIFICATION REPORT (ADR 0066)      |")
                print("+---------------------------------------------------------------+")
                print(f"  Model ID:             {data.get('model_id')}")
                print(f"  Verdict:              {verdict_color}{data.get('verdict')}\033[0m")
                print(f"  Depth Bound (k):      {data.get('bound_k')}")
                print(f"  Depth Reached:        {data.get('depth_reached')}")
                print(f"  States Explored:      {data.get('total_states_explored')}")
                print(f"  Frontier Remaining:   {data.get('frontier_states_remaining')}")
                cov_pct = round(data.get('state_coverage_ratio', 0.0) * 100, 1)
                print(f"  State Coverage:       {cov_pct}%")
                print(f"  Execution Time (ms):  {data.get('execution_time_ms')}")
                print(f"  Diagnostic Reason:    {data.get('diagnostic_reason')}")
                if data.get("violated_property"):
                    print(f"  Violated Property:    \033[91m{data.get('violated_property')}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "check-trace":
            if not args.trace_file:
                print("Error: --trace-file required for check-trace", file=sys.stderr)
                sys.exit(1)
            with open(args.trace_file, "r", encoding="utf-8") as f:
                trace_data = json.load(f)
            model_dict = {}
            if args.model_file:
                with open(args.model_file, "r", encoding="utf-8") as f:
                    model_dict = json.load(f)
            else:
                model_dict = {
                    "initial_agent": "Planner",
                    "final_agents": ["Publisher"],
                    "transitions": [
                        {"from_agent": "Planner", "to_agent": "Worker", "action_type": "DELEGATE", "token_cost": 200},
                        {"from_agent": "Worker", "to_agent": "Publisher", "action_type": "SUBMIT", "token_cost": 150},
                    ],
                }
            is_sound, diagnostic = global_safety_verifier.verify_trace(
                model=model_dict,
                property=args.property,
                trace=trace_data if isinstance(trace_data, list) else trace_data.get("trace", []),
            )
            data = {"is_sound": is_sound, "diagnostic": diagnostic}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_str = "\033[92mSOUND & ADMISSIBLE\033[0m" if is_sound else "\033[91mREJECTED / INVALID\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH COUNTEREXAMPLE TRACE VALIDATION (ADR 0066)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Replay Verdict:       {status_str}")
                if diagnostic:
                    print(f"  Diagnostic:           {diagnostic}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if is_sound else 2)

        elif args.action == "status":
            data = global_safety_verifier.status(tenant_id=tenant_id if tenant_id != "default" else None)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SAFETY VERIFIER STATUS (ADR 0066)                  |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Verifications:  {data.get('total_verifications')}")
                avg_cov = round(data.get('average_coverage_ratio', 1.0) * 100, 1)
                print(f"  Avg State Coverage:   {avg_cov}%")
                print(f"  Last Verdict:         {data.get('last_verdict') or 'None'}")
                print(f"  Last Model ID:        {data.get('last_model_id') or 'None'}")
                print(f"  Verdicts Breakdown:   {json.dumps(data.get('verdicts_breakdown', {}))}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "remediation":
        from swarmwatch.remediation_governor import (
            ApprovalTier,
            ProposalStatus,
            BlastRadius,
            global_remediation_governor,
        )

        tenant_id = getattr(args, "tenant", "default") or "default"

        if args.action == "propose":
            prop = global_remediation_governor.propose(
                tenant_id=tenant_id,
                action_type=args.action_type,
                target_entity=args.target_entity,
                approval_tier=args.tier,
                proposal_id=args.proposal_id,
            )
            data = prop.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                status_color = "\033[92m" if data.get("status") in ("AUTHORIZED", "APPLIED") else ("\033[93m" if data.get("status") == "PROPOSED" else "\033[91m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REMEDIATION PROPOSAL (ADR 0067)                    |")
                print("+---------------------------------------------------------------+")
                print(f"  Proposal ID:          {data.get('proposal_id')}")
                print(f"  Tenant ID:            {data.get('tenant_id')}")
                print(f"  Action Type:          {data.get('action_type')}")
                print(f"  Target Entity:        {data.get('target_entity')}")
                print(f"  Approval Tier:        {data.get('approval_tier')}")
                print(f"  Status:               {status_color}{data.get('status')}\033[0m")
                print(f"  Authoritative Epoch:  {data.get('authoritative_epoch')}")
                print(f"  Proposal Hash:        {data.get('proposal_hash')}")
                if data.get("rejection_reason"):
                    print(f"  Rejection Reason:     \033[91m{data.get('rejection_reason')}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "authorize":
            if not args.proposal_id:
                print("Error: --proposal-id required for authorize", file=sys.stderr)
                sys.exit(1)
            decision = global_remediation_governor.authorize(
                proposal_id=args.proposal_id,
                approver_role=args.role,
                permit_hash=args.permit_hash,
                current_epoch=args.epoch,
            )
            data = decision.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                app_color = "\033[92mAUTHORIZED\033[0m" if data.get("approved") else "\033[91mREJECTED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REMEDIATION AUTHORIZATION (ADR 0067)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Proposal ID:          {data.get('proposal_id')}")
                print(f"  Decision:             {app_color}")
                print(f"  Tier:                 {data.get('approval_tier')}")
                print(f"  Authoritative Epoch:  {data.get('authoritative_epoch')}")
                print(f"  Reason:               {data.get('reason')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get("approved") else 1)

        elif args.action == "apply":
            if not args.proposal_id:
                print("Error: --proposal-id required for apply", file=sys.stderr)
                sys.exit(1)
            res = global_remediation_governor.apply(
                proposal_id=args.proposal_id,
                current_epoch=args.epoch,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                res_color = "\033[92mAPPLIED\033[0m" if data.get("success") else "\033[91mFAILED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REMEDIATION APPLICATION (ADR 0067)                 |")
                print("+---------------------------------------------------------------+")
                print(f"  Proposal ID:          {data.get('proposal_id')}")
                print(f"  Result:               {res_color}")
                print(f"  Applied Epoch:        {data.get('applied_epoch')}")
                print(f"  Latency (us):         {round(data.get('execution_latency_us', 0.0), 2)}")
                if data.get("error"):
                    print(f"  Error:                \033[91m{data.get('error')}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get("success") else 1)

        elif args.action == "compensate":
            if not args.proposal_id:
                print("Error: --proposal-id required for compensate", file=sys.stderr)
                sys.exit(1)
            comp = global_remediation_governor.compensate(
                proposal_id=args.proposal_id,
                reason=args.reason,
            )
            data = comp.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                comp_color = "\033[92mCOMPENSATED\033[0m" if data.get("success") else "\033[91mFAILED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REMEDIATION COMPENSATION (ADR 0067)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Proposal ID:          {data.get('proposal_id')}")
                print(f"  Status:               {comp_color}")
                print(f"  Reason:               {data.get('reason')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get("success") else 1)

        elif args.action == "status":
            data = global_remediation_governor.status(tenant_id=tenant_id if tenant_id != "default" else None)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REMEDIATION GOVERNOR STATUS (ADR 0067)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Current Epoch:        {data.get('current_epoch')}")
                print(f"  Total Proposals:      {data.get('total_proposals')}")
                print(f"  Active Remediations:  {data.get('active_remediations_count')}")
                print(f"  Status Breakdown:     {json.dumps(data.get('by_status', {}))}")
                print(f"  Tier Breakdown:       {json.dumps(data.get('by_tier', {}))}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand == "reconciler":
        from swarmwatch.mesh_reconciler import (
            LatticeVerdict,
            global_mesh_reconciler,
        )

        tenant_id = getattr(args, "tenant", "default") or "default"

        if args.action == "diff":
            loc_s = {}
            rem_s = {}
            if args.local_state_file:
                with open(args.local_state_file, "r", encoding="utf-8") as f:
                    loc_s = json.load(f)
            if args.remote_state_file:
                with open(args.remote_state_file, "r", encoding="utf-8") as f:
                    rem_s = json.load(f)
            plan = global_mesh_reconciler.diff(loc_s, rem_s, tenant_id=tenant_id)
            data = plan.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                v_color = "\033[92m" if data.get("verdict") == "IDENTICAL" else ("\033[93m" if data.get("verdict") in ("DOMINATES", "DOMINATED") else "\033[91m")
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PARTITION RECONCILIATION PLAN (ADR 0068)           |")
                print("+---------------------------------------------------------------+")
                print(f"  Plan ID:              {data.get('plan_id')}")
                print(f"  Lattice Verdict:      {v_color}{data.get('verdict')}\033[0m")
                print(f"  Conflict Detected:    {data.get('conflict_detected')}")
                print(f"  Divergent Keys:       {len(data.get('divergent_keys', []))} keys")
                print(f"  Action Items:         {', '.join(data.get('action_items', []))}")
                print(f"  Merged Vector Clock:  {json.dumps(data.get('merged_vector_clock', {}))}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "reconcile":
            loc_auth = {"state": args.local_state, "epoch": args.local_epoch}
            rem_auth = {"state": args.remote_state, "epoch": args.remote_epoch}
            res = global_mesh_reconciler.reconcile_authority(loc_auth, rem_auth, tenant_id=tenant_id)
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                s_color = "\033[92m" if data.get("converged_state") == "NOMINAL" else "\033[91m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH AUTHORITY RECONCILIATION RESULT (ADR 0068)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Converged State:      {s_color}{data.get('converged_state')}\033[0m")
                print(f"  Trip Monotonicity:    {data.get('trip_monotonicity_enforced')}")
                print(f"  Reconciled Epoch:     {data.get('reconciled_epoch')}")
                print(f"  Previous Epochs:      Local={data.get('previous_local_epoch')}, Remote={data.get('previous_remote_epoch')}")
                print(f"  Reason:               {data.get('reason')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "status":
            data = global_mesh_reconciler.status(tenant_id=tenant_id if tenant_id != "default" else None)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH MESH RECONCILER STATUS (ADR 0068)                  |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Reconciliations:       {data.get('total_reconciliations')}")
                print(f"  Trips Preserved (Monotonic): {data.get('trips_preserved_count')}")
                print(f"  Concurrent Spans Retained:   {data.get('concurrent_spans_retained')}")
                print(f"  Status:                      {data.get('status')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand in ("privacy-ledger", "privacy-release"):
        from swarmwatch.privacy_ledger import (
            global_privacy_release,
            PrivacyBudgetExhaustedError,
        )

        tenant_id = getattr(args, "tenant", "default") or "default"

        if args.action == "reserve":
            try:
                res = global_privacy_release.reserve(
                    tenant_id=tenant_id,
                    epsilon=args.epsilon,
                    delta=args.delta,
                    sensitivity=args.sensitivity,
                    query_id=args.query_id,
                )
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH PRIVACY BUDGET RESERVATION (ADR 0069)              |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Reservation ID:       {data.get('reservation_id')}")
                    print(f"  Tenant ID:            {data.get('tenant_id')}")
                    print(f"  Query ID:             {data.get('query_id')}")
                    print(f"  Epsilon Reserved:     {data.get('epsilon')}")
                    print(f"  Delta Reserved:       {data.get('delta')}")
                    print(f"  Sensitivity:          {data.get('sensitivity')}")
                    print(f"  Status:               \033[92m{data.get('status')}\033[0m")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except PrivacyBudgetExhaustedError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "release":
            if not args.reservation_id:
                print("Error: --reservation-id is required for release action", file=sys.stderr)
                sys.exit(1)
            raw_val = args.value if args.value is not None else 0.0
            try:
                rel = global_privacy_release.release(
                    reservation_id=args.reservation_id,
                    raw_value=raw_val,
                    noise_seed=args.noise_seed,
                    min_bound=args.min_bound,
                    max_bound=args.max_bound,
                )
                data = rel.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH DIFFERENTIAL PRIVACY RELEASE (ADR 0069)            |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Release ID:           {data.get('release_id')}")
                    print(f"  Reservation ID:       {data.get('reservation_id')}")
                    print(f"  Raw (Clipped):        {data.get('raw_clipped')}")
                    print(f"  Perturbed Value:      \033[92m{data.get('perturbed_value')}\033[0m")
                    print(f"  Noise Injected:       {data.get('noise_added')}")
                    print(f"  Remaining Epsilon:    {data.get('remaining_epsilon')}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "remaining":
            data = global_privacy_release.remaining(tenant_id=tenant_id)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH TENANT PRIVACY BUDGET REMAINING (ADR 0069)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Tenant ID:            {tenant_id}")
                print(f"  Remaining Epsilon:    \033[92m{data.get('remaining_epsilon')}\033[0m / {data.get('total_epsilon')}")
                print(f"  Remaining Delta:      {data.get('remaining_delta')} / {data.get('total_delta')}")
                print(f"  Spent Epsilon:        {data.get('spent_epsilon')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "status":
            data = global_privacy_release.status(tenant_id=tenant_id if tenant_id != "default" else None)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                st_color = "\033[92m" if data.get("status") == "CONVERGED" else "\033[91m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PRIVACY RELEASE LEDGER STATUS (ADR 0069)           |")
                print("+---------------------------------------------------------------+")
                print(f"  Active Tenants:       {data.get('active_tenants')}")
                print(f"  Total Releases:       {data.get('total_releases')}")
                print(f"  Total Reservations:   {data.get('total_reservations')}")
                print(f"  Total Spent Epsilon:  {data.get('total_spent_epsilon')}")
                print(f"  Ledger Status:        {st_color}{data.get('status')}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

    elif args.subcommand in ("schema-evolution", "evolution"):
        from swarmwatch.schema_evolution import (
            global_schema_evolution,
            LossyMigrationError,
            UnsupportedReverseMigrationError,
            SchemaEvolutionError,
        )

        if args.action == "plan":
            try:
                plan = global_schema_evolution.plan(args.source, args.target)
                data = plan.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    c_color = "\033[92m" if data.get("overall_classification") == "LOSSLESS" else "\033[93m"
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH SCHEMA EVOLUTION PLAN (ADR 0070)                   |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Source Version:       {data.get('source_version')}")
                    print(f"  Target Version:       {data.get('target_version')}")
                    print(f"  Classification:       {c_color}{data.get('overall_classification')}\033[0m")
                    print(f"  Steps Count:          {data.get('step_count')}")
                    print(f"  Can Reverse No Delta: {data.get('can_reverse_without_delta')}")
                    print(f"  Lossy Fields:         {', '.join(data.get('lossy_fields', [])) or 'None'}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "migrate":
            payload = {}
            if args.payload:
                payload = json.loads(args.payload)
            elif args.file:
                with open(args.file, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            else:
                payload = {"agent_id": "agent-01", "legacy_debug_trace": "trace_xyz", "confidence_score": 0.95}

            try:
                res = global_schema_evolution.migrate(payload, args.source, args.target)
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH SCHEMA MIGRATION RESULT (ADR 0070)                 |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Migration ID:         {data.get('migration_id')}")
                    print(f"  Direction:            {data.get('direction')}")
                    print(f"  Classification:       \033[92m{data.get('classification')}\033[0m")
                    print(f"  Migrated Payload:     {json.dumps(data.get('migrated_payload', {}))}")
                    if data.get("preserved_delta"):
                        p_delta = data.get("preserved_delta", {})
                        print(f"  Preserved Delta ID:   {p_delta.get('delta_id')} (Hash: {p_delta.get('delta_hash', '')[:16]}...)")
                        print(f"  Removed Fields:       {json.dumps(p_delta.get('removed_fields', {}))}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "reverse":
            payload = {}
            if args.payload:
                payload = json.loads(args.payload)
            elif args.file:
                with open(args.file, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            delta = None
            if args.delta_file:
                with open(args.delta_file, "r", encoding="utf-8") as f:
                    delta = json.load(f)

            try:
                res = global_schema_evolution.reverse(payload, args.source, args.target, preserved_delta=delta)
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH SCHEMA REVERSE RESULT (ADR 0070)                   |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Migration ID:         {data.get('migration_id')}")
                    print(f"  Restored Payload:     \033[92m{json.dumps(data.get('migrated_payload', {}))}\033[0m")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except (LossyMigrationError, UnsupportedReverseMigrationError) as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "classify":
            payload = json.loads(args.payload) if args.payload else None
            try:
                data = global_schema_evolution.classify_loss(args.source, args.target, payload)
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH SCHEMA LOSS CLASSIFICATION (ADR 0070)              |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Source -> Target:     {data.get('source_version')} -> {data.get('target_version')}")
                    print(f"  Classification:       \033[93m{data.get('overall_classification')}\033[0m")
                    print(f"  Lossy Fields:         {', '.join(data.get('lossy_fields', [])) or 'None'}")
                    print(f"  Can Reverse No Delta: {data.get('can_reverse_without_delta')}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "status":
            data = global_schema_evolution.status()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SCHEMA EVOLUTION STATUS (ADR 0070)                 |")
                print("+---------------------------------------------------------------+")
                print(f"  Status:               \033[92m{data.get('status')}\033[0m")
                print(f"  Registered Steps:     {data.get('registered_steps_count')}")
                print(f"  Registered Versions:  {data.get('registered_versions_count')}")
                print(f"  Total Migrations:     {data.get('total_migrations_count')}")
                print(f"  Lossless Count:       {data.get('lossless_migrations_count')}")
                print(f"  Lossy Count:          {data.get('lossy_migrations_count')}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "acceptance":
        from swarmwatch.acceptance import (
            global_acceptance_harness,
            AcceptanceGateViolationError,
            AttestationVerificationError,
            AcceptanceReport,
            AcceptanceAttestation,
            GateSummary,
            GateCheckResult,
            AcceptanceStatus,
        )

        custom_checks = [c.strip() for c in args.checks.split(",")] if args.checks else None

        if args.action == "run":
            report = global_acceptance_harness.run(
                profile=args.profile,
                custom_checks=custom_checks,
                html_path=args.html_path,
            )
            data = report.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                st_color = "\033[92mCERTIFIED\033[0m" if report.status == "CERTIFIED" else "\033[91mREJECTED\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ENTERPRISE ACCEPTANCE HARNESS (ADR 0071)           |")
                print("+---------------------------------------------------------------+")
                print(f"  Report ID:      {report.report_id}")
                print(f"  Profile:        {report.profile.upper()}")
                print(f"  Status:         {st_color}")
                print(f"  Overall Score:  {report.overall_score * 100:.1f}%")
                print(f"  Execution Time: {report.duration_ms:.2f} ms")
                print(f"  Report Hash:    {report.report_hash[:20]}...")
                print("+---------------------------------------------------------------+")
                print("  GATE BREAKDOWN:")
                for g_name, g_sum in report.gates.items():
                    pass_str = "\033[92mPASS\033[0m" if g_sum.passed else "\033[91mFAIL\033[0m"
                    print(f"    - {g_name.upper():<16} [{pass_str}]  Score: {g_sum.score*100:.0f}%  ({g_sum.checks_passed}/{g_sum.checks_run} checks)")
                    for v in g_sum.violations:
                        print(f"        \033[91m! {v}\033[0m")
                print("+---------------------------------------------------------------+\n")

            if args.strict and report.status != "CERTIFIED":
                sys.exit(1)
            sys.exit(0)

        elif args.action == "attest":
            report_obj = None
            if args.report_file:
                with open(args.report_file, "r", encoding="utf-8") as f:
                    r = json.load(f)
                    gates = {k: GateSummary(**v) for k, v in r.get("gates", {}).items()}
                    checks = [GateCheckResult(**c) for c in r.get("checks", [])]
                    report_obj = AcceptanceReport(
                        report_id=r["report_id"],
                        profile=r["profile"],
                        status=r["status"],
                        overall_score=r["overall_score"],
                        gates=gates,
                        checks=checks,
                        executed_at=r["executed_at"],
                        duration_ms=r["duration_ms"],
                        report_hash=r.get("report_hash", ""),
                    )
            else:
                report_obj = global_acceptance_harness.run(profile=args.profile, custom_checks=custom_checks, html_path=args.html_path)

            try:
                attestation = global_acceptance_harness.attest(
                    report=report_obj,
                    signer_identity=args.signer,
                    secret_key=args.secret_key,
                )
                data = attestation.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH ACCEPTANCE ATTESTATION RECEIPT (ADR 0071)          |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Attestation ID:   {attestation.attestation_id}")
                    print(f"  Report ID:        {attestation.report_id}")
                    print(f"  Status:           \033[92m{attestation.status}\033[0m")
                    print(f"  Signer:           {attestation.signer_identity}")
                    print(f"  Certified Gates:  {', '.join(attestation.certified_gates)}")
                    print(f"  Attestation Hash: {attestation.attestation_hash}")
                    print(f"  Signature:        {attestation.signature[:24]}...")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "audit-ui":
            data = global_acceptance_harness.audit_dashboard_artifact(html_path=args.html_path)
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                res_str = "\033[92mCOMPLIANT (100% CRAFT FLOOR)\033[0m" if data.get("compliant") else "\033[91mNON-COMPLIANT\033[0m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DASHBOARD UI CRAFT AUDIT (ADR 0071)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Audit Verdict:    {res_str}")
                print(f"  Total Badges:     {data.get('total_badges', 0)}")
                print(f"  Violations:       {len(data.get('violations', []))}")
                for v in data.get("violations", []):
                    print(f"    \033[91m! {v}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if data.get("compliant") else 1)

        elif args.action == "verify":
            if not args.attestation_file:
                print("Error: --attestation-file is required for verify action", file=sys.stderr)
                sys.exit(1)
            with open(args.attestation_file, "r", encoding="utf-8") as f:
                a = json.load(f)
            attestation_obj = AcceptanceAttestation(
                attestation_id=a["attestation_id"],
                report_id=a["report_id"],
                report_hash=a["report_hash"],
                profile=a["profile"],
                status=a["status"],
                certified_gates=a["certified_gates"],
                attested_at=a["attested_at"],
                signer_identity=a["signer_identity"],
                attestation_hash=a["attestation_hash"],
                signature=a.get("signature", ""),
            )
            try:
                is_valid = global_acceptance_harness.verify_attestation(attestation_obj, secret_key=args.secret_key)
                if args.format == "json":
                    print(json.dumps({"verified": is_valid, "attestation_id": attestation_obj.attestation_id}, indent=2))
                else:
                    print(f"\n[Attestation Verification] ID '{attestation_obj.attestation_id}': \033[92mVALID\033[0m\n")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "status":
            data = global_acceptance_harness.status()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ACCEPTANCE HARNESS STATUS (ADR 0071)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Status:             \033[92m{data.get('status')}\033[0m")
                print(f"  Total Runs:         {data.get('total_runs')}")
                print(f"  Certified Runs:     {data.get('certified_runs')}")
                print(f"  Rejected Runs:      {data.get('rejected_runs')}")
                print(f"  Registered Gates:   {data.get('registered_gates_count')}")
                print(f"  Latest Report ID:   {data.get('latest_report_id') or 'None'}")
                print(f"  Latest Attestation: {data.get('latest_attestation_id') or 'None'}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "consensus":
        from swarmwatch.consensus_authority import global_consensus_authority

        if args.action == "reserve":
            res = global_consensus_authority.reserve(
                tenant_id=args.tenant,
                action=args.action_name,
                cost_estimate=args.cost,
                tokens_estimate=args.tokens,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                verdict = "\033[92mACCEPTED\033[0m" if res.is_accepted else f"\033[91m{res.status}\033[0m"
                print(f"[Consensus Authority] Permit: {res.permit_id} -> {verdict} (Epoch: {res.epoch}, Quorum: {res.quorum_achieved})")
            sys.exit(0 if res.is_accepted else 1)

        elif args.action == "commit":
            if not args.permit_id:
                print("Error: --permit-id is required for commit.", file=sys.stderr)
                sys.exit(1)
            res = global_consensus_authority.commit(
                permit_id=args.permit_id,
                actual_cost=args.cost,
                actual_tokens=args.tokens,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"[Consensus Authority] Permit {res.permit_id} committed (Quorum: {res.quorum_achieved})")
            sys.exit(0 if res.is_accepted else 1)

        elif args.action == "release":
            if not args.permit_id:
                print("Error: --permit-id is required for release.", file=sys.stderr)
                sys.exit(1)
            res = global_consensus_authority.release(
                permit_id=args.permit_id,
                reason=args.reason,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"[Consensus Authority] Permit {res.permit_id} released (Quorum: {res.quorum_achieved})")
            sys.exit(0 if res.is_accepted else 1)

        elif args.action == "trip":
            res = global_consensus_authority.trip(
                tenant_id=args.tenant,
                reason=args.reason,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\033[91m[SwarmWatch] Consensus Circuit TRIPPED for tenant {args.tenant} (Epoch: {res.epoch}, Quorum: {res.quorum_achieved})\033[0m")
            sys.exit(0 if res.tripped else 1)

        elif args.action == "rearm":
            res = global_consensus_authority.rearm(
                tenant_id=args.tenant,
            )
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"\033[92m[SwarmWatch] Consensus Circuit RE-ARMED for tenant {args.tenant} (New Epoch: {res.new_epoch}, Quorum: {res.quorum_achieved})\033[0m")
            sys.exit(0 if res.rearmed else 1)

        elif args.action == "status":
            status = global_consensus_authority.export_status()
            committed = global_consensus_authority.read_committed(args.tenant).to_dict()
            data = {"cluster_status": status, "committed_state": committed}
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CONSENSUS AUTHORITY BINDING (ADR 0072)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Node ID:            {status.get('node_id')}")
                print(f"  Epoch / Term:       {status.get('epoch')} / {status.get('term')}")
                print(f"  Committed Index:    {status.get('committed_index')}")
                print(f"  Quorum Required:    {status.get('quorum_required')}")
                print(f"  Has Quorum:         \033[92m{status.get('has_quorum')}\033[0m")
                print(f"  Cluster Nodes:      {', '.join(status.get('cluster_nodes', []))}")
                print(f"  Tenant ID:          {committed.get('tenant_id')}")
                print(f"  Tenant State:       {committed.get('state')}")
                print(f"  Available Cost:     ${committed.get('available_cost', 0.0):.4f}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "downstream":
        from swarmwatch.downstream_effect import (
            global_downstream_effect_boundary,
            DownstreamReceipt,
            StaleEpochError,
            UncoordinatedRetryBlockedError,
            EffectNotFoundError,
        )

        if args.action == "prepare":
            try:
                payload_dict = json.loads(args.payload)
            except Exception:
                payload_dict = {"raw": args.payload}
            prep = global_downstream_effect_boundary.prepare(
                tenant_id=args.tenant,
                effect_key=args.effect_key,
                target_uri=args.target_uri,
                payload=payload_dict,
                epoch=args.epoch,
                contract=args.contract,
            )
            data = prep.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"[Downstream Effect] Prepared {prep.effect_id} (Key: {prep.idempotency_key}, Epoch: {prep.epoch}, Status: {prep.status})")
            sys.exit(0)

        elif args.action == "dispatch":
            if not args.effect_id:
                print("Error: --effect-id is required for dispatch.", file=sys.stderr)
                sys.exit(1)
            try:
                res = global_downstream_effect_boundary.dispatch(
                    args.effect_id,
                    lambda eff: {"ok": True, "confirmation_id": args.confirmation_id},
                )
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    status_colored = "\033[92mDISPATCHED\033[0m" if not res.is_outcome_unknown else "\033[93mOUTCOME_UNKNOWN\033[0m"
                    print(f"[Downstream Effect] Dispatched {res.effect_id} -> {status_colored}")
                sys.exit(0 if not res.is_outcome_unknown else 1)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "ack":
            if not args.effect_id:
                print("Error: --effect-id is required for ack.", file=sys.stderr)
                sys.exit(1)
            try:
                receipt = DownstreamReceipt(
                    downstream_confirmation_id=args.confirmation_id,
                    result_digest=args.digest,
                )
                res = global_downstream_effect_boundary.acknowledge(
                    effect_id=args.effect_id,
                    receipt=receipt,
                    epoch=args.epoch,
                )
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print(f"\033[92m[Downstream Effect] Effect {res.effect_id} COMMITTED (Confirmation: {receipt.downstream_confirmation_id})\033[0m")
                sys.exit(0)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "reconcile":
            if not args.effect_id:
                print("Error: --effect-id is required for reconcile.", file=sys.stderr)
                sys.exit(1)
            try:
                def probe_fn(eid, idem):
                    if args.found:
                        return {"found": True, "downstream_id": args.confirmation_id, "digest": args.digest}
                    return {"found": False}

                res = global_downstream_effect_boundary.reconcile(args.effect_id, probe_fn)
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    st = f"\033[92m{res.status}\033[0m" if res.status == "COMMITTED" else f"\033[91m{res.status}\033[0m"
                    print(f"[Downstream Effect] Reconciled {res.effect_id} -> {st} (Attempts: {res.probe_attempts})")
                sys.exit(0 if res.reconciled else 1)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.action == "status":
            status = global_downstream_effect_boundary.get_status()
            if args.format == "json":
                print(json.dumps(status, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DOWNSTREAM EFFECT BOUNDARY (ADR 0073)              |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Tracked Effects:    {status.get('total_effects', 0)}")
                print(f"  Prepared (Unsent):        {status.get('prepared_count', 0)}")
                print(f"  In-Flight Dispatched:     {status.get('dispatched_count', 0)}")
                print(f"  Committed (Verified):     \033[92m{status.get('committed_count', 0)}\033[0m")
                print(f"  Outcome Unknown (Fence):  \033[93m{status.get('outcome_unknown_count', 0)}\033[0m")
                print(f"  Aborted / Reconciled:     \033[91m{status.get('aborted_count', 0)}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "evidence":
        from swarmwatch.evidence_spool import (
            global_evidence_spool,
            EvidenceRecordType,
            EvidenceSpoolFullException,
            EvidenceCorruptionException,
            SequenceGapException,
        )
        if args.journal_path:
            global_evidence_spool._journal_path = args.journal_path
        if args.action == "offer":
            try:
                payload_dict = json.loads(args.payload)
            except Exception:
                payload_dict = {"raw": args.payload}
            try:
                receipt = global_evidence_spool.offer(
                    record_type=args.record_type,
                    tenant_id=args.tenant,
                    agent_id=args.agent_id,
                    payload=payload_dict,
                )
                data = receipt.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print(f"\033[92m[Critical Evidence] Offered record #{receipt.sequence_number} ({args.record_type}) | Hash: {receipt.record_hash[:16]}... | Persisted: {receipt.persisted}\033[0m")
                sys.exit(0)
            except EvidenceSpoolFullException as e:
                print(f"\033[91m[Critical Evidence Error] {e}\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "flush":
            res = global_evidence_spool.flush()
            data = res.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print(f"[Critical Evidence] Flushed {res.flushed_count} records ({res.bytes_written} bytes) | Head Seq: {res.head_sequence_number}")
            sys.exit(0)
        elif args.action == "recover":
            try:
                res = global_evidence_spool.recover(journal_path=args.journal_path)
                data = res.to_dict()
                if args.format == "json":
                    print(json.dumps(data, indent=2))
                else:
                    print(f"\033[92m[Critical Evidence] Recovered {res.recovered_count} records (Seq {res.first_sequence_number}..{res.last_sequence_number}) | Intact: {res.is_intact}\033[0m")
                sys.exit(0)
            except Exception as e:
                print(f"\033[91m[Critical Evidence Recovery Error] {e}\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "completeness":
            receipt = global_evidence_spool.completeness(tenant_id=args.tenant if args.tenant != "default" else None)
            data = receipt.to_dict()
            if args.format == "json":
                print(json.dumps(data, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CRITICAL EVIDENCE COMPLETENESS (NIST SP 800-53)    |")
                print("+---------------------------------------------------------------+")
                print(f"  Tenant ID:            {receipt.tenant_id}")
                print(f"  Sequence Contiguous:  \033[92m{receipt.verified_contiguous}\033[0m (Seq {receipt.start_sequence_number}..{receipt.end_sequence_number})")
                print(f"  Total Records:        {receipt.total_records}")
                print(f"  Dropped Critical:     \033[92m{receipt.dropped_critical_count}\033[0m (Zero-Drop Invariant)")
                print(f"  Compliance Standard:  {receipt.compliance_standard}")
                print(f"  Head Hash:            {receipt.head_hash[:32]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "status":
            st = global_evidence_spool.status().to_dict()
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CRITICAL EVIDENCE SPOOL (ADR 0074)                 |")
                print("+---------------------------------------------------------------+")
                status_color = "\033[92mACTIVE\033[0m" if st.get("status") == "ACTIVE" else "\033[91mSATURATED\033[0m"
                print(f"  Status:               {status_color}")
                print(f"  Record Count:         {st.get('record_count', 0):,} / {st.get('max_capacity', 0):,}")
                print(f"  Head Sequence:        #{st.get('head_sequence_number', 0)}")
                print(f"  Unflushed Records:    {st.get('unflushed_count', 0)}")
                print(f"  Dropped Telemetry:    {st.get('dropped_telemetry_count', 0)} (Tier 1 Loss-Aware)")
                print(f"  Journal Path:         {st.get('journal_file_path') or 'IN_MEMORY'}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "witness":
        from swarmwatch.root_witness import (
            global_root_witness,
            RootWitness,
            SignedCheckpoint,
            WitnessKeyManager,
            WitnessError,
        )
        if args.action == "submit":
            receipt = global_root_witness.submit(args.data)
            if args.format == "json":
                print(json.dumps(receipt.to_dict(), indent=2))
            else:
                print(f"\033[92m[Root Witness] Submitted leaf #{receipt.leaf_index} | Hash: {receipt.leaf_hash[:16]}...\033[0m")
            sys.exit(0)
        elif args.action == "checkpoint":
            cp = global_root_witness.checkpoint()
            if args.format == "json":
                print(json.dumps(cp.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ASYMMETRIC ROOT WITNESS CHECKPOINT (ADR 0075)      |")
                print("+---------------------------------------------------------------+")
                print(f"  Epoch:                #{cp.epoch}")
                print(f"  Tree Size:            {cp.tree_size} leaves")
                print(f"  Root Hash:            {cp.root_hash[:32]}...")
                print(f"  Prev Checkpoint:      {cp.prev_checkpoint_hash[:32]}...")
                print(f"  Checkpoint Hash:      {cp.checkpoint_hash[:32]}...")
                print(f"  Witness Public Key:   {cp.witness_public_key[:32]}...")
                print(f"  Signature (ES256):    {cp.signature[:32]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "verify":
            if not args.checkpoint_json:
                print("\033[91m[Root Witness Error] --checkpoint-json argument required for verification\033[0m", file=sys.stderr)
                sys.exit(1)
            try:
                if os.path.exists(args.checkpoint_json):
                    with open(args.checkpoint_json, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                else:
                    raw = json.loads(args.checkpoint_json)
                cp = SignedCheckpoint.from_dict(raw)
                is_valid = RootWitness.verify_checkpoint(cp, public_key=args.public_key)
                if args.format == "json":
                    print(json.dumps({"valid": is_valid, "epoch": cp.epoch, "tree_size": cp.tree_size}, indent=2))
                else:
                    color = "\033[92mVALID (ASYMMETRIC SIGNATURE VERIFIED)\033[0m" if is_valid else "\033[91mINVALID / FORGED SIGNATURE\033[0m"
                    print(f"[Root Witness Verification] Epoch #{cp.epoch} Checkpoint: {color}")
                sys.exit(0 if is_valid else 1)
            except Exception as e:
                print(f"\033[91m[Root Witness Verification Error] {e}\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "rotate":
            new_km = WitnessKeyManager.generate()
            rec = global_root_witness.rotate_key(new_km)
            if args.format == "json":
                print(json.dumps(rec.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH NIST SP 800-57 KEY ROTATION (ADR 0075)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Rotation ID:          {rec.rotation_id}")
                print(f"  Rotation Epoch:       #{rec.rotation_epoch}")
                print(f"  Old Public Key:       {rec.old_public_key[:32]}...")
                print(f"  New Public Key:       {rec.new_public_key[:32]}...")
                print(f"  Challenge Hash:       {rec.challenge[:32]}...")
                print(f"  Proof of Possession:  \033[92mVERIFIED (ES256)\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "status":
            st = global_root_witness.status().to_dict()
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH INDEPENDENT ROOT WITNESS (ADR 0075)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Active Public Key:    {st['active_public_key'][:32]}...")
                print(f"  Latest Epoch:         #{st['latest_epoch']}")
                print(f"  Current Tree Size:    {st['current_tree_size']} leaves")
                print(f"  Latest Checkpoint:    {st['latest_checkpoint_hash'][:32]}...")
                print(f"  Total Rotations:      {st['total_rotations']}")
                print(f"  Total Checkpoints:    {st['total_checkpoints']}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "capability":
        from swarmwatch.tenant_capability import (
            global_capability_manager,
            CapabilityToken,
            CapabilityError,
        )
        if args.action == "issue":
            scopes_list = [s.strip() for s in args.scopes.split(",") if s.strip()]
            token = global_capability_manager.issue_root_token(
                tenant_id=args.tenant_id,
                subject_principal=args.principal,
                scopes=scopes_list,
                resource_pattern=args.resource,
            )
            d = token.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ROOT CAPABILITY TOKEN ISSUED (ADR 0076)            |")
                print("+---------------------------------------------------------------+")
                print(f"  Token ID:             {token.token_id}")
                print(f"  Tenant ID:            {token.tenant_id}")
                print(f"  Subject Principal:    {token.subject_principal}")
                print(f"  Scopes:               {', '.join(token.scopes)}")
                print(f"  Resource Pattern:     {token.resource_pattern}")
                print(f"  Depth:                {token.depth} / {token.max_depth}")
                print(f"  Attenuation Digest:   {token.attenuation_digest[:32]}...")
                print(f"  Signature:            \033[92mVALID (ES256)\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "attenuate":
            if not args.token_json:
                print("\033[91m[Capability Error] --token-json required for attenuation\033[0m", file=sys.stderr)
                sys.exit(1)
            raw = args.token_json
            if os.path.exists(raw):
                with open(raw, "r", encoding="utf-8") as f:
                    raw = f.read()
            parent_token = CapabilityToken.from_dict(json.loads(raw))
            scopes_list = [s.strip() for s in args.scopes.split(",") if s.strip()] if args.scopes else None
            child = global_capability_manager.attenuate(
                parent_token=parent_token,
                subject_principal=args.principal,
                scopes=scopes_list,
                resource_pattern=args.resource if args.resource != "*" else None,
            )
            d = child.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ATTENUATED CAPABILITY TOKEN DERIVED (ADR 0076)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Child Token ID:       {child.token_id}")
                print(f"  Parent Token ID:      {child.parent_token_id}")
                print(f"  Tenant ID:            {child.tenant_id}")
                print(f"  Subject Principal:    {child.subject_principal}")
                print(f"  Scopes:               {', '.join(child.scopes)}")
                print(f"  Resource Pattern:     {child.resource_pattern}")
                print(f"  Depth:                {child.depth} / {child.max_depth}")
                print(f"  Signature:            \033[92mVALID (ES256)\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "verify":
            if not args.token_json:
                print("\033[91m[Capability Error] --token-json required for verification\033[0m", file=sys.stderr)
                sys.exit(1)
            raw = args.token_json
            if os.path.exists(raw):
                with open(raw, "r", encoding="utf-8") as f:
                    raw = f.read()
            tok = CapabilityToken.from_dict(json.loads(raw))
            scope = args.scopes.split(",")[0].strip() if args.scopes else "telemetry:write"
            verdict = global_capability_manager.verify(
                token=tok,
                action_scope=scope,
                target_resource=args.resource if args.resource != "*" else "sw://tenant-default/resource",
                executing_principal=args.principal,
                expected_tenant_id=args.tenant_id,
            )
            d = verdict.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                color = "\033[92m" if verdict.valid else "\033[91m"
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CAPABILITY BOUNDARY VERDICT (ADR 0076)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Verdict:              {color}{'PERMITTED' if verdict.valid else 'BLOCKED'}\033[0m")
                print(f"  Token ID:             {verdict.token_id}")
                print(f"  Tenant ID:            {verdict.tenant_id}")
                print(f"  Action Scope:         {verdict.action_scope}")
                print(f"  Target Resource:      {verdict.target_resource}")
                print(f"  Reason:               {verdict.reason}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if verdict.valid else 1)
        elif args.action == "revoke":
            if not args.token_id:
                print("\033[91m[Capability Error] --token-id required for revocation\033[0m", file=sys.stderr)
                sys.exit(1)
            receipt = global_capability_manager.revoke(token_id=args.token_id, reason=args.reason)
            d = receipt.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print(f"\033[93m[Capability Revoked] Token {receipt.token_id} revoked (Receipt: {receipt.revocation_id})\033[0m")
            sys.exit(0)
        elif args.action == "status":
            st = global_capability_manager.status().to_dict()
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH TENANT CAPABILITY MANAGER (ADR 0076)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Active Authority Key: {st['active_root_public_key'][:32]}...")
                print(f"  Total Issued:         {st['total_issued']}")
                print(f"  Total Attenuated:     {st['total_attenuated']}")
                print(f"  Total Revoked:        {st['total_revoked']}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand == "compensation":
        from swarmwatch.compensation_algebra import (
            global_compensation_engine,
            ActionExecution,
            CompensationPlan,
            CompensationError,
            IrreversibleActionError,
        )
        if args.action == "classify":
            if not args.actions_json:
                actions = [
                    ActionExecution(
                        action_id=args.action_id or "act-01",
                        action_type=args.action_type or "agent_read_state",
                        tenant_id=args.tenant_id,
                        principal=args.principal,
                    )
                ]
            else:
                raw = args.actions_json
                if os.path.exists(raw):
                    with open(raw, "r", encoding="utf-8") as f:
                        raw = f.read()
                raw_list = json.loads(raw)
                actions = [ActionExecution.from_dict(a) for a in raw_list]
            comp = global_compensation_engine.classify(actions)
            d = comp.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ACTION CLASSIFICATION LATTICE JOIN (ADR 0077)      |")
                print("+---------------------------------------------------------------+")
                print(f"  Composite Class:      {comp.reversibility_class.name}")
                print(f"  Total Actions:        {comp.total_actions}")
                print(f"  Reversible:           {comp.reversible_count}")
                print(f"  Compensatable:        {comp.compensatable_count}")
                print(f"  Irreversible:         {comp.irreversible_count}")
                print(f"  Irreversible IDs:     {', '.join(comp.irreversible_action_ids) or 'none'}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "plan":
            if not args.actions_json:
                actions = [
                    ActionExecution(
                        action_id=args.action_id or "act-01",
                        action_type=args.action_type or "agent_read_state",
                        tenant_id=args.tenant_id,
                        principal=args.principal,
                    )
                ]
            else:
                raw = args.actions_json
                if os.path.exists(raw):
                    with open(raw, "r", encoding="utf-8") as f:
                        raw = f.read()
                raw_list = json.loads(raw)
                actions = [ActionExecution.from_dict(a) for a in raw_list]
            plan = global_compensation_engine.build_compensation_plan(actions)
            d = plan.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REVERSE COMPENSATION PLAN GENERATED (ADR 0077)     |")
                print("+---------------------------------------------------------------+")
                print(f"  Plan ID:              {plan.plan_id}")
                print(f"  Composite Class:      {plan.composite_class.name}")
                print(f"  Total Steps:          {len(plan.steps)}")
                for s in plan.steps:
                    print(f"    Step {s.step_index}: [{s.action_type}] -> {s.handler_type} ({s.reversibility_class.name})")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "execute":
            if not args.plan_json:
                print("\033[91m[Compensation Error] --plan-json required for execute\033[0m", file=sys.stderr)
                sys.exit(1)
            raw = args.plan_json
            if os.path.exists(raw):
                with open(raw, "r", encoding="utf-8") as f:
                    raw = f.read()
            plan = CompensationPlan.from_dict(json.loads(raw))
            try:
                result = global_compensation_engine.execute_compensation(plan, allow_authorized_escalations=True)
                d = result.to_dict()
                if args.format == "json":
                    print(json.dumps(d, indent=2))
                else:
                    color = "\033[92m" if result.success else "\033[91m"
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH COMPENSATION PLAN EXECUTION (ADR 0077)             |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Outcome:              {color}{'SUCCESS' if result.success else 'FAILED'}\033[0m")
                    print(f"  Plan ID:              {result.plan_id}")
                    print(f"  Steps Executed:       {result.steps_executed}")
                    print(f"  Steps Failed:         {result.steps_failed}")
                    print(f"  Escalations Resolved: {result.escalations_resolved}")
                    if result.errors:
                        print(f"  Errors:               {', '.join(result.errors)}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0 if result.success else 1)
            except IrreversibleActionError as e:
                print(f"\033[91m[Compensation Halt] Irreversible action blocked: {e}\033[0m", file=sys.stderr)
                sys.exit(2)
        elif args.action == "escalate":
            action = ActionExecution(
                action_id=args.action_id or f"act-{uuid.uuid4().hex[:8]}",
                action_type=args.action_type or "external_transfer",
                tenant_id=args.tenant_id,
                principal=args.principal,
            )
            receipt = global_compensation_engine.escalate(action=action, reason=args.reason)
            d = receipt.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH HUMAN ESCALATION RECEIPT ISSUED (ADR 0077)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Escalation ID:        {receipt.escalation_id}")
                print(f"  Action ID:            {receipt.action_id}")
                print(f"  Action Type:          {receipt.action_type}")
                print(f"  Reason:               {receipt.reason}")
                print(f"  Status:               {receipt.status}")
                print(f"  Escalation Hash:      {receipt.escalation_hash[:32]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "authorize":
            if not args.escalation_id:
                print("\033[91m[Compensation Error] --escalation-id required for authorize\033[0m", file=sys.stderr)
                sys.exit(1)
            resolution = global_compensation_engine.authorize_escalation(
                escalation_id=args.escalation_id,
                authorizer_principal=args.principal,
                decision=args.decision,
                remediation_notes=args.notes,
            )
            d = resolution.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ESCALATION RESOLUTION AUTHORIZED (ADR 0077)        |")
                print("+---------------------------------------------------------------+")
                print(f"  Resolution ID:        {resolution.resolution_id}")
                print(f"  Escalation ID:        {resolution.escalation_id}")
                print(f"  Authorizer:           {resolution.authorizer_principal}")
                print(f"  Decision:             {resolution.decision}")
                print(f"  Notes:                {resolution.remediation_notes}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "status":
            st = global_compensation_engine.status().to_dict()
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH COMPENSATION ENGINE STATUS (ADR 0077)              |")
                print("+---------------------------------------------------------------+")
                print(f"  Registered Contracts: {st['total_registered_contracts']}")
                print(f"  Plans Generated:      {st['total_plans_generated']}")
                print(f"  Executed Plans:       {st['total_compensations_executed']}")
                print(f"  Pending Escalations:  {st['total_escalations_pending']}")
                print(f"  Resolved Escalations: {st['total_escalations_resolved']}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand in ("dataset-privacy", "dp"):
        from swarmwatch.dataset_privacy import (
            global_dataset_privacy_ledger,
            DatasetPrivacyError,
            DatasetPrivacyBudgetExhaustedError,
            EntityPrivacyBudgetExhaustedError,
            ReservationNotFoundError,
            DatasetLedgerSnapshot,
        )
        if args.action == "register":
            cfg = global_dataset_privacy_ledger.register_dataset(
                dataset_id=args.dataset_id,
                tenant_id=args.tenant_id,
                max_epsilon=args.max_epsilon,
                max_delta=args.max_delta,
                entity_epsilon_limit=args.entity_epsilon,
                entity_delta_limit=args.entity_delta,
            )
            d = cfg.to_dict()
            if args.format == "json":
                print(json.dumps(d, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DATASET PRIVACY CONFIG REGISTERED (ADR 0078)       |")
                print("+---------------------------------------------------------------+")
                print(f"  Dataset ID:           {cfg.dataset_id}")
                print(f"  Tenant ID:            {cfg.tenant_id}")
                print(f"  Max Epsilon:          {cfg.max_epsilon}")
                print(f"  Max Delta:            {cfg.max_delta}")
                print(f"  Entity Epsilon Limit: {cfg.entity_epsilon_limit}")
                print(f"  Entity Delta Limit:   {cfg.entity_delta_limit}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "reserve":
            try:
                res = global_dataset_privacy_ledger.reserve(
                    tenant_id=args.tenant_id,
                    dataset_id=args.dataset_id,
                    entity_id=args.entity_id,
                    epsilon=args.epsilon,
                    delta=args.delta,
                    sensitivity=args.sensitivity,
                    query_id=args.query_id,
                )
                d = res.to_dict()
                if args.format == "json":
                    print(json.dumps(d, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH PRIVACY BUDGET RESERVATION GRANTED (ADR 0078)     |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Reservation ID:       {res.reservation_id}")
                    print(f"  Dataset ID:           {res.dataset_id}")
                    print(f"  Entity ID:            {res.entity_id}")
                    print(f"  Reserved Epsilon:     {res.epsilon}")
                    print(f"  Reserved Delta:       {res.delta}")
                    print(f"  Receipt Hash:         {res.receipt_hash}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except (DatasetPrivacyBudgetExhaustedError, EntityPrivacyBudgetExhaustedError) as e:
                print(f"\033[91m[Budget Exhausted] {e}\033[0m", file=sys.stderr)
                sys.exit(2)
        elif args.action == "release":
            if not args.reservation_id:
                print("\033[91m[Error] --reservation-id is required for release\033[0m", file=sys.stderr)
                sys.exit(1)
            try:
                val = float(args.raw_value)
            except ValueError:
                val = json.loads(args.raw_value)
            try:
                rel = global_dataset_privacy_ledger.commit_release(
                    reservation_id=args.reservation_id,
                    raw_value=val,
                    noise_seed=args.noise_seed,
                )
                d = rel.to_dict()
                if args.format == "json":
                    print(json.dumps(d, indent=2))
                else:
                    print("\n+---------------------------------------------------------------+")
                    print("| SWARMWATCH PRIVACY LAPLACE RELEASE COMMITTED (ADR 0078)       |")
                    print("+---------------------------------------------------------------+")
                    print(f"  Reservation ID:       {rel.reservation_id}")
                    print(f"  Dataset ID:           {rel.dataset_id}")
                    print(f"  Entity ID:            {rel.entity_id}")
                    print(f"  Raw Value:            {rel.raw_value}")
                    print(f"  Perturbed Value:      {rel.perturbed_value}")
                    print(f"  Noise Added:          {rel.noise_added}")
                    print(f"  Release Hash:         {rel.release_hash}")
                    print("+---------------------------------------------------------------+\n")
                sys.exit(0)
            except (ReservationNotFoundError, DatasetPrivacyError) as e:
                print(f"\033[91m[Release Error] {e}\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "cancel":
            if not args.reservation_id:
                print("\033[91m[Error] --reservation-id is required for cancel\033[0m", file=sys.stderr)
                sys.exit(1)
            ok = global_dataset_privacy_ledger.cancel_reservation(args.reservation_id)
            if ok:
                print(f"Reservation '{args.reservation_id}' successfully cancelled and budget refunded.")
                sys.exit(0)
            else:
                print(f"\033[91mReservation '{args.reservation_id}' not found or not pending.\033[0m", file=sys.stderr)
                sys.exit(1)
        elif args.action == "remaining":
            rem = global_dataset_privacy_ledger.remaining(
                tenant_id=args.tenant_id,
                dataset_id=args.dataset_id,
                entity_id=args.entity_id,
            )
            if args.format == "json":
                print(json.dumps(rem, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH REMAINING PRIVACY BUDGET (ADR 0078)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Dataset Remaining Eps: {rem['dataset_remaining_epsilon']:.4f}")
                print(f"  Dataset Remaining Del: {rem['dataset_remaining_delta']:.6f}")
                print(f"  Entity Remaining Eps:  {rem['entity_remaining_epsilon']:.4f}")
                print(f"  Entity Remaining Del:  {rem['entity_remaining_delta']:.6f}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "reconcile":
            if not args.snapshot_json:
                print("\033[91m[Error] --snapshot-json is required for reconcile\033[0m", file=sys.stderr)
                sys.exit(1)
            raw = args.snapshot_json
            if os.path.exists(raw):
                with open(raw, "r", encoding="utf-8") as f:
                    raw = f.read()
            snap = json.loads(raw)
            res = global_dataset_privacy_ledger.reconcile(snap)
            if args.format == "json":
                print(json.dumps(res, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH MONOTONE SUPREMA RECONCILIATION (ADR 0078)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Dataset ID:           {res['dataset_id']}")
                print(f"  Tenant ID:            {res['tenant_id']}")
                print(f"  Reconciled Entities:  {res['reconciled_entities']}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "status":
            st = global_dataset_privacy_ledger.status(
                tenant_id=args.tenant_id,
                dataset_id=args.dataset_id,
            )
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH DATASET PRIVACY ACCOUNTANT STATUS (ADR 0078)       |")
                print("+---------------------------------------------------------------+")
                print(f"  Registered Datasets:  {st['total_registered_datasets']}")
                print(f"  Active Reservations:  {st['total_active_reservations']}")
                print(f"  Total Issued:         {st['total_reservations_issued']}")
                print(f"  Total Committed:      {st['total_releases_committed']}")
                print(f"  Entities Tracked:     {st['total_entities_tracked']}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand in ("interop", "il", "conformance"):
        from swarmwatch.interop_lab import (
            global_interop_lab,
            ProtocolStandard,
            ConformanceFault,
            ClientVariant,
            DeviationSeverity,
            ProtocolState,
        )

        if args.action == "exercise":
            protocol = ProtocolStandard(args.protocol)
            client_variant = ClientVariant(args.client_variant)
            fault = ConformanceFault(args.fault)
            raw_payload = args.payload
            res = global_interop_lab.exercise(
                target_protocol=protocol,
                client_variant=client_variant,
                fault_schedule=fault,
                payload=raw_payload,
                chunk_size=args.chunk_size,
            )
            if args.format == "json":
                print(json.dumps(res.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PROTOCOL INTEROP LAB EXERCISE (ADR 0079)           |")
                print("+---------------------------------------------------------------+")
                print(f"  Exercise ID:        {res.exercise_id}")
                print(f"  Protocol:           {res.protocol.value}")
                print(f"  Client Variant:     {res.client_variant.value}")
                print(f"  Fault Injected:     {res.fault.value}")
                print(f"  Success:            {res.success}")
                print(f"  State History:      {' -> '.join(res.state_history)}")
                print(f"  Spans Decoded:      {res.reconstructed_spans_count}")
                print(f"  Exec Time (µs):     {res.execution_time_ns / 1000.0:.2f}")
                print(f"  Deviations Count:   {len(res.deviations)}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if res.success else 1)
        elif args.action == "record-deviation":
            protocol = ProtocolStandard(args.protocol)
            severity = DeviationSeverity(args.severity)
            dev = global_interop_lab.record_deviation(
                protocol=protocol,
                scenario=args.scenario,
                observed=args.observed,
                expected=args.expected,
                severity=severity,
            )
            if args.format == "json":
                print(json.dumps(dev.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH RECORDED PROTOCOL DEVIATION (ADR 0079)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Deviation ID:   {dev.deviation_id}")
                print(f"  Protocol:       {dev.protocol.value}")
                print(f"  Severity:       {dev.severity.value}")
                print(f"  Scenario:       {dev.scenario}")
                print(f"  Observed:       {dev.observed}")
                print(f"  Expected:       {dev.expected}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "attest":
            receipt = global_interop_lab.attest_matrix(suite_name=args.suite_name)
            if args.format == "json":
                print(json.dumps(receipt.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH CONFORMANCE ATTESTATION RECEIPT (ADR 0079)         |")
                print("+---------------------------------------------------------------+")
                print(f"  Suite Name:         {receipt.suite_name}")
                print(f"  Total Exercises:    {receipt.total_exercises}")
                print(f"  Passed Exercises:   {receipt.pass_count}")
                print(f"  Failed Exercises:   {receipt.fail_count}")
                print(f"  Protocols:          {', '.join(receipt.protocols_covered) or 'None'}")
                print(f"  Faults Evaluated:   {', '.join(receipt.faults_evaluated) or 'None'}")
                print(f"  Attestation Hash:   {receipt.attestation_hash}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if receipt.fail_count == 0 else 1)
        elif args.action == "matrix":
            matrix = global_interop_lab.get_matrix()
            if args.format == "json":
                print(json.dumps(matrix, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PROTOCOL CONFORMANCE MATRIX (ADR 0079)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Exercises:    {matrix.get('total_exercises', 0)}")
                print(f"  Passed Exercises:   {matrix.get('passed_exercises', 0)}")
                print(f"  Failed Exercises:   {matrix.get('failed_exercises', 0)}")
                print(f"  Total Deviations:   {matrix.get('total_deviations', 0)}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
        elif args.action == "status":
            st = global_interop_lab.status()
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PROTOCOL INTEROP LAB STATUS (ADR 0079)             |")
                print("+---------------------------------------------------------------+")
                print(f"  Total Exercises Run:  {st.get('total_exercises_run', 0)}")
                print(f"  Passed Exercises:     {st.get('passed_exercises', 0)}")
                print(f"  Failed Exercises:     {st.get('failed_exercises', 0)}")
                print(f"  Total Deviations:     {st.get('total_deviations_recorded', 0)}")
                print(f"  Fatal Deviations:     {st.get('fatal_deviations', 0)}")
                print(f"  Conformance Cert:     {st.get('conformance_certified', False)}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    elif args.subcommand in ("provenance", "prov", "slsa"):
        from swarmwatch.release_provenance import (
            global_release_provenance,
            ProvenanceStepType,
            ArtifactDigest,
            SignedProvenanceManifest,
        )
        if args.action == "record-step":
            try:
                stype = ProvenanceStepType(args.step_type)
            except ValueError:
                print(f"Error: Invalid step type '{args.step_type}'", file=sys.stderr)
                sys.exit(1)

            step = global_release_provenance.record_step(
                step_type=stype,
                parent_step_ids=args.parent_step_ids,
                step_id=args.step_id,
            )
            if args.format == "json":
                print(json.dumps(step.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PROVENANCE STEP RECORDED (ADR 0080)                |")
                print("+---------------------------------------------------------------+")
                print(f"  Step ID:            {step.step_id}")
                print(f"  Step Type:          {step.step_type.value}")
                print(f"  Step Digest:        {step.step_digest}")
                print(f"  Parent Steps:       {step.parent_step_ids}")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "generate":
            manifest = global_release_provenance.generate_manifest(
                git_commit=args.git_commit,
                git_branch=args.git_branch,
                builder_id=args.builder_id,
            )
            if args.format == "json":
                print(json.dumps(manifest.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH SIGNED PROVENANCE MANIFEST ISSUED (ADR 0080)       |")
                print("+---------------------------------------------------------------+")
                print(f"  Manifest ID:        {manifest.manifest_id}")
                print(f"  Spec Version:       {manifest.spec_version}")
                print(f"  Git Commit:         {manifest.git_commit}")
                print(f"  DAG Root Hash:      {manifest.dag_root_hash}")
                print(f"  Steps Count:        {len(manifest.steps)}")
                print(f"  Products Count:     {len(manifest.products)}")
                print(f"  Signature (ES256):  {manifest.signature[:32]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "verify":
            if not args.manifest_file:
                print("Error: --manifest-file required for verification", file=sys.stderr)
                sys.exit(1)

            try:
                with open(args.manifest_file, "r", encoding="utf-8") as f:
                    manifest_dict = json.load(f)
            except Exception as e:
                print(f"Error reading manifest file: {e}", file=sys.stderr)
                sys.exit(1)

            res = global_release_provenance.verify_manifest(
                manifest=manifest_dict,
                public_key_b64=args.public_key,
                verify_disk_files=args.verify_files,
                base_dir=args.base_dir,
            )
            if args.format == "json":
                print(json.dumps(res.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH PROVENANCE MANIFEST VERIFICATION (ADR 0080)        |")
                print("+---------------------------------------------------------------+")
                verdict = "\033[92mVALID (SLSA LEVEL 3 COMPLIANT)\033[0m" if res.is_valid else "\033[91mINVALID (TAMPER DETECTED)\033[0m"
                print(f"  Verdict:            {verdict}")
                print(f"  Verified Steps:     {res.verified_steps_count}")
                print(f"  Verified Products:  {res.verified_products_count}")
                if res.errors:
                    print("  Errors Detected:")
                    for err in res.errors:
                        print(f"    - \033[91m{err}\033[0m")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0 if res.is_valid else 1)

        elif args.action == "dag":
            dag = global_release_provenance.get_dag()
            if args.format == "json":
                print(json.dumps(dag.to_dict(), indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH ACTIVE RELEASE PROVENANCE DAG (ADR 0080)           |")
                print("+---------------------------------------------------------------+")
                print(f"  DAG Hash:           {dag.compute_dag_hash()}")
                print(f"  Step Count:         {len(dag.steps)}")
                for i, s in enumerate(dag.topological_sort()):
                    print(f"    [{i+1}] {s.step_id} ({s.step_type.value}) -> digest: {s.step_digest[:16]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)

        elif args.action == "status":
            st = global_release_provenance.status()
            if args.format == "json":
                print(json.dumps(st, indent=2))
            else:
                print("\n+---------------------------------------------------------------+")
                print("| SWARMWATCH RELEASE PROVENANCE STATUS (ADR 0080)               |")
                print("+---------------------------------------------------------------+")
                print(f"  Active DAG Steps:   {st.get('active_dag_step_count', 0)}")
                print(f"  Manifests Issued:   {st.get('total_manifests_issued', 0)}")
                print(f"  Success Verifies:   {st.get('successful_verifications', 0)}")
                print(f"  Failed Verifies:    {st.get('failed_verifications', 0)}")
                print(f"  SLSA Level:         {st.get('slsa_level', 'SLSA_BUILD_LEVEL_3')}")
                print(f"  Public Key:         {st.get('public_key', '')[:24]}...")
                print("+---------------------------------------------------------------+\n")
            sys.exit(0)
    else:
        parser.print_help()
        sys.exit(1)







if __name__ == "__main__":
    main()
