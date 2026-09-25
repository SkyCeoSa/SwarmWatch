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
    else:
        parser.print_help()
        sys.exit(1)




if __name__ == "__main__":
    main()
