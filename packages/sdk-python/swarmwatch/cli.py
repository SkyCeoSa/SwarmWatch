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
    else:
        parser.print_help()
        sys.exit(1)



if __name__ == "__main__":
    main()
