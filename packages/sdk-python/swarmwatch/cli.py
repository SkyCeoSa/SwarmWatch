# -*- coding: utf-8 -*-
"""
SwarmWatch Command-Line Interface.
Allows starting the live dashboard and ingestion server via `swarmwatch ui`.
"""

import argparse
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
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
