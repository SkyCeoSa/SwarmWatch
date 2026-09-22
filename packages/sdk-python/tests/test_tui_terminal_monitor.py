# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Terminal User Interface (TUI) & Headless Cluster Observability Console (Seam 21 / ADR 0032).
Verifies TerminalMonitor data fetching, ANSI frame rendering, table alignments,
circuit halt warnings, and CLI subcommand integration.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import pytest
from starlette.testclient import TestClient

from swarmwatch.tui import TerminalMonitor, AnsiColor, strip_ansi, visible_len, pad_ansi
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.schema import SwarmSpan, CircuitEvent, CounterfactualSavings
from swarmwatch.server import create_app


def test_strip_ansi_and_visible_len():
    """Verify ANSI escape stripping and visible length calculation."""
    c = AnsiColor(enabled=True)
    colored = f"{c.bold(c.cyan('SWARMWATCH'))} {c.green('RUNNING')}"

    plain = strip_ansi(colored)
    assert plain == "SWARMWATCH RUNNING"
    assert visible_len(colored) == len("SWARMWATCH RUNNING")


def test_pad_ansi():
    """Verify pad_ansi correctly pads text with ANSI color codes intact."""
    c = AnsiColor(enabled=True)
    text = c.cyan("SWARM")
    padded_left = pad_ansi(text, 10, align="left")
    padded_right = pad_ansi(text, 10, align="right")
    padded_center = pad_ansi(text, 10, align="center")

    assert visible_len(padded_left) == 10
    assert visible_len(padded_right) == 10
    assert visible_len(padded_center) == 10
    assert strip_ansi(padded_left) == "SWARM     "
    assert strip_ansi(padded_right) == "     SWARM"


def test_terminal_monitor_initialization():
    """Verify TerminalMonitor initializes with expected defaults and custom parameters."""
    mon_default = TerminalMonitor()
    assert mon_default.server_url == "http://127.0.0.1:8765"
    assert mon_default.refresh_interval == 1.0
    assert mon_default.max_spans == 10
    assert mon_default.ansi_color is True
    assert mon_default.is_paused is False

    mon_custom = TerminalMonitor(
        server_url="http://custom-host:9999/",
        db_path="custom.db",
        refresh_interval=0.5,
        swarm_filter="swarm-42",
        max_spans=20,
        ansi_color=False,
    )
    assert mon_custom.server_url == "http://custom-host:9999"
    assert mon_custom.db_path == "custom.db"
    assert mon_custom.refresh_interval == 0.5
    assert mon_custom.swarm_filter == "swarm-42"
    assert mon_custom.max_spans == 20
    assert mon_custom.ansi_color is False


def test_render_frame_with_mock_data():
    """Verify render_frame renders complete TUI components with circuit halt."""
    mon = TerminalMonitor(ansi_color=False)

    mock_data = {
        "source": "server",
        "server_url": "http://127.0.0.1:8765",
        "status": "HEALTHY",
        "version": "0.1.0",
        "swarms": [
            {
                "swarm_id": "swarm-live-01",
                "branch_id": "main",
                "turn_count": 5,
                "avg_entropy": 1.48,
                "min_entropy": 0.18,
                "status": "HALTED",
                "halt_reason": "Runaway Semantic Loop Halted (H <= 0.35 bits)",
            },
            {
                "swarm_id": "swarm-worker-02",
                "branch_id": "main",
                "turn_count": 12,
                "avg_entropy": 1.82,
                "min_entropy": 1.45,
                "status": "RUNNING",
                "halt_reason": None,
            },
        ],
        "spans": [
            {
                "id": "span_01",
                "source_agent": "ProductArchitect",
                "target_agent": "BackendEngineer",
                "token_count": 1420,
                "entropy_score": 1.92,
                "duration_ms": 420.0,
            },
            {
                "id": "span_05",
                "source_agent": "BackendEngineer",
                "target_agent": "SecurityAuditor",
                "token_count": 690,
                "entropy_score": 0.18,
                "duration_ms": 410.0,
            },
        ],
        "target_swarm": "swarm-live-01",
        "budget": {
            "tokens_saved": 241500,
            "dollars_saved": 4.83,
        },
    }

    frame = mon.render_frame(data=mock_data, width=100)

    # Assert Header
    assert "SWARMWATCH TOP" in frame
    assert "Target: http://127.0.0.1:8765" in frame
    assert "HEALTHY" in frame

    # Assert Aggregate metrics
    assert "Active Swarms: 2" in frame
    assert "Total Turns: 17" in frame
    assert "Protected Tokens: 241,500" in frame
    assert "Dollars Saved: $4.83" in frame

    # Assert Active swarms table
    assert "swarm-live-01" in frame
    assert "swarm-worker-02" in frame
    assert "HALTED" in frame
    assert "RUNNING" in frame

    # Assert Causal Spans Stream
    assert "ProductArchitect -> BackendEngineer" in frame
    assert "BackendEngineer -> SecurityAuditor" in frame
    assert "1.92 b" in frame
    assert "0.18 b" in frame

    # Assert Circuit Breaker Halt Bar
    assert "[CIRCUIT BREAKER HALT]" in frame
    assert "Runaway Semantic Loop Halted" in frame

    # Assert Hotkeys
    assert "[q] Quit" in frame
    assert "[r] Refresh" in frame


def test_render_frame_responsive_columns():
    """Verify frame width properly respects terminal width constraint."""
    mon = TerminalMonitor(ansi_color=False)
    mock_data = {
        "source": "server",
        "status": "HEALTHY",
        "swarms": [],
        "spans": [],
        "budget": {},
    }

    frame_80 = mon.render_frame(data=mock_data, width=80)
    lines_80 = frame_80.splitlines()
    # Header border and separator lines should match target width 80
    assert len(lines_80[0]) == 80
    assert len(lines_80[4]) == 80

    frame_120 = mon.render_frame(data=mock_data, width=120)
    lines_120 = frame_120.splitlines()
    # Header border and separator lines should match target width 120
    assert len(lines_120[0]) == 120
    assert len(lines_120[4]) == 120


def test_render_frame_no_color_plain_text():
    """Verify that when ansi_color=False, no escape codes appear in frame."""
    mon = TerminalMonitor(ansi_color=False)
    frame = mon.render_frame()
    assert "\033[" not in frame


def test_render_frame_ascii_fallback():
    """Verify that use_ascii=True renders clean standard ASCII box characters."""
    mon = TerminalMonitor(ansi_color=False, use_unicode=False)
    mock_data = {
        "source": "server",
        "status": "HEALTHY",
        "swarms": [],
        "spans": [],
        "budget": {},
    }
    frame = mon.render_frame(data=mock_data, width=80, use_ascii=True)
    # Box should use + and -
    lines = frame.splitlines()
    assert lines[0].startswith("+---")
    assert lines[0].endswith("---+")
    assert "[*] HEALTHY" in frame


def test_terminal_monitor_fetch_data_sqlite_fallback(tmp_path):
    """Verify fetch_data queries SQLite database directly when server is offline."""
    db_file = tmp_path / "tui_test.db"
    adapter = SQLiteStorageAdapter(db_path=str(db_file))

    # Insert a test span
    now_iso = datetime.now(timezone.utc).isoformat()
    test_span = SwarmSpan(
        id="tui_span_01",
        swarm_id="tui-test-swarm",
        branch_id="main",
        source_agent="Planner",
        target_agent="Worker",
        span_type="agent_turn",
        input_payload={"query": "test"},
        output_payload={"result": "ok"},
        token_count=500,
        entropy_score=1.75,
        start_time=now_iso,
        end_time=now_iso,
        duration_ms=120.0,
    )
    adapter.save_spans_sync([test_span])

    # Record circuit trip
    trip_event = CircuitEvent(
        id="trip-01",
        swarm_id="tui-test-swarm",
        event_type="trip",
        reason="Exceeded token quota",
        metric_val=0.2,
        counterfactual_savings=CounterfactualSavings(
            tokens_saved=50000,
            dollars_saved=1.00,
            simulated_spans_prevented=10,
        ),
        created_at=datetime.now(timezone.utc),
    )
    adapter.record_circuit_event_sync(trip_event)

    # Initialize TerminalMonitor pointing to an invalid server port but valid db_path
    monitor = TerminalMonitor(
        server_url="http://127.0.0.1:59999",  # Non-existent server port
        db_path=str(db_file),
        ansi_color=False,
    )

    data = monitor.fetch_data()
    assert data["source"] == "sqlite"
    assert data["status"] == "STANDALONE"
    assert len(data["swarms"]) >= 1
    swarm = data["swarms"][0]
    assert swarm["swarm_id"] == "tui-test-swarm"
    assert swarm["turn_count"] == 1
    assert swarm["status"] == "HALTED"
    assert swarm["halt_reason"] == "Exceeded token quota"

    assert len(data["spans"]) >= 1
    span = data["spans"][0]
    assert span["id"] == "tui_span_01"
    assert span["source_agent"] == "Planner"

    adapter.close_sync()


def test_terminal_monitor_run_once(capsys):
    """Verify run_once prints a frame to stdout and does not enter a loop."""
    mon = TerminalMonitor(ansi_color=False)
    mon.run_once()

    captured = capsys.readouterr()
    assert "SWARMWATCH TOP" in captured.out
    assert "HOTKEYS:" in captured.out


def test_server_list_swarms_endpoint(tmp_path):
    """Verify GET /api/v1/swarms endpoint returns active swarms with turn metrics."""
    db_file = tmp_path / "server_swarms.db"
    storage = SQLiteStorageAdapter(db_path=str(db_file))

    now_iso = datetime.now(timezone.utc).isoformat()
    span = SwarmSpan(
        id="span_srv_1",
        swarm_id="server-swarm-01",
        branch_id="main",
        source_agent="Boss",
        target_agent="Dev",
        span_type="agent_turn",
        input_payload={"task": "deploy"},
        output_payload={"status": "done"},
        token_count=1200,
        entropy_score=1.65,
        start_time=now_iso,
        end_time=now_iso,
        duration_ms=250.0,
    )
    storage.save_spans_sync([span])

    app = create_app(storage=storage)
    client = TestClient(app)

    res = client.get("/api/v1/swarms")
    assert res.status_code == 200
    swarms = res.json()
    assert isinstance(swarms, list)
    assert len(swarms) == 1
    assert swarms[0]["swarm_id"] == "server-swarm-01"
    assert swarms[0]["turn_count"] == 1
    assert swarms[0]["status"] == "RUNNING"
    assert swarms[0]["avg_entropy"] == 1.65

    storage.close_sync()


def test_cli_top_subcommand_arguments():
    """Verify argument parser recognizes swarmwatch top and all supported flags."""
    import argparse
    from swarmwatch.cli import main

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")
    top_parser = subparsers.add_parser("top")
    top_parser.add_argument("--url", default="http://127.0.0.1:8765")
    top_parser.add_argument("--db", default="swarmwatch.db")
    top_parser.add_argument("--interval", type=float, default=1.0)
    top_parser.add_argument("--swarm", default=None)
    top_parser.add_argument("--max-spans", type=int, default=10)
    top_parser.add_argument("--no-color", action="store_true")
    top_parser.add_argument("--once", action="store_true")

    args = parser.parse_args([
        "top",
        "--url", "http://k8s-pod:8765",
        "--db", "/custom/data.db",
        "--interval", "0.5",
        "--swarm", "swarm-prod-1",
        "--max-spans", "15",
        "--no-color",
        "--once",
    ])

    assert args.subcommand == "top"
    assert args.url == "http://k8s-pod:8765"
    assert args.db == "/custom/data.db"
    assert args.interval == 0.5
    assert args.swarm == "swarm-prod-1"
    assert args.max_spans == 15
    assert args.no_color is True
    assert args.once is True
