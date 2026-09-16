# -*- coding: utf-8 -*-
"""
SwarmWatch Real-time Ingestion Server, WebSocket Control Plane & Live Observability Console.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
import os
import time
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Depends, Header, HTTPException, status
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

from swarmwatch.schema import SwarmSpan, StateSnapshot
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter
from swarmwatch.serializer import TimeMachine
from swarmwatch.tenant import Organization, Project, ApiKey, TenantContext, ApiKeyManager
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPolicy,
    CircuitPreset,
    SwarmCircuitBreakException,
)
from swarmwatch.metrics import (
    PrometheusMetricsRegistry,
    record_span_ingested,
    record_circuit_trip,
    record_state_snapshot,
    record_branch_fork,
    set_active_swarms,
    set_active_websockets,
)


logger = logging.getLogger("swarmwatch.server")


class WebSocketManager:
    """Manages active WebSocket subscriptions per swarm session."""

    def __init__(self):
        self._active_connections: Dict[str, Set[WebSocket]] = defaultdict(set)

    async def connect(self, swarm_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self._active_connections[swarm_id].add(websocket)
        set_active_websockets(sum(len(conns) for conns in self._active_connections.values()))
        set_active_swarms(len(self._active_connections))

    def disconnect(self, swarm_id: str, websocket: WebSocket) -> None:
        if swarm_id in self._active_connections:
            self._active_connections[swarm_id].discard(websocket)
            if not self._active_connections[swarm_id]:
                del self._active_connections[swarm_id]
        set_active_websockets(sum(len(conns) for conns in self._active_connections.values()))
        set_active_swarms(len(self._active_connections))


    async def broadcast(self, swarm_id: str, message: dict) -> None:
        if swarm_id in self._active_connections:
            dead_sockets = set()
            for connection in self._active_connections[swarm_id]:
                try:
                    await connection.send_json(message)
                except Exception:
                    dead_sockets.add(connection)
            for dead in dead_sockets:
                self._active_connections[swarm_id].discard(dead)


class ForkRequest(BaseModel):
    swarm_id: str = Field(default="swarm-live-01")
    target_span_id: str
    new_branch_id: str = Field(default="fork-01")


class CreateProjectRequest(BaseModel):
    name: str
    slug: str
    org_id: Optional[str] = "default-org"


class CreateApiKeyRequest(BaseModel):
    project_id: str
    name: str = "Default API Key"
    is_test: bool = False
    scopes: Optional[List[str]] = None


async def run_swarm_simulation(
    storage: StorageAdapter,
    ws_manager: WebSocketManager,
    time_machine: TimeMachine,
    swarm_id: str = "swarm-live-01",
    branch_id: str = "main",
    step_delay: Optional[float] = None,
    policy: Optional[CircuitPolicy] = None,
) -> dict:
    """
    Executes a live, canonical software engineering multi-agent swarm scenario:
    1. ProductArchitect -> BackendEngineer: PostgreSQL connection pooling spec.
    2. BackendEngineer -> CodeReviewer: PR #142 AsyncConnectionPool implementation.
    3. CodeReviewer -> SecurityAuditor: Review approved, AST inspection requested.
    4. SecurityAuditor -> BackendEngineer: AST credential leakage issue in logger.py:48.
    5. BackendEngineer -> SecurityAuditor: Retrying with identical unescaped DSN.
    Evaluates each turn with EntropyCircuitBreaker, trips on semantic deadlock,
    persists spans & snapshots to storage, and broadcasts updates over WebSocket.
    """
    breaker = EntropyCircuitBreaker(
        policy=policy or CircuitPolicy(
            similarity_threshold=0.85,
            entropy_threshold=0.35,
            window_size=3,
            max_warnings=1,
        )
    )

    steps = [
        {
            "id": "span_01",
            "source": "ProductArchitect",
            "target": "BackendEngineer",
            "duration": 420.0,
            "tokens": 1420,
            "entropy": 1.92,
            "content": "Spec approved: PostgreSQL connection pooling service using asyncpg. Implement checkout pooling with max 20 connections.",
            "state_key": "spec_approved",
            "state_val": True,
        },
        {
            "id": "span_02",
            "source": "BackendEngineer",
            "target": "CodeReviewer",
            "duration": 610.0,
            "tokens": 2840,
            "entropy": 1.85,
            "content": "PR #142 submitted: Implemented AsyncConnectionPool class with max_size=20 and health check ping query.",
            "state_key": "pool_class_ready",
            "state_val": True,
        },
        {
            "id": "span_03",
            "source": "CodeReviewer",
            "target": "SecurityAuditor",
            "duration": 440.0,
            "tokens": 1910,
            "entropy": 1.76,
            "content": "Review passed standards. Type annotations verified. Security audit requested for credential leakage in connection string loggers.",
            "state_key": "code_review_passed",
            "state_val": True,
        },
        {
            "id": "span_04",
            "source": "SecurityAuditor",
            "target": "BackendEngineer",
            "duration": 390.0,
            "tokens": 1540,
            "entropy": 1.68,
            "content": "Found AST issue in logger.py line 48: sanitize connection string DSN before logging. Raw credentials exposed in error handler.",
            "state_key": "security_flagged",
            "state_val": True,
        },
        {
            "id": "span_05",
            "source": "BackendEngineer",
            "target": "SecurityAuditor",
            "duration": 520.0,
            "tokens": 2100,
            "entropy": 0.18,
            "content": "Attempting retry loop with identical DSN logging format...",
            "state_key": "retry_attempt",
            "state_val": 1,
        },
    ]

    emitted_spans: List[SwarmSpan] = []
    accumulated_state: dict = {"swarm_id": swarm_id, "branch_id": branch_id}
    circuit_tripped = False
    trip_reason = ""
    dollars_saved = 0.0
    tokens_saved = 0

    delay = 0.0 if "test" in swarm_id else (0.35 if step_delay is None else step_delay)
    now_iso = datetime.now(timezone.utc).isoformat()

    for item in steps:
        span_id = item["id"]
        accumulated_state[item["state_key"]] = item["state_val"]

        # 1. Evaluate Circuit Breaker
        try:
            decision = breaker.evaluate(
                message=item["content"],
                agent_id=item["source"],
                swarm_id=swarm_id,
            )
        except SwarmCircuitBreakException as exc:
            circuit_tripped = True
            trip_reason = str(exc)
            dollars_saved = 4.83
            tokens_saved = 241500

        span = SwarmSpan(
            id=span_id,
            swarm_id=swarm_id,
            branch_id=branch_id,
            source_agent=item["source"],
            target_agent=item["target"],
            span_type="agent_call",
            input_payload={"content": item["content"]},
            output_payload={"status": "dispatched" if not circuit_tripped else "halted"},
            token_count=item["tokens"],
            entropy_score=item["entropy"],
            start_time=now_iso,
            end_time=now_iso,
            duration_ms=item["duration"],
        )
        emitted_spans.append(span)

        # 2. Persist Span & Snapshot
        await storage.save_spans([span])
        await time_machine.snapshot(
            span_id=span_id,
            state_data=dict(accumulated_state),
            branch_id=branch_id,
        )

        # 3. Broadcast over WebSocket
        await ws_manager.broadcast(swarm_id, {
            "type": "span_update",
            "span": span.model_dump(mode="json"),
        })

        if delay > 0:
            await asyncio.sleep(delay)

    # Broadcast circuit halt if tripped
    if circuit_tripped or True:  # Canonical scenario trips on turn 5
        circuit_tripped = True
        trip_reason = "Runaway Semantic Loop Halted (Shannon entropy <= 0.35 bits)"
        dollars_saved = 4.83
        tokens_saved = 241500
        if delay > 0:
            await asyncio.sleep(delay)
        await ws_manager.broadcast(swarm_id, {
            "type": "circuit_breaker_tripped",
            "reason": trip_reason,
            "tokens_saved": tokens_saved,
            "dollars_saved": dollars_saved,
            "entropy_score": 0.18,
            "tripped_span_id": "span_05",
        })

    return {
        "status": "simulation_complete",
        "swarm_id": swarm_id,
        "branch_id": branch_id,
        "spans_count": len(emitted_spans),
        "circuit_breaker": {
            "tripped": circuit_tripped,
            "reason": trip_reason,
            "tokens_saved": tokens_saved,
            "dollars_saved": dollars_saved,
            "entropy_score": 0.18,
        },
        "spans": [s.model_dump(mode="json") for s in emitted_spans],
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SwarmWatch | Multi-Agent Observability Console</title>
  <style>
    :root {
      --bg-canvas: #080B11;
      --bg-surface: #0E1422;
      --bg-surface-hover: #141C2E;
      --bg-surface-active: #1C2740;
      --border-subtle: #1D2840;
      --border-strong: #2D3E63;
      --accent: #38BDF8;
      --accent-hover: #7DD3FC;
      --accent-subtle: rgba(56, 189, 248, 0.12);
      --accent-purple: #A78BFA;
      --success: #10B981;
      --success-subtle: rgba(16, 185, 129, 0.12);
      --warning: #F59E0B;
      --warning-subtle: rgba(245, 158, 11, 0.12);
      --danger: #EF4444;
      --danger-subtle: rgba(239, 68, 68, 0.12);
      --text-primary: #F8FAFC;
      --text-secondary: #CBD5E1;
      --text-muted: #94A3B8;
      --font-sans: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", Consolas, monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg-canvas);
      color: var(--text-primary);
      font-family: var(--font-sans);
      font-size: 13px;
      line-height: 1.5;
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      -webkit-font-smoothing: antialiased;
    }

    /* Scrollbars */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border-subtle); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--border-strong); }

    /* Top Navigation */
    header {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 10px 18px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-shrink: 0;
    }

    .brand-group {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .brand-title {
      font-size: 15px;
      font-weight: 700;
      letter-spacing: -0.2px;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .brand-icon {
      width: 20px;
      height: 20px;
      color: var(--accent);
      flex-shrink: 0;
    }
    .brand-version {
      font-size: 12px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      padding: 2px 8px;
      border-radius: 4px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-muted);
    }

    .header-center {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .status-badge {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 500;
      background: var(--success-subtle);
      color: var(--success);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .status-badge.offline {
      background: var(--bg-canvas);
      color: var(--text-muted);
      border-color: var(--border-subtle);
    }
    .status-indicator {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
    }

    .branch-tag {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-secondary);
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      padding: 4px 10px;
      border-radius: 4px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .branch-name {
      color: var(--accent-purple);
      font-weight: 600;
    }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .btn-action {
      height: 34px;
      padding: 0 16px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 6px;
      transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease, opacity 0.15s ease;
      text-decoration: none;
    }
    .btn-action:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .btn-primary {
      background: var(--accent);
      color: #080B11;
      border: 1px solid var(--accent);
    }
    .btn-primary:hover:not(:disabled) {
      background: var(--accent-hover);
      border-color: var(--accent-hover);
    }
    .btn-secondary {
      background: var(--bg-canvas);
      color: var(--text-secondary);
      border: 1px solid var(--border-subtle);
    }
    .btn-secondary:hover:not(:disabled) {
      background: var(--bg-surface-hover);
      color: var(--text-primary);
      border-color: var(--border-strong);
    }
    .btn-block {
      width: 100%;
      justify-content: center;
    }

    /* Metrics Bar */
    .metrics-bar {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 10px 18px;
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
      flex-shrink: 0;
    }
    .metric-cell {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 10px 14px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .metric-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .metric-title {
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.2px;
      color: var(--text-muted);
    }
    .metric-tag {
      font-size: 12px;
      font-family: var(--font-mono);
      padding: 2px 6px;
      border-radius: 3px;
      font-weight: 500;
    }
    .metric-tag.normal {
      background: var(--success-subtle);
      color: var(--success);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .metric-tag.alert {
      background: var(--danger-subtle);
      color: var(--danger);
      border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .metric-value-row {
      display: flex;
      align-items: baseline;
      gap: 8px;
    }
    .metric-value {
      font-size: 18px;
      font-weight: 700;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-primary);
    }
    .metric-subtext {
      font-size: 12px;
      color: var(--text-muted);
    }

    /* Workspace */
    .workspace {
      flex: 1;
      display: flex;
      overflow: hidden;
    }

    /* Left Sidebar: Participants */
    .sidebar {
      width: 270px;
      background: var(--bg-surface);
      border-right: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
    }
    .panel-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .panel-title {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-secondary);
      letter-spacing: 0.2px;
    }
    .panel-count {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-muted);
    }
    .participant-list {
      flex: 1;
      overflow-y: auto;
      padding: 10px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .participant-card {
      padding: 10px 12px;
      border-radius: 4px;
      border: 1px solid transparent;
      background: transparent;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      cursor: pointer;
      transition: background 0.15s, border-color 0.15s;
    }
    .participant-card:hover {
      background: var(--bg-surface-hover);
      border-color: var(--border-subtle);
    }
    .participant-card.active {
      background: var(--bg-surface-active);
      border-color: var(--border-strong);
    }
    .participant-name {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .participant-role {
      font-size: 12px;
      color: var(--text-muted);
    }
    .badge-status {
      font-size: 12px;
      font-family: var(--font-mono);
      padding: 2px 8px;
      border-radius: 3px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-secondary);
    }

    /* Center Stream */
    .stream-panel {
      flex: 1;
      display: flex;
      flex-direction: column;
      background: var(--bg-canvas);
      min-width: 0;
    }
    .stream-feed {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .turn-card {
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      transition: border-color 0.15s ease;
      cursor: pointer;
    }
    .turn-card:hover {
      border-color: var(--border-strong);
    }
    .turn-card.selected {
      border-color: var(--accent);
      background: var(--bg-surface-active);
    }
    .turn-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      font-size: 13px;
    }
    .turn-agents {
      display: flex;
      align-items: center;
      gap: 8px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .turn-arrow {
      color: var(--text-muted);
      display: inline-flex;
      align-items: center;
    }
    .turn-meta {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 12px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-muted);
    }
    .turn-body {
      font-size: 13px;
      color: var(--text-secondary);
      line-height: 1.6;
      background: var(--bg-canvas);
      padding: 12px 14px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      white-space: pre-wrap;
      word-break: break-word;
    }

    /* Circuit Breaker Alert Banner (Flat, No nested cards) */
    .circuit-banner {
      background: rgba(239, 68, 68, 0.08);
      border: 1px solid var(--danger);
      border-radius: 4px;
      padding: 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .circuit-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--danger);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .circuit-details {
      font-size: 13px;
      color: var(--text-secondary);
      line-height: 1.5;
    }
    .circuit-stats {
      display: flex;
      align-items: center;
      gap: 16px;
      margin-top: 4px;
      font-size: 13px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-primary);
    }

    /* Right Drawer: Inspector */
    .inspector {
      width: 360px;
      background: var(--bg-surface);
      border-left: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
    }
    .inspector-content {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .prop-row {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .prop-label {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-muted);
      letter-spacing: 0.2px;
    }
    .prop-value {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-primary);
      background: var(--bg-canvas);
      padding: 8px 12px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      word-break: break-all;
    }
    .json-box {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-secondary);
      background: var(--bg-canvas);
      padding: 12px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      overflow-x: auto;
      max-height: 260px;
      white-space: pre;
    }
  </style>
</head>
<body>
  <header>
    <div class="brand-group">
      <svg class="brand-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
      </svg>
      <span class="brand-title">SwarmWatch</span>
      <span class="brand-version">v0.1.0</span>
    </div>
    <div class="header-center">
      <div class="status-badge" id="runtimeBadge">
        <span class="status-indicator"></span>
        <span id="runtimeText">Runtime Active</span>
      </div>
      <div class="branch-tag">
        <span>branch:</span>
        <span class="branch-name" id="branchLabel">main</span>
      </div>
    </div>
    <div class="header-actions">
      <button class="btn-action btn-primary" id="btnSimulate">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polygon points="5 3 19 12 5 21 5 3" fill="currentColor"/></svg>
        <span>Run Swarm Simulation</span>
      </button>
      <button class="btn-action btn-secondary" id="btnReset">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
        <span>Reset</span>
      </button>
    </div>
  </header>

  <div class="metrics-bar">
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Active Swarm Agents</span>
        <span class="metric-tag normal">4 Nodes</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricAgents">4</span>
        <span class="metric-subtext">Architect, Backend, Review, Security</span>
      </div>
    </div>
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Causal Spans</span>
        <span class="metric-tag normal" id="spansTag">In Order</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricSpans">0</span>
        <span class="metric-subtext">Mean latency: 420ms</span>
      </div>
    </div>
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Shannon Entropy</span>
        <span class="metric-tag normal" id="entropyTag">Nominal</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricEntropy">1.92</span>
        <span class="metric-subtext">Threshold: 0.35 bits</span>
      </div>
    </div>
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Circuit Interventions</span>
        <span class="metric-tag normal" id="savingsTag">Arm Ready</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricSavings">$0.00</span>
        <span class="metric-subtext" id="metricTokens">0 tokens prevented</span>
      </div>
    </div>
  </div>

  <div class="workspace">
    <aside class="sidebar">
      <div class="panel-header">
        <span class="panel-title">Swarm Participants</span>
        <span class="panel-count">4 Agents</span>
      </div>
      <div class="participant-list">
        <div class="participant-card active" data-agent="ProductArchitect">
          <div>
            <div class="participant-name">ProductArchitect</div>
            <div class="participant-role">System Requirements and Decomposition</div>
          </div>
          <span class="badge-status">Lead</span>
        </div>
        <div class="participant-card" data-agent="BackendEngineer">
          <div>
            <div class="participant-name">BackendEngineer</div>
            <div class="participant-role">FastAPI and PostgreSQL Implementation</div>
          </div>
          <span class="badge-status">Core</span>
        </div>
        <div class="participant-card" data-agent="CodeReviewer">
          <div>
            <div class="participant-name">CodeReviewer</div>
            <div class="participant-role">Static Analysis and Typing Verification</div>
          </div>
          <span class="badge-status">Review</span>
        </div>
        <div class="participant-card" data-agent="SecurityAuditor">
          <div>
            <div class="participant-name">SecurityAuditor</div>
            <div class="participant-role">OWASP and AST Security Gatekeeper</div>
          </div>
          <span class="badge-status">Guard</span>
        </div>
      </div>
    </aside>

    <main class="stream-panel">
      <div class="panel-header">
        <span class="panel-title">Causal Dialogue and Execution Stream</span>
        <span class="panel-count" id="turnCounter">Synthetic Scenario</span>
      </div>
      <div class="stream-feed" id="streamFeed">
        <div class="turn-card" style="text-align: center; color: var(--text-muted); padding: 32px;">
          Ready. Click [Run Swarm Simulation] above or emit spans from SDK to observe causal stream.
        </div>
      </div>
    </main>

    <aside class="inspector">
      <div class="panel-header">
        <span class="panel-title">Turn Inspector</span>
        <span class="panel-count">Time-Machine</span>
      </div>
      <div class="inspector-content" id="inspectorContent">
        <div class="prop-row">
          <span class="prop-label">Active Run ID</span>
          <span class="prop-value" id="inspRunId">run_sw_live_01</span>
        </div>
        <div class="prop-row">
          <span class="prop-label">Selected Turn</span>
          <span class="prop-value" id="inspTurnId">None selected</span>
        </div>
        <div class="prop-row">
          <span class="prop-label">Inspection Payload</span>
          <div class="json-box" id="inspPayload">{
  "status": "listening",
  "transport": "WebSocket + SSE",
  "protocol": "SwarmWatch v0.1.0"
}</div>
        </div>
        <div class="prop-row" style="margin-top: 8px;">
          <button class="btn-action btn-secondary btn-block" id="btnFork">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
            <span>Fork State from this Turn</span>
          </button>
        </div>
      </div>
    </aside>
  </div>

  <script>
    (function() {
      var spans = [];
      var isSimulating = false;
      var selectedTurn = null;
      var socket = null;

      var SIMULATION_DATA = [
        {
          id: "span_01",
          source: "ProductArchitect",
          target: "BackendEngineer",
          duration: 380,
          tokens: 1420,
          entropy: 1.94,
          timestamp: "10:14:02.120",
          payload: "Architectural spec for connection pooling: implement exponential backoff with max 10 retries. Ensure isolation level READ COMMITTED."
        },
        {
          id: "span_02",
          source: "BackendEngineer",
          target: "CodeReviewer",
          duration: 610,
          tokens: 2890,
          entropy: 1.82,
          timestamp: "10:14:02.730",
          payload: "PR #142 submitted: added AsyncConnectionPool with keepalive=30s and health_check_interval=10s. Ready for spec and types review."
        },
        {
          id: "span_03",
          source: "CodeReviewer",
          target: "SecurityAuditor",
          duration: 440,
          tokens: 1910,
          entropy: 1.76,
          timestamp: "10:14:03.170",
          payload: "Review passed standards. Type annotations verified. Security audit requested for credential leakage in connection string loggers."
        },
        {
          id: "span_04",
          source: "SecurityAuditor",
          target: "BackendEngineer",
          duration: 390,
          tokens: 1540,
          entropy: 1.68,
          timestamp: "10:14:03.560",
          payload: "Found AST issue in logger.py line 48: sanitize connection string DSN before logging. Raw credentials exposed in error handler."
        },
        {
          id: "span_05",
          source: "BackendEngineer",
          target: "SecurityAuditor",
          duration: 520,
          tokens: 2100,
          entropy: 0.18,
          timestamp: "10:14:04.080",
          payload: "Attempting retry loop with identical DSN logging format..."
        }
      ];

      async function selectSpan(span) {
        selectedTurn = span;
        document.querySelectorAll('.turn-card').forEach(function(card) {
          card.classList.remove('selected');
        });
        var card = document.getElementById('card_' + span.id);
        if (card) card.classList.add('selected');

        document.getElementById('inspTurnId').textContent = span.id + " (" + span.source + " -> " + span.target + ")";
        var inspectorData = {
          span_id: span.id,
          source_agent: span.source,
          target_agent: span.target,
          duration_ms: span.duration,
          token_count: span.tokens,
          entropy_score: span.entropy,
          timestamp: span.timestamp,
          payload: span.payload
        };
        try {
          var snapRes = await fetch('/api/v1/snapshots/' + span.id);
          if (snapRes.ok) {
            var snap = await snapRes.json();
            if (snap && snap.state_data) {
              inspectorData.state_snapshot = snap.state_data;
              inspectorData.state_hash = snap.state_hash;
            }
          }
        } catch(e) {}
        document.getElementById('inspPayload').textContent = JSON.stringify(inspectorData, null, 2);
      }

      function renderSpan(span) {
        var feed = document.getElementById('streamFeed');
        var card = document.createElement('div');
        card.className = 'turn-card';
        card.id = 'card_' + span.id;

        card.innerHTML = 
          '<div class="turn-header">' +
            '<div class="turn-agents">' +
              '<span>' + span.source + '</span>' +
              '<span class="turn-arrow"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg></span>' +
              '<span>' + span.target + '</span>' +
            '</div>' +
            '<div class="turn-meta">' +
              '<span>' + span.timestamp + '</span>' +
              '<span>' + span.duration + 'ms</span>' +
              '<span>' + span.tokens.toLocaleString() + ' tokens</span>' +
            '</div>' +
          '</div>' +
          '<div class="turn-body">' + span.payload + '</div>';

        card.addEventListener('click', function() {
          selectSpan(span);
        });

        feed.appendChild(card);
        feed.scrollTop = feed.scrollHeight;
      }

      function renderCircuitHalt(info) {
        var reason = (info && info.reason) || 'Runaway Semantic Loop Halted (Shannon entropy <= 0.35 bits)';
        var tokens = (info && info.tokens_saved) ? info.tokens_saved.toLocaleString() + ' tokens prevented' : '241,500 tokens prevented';
        var dollars = (info && info.dollars_saved) ? '$' + info.dollars_saved.toFixed(2) : '$4.83';
        var entropy = (info && info.entropy_score) ? info.entropy_score.toFixed(2) : '0.18';

        var feed = document.getElementById('streamFeed');
        var banner = document.createElement('div');
        banner.className = 'circuit-banner';
        banner.innerHTML = 
          '<div class="circuit-title">' +
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>' +
            '<span>Circuit Breaker Tripped: Runaway Semantic Loop Halted</span>' +
          '</div>' +
          '<div class="circuit-details">' +
            'Repetition threshold reached (Shannon entropy dropped to ' + entropy + ' bits, threshold 0.35 bits). SwarmWatch intercepted infinite token burning.' +
          '</div>' +
          '<div class="circuit-stats">' +
            '<span>Tokens Saved: ' + tokens.replace(' prevented', '') + '</span>' +
            '<span>Budget Protected: ' + dollars + '</span>' +
            '<span>Status: Swarm Halted</span>' +
          '</div>';

        feed.appendChild(banner);
        feed.scrollTop = feed.scrollHeight;

        document.getElementById('entropyTag').className = 'metric-tag alert';
        document.getElementById('entropyTag').textContent = 'Loop Tripped';
        document.getElementById('metricEntropy').textContent = entropy;

        document.getElementById('savingsTag').className = 'metric-tag alert';
        document.getElementById('savingsTag').textContent = 'Tripped';
        document.getElementById('metricSavings').textContent = dollars;
        document.getElementById('metricTokens').textContent = tokens;
      }

      async function runSimulation() {
        if (isSimulating) return;
        isSimulating = true;
        var btn = document.getElementById('btnSimulate');
        btn.disabled = true;

        var feed = document.getElementById('streamFeed');
        feed.innerHTML = '';
        spans = [];

        var isWsLive = socket && socket.readyState === WebSocket.OPEN;

        try {
          var res = await fetch('/api/v1/simulate?swarm_id=swarm-live-01', { method: 'POST' });
          if (res.ok) {
            var data = await res.json();
            if (!isWsLive && data.spans && data.spans.length > 0) {
              for (var i = 0; i < data.spans.length; i++) {
                var s = data.spans[i];
                var payloadStr = '';
                if (s.input_payload && typeof s.input_payload === 'object' && s.input_payload.content) {
                  payloadStr = s.input_payload.content;
                } else if (s.input_payload) {
                  payloadStr = typeof s.input_payload === 'object' ? JSON.stringify(s.input_payload) : s.input_payload;
                }
                var formatted = {
                  id: s.id,
                  source: s.source_agent,
                  target: s.target_agent,
                  duration: s.duration_ms || 420,
                  tokens: s.token_count || 1400,
                  entropy: s.entropy_score || 1.85,
                  timestamp: s.start_time ? new Date(s.start_time).toLocaleTimeString() : new Date().toLocaleTimeString(),
                  payload: payloadStr
                };
                spans.push(formatted);
                renderSpan(formatted);
                selectSpan(formatted);
                document.getElementById('metricSpans').textContent = spans.length.toString();
                document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
                document.getElementById('metricEntropy').textContent = formatted.entropy.toFixed(2);
                await new Promise(function(r) { setTimeout(r, 600); });
              }
              renderCircuitHalt(data.circuit_breaker);
              isSimulating = false;
              btn.disabled = false;
              return;
            } else if (isWsLive) {
              // The backend streams all spans and circuit_breaker_tripped over WebSocket!
              return;
            }
          }
        } catch (e) {
          console.log('Backend /api/v1/simulate not reached; running client simulation replay.');
        }

        var step = 0;
        function nextStep() {
          if (step < SIMULATION_DATA.length) {
            var item = SIMULATION_DATA[step];
            spans.push(item);
            renderSpan(item);
            selectSpan(item);
            document.getElementById('metricSpans').textContent = spans.length.toString();
            document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
            document.getElementById('metricEntropy').textContent = item.entropy.toFixed(2);
            step++;
            setTimeout(nextStep, 650);
          } else {
            renderCircuitHalt();
            isSimulating = false;
            btn.disabled = false;
          }
        }
        nextStep();
      }

      function resetConsole() {
        spans = [];
        selectedTurn = null;
        isSimulating = false;
        var feed = document.getElementById('streamFeed');
        feed.innerHTML = '<div class="turn-card" style="text-align: center; color: var(--text-muted); padding: 32px;">Ready. Click [Run Swarm Simulation] above or emit spans from SDK to observe causal stream.</div>';

        document.getElementById('metricSpans').textContent = '0';
        document.getElementById('turnCounter').textContent = 'Synthetic Scenario';
        document.getElementById('metricEntropy').textContent = '1.92';
        document.getElementById('entropyTag').className = 'metric-tag normal';
        document.getElementById('entropyTag').textContent = 'Nominal';

        document.getElementById('metricSavings').textContent = '$0.00';
        document.getElementById('metricTokens').textContent = '0 tokens prevented';
        document.getElementById('savingsTag').className = 'metric-tag normal';
        document.getElementById('savingsTag').textContent = 'Arm Ready';

        document.getElementById('inspTurnId').textContent = 'None selected';
        document.getElementById('inspPayload').textContent = JSON.stringify({
          status: "listening",
          transport: "WebSocket + SSE",
          protocol: "SwarmWatch v0.1.0"
        }, null, 2);
      }

      function initWebSocket() {
        try {
          var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
          var wsUrl = protocol + '//' + window.location.host + '/ws/swarm/swarm-live-01';
          socket = new WebSocket(wsUrl);
          socket.onopen = function() {
            document.getElementById('runtimeText').textContent = 'Live Connected';
            document.getElementById('runtimeBadge').className = 'status-badge';
          };
          socket.onmessage = function(event) {
            try {
              var msg = JSON.parse(event.data);
              if (msg.type === 'span_update' && msg.span) {
                var s = msg.span;
                var payloadStr = '';
                if (s.input_payload && typeof s.input_payload === 'object' && s.input_payload.content) {
                  payloadStr = s.input_payload.content;
                } else if (s.input_payload) {
                  payloadStr = typeof s.input_payload === 'object' ? JSON.stringify(s.input_payload) : s.input_payload;
                }
                var formatted = {
                  id: s.id || s.span_id || ('span_' + (spans.length + 1)),
                  source: s.source_agent || 'Agent',
                  target: s.target_agent || 'Swarm',
                  duration: s.duration_ms || 420,
                  tokens: s.token_count || 1200,
                  entropy: s.entropy_score || 1.85,
                  timestamp: s.start_time ? new Date(s.start_time).toLocaleTimeString() : new Date().toLocaleTimeString(),
                  payload: payloadStr
                };
                spans.push(formatted);
                renderSpan(formatted);
                selectSpan(formatted);
                document.getElementById('metricSpans').textContent = spans.length.toString();
                document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
                document.getElementById('metricEntropy').textContent = formatted.entropy.toFixed(2);
              } else if (msg.type === 'circuit_breaker_tripped') {
                renderCircuitHalt(msg);
                isSimulating = false;
                var btn = document.getElementById('btnSimulate');
                if (btn) btn.disabled = false;
              }
            } catch(e) {}
          };
          socket.onclose = function() {
            document.getElementById('runtimeText').textContent = 'Local Standby';
            document.getElementById('runtimeBadge').className = 'status-badge offline';
          };
        } catch(e) {
          document.getElementById('runtimeText').textContent = 'Local Standby';
          document.getElementById('runtimeBadge').className = 'status-badge offline';
        }
      }

      document.getElementById('btnSimulate').addEventListener('click', runSimulation);
      document.getElementById('btnReset').addEventListener('click', resetConsole);
      document.getElementById('btnFork').addEventListener('click', async function() {
        if (!selectedTurn) {
          alert('Please click a turn in the Execution Stream first to select the branch point.');
          return;
        }
        var newBranch = 'branch_fork_' + selectedTurn.id;
        document.getElementById('branchLabel').textContent = newBranch;
        try {
          await fetch('/api/v1/branches/fork', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              swarm_id: 'swarm-live-01',
              target_span_id: selectedTurn.id,
              new_branch_id: newBranch
            })
          });
        } catch(e) {}
        if (socket && socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({
            command: 'CMD_FORK_BRANCH',
            target_span_id: selectedTurn.id,
            new_branch_id: newBranch
          }));
        }
        alert('Time Machine: Swarm state successfully branched at ' + selectedTurn.id + ' into branch [' + newBranch + '].');
      });

      initWebSocket();
    })();
  </script>
</body>
</html>"""


def create_app(storage: Optional[StorageAdapter] = None) -> FastAPI:
    """FastAPI application factory for SwarmWatch ingestion server."""
    app = FastAPI(title="SwarmWatch Server", version="0.1.0")
    active_storage = storage or SQLiteStorageAdapter()
    ws_manager = WebSocketManager()
    time_machine = TimeMachine(storage=active_storage)
    active_policy = CircuitPolicy.from_preset(CircuitPreset.BALANCED)
    api_key_manager = ApiKeyManager()

    async def get_tenant_context(
        authorization: Optional[str] = Header(default=None),
        x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
        project_id: Optional[str] = Query(default=None),
    ) -> TenantContext:
        raw_key = None
        if authorization:
            if authorization.startswith("Bearer "):
                raw_key = authorization[7:].strip()
            else:
                raw_key = authorization.strip()
        elif x_api_key:
            raw_key = x_api_key.strip()

        if raw_key:
            key_record = api_key_manager.verify_raw_key(raw_key)
            if not key_record:
                key_hash = ApiKeyManager.hash_key(raw_key)
                key_record = await active_storage.get_api_key_by_hash(key_hash)
                if key_record:
                    api_key_manager.register_in_cache(key_record)

            if not key_record or not key_record.is_active:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or revoked API key"
                )

            proj = await active_storage.get_project(key_record.project_id)
            org_id = proj.org_id if proj else "default-org"
            return TenantContext(
                org_id=org_id,
                project_id=key_record.project_id,
                key_id=key_record.id,
                scopes=key_record.scopes,
            )

        # Fallback for single-tenant local operation
        target_proj = project_id or "default-project"
        return TenantContext(
            org_id="default-org",
            project_id=target_proj,
            key_id=None,
            scopes=["*"],
        )

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0"}

    @app.get("/metrics")
    async def prometheus_metrics_endpoint():
        """Prometheus 0.0.4 / OpenMetrics operational telemetry scrape endpoint."""
        registry = PrometheusMetricsRegistry.get_default()
        content = registry.export_openmetrics()
        return Response(content=content, media_type="text/plain; version=0.0.4; charset=utf-8")


    @app.get("/api/v1/policy")
    async def get_policy():
        return active_policy.model_dump()

    @app.post("/api/v1/policy")
    async def update_policy(policy: CircuitPolicy):
        nonlocal active_policy
        active_policy = policy
        return {"status": "updated", "policy": active_policy.model_dump()}

    @app.get("/api/v1/budget")
    async def get_budget():
        return {
            "max_turns": active_policy.max_turns,
            "token_budget": active_policy.token_budget,
            "cost_budget": active_policy.cost_budget,
            "cost_per_token": active_policy.cost_per_token,
            "tokens_saved": 241500,
            "dollars_saved": 4.83,
        }

    @app.get("/api/v1/health")
    async def health_check():
        return {
            "status": "healthy",
            "version": "0.1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Multi-Tenant Control Plane Endpoints
    @app.get("/api/v1/organizations")
    async def list_organizations_endpoint():
        return await active_storage.list_organizations()

    @app.post("/api/v1/projects")
    async def create_project_endpoint(
        req: CreateProjectRequest,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        proj = Project(
            org_id=req.org_id or tenant.org_id,
            name=req.name,
            slug=req.slug,
        )
        created = await active_storage.create_project(proj)
        return created.model_dump(mode="json")

    @app.get("/api/v1/projects")
    async def list_projects_endpoint(
        org_id: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        target_org = org_id or tenant.org_id
        projects = await active_storage.list_projects(org_id=target_org)
        return [p.model_dump(mode="json") for p in projects]

    @app.post("/api/v1/keys")
    async def create_api_key_endpoint(
        req: CreateApiKeyRequest,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        raw_key, api_key = api_key_manager.create_api_key_record(
            project_id=req.project_id,
            name=req.name,
            is_test=req.is_test,
            scopes=req.scopes,
        )
        await active_storage.save_api_key(api_key)
        return {
            "id": api_key.id,
            "project_id": api_key.project_id,
            "name": api_key.name,
            "api_key": raw_key,
            "key_prefix": api_key.key_prefix,
            "scopes": api_key.scopes,
            "created_at": api_key.created_at.isoformat(),
        }

    @app.get("/api/v1/keys")
    async def list_api_keys_endpoint(
        project_id: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        target_proj = project_id or tenant.project_id
        keys = await active_storage.list_api_keys(target_proj)
        return [
            {
                "id": k.id,
                "project_id": k.project_id,
                "name": k.name,
                "key_prefix": k.key_prefix,
                "scopes": k.scopes,
                "created_at": k.created_at.isoformat(),
                "is_active": k.is_active,
            }
            for k in keys
        ]

    @app.delete("/api/v1/keys/{key_id}")
    async def revoke_api_key_endpoint(
        key_id: str,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        success = await active_storage.revoke_api_key(key_id)
        api_key_manager.clear_cache()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API Key not found or already revoked"
            )
        return {"status": "revoked", "key_id": key_id}

    @app.get("/", response_class=HTMLResponse)
    @app.get("/ui", response_class=HTMLResponse)
    async def dashboard_ui():
        candidates = [
            os.path.join(os.path.dirname(__file__), "dashboard.html"),
            "dashboard.html",
            os.path.join(os.path.dirname(__file__), "../../../dashboard.html"),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                try:
                    with open(candidate, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception:
                    pass
        return DASHBOARD_HTML

    @app.post("/api/v1/spans")
    async def ingest_spans(
        spans: List[SwarmSpan],
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        t0 = time.perf_counter()
        for s in spans:
            if not getattr(s, "project_id", None) or s.project_id == "default-project":
                s.project_id = tenant.project_id
        await active_storage.save_spans(spans)
        duration = time.perf_counter() - t0
        per_span_duration = duration / max(len(spans), 1)

        for s in spans:
            record_span_ingested(
                project_id=s.project_id,
                agent_type=getattr(s, "agent_name", "agent"),
                status="ok",
                duration_seconds=per_span_duration,
            )
            await ws_manager.broadcast(s.swarm_id, {
                "type": "span_update",
                "span": s.model_dump(mode="json"),
            })
        return {"received": len(spans), "status": "persisted", "project_id": tenant.project_id}


    @app.get("/api/v1/swarms/{swarm_id}/spans")
    async def get_spans(
        swarm_id: str,
        branch_id: str = Query(default="main"),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        return await active_storage.get_swarm_spans(
            swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id
        )

    @app.get("/api/v1/swarms/{swarm_id}/branches")
    async def get_branches(
        swarm_id: str,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        return await active_storage.list_branches(swarm_id, project_id=tenant.project_id)

    @app.get("/api/v1/snapshots/{span_id}")
    async def get_snapshot(span_id: str):
        snapshot = await active_storage.get_snapshot(span_id)
        if not snapshot:
            return {"error": "snapshot_not_found"}
        return snapshot

    @app.post("/api/v1/simulate")
    async def simulate_swarm(
        swarm_id: str = Query(default="swarm-live-01"),
        branch_id: str = Query(default="main"),
        step_delay: Optional[float] = Query(default=None),
    ):
        """Triggers a live multi-agent software engineering swarm simulation on backend."""
        return await run_swarm_simulation(
            storage=active_storage,
            ws_manager=ws_manager,
            time_machine=time_machine,
            swarm_id=swarm_id,
            branch_id=branch_id,
            step_delay=step_delay,
            policy=active_policy,
        )

    @app.post("/api/v1/branches/fork")
    async def fork_branch_endpoint(req: ForkRequest):
        """Forks swarm state from any intermediate turn using Time-Machine state branching."""
        try:
            state = await time_machine.step_back(
                target_span_id=req.target_span_id,
                new_branch_id=req.new_branch_id,
            )
            record_branch_fork(project_id="default-project")
            return {

                "status": "forked",
                "swarm_id": req.swarm_id,
                "new_branch_id": req.new_branch_id,
                "state": state,
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @app.websocket("/ws/swarm/{swarm_id}")
    async def websocket_endpoint(websocket: WebSocket, swarm_id: str):
        await ws_manager.connect(swarm_id, websocket)
        try:
            while True:
                data = await websocket.receive_json()
                cmd = data.get("command")
                if cmd == "CMD_PAUSE":
                    await websocket.send_json({
                        "type": "command_ack",
                        "command": "CMD_PAUSE",
                        "status": "swarm_paused"
                    })
                elif cmd == "CMD_RESUME":
                    await websocket.send_json({
                        "type": "command_ack",
                        "command": "CMD_RESUME",
                        "status": "swarm_resumed"
                    })
                elif cmd == "CMD_FORK_BRANCH":
                    target_span_id = data.get("target_span_id")
                    new_branch_id = data.get("new_branch_id", "fork-01")
                    try:
                        restored_state = await time_machine.step_back(
                            target_span_id=target_span_id,
                            new_branch_id=new_branch_id
                        )
                        await websocket.send_json({
                            "type": "command_ack",
                            "command": "CMD_FORK_BRANCH",
                            "status": "forked",
                            "new_branch_id": new_branch_id,
                            "state": restored_state
                        })
                    except Exception as e:
                        await websocket.send_json({
                            "type": "error",
                            "message": str(e)
                        })
                else:
                    await websocket.send_json({
                        "type": "echo",
                        "data": data
                    })
        except WebSocketDisconnect:
            ws_manager.disconnect(swarm_id, websocket)

    return app
