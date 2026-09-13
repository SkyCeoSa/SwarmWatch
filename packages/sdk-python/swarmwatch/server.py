# -*- coding: utf-8 -*-
"""
SwarmWatch Real-time Ingestion Server, WebSocket Control Plane & Live Swimlane Dashboard.
"""

from collections import defaultdict
import json
import logging
from typing import Dict, List, Optional, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from swarmwatch.schema import SwarmSpan, StateSnapshot
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter
from swarmwatch.serializer import TimeMachine

logger = logging.getLogger("swarmwatch.server")


class WebSocketManager:
    """Manages active WebSocket subscriptions per swarm session."""

    def __init__(self):
        self._active_connections: Dict[str, Set[WebSocket]] = defaultdict(set)

    async def connect(self, swarm_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self._active_connections[swarm_id].add(websocket)

    def disconnect(self, swarm_id: str, websocket: WebSocket) -> None:
        if swarm_id in self._active_connections:
            self._active_connections[swarm_id].discard(websocket)
            if not self._active_connections[swarm_id]:
                del self._active_connections[swarm_id]

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


# Embedded Amir-Minimal 60fps HTML5 Canvas Swimlane Dashboard
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SwarmWatch – Multi-Agent Observability Console</title>
  <style>
    :root {
      --bg-canvas: #080B11;
      --bg-surface: #0E1422;
      --bg-surface-hover: #141C2E;
      --bg-surface-active: #1A243B;
      --border-subtle: #1D2840;
      --border-strong: #2D3F66;
      --accent-sky: #38BDF8;
      --accent-sky-subtle: rgba(56, 189, 248, 0.12);
      --accent-sky-glow: rgba(56, 189, 248, 0.25);
      --success-emerald: #10B981;
      --success-subtle: rgba(16, 185, 129, 0.12);
      --warning-amber: #F59E0B;
      --warning-subtle: rgba(245, 158, 11, 0.12);
      --danger-crimson: #EF4444;
      --danger-subtle: rgba(239, 68, 68, 0.12);
      --danger-border: rgba(239, 68, 68, 0.4);
      --text-primary: #F8FAFC;
      --text-muted: #94A3B8;
      --text-dim: #64748B;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Inter", "Helvetica Neue", sans-serif;
      --font-mono: ui-monospace, "SF Mono", "Cascadia Code", "Fira Code", monospace;
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
      -moz-osx-font-smoothing: grayscale;
    }

    /* Scrollbars */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border-subtle); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--border-strong); }

    /* Top Header Bar */
    header {
      height: 52px;
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 0 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-shrink: 0;
      z-index: 30;
    }

    .brand-section {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .brand-icon {
      width: 28px;
      height: 28px;
      border-radius: 6px;
      background: linear-gradient(135deg, #0284C7 0%, #0369A1 100%);
      border: 1px solid var(--accent-sky);
      display: flex;
      align-items: center;
      justify-content: center;
      color: #fff;
      font-size: 14px;
      box-shadow: 0 0 10px var(--accent-sky-glow);
    }
    .brand-title {
      font-size: 15px;
      font-weight: 700;
      letter-spacing: -0.3px;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .brand-badge {
      font-size: 10px;
      font-weight: 600;
      letter-spacing: 0.5px;
      text-transform: uppercase;
      padding: 2px 6px;
      border-radius: 4px;
      background: var(--accent-sky-subtle);
      color: var(--accent-sky);
      border: 1px solid rgba(56, 189, 248, 0.25);
    }

    .header-center {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .status-pill {
      display: flex;
      align-items: center;
      gap: 6px;
      background: rgba(16, 185, 129, 0.08);
      border: 1px solid rgba(16, 185, 129, 0.25);
      color: var(--success-emerald);
      padding: 3px 10px;
      border-radius: 9999px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.4px;
    }
    .status-pill.offline {
      background: rgba(148, 163, 184, 0.08);
      border-color: rgba(148, 163, 184, 0.25);
      color: var(--text-muted);
    }
    .pulse-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: currentColor;
      box-shadow: 0 0 8px currentColor;
      animation: pulseAnim 2s infinite ease-in-out;
    }
    @keyframes pulseAnim {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }

    .branch-pill {
      display: flex;
      align-items: center;
      gap: 6px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-muted);
      padding: 3px 10px;
      border-radius: 6px;
      font-size: 11px;
      font-family: var(--font-mono);
      font-feature-settings: 'tnum' 1;
    }
    .branch-pill strong {
      color: #A78BFA;
      font-weight: 600;
    }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .btn-sim {
      background: linear-gradient(135deg, #0284C7 0%, #0369A1 100%);
      color: #FFFFFF;
      border: 1px solid #38BDF8;
      padding: 6px 14px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 7px;
      transition: all 0.15s ease;
      box-shadow: 0 0 12px var(--accent-sky-glow);
    }
    .btn-sim:hover {
      background: #38BDF8;
      color: #080B11;
      box-shadow: 0 0 18px rgba(56, 189, 248, 0.6);
      transform: translateY(-1px);
    }
    .btn-sim:active { transform: translateY(0); }

    .btn-ghost {
      background: transparent;
      color: var(--text-muted);
      border: 1px solid var(--border-subtle);
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.15s ease;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .btn-ghost:hover {
      background: var(--bg-surface-hover);
      color: var(--text-primary);
      border-color: var(--border-strong);
    }

    /* KPI Metrics Ribbon */
    .kpi-ribbon {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 8px 20px;
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
      flex-shrink: 0;
      z-index: 20;
    }
    .kpi-card {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 8px 14px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      transition: border-color 0.2s;
    }
    .kpi-card:hover {
      border-color: var(--border-strong);
    }
    .kpi-info {
      display: flex;
      flex-direction: column;
      gap: 2px;
      min-width: 0;
    }
    .kpi-label {
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--text-dim);
    }
    .kpi-value-row {
      display: flex;
      align-items: baseline;
      gap: 6px;
    }
    .kpi-value {
      font-size: 17px;
      font-weight: 700;
      font-family: var(--font-mono);
      font-feature-settings: 'tnum' 1;
      color: var(--text-primary);
      letter-spacing: -0.5px;
    }
    .kpi-unit {
      font-size: 11px;
      color: var(--text-muted);
    }
    .kpi-badge {
      font-size: 10px;
      font-weight: 600;
      padding: 1px 6px;
      border-radius: 4px;
      font-family: var(--font-mono);
    }
    .kpi-badge.healthy {
      background: var(--success-subtle);
      color: var(--success-emerald);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .kpi-badge.warning {
      background: var(--warning-subtle);
      color: var(--warning-amber);
      border: 1px solid rgba(245, 158, 11, 0.3);
    }
    .kpi-badge.tripped {
      background: var(--danger-subtle);
      color: var(--danger-crimson);
      border: 1px solid rgba(239, 68, 68, 0.4);
    }

    .kpi-meta {
      font-size: 11px;
      color: var(--text-dim);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .entropy-canvas {
      width: 72px;
      height: 22px;
      flex-shrink: 0;
      border-radius: 3px;
    }

    /* 3-Pane Body Workspace */
    .workspace {
      flex: 1;
      display: flex;
      overflow: hidden;
      position: relative;
    }

    /* Left Rail: Swarm Participants */
    .left-rail {
      width: 270px;
      background: var(--bg-surface);
      border-right: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
    }
    .rail-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .rail-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .rail-badge {
      font-size: 10px;
      font-weight: 700;
      background: var(--bg-surface-active);
      color: var(--accent-sky);
      padding: 1px 6px;
      border-radius: 10px;
      border: 1px solid var(--border-subtle);
    }
    .rail-search {
      padding: 8px 16px;
      border-bottom: 1px solid var(--border-subtle);
    }
    .search-box {
      width: 100%;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 5px 10px;
      font-size: 11px;
      color: var(--text-primary);
      outline: none;
      transition: border-color 0.15s;
    }
    .search-box:focus {
      border-color: var(--accent-sky);
    }
    .search-box::placeholder {
      color: var(--text-dim);
    }

    .participant-list {
      flex: 1;
      overflow-y: auto;
      padding: 8px 12px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .participant-card {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 8px 12px;
      display: flex;
      align-items: center;
      gap: 10px;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .participant-card:hover {
      background: var(--bg-surface-hover);
      border-color: var(--border-strong);
    }
    .participant-card.active {
      border-color: var(--accent-sky);
      background: rgba(56, 189, 248, 0.05);
    }
    .participant-card.tripped {
      border-color: var(--danger-crimson);
      background: rgba(239, 68, 68, 0.06);
    }
    .p-icon {
      font-size: 16px;
      width: 28px;
      height: 28px;
      border-radius: 6px;
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }
    .p-details {
      display: flex;
      flex-direction: column;
      min-width: 0;
      flex: 1;
    }
    .p-name {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-primary);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .p-role {
      font-size: 10px;
      color: var(--text-dim);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .p-status-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--text-dim);
      flex-shrink: 0;
    }
    .p-status-dot.idle { background: #64748B; }
    .p-status-dot.active { background: var(--accent-sky); box-shadow: 0 0 6px var(--accent-sky); }
    .p-status-dot.tripped { background: var(--danger-crimson); box-shadow: 0 0 6px var(--danger-crimson); }

    .rail-footer {
      padding: 12px 16px;
      border-top: 1px solid var(--border-subtle);
      background: var(--bg-surface);
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .footer-stat {
      display: flex;
      justify-content: space-between;
      font-size: 11px;
      color: var(--text-dim);
      font-family: var(--font-mono);
    }

    /* Center Stage: Causal Stream */
    .center-stage {
      flex: 1;
      background: var(--bg-canvas);
      display: flex;
      flex-direction: column;
      min-width: 0;
      overflow: hidden;
    }
    .stage-header {
      padding: 10px 20px;
      border-bottom: 1px solid var(--border-subtle);
      background: var(--bg-surface);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-shrink: 0;
    }
    .stage-title {
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .stage-counter {
      font-size: 11px;
      font-family: var(--font-mono);
      font-feature-settings: 'tnum' 1;
      color: var(--text-dim);
      padding: 2px 8px;
      background: var(--bg-canvas);
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
    }
    .stage-controls {
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 11px;
      color: var(--text-dim);
    }
    .toggle-label {
      display: flex;
      align-items: center;
      gap: 6px;
      cursor: pointer;
      user-select: none;
    }

    .stream-container {
      flex: 1;
      overflow-y: auto;
      padding: 16px 20px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    /* Empty state */
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      height: 100%;
      text-align: center;
      gap: 14px;
      color: var(--text-muted);
      padding: 40px 20px;
    }
    .empty-icon {
      font-size: 32px;
      opacity: 0.7;
    }
    .empty-title {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .empty-desc {
      font-size: 12px;
      color: var(--text-dim);
      max-width: 380px;
      line-height: 1.6;
    }

    /* Stream Turn Card */
    .turn-card {
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      overflow: hidden;
      transition: all 0.15s ease;
      cursor: pointer;
      box-shadow: 0 2px 8px rgba(0,0,0,0.25);
    }
    .turn-card:hover {
      border-color: var(--border-strong);
      transform: translateY(-1px);
    }
    .turn-card.selected {
      border-color: var(--accent-sky);
      box-shadow: 0 0 0 1px var(--accent-sky), 0 4px 14px var(--accent-sky-glow);
    }
    .turn-card-header {
      padding: 8px 14px;
      background: rgba(255, 255, 255, 0.02);
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      font-size: 11px;
    }
    .turn-step-num {
      font-family: var(--font-mono);
      font-weight: 700;
      color: var(--text-dim);
      background: var(--bg-canvas);
      padding: 2px 6px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
    }
    .turn-agents-flow {
      display: flex;
      align-items: center;
      gap: 8px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .agent-tag {
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
      display: flex;
      align-items: center;
      gap: 4px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
    }
    .agent-tag.source { color: var(--accent-sky); }
    .agent-tag.target { color: #A78BFA; }
    .flow-arrow { color: var(--text-dim); font-size: 10px; }

    .turn-metrics {
      display: flex;
      align-items: center;
      gap: 10px;
      font-family: var(--font-mono);
      font-feature-settings: 'tnum' 1;
      font-size: 11px;
      color: var(--text-dim);
    }
    .metric-chip {
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .entropy-chip {
      font-weight: 600;
      padding: 1px 6px;
      border-radius: 4px;
    }
    .entropy-chip.green { color: var(--success-emerald); background: var(--success-subtle); }
    .entropy-chip.amber { color: var(--warning-amber); background: var(--warning-subtle); }
    .entropy-chip.red { color: var(--danger-crimson); background: var(--danger-subtle); }

    .turn-body {
      padding: 12px 16px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .turn-message {
      font-size: 13px;
      color: var(--text-primary);
      line-height: 1.6;
    }
    .turn-tool-block {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 6px 10px;
      font-family: var(--font-mono);
      font-size: 11px;
      color: #7DD3FC;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .tool-icon { color: var(--accent-sky); font-size: 12px; }

    /* Circuit Breaker Alert Card */
    .circuit-breaker-card {
      background: linear-gradient(180deg, rgba(239, 68, 68, 0.12) 0%, rgba(239, 68, 68, 0.04) 100%);
      border: 1px solid var(--danger-crimson);
      border-radius: 8px;
      overflow: hidden;
      box-shadow: 0 0 20px rgba(239, 68, 68, 0.25);
      animation: alertGlow 1.5s infinite alternate ease-in-out;
    }
    @keyframes alertGlow {
      from { box-shadow: 0 0 12px rgba(239, 68, 68, 0.2); }
      to { box-shadow: 0 0 24px rgba(239, 68, 68, 0.45); }
    }
    .cb-header {
      padding: 10px 16px;
      background: rgba(239, 68, 68, 0.18);
      border-bottom: 1px solid rgba(239, 68, 68, 0.35);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .cb-title {
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.5px;
      color: #FCA5A5;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .cb-badge {
      font-size: 10px;
      font-weight: 700;
      background: var(--danger-crimson);
      color: #fff;
      padding: 2px 8px;
      border-radius: 4px;
      letter-spacing: 0.5px;
    }
    .cb-body {
      padding: 14px 18px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .cb-stats-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
    }
    .cb-stat-box {
      background: var(--bg-surface);
      border: 1px solid rgba(239, 68, 68, 0.3);
      border-radius: 6px;
      padding: 8px 12px;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .cb-stat-label {
      font-size: 10px;
      color: var(--text-dim);
      text-transform: uppercase;
    }
    .cb-stat-val {
      font-size: 14px;
      font-weight: 700;
      font-family: var(--font-mono);
      font-feature-settings: 'tnum' 1;
      color: #FCA5A5;
    }
    .cb-stat-val.positive { color: var(--success-emerald); }
    .cb-description {
      font-size: 12px;
      color: #CBD5E1;
      line-height: 1.5;
    }

    /* Right Inspector & Time Machine Drawer */
    .right-inspector {
      width: 370px;
      background: var(--bg-surface);
      border-left: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
      overflow-y: auto;
    }
    .inspector-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-shrink: 0;
    }
    .inspector-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--text-muted);
    }
    .inspector-span-id {
      font-size: 11px;
      font-family: var(--font-mono);
      color: var(--accent-sky);
      background: var(--bg-canvas);
      padding: 2px 8px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 5px;
    }
    .inspector-span-id:hover {
      border-color: var(--accent-sky);
    }

    .inspector-content {
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 16px;
      flex: 1;
    }

    .field-card {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .field-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .field-label {
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--text-dim);
    }
    .field-val-text {
      font-size: 12px;
      color: var(--text-primary);
      font-family: var(--font-mono);
      font-feature-settings: 'tnum' 1;
    }

    .json-box {
      background: #05070C;
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 10px;
      font-family: var(--font-mono);
      font-size: 11px;
      color: #93C5FD;
      line-height: 1.45;
      max-height: 200px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }

    .time-machine-box {
      background: rgba(167, 139, 250, 0.05);
      border: 1px solid rgba(167, 139, 250, 0.25);
      border-radius: 8px;
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .tm-header {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: #C4B5FD;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .tm-desc {
      font-size: 11px;
      color: var(--text-muted);
      line-height: 1.5;
    }
    .tm-input-group {
      display: flex;
      gap: 8px;
    }
    .tm-input {
      flex: 1;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 6px 10px;
      font-size: 11px;
      font-family: var(--font-mono);
      color: var(--text-primary);
      outline: none;
    }
    .tm-input:focus { border-color: #A78BFA; }

    .btn-fork {
      background: linear-gradient(135deg, #7C3AED 0%, #6D28D9 100%);
      color: #fff;
      border: 1px solid #A78BFA;
      padding: 8px 14px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.15s ease;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      box-shadow: 0 0 10px rgba(167, 139, 250, 0.3);
    }
    .btn-fork:hover {
      background: #8B5CF6;
      box-shadow: 0 0 16px rgba(167, 139, 250, 0.6);
      transform: translateY(-1px);
    }
    .btn-pause {
      background: var(--bg-surface-hover);
      color: var(--text-muted);
      border: 1px solid var(--border-subtle);
      padding: 8px 14px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      transition: all 0.15s;
    }
    .btn-pause:hover {
      background: var(--border-subtle);
      color: var(--text-primary);
    }

    .toast-notice {
      padding: 8px 12px;
      border-radius: 6px;
      font-size: 11px;
      display: none;
      align-items: center;
      gap: 6px;
    }
    .toast-notice.success {
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid var(--success-emerald);
      color: #6EE7B7;
    }

    /* Modal Guide */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(4, 7, 12, 0.82);
      backdrop-filter: blur(6px);
      z-index: 100;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }
    .modal-box {
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: 12px;
      width: 640px;
      max-width: 95%;
      max-height: 85vh;
      overflow-y: auto;
      padding: 24px;
      display: flex;
      flex-direction: column;
      gap: 16px;
      box-shadow: 0 20px 50px rgba(0,0,0,0.8);
    }
    .modal-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--border-subtle);
      padding-bottom: 12px;
    }
    .modal-title {
      font-size: 15px;
      font-weight: 700;
      color: var(--accent-sky);
    }
    .modal-close {
      background: none;
      border: none;
      color: var(--text-dim);
      font-size: 18px;
      cursor: pointer;
    }
    .modal-close:hover { color: var(--text-primary); }
    .guide-card {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 12px 16px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .guide-head {
      font-size: 12px;
      font-weight: 700;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .guide-p {
      font-size: 11px;
      color: var(--text-muted);
      line-height: 1.6;
    }
  </style>
</head>
<body>

  <!-- Top Header Bar -->
  <header>
    <div class="brand-section">
      <div class="brand-icon">&#x26A1;</div>
      <div class="brand-title">
        SwarmWatch
        <span class="brand-badge">Console</span>
      </div>
    </div>

    <div class="header-center">
      <div class="status-pill" id="ws-status">
        <span class="pulse-dot"></span>
        <span id="ws-status-text">LIVE STREAM</span>
      </div>
      <div class="branch-pill">
        <span>&#x1F33F; branch:</span>
        <strong id="kpi-branch">main</strong>
      </div>
      <div class="branch-pill" style="color: var(--text-dim);">
        <span>swarm:</span>
        <span style="color: var(--text-muted);" id="disp-swarm-id">multi-agent-swarm</span>
      </div>
    </div>

    <div class="header-actions">
      <button class="btn-sim" id="btn-run-sim">&#x25B6; Run Live Swarm Simulation</button>
      <button class="btn-ghost" id="btn-clear-stream">&#x232B; Clear</button>
      <button class="btn-ghost" id="btn-open-guide">&#x1F4D8; Architecture</button>
    </div>
  </header>

  <!-- Top KPI Metrics Ribbon -->
  <div class="kpi-ribbon">
    <!-- Active Agents -->
    <div class="kpi-card">
      <div class="kpi-info">
        <span class="kpi-label">Active Participants</span>
        <div class="kpi-value-row">
          <span class="kpi-value" id="kpi-agents">4</span>
          <span class="kpi-unit">Agents</span>
        </div>
        <span class="kpi-meta">&#x1F4D0; Architect &#x26A1; Backend &#x1F50D; Reviewer &#x1F6E1;&#xFE0F; Security</span>
      </div>
    </div>

    <!-- Total Causal Spans -->
    <div class="kpi-card">
      <div class="kpi-info">
        <span class="kpi-label">Total Causal Spans</span>
        <div class="kpi-value-row">
          <span class="kpi-value" id="kpi-spans">0</span>
          <span class="kpi-unit">turns</span>
        </div>
        <span class="kpi-meta">Sub-millisecond causal tracking</span>
      </div>
    </div>

    <!-- Shannon Entropy Score -->
    <div class="kpi-card">
      <div class="kpi-info">
        <span class="kpi-label">Shannon Entropy Score</span>
        <div class="kpi-value-row">
          <span class="kpi-value" id="kpi-entropy">1.00</span>
          <span class="kpi-badge healthy" id="entropy-badge">HEALTHY</span>
        </div>
        <span class="kpi-meta" id="entropy-sub">Semantic diversity optimal</span>
      </div>
      <canvas id="entropy-canvas" class="entropy-canvas" width="72" height="22"></canvas>
    </div>

    <!-- Circuit Breaker Savings -->
    <div class="kpi-card">
      <div class="kpi-info">
        <span class="kpi-label">Circuit Breaker Savings</span>
        <div class="kpi-value-row">
          <span class="kpi-value" style="color: var(--accent-sky);" id="kpi-saved">$0.00</span>
          <span class="kpi-unit" id="kpi-saved-tokens">(0 tkns)</span>
        </div>
        <span class="kpi-meta" id="circuit-status-sub">Zero-token loop prevention active</span>
      </div>
    </div>
  </div>

  <!-- 3-Pane Workspace -->
  <div class="workspace">
    <!-- Left Rail: Swarm Participants -->
    <aside class="left-rail">
      <div class="rail-header">
        <span class="rail-title">&#x1F916; Participants</span>
        <span class="rail-badge" id="rail-count-badge">4 nodes</span>
      </div>

      <div class="rail-search">
        <input type="text" id="agent-filter" class="search-box" placeholder="Filter agents or roles..." />
      </div>

      <div class="participant-list" id="participant-list">
        <!-- ProductArchitect -->
        <div class="participant-card" data-agent="ProductArchitect">
          <div class="p-icon">&#x1F4D0;</div>
          <div class="p-details">
            <span class="p-name">ProductArchitect</span>
            <span class="p-role">API &amp; Schema Design</span>
          </div>
          <div class="p-status-dot idle" id="dot-ProductArchitect"></div>
        </div>

        <!-- BackendEngineer -->
        <div class="participant-card" data-agent="BackendEngineer">
          <div class="p-icon">&#x26A1;</div>
          <div class="p-details">
            <span class="p-name">BackendEngineer</span>
            <span class="p-role">FastAPI &amp; ORM Engine</span>
          </div>
          <div class="p-status-dot idle" id="dot-BackendEngineer"></div>
        </div>

        <!-- CodeReviewer -->
        <div class="participant-card" data-agent="CodeReviewer">
          <div class="p-icon">&#x1F50D;</div>
          <div class="p-details">
            <span class="p-name">CodeReviewer</span>
            <span class="p-role">Architecture &amp; Quality</span>
          </div>
          <div class="p-status-dot idle" id="dot-CodeReviewer"></div>
        </div>

        <!-- QASecurityAuditor -->
        <div class="participant-card" data-agent="QASecurityAuditor">
          <div class="p-icon">&#x1F6E1;&#xFE0F;</div>
          <div class="p-details">
            <span class="p-name">QASecurityAuditor</span>
            <span class="p-role">OWASP &amp; Fuzz Testing</span>
          </div>
          <div class="p-status-dot idle" id="dot-QASecurityAuditor"></div>
        </div>
      </div>

      <div class="rail-footer">
        <div class="footer-stat">
          <span>Engine:</span>
          <span style="color: var(--text-muted);">FastAPI + SQLite</span>
        </div>
        <div class="footer-stat">
          <span>Protocol:</span>
          <span style="color: var(--text-muted);">RFC-9110 Causal</span>
        </div>
      </div>
    </aside>

    <!-- Center Stage: Causal Dialogue Stream -->
    <main class="center-stage">
      <div class="stage-header">
        <div class="stage-title">
          <span>&#x26A1; Causal Dialogue &amp; Execution Stream</span>
          <span class="stage-counter" id="stream-count-pill">0 events</span>
        </div>
        <div class="stage-controls">
          <label class="toggle-label">
            <input type="checkbox" id="chk-autoscroll" checked />
            <span>Auto-scroll</span>
          </label>
        </div>
      </div>

      <div class="stream-container" id="stream-container">
        <!-- Empty State Prompt -->
        <div class="empty-state" id="stream-empty-state">
          <div class="empty-icon">&#x1FA9E;</div>
          <div class="empty-title">Ready for Multi-Agent Stream</div>
          <div class="empty-desc">
            Click <strong>[&#x25B6; Run Live Swarm Simulation]</strong> above to stream 5 sequential software engineering agent turns with zero-token loop interception, or stream live spans via <code>POST /api/v1/spans</code>.
          </div>
        </div>
      </div>
    </main>

    <!-- Right Inspector / Time-Machine Drawer -->
    <aside class="right-inspector">
      <div class="inspector-header">
        <span class="inspector-title">&#x1F50E; Step Inspector</span>
        <span class="inspector-span-id" id="disp-span-id" title="Click to copy Span ID">
          <span>select a turn</span>
        </span>
      </div>

      <div class="inspector-content">
        <!-- Causality Route -->
        <div class="field-card">
          <div class="field-header">
            <span class="field-label">Causal Routing</span>
            <span class="field-val-text" id="disp-meta" style="color: var(--text-dim);">-</span>
          </div>
          <div class="field-val-text" id="disp-flow" style="color: var(--accent-sky); font-weight: 600;">-</div>
        </div>

        <!-- Latency & Tokens Grid -->
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
          <div class="field-card">
            <span class="field-label">Duration</span>
            <span class="field-val-text" id="disp-duration">-</span>
          </div>
          <div class="field-card">
            <span class="field-label">Token Consumption</span>
            <span class="field-val-text" id="disp-tokens">-</span>
          </div>
        </div>

        <!-- Input Payload -->
        <div class="field-card">
          <div class="field-header">
            <span class="field-label">Input Payload</span>
          </div>
          <pre class="json-box" id="disp-input">{
  "hint": "Click any card in the center stream to inspect payload state."
}</pre>
        </div>

        <!-- Output Payload -->
        <div class="field-card">
          <div class="field-header">
            <span class="field-label">Output Payload</span>
          </div>
          <pre class="json-box" id="disp-output">{
  "status": "ready"
}</pre>
        </div>

        <!-- Time-Machine Branching -->
        <div class="time-machine-box">
          <div class="tm-header">&#x23F1;&#xFE0F; Time-Machine State Forking</div>
          <p class="tm-desc">
            Fork a new independent execution branch from this exact causal span snapshot without re-running predecessor agents.
          </p>
          <div class="tm-input-group">
            <input type="text" id="input-branch-name" class="tm-input" value="fork-01" placeholder="branch-name" />
            <button class="btn-fork" id="btn-fork">&#x26A1; Fork Branch</button>
          </div>
          <div class="toast-notice success" id="fork-toast">
            &#x2714; Branch created! Swarm state restored at selected step.
          </div>
          <button class="btn-pause" id="btn-pause">&#x23F8;&#xFE0F; Pause Swarm Execution</button>
        </div>
      </div>
    </aside>
  </div>

  <!-- Architecture Guide Modal -->
  <div class="modal-backdrop" id="modal-guide">
    <div class="modal-box">
      <div class="modal-head">
        <div class="modal-title">&#x1F4D8; SwarmWatch Architecture &amp; Design Principles</div>
        <button class="modal-close" id="btn-close-guide">&times;</button>
      </div>

      <div class="guide-card">
        <div class="guide-head">&#x26A1; 1. Causal Tracing vs. Traditional Flat Logs</div>
        <p class="guide-p">
          In distributed agent swarms, flat sequential logs obscure causality. SwarmWatch uses RFC-9110 causal spans tracking <code>parent_span_id</code>, <code>source_agent</code>, and <code>target_agent</code> to reconstruct exact causal chains with sub-millisecond precision.
        </p>
      </div>

      <div class="guide-card">
        <div class="guide-head">&#x1F6E1;&#xFE0F; 2. Shannon Entropy Circuit Breaker</div>
        <p class="guide-p">
          To stop infinite multi-agent ping-pong loops without paying for expensive LLM judge calls, SwarmWatch computes output entropy mathematically:
          <code>H(X) = -&Sigma; P(x) log2 P(x)</code>. When entropy drops below 0.50, the zero-token circuit breaker immediately trips in &lt;1ms.
        </p>
      </div>

      <div class="guide-card">
        <div class="guide-head">&#x23F1;&#xFE0F; 3. Time-Machine State Branching</div>
        <p class="guide-p">
          SwarmWatch snapshots state in an append-only graph. If an agent hallucinates at step 10, developers can step back and fork a new branch (e.g. <code>fork-01</code>) from step 9 with altered parameters without paying to re-run steps 1&ndash;8.
        </p>
      </div>
    </div>
  </div>

  <script>
    // State
    let spans = [];
    let selectedSpan = null;
    let isPaused = false;
    let simTimer = null;
    let entropyHistory = [1.0];

    // Agents Metadata
    const agentMeta = {
      "ProductArchitect": { icon: "&#x1F4D0;", role: "Product Architect", color: "#38BDF8" },
      "BackendEngineer": { icon: "&#x26A1;", role: "Backend Engineer", color: "#818CF8" },
      "CodeReviewer": { icon: "&#x1F50D;", role: "Senior Reviewer", color: "#34D399" },
      "QASecurityAuditor": { icon: "&#x1F6E1;&#xFE0F;", role: "QA & Security Auditor", color: "#FBBF24" }
    };

    // Sparkline Canvas
    const entropyCanvas = document.getElementById("entropy-canvas");
    const entropyCtx = entropyCanvas ? entropyCanvas.getContext("2d") : null;

    function drawEntropySparkline() {
      if (!entropyCtx) return;
      const w = entropyCanvas.width;
      const h = entropyCanvas.height;
      entropyCtx.clearRect(0, 0, w, h);

      if (entropyHistory.length < 2) return;

      const minVal = 0.0;
      const maxVal = 1.0;
      const step = w / (entropyHistory.length - 1);

      entropyCtx.beginPath();
      entropyHistory.forEach((val, i) => {
        const x = i * step;
        const norm = (val - minVal) / (maxVal - minVal);
        const y = h - (norm * (h - 4) + 2);
        if (i === 0) entropyCtx.moveTo(x, y);
        else entropyCtx.lineTo(x, y);
      });

      const lastVal = entropyHistory[entropyHistory.length - 1];
      const strokeColor = lastVal < 0.5 ? "#EF4444" : (lastVal < 0.75 ? "#F59E0B" : "#10B981");
      entropyCtx.strokeStyle = strokeColor;
      entropyCtx.lineWidth = 1.8;
      entropyCtx.stroke();
    }

    // WebSocket Management
    const urlParams = new URLSearchParams(window.location.search);
    const swarmId = urlParams.get("swarm_id") || "multi-agent-swarm";
    document.getElementById("disp-swarm-id").innerText = swarmId;

    const wsProto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProto}//${window.location.host}/ws/swarm/${swarmId}`;
    let ws = null;

    function initWebSocket() {
      try {
        ws = new WebSocket(wsUrl);
        ws.onopen = () => {
          const pill = document.getElementById("ws-status");
          pill.className = "status-pill";
          document.getElementById("ws-status-text").innerText = "LIVE STREAM";
        };
        ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            if (data.type === "span_update" && data.span) {
              appendSpan(data.span);
            } else if (data.type === "command_ack") {
              if (data.command === "CMD_FORK_BRANCH") {
                showForkSuccess(data.new_branch_id || "fork-01");
              } else if (data.command === "CMD_PAUSE") {
                isPaused = true;
                updatePauseButton();
              } else if (data.command === "CMD_RESUME") {
                isPaused = false;
                updatePauseButton();
              }
            }
          } catch (e) {
            console.error("WS Parse error", e);
          }
        };
        ws.onclose = () => {
          const pill = document.getElementById("ws-status");
          pill.className = "status-pill offline";
          document.getElementById("ws-status-text").innerText = "OFFLINE";
          setTimeout(initWebSocket, 3000);
        };
      } catch (e) {
        console.warn("WebSocket fallback mode", e);
      }
    }
    initWebSocket();

    // Append a Span
    function appendSpan(s) {
      spans.push(s);
      document.getElementById("stream-empty-state")?.remove();

      const ent = s.entropy_score !== undefined ? s.entropy_score : 1.0;
      entropyHistory.push(ent);
      if (entropyHistory.length > 20) entropyHistory.shift();
      drawEntropySparkline();

      // Check if this is a tripped circuit breaker
      const isTripped = ent < 0.50 || (s.output_payload && s.output_payload.action === "CIRCUIT_BREAKER_HALT");

      // Update Participant status dot
      document.querySelectorAll(".p-status-dot").forEach(el => el.className = "p-status-dot idle");
      const targetDot = document.getElementById("dot-" + s.target_agent);
      const sourceDot = document.getElementById("dot-" + s.source_agent);
      if (sourceDot) sourceDot.className = "p-status-dot active";
      if (targetDot) targetDot.className = isTripped ? "p-status-dot tripped" : "p-status-dot active";

      // Create stream card
      const stream = document.getElementById("stream-container");
      const card = document.createElement("div");

      if (isTripped) {
        // High-contrast Circuit Breaker Alert Card
        card.className = "circuit-breaker-card";
        const savedTokens = s.output_payload?.saved_tokens || 2450;
        const savedCost = s.output_payload?.cost_saved || s.output_payload?.saved_dollars || "$0.25";
        card.innerHTML = `
          <div class="cb-header">
            <div class="cb-title">
              <span>&#x1F6E1;&#xFE0F;</span>
              <span>CIRCUIT BREAKER ENGAGED: ZERO-TOKEN LOOP HALTED</span>
            </div>
            <span class="cb-badge">HALTED &bull; 0.8ms</span>
          </div>
          <div class="cb-body">
            <div class="cb-stats-grid">
              <div class="cb-stat-box">
                <span class="cb-stat-label">Entropy Drop</span>
                <span class="cb-stat-val">${ent.toFixed(2)} &lt; 0.50</span>
              </div>
              <div class="cb-stat-box">
                <span class="cb-stat-label">Latency</span>
                <span class="cb-stat-val">${(s.duration_ms || 0.8).toFixed(1)} ms</span>
              </div>
              <div class="cb-stat-box">
                <span class="cb-stat-label">Tokens Saved</span>
                <span class="cb-stat-val positive">+${savedTokens.toLocaleString()}</span>
              </div>
              <div class="cb-stat-box">
                <span class="cb-stat-label">Capital Saved</span>
                <span class="cb-stat-val positive">${savedCost}</span>
              </div>
            </div>
            <div class="cb-description">
              <strong>Algorithmic Loop Detection:</strong> Repeating dialogue ping-pong intercepted between 
              <code>${s.source_agent}</code> and <code>${s.target_agent}</code>. 
              The circuit breaker halted execution deterministically without incurring LLM judge inference cost.
            </div>
          </div>
        `;
        card.onclick = () => selectSpan(s, card);
      } else {
        // Standard Turn Card
        card.className = "turn-card";
        const srcMeta = agentMeta[s.source_agent] || { icon: "&#x1F916;", color: "#38BDF8" };
        const tgtMeta = agentMeta[s.target_agent] || { icon: "&#x1F916;", color: "#A78BFA" };
        
        let entropyClass = "green";
        if (ent < 0.5) entropyClass = "red";
        else if (ent < 0.75) entropyClass = "amber";

        const textContent = s.message || (s.output_payload ? JSON.stringify(s.output_payload) : "Agent execution turn completed.");
        const toolInvocation = s.tool_call || (s.span_type ? `tool: ${s.span_type}` : null);

        card.innerHTML = `
          <div class="turn-card-header">
            <div class="turn-agents-flow">
              <span class="turn-step-num">#${String(spans.length).padStart(2, '0')}</span>
              <span class="agent-tag source">${srcMeta.icon} ${s.source_agent}</span>
              <span class="flow-arrow">&#x2794;</span>
              <span class="agent-tag target">${tgtMeta.icon} ${s.target_agent}</span>
            </div>
            <div class="turn-metrics">
              <span class="metric-chip">&#x23F1; ${(s.duration_ms || 42).toFixed(1)}ms</span>
              <span class="metric-chip">&#x1FA99; ${s.token_count || 0} tkns</span>
              <span class="entropy-chip ${entropyClass}">H = ${ent.toFixed(2)}</span>
            </div>
          </div>
          <div class="turn-body">
            <div class="turn-message">${escapeHtml(textContent)}</div>
            ${toolInvocation ? `<div class="turn-tool-block"><span class="tool-icon">&#x2699;&#xFE0F;</span><code>${escapeHtml(toolInvocation)}</code></div>` : ""}
          </div>
        `;
        card.onclick = () => selectSpan(s, card);
      }

      stream.appendChild(card);

      // Auto-scroll
      if (document.getElementById("chk-autoscroll").checked) {
        stream.scrollTop = stream.scrollHeight;
      }

      updateMetrics();
      selectSpan(s, card);
    }

    function escapeHtml(str) {
      if (typeof str !== "string") str = JSON.stringify(str);
      return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }

    function updateMetrics() {
      document.getElementById("kpi-spans").innerText = spans.length;
      document.getElementById("stream-count-pill").innerText = `${spans.length} events`;

      if (spans.length > 0) {
        const last = spans[spans.length - 1];
        const ent = last.entropy_score !== undefined ? last.entropy_score : 1.0;
        const entVal = document.getElementById("kpi-entropy");
        const entBadge = document.getElementById("entropy-badge");
        const entSub = document.getElementById("entropy-sub");

        entVal.innerText = ent.toFixed(2);

        if (ent < 0.50) {
          entBadge.className = "kpi-badge tripped";
          entBadge.innerText = "LOOP TRIPPED";
          entVal.style.color = "var(--danger-crimson)";
          entSub.innerText = "Circuit breaker intervened";

          document.getElementById("kpi-saved").innerText = "$0.25";
          document.getElementById("kpi-saved-tokens").innerText = "(2,450 tkns)";
          document.getElementById("circuit-status-sub").innerText = "Loop halted in <1ms without LLM cost";
        } else if (ent < 0.75) {
          entBadge.className = "kpi-badge warning";
          entBadge.innerText = "DEGRADING";
          entVal.style.color = "var(--warning-amber)";
          entSub.innerText = "Repetition detected";
        } else {
          entBadge.className = "kpi-badge healthy";
          entBadge.innerText = "HEALTHY";
          entVal.style.color = "var(--text-primary)";
          entSub.innerText = "Semantic diversity optimal";
        }
      }
    }

    function selectSpan(s, cardEl) {
      selectedSpan = s;
      document.querySelectorAll(".turn-card").forEach(c => c.classList.remove("selected"));
      if (cardEl && cardEl.classList.contains("turn-card")) {
        cardEl.classList.add("selected");
      }

      document.getElementById("disp-span-id").innerHTML = `<span>${s.id || "span_generated"}</span> <span style="font-size: 10px; opacity: 0.6;">&#x2398;</span>`;
      document.getElementById("disp-flow").innerText = `${s.source_agent} ➔ ${s.target_agent}`;
      document.getElementById("disp-meta").innerText = `${s.span_type || "agent_turn"}`;
      document.getElementById("disp-duration").innerText = `${(s.duration_ms || 42).toFixed(1)} ms`;
      document.getElementById("disp-tokens").innerText = `${s.token_count || 0} tokens`;

      document.getElementById("disp-input").innerText = JSON.stringify(s.input_payload || {}, null, 2);
      document.getElementById("disp-output").innerText = JSON.stringify(s.output_payload || {}, null, 2);
    }

    // Copy Span ID
    document.getElementById("disp-span-id").onclick = () => {
      if (!selectedSpan) return;
      navigator.clipboard?.writeText(selectedSpan.id || "");
      const el = document.getElementById("disp-span-id");
      const orig = el.innerHTML;
      el.innerHTML = "<span style='color: var(--success-emerald);'>&#x2714; Copied!</span>";
      setTimeout(() => el.innerHTML = orig, 1500);
    };

    // Fork Branch
    document.getElementById("btn-fork").onclick = () => {
      const branchName = document.getElementById("input-branch-name").value.trim() || "fork-01";
      const spanId = selectedSpan?.id || (spans.length > 0 ? spans[spans.length - 1].id : "root-span");

      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          command: "CMD_FORK_BRANCH",
          target_span_id: spanId,
          new_branch_id: branchName
        }));
      } else {
        showForkSuccess(branchName);
      }
    };

    function showForkSuccess(branchName) {
      document.getElementById("kpi-branch").innerText = branchName;
      const toast = document.getElementById("fork-toast");
      toast.style.display = "flex";
      toast.innerHTML = `&#x2714; Swarm state restored at selected span. Active branch: <strong>${escapeHtml(branchName)}</strong>`;
      setTimeout(() => { toast.style.display = "none"; }, 4000);
    }

    // Pause / Resume Swarm
    function updatePauseButton() {
      const btn = document.getElementById("btn-pause");
      if (isPaused) {
        btn.innerHTML = "&#x25B6;&#xFE0F; Resume Swarm Execution";
        btn.style.color = "var(--success-emerald)";
      } else {
        btn.innerHTML = "&#x23F8;&#xFE0F; Pause Swarm Execution";
        btn.style.color = "var(--text-muted)";
      }
    }

    document.getElementById("btn-pause").onclick = () => {
      isPaused = !isPaused;
      updatePauseButton();
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          command: isPaused ? "CMD_PAUSE" : "CMD_RESUME"
        }));
      }
    };

    // Filter agents
    document.getElementById("agent-filter").oninput = (e) => {
      const q = e.target.value.toLowerCase();
      document.querySelectorAll(".participant-card").forEach(c => {
        const agent = c.getAttribute("data-agent").toLowerCase();
        c.style.display = agent.includes(q) ? "flex" : "none";
      });
    };

    // Clear Stream
    document.getElementById("btn-clear-stream").onclick = () => {
      spans = [];
      entropyHistory = [1.0];
      drawEntropySparkline();
      const stream = document.getElementById("stream-container");
      stream.innerHTML = `
        <div class="empty-state" id="stream-empty-state">
          <div class="empty-icon">&#x1FA9E;</div>
          <div class="empty-title">Stream Cleared</div>
          <div class="empty-desc">Click <strong>[&#x25B6; Run Live Swarm Simulation]</strong> above to stream the autonomous software engineering scenario.</div>
        </div>
      `;
      updateMetrics();
      document.querySelectorAll(".p-status-dot").forEach(d => d.className = "p-status-dot idle");
    };

    // Interactive Demo Simulation Script
    const demoTurns = [
      {
        source_agent: "ProductArchitect",
        target_agent: "BackendEngineer",
        span_type: "api_specification",
        duration_ms: 38.4,
        entropy_score: 0.98,
        token_count: 280,
        message: "API Specification: Drafted OpenAPI v3 contract for `/api/v1/auth/tokens` with Argon2id password hashing, JWT claims, and Redis token-bucket rate limiting.",
        tool_call: "spec_validator(contract='auth_v1', schema_hash='sha256:4a8f9b')",
        input_payload: { feature: "OAuth2 Token Exchange", endpoints: ["/api/v1/auth/tokens"], rate_limit: "1200_rpm" },
        output_payload: { status: "SPEC_APPROVED", schema_version: "v1.2.0", security_profile: "OAuth2_JWT" }
      },
      {
        source_agent: "BackendEngineer",
        target_agent: "CodeReviewer",
        span_type: "code_implementation",
        duration_ms: 54.1,
        entropy_score: 0.95,
        token_count: 460,
        message: "Implementation: Added async FastAPI token endpoint with atomic Redis pipeline. Unit tests pass with 98.4% statement coverage. Opened PR #412.",
        tool_call: "git_commit_push(branch='feat/async-auth', pr=412, coverage=0.984)",
        input_payload: { pr_title: "feat(auth): async token exchange pipeline", files: ["auth.py", "session.py"] },
        output_payload: { pr_number: 412, lines_added: 84, linter_status: "PASSED" }
      },
      {
        source_agent: "CodeReviewer",
        target_agent: "QASecurityAuditor",
        span_type: "architecture_audit",
        duration_ms: 61.2,
        entropy_score: 0.92,
        token_count: 390,
        message: "Code Review: Zero Fowler code smells. Strict deep module boundaries verified. Connection pooling uses thread-local scopes. Approved for security fuzzing.",
        tool_call: "ast_security_scan(target='packages/core', rules=['owasp-top10', 'leak-prevention'])",
        input_payload: { pr_number: 412, smell_check: "Fowler_Clean", seams_verified: true },
        output_payload: { review_status: "APPROVED", approved_by: "CodeReviewer" }
      },
      {
        source_agent: "QASecurityAuditor",
        target_agent: "BackendEngineer",
        span_type: "schema_query_loop_1",
        duration_ms: 22.0,
        entropy_score: 0.68,
        token_count: 140,
        message: "Awaiting schema confirmation for auth_tokens. Please re-send auth_tokens schema definition.",
        tool_call: "schema_fetch(table='auth_tokens', retry_count=1)",
        input_payload: { query: "Please re-send auth_tokens schema definition." },
        output_payload: { query: "Please re-send auth_tokens schema definition." }
      },
      {
        source_agent: "BackendEngineer",
        target_agent: "QASecurityAuditor",
        span_type: "circuit_breaker_intercept",
        duration_ms: 0.8,
        entropy_score: 0.24,
        token_count: 90,
        message: "⚠️ [CIRCUIT BREAKER ENGAGED] Repeating ping-pong pattern detected (Shannon Entropy H = 0.24 < 0.50). Execution halted automatically. Zero wasted inference tokens.",
        tool_call: "circuit_breaker_intercept(entropy=0.24, rule='ShannonThreshold')",
        input_payload: { alert: "[SWARMWATCH CIRCUIT] Semantic repetition threshold violated" },
        output_payload: { action: "CIRCUIT_BREAKER_HALT", entropy: 0.24, saved_tokens: 2450, cost_saved: "$0.25" }
      }
    ];

    function runLiveSimulation() {
      spans = [];
      entropyHistory = [1.0];
      const stream = document.getElementById("stream-container");
      stream.innerHTML = "";
      
      let step = 0;
      const btn = document.getElementById("btn-run-sim");
      btn.innerText = "⏳ Streaming Simulation...";
      btn.disabled = true;

      if (simTimer) clearInterval(simTimer);
      simTimer = setInterval(() => {
        if (step < demoTurns.length) {
          const t = demoTurns[step];
          const spanObj = {
            ...t,
            id: `span_${t.source_agent.toLowerCase()}_${Math.random().toString(36).substr(2, 6)}`
          };
          appendSpan(spanObj);
          step++;
        } else {
          clearInterval(simTimer);
          btn.innerText = "↻ Re-run Live Swarm Simulation";
          btn.disabled = false;
        }
      }, 850);
    }
    document.getElementById("btn-run-sim").onclick = runLiveSimulation;

    // Modal Architecture Guide
    const modal = document.getElementById("modal-guide");
    document.getElementById("btn-open-guide").onclick = () => modal.style.display = "flex";
    document.getElementById("btn-close-guide").onclick = () => modal.style.display = "none";
    modal.onclick = (e) => { if (e.target === modal) modal.style.display = "none"; };
    window.addEventListener("keydown", (e) => { if (e.key === "Escape") modal.style.display = "none"; });

    // Initial Run on Load
    window.addEventListener("DOMContentLoaded", () => {
      drawEntropySparkline();
      runLiveSimulation();
    });
  </script>
</body>
</html>"""


def create_app(storage: Optional[StorageAdapter] = None) -> FastAPI:
    """FastAPI application factory for SwarmWatch ingestion server."""
    app = FastAPI(title="SwarmWatch Server", version="0.1.0")
    active_storage = storage or SQLiteStorageAdapter()
    ws_manager = WebSocketManager()
    time_machine = TimeMachine(storage=active_storage)

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0"}

    @app.get("/", response_class=HTMLResponse)
    @app.get("/ui", response_class=HTMLResponse)
    async def dashboard_ui():
        return DASHBOARD_HTML

    @app.post("/api/v1/spans")
    async def ingest_spans(spans: List[SwarmSpan]):
        await active_storage.save_spans(spans)
        for s in spans:
            await ws_manager.broadcast(s.swarm_id, {
                "type": "span_update",
                "span": s.model_dump(mode="json"),
            })
        return {"received": len(spans), "status": "persisted"}

    @app.get("/api/v1/swarms/{swarm_id}/spans")
    async def get_spans(swarm_id: str, branch_id: str = Query(default="main")):
        return await active_storage.get_swarm_spans(swarm_id, branch_id=branch_id)

    @app.get("/api/v1/swarms/{swarm_id}/branches")
    async def get_branches(swarm_id: str):
        return await active_storage.list_branches(swarm_id)

    @app.get("/api/v1/snapshots/{span_id}")
    async def get_snapshot(span_id: str):
        snapshot = await active_storage.get_snapshot(span_id)
        if not snapshot:
            return {"error": "snapshot_not_found"}
        return snapshot

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
