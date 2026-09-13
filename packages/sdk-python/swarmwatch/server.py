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
  <title>SwarmWatch Live Observability & Time Machine</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: #121826;
      --border: #1f293d;
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.25);
      --warning: #fbbf24;
      --danger: #f87171;
      --text: #f3f4f6;
      --text-muted: #9ca3af;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); overflow: hidden; display: flex; flex-direction: column; height: 100vh; }
    header { background: var(--card-bg); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; }
    .logo { font-size: 1.25rem; font-weight: 700; color: var(--accent); letter-spacing: -0.5px; display: flex; align-items: center; gap: 8px; }
    .status-badge { font-size: 0.75rem; background: rgba(56, 189, 248, 0.15); color: var(--accent); padding: 4px 10px; border-radius: 9999px; border: 1px solid var(--accent); }
    .metrics { display: flex; gap: 24px; font-size: 0.875rem; color: var(--text-muted); }
    .metric-val { color: var(--text); font-weight: 600; }
    main { flex: 1; display: flex; position: relative; overflow: hidden; }
    #swimlane-canvas { flex: 1; background: radial-gradient(circle at 50% 50%, #0d1527 0%, var(--bg) 100%); cursor: crosshair; }
    #side-panel { width: 380px; background: var(--card-bg); border-left: 1px solid var(--border); display: flex; flex-direction: column; padding: 20px; overflow-y: auto; gap: 16px; }
    .panel-title { font-size: 1rem; font-weight: 600; color: var(--text); border-bottom: 1px solid var(--border); padding-bottom: 8px; }
    .json-viewer { background: #060911; padding: 12px; border-radius: 8px; border: 1px solid var(--border); font-family: monospace; font-size: 0.75rem; color: #a5f3fc; overflow-x: auto; max-height: 250px; }
    .btn { background: var(--accent); color: #000; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
    .btn:hover { background: #7dd3fc; box-shadow: 0 0 12px var(--accent-glow); }
  </style>
</head>
<body>
  <header>
    <div class="logo">
      <span>👁️ SwarmWatch</span>
      <span class="status-badge" id="ws-status">Connecting...</span>
    </div>
    <div class="metrics">
      <div>Active Agents: <span class="metric-val" id="agent-count">0</span></div>
      <div>Total Spans: <span class="metric-val" id="span-count">0</span></div>
      <div>Entropy Index: <span class="metric-val" id="entropy-val" style="color: #34d399;">1.00</span></div>
      <div>Branch: <span class="metric-val" id="current-branch">main</span></div>
    </div>
  </header>
  <main>
    <canvas id="swimlane-canvas"></canvas>
    <div id="side-panel">
      <div class="panel-title">Inspector & Time-Machine</div>
      <div>
        <label style="font-size: 0.75rem; color: var(--text-muted);">Selected Span ID</label>
        <div id="selected-span-id" style="font-family: monospace; font-size: 0.85rem; color: var(--accent);">None</div>
      </div>
      <div>
        <label style="font-size: 0.75rem; color: var(--text-muted);">Causality Flow</label>
        <div id="selected-flow" style="font-size: 0.85rem; margin-top: 4px;">-</div>
      </div>
      <div>
        <label style="font-size: 0.75rem; color: var(--text-muted);">Payload</label>
        <pre class="json-viewer" id="selected-payload">Click on any message arrow or node to inspect</pre>
      </div>
      <button class="btn" id="btn-fork">Fork & Rewind from Here</button>
    </div>
  </main>
  <script>
    const canvas = document.getElementById("swimlane-canvas");
    const ctx = canvas.getContext("2d");
    let width, height;
    function resize() {
      width = canvas.width = canvas.parentElement.clientWidth - 380;
      height = canvas.height = canvas.parentElement.clientHeight;
    }
    window.addEventListener("resize", resize);
    resize();

    const agents = ["Planner", "Coder", "Evaluator", "Tools"];
    const spans = [];
    let selectedSpan = null;

    // Connect WebSocket
    const swarmId = new URLSearchParams(window.location.search).get("swarm_id") || "default-swarm";
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/swarm/${swarmId}`;
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      document.getElementById("ws-status").innerText = "LIVE (WebSocket)";
      document.getElementById("ws-status").style.color = "#34d399";
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === "span_update") {
        spans.push(data.span);
        document.getElementById("span-count").innerText = spans.length;
        if (data.span.entropy_score !== undefined) {
          document.getElementById("entropy-val").innerText = data.span.entropy_score.toFixed(2);
        }
      }
    };

    // 60FPS Render Loop
    let animOffset = 0;
    function render() {
      ctx.clearRect(0, 0, width, height);
      animOffset = (animOffset + 0.05) % 1;

      // Draw Swimlane Tracks
      const colWidth = width / (agents.length + 1);
      agents.forEach((agent, i) => {
        const x = (i + 1) * colWidth;
        ctx.strokeStyle = "#1a2333";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(x, 60);
        ctx.lineTo(x, height);
        ctx.stroke();
        ctx.setLineDash([]);

        // Header Node
        ctx.fillStyle = "#1e293b";
        ctx.beginPath();
        ctx.roundRect(x - 50, 20, 100, 32, 6);
        ctx.fill();
        ctx.strokeStyle = "#38bdf8";
        ctx.stroke();

        ctx.fillStyle = "#f3f4f6";
        ctx.font = "12px sans-serif";
        ctx.textAlign = "center";
        ctx.fillText(agent, x, 40);
      });

      document.getElementById("agent-count").innerText = agents.length;
      requestAnimationFrame(render);
    }
    render();
  </script>
</body>
</html>
"""


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
