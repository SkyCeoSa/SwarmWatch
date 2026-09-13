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
      --bg: #07090e;
      --card-bg: #0e131f;
      --border: #1a2234;
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.35);
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --text: #f8fafc;
      --text-muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; }
    body { background: var(--bg); color: var(--text); overflow: hidden; display: flex; flex-direction: column; height: 100vh; }
    header { background: var(--card-bg); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; }
    .brand { display: flex; align-items: center; gap: 12px; }
    .logo { font-size: 1.25rem; font-weight: 800; color: var(--accent); letter-spacing: -0.5px; text-shadow: 0 0 16px var(--accent-glow); }
    .badge { font-size: 0.75rem; background: rgba(56, 189, 248, 0.12); color: var(--accent); padding: 3px 10px; border-radius: 9999px; border: 1px solid rgba(56, 189, 248, 0.3); }
    .metrics { display: flex; gap: 28px; font-size: 0.85rem; color: var(--text-muted); align-items: center; }
    .metric-val { color: var(--text); font-weight: 700; margin-left: 4px; }
    .entropy-pill { padding: 2px 8px; border-radius: 4px; font-weight: bold; background: rgba(16, 185, 129, 0.15); color: var(--success); }
    main { flex: 1; display: flex; position: relative; overflow: hidden; }
    #swimlane-canvas { flex: 1; background: radial-gradient(circle at 40% 40%, #0d1527 0%, var(--bg) 100%); cursor: default; }
    #side-panel { width: 400px; background: var(--card-bg); border-left: 1px solid var(--border); display: flex; flex-direction: column; padding: 20px; overflow-y: auto; gap: 16px; box-shadow: -8px 0 24px rgba(0,0,0,0.5); }
    .panel-title { font-size: 1rem; font-weight: 700; color: var(--text); border-bottom: 1px solid var(--border); padding-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
    .field-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
    .field-value { font-size: 0.85rem; color: var(--accent); font-family: monospace; word-break: break-all; }
    .json-viewer { background: #05070c; padding: 12px; border-radius: 6px; border: 1px solid var(--border); font-family: monospace; font-size: 0.75rem; color: #a5f3fc; overflow-x: auto; max-height: 240px; }
    .btn { background: var(--accent); color: #07090e; border: none; padding: 10px 16px; border-radius: 6px; font-weight: 700; font-size: 0.85rem; cursor: pointer; transition: all 0.2s; display: flex; align-items: center; justify-content: center; gap: 8px; }
    .btn:hover { background: #7dd3fc; box-shadow: 0 0 16px var(--accent-glow); transform: translateY(-1px); }
    .btn-danger { background: #331518; color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.4); }
    .btn-danger:hover { background: var(--danger); color: #fff; box-shadow: 0 0 16px rgba(239, 68, 68, 0.4); }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div class="logo">👁️ SwarmWatch</div>
      <div class="badge" id="ws-status">Connecting...</div>
    </div>
    <div class="metrics">
      <div>Agents: <span class="metric-val" id="agent-count">0</span></div>
      <div>Spans: <span class="metric-val" id="span-count">0</span></div>
      <div>Entropy Health: <span class="entropy-pill" id="entropy-val">1.00</span></div>
      <div>Active Branch: <span class="metric-val" style="color: var(--accent);" id="current-branch">main</span></div>
    </div>
  </header>
  <main>
    <canvas id="swimlane-canvas"></canvas>
    <div id="side-panel">
      <div class="panel-title">
        <span>Inspector & Time-Machine</span>
        <span style="font-size: 0.75rem; font-weight: normal; color: var(--text-muted);" id="span-status-badge">Live</span>
      </div>
      <div>
        <div class="field-label">Selected Span ID</div>
        <div class="field-value" id="selected-span-id">Click any span node or message arrow</div>
      </div>
      <div>
        <div class="field-label">Causality Flow</div>
        <div class="field-value" id="selected-flow">-</div>
      </div>
      <div>
        <div class="field-label">Type & Duration</div>
        <div class="field-value" id="selected-meta">-</div>
      </div>
      <div>
        <div class="field-label">Input / Output Payloads</div>
        <pre class="json-viewer" id="selected-payload">{\n  "status": "Ready for live inspection"\n}</pre>
      </div>
      <button class="btn" id="btn-fork">⚡ Fork Execution Branch Here</button>
      <button class="btn btn-danger" id="btn-pause">⏸️ Pause Swarm</button>
    </div>
  </main>
  <script>
    const canvas = document.getElementById("swimlane-canvas");
    const ctx = canvas.getContext("2d");
    let width, height;

    function resize() {
      width = canvas.width = canvas.parentElement.clientWidth - 400;
      height = canvas.height = canvas.parentElement.clientHeight;
    }
    window.addEventListener("resize", resize);
    resize();

    const agents = new Set(["Director", "Planner", "Coder", "Reviewer"]);
    const spans = [];
    const interactiveElements = [];
    let selectedSpan = null;
    let isPaused = false;

    // Connect WebSocket
    const swarmId = new URLSearchParams(window.location.search).get("swarm_id") || "default-swarm";
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/swarm/${swarmId}`;
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      document.getElementById("ws-status").innerText = "LIVE (WebSocket 10ms)";
      document.getElementById("ws-status").style.borderColor = "#10b981";
      document.getElementById("ws-status").style.color = "#10b981";
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === "span_update") {
        const s = data.span;
        spans.push(s);
        if (s.source_agent) agents.add(s.source_agent);
        if (s.target_agent) agents.add(s.target_agent);
        document.getElementById("span-count").innerText = spans.length;
        document.getElementById("agent-count").innerText = agents.size;

        if (s.entropy_score !== undefined) {
          const pill = document.getElementById("entropy-val");
          pill.innerText = s.entropy_score.toFixed(2);
          if (s.entropy_score < 0.4) {
            pill.style.background = "rgba(239, 68, 68, 0.2)";
            pill.style.color = "#ef4444";
          } else if (s.entropy_score < 0.7) {
            pill.style.background = "rgba(245, 158, 11, 0.2)";
            pill.style.color = "#f59e0b";
          } else {
            pill.style.background = "rgba(16, 185, 129, 0.2)";
            pill.style.color = "#10b981";
          }
        }
      } else if (data.type === "command_ack") {
        if (data.command === "CMD_PAUSE") {
          isPaused = true;
          document.getElementById("btn-pause").innerText = "▶️ Resume Swarm";
        } else if (data.command === "CMD_RESUME") {
          isPaused = false;
          document.getElementById("btn-pause").innerText = "⏸️ Pause Swarm";
        } else if (data.command === "CMD_FORK_BRANCH") {
          document.getElementById("current-branch").innerText = data.new_branch_id;
          alert(`Branch successfully forked: ${data.new_branch_id}`);
        }
      }
    };

    document.getElementById("btn-pause").onclick = () => {
      const cmd = isPaused ? "CMD_RESUME" : "CMD_PAUSE";
      ws.send(JSON.stringify({ command: cmd }));
    };

    document.getElementById("btn-fork").onclick = () => {
      if (!selectedSpan) {
        alert("Please select a span first to fork from.");
        return;
      }
      ws.send(JSON.stringify({
        command: "CMD_FORK_BRANCH",
        target_span_id: selectedSpan.id,
        new_branch_id: `fork-${Math.floor(Math.random() * 1000)}`
      }));
    };

    // Canvas Click Interaction
    canvas.addEventListener("click", (e) => {
      const rect = canvas.getBoundingClientRect();
      const clickX = e.clientX - rect.left;
      const clickY = e.clientY - rect.top;

      for (const el of interactiveElements) {
        if (Math.hypot(clickX - el.x, clickY - el.y) < el.r + 8) {
          selectSpan(el.span);
          return;
        }
      }
    });

    function selectSpan(s) {
      selectedSpan = s;
      document.getElementById("selected-span-id").innerText = s.id;
      document.getElementById("selected-flow").innerText = `${s.source_agent} → ${s.target_agent}`;
      document.getElementById("selected-meta").innerText = `${s.span_type} | ${s.duration_ms.toFixed(1)}ms | Entropy: ${(s.entropy_score || 1).toFixed(2)}`;
      document.getElementById("selected-payload").innerText = JSON.stringify({
        inputs: s.input_payload,
        outputs: s.output_payload,
      }, null, 2);
    }

    // 60FPS Render Loop with Amir-Minimal Aesthetics
    let t = 0;
    function render() {
      ctx.clearRect(0, 0, width, height);
      t += 0.02;
      interactiveElements.length = 0;

      const agentList = Array.from(agents);
      const colWidth = width / (agentList.length + 1);
      const agentX = {};

      // Draw Swimlane Tracks & Glowing Headers
      agentList.forEach((agent, i) => {
        const x = (i + 1) * colWidth;
        agentX[agent] = x;

        // Vertical Track Guideline
        ctx.strokeStyle = "rgba(26, 34, 52, 0.7)";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 6]);
        ctx.beginPath();
        ctx.moveTo(x, 60);
        ctx.lineTo(x, height);
        ctx.stroke();
        ctx.setLineDash([]);

        // Agent Header Capsule
        ctx.fillStyle = "#0e131f";
        ctx.beginPath();
        ctx.roundRect(x - 60, 18, 120, 36, 18);
        ctx.fill();
        ctx.strokeStyle = "rgba(56, 189, 248, 0.4)";
        ctx.lineWidth = 1.5;
        ctx.stroke();

        ctx.fillStyle = "#f8fafc";
        ctx.font = "600 12px monospace";
        ctx.textAlign = "center";
        ctx.fillText(agent, x, 40);
      });

      // Draw Message Flow Nodes and Bezier Arrows
      const startY = 90;
      const stepY = 55;
      const visibleSpans = spans.slice(-12);

      visibleSpans.forEach((s, idx) => {
        const y = startY + idx * stepY;
        const x1 = agentX[s.source_agent] || (width / 2);
        const x2 = agentX[s.target_agent] || (x1 + 80);

        // Curved Bezier Connection
        ctx.strokeStyle = s.id === selectedSpan?.id ? "#38bdf8" : "rgba(56, 189, 248, 0.3)";
        ctx.lineWidth = s.id === selectedSpan?.id ? 2.5 : 1.5;
        ctx.beginPath();
        ctx.moveTo(x1, y);
        const midX = (x1 + x2) / 2;
        ctx.bezierCurveTo(midX, y - 15, midX, y + 15, x2, y);
        ctx.stroke();

        // Pulsing Particle along curve
        const progress = ((t * 0.5 + idx * 0.25) % 1);
        const px = (1 - progress) * (1 - progress) * x1 + 2 * (1 - progress) * progress * midX + progress * progress * x2;
        const py = y + Math.sin(progress * Math.PI) * -10;

        ctx.fillStyle = "#38bdf8";
        ctx.shadowColor = "rgba(56, 189, 248, 0.8)";
        ctx.shadowBlur = 8;
        ctx.beginPath();
        ctx.arc(px, py, 3, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;

        // Interactive Target Node
        ctx.fillStyle = s.id === selectedSpan?.id ? "#38bdf8" : "#0e131f";
        ctx.strokeStyle = "#38bdf8";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(x2, y, 6, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();

        interactiveElements.push({ x: x2, y, r: 8, span: s });
      });

      requestAnimationFrame(render);
    }
    render();
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
