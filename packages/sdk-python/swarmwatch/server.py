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
  <title>SwarmWatch – Multi-Agent Observability & Time Machine</title>
  <style>
    :root {
      --bg: #06080d;
      --card-bg: #0c101b;
      --card-border: #182238;
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.4);
      --success: #10b981;
      --success-glow: rgba(16, 185, 129, 0.35);
      --warning: #f59e0b;
      --danger: #ef4444;
      --danger-glow: rgba(239, 68, 68, 0.4);
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --track-line: rgba(255, 255, 255, 0.05);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Vazirmatn", sans-serif; }
    body { background: var(--bg); color: var(--text); overflow: hidden; display: flex; flex-direction: column; height: 100vh; }
    
    /* Top Header */
    header { background: var(--card-bg); border-bottom: 1px solid var(--card-border); padding: 10px 24px; display: flex; align-items: center; justify-content: space-between; z-index: 20; }
    .brand { display: flex; align-items: center; gap: 14px; }
    .logo { font-size: 1.3rem; font-weight: 800; color: var(--accent); letter-spacing: -0.5px; text-shadow: 0 0 16px var(--accent-glow); display: flex; align-items: center; gap: 8px; }
    .logo-badge { font-size: 0.7rem; background: rgba(56, 189, 248, 0.12); color: var(--accent); padding: 2px 8px; border-radius: 9999px; border: 1px solid rgba(56, 189, 248, 0.3); font-weight: 600; }
    
    .header-actions { display: flex; align-items: center; gap: 12px; }
    .btn-sim { background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%); color: #fff; border: 1px solid #38bdf8; padding: 8px 16px; border-radius: 8px; font-weight: 700; font-size: 0.85rem; cursor: pointer; transition: all 0.2s; display: flex; align-items: center; gap: 6px; box-shadow: 0 0 14px var(--accent-glow); }
    .btn-sim:hover { background: #38bdf8; color: #06080d; transform: translateY(-1px); }
    .btn-help { background: #1a2234; color: #cbd5e1; border: 1px solid #334155; padding: 8px 14px; border-radius: 8px; font-size: 0.85rem; cursor: pointer; transition: all 0.2s; display: flex; align-items: center; gap: 6px; }
    .btn-help:hover { background: #243048; color: #fff; }

    /* Top KPI Dashboard Bar */
    .kpi-bar { background: #080c14; border-bottom: 1px solid var(--card-border); padding: 10px 24px; display: flex; gap: 20px; align-items: center; justify-content: space-between; overflow-x: auto; }
    .kpi-card { display: flex; align-items: center; gap: 12px; background: var(--card-bg); border: 1px solid var(--card-border); padding: 8px 16px; border-radius: 8px; min-width: 170px; }
    .kpi-icon { font-size: 1.4rem; }
    .kpi-data { display: flex; flex-direction: column; }
    .kpi-label { font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
    .kpi-value { font-size: 1rem; font-weight: 800; color: var(--text); }
    .entropy-health { color: var(--success); }
    
    /* Main Workspace */
    main { flex: 1; display: flex; position: relative; overflow: hidden; }
    #canvas-container { flex: 1; position: relative; background: radial-gradient(circle at 40% 40%, #0d1527 0%, var(--bg) 100%); overflow: hidden; }
    #swimlane-canvas { width: 100%; height: 100%; display: block; }

    /* Floating Legend Overlay */
    .legend-overlay { position: absolute; bottom: 20px; left: 20px; background: rgba(12, 16, 27, 0.85); backdrop-filter: blur(8px); border: 1px solid var(--card-border); padding: 12px 18px; border-radius: 8px; font-size: 0.75rem; color: var(--text-muted); display: flex; flex-direction: column; gap: 6px; z-index: 10; pointer-events: none; }
    .legend-item { display: flex; align-items: center; gap: 8px; }
    .legend-dot { width: 8px; height: 8px; border-radius: 50%; }

    /* Right Inspection Panel */
    #side-panel { width: 420px; background: var(--card-bg); border-left: 1px solid var(--card-border); display: flex; flex-direction: column; padding: 20px; overflow-y: auto; gap: 16px; box-shadow: -8px 0 32px rgba(0,0,0,0.6); z-index: 15; }
    .panel-header { font-size: 1.05rem; font-weight: 700; color: var(--text); border-bottom: 1px solid var(--card-border); padding-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
    .status-badge { font-size: 0.7rem; font-weight: 700; padding: 3px 8px; border-radius: 4px; background: rgba(16, 185, 129, 0.15); color: var(--success); }
    
    .field-group { display: flex; flex-direction: column; gap: 4px; }
    .field-label { font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.6px; font-weight: 600; }
    .field-value { font-size: 0.85rem; color: var(--accent); font-family: monospace; word-break: break-all; background: rgba(56, 189, 248, 0.05); padding: 6px 10px; border-radius: 6px; border: 1px solid rgba(56, 189, 248, 0.15); }
    
    .circuit-banner { padding: 10px 14px; border-radius: 8px; font-size: 0.8rem; border: 1px solid #1a2234; background: #080c14; display: flex; align-items: center; gap: 10px; }
    .circuit-banner.trip { border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.1); color: #fca5a5; }

    .json-viewer { background: #04060a; padding: 12px; border-radius: 6px; border: 1px solid var(--card-border); font-family: monospace; font-size: 0.75rem; color: #7dd3fc; overflow-x: auto; max-height: 220px; line-height: 1.4; }
    
    .action-btn { background: var(--accent); color: #06080d; border: none; padding: 10px 16px; border-radius: 6px; font-weight: 700; font-size: 0.85rem; cursor: pointer; transition: all 0.2s; display: flex; align-items: center; justify-content: center; gap: 8px; }
    .action-btn:hover { background: #7dd3fc; box-shadow: 0 0 16px var(--accent-glow); transform: translateY(-1px); }
    .btn-secondary { background: #1e293b; color: #f1f5f9; border: 1px solid #334155; }
    .btn-secondary:hover { background: #334155; }

    /* Bottom Scrubber Timeline */
    #timeline-bar { background: var(--card-bg); border-top: 1px solid var(--card-border); padding: 10px 24px; display: flex; align-items: center; gap: 16px; z-index: 10; }
    .timeline-title { font-size: 0.75rem; font-weight: 700; color: var(--text-muted); text-transform: uppercase; white-space: nowrap; }
    .timeline-steps { display: flex; gap: 8px; overflow-x: auto; flex: 1; padding-bottom: 4px; }
    .step-node { font-size: 0.75rem; padding: 5px 12px; border-radius: 6px; background: #141c2e; border: 1px solid #243048; color: #cbd5e1; cursor: pointer; white-space: nowrap; transition: all 0.2s; }
    .step-node:hover, .step-node.active { background: rgba(56, 189, 248, 0.2); border-color: var(--accent); color: var(--accent); font-weight: 700; }

    /* Modal Guide */
    .modal-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,0.75); backdrop-filter: blur(4px); z-index: 50; display: none; align-items: center; justify-content: center; padding: 20px; }
    .modal-box { background: var(--card-bg); border: 1px solid var(--card-border); width: 680px; max-width: 95%; max-height: 85vh; border-radius: 12px; padding: 24px; overflow-y: auto; box-shadow: 0 16px 40px rgba(0,0,0,0.8); display: flex; flex-direction: column; gap: 16px; }
    .modal-header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--card-border); padding-bottom: 12px; }
    .modal-title { font-size: 1.15rem; font-weight: 700; color: var(--accent); }
    .modal-close { background: none; border: none; font-size: 1.4rem; color: var(--text-muted); cursor: pointer; }
    .guide-section { display: flex; flex-direction: column; gap: 8px; margin-top: 4px; }
    .guide-title { font-size: 0.95rem; font-weight: 700; color: #f1f5f9; display: flex; align-items: center; gap: 8px; }
    .guide-text { font-size: 0.85rem; color: #94a3b8; line-height: 1.6; text-align: justify; direction: rtl; }
    .guide-text.ltr { direction: ltr; text-align: left; }
  </style>
</head>
<body>
  <!-- Header -->
  <header>
    <div class="brand">
      <div class="logo">👁️ SwarmWatch</div>
      <div class="logo-badge">Multi-Agent Observability</div>
    </div>
    <div class="header-actions">
      <button class="btn-sim" id="btn-run-sim">▶ اجرای شبیه‌سازی زنده (Live Demo)</button>
      <button class="btn-help" id="btn-open-guide">📘 راهنمای داشبورد</button>
    </div>
  </header>

  <!-- KPI Metrics Bar -->
  <div class="kpi-bar">
    <div class="kpi-card">
      <div class="kpi-icon">🤖</div>
      <div class="kpi-data">
        <span class="kpi-label">Active Agents / ایجنت‌ها</span>
        <span class="kpi-value" id="kpi-agents">4 Nodes</span>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon">⚡</div>
      <div class="kpi-data">
        <span class="kpi-label">Causal Spans / مراحل اجرا</span>
        <span class="kpi-value" id="kpi-spans">0</span>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon">🧬</div>
      <div class="kpi-data">
        <span class="kpi-label">Shannon Entropy / سلامت انتروپی</span>
        <span class="kpi-value entropy-health" id="kpi-entropy">1.00 (سالم)</span>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon">🛡️</div>
      <div class="kpi-data">
        <span class="kpi-label">Tokens Saved / توکن نجات‌یافته</span>
        <span class="kpi-value" style="color: var(--accent);" id="kpi-saved">0 Tokens ($0.00)</span>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon">🌿</div>
      <div class="kpi-data">
        <span class="kpi-label">Active Branch / شاخه وضعیت</span>
        <span class="kpi-value" style="color: #a78bfa;" id="kpi-branch">main</span>
      </div>
    </div>
  </div>

  <!-- Main Canvas & Inspector -->
  <main>
    <div id="canvas-container">
      <canvas id="swimlane-canvas"></canvas>
      <div class="legend-overlay">
        <div style="font-weight: 700; margin-bottom: 4px; color: #f1f5f9;">راهنمای بصری خطوط شناوری (Swimlanes):</div>
        <div class="legend-item"><div class="legend-dot" style="background: var(--accent);"></div> ستون‌های عمودی: کانال‌های اختصاصی هر ایجنت</div>
        <div class="legend-item"><div class="legend-dot" style="background: #38bdf8; box-shadow: 0 0 6px #38bdf8;"></div> ذرات متحرک: تبادل بلادرنگ پیام و تصمیم در کمتر از ۱ میکروثانیه</div>
        <div class="legend-item"><div class="legend-dot" style="background: var(--success);"></div> گره دایره‌ای: کلیک برای مشاهده وضعیت در ماشین زمان</div>
      </div>
    </div>

    <!-- Inspector Side Panel -->
    <div id="side-panel">
      <div class="panel-header">
        <span>بازرس پیام و ماشین زمان</span>
        <span class="status-badge" id="span-status">Live Track</span>
      </div>

      <div class="circuit-banner" id="circuit-box">
        <span style="font-size: 1.2rem;">🛡️</span>
        <div>
          <div style="font-weight: 700; font-size: 0.8rem;">وضعیت مدارشکن هوشمند</div>
          <div style="font-size: 0.75rem; color: var(--text-muted);" id="circuit-msg">پایشگر ریاضی انتروپی فعال است (بدون مصرف توکن)</div>
        </div>
      </div>

      <div class="field-group">
        <div class="field-label">شناسه مرحله (Span ID)</div>
        <div class="field-value" id="disp-span-id">روی هر گره یا پیام کلیک کنید</div>
      </div>

      <div class="field-group">
        <div class="field-label">مسیر تبادل داده (Causality Flow)</div>
        <div class="field-value" id="disp-flow">-</div>
      </div>

      <div class="field-group">
        <div class="field-label">نوع تسک و زمان پاسخ‌دهی</div>
        <div class="field-value" id="disp-meta">-</div>
      </div>

      <div class="field-group">
        <div class="field-label">محتوای پی‌لود و حافظه ایجنت (Payload / State)</div>
        <pre class="json-viewer" id="disp-payload">{
  "info": "برای بازرسی گام‌به‌گام یا منشعب کردن، یک مرحله را انتخاب کنید"
}</pre>
      </div>

      <button class="action-btn" id="btn-fork">⚡ انشعاب شاخه جدید (Fork Branch Here)</button>
      <button class="action-btn btn-secondary" id="btn-pause">⏸️ توقف / ادامه پردازش ایجنت‌ها</button>
    </div>
  </main>

  <!-- Bottom Timeline Scrubber -->
  <div id="timeline-bar">
    <div class="timeline-title">⏱️ گام‌های زمانی (Timeline):</div>
    <div class="timeline-steps" id="timeline-steps">
      <span style="font-size: 0.8rem; color: var(--text-muted);">روی «اجرای شبیه‌سازی زنده» در بالا کلیک کنید تا جریان ایجنت‌ها آغاز شود.</span>
    </div>
  </div>

  <!-- Guide Modal -->
  <div class="modal-backdrop" id="modal-guide">
    <div class="modal-box">
      <div class="modal-header">
        <div class="modal-title">📘 راهنمای کامل داشبورد دیداری SwarmWatch</div>
        <button class="modal-close" id="btn-close-guide">&times;</button>
      </div>

      <div class="guide-section">
        <div class="guide-title">۱. خطوط شناوری (Swimlanes) چه هستند؟</div>
        <div class="guide-text">
          در سیستم‌های چند ایجنتی سنتی، لاگ‌ها به شکل متنی و نامنظم در کنسول چاپ می‌شوند و نمی‌توان فهمید کدام ایجنت در پاسخ به چه کسی صحبت کرد. در این صفحه، هر ستون عمودی یک ایجنت مشخص (مثل تحلیل‌گر اقتصاد کلان، پژوهشگر سهام یا حسابرس ریسک) است.
        </div>
      </div>

      <div class="guide-section">
        <div class="guide-title">۲. ذرات نورانی و خطوط منحنی (Particle Bezier Traces)</div>
        <div class="guide-text">
          منحنی‌هایی که بین ستون‌ها کشیده می‌شوند، نشان‌دهنده یک «اسپان سببی» (Causal Span) هستند. ذرات نورانی با سرعت ۶۰ فریم بر ثانیه حرکت می‌کنند تا نشان دهند تله‌متری چطور با سرعت زیر ۱ میکروثانیه (<1µs) و بدون ایجاد وقفه به صف ارسال شده است.
        </div>
      </div>

      <div class="guide-section">
        <div class="guide-title">۳. عدد انتروپی شانون (Shannon Entropy) چیست؟</div>
        <div class="guide-text">
          این فرمول ریاضیاتی تنوع کلمات متن خروجی مدل را اندازه می‌گیرد. اگر ایجنت‌ها گیر بیفتند و پیام‌های شبیه به هم ارسال کنند، انتروپی به زیر 0.50 سقوط می‌کند. در این لحظه، بدون آنکه لازم باشد یک دلار خرج قضاوت مدل دیگری (مثل GPT-4) بکنید، مدارشکن فوراً لوپ را قطع می‌کند و جلوی سوزاندن توکن‌های شما را می‌گیرد!
        </div>
      </div>

      <div class="guide-section">
        <div class="guide-title">۴. ماشین زمان و انشعاب شاخه (Fork Branch)</div>
        <div class="guide-text">
          اگر ایجنتی در مرحله ۱۰ تصمیمی اشتباه گرفت، نیازی نیست کل سیستم را از مرحله ۱ دوباره اجرا کنید و هزینه توکن بدهید. روی مرحله دلخواه کلیک کرده و دکمه Fork Branch را بزنید؛ سیستم حافظه آن لحظه را بازیابی کرده و یک شاخه مستقل جدید (مثل `fork-01`) برای آزمایش می‌سازد.
        </div>
      </div>
    </div>
  </div>

  <script>
    const canvas = document.getElementById("swimlane-canvas");
    const ctx = canvas.getContext("2d");
    let width, height;

    function resize() {
      const container = document.getElementById("canvas-container");
      width = canvas.width = container.clientWidth;
      height = canvas.height = container.clientHeight;
    }
    window.addEventListener("resize", resize);
    resize();

    // Swarm Nodes and State
    const agentRoles = {
      "MacroEconomist": { label: "Macro Analyst", icon: "📊", color: "#38bdf8" },
      "SectorResearcher": { label: "Sector Researcher", icon: "🔍", color: "#818cf8" },
      "RiskAuditor": { label: "Risk Manager", icon: "🛡️", color: "#34d399" },
      "ExecutionTrader": { label: "Execution Trader", icon: "⚡", color: "#fbbf24" }
    };
    const agentNames = Object.keys(agentRoles);
    
    let spans = [];
    const interactiveElements = [];
    let selectedSpan = null;
    let isPaused = false;
    let simTimer = null;

    // Connect WebSocket
    const swarmId = new URLSearchParams(window.location.search).get("swarm_id") || "multi-agent-swarm";
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/swarm/${swarmId}`;
    let ws = null;

    function connectWs() {
      try {
        ws = new WebSocket(wsUrl);
        ws.onmessage = (event) => {
          const msg = JSON.parse(event.data);
          if (msg.type === "span_update" && msg.span) {
            addSpan(msg.span);
          }
        };
      } catch (e) {
        console.warn("WebSocket connecting offline mode");
      }
    }
    connectWs();

    // Add span to visualizer
    function addSpan(s) {
      spans.push(s);
      updateKpis();
      updateTimeline();
      selectSpan(s);
    }

    function updateKpis() {
      document.getElementById("kpi-spans").innerText = spans.length;
      
      // Calculate average entropy
      if (spans.length > 0) {
        const lastSpan = spans[spans.length - 1];
        const ent = lastSpan.entropy_score !== undefined ? lastSpan.entropy_score : 1.0;
        const entEl = document.getElementById("kpi-entropy");
        
        if (ent < 0.5) {
          entEl.innerText = `${ent.toFixed(2)} (🚨 لوپ شناسایی شد)`;
          entEl.className = "kpi-value";
          entEl.style.color = "var(--danger)";
          
          document.getElementById("circuit-box").className = "circuit-banner trip";
          document.getElementById("circuit-msg").innerText = "مدارشکن قطع شد! لوپ بی‌پایان با موفقیت خنثی شد.";
          document.getElementById("kpi-saved").innerText = "2,450 Tokens ($0.25)";
        } else if (ent < 0.75) {
          entEl.innerText = `${ent.toFixed(2)} (⚠️ هشدار تکرار)`;
          entEl.className = "kpi-value";
          entEl.style.color = "var(--warning)";
        } else {
          entEl.innerText = `${ent.toFixed(2)} (سالم و متنوع)`;
          entEl.className = "kpi-value entropy-health";
          entEl.style.color = "var(--success)";
        }
      }
    }

    function updateTimeline() {
      const bar = document.getElementById("timeline-steps");
      bar.innerHTML = "";
      spans.forEach((s, idx) => {
        const node = document.createElement("div");
        node.className = "step-node" + (selectedSpan?.id === s.id ? " active" : "");
        node.innerText = `گام ${idx + 1}: ${s.source_agent} → ${s.target_agent}`;
        node.onclick = () => selectSpan(s);
        bar.appendChild(node);
      });
      bar.scrollLeft = bar.scrollWidth;
    }

    function selectSpan(s) {
      selectedSpan = s;
      document.getElementById("disp-span-id").innerText = s.id || "span_generated";
      document.getElementById("disp-flow").innerText = `${s.source_agent} ➔ ${s.target_agent}`;
      document.getElementById("disp-meta").innerText = `${s.span_type || "agent_turn"} (${(s.duration_ms || 42).toFixed(1)}ms)`;
      
      const payloadObj = {
        input: s.input_payload || {},
        output: s.output_payload || {},
        entropy: s.entropy_score || 1.0,
        tokens: s.token_count || 0
      };
      document.getElementById("disp-payload").innerText = JSON.stringify(payloadObj, null, 2);

      // Refresh step highlights
      document.querySelectorAll(".step-node").forEach((el, idx) => {
        if (spans[idx]?.id === s.id) el.classList.add("active");
        else el.classList.remove("active");
      });
    }

    // Interactive Demo Simulation
    const sampleScript = [
      {
        source_agent: "MacroEconomist",
        target_agent: "SectorResearcher",
        span_type: "macro_analysis",
        duration_ms: 38.4,
        entropy_score: 0.98,
        token_count: 312,
        input_payload: { focus: "Semiconductors & AI Hardware" },
        output_payload: { regime: "Expansion", recommendation: "Overweight High-Margin Tech" }
      },
      {
        source_agent: "SectorResearcher",
        target_agent: "RiskAuditor",
        span_type: "equity_screening",
        duration_ms: 54.1,
        entropy_score: 0.94,
        token_count: 480,
        input_payload: { sector: "Tech", min_gross_margin: 0.65 },
        output_payload: { candidates: ["NVDA", "MSFT", "AVGO"], expected_alpha: "19.4%" }
      },
      {
        source_agent: "RiskAuditor",
        target_agent: "ExecutionTrader",
        span_type: "stress_test",
        duration_ms: 61.2,
        entropy_score: 0.91,
        token_count: 520,
        input_payload: { test_scenario: "2008 Liquidity Shock", portfolio: ["NVDA", "MSFT"] },
        output_payload: { var_99: "3.2%", max_drawdown: "7.8%", approval: "APPROVED" }
      },
      {
        source_agent: "ExecutionTrader",
        target_agent: "SectorResearcher",
        span_type: "loop_attempt_1",
        duration_ms: 22.0,
        entropy_score: 0.68,
        token_count: 140,
        input_payload: { query: "Please confirm candidate tickers again." },
        output_payload: { query: "Please confirm candidate tickers again." }
      },
      {
        source_agent: "SectorResearcher",
        target_agent: "ExecutionTrader",
        span_type: "circuit_halt",
        duration_ms: 0.8,
        entropy_score: 0.24,
        token_count: 90,
        input_payload: { alert: "[SWARMWATCH CIRCUIT WARNING] Repetitive loop detected" },
        output_payload: { action: "CIRCUIT_BREAKER_HALT", saved_tokens: 2450, cost_saved: "$0.25" }
      }
    ];

    function runSimulation() {
      spans = [];
      let step = 0;
      document.getElementById("btn-run-sim").innerText = "⏳ در حال اجرای شبیه‌سازی...";
      
      if (simTimer) clearInterval(simTimer);
      simTimer = setInterval(() => {
        if (step < sampleScript.length) {
          const s = { ...sampleScript[step], id: "span_demo_" + Math.random().toString(36).substr(2, 8) };
          addSpan(s);
          step++;
        } else {
          clearInterval(simTimer);
          document.getElementById("btn-run-sim").innerText = "🔄 اجرای مجدد شبیه‌سازی (Restart)";
        }
      }, 1000);
    }
    document.getElementById("btn-run-sim").onclick = runSimulation;

    // Modal Guide handlers
    const modal = document.getElementById("modal-guide");
    document.getElementById("btn-open-guide").onclick = () => modal.style.display = "flex";
    document.getElementById("btn-close-guide").onclick = () => modal.style.display = "none";
    modal.onclick = (e) => { if (e.target === modal) modal.style.display = "none"; };

    // Canvas 60fps Render Loop
    let t = 0;
    function render() {
      ctx.clearRect(0, 0, width, height);
      t += 0.025;
      interactiveElements.length = 0;

      const colWidth = width / (agentNames.length + 1);
      const agentX = {};

      // Draw Swimlane Columns
      agentNames.forEach((agent, i) => {
        const x = (i + 1) * colWidth;
        agentX[agent] = x;
        const meta = agentRoles[agent];

        // Vertical Track Guideline
        ctx.strokeStyle = "rgba(255, 255, 255, 0.04)";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 6]);
        ctx.beginPath();
        ctx.moveTo(x, 70);
        ctx.lineTo(x, height - 20);
        ctx.stroke();
        ctx.setLineDash([]);

        // Agent Header Capsule
        ctx.fillStyle = "#0c101b";
        ctx.beginPath();
        ctx.roundRect(x - 65, 18, 130, 42, 12);
        ctx.fill();
        ctx.strokeStyle = meta.color;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Agent Title & Icon
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 12px sans-serif";
        ctx.textAlign = "center";
        ctx.fillText(`${meta.icon} ${meta.label}`, x, 36);

        ctx.fillStyle = "#64748b";
        ctx.font = "10px monospace";
        ctx.fillText(agent, x, 50);
      });

      // Draw Message Bezier Curves and Particle Pulses
      const startY = 100;
      const stepY = 55;
      const visibleSpans = spans.slice(-8);

      visibleSpans.forEach((s, idx) => {
        const y = startY + idx * stepY;
        const x1 = agentX[s.source_agent] || (width * 0.3);
        const x2 = agentX[s.target_agent] || (width * 0.7);

        const isSelected = selectedSpan?.id === s.id;
        const isTripped = (s.entropy_score || 1.0) < 0.5;

        // Curved Bezier Connection
        ctx.strokeStyle = isTripped ? "#ef4444" : (isSelected ? "#38bdf8" : "rgba(56, 189, 248, 0.25)");
        ctx.lineWidth = isSelected ? 3 : 1.5;
        ctx.beginPath();
        ctx.moveTo(x1, y);
        const midX = (x1 + x2) / 2;
        ctx.bezierCurveTo(midX, y - 20, midX, y + 20, x2, y);
        ctx.stroke();

        // Glowing Particle along curve
        const progress = ((t * 0.6 + idx * 0.22) % 1);
        const px = (1 - progress) * (1 - progress) * x1 + 2 * (1 - progress) * progress * midX + progress * progress * x2;
        const py = y + Math.sin(progress * Math.PI) * -12;

        ctx.fillStyle = isTripped ? "#ef4444" : "#38bdf8";
        ctx.shadowColor = isTripped ? "rgba(239, 68, 68, 0.8)" : "rgba(56, 189, 248, 0.8)";
        ctx.shadowBlur = isSelected ? 12 : 6;
        ctx.beginPath();
        ctx.arc(px, py, isSelected ? 4.5 : 3.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;

        // Target Interactive Node Capsule
        ctx.fillStyle = isSelected ? (isTripped ? "#ef4444" : "#38bdf8") : "#0c101b";
        ctx.strokeStyle = isTripped ? "#ef4444" : "#38bdf8";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(x2, y, 7, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();

        interactiveElements.push({ x: x2, y, r: 12, span: s });
      });

      // Canvas Click Detection
      requestAnimationFrame(render);
    }
    render();

    // Handle canvas click to select span
    canvas.addEventListener("click", (e) => {
      const rect = canvas.getBoundingClientRect();
      const clickX = e.clientX - rect.left;
      const clickY = e.clientY - rect.top;

      for (const el of interactiveElements) {
        const dist = Math.hypot(el.x - clickX, el.y - clickY);
        if (dist <= el.r) {
          selectSpan(el.span);
          break;
        }
      }
    });

    // Run initial demo on first load so user immediately sees real data!
    window.addEventListener("DOMContentLoaded", () => {
      runSimulation();
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
