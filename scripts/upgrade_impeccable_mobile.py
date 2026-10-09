import re
import os
import subprocess

html_path = "dashboard.html"
with open(html_path, "r", encoding="utf-8") as f:
    content = f.read()

# ==============================================================================
# 1. ENHANCE DESIGN TOKENS IN :root (IMPECCABLE CONTRAST & VISIBILITY)
# ==============================================================================
old_root = """:root {
      --bg-canvas: #080B11;
      --bg-surface: #0E1422;
      --bg-surface-hover: #141C2E;
      --bg-surface-active: #1C2740;
      --border-subtle: #1D2840;
      --border-strong: #2D3E63;"""

new_root = """:root {
      --bg-canvas: #080B11;
      --bg-surface: #0F1728;
      --bg-surface-hover: #16233B;
      --bg-surface-active: #1E3050;
      --border-subtle: #243552;
      --border-strong: #3A537D;"""

assert old_root in content, "Root tokens not found"
content = content.replace(old_root, new_root, 1)

# ==============================================================================
# 2. ENHANCE BASE STYLES & CONTRAST
# ==============================================================================
# Enhance .btn-secondary for solid tactile presence
old_btn_sec = """.btn-secondary {
      background: var(--bg-canvas);
      color: var(--text-secondary);
      border: 1px solid var(--border-subtle);
    }"""
new_btn_sec = """.btn-secondary {
      background: #141E30;
      color: var(--text-primary);
      border: 1px solid var(--border-subtle);
    }"""
if old_btn_sec in content:
    content = content.replace(old_btn_sec, new_btn_sec, 1)

# Enhance metric cells with semantic top accents & visible crisp borders
old_metric_cell = """.metric-cell {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 10px 14px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }"""
new_metric_cell = """.metric-cell {
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 10px 14px;
      display: flex;
      flex-direction: column;
      gap: 6px;
      position: relative;
    }
    .metric-cell:nth-child(1) { border-top: 2px solid var(--accent); }
    .metric-cell:nth-child(2) { border-top: 2px solid var(--success); }
    .metric-cell:nth-child(3) { border-top: 2px solid var(--accent-purple); }
    .metric-cell:nth-child(4) { border-top: 2px solid var(--warning); }"""
if old_metric_cell in content:
    content = content.replace(old_metric_cell, new_metric_cell, 1)

# ==============================================================================
# 3. REPLACE MOBILE CSS WITH IMPECCABLE MOBILE ARCHITECTURE
# ==============================================================================
# Find the start of mobile CSS
mobile_start_idx = content.find("/* ==========================================================================\n       Mobile & Tablet Responsive UI/UX Architecture")
if mobile_start_idx == -1:
    mobile_start_idx = content.find("/* ==========================================================================\r\n       Mobile & Tablet Responsive UI/UX Architecture")

mobile_end_idx = content.find("</style>", mobile_start_idx)
assert mobile_start_idx != -1 and mobile_end_idx != -1, "Mobile CSS markers not found"

impeccable_mobile_css = """/* ==========================================================================
       Impeccable Mobile Architecture (Single-Scroll, Zero-Overflow, High Contrast)
       ========================================================================== */
    .mobile-nav-bar {
      display: none;
    }
    .mobile-close-btn {
      display: none;
    }
    .mobile-tool-ribbon {
      display: none;
    }
    .mobile-drawer-modal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(8, 11, 17, 0.88);
      z-index: 1100;
      align-items: flex-end;
      justify-content: center;
      backdrop-filter: blur(2px);
    }
    .mobile-drawer-sheet {
      background: var(--bg-surface);
      border-top: 1px solid var(--border-strong);
      border-radius: 8px 8px 0 0;
      width: 100%;
      max-width: 600px;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
      max-height: 80vh;
      overflow-y: auto;
    }
    .mobile-quick-action-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .mobile-quick-action-btn {
      display: flex;
      align-items: center;
      gap: 8px;
      background: #141E30;
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 12px 14px;
      color: var(--text-primary);
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      text-align: left;
      transition: background 0.15s, border-color 0.15s;
      min-height: 44px;
    }
    .mobile-quick-action-btn:hover {
      background: var(--bg-surface-hover);
      border-color: var(--accent);
    }

    @media (max-width: 900px) {
      /* 1. Global Document Natural Scrolling & Overflow Protection */
      html {
        height: auto !important;
        min-height: 100% !important;
        overflow-x: hidden !important;
        width: 100% !important;
        max-width: 100vw !important;
      }
      body {
        height: auto !important;
        min-height: 100dvh !important;
        overflow-y: auto !important;
        overflow-x: hidden !important;
        width: 100% !important;
        max-width: 100vw !important;
        padding-bottom: calc(64px + env(safe-area-inset-bottom, 0px)) !important;
        -webkit-overflow-scrolling: touch !important;
      }

      *, *::before, *::after {
        box-sizing: border-box !important;
      }

      /* 2. Sticky Mobile Header (Single Clean Row) */
      header {
        position: sticky !important;
        top: 0 !important;
        z-index: 800 !important;
        padding: 8px 12px !important;
        background: #0F1728 !important;
        border-bottom: 1px solid var(--border-subtle) !important;
        display: flex !important;
        flex-direction: column !important;
        gap: 6px !important;
        width: 100% !important;
        max-width: 100vw !important;
      }
      .brand-group {
        gap: 8px !important;
      }
      .brand-title {
        font-size: 14px !important;
      }
      .brand-icon {
        width: 18px !important;
        height: 18px !important;
      }
      .brand-version {
        display: none !important;
      }

      /* Header actions on mobile: Show only Run Sim & Reset */
      .header-actions {
        gap: 6px !important;
        order: 2 !important;
        margin-left: auto !important;
      }
      .header-actions #btnFleetToggle,
      .header-actions #btnApiKeys,
      .header-actions #btnMfaModal {
        display: none !important;
      }
      .header-actions #btnSimulate {
        height: 32px !important;
        padding: 0 12px !important;
        font-size: 11px !important;
        font-weight: 700 !important;
      }
      .header-actions #btnReset {
        height: 32px !important;
        padding: 0 10px !important;
        font-size: 11px !important;
      }

      /* Header Sub-bar: Status & Project */
      .header-center {
        width: 100% !important;
        max-width: 100% !important;
        order: 3 !important;
        padding-top: 4px !important;
        border-top: 1px solid var(--border-subtle) !important;
        display: flex !important;
        align-items: center !important;
        gap: 6px !important;
        overflow-x: auto !important;
        scrollbar-width: none !important;
        white-space: nowrap !important;
      }
      .header-center::-webkit-scrollbar { display: none; }
      .branch-tag {
        max-width: 180px !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        white-space: nowrap !important;
        padding: 2px 6px !important;
        font-size: 11px !important;
      }
      .branch-tag select {
        max-width: 110px !important;
        font-size: 11px !important;
      }

      /* 3. Metrics Bar (2-Column Grid with High-Contrast Hairline Borders) */
      .metrics-bar {
        grid-template-columns: repeat(2, 1fr) !important;
        gap: 8px !important;
        padding: 8px 12px !important;
        width: 100% !important;
        max-width: 100vw !important;
      }
      .metric-cell {
        padding: 8px 10px !important;
        gap: 4px !important;
        background: #0F1728 !important;
        border: 1px solid var(--border-subtle) !important;
      }
      .metric-title {
        font-size: 11px !important;
      }
      .metric-value {
        font-size: 16px !important;
      }
      .metric-subtext {
        font-size: 11px !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
      }

      /* 4. Natural Unified Workspace Flow */
      .workspace {
        flex: none !important;
        display: block !important;
        overflow: visible !important;
        width: 100% !important;
        max-width: 100vw !important;
      }
      .stream-panel {
        flex: none !important;
        display: flex !important;
        flex-direction: column !important;
        overflow: visible !important;
        width: 100% !important;
        max-width: 100vw !important;
        min-height: auto !important;
      }
      .stream-feed {
        flex: none !important;
        overflow: visible !important;
        height: auto !important;
        padding: 8px 12px !important;
        width: 100% !important;
        max-width: 100vw !important;
      }

      /* 5. Mobile Tool Ribbon (Accordion Controls for Secondary Tools) */
      .mobile-tool-ribbon {
        display: flex !important;
        align-items: center !important;
        gap: 6px !important;
        padding: 6px 12px !important;
        background: var(--bg-surface) !important;
        border-bottom: 1px solid var(--border-subtle) !important;
        width: 100% !important;
        max-width: 100vw !important;
        overflow-x: auto !important;
        scrollbar-width: none !important;
        white-space: nowrap !important;
      }
      .mobile-tool-ribbon::-webkit-scrollbar { display: none; }
      .mobile-tool-btn {
        display: inline-flex !important;
        align-items: center !important;
        gap: 5px !important;
        padding: 5px 10px !important;
        border-radius: 4px !important;
        background: #141E30 !important;
        border: 1px solid var(--border-subtle) !important;
        color: var(--text-secondary) !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        cursor: pointer !important;
        white-space: nowrap !important;
        touch-action: manipulation !important;
      }
      .mobile-tool-btn.active {
        background: var(--bg-surface-active) !important;
        border-color: var(--accent) !important;
        color: var(--text-primary) !important;
      }

      /* Secondary toolbars collapsed on mobile by default to free 200px+ space */
      .chaos-bar {
        display: none !important;
        width: 100% !important;
        max-width: 100vw !important;
        overflow-x: auto !important;
        padding: 8px 12px !important;
        white-space: nowrap !important;
        -webkit-overflow-scrolling: touch !important;
        scrollbar-width: none !important;
        background: #0B101C !important;
      }
      .chaos-bar::-webkit-scrollbar { display: none; }
      .chaos-bar.mobile-open {
        display: flex !important;
      }
      .chaos-group {
        display: flex !important;
        align-items: center !important;
        gap: 6px !important;
        flex-wrap: nowrap !important;
      }
      .chaos-btn {
        padding: 6px 10px !important;
        font-size: 11px !important;
        white-space: nowrap !important;
        flex-shrink: 0 !important;
        background: #141E30 !important;
        border: 1px solid var(--border-subtle) !important;
      }

      .time-machine-bar {
        display: none !important;
        width: 100% !important;
        max-width: 100vw !important;
        flex-direction: column !important;
        padding: 8px 12px !important;
        gap: 8px !important;
        background: #0B101C !important;
      }
      .time-machine-bar.mobile-open {
        display: flex !important;
      }
      .tm-left-group {
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
        justify-content: space-between !important;
        gap: 6px !important;
      }
      .tm-left-group .tm-badge {
        display: none !important;
      }
      .tm-btn {
        padding: 5px 10px !important;
        font-size: 11px !important;
        background: #141E30 !important;
        border: 1px solid var(--border-subtle) !important;
      }
      .tm-turn-display {
        font-size: 11px !important;
        min-width: 75px !important;
        padding: 4px 6px !important;
        background: #0F1728 !important;
        border: 1px solid var(--border-subtle) !important;
      }
      .tm-center-track {
        width: 100% !important;
        max-width: 100% !important;
        min-height: 24px !important;
      }
      .tm-right-group {
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
        justify-content: space-between !important;
        gap: 6px !important;
      }

      .fleet-bar {
        display: none !important;
        width: 100% !important;
        max-width: 100vw !important;
        overflow-x: auto !important;
        padding: 8px 12px !important;
        white-space: nowrap !important;
        -webkit-overflow-scrolling: touch !important;
        scrollbar-width: none !important;
        background: #0B101C !important;
      }
      .fleet-bar::-webkit-scrollbar { display: none; }
      .fleet-bar.mobile-open {
        display: flex !important;
      }
      .fleet-meters-container {
        display: none !important;
      }

      /* Stream Panel Header */
      .panel-header {
        padding: 8px 12px !important;
        gap: 8px !important;
        flex-wrap: wrap !important;
      }
      .panel-title {
        font-size: 12px !important;
      }
      .panel-count {
        font-size: 11px !important;
      }

      /* Turn Cards in Stream: Crisp borders & Distinct Background */
      .turn-card {
        padding: 12px !important;
        background: #0F1728 !important;
        border: 1px solid var(--border-subtle) !important;
        border-radius: 4px !important;
        margin-bottom: 8px !important;
      }
      .turn-header {
        flex-wrap: wrap !important;
        gap: 6px !important;
      }
      .turn-agents {
        font-size: 12px !important;
      }
      .turn-meta {
        font-size: 11px !important;
      }
      .turn-body {
        font-size: 12px !important;
        padding: 10px 12px !important;
        background: #080B11 !important;
        border: 1px solid #1C2A42 !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
      }

      /* 6. Workspace View Switching on Mobile */
      body[data-mobile-tab="stream"] .workspace .sidebar { display: none !important; }
      body[data-mobile-tab="stream"] .workspace .inspector { display: none !important; }
      body[data-mobile-tab="stream"] .workspace .stream-panel { display: flex !important; width: 100% !important; }

      body[data-mobile-tab="agents"] .workspace .stream-panel { display: none !important; }
      body[data-mobile-tab="agents"] .workspace .inspector { display: none !important; }
      body[data-mobile-tab="agents"] .workspace .sidebar {
        display: flex !important;
        width: 100% !important;
        height: auto !important;
        min-height: calc(100vh - 120px) !important;
        border-right: none !important;
      }

      body[data-mobile-tab="inspector"] .workspace .sidebar { display: none !important; }
      body[data-mobile-tab="inspector"] .workspace .stream-panel { display: none !important; }
      body[data-mobile-tab="inspector"] .workspace .inspector {
        display: flex !important;
        width: 100% !important;
        height: auto !important;
        min-height: calc(100vh - 120px) !important;
        border-left: none !important;
      }

      body[data-mobile-tab="topology"] .workspace .sidebar { display: none !important; }
      body[data-mobile-tab="topology"] .workspace .inspector { display: none !important; }
      body[data-mobile-tab="topology"] .workspace .stream-panel { display: flex !important; width: 100% !important; }
      body[data-mobile-tab="topology"] .workspace .stream-panel #streamFeed { display: none !important; }
      body[data-mobile-tab="topology"] .workspace .stream-panel #dagPanel { display: flex !important; flex: 1 !important; height: auto !important; }

      /* Mobile Close Buttons */
      .mobile-close-btn {
        display: inline-flex !important;
        align-items: center !important;
        gap: 4px !important;
        background: #141E30 !important;
        border: 1px solid var(--border-subtle) !important;
        border-radius: 4px !important;
        color: var(--text-secondary) !important;
        font-size: 11px !important;
        padding: 4px 8px !important;
        cursor: pointer !important;
        touch-action: manipulation !important;
      }

      /* 7. Bottom Navigation Bar */
      .mobile-nav-bar {
        display: flex !important;
        position: fixed !important;
        bottom: 0 !important;
        left: 0 !important;
        right: 0 !important;
        height: calc(52px + env(safe-area-inset-bottom, 0px)) !important;
        padding-bottom: env(safe-area-inset-bottom, 0px) !important;
        background: #0B101C !important;
        border-top: 1px solid var(--border-subtle) !important;
        z-index: 950 !important;
        align-items: center !important;
        justify-content: space-around !important;
        width: 100% !important;
        max-width: 100vw !important;
      }
      .mobile-nav-btn {
        flex: 1 !important;
        height: 100% !important;
        background: transparent !important;
        border: none !important;
        color: var(--text-muted) !important;
        display: flex !important;
        flex-direction: column !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 2px !important;
        font-size: 10px !important;
        font-weight: 600 !important;
        letter-spacing: 0.2px !important;
        cursor: pointer !important;
        user-select: none !important;
        touch-action: manipulation !important;
        padding: 0 !important;
      }
      .mobile-nav-btn svg {
        width: 17px !important;
        height: 17px !important;
      }
      .mobile-nav-btn.active {
        color: var(--accent) !important;
      }
      .mobile-nav-btn:active {
        background: var(--bg-surface-hover) !important;
      }

      /* Modals on Mobile */
      #apiKeyModal > div,
      #mfaModal > div,
      #forkModal > div {
        width: 95vw !important;
        max-width: 95vw !important;
        max-height: 88vh !important;
        overflow-y: auto !important;
        padding: 14px !important;
        background: #0F1728 !important;
        border: 1px solid var(--border-strong) !important;
      }
    }
  </style>"""

content = content[:mobile_start_idx] + impeccable_mobile_css + content[mobile_end_idx + len("</style>"):]

# ==============================================================================
# 4. ADD MOBILE TOOL RIBBON BEFORE CHAOS BAR
# ==============================================================================
ribbon_html = """      <!-- Mobile Tool Ribbon (Allows user to toggle Chaos, Time-Machine, Fleet on demand) -->
      <div class="mobile-tool-ribbon" id="mobileToolRibbon">
        <button class="mobile-tool-btn" id="btnToggleChaosBar" title="Toggle Chaos Injection">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
          <span>Chaos (4)</span>
        </button>
        <button class="mobile-tool-btn" id="btnToggleTimeMachineBar" title="Toggle Time-Machine Scrubber">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 8 14"/></svg>
          <span>Time-Machine</span>
        </button>
        <button class="mobile-tool-btn" id="btnToggleFleetBar" title="Toggle Redis Fleet HUD">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect><rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect></svg>
          <span>Fleet HUD</span>
        </button>
      </div>
"""

chaos_bar_target = '<div class="chaos-bar" id="chaosBar">'
assert chaos_bar_target in content, "chaosBar not found"
if "mobileToolRibbon" not in content:
    content = content.replace(chaos_bar_target, ribbon_html + "      " + chaos_bar_target, 1)

# ==============================================================================
# 5. JAVASCRIPT: WIRE UP TOOL RIBBON TOGGLES
# ==============================================================================
js_ribbon_toggles = """
      // Mobile Tool Ribbon Toggles
      var btnToggleChaosBar = document.getElementById('btnToggleChaosBar');
      var btnToggleTimeMachineBar = document.getElementById('btnToggleTimeMachineBar');
      var btnToggleFleetBar = document.getElementById('btnToggleFleetBar');
      var chaosBarEl = document.getElementById('chaosBar');
      var tmBarEl = document.getElementById('timeMachineBar');
      var fleetBarEl = document.getElementById('fleetBar');

      if (btnToggleChaosBar && chaosBarEl) {
        btnToggleChaosBar.addEventListener('click', function() {
          var isOpen = chaosBarEl.classList.toggle('mobile-open');
          btnToggleChaosBar.classList.toggle('active', isOpen);
        });
      }
      if (btnToggleTimeMachineBar && tmBarEl) {
        btnToggleTimeMachineBar.addEventListener('click', function() {
          var isOpen = tmBarEl.classList.toggle('mobile-open');
          btnToggleTimeMachineBar.classList.toggle('active', isOpen);
        });
      }
      if (btnToggleFleetBar && fleetBarEl) {
        btnToggleFleetBar.addEventListener('click', function() {
          var isOpen = fleetBarEl.classList.toggle('mobile-open');
          btnToggleFleetBar.classList.toggle('active', isOpen);
        });
      }
"""

if "btnToggleChaosBar" not in content:
    js_mobile_anchor = "// Mobile Tab Navigation Logic"
    assert js_mobile_anchor in content, "js_mobile_anchor not found"
    content = content.replace(js_mobile_anchor, js_ribbon_toggles + "\n      " + js_mobile_anchor, 1)

# Write to root dashboard.html
with open("dashboard.html", "w", encoding="utf-8") as f:
    f.write(content)

# Write to packages/sdk-python/swarmwatch/dashboard.html
pkg_path = os.path.join("packages", "sdk-python", "swarmwatch", "dashboard.html")
with open(pkg_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Impeccable Mobile Architecture applied to dashboard.html!")
