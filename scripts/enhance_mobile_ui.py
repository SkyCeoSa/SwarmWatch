import re
import os
import subprocess
from swarmwatch.acceptance import global_acceptance_harness

html_path = "dashboard.html"
with open(html_path, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Update Viewport
old_viewport = '<meta name="viewport" content="width=device-width, initial-scale=1.0" />'
new_viewport = '<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover" />'
assert old_viewport in content, "Viewport not found"
content = content.replace(old_viewport, new_viewport, 1)

# 2. Add Mobile CSS before </style>
mobile_css = """
    /* ==========================================================================
       Mobile & Tablet Responsive UI/UX Architecture (ADR 0071 Compliant)
       ========================================================================== */
    .mobile-nav-bar {
      display: none;
    }
    .mobile-close-btn {
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
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 10px 12px;
      color: var(--text-primary);
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      text-align: left;
      transition: background 0.15s, border-color 0.15s;
    }
    .mobile-quick-action-btn:hover {
      background: var(--bg-surface-hover);
      border-color: var(--accent);
    }

    @media (max-width: 900px) {
      body {
        height: 100dvh;
        height: 100vh;
        padding-bottom: calc(52px + env(safe-area-inset-bottom, 0px));
        overflow-x: hidden;
      }

      /* Mobile Header Adaptations */
      header {
        padding: 8px 12px;
        flex-wrap: wrap;
        gap: 8px;
      }
      .brand-title {
        font-size: 14px;
      }
      .brand-icon {
        width: 18px;
        height: 18px;
      }
      .brand-version {
        font-size: 11px;
        padding: 1px 6px;
      }
      .header-center {
        gap: 6px;
        order: 3;
        width: 100%;
        overflow-x: auto;
        padding-top: 6px;
        border-top: 1px solid var(--border-subtle);
        -webkit-overflow-scrolling: touch;
      }
      .header-actions {
        gap: 6px;
        order: 2;
        margin-left: auto;
      }
      .header-actions .btn-action {
        height: 32px;
        padding: 0 10px;
        font-size: 11px;
      }

      /* Responsive Metrics Grid */
      .metrics-bar {
        grid-template-columns: repeat(2, 1fr);
        gap: 8px;
        padding: 8px 12px;
      }
      .metric-cell {
        padding: 8px 10px;
        gap: 4px;
      }
      .metric-title {
        font-size: 11px;
      }
      .metric-value {
        font-size: 16px;
      }
      .metric-subtext {
        font-size: 11px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      /* Mobile Navigation Bar */
      .mobile-nav-bar {
        display: flex;
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        height: calc(52px + env(safe-area-inset-bottom, 0px));
        padding-bottom: env(safe-area-inset-bottom, 0px);
        background: #0E1422;
        border-top: 1px solid var(--border-subtle);
        z-index: 950;
        align-items: center;
        justify-content: space-around;
      }
      .mobile-nav-btn {
        flex: 1;
        height: 100%;
        background: transparent;
        border: none;
        color: var(--text-muted);
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 3px;
        font-size: 11px;
        font-weight: 500;
        cursor: pointer;
        user-select: none;
        touch-action: manipulation;
        transition: color 0.15s ease, background 0.15s ease;
      }
      .mobile-nav-btn svg {
        width: 17px;
        height: 17px;
      }
      .mobile-nav-btn.active {
        color: var(--accent);
      }
      .mobile-nav-btn:active {
        background: var(--bg-surface-hover);
      }

      /* Workspace View Switching on Mobile */
      body[data-mobile-tab="stream"] .workspace .sidebar { display: none !important; }
      body[data-mobile-tab="stream"] .workspace .inspector { display: none !important; }
      body[data-mobile-tab="stream"] .workspace .stream-panel { display: flex !important; width: 100% !important; }

      body[data-mobile-tab="agents"] .workspace .stream-panel { display: none !important; }
      body[data-mobile-tab="agents"] .workspace .inspector { display: none !important; }
      body[data-mobile-tab="agents"] .workspace .sidebar {
        display: flex !important;
        width: 100% !important;
        height: 100% !important;
        border-right: none !important;
      }

      body[data-mobile-tab="inspector"] .workspace .sidebar { display: none !important; }
      body[data-mobile-tab="inspector"] .workspace .stream-panel { display: none !important; }
      body[data-mobile-tab="inspector"] .workspace .inspector {
        display: flex !important;
        width: 100% !important;
        height: 100% !important;
        border-left: none !important;
      }

      body[data-mobile-tab="topology"] .workspace .sidebar { display: none !important; }
      body[data-mobile-tab="topology"] .workspace .inspector { display: none !important; }
      body[data-mobile-tab="topology"] .workspace .stream-panel { display: flex !important; width: 100% !important; }
      body[data-mobile-tab="topology"] .workspace .stream-panel #streamFeed { display: none !important; }
      body[data-mobile-tab="topology"] .workspace .stream-panel #dagPanel { display: flex !important; flex: 1 !important; height: auto !important; }

      /* Mobile Close Buttons */
      .mobile-close-btn {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        background: var(--bg-canvas);
        border: 1px solid var(--border-subtle);
        border-radius: 4px;
        color: var(--text-secondary);
        font-size: 11px;
        padding: 4px 8px;
        cursor: pointer;
        touch-action: manipulation;
      }

      /* Touch Ergonomics for Bars */
      .chaos-bar {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        padding: 6px 12px;
      }
      .chaos-group {
        flex-wrap: nowrap;
      }
      .chaos-btn {
        padding: 6px 10px;
        white-space: nowrap;
        font-size: 11px;
      }

      .time-machine-bar {
        flex-direction: column;
        align-items: stretch;
        padding: 8px 12px;
        gap: 8px;
      }
      .tm-left-group {
        justify-content: space-between;
        width: 100%;
      }
      .tm-right-group {
        justify-content: space-between;
        width: 100%;
      }
      .tm-center-track {
        width: 100%;
        min-height: 24px;
      }
      input[type="range"]::-webkit-slider-thumb {
        width: 18px;
        height: 18px;
      }

      .fleet-bar {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        padding: 6px 12px;
        justify-content: flex-start;
      }
      .fleet-left-group {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
      }
      .fleet-meters-container {
        display: none;
      }

      /* Stream Feed & Cards */
      .stream-feed {
        padding: 10px 12px;
      }
      .turn-card {
        padding: 12px;
      }
      .turn-header {
        flex-wrap: wrap;
        gap: 6px;
      }
      .turn-body {
        font-size: 12px;
        padding: 10px 12px;
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
      }
    }

    @media (max-width: 480px) {
      .header-actions #btnMfaModal span,
      .header-actions #btnFleetToggle span,
      .header-actions #btnApiKeys span {
        display: none;
      }
      .header-actions .btn-action {
        padding: 0 8px;
      }
      .brand-version {
        display: none;
      }
      .metric-value {
        font-size: 15px;
      }
      .turn-agents {
        font-size: 12px;
      }
    }
  </style>"""

content = content.replace("  </style>", mobile_css, 1)

# 3. Add Close button to Sidebar Header
sidebar_hdr_old = """      <div class="panel-header">
        <span class="panel-title">Swarm Participants</span>
        <span class="panel-count">4 Agents</span>
      </div>"""
sidebar_hdr_new = """      <div class="panel-header">
        <span class="panel-title">Swarm Participants</span>
        <div style="display: flex; align-items: center; gap: 8px;">
          <span class="panel-count">4 Agents</span>
          <button class="mobile-close-btn" id="btnMobileCloseSidebar" title="Return to Causal Stream">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            <span>Stream</span>
          </button>
        </div>
      </div>"""
assert sidebar_hdr_old in content, "Sidebar header not found"
content = content.replace(sidebar_hdr_old, sidebar_hdr_new, 1)

# 4. Add Close button to Inspector Header
inspector_hdr_old = """      <div class="panel-header">
        <span class="panel-title">Turn Inspector</span>
        <span class="panel-count">Time-Machine</span>
      </div>"""
inspector_hdr_new = """      <div class="panel-header">
        <span class="panel-title">Turn Inspector</span>
        <div style="display: flex; align-items: center; gap: 8px;">
          <span class="panel-count">Time-Machine</span>
          <button class="mobile-close-btn" id="btnMobileCloseInspector" title="Return to Causal Stream">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            <span>Stream</span>
          </button>
        </div>
      </div>"""
assert inspector_hdr_old in content, "Inspector header not found"
content = content.replace(inspector_hdr_old, inspector_hdr_new, 1)

# 5. Add Mobile Navigation Bar & Mobile Controls Bottom Sheet after </aside>\n  </div>
workspace_end = """    </aside>
  </div>"""

mobile_nav_html = """    </aside>
  </div>

  <!-- Mobile Bottom Navigation Bar (Visible only on screens <= 900px) -->
  <nav class="mobile-nav-bar" id="mobileNavBar" aria-label="Mobile Navigation">
    <button class="mobile-nav-btn active" id="btnMobileNavStream" data-target="stream">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <span>Stream</span>
    </button>
    <button class="mobile-nav-btn" id="btnMobileNavAgents" data-target="agents">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      <span>Agents</span>
    </button>
    <button class="mobile-nav-btn" id="btnMobileNavInspector" data-target="inspector">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
      <span>Inspector</span>
    </button>
    <button class="mobile-nav-btn" id="btnMobileNavTopology" data-target="topology">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/></svg>
      <span>Topology</span>
    </button>
    <button class="mobile-nav-btn" id="btnMobileNavControls" data-target="controls">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
      <span>Controls</span>
    </button>
  </nav>

  <!-- Mobile Quick Controls Bottom Sheet Drawer -->
  <div class="mobile-drawer-modal" id="mobileControlsDrawer">
    <div class="mobile-drawer-sheet">
      <div style="display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border-subtle); padding-bottom: 10px;">
        <span style="font-weight: 700; font-size: 13px; color: var(--text-primary); text-transform: uppercase; letter-spacing: 0.3px;">Swarm Controls</span>
        <button id="btnCloseMobileDrawer" style="background: transparent; border: 1px solid var(--border-subtle); color: var(--text-muted); border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 12px;">Close</button>
      </div>
      <div class="mobile-quick-action-grid">
        <button class="mobile-quick-action-btn" id="btnMobileSimulate" style="background: rgba(56, 189, 248, 0.12); border-color: rgba(56, 189, 248, 0.3); color: var(--accent);">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
          <span>Run Simulation</span>
        </button>
        <button class="mobile-quick-action-btn" id="btnMobileReset">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
          <span>Reset Console</span>
        </button>
        <button class="mobile-quick-action-btn" id="btnMobileApiKeys">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>
          <span>API Keys</span>
        </button>
        <button class="mobile-quick-action-btn" id="btnMobileMfa">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="5" y="11" width="14" height="10" rx="2"></rect><circle cx="12" cy="16" r="1"></circle><path d="M8 11V7a4 4 0 0 1 8 0v4"></path></svg>
          <span>Security / MFA</span>
        </button>
        <button class="mobile-quick-action-btn" id="btnMobileFleet">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect><rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect><line x1="6" y1="6" x2="6.01" y2="6"></line><line x1="6" y1="18" x2="6.01" y2="18"></line></svg>
          <span>Toggle Fleet</span>
        </button>
        <button class="mobile-quick-action-btn" id="btnMobilePolicy">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/><line x1="17" y1="16" x2="23" y2="16"/></svg>
          <span>Tune Policy</span>
        </button>
      </div>
    </div>
  </div>"""

assert workspace_end in content, "Workspace end not found"
content = content.replace(workspace_end, mobile_nav_html, 1)

# 6. Card tap automatically switches to inspector tab on mobile
old_card_click = """        card.addEventListener('click', function() {
          selectSpan(span);
        });"""
new_card_click = """        card.addEventListener('click', function() {
          selectSpan(span);
          if (window.innerWidth <= 900) {
            setMobileTab('inspector');
          }
        });"""
assert old_card_click in content, "Old card click listener not found"
content = content.replace(old_card_click, new_card_click, 1)

# 7. Add JavaScript mobile navigation handlers at the end of the script before })();
js_mobile = """
      // Mobile Tab Navigation Logic
      function setMobileTab(tabName) {
        document.body.setAttribute('data-mobile-tab', tabName);
        document.querySelectorAll('.mobile-nav-btn').forEach(function(btn) {
          if (btn.getAttribute('data-target') === tabName) {
            btn.classList.add('active');
          } else {
            btn.classList.remove('active');
          }
        });
      }

      // Initialize default mobile tab
      document.body.setAttribute('data-mobile-tab', 'stream');

      var btnMobileNavStream = document.getElementById('btnMobileNavStream');
      var btnMobileNavAgents = document.getElementById('btnMobileNavAgents');
      var btnMobileNavInspector = document.getElementById('btnMobileNavInspector');
      var btnMobileNavTopology = document.getElementById('btnMobileNavTopology');
      var btnMobileNavControls = document.getElementById('btnMobileNavControls');
      var btnMobileCloseSidebar = document.getElementById('btnMobileCloseSidebar');
      var btnMobileCloseInspector = document.getElementById('btnMobileCloseInspector');
      var mobileControlsDrawer = document.getElementById('mobileControlsDrawer');
      var btnCloseMobileDrawer = document.getElementById('btnCloseMobileDrawer');

      if (btnMobileNavStream) btnMobileNavStream.addEventListener('click', function() { setMobileTab('stream'); });
      if (btnMobileNavAgents) btnMobileNavAgents.addEventListener('click', function() { setMobileTab('agents'); });
      if (btnMobileNavInspector) btnMobileNavInspector.addEventListener('click', function() { setMobileTab('inspector'); });
      if (btnMobileNavTopology) btnMobileNavTopology.addEventListener('click', function() { setMobileTab('topology'); });
      if (btnMobileNavControls) btnMobileNavControls.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'flex';
      });
      if (btnCloseMobileDrawer) btnCloseMobileDrawer.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
      });
      if (mobileControlsDrawer) {
        mobileControlsDrawer.addEventListener('click', function(e) {
          if (e.target === mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        });
      }
      if (btnMobileCloseSidebar) btnMobileCloseSidebar.addEventListener('click', function() { setMobileTab('stream'); });
      if (btnMobileCloseInspector) btnMobileCloseInspector.addEventListener('click', function() { setMobileTab('stream'); });

      // Mobile Quick Control Buttons
      var btnMobileSimulate = document.getElementById('btnMobileSimulate');
      var btnMobileReset = document.getElementById('btnMobileReset');
      var btnMobileApiKeys = document.getElementById('btnMobileApiKeys');
      var btnMobileMfa = document.getElementById('btnMobileMfa');
      var btnMobileFleet = document.getElementById('btnMobileFleet');
      var btnMobilePolicy = document.getElementById('btnMobilePolicy');

      if (btnMobileSimulate) btnMobileSimulate.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        setMobileTab('stream');
        runSimulation();
      });
      if (btnMobileReset) btnMobileReset.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        resetConsole();
      });
      if (btnMobileApiKeys) btnMobileApiKeys.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        if (apiKeyModal) { apiKeyModal.style.display = 'flex'; loadApiKeys(); }
      });
      if (btnMobileMfa) btnMobileMfa.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        if (mfaModal) { mfaModal.style.display = 'flex'; loadMfaStatus(); }
      });
      if (btnMobileFleet) btnMobileFleet.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        var fBar = document.getElementById('fleetBar');
        if (fBar) fBar.style.display = (fBar.style.display === 'none' ? 'flex' : 'none');
      });
      if (btnMobilePolicy) btnMobilePolicy.addEventListener('click', function() {
        if (mobileControlsDrawer) mobileControlsDrawer.style.display = 'none';
        setMobileTab('inspector');
        var insp = document.getElementById('inspectorContent');
        if (insp) insp.scrollTop = insp.scrollHeight;
      });

      // Participant Cards in Sidebar on mobile: tap to switch to stream and show selected agent
      document.querySelectorAll('.participant-card').forEach(function(card) {
        card.addEventListener('click', function() {
          document.querySelectorAll('.participant-card').forEach(function(c) { c.classList.remove('active'); });
          card.classList.add('active');
          var agentName = card.getAttribute('data-agent');
          if (agentName) {
            showToast('Selected Agent: ' + agentName, 'info', 1800);
          }
          if (window.innerWidth <= 900) {
            setMobileTab('stream');
          }
        });
      });
"""

old_end = "    })();\n  </script>"
assert old_end in content, "Script end not found"
content = content.replace(old_end, js_mobile + "\n" + old_end, 1)

# Write to root dashboard.html
with open("dashboard.html", "w", encoding="utf-8") as f:
    f.write(content)

# Write to packages/sdk-python/swarmwatch/dashboard.html
pkg_path = os.path.join("packages", "sdk-python", "swarmwatch", "dashboard.html")
with open(pkg_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Enhanced dashboard.html written successfully!")
