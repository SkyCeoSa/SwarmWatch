import os
import re

html_path = "dashboard.html"
with open(html_path, "r", encoding="utf-8") as f:
    content = f.read()

# Target block to replace:
target_old_metrics = """      /* 3. Metrics Bar (2-Column Grid with High-Contrast Hairline Borders) */
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
      }"""

replacement_metrics = """      /* 3. Metrics Bar (2-Column Rigid Grid, Zero-Overflow, Stacked Values) */
      .metrics-bar {
        display: grid !important;
        grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
        gap: 8px !important;
        padding: 8px 10px !important;
        width: 100% !important;
        max-width: 100vw !important;
        box-sizing: border-box !important;
        overflow: hidden !important;
      }
      .metric-cell {
        padding: 8px 8px !important;
        gap: 4px !important;
        background: #0F1728 !important;
        border: 1px solid var(--border-subtle) !important;
        border-radius: 4px !important;
        min-width: 0 !important;
        max-width: 100% !important;
        width: 100% !important;
        box-sizing: border-box !important;
        overflow: hidden !important;
        display: flex !important;
        flex-direction: column !important;
        justify-content: space-between !important;
      }
      .metric-header {
        display: flex !important;
        align-items: flex-start !important;
        justify-content: space-between !important;
        gap: 4px !important;
        width: 100% !important;
        min-width: 0 !important;
        box-sizing: border-box !important;
      }
      .metric-title {
        font-size: 11px !important;
        line-height: 1.25 !important;
        color: var(--text-muted) !important;
        font-weight: 600 !important;
        flex: 1 1 0% !important;
        min-width: 0 !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
      }
      .metric-tag {
        font-size: 9.5px !important;
        padding: 1px 5px !important;
        border-radius: 3px !important;
        font-family: var(--font-mono) !important;
        font-weight: 600 !important;
        white-space: nowrap !important;
        flex-shrink: 0 !important;
        line-height: 1.3 !important;
        max-width: 48% !important;
        text-overflow: ellipsis !important;
        overflow: hidden !important;
      }
      .metric-value-row {
        display: flex !important;
        flex-direction: column !important;
        align-items: flex-start !important;
        gap: 1px !important;
        width: 100% !important;
        min-width: 0 !important;
        box-sizing: border-box !important;
        overflow: hidden !important;
      }
      .metric-value {
        font-size: 17px !important;
        line-height: 1.15 !important;
        font-weight: 700 !important;
        font-family: var(--font-mono) !important;
        font-feature-settings: "tnum" 1 !important;
        color: var(--text-primary) !important;
        min-width: 0 !important;
      }
      .metric-subtext {
        font-size: 10px !important;
        line-height: 1.2 !important;
        color: var(--text-muted) !important;
        width: 100% !important;
        min-width: 0 !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        display: block !important;
      }"""

# Normalize line breaks for robust match
content_norm = content.replace("\r\n", "\n")
target_norm = target_old_metrics.replace("\r\n", "\n")
replacement_norm = replacement_metrics.replace("\r\n", "\n")

assert target_norm in content_norm, "Target metrics CSS not found in dashboard.html"
content_norm = content_norm.replace(target_norm, replacement_norm, 1)

# Also enhance .panel-header for mobile
old_panel_header = """      /* Stream Panel Header */
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
      }"""

new_panel_header = """      /* Stream Panel Header */
      .panel-header {
        padding: 8px 12px !important;
        gap: 6px !important;
        flex-wrap: wrap !important;
        width: 100% !important;
        max-width: 100% !important;
        box-sizing: border-box !important;
      }
      .panel-header > div {
        display: flex !important;
        align-items: center !important;
        justify-content: space-between !important;
        width: 100% !important;
        gap: 8px !important;
      }
      .panel-title {
        font-size: 12px !important;
      }
      .panel-count {
        font-size: 11px !important;
      }
      .view-btn {
        padding: 4px 8px !important;
        font-size: 11px !important;
      }"""

if old_panel_header in content_norm:
    content_norm = content_norm.replace(old_panel_header, new_panel_header, 1)

# Write back to dashboard.html
with open(html_path, "w", encoding="utf-8") as f:
    f.write(content_norm)

# Write to packages/sdk-python/swarmwatch/dashboard.html
pkg_path = os.path.join("packages", "sdk-python", "swarmwatch", "dashboard.html")
with open(pkg_path, "w", encoding="utf-8") as f:
    f.write(content_norm)

# Synchronize server.py
server_path = os.path.join("packages", "sdk-python", "swarmwatch", "server.py")
with open(server_path, "r", encoding="utf-8") as f:
    server_content = f.read()

q3 = '"""'
start_marker = 'DASHBOARD_HTML = ' + q3
start_idx = server_content.find(start_marker)
assert start_idx != -1, "Start marker not found in server.py"
content_start = start_idx + len(start_marker)
end_idx = server_content.find(q3, content_start + 100)
assert end_idx != -1, "End marker not found in server.py"

new_server = server_content[:content_start] + content_norm + server_content[end_idx:]
with open(server_path, "w", encoding="utf-8") as f:
    f.write(new_server)

print("Applied fix_mobile_metrics_overflow successfully across all 3 files!")
