---
name: SwarmWatch Observability Console
description: An austere, calm, high-density developer console for multi-agent LLM systems observability and runtime circuit-breaking.
colors:
  canvas: "#080B11"
  surface: "#0E1422"
  surface-hover: "#141C2E"
  surface-active: "#1C2740"
  border-subtle: "#1D2840"
  border-strong: "#2D3E63"
  accent: "#38BDF8"
  accent-hover: "#7DD3FC"
  accent-subtle: "rgba(56, 189, 248, 0.12)"
  success: "#10B981"
  success-subtle: "rgba(16, 185, 129, 0.12)"
  warning: "#F59E0B"
  warning-subtle: "rgba(245, 158, 11, 0.12)"
  danger: "#EF4444"
  danger-subtle: "rgba(239, 68, 68, 0.12)"
  danger-surface: "rgba(239, 68, 68, 0.08)"
  text-primary: "#F8FAFC"
  text-secondary: "#CBD5E1"
  text-muted: "#94A3B8"
  branch-purple: "#A78BFA"
typography:
  display:
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif"
    fontSize: "15px"
    fontWeight: 700
    letterSpacing: "-0.2px"
    lineHeight: 1.5
  headline:
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif"
    fontSize: "14px"
    fontWeight: 700
    lineHeight: 1.5
  title:
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif"
    fontSize: "13px"
    fontWeight: 600
    letterSpacing: "0.2px"
    lineHeight: 1.5
  body:
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif"
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.6
  label:
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif"
    fontSize: "12px"
    fontWeight: 600
    letterSpacing: "0.2px"
    lineHeight: 1.4
  mono-metric:
    fontFamily: "'JetBrains Mono', 'SF Mono', Consolas, monospace"
    fontSize: "18px"
    fontWeight: 700
    lineHeight: 1.2
    fontFeatureSettings: '"tnum" 1'
  mono-body:
    fontFamily: "'JetBrains Mono', 'SF Mono', Consolas, monospace"
    fontSize: "12px"
    fontWeight: 400
    lineHeight: 1.5
    fontFeatureSettings: '"tnum" 1'
  mono-badge:
    fontFamily: "'JetBrains Mono', 'SF Mono', Consolas, monospace"
    fontSize: "12px"
    fontWeight: 500
    lineHeight: 1.4
    fontFeatureSettings: '"tnum" 1'
rounded:
  sm: "3px"
  md: "4px"
  full: "50%"
spacing:
  2xs: "2px"
  xs: "4px"
  sm: "6px"
  md: "8px"
  lg: "10px"
  xl: "12px"
  2xl: "14px"
  3xl: "16px"
  4xl: "18px"
  5xl: "32px"
components:
  button-primary:
    backgroundColor: "{colors.accent}"
    hoverBackgroundColor: "{colors.accent-hover}"
    textColor: "#080B11"
    height: "34px"
    padding: "0 16px"
    rounded: "{rounded.md}"
    fontSize: "12px"
    fontWeight: 600
    border: "1px solid {colors.accent}"
  button-secondary:
    backgroundColor: "{colors.canvas}"
    hoverBackgroundColor: "{colors.surface-hover}"
    textColor: "{colors.text-secondary}"
    hoverTextColor: "{colors.text-primary}"
    height: "34px"
    padding: "0 16px"
    rounded: "{rounded.md}"
    fontSize: "12px"
    fontWeight: 600
    border: "1px solid {colors.border-subtle}"
    hoverBorder: "1px solid {colors.border-strong}"
  status-badge:
    backgroundColor: "{colors.success-subtle}"
    textColor: "{colors.success}"
    border: "1px solid rgba(16, 185, 129, 0.3)"
    rounded: "{rounded.md}"
    padding: "4px 10px"
    fontSize: "12px"
    fontWeight: 500
  metric-cell:
    backgroundColor: "{colors.canvas}"
    border: "1px solid {colors.border-subtle}"
    rounded: "{rounded.md}"
    padding: "10px 14px"
  participant-card:
    backgroundColor: "transparent"
    hoverBackgroundColor: "{colors.surface-hover}"
    activeBackgroundColor: "{colors.surface-active}"
    border: "1px solid transparent"
    hoverBorder: "1px solid {colors.border-subtle}"
    activeBorder: "1px solid {colors.border-strong}"
    rounded: "{rounded.md}"
    padding: "10px 12px"
  turn-card:
    backgroundColor: "{colors.surface}"
    selectedBackgroundColor: "{colors.surface-active}"
    border: "1px solid {colors.border-subtle}"
    hoverBorder: "1px solid {colors.border-strong}"
    selectedBorder: "1px solid {colors.accent}"
    rounded: "{rounded.md}"
    padding: "14px 16px"
  turn-body:
    backgroundColor: "{colors.canvas}"
    border: "1px solid {colors.border-subtle}"
    textColor: "{colors.text-secondary}"
    rounded: "{rounded.md}"
    padding: "12px 14px"
  circuit-banner:
    backgroundColor: "{colors.danger-surface}"
    border: "1px solid {colors.danger}"
    textColor: "{colors.danger}"
    rounded: "{rounded.md}"
    padding: "14px 16px"
  inspector-box:
    backgroundColor: "{colors.canvas}"
    border: "1px solid {colors.border-subtle}"
    textColor: "{colors.text-secondary}"
    rounded: "{rounded.md}"
    padding: "12px"
---

# Design System: SwarmWatch Observability Console

## Overview

**Creative North Star: "The Austere Developer Console"** — grounded strictly in the shipped runtime artifact (`dashboard.html`), not aspirational or hypothetical mockups.

SwarmWatch provides an austere, calm, high-density developer console engineered specifically for multi-agent LLM systems observability and runtime circuit-breaking. It is designed for engineers operating live autonomous agent swarms where high-frequency inter-agent dialogue, causal order, token burn rates, and loop entropy must be evaluated instantly with zero visual noise.

The canonical simulation scenario embodies this mission: a four-agent software engineering swarm (`ProductArchitect`, `BackendEngineer`, `CodeReviewer`, `SecurityAuditor`) collaborating on database connection pooling. When the `BackendEngineer` and `SecurityAuditor` enter an infinite cyclic retry loop over raw DSN sanitization in error loggers, the runtime circuit breaker trips as Shannon entropy drops to 0.18 bits (threshold: 0.35 bits), instantly halting execution, saving 241,500 tokens ($4.83 budget preserved), and exposing a one-click time-machine state fork.

**Key System Principles:**
- **Austere Slate Console**: Deep `#080B11` canvas background paired with layered `#0E1422` console surfaces and sharp 1px `#1D2840` structural hairline borders.
- **Content is the Hero**: Telemetry metrics, agent dialogue payloads, and causal traces take precedence over decorative chrome.
- **Dual-Font Discipline**: System sans (`Inter` / system-ui) for interface navigation and labels; `JetBrains Mono` with tabular numbers (`tnum`) for metrics, durations, token counts, and JSON state.
- **Flat Elevation Hierarchy**: Zero box-shadows or synthetic glow effects. Contrast and depth are communicated solely through tonal background shifts and hairline borders.
- **Restrained Semantic Signals**: Instrument Cyan (`#38BDF8`) for primary focus, Emerald Green (`#10B981`) for nominal health, Amber (`#F59E0B`) for warnings, and Crimson Red (`#EF4444`) strictly for circuit-breaker halts.

---

## Colors

The SwarmWatch palette is rooted in cold, deep slate tones, accented by high-visibility cyan instrumentation and precise semantic status indicators.

### Canvas & Surface Layers
- **Console Canvas** (`{colors.canvas}` / `#080B11`): Deepest background layer for the viewport, metric cell interiors, turn payload blocks, and inspector data boxes.
- **Work Surface** (`{colors.surface}` / `#0E1422`): Chrome surfaces including the global header, metrics ribbon, participant sidebar, turn cards, and inspector drawer.
- **Surface Hover** (`{colors.surface-hover}` / `#141C2E`): Subtle interactive hover state for participant cards and secondary buttons.
- **Surface Active** (`{colors.surface-active}` / `#1C2740`): Selected state for participant cards and focused turn cards in the causal stream.

### Structural Borders
- **Subtle Hairline Border** (`{colors.border-subtle}` / `#1D2840`): 1px perimeter border used to separate panels, headers, metric cells, cards, and input containers.
- **Strong Interactive Border** (`{colors.border-strong}` / `#2D3E63`): Active and hover boundary for cards, scrollbar thumbs, and interactive controls.

### Brand & Instrumentation Accents
- **Instrument Cyan** (`{colors.accent}` / `#38BDF8`): Primary action button fill, brand glyph fill/stroke, and selection border for the active turn card.
- **Cyan Hover** (`{colors.accent-hover}` / `#7DD3FC`): Hover state for primary action buttons.
- **Cyan Subtle** (`{colors.accent-subtle}` / `rgba(56, 189, 248, 0.12)`): Low-intensity highlight tint.
- **Branch Purple** (`{colors.branch-purple}` / `#A78BFA`): Monospace branch name marker indicating current execution line (`main`).

### Semantic Status Palette
- **Success / Nominal** (`{colors.success}` / `#10B981`): Healthy agent status, "Runtime Active" live pulse, and nominal entropy tags.
- **Success Subtle** (`{colors.success-subtle}` / `rgba(16, 185, 129, 0.12)`): Background fill for nominal metric tags and runtime status badges.
- **Warning** (`{colors.warning}` / `#F59E0B`): Approaching loop thresholds or soft entropy degradation.
- **Warning Subtle** (`{colors.warning-subtle}` / `rgba(245, 158, 11, 0.12)`): Background fill for warning states.
- **Danger / Circuit Breaker** (`{colors.danger}` / `#EF4444`): Runaway semantic loop halt, circuit intervention badge, and alert tag borders.
- **Danger Subtle** (`{colors.danger-subtle}` / `rgba(239, 68, 68, 0.12)`): Background fill for circuit-break metric tags.
- **Danger Surface** (`{colors.danger-surface}` / `rgba(239, 68, 68, 0.08)`): Flat background fill for the circuit breaker emergency banner.

### Typography Colors
- **Primary Text** (`{colors.text-primary}` / `#F8FAFC`): Brand title, turn participant headers, large metric numerals, and active text.
- **Secondary Text** (`{colors.text-secondary}` / `#CBD5E1`): Agent message payload copy, panel header labels, and inspector JSON output.
- **Muted Text** (`{colors.text-muted}` / `#94A3B8`): Metric category titles, participant roles, span duration/token metadata, and timestamps.

### Named Color Rules
1. **The Circuit Exclusivity Rule**: Red (`#EF4444`) is strictly reserved for tripped circuit breakers, budget exhaustion, or fatal execution errors; it must never appear as decorative trim.
2. **The Hairline Demarcation Rule**: Structural separation is achieved through 1px hairline borders (`#1D2840`), never through dropshadows or gradient dividers.

---

## Typography

SwarmWatch implements a strict dual-font typography stack: **Inter / system-ui** for interface structure and labels, paired with **JetBrains Mono** for technical telemetry and payloads.

### Typefaces
- **UI Sans (`--font-sans`)**: `-apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif`
- **Telemetry Mono (`--font-mono`)**: `"JetBrains Mono", "SF Mono", Consolas, monospace`

### Tabular Numerals Requirement
All dynamic metric values, timestamps, token counts, and durations apply `font-feature-settings: "tnum" 1`. This prevents visual jitter or horizontal reflow when streaming spans arrive in real time.

### Hierarchy & Scale

| Style Name | Font Family | Size | Weight | Line Height | Tracking | Purpose / Shipped Element |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Brand Display** | UI Sans | `15px` | 700 | 1.5 | `-0.2px` | Header `.brand-title` |
| **Alert Headline** | UI Sans | `14px` | 700 | 1.5 | `normal` | `.circuit-title` |
| **Panel Title** | UI Sans | `13px` | 600 | 1.5 | `0.2px` | `.panel-title`, `.turn-agents` |
| **Body Content** | UI Sans | `13px` | 400 | 1.6 | `normal` | `.turn-body`, `.circuit-details` |
| **Micro Label** | UI Sans | `12px` | 600 | 1.4 | `0.2px` | `.metric-title`, `.prop-label` |
| **Metric Monospace**| Telemetry Mono | `18px` | 700 | 1.2 | `tnum` | `.metric-value` |
| **Monospace Body** | Telemetry Mono | `12px` | 400 | 1.5 | `tnum` | `.prop-value`, `.json-box`, `.turn-meta` |
| **Status Tag** | Telemetry Mono | `12px` | 500 | 1.4 | `tnum` | `.metric-tag`, `.badge-status`, `.brand-version` |

---

## Layout

SwarmWatch uses a fixed, non-scrolling full-viewport layout (`100vh`, `overflow: hidden`) modeled after hardware instrumentation workstations.

```
+---------------------------------------------------------------------------------------+
|  HEADER: [Brand: SwarmWatch v0.1.0]   [Status: Runtime Active] [Branch: main]   [Run] [Reset] |
+---------------------------------------------------------------------------------------+
|  METRICS BAR: [Active Agents: 4]  [Spans: 5]  [Entropy: 0.18]  [Savings: $4.83]       |
+---------------------------------------------------------------------------------------+
| PARTICIPANTS (270px) | CAUSAL DIALOGUE STREAM (flex: 1)         | INSPECTOR (360px)   |
| - ProductArchitect   | Turn 1: Architect -> Backend             | Run ID: sw_live_01  |
| - BackendEngineer    | Turn 2: Backend -> Reviewer              | Turn: span_05       |
| - CodeReviewer       | Turn 3: Reviewer -> Security             | Payload JSON:       |
| - SecurityAuditor    | Turn 4: Security -> Backend              | { entropy: 0.18 }   |
|                      | Turn 5: Backend -> Security (Loop)       |                     |
|                      | [CIRCUIT BREAKER BANNER: Halted]         | [Fork State Branch] |
+---------------------------------------------------------------------------------------+
```

### Spatial Structure
1. **Console Header** (`55px` fixed height):
   - Left: Brand icon, title, version chip.
   - Center: Live connection badge (`.status-badge`) and branch descriptor (`.branch-tag`).
   - Right: Quick-action controls (`Run Swarm Simulation`, `Reset`).
2. **Global Metrics Ribbon** (`padding: 10px 18px`, `gap: 14px`):
   - 4-column equal-width grid (`repeat(4, 1fr)`) containing summary cards for nodes, span volume, Shannon entropy, and financial/token savings.
3. **Tri-Pane Workspace** (`flex: 1`, `overflow: hidden`):
   - **Left Sidebar** (`270px` fixed width, `border-right: 1px solid var(--border-subtle)`): Scrollable roster of participating agents with status pills.
   - **Center Causal Stream** (`flex: 1`, `background: var(--bg-canvas)`): Scrollable vertical feed (`padding: 16px`, `gap: 12px`) containing chronologically ordered causal turns and intervention banners.
   - **Right Inspector Drawer** (`360px` fixed width, `border-left: 1px solid var(--border-subtle)`): Deep-dive inspection pane for the selected turn payload with Time-Machine branching controls.

---

## Elevation & Depth

SwarmWatch deliberately discards decorative shadows, ambient blurs, and floating cards. Elevation is created strictly through value contrast and hairline borders.

### Surface Tiers
1. **Tier 0 (Canvas Base - `#080B11`)**: Deepest recessed plane. Applied to the stream feed canvas, internal metric values, message content containers, and JSON inspection boxes.
2. **Tier 1 (Surface Frame - `#0E1422`)**: Structural boundary plane. Applied to the header, metric cells, sidebars, and turn card shells.
3. **Tier 2 (Interactive Hover - `#141C2E`)**: Subtle lightness lift triggered on hover over clickable list items and buttons.
4. **Tier 3 (Active Focus - `#1C2740`)**: High-contrast selected state for active agent rows and selected turn cards.

### Hairline Rules
- Every container boundary is marked by a crisp 1px solid border (`#1D2840`).
- Interactive hover promotes the border to `#2D3E63`.
- Active focus on the selected turn card highlights the border with `#38BDF8` (Instrument Cyan).
- Scrollbars are constrained to `6px` width/height with `#1D2840` thumbs and `#2D3E63` hover states over transparent tracks.

---

## Shapes

The design system enforces a strict, disciplined micro-radius scale. Large pills and playful bubbly curves are prohibited.

### Radius Tokens
- **Micro (`3px` / `{rounded.sm}`)**: Applied to `.metric-tag`, `.badge-status`, and scrollbar thumbs.
- **Control & Card (`4px` / `{rounded.md}`)**: Applied universally to interactive buttons (`.btn-action`), cards (`.turn-card`, `.participant-card`, `.metric-cell`), inputs, banners (`.circuit-banner`), and data viewports (`.json-box`).
- **Indicator Dot (`50%` / `{rounded.full}`)**: Applied strictly to the 8px circular live status indicator (`.status-indicator`).

---

## Components

### 1. Action Buttons (`.btn-action`)
- **Dimensions**: Fixed `34px` height, `0 16px` padding, `4px` border-radius, `12px` font size, `600` weight.
- **Primary Variant (`.btn-primary`)**:
  - Background: `{colors.accent}` (`#38BDF8`).
  - Text: `#080B11` (dark contrast).
  - Hover: `{colors.accent-hover}` (`#7DD3FC`).
  - Border: 1px solid `{colors.accent}`.
- **Secondary Variant (`.btn-secondary`)**:
  - Background: `{colors.canvas}` (`#080B11`).
  - Text: `{colors.text-secondary}` (`#CBD5E1`).
  - Border: 1px solid `{colors.border-subtle}` (`#1D2840`).
  - Hover: Background `{colors.surface-hover}` (`#141C2E`), text `{colors.text-primary}` (`#F8FAFC`), border `{colors.border-strong}` (`#2D3E63`).

### 2. Status Badge (`.status-badge`)
- **Dimensions**: `padding: 4px 10px`, `4px` radius, `12px` font size, `500` weight.
- **Online State**: Background `rgba(16, 185, 129, 0.12)`, text `#10B981`, border `1px solid rgba(16, 185, 129, 0.3)`.
- **Offline State (`.offline`)**: Background `{colors.canvas}`, text `{colors.text-muted}`, border `1px solid {colors.border-subtle}`.
- **Pulse Dot (`.status-indicator`)**: `8px` circle, background `currentColor`.

### 3. Metric Cells (`.metric-cell`)
- **Structure**: Recessed card (`#080B11`), `10px 14px` padding, `4px` radius, 1px subtle border.
- **Header**: Flex row with title (`12px`, weight 600, muted color) and status tag (`.metric-tag`).
- **Value**: Monospace tabular number (`18px`, weight 700, `#F8FAFC`).
- **Subtext**: Secondary context (`12px`, `#94A3B8`).

### 4. Participant Card (`.participant-card`)
- **Structure**: Inset list row (`10px 12px` padding, `4px` radius, transparent border).
- **Typography**: Agent name (`13px`, weight 600, `#F8FAFC`), role description (`12px`, `#94A3B8`).
- **Role Tag (`.badge-status`)**: Monospace badge (`12px`, `2px 8px` padding, `3px` radius, `#080B11` background).
- **Active State (`.active`)**: Background `#1C2740`, border `1px solid #2D3E63`.

### 5. Causal Turn Card (`.turn-card`)
- **Structure**: Surface container (`#0E1422`), `14px 16px` padding, `4px` radius, 1px subtle border.
- **Routing Header**:
  - Agents: Source and target names (`13px`, weight 600) joined by directional arrow SVG.
  - Telemetry Meta: Monospace timestamp, step duration (e.g. `380ms`), and token burn (e.g. `1,420 tokens`).
- **Payload Container (`.turn-body`)**: Inset dark box (`#080B11`), `12px 14px` padding, `4px` radius, 1px subtle border, `13px` secondary text, pre-wrap whitespace.
- **Selected State (`.selected`)**: Border `#38BDF8`, background `#1C2740`.

### 6. Circuit Breaker Alert Banner (`.circuit-banner`)
- **Structure**: Single flat warning card (`padding: 14px 16px`, `4px` radius).
- **Background**: `rgba(239, 68, 68, 0.08)`.
- **Border**: `1px solid #EF4444`.
- **Title**: Alert icon (triangle SVG) with headline (`14px`, weight 700, `#EF4444`).
- **Details**: Explanation of entropy collapse (`13px`, line-height 1.5, `#CBD5E1`).
- **Stats Row**: Tabular metrics row (`13px`, monospace, tnum, `#F8FAFC`) exposing exact tokens saved, dollar budget preserved, and swarm halt status.

### 7. Turn Inspector & Branching Drawer (`.inspector`)
- **Header**: Panel title ("Turn Inspector") and mode badge ("Time-Machine").
- **Property Rows (`.prop-row`)**: Label (`12px`, weight 600, `#94A3B8`) paired with monospace value container (`.prop-value`).
- **JSON Payload Box (`.json-box`)**: Monospace code view (`#080B11` background, `12px` font size, max-height `260px`, overflow scroll).
- **Branch Action**: Full-width secondary action button to fork execution state from the selected turn.

---

## Do's and Don'ts

### Do:
- **Do** format all numerical counters, latencies, timestamps, and entropy metrics with `font-feature-settings: "tnum" 1` in `JetBrains Mono`.
- **Do** maintain a flat, shadow-free console hierarchy delimited by 1px hairline borders (`#1D2840`).
- **Do** keep agent communication text cleanly readable inside inset `#080B11` code blocks with `pre-wrap` whitespace.
- **Do** reserve high-saturation red (`#EF4444`) exclusively for circuit-breaker trips, anomalies, and safety halts.
- **Do** provide immediate, actionable interventions (e.g., Time-Machine state forking) alongside every circuit-breaker alert.
- **Do** support full keyboard and cursor accessibility across all interactive turn cards and simulation buttons.

### Don't:
- **Don't** use marketing kickers, eyebrows, or tracked-out uppercase micro-headers.
- **Don't** apply text gradients, border gradients, or glossy backdrop blurs.
- **Don't** use emoji icons; use monochrome, single-stroke (1.5px) SVGs exclusively.
- **Don't** introduce rounded pills (`999px`) or large card corner radii (`> 4px`) for standard controls.
- **Don't** add decorative box-shadows or glowing halos that clutter high-density telemetry views.
- **Don't** contaminate the autonomous systems and software engineering domain with financial trading or crypto metaphors.
