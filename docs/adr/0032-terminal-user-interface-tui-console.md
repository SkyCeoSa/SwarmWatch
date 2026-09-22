# ADR 0032: Live Terminal User Interface (TUI) & Headless Cluster Observability Console (`swarmwatch top`)

## Status
Accepted

## Context
Following the delivery of cloud-native Kubernetes deployment (ADR 0031) and enterprise monitoring topologies, SwarmWatch is increasingly deployed in remote Linux servers, Kubernetes worker pods, and SSH bastion hosts where graphical web browsers are inaccessible.

When autonomous multi-agent swarms experience runaway loops, recursive tool invocations, or semantic entropy collapse in these headless production environments, SREs and developers must quickly diagnose and triage active swarms. Relying solely on raw log scraping or piping `curl` commands through `jq` is slow, error-prone, and hinders rapid incident mitigation.

Developers need an interactive, high-frequency, zero-overhead terminal user interface (similar in developer ergonomics to `htop`, `k9s`, or `btop`) that provides instant real-time visualization of active swarms, recent causal agent turns, Shannon entropy plunge warnings, and circuit breaker trip interventions directly within the terminal shell.

## Decision
We introduce **Seam 21**: Live Terminal User Interface (TUI) & Headless Cluster Observability Console (`swarmwatch top`), powered by `TerminalMonitor` in `packages/sdk-python/swarmwatch/tui.py`.

### Architectural Invariants:
1. **Zero-Token Cost Guarantee**:
   All state formatting, entropy score presentation, and counterfactual ROI metrics are evaluated locally in-process with 0 remote LLM tokens consumed.
2. **Pure Standard Library Engine**:
   Zero third-party terminal dependencies (no heavy `curses` or `textual` wheels). Implemented with Python's standard library (`urllib.request`, `shutil.get_terminal_size`, `time`, `os`) ensuring instantaneous startup, zero install friction, and cross-platform fidelity on both Windows and POSIX operating systems.
3. **Dual Data Sourcing**:
   `TerminalMonitor` queries either a live running SwarmWatch HTTP server (`/api/v1/health`, `/api/v1/swarms`, `/api/v1/swarms/{id}/spans`, `/api/v1/budget`, `/metrics`) or directly queries a local SQLite database (`swarmwatch.db`) when running in standalone mode or when the server is offline.
4. **Dual Operational Modes**:
   - *Live Interactive TUI* (Default): Alternate screen buffer (`\033[?1049h`), periodic auto-refresh, keyboard triage (`q`, `r`, `p`, `s`, `h`), and terminal resize adaptation.
   - *Snapshot Mode* (`--once`): Prints a single formatted frame directly to standard output without clearing the screen, enabling automated CI validation, cron health probes, and shell pipes (`swarmwatch top --once | grep HALTED`).
5. **Impeccable Monospace Craft Floor**:
   Austere developer console aesthetics matching SwarmWatch design tokens: `#080B11` slate background, instrument cyan accents, emerald green running status, crimson circuit halts, amber warning indicators, tabular numerals (`tnum`), clean ASCII box lines, and zero emojis.

## Consequences
- SREs and engineers can triage production multi-agent incidents inside `kubectl exec`, SSH bastions, and Docker containers without forwarding ports or opening a browser.
- Provides a headless companion to the graphical `dashboard.html` web console.
- Extends the core storage interface with `list_swarms` across SQLite and ClickHouse backends.
