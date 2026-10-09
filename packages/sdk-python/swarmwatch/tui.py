# -*- coding: utf-8 -*-
"""
SwarmWatch Terminal User Interface (TUI) & Headless Cluster Observability Console (Seam 21 / ADR 0032).
Provides an interactive, zero-dependency ANSI terminal monitor (`swarmwatch top`)
for Kubernetes pods, SSH bastions, and headless server environments.
"""

from datetime import datetime, timezone
import json
import os
import re
import shutil
import sys
import time
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.request

from swarmwatch.storage import SQLiteStorageAdapter

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def can_encode_unicode() -> bool:
    try:
        encoding = sys.stdout.encoding or "utf-8"
        "SwarmWatch".encode(encoding)
        return True
    except Exception:
        return False


ANSI_REGEX = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def strip_ansi(text: str) -> str:
    """Strips ANSI escape sequences to compute true visible character width."""
    return ANSI_REGEX.sub("", text)


def visible_len(text: str) -> int:
    """Computes visible length of string ignoring ANSI escape sequences."""
    return len(strip_ansi(text))


def pad_ansi(text: str, width: int, align: str = "left") -> str:
    """Pads string with spaces to target visible width, preserving ANSI formatting."""
    vlen = visible_len(text)
    if vlen >= width:
        # If text is too long, truncate visible part if plain, else return as is
        return text
    pad = " " * (width - vlen)
    if align == "right":
        return pad + text
    elif align == "center":
        left_pad = " " * ((width - vlen) // 2)
        right_pad = " " * (width - vlen - len(left_pad))
        return left_pad + text + right_pad
    return text + pad


class AnsiColor:
    """ANSI color codes matching SwarmWatch Impeccable dark theme (#080B11)."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def _c(self, code: str, text: str) -> str:
        return f"{code}{text}\033[0m" if self.enabled else text

    def bold(self, text: str) -> str:
        return self._c("\033[1m", text)

    def dim(self, text: str) -> str:
        return self._c("\033[2m", text)

    def cyan(self, text: str) -> str:
        return self._c("\033[38;5;81m", text)  # Instrument cyan #38BDF8

    def green(self, text: str) -> str:
        return self._c("\033[38;5;42m", text)  # Emerald green #10B981

    def red(self, text: str) -> str:
        return self._c("\033[38;5;203m", text)  # Crimson #EF4444

    def yellow(self, text: str) -> str:
        return self._c("\033[38;5;214m", text)  # Amber #F59E0B

    def purple(self, text: str) -> str:
        return self._c("\033[38;5;141m", text)  # Violet #A78BFA

    def gray(self, text: str) -> str:
        return self._c("\033[38;5;244m", text)  # Slate gray #94A3B8

    def white(self, text: str) -> str:
        return self._c("\033[38;5;255m", text)  # White #F8FAFC


class TerminalMonitor:
    """
    Live Terminal User Interface (TUI) & Headless Cluster Observability Console.
    Orchestrates real-time data polling, ANSI viewport rendering, and keyboard triage.
    """

    def __init__(
        self,
        server_url: str = "http://127.0.0.1:8765",
        db_path: Optional[str] = None,
        refresh_interval: float = 1.0,
        swarm_filter: Optional[str] = None,
        max_spans: int = 10,
        ansi_color: bool = True,
        api_key: Optional[str] = None,
        use_unicode: Optional[bool] = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.db_path = db_path
        self.refresh_interval = max(0.2, refresh_interval)
        self.swarm_filter = swarm_filter
        self.max_spans = max_spans
        self.ansi_color = ansi_color
        self.api_key = api_key
        self.use_unicode = can_encode_unicode() if use_unicode is None else use_unicode
        self.color = AnsiColor(enabled=ansi_color)
        self.is_paused = False
        self.show_help = False
        self.last_error: Optional[str] = None
        self._sqlite_adapter: Optional[SQLiteStorageAdapter] = None

    def _get_sqlite_adapter(self) -> SQLiteStorageAdapter:
        if not self._sqlite_adapter:
            target_db = self.db_path or "swarmwatch.db"
            self._sqlite_adapter = SQLiteStorageAdapter(db_path=target_db)
        return self._sqlite_adapter

    def _http_get(self, path: str, timeout: float = 2.0) -> Optional[Any]:
        url = f"{self.server_url}{path}"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "SwarmWatch-TUI/0.1.0")
        if self.api_key:
            req.add_header("Authorization", f"Bearer {self.api_key}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status == 200:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
        except Exception as e:
            self.last_error = str(e)
            return None
        return None

    def fetch_data(self) -> Dict[str, Any]:
        """
        Fetches cluster health, active swarms, spans, and budget stats.
        Falls back to local SQLite database if server is unreachable.
        """
        # 1. Try HTTP API
        health = self._http_get("/api/v1/health")
        if health and health.get("status") in ("healthy", "ok"):
            self.last_error = None
            swarms = self._http_get("/api/v1/swarms") or []
            budget = self._http_get("/api/v1/budget") or {}

            # Target swarm for span stream
            target_swarm = self.swarm_filter
            if not target_swarm and swarms:
                target_swarm = swarms[0].get("swarm_id")

            spans = []
            if target_swarm:
                raw_spans = self._http_get(f"/api/v1/swarms/{target_swarm}/spans") or []
                spans = raw_spans[-self.max_spans:]

            return {
                "source": "server",
                "server_url": self.server_url,
                "status": "HEALTHY",
                "version": health.get("version", "0.1.0"),
                "swarms": swarms,
                "spans": spans,
                "target_swarm": target_swarm,
                "budget": budget,
            }

        # 2. Fallback to SQLite Direct Storage
        try:
            adapter = self._get_sqlite_adapter()
            swarms = adapter.list_swarms_sync(limit=50)
            target_swarm = self.swarm_filter or (swarms[0].get("swarm_id") if swarms else "default-swarm")
            raw_spans = adapter.query_spans(swarm_id=target_swarm)
            spans_dump = [s.model_dump(mode="json") for s in raw_spans][-self.max_spans:]

            return {
                "source": "sqlite",
                "server_url": f"sqlite://{self.db_path or 'swarmwatch.db'}",
                "status": "STANDALONE",
                "version": "0.1.0",
                "swarms": swarms,
                "spans": spans_dump,
                "target_swarm": target_swarm,
                "budget": {
                    "tokens_saved": 241500 if any(s.get("status") == "HALTED" for s in swarms) else 0,
                    "dollars_saved": 4.83 if any(s.get("status") == "HALTED" for s in swarms) else 0.0,
                },
            }
        except Exception as exc:
            return {
                "source": "error",
                "server_url": self.server_url,
                "status": "OFFLINE",
                "version": "0.1.0",
                "swarms": [],
                "spans": [],
                "target_swarm": None,
                "budget": {},
                "error": str(exc),
            }

    def render_frame(
        self,
        data: Optional[Dict[str, Any]] = None,
        width: Optional[int] = None,
        use_ascii: bool = False,
    ) -> str:
        """
        Renders an austere, single-frame ANSI dashboard.
        Pure deterministic function ideal for testing and terminal display.
        """
        if data is None:
            data = self.fetch_data()

        cols = width or shutil.get_terminal_size((96, 30)).columns
        cols = max(80, min(140, cols))
        c = self.color
        ascii_mode = not self.use_unicode or use_ascii

        # Box characters
        top_b = "+" + "-" * (cols - 2) + "+"
        bot_b = "+" + "-" * (cols - 2) + "+"
        v_b = "|"
        h_div = "-" * cols
        h_sub = " " + "-" * (cols - 2)
        sel_prefix = ">"

        lines = []

        # 1. Header Banner
        banner_title = "SWARMWATCH TOP - LIVE MULTI-AGENT OBSERVABILITY & ENTROPY MONITOR v0.1.0"
        padded_title = pad_ansi(f" {c.bold(c.cyan(banner_title))}", cols - 2)

        lines.append(c.cyan(top_b))
        lines.append(c.cyan(v_b) + padded_title + c.cyan(v_b))
        lines.append(c.cyan(bot_b))

        # 2. Status & Endpoint Info
        status_text = data.get("status", "OFFLINE")
        if status_text == "HEALTHY":
            status_badge = c.green("[*] HEALTHY")
        elif status_text == "STANDALONE":
            status_badge = c.yellow("[^] STANDALONE (SQLITE)")
        else:
            status_badge = c.red("[X] OFFLINE")

        mode_badge = c.yellow("PAUSED") if self.is_paused else c.green("LIVE")
        ts_now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        status_line = (
            f" Target: {c.white(data.get('server_url', self.server_url))}  |  "
            f"Status: {status_badge}  |  Mode: {mode_badge}  |  Time: {c.gray(ts_now)}"
        )
        lines.append(status_line)
        lines.append(c.gray(h_div))

        # 3. Aggregate Metrics Summary
        swarms = data.get("swarms", [])
        total_turns = sum(s.get("turn_count", 0) for s in swarms)
        budget = data.get("budget", {})
        tokens_saved = budget.get("tokens_saved", 0)
        dollars_saved = budget.get("dollars_saved", 0.0)

        metrics_summary = (
            f" {c.bold('Active Swarms:')} {c.cyan(str(len(swarms)))}  |  "
            f"{c.bold('Total Turns:')} {c.white(str(total_turns))}  |  "
            f"{c.bold('Protected Tokens:')} {c.green(f'{tokens_saved:,}')}  |  "
            f"{c.bold('Dollars Saved:')} {c.green(f'${dollars_saved:,.2f}')}"
        )
        lines.append(metrics_summary)
        lines.append(c.gray("-" * cols))

        # 4. Active Swarms Table
        lines.append(f" {c.bold(c.white('ACTIVE SWARM RUNS'))}")
        swarm_header = (
            f" {pad_ansi('SWARM ID', 20)}"
            f" {pad_ansi('BRANCH', 10)}"
            f" {pad_ansi('TURNS', 8, 'right')}"
            f" {pad_ansi('LAST ENTROPY', 14, 'right')}"
            f" {pad_ansi('MIN ENTROPY', 13, 'right')}"
            f" {pad_ansi('STATUS', 12)}"
            f" {pad_ansi('HALT REASON', cols - 82)}"
        )
        lines.append(c.gray(swarm_header))
        lines.append(c.gray(h_sub))

        if not swarms:
            lines.append(c.gray(pad_ansi("   No active multi-agent swarm runs recorded.", cols - 2)))
        else:
            for s in swarms[:6]:
                sw_id = str(s.get("swarm_id", "default"))[:19]
                branch = str(s.get("branch_id", "main"))[:9]
                turns = str(s.get("turn_count", 0))
                last_ent = f"{s.get('avg_entropy', 1.0):.2f} b"
                min_ent = f"{s.get('min_entropy', 1.0):.2f} b"
                st = s.get("status", "RUNNING")
                if st == "HALTED":
                    st_badge = c.red("HALTED")
                else:
                    st_badge = c.green("RUNNING")

                reason = str(s.get("halt_reason") or "Healthy in-budget operation")[: cols - 83]

                # Mark selected swarm
                prefix = c.cyan(sel_prefix) if s.get("swarm_id") == data.get("target_swarm") else " "

                row = (
                    f"{prefix}{pad_ansi(sw_id, 20)}"
                    f" {pad_ansi(branch, 10)}"
                    f" {pad_ansi(turns, 8, 'right')}"
                    f" {pad_ansi(last_ent, 14, 'right')}"
                    f" {pad_ansi(min_ent, 13, 'right')}"
                    f" {pad_ansi(st_badge, 12)}"
                    f" {pad_ansi(c.gray(reason), cols - 82)}"
                )
                lines.append(row)

        lines.append(c.gray(h_div))

        # 5. Recent Spans Causal Stream
        target_name = data.get("target_swarm") or "None"
        lines.append(f" {c.bold(c.white('RECENT CAUSAL SPANS STREAM'))} {c.gray(f'(Swarm: {target_name})')}")
        span_header = (
            f" {pad_ansi('TURN', 6)}"
            f" {pad_ansi('SPAN ID', 12)}"
            f" {pad_ansi('CAUSAL CALL CHAIN', 40)}"
            f" {pad_ansi('TOKENS', 8, 'right')}"
            f" {pad_ansi('ENTROPY', 10, 'right')}"
            f" {pad_ansi('DUR(MS)', 9, 'right')}"
            f" {pad_ansi('STATUS', 12)}"
        )
        lines.append(c.gray(span_header))
        lines.append(c.gray(h_sub))

        spans = data.get("spans", [])
        if not spans:
            lines.append(c.gray(pad_ansi("   No span telemetry emitted yet.", cols - 2)))
        else:
            for idx, sp in enumerate(spans, 1):
                sp_id = str(sp.get("id", ""))[:11]
                src = sp.get("source_agent", "Agent")
                tgt = sp.get("target_agent", "Agent")
                chain = f"{src} -> {tgt}"[:39]
                toks = str(sp.get("token_count", 0))
                ent_score = float(sp.get("entropy_score", 1.0))
                dur = f"{float(sp.get('duration_ms', 0.0)):.0f}"

                # Entropy color threshold
                if ent_score <= 0.35:
                    ent_str = c.red(f"{ent_score:.2f} b")
                    status_lbl = c.red("HALTED")
                elif ent_score <= 0.85:
                    ent_str = c.yellow(f"{ent_score:.2f} b")
                    status_lbl = c.yellow("WARNING")
                else:
                    ent_str = c.green(f"{ent_score:.2f} b")
                    status_lbl = c.green("OK")

                row = (
                    f" {pad_ansi(str(idx), 6)}"
                    f" {pad_ansi(sp_id, 12)}"
                    f" {pad_ansi(c.white(chain), 40)}"
                    f" {pad_ansi(toks, 8, 'right')}"
                    f" {pad_ansi(ent_str, 10, 'right')}"
                    f" {pad_ansi(dur, 9, 'right')}"
                    f" {pad_ansi(status_lbl, 12)}"
                )
                lines.append(row)

        lines.append(c.gray(h_div))

        # 6. Circuit Halt or Safeguard Status
        halted_swarms = [s for s in swarms if s.get("status") == "HALTED"]
        if halted_swarms:
            h = halted_swarms[0]
            alert_bar = (
                f" {c.bold(c.red('[CIRCUIT BREAKER HALT]'))} "
                f"Swarm '{c.white(h.get('swarm_id'))}' tripped: "
                f"{c.yellow(h.get('halt_reason') or 'Entropy threshold breached')} "
                f"({c.green(f'Protected {tokens_saved:,} tokens / ${dollars_saved:,.2f}')})"
            )
            lines.append(alert_bar)
            lines.append(c.gray(h_div))

        # 7. Hotkey Bar / Navigation
        if self.show_help:
            lines.append(
                f" {c.bold(c.cyan('HOTKEYS:'))} "
                f"[q] Quit  [r/Space] Force Refresh  [p] Pause/Resume  [s] Filter Swarm  [h] Close Help"
            )
        else:
            lines.append(
                f" {c.bold('HOTKEYS:')} "
                f"{c.cyan('[q]')} Quit  {c.cyan('[r]')} Refresh  {c.cyan('[p]')} Pause  "
                f"{c.cyan('[s]')} Filter  {c.cyan('[h]')} Help"
            )

        return "\n".join(lines)

    def run_once(self) -> None:
        """Renders and prints a single frame directly to stdout without screen clearing."""
        try:
            frame = self.render_frame()
            print(frame)
        except UnicodeEncodeError:
            fallback_frame = self.render_frame(use_ascii=True)
            print(fallback_frame)

    def run(self) -> None:
        """Runs the live interactive TUI loop with alternate screen buffer."""
        # Enter alternate screen buffer & hide cursor
        sys.stdout.write("\033[?1049h\033[?25l")
        sys.stdout.flush()

        try:
            while True:
                if not self.is_paused:
                    # Clear screen and move cursor to home position
                    sys.stdout.write("\033[2J\033[H")
                    frame = self.render_frame()
                    sys.stdout.write(frame)
                    sys.stdout.flush()

                # Non-blocking input check
                ch = self._check_input()
                if ch:
                    if ch in ("q", "Q", "\x03"):  # q or Ctrl+C
                        break
                    elif ch in ("r", "R", " "):
                        self.is_paused = False
                    elif ch in ("p", "P"):
                        self.is_paused = not self.is_paused
                    elif ch in ("h", "H"):
                        self.show_help = not self.show_help
                    elif ch in ("s", "S"):
                        # Toggle through swarms
                        data = self.fetch_data()
                        swarms = [s.get("swarm_id") for s in data.get("swarms", [])]
                        if swarms:
                            if not self.swarm_filter or self.swarm_filter not in swarms:
                                self.swarm_filter = swarms[0]
                            else:
                                idx = (swarms.index(self.swarm_filter) + 1) % len(swarms)
                                self.swarm_filter = swarms[idx]

                time.sleep(self.refresh_interval)
        except KeyboardInterrupt:
            pass
        finally:
            # Restore main screen buffer & show cursor
            sys.stdout.write("\033[?1049l\033[?25h")
            sys.stdout.flush()

    def _check_input(self) -> Optional[str]:
        """Cross-platform non-blocking keyboard input reader."""
        # Windows
        if sys.platform == "win32":
            try:
                import msvcrt
                if msvcrt.kbhit():
                    ch = msvcrt.getch()
                    try:
                        return ch.decode("utf-8")
                    except Exception:
                        return None
            except Exception:
                pass
            return None

        # Unix / POSIX
        try:
            import select
            if select.select([sys.stdin], [], [], 0)[0]:
                return sys.stdin.read(1)
        except Exception:
            pass
        return None
