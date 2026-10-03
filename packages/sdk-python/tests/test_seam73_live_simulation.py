# -*- coding: utf-8 -*-
"""
Test Suite for Seam 73 Live Console Simulation (ADR 0084).
Multi-Modal Cognitive Grounding & Vector Evidence Fusion Engine.
"""

import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent


def test_seam73_live_console_simulation():
    """Runs scripts/simulate_seam73_cognitive_grounding.py and verifies all 8 invariants pass."""
    sim_script = ROOT_DIR / "scripts" / "simulate_seam73_cognitive_grounding.py"
    res = subprocess.run(
        [sys.executable, str(sim_script)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, f"Simulation failed with output:\n{res.stdout}\n{res.stderr}"
    assert "ALL 8/8 INVARIANTS CERTIFIED PASS (100%)" in res.stdout
    assert "[PHASE 1]" in res.stdout
    assert "[PHASE 8]" in res.stdout
