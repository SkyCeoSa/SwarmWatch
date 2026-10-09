# -*- coding: utf-8 -*-
"""
End-to-end regression test for Seam 80 live console simulation:
Autonomous Cross-Cluster Causality Barrier & Multi-Swarm Epoch Cut-Over Synchronization (ADR 0091).
"""

import os
import sys
import pytest

# Ensure repository root is in sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.simulate_seam80_causality_barrier import run_live_simulation


def test_seam80_live_simulation_end_to_end():
    result = run_live_simulation()
    assert result["success"] is True
    assert result["epoch"] == 2
    assert result["cert_valid"] is True
    assert result["rcpt_valid"] is True
    assert result["calc_us"] < 250.0
    assert result["arm_us"] < 250.0
    assert result["cut_us"] < 250.0
