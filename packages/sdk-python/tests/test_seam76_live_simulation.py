# -*- coding: utf-8 -*-
"""
Pytest integration wrapper for Seam 76 (ADR 0087) Epistemic Proof Arbitration Simulation.
"""

import os
import sys

# Ensure repository root is in sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.simulate_seam76_epistemic_arbitration import run_live_simulation


def test_seam76_live_simulation_full_invariants():
    """Verify all 8 operational invariants pass in live simulation."""
    result = run_live_simulation()
    assert result["status"] == "CERTIFIED"
    assert result["total_claims"] > 0
    assert result["total_disputes"] >= 2
    assert result["total_receipts"] >= 2
    assert result["circuit_tripped"] is True
    assert result["elapsed_ms"] < 1000.0
