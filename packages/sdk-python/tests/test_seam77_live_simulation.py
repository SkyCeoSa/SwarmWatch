# -*- coding: utf-8 -*-
"""
Pytest integration wrapper for Seam 77 (ADR 0088) Cross-Cluster State Synchronization Simulation.
"""

import os
import sys

# Ensure repository root is in sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.simulate_seam77_cross_cluster_sync import run_live_simulation


def test_seam77_live_simulation_full_invariants():
    """Verify all 8 operational invariants pass in live simulation."""
    result = run_live_simulation()
    assert result["status"] == "CERTIFIED"
    assert result["nodes_count"] >= 3
    assert result["edges_count"] >= 1
    assert result["receipt_hash"].startswith("sw:sync:v1:")
    assert result["duration_ms"] < 2000.0
