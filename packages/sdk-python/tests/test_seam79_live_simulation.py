# -*- coding: utf-8 -*-
"""
End-to-end regression test for Seam 79 live console simulation:
Dynamic Topology Re-Partitioning & Adaptive Sub-Graph Migration (ADR 0090).
"""

import os
import sys
import pytest

# Ensure repository root is in sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.simulate_seam79_subgraph_migration import run_live_simulation


def test_seam79_live_simulation_end_to_end():
    result = run_live_simulation()
    assert result["status"] == "CERTIFIED"
    assert result["migrated_nodes"] >= 2
    assert result["migrated_edges"] >= 1
    assert result["receipt_hash"].startswith("sw:mig:rcpt:v1:")
    assert result["duration_ms"] > 0
