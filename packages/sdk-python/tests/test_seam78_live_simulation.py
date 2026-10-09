# -*- coding: utf-8 -*-
"""
End-to-end regression test for Seam 78 live console simulation:
Autonomous Distributed Hyper-Graph Partition Healing & Causal Vector Resumption (ADR 0089).
"""

import os
import sys
import pytest

# Ensure repository root is in sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.simulate_seam78_partition_healing import run_live_simulation


def test_seam78_live_simulation_end_to_end():
    result = run_live_simulation()
    assert result["status"] == "CERTIFIED"
    assert result["nodes_count"] >= 3
    assert result["edges_count"] >= 1
    assert result["receipt_hash"].startswith("sw:heal:v1:")
    assert result["duration_ms"] > 0
