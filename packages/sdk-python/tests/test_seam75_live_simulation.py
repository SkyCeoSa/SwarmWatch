# -*- coding: utf-8 -*-
"""
Live simulation test wrapper for Seam 75 (ADR 0086):
Zero-Knowledge State Transition Rollups & Succinct Proof Verification.
"""

from scripts.simulate_seam75_zk_rollup import run_live_simulation


def test_seam75_live_simulation_suite():
    """Verify live simulation runs to completion and certifies all 8 invariants."""
    res = run_live_simulation()
    assert res["status"] == "PASSED"
    assert res["transitions_rolled_up"] == 50
    assert res["queue_latency_us"] < 250.0
    assert res["verify_latency_us"] < 250.0
    assert res["circuit_latched"] is True
    assert res["receipt_hash"].startswith("sw:rollup:v1:")
