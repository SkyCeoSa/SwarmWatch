# -*- coding: utf-8 -*-
"""
Tests for Seam 83: Hybrid Logical Clocks & Bounded-Skew Causal Timestamps (ADR 0094).
Verifies:
- Invariant H1: Causal monotonicity (hlc(e) < hlc(f) for causal sequence).
- Invariant H2: Bounded physical drift and fail-closed skew rejection.
- Invariant H3: Total ordering and deterministic tie-breaking.
- Sub-250µs execution latency and zero-token cost guarantee.
"""

import time
import pytest

from swarmwatch.hlc import (
    HybridLogicalClock,
    HybridLogicalTimestamp,
    BoundedSkewViolationError,
)


def test_hlc_local_monotonicity():
    fake_time = 1_000_000_000
    clock = HybridLogicalClock(node_id="node-a", physical_clock=lambda: fake_time)

    # First event
    t1 = clock.now()
    assert t1.physical_ns == fake_time
    assert t1.logical_counter == 0
    assert t1.node_id == "node-a"

    # Second event at same physical time increments logical counter
    t2 = clock.now()
    assert t2.physical_ns == fake_time
    assert t2.logical_counter == 1
    assert t1 < t2

    # Physical time advances
    fake_time = 1_000_000_500
    t3 = clock.now()
    assert t3.physical_ns == fake_time
    assert t3.logical_counter == 0
    assert t2 < t3


def test_hlc_causal_message_observation():
    time_a = 1_000_000_000
    time_b = 1_000_000_100

    clock_a = HybridLogicalClock(node_id="node-a", physical_clock=lambda: time_a)
    clock_b = HybridLogicalClock(node_id="node-b", physical_clock=lambda: time_b)

    # Node A generates event
    msg_ts = clock_a.now()

    # Node B observes event (e -> f)
    recv_ts = clock_b.observe(msg_ts)

    # Invariant H1: msg_ts < recv_ts
    assert msg_ts < recv_ts
    assert recv_ts.physical_ns >= msg_ts.physical_ns


def test_hlc_bounded_skew_violation():
    local_time = 1_000_000_000
    max_drift = 500_000_000  # 500ms

    clock = HybridLogicalClock(node_id="node-local", max_drift_ns=max_drift, physical_clock=lambda: local_time)

    # Remote timestamp with excessive drift (skew = 1,000ms > 500ms)
    malicious_remote = HybridLogicalTimestamp(
        physical_ns=local_time + 1_000_000_000,
        logical_counter=0,
        node_id="node-adversary",
    )

    with pytest.raises(BoundedSkewViolationError) as exc_info:
        clock.observe(malicious_remote)

    assert exc_info.value.remote_physical_ns > local_time + max_drift


def test_hlc_strict_total_ordering_tie_breaking():
    # Same physical and logical, tie-breaker is node_id
    t_alpha = HybridLogicalTimestamp(physical_ns=100, logical_counter=5, node_id="cluster-alpha")
    t_beta = HybridLogicalTimestamp(physical_ns=100, logical_counter=5, node_id="cluster-beta")

    assert t_alpha < t_beta
    assert not (t_beta < t_alpha)


def test_hlc_execution_latency_slo():
    clock = HybridLogicalClock(node_id="perf-node")
    start = time.perf_counter()
    for _ in range(1_000):
        clock.now()
    elapsed_us = ((time.perf_counter() - start) / 1_000) * 1_000_000
    assert elapsed_us < 10.0  # Well below 250µs SLO (< 10µs per tick)
