# -*- coding: utf-8 -*-
"""
Tests for Loss-Aware Causal Ingestion & Evidentiary Completeness Rule (Seam 53 / ADR 0064).
"""

import pytest
from starlette.testclient import TestClient

from swarmwatch.causal_ingress import (
    CausalIngress,
    IngressPriority,
    CompletenessStatus,
    SequenceGap,
    IngressResult,
    CompletenessReport,
    global_causal_ingress,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


def test_monotonic_stream_sequencing():
    """Verify strictly monotonic sequence allocation per stream."""
    ingress = CausalIngress(max_capacity=100)
    
    # Stream A
    r1 = ingress.submit({"span_id": "s1", "swarm_id": "sw1"}, stream_id="streamA")
    r2 = ingress.submit({"span_id": "s2", "swarm_id": "sw1"}, stream_id="streamA")
    r3 = ingress.submit({"span_id": "s3", "swarm_id": "sw1"}, stream_id="streamA")
    
    assert r1.seq_num == 1
    assert r2.seq_num == 2
    assert r3.seq_num == 3
    assert r1.accepted and r2.accepted and r3.accepted
    assert not r1.gap_detected and not r2.gap_detected and not r3.gap_detected
    
    # Stream B operates independently
    rb1 = ingress.submit({"span_id": "sb1", "swarm_id": "sw1"}, stream_id="streamB")
    assert rb1.seq_num == 1


def test_normal_priority_headroom_shedding():
    """Normal priority spans are shed once normal_threshold is reached (80%)."""
    # capacity = 10, reserve = 2 (20%), normal_threshold = 8
    ingress = CausalIngress(max_capacity=10, reserve_ratio=0.2)
    
    for i in range(8):
        res = ingress.submit({"span_id": f"s_{i}", "swarm_id": "sw1"}, priority=IngressPriority.NORMAL)
        assert res.accepted is True
        assert res.status == "ACCEPTED"
        
    # 9th normal span should be shed
    res_shed = ingress.submit({"span_id": "s_shed", "swarm_id": "sw1"}, priority=IngressPriority.NORMAL)
    assert res_shed.accepted is False
    assert res_shed.status == "DROPPED_SATURATION"
    assert "Normal priority shed" in (res_shed.reason or "")
    
    rep = ingress.completeness("sw1")
    assert rep.total_submitted == 9
    assert rep.total_accepted == 8
    assert rep.total_dropped == 1
    assert rep.loss_ratio > 0.0
    assert rep.status == CompletenessStatus.SATURATED


def test_critical_priority_headroom_guarantee():
    """Critical safety interventions are 100% delivered even under saturation."""
    ingress = CausalIngress(max_capacity=10, reserve_ratio=0.2)
    
    # Fill normal threshold
    for i in range(8):
        ingress.submit({"span_id": f"s_{i}", "swarm_id": "sw1"}, priority=IngressPriority.NORMAL)
        
    # 9th normal drops
    ingress.submit({"span_id": "s_dropped", "swarm_id": "sw1"}, priority=IngressPriority.NORMAL)
    
    # Submit critical spans (e.g. circuit breaker trip)
    c1 = ingress.submit({"span_id": "crit_1", "swarm_id": "sw1"}, priority=IngressPriority.CRITICAL)
    c2 = ingress.submit({"span_id": "crit_2", "swarm_id": "sw1"}, priority=IngressPriority.CRITICAL)
    
    assert c1.accepted is True
    assert c2.accepted is True
    
    rep = ingress.completeness("sw1")
    assert rep.critical_submitted == 2
    assert rep.critical_accepted == 2
    assert rep.critical_delivery_ratio == 1.0


def test_sequence_gap_detection_on_drop():
    """Detects sequence gaps when dropped spans cause non-contiguous arrival."""
    ingress = CausalIngress(max_capacity=5, reserve_ratio=0.2)
    # normal_threshold = 4
    for i in range(4):
        ingress.submit({"span_id": f"s_{i}", "swarm_id": "sw_gap"}, stream_id="main")
        
    # 5th normal span dropped (seq 5 dropped)
    d5 = ingress.submit({"span_id": "s_5", "swarm_id": "sw_gap"}, priority=IngressPriority.NORMAL, stream_id="main")
    assert d5.accepted is False
    assert d5.seq_num == 5
    
    # 6th span is CRITICAL (accepted with seq 6)
    c6 = ingress.submit({"span_id": "s_6", "swarm_id": "sw_gap"}, priority=IngressPriority.CRITICAL, stream_id="main")
    assert c6.accepted is True
    assert c6.seq_num == 6
    assert c6.gap_detected is True
    assert c6.gap is not None
    assert c6.gap.expected_seq == 5
    assert c6.gap.received_seq == 6
    assert c6.gap.missing_count == 1
    
    rep = ingress.completeness("sw_gap")
    assert len(rep.sequence_gaps) == 1
    assert rep.sequence_gaps[0]["missing_count"] == 1


def test_causal_parent_evidentiary_completeness():
    """Unanchored causal parent references result in INCOMPLETE trace status."""
    ingress = CausalIngress(max_capacity=100)
    
    # Span 1 is root
    ingress.submit({"span_id": "root_01", "swarm_id": "sw_parent"})
    
    # Span 2 references existing root
    ingress.submit({"span_id": "child_01", "swarm_id": "sw_parent", "parent_span_ids": ["root_01"]})
    
    rep1 = ingress.completeness("sw_parent")
    assert rep1.status == CompletenessStatus.COMPLETE
    assert len(rep1.missing_causal_parents) == 0
    
    # Span 3 references non-existent / dropped parent
    ingress.submit({"span_id": "child_02", "swarm_id": "sw_parent", "parent_span_ids": ["ghost_parent_99"]})
    
    rep2 = ingress.completeness("sw_parent")
    assert rep2.status == CompletenessStatus.INCOMPLETE
    assert "ghost_parent_99" in rep2.missing_causal_parents


def test_fifo_drain_ordering():
    """Buffer drains items in exact FIFO sequence."""
    ingress = CausalIngress(max_capacity=50)
    for i in range(5):
        ingress.submit({"span_id": f"spn_{i}", "swarm_id": "sw_fifo"})
        
    drained = ingress.drain(batch_size=3)
    assert len(drained) == 3
    assert drained[0]["span_id"] == "spn_0"
    assert drained[1]["span_id"] == "spn_1"
    assert drained[2]["span_id"] == "spn_2"
    
    remaining = ingress.drain(batch_size=10)
    assert len(remaining) == 2
    assert remaining[0]["span_id"] == "spn_3"
    assert remaining[1]["span_id"] == "spn_4"
    
    empty = ingress.drain(batch_size=10)
    assert len(empty) == 0


def test_server_endpoints_ingress():
    """Verify REST endpoints for causal ingress."""
    global_causal_ingress.reset()
    app = create_app()
    client = TestClient(app)
    
    # 1. Submit span
    res = client.post(
        "/api/v1/ingress/submit",
        json={
            "span": {"span_id": "spn_srv_1", "swarm_id": "sw_srv", "payload": "server test"},
            "priority": "HIGH",
            "stream_id": "srv_stream",
        }
    )
    assert res.status_code == 200
    data = res.json()
    assert data["accepted"] is True
    assert data["seq_num"] == 1
    assert data["status"] == "ACCEPTED"
    
    # 2. Ingress status
    status_res = client.get("/api/v1/ingress/status?swarm_id=sw_srv")
    assert status_res.status_code == 200
    sdata = status_res.json()
    assert sdata["queue_length"] >= 1
    assert sdata["completeness_status"] == CompletenessStatus.COMPLETE
    
    # 3. Completeness report
    comp_res = client.get("/api/v1/ingress/completeness/sw_srv")
    assert comp_res.status_code == 200
    cdata = comp_res.json()
    assert cdata["total_accepted"] == 1
    assert cdata["loss_ratio"] == 0.0
    
    # 4. Drain buffer
    drain_res = client.post("/api/v1/ingress/drain", json={"batch_size": 10})
    assert drain_res.status_code == 200
    ddata = drain_res.json()
    assert ddata["count"] == 1
    assert ddata["drained_spans"][0]["span_id"] == "spn_srv_1"


def test_cli_ingress_subcommands(capsys):
    """Verify CLI dispatch for ingress submit, completeness, and status."""
    global_causal_ingress.reset()
    
    # 1. CLI status
    with pytest.raises(SystemExit) as exc1:
        main(["ingress", "status", "--format", "json"])
    assert exc1.value.code == 0
    out1 = capsys.readouterr().out
    assert "queue_length" in out1
    
    # 2. CLI submit
    with pytest.raises(SystemExit) as exc2:
        main(["ingress", "submit", "--span-id", "spn_cli_01", "--swarm-id", "sw_cli", "--format", "json"])
    assert exc2.value.code == 0
    out2 = capsys.readouterr().out
    assert '"accepted": true' in out2
    
    # 3. CLI completeness
    with pytest.raises(SystemExit) as exc3:
        main(["ingress", "completeness", "--swarm-id", "sw_cli", "--format", "json"])
    assert exc3.value.code == 0
    out3 = capsys.readouterr().out
    assert '"status": "COMPLETE"' in out3
