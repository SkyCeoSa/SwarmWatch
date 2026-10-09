"""
Tests for SwarmWatch Multi-Cloud Byzantine Partition Tolerant Gossip Membership Protocol (Seam 96 / ADR 0107).
Verifies BGM1 (Authenticated Heartbeats), BGM2 (Quorum Suspicion & Refutation),
BGM3 (Anti-Entropy Partition Healing), BGM4 (Equivocation Quarantine & Circuit Latching),
and sub-250 microsecond execution latency SLO.
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.hlc import HybridLogicalClock
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload
from swarmwatch.byzantine_gossip import (
    ByzantineGossipCoordinator,
    GossipMember,
    HeartbeatBeacon,
    SuspicionReport,
    UnauthenticatedGossipBeaconError,
    GossipPeerQuarantinedError,
    GossipEquivocationDetectedError,
    compute_beacon_digest,
    compute_suspicion_digest,
    compute_membership_merkle_root,
)


@pytest.fixture
def clean_gossip():
    bus = SafetyCircuitBus()
    coord = ByzantineGossipCoordinator(
        local_node_id="test-coord-node",
        suspicion_threshold=2,
        circuit_bus=bus,
    )
    return coord, bus


def make_signed_beacon(
    node_id: str,
    seq: int,
    priv: str,
    status: str = "ALIVE",
    state_root: str = "00" * 32,
    hlc_ts: str = "hlc:100:0:node",
) -> HeartbeatBeacon:
    digest = compute_beacon_digest(node_id, seq, status, hlc_ts, state_root)
    sig = sign_bft_payload(priv, digest)
    return HeartbeatBeacon(
        node_id=node_id,
        sequence_number=seq,
        status=status,
        hlc_timestamp=hlc_ts,
        state_root=state_root,
        signature=sig,
    )


def make_signed_suspicion(
    reporter_id: str,
    reporter_priv: str,
    suspect_id: str,
    seq: int,
    reason: str = "PING_TIMEOUT",
    hlc_ts: str = "hlc:100:0:node",
) -> SuspicionReport:
    digest = compute_suspicion_digest(reporter_id, suspect_id, seq, reason, hlc_ts)
    sig = sign_bft_payload(reporter_priv, digest)
    return SuspicionReport(
        reporter_id=reporter_id,
        suspect_id=suspect_id,
        sequence_number=seq,
        reason=reason,
        signature=sig,
        hlc_timestamp=hlc_ts,
    )


def test_authenticated_heartbeats_and_monotonic_progression_bgm1(clean_gossip):
    coord, bus = clean_gossip
    priv, pub = generate_node_keypair()
    m = GossipMember(node_id="node-01", cloud_provider="aws", region="us-east-1", public_key=pub)
    coord.register_member(m)

    b1 = make_signed_beacon("node-01", 1, priv)
    assert coord.process_heartbeat(b1) is True
    assert m.sequence_number == 1

    # Stale sequence number is accepted as idempotent false
    b_stale = make_signed_beacon("node-01", 0, priv)
    assert coord.process_heartbeat(b_stale) is False

    # Unauthenticated beacon (bad signature) fails fail-closed
    wrong_priv, _ = generate_node_keypair()
    b_bad = make_signed_beacon("node-01", 2, wrong_priv)
    with pytest.raises(UnauthenticatedGossipBeaconError):
        coord.process_heartbeat(b_bad)


def test_quorum_gated_suspicion_escalation_and_refutation_bgm2(clean_gossip):
    coord, bus = clean_gossip
    priv_victim, pub_victim = generate_node_keypair()
    priv_rep1, pub_rep1 = generate_node_keypair()
    priv_rep2, pub_rep2 = generate_node_keypair()

    victim = GossipMember(node_id="node-victim", cloud_provider="gcp", region="us-central1", public_key=pub_victim)
    r1 = GossipMember(node_id="rep-1", cloud_provider="aws", region="us-east-1", public_key=pub_rep1)
    r2 = GossipMember(node_id="rep-2", cloud_provider="azure", region="eastus", public_key=pub_rep2)

    coord.register_member(victim)
    coord.register_member(r1)
    coord.register_member(r2)

    # First suspicion does not reach threshold 2
    s1 = make_signed_suspicion("rep-1", priv_rep1, "node-victim", 1)
    assert coord.report_suspicion(s1) is False
    assert victim.status == "ALIVE"

    # Second suspicion reaches threshold 2 -> escalates to SUSPECT
    s2 = make_signed_suspicion("rep-2", priv_rep2, "node-victim", 1)
    assert coord.report_suspicion(s2) is True
    assert victim.status == "SUSPECT"

    # Victim broadcasts refutation beacon with higher sequence number (seq 2)
    refute_b = make_signed_beacon("node-victim", 2, priv_victim)
    assert coord.refute_suspicion(refute_b) is True
    assert victim.status == "ALIVE"
    assert len(victim.suspicion_reporters) == 0


def test_partition_healing_and_anti_entropy_bgm3(clean_gossip):
    coord, bus = clean_gossip
    _, pub1 = generate_node_keypair()
    _, pub2 = generate_node_keypair()
    m1 = GossipMember("node-01", "aws", "us-east-1", pub1, sequence_number=1)
    coord.register_member(m1)

    remote_members = [
        GossipMember("node-01", "aws", "us-east-1", pub1, sequence_number=5),
        GossipMember("node-02", "gcp", "europe-west1", pub2, sequence_number=3),
    ]

    updated = coord.reconcile_anti_entropy(remote_members)
    assert updated == 2
    assert coord.get_member("node-01").sequence_number == 5
    assert coord.get_member("node-02") is not None
    assert coord.get_status()["total_members"] == 2


def test_equivocation_detection_quarantine_and_circuit_latching_bgm4(clean_gossip):
    coord, bus = clean_gossip
    priv, pub = generate_node_keypair()
    m = GossipMember(node_id="node-bad", cloud_provider="aws", region="us-west-2", public_key=pub)
    coord.register_member(m)

    # First beacon at seq 10
    b10a = make_signed_beacon("node-bad", 10, priv, state_root="11" * 32)
    assert coord.process_heartbeat(b10a) is True

    # Second conflicting beacon at seq 10 with different state root!
    b10b = make_signed_beacon("node-bad", 10, priv, state_root="22" * 32)
    with pytest.raises(GossipEquivocationDetectedError):
        coord.process_heartbeat(b10b)

    assert m.status == "QUARANTINED"
    assert bus.is_tripped()
    assert bus.get_status().last_trip_event.reason == "BYZANTINE_GOSSIP_EQUIVOCATION"

    # Subsequent beacons from quarantined node are rejected
    b11 = make_signed_beacon("node-bad", 11, priv)
    with pytest.raises(GossipPeerQuarantinedError):
        coord.process_heartbeat(b11)


def test_adversarial_forged_gossip_signatures_rejected(clean_gossip):
    coord, bus = clean_gossip
    priv, pub = generate_node_keypair()
    m = GossipMember(node_id="node-adversary", cloud_provider="aws", region="us-east-1", public_key=pub)
    coord.register_member(m)

    digest = compute_beacon_digest("node-adversary", 1, "ALIVE", "hlc:100:0:node", "00" * 32)

    # 1. Test literal string pattern
    forged_literal = f"sig:{pub}:{digest}"
    b_literal = HeartbeatBeacon(
        node_id="node-adversary",
        sequence_number=1,
        status="ALIVE",
        hlc_timestamp="hlc:100:0:node",
        state_root="00" * 32,
        signature=forged_literal,
    )
    with pytest.raises(UnauthenticatedGossipBeaconError):
        coord.process_heartbeat(b_literal)

    # 2. Corrupted signature bytes
    real_sig = sign_bft_payload(priv, digest)
    corrupted_sig = "ff" + real_sig[2:]
    b_corrupted = HeartbeatBeacon(
        node_id="node-adversary",
        sequence_number=1,
        status="ALIVE",
        hlc_timestamp="hlc:100:0:node",
        state_root="00" * 32,
        signature=corrupted_sig,
    )
    with pytest.raises(UnauthenticatedGossipBeaconError):
        coord.process_heartbeat(b_corrupted)


def test_sub_250_microsecond_latency_slo(clean_gossip):
    import gc
    coord, bus = clean_gossip
    priv, pub = generate_node_keypair()
    m = GossipMember(node_id="node-perf", cloud_provider="gcp", region="us-west1", public_key=pub)
    coord.register_member(m)

    beacons = [make_signed_beacon("node-perf", i, priv) for i in range(1, 105)]
    # Warmup
    for b in beacons[:10]:
        coord.process_heartbeat(b)

    gc.collect()
    gc.disable()
    latencies_us = []
    try:
        for b in beacons[10:]:
            t0 = time.perf_counter_ns()
            coord.process_heartbeat(b)
            latencies_us.append((time.perf_counter_ns() - t0) / 1000.0)
    finally:
        gc.enable()

    latencies_us.sort()
    trimmed = latencies_us[10:-10]
    median_us = trimmed[len(trimmed) // 2]
    assert median_us < 250.0, f"Median execution latency {median_us:.2f}µs exceeded 250µs SLO"




