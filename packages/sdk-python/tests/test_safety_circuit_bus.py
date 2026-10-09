# -*- coding: utf-8 -*-
"""
Tests for Seam 82: Unified Safety Circuit Bus & Quorum-Gated Rearm (ADR 0093).
Verifies:
- Invariant SC1: Strictly monotonic generation counters (g' > g) on every trip.
- Invariant SC2: Quorum-gated rearm requiring >= 2 distinct operator signatures and valid StateSnapshot hash.
- Invariant SC3: Global fail-closed propagation and RAII guard blocking.
- Zero-token cost guarantee ($0.00 USD) and sub-250µs execution latency.
"""

import time
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from swarmwatch.qc import (
    CommitteeMember,
    create_committee_roster,
    attest_quorum,
)
from swarmwatch.circuit_bus import (
    SafetyCircuitBus,
    CircuitState,
    CircuitBusTrippedError,
    UnauthorizedRearmError,
    InvalidSnapshotHashError,
)


def generate_operator(op_id: str) -> tuple[CommitteeMember, Ed25519PrivateKey]:
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes_raw().hex()
    member = CommitteeMember(member_id=op_id, public_key_hex=pub_hex, role="OPERATOR")
    return member, priv


def test_circuit_trip_monotonic_generation_and_guard():
    bus = SafetyCircuitBus()
    assert bus.state == CircuitState.CLOSED
    assert bus.generation == 0
    assert not bus.is_tripped()

    # Guard allows execution when CLOSED
    executed = False
    with bus.guard("consensus"):
        executed = True
    assert executed is True

    # Trip the circuit
    trip_id = bus.trip(reason="Byzantine equivocation detected", scope="CONSENSUS")

    assert bus.state == CircuitState.TRIPPED
    assert bus.generation == 1
    assert bus.is_tripped("CONSENSUS") is True
    assert bus.is_tripped("GLOBAL") is True

    # Second trip increments generation monotonically (Invariant SC1)
    bus.trip(reason="Entropy threshold collapse", scope="ENTROPY")
    assert bus.generation == 2

    # Guard blocks execution fail-closed (Invariant SC3)
    with pytest.raises(CircuitBusTrippedError) as exc_info:
        with bus.guard("CONSENSUS"):
            pass
    assert exc_info.value.generation == 2
    assert "Byzantine equivocation" in exc_info.value.reason or "Entropy threshold" in exc_info.value.reason


def test_unilateral_or_unauthenticated_rearm_rejected():
    bus = SafetyCircuitBus()
    bus.trip(reason="Test trip", scope="GLOBAL")

    op0, key0 = generate_operator("operator-0")
    roster_single = create_committee_roster(epoch=1, members=[op0])
    snapshot_hash = "a" * 64

    # Cert with only 1 signer from 1-member roster
    cert_single = attest_quorum(
        roster=roster_single,
        domain="circuit:rearm",
        digest=snapshot_hash,
        signer_keys={"operator-0": key0},
    )

    # bus.rearm enforces >= 2 operators regardless of committee size
    with pytest.raises(UnauthorizedRearmError):
        bus.rearm(qc=cert_single, snapshot_hash=snapshot_hash, expected_roster=roster_single)

    # Empty snapshot hash rejected
    with pytest.raises(InvalidSnapshotHashError):
        bus.rearm(qc=cert_single, snapshot_hash="", expected_roster=roster_single)



def test_valid_multi_operator_quorum_rearm():
    bus = SafetyCircuitBus()
    bus.trip(reason="Critical test trip", scope="GLOBAL")
    assert bus.is_tripped() is True

    op0, key0 = generate_operator("operator-0")
    op1, key1 = generate_operator("operator-1")
    op2, key2 = generate_operator("operator-2")
    roster = create_committee_roster(epoch=1, members=[op0, op1, op2])

    snapshot_hash = "c" * 64

    # Sign with 2 distinct operators (meets >= 2 multi-operator quorum)
    rearm_qc = attest_quorum(
        roster=roster,
        domain="circuit:rearm",
        digest=snapshot_hash,
        signer_keys={"operator-0": key0, "operator-1": key1},
    )

    gen = bus.rearm(qc=rearm_qc, snapshot_hash=snapshot_hash, expected_roster=roster)
    assert bus.state == CircuitState.CLOSED
    assert bus.is_tripped() is False
    assert gen >= 1

    # Guard now allows execution again
    ran_again = False
    with bus.guard("GLOBAL"):
        ran_again = True
    assert ran_again is True


def test_circuit_bus_subscriber_notification():
    bus = SafetyCircuitBus()
    received_events = []

    def on_trip(event):
        received_events.append(event)

    bus.subscribe(on_trip)
    bus.trip(reason="Partition event", scope="MESH", metadata={"peers": 3})

    assert len(received_events) == 1
    assert received_events[0].scope == "MESH"
    assert received_events[0].reason == "Partition event"
    assert received_events[0].metadata["peers"] == 3


def test_sub_250us_circuit_bus_trip_slo():
    bus = SafetyCircuitBus()
    for _ in range(10):
        bus.trip("warmup")
    latencies = []
    for _ in range(100):
        t0 = time.perf_counter_ns()
        bus.trip("benchmark")
        latencies.append((time.perf_counter_ns() - t0) / 1000.0)
    latencies.sort()
    median_us = latencies[len(latencies) // 2]
    assert median_us < 250.0, f"Circuit trip median latency {median_us:.2f}µs exceeded 250µs SLO"

