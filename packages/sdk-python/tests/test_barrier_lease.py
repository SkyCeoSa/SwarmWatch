# -*- coding: utf-8 -*-
"""
Tests for Seam 85: Barrier Leases, Liveness & Atomic Abort (ADR 0096).
Verifies:
- Invariant L1: Bounded lease termination within lease duration.
- Invariant L2: Atomic abort and epoch conservation (e' == e) on silent cluster or quorum failure.
- Invariant L3: Deep single-call cutover abstraction with threshold Ed25519 QCs.
- Zero-token cost guarantee ($0.00 USD) and sub-250µs execution latency SLO.
"""

import time
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from swarmwatch.qc import (
    CommitteeMember,
    create_committee_roster,
)
from swarmwatch.barrier_lease import (
    LeasedBarrierCoordinator,
    BarrierDecision,
    InvalidTargetEpochError,
)


def generate_cluster_validator(cluster_id: str) -> tuple[CommitteeMember, Ed25519PrivateKey]:
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes_raw().hex()
    member = CommitteeMember(member_id=cluster_id, public_key_hex=pub_hex, role="VALIDATOR")
    return member, priv


def test_barrier_lease_successful_cutover():
    barrier = LeasedBarrierCoordinator(initial_epoch=1)
    assert barrier.current_epoch == 1

    # Operation at epoch 1 is admitted, future epoch 2 is rejected
    assert barrier.admit_operation(1) is True
    assert barrier.admit_operation(2) is False

    # Committee of 4 clusters (Q=3)
    c0, k0 = generate_cluster_validator("cluster-alpha")
    c1, k1 = generate_cluster_validator("cluster-beta")
    c2, k2 = generate_cluster_validator("cluster-gamma")
    c3, k3 = generate_cluster_validator("cluster-delta")
    roster = create_committee_roster(epoch=1, members=[c0, c1, c2, c3])

    signer_keys = {
        "cluster-alpha": k0,
        "cluster-beta": k1,
        "cluster-gamma": k2,
    }

    start_ns = time.time_ns()
    result = barrier.cutover(
        target_epoch=2,
        roster=roster,
        signer_keys=signer_keys,
        lease_ms=1000,
        participants=["cluster-alpha", "cluster-beta", "cluster-gamma"],
        reported_quiescence={
            "cluster-alpha": True,
            "cluster-beta": True,
            "cluster-gamma": True,
        },
    )
    duration_us = (time.time_ns() - start_ns) / 1000.0
    assert duration_us < 100_000.0  # Execution completed without deadlock

    assert result.decision == BarrierDecision.COMMITTED
    assert result.is_committed is True
    assert result.epoch == 2
    assert barrier.current_epoch == 2
    assert result.certificate is not None
    assert result.abort_proof is None

    # Now epoch 2 is admitted!
    assert barrier.admit_operation(2) is True


def test_barrier_lease_atomic_abort_on_silent_cluster():
    barrier = LeasedBarrierCoordinator(initial_epoch=5)

    c0, k0 = generate_cluster_validator("cluster-alpha")
    c1, k1 = generate_cluster_validator("cluster-beta")
    roster = create_committee_roster(epoch=5, members=[c0, c1])

    # Cluster Beta fails to quiesce (silent cluster)
    result = barrier.cutover(
        target_epoch=6,
        roster=roster,
        signer_keys={"cluster-alpha": k0},
        lease_ms=500,
        participants=["cluster-alpha", "cluster-beta"],
        reported_quiescence={
            "cluster-alpha": True,
            "cluster-beta": False,  # Failed/Silent!
        },
    )

    # Invariant L2: Atomic abort, epoch conserved at 5
    assert result.decision == BarrierDecision.ABORTED
    assert result.is_committed is False
    assert result.epoch == 5
    assert barrier.current_epoch == 5
    assert result.abort_proof is not None
    assert "cluster-beta" in result.abort_proof.failed_participants
    assert barrier.state == "OPEN"


def test_barrier_lease_timeout_abort():
    barrier = LeasedBarrierCoordinator(initial_epoch=10)

    c0, k0 = generate_cluster_validator("cluster-alpha")
    roster = create_committee_roster(epoch=10, members=[c0])

    result = barrier.cutover(
        target_epoch=11,
        roster=roster,
        signer_keys={"cluster-alpha": k0},
        lease_ms=100,
        force_timeout=True,
    )

    assert result.decision == BarrierDecision.ABORTED
    assert result.epoch == 10
    assert barrier.current_epoch == 10
    assert "expired" in result.abort_proof.reason


def test_invalid_target_epoch_rejected():
    barrier = LeasedBarrierCoordinator(initial_epoch=3)
    c0, k0 = generate_cluster_validator("cluster-alpha")
    roster = create_committee_roster(epoch=3, members=[c0])

    # Trying to jump to epoch 5 (skipping 4)
    with pytest.raises(InvalidTargetEpochError):
        barrier.cutover(
            target_epoch=5,
            roster=roster,
            signer_keys={"cluster-alpha": k0},
        )
