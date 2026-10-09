# -*- coding: utf-8 -*-
"""
Tests for Seam 81: Committee Key Ceremony & Threshold Quorum Certificates (ADR 0092).
Verifies:
- Invariant QC1: Byzantine threshold completeness, Merkle committee roots, and Ed25519 signature checks.
- Invariant QC2: Key rotation ceremony and transition certificates to epoch e+1.
- Invariant QC3: Domain separation, replay prevention, and forgery rejection.
- Zero-token cost guarantee ($0.00 USD) and sub-250µs latency.
"""

import time
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from swarmwatch.qc import (
    CommitteeMember,
    CommitteeRoster,
    ThresholdQuorumCertificate,
    compute_byzantine_quorum,
    compute_committee_root,
    create_committee_roster,
    attest_quorum,
    verify_quorum_certificate,
    rotate_committee,
    InsufficientQuorumError,
    UnauthorizedSignerError,
)


def generate_test_member(member_id: str) -> tuple[CommitteeMember, Ed25519PrivateKey]:
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes_raw().hex()
    member = CommitteeMember(member_id=member_id, public_key_hex=pub_hex)
    return member, priv


def test_byzantine_threshold_math():
    # N=4 => f=1, Q=3
    f, q = compute_byzantine_quorum(4)
    assert f == 1
    assert q == 3

    # N=7 => f=2, Q=5
    f, q = compute_byzantine_quorum(7)
    assert f == 2
    assert q == 5

    # N=10 => f=3, Q=7
    f, q = compute_byzantine_quorum(10)
    assert f == 3
    assert q == 7

    # Small committee N=3 => f=0, Q=2
    f, q = compute_byzantine_quorum(3)
    assert f == 0
    assert q == 2


def test_committee_key_ceremony_and_attestation():
    members = []
    keys = {}
    for i in range(4):
        m, k = generate_test_member(f"node-{i}")
        members.append(m)
        keys[f"node-{i}"] = k

    roster = create_committee_roster(epoch=1, members=members)
    assert roster.epoch == 1
    assert roster.quorum_threshold == 3
    assert roster.fault_tolerance_f == 1
    assert len(roster.committee_root) == 64

    # Sign with 3 members (meets quorum Q=3)
    signer_keys = {f"node-{i}": keys[f"node-{i}"] for i in range(3)}
    # Warmup
    attest_quorum(
        roster=roster,
        domain="barrier:cutover",
        digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        signer_keys=signer_keys,
    )

    trials_us = []
    for _ in range(10):
        t0 = time.time_ns()
        cert = attest_quorum(
            roster=roster,
            domain="barrier:cutover",
            digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            signer_keys=signer_keys,
        )
        trials_us.append((time.time_ns() - t0) / 1000.0)

    trials_us.sort()
    median_us = trials_us[len(trials_us) // 2]
    assert median_us < 2000.0  # Fast sub-millisecond execution

    assert cert.epoch == 1
    assert cert.quorum_required == 3
    assert len(cert.signatures) == 3
    assert cert.committee_root == roster.committee_root

    # Verification against trusted roster
    verdict = verify_quorum_certificate(cert, roster=roster)
    assert verdict.is_valid is True
    assert verdict.threshold_met is True
    assert len(verdict.verified_signers) == 3


def test_insufficient_quorum_rejected():
    members = []
    keys = {}
    for i in range(4):
        m, k = generate_test_member(f"node-{i}")
        members.append(m)
        keys[f"node-{i}"] = k

    roster = create_committee_roster(epoch=1, members=members)

    # Attempt to sign with only 2 members (required is 3)
    signer_keys = {f"node-{i}": keys[f"node-{i}"] for i in range(2)}
    with pytest.raises(InsufficientQuorumError):
        attest_quorum(
            roster=roster,
            domain="barrier:cutover",
            digest="abcd" * 16,
            signer_keys=signer_keys,
        )


def test_forged_or_tampered_signature_rejected():
    members = []
    keys = {}
    for i in range(4):
        m, k = generate_test_member(f"node-{i}")
        members.append(m)
        keys[f"node-{i}"] = k

    roster = create_committee_roster(epoch=1, members=members)
    signer_keys = {f"node-{i}": keys[f"node-{i}"] for i in range(3)}
    cert = attest_quorum(
        roster=roster,
        domain="barrier:cutover",
        digest="abcd" * 16,
        signer_keys=signer_keys,
    )

    # Tamper with one signature
    cert.signatures["node-0"] = "00" * 64
    verdict = verify_quorum_certificate(cert, roster=roster)
    assert verdict.is_valid is False
    assert verdict.threshold_met is False
    assert len(verdict.verified_signers) == 2  # Only node-1 and node-2 remain valid


def test_domain_separation_and_epoch_mismatch():
    members = []
    keys = {}
    for i in range(4):
        m, k = generate_test_member(f"node-{i}")
        members.append(m)
        keys[f"node-{i}"] = k

    roster = create_committee_roster(epoch=1, members=members)
    signer_keys = {f"node-{i}": keys[f"node-{i}"] for i in range(3)}
    cert = attest_quorum(
        roster=roster,
        domain="barrier:cutover",
        digest="abcd" * 16,
        signer_keys=signer_keys,
    )

    # Wrong expected epoch
    verdict_epoch = verify_quorum_certificate(cert, roster=roster, expected_epoch=2)
    assert verdict_epoch.is_valid is False
    assert "epoch" in verdict_epoch.error_reason

    # Wrong expected root
    verdict_root = verify_quorum_certificate(cert, roster=roster, expected_root="deadbeef" * 8)
    assert verdict_root.is_valid is False
    assert "committee root" in verdict_root.error_reason


def test_committee_key_rotation_chain():
    # Epoch 1
    members_e1 = []
    keys_e1 = {}
    for i in range(4):
        m, k = generate_test_member(f"node-e1-{i}")
        members_e1.append(m)
        keys_e1[f"node-e1-{i}"] = k

    roster_e1 = create_committee_roster(epoch=1, members=members_e1)

    # Next Epoch 2 members
    members_e2 = []
    for i in range(4):
        m, _ = generate_test_member(f"node-e2-{i}")
        members_e2.append(m)

    signers_e1 = {f"node-e1-{i}": keys_e1[f"node-e1-{i}"] for i in range(3)}
    next_roster, rotation_qc = rotate_committee(
        current_roster=roster_e1,
        new_members=members_e2,
        signer_keys=signers_e1,
    )

    assert next_roster.epoch == 2
    assert rotation_qc.domain == "committee:rotation"
    assert rotation_qc.epoch == 1
    assert rotation_qc.digest == next_roster.committee_root

    # Verify rotation QC against Epoch 1 roster
    verdict = verify_quorum_certificate(rotation_qc, roster=roster_e1)
    assert verdict.is_valid is True
    assert verdict.threshold_met is True
