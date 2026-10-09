"""
Tests for SwarmWatch Global Master Singularity & Autonomous Self-Governing Sovereign Registry (Seam 100 / ADR 0111).
Verifies:
- SNG1: Monotonic Singularity Epoch & Sovereign State Tree Evolution
- SNG2: Byzantine Constitutional Quorum Ratification
- SNG3: Autonomous Universal Proof-of-Solvency & Invariant Convergence
- SNG4: Universal Fail-Closed Circuit Latching & Emergency Quarantine
- Sub-250 microsecond execution latency SLO.
"""

import hashlib
import hmac
import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload
from swarmwatch.master_singularity import (
    MasterSingularityCoordinator,
    SingularityGovernor,
    ConstitutionalProposal,
    SingularityStateCommitment,
    SingularityCertificate,
    SingularityError,
    StaleSingularityEpochError,
    InsufficientGovernanceQuorumError,
    SingularitySolvencyBreachError,
    SingularityQuarantinedError,
    SingularityCircuitTrippedError,
    compute_singularity_digest,
)


@pytest.fixture
def clean_singularity():
    bus = SafetyCircuitBus()
    coord = MasterSingularityCoordinator(
        singularity_id="test-singularity-root",
        min_solvency_threshold=500_000,
        circuit_bus=bus,
    )
    # Register 3 governors with genuine Ed25519 keypairs (K=3, required quorum = floor(2*3/3) + 1 = 3)
    keys = {}
    for i in range(1, 4):
        gid = f"gov-{i}"
        priv, pub = generate_node_keypair()
        keys[gid] = {"priv": priv, "pub": pub}
        g = SingularityGovernor(governor_id=gid, public_key_hex=pub)
        coord.register_governor(g)
    return coord, bus, keys


def _sign_for_governor(keys: dict, governor_id: str, digest: str) -> str:
    return sign_bft_payload(keys[governor_id]["priv"], digest)


def test_monotonic_epoch_and_state_tree_evolution_sng1(clean_singularity):
    coord, bus, keys = clean_singularity

    assert coord.current_epoch == 0
    assert coord.current_state_root == "0" * 64

    # Non-monotonic or skipped epoch fails with StaleSingularityEpochError (SNG1)
    with pytest.raises(StaleSingularityEpochError):
        coord.commit_subsystem_state(
            epoch=2,  # Expected 1
            mesh_root="mesh-0",
            fed_root="fed-0",
            snark_root="snark-0",
            fence_root="fence-0",
            total_staked_solvency=1_000_000,
        )

    # Valid epoch 1 transition
    c1 = coord.commit_subsystem_state(
        epoch=1,
        mesh_root="mesh-1",
        fed_root="fed-1",
        snark_root="snark-1",
        fence_root="fence-1",
        total_staked_solvency=1_000_000,
    )
    assert coord.current_epoch == 1
    assert c1.epoch == 1
    assert coord.current_state_root == c1.state_digest

    # Attempting to re-commit epoch 1 or older fails
    with pytest.raises(StaleSingularityEpochError):
        coord.commit_subsystem_state(
            epoch=1,
            mesh_root="mesh-1",
            fed_root="fed-1",
            snark_root="snark-1",
            fence_root="fence-1",
            total_staked_solvency=1_000_000,
        )


def test_byzantine_constitutional_quorum_ratification_sng2(clean_singularity):
    coord, bus, keys = clean_singularity

    # Submit proposal targeting epoch 1
    prop = coord.submit_proposal(
        proposal_id="prop-constitutional-reweight",
        proposer_id="gov-1",
        title="Increase minimum solvency threshold",
        constitution_payload={"min_solvency_threshold": 750_000},
        target_epoch=1,
    )
    assert prop.status == "PROPOSED"

    # With 3 active governors, required quorum is 3. Vote with only 2
    sig1 = _sign_for_governor(keys, "gov-1", prop.payload_digest)
    sig2 = _sign_for_governor(keys, "gov-2", prop.payload_digest)
    coord.vote_proposal(prop.proposal_id, "gov-1", sig1)
    coord.vote_proposal(prop.proposal_id, "gov-2", sig2)

    # Ratification fails with InsufficientGovernanceQuorumError (SNG2)
    with pytest.raises(InsufficientGovernanceQuorumError):
        coord.ratify_proposal(prop.proposal_id)

    # Third governor votes
    sig3 = _sign_for_governor(keys, "gov-3", prop.payload_digest)
    coord.vote_proposal(prop.proposal_id, "gov-3", sig3)

    # Now ratification succeeds
    ratified = coord.ratify_proposal(prop.proposal_id)
    assert ratified.status == "RATIFIED"
    assert ratified.ratified_at_ns is not None


def test_universal_proof_of_solvency_and_certificate_sng3(clean_singularity):
    coord, bus, keys = clean_singularity

    # Solvency breach: total stake below threshold fails with SingularitySolvencyBreachError (SNG3)
    with pytest.raises(SingularitySolvencyBreachError):
        coord.commit_subsystem_state(
            epoch=1,
            mesh_root="mesh-1",
            fed_root="fed-1",
            snark_root="snark-1",
            fence_root="fence-1",
            total_staked_solvency=200_000,  # Below 500_000
        )

    # Solvency breach: active breaches > 0 fails with SingularitySolvencyBreachError (SNG3)
    with pytest.raises(SingularitySolvencyBreachError):
        coord.commit_subsystem_state(
            epoch=1,
            mesh_root="mesh-1",
            fed_root="fed-1",
            snark_root="snark-1",
            fence_root="fence-1",
            total_staked_solvency=1_000_000,
            breach_count=2,
        )

    # Successful commitment with adequate solvency
    coord.commit_subsystem_state(
        epoch=1,
        mesh_root="mesh-1",
        fed_root="fed-1",
        snark_root="snark-1",
        fence_root="fence-1",
        total_staked_solvency=1_000_000,
        breach_count=0,
    )

    # Issue singularity certificate
    cert = coord.issue_singularity_certificate(epoch=1)
    assert cert.epoch == 1
    assert cert.certificate_id.startswith("sng-cert-1-")
    assert len(cert.master_singularity_signature) == 128


def test_emergency_halt_and_recovery_ceremony_sng4(clean_singularity):
    coord, bus, keys = clean_singularity

    # Trigger emergency halt (SNG4)
    coord.emergency_halt(reason="CRITICAL_BYZANTINE_COLLAPSE")
    assert coord.is_quarantined is True
    assert bus.is_tripped() is True

    # Mutating operations raise SingularityQuarantinedError or SingularityCircuitTrippedError
    with pytest.raises((SingularityQuarantinedError, SingularityCircuitTrippedError)):
        coord.submit_proposal(
            proposal_id="prop-blocked",
            proposer_id="gov-1",
            title="Blocked proposal",
            constitution_payload={},
            target_epoch=1,
        )

    # Recovery ceremony with insufficient signatures fails
    recov_digest = coord.get_recovery_digest()
    recov_sigs = {
        "gov-1": _sign_for_governor(keys, "gov-1", recov_digest),
        "gov-2": _sign_for_governor(keys, "gov-2", recov_digest),
    }
    assert coord.execute_recovery_ceremony(recov_sigs) is False
    assert coord.is_quarantined is True

    # Full super-majority quorum participates
    recov_sigs["gov-3"] = _sign_for_governor(keys, "gov-3", recov_digest)
    assert coord.execute_recovery_ceremony(recov_sigs) is True
    assert coord.is_quarantined is False
    assert bus.is_tripped() is False


def test_adversarial_forged_governor_signatures_rejected(clean_singularity):
    coord, bus, keys = clean_singularity

    prop = coord.submit_proposal(
        proposal_id="prop-malicious-disable-guardrails",
        proposer_id="attacker",
        title="Disable guardrails",
        constitution_payload={"guardrails": "disabled"},
        target_epoch=1,
    )

    # Adversarial Attempt 1: Forged HMAC keyed by public governor ID is rejected
    forged_hmac = hmac.new("gov-1".encode("utf-8"), prop.payload_digest.encode("utf-8"), hashlib.sha256).hexdigest()
    assert coord.vote_proposal(prop.proposal_id, "gov-1", forged_hmac) is False

    # Adversarial Attempt 2: Forged test format "sig:<pubkey>:<digest>" is rejected
    forged_direct = f"sig:{keys['gov-1']['pub']}:{prop.payload_digest[:16]}"
    assert coord.vote_proposal(prop.proposal_id, "gov-1", forged_direct) is False

    # Adversarial Attempt 3: Signature byte corruption is rejected
    valid_sig = _sign_for_governor(keys, "gov-1", prop.payload_digest)
    corrupted_sig = valid_sig[:-2] + ("00" if valid_sig[-2:] != "00" else "ff")
    assert coord.vote_proposal(prop.proposal_id, "gov-1", corrupted_sig) is False

    # Adversarial Attempt 4: Registering governor with invalid public key is rejected
    with pytest.raises(SingularityError):
        coord.register_governor(SingularityGovernor(governor_id="evil-gov", public_key_hex="not-a-valid-key"))

    # Adversarial Attempt 5: Cannot lift emergency lockdown with forged HMAC signatures
    coord.emergency_halt(reason="ADVERSARIAL_TEST")
    recov_digest = hashlib.sha256(f"RECOVERY:{coord.singularity_id}:{coord.current_epoch}".encode("utf-8")).hexdigest()
    forged_recov = {
        "gov-1": hmac.new("gov-1".encode(), recov_digest.encode(), hashlib.sha256).hexdigest(),
        "gov-2": hmac.new("gov-2".encode(), recov_digest.encode(), hashlib.sha256).hexdigest(),
        "gov-3": hmac.new("gov-3".encode(), recov_digest.encode(), hashlib.sha256).hexdigest(),
    }
    assert coord.execute_recovery_ceremony(forged_recov) is False
    assert coord.is_quarantined is True


def test_singularity_execution_latency_slo(clean_singularity):
    coord, bus, keys = clean_singularity

    # Pre-commit epoch 1
    coord.commit_subsystem_state(
        epoch=1,
        mesh_root="mesh-1",
        fed_root="fed-1",
        snark_root="snark-1",
        fence_root="fence-1",
        total_staked_solvency=1_000_000,
    )

    # Benchmark issue_singularity_certificate over 50 iterations
    iterations = 50
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        coord.issue_singularity_certificate(epoch=1)
    t1 = time.perf_counter_ns()

    avg_latency_us = (t1 - t0) / (iterations * 1000.0)
    assert avg_latency_us < 250.0, f"Average latency {avg_latency_us:.2f}us exceeds 250us SLO."
