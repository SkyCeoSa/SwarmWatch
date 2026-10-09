# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Multi-Region Quorum State Synchronization & Sovereign Federation (Seam 92 / ADR 0103).
"""

import time
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.sovereign_federation import (
    SovereignFederationError,
    StaleFederationEpochError,
    InsufficientRegionalQuorumError,
    InsufficientFederatedClustersError,
    SovereignClusterQuarantinedError,
    EquivocationDetectedError,
    SovereignCluster,
    RegionalStateCommitment,
    FederatedQuorumCertificate,
    SovereignSlashingProof,
    compute_cluster_committee_root,
    compute_commitment_digest,
    verify_ed25519_signature,
    SovereignFederationCoordinator,
)


def _generate_test_cluster(cluster_id: str, region: str, node_count: int = 4):
    priv_keys = [Ed25519PrivateKey.generate() for _ in range(node_count)]
    pub_keys_hex = [k.public_key().public_bytes_raw().hex() for k in priv_keys]
    committee_root = compute_cluster_committee_root(pub_keys_hex)
    cluster = SovereignCluster(
        cluster_id=cluster_id,
        region=region,
        public_keys=pub_keys_hex,
        committee_root=committee_root,
    )
    return cluster, priv_keys


def _create_signed_commitment(
    cluster: SovereignCluster,
    priv_keys: list,
    epoch: int = 1,
    state_digest: str = "deadbeef12345678",
    signer_indices: list = None,
) -> RegionalStateCommitment:
    if signer_indices is None:
        signer_indices = list(range(len(priv_keys)))
    hlc_ts = f"{epoch}000000000:0:{cluster.cluster_id}"
    digest = compute_commitment_digest(
        cluster_id=cluster.cluster_id,
        epoch=epoch,
        hlc_timestamp=hlc_ts,
        state_digest=state_digest,
    )
    sigs = {}
    for idx in signer_indices:
        pk = priv_keys[idx]
        pub_hex = cluster.public_keys[idx]
        sig_hex = pk.sign(digest).hex()
        sigs[pub_hex] = sig_hex

    return RegionalStateCommitment(
        cluster_id=cluster.cluster_id,
        epoch=epoch,
        hlc_timestamp=hlc_ts,
        state_digest=state_digest,
        signatures=sigs,
    )


def test_cluster_committee_root_and_digest():
    cluster, _ = _generate_test_cluster("cluster-us", "us-east-1", 3)
    assert len(cluster.committee_root) == 64
    digest = compute_commitment_digest("cluster-us", 1, "100:0:node", "hash123")
    assert len(digest) == 32


def test_intra_cluster_threshold_quorum_mrq2():
    coordinator = SovereignFederationCoordinator(local_cluster_id="cluster-main")
    cluster, priv_keys = _generate_test_cluster("cluster-eu", "eu-central-1", 4)
    # N=4, f=1, Q=ceil((4+1+1)/2) = 3
    coordinator.register_cluster(cluster)

    # 1. Under-threshold: only 2 signatures (requires 3)
    under_commit = _create_signed_commitment(cluster, priv_keys, epoch=1, signer_indices=[0, 1])
    with pytest.raises(InsufficientRegionalQuorumError, match="valid signatures, requiring threshold 3"):
        coordinator.propose_regional_state(under_commit)

    # 2. Threshold met: 3 signatures
    valid_commit = _create_signed_commitment(cluster, priv_keys, epoch=1, signer_indices=[0, 1, 2])
    coordinator.propose_regional_state(valid_commit)

    status = coordinator.get_federation_status()
    assert status["cluster_count"] == 1


def test_stale_epoch_rejected_mrq3():
    coordinator = SovereignFederationCoordinator(local_cluster_id="cluster-main", current_epoch=5)
    cluster, priv_keys = _generate_test_cluster("cluster-us", "us-east-1", 4)
    coordinator.register_cluster(cluster)

    stale_commit = _create_signed_commitment(cluster, priv_keys, epoch=4, signer_indices=[0, 1, 2])
    with pytest.raises(StaleFederationEpochError, match="is stale"):
        coordinator.propose_regional_state(stale_commit)


def test_inter_cluster_supermajority_synthesis_and_verification_mrq2():
    coordinator = SovereignFederationCoordinator(local_cluster_id="cluster-main", current_epoch=1)

    c1, k1 = _generate_test_cluster("cluster-us", "us-east-1", 4)
    c2, k2 = _generate_test_cluster("cluster-eu", "eu-west-1", 4)
    c3, k3 = _generate_test_cluster("cluster-ap", "ap-south-1", 4)

    coordinator.register_cluster(c1)
    coordinator.register_cluster(c2)
    coordinator.register_cluster(c3)
    # K=3 clusters -> super-majority is floor(2*3/3) + 1 = 3

    # Submit for c1 and c2 only (2 out of 3)
    commit1 = _create_signed_commitment(c1, k1, epoch=1, state_digest="hash-us")
    commit2 = _create_signed_commitment(c2, k2, epoch=1, state_digest="hash-eu")
    coordinator.propose_regional_state(commit1)
    coordinator.propose_regional_state(commit2)

    # Synthesize should fail because m=2 < required 3
    with pytest.raises(InsufficientFederatedClustersError, match="requiring super-majority 3 of 3"):
        coordinator.synthesize_federated_qc(epoch=1)

    # Now submit for c3
    commit3 = _create_signed_commitment(c3, k3, epoch=1, state_digest="hash-ap")
    coordinator.propose_regional_state(commit3)

    # Synthesize succeeds
    qc = coordinator.synthesize_federated_qc(epoch=1)
    assert qc.epoch == 1
    assert len(qc.participating_clusters) == 3
    assert coordinator.verify_federated_qc(qc) is True
    assert coordinator.current_epoch == 2

    # Tampered QC fails
    tampered_qc = FederatedQuorumCertificate(
        federated_qc_id=qc.federated_qc_id,
        epoch=qc.epoch,
        global_state_root="0000000000000000000000000000000000000000000000000000000000000000",
        participating_clusters=qc.participating_clusters,
        regional_commitments=qc.regional_commitments,
        created_at_hlc=qc.created_at_hlc,
    )
    assert coordinator.verify_federated_qc(tampered_qc) is False


def test_equivocation_detection_and_quarantine_mrq4():
    circuit_bus = SafetyCircuitBus()
    coordinator = SovereignFederationCoordinator(local_cluster_id="cluster-main", circuit_bus=circuit_bus)

    cluster, priv_keys = _generate_test_cluster("cluster-equivocal", "us-east-1", 4)
    coordinator.register_cluster(cluster)

    commit_a = _create_signed_commitment(cluster, priv_keys, epoch=1, state_digest="state-version-A")
    coordinator.propose_regional_state(commit_a)

    assert cluster.is_quarantined is False
    assert circuit_bus.is_tripped() is False

    # Submit conflicting commitment for same epoch
    commit_b = _create_signed_commitment(cluster, priv_keys, epoch=1, state_digest="state-version-B-CONTRADICTION")
    with pytest.raises(EquivocationDetectedError, match="Equivocation detected"):
        coordinator.propose_regional_state(commit_b)

    # Offending cluster must be permanently quarantined
    assert cluster.is_quarantined is True
    assert cluster.is_active is False

    # Safety circuit bus must be tripped fail-closed
    assert circuit_bus.is_tripped() is True
    assert circuit_bus.get_status().last_trip_event.reason == "SOVEREIGN_EQUIVOCATION_DETECTED"

    # Subsequent proposals from quarantined cluster are rejected
    commit_c = _create_signed_commitment(cluster, priv_keys, epoch=2, state_digest="state-c")
    with pytest.raises(SovereignClusterQuarantinedError):
        coordinator.propose_regional_state(commit_c)


def test_sub_250_microsecond_latency_slo():
    coordinator = SovereignFederationCoordinator(local_cluster_id="cluster-bench")
    c1, k1 = _generate_test_cluster("cluster-bench-1", "us-east-1", 4)
    coordinator.register_cluster(c1)

    commit = _create_signed_commitment(c1, k1, epoch=1)

    # Warmup
    coordinator.propose_regional_state(commit)

    # Benchmark propose checks
    t0 = time.perf_counter_ns()
    for i in range(100):
        coordinator.propose_regional_state(commit)
    t1 = time.perf_counter_ns()

    mean_ns = (t1 - t0) / 100
    mean_us = mean_ns / 1000.0
    assert mean_us < 250.0, f"Mean propose latency {mean_us:.2f}µs exceeded 250µs SLO"
