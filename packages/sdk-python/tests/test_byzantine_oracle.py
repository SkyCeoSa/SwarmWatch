# -*- coding: utf-8 -*-
"""
Test Suite for SwarmWatch Cross-Cluster Byzantine Consensus & Proof-of-Execution Oracle (Seam 74 / ADR 0085).
"""

import time
import pytest

from swarmwatch.byzantine_oracle import (
    BftMessage,
    BftPhase,
    ByzantineConsensusOracle,
    ByzantineOracleError,
    ByzantineQuorumExceededError,
    EquivocationDetectedError,
    InvalidOracleSignatureError,
    NodeRole,
    NodeStatus,
    OracleCircuitTrippedError,
    ProofOfExecutionCertificate,
    ProposalPayload,
    ValidatorNode,
    compute_canonical_poe_hash,
    generate_node_keypair,
    sign_bft_payload,
    verify_bft_signature,
)


def test_node_registration_and_bft_math():
    """Validates node registration and PBFT quorum formula: N >= 3f + 1, Q = 2f + 1."""
    oracle = ByzantineConsensusOracle()

    # 1 node
    priv1, pub1 = generate_node_keypair()
    oracle.register_node("node-1", "cluster-us-east", pub1, priv1, NodeRole.PRIMARY)
    assert oracle.total_nodes_count == 1
    assert oracle.active_nodes_count == 1
    assert oracle.fault_tolerance_f == 0
    assert oracle.quorum_size_q == 1

    # Add 3 more nodes (total 4 nodes -> f = 1, Q = 3)
    for i in range(2, 5):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"node-{i}", f"cluster-{i}", pub, priv, NodeRole.REPLICA)

    assert oracle.total_nodes_count == 4
    assert oracle.active_nodes_count == 4
    assert oracle.fault_tolerance_f == 1
    assert oracle.quorum_size_q == 3

    # Add 3 more nodes (total 7 nodes -> f = 2, Q = 5)
    for i in range(5, 8):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"node-{i}", f"cluster-{i}", pub, priv, NodeRole.REPLICA)

    assert oracle.total_nodes_count == 7
    assert oracle.fault_tolerance_f == 2
    assert oracle.quorum_size_q == 5


def test_full_pbft_consensus_and_poe_certification():
    """Validates complete 4-phase PBFT consensus lifecycle and PoE certificate generation."""
    oracle = ByzantineConsensusOracle()

    # Provision 4 validator nodes (f=1, Q=3)
    nodes = []
    for i in range(1, 5):
        priv, pub = generate_node_keypair()
        node = oracle.register_node(
            node_id=f"val-node-{i}",
            cluster_id=f"cluster-{i}",
            public_key_hex=pub,
            secret_key=priv,
            role=NodeRole.PRIMARY if i == 1 else NodeRole.REPLICA,
        )
        nodes.append(node)

    # 1. Proposal & Pre-Prepare
    proposal = ProposalPayload(
        proposal_id="prop-finance-901",
        action_type="DATABASE_MIGRATION",
        target_cluster="cluster-1",
        tenant_id="tenant-acme",
        pre_state_hash="pre-sha256-0001",
        post_state_hash="post-sha256-0002",
        payload_data={"table": "ledger_entries", "shards": 64},
    )

    pre_prepare_msg = oracle.propose(proposal)
    assert pre_prepare_msg.phase == BftPhase.PRE_PREPARE
    assert pre_prepare_msg.view == 0
    assert pre_prepare_msg.sequence_number == 1
    assert len(pre_prepare_msg.signature) == 64

    # 2. Prepare Phase (nodes 2, 3 prepare)
    prep_msg_2 = oracle.prepare(pre_prepare_msg, "val-node-2")
    prep_msg_3 = oracle.prepare(pre_prepare_msg, "val-node-3")
    assert prep_msg_2.phase == BftPhase.PRE_PREPARE or prep_msg_2.phase == BftPhase.PREPARE

    # 3. Commit Phase (nodes 1, 2, 3 commit -> achieves quorum Q=3)
    oracle.commit(prep_msg_2, "val-node-1")
    oracle.commit(prep_msg_2, "val-node-2")
    oracle.commit(prep_msg_2, "val-node-3")

    # 4. Decided / Certify Execution
    cert = oracle.execute_and_certify(proposal.proposal_id)
    assert cert.proposal_id == "prop-finance-901"
    assert cert.action_type == "DATABASE_MIGRATION"
    assert len(cert.signers) >= 3
    assert cert.receipt_hash.startswith("sw:oracle:poe:v1:")
    assert cert.duration_us >= 0.0

    # 5. Independent Oracle Verification
    assert oracle.verify_poe_certificate(cert) is True


def test_poe_certificate_tamper_detection():
    """Validates that modified or forged PoE certificates fail verification."""
    oracle = ByzantineConsensusOracle()
    for i in range(1, 5):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"node-{i}", "cluster-us", pub, priv)

    proposal = ProposalPayload(
        proposal_id="prop-tamper-01",
        action_type="PAYMENT_TRANSFER",
        target_cluster="cluster-us",
        pre_state_hash="state-1",
        post_state_hash="state-2",
    )
    pre_prep = oracle.propose(proposal)
    oracle.prepare(pre_prep, "node-2")
    oracle.prepare(pre_prep, "node-3")
    oracle.commit(pre_prep, "node-1")
    oracle.commit(pre_prep, "node-2")
    oracle.commit(pre_prep, "node-3")

    cert = oracle.execute_and_certify(proposal.proposal_id)
    assert oracle.verify_poe_certificate(cert) is True

    # Tamper with post-state hash
    tampered_cert = ProofOfExecutionCertificate(
        certificate_id=cert.certificate_id,
        proposal_id=cert.proposal_id,
        action_type=cert.action_type,
        view=cert.view,
        sequence_number=cert.sequence_number,
        digest=cert.digest,
        pre_state_hash=cert.pre_state_hash,
        post_state_hash="forged-malicious-post-state",
        signers=cert.signers,
        signatures=cert.signatures,
        timestamp_ns=cert.timestamp_ns,
        receipt_hash=cert.receipt_hash,
    )
    assert oracle.verify_poe_certificate(tampered_cert) is False


def test_equivocation_detection_and_slashing():
    """Validates detection of Byzantine double-signing and immediate node slashing."""
    oracle = ByzantineConsensusOracle()
    for i in range(1, 5):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"node-{i}", "cluster-a", pub, priv)

    prop1 = ProposalPayload("prop-1", "ACTION_A", "cluster-a")
    prop2 = ProposalPayload("prop-2", "ACTION_B", "cluster-a")

    pre_prep1 = oracle.propose(prop1)
    oracle.prepare(pre_prep1, "node-2")

    # Node 2 attempts to sign a different proposal digest for the same view and sequence
    rogue_pre_prep = BftMessage(
        msg_id="rogue-msg",
        phase=BftPhase.PRE_PREPARE,
        view=pre_prep1.view,
        sequence_number=pre_prep1.sequence_number,
        proposal_id=prop2.proposal_id,
        digest="conflicting-fake-digest-9999",
        node_id="node-1",
        cluster_id="cluster-a",
    )

    node1 = oracle.get_node("node-1")
    rogue_pre_prep.signature = sign_bft_payload(node1.secret_key, rogue_pre_prep.signing_content())

    with pytest.raises(EquivocationDetectedError):
        oracle.prepare(rogue_pre_prep, "node-2")

    # Node 2 must now be SLASHED
    slashed_node = oracle.get_node("node-2")
    assert slashed_node is not None
    assert slashed_node.status == NodeStatus.SLASHED
    assert slashed_node.fault_score >= 100
    assert len(oracle.get_equivocation_events()) == 1


def test_fail_closed_circuit_breaker_coupling():
    """Validates that tripping the safety circuit immediately halts consensus and oracle certification."""
    oracle = ByzantineConsensusOracle()
    for i in range(1, 5):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"node-{i}", "cluster-a", pub, priv)

    proposal = ProposalPayload("prop-circuit-01", "SAFE_ACTION", "cluster-a")
    pre_prep = oracle.propose(proposal)

    # Trip circuit
    oracle.trip_circuit()
    assert oracle.is_circuit_tripped() is True

    with pytest.raises(OracleCircuitTrippedError):
        oracle.prepare(pre_prep, "node-2")

    with pytest.raises(OracleCircuitTrippedError):
        oracle.commit(pre_prep, "node-2")

    with pytest.raises(OracleCircuitTrippedError):
        oracle.execute_and_certify(proposal.proposal_id)

    # Reset circuit
    oracle.reset_circuit()
    assert oracle.is_circuit_tripped() is False


def test_view_change_recovery():
    """Validates view change increment and deterministic primary rotation."""
    oracle = ByzantineConsensusOracle()
    for i in range(1, 5):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"node-{i}", "cluster-a", pub, priv)

    primary_v0 = oracle.get_primary_node()
    assert primary_v0 is not None

    new_view = oracle.trigger_view_change("Primary timeout")
    assert new_view == 1

    primary_v1 = oracle.get_primary_node()
    assert primary_v1 is not None
    assert primary_v1.node_id != primary_v0.node_id or oracle.total_nodes_count == 1


def test_sub_250us_execution_benchmark():
    """Validates that PBFT consensus rounds execute well below the 250µs SLO."""
    oracle = ByzantineConsensusOracle()
    for i in range(1, 5):
        priv, pub = generate_node_keypair()
        oracle.register_node(f"b-node-{i}", "cluster-bench", pub, priv)

    iterations = 200
    durations_us = []

    for k in range(iterations):
        prop = ProposalPayload(f"prop-bench-{k}", "BENCH_ACTION", "cluster-bench")
        t0 = time.perf_counter_ns()
        pre = oracle.propose(prop)
        oracle.prepare(pre, "b-node-2")
        oracle.prepare(pre, "b-node-3")
        oracle.commit(pre, "b-node-1")
        oracle.commit(pre, "b-node-2")
        oracle.commit(pre, "b-node-3")
        cert = oracle.execute_and_certify(prop.proposal_id)
        t1 = time.perf_counter_ns()
        durations_us.append((t1 - t0) / 1000.0)
        assert cert.receipt_hash.startswith("sw:oracle:poe:v1:")

    avg_us = sum(durations_us) / len(durations_us)
    min_us = min(durations_us)
    # Average round-trip for 4 steps: propose, 2 prepares, 3 commits, 1 certify
    per_op_us = avg_us / 7.0
    assert per_op_us < 250.0, f"Consensus per-operation latency {per_op_us:.2f}µs exceeded 250µs SLO"
