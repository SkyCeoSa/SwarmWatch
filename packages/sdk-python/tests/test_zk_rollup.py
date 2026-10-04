# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Zero-Knowledge State Transition Rollup & Succinct Proof Verification (Seam 75 / ADR 0086).
"""

import pytest
from swarmwatch.zk_rollup import (
    BN254_PRIME,
    StateMerkleTree,
    ZkRollupEngine,
    RollupStatus,
    ProofSystem,
    RollupProofReceipt,
    RollupCircuitTrippedError,
    SequenceDiscontinuityError,
    FraudulentStateTransitionError,
    RollupVerificationFailedError,
    ZkRollupError,
    sha256_hex,
)


def test_state_merkle_tree_operations():
    """Verify Sparse State Merkle Tree deterministic root derivation and updates."""
    tree = StateMerkleTree()
    initial_root = tree.get_root()
    assert initial_root == "0" * 64

    # Set slot 1
    val1 = sha256_hex("agent_alpha_state_v1")
    tree.set_slot("agent_alpha", val1)
    root1 = tree.get_root()
    assert root1 != initial_root
    assert len(root1) == 64

    # Set slot 2
    val2 = sha256_hex("agent_beta_state_v1")
    tree.set_slot("agent_beta", val2)
    root2 = tree.get_root()
    assert root2 != root1

    # Clone tree and verify isolation
    clone = tree.clone()
    assert clone.get_root() == root2
    clone.set_slot("agent_gamma", sha256_hex("gamma_v1"))
    assert clone.get_root() != tree.get_root()
    assert tree.get_slot("agent_gamma") == "0" * 64


def test_transition_queuing_and_monotonicity():
    """Verify strict sequence monotonicity and transition ingestion."""
    engine = ZkRollupEngine()
    v0 = "0" * 64
    v1 = sha256_hex("state_1")
    v2 = sha256_hex("state_2")

    t1 = engine.queue_transition(
        agent_id="agent_1",
        slot_key="slot_main",
        from_value_hash=v0,
        to_value_hash=v1,
        delta_payload="increment(1)",
    )
    assert t1.sequence == 1
    assert t1.slot_key == "slot_main"
    assert t1.to_value_hash == v1

    t2 = engine.queue_transition(
        agent_id="agent_1",
        slot_key="slot_main",
        from_value_hash=v1,
        to_value_hash=v2,
        delta_payload="increment(2)",
    )
    assert t2.sequence == 2
    assert t2.to_value_hash == v2

    # Attempt gapped sequence (seq 4 instead of 3)
    with pytest.raises(SequenceDiscontinuityError):
        engine.queue_transition(
            agent_id="agent_1",
            slot_key="slot_main",
            from_value_hash=v2,
            to_value_hash=sha256_hex("state_3"),
            delta_payload="increment(3)",
            sequence=4,
        )

    # Verify circuit tripped after discontinuity
    assert engine.get_status()["circuit_tripped"] is True


def test_fraudulent_pre_state_rejection():
    """Verify detection and rejection of fraudulent pre-state assertions."""
    engine = ZkRollupEngine()
    v0 = "0" * 64
    v1 = sha256_hex("state_1")

    engine.queue_transition(
        agent_id="agent_1",
        slot_key="slot_account",
        from_value_hash=v0,
        to_value_hash=v1,
        delta_payload="init()",
    )

    # Submit next transition claiming wrong pre-state hash
    fraudulent_from = sha256_hex("forged_pre_state")
    with pytest.raises(FraudulentStateTransitionError):
        engine.queue_transition(
            agent_id="agent_1",
            slot_key="slot_account",
            from_value_hash=fraudulent_from,
            to_value_hash=sha256_hex("state_2"),
            delta_payload="mutate()",
        )

    assert engine.get_status()["circuit_tripped"] is True


def test_batch_commitment_and_succinct_proof():
    """Verify batch aggregation and zero-knowledge succinct proof synthesis."""
    engine = ZkRollupEngine()

    curr_slots = {f"slot_{i}": "0" * 64 for i in range(5)}
    for i in range(5):
        next_val = sha256_hex(f"value_{i}")
        engine.queue_transition(
            agent_id=f"agent_{i}",
            slot_key=f"slot_{i}",
            from_value_hash=curr_slots[f"slot_{i}"],
            to_value_hash=next_val,
            delta_payload=f"write_{i}",
            private_scratchpad=f"secret_scratchpad_{i}",
        )
        curr_slots[f"slot_{i}"] = next_val

    receipt = engine.commit_batch(batch_id="batch_alpha_001")
    assert receipt.batch_id == "batch_alpha_001"
    assert receipt.transition_count == 5
    assert receipt.sequence_start == 1
    assert receipt.sequence_end == 5
    assert receipt.receipt_hash.startswith("sw:rollup:v1:")
    assert receipt.is_valid is True

    # Check ZK proof data fields
    zk_data = receipt.zk_proof_data
    assert "eval_point" in zk_data
    assert "proof_eval" in zk_data
    assert "blinding_commitment" in zk_data
    assert "delta_merkle_root" in zk_data

    # Verify proof in O(1)
    is_valid, err = engine.verify_rollup_proof(receipt)
    assert is_valid is True
    assert err is None


def test_proof_verification_fails_on_tampering():
    """Verify proof verification rejects tampered receipts and forgery."""
    engine = ZkRollupEngine()
    engine.queue_transition(
        agent_id="agent_x",
        slot_key="x_slot",
        from_value_hash="0" * 64,
        to_value_hash=sha256_hex("x_val"),
        delta_payload="set_x",
    )
    receipt = engine.commit_batch()

    # 1. Tamper with evaluation proof
    tampered_data = dict(receipt.zk_proof_data)
    tampered_data["proof_eval"] = hex(123456789)
    tampered_receipt = RollupProofReceipt(
        rollup_id=receipt.rollup_id,
        batch_id=receipt.batch_id,
        sequence_start=receipt.sequence_start,
        sequence_end=receipt.sequence_end,
        pre_state_root=receipt.pre_state_root,
        post_state_root=receipt.post_state_root,
        transition_count=receipt.transition_count,
        batch_digest=receipt.batch_digest,
        proof_system=receipt.proof_system,
        zk_proof_data=tampered_data,
        receipt_hash=receipt.receipt_hash,
    )

    is_valid, err = engine.verify_rollup_proof(tampered_receipt)
    assert is_valid is False
    assert "Receipt hash mismatch" in err or "failed" in err

    # 2. Tamper with receipt hash domain tag
    invalid_tag_receipt = RollupProofReceipt(
        rollup_id=receipt.rollup_id,
        batch_id=receipt.batch_id,
        sequence_start=receipt.sequence_start,
        sequence_end=receipt.sequence_end,
        pre_state_root=receipt.pre_state_root,
        post_state_root=receipt.post_state_root,
        transition_count=receipt.transition_count,
        batch_digest=receipt.batch_digest,
        proof_system=receipt.proof_system,
        zk_proof_data=receipt.zk_proof_data,
        receipt_hash="invalid_prefix:12345",
    )
    is_valid, err = engine.verify_rollup_proof(invalid_tag_receipt)
    assert is_valid is False
    assert "Invalid receipt hash domain prefix" in err


def test_fraud_challenge_flow():
    """Verify fraud dispute challenges and state invalidation."""
    engine = ZkRollupEngine()
    engine.queue_transition(
        agent_id="agent_y",
        slot_key="y_slot",
        from_value_hash="0" * 64,
        to_value_hash=sha256_hex("y_val"),
        delta_payload="set_y",
    )
    receipt = engine.commit_batch("batch_fraud_test")

    # Challenge with corrupt pre-state evidence
    evidence = {"pre_state_root": "0" * 64}
    # If claimed pre_state_root does not match batch.pre_state_root, fraud is detected
    fraud_detected = engine.challenge_fraud(
        batch_id="batch_fraud_test",
        challenge_type="PRE_STATE_CORRUPTION",
        evidence={"pre_state_root": "f" * 64},
    )
    assert fraud_detected is True
    batch = engine.get_batch("batch_fraud_test")
    assert batch.status == RollupStatus.CHALLENGED
    assert receipt.is_valid is False
    assert engine.get_status()["circuit_tripped"] is True


def test_circuit_breaker_lockout():
    """Verify fail-closed circuit breaker halts subsequent transitions."""
    engine = ZkRollupEngine()
    engine.trip_circuit("Manual operator emergency trip")

    with pytest.raises(RollupCircuitTrippedError):
        engine.queue_transition(
            agent_id="agent_z",
            slot_key="z_slot",
            from_value_hash="0" * 64,
            to_value_hash=sha256_hex("z_val"),
            delta_payload="op",
        )

    with pytest.raises(RollupCircuitTrippedError):
        engine.commit_batch()


def test_zero_knowledge_privacy_masking():
    """Verify private scratchpads are masked and never exposed in public transitions."""
    engine = ZkRollupEngine()
    secret_scratchpad = "Confidential prompt reasoning: transfer $10M to vault"

    t = engine.queue_transition(
        agent_id="agent_secret",
        slot_key="vault_slot",
        from_value_hash="0" * 64,
        to_value_hash=sha256_hex("vault_funded"),
        delta_payload="fund_vault()",
        private_scratchpad=secret_scratchpad,
    )

    # Public dict must omit the private scratchpad string
    pub_dict = t.to_dict(include_private_mask=False)
    assert "private_payload_mask" not in pub_dict
    assert secret_scratchpad not in str(pub_dict)

    # Internal masked dict has only the cryptographic mask hash
    masked_dict = t.to_dict(include_private_mask=True)
    assert "private_payload_mask" in masked_dict
    assert masked_dict["private_payload_mask"] != secret_scratchpad
    assert len(masked_dict["private_payload_mask"]) == 64
