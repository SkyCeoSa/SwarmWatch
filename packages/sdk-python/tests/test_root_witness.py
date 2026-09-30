# -*- coding: utf-8 -*-
"""
Tests for Independent Root Witness & Public-Key Chains (Seam 64 / ADR 0075).
Verifies asymmetric ECDSA (NIST P-256 / ES256) signing, RFC 6962 logarithmic
consistency proofs, rollback and split-brain fork attack detection, NIST SP 800-57
verifiable key rotation logs, and sub-250 microsecond verification SLO.
"""

import json
import os
import shutil
import tempfile
import time
from typing import Generator
import pytest

from swarmwatch.root_witness import (
    ConsistencyProof,
    ConsistencyVerificationError,
    KeyRotationError,
    KeyRotationRecord,
    LeafReceipt,
    RollbackAttackError,
    RootWitness,
    SignedCheckpoint,
    SplitBrainForkError,
    WitnessError,
    WitnessKeyManager,
    WitnessStatus,
    global_root_witness,
)


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    path = tempfile.mkdtemp(prefix="swarmwatch_witness_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_witness_key_manager_generation_and_serialization() -> None:
    km = WitnessKeyManager.generate()
    assert km.public_key_b64 is not None
    assert km.private_key_b64 is not None

    pub_raw = km.get_raw_public_key()
    assert len(pub_raw) == 65
    assert pub_raw[0] == 0x04  # Uncompressed EC point

    # Restore from serialized base64url keys
    km_restored = WitnessKeyManager(
        private_key_b64=km.private_key_b64,
        public_key_b64=km.public_key_b64,
    )
    assert km_restored.public_key_b64 == km.public_key_b64
    assert km_restored.get_raw_public_key() == pub_raw


def test_asymmetric_signing_and_verification() -> None:
    km = WitnessKeyManager.generate()
    message = b"SwarmWatch Root Witness Checkpoint Anchor"

    sig = km.sign(message)
    assert len(sig) == 64  # IEEE P1363 (32 bytes r || 32 bytes s)

    # Valid verification
    assert km.verify(message, sig) is True

    # Tampered message fails
    assert km.verify(b"Tampered message content", sig) is False

    # Independent verifier with ONLY the public key
    verifier = WitnessKeyManager.from_public_key(km.public_key_b64)
    assert verifier.verify(message, sig) is True
    assert verifier.verify(b"Tampered message content", sig) is False

    # Different key fails
    other_km = WitnessKeyManager.generate()
    assert other_km.verify(message, sig) is False


def test_witness_leaf_submission_and_receipt() -> None:
    witness = RootWitness()
    assert witness.current_tree_size == 0

    r1 = witness.submit("span-001-entropy-commitment")
    assert isinstance(r1, LeafReceipt)
    assert r1.leaf_index == 0
    assert len(r1.leaf_hash) == 64
    assert witness.current_tree_size == 1

    r2 = witness.submit(b"span-002-circuit-trip-event")
    assert r2.leaf_index == 1
    assert r2.leaf_hash != r1.leaf_hash
    assert witness.current_tree_size == 2


def test_signed_checkpoint_creation_and_hash_chain() -> None:
    witness = RootWitness()
    
    # Checkpoint on empty tree (Genesis checkpoint)
    cp0 = witness.checkpoint(metadata={"description": "Genesis Checkpoint"})
    assert isinstance(cp0, SignedCheckpoint)
    assert cp0.epoch == 0
    assert cp0.tree_size == 0
    assert cp0.prev_checkpoint_hash == "0" * 64
    assert len(cp0.checkpoint_hash) == 64
    assert len(cp0.signature) > 0
    assert RootWitness.verify_checkpoint(cp0) is True

    # Submit leaves and create epoch 1
    witness.submit("leaf-1")
    witness.submit("leaf-2")
    cp1 = witness.checkpoint(metadata={"reason": "Batch epoch 1"})
    assert cp1.epoch == 1
    assert cp1.tree_size == 2
    assert cp1.prev_checkpoint_hash == cp0.checkpoint_hash
    assert RootWitness.verify_checkpoint(cp1) is True

    # Submit more leaves and create epoch 2
    witness.submit("leaf-3")
    witness.submit("leaf-4")
    witness.submit("leaf-5")
    cp2 = witness.checkpoint()
    assert cp2.epoch == 2
    assert cp2.tree_size == 5
    assert cp2.prev_checkpoint_hash == cp1.checkpoint_hash
    assert RootWitness.verify_checkpoint(cp2) is True


def test_rfc6962_consistency_proof_generation_and_verification() -> None:
    witness = RootWitness()

    # Build a tree of 15 leaves across multiple checkpoints
    for i in range(15):
        witness.submit(f"transaction-leaf-{i}")

    cp_small = witness.checkpoint()  # epoch 0, tree_size 15

    for i in range(15, 35):
        witness.submit(f"transaction-leaf-{i}")

    cp_large = witness.checkpoint()  # epoch 1, tree_size 35

    proof = witness.get_consistency_proof(cp_small.tree_size, cp_large.tree_size)
    assert isinstance(proof, ConsistencyProof)
    assert proof.old_size == 15
    assert proof.new_size == 35
    assert proof.old_root == cp_small.root_hash
    assert proof.new_root == cp_large.root_hash
    assert len(proof.proof_hashes) > 0

    # Verify consistency proof mathematically
    valid = RootWitness.verify_consistency(cp_small, cp_large, proof)
    assert valid is True

    # Verify with identical size (empty proof)
    same_proof = witness.get_consistency_proof(cp_large.tree_size, cp_large.tree_size)
    assert len(same_proof.proof_hashes) == 0
    assert RootWitness.verify_consistency(cp_large, cp_large, same_proof) is True

    # Tampering with a proof hash fails verification
    tampered_hashes = list(proof.proof_hashes)
    tampered_hashes[0] = "f" * 64
    tampered_proof = ConsistencyProof(
        old_size=proof.old_size,
        new_size=proof.new_size,
        old_root=proof.old_root,
        new_root=proof.new_root,
        proof_hashes=tampered_hashes,
    )
    with pytest.raises(ConsistencyVerificationError):
        RootWitness.verify_consistency(cp_small, cp_large, tampered_proof)


def test_rollback_attack_detection() -> None:
    witness = RootWitness()
    witness.submit("data-1")
    cp1 = witness.checkpoint()  # epoch 0, tree_size 1

    witness.submit("data-2")
    cp2 = witness.checkpoint()  # epoch 1, tree_size 2

    # Attempt to submit an old or rolled-back checkpoint
    with pytest.raises(RollbackAttackError, match="Rollback attack detected"):
        witness.ingest_external_checkpoint(cp1)

    # Fabricate a validly signed checkpoint with smaller tree_size but advancing epoch
    now_ns = time.time_ns()
    forged_hash = SignedCheckpoint.compute_checkpoint_hash(
        epoch=2,
        tree_size=1,  # Truncated tree!
        root_hash=cp1.root_hash,
        prev_checkpoint_hash=cp2.checkpoint_hash,
        timestamp_ns=now_ns,
        witness_public_key=witness.key_manager.public_key_b64,
    )
    forged_sig = witness.key_manager.sign_b64(forged_hash.encode("utf-8"))
    forged_cp = SignedCheckpoint(
        epoch=2,
        tree_size=1,  # Truncated tree!
        root_hash=cp1.root_hash,
        prev_checkpoint_hash=cp2.checkpoint_hash,
        checkpoint_hash=forged_hash,
        witness_public_key=witness.key_manager.public_key_b64,
        signature=forged_sig,
        timestamp_ns=now_ns,
    )
    with pytest.raises(RollbackAttackError, match="Tree size truncation detected"):
        witness.ingest_external_checkpoint(forged_cp)


def test_split_brain_fork_attack_detection() -> None:
    witness1 = RootWitness()
    witness1.submit("branch-a-1")
    cp1 = witness1.checkpoint()  # epoch 0

    # Parallel divergent witness advancing to epoch 1
    witness2 = RootWitness()
    witness2.submit("branch-b-0")
    witness2.checkpoint()  # epoch 0
    witness2.submit("branch-b-1")
    cp2_divergent = witness2.checkpoint()  # epoch 1, points to witness2's epoch 0

    # Submitting divergent checkpoint to witness1 without matching prev_hash
    with pytest.raises(SplitBrainForkError, match="Split-brain fork detected"):
        witness1.ingest_external_checkpoint(cp2_divergent)


def test_public_verification_zero_knowledge_of_private_key() -> None:
    witness = RootWitness()
    witness.submit("public-evidence-item")
    cp = witness.checkpoint()

    # External auditor has ONLY public key string
    public_key_b64 = cp.witness_public_key

    # Verifies checkpoint signature with public key
    assert RootWitness.verify_checkpoint(cp, public_key=public_key_b64) is True

    # Checkpoint tampered with modified root_hash fails
    tampered_dict = cp.to_dict()
    tampered_dict["root_hash"] = "e" * 64
    tampered_cp = SignedCheckpoint.from_dict(tampered_dict)
    assert RootWitness.verify_checkpoint(tampered_cp, public_key=public_key_b64) is False

    # Checkpoint tampered with modified epoch fails
    tampered_dict2 = cp.to_dict()
    tampered_dict2["epoch"] = 999
    tampered_cp2 = SignedCheckpoint.from_dict(tampered_dict2)
    assert RootWitness.verify_checkpoint(tampered_cp2, public_key=public_key_b64) is False


def test_nist_sp_800_57_key_rotation() -> None:
    witness = RootWitness()
    initial_pub = witness.key_manager.public_key_b64

    witness.submit("leaf-before-rotation")
    cp1 = witness.checkpoint()

    # Rotate to new key
    new_km = WitnessKeyManager.generate()
    new_pub = new_km.public_key_b64

    rotation_rec = witness.rotate_key(new_km)
    assert isinstance(rotation_rec, KeyRotationRecord)
    assert rotation_rec.old_public_key == initial_pub
    assert rotation_rec.new_public_key == new_pub
    assert rotation_rec.rotation_epoch == cp1.epoch + 1

    # Verify rotation record signatures (proof of possession + authorization)
    assert RootWitness.verify_rotation_record(rotation_rec) is True

    # Next checkpoint is signed by NEW key
    witness.submit("leaf-after-rotation")
    cp2 = witness.checkpoint()
    assert cp2.witness_public_key == new_pub
    assert RootWitness.verify_checkpoint(cp2) is True

    # Verify rotation log from genesis public key
    assert witness.verify_rotation_log(initial_pub) is True

    # Tampered rotation record fails
    bad_record = KeyRotationRecord.from_dict(rotation_rec.to_dict())
    bad_record.new_public_key = WitnessKeyManager.generate().public_key_b64
    with pytest.raises(KeyRotationError):
        RootWitness.verify_rotation_record(bad_record)


def test_witness_durability_and_checkpoint_journal(temp_dir: str) -> None:
    storage_path = os.path.join(temp_dir, "witness_checkpoints.jsonl")

    # Instance 1: write checkpoints to file
    w1 = RootWitness(storage_path=storage_path)
    w1.submit("record-alpha")
    cp0 = w1.checkpoint()
    w1.submit("record-beta")
    cp1 = w1.checkpoint()
    assert w1.total_checkpoints == 2

    # Instance 2: reload from file
    w2 = RootWitness(storage_path=storage_path, key_manager=w1.key_manager)
    assert w2.total_checkpoints == 2
    assert w2.latest_epoch == 1
    assert w2.current_tree_size == 2
    assert w2.get_latest_checkpoint().checkpoint_hash == cp1.checkpoint_hash

    # Ensure continuous appending works after reload
    w2.submit("record-gamma")
    cp2 = w2.checkpoint()
    assert cp2.epoch == 2
    assert cp2.prev_checkpoint_hash == cp1.checkpoint_hash


def test_sub_250us_consistency_verification_slo() -> None:
    witness = RootWitness()
    # Populate tree with 64 leaves
    for i in range(64):
        witness.submit(f"perf-leaf-{i}")
    cp_32 = witness.checkpoint()  # at tree_size 64, but let's make consistency proof between 32 and 64

    # We can request proof between any 0 < m < n
    proof = witness.get_consistency_proof(32, 64)

    # Warmup
    for _ in range(10):
        RootWitness.verify_consistency(
            SignedCheckpoint(epoch=0, tree_size=32, root_hash=proof.old_root, prev_checkpoint_hash="", checkpoint_hash="", witness_public_key="", signature="", timestamp_ns=0),
            SignedCheckpoint(epoch=1, tree_size=64, root_hash=proof.new_root, prev_checkpoint_hash="", checkpoint_hash="", witness_public_key="", signature="", timestamp_ns=0),
            proof,
        )

    # Microbenchmark: best of 3 runs of 500 iterations
    best_avg_us = float("inf")
    iterations = 500

    dummy_cp1 = SignedCheckpoint(epoch=0, tree_size=32, root_hash=proof.old_root, prev_checkpoint_hash="", checkpoint_hash="", witness_public_key="", signature="", timestamp_ns=0)
    dummy_cp2 = SignedCheckpoint(epoch=1, tree_size=64, root_hash=proof.new_root, prev_checkpoint_hash="", checkpoint_hash="", witness_public_key="", signature="", timestamp_ns=0)

    for _ in range(3):
        t0 = time.perf_counter()
        for _ in range(iterations):
            RootWitness.verify_consistency(dummy_cp1, dummy_cp2, proof)
        t1 = time.perf_counter()
        avg_us = ((t1 - t0) / iterations) * 1_000_000
        if avg_us < best_avg_us:
            best_avg_us = avg_us

    # Assert under the 250 microsecond SLO (typically ~25-35 µs)
    assert best_avg_us < 250.0, f"Consistency verification took {best_avg_us:.2f}µs, exceeding 250µs SLO"


def test_status_and_global_instance() -> None:
    status = global_root_witness.status()
    assert isinstance(status, WitnessStatus)
    assert status.latest_epoch >= -1
    assert len(status.active_public_key) > 0
    d = status.to_dict()
    assert "active_public_key" in d
    assert "latest_epoch" in d
    assert "current_tree_size" in d


def test_root_witness_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app

    app = create_app()
    client = TestClient(app)

    # 1. Submit leaf
    res = client.post("/api/v1/witness/submit", json={"leaf_data": "rest-api-evidence-01", "metadata": {"origin": "test"}})
    assert res.status_code == 200
    receipt = res.json()
    assert "leaf_index" in receipt
    assert "leaf_hash" in receipt

    # 2. Checkpoint
    res = client.post("/api/v1/witness/checkpoint", json={"metadata": {"batch": "rest-batch-1"}})
    assert res.status_code == 200
    cp = res.json()
    assert "epoch" in cp
    assert "root_hash" in cp
    assert "signature" in cp

    # 3. Get checkpoint
    res = client.get("/api/v1/witness/checkpoint")
    assert res.status_code == 200
    assert res.json()["epoch"] == cp["epoch"]

    # 4. Verify checkpoint
    res = client.post("/api/v1/witness/verify", json={"checkpoint": cp})
    assert res.status_code == 200
    assert res.json()["valid"] is True

    # 5. Rotate key
    res = client.post("/api/v1/witness/rotate", json={})
    assert res.status_code == 200
    rot = res.json()
    assert "old_public_key" in rot
    assert "new_public_key" in rot

    # 6. Status
    res = client.get("/api/v1/witness/status")
    assert res.status_code == 200
    stat = res.json()
    assert stat["total_rotations"] >= 1
    assert stat["total_checkpoints"] >= 1

