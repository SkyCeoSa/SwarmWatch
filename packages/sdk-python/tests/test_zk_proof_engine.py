# -*- coding: utf-8 -*-
"""
Tests for Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine.
Seam 49 / ADR 0060.
"""

import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.zk_proof import (
    ProofCommitmentType,
    SpanCommitmentLeaf,
    MerkleAuditProof,
    CausalAuditReceipt,
    CausalProofAccumulator,
    ZkProofEngine,
    global_zk_proof_engine,
    sha256_digest,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


def test_span_commitment_leaf_and_hashing():
    leaf1 = SpanCommitmentLeaf(
        span_id="span-test-01",
        turn_index=1,
        agent_id="agent-planner",
        payload_hash=sha256_digest("Plan execution steps"),
        parent_span_ids=["span-root"],
        circuit_tripped=False,
    )
    assert leaf1.leaf_hash != ""
    assert len(leaf1.leaf_hash) == 64

    # Serialization roundtrip
    d = leaf1.to_dict()
    assert d["span_id"] == "span-test-01"
    assert d["agent_id"] == "agent-planner"
    assert d["circuit_tripped"] is False

    leaf2 = SpanCommitmentLeaf.from_dict(d)
    assert leaf2.span_id == leaf1.span_id
    assert leaf2.leaf_hash == leaf1.leaf_hash

    # Determinism & Sensitivity: changing any attribute changes leaf_hash
    leaf_tampered = SpanCommitmentLeaf(
        span_id="span-test-01",
        turn_index=2,  # changed
        agent_id="agent-planner",
        payload_hash=sha256_digest("Plan execution steps"),
        parent_span_ids=["span-root"],
        circuit_tripped=False,
    )
    assert leaf_tampered.leaf_hash != leaf1.leaf_hash


def test_accumulator_empty_and_single_leaf():
    acc = CausalProofAccumulator(swarm_id="swarm-unit-empty")
    assert acc.total_leaves == 0
    empty_root = acc.get_merkle_root()
    assert empty_root == sha256_digest("empty_swarm_session")

    # Single leaf
    leaf = acc.commit_span(
        span_id="span-singleton",
        turn_index=0,
        agent_id="agent-0",
        payload_content="singleton payload",
    )
    assert acc.total_leaves == 1
    root = acc.get_merkle_root()
    assert root == leaf.leaf_hash

    proof = acc.generate_inclusion_proof("span-singleton")
    assert proof.leaf_hash == leaf.leaf_hash
    assert proof.leaf_index == 0
    assert len(proof.sibling_hashes) == 0
    assert CausalProofAccumulator.verify_inclusion_proof(proof) is True


def test_accumulator_balanced_merkle_tree_and_proofs():
    # Test with 5 leaves (odd number to exercise right-duplication in Merkle tree balancing)
    acc = CausalProofAccumulator(swarm_id="swarm-unit-odd")
    span_ids = [f"span-{i}" for i in range(5)]

    for i, s_id in enumerate(span_ids):
        acc.commit_span(
            span_id=s_id,
            turn_index=i,
            agent_id=f"agent-{i % 2}",
            payload_content=f"Turn message payload #{i}",
            parent_span_ids=[span_ids[i - 1]] if i > 0 else [],
        )

    assert acc.total_leaves == 5
    root = acc.get_merkle_root()
    assert len(root) == 64

    # Verify proofs for each leaf
    for i, s_id in enumerate(span_ids):
        proof = acc.generate_inclusion_proof(s_id)
        assert proof.leaf_index == i
        assert proof.total_leaves == 5
        assert proof.merkle_root == root
        is_valid = CausalProofAccumulator.verify_inclusion_proof(proof)
        assert is_valid is True

        # Test index-based lookup
        proof_by_idx = acc.generate_inclusion_proof(i)
        assert proof_by_idx.leaf_hash == proof.leaf_hash
        assert CausalProofAccumulator.verify_inclusion_proof(proof_by_idx) is True


def test_causal_audit_receipt_generation_and_verification():
    acc = CausalProofAccumulator(swarm_id="swarm-unit-receipt")
    for i in range(4):
        acc.commit_span(
            span_id=f"step-{i}",
            turn_index=i,
            agent_id="agent-coder",
            payload_content=f"Code generation output part {i}",
        )

    receipt = acc.generate_audit_receipt("step-2", commitment_type=ProofCommitmentType.TURN_INCLUSION)
    assert receipt.receipt_id.startswith("rcpt_")
    assert receipt.swarm_id == "swarm-unit-receipt"
    assert receipt.commitment_type == ProofCommitmentType.TURN_INCLUSION
    assert receipt.verified is True
    assert len(receipt.provenance_signature) == 64

    # Verification passes
    valid, reason = acc.verify_audit_receipt(receipt)
    assert valid is True
    assert reason is None

    # Serialization roundtrip
    d = receipt.to_dict()
    receipt_deserialized = CausalAuditReceipt.from_dict(d)
    assert receipt_deserialized.receipt_id == receipt.receipt_id
    valid2, reason2 = acc.verify_audit_receipt(receipt_deserialized)
    assert valid2 is True
    assert reason2 is None


def test_tamper_detection_in_audit_receipt():
    acc = CausalProofAccumulator(swarm_id="swarm-unit-tamper")
    acc.commit_span(
        span_id="valid-span-01",
        turn_index=0,
        agent_id="agent-verifier",
        payload_content="Critical compliance text",
    )
    receipt = acc.generate_audit_receipt("valid-span-01")

    # 1. Tamper span attributes (e.g. malicious claim that payload was different)
    tampered_leaf = SpanCommitmentLeaf(
        span_id="valid-span-01",
        turn_index=0,
        agent_id="agent-verifier",
        payload_hash=sha256_digest("Malicious altered text"),
        parent_span_ids=[],
        leaf_hash=receipt.span_commitment.leaf_hash,  # keeping old hash claiming it matches
    )
    bad_receipt1 = CausalAuditReceipt(
        receipt_id=receipt.receipt_id,
        swarm_id=receipt.swarm_id,
        commitment_type=receipt.commitment_type,
        span_commitment=tampered_leaf,
        merkle_proof=receipt.merkle_proof,
        session_merkle_root=receipt.session_merkle_root,
        provenance_signature=receipt.provenance_signature,
    )
    valid1, reason1 = acc.verify_audit_receipt(bad_receipt1)
    assert valid1 is False
    assert "Leaf hash does not match" in reason1

    # 2. Tamper provenance HMAC signature
    bad_receipt2 = CausalAuditReceipt(
        receipt_id=receipt.receipt_id,
        swarm_id=receipt.swarm_id,
        commitment_type=receipt.commitment_type,
        span_commitment=receipt.span_commitment,
        merkle_proof=receipt.merkle_proof,
        session_merkle_root=receipt.session_merkle_root,
        provenance_signature="deadbeef0123456789abcdefdeadbeef0123456789abcdefdeadbeef0123456789",
    )
    valid2, reason2 = acc.verify_audit_receipt(bad_receipt2)
    assert valid2 is False
    assert "Invalid provenance HMAC" in reason2

    # 3. Wrong secret key
    valid3, reason3 = acc.verify_audit_receipt(receipt, secret_key="wrong_secret_key")
    assert valid3 is False
    assert "Invalid provenance HMAC" in reason3


def test_circuit_trip_commitment_and_verification():
    acc = CausalProofAccumulator(swarm_id="swarm-circuit-trip")
    acc.commit_span(
        span_id="normal-step",
        turn_index=0,
        agent_id="agent-runner",
        payload_content="Running tool query",
    )
    acc.commit_span(
        span_id="tripped-step",
        turn_index=1,
        agent_id="agent-runner",
        payload_content="Exceeded recursion limit",
        circuit_tripped=True,
        trip_reason="Conversational recursion entropy collapse",
    )

    receipt = acc.generate_audit_receipt("tripped-step", commitment_type=ProofCommitmentType.CIRCUIT_TRIP)
    assert receipt.commitment_type == ProofCommitmentType.CIRCUIT_TRIP
    assert receipt.span_commitment.circuit_tripped is True
    assert receipt.span_commitment.trip_reason_hash is not None

    valid, reason = acc.verify_audit_receipt(receipt)
    assert valid is True
    assert reason is None


def test_zk_proof_engine_multi_swarm_isolation():
    engine = ZkProofEngine()
    engine.commit_span("swarm-a", "span-a1", 0, "agent-1", payload_content="data a")
    engine.commit_span("swarm-b", "span-b1", 0, "agent-2", payload_content="data b")

    root_a = engine.get_root("swarm-a")
    root_b = engine.get_root("swarm-b")
    assert root_a != root_b

    receipt_a = engine.generate_receipt("swarm-a", "span-a1")
    valid_a, _ = engine.verify_receipt(receipt_a)
    assert valid_a is True

    manifest_a = engine.get_manifest("swarm-a")
    assert manifest_a["total_leaves"] == 1
    assert manifest_a["swarm_id"] == "swarm-a"

    # Reset swarm-a only
    engine.reset("swarm-a")
    assert engine.get_manifest("swarm-a")["total_leaves"] == 0
    assert engine.get_manifest("swarm-b")["total_leaves"] == 1


def test_zk_proof_server_api_endpoints():
    app = create_app()
    client = TestClient(app)
    swarm_id = "swarm-api-zk-test"

    # 1. Commit span
    commit_payload = {
        "span_id": "span-api-01",
        "turn_index": 0,
        "agent_id": "agent-api",
        "payload_content": "User input prompt payload",
        "parent_span_ids": [],
        "circuit_tripped": False,
    }
    res = client.post(f"/api/v1/swarms/{swarm_id}/proof/commit", json=commit_payload)
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "committed"
    assert data["leaf"]["span_id"] == "span-api-01"
    assert "leaf_hash" in data["leaf"]

    # 2. Get Merkle root
    res_root = client.get(f"/api/v1/swarms/{swarm_id}/proof/root")
    assert res_root.status_code == 200
    root_data = res_root.json()
    assert root_data["swarm_id"] == swarm_id
    assert root_data["session_merkle_root"] == data["leaf"]["leaf_hash"]
    assert root_data["total_leaves"] == 1

    # 3. Generate receipt
    gen_payload = {
        "span_id": "span-api-01",
        "commitment_type": "TURN_INCLUSION",
    }
    res_gen = client.post(f"/api/v1/swarms/{swarm_id}/proof/generate", json=gen_payload)
    assert res_gen.status_code == 200
    receipt_data = res_gen.json()["receipt"]
    assert receipt_data["verified"] is True
    assert "provenance_signature" in receipt_data

    # 4. Verify receipt
    verify_payload = {
        "receipt": receipt_data,
    }
    res_verify = client.post(f"/api/v1/swarms/{swarm_id}/proof/verify", json=verify_payload)
    assert res_verify.status_code == 200
    v_data = res_verify.json()
    assert v_data["is_valid"] is True
    assert v_data["error"] is None

    # 5. Get manifest
    res_man = client.get(f"/api/v1/swarms/{swarm_id}/proof/manifest")
    assert res_man.status_code == 200
    man_data = res_man.json()
    assert man_data["total_leaves"] == 1
    assert len(man_data["leaves"]) == 1

    # 6. Reset
    res_reset = client.post(f"/api/v1/swarms/{swarm_id}/proof/reset")
    assert res_reset.status_code == 200
    assert res_reset.json()["status"] == "reset"


def test_zk_proof_cli_commands(capsys):
    swarm_id = "swarm-cli-zk-test"

    # Commit span
    with pytest.raises(SystemExit) as exc:
        main([
            "proof", "commit",
            "--swarm-id", swarm_id,
            "--span-id", "span-cli-01",
            "--turn", "0",
            "--agent", "agent-cli",
            "--payload", "CLI generated payload",
        ])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    assert "span-cli-01" in out

    # Root
    with pytest.raises(SystemExit) as exc:
        main(["proof", "root", "--swarm-id", swarm_id])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    assert "merkle_root" in out or "Session Merkle Root" in out

    # Generate receipt
    with pytest.raises(SystemExit) as exc:
        main(["proof", "generate", "--swarm-id", swarm_id, "--span-id", "span-cli-01"])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    assert "rcpt_" in out

    # Manifest
    with pytest.raises(SystemExit) as exc:
        main(["proof", "manifest", "--swarm-id", swarm_id])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    assert "Total Committed" in out or "total_leaves" in out

    # Reset
    with pytest.raises(SystemExit) as exc:
        main(["proof", "reset", "--swarm-id", swarm_id])
    assert exc.value.code == 0
    out, _ = capsys.readouterr()
    assert "reset" in out
