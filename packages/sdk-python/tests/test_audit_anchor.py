# -*- coding: utf-8 -*-
"""
Tests for Durable Audit Anchor & Merkle Audit Receipt Engine (Seam 54 / ADR 0065).
"""

import json
import os
import tempfile
import pytest
from starlette.testclient import TestClient

from swarmwatch.audit_anchor import (
    AuditAnchor,
    AuditIntegrityError,
    MerkleAuditReceipt,
    RootEpoch,
    VerificationResult,
    global_audit_anchor,
    sha256_digest,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main


def test_genesis_initialization():
    """Verify Genesis epoch (e=0) is deterministically anchored upon initialization."""
    anchor = AuditAnchor(storage_path=":memory:")
    assert anchor.total_epochs == 1
    assert anchor.latest_epoch == 0

    st = anchor.status()
    assert st["status"] == "ANCHORED"
    assert st["total_epochs"] == 1
    assert st["latest_epoch"] == 0
    assert len(st["genesis_hash"]) == 64
    assert len(st["latest_digest"]) == 64

    # Audit chain check
    audit = anchor.audit_chain()
    assert audit["valid"] is True
    assert len(audit["errors"]) == 0


def test_append_root_monotonic_chain():
    """Verify append_root creates strictly monotonic epochs and updates rolling chain digest."""
    anchor = AuditAnchor(storage_path=":memory:")

    root_1 = sha256_digest("merkle_root_session_1")
    epoch_1 = anchor.append_root(root_hash=root_1, tenant_id="tenant-alpha")
    assert epoch_1.epoch == 1
    assert epoch_1.root_hash == root_1
    assert epoch_1.tenant_id == "tenant-alpha"
    assert epoch_1.prev_chain_digest == anchor._epochs[0].chain_digest
    assert len(epoch_1.signature) == 64

    root_2 = sha256_digest("merkle_root_session_2")
    epoch_2 = anchor.append_root(root_hash=root_2, tenant_id="tenant-beta")
    assert epoch_2.epoch == 2
    assert epoch_2.prev_chain_digest == epoch_1.chain_digest

    assert anchor.total_epochs == 3
    assert anchor.latest_epoch == 2
    assert anchor.latest_digest == epoch_2.chain_digest


def test_chain_digest_continuity():
    """Verify cryptographic chain digest calculation matches formal specification."""
    anchor = AuditAnchor(storage_path=":memory:")
    gen_digest = anchor._epochs[0].chain_digest

    r1 = sha256_digest("root_1")
    ep1 = anchor.append_root(root_hash=r1, tenant_id="tenant-x")
    expected_digest_1 = sha256_digest(f"{gen_digest}:1:{r1}:tenant-x")
    assert ep1.chain_digest == expected_digest_1

    r2 = sha256_digest("root_2")
    ep2 = anchor.append_root(root_hash=r2, tenant_id="tenant-y")
    expected_digest_2 = sha256_digest(f"{ep1.chain_digest}:2:{r2}:tenant-y")
    assert ep2.chain_digest == expected_digest_2


def test_merkle_receipt_issuance_and_verification():
    """Constructs a 2-leaf Merkle root, anchors it, issues an authentic receipt, and verifies it."""
    anchor = AuditAnchor(storage_path=":memory:")

    leaf_a = sha256_digest("span_leaf_01")
    leaf_b = sha256_digest("span_leaf_02")
    root_ab = sha256_digest(leaf_a + leaf_b)

    ep = anchor.append_root(root_hash=root_ab, tenant_id="prod-tenant")

    # Path for leaf_a: sibling is leaf_b on the right
    receipt = anchor.issue_receipt(
        epoch=ep.epoch,
        leaf_hash=leaf_a,
        audit_path=[("right", leaf_b)],
        tenant_id="prod-tenant",
    )
    assert isinstance(receipt, MerkleAuditReceipt)
    assert receipt.epoch == ep.epoch
    assert receipt.root_hash == root_ab
    assert receipt.leaf_hash == leaf_a
    assert len(receipt.signature) == 64

    # Verify receipt independently
    result = anchor.verify_receipt(receipt)
    assert isinstance(result, VerificationResult)
    assert result.valid is True
    assert result.reason is None
    assert result.epoch == ep.epoch


def test_tampered_receipt_detection():
    """Verifies that tampering with leaf, sibling, root, or signature is detected and rejected."""
    anchor = AuditAnchor(storage_path=":memory:")

    leaf_a = sha256_digest("valid_leaf_a")
    leaf_b = sha256_digest("valid_leaf_b")
    root_ab = sha256_digest(leaf_a + leaf_b)
    ep = anchor.append_root(root_hash=root_ab, tenant_id="security-tenant")

    receipt = anchor.issue_receipt(
        epoch=ep.epoch,
        leaf_hash=leaf_a,
        audit_path=[("right", leaf_b)],
        tenant_id="security-tenant",
    )

    # 1. Tampered leaf hash
    tampered_1 = MerkleAuditReceipt.from_dict(receipt.to_dict())
    tampered_1.leaf_hash = sha256_digest("forged_leaf")
    res1 = anchor.verify_receipt(tampered_1)
    assert res1.valid is False
    assert "Merkle path reconstruction" in (res1.reason or "")

    # 2. Tampered root hash
    tampered_2 = MerkleAuditReceipt.from_dict(receipt.to_dict())
    tampered_2.root_hash = sha256_digest("forged_root")
    tampered_2.audit_path = [("right", sha256_digest("dummy"))]
    res2 = anchor.verify_receipt(tampered_2)
    assert res2.valid is False

    # 3. Tampered signature
    tampered_3 = MerkleAuditReceipt.from_dict(receipt.to_dict())
    tampered_3.signature = "a" * 64
    res3 = anchor.verify_receipt(tampered_3)
    assert res3.valid is False
    assert "Invalid cryptographic HMAC" in (res3.reason or "")

    # 4. Unknown epoch
    tampered_4 = MerkleAuditReceipt.from_dict(receipt.to_dict())
    tampered_4.epoch = 9999
    res4 = anchor.verify_receipt(tampered_4)
    assert res4.valid is False
    assert "not found in durable audit anchor" in (res4.reason or "")


def test_post_crash_chain_of_custody_and_fail_closed():
    """Durable JSONL persistence survives restarts; tampered historical records fail closed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_file = os.path.join(tmpdir, "audit_test.jsonl")

        # 1. Initial process: anchor genesis and 2 epochs
        anchor_1 = AuditAnchor(storage_path=storage_file)
        r1 = sha256_digest("commit_1")
        anchor_1.append_root(root_hash=r1, tenant_id="tenant-1")
        r2 = sha256_digest("commit_2")
        anchor_1.append_root(root_hash=r2, tenant_id="tenant-2")
        assert anchor_1.total_epochs == 3

        # 2. Simulated process restart: instantiate new anchor pointing to same file
        anchor_restart = AuditAnchor(storage_path=storage_file)
        assert anchor_restart.total_epochs == 3
        assert anchor_restart.latest_epoch == 2
        assert anchor_restart.latest_digest == anchor_1.latest_digest
        assert anchor_restart.audit_chain()["valid"] is True

        # Append a new root after restart
        r3 = sha256_digest("commit_3")
        ep3 = anchor_restart.append_root(root_hash=r3, tenant_id="tenant-3")
        assert ep3.epoch == 3

        # 3. Tamper with historical epoch in file: rewrite root_hash of epoch 1
        with open(storage_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        tampered_record = json.loads(lines[1])
        tampered_record["root_hash"] = sha256_digest("tampered_historical_root")
        lines[1] = json.dumps(tampered_record) + "\n"

        with open(storage_file, "w", encoding="utf-8") as f:
            f.writelines(lines)

        # 4. Starting new anchor must detect tampering and fail closed
        with pytest.raises(AuditIntegrityError) as exc_info:
            AuditAnchor(storage_path=storage_file)
        assert "Chain digest mismatch" in str(exc_info.value) or "History rewriting detected" in str(exc_info.value)


def test_audit_rest_endpoints():
    """Verify REST API endpoints for append-root, issue-receipt, verify-receipt, chain, and status."""
    global_audit_anchor.reset(clear_storage=False)
    app = create_app()
    client = TestClient(app)

    # 1. Status endpoint
    st_res = client.get("/api/v1/audit/status")
    assert st_res.status_code == 200
    st_data = st_res.json()
    assert st_data["status"] == "ANCHORED"
    assert st_data["total_epochs"] >= 1

    # 2. Append root
    test_root = sha256_digest("rest_merkle_root_test")
    app_res = client.post(
        "/api/v1/audit/append-root",
        json={"root_hash": test_root, "tenant_id": "api-tenant", "metadata": {"origin": "test"}},
    )
    assert app_res.status_code == 200
    app_data = app_res.json()
    curr_epoch = app_data["epoch"]
    assert app_data["root_hash"] == test_root
    assert app_data["tenant_id"] == "api-tenant"

    # 3. Issue receipt
    leaf = test_root  # Single leaf root
    issue_res = client.post(
        "/api/v1/audit/issue-receipt",
        json={
            "epoch": curr_epoch,
            "leaf_hash": leaf,
            "audit_path": [],
            "tenant_id": "api-tenant",
        },
    )
    assert issue_res.status_code == 200
    rcpt_data = issue_res.json()
    assert rcpt_data["epoch"] == curr_epoch
    assert rcpt_data["leaf_hash"] == leaf

    # 4. Verify receipt
    verify_res = client.post(
        "/api/v1/audit/verify-receipt",
        json={"receipt": rcpt_data},
    )
    assert verify_res.status_code == 200
    v_data = verify_res.json()
    assert v_data["valid"] is True
    assert v_data["epoch"] == curr_epoch

    # 5. Chain audit
    chain_res = client.get("/api/v1/audit/chain")
    assert chain_res.status_code == 200
    c_data = chain_res.json()
    assert c_data["valid"] is True


def test_cli_audit_subcommands(capsys):
    """Verify CLI dispatch for audit status, append-root, chain, and verify-receipt."""
    global_audit_anchor.reset(clear_storage=False)

    # 1. CLI status
    with pytest.raises(SystemExit) as exc1:
        main(["audit", "status", "--format", "json"])
    assert exc1.value.code == 0
    out1 = capsys.readouterr().out
    assert '"status": "ANCHORED"' in out1

    # 2. CLI append-root
    test_root = sha256_digest("cli_root_leaf")
    with pytest.raises(SystemExit) as exc2:
        main(["audit", "append-root", "--root-hash", test_root, "--tenant", "cli-tenant", "--format", "json"])
    assert exc2.value.code == 0
    out2 = capsys.readouterr().out
    assert test_root in out2

    # 3. CLI chain
    with pytest.raises(SystemExit) as exc3:
        main(["audit", "chain", "--format", "json"])
    assert exc3.value.code == 0
    out3 = capsys.readouterr().out
    assert '"valid": true' in out3
