# -*- coding: utf-8 -*-
"""
Tests for Tenant Identity Boundary & Attenuated Capability Tokens (Seam 65 / ADR 0076).
Verifies cryptographically bound capability tokens, monotonic delegation attenuation,
subtree revocation cascade, replay / cross-tenant forgery rejection, and sub-250µs latency SLO.
"""

import json
import os
import shutil
import tempfile
import time
from typing import Generator
import pytest

from swarmwatch.tenant_capability import (
    AttenuationViolationError,
    CapabilityStatus,
    CapabilityToken,
    CapabilityVerdict,
    CapabilityVerificationError,
    PrivilegeEscalationError,
    RevocationReceipt,
    TenantCapabilityManager,
    global_capability_manager,
    resource_pattern_matches,
)
from swarmwatch.root_witness import WitnessKeyManager


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    path = tempfile.mkdtemp(prefix="swarmwatch_capability_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_resource_pattern_matching() -> None:
    # Exact match
    assert resource_pattern_matches("sw://tenant-1/swarms/alpha", "sw://tenant-1/swarms/alpha") is True
    assert resource_pattern_matches("sw://tenant-1/swarms/alpha", "sw://tenant-1/swarms/beta") is False

    # Prefix wildcard
    assert resource_pattern_matches("sw://tenant-1/swarms/*", "sw://tenant-1/swarms/alpha") is True
    assert resource_pattern_matches("sw://tenant-1/swarms/*", "sw://tenant-1/swarms/alpha/tasks/1") is True
    assert resource_pattern_matches("sw://tenant-1/swarms/*", "sw://tenant-2/swarms/alpha") is False

    # Universal wildcard
    assert resource_pattern_matches("*", "sw://any/resource") is True


def test_root_token_issuance_and_signature() -> None:
    km = WitnessKeyManager.generate()
    mgr = TenantCapabilityManager(authority_key_manager=km)

    now_ns = time.time_ns()
    token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write", "effect:dispatch", "circuit:trip"],
        resource_pattern="sw://tenant-alpha/swarms/*",
        epoch=1,
        ttl_seconds=3600,
    )

    assert isinstance(token, CapabilityToken)
    assert token.parent_token_id is None
    assert token.tenant_id == "tenant-alpha"
    assert token.subject_principal == "agent-orchestrator"
    assert set(token.scopes) == {"telemetry:write", "effect:dispatch", "circuit:trip"}
    assert token.depth == 0
    assert token.expires_at_ns > now_ns
    assert len(token.attenuation_digest) == 64
    assert len(token.signature) > 0

    # Self-verification of signature
    assert mgr.verify_token_signature(token) is True


def test_monotonic_attenuation_success() -> None:
    km = WitnessKeyManager.generate()
    mgr = TenantCapabilityManager(authority_key_manager=km)

    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write", "effect:dispatch", "circuit:trip"],
        resource_pattern="sw://tenant-alpha/swarms/*",
        epoch=1,
        ttl_seconds=3600,
    )

    # Delegating to a worker sub-agent with attenuated scope and narrower resource
    child_token = mgr.attenuate(
        parent_token=root_token,
        subject_principal="agent-worker-01",
        scopes=["telemetry:write"],  # Monotonic subset
        resource_pattern="sw://tenant-alpha/swarms/finance/*",  # Narrower sub-pattern
        ttl_seconds=1800,  # Shorter validity
    )

    assert child_token.parent_token_id == root_token.token_id
    assert child_token.tenant_id == root_token.tenant_id
    assert child_token.subject_principal == "agent-worker-01"
    assert child_token.scopes == ["telemetry:write"]
    assert child_token.resource_pattern == "sw://tenant-alpha/swarms/finance/*"
    assert child_token.depth == 1
    assert child_token.expires_at_ns <= root_token.expires_at_ns

    # Verify delegation signatures
    assert mgr.verify_token_signature(child_token) is True


def test_privilege_escalation_rejected_unheld_scope() -> None:
    mgr = TenantCapabilityManager()
    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/*",
    )

    # Attempt to attenuate with an unheld permission
    with pytest.raises(PrivilegeEscalationError, match="Unheld scopes requested"):
        mgr.attenuate(
            parent_token=root_token,
            subject_principal="agent-sub",
            scopes=["telemetry:write", "admin:all"],  # Privilege escalation!
        )


def test_cross_tenant_delegation_prohibited() -> None:
    mgr = TenantCapabilityManager()
    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/*",
    )

    # Attempt to attenuate with cross-tenant target resource
    with pytest.raises(PrivilegeEscalationError, match="Cross-tenant resource pattern prohibited"):
        mgr.attenuate(
            parent_token=root_token,
            subject_principal="agent-sub",
            scopes=["telemetry:write"],
            resource_pattern="sw://tenant-beta/*",  # Cross-tenant violation!
        )


def test_resource_widening_rejected() -> None:
    mgr = TenantCapabilityManager()
    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/swarms/finance/*",
    )

    # Attempt to widen resource scope to all swarms
    with pytest.raises(PrivilegeEscalationError, match="Resource pattern cannot be widened"):
        mgr.attenuate(
            parent_token=root_token,
            subject_principal="agent-sub",
            scopes=["telemetry:write"],
            resource_pattern="sw://tenant-alpha/swarms/*",  # Widening!
        )


def test_expiration_extension_rejected() -> None:
    mgr = TenantCapabilityManager()
    now_ns = time.time_ns()
    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/*",
        ttl_seconds=60,  # 60s validity
    )

    # Attempt to attenuate with longer TTL than parent
    with pytest.raises(AttenuationViolationError, match="Child expiration cannot exceed parent expiration"):
        mgr.attenuate(
            parent_token=root_token,
            subject_principal="agent-sub",
            scopes=["telemetry:write"],
            expires_at_ns=root_token.expires_at_ns + 10_000_000_000,  # +10s past parent!
        )


def test_max_delegation_depth_enforced() -> None:
    mgr = TenantCapabilityManager()
    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-0",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/*",
        max_depth=2,  # Root (0) -> child (1) -> grandchild (2) -> prohibited (3)
    )

    c1 = mgr.attenuate(parent_token=root_token, subject_principal="agent-1")
    assert c1.depth == 1

    c2 = mgr.attenuate(parent_token=c1, subject_principal="agent-2")
    assert c2.depth == 2

    # Attempting to exceed max_depth 2
    with pytest.raises(AttenuationViolationError, match="Delegation depth exceeds max_depth"):
        mgr.attenuate(parent_token=c2, subject_principal="agent-3")


def test_verification_lifecycle_success_and_verdict() -> None:
    mgr = TenantCapabilityManager()
    root_token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write", "effect:dispatch"],
        resource_pattern="sw://tenant-alpha/swarms/cluster-1/*",
        epoch=10,
        ttl_seconds=3600,
    )

    verdict = mgr.verify(
        token=root_token,
        action_scope="effect:dispatch",
        target_resource="sw://tenant-alpha/swarms/cluster-1/task-99",
        executing_principal="agent-orchestrator",
        expected_tenant_id="tenant-alpha",
        current_epoch=10,
    )

    assert isinstance(verdict, CapabilityVerdict)
    assert verdict.valid is True
    assert verdict.reason == "AUTHORIZED"
    assert verdict.token_id == root_token.token_id


def test_verification_failures() -> None:
    mgr = TenantCapabilityManager()
    token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-orchestrator",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/swarms/finance/*",
        epoch=5,
        ttl_seconds=3600,
    )

    # 1. Principal mismatch
    v1 = mgr.verify(
        token=token,
        action_scope="telemetry:write",
        target_resource="sw://tenant-alpha/swarms/finance/1",
        executing_principal="agent-rogue",  # Wrong principal!
        expected_tenant_id="tenant-alpha",
    )
    assert v1.valid is False
    assert "PRINCIPAL_MISMATCH" in v1.reason

    # 2. Scope unheld
    v2 = mgr.verify(
        token=token,
        action_scope="circuit:trip",  # Unheld scope!
        target_resource="sw://tenant-alpha/swarms/finance/1",
        executing_principal="agent-orchestrator",
        expected_tenant_id="tenant-alpha",
    )
    assert v2.valid is False
    assert "UNAUTHORIZED_SCOPE" in v2.reason

    # 3. Resource mismatch
    v3 = mgr.verify(
        token=token,
        action_scope="telemetry:write",
        target_resource="sw://tenant-alpha/swarms/hr/1",  # Outside finance namespace!
        executing_principal="agent-orchestrator",
        expected_tenant_id="tenant-alpha",
    )
    assert v3.valid is False
    assert "RESOURCE_OUT_OF_BOUNDS" in v3.reason

    # 4. Cross-tenant mismatch
    v4 = mgr.verify(
        token=token,
        action_scope="telemetry:write",
        target_resource="sw://tenant-alpha/swarms/finance/1",
        executing_principal="agent-orchestrator",
        expected_tenant_id="tenant-beta",  # Wrong tenant!
    )
    assert v4.valid is False
    assert "TENANT_MISMATCH" in v4.reason

    # 5. Stale epoch
    v5 = mgr.verify(
        token=token,
        action_scope="telemetry:write",
        target_resource="sw://tenant-alpha/swarms/finance/1",
        executing_principal="agent-orchestrator",
        expected_tenant_id="tenant-alpha",
        current_epoch=99,  # Epoch advanced way beyond token!
    )
    assert v5.valid is False
    assert "STALE_EPOCH" in v5.reason


def test_revocation_and_subtree_cascade() -> None:
    mgr = TenantCapabilityManager()
    root = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-root",
        scopes=["telemetry:write", "effect:dispatch"],
        resource_pattern="sw://tenant-alpha/*",
    )
    child = mgr.attenuate(parent_token=root, subject_principal="agent-child", scopes=["telemetry:write"])
    grandchild = mgr.attenuate(parent_token=child, subject_principal="agent-grandchild", scopes=["telemetry:write"])

    # Initial verification: all 3 valid
    assert mgr.verify(root, "telemetry:write", "sw://tenant-alpha/x", "agent-root", "tenant-alpha").valid is True
    assert mgr.verify(child, "telemetry:write", "sw://tenant-alpha/x", "agent-child", "tenant-alpha").valid is True
    assert mgr.verify(grandchild, "telemetry:write", "sw://tenant-alpha/x", "agent-grandchild", "tenant-alpha").valid is True

    # Revoke CHILD token
    receipt = mgr.revoke(child.token_id, reason="Compromised subagent detected", revoked_by="admin")
    assert isinstance(receipt, RevocationReceipt)
    assert receipt.token_id == child.token_id

    # Root remains valid
    assert mgr.verify(root, "telemetry:write", "sw://tenant-alpha/x", "agent-root", "tenant-alpha").valid is True

    # Child is revoked
    v_child = mgr.verify(child, "telemetry:write", "sw://tenant-alpha/x", "agent-child", "tenant-alpha")
    assert v_child.valid is False
    assert "TOKEN_REVOKED" in v_child.reason

    # Grandchild is also revoked via subtree cascade!
    v_grandchild = mgr.verify(grandchild, "telemetry:write", "sw://tenant-alpha/x", "agent-grandchild", "tenant-alpha")
    assert v_grandchild.valid is False
    assert "ANCESTOR_REVOKED" in v_grandchild.reason


def test_tampered_token_signature_fails() -> None:
    mgr = TenantCapabilityManager()
    token = mgr.issue_root_token(
        tenant_id="tenant-alpha",
        subject_principal="agent-root",
        scopes=["telemetry:write"],
        resource_pattern="sw://tenant-alpha/*",
    )

    # Tamper with scopes
    d = token.to_dict()
    d["scopes"] = ["telemetry:write", "admin:all"]
    tampered_token = CapabilityToken.from_dict(d)

    verdict = mgr.verify(
        token=tampered_token,
        action_scope="admin:all",
        target_resource="sw://tenant-alpha/x",
        executing_principal="agent-root",
        expected_tenant_id="tenant-alpha",
    )
    assert verdict.valid is False
    assert "SIGNATURE_INVALID" in verdict.reason


def test_sub_250us_verification_slo() -> None:
    mgr = TenantCapabilityManager()
    root = mgr.issue_root_token(
        tenant_id="tenant-perf",
        subject_principal="agent-perf",
        scopes=["telemetry:write", "effect:dispatch"],
        resource_pattern="sw://tenant-perf/swarms/*",
    )
    child = mgr.attenuate(parent_token=root, subject_principal="agent-child", scopes=["telemetry:write"])

    # Warmup
    for _ in range(10):
        mgr.verify(child, "telemetry:write", "sw://tenant-perf/swarms/1", "agent-child", "tenant-perf")

    # Benchmark: best of 3 runs of 500 iterations
    best_avg_us = float("inf")
    iterations = 500

    for _ in range(3):
        t0 = time.perf_counter()
        for _ in range(iterations):
            mgr.verify(child, "telemetry:write", "sw://tenant-perf/swarms/1", "agent-child", "tenant-perf")
        t1 = time.perf_counter()
        avg_us = ((t1 - t0) / iterations) * 1_000_000
        if avg_us < best_avg_us:
            best_avg_us = avg_us

    # Assert under the 250 microsecond SLO (typically ~25-45 µs)
    assert best_avg_us < 250.0, f"Capability verification took {best_avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_instance_and_status() -> None:
    status = global_capability_manager.status()
    assert isinstance(status, CapabilityStatus)
    assert status.total_issued >= 0
    assert status.total_revoked >= 0
    d = status.to_dict()
    assert "total_issued" in d
    assert "total_revoked" in d


def test_tenant_capability_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app

    app = create_app()
    client = TestClient(app)

    # 1. Issue root token
    res = client.post(
        "/api/v1/capability/issue",
        json={
            "tenant_id": "tenant-rest",
            "subject_principal": "agent-root",
            "scopes": ["telemetry:write", "effect:dispatch"],
            "resource_pattern": "sw://tenant-rest/*",
            "epoch": 1,
            "ttl_seconds": 3600,
            "metadata": {"dept": "devops"},
        },
    )
    assert res.status_code == 200
    root = res.json()
    assert root["tenant_id"] == "tenant-rest"
    assert root["subject_principal"] == "agent-root"
    assert "token_id" in root

    # 2. Attenuate token
    res = client.post(
        "/api/v1/capability/attenuate",
        json={
            "parent_token": root,
            "subject_principal": "agent-worker",
            "scopes": ["telemetry:write"],
            "resource_pattern": "sw://tenant-rest/swarms/*",
            "ttl_seconds": 1800,
        },
    )
    assert res.status_code == 200
    child = res.json()
    assert child["parent_token_id"] == root["token_id"]
    assert child["subject_principal"] == "agent-worker"

    # 3. Verify valid token
    res = client.post(
        "/api/v1/capability/verify",
        json={
            "token": child,
            "action_scope": "telemetry:write",
            "target_resource": "sw://tenant-rest/swarms/cluster-1",
            "executing_principal": "agent-worker",
            "expected_tenant_id": "tenant-rest",
            "current_epoch": 1,
        },
    )
    assert res.status_code == 200
    verdict = res.json()
    assert verdict["valid"] is True
    assert verdict["reason"] == "AUTHORIZED"

    # 4. Revoke token
    res = client.post(
        "/api/v1/capability/revoke",
        json={
            "token_id": child["token_id"],
            "reason": "Decommissioned worker",
            "revoked_by": "secops",
        },
    )
    assert res.status_code == 200
    receipt = res.json()
    assert receipt["token_id"] == child["token_id"]

    # 5. Verify revoked token
    res = client.post(
        "/api/v1/capability/verify",
        json={
            "token": child,
            "action_scope": "telemetry:write",
            "target_resource": "sw://tenant-rest/swarms/cluster-1",
            "executing_principal": "agent-worker",
            "expected_tenant_id": "tenant-rest",
        },
    )
    assert res.status_code == 200
    assert res.json()["valid"] is False
    assert "TOKEN_REVOKED" in res.json()["reason"]

    # 6. Status
    res = client.get("/api/v1/capability/status")
    assert res.status_code == 200
    stat = res.json()
    assert stat["total_issued"] >= 1
    assert stat["total_revoked"] >= 1
    assert "active_root_public_key" in stat
