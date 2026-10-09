# -*- coding: utf-8 -*-
"""
Pytest configuration for SwarmWatch SDK.
Ensures seamless test execution across legacy test suites while preserving
strict production security validation in adversarial and RBAC suites.
"""
import os
import pytest


@pytest.fixture(autouse=True)
def configure_swarmwatch_test_environment(monkeypatch, request):
    """
    In accordance with the audit remediation (P0 #1):
    Production mode strictly denies unauthenticated control plane operations (returning 401/403).
    Adversarial exploit and MFA/RBAC test suites run with SWARMWATCH_SINGLE_TENANT_DEV unset
    to verify production security gates fail closed.
    Other seam test suites execute under SWARMWATCH_SINGLE_TENANT_DEV=1 to support
    local developer single-tenant mode without requiring live auth key ceremonies.
    """
    node_id = request.node.nodeid
    if "test_adversarial_audit_exploits" in node_id or "test_mfa_and_rbac" in node_id:
        monkeypatch.delenv("SWARMWATCH_SINGLE_TENANT_DEV", raising=False)
    else:
        monkeypatch.setenv("SWARMWATCH_SINGLE_TENANT_DEV", "1")


@pytest.fixture(autouse=True)
def cleanup_swarmwatch_runtime():
    """Ensures background telemetry transports and workers do not leak across tests."""
    yield
    try:
        import swarmwatch
        swarmwatch.shutdown()
    except Exception:
        pass

