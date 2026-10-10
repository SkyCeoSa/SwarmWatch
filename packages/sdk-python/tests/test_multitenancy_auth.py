# -*- coding: utf-8 -*-
"""
TDD Test Suite for SwarmWatch Multi-Tenant Architecture & Cryptographic Auth (Phase 6).
"""
import pytest
from starlette.testclient import TestClient

from swarmwatch.tenant import Organization, Project, ApiKey, ApiKeyManager, TenantContext
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app
from swarmwatch.schema import SwarmSpan


def test_api_key_generation_and_hashing():
    manager = ApiKeyManager()
    raw_live, key_live = manager.create_api_key_record(project_id="proj-1", name="Live Key", is_test=False)
    assert raw_live.startswith("sw_live_")
    assert key_live.project_id == "proj-1"
    assert key_live.key_prefix.startswith("sw_live_")
    assert key_live.key_hash == manager.hash_key(raw_live)
    assert key_live.is_active is True

    # Test key verification
    verified = manager.verify_raw_key(raw_live)
    assert verified is not None
    assert verified.id == key_live.id

    # Test invalid key
    assert manager.verify_raw_key("sw_live_invalidkey1234567890") is None
    assert manager.verify_raw_key("") is None


def test_storage_tenant_crud_and_row_level_isolation(tmp_path):
    db_path = str(tmp_path / "tenant_test.db")
    storage = SQLiteStorageAdapter(db_path=db_path)

    # 1. Organization CRUD
    org = Organization(id="org-alpha", name="Alpha Corp", slug="alpha-corp")
    storage.create_organization_sync(org)
    fetched_org = storage.get_organization_sync("org-alpha")
    assert fetched_org is not None
    assert fetched_org.name == "Alpha Corp"

    # 2. Project CRUD
    p1 = Project(id="proj-1", org_id="org-alpha", name="Billing Swarm", slug="billing-swarm")
    p2 = Project(id="proj-2", org_id="org-alpha", name="Support Swarm", slug="support-swarm")
    storage.create_project_sync(p1)
    storage.create_project_sync(p2)

    projects = storage.list_projects_sync(org_id="org-alpha")
    assert len(projects) == 2

    # 3. Row-Level Tenant Isolation
    span_p1 = SwarmSpan(
        id="span-p1",
        swarm_id="swarm-alpha",
        project_id="proj-1",
        source_agent="AgentA",
        target_agent="AgentB",
        span_type="agent_call",
    )
    span_p2 = SwarmSpan(
        id="span-p2",
        swarm_id="swarm-alpha",
        project_id="proj-2",
        source_agent="AgentC",
        target_agent="AgentD",
        span_type="agent_call",
    )
    storage.save_spans_sync([span_p1, span_p2])

    # Query with Project 1 filter
    p1_spans = storage.query_spans(swarm_id="swarm-alpha", project_id="proj-1")
    assert len(p1_spans) == 1
    assert p1_spans[0].id == "span-p1"

    # Query with Project 2 filter
    p2_spans = storage.query_spans(swarm_id="swarm-alpha", project_id="proj-2")
    assert len(p2_spans) == 1
    assert p2_spans[0].id == "span-p2"


def test_server_authentication_and_key_revocation(tmp_path):
    db_path = str(tmp_path / "server_auth.db")
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. Create a key via REST API (unauthenticated local creates for default-project)
    key_resp = client.post("/api/v1/keys", json={
        "project_id": "default-project",
        "name": "Test Ingestion Key",
        "is_test": False
    })
    assert key_resp.status_code == 200
    key_data = key_resp.json()
    raw_key = key_data["api_key"]
    key_id = key_data["id"]
    assert raw_key.startswith("sw_live_")

    # 2. Ingest span with valid Authorization header
    ingest_resp = client.post(
        "/api/v1/spans",
        headers={"Authorization": f"Bearer {raw_key}"},
        json=[{
            "id": "auth-span-01",
            "swarm_id": "auth-swarm",
            "source_agent": "AuthAgent",
            "target_agent": "TargetAgent",
            "span_type": "agent_call"
        }]
    )
    assert ingest_resp.status_code == 200
    assert ingest_resp.json()["status"] == "persisted"

    # 3. Ingest span with INVALID Authorization header -> 401
    bad_resp = client.post(
        "/api/v1/spans",
        headers={"Authorization": "Bearer invalid_test_token"},
        json=[{
            "id": "bad-span",
            "swarm_id": "auth-swarm",
            "source_agent": "BadAgent",
            "target_agent": "TargetAgent",
            "span_type": "agent_call"
        }]
    )
    assert bad_resp.status_code == 401

    # 4. Revoke key via DELETE endpoint
    del_resp = client.delete(f"/api/v1/keys/{key_id}")
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "revoked"

    # 5. Ingest span with REVOKED key -> 401
    revoked_resp = client.post(
        "/api/v1/spans",
        headers={"Authorization": f"Bearer {raw_key}"},
        json=[{
            "id": "revoked-span",
            "swarm_id": "auth-swarm",
            "source_agent": "RevokedAgent",
            "target_agent": "TargetAgent",
            "span_type": "agent_call"
        }]
    )
    assert revoked_resp.status_code == 401
