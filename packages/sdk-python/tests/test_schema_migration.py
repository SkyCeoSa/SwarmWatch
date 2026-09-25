"""
Unit and integration tests for Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine (Seam 47 / ADR 0058).
"""

import pytest
import json
from starlette.testclient import TestClient

from swarmwatch.schema_migration import (
    FieldTransformAction,
    SchemaVersion,
    FieldTransformRule,
    MigrationStep,
    MigrationPlan,
    MigrationResult,
    SchemaMigrationEngine,
    global_schema_migration_engine,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.cli import main


def test_schema_version_parsing_and_ordering():
    """Verifies semver semantic comparison and normalization."""
    v1 = SchemaVersion.from_string("1.0.0")
    v2 = SchemaVersion.from_string("1.1.0")
    v2_beta = SchemaVersion.from_string("2.0.0-beta.1")
    v2_clean = SchemaVersion.from_string("v2.0.0")

    assert v1 < v2
    assert v2 < v2_clean
    assert v2_clean.major == 2
    assert v2_clean.to_string() == "2.0.0"
    assert v1 <= SchemaVersion.from_string("1.0.0")
    assert v2 > v1
    assert v1 == SchemaVersion(1, 0, 0)
    assert str(v1) == "1.0.0"


def test_field_transform_primitives():
    """Tests all atomic FieldTransformAction operations in isolation."""
    # 1. RENAME
    data = {"old_name": "worker_agent", "nested": {"old_sub": 42}}
    r1 = FieldTransformRule(action=FieldTransformAction.RENAME, source_path="old_name", target_path="new_name")
    r1.apply(data)
    assert "old_name" not in data
    assert data["new_name"] == "worker_agent"

    r2 = FieldTransformRule(action=FieldTransformAction.RENAME, source_path="nested.old_sub", target_path="nested.new_sub")
    r2.apply(data)
    assert data["nested"]["new_sub"] == 42
    assert "old_sub" not in data["nested"]

    # 2. CAST_TYPE
    data_cast = {"count": "100", "ratio": "0.75", "flag": "true", "items": "alpha"}
    FieldTransformRule(action=FieldTransformAction.CAST_TYPE, source_path="count", target_type="int").apply(data_cast)
    FieldTransformRule(action=FieldTransformAction.CAST_TYPE, source_path="ratio", target_type="float").apply(data_cast)
    FieldTransformRule(action=FieldTransformAction.CAST_TYPE, source_path="items", target_type="list").apply(data_cast)
    assert data_cast["count"] == 100
    assert data_cast["ratio"] == 0.75
    assert data_cast["items"] == ["alpha"]

    # 3. ADD_DEFAULT
    data_default = {"existing": "ok"}
    FieldTransformRule(action=FieldTransformAction.ADD_DEFAULT, source_path="status", default_value="nominal").apply(data_default)
    FieldTransformRule(action=FieldTransformAction.ADD_DEFAULT, source_path="existing", default_value="overwrite").apply(data_default)
    assert data_default["status"] == "nominal"
    assert data_default["existing"] == "ok"

    # 4. REMOVE_DEPRECATED
    data_remove = {"keep": 1, "deprecated_key": "junk"}
    FieldTransformRule(action=FieldTransformAction.REMOVE_DEPRECATED, source_path="deprecated_key").apply(data_remove)
    assert "deprecated_key" not in data_remove
    assert data_remove["keep"] == 1

    # 5. NEST_FIELDS & FLATTEN_FIELDS
    data_nest = {"tokens": 500, "cost": 0.02, "name": "agent-01"}
    FieldTransformRule(
        action=FieldTransformAction.NEST_FIELDS,
        source_path="",
        target_path="resources",
        nested_keys=["tokens", "cost"]
    ).apply(data_nest)
    assert data_nest["resources"] == {"tokens": 500, "cost": 0.02}
    assert "tokens" not in data_nest

    # Flatten back
    FieldTransformRule(
        action=FieldTransformAction.FLATTEN_FIELDS,
        source_path="resources",
        target_path=""
    ).apply(data_nest)
    assert data_nest["tokens"] == 500
    assert data_nest["cost"] == 0.02
    assert "resources" not in data_nest

    # 6. COMPUTE_DERIVED
    data_derived = {"first": "Deep", "last": "Mind"}
    FieldTransformRule(
        action=FieldTransformAction.COMPUTE_DERIVED,
        source_path="",
        target_path="full_name",
        derived_fn=lambda d: f"{d.get('first')} {d.get('last')}"
    ).apply(data_derived)
    assert data_derived["full_name"] == "Deep Mind"


def test_migration_step_forward_and_backward():
    """Verifies that MigrationStep applies forward and backward transformations correctly."""
    step = MigrationStep(
        source_version="1.0.0",
        target_version="1.1.0",
        forward_rules=[
            FieldTransformRule(action=FieldTransformAction.RENAME, source_path="user", target_path="user_id"),
            FieldTransformRule(action=FieldTransformAction.ADD_DEFAULT, source_path="status", default_value="active"),
        ],
        backward_rules=[
            FieldTransformRule(action=FieldTransformAction.RENAME, source_path="user_id", target_path="user"),
            FieldTransformRule(action=FieldTransformAction.REMOVE_DEPRECATED, source_path="status"),
        ],
        description="User ID rename and status default"
    )

    original = {"user": "alice", "score": 98}
    forward = step.apply_forward(original)
    assert forward == {"user_id": "alice", "status": "active", "score": 98}
    assert original == {"user": "alice", "score": 98}  # original unharmed

    backward = step.apply_backward(forward)
    assert backward == {"user": "alice", "score": 98}


def test_bfs_shortest_path_planning_and_execution():
    """Tests multi-hop shortest path graph planning across discrete schema versions."""
    engine = SchemaMigrationEngine()
    engine.register_version("1.0.0")
    engine.register_version("1.1.0")
    engine.register_version("2.0.0")
    engine.register_version("3.0.0")

    # 1.0.0 <-> 1.1.0
    engine.register_step(
        MigrationStep(
            source_version="1.0.0",
            target_version="1.1.0",
            forward_rules=[FieldTransformRule(action=FieldTransformAction.RENAME, source_path="actor", target_path="agent_id")],
            backward_rules=[FieldTransformRule(action=FieldTransformAction.RENAME, source_path="agent_id", target_path="actor")],
        )
    )
    # 1.1.0 <-> 2.0.0
    engine.register_step(
        MigrationStep(
            source_version="1.1.0",
            target_version="2.0.0",
            forward_rules=[FieldTransformRule(action=FieldTransformAction.ADD_DEFAULT, source_path="tier", default_value="enterprise")],
            backward_rules=[FieldTransformRule(action=FieldTransformAction.REMOVE_DEPRECATED, source_path="tier")],
        )
    )
    # 2.0.0 <-> 3.0.0
    engine.register_step(
        MigrationStep(
            source_version="2.0.0",
            target_version="3.0.0",
            forward_rules=[FieldTransformRule(action=FieldTransformAction.CAST_TYPE, source_path="token_val", target_type="int")],
            backward_rules=[],
        )
    )

    # Multi-hop Forward (1.0.0 -> 3.0.0)
    plan = engine.plan_migration("1.0.0", "3.0.0")
    assert plan is not None
    assert plan.total_steps == 3
    assert [d for _, d in plan.steps] == ["forward", "forward", "forward"]

    payload = {"actor": "planner", "token_val": "250"}
    res = engine.migrate(payload, "1.0.0", "3.0.0")
    assert res.is_valid
    assert res.applied_steps_count == 3
    assert res.applied_path == ["1.0.0", "1.1.0", "2.0.0", "3.0.0"]
    assert res.transformed_payload == {
        "agent_id": "planner",
        "tier": "enterprise",
        "token_val": 250,
    }

    # Reverse Hop (2.0.0 -> 1.0.0)
    rev_res = engine.migrate({"agent_id": "planner", "tier": "enterprise"}, "2.0.0", "1.0.0")
    assert rev_res.applied_steps_count == 2
    assert rev_res.applied_path == ["2.0.0", "1.1.0", "1.0.0"]
    assert rev_res.transformed_payload == {"actor": "planner"}


def test_round_trip_idempotency():
    """Verifies round-trip transformation consistency using default global registered schemas."""
    initial_payload = {
        "agent": "coordinator",
        "tokens": "500",
        "cost_dollars": 0.02,
    }

    # Migrate 1.0.0 -> 2.0.0
    fwd_res = global_schema_migration_engine.migrate(initial_payload, "1.0.0", "2.0.0")
    assert fwd_res.applied_steps_count == 2
    assert fwd_res.transformed_payload["agent_id"] == "coordinator"
    assert fwd_res.transformed_payload["turn_status"] == "completed"
    assert fwd_res.transformed_payload["resource_usage"]["tokens"] == 500

    # Migrate back 2.0.0 -> 1.0.0
    back_res = global_schema_migration_engine.migrate(fwd_res.transformed_payload, "2.0.0", "1.0.0")
    assert back_res.applied_steps_count == 2
    assert back_res.transformed_payload["agent"] == "coordinator"
    assert back_res.transformed_payload["tokens"] == 500
    assert "resource_usage" not in back_res.transformed_payload
    assert "turn_status" not in back_res.transformed_payload


def test_fastapi_schema_endpoints():
    """Tests control plane REST endpoints for schema migration."""
    storage = SQLiteStorageAdapter(":memory:")
    app = create_app(storage=storage)
    client = TestClient(app)

    # 1. GET /api/v1/schemas/status
    resp = client.get("/api/v1/schemas/status")
    assert resp.status_code == 200
    status_data = resp.json()
    assert "total_versions" in status_data
    assert "registered_versions" in status_data
    assert status_data["total_versions"] >= 3

    # 2. GET /api/v1/schemas/versions
    resp_v = client.get("/api/v1/schemas/versions")
    assert resp_v.status_code == 200
    versions_data = resp_v.json()
    assert "1.0.0" in versions_data["versions"]
    assert "2.0.0" in versions_data["versions"]

    # 3. POST /api/v1/schemas/migrate
    migrate_body = {
        "source_version": "1.0.0",
        "target_version": "2.0.0",
        "payload": {"agent": "synthesizer", "tokens": "880"},
    }
    resp_m = client.post("/api/v1/schemas/migrate", json=migrate_body)
    assert resp_m.status_code == 200
    res_data = resp_m.json()
    assert res_data["applied_steps_count"] == 2
    assert res_data["transformed_payload"]["agent_id"] == "synthesizer"
    assert res_data["transformed_payload"]["resource_usage"]["tokens"] == 880

    # 4. POST /api/v1/schemas/validate
    resp_val = client.post("/api/v1/schemas/validate", json={"version": "1.0.0", "payload": {"agent": "bot", "tokens": 10}})
    assert resp_val.status_code == 200
    assert resp_val.json()["is_valid"] is True

    # 5. POST /api/v1/schemas/register-migration
    reg_body = {
        "source_version": "2.0.0",
        "target_version": "2.1.0",
        "forward_rules": [{"action": "add_default", "source_path": "region", "default_value": "us-east"}],
        "backward_rules": [{"action": "remove_deprecated", "source_path": "region"}],
        "description": "Add default cloud region",
    }
    resp_reg = client.post("/api/v1/schemas/register-migration", json=reg_body)
    assert resp_reg.status_code == 200
    assert resp_reg.json()["status"] == "registered"


def test_cli_schema_subcommands(capsys):
    """Verifies that the CLI schema subcommand executes status, versions, migrate, and validate."""
    # 1. status
    with pytest.raises(SystemExit) as exc:
        main(["schema", "status", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    status_obj = json.loads(captured.out)
    assert "total_versions" in status_obj

    # 2. versions
    with pytest.raises(SystemExit) as exc:
        main(["schema", "versions", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    versions_obj = json.loads(captured.out)
    assert "versions" in versions_obj

    # 3. migrate
    with pytest.raises(SystemExit) as exc:
        main(["schema", "migrate", "--source", "1.0.0", "--target", "1.1.0", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    migrated_obj = json.loads(captured.out)
    assert migrated_obj["source_version"] == "1.0.0"
    assert migrated_obj["target_version"] == "1.1.0"
    assert migrated_obj["transformed_payload"]["agent_id"] == "agent-planner"

    # 4. validate
    with pytest.raises(SystemExit) as exc:
        main(["schema", "validate", "--version", "1.0.0", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    val_obj = json.loads(captured.out)
    assert val_obj["is_valid"] is True
