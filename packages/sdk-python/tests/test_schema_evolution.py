# -*- coding: utf-8 -*-
"""
Unit and integration tests for Migration Preservation & Partial Isomorphisms (Seam 59 / ADR 0070).
Verifies:
1. Versioned Partial Isomorphisms: (f, g, Delta) where g(f(x).y, f(x).Delta) == x byte-for-byte.
2. Fail-Closed Reverse Invariant: Reversing a lossy migration without PreservedDelta raises LossyMigrationError.
3. UnsupportedReverseMigrationError for irreversible transitions.
4. Pre-flight loss classification via classify_loss.
5. REST API endpoints and CLI commands.
"""

import copy
import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.schema_evolution import (
    LossClassification,
    EvolutionAction,
    PreservedDelta,
    EvolutionRule,
    EvolutionStep,
    EvolutionPlan,
    MigrationResult,
    LossyMigrationError,
    UnsupportedReverseMigrationError,
    SchemaEvolutionError,
    SchemaEvolution,
    global_schema_evolution,
)
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


@pytest.fixture(autouse=True)
def reset_evolution():
    global_schema_evolution.reset()
    yield
    global_schema_evolution.reset()


def test_plan_lossless_and_lossy_paths():
    engine = SchemaEvolution()

    # Identical version
    plan_ident = engine.plan("1.0.0", "1.0.0")
    assert plan_ident.overall_classification == LossClassification.LOSSLESS
    assert plan_ident.can_reverse_without_delta is True
    assert len(plan_ident.steps) == 0

    # 1.0.0 -> 1.1.0 (Lossless)
    plan_1_1 = engine.plan("1.0.0", "1.1.0")
    assert plan_1_1.overall_classification == LossClassification.LOSSLESS
    assert plan_1_1.can_reverse_without_delta is True
    assert len(plan_1_1.steps) == 1

    # 1.0.0 -> 2.0.0 (Lossy)
    plan_2_0 = engine.plan("1.0.0", "2.0.0")
    assert plan_2_0.overall_classification == LossClassification.LOSSY_PRESERVED
    assert plan_2_0.can_reverse_without_delta is False
    assert len(plan_2_0.steps) == 2
    assert any("legacy_debug_trace" in f for f in plan_2_0.lossy_fields)

    # Missing path raises SchemaEvolutionError
    with pytest.raises(SchemaEvolutionError):
        engine.plan("1.0.0", "99.9.9")


def test_migrate_lossless_roundtrip():
    engine = SchemaEvolution()
    original_payload = {
        "agent_id": "agent-arch",
        "description": "System Architect",
    }

    # Forward 1.0.0 -> 1.1.0 (renames agent_id -> agent_key, adds default priority=1)
    res_fwd = engine.migrate(original_payload, "1.0.0", "1.1.0")
    assert res_fwd.classification == LossClassification.LOSSLESS
    assert res_fwd.migrated_payload["agent_key"] == "agent-arch"
    assert "agent_id" not in res_fwd.migrated_payload
    assert res_fwd.migrated_payload["priority"] == 1

    # Reverse 1.1.0 -> 1.0.0 WITHOUT delta (pure isomorphism succeeds)
    res_rev = engine.reverse(res_fwd.migrated_payload, "1.1.0", "1.0.0")
    assert res_rev.migrated_payload["agent_id"] == "agent-arch"
    assert "agent_key" not in res_rev.migrated_payload
    assert "priority" not in res_rev.migrated_payload
    assert res_rev.migrated_payload == original_payload


def test_migrate_lossy_preservation_and_fail_closed_reverse():
    engine = SchemaEvolution()
    original_payload = {
        "legacy_debug_trace": {"call_stack": ["turn1", "turn2"], "entropy": 1.45},
        "confidence_score": 0.875,
        "cpu_usage": 42.5,
        "memory_mb": 512,
        "agent_key": "agent-core",
    }

    # Forward 1.1.0 -> 2.0.0
    # Drops legacy_debug_trace, casts confidence_score float->int, nests cpu/mem into telemetry
    res = engine.migrate(original_payload, "1.1.0", "2.0.0", preserve_shadow=True)
    assert res.classification == LossClassification.LOSSY_PRESERVED
    migrated = res.migrated_payload

    assert "legacy_debug_trace" not in migrated
    assert migrated["confidence_score"] == 0  # int downcast
    assert migrated["telemetry"] == {"cpu_usage": 42.5, "memory_mb": 512}

    # PreservedDelta captured
    delta = res.preserved_delta
    assert delta is not None
    assert "legacy_debug_trace" in delta.removed_fields
    assert delta.pre_cast_values["confidence_score"] == 0.875
    assert delta.delta_hash is not None
    assert len(delta.delta_hash) == 64

    # INVARIANT: Reverse without delta MUST fail closed with LossyMigrationError
    with pytest.raises(LossyMigrationError) as exc_info:
        engine.reverse(migrated, "2.0.0", "1.1.0", preserved_delta=None)
    assert "legacy_debug_trace" in str(exc_info.value)

    # INVARIANT: Reverse WITH delta restores exact original state
    res_rev = engine.reverse(migrated, "2.0.0", "1.1.0", preserved_delta=delta)
    restored = res_rev.migrated_payload
    assert restored["legacy_debug_trace"] == original_payload["legacy_debug_trace"]
    assert restored["confidence_score"] == 0.875
    assert restored["cpu_usage"] == 42.5
    assert restored["memory_mb"] == 512
    assert restored == original_payload


def test_multi_hop_evolution_roundtrip():
    engine = SchemaEvolution()
    original_payload = {
        "agent_id": "agent-delta",
        "legacy_debug_trace": "critical_stack_trace_v1",
        "confidence_score": 0.999,
        "cpu_usage": 15.0,
        "memory_mb": 256,
    }

    # Migrate 1.0.0 -> 2.1.0 (traverses 1.0.0 -> 1.1.0 -> 2.0.0 -> 2.1.0)
    res_fwd = engine.migrate(original_payload, "1.0.0", "2.1.0")
    assert res_fwd.classification == LossClassification.LOSSY_PRESERVED
    v21_payload = res_fwd.migrated_payload

    assert v21_payload["agent_key"] == "agent-delta"
    assert v21_payload["summary_tag"] == "agent-agent-delta"
    assert v21_payload["cpu_usage"] == 15.0

    # Reverse 2.1.0 -> 1.0.0 using PreservedDelta
    res_rev = engine.reverse(v21_payload, "2.1.0", "1.0.0", preserved_delta=res_fwd.preserved_delta)
    restored = res_rev.migrated_payload

    assert restored == original_payload


def test_classify_loss_preflight():
    engine = SchemaEvolution()

    # Payload with lossy field
    lossy_payload = {
        "legacy_debug_trace": "some_trace",
        "confidence_score": 0.55,
    }
    loss_report = engine.classify_loss("1.1.0", "2.0.0", lossy_payload)
    assert loss_report["overall_classification"] == "LOSSY_PRESERVED"
    assert loss_report["can_reverse_without_delta"] is False
    assert any("legacy_debug_trace" in f for f in loss_report["lossy_fields"])

    # Payload without lossy field (confidence_score already integer, no legacy trace)
    clean_payload = {
        "confidence_score": 1,
        "cpu_usage": 10.0,
    }
    clean_report = engine.classify_loss("1.1.0", "2.0.0", clean_payload)
    # legacy_debug_trace was not present in clean_payload
    assert "legacy_debug_trace" not in clean_report["lossy_fields"]


def test_irreversible_step():
    engine = SchemaEvolution()
    step_irrev = EvolutionStep(
        source_version="2.1.0",
        target_version="3.0.0",
        rules=[
            EvolutionRule(
                action=EvolutionAction.COMPUTE_DERIVED,
                source_path="",
                target_path="one_way_hash",
                derived_fn=lambda d: "fixed_hash",
                is_irreversible=True,
            )
        ],
        declared_classification=LossClassification.IRREVERSIBLE,
        description="One-way destructive migration",
    )
    engine.register_step(step_irrev)

    plan = engine.plan("2.1.0", "3.0.0")
    assert plan.overall_classification == LossClassification.IRREVERSIBLE

    mig = engine.migrate({"test": 123}, "2.1.0", "3.0.0")
    assert mig.classification == LossClassification.IRREVERSIBLE

    with pytest.raises(UnsupportedReverseMigrationError):
        engine.reverse(mig.migrated_payload, "3.0.0", "2.1.0")


def test_server_schema_evolution_endpoints():
    app = create_app()
    client = TestClient(app)

    # 1. Plan endpoint
    plan_resp = client.post("/api/v1/schema-evolution/plan", json={
        "source_version": "1.0.0",
        "target_version": "2.0.0",
    })
    assert plan_resp.status_code == 200
    plan_data = plan_resp.json()
    assert plan_data["overall_classification"] == "LOSSY_PRESERVED"
    assert plan_data["step_count"] == 2

    # 2. Migrate endpoint
    migrate_resp = client.post("/api/v1/schema-evolution/migrate", json={
        "source_version": "1.1.0",
        "target_version": "2.0.0",
        "payload": {
            "legacy_debug_trace": "debug_data",
            "confidence_score": 0.77,
            "cpu_usage": 33.0,
            "memory_mb": 1024,
        },
        "preserve_shadow": True,
    })
    assert migrate_resp.status_code == 200
    mig_data = migrate_resp.json()
    assert mig_data["classification"] == "LOSSY_PRESERVED"
    assert "legacy_debug_trace" not in mig_data["migrated_payload"]
    delta = mig_data["preserved_delta"]
    assert delta is not None

    # 3. Reverse endpoint without delta (fails 422)
    rev_fail = client.post("/api/v1/schema-evolution/reverse", json={
        "source_version": "2.0.0",
        "target_version": "1.1.0",
        "payload": mig_data["migrated_payload"],
    })
    assert rev_fail.status_code == 422

    # 4. Reverse endpoint with delta (succeeds 200)
    rev_ok = client.post("/api/v1/schema-evolution/reverse", json={
        "source_version": "2.0.0",
        "target_version": "1.1.0",
        "payload": mig_data["migrated_payload"],
        "preserved_delta": delta,
    })
    assert rev_ok.status_code == 200
    assert rev_ok.json()["migrated_payload"]["legacy_debug_trace"] == "debug_data"
    assert rev_ok.json()["migrated_payload"]["confidence_score"] == 0.77

    # 5. Classify loss endpoint
    classify_resp = client.post("/api/v1/schema-evolution/classify-loss", json={
        "source_version": "1.0.0",
        "target_version": "1.1.0",
    })
    assert classify_resp.status_code == 200
    assert classify_resp.json()["overall_classification"] == "LOSSLESS"

    # 6. Status endpoint
    status_resp = client.get("/api/v1/schema-evolution/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["status"] == "PRESERVED"


def test_cli_schema_evolution(capsys):
    # CLI plan
    with pytest.raises(SystemExit) as exc:
        cli_main(["schema-evolution", "plan", "--source", "1.0.0", "--target", "2.0.0", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    plan_out = json.loads(captured.out)
    assert plan_out["overall_classification"] == "LOSSY_PRESERVED"

    # CLI classify
    with pytest.raises(SystemExit) as exc:
        cli_main(["schema-evolution", "classify", "--source", "1.0.0", "--target", "1.1.0"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "LOSSLESS" in captured.out

    # CLI status
    with pytest.raises(SystemExit) as exc:
        cli_main(["schema-evolution", "status"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "SWARMWATCH SCHEMA EVOLUTION STATUS" in captured.out
