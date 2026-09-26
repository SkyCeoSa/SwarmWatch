/**
 * Unit and integration tests for Migration Preservation & Partial Isomorphisms (Seam 59 / ADR 0070).
 * Verifies:
 * 1. Versioned Partial Isomorphisms: (f, g, Delta) where g(f(x).y, f(x).Delta) == x byte-for-byte.
 * 2. Fail-Closed Reverse Invariant: Reversing a lossy migration without PreservedDelta raises LossyMigrationError.
 * 3. UnsupportedReverseMigrationError for irreversible transitions.
 * 4. Pre-flight loss classification via classifyLoss.
 * 5. Multi-hop evolution roundtrip.
 */

import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  LossClassification,
  EvolutionAction,
  LossyMigrationError,
  UnsupportedReverseMigrationError,
  SchemaEvolutionError,
  SchemaEvolution,
  globalSchemaEvolution,
} from "../src/schema_evolution.ts";

describe("SchemaEvolution & Partial Isomorphisms (Seam 59 / ADR 0070)", () => {
  let engine;

  beforeEach(() => {
    engine = new SchemaEvolution();
    globalSchemaEvolution.reset();
  });

  test("plan calculates lossless and lossy evolution paths", () => {
    // Identical version
    const planIdent = engine.plan("1.0.0", "1.0.0");
    assert.equal(planIdent.overall_classification, LossClassification.LOSSLESS);
    assert.equal(planIdent.can_reverse_without_delta, true);
    assert.equal(planIdent.steps.length, 0);

    // 1.0.0 -> 1.1.0 (Lossless)
    const plan11 = engine.plan("1.0.0", "1.1.0");
    assert.equal(plan11.overall_classification, LossClassification.LOSSLESS);
    assert.equal(plan11.can_reverse_without_delta, true);
    assert.equal(plan11.steps.length, 1);

    // 1.0.0 -> 2.0.0 (Lossy)
    const plan20 = engine.plan("1.0.0", "2.0.0");
    assert.equal(plan20.overall_classification, LossClassification.LOSSY_PRESERVED);
    assert.equal(plan20.can_reverse_without_delta, false);
    assert.equal(plan20.steps.length, 2);
    assert.ok(plan20.lossy_fields.some((f) => f.includes("legacy_debug_trace")));

    // Missing path throws SchemaEvolutionError
    assert.throws(
      () => engine.plan("1.0.0", "99.9.9"),
      (err) => err instanceof SchemaEvolutionError
    );
  });

  test("migrate and reverse lossless transitions (pure isomorphism)", () => {
    const originalPayload = {
      agent_id: "agent-arch",
      description: "System Architect",
    };

    // Forward 1.0.0 -> 1.1.0 (renames agent_id -> agent_key, adds default priority=1)
    const resFwd = engine.migrate(originalPayload, "1.0.0", "1.1.0");
    assert.equal(resFwd.classification, LossClassification.LOSSLESS);
    assert.equal(resFwd.migrated_payload.agent_key, "agent-arch");
    assert.equal(resFwd.migrated_payload.agent_id, undefined);
    assert.equal(resFwd.migrated_payload.priority, 1);

    // Reverse 1.1.0 -> 1.0.0 WITHOUT delta (pure isomorphism succeeds)
    const resRev = engine.reverse(resFwd.migrated_payload, "1.1.0", "1.0.0");
    assert.equal(resRev.migrated_payload.agent_id, "agent-arch");
    assert.equal(resRev.migrated_payload.agent_key, undefined);
    assert.equal(resRev.migrated_payload.priority, undefined);
    assert.deepEqual(resRev.migrated_payload, originalPayload);
  });

  test("migrate lossy preservation and fail-closed reverse invariant", () => {
    const originalPayload = {
      legacy_debug_trace: { call_stack: ["turn1", "turn2"], entropy: 1.45 },
      confidence_score: 0.875,
      cpu_usage: 42.5,
      memory_mb: 512,
      agent_key: "agent-core",
    };

    // Forward 1.1.0 -> 2.0.0
    // Drops legacy_debug_trace, casts confidence_score float->int, nests cpu/mem into telemetry
    const res = engine.migrate(originalPayload, "1.1.0", "2.0.0", true);
    assert.equal(res.classification, LossClassification.LOSSY_PRESERVED);
    const migrated = res.migrated_payload;

    assert.equal(migrated.legacy_debug_trace, undefined);
    assert.equal(migrated.confidence_score, 0); // int downcast
    assert.deepEqual(migrated.telemetry, { cpu_usage: 42.5, memory_mb: 512 });

    // PreservedDelta captured
    const delta = res.preserved_delta;
    assert.ok(delta);
    assert.ok("legacy_debug_trace" in delta.removed_fields);
    assert.equal(delta.pre_cast_values.confidence_score, 0.875);
    assert.ok(delta.delta_hash);
    assert.equal(delta.delta_hash.length, 64);

    // INVARIANT: Reverse without delta MUST fail closed with LossyMigrationError
    assert.throws(
      () => engine.reverse(migrated, "2.0.0", "1.1.0", undefined),
      (err) => {
        return (
          err instanceof LossyMigrationError &&
          err.message.includes("legacy_debug_trace")
        );
      }
    );

    // INVARIANT: Reverse WITH delta restores exact original state
    const resRev = engine.reverse(migrated, "2.0.0", "1.1.0", delta);
    const restored = resRev.migrated_payload;
    assert.deepEqual(restored.legacy_debug_trace, originalPayload.legacy_debug_trace);
    assert.equal(restored.confidence_score, 0.875);
    assert.equal(restored.cpu_usage, 42.5);
    assert.equal(restored.memory_mb, 512);
    assert.deepEqual(restored, originalPayload);
  });

  test("multi-hop evolution roundtrip (1.0.0 -> 2.1.0 -> 1.0.0)", () => {
    const originalPayload = {
      agent_id: "agent-delta",
      legacy_debug_trace: "critical_stack_trace_v1",
      confidence_score: 0.999,
      cpu_usage: 15.0,
      memory_mb: 256,
    };

    // Migrate 1.0.0 -> 2.1.0 (traverses 1.0.0 -> 1.1.0 -> 2.0.0 -> 2.1.0)
    const resFwd = engine.migrate(originalPayload, "1.0.0", "2.1.0");
    assert.equal(resFwd.classification, LossClassification.LOSSY_PRESERVED);
    const v21Payload = resFwd.migrated_payload;

    assert.equal(v21Payload.agent_key, "agent-delta");
    assert.equal(v21Payload.summary_tag, "agent-agent-delta");
    assert.equal(v21Payload.cpu_usage, 15.0);

    // Reverse 2.1.0 -> 1.0.0 using PreservedDelta
    const resRev = engine.reverse(v21Payload, "2.1.0", "1.0.0", resFwd.preserved_delta);
    const restored = resRev.migrated_payload;

    assert.deepEqual(restored, originalPayload);
  });

  test("classifyLoss preflight audit", () => {
    // Payload with lossy field
    const lossyPayload = {
      legacy_debug_trace: "some_trace",
      confidence_score: 0.55,
    };
    const lossReport = engine.classifyLoss("1.1.0", "2.0.0", lossyPayload);
    assert.equal(lossReport.overall_classification, "LOSSY_PRESERVED");
    assert.equal(lossReport.can_reverse_without_delta, false);
    assert.ok(lossReport.lossy_fields.some((f) => f.includes("legacy_debug_trace")));

    // Payload without lossy field
    const cleanPayload = {
      confidence_score: 1,
      cpu_usage: 10.0,
    };
    const cleanReport = engine.classifyLoss("1.1.0", "2.0.0", cleanPayload);
    assert.ok(!cleanReport.lossy_fields.includes("legacy_debug_trace"));
  });

  test("irreversible step raises UnsupportedReverseMigrationError", () => {
    const stepIrrev = {
      source_version: "2.1.0",
      target_version: "3.0.0",
      rules: [
        {
          action: EvolutionAction.COMPUTE_DERIVED,
          source_path: "",
          target_path: "one_way_hash",
          derived_fn: () => "fixed_hash",
          is_irreversible: true,
        },
      ],
      declared_classification: LossClassification.IRREVERSIBLE,
      description: "One-way destructive migration",
    };
    engine.registerStep(stepIrrev);

    const plan = engine.plan("2.1.0", "3.0.0");
    assert.equal(plan.overall_classification, LossClassification.IRREVERSIBLE);

    const mig = engine.migrate({ test: 123 }, "2.1.0", "3.0.0");
    assert.equal(mig.classification, LossClassification.IRREVERSIBLE);

    assert.throws(
      () => engine.reverse(mig.migrated_payload, "3.0.0", "2.1.0"),
      (err) => err instanceof UnsupportedReverseMigrationError
    );
  });

  test("globalSchemaEvolution status tracking and reset", () => {
    const statusBefore = globalSchemaEvolution.status();
    assert.ok(statusBefore.registered_steps_count >= 3);
    assert.ok(statusBefore.registered_versions_count >= 4);

    globalSchemaEvolution.migrate({ agent_id: "agent-a" }, "1.0.0", "1.1.0");
    const statusAfter = globalSchemaEvolution.status();
    assert.equal(statusAfter.total_migrations_count, 1);
    assert.equal(statusAfter.lossless_migrations_count, 1);
    assert.equal(statusAfter.status, "PRESERVED");

    globalSchemaEvolution.reset();
    const statusReset = globalSchemaEvolution.status();
    assert.equal(statusReset.total_migrations_count, 0);
  });
});
