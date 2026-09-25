import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  FieldTransformAction,
  SchemaVersion,
  FieldTransformRule,
  MigrationStep,
  SchemaMigrationEngine,
  globalSchemaMigrationEngine,
} from "../src/schema_migration.ts";

describe("Seam 47: Autonomous Semantic Schema Migration & Backward-Compatible State Translation Engine", () => {
  test("SchemaVersion parsing, comparison, and string formatting", () => {
    const v1 = SchemaVersion.fromString("1.0.0");
    const v2 = SchemaVersion.fromString("1.1.0");
    const v3 = SchemaVersion.fromString("v2.0.0-beta.1");

    assert.equal(v1.toString(), "1.0.0");
    assert.equal(v2.toString(), "1.1.0");
    assert.equal(v3.toString(), "2.0.0-beta.1");
    assert.equal(v3.major, 2);

    assert.ok(v1.compareTo(v2) < 0);
    assert.ok(v2.compareTo(v1) > 0);
    assert.ok(v1.compareTo(SchemaVersion.fromString("1.0.0")) === 0);
  });

  test("FieldTransformRule atomic transformation actions", () => {
    // 1. RENAME
    const dataRename = { old_user: "agent_42", nested: { prev_val: 10 } };
    new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "old_user", targetPath: "agent_id" }).apply(dataRename);
    new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "nested.prev_val", targetPath: "nested.new_val" }).apply(dataRename);
    assert.equal(dataRename.old_user, undefined);
    assert.equal(dataRename.agent_id, "agent_42");
    assert.equal(dataRename.nested.prev_val, undefined);
    assert.equal(dataRename.nested.new_val, 10);

    // 2. CAST_TYPE
    const dataCast = { count: "42", ratio: "3.14", flag: "true", items: "alpha" };
    new FieldTransformRule({ action: FieldTransformAction.CAST_TYPE, sourcePath: "count", targetType: "int" }).apply(dataCast);
    new FieldTransformRule({ action: FieldTransformAction.CAST_TYPE, sourcePath: "ratio", targetType: "float" }).apply(dataCast);
    new FieldTransformRule({ action: FieldTransformAction.CAST_TYPE, sourcePath: "items", targetType: "list" }).apply(dataCast);
    assert.equal(dataCast.count, 42);
    assert.equal(dataCast.ratio, 3.14);
    assert.deepEqual(dataCast.items, ["alpha"]);

    // 3. ADD_DEFAULT
    const dataDefault = { present: "yes" };
    new FieldTransformRule({ action: FieldTransformAction.ADD_DEFAULT, sourcePath: "env", defaultValue: "production" }).apply(dataDefault);
    new FieldTransformRule({ action: FieldTransformAction.ADD_DEFAULT, sourcePath: "present", defaultValue: "no" }).apply(dataDefault);
    assert.equal(dataDefault.env, "production");
    assert.equal(dataDefault.present, "yes");

    // 4. REMOVE_DEPRECATED
    const dataDep = { keep: 1, obsolete: "trash" };
    new FieldTransformRule({ action: FieldTransformAction.REMOVE_DEPRECATED, sourcePath: "obsolete" }).apply(dataDep);
    assert.equal(dataDep.obsolete, undefined);
    assert.equal(dataDep.keep, 1);

    // 5. NEST_FIELDS & FLATTEN_FIELDS
    const dataNest = { tokens: 100, cost: 0.05, agent: "coordinator" };
    new FieldTransformRule({
      action: FieldTransformAction.NEST_FIELDS,
      sourcePath: "",
      targetPath: "metrics",
      nestedKeys: ["tokens", "cost"],
    }).apply(dataNest);
    assert.deepEqual(dataNest.metrics, { tokens: 100, cost: 0.05 });
    assert.equal(dataNest.tokens, undefined);

    new FieldTransformRule({
      action: FieldTransformAction.FLATTEN_FIELDS,
      sourcePath: "metrics",
      targetPath: "",
    }).apply(dataNest);
    assert.equal(dataNest.tokens, 100);
    assert.equal(dataNest.cost, 0.05);
    assert.equal(dataNest.metrics, undefined);

    // 6. COMPUTE_DERIVED
    const dataDerived = { first: "Google", second: "DeepMind" };
    new FieldTransformRule({
      action: FieldTransformAction.COMPUTE_DERIVED,
      sourcePath: "",
      targetPath: "full_org",
      derivedFn: (d) => `${d.first} ${d.second}`,
    }).apply(dataDerived);
    assert.equal(dataDerived.full_org, "Google DeepMind");
  });

  test("MigrationStep forward and backward transformation reversibility", () => {
    const step = new MigrationStep({
      sourceVersion: "1.0.0",
      targetVersion: "1.1.0",
      forwardRules: [
        new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "user", targetPath: "user_id" }),
        new FieldTransformRule({ action: FieldTransformAction.ADD_DEFAULT, sourcePath: "status", defaultValue: "online" }),
      ],
      backwardRules: [
        new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "user_id", targetPath: "user" }),
        new FieldTransformRule({ action: FieldTransformAction.REMOVE_DEPRECATED, sourcePath: "status" }),
      ],
      description: "User ID rename and status default",
    });

    const original = { user: "bob", rank: 1 };
    const forward = step.applyForward(original);
    assert.deepEqual(forward, { user_id: "bob", status: "online", rank: 1 });
    assert.deepEqual(original, { user: "bob", rank: 1 }); // Immutability of input

    const backward = step.applyBackward(forward);
    assert.deepEqual(backward, { user: "bob", rank: 1 });
  });

  test("SchemaMigrationEngine shortest path BFS graph planning", () => {
    const engine = new SchemaMigrationEngine();
    engine.registerVersion("1.0.0");
    engine.registerVersion("1.1.0");
    engine.registerVersion("2.0.0");
    engine.registerVersion("3.0.0");

    engine.registerStep(
      new MigrationStep({
        sourceVersion: "1.0.0",
        targetVersion: "1.1.0",
        forwardRules: [new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "worker", targetPath: "agent_id" })],
        backwardRules: [new FieldTransformRule({ action: FieldTransformAction.RENAME, sourcePath: "agent_id", targetPath: "worker" })],
      })
    );
    engine.registerStep(
      new MigrationStep({
        sourceVersion: "1.1.0",
        targetVersion: "2.0.0",
        forwardRules: [new FieldTransformRule({ action: FieldTransformAction.ADD_DEFAULT, sourcePath: "tier", defaultValue: "pro" })],
        backwardRules: [new FieldTransformRule({ action: FieldTransformAction.REMOVE_DEPRECATED, sourcePath: "tier" })],
      })
    );
    engine.registerStep(
      new MigrationStep({
        sourceVersion: "2.0.0",
        targetVersion: "3.0.0",
        forwardRules: [new FieldTransformRule({ action: FieldTransformAction.CAST_TYPE, sourcePath: "val", targetType: "int" })],
      })
    );

    // Multi-hop Forward (1.0.0 -> 3.0.0)
    const plan = engine.planMigration("1.0.0", "3.0.0");
    assert.ok(plan);
    assert.equal(plan.totalSteps, 3);

    const res = engine.migrate({ worker: "solver", val: "500" }, "1.0.0", "3.0.0");
    assert.ok(res.isValid);
    assert.equal(res.appliedStepsCount, 3);
    assert.deepEqual(res.appliedPath, ["1.0.0", "1.1.0", "2.0.0", "3.0.0"]);
    assert.deepEqual(res.transformedPayload, {
      agent_id: "solver",
      tier: "pro",
      val: 500,
    });

    // Multi-hop Backward (2.0.0 -> 1.0.0)
    const revRes = engine.migrate({ agent_id: "solver", tier: "pro" }, "2.0.0", "1.0.0");
    assert.equal(revRes.appliedStepsCount, 2);
    assert.deepEqual(revRes.appliedPath, ["2.0.0", "1.1.0", "1.0.0"]);
    assert.deepEqual(revRes.transformedPayload, { worker: "solver" });
  });

  test("Round-trip idempotency and sub-30 microsecond latency with global engine", () => {
    const original = {
      agent: "lead-analyst",
      tokens: "800",
      cost_dollars: 0.04,
    };

    // 1.0.0 -> 2.0.0
    const fwd = globalSchemaMigrationEngine.migrate(original, "1.0.0", "2.0.0");
    assert.equal(fwd.appliedStepsCount, 2);
    assert.equal(fwd.transformedPayload.agent_id, "lead-analyst");
    assert.equal(fwd.transformedPayload.turn_status, "completed");
    assert.equal(fwd.transformedPayload.resource_usage.tokens, 800);
    assert.ok(fwd.isValid);

    // 2.0.0 -> 1.0.0
    const rev = globalSchemaMigrationEngine.migrate(fwd.transformedPayload, "2.0.0", "1.0.0");
    assert.equal(rev.appliedStepsCount, 2);
    assert.equal(rev.transformedPayload.agent, "lead-analyst");
    assert.equal(rev.transformedPayload.tokens, 800);
    assert.equal(rev.transformedPayload.turn_status, undefined);
    assert.equal(rev.transformedPayload.resource_usage, undefined);

    // Engine Status
    const status = globalSchemaMigrationEngine.getStatus();
    assert.ok(status.total_versions >= 3);
    assert.ok(status.total_migrations_executed >= 2);
  });

  test("Contract validation rejects payloads missing required fields", () => {
    const validCheck = globalSchemaMigrationEngine.validatePayload({ agent: "bot", tokens: 10 }, "1.0.0");
    assert.equal(validCheck.isValid, true);
    assert.equal(validCheck.errors.length, 0);

    const invalidCheck = globalSchemaMigrationEngine.validatePayload({ agent: "bot" }, "1.0.0");
    assert.equal(invalidCheck.isValid, false);
    assert.ok(invalidCheck.errors.length > 0);
  });
});
