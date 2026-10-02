/**
 * Tests for Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66 / ADR 0077).
 * Verifies action classification, lattice join composition, automated reversible rollbacks,
 * semantic forward compensations, fail-closed irreversible halting, and human escalation workflows.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  ReversibilityClass,
  parseReversibilityClass,
  reversibilityJoin,
  CompensationError,
  IrreversibleActionError,
  ActionExecution,
  ActionContract,
  CompositeClassification,
  CompensationStep,
  CompensationPlan,
  CompensationResult,
  HumanEscalationReceipt,
  EscalationResolution,
  CompensationStatus,
  CompensationEngine,
  globalCompensationEngine,
} from "../src/compensation_algebra.ts";

test("Seam 66: Effect-Aware Compensation Algebra & Monotone Approval Lattices (ADR 0077)", async (t) => {
  await t.test("Action classification and lattice ordering", () => {
    assert.ok(ReversibilityClass.REVERSIBLE < ReversibilityClass.COMPENSATABLE);
    assert.ok(ReversibilityClass.COMPENSATABLE < ReversibilityClass.IRREVERSIBLE);
    assert.ok(ReversibilityClass.REVERSIBLE < ReversibilityClass.IRREVERSIBLE);

    assert.strictEqual(reversibilityJoin([ReversibilityClass.REVERSIBLE]), ReversibilityClass.REVERSIBLE);
    assert.strictEqual(
      reversibilityJoin([ReversibilityClass.REVERSIBLE, ReversibilityClass.COMPENSATABLE]),
      ReversibilityClass.COMPENSATABLE
    );
    assert.strictEqual(
      reversibilityJoin([ReversibilityClass.REVERSIBLE, ReversibilityClass.IRREVERSIBLE]),
      ReversibilityClass.IRREVERSIBLE
    );
    assert.strictEqual(
      reversibilityJoin([ReversibilityClass.COMPENSATABLE, ReversibilityClass.IRREVERSIBLE]),
      ReversibilityClass.IRREVERSIBLE
    );
  });

  await t.test("Unregistered action strictly defaults to irreversible (fail-closed)", () => {
    const engine = new CompensationEngine();
    const action = new ActionExecution({
      actionId: "act-001",
      actionType: "unknown.external.mutation",
      tenantId: "tenant-1",
      principal: "agent-0",
    });

    const classification = engine.classify([action]);
    assert.strictEqual(classification.reversibilityClass, ReversibilityClass.IRREVERSIBLE);
    assert.strictEqual(classification.isFullyReversible, false);
    assert.strictEqual(classification.isCompensatable, false);
    assert.strictEqual(classification.hasIrreversibleSteps, true);
    assert.deepStrictEqual(classification.irreversibleActionIds, ["act-001"]);
  });

  await t.test("Composite classification: all reversible", () => {
    const engine = new CompensationEngine();
    engine.registerContract(
      new ActionContract({
        actionType: "memory.set_scratch_key",
        reversibilityClass: ReversibilityClass.REVERSIBLE,
      })
    );
    engine.registerContract(
      new ActionContract({
        actionType: "db.start_transaction",
        reversibilityClass: ReversibilityClass.REVERSIBLE,
      })
    );

    const actions = [
      new ActionExecution({ actionId: "act-1", actionType: "memory.set_scratch_key", tenantId: "t1", principal: "p1" }),
      new ActionExecution({ actionId: "act-2", actionType: "db.start_transaction", tenantId: "t1", principal: "p1" }),
    ];

    const res = engine.classify(actions);
    assert.strictEqual(res.reversibilityClass, ReversibilityClass.REVERSIBLE);
    assert.strictEqual(res.isFullyReversible, true);
    assert.strictEqual(res.isCompensatable, true);
    assert.strictEqual(res.hasIrreversibleSteps, false);
    assert.strictEqual(res.irreversibleActionIds.length, 0);
  });

  await t.test("Composite classification: with compensatable", () => {
    const engine = new CompensationEngine();
    engine.registerContract(
      new ActionContract({
        actionType: "memory.set_scratch_key",
        reversibilityClass: ReversibilityClass.REVERSIBLE,
      })
    );
    engine.registerContract(
      new ActionContract({
        actionType: "payment.charge_card",
        reversibilityClass: ReversibilityClass.COMPENSATABLE,
      })
    );

    const actions = [
      new ActionExecution({ actionId: "act-1", actionType: "memory.set_scratch_key", tenantId: "t1", principal: "p1" }),
      new ActionExecution({ actionId: "act-2", actionType: "payment.charge_card", tenantId: "t1", principal: "p1" }),
    ];

    const res = engine.classify(actions);
    assert.strictEqual(res.reversibilityClass, ReversibilityClass.COMPENSATABLE);
    assert.strictEqual(res.isFullyReversible, false);
    assert.strictEqual(res.isCompensatable, true);
    assert.strictEqual(res.hasIrreversibleSteps, false);
  });

  await t.test("Composite classification: with irreversible", () => {
    const engine = new CompensationEngine();
    engine.registerContract(
      new ActionContract({
        actionType: "memory.set_scratch_key",
        reversibilityClass: ReversibilityClass.REVERSIBLE,
      })
    );
    engine.registerContract(
      new ActionContract({
        actionType: "payment.charge_card",
        reversibilityClass: ReversibilityClass.COMPENSATABLE,
      })
    );
    engine.registerContract(
      new ActionContract({
        actionType: "webhook.dispatch_public",
        reversibilityClass: ReversibilityClass.IRREVERSIBLE,
      })
    );

    const actions = [
      new ActionExecution({ actionId: "act-1", actionType: "memory.set_scratch_key", tenantId: "t1", principal: "p1" }),
      new ActionExecution({ actionId: "act-2", actionType: "payment.charge_card", tenantId: "t1", principal: "p1" }),
      new ActionExecution({ actionId: "act-3", actionType: "webhook.dispatch_public", tenantId: "t1", principal: "p1" }),
    ];

    const res = engine.classify(actions);
    assert.strictEqual(res.reversibilityClass, ReversibilityClass.IRREVERSIBLE);
    assert.strictEqual(res.isFullyReversible, false);
    assert.strictEqual(res.isCompensatable, false);
    assert.strictEqual(res.hasIrreversibleSteps, true);
    assert.deepStrictEqual(res.irreversibleActionIds, ["act-3"]);
  });

  await t.test("Compensation plan generation reverses action sequence", () => {
    const engine = new CompensationEngine();
    engine.registerContract(new ActionContract({ actionType: "step.1", reversibilityClass: ReversibilityClass.REVERSIBLE }));
    engine.registerContract(new ActionContract({ actionType: "step.2", reversibilityClass: ReversibilityClass.COMPENSATABLE }));
    engine.registerContract(new ActionContract({ actionType: "step.3", reversibilityClass: ReversibilityClass.REVERSIBLE }));

    const actions = [
      new ActionExecution({ actionId: "a1", actionType: "step.1", tenantId: "t1", principal: "p1" }),
      new ActionExecution({ actionId: "a2", actionType: "step.2", tenantId: "t1", principal: "p1" }),
      new ActionExecution({ actionId: "a3", actionType: "step.3", tenantId: "t1", principal: "p1" }),
    ];

    const plan = engine.buildCompensationPlan(actions);
    assert.ok(plan instanceof CompensationPlan);
    assert.strictEqual(plan.compositeClass, ReversibilityClass.COMPENSATABLE);
    assert.strictEqual(plan.steps.length, 3);
    assert.deepStrictEqual(
      plan.steps.map((s) => s.targetActionId),
      ["a3", "a2", "a1"]
    );
  });

  await t.test("Execute compensation for reversible workflow", async () => {
    const state = { val: 100 };

    const engine = new CompensationEngine();
    engine.registerContract(
      new ActionContract({
        actionType: "math.add",
        reversibilityClass: ReversibilityClass.REVERSIBLE,
        inverseHandler: (action) => {
          state.val -= Number(action.payload.amount) || 0;
          return true;
        },
      })
    );

    const actions = [
      new ActionExecution({ actionId: "a1", actionType: "math.add", tenantId: "t1", principal: "p1", payload: { amount: 10 } }),
      new ActionExecution({ actionId: "a2", actionType: "math.add", tenantId: "t1", principal: "p1", payload: { amount: 25 } }),
    ];

    state.val += 10 + 25; // 135

    const plan = engine.buildCompensationPlan(actions);
    const result = await engine.executeCompensation(plan);

    assert.strictEqual(result.success, true);
    assert.strictEqual(result.stepsExecuted, 2);
    assert.strictEqual(result.stepsFailed, 0);
    assert.strictEqual(state.val, 100);
  });

  await t.test("Execute compensation for compensatable workflow", async () => {
    const ledger = [];

    const engine = new CompensationEngine();
    engine.registerContract(
      new ActionContract({
        actionType: "billing.charge",
        reversibilityClass: ReversibilityClass.COMPENSATABLE,
        compensationHandler: (action) => {
          ledger.push(`REFUND:${action.payload.charge_id}`);
          return true;
        },
      })
    );

    const actions = [
      new ActionExecution({ actionId: "c1", actionType: "billing.charge", tenantId: "t1", principal: "p1", payload: { charge_id: "ch_999" } }),
    ];

    const plan = engine.buildCompensationPlan(actions);
    const result = await engine.executeCompensation(plan);

    assert.strictEqual(result.success, true);
    assert.strictEqual(result.stepsExecuted, 1);
    assert.deepStrictEqual(ledger, ["REFUND:ch_999"]);
  });

  await t.test("Execute compensation on irreversible action halts fail-closed", async () => {
    const engine = new CompensationEngine();
    engine.registerContract(
      new ActionContract({
        actionType: "email.send_customer_email",
        reversibilityClass: ReversibilityClass.IRREVERSIBLE,
      })
    );

    const actions = [
      new ActionExecution({ actionId: "e1", actionType: "email.send_customer_email", tenantId: "t1", principal: "p1" }),
    ];

    const plan = engine.buildCompensationPlan(actions);
    assert.strictEqual(plan.compositeClass, ReversibilityClass.IRREVERSIBLE);

    await assert.rejects(
      async () => {
        await engine.executeCompensation(plan);
      },
      (err) => err instanceof IrreversibleActionError && err.message.includes("Automated rollback prohibited")
    );
  });

  await t.test("Human escalation lifecycle and resolution", async () => {
    const engine = new CompensationEngine();
    const action = new ActionExecution({
      actionId: "irr-01",
      actionType: "sns.broadcast_public_alert",
      tenantId: "tenant-alpha",
      principal: "agent-root",
      payload: { topic: "market-outage", msg: "Critical alert" },
    });

    // 1. Escalate
    const receipt = engine.escalate(action, "Action produced external broadcast; manual remediation required");
    assert.ok(receipt instanceof HumanEscalationReceipt);
    assert.strictEqual(receipt.actionId, "irr-01");
    assert.strictEqual(receipt.status, "PENDING");
    assert.strictEqual(receipt.escalationHash.length, 64);

    // 2. Cannot execute unapproved plan
    const plan = engine.buildCompensationPlan([action]);
    await assert.rejects(async () => {
      await engine.executeCompensation(plan);
    }, IrreversibleActionError);

    // 3. Human authorizes remediation
    const resolution = engine.authorizeEscalation(
      receipt.escalationId,
      "secops-lead",
      "MANUAL_REMEDIATED",
      "Corrective broadcast dispatched manually via console"
    );
    assert.ok(resolution instanceof EscalationResolution);
    assert.strictEqual(resolution.decision, "MANUAL_REMEDIATED");
    assert.strictEqual(resolution.authorizerPrincipal, "secops-lead");

    // 4. Plan now executes successfully with authorized escalation bypass
    const result = await engine.executeCompensation(plan, true);
    assert.strictEqual(result.success, true);
    assert.strictEqual(result.escalationsResolved, 1);
  });

  await t.test("Sub-250µs classification SLO benchmark", () => {
    const engine = new CompensationEngine();
    for (let i = 0; i < 10; i++) {
      engine.registerContract(
        new ActionContract({
          actionType: `test.action.${i}`,
          reversibilityClass: i % 2 === 0 ? ReversibilityClass.REVERSIBLE : ReversibilityClass.COMPENSATABLE,
        })
      );
    }

    const actions = Array.from({ length: 10 }, (_, i) =>
      new ActionExecution({ actionId: `a_${i}`, actionType: `test.action.${i}`, tenantId: "t", principal: "p" })
    );

    // Warmup
    for (let i = 0; i < 10; i++) {
      engine.classify(actions);
    }

    // Benchmark: best of 3 runs of 500 iterations
    let bestAvgUs = Infinity;
    const iterations = 500;

    for (let run = 0; run < 3; run++) {
      const t0 = process.hrtime.bigint();
      for (let i = 0; i < iterations; i++) {
        engine.classify(actions);
      }
      const t1 = process.hrtime.bigint();
      const totalNs = Number(t1 - t0);
      const avgUs = (totalNs / iterations) / 1000;
      if (avgUs < bestAvgUs) {
        bestAvgUs = avgUs;
      }
    }

    assert.ok(bestAvgUs < 250.0, `Classification took ${bestAvgUs.toFixed(2)}µs, exceeding 250µs SLO`);
  });

  await t.test("Global instance and status reporting", () => {
    const status = globalCompensationEngine.status();
    assert.ok(status instanceof CompensationStatus);
    assert.ok(status.totalRegisteredContracts >= 0);
    assert.ok(status.totalPlansGenerated >= 0);
    const d = status.toDict();
    assert.ok("total_registered_contracts" in d);
    assert.ok("total_escalations_pending" in d);
  });
});
