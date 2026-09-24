import test from "node:test";
import assert from "node:assert/strict";
import {
  LaplaceMechanism,
  DifferentialPrivacyEngine,
  globalPrivacyEngine,
} from "../src/index.ts";

test("Differential Privacy & Cryptographic State Anonymization Suite (Seam 38 / ADR 0049)", async (t) => {
  await t.test("LaplaceMechanism produces calibrated noise and clamps boundaries", () => {
    const mech = new LaplaceMechanism();

    assert.equal(mech.sampleNoise(0.0), 0.0);
    assert.equal(mech.sampleNoise(-5.0), 0.0);

    const samples = [];
    for (let i = 0; i < 500; i++) {
      samples.push(mech.sampleNoise(2.0));
    }
    const mean = samples.reduce((acc, v) => acc + v, 0) / samples.length;
    assert.ok(Math.abs(mean) < 1.0, `Laplace mean should be near 0, got ${mean}`);

    // Clamping test with high epsilon (small noise scale) to verify clamp bounds
    const highPerturb = mech.perturb(100, 0, 10, 100.0);
    assert.ok(Math.abs(highPerturb - 10) < 2.0, `Upper clamped value should be near 10, got ${highPerturb}`);

    const lowPerturb = mech.perturb(-50, 0, 10, 100.0);
    assert.ok(Math.abs(lowPerturb - 0) < 2.0, `Lower clamped value should be near 0, got ${lowPerturb}`);
  });

  await t.test("Deterministic HMAC pseudonymization preserves topological links", () => {
    const engine1 = new DifferentialPrivacyEngine({ tenantSalt: "salt_alpha" });
    const p1 = engine1.pseudonymize("SecurityAuditor", "agent_");
    const p2 = engine1.pseudonymize("SecurityAuditor", "agent_");
    assert.equal(p1, p2);
    assert.ok(p1.startsWith("agent_"));

    const p3 = engine1.pseudonymize("BackendEngineer", "agent_");
    assert.notEqual(p1, p3);

    const engine2 = new DifferentialPrivacyEngine({ tenantSalt: "salt_beta" });
    const pDiffSalt = engine2.pseudonymize("SecurityAuditor", "agent_");
    assert.notEqual(p1, pDiffSalt);
  });

  await t.test("Anonymizes causal span fields and protects payloads", () => {
    const engine = new DifferentialPrivacyEngine({
      epsilon: 1.0,
      tenantSalt: "ts_salt_01",
      anonymizeIdentities: true,
      anonymizeMetrics: true,
      anonymizePayloads: true,
    });

    const originalSpan = {
      id: "ts_span_01",
      swarm_id: "swarm-live-01",
      agent: "ProductArchitect",
      source: "UserProxy",
      target: "BackendEngineer",
      role: "System Designer",
      tokens: 2400,
      duration: 520.5,
      cost: 0.045,
      entropy: 1.85,
      payload: "Confidential API secret: sk-prod-99482818",
    };

    const anon = engine.anonymizeSpan(originalSpan);

    assert.ok(anon.agent.startsWith("agent_"));
    assert.notEqual(anon.agent, "ProductArchitect");
    assert.ok(anon.source.startsWith("agent_"));
    assert.ok(anon.target.startsWith("agent_"));
    assert.ok(anon.role.startsWith("role_"));

    assert.equal(typeof anon.tokens, "number");
    assert.equal(typeof anon.duration, "number");
    assert.equal(typeof anon.cost, "number");
    assert.equal(typeof anon.entropy, "number");
    assert.ok(anon.entropy >= 0.0 && anon.entropy <= 8.0);

    assert.ok(!anon.payload.includes("sk-prod-99482818"));
    assert.ok(anon.payload.startsWith("[ANONYMIZED_PAYLOAD:"));
    assert.ok(anon.payload.includes("SHA256:"));

    assert.equal(anon._privacy.anonymized, true);
    assert.equal(anon._privacy.epsilon, 1.0);
  });

  await t.test("Tracks epsilon privacy budget and flags exhaustion", () => {
    const engine = new DifferentialPrivacyEngine({
      epsilon: 1.0,
      totalEpsilonBudget: 0.03,
    });

    const initial = engine.getStatus();
    assert.equal(initial.isExhausted, false);
    assert.equal(initial.spentBudget, 0.0);

    const testSpan = { agent: "WorkerNode", tokens: 100 };
    engine.anonymizeSpan(testSpan);
    engine.anonymizeSpan(testSpan);
    engine.anonymizeSpan(testSpan);

    const exhaustedStatus = engine.getStatus();
    assert.ok(exhaustedStatus.spentBudget >= 0.03);
    assert.equal(exhaustedStatus.isExhausted, true);
    assert.equal(exhaustedStatus.remainingBudget, 0.0);

    const postExhaustion = engine.anonymizeSpan(testSpan);
    assert.equal(postExhaustion._privacy.budget_exhausted, true);

    engine.resetBudget();
    const resetStatus = engine.getStatus();
    assert.equal(resetStatus.spentBudget, 0.0);
    assert.equal(resetStatus.isExhausted, false);
  });

  await t.test("Batch anonymization processes multiple spans correctly", () => {
    const engine = new DifferentialPrivacyEngine();
    const batch = [
      { id: "b1", agent: "AgentA", tokens: 100 },
      { id: "b2", agent: "AgentB", tokens: 200 },
      { id: "b3", agent: "AgentC", tokens: 300 },
    ];
    const results = engine.anonymizeBatch(batch);
    assert.equal(results.length, 3);
    assert.ok(results[0].agent.startsWith("agent_"));
    assert.ok(results[1].agent.startsWith("agent_"));
    assert.ok(results[2].agent.startsWith("agent_"));
  });

  await t.test("globalPrivacyEngine singleton provides ready-to-use privacy status", () => {
    const status = globalPrivacyEngine.getStatus();
    assert.ok(typeof status.epsilon === "number");
    assert.ok(typeof status.totalBudget === "number");
    assert.ok(typeof status.spentBudget === "number");
  });
});
