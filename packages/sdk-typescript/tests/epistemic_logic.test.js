import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  EpistemicTruthValue,
  EpistemicStatus,
  PropositionalClaim,
  EpistemicConflict,
  EpistemicKnowledgeBase,
  EpistemicEngine,
  globalEpistemicEngine,
} from "../src/epistemic.ts";

describe("Seam 48: Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization Engine", () => {
  test("PropositionalClaim creation, constraints, and serialization", () => {
    const claim = new PropositionalClaim({
      claimId: "prop-ts-01",
      agentId: "agent-architect",
      topic: "database.schema",
      predicate: "is_migrated",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.95,
      evidenceSpanId: "span-abc-123",
      metadata: { env: "prod" },
    });

    const dict = claim.toDict();
    assert.equal(dict.claim_id, "prop-ts-01");
    assert.equal(dict.agent_id, "agent-architect");
    assert.equal(dict.truth_value, "TRUE");
    assert.equal(dict.confidence, 0.95);
    assert.equal(dict.evidence_span_id, "span-abc-123");
    assert.equal(dict.is_active, true);

    const restored = PropositionalClaim.fromDict(dict);
    assert.equal(restored.claimId, "prop-ts-01");
    assert.equal(restored.truthValue, EpistemicTruthValue.TRUE);
    assert.equal(restored.confidence, 0.95);
  });

  test("Epistemic contradiction detection across disparate agents", () => {
    const kb = new EpistemicKnowledgeBase("swarm-ts-epistemic-01");

    // Agent A asserts TRUE
    const [claimA, isConsA, confA] = kb.assertClaim({
      agentId: "agent-alpha",
      topic: "cache.redis",
      predicate: "is_clustered",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.9,
    });
    assert.equal(isConsA, true);
    assert.equal(confA, null);
    assert.equal(kb.computeDissonanceScore(), 0.0);
    assert.equal(kb.getEpistemicStatus(), EpistemicStatus.CONSISTENT);

    // Agent B asserts FALSE (contradiction!)
    const [claimB, isConsB, confB] = kb.assertClaim({
      agentId: "agent-beta",
      topic: "cache.redis",
      predicate: "is_clustered",
      truthValue: EpistemicTruthValue.FALSE,
      confidence: 0.8,
    });
    assert.equal(isConsB, false);
    assert.ok(confB !== null);
    assert.equal(confB.priorAgentId, "agent-alpha");
    assert.equal(confB.conflictingAgentId, "agent-beta");
    assert.equal(confB.severity, 0.8);
    assert.ok(kb.computeDissonanceScore() > 0.0);
    assert.equal(kb.getEpistemicStatus(), EpistemicStatus.DISSONANT);
  });

  test("Epistemic agent self-update supersedes prior claim without conflict", () => {
    const kb = new EpistemicKnowledgeBase("swarm-ts-self-update");

    // Worker asserts TRUE
    kb.assertClaim({
      agentId: "agent-worker",
      topic: "task.worker",
      predicate: "is_running",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.8,
    });

    // Worker updates own belief to FALSE
    const [claim, isCons, conf] = kb.assertClaim({
      agentId: "agent-worker",
      topic: "task.worker",
      predicate: "is_running",
      truthValue: EpistemicTruthValue.FALSE,
      confidence: 0.85,
    });

    assert.equal(isCons, true);
    assert.equal(conf, null);
    assert.equal(kb.claims.length, 1);
    assert.equal(kb.claims[0].truthValue, EpistemicTruthValue.FALSE);
    assert.equal(kb.computeDissonanceScore(), 0.0);
  });

  test("Mutual Knowledge (E_G phi) computation", () => {
    const kb = new EpistemicKnowledgeBase("swarm-ts-mutual");

    kb.assertClaim({
      agentId: "agent-1",
      topic: "network.vpc",
      predicate: "is_isolated",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.9,
    });
    kb.assertClaim({
      agentId: "agent-2",
      topic: "network.vpc",
      predicate: "is_isolated",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.85,
    });

    const mutual = kb.getMutualKnowledge();
    assert.equal(mutual.length, 1);
    assert.equal(mutual[0].topic, "network.vpc");
    assert.equal(mutual[0].predicate, "is_isolated");
    assert.equal(mutual[0].truth_value, "TRUE");
    assert.ok(mutual[0].affirming_agents.includes("agent-1"));
    assert.ok(mutual[0].affirming_agents.includes("agent-2"));
  });

  test("Deterministic AGM belief revision with CONFIDENCE_ENTRENCHMENT", () => {
    const kb = new EpistemicKnowledgeBase("swarm-ts-agm-conf");

    // Senior architect assertion
    kb.assertClaim({
      agentId: "senior-architect",
      topic: "auth.method",
      predicate: "is_jwt",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.95,
    });
    // Junior agent assertion
    kb.assertClaim({
      agentId: "junior-agent",
      topic: "auth.method",
      predicate: "is_jwt",
      truthValue: EpistemicTruthValue.FALSE,
      confidence: 0.35,
    });

    assert.equal(kb.getEpistemicStatus(), EpistemicStatus.DISSONANT);

    const res = kb.reconcileBeliefs("CONFIDENCE_ENTRENCHMENT");
    assert.equal(res.conflicts_resolved, 1);
    assert.equal(res.retracted_claim_ids.length, 1);
    assert.equal(res.new_dissonance_score, 0.0);
    assert.equal(res.epistemic_status, "CONSISTENT");

    const active = kb.claims.filter((c) => c.isActive);
    assert.equal(active.length, 1);
    assert.equal(active[0].agentId, "senior-architect");
    assert.equal(active[0].truthValue, EpistemicTruthValue.TRUE);
  });

  test("Deterministic AGM belief revision with TEMPORAL_RECENCY", () => {
    const kb = new EpistemicKnowledgeBase("swarm-ts-agm-rec");

    // Old claim
    const [oldClaim] = kb.assertClaim({
      agentId: "agent-legacy",
      topic: "cluster.mode",
      predicate: "is_standalone",
      truthValue: EpistemicTruthValue.TRUE,
      confidence: 0.9,
    });
    oldClaim.assertedAt = 1000;

    // Newer claim
    const [newClaim] = kb.assertClaim({
      agentId: "agent-modern",
      topic: "cluster.mode",
      predicate: "is_standalone",
      truthValue: EpistemicTruthValue.FALSE,
      confidence: 0.8,
    });
    newClaim.assertedAt = 2000;

    assert.equal(kb.getEpistemicStatus(), EpistemicStatus.DISSONANT);

    const res = kb.reconcileBeliefs("TEMPORAL_RECENCY");
    assert.equal(res.conflicts_resolved, 1);
    assert.equal(res.epistemic_status, "CONSISTENT");

    const active = kb.claims.filter((c) => c.isActive);
    assert.equal(active.length, 1);
    assert.equal(active[0].agentId, "agent-modern");
    assert.equal(active[0].truthValue, EpistemicTruthValue.FALSE);
  });

  test("EpistemicEngine multi-swarm partition management and reset", () => {
    const engine = new EpistemicEngine();

    engine.assertProposition(
      "swarm-alpha",
      "agent-a",
      "model.tier",
      "is_fast",
      EpistemicTruthValue.TRUE,
      0.9
    );
    engine.assertProposition(
      "swarm-beta",
      "agent-b",
      "model.tier",
      "is_fast",
      EpistemicTruthValue.FALSE,
      0.85
    );

    const stAlpha = engine.getStatus("swarm-alpha");
    assert.equal(stAlpha.total_claims, 1);
    assert.equal(stAlpha.status, "CONSISTENT");

    const stBeta = engine.getStatus("swarm-beta");
    assert.equal(stBeta.total_claims, 1);
    assert.equal(stBeta.status, "CONSISTENT");

    // Reset alpha
    engine.reset("swarm-alpha");
    assert.equal(engine.getStatus("swarm-alpha").total_claims, 0);
    assert.equal(engine.getStatus("swarm-beta").total_claims, 1);
  });
});
