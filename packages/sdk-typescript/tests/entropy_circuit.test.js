/**
 * Tests for SwarmWatch Shannon Entropy, Lexical Similarity,
 * Quota Budgets, and Circuit Breaker (TypeScript SDK).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  computeShannonEntropy,
  computeLexicalSimilarity,
  CircuitPolicy,
  EntropyCircuitBreaker,
  SwarmCircuitBreakException,
} from "../src/index.ts";

test("Shannon Entropy Calculation", async (t) => {
  await t.test("returns 1.0 for empty or single-word strings", () => {
    assert.strictEqual(computeShannonEntropy(""), 1.0);
    assert.strictEqual(computeShannonEntropy("hello"), 1.0);
  });

  await t.test("returns low entropy for highly repetitive text", () => {
    const repetitive = "repeat ".repeat(20);
    const entropy = computeShannonEntropy(repetitive);
    assert.ok(entropy < 0.2, `Expected entropy < 0.2, got ${entropy}`);
  });

  await t.test("returns high entropy for diverse vocabulary", () => {
    const diverse = "The swift brown fox jumps gracefully over a lazy sleeping dog near the riverbank";
    const entropy = computeShannonEntropy(diverse);
    assert.ok(entropy > 0.85, `Expected entropy > 0.85, got ${entropy}`);
  });
});

test("Lexical Jaccard Similarity", async (t) => {
  await t.test("identical strings return 1.0", () => {
    const text = "Searching database for relevant transaction records";
    assert.strictEqual(computeLexicalSimilarity(text, text), 1.0);
  });

  await t.test("completely disjoint strings return 0.0", () => {
    assert.strictEqual(
      computeLexicalSimilarity("apples bananas oranges", "quantum gravity relativity"),
      0.0
    );
  });

  await t.test("partially overlapping strings return intermediate value", () => {
    const a = "agent analyzing customer request";
    const b = "agent validating customer query";
    const sim = computeLexicalSimilarity(a, b);
    assert.ok(sim > 0.3 && sim < 0.8, `Expected intermediate similarity, got ${sim}`);
  });
});

test("Circuit Breaker Interventions", async (t) => {
  await t.test("passes healthy diverse messages", () => {
    const breaker = new EntropyCircuitBreaker({ preset: "balanced" });
    const decision = breaker.evaluate("First turn: exploring system architecture.");
    assert.strictEqual(decision.decision, "pass");
  });

  await t.test("2-step intervention: warning then trip on repetitive loops", () => {
    const breaker = new EntropyCircuitBreaker({
      policy: new CircuitPolicy({ similarity_threshold: 0.8, max_warnings: 1 }),
    });

    const msg = "I need to check the exact user records again in the database table.";
    // Turn 1: healthy pass
    const d1 = breaker.evaluate(msg);
    assert.strictEqual(d1.decision, "pass");

    // Turn 2: high similarity -> warning 1
    const d2 = breaker.evaluate(msg);
    assert.strictEqual(d2.decision, "warning");
    assert.ok(d2.warning_hint?.includes("SWARMWATCH CIRCUIT WARNING"));

    // Turn 3: repetitive loop persists -> trip circuit break exception!
    assert.throws(
      () => breaker.evaluate(msg),
      (err) => {
        assert.ok(err instanceof SwarmCircuitBreakException);
        assert.ok(err.reason.includes("High lexical repetition detected"));
        return true;
      }
    );
  });
});

test("Quota Enforcement & Counterfactual Savings", async (t) => {
  await t.test("enforces max_turns ceiling and computes counterfactual savings", () => {
    const breaker = new EntropyCircuitBreaker({
      policy: new CircuitPolicy({ max_turns: 2, cost_per_token: 0.00002 }),
    });

    breaker.evaluate("Turn one: initializing planning.", "planner", "swarm-1", 100);
    breaker.evaluate("Turn two: dispatching subagent task.", "planner", "swarm-1", 200);

    // Turn 3 exceeds max_turns (2)
    assert.throws(
      () => breaker.evaluate("Turn three: extra redundant iteration.", "planner", "swarm-1", 150),
      (err) => {
        assert.ok(err instanceof SwarmCircuitBreakException);
        assert.ok(err.reason.includes("Maximum turns ceiling exceeded"));
        assert.ok(err.savings !== undefined);
        return true;
      }
    );
  });

  await t.test("enforces token_budget quota", () => {
    const breaker = new EntropyCircuitBreaker({
      policy: new CircuitPolicy({ token_budget: 500 }),
    });

    breaker.evaluate("Message with 300 tokens", "agent", "swarm-1", 300);
    assert.throws(
      () => breaker.evaluate("Message with another 300 tokens", "agent", "swarm-1", 300),
      (err) => {
        assert.ok(err instanceof SwarmCircuitBreakException);
        assert.ok(err.reason.includes("Session token budget exceeded"));
        return true;
      }
    );
  });

  await t.test("enforces cost_budget quota", () => {
    const breaker = new EntropyCircuitBreaker({
      policy: new CircuitPolicy({ cost_budget: 0.01, cost_per_token: 0.0001 }), // $0.01 budget, 100 tokens max
    });

    breaker.evaluate("Message with 60 tokens", "agent", "swarm-1", 60);
    assert.throws(
      () => breaker.evaluate("Message with 60 more tokens", "agent", "swarm-1", 60),
      (err) => {
        assert.ok(err instanceof SwarmCircuitBreakException);
        assert.ok(err.reason.includes("Session cost budget exceeded"));
        return true;
      }
    );
  });
});
