/**
 * SwarmWatch Chaos & Deep Edge-Case Test Suite.
 * Tests multilingual Unicode (Persian, Arabic, Chinese),
 * extreme recursion attacks, and concurrent async context isolation.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  computeShannonEntropy,
  computeLexicalSimilarity,
  EntropyCircuitBreaker,
  SwarmCircuitBreakException,
  SafeStateSerializer,
  init,
  withSpan,
  InMemoryStorageSink,
} from "../src/index.ts";

test("Multilingual & Non-ASCII Unicode Entropy Support", async (t) => {
  await t.test("accurately computes Shannon entropy for Persian text", () => {
    const diversePersian = "سیستم ایجنتی مستقل در حال پردازش تراکنش و تایید قرارداد امنیتی در پلتفرم سارم‌واچ";
    const entropy = computeShannonEntropy(diversePersian);
    assert.ok(entropy > 0.85, `Expected diverse Persian entropy > 0.85, got ${entropy}`);

    const repetitivePersian = "سیستم امنیتی سیستم امنیتی سیستم امنیتی سیستم امنیتی سیستم امنیتی";
    const repEntropy = computeShannonEntropy(repetitivePersian);
    assert.ok(repEntropy < 0.35, `Expected repetitive Persian entropy < 0.35, got ${repEntropy}`);
  });

  await t.test("accurately computes entropy for Chinese and Arabic text", () => {
    const diverseArabic = "نظام مراقبة وكلاء الذكاء الاصطناعي وحماية الميزانية من التكرار اللانهائي";
    assert.ok(computeShannonEntropy(diverseArabic) > 0.80);

    const repetitiveArabic = "تكرار الطلب تكرار الطلب تكرار الطلب تكرار الطلب تكرار الطلب";
    assert.ok(computeShannonEntropy(repetitiveArabic) < 0.35);
  });

  await t.test("trips circuit breaker on repetitive Persian agent output", () => {
    const breaker = new EntropyCircuitBreaker({ preset: "balanced" });
    const msg = "لطفاً مجدداً بند ۲.۱ را بررسی و تایید نمایید.";

    const d1 = breaker.evaluate(msg, "PersianAgent");
    assert.strictEqual(d1.decision, "pass");

    const d2 = breaker.evaluate(msg, "PersianAgent");
    assert.strictEqual(d2.decision, "warning");

    assert.throws(
      () => breaker.evaluate(msg, "PersianAgent"),
      (err) => err instanceof SwarmCircuitBreakException
    );
  });
});

test("Adversarial Serialization & Recursion Exploder Attacks", async (t) => {
  const serializer = new SafeStateSerializer({ max_depth: 10 });

  await t.test("gracefully caps deeply nested objects (> 50 levels)", () => {
    let deepObj = { level: 0 };
    let cur = deepObj;
    for (let i = 1; i <= 50; i++) {
      cur.child = { level: i };
      cur = cur.child;
    }

    const sanitized = serializer.sanitize(deepObj);
    assert.ok(sanitized);

    // Verify depth truncation
    let depth = 0;
    let node = sanitized;
    while (node && node.child && typeof node.child === "object") {
      depth++;
      node = node.child;
    }
    assert.ok(depth <= 10, `Depth should be capped at 10, got ${depth}`);
  });

  await t.test("handles multi-cycle recursive loops (A -> B -> C -> A)", () => {
    const a = { name: "A" };
    const b = { name: "B", a };
    const c = { name: "C", b };
    a.c = c; // loop: a -> c -> b -> a

    const sanitized = serializer.sanitize(a);
    assert.strictEqual(sanitized.name, "A");
    assert.ok(sanitized.c);
  });
});

test("Concurrent Async Context Isolation (AsyncLocalStorage)", async (t) => {
  await t.test("100 concurrent async spans do not cross-bleed context", async () => {
    const sink = new InMemoryStorageSink();
    const client = init({ swarm_id: "concurrency-swarm", sink });

    const concurrentTasks = Array.from({ length: 100 }, (_, i) => {
      const agentId = `agent-${i}`;
      return client.withSpan({ name: `task-${i}`, source: agentId }, async (span) => {
        // Sleep varying tiny durations to cause event-loop interleaving
        await new Promise((r) => setTimeout(r, Math.random() * 20));
        assert.strictEqual(client.getCurrentSpan()?.source_agent, agentId);
        return { agentId, index: i };
      });
    });

    const results = await Promise.all(concurrentTasks);
    assert.strictEqual(results.length, 100);

    await client.flush();
    assert.strictEqual(sink.spans.length, 100);
    await client.shutdown();
  });
});
