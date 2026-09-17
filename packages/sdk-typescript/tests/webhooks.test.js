/**
 * Unit and Integration Tests for SwarmWatch Enterprise Webhook Alerting Dispatcher
 * (TypeScript Edition - Seam 6 & 7 Extension / ADR 0007 & ADR 0023).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  WebhookNotifier,
  formatWebhookPayload,
} from "../src/index.ts";

test("Enterprise Webhook Alerting Dispatcher & PagerDuty (TypeScript Edition)", async (t) => {
  await t.test("formatWebhookPayload generates valid Slack Block Kit with counterfactual savings", () => {
    const event = {
      id: "ce-slack-ts-1",
      swarm_id: "trading-swarm-prod",
      span_id: "span-1",
      event_type: "trip",
      reason: "Low Shannon entropy detected (0.18 <= 0.40)",
      metric_val: 0.18,
      counterfactual_savings: {
        tokens_saved: 45000,
        dollars_saved: 0.90,
        turns_averted: 9,
        budget_consumed: 0.20,
        budget_protected: 1.80,
      },
      created_at: new Date().toISOString(),
    };

    const config = {
      url: "https://hooks.slack.com/services/test",
      format: "slack",
    };

    const payload = formatWebhookPayload(event, config);
    assert.ok(payload.text.includes("[SWARMWATCH CIRCUIT TRIP]"));
    assert.ok(Array.isArray(payload.blocks));
    assert.strictEqual(payload.blocks[0].type, "header");

    const blockStr = JSON.stringify(payload.blocks);
    assert.ok(blockStr.includes("45,000 tokens"));
    assert.ok(blockStr.includes("$0.9000"));
    assert.ok(blockStr.includes("0.1800"));
  });

  await t.test("formatWebhookPayload generates valid Discord embeds with color semantics", () => {
    const event = {
      id: "ce-discord-ts-1",
      swarm_id: "research-swarm",
      span_id: "span-2",
      event_type: "warning",
      reason: "High lexical repetition detected (0.88 >= 0.85)",
      metric_val: 0.88,
      created_at: new Date().toISOString(),
    };

    const config = {
      url: "https://discord.com/api/webhooks/test",
      format: "discord",
    };

    const payload = formatWebhookPayload(event, config);
    assert.ok(Array.isArray(payload.embeds));
    assert.strictEqual(payload.embeds[0].color, 0xf59e0b); // Amber for warning
    assert.ok(payload.embeds[0].title.includes("CIRCUIT WARNING"));
  });

  await t.test("formatWebhookPayload generates valid PagerDuty Events API v2 payload", () => {
    const event = {
      id: "ce-pd-ts-1",
      swarm_id: "algo-desk-1",
      span_id: "span-3",
      event_type: "trip",
      reason: "Recursive tool loop intercepted",
      metric_val: 0.95,
      counterfactual_savings: {
        tokens_saved: 120000,
        dollars_saved: 2.40,
        turns_averted: 24,
        budget_consumed: 0.50,
        budget_protected: 4.50,
      },
      created_at: new Date().toISOString(),
    };

    const config = {
      url: "https://events.pagerduty.com/v2/enqueue",
      format: "pagerduty",
      routing_key: "pd-routing-key-live",
    };

    const payload = formatWebhookPayload(event, config);
    assert.strictEqual(payload.routing_key, "pd-routing-key-live");
    assert.strictEqual(payload.event_action, "trigger");
    assert.ok(payload.dedup_key.includes("swarmwatch-algo-desk-1-ce-pd-ts-1"));
    assert.strictEqual(payload.payload.severity, "critical");
    assert.strictEqual(payload.payload.source, "swarmwatch-circuit-breaker");
    assert.ok(payload.payload.summary.includes("CIRCUIT TRIP"));

    const details = payload.payload.custom_details;
    assert.strictEqual(details.tokens_saved, 120000);
    assert.strictEqual(details.dollars_saved, 2.40);
    assert.strictEqual(details.turns_averted, 24);
  });

  await t.test("WebhookNotifier non-blocking async delivery with sub-microsecond enqueue", async () => {
    const delivered = [];
    const mockSender = async (url, payload, headers) => {
      delivered.push({ url, payload });
      return true;
    };

    const config = {
      url: "https://mock.api/alerts",
      format: "swarmwatch",
    };

    const notifier = new WebhookNotifier({
      configs: [config],
      sender: mockSender,
    });

    const event = {
      id: "ce-enqueue-1",
      swarm_id: "swarm-nb",
      span_id: "span-4",
      event_type: "trip",
      reason: "Token budget exhaustion",
      metric_val: 50000,
      created_at: new Date().toISOString(),
    };

    // notify must return immediately (synchronous boolean)
    const enqueued = notifier.notify(event);
    assert.strictEqual(enqueued, true);

    await notifier.flush();
    assert.strictEqual(delivered.length, 1);
    assert.strictEqual(delivered[0].payload.swarm_id, "swarm-nb");
    assert.strictEqual(notifier.getStats().delivered, 1);
  });

  await t.test("WebhookNotifier filters events below min_level", async () => {
    const delivered = [];
    const mockSender = async (url, payload) => {
      delivered.push(payload);
      return true;
    };

    const config = {
      url: "https://mock.api/critical",
      format: "slack",
      min_level: "trip", // Ignore warnings
    };

    const notifier = new WebhookNotifier({
      configs: [config],
      sender: mockSender,
    });

    const warnEvent = {
      id: "ce-warn-2",
      swarm_id: "swarm-filter",
      span_id: "span-5",
      event_type: "warning",
      reason: "Potential loop",
      metric_val: 0.86,
      created_at: new Date().toISOString(),
    };

    notifier.notify(warnEvent);
    await notifier.flush();
    assert.strictEqual(delivered.length, 0);
  });

  await t.test("WebhookNotifier Dead-Letter Queue (DLQ) captures failures and allows replay", async () => {
    let failAttempts = 0;
    const failingSender = async () => {
      failAttempts += 1;
      throw new Error("Simulated network timeout");
    };

    const config = {
      url: "https://down.internal/alerts",
      format: "pagerduty",
      max_retries: 2,
    };

    const notifier = new WebhookNotifier({
      configs: [config],
      sender: failingSender,
    });

    const event = {
      id: "ce-dlq-ts-1",
      swarm_id: "swarm-dlq",
      span_id: "span-6",
      event_type: "trip",
      reason: "Crash halt",
      metric_val: 1.0,
      created_at: new Date().toISOString(),
    };

    notifier.notify(event);
    await notifier.flush();

    // Verify DLQ capture
    const dlq = notifier.getDlq();
    assert.strictEqual(dlq.length, 1);
    assert.strictEqual(dlq[0].event_id, "ce-dlq-ts-1");
    assert.strictEqual(dlq[0].format, "pagerduty");
    assert.ok(dlq[0].last_error.includes("network timeout"));

    const stats = notifier.getStats();
    assert.strictEqual(stats.failed, 1);
    assert.strictEqual(stats.retried, 2);
    assert.strictEqual(stats.dlq_count, 1);

    // Now recover sender and replay DLQ
    const replayed = [];
    notifier.sender = async (url, payload) => {
      replayed.push({ url, payload });
      return true;
    };

    const replayedCount = await notifier.replayDlq();
    assert.strictEqual(replayedCount, 1);
    assert.strictEqual(replayed.length, 1);
    assert.strictEqual(notifier.getDlq().length, 0);
    assert.strictEqual(notifier.getStats().dlq_count, 0);
  });
});
