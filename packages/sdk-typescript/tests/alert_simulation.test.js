/**
 * SwarmWatch Automated Alert Route Webhook Testing & Synthetic Simulation (TypeScript Edition - Seam 18 / ADR 0029).
 *
 * Spins up an ephemeral local HTTP synthetic receiver, validates multi-channel alert delivery,
 * fault injection, DLQ buffering, and zero-loss replay with zero remote cloud dependencies.
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as http from "node:http";

import {
  WebhookNotifier,
} from "../src/index.ts";

test("Automated Alert Route Webhook Testing & Synthetic Simulation (TypeScript / ADR 0029)", async (t) => {
  let server;
  let baseUrl;
  let receivedEvents = [];
  const faults = new Map();

  // 1. Setup local synthetic receiver on ephemeral port (127.0.0.1:0)
  await new Promise((resolve) => {
    server = http.createServer((req, res) => {
      let body = "";
      req.on("data", (chunk) => {
        body += chunk;
      });
      req.on("end", () => {
        const route = req.url || "/";
        const pendingFault = faults.get(route);
        if (pendingFault && pendingFault > 0) {
          faults.set(route, pendingFault - 1);
          res.writeHead(503, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Simulated 503 Service Unavailable" }));
          return;
        }

        let parsed = {};
        try {
          parsed = JSON.parse(body);
        } catch {
          // ignore parse error
        }

        receivedEvents.push({
          route,
          headers: req.headers,
          payload: parsed,
          receivedAt: new Date().toISOString(),
        });

        const statusCode = route.includes("pagerduty") ? 202 : 200;
        res.writeHead(statusCode, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "received", schema_valid: true }));
      });
    });

    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      baseUrl = `http://127.0.0.1:${addr.port}`;
      resolve();
    });
  });

  t.after(() => {
    if (server) {
      server.close();
    }
  });

  await t.test("multi-channel happy-path fan-out (Slack, Discord, PagerDuty)", async () => {
    receivedEvents = [];

    const notifier = new WebhookNotifier({
      configs: [
        { url: `${baseUrl}/webhook/slack`, format: "slack", min_level: "warning" },
        { url: `${baseUrl}/webhook/discord`, format: "discord", min_level: "warning" },
        { url: `${baseUrl}/webhook/pagerduty`, format: "pagerduty", routing_key: "pd-test-key", min_level: "warning" },
      ],
    });

    const testEvent = {
      id: "ce-ts-sim-101",
      swarm_id: "ts-swarm-sim",
      span_id: "span-ts-1",
      event_type: "trip",
      reason: "Shannon entropy collapse below threshold (0.19 <= 0.40)",
      metric_val: 0.19,
      counterfactual_savings: {
        tokens_saved: 60000,
        dollars_saved: 1.20,
        turns_averted: 12,
        budget_consumed: 0.30,
        budget_protected: 1.20,
      },
      created_at: new Date().toISOString(),
    };

    notifier.notify(testEvent);
    await notifier.flush();

    // Verify that all 3 routes received the notification
    assert.strictEqual(receivedEvents.length, 3);

    const slackEvent = receivedEvents.find((e) => e.route.includes("slack"));
    assert.ok(slackEvent);
    assert.ok(slackEvent.payload.blocks);
    assert.ok(JSON.stringify(slackEvent.payload).includes("60,000 tokens"));

    const discordEvent = receivedEvents.find((e) => e.route.includes("discord"));
    assert.ok(discordEvent);
    assert.ok(discordEvent.payload.embeds);

    const pdEvent = receivedEvents.find((e) => e.route.includes("pagerduty"));
    assert.ok(pdEvent);
    assert.strictEqual(pdEvent.payload.event_action, "trigger");
    assert.strictEqual(pdEvent.payload.routing_key, "pd-test-key");

    await notifier.shutdown();
  });

  await t.test("fault injection, dead-letter queue buffering, and zero-loss replay", async () => {
    receivedEvents = [];
    const route = "/webhook/slack-fault";
    // Inject 503 for 3 calls
    faults.set(route, 3);

    const notifier = new WebhookNotifier({
      configs: [
        { url: `${baseUrl}${route}`, format: "slack", max_retries: 2 },
      ],
    });

    const faultEvent = {
      id: "ce-ts-fault-999",
      swarm_id: "ts-swarm-fault",
      span_id: "span-ts-99",
      event_type: "trip",
      reason: "Simulated network timeout",
      metric_val: 0.99,
      created_at: new Date().toISOString(),
    };

    notifier.notify(faultEvent);
    await notifier.flush();

    // Delivery should have failed and accumulated in DLQ
    const dlq = notifier.getDlq();
    assert.strictEqual(dlq.length, 1);
    assert.strictEqual(dlq[0].event_id, "ce-ts-fault-999");
    assert.ok(dlq[0].destination_url.includes(route));

    // Clear fault on receiver to simulate network recovery
    faults.delete(route);

    // Replay DLQ
    const replayed = await notifier.replayDlq();
    assert.strictEqual(replayed, 1);
    assert.strictEqual(notifier.getDlq().length, 0);

    // Assert that the synthetic receiver received the replayed payload
    assert.strictEqual(receivedEvents.length, 1);
    assert.ok(JSON.stringify(receivedEvents[0].payload).includes("Simulated network timeout"));

    await notifier.shutdown();
  });
});
