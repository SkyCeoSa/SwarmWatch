/**
 * TDD Test Suite: SwarmWatch TypeScript Multi-Tenancy & Cryptographic API Key Engine.
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";
import {
  ApiKeyManager,
  isApiKeyActive,
  HttpStorageSink,
  SwarmClient,
  InMemoryStorageSink,
} from "../src/index.ts";

test("ApiKeyManager Cryptographic Key Operations", async (t) => {
  await t.test("generates raw keys with correct live and test prefixes", () => {
    const liveKey = ApiKeyManager.generateRawKey(false);
    const testKey = ApiKeyManager.generateRawKey(true);

    assert.ok(liveKey.startsWith("sw_live_"), "Live key must start with sw_live_");
    assert.ok(testKey.startsWith("sw_test_"), "Test key must start with sw_test_");
    assert.ok(liveKey.length > 30, "Raw key must contain sufficient cryptographic entropy");
  });

  await t.test("computes deterministic SHA-256 hash matching node:crypto", () => {
    const sampleKey = "sw_live_testkey1234567890abcdef";
    const expected = crypto.createHash("sha256").update(sampleKey, "utf8").digest("hex");
    const actual = ApiKeyManager.hashKey(sampleKey);

    assert.equal(actual, expected);
  });

  await t.test("extracts safe prefix with ellipsis", () => {
    const key = "sw_live_1234567890abcdef";
    const prefix = ApiKeyManager.extractPrefix(key);
    assert.equal(prefix, "sw_live_1234...");
  });

  await t.test("creates, verifies, and invalidates API key records", () => {
    const manager = new ApiKeyManager();
    const [rawKey, record] = manager.createApiKeyRecord("proj-alpha", "Production Agent Key", false);

    assert.equal(record.project_id, "proj-alpha");
    assert.equal(record.name, "Production Agent Key");
    assert.ok(isApiKeyActive(record), "Key must be active upon creation");

    // Verification succeeds with raw secret
    const verified = manager.verifyRawKey(rawKey);
    assert.ok(verified, "Verification must succeed for valid raw key");
    assert.equal(verified?.id, record.id);

    // Constant-time rejection on incorrect or altered secret
    const tampered = rawKey.endsWith("X") ? rawKey.slice(0, -1) + "Y" : rawKey.slice(0, -1) + "X";
    assert.equal(manager.verifyRawKey(tampered), null, "Tampered key must be rejected");
    assert.equal(manager.verifyRawKey("invalid-prefix-1234"), null, "Invalid prefix must be rejected");

    // Revocation check
    record.revoked_at = new Date().toISOString();
    assert.equal(isApiKeyActive(record), false);
    assert.equal(manager.verifyRawKey(rawKey), null, "Revoked key must be rejected");
  });
});

test("HttpStorageSink Tenant Headers & Multi-Tenancy", async (t) => {
  await t.test("configures apiKey and projectId and injects proper headers", async () => {
    const capturedHeaders = [];
    const capturedBodies = [];

    // Mock global fetch
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url, init) => {
      capturedHeaders.push(init?.headers);
      capturedBodies.push(JSON.parse(init?.body || "[]"));
      return { status: 200, ok: true, json: async () => ({ status: "ok" }) };
    };

    try {
      const sink = new HttpStorageSink({
        endpoint: "http://localhost:8765",
        apiKey: "sw_live_sec123456",
        projectId: "proj-risk-gate",
      });

      assert.equal(sink.endpoint, "http://localhost:8765");
      assert.equal(sink.apiKey, "sw_live_sec123456");
      assert.equal(sink.projectId, "proj-risk-gate");

      const span = {
        id: "span-01",
        swarm_id: "swarm-01",
        source_agent: "Alpha",
        target_agent: "Beta",
        start_time: new Date().toISOString(),
        end_time: new Date().toISOString(),
      };

      await sink.saveSpans([span]);

      assert.equal(capturedHeaders.length, 1);
      const headers = capturedHeaders[0];
      assert.equal(headers["Authorization"], "Bearer sw_live_sec123456");
      assert.equal(headers["X-API-Key"], "sw_live_sec123456");
      assert.equal(headers["X-Project-ID"], "proj-risk-gate");

      // Verify span was stamped with project_id
      assert.equal(capturedBodies[0][0].project_id, "proj-risk-gate");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

test("SwarmClient Multi-Tenant Isolation & Context Propagation", async (t) => {
  await t.test("stamps client project_id on all emitted spans", async () => {
    const memorySink = new InMemoryStorageSink();
    const client = new SwarmClient({
      swarm_id: "swarm-trade-01",
      project_id: "fintech-core",
      api_key: "sw_live_fintech_key",
      sink: memorySink,
    });

    assert.equal(client.projectId, "fintech-core");
    assert.equal(client.apiKey, "sw_live_fintech_key");

    await client.withSpan({ name: "ArbitrageEvaluator", target: "RiskDesk" }, async (span) => {
      assert.equal(span.project_id, "fintech-core");
      assert.equal(span.swarm_id, "swarm-trade-01");
      return 42;
    });

    await client.transport.flush();
    assert.equal(memorySink.spans.length, 1);
    assert.equal(memorySink.spans[0].project_id, "fintech-core");

    await client.transport.shutdown();
  });
});
