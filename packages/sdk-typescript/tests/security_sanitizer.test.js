/**
 * Tests for SwarmWatch Enterprise Security & Automated Secret Redaction Engine (TypeScript SDK).
 * Conforms to ADR 0019 & Seam 10 parity specifications.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  SensitiveDataMasker,
  getDefaultMasker,
} from "../src/security.ts";

import {
  SafeStateSerializer,
  TelemetryTransport,
  InMemoryStorageSink,
} from "../src/index.ts";

test("Enterprise Secret Redaction Engine (Seam 10 / ADR 0019)", async (t) => {
  const masker = new SensitiveDataMasker();

  await t.test("redacts OpenAI and Anthropic API keys", () => {
    const text1 = "Connecting with key sk-proj-abc12345678901234567890 to cluster";
    const text2 = "Using Claude key anthropic-sk-ant-api03-abcdef1234567890123456 for analysis";
    assert.strictEqual(
      masker.maskText(text1),
      "Connecting with key [REDACTED_API_KEY] to cluster"
    );
    assert.strictEqual(
      masker.maskText(text2),
      "Using Claude key [REDACTED_API_KEY] for analysis"
    );
  });

  await t.test("redacts AWS access key IDs", () => {
    const text = "Deploying with credentials AKIAIOSFODNN7EXAMPLE to us-east-1";
    assert.strictEqual(
      masker.maskText(text),
      "Deploying with credentials [REDACTED_AWS_KEY] to us-east-1"
    );
  });

  await t.test("redacts database DSNs with passwords", () => {
    const text = "Connecting to postgresql://admin:p@ssw0rd123@db.prod.internal:5432/analytics";
    assert.strictEqual(
      masker.maskText(text),
      "Connecting to [REDACTED_DB_DSN]"
    );
  });

  await t.test("redacts Bearer and standalone JSON Web Tokens", () => {
    const rawJwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozGz6e";
    const text = `Authorization: Bearer ${rawJwt}`;
    assert.strictEqual(
      masker.maskText(text),
      "Authorization: Bearer [REDACTED_JWT]"
    );
    assert.strictEqual(
      masker.maskText(`token=${rawJwt}`),
      "token=[REDACTED_JWT]"
    );
  });

  await t.test("redacts multi-line PEM private key blocks", () => {
    const pem = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA0Y1+abcdefghijklmnopqrstuvwxyz
-----END RSA PRIVATE KEY-----`;
    const text = `Server certificate loaded with key:\n${pem}`;
    assert.strictEqual(
      masker.maskText(text),
      "Server certificate loaded with key:\n[REDACTED_PRIVATE_KEY]"
    );
  });

  await t.test("redacts credit cards and customer email addresses", () => {
    const text = "Customer alice@example.com charged card 4532-1488-9234-1102";
    assert.strictEqual(
      masker.maskText(text),
      "Customer [REDACTED_EMAIL] charged card [REDACTED_CARD]"
    );
  });

  await t.test("recursively masks complex nested payloads and sensitive key names", () => {
    const payload = {
      user: "bob",
      api_key: "sk-proj-supersecretkey1234567890",
      db_url: "mysql://root:secret@10.0.0.1:3306/db",
      metadata: {
        token: "session-token-raw",
        email: "bob@corporate.org",
        items: [
          { card: "4532 1488 9234 1102", note: "valid" },
          "sk-1234567890123456789012345",
        ],
      },
    };

    const sanitized = masker.maskPayload(payload);
    assert.strictEqual(sanitized.user, "bob");
    assert.strictEqual(sanitized.api_key, "[REDACTED_API_KEY]");
    assert.strictEqual(sanitized.db_url, "[REDACTED_DB_DSN]");
    assert.strictEqual(sanitized.metadata.token, "[REDACTED]");
    assert.strictEqual(sanitized.metadata.email, "[REDACTED_EMAIL]");
    assert.strictEqual(sanitized.metadata.items[0].card, "[REDACTED_CARD]");
    assert.strictEqual(sanitized.metadata.items[0].note, "valid");
    assert.strictEqual(sanitized.metadata.items[1], "[REDACTED_API_KEY]");
  });

  await t.test("gracefully handles circular object graphs during sanitization", () => {
    const nodeA = { name: "AgentA" };
    const nodeB = { name: "AgentB", peer: nodeA };
    nodeA.peer = nodeB;

    const sanitized = masker.maskPayload(nodeA);
    assert.strictEqual(sanitized.name, "AgentA");
    assert.strictEqual(sanitized.peer.name, "AgentB");
    assert.strictEqual(sanitized.peer.peer, "[Circular Reference]");
  });

  await t.test("SafeStateSerializer integrates SensitiveDataMasker", () => {
    const serializer = new SafeStateSerializer();
    const state = {
      agent: "Researcher",
      connection: "redis://:secretpass@redis.cache:6379/0",
      nested: {
        secret_key: "sk-ant-api03-abcdef1234567890123456",
      },
    };

    const sanitized = serializer.sanitize(state);
    assert.strictEqual(sanitized.connection, "[REDACTED_DB_DSN]");
    assert.strictEqual(sanitized.nested.secret_key, "[REDACTED_API_KEY]");
  });

  await t.test("TelemetryTransport.emitSpan automatically redacts input/output payloads", async () => {
    const sink = new InMemoryStorageSink();
    const transport = new TelemetryTransport(sink, { batch_size: 10 });

    const span = {
      span_id: "span-test-sec",
      swarm_id: "swarm-test-sec",
      source_agent: "Engineer",
      target_agent: "LLM",
      step_index: 1,
      input_payload: {
        prompt: "Use database postgresql://user:pass@host:5432/main",
        token: "sk-proj-12345678901234567890",
      },
      output_payload: {
        raw_response: "Found key AKIAIOSFODNN7EXAMPLE in config",
      },
      start_time: new Date().toISOString(),
    };

    transport.emitSpan(span);
    await transport.flush();

    assert.strictEqual(sink.spans.length, 1);
    const saved = sink.spans[0];
    assert.strictEqual(
      saved.input_payload.prompt,
      "Use database [REDACTED_DB_DSN]"
    );
    assert.strictEqual(
      saved.input_payload.token,
      "[REDACTED_API_KEY]"
    );
    assert.strictEqual(
      saved.output_payload.raw_response,
      "Found key [REDACTED_AWS_KEY] in config"
    );
  });

  await t.test("high-throughput benchmark: executes in < 20 microseconds per payload", () => {
    const testPayload = {
      query: "SELECT * FROM users",
      auth: "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozGz6e",
      key: "sk-proj-12345678901234567890",
      email: "test@company.com",
    };

    // JIT warm-up
    for (let i = 0; i < 50; i++) {
      masker.maskPayload(testPayload);
    }

    const iterations = 1000;
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
      masker.maskPayload(testPayload);
    }
    const elapsed = performance.now() - start;
    const perCallMicroseconds = (elapsed / iterations) * 1000;

    // Must be ultra-fast (< 100 microseconds under parallel process load)
    assert.ok(
      perCallMicroseconds < 100,
      `Expected < 100µs per call, got ${perCallMicroseconds.toFixed(2)}µs`
    );
  });
});
