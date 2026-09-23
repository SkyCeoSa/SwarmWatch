import { test, describe } from "node:test";
import assert from "node:assert";
import {
  SseStreamParser,
  SwarmEventSource,
  VapidSigner,
  listenEvents,
  init,
} from "../src/index.ts";

describe("Server-Sent Events (SSE) Stream Parser (ADR 0039)", () => {
  test("parses single complete SSE event with id, event, retry, and JSON data", () => {
    const parser = new SseStreamParser();
    const chunk = "id: evt-101\nevent: circuit_trip\nretry: 4000\ndata: {\"swarm_id\":\"swarm-test\",\"metric_val\":0.15,\"reason\":\"Entropy collapsed\"}\n\n";

    const events = parser.feed(chunk);
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].id, "evt-101");
    assert.strictEqual(events[0].event, "circuit_trip");
    assert.strictEqual(events[0].retry, 4000);
    assert.strictEqual(events[0].data.swarm_id, "swarm-test");
    assert.strictEqual(events[0].data.metric_val, 0.15);
    assert.strictEqual(events[0].data.reason, "Entropy collapsed");
  });

  test("handles chunked fragmented delivery across multiple feed calls", () => {
    const parser = new SseStreamParser();

    // Chunk 1: partial headers
    const evts1 = parser.feed("id: evt-102\nevent: circuit_warn");
    assert.strictEqual(evts1.length, 0);

    // Chunk 2: rest of event header and part of data
    const evts2 = parser.feed("ing\ndata: {\"reason\":\"approaching ");
    assert.strictEqual(evts2.length, 0);

    // Chunk 3: rest of data and double newline
    const evts3 = parser.feed("threshold\"}\n\n");
    assert.strictEqual(evts3.length, 1);
    assert.strictEqual(evts3[0].id, "evt-102");
    assert.strictEqual(evts3[0].event, "circuit_warning");
    assert.strictEqual(evts3[0].data.reason, "approaching threshold");
  });

  test("ignores comment lines and keep-alive pings (: ping)", () => {
    const parser = new SseStreamParser();
    const chunk = ": ping\n\nid: evt-103\nevent: ping\ndata: {\"time\":\"2026-09-23T14:00:00Z\"}\n\n: another comment\n\n";

    const events = parser.feed(chunk);
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].id, "evt-103");
    assert.strictEqual(events[0].event, "ping");
    assert.strictEqual(events[0].data.time, "2026-09-23T14:00:00Z");
  });

  test("parses multiple events in a single batch chunk", () => {
    const parser = new SseStreamParser();
    const chunk = "id: evt-1\nevent: warn\ndata: {\"n\":1}\n\nid: evt-2\nevent: trip\ndata: {\"n\":2}\n\n";

    const events = parser.feed(chunk);
    assert.strictEqual(events.length, 2);
    assert.strictEqual(events[0].id, "evt-1");
    assert.strictEqual(events[0].data.n, 1);
    assert.strictEqual(events[1].id, "evt-2");
    assert.strictEqual(events[1].data.n, 2);
  });
});

describe("VAPID Cryptographic Signer & Header Minting (ADR 0039)", () => {
  test("generates valid NIST P-256 (prime256v1) VAPID keypair", () => {
    const signer = VapidSigner.generate();
    assert.ok(signer.publicKey);
    assert.ok(signer.privateKey);

    // 65 bytes in Base64URL is 87 characters (or 86-88 without padding)
    assert.ok(signer.publicKey.length >= 86 && signer.publicKey.length <= 88);
    // Uncompressed P-256 public key starts with 0x04, which in Base64URL starts with 'B'
    assert.strictEqual(signer.publicKey[0], "B");
  });

  test("mints RFC 8292 standard ES256 VAPID JWT tokens", () => {
    const signer = VapidSigner.generate();
    const token = signer.mintVapidToken("https://updates.push.services.mozilla.com", "mailto:ops@swarmwatch.local", 3600);

    const parts = token.split(".");
    assert.strictEqual(parts.length, 3);

    const header = JSON.parse(Buffer.from(parts[0], "base64url").toString("utf8"));
    const claims = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));

    assert.strictEqual(header.typ, "JWT");
    assert.strictEqual(header.alg, "ES256");
    assert.strictEqual(claims.aud, "https://updates.push.services.mozilla.com");
    assert.strictEqual(claims.sub, "mailto:ops@swarmwatch.local");
    assert.ok(claims.exp > Math.floor(Date.now() / 1000));

    // IEEE P1363 64-byte signature in Base64URL is 86 characters
    assert.strictEqual(parts[2].length, 86);
  });

  test("builds standard WebPush HTTP Authorization and Crypto-Key headers", () => {
    const signer = VapidSigner.generate();
    const endpoint = "https://fcm.googleapis.com/fcm/send/sample-token";
    const headers = signer.buildAuthHeaders(endpoint);

    assert.ok(headers.Authorization.startsWith("vapid t="));
    assert.ok(headers.Authorization.includes(`, k=${signer.publicKey}`));
    assert.strictEqual(headers["Crypto-Key"], `p256ecdsa=${signer.publicKey}`);
  });
});

describe("SwarmClient SSE Integration & EventSource", () => {
  test("SwarmEventSource builds proper endpoint URL with query filters", () => {
    const sse = new SwarmEventSource({
      url: "http://127.0.0.1:8765",
      swarmId: "test-swarm-99",
      minLevel: "trip",
      projectId: "proj-alpha",
      lastEventId: "evt-prev-01",
    });

    assert.ok(sse.url.includes("/api/v1/swarms/test-swarm-99/events/stream"));
    assert.ok(sse.url.includes("min_level=trip"));
    assert.ok(sse.url.includes("project_id=proj-alpha"));
    assert.strictEqual(sse.lastEventId, "evt-prev-01");
  });

  test("listenEvents helper creates and returns unlisten closer", () => {
    init({ swarmId: "swarm-client-test" });
    let received = false;
    const unsub = listenEvents((evt) => {
      received = true;
    });

    assert.strictEqual(typeof unsub, "function");
    // Closing immediately
    unsub();
    assert.strictEqual(received, false);
  });
});
