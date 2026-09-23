import { test } from "node:test";
import assert from "node:assert/strict";
import { Buffer } from "node:buffer";

import {
  GrpcStatusCode,
  GrpcMessageFramer,
  GrpcStreamClient,
} from "../src/grpc.ts";

test("GrpcMessageFramer encodes and decodes 5-byte framed messages", () => {
  const payloadStr = JSON.stringify({ message: "telemetry frame 1", turns: 5 });
  const framed = GrpcMessageFramer.encodeFrame(payloadStr, false);

  // Assert 5-byte header prefix
  assert.equal(framed.length, 5 + Buffer.byteLength(payloadStr));
  assert.equal(framed.readUInt8(0), 0); // uncompressed
  assert.equal(framed.readUInt32BE(1), Buffer.byteLength(payloadStr));
  assert.equal(framed.subarray(5).toString("utf-8"), payloadStr);

  // Decode frame
  const { frames, remaining } = GrpcMessageFramer.decodeFrames(framed);
  assert.equal(frames.length, 1);
  assert.equal(frames[0].toString("utf-8"), payloadStr);
  assert.equal(remaining.length, 0);
});

test("GrpcMessageFramer handles multiple frames in single stream buffer", () => {
  const msg1 = "span-alpha";
  const msg2 = "span-beta";
  const msg3 = "span-gamma";

  const frame1 = GrpcMessageFramer.encodeFrame(msg1);
  const frame2 = GrpcMessageFramer.encodeFrame(msg2);
  const frame3 = GrpcMessageFramer.encodeFrame(msg3);

  const combined = Buffer.concat([frame1, frame2, frame3]);
  const { frames, remaining } = GrpcMessageFramer.decodeFrames(combined);

  assert.equal(frames.length, 3);
  assert.equal(frames[0].toString("utf-8"), msg1);
  assert.equal(frames[1].toString("utf-8"), msg2);
  assert.equal(frames[2].toString("utf-8"), msg3);
  assert.equal(remaining.length, 0);
});

test("GrpcMessageFramer reassembles partial chunk streams across network boundaries", () => {
  const payload = "large-payload-requiring-multiple-tcp-packets";
  const framed = GrpcMessageFramer.encodeFrame(payload);

  // Split packet at byte 8 (5 bytes header + 3 bytes payload)
  const chunk1 = framed.subarray(0, 8);
  const chunk2 = framed.subarray(8);

  const res1 = GrpcMessageFramer.decodeFrames(chunk1);
  assert.equal(res1.frames.length, 0); // Not enough for complete frame
  assert.equal(res1.remaining.length, 8);

  // Feed second chunk with remaining from first chunk
  const combined = Buffer.concat([res1.remaining, chunk2]);
  const res2 = GrpcMessageFramer.decodeFrames(combined);
  assert.equal(res2.frames.length, 1);
  assert.equal(res2.frames[0].toString("utf-8"), payload);
  assert.equal(res2.remaining.length, 0);
});

test("GrpcStatusCode constants conform to canonical gRPC status codes", () => {
  assert.equal(GrpcStatusCode.OK, 0);
  assert.equal(GrpcStatusCode.CANCELLED, 1);
  assert.equal(GrpcStatusCode.INVALID_ARGUMENT, 3);
  assert.equal(GrpcStatusCode.RESOURCE_EXHAUSTED, 8);
  assert.equal(GrpcStatusCode.INTERNAL, 13);
  assert.equal(GrpcStatusCode.UNAUTHENTICATED, 16);
});

test("GrpcStreamClient constructs and formats framed telemetry payload", async () => {
  const client = new GrpcStreamClient({
    serverUrl: "http://127.0.0.1:8765",
    projectId: "prod-trading-01",
  });

  assert.equal(client.serverUrl, "http://127.0.0.1:8765");
  assert.equal(client.projectId, "prod-trading-01");

  // Verify response message encoding helper
  const respFrame = GrpcMessageFramer.encodeResponse(42, "persisted", "All spans saved");
  const { frames } = GrpcMessageFramer.decodeFrames(respFrame);
  assert.equal(frames.length, 1);
  const parsed = JSON.parse(frames[0].toString("utf-8"));
  assert.equal(parsed.received_count, 42);
  assert.equal(parsed.status, "persisted");
});
