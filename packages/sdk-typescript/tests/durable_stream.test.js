import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { DurableCausalStream } from "../src/durable_stream.ts";

test("Seam 86: Durable stream append and recovery (TypeScript)", () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sw-stream-test-"));
  try {
    const stream = new DurableCausalStream("stream-test-01", tmpDir);

    const off0 = stream.append({ event: "agent_started", agent_id: "agent-1" });
    const off1 = stream.append({ event: "tool_executed", tool: "search" });
    const off2 = stream.append({ event: "agent_finished", tokens: 42 });

    assert.equal(off0, 0);
    assert.equal(off1, 1);
    assert.equal(off2, 2);

    const recoveredStream = new DurableCausalStream("stream-test-01", tmpDir);
    const head = recoveredStream.recover();

    assert.equal(head.head_offset, 2);
    assert.equal(head.total_records, 3);
    const records = recoveredStream.readRange(0);
    assert.equal(records.length, 3);
    assert.equal(records[0].payload.event, "agent_started");
    assert.equal(records[2].payload.tokens, 42);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("Seam 86: Durable stream torn tail crash truncation (TypeScript)", () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sw-stream-torn-"));
  try {
    const stream = new DurableCausalStream("stream-torn-01", tmpDir);
    stream.append({ turn: 1, msg: "hello" });
    stream.append({ turn: 2, msg: "world" });

    const walFile = path.join(tmpDir, "stream-torn-01.wal");
    fs.appendFileSync(
      walFile,
      '{"offset": 2, "timestamp_hlc": "CORRUPT_JSON_HALF_WRITE\n'
    );

    const newStream = new DurableCausalStream("stream-torn-01", tmpDir);
    const head = newStream.recover();

    assert.equal(head.head_offset, 1);
    assert.equal(head.total_records, 2);
    const records = newStream.readRange(0);
    assert.equal(records.length, 2);
    assert.equal(records[1].payload.msg, "world");
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("Seam 86: Bounded memory compaction (TypeScript)", () => {
  const stream = new DurableCausalStream("stream-compact-01");

  for (let i = 0; i < 10; i++) {
    stream.append({ index: i, data: `payload-${i}` });
  }

  assert.equal(stream.getStatus().active_records_in_ram, 10);

  const snapshot = stream.compact(7);
  assert.equal(snapshot.compacted_before_offset, 7);
  assert.equal(snapshot.record_count, 7);
  assert.equal(snapshot.merkle_root.length, 64);

  assert.equal(stream.getStatus().active_records_in_ram, 3);
  const remaining = stream.readRange(0);
  assert.equal(remaining.length, 3);
  assert.equal(remaining[0].offset, 7);
  assert.equal(remaining[2].offset, 9);
});

test("Seam 86: Sub-250us execution latency SLO (TypeScript)", () => {
  const stream = new DurableCausalStream("stream-perf-01");

  // Warmup
  for (let i = 0; i < 50; i++) stream.append({ warmup: i });

  const startNs = process.hrtime.bigint();
  const count = 500;
  for (let i = 0; i < count; i++) {
    stream.append({ tick: i });
  }
  const avgUs = Number(process.hrtime.bigint() - startNs) / (count * 1000);
  assert.ok(avgUs < 250, `Average append latency ${avgUs}us must be within 250us SLO`);
});
