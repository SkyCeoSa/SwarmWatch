import test from "node:test";
import assert from "node:assert/strict";
import {
  ShmRingBuffer,
  ShmRingBufferWriter,
  ShmRingBufferReader,
  ShmIpcGateway,
  globalShmGateway,
  SHM_MAGIC,
  SHM_HEADER_SIZE,
} from "../src/index.ts";

test("Zero-Copy Shared-Memory Ring Buffer IPC Gateway Suite (Seam 37 / ADR 0048)", async (t) => {
  await t.test("initializes 64-byte binary header with correct layout and magic", () => {
    const ring = new ShmRingBuffer("ts_test_ring_01", 64, 512);
    const hdr = ring.readHeader();

    assert.equal(hdr.magic, SHM_MAGIC);
    assert.equal(hdr.version, 1);
    assert.equal(hdr.capacitySlots, 64);
    assert.equal(hdr.slotSize, 512);
    assert.equal(hdr.writeHead, 0);
    assert.equal(hdr.readTail, 0);
    assert.equal(hdr.droppedRecords, 0);
    assert.equal(hdr.totalWritten, 0);
    assert.equal(hdr.totalRead, 0);
    assert.equal(ring.totalSize, SHM_HEADER_SIZE + 64 * 512);
  });

  await t.test("writes spans into ring buffer slots and reads them back in FIFO order", () => {
    const ring = new ShmRingBuffer("ts_test_io_01", 128, 1024);
    const writer = new ShmRingBufferWriter(ring);
    const reader = new ShmRingBufferReader(ring);

    for (let i = 0; i < 10; i++) {
      const ok = writer.writeSpan({
        span_id: `ts_span_${i}`,
        agent: `Agent_${i}`,
        tokens: i * 20,
      });
      assert.equal(ok, true);
    }

    const hdr = ring.readHeader();
    assert.equal(hdr.writeHead, 10);
    assert.equal(hdr.readTail, 0);
    assert.equal(hdr.totalWritten, 10);

    const drained = reader.readAvailable(100);
    assert.equal(drained.length, 10);
    for (let i = 0; i < 10; i++) {
      assert.equal(drained[i].span_id, `ts_span_${i}`);
      assert.equal(drained[i].tokens, i * 20);
    }

    const hdrAfter = ring.readHeader();
    assert.equal(hdrAfter.readTail, 10);
    assert.equal(hdrAfter.totalRead, 10);
  });

  await t.test("handles ring buffer saturation and increments droppedRecords on overflow", () => {
    const capacity = 8;
    const ring = new ShmRingBuffer("ts_test_sat_01", capacity, 256);
    const writer = new ShmRingBufferWriter(ring);

    for (let i = 0; i < capacity; i++) {
      const ok = writer.writeSpan({ index: i });
      assert.equal(ok, true);
    }

    // Next 3 should be dropped
    for (let i = 0; i < 3; i++) {
      const ok = writer.writeSpan({ overflow: i });
      assert.equal(ok, false);
    }

    const hdr = ring.readHeader();
    assert.equal(hdr.writeHead, capacity);
    assert.equal(hdr.droppedRecords, 3);
  });

  await t.test("manages gateway lifecycle, flushes pending spans, and resets buffer", () => {
    const gateway = new ShmIpcGateway("ts_test_gateway_01", 32, 512);

    assert.equal(gateway.getStatus().pendingRecords, 0);

    for (let i = 0; i < 6; i++) {
      gateway.writeSpan({ span_id: `gw_${i}`, tokens: 40 });
    }

    let status = gateway.getStatus();
    assert.equal(status.pendingRecords, 6);
    assert.equal(status.totalWritten, 6);

    const flushed = gateway.flush();
    assert.equal(flushed.length, 6);

    status = gateway.getStatus();
    assert.equal(status.pendingRecords, 0);
    assert.equal(status.totalRead, 6);

    gateway.reset();
    status = gateway.getStatus();
    assert.equal(status.totalWritten, 0);
    assert.equal(status.totalRead, 0);
    assert.equal(status.droppedRecords, 0);
  });
});
