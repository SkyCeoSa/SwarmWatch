/**
 * Tests for Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal (Seam 63 / ADR 0074).
 * Verifies dual-tier durability, fail-closed bounded admission, cryptographic hash chaining,
 * crash recovery replay, sequence gap detection, and sub-250 microsecond latency SLO.
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

import {
  EvidenceRecordType,
  EvidenceSpoolFullException,
  EvidenceCorruptionException,
  SequenceGapException,
  computeCrc32,
  EvidenceRecord,
  EvidenceSpool,
  globalEvidenceSpool,
} from "../src/evidence_spool.ts";

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "swarmwatch_ts_evidence_"));
}

test("Seam 63: Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal", async (t) => {
  await t.test("Record creation, canonical hashing, and CRC32 checksum", () => {
    const payload = { reason: "Shannon entropy collapsed below threshold", entropy: 0.05 };
    const rec = new EvidenceRecord({
      record_id: "rec-ts-001",
      sequence_number: 1,
      timestamp_ns: 1_000_000_000,
      record_type: EvidenceRecordType.SAFETY_TRIP,
      tenant_id: "tenant-acme",
      agent_id: "agent-evaluator",
      payload,
      prev_record_hash: "0".repeat(64),
    });

    assert.ok(rec.record_hash);
    assert.strictEqual(rec.record_hash.length, 64);
    assert.ok(rec.checksum > 0);

    const d = rec.toDict();
    assert.strictEqual(d.record_id, "rec-ts-001");
    assert.strictEqual(d.sequence_number, 1);
    assert.strictEqual(d.record_type, "SAFETY_TRIP");
    assert.strictEqual(d.tenant_id, "tenant-acme");

    const restored = EvidenceRecord.fromDict(d);
    assert.strictEqual(restored.record_id, rec.record_id);
    assert.strictEqual(restored.record_hash, rec.record_hash);
    assert.strictEqual(restored.checksum, rec.checksum);
  });

  await t.test("Spool offer, monotonic sequence, and cryptographic hash chain", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: true });

    try {
      const r1 = spool.offer(
        EvidenceRecordType.SAFETY_TRIP,
        "tenant-alpha",
        "agent-1",
        { action: "CIRCUIT_TRIP", counterfactual_savings_usd: 142.5 }
      );
      assert.strictEqual(r1.sequence_number, 1);
      assert.strictEqual(r1.persisted, true);

      const r2 = spool.offer(
        EvidenceRecordType.BYZANTINE_QUARANTINE,
        "tenant-alpha",
        "agent-2",
        { attack_vector: "PROMPT_INJECTION", confidence: 0.99 }
      );
      assert.strictEqual(r2.sequence_number, 2);

      const r3 = spool.offer(
        EvidenceRecordType.EMERGENCY_HALT,
        "tenant-alpha",
        "agent-coordinator",
        { directive: "SYSTEM_SHUTDOWN", operator: "sec-ops" }
      );
      assert.strictEqual(r3.sequence_number, 3);

      assert.strictEqual(spool.getRecordCount(), 3);
      assert.strictEqual(spool.getHeadSequenceNumber(), 3);

      const records = spool.getRecords();
      assert.strictEqual(records[0].prev_record_hash, "0".repeat(64));
      assert.strictEqual(records[1].prev_record_hash, records[0].record_hash);
      assert.strictEqual(records[2].prev_record_hash, records[1].record_hash);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Spool flush and persistent file write with fsync", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: false });

    try {
      spool.offer(
        EvidenceRecordType.POLICY_BREACH,
        "tenant-corp",
        "agent-worker",
        { violation: "UNAUTHORIZED_FILE_ACCESS", target: "/etc/shadow" }
      );
      assert.strictEqual(spool.getUnflushedCount(), 1);

      const flushRes = spool.flush();
      assert.strictEqual(flushRes.flushed_count, 1);
      assert.ok(flushRes.bytes_written > 0);
      assert.strictEqual(spool.getUnflushedCount(), 0);

      assert.ok(fs.existsSync(journalPath));
      const content = fs.readFileSync(journalPath, "utf8");
      const lines = content.trim().split("\n");
      assert.strictEqual(lines.length, 1);
      const data = JSON.parse(lines[0]);
      assert.strictEqual(data.sequence_number, 1);
      assert.strictEqual(data.record_type, "POLICY_BREACH");
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Crash recovery replay: intact journal verification", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool1 = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: true });

    try {
      for (let i = 1; i <= 5; i++) {
        spool1.offer(
          EvidenceRecordType.SAFETY_TRIP,
          "tenant-recovery",
          `agent-${i}`,
          { turn: i, reason: `trip-${i}` }
        );
      }

      const headHashOriginal = spool1.getHeadHash();
      spool1.close();

      const spool2 = new EvidenceSpool({ journalPath, maxCapacity: 1000 });
      const recRes = spool2.recover();

      assert.strictEqual(recRes.is_intact, true);
      assert.strictEqual(recRes.recovered_count, 5);
      assert.strictEqual(recRes.first_sequence_number, 1);
      assert.strictEqual(recRes.last_sequence_number, 5);
      assert.strictEqual(recRes.head_hash, headHashOriginal);
      assert.strictEqual(spool2.getRecordCount(), 5);
      assert.strictEqual(spool2.getHeadSequenceNumber(), 5);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Crash recovery rejection on corrupt checksum", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: true });

    try {
      spool.offer(
        EvidenceRecordType.SAFETY_TRIP,
        "tenant-x",
        "agent-x",
        { v: 1 }
      );
      spool.close();

      // Tamper with payload byte without updating checksum
      const raw = fs.readFileSync(journalPath, "utf8");
      const data = JSON.parse(raw.trim());
      data.payload.v = 999;
      fs.writeFileSync(journalPath, JSON.stringify(data) + "\n");

      const spoolCorrupt = new EvidenceSpool({ journalPath });
      assert.throws(
        () => spoolCorrupt.recover(),
        (err) => err instanceof EvidenceCorruptionException && (err.code === "CORRUPT_CHECKSUM" || /checksum/i.test(err.message))
      );
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Crash recovery rejection on sequence gap", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: true });

    try {
      for (let i = 1; i <= 3; i++) {
        spool.offer(
          EvidenceRecordType.SAFETY_TRIP,
          "tenant-y",
          `agent-${i}`,
          { turn: i }
        );
      }
      spool.close();

      // Remove record with sequence_number == 2
      const raw = fs.readFileSync(journalPath, "utf8");
      const lines = raw.trim().split("\n");
      lines.splice(1, 1); // remove seq 2
      fs.writeFileSync(journalPath, lines.join("\n") + "\n");

      const spoolGap = new EvidenceSpool({ journalPath });
      assert.throws(
        () => spoolGap.recover(),
        (err) => err instanceof SequenceGapException && /sequence gap/i.test(err.message)
      );
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Crash recovery rejection on tampered hash chain", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: true });

    try {
      for (let i = 1; i <= 3; i++) {
        spool.offer(
          EvidenceRecordType.SAFETY_TRIP,
          "tenant-z",
          `agent-${i}`,
          { turn: i }
        );
      }
      spool.close();

      // Tamper with prev_record_hash of sequence 3 and update checksum
      const raw = fs.readFileSync(journalPath, "utf8");
      const lines = raw.trim().split("\n");
      const data = JSON.parse(lines[2]);
      data.prev_record_hash = "f".repeat(64);
      const tamperedRec = EvidenceRecord.fromDict(data);
      data.checksum = tamperedRec.computeChecksum();
      lines[2] = JSON.stringify(data);
      fs.writeFileSync(journalPath, lines.join("\n") + "\n");

      const spoolChainBreak = new EvidenceSpool({ journalPath });
      assert.throws(
        () => spoolChainBreak.recover(),
        (err) => err instanceof EvidenceCorruptionException && /hash/i.test(err.message)
      );
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Fail-closed bounded capacity: EVIDENCE_JOURNAL_FULL", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 3, autoFlush: true });

    try {
      spool.offer(EvidenceRecordType.SAFETY_TRIP, "t", "a", { k: 1 });
      spool.offer(EvidenceRecordType.SAFETY_TRIP, "t", "a", { k: 2 });
      spool.offer(EvidenceRecordType.SAFETY_TRIP, "t", "a", { k: 3 });

      assert.throws(
        () => spool.offer(EvidenceRecordType.SAFETY_TRIP, "t", "a", { k: 4 }),
        (err) => err instanceof EvidenceSpoolFullException && err.code === "EVIDENCE_JOURNAL_FULL"
      );
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Dual-tier isolation: telemetry drop does not affect critical zero-drop", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 10, autoFlush: true });

    try {
      spool.recordTelemetryDrop(150);
      assert.strictEqual(spool.getDroppedTelemetryCount(), 150);

      spool.offer(
        EvidenceRecordType.SAFETY_TRIP,
        "t-sec",
        "a-guard",
        { critical: true }
      );

      const completeness = spool.completeness("t-sec");
      assert.strictEqual(completeness.dropped_critical_count, 0);
      assert.strictEqual(completeness.verified_contiguous, true);
      assert.strictEqual(completeness.compliance_standard, "NIST_SP_800_53_REV5_AU4");
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("Audit completeness receipt generation", () => {
    const tempDir = createTempDir();
    const journalPath = path.join(tempDir, "evidence.wal");
    const spool = new EvidenceSpool({ journalPath, maxCapacity: 1000, autoFlush: true });

    try {
      for (let i = 0; i < 10; i++) {
        spool.offer(
          EvidenceRecordType.SAFETY_TRIP,
          "tenant-audit",
          `agent-${i}`,
          { turn: i }
        );
      }

      const receipt = spool.completeness("tenant-audit");
      assert.strictEqual(receipt.tenant_id, "tenant-audit");
      assert.strictEqual(receipt.start_sequence_number, 1);
      assert.strictEqual(receipt.end_sequence_number, 10);
      assert.strictEqual(receipt.total_records, 10);
      assert.strictEqual(receipt.dropped_critical_count, 0);
      assert.strictEqual(receipt.verified_contiguous, true);
      assert.strictEqual(receipt.head_hash, spool.getHeadHash());
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  await t.test("In-memory latency & micro-benchmark: sub-250 microsecond execution (audit SLO)", () => {
    const spool = new EvidenceSpool({ journalPath: null, maxCapacity: 100_000, autoFlush: false });

    // Warmup
    for (let i = 0; i < 50; i++) {
      spool.offer(EvidenceRecordType.SAFETY_TRIP, "bench", "agent-bench", { warmup: true });
    }
    spool.reset();

    const iterations = 1000;
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
      spool.offer(EvidenceRecordType.SAFETY_TRIP, "bench", "agent-bench", { iteration: i, status: "TRIPPED" });
    }
    const elapsedMs = performance.now() - start;
    const avgUs = (elapsedMs / iterations) * 1000.0;

    console.log(`\n[TS EVIDENCE BENCHMARK] EvidenceSpool.offer avg: ${avgUs.toFixed(2)}µs (audit SLO: <250µs)`);
    assert.ok(avgUs < 250.0, `Evidence offer latency ${avgUs.toFixed(2)}µs exceeded 250µs audit SLO`);
  });

  await t.test("Global evidence spool singleton & status reporting", () => {
    globalEvidenceSpool.reset();
    const statusBefore = globalEvidenceSpool.status();
    assert.strictEqual(statusBefore.record_count, 0);
    assert.strictEqual(statusBefore.status, "ACTIVE");

    const rec = globalEvidenceSpool.offer(
      EvidenceRecordType.FENCE_REVOCATION,
      "global-tenant",
      "global-agent",
      { epoch: 42 }
    );
    assert.strictEqual(rec.sequence_number, 1);

    const statusAfter = globalEvidenceSpool.status();
    assert.strictEqual(statusAfter.record_count, 1);
    assert.strictEqual(statusAfter.head_sequence_number, 1);

    const comp = globalEvidenceSpool.completeness();
    assert.strictEqual(comp.total_records, 1);
    assert.strictEqual(comp.dropped_critical_count, 0);
    globalEvidenceSpool.reset();
  });
});
