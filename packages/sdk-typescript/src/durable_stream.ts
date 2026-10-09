/**
 * SwarmWatch Durable Causal Stream Segments & Crash-Consistent Recovery (Seam 86 / ADR 0097).
 *
 * Implements append-only durable stream segments with fsync durability (D1),
 * prefix-closed crash recovery and checksum integrity (D2),
 * and bounded memory compaction into cryptographic state snapshots (D3).
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  type HybridLogicalTimestamp,
  HybridLogicalClock,
  formatHlc,
} from "./hlc.ts";
import { computeMerkleRoot } from "./qc.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class DurableStreamError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DurableStreamError";
  }
}

export class CorruptRecordError extends DurableStreamError {
  constructor(message: string) {
    super(message);
    this.name = "CorruptRecordError";
  }
}

// ==============================================================================
// 2. Domain Types & Interfaces
// ==============================================================================

export interface StreamRecord {
  readonly offset: number;
  readonly timestamp_hlc: string;
  readonly payload: Record<string, unknown>;
  readonly prev_hash: string;
  readonly record_hash: string;
}

export interface StreamHead {
  readonly stream_id: string;
  readonly head_offset: number;
  readonly total_records: number;
  readonly head_hash: string;
  readonly compacted_offset: number;
}

// ==============================================================================
// 3. Durable Causal Stream Deep Module
// ==============================================================================

export class DurableCausalStream {
  readonly stream_id: string;
  readonly data_dir: string | null;
  readonly clock: HybridLogicalClock;

  private _records: StreamRecord[] = [];
  private _headOffset: number = -1;
  private _lastHash: string = "00".repeat(32);
  private _compactedOffset: number = -1;
  private _snapshots: Array<Record<string, unknown>> = [];
  private _journalFile: string | null = null;

  constructor(
    streamId: string,
    dataDir?: string,
    clock?: HybridLogicalClock
  ) {
    this.stream_id = streamId;
    this.data_dir = dataDir ? path.resolve(dataDir) : null;
    this.clock = clock ?? new HybridLogicalClock(streamId);

    if (this.data_dir) {
      if (!fs.existsSync(this.data_dir)) {
        fs.mkdirSync(this.data_dir, { recursive: true });
      }
      this._journalFile = path.join(this.data_dir, `${this.stream_id}.wal`);
      this.recover();
    }
  }

  append(
    payload: Record<string, unknown>,
    timestamp?: HybridLogicalTimestamp
  ): number {
    const ts = timestamp ?? this.clock.now();
    const tsStr = formatHlc(ts);
    const nextOffset = this._headOffset + 1;

    const payloadCanonical = JSON.stringify(payload);
    const hashMaterial = `${nextOffset}:${tsStr}:${this._lastHash}:${payloadCanonical}`;
    const recordHash = crypto
      .createHash("sha256")
      .update(hashMaterial, "utf-8")
      .digest("hex");

    const record: StreamRecord = {
      offset: nextOffset,
      timestamp_hlc: tsStr,
      payload,
      prev_hash: this._lastHash,
      record_hash: recordHash,
    };

    if (this._journalFile) {
      const line = JSON.stringify(record) + "\n";
      const fd = fs.openSync(this._journalFile, "a");
      fs.writeSync(fd, line);
      fs.fsyncSync(fd); // Invariant D1: Explicit fsync durability
      fs.closeSync(fd);
    }

    this._records.push(record);
    this._headOffset = nextOffset;
    this._lastHash = recordHash;

    return nextOffset;
  }

  recover(): StreamHead {
    if (!this._journalFile || !fs.existsSync(this._journalFile)) {
      return {
        stream_id: this.stream_id,
        head_offset: this._headOffset,
        total_records: this._records.length,
        head_hash: this._lastHash,
        compacted_offset: this._compactedOffset,
      };
    }

    const validRecords: StreamRecord[] = [];
    let expectedOffset = 0;
    let prevHash = "00".repeat(32);

    const content = fs.readFileSync(this._journalFile, "utf-8");
    const lines = content.split("\n");

    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line) continue;

      try {
        const data = JSON.parse(line);
        const rec: StreamRecord = {
          offset: data.offset,
          timestamp_hlc: data.timestamp_hlc,
          payload: data.payload,
          prev_hash: data.prev_hash,
          record_hash: data.record_hash,
        };

        if (rec.offset !== expectedOffset || rec.prev_hash !== prevHash) {
          break; // Invariant D2: Discard torn tail writes, retain prefix-closed log
        }

        const payloadCanonical = JSON.stringify(rec.payload);
        const expectedHash = crypto
          .createHash("sha256")
          .update(
            `${rec.offset}:${rec.timestamp_hlc}:${rec.prev_hash}:${payloadCanonical}`,
            "utf-8"
          )
          .digest("hex");

        if (rec.record_hash !== expectedHash) {
          break; // Stop at corrupted tail
        }

        validRecords.push(rec);
        expectedOffset += 1;
        prevHash = rec.record_hash;
      } catch {
        break; // Corrupted JSON: truncate
      }
    }

    this._records = validRecords;
    this._headOffset = expectedOffset > 0 ? expectedOffset - 1 : -1;
    this._lastHash = prevHash;

    return {
      stream_id: this.stream_id,
      head_offset: this._headOffset,
      total_records: this._records.length,
      head_hash: this._lastHash,
      compacted_offset: this._compactedOffset,
    };
  }

  compact(beforeOffset: number): Record<string, unknown> {
    if (beforeOffset <= this._compactedOffset) {
      return { status: "no_op", compacted_offset: this._compactedOffset };
    }

    const toCompact = this._records.filter((r) => r.offset < beforeOffset);
    if (toCompact.length === 0) {
      return { status: "no_op", compacted_offset: this._compactedOffset };
    }

    const leafHashes = toCompact.map((r) => r.record_hash);
    const merkleRoot = computeMerkleRoot(leafHashes);

    const snapshot = {
      snapshot_id: crypto
        .createHash("sha256")
        .update(`snap:${beforeOffset}:${merkleRoot}`, "utf-8")
        .digest("hex"),
      compacted_before_offset: beforeOffset,
      record_count: toCompact.length,
      merkle_root: merkleRoot,
      compacted_at_ns: Date.now() * 1_000_000,
    };

    this._snapshots.push(snapshot);
    this._records = this._records.filter((r) => r.offset >= beforeOffset);
    this._compactedOffset = beforeOffset - 1;

    return snapshot;
  }

  readRange(startOffset: number, limit: number = 100): StreamRecord[] {
    return this._records
      .filter((r) => r.offset >= startOffset)
      .slice(0, limit);
  }

  getStatus(): Record<string, unknown> {
    return {
      stream_id: this.stream_id,
      head_offset: this._headOffset,
      active_records_in_ram: this._records.length,
      compacted_offset: this._compactedOffset,
      total_snapshots: this._snapshots.length,
      last_hash: this._lastHash,
    };
  }
}
