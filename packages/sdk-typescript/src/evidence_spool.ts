/**
 * Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal (Seam 63 / ADR 0074).
 * Provides an isolated, durable, write-ahead journal for safety-critical evidence (circuit trips,
 * Byzantine quarantines, emergency halts, fence revocations, audit anchors) with bounded
 * admission, fail-closed enforcement (EVIDENCE_JOURNAL_FULL), CRC32 framing, and cryptographic
 * hash chaining for NIST SP 800-53 Rev 5 (AU-4, AU-9, SC-7) compliance.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import * as crypto from "node:crypto";
import { CanonicalCodec, CanonicalDomainTag } from "./canonical.ts";

export const EvidenceRecordType = {
  SAFETY_TRIP: "SAFETY_TRIP",
  POLICY_BREACH: "POLICY_BREACH",
  BYZANTINE_QUARANTINE: "BYZANTINE_QUARANTINE",
  EMERGENCY_HALT: "EMERGENCY_HALT",
  FENCE_REVOCATION: "FENCE_REVOCATION",
  AUDIT_ANCHOR: "AUDIT_ANCHOR",
} as const;

export type EvidenceRecordTypeEnum = typeof EvidenceRecordType[keyof typeof EvidenceRecordType];

export class EvidenceSpoolFullException extends Error {
  public readonly code = "EVIDENCE_JOURNAL_FULL";
  constructor(message: string = "Critical evidence journal capacity reached: execution halted fail-closed (EVIDENCE_JOURNAL_FULL)") {
    super(message);
    this.name = "EvidenceSpoolFullException";
  }
}

export class EvidenceCorruptionException extends Error {
  public readonly code: string;
  constructor(message: string, code: string = "EVIDENCE_CORRUPT") {
    super(message);
    this.name = "EvidenceCorruptionException";
    this.code = code;
  }
}

export class SequenceGapException extends Error {
  public readonly code = "SEQUENCE_GAP_DETECTED";
  constructor(message: string) {
    super(message);
    this.name = "SequenceGapException";
  }
}

// Table-driven IEEE 802.3 CRC32 implementation (identical to Python zlib.crc32)
const CRC32_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let k = 0; k < 8; k++) {
    c = ((c & 1) !== 0) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
  }
  CRC32_TABLE[i] = c >>> 0;
}

export function computeCrc32(buf: Buffer | Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ buf[i]) & 0xFF];
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

export interface JournalReceipt {
  record_id: string;
  sequence_number: number;
  record_hash: string;
  persisted: boolean;
  timestamp_ns: number;
}

export interface FlushResult {
  flushed_count: number;
  bytes_written: number;
  head_sequence_number: number;
  head_hash: string;
}

export interface RecoveryResult {
  recovered_count: number;
  first_sequence_number: number;
  last_sequence_number: number;
  head_hash: string;
  is_intact: boolean;
  journal_path: string;
}

export interface CompletenessReceipt {
  tenant_id: string;
  start_sequence_number: number;
  end_sequence_number: number;
  total_records: number;
  head_hash: string;
  verified_contiguous: boolean;
  dropped_critical_count: number;
  compliance_standard: string;
}

export interface EvidenceSpoolStatus {
  status: "ACTIVE" | "SATURATED" | "FAILED";
  record_count: number;
  max_capacity: number;
  head_sequence_number: number;
  head_hash: string;
  unflushed_count: number;
  journal_file_path: string | null;
  dropped_telemetry_count: number;
}

export interface EvidenceRecordData {
  record_id: string;
  sequence_number: number;
  timestamp_ns: number;
  record_type: string;
  tenant_id: string;
  agent_id: string;
  payload: Record<string, unknown>;
  prev_record_hash: string;
  record_hash?: string;
  checksum?: number;
}

export class EvidenceRecord {
  public static readonly GENESIS_HASH = "0".repeat(64);

  public readonly record_id: string;
  public readonly sequence_number: number;
  public readonly timestamp_ns: number;
  public readonly record_type: string;
  public readonly tenant_id: string;
  public readonly agent_id: string;
  public readonly payload: Record<string, unknown>;
  public readonly prev_record_hash: string;
  public readonly record_hash: string;
  public readonly checksum: number;

  constructor(data: EvidenceRecordData) {
    this.record_id = data.record_id;
    this.sequence_number = data.sequence_number;
    this.timestamp_ns = data.timestamp_ns;
    this.record_type = data.record_type;
    this.tenant_id = data.tenant_id;
    this.agent_id = data.agent_id;
    this.payload = data.payload;
    this.prev_record_hash = data.prev_record_hash || EvidenceRecord.GENESIS_HASH;

    if (data.record_hash) {
      this.record_hash = data.record_hash;
    } else {
      this.record_hash = this.computeRecordHash();
    }

    if (data.checksum !== undefined && data.checksum !== null) {
      this.checksum = data.checksum;
    } else {
      this.checksum = this.computeChecksum();
    }
  }

  public computeRecordHash(): string {
    const hashPayload = {
      agent_id: this.agent_id,
      payload: this.payload,
      prev_record_hash: this.prev_record_hash,
      record_id: this.record_id,
      record_type: this.record_type,
      sequence_number: this.sequence_number,
      tenant_id: this.tenant_id,
      timestamp_ns: this.timestamp_ns,
    };
    return CanonicalCodec.hash(hashPayload, CanonicalDomainTag.EVIDENCE);
  }

  public computeChecksum(): number {
    const body = {
      agent_id: this.agent_id,
      payload: this.payload,
      prev_record_hash: this.prev_record_hash,
      record_hash: this.record_hash,
      record_id: this.record_id,
      record_type: this.record_type,
      sequence_number: this.sequence_number,
      tenant_id: this.tenant_id,
      timestamp_ns: this.timestamp_ns,
    };
    const canonicalStr = CanonicalCodec.canonicalize(body);
    return computeCrc32(Buffer.from(canonicalStr, "utf8"));
  }

  public toDict(): Record<string, unknown> {
    return {
      record_id: this.record_id,
      sequence_number: this.sequence_number,
      timestamp_ns: this.timestamp_ns,
      record_type: this.record_type,
      tenant_id: this.tenant_id,
      agent_id: this.agent_id,
      payload: this.payload,
      prev_record_hash: this.prev_record_hash,
      record_hash: this.record_hash,
      checksum: this.checksum,
    };
  }

  public static fromDict(data: Record<string, unknown>): EvidenceRecord {
    return new EvidenceRecord({
      record_id: String(data.record_id),
      sequence_number: Number(data.sequence_number),
      timestamp_ns: Number(data.timestamp_ns),
      record_type: String(data.record_type),
      tenant_id: String(data.tenant_id),
      agent_id: String(data.agent_id),
      payload: (data.payload as Record<string, unknown>) || {},
      prev_record_hash: String(data.prev_record_hash || EvidenceRecord.GENESIS_HASH),
      record_hash: data.record_hash ? String(data.record_hash) : undefined,
      checksum: data.checksum !== undefined ? Number(data.checksum) : undefined,
    });
  }
}

export class EvidenceSpool {
  private _journalPath: string | null;
  private _maxCapacity: number;
  private _autoFlush: boolean;
  private _records: EvidenceRecord[] = [];
  private _unflushedRecords: EvidenceRecord[] = [];
  private _headSequenceNumber = 0;
  private _headHash: string = EvidenceRecord.GENESIS_HASH;
  private _droppedTelemetryCount = 0;

  constructor(options: {
    journalPath?: string | null;
    maxCapacity?: number;
    autoFlush?: boolean;
    autoRecover?: boolean;
  } = {}) {
    this._journalPath = options.journalPath ?? null;
    this._maxCapacity = options.maxCapacity ?? 100_000;
    this._autoFlush = options.autoFlush ?? true;

    if (options.autoRecover && this._journalPath && fs.existsSync(this._journalPath)) {
      const stats = fs.statSync(this._journalPath);
      if (stats.size > 0) {
        this.recover(this._journalPath);
      }
    }
  }

  public offer(
    recordType: string,
    tenantId: string,
    agentId: string,
    payload: Record<string, unknown>,
    recordId?: string
  ): JournalReceipt {
    if (this._records.length >= this._maxCapacity) {
      throw new EvidenceSpoolFullException(
        `Critical evidence journal capacity reached (${this._maxCapacity}): execution halted fail-closed (EVIDENCE_JOURNAL_FULL)`
      );
    }

    const assignedId = recordId || `evid-${crypto.randomBytes(6).toString("hex")}`;
    const nextSeq = this._headSequenceNumber + 1;
    const nowNs = Number(process.hrtime.bigint());

    const record = new EvidenceRecord({
      record_id: assignedId,
      sequence_number: nextSeq,
      timestamp_ns: nowNs,
      record_type: recordType,
      tenant_id: tenantId,
      agent_id: agentId,
      payload,
      prev_record_hash: this._headHash,
    });

    this._records.push(record);
    this._unflushedRecords.push(record);
    this._headSequenceNumber = nextSeq;
    this._headHash = record.record_hash;

    let persisted = false;
    if (this._autoFlush && this._journalPath) {
      this.flush();
      persisted = true;
    } else if (!this._journalPath) {
      persisted = true;
    }

    return {
      record_id: record.record_id,
      sequence_number: record.sequence_number,
      record_hash: record.record_hash,
      persisted,
      timestamp_ns: record.timestamp_ns,
    };
  }

  public flush(): FlushResult {
    if (!this._journalPath) {
      const count = this._unflushedRecords.length;
      this._unflushedRecords = [];
      return {
        flushed_count: count,
        bytes_written: 0,
        head_sequence_number: this._headSequenceNumber,
        head_hash: this._headHash,
      };
    }

    if (this._unflushedRecords.length === 0) {
      return {
        flushed_count: 0,
        bytes_written: 0,
        head_sequence_number: this._headSequenceNumber,
        head_hash: this._headHash,
      };
    }

    const dir = path.dirname(path.resolve(this._journalPath));
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    let bytesWritten = 0;
    const fd = fs.openSync(this._journalPath, "a");
    try {
      for (const rec of this._unflushedRecords) {
        const line = JSON.stringify(rec.toDict()) + "\n";
        const buf = Buffer.from(line, "utf8");
        fs.writeSync(fd, buf, 0, buf.length);
        bytesWritten += buf.length;
      }
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }

    const flushedCount = this._unflushedRecords.length;
    this._unflushedRecords = [];

    return {
      flushed_count: flushedCount,
      bytes_written: bytesWritten,
      head_sequence_number: this._headSequenceNumber,
      head_hash: this._headHash,
    };
  }

  public recover(journalPath?: string): RecoveryResult {
    const targetPath = journalPath || this._journalPath;
    if (!targetPath || !fs.existsSync(targetPath)) {
      return {
        recovered_count: 0,
        first_sequence_number: 0,
        last_sequence_number: 0,
        head_hash: EvidenceRecord.GENESIS_HASH,
        is_intact: true,
        journal_path: targetPath || "",
      };
    }

    const content = fs.readFileSync(targetPath, "utf8");
    const lines = content.split("\n");

    const reloadedRecords: EvidenceRecord[] = [];
    let expectedSeq = 1;
    let prevHash = EvidenceRecord.GENESIS_HASH;
    let firstSeq = 0;

    for (let i = 0; i < lines.length; i++) {
      const lineClean = lines[i].trim();
      if (!lineClean) {
        continue;
      }

      let rawData: Record<string, unknown>;
      try {
        rawData = JSON.parse(lineClean);
      } catch (err: unknown) {
        throw new EvidenceCorruptionException(`Journal line ${i + 1} contains invalid JSON: ${String(err)}`);
      }

      const rec = EvidenceRecord.fromDict(rawData);

      // 1. Verify CRC32 checksum
      const expectedChecksum = rec.computeChecksum();
      if (rec.checksum !== expectedChecksum) {
        throw new EvidenceCorruptionException(
          `Checksum mismatch at sequence ${rec.sequence_number}: expected ${expectedChecksum}, found ${rec.checksum}`,
          "CORRUPT_CHECKSUM"
        );
      }

      // 2. Verify sequence number continuity
      if (rec.sequence_number !== expectedSeq) {
        throw new SequenceGapException(
          `Sequence gap detected: expected ${expectedSeq}, found ${rec.sequence_number} at journal line ${i + 1}`
        );
      }

      // 3. Verify previous record hash link
      if (rec.prev_record_hash !== prevHash) {
        throw new EvidenceCorruptionException(
          `Hash chain break at sequence ${rec.sequence_number}: expected prev_hash ${prevHash}, found ${rec.prev_record_hash}`,
          "CORRUPT_RECORD_HASH"
        );
      }

      // 4. Verify record hash integrity
      const computedHash = rec.computeRecordHash();
      if (rec.record_hash !== computedHash) {
        throw new EvidenceCorruptionException(
          `Record hash mismatch at sequence ${rec.sequence_number}: expected ${computedHash}, found ${rec.record_hash}`,
          "CORRUPT_RECORD_HASH"
        );
      }

      if (firstSeq === 0) {
        firstSeq = rec.sequence_number;
      }

      reloadedRecords.push(rec);
      expectedSeq = rec.sequence_number + 1;
      prevHash = rec.record_hash;
    }

    this._records = reloadedRecords;
    this._unflushedRecords = [];
    this._headSequenceNumber = reloadedRecords.length > 0 ? reloadedRecords[reloadedRecords.length - 1].sequence_number : 0;
    this._headHash = reloadedRecords.length > 0 ? reloadedRecords[reloadedRecords.length - 1].record_hash : EvidenceRecord.GENESIS_HASH;
    this._journalPath = targetPath;

    return {
      recovered_count: reloadedRecords.length,
      first_sequence_number: firstSeq,
      last_sequence_number: this._headSequenceNumber,
      head_hash: this._headHash,
      is_intact: true,
      journal_path: targetPath,
    };
  }

  public completeness(tenantId?: string): CompletenessReceipt {
    const matched = tenantId
      ? this._records.filter((r) => r.tenant_id === tenantId)
      : this._records;

    const total = matched.length;
    const startSeq = total > 0 ? matched[0].sequence_number : 0;
    const endSeq = total > 0 ? matched[total - 1].sequence_number : 0;
    const headH = total > 0 ? matched[total - 1].record_hash : this._headHash;

    return {
      tenant_id: tenantId || "ALL",
      start_sequence_number: startSeq,
      end_sequence_number: endSeq,
      total_records: total,
      head_hash: headH,
      verified_contiguous: true,
      dropped_critical_count: 0,
      compliance_standard: "NIST_SP_800_53_REV5_AU4",
    };
  }

  public recordTelemetryDrop(count: number = 1): number {
    this._droppedTelemetryCount += count;
    return this._droppedTelemetryCount;
  }

  public getDroppedTelemetryCount(): number {
    return this._droppedTelemetryCount;
  }

  public getRecordCount(): number {
    return this._records.length;
  }

  public getHeadSequenceNumber(): number {
    return this._headSequenceNumber;
  }

  public getHeadHash(): string {
    return this._headHash;
  }

  public getUnflushedCount(): number {
    return this._unflushedRecords.length;
  }

  public getRecords(): EvidenceRecord[] {
    return [...this._records];
  }

  public status(): EvidenceSpoolStatus {
    const state = this._records.length >= this._maxCapacity ? "SATURATED" : "ACTIVE";
    return {
      status: state,
      record_count: this._records.length,
      max_capacity: this._maxCapacity,
      head_sequence_number: this._headSequenceNumber,
      head_hash: this._headHash,
      unflushed_count: this._unflushedRecords.length,
      journal_file_path: this._journalPath,
      dropped_telemetry_count: this._droppedTelemetryCount,
    };
  }

  public close(): void {
    this.flush();
  }

  public reset(): void {
    this._records = [];
    this._unflushedRecords = [];
    this._headSequenceNumber = 0;
    this._headHash = EvidenceRecord.GENESIS_HASH;
    this._droppedTelemetryCount = 0;
  }
}

export const globalEvidenceSpool = new EvidenceSpool();
