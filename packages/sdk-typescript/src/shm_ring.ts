/**
 * SwarmWatch Zero-Copy Shared-Memory Ring Buffer IPC Gateway (TypeScript Parity).
 * Provides high-throughput cross-process telemetry ingestion using binary framing (ADR 0048).
 *
 * Conforms strictly to Node 22 type-stripping rules: no TS enums, no parameter properties.
 */

export const SHM_MAGIC = "SWRB";
export const SHM_HEADER_SIZE = 64;
export const SHM_SLOT_HEADER_SIZE = 5;

export const ShmSlotStatus = {
  EMPTY: 0,
  WRITING: 1,
  READY: 2,
  READING: 3,
} as const;

export type ShmSlotStatusType = typeof ShmSlotStatus[keyof typeof ShmSlotStatus];

export interface ShmRingHeader {
  magic: string;
  version: number;
  flags: number;
  capacitySlots: number;
  slotSize: number;
  writeHead: number;
  readTail: number;
  droppedRecords: number;
  totalWritten: number;
  totalRead: number;
}

export interface ShmRingStatus {
  name: string;
  version: number;
  capacitySlots: number;
  slotSize: number;
  totalBytes: number;
  writeHead: number;
  readTail: number;
  pendingRecords: number;
  droppedRecords: number;
  totalWritten: number;
  totalRead: number;
  utilizationPct: number;
}

export class ShmRingBuffer {
  public name: string;
  public capacitySlots: number;
  public slotSize: number;
  public totalSize: number;
  public buffer: Buffer;

  constructor(
    name: string = "swarmwatch_ring_01",
    capacitySlots: number = 1024,
    slotSize: number = 2048,
    existingBuffer: Buffer | null = null
  ) {
    this.name = name;
    this.capacitySlots = capacitySlots;
    this.slotSize = slotSize;
    this.totalSize = SHM_HEADER_SIZE + capacitySlots * slotSize;

    if (existingBuffer && existingBuffer.length >= this.totalSize) {
      this.buffer = existingBuffer;
    } else {
      this.buffer = Buffer.alloc(this.totalSize);
      this.initHeader();
    }
  }

  public initHeader(): void {
    this.buffer.write(SHM_MAGIC, 0, 4, "latin1");
    this.buffer.writeUInt16LE(1, 4); // version
    this.buffer.writeUInt16LE(0, 6); // flags
    this.buffer.writeUInt32LE(this.capacitySlots, 8);
    this.buffer.writeUInt32LE(this.slotSize, 12);
    this.buffer.writeBigUInt64LE(0n, 16); // writeHead
    this.buffer.writeBigUInt64LE(0n, 24); // readTail
    this.buffer.writeBigUInt64LE(0n, 32); // droppedRecords
    this.buffer.writeBigUInt64LE(0n, 40); // totalWritten
    this.buffer.writeBigUInt64LE(0n, 48); // totalRead
    this.buffer.fill(0, 56, 64); // reserved
  }

  public readHeader(): ShmRingHeader {
    const magic = this.buffer.toString("latin1", 0, 4);
    const version = this.buffer.readUInt16LE(4);
    const flags = this.buffer.readUInt16LE(6);
    const capacitySlots = this.buffer.readUInt32LE(8);
    const slotSize = this.buffer.readUInt32LE(12);
    const writeHead = Number(this.buffer.readBigUInt64LE(16));
    const readTail = Number(this.buffer.readBigUInt64LE(24));
    const droppedRecords = Number(this.buffer.readBigUInt64LE(32));
    const totalWritten = Number(this.buffer.readBigUInt64LE(40));
    const totalRead = Number(this.buffer.readBigUInt64LE(48));

    return {
      magic,
      version,
      flags,
      capacitySlots,
      slotSize,
      writeHead,
      readTail,
      droppedRecords,
      totalWritten,
      totalRead,
    };
  }

  public writeHeaderFields(fields: {
    writeHead?: number;
    readTail?: number;
    droppedRecords?: number;
    totalWritten?: number;
    totalRead?: number;
  }): void {
    if (fields.writeHead !== undefined) {
      this.buffer.writeBigUInt64LE(BigInt(fields.writeHead), 16);
    }
    if (fields.readTail !== undefined) {
      this.buffer.writeBigUInt64LE(BigInt(fields.readTail), 24);
    }
    if (fields.droppedRecords !== undefined) {
      this.buffer.writeBigUInt64LE(BigInt(fields.droppedRecords), 32);
    }
    if (fields.totalWritten !== undefined) {
      this.buffer.writeBigUInt64LE(BigInt(fields.totalWritten), 40);
    }
    if (fields.totalRead !== undefined) {
      this.buffer.writeBigUInt64LE(BigInt(fields.totalRead), 48);
    }
  }

  public getSlotOffset(slotIndex: number): number {
    const idx = slotIndex % this.capacitySlots;
    return SHM_HEADER_SIZE + idx * this.slotSize;
  }
}

export class ShmRingBufferWriter {
  public ring: ShmRingBuffer;

  constructor(ringBuffer: ShmRingBuffer) {
    this.ring = ringBuffer;
  }

  public writeSpan(spanPayload: Record<string, any> | string): boolean {
    const rawString = typeof spanPayload === "string" ? spanPayload : JSON.stringify(spanPayload);
    const payloadBuffer = Buffer.from(rawString, "utf8");

    const maxPayload = this.ring.slotSize - SHM_SLOT_HEADER_SIZE;
    const finalPayload = payloadBuffer.length > maxPayload ? payloadBuffer.subarray(0, maxPayload) : payloadBuffer;

    const hdr = this.ring.readHeader();
    if (hdr.writeHead - hdr.readTail >= hdr.capacitySlots) {
      // Saturated: drop span and increment counter
      this.ring.writeHeaderFields({ droppedRecords: hdr.droppedRecords + 1 });
      return false;
    }

    const slotOffset = this.ring.getSlotOffset(hdr.writeHead);

    // Write WRITING status and length
    this.ring.buffer.writeUInt8(ShmSlotStatus.WRITING, slotOffset);
    this.ring.buffer.writeUInt32LE(finalPayload.length, slotOffset + 1);

    // Copy bytes
    finalPayload.copy(this.ring.buffer, slotOffset + SHM_SLOT_HEADER_SIZE);

    // Mark READY
    this.ring.buffer.writeUInt8(ShmSlotStatus.READY, slotOffset);

    // Advance head
    this.ring.writeHeaderFields({
      writeHead: hdr.writeHead + 1,
      totalWritten: hdr.totalWritten + 1,
    });

    return true;
  }
}

export class ShmRingBufferReader {
  public ring: ShmRingBuffer;

  constructor(ringBuffer: ShmRingBuffer) {
    this.ring = ringBuffer;
  }

  public readAvailable(maxRecords: number = 256): Array<Record<string, any>> {
    const records: Array<Record<string, any>> = [];
    const hdr = this.ring.readHeader();
    let currentTail = hdr.readTail;
    const head = hdr.writeHead;

    let count = 0;
    while (currentTail < head && count < maxRecords) {
      const slotOffset = this.ring.getSlotOffset(currentTail);
      const status = this.ring.buffer.readUInt8(slotOffset);
      const payloadLen = this.ring.buffer.readUInt32LE(slotOffset + 1);

      if (status !== ShmSlotStatus.READY) {
        break;
      }

      this.ring.buffer.writeUInt8(ShmSlotStatus.READING, slotOffset);
      const payloadBytes = this.ring.buffer.subarray(
        slotOffset + SHM_SLOT_HEADER_SIZE,
        slotOffset + SHM_SLOT_HEADER_SIZE + payloadLen
      );

      try {
        const parsed = JSON.parse(payloadBytes.toString("utf8"));
        records.push(parsed);
      } catch {
        records.push({ raw_payload: payloadBytes.toString("utf8") });
      }

      // Mark EMPTY
      this.ring.buffer.writeUInt8(ShmSlotStatus.EMPTY, slotOffset);
      this.ring.buffer.writeUInt32LE(0, slotOffset + 1);

      currentTail++;
      count++;
    }

    if (count > 0) {
      this.ring.writeHeaderFields({
        readTail: currentTail,
        totalRead: hdr.totalRead + count,
      });
    }

    return records;
  }
}

export class ShmIpcGateway {
  public name: string;
  public capacitySlots: number;
  public slotSize: number;
  public ring: ShmRingBuffer;
  public writer: ShmRingBufferWriter;
  public reader: ShmRingBufferReader;
  private ingestedSpans: Array<Record<string, any>>;

  constructor(name: string = "swarmwatch_ring_01", capacitySlots: number = 1024, slotSize: number = 2048) {
    this.name = name;
    this.capacitySlots = capacitySlots;
    this.slotSize = slotSize;
    this.ring = new ShmRingBuffer(this.name, this.capacitySlots, this.slotSize);
    this.writer = new ShmRingBufferWriter(this.ring);
    this.reader = new ShmRingBufferReader(this.ring);
    this.ingestedSpans = [];
  }

  public writeSpan(span: Record<string, any> | string): boolean {
    return this.writer.writeSpan(span);
  }

  public flush(): Array<Record<string, any>> {
    const spans = this.reader.readAvailable(512);
    if (spans.length > 0) {
      this.ingestedSpans.push(...spans);
    }
    return spans;
  }

  public getStatus(): ShmRingStatus {
    const hdr = this.ring.readHeader();
    const pending = Math.max(0, hdr.writeHead - hdr.readTail);
    const utilizationPct = Math.round((pending / Math.max(1, hdr.capacitySlots)) * 10000) / 100;

    return {
      name: this.name,
      version: hdr.version,
      capacitySlots: hdr.capacitySlots,
      slotSize: hdr.slotSize,
      totalBytes: this.ring.totalSize,
      writeHead: hdr.writeHead,
      readTail: hdr.readTail,
      pendingRecords: pending,
      droppedRecords: hdr.droppedRecords,
      totalWritten: hdr.totalWritten,
      totalRead: hdr.totalRead,
      utilizationPct,
    };
  }

  public reset(): void {
    this.ring.writeHeaderFields({
      writeHead: 0,
      readTail: 0,
      droppedRecords: 0,
      totalWritten: 0,
      totalRead: 0,
    });
    this.ingestedSpans = [];
  }
}

export const globalShmGateway = new ShmIpcGateway();
