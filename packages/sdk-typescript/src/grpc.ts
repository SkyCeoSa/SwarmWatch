/**
 * SwarmWatch Ultra-High-Throughput gRPC Streaming Ingestion Gateway (Seam 33 / ADR 0044).
 * Node.js 22 type-stripping compliant (no TypeScript enums, no parameter properties).
 */

import { Buffer } from "node:buffer";
import type { SwarmSpan } from "./schema.ts";

/**
 * Canonical gRPC status codes.
 */
export const GrpcStatusCode = {
  OK: 0,
  CANCELLED: 1,
  UNKNOWN: 2,
  INVALID_ARGUMENT: 3,
  DEADLINE_EXCEEDED: 4,
  NOT_FOUND: 5,
  ALREADY_EXISTS: 6,
  PERMISSION_DENIED: 7,
  RESOURCE_EXHAUSTED: 8,
  FAILED_PRECONDITION: 9,
  ABORTED: 10,
  OUT_OF_RANGE: 11,
  UNIMPLEMENTED: 12,
  INTERNAL: 13,
  UNAVAILABLE: 14,
  DATA_LOSS: 15,
  UNAUTHENTICATED: 16,
} as const;

export type GrpcStatusCode = (typeof GrpcStatusCode)[keyof typeof GrpcStatusCode];

/**
 * Binary gRPC wire framer managing 5-byte prefixes.
 * [compression_flag: 1 byte][length: 4 bytes uint32 big-endian][payload: length bytes]
 */
export class GrpcMessageFramer {
  static readonly HEADER_SIZE = 5;

  /**
   * Encodes a payload into a 5-byte framed gRPC wire buffer.
   */
  static encodeFrame(payload: Buffer | Uint8Array | string, compressed: boolean = false): Buffer {
    const payloadBuf = typeof payload === "string"
      ? Buffer.from(payload, "utf-8")
      : Buffer.from(payload);

    const header = Buffer.alloc(this.HEADER_SIZE);
    header.writeUInt8(compressed ? 1 : 0, 0);
    header.writeUInt32BE(payloadBuf.length, 1);

    return Buffer.concat([header, payloadBuf]);
  }

  /**
   * Decodes all complete gRPC frames from an accumulated byte buffer.
   */
  static decodeFrames(buffer: Buffer): { frames: Buffer[]; remaining: Buffer } {
    const frames: Buffer[] = [];
    let offset = 0;
    const bufLen = buffer.length;

    while (offset + this.HEADER_SIZE <= bufLen) {
      const length = buffer.readUInt32BE(offset + 1);
      const frameEnd = offset + this.HEADER_SIZE + length;

      if (frameEnd <= bufLen) {
        const payload = buffer.subarray(offset + this.HEADER_SIZE, frameEnd);
        frames.push(Buffer.from(payload));
        offset = frameEnd;
      } else {
        break;
      }
    }

    const remaining = Buffer.from(buffer.subarray(offset));
    return { frames, remaining };
  }

  /**
   * Encodes a standard IngestSpansResponse into a framed gRPC message.
   */
  static encodeResponse(receivedCount: number, status: string = "ok", message: string = ""): Buffer {
    const jsonStr = JSON.stringify({ received_count: receivedCount, status, message });
    return this.encodeFrame(jsonStr);
  }
}

/**
 * Client for streaming telemetry spans to SwarmWatch gRPC gateway.
 */
export class GrpcStreamClient {
  serverUrl: string;
  projectId: string;

  constructor(options?: { serverUrl?: string; projectId?: string }) {
    this.serverUrl = options?.serverUrl ?? "http://127.0.0.1:8765";
    this.projectId = options?.projectId ?? "default-project";
  }

  /**
   * Streams a batch of spans encoded as framed gRPC messages.
   */
  async streamSpans(spans: SwarmSpan[]): Promise<{ status: GrpcStatusCode; error?: string }> {
    const payload = JSON.stringify(spans);
    const framed = GrpcMessageFramer.encodeFrame(payload);

    const url = `${this.serverUrl.replace(/\/$/, "")}/swarmwatch.v1.TelemetryService/StreamSpans`;
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/grpc",
          "x-swarmwatch-project-id": this.projectId,
        },
        body: framed as any,
      });

      const grpcStatusHeader = resp.headers.get("grpc-status");
      const statusCode = grpcStatusHeader !== null ? parseInt(grpcStatusHeader, 10) : GrpcStatusCode.OK;
      const grpcMessage = resp.headers.get("grpc-message") ?? undefined;

      return {
        status: (statusCode as GrpcStatusCode) || GrpcStatusCode.OK,
        error: grpcMessage,
      };
    } catch (e: any) {
      return {
        status: GrpcStatusCode.INTERNAL,
        error: e?.message ?? String(e),
      };
    }
  }
}
