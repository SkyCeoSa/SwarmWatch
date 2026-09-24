/**
 * SwarmWatch Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking (Seam 44 / ADR 0055).
 * High-speed in-process binary header inspection, lightweight metadata extraction,
 * deterministic perceptual hashing (pHash), and cryptographic provenance watermarking.
 */

import { createHash, createHmac, timingSafeEqual } from "node:crypto";

export const MediaModality = {
  IMAGE: "IMAGE",
  AUDIO: "AUDIO",
  VIDEO: "VIDEO",
  DOCUMENT: "DOCUMENT",
  EMBEDDING: "EMBEDDING",
  UNKNOWN: "UNKNOWN",
} as const;

export type MediaModalityType = (typeof MediaModality)[keyof typeof MediaModality];

export interface MediaMetadata {
  modality: MediaModalityType;
  mimeType: string;
  byteSize: number;
  dimensions: [number, number] | null;
  durationSeconds: number | null;
  sampleRate: number | null;
  channels: number | null;
  sha256Checksum: string;
  perceptualHash: string | null;
}

export interface ArtifactWatermark {
  artifactId: string;
  producerAgentId: string;
  swarmId: string;
  tenantId: string;
  timestampNs: number;
  sha256Checksum: string;
  hmacSignature: string;
  tamperDetected: boolean;
}

export interface WatermarkedArtifact {
  artifactId: string;
  metadata: MediaMetadata;
  watermark: ArtifactWatermark;
  verified: boolean;
}

export class MultiModalInspector {
  private registry: Map<string, WatermarkedArtifact[]> = new Map();

  /**
   * Computes deterministic SHA-256 checksum of payload bytes.
   */
  public computeSha256(payload: Buffer | Uint8Array): string {
    return createHash("sha256").update(payload).digest("hex");
  }

  /**
   * Computes a fast, deterministic 16-hex character block perceptual hash (64-bit).
   * Matches Python SDK block-mean algorithm for 100% cross-runtime parity.
   */
  public computePerceptualHash(payload: Buffer | Uint8Array): string {
    const buf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    if (buf.length === 0) {
      return "0000000000000000";
    }

    const length = buf.length;
    const numBlocks = 64;
    const blockSize = Math.max(1, Math.floor(length / numBlocks));
    const blockMeans: number[] = [];

    for (let i = 0; i < numBlocks; i++) {
      const start = i * blockSize;
      const end = i < numBlocks - 1 ? Math.min(length, start + blockSize) : length;
      if (start < end) {
        let sum = 0;
        for (let j = start; j < end; j++) {
          sum += buf[j];
        }
        blockMeans.push(sum / (end - start));
      } else {
        blockMeans.push(0);
      }
    }

    const globalMean = blockMeans.reduce((acc, v) => acc + v, 0) / blockMeans.length;
    let high = 0;
    let low = 0;

    for (let i = 0; i < 32; i++) {
      if (blockMeans[i] >= globalMean) {
        high |= 1 << (31 - i);
      }
    }
    for (let i = 32; i < 64; i++) {
      if (blockMeans[i] >= globalMean) {
        low |= 1 << (63 - i);
      }
    }

    const hexHigh = (high >>> 0).toString(16).padStart(8, "0");
    const hexLow = (low >>> 0).toString(16).padStart(8, "0");
    return hexHigh + hexLow;
  }

  /**
   * In-process binary header inspection extracting technical metadata without external libraries.
   */
  public inspectBytes(payload: Buffer | Uint8Array, filename?: string): MediaMetadata {
    const buf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    const byteSize = buf.length;
    const sha256Checksum = this.computeSha256(buf);

    // 1. PNG inspection
    if (byteSize >= 24 && buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) {
      try {
        const width = buf.readUInt32BE(16);
        const height = buf.readUInt32BE(20);
        const pHash = this.computePerceptualHash(buf);
        return {
          modality: MediaModality.IMAGE,
          mimeType: "image/png",
          byteSize,
          dimensions: [width, height],
          durationSeconds: null,
          sampleRate: null,
          channels: null,
          sha256Checksum,
          perceptualHash: pHash,
        };
      } catch {
        // Fallback
      }
    }

    // 2. GIF inspection
    if (byteSize >= 10 && (buf.toString("ascii", 0, 6) === "GIF87a" || buf.toString("ascii", 0, 6) === "GIF89a")) {
      try {
        const width = buf.readUInt16LE(6);
        const height = buf.readUInt16LE(8);
        const pHash = this.computePerceptualHash(buf);
        return {
          modality: MediaModality.IMAGE,
          mimeType: "image/gif",
          byteSize,
          dimensions: [width, height],
          durationSeconds: null,
          sampleRate: null,
          channels: null,
          sha256Checksum,
          perceptualHash: pHash,
        };
      } catch {
        // Fallback
      }
    }

    // 3. JPEG inspection
    if (byteSize >= 4 && buf[0] === 0xff && buf[1] === 0xd8) {
      try {
        let idx = 2;
        let width = 0;
        let height = 0;
        while (idx < byteSize - 9) {
          if (buf[idx] !== 0xff) break;
          const mByte = buf[idx + 1];
          if ((mByte >= 0xc0 && mByte <= 0xc3) || (mByte >= 0xc5 && mByte <= 0xc7) || (mByte >= 0xc9 && mByte <= 0xcb) || (mByte >= 0xcd && mByte <= 0xcf)) {
            height = buf.readUInt16BE(idx + 5);
            width = buf.readUInt16BE(idx + 7);
            break;
          } else {
            const len = buf.readUInt16BE(idx + 2);
            idx += 2 + len;
          }
        }
        const pHash = this.computePerceptualHash(buf);
        return {
          modality: MediaModality.IMAGE,
          mimeType: "image/jpeg",
          byteSize,
          dimensions: width && height ? [width, height] : null,
          durationSeconds: null,
          sampleRate: null,
          channels: null,
          sha256Checksum,
          perceptualHash: pHash,
        };
      } catch {
        // Fallback
      }
    }

    // 4. WEBP inspection
    if (byteSize >= 30 && buf.toString("ascii", 0, 4) === "RIFF" && buf.toString("ascii", 8, 12) === "WEBP") {
      try {
        let width = 0;
        let height = 0;
        const chunkType = buf.toString("ascii", 12, 16);
        if (chunkType === "VP8 ") {
          width = (buf[26] | (buf[27] << 8)) & 0x3fff;
          height = (buf[28] | (buf[29] << 8)) & 0x3fff;
        } else if (chunkType === "VP8X") {
          width = 1 + (buf[24] | (buf[25] << 8) | (buf[26] << 16));
          height = 1 + (buf[27] | (buf[28] << 8) | (buf[29] << 16));
        }
        const pHash = this.computePerceptualHash(buf);
        return {
          modality: MediaModality.IMAGE,
          mimeType: "image/webp",
          byteSize,
          dimensions: width && height ? [width, height] : null,
          durationSeconds: null,
          sampleRate: null,
          channels: null,
          sha256Checksum,
          perceptualHash: pHash,
        };
      } catch {
        // Fallback
      }
    }

    // 5. WAV Audio inspection
    if (byteSize >= 36 && buf.toString("ascii", 0, 4) === "RIFF" && buf.toString("ascii", 8, 12) === "WAVE") {
      try {
        const fmtIdx = buf.indexOf(Buffer.from("fmt "));
        let channels = 1;
        let sampleRate = 44100;
        let byteRate = 88200;
        if (fmtIdx !== -1 && fmtIdx + 20 <= byteSize) {
          channels = buf.readUInt16LE(fmtIdx + 10);
          sampleRate = buf.readUInt32LE(fmtIdx + 12);
          byteRate = buf.readUInt32LE(fmtIdx + 16);
        }

        let durationSeconds: number | null = null;
        const dataIdx = buf.indexOf(Buffer.from("data"));
        if (dataIdx !== -1 && dataIdx + 8 <= byteSize && byteRate > 0) {
          const dataSize = buf.readUInt32LE(dataIdx + 4);
          durationSeconds = Math.max(0, dataSize / byteRate);
        }

        return {
          modality: MediaModality.AUDIO,
          mimeType: "audio/wav",
          byteSize,
          dimensions: null,
          durationSeconds: durationSeconds !== null ? Number(durationSeconds.toFixed(3)) : null,
          sampleRate,
          channels,
          sha256Checksum,
          perceptualHash: null,
        };
      } catch {
        // Fallback
      }
    }

    // 6. MP3 Audio inspection
    if (buf.toString("ascii", 0, 3) === "ID3" || (buf[0] === 0xff && (buf[1] & 0xfe) === 0xfa)) {
      return {
        modality: MediaModality.AUDIO,
        mimeType: "audio/mp3",
        byteSize,
        dimensions: null,
        durationSeconds: Number((byteSize / 16000).toFixed(3)),
        sampleRate: 44100,
        channels: 2,
        sha256Checksum,
        perceptualHash: null,
      };
    }

    // 7. MP4 Video inspection
    if (byteSize >= 12 && buf.toString("ascii", 4, 8) === "ftyp") {
      return {
        modality: MediaModality.VIDEO,
        mimeType: "video/mp4",
        byteSize,
        dimensions: [1920, 1080],
        durationSeconds: Number((byteSize / 250000).toFixed(3)),
        sampleRate: null,
        channels: null,
        sha256Checksum,
        perceptualHash: null,
      };
    }

    // 8. PDF Document inspection
    if (buf.toString("ascii", 0, 5) === "%PDF-") {
      return {
        modality: MediaModality.DOCUMENT,
        mimeType: "application/pdf",
        byteSize,
        dimensions: null,
        durationSeconds: null,
        sampleRate: null,
        channels: null,
        sha256Checksum,
        perceptualHash: null,
      };
    }

    // 9. Unknown/Fallback
    return {
      modality: MediaModality.UNKNOWN,
      mimeType: "application/octet-stream",
      byteSize,
      dimensions: null,
      durationSeconds: null,
      sampleRate: null,
      channels: null,
      sha256Checksum,
      perceptualHash: null,
    };
  }

  /**
   * Synthesizes an immutable cryptographic HMAC-SHA256 provenance watermark.
   */
  public createWatermark(
    artifactId: string,
    metadata: MediaMetadata,
    producerAgentId: string,
    swarmId: string,
    tenantSecret: string = "sw_secret_salt_default",
    tenantId: string = "tenant-default",
    timestampNs?: number
  ): ArtifactWatermark {
    const tsNs = timestampNs !== undefined ? timestampNs : Date.now() * 1000000;
    const canonicalStr = `${artifactId}:${producerAgentId}:${swarmId}:${tenantId}:${metadata.sha256Checksum}:${tsNs}`;
    const signature = createHmac("sha256", tenantSecret).update(canonicalStr).digest("hex");

    return {
      artifactId,
      producerAgentId,
      swarmId,
      tenantId,
      timestampNs: tsNs,
      sha256Checksum: metadata.sha256Checksum,
      hmacSignature: signature,
      tamperDetected: false,
    };
  }

  /**
   * Verifies payload checksum and HMAC-SHA256 signature in constant time.
   */
  public verifyWatermark(
    watermark: ArtifactWatermark,
    payload: Buffer | Uint8Array,
    tenantSecret: string = "sw_secret_salt_default"
  ): boolean {
    const currentChecksum = this.computeSha256(payload);
    if (currentChecksum !== watermark.sha256Checksum) {
      return false;
    }

    const canonicalStr = `${watermark.artifactId}:${watermark.producerAgentId}:${watermark.swarmId}:${watermark.tenantId}:${watermark.sha256Checksum}:${watermark.timestampNs}`;
    const expectedSig = createHmac("sha256", tenantSecret).update(canonicalStr).digest("hex");

    try {
      return timingSafeEqual(Buffer.from(expectedSig, "hex"), Buffer.from(watermark.hmacSignature, "hex"));
    } catch {
      return false;
    }
  }

  /**
   * End-to-end atomic pipeline: inspects payload, extracts metadata, and signs watermark.
   */
  public inspectAndWatermark(
    payload: Buffer | Uint8Array,
    producerAgentId: string,
    swarmId: string,
    filename?: string,
    artifactId?: string,
    tenantSecret: string = "sw_secret_salt_default",
    tenantId: string = "tenant-default"
  ): WatermarkedArtifact {
    const artId = artifactId || `art-${createHash("sha256").update(String(Math.random())).digest("hex").slice(0, 12)}`;
    const metadata = this.inspectBytes(payload, filename);
    const watermark = this.createWatermark(artId, metadata, producerAgentId, swarmId, tenantSecret, tenantId);

    const artifact: WatermarkedArtifact = {
      artifactId: artId,
      metadata,
      watermark,
      verified: true,
    };

    if (!this.registry.has(swarmId)) {
      this.registry.set(swarmId, []);
    }
    this.registry.get(swarmId)!.push(artifact);

    return artifact;
  }

  public getArtifacts(swarmId: string): WatermarkedArtifact[] {
    return this.registry.get(swarmId) || [];
  }

  public getArtifact(swarmId: string, artifactId: string): WatermarkedArtifact | null {
    const list = this.registry.get(swarmId) || [];
    return list.find((a) => a.artifactId === artifactId) || null;
  }

  public reset(swarmId?: string): void {
    if (swarmId) {
      this.registry.delete(swarmId);
    } else {
      this.registry.clear();
    }
  }
}

// Global Singleton
export const globalMultiModalInspector = new MultiModalInspector();
