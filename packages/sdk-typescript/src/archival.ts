/**
 * SwarmWatch S3 / Google Cloud Storage Streaming Log Archival & Cold Storage Tier (Seam 25 / ADR 0036).
 * TypeScript SDK Parity Edition.
 */

import * as crypto from "node:crypto";
import * as zlib from "node:zlib";

export interface ObjectStorageClient {
  uploadObject(
    bucket: string,
    key: string,
    data: Buffer | Uint8Array,
    contentType?: string,
    metadata?: Record<string, string>
  ): Promise<void>;

  downloadObject(bucket: string, key: string): Promise<Buffer>;

  listObjects(bucket: string, prefix?: string): Promise<string[]>;

  deleteObject(bucket: string, key: string): Promise<boolean>;

  headObject(
    bucket: string,
    key: string
  ): Promise<{ contentType: string; sizeBytes: number; sha256?: string } | null>;
}

export class MockObjectStorageClient implements ObjectStorageClient {
  private _buckets: Map<string, Map<string, Buffer>> = new Map();
  private _metadata: Map<string, Map<string, any>> = new Map();

  async uploadObject(
    bucket: string,
    key: string,
    data: Buffer | Uint8Array,
    contentType: string = "application/octet-stream",
    metadata?: Record<string, string>
  ): Promise<void> {
    if (!this._buckets.has(bucket)) {
      this._buckets.set(bucket, new Map());
      this._metadata.set(bucket, new Map());
    }

    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    this._buckets.get(bucket)!.set(key, buf);

    const hash = crypto.createHash("sha256").update(buf).digest("hex");
    this._metadata.get(bucket)!.set(key, {
      contentType,
      sizeBytes: buf.length,
      sha256: hash,
      createdAt: new Date().toISOString(),
      customMetadata: { ...metadata },
    });
  }

  async downloadObject(bucket: string, key: string): Promise<Buffer> {
    const b = this._buckets.get(bucket);
    if (!b || !b.has(key)) {
      throw new Error(`Object '${key}' not found in bucket '${bucket}'`);
    }
    return b.get(key)!;
  }

  async listObjects(bucket: string, prefix: string = ""): Promise<string[]> {
    const b = this._buckets.get(bucket);
    if (!b) return [];
    return Array.from(b.keys())
      .filter((k) => k.startsWith(prefix))
      .sort();
  }

  async deleteObject(bucket: string, key: string): Promise<boolean> {
    const b = this._buckets.get(bucket);
    if (!b || !b.has(key)) return false;
    b.delete(key);
    this._metadata.get(bucket)?.delete(key);
    return true;
  }

  async headObject(
    bucket: string,
    key: string
  ): Promise<{ contentType: string; sizeBytes: number; sha256?: string } | null> {
    const meta = this._metadata.get(bucket)?.get(key);
    if (!meta) return null;
    return {
      contentType: meta.contentType,
      sizeBytes: meta.sizeBytes,
      sha256: meta.sha256,
    };
  }
}

export interface ArchiveManifest {
  swarm_id: string;
  project_id: string;
  organization_id: string;
  spans_count: number;
  snapshots_count: number;
  circuit_events_count: number;
  total_tokens: number;
  total_cost: number;
  compressed_size_bytes: number;
  uncompressed_size_bytes: number;
  compression_ratio: number;
  sha256_checksum: string;
  archived_at: string;
  archived_by: string;
  version: string;
}

export interface ArchiveRecord {
  swarm_id: string;
  archive_key: string;
  manifest_key: string;
  bucket: string;
  compressed_size_bytes: number;
  uncompressed_size_bytes: number;
  compression_ratio: number;
  spans_count: number;
  snapshots_count: number;
  archived_at: string;
  sha256_checksum: string;
}

export interface RestoredSwarmSummary {
  swarm_id: string;
  spans_restored: number;
  snapshots_restored: number;
  sha256_verified: boolean;
  manifest: ArchiveManifest;
}

export class ColdStorageArchiver {
  public readonly client: ObjectStorageClient;
  public readonly defaultBucket: string;

  constructor(options?: { client?: ObjectStorageClient; defaultBucket?: string }) {
    this.client = options?.client || new MockObjectStorageClient();
    this.defaultBucket = options?.defaultBucket || "swarmwatch-cold-storage";
  }

  private _buildPartitionPath(
    swarmId: string,
    projectId: string,
    orgId: string,
    now: Date = new Date()
  ): string {
    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, "0");
    const day = String(now.getUTCDate()).padStart(2, "0");

    return `swarmwatch/v1/org=${orgId}/proj=${projectId}/year=${year}/month=${month}/day=${day}/swarm=${swarmId}`;
  }

  async archiveSwarm(
    swarmId: string,
    options?: {
      bucket?: string;
      projectId?: string;
      orgId?: string;
      spans?: any[];
      snapshots?: any[];
    }
  ): Promise<ArchiveRecord> {
    const bucket = options?.bucket || this.defaultBucket;
    const proj = options?.projectId || "default-project";
    const org = options?.orgId || "default-org";
    const spans = options?.spans || [];
    const snapshots = options?.snapshots || [];

    // Serialize to JSON Lines
    const lines: string[] = [];
    let totalTokens = 0;

    for (const span of spans) {
      totalTokens += span.token_count || span.tokens || 0;
      lines.push(`SPAN:${JSON.stringify(span)}`);
    }

    for (const snap of snapshots) {
      lines.push(`SNAPSHOT:${JSON.stringify(snap)}`);
    }

    const uncompressedText = lines.join("\n") + "\n";
    const uncompressedBuffer = Buffer.from(uncompressedText, "utf-8");
    const uncompressedSize = uncompressedBuffer.length;

    // Gzip compression
    const compressedBytes = zlib.gzipSync(uncompressedBuffer, { level: 9 });
    const compressedSize = compressedBytes.length;
    const compressionRatio = Number(
      (uncompressedSize / Math.max(compressedSize, 1)).toFixed(2)
    );
    const checksum = crypto.createHash("sha256").update(compressedBytes).digest("hex");

    const basePath = this._buildPartitionPath(swarmId, proj, org);
    const archiveKey = `${basePath}.jsonl.gz`;
    const manifestKey = `${basePath}.manifest.json`;

    const manifest: ArchiveManifest = {
      swarm_id: swarmId,
      project_id: proj,
      organization_id: org,
      spans_count: spans.length,
      snapshots_count: snapshots.length,
      circuit_events_count: 0,
      total_tokens: totalTokens,
      total_cost: 0.0,
      compressed_size_bytes: compressedSize,
      uncompressed_size_bytes: uncompressedSize,
      compression_ratio: compressionRatio,
      sha256_checksum: checksum,
      archived_at: new Date().toISOString(),
      archived_by: "swarmwatch-archiver-ts",
      version: "1.0",
    };

    await this.client.uploadObject(bucket, archiveKey, compressedBytes, "application/gzip", {
      swarm_id: swarmId,
      sha256: checksum,
    });

    await this.client.uploadObject(
      bucket,
      manifestKey,
      Buffer.from(JSON.stringify(manifest, null, 2), "utf-8"),
      "application/json",
      { swarm_id: swarmId }
    );

    return {
      swarm_id: swarmId,
      archive_key: archiveKey,
      manifest_key: manifestKey,
      bucket,
      compressed_size_bytes: compressedSize,
      uncompressed_size_bytes: uncompressedSize,
      compression_ratio: compressionRatio,
      spans_count: spans.length,
      snapshots_count: snapshots.length,
      archived_at: manifest.archived_at,
      sha256_checksum: checksum,
    };
  }

  async restoreSwarm(
    swarmId: string,
    options?: {
      bucket?: string;
      onRestoreSpans?: (spans: any[]) => Promise<void>;
      onRestoreSnapshots?: (snapshots: any[]) => Promise<void>;
    }
  ): Promise<RestoredSwarmSummary> {
    const bucket = options?.bucket || this.defaultBucket;
    const allKeys = await this.client.listObjects(bucket, "swarmwatch/v1/");

    const manifestKey = allKeys.find((k) => k.endsWith(`swarm=${swarmId}.manifest.json`));
    const archiveKey = allKeys.find((k) => k.endsWith(`swarm=${swarmId}.jsonl.gz`));

    if (!manifestKey || !archiveKey) {
      throw new Error(`Archive bundle for swarm '${swarmId}' not found in bucket '${bucket}'`);
    }

    const manifestRaw = await this.client.downloadObject(bucket, manifestKey);
    const manifest: ArchiveManifest = JSON.parse(manifestRaw.toString("utf-8"));

    const compressedBytes = await this.client.downloadObject(bucket, archiveKey);
    const computedHash = crypto.createHash("sha256").update(compressedBytes).digest("hex");

    if (computedHash !== manifest.sha256_checksum) {
      throw new Error(
        `Cryptographic integrity breach: SHA-256 checksum mismatch for swarm '${swarmId}'! ` +
          `Expected: ${manifest.sha256_checksum}, Computed: ${computedHash}`
      );
    }

    const decompressed = zlib.gunzipSync(compressedBytes).toString("utf-8");
    const restoredSpans: any[] = [];
    const restoredSnapshots: any[] = [];

    for (const line of decompressed.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      if (trimmed.startsWith("SPAN:")) {
        restoredSpans.push(JSON.parse(trimmed.slice(5)));
      } else if (trimmed.startsWith("SNAPSHOT:")) {
        restoredSnapshots.push(JSON.parse(trimmed.slice(9)));
      }
    }

    if (options?.onRestoreSpans && restoredSpans.length > 0) {
      await options.onRestoreSpans(restoredSpans);
    }
    if (options?.onRestoreSnapshots && restoredSnapshots.length > 0) {
      await options.onRestoreSnapshots(restoredSnapshots);
    }

    return {
      swarm_id: swarmId,
      spans_restored: restoredSpans.length,
      snapshots_restored: restoredSnapshots.length,
      sha256_verified: true,
      manifest,
    };
  }

  async listArchives(options?: {
    bucket?: string;
    projectId?: string;
    prefix?: string;
  }): Promise<ArchiveRecord[]> {
    const bucket = options?.bucket || this.defaultBucket;
    const prefix = options?.prefix || "swarmwatch/v1/";

    const allKeys = await this.client.listObjects(bucket, prefix);
    const manifestKeys = allKeys.filter((k) => k.endsWith(".manifest.json"));

    const records: ArchiveRecord[] = [];
    for (const mk of manifestKeys) {
      try {
        const raw = await this.client.downloadObject(bucket, mk);
        const manifest: ArchiveManifest = JSON.parse(raw.toString("utf-8"));
        if (options?.projectId && manifest.project_id !== options.projectId) {
          continue;
        }

        const archiveKey = mk.replace(".manifest.json", ".jsonl.gz");
        records.push({
          swarm_id: manifest.swarm_id,
          archive_key: archiveKey,
          manifest_key: mk,
          bucket,
          compressed_size_bytes: manifest.compressed_size_bytes,
          uncompressed_size_bytes: manifest.uncompressed_size_bytes,
          compression_ratio: manifest.compression_ratio,
          spans_count: manifest.spans_count,
          snapshots_count: manifest.snapshots_count,
          archived_at: manifest.archived_at,
          sha256_checksum: manifest.sha256_checksum,
        });
      } catch {}
    }

    return records.sort((a, b) => b.archived_at.localeCompare(a.archived_at));
  }

  async getArchiveManifest(swarmId: string, bucket?: string): Promise<ArchiveManifest | null> {
    const b = bucket || this.defaultBucket;
    const allKeys = await this.client.listObjects(b, "swarmwatch/v1/");
    const manifestKey = allKeys.find((k) => k.endsWith(`swarm=${swarmId}.manifest.json`));
    if (!manifestKey) return null;

    const raw = await this.client.downloadObject(b, manifestKey);
    return JSON.parse(raw.toString("utf-8"));
  }

  async verifyArchiveIntegrity(swarmId: string, bucket?: string): Promise<boolean> {
    const b = bucket || this.defaultBucket;
    const manifest = await this.getArchiveManifest(swarmId, b);
    if (!manifest) return false;

    const allKeys = await this.client.listObjects(b, "swarmwatch/v1/");
    const archiveKey = allKeys.find((k) => k.endsWith(`swarm=${swarmId}.jsonl.gz`));
    if (!archiveKey) return false;

    const compressed = await this.client.downloadObject(b, archiveKey);
    const hash = crypto.createHash("sha256").update(compressed).digest("hex");
    return hash === manifest.sha256_checksum;
  }
}
