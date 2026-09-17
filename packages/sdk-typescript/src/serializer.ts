/**
 * SwarmWatch Safe State Serializer & Hash for Time-Machine (TypeScript Edition).
 * Sanitizes state objects, masks credentials/keys, breaks circular references,
 * and generates deterministic SHA-256 hashes for state verification.
 */

import * as crypto from "node:crypto";
import type { StateSnapshot } from "./schema.ts";
import { SensitiveDataMasker, getDefaultMasker } from "./security.ts";

export interface SerializerConfig {
  max_string_length?: number;
  max_depth?: number;
  mask_keys?: string[];
  masker?: SensitiveDataMasker;
}

export class SafeStateSerializer {
  private maxStringLength: number;
  private maxDepth: number;
  private maskKeys: Set<string>;
  private masker: SensitiveDataMasker;

  constructor(config: SerializerConfig = {}) {
    this.maxStringLength = config.max_string_length ?? 5000;
    this.maxDepth = config.max_depth ?? 8;
    this.masker = config.masker ?? getDefaultMasker();
    const defaultMasks = [
      "password", "secret", "api_key", "apikey", "token",
      "access_token", "private_key", "authorization", "auth",
    ];
    this.maskKeys = new Set(
      (config.mask_keys ?? defaultMasks).map((k) => k.toLowerCase())
    );
  }

  public sanitize(data: any, depth: number = 0, seen: WeakSet<object> = new WeakSet()): any {
    if (depth > this.maxDepth) {
      return "[TRUNCATED: MAX_DEPTH]";
    }

    if (data === null || data === undefined) {
      return data;
    }

    const t = typeof data;
    if (t === "number" || t === "boolean") {
      return data;
    }

    if (t === "string") {
      const masked = this.masker.maskText(data);
      if (masked.length > this.maxStringLength) {
        return masked.slice(0, this.maxStringLength) + "... [TRUNCATED]";
      }
      return masked;
    }

    if (t === "bigint" || t === "symbol") {
      return String(data);
    }

    if (typeof data === "object") {
      if (seen.has(data)) {
        return "[CIRCULAR_REFERENCE]";
      }
      seen.add(data);

      if (Array.isArray(data)) {
        return data.map((item) => this.sanitize(item, depth + 1, seen));
      }

      if (data instanceof Date) {
        return data.toISOString();
      }

      if (data instanceof Error) {
        return {
          name: data.name,
          message: data.message,
          stack: data.stack ? data.stack.slice(0, 1000) : undefined,
        };
      }

      const result: Record<string, any> = {};
      for (const [key, value] of Object.entries(data)) {
        if (this.maskKeys.has(key.toLowerCase())) {
          result[key] = "******** [REDACTED]";
        } else {
          result[key] = this.sanitize(value, depth + 1, seen);
        }
      }
      return result;
    }

    return String(data);
  }

  public computeHash(data: any): string {
    const canonicalJson = JSON.stringify(data, Object.keys(data || {}).sort());
    return crypto.createHash("sha256").update(canonicalJson || "").digest("hex");
  }

  public createSnapshot(
    spanId: string,
    stateData: Record<string, any>,
    branchId: string = "main"
  ): StateSnapshot {
    const sanitized = this.sanitize(stateData);
    const stateHash = this.computeHash(sanitized);

    return {
      id: crypto.randomUUID(),
      span_id: spanId,
      branch_id: branchId,
      state_hash: stateHash,
      state_data: sanitized,
      created_at: new Date().toISOString(),
    };
  }
}
