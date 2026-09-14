/**
 * SwarmWatch TypeScript Multi-Tenant Core & Cryptographic API Key Engine.
 * Provides Organization, Project, ApiKey, TenantContext, and ApiKeyManager
 * with SHA-256 one-way hashing and constant-time verification.
 */

import * as crypto from "node:crypto";

export interface Organization {
  id: string;
  name: string;
  slug: string;
  created_at: string;
}

export interface Project {
  id: string;
  org_id: string;
  name: string;
  slug: string;
  created_at: string;
}

export interface ApiKey {
  id: string;
  project_id: string;
  name: string;
  key_prefix: string;
  key_hash: string;
  scopes: string[];
  created_at: string;
  revoked_at?: string | null;
}

export function isApiKeyActive(key: ApiKey): boolean {
  return !key.revoked_at;
}

export interface TenantContext {
  org_id: string;
  project_id: string;
  key_id?: string | null;
  scopes: string[];
}

export class ApiKeyManager {
  public static readonly DEFAULT_PREFIX_LIVE = "sw_live_";
  public static readonly DEFAULT_PREFIX_TEST = "sw_test_";

  private cache: Map<string, ApiKey> = new Map();

  public static generateRawKey(isTest: boolean = false): string {
    const prefix = isTest ? ApiKeyManager.DEFAULT_PREFIX_TEST : ApiKeyManager.DEFAULT_PREFIX_LIVE;
    const randomBytes = crypto.randomBytes(24).toString("base64url");
    return `${prefix}${randomBytes}`;
  }

  public static hashKey(rawKey: string): string {
    return crypto.createHash("sha256").update(rawKey, "utf8").digest("hex");
  }

  public static extractPrefix(rawKey: string): string {
    if (rawKey.length >= 12) {
      return rawKey.slice(0, 12) + "...";
    }
    return rawKey.slice(0, 6) + "...";
  }

  public createApiKeyRecord(
    projectId: string,
    name: string = "Default Key",
    isTest: boolean = false,
    scopes: string[] = ["telemetry:write", "telemetry:read"]
  ): [string, ApiKey] {
    const rawKey = ApiKeyManager.generateRawKey(isTest);
    const keyHash = ApiKeyManager.hashKey(rawKey);
    const prefix = ApiKeyManager.extractPrefix(rawKey);

    const apiKey: ApiKey = {
      id: crypto.randomUUID(),
      project_id: projectId,
      name,
      key_prefix: prefix,
      key_hash: keyHash,
      scopes,
      created_at: new Date().toISOString(),
      revoked_at: null,
    };

    this.cache.set(keyHash, apiKey);
    return [rawKey, apiKey];
  }

  public verifyRawKey(rawKey: string): ApiKey | null {
    if (
      !rawKey ||
      (!rawKey.startsWith(ApiKeyManager.DEFAULT_PREFIX_LIVE) &&
        !rawKey.startsWith(ApiKeyManager.DEFAULT_PREFIX_TEST))
    ) {
      return null;
    }

    const targetHash = ApiKeyManager.hashKey(rawKey);
    const cached = this.cache.get(targetHash);
    if (!cached) {
      return null;
    }

    if (!isApiKeyActive(cached)) {
      return null;
    }

    // Constant-time digest comparison to prevent timing attacks
    const bufA = Buffer.from(cached.key_hash, "utf8");
    const bufB = Buffer.from(targetHash, "utf8");

    if (bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB)) {
      return cached;
    }

    return null;
  }

  public registerInCache(apiKey: ApiKey): void {
    this.cache.set(apiKey.key_hash, apiKey);
  }

  public invalidateCache(keyHash: string): void {
    this.cache.delete(keyHash);
  }

  public clearCache(): void {
    this.cache.clear();
  }
}
