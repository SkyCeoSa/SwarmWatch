/**
 * SwarmWatch TypeScript Enterprise Multi-Factor Authentication (MFA / TOTP) & Strict RBAC Engine.
 * Implements RFC 6238 Time-Based One-Time Passwords, single-use SHA-256 recovery backup codes,
 * and a 4-tier Role-Based Access Control matrix (Admin, Operator, Auditor, Viewer).
 * Zero third-party dependencies (pure node:crypto).
 */

import * as crypto from "node:crypto";

export const Role = {
  Admin: "admin",
  Operator: "operator",
  Auditor: "auditor",
  Viewer: "viewer",
} as const;

export type Role = (typeof Role)[keyof typeof Role];

export const Permission = {
  SwarmsRead: "swarms:read",
  SwarmsRun: "swarms:run",
  SwarmsTrip: "swarms:trip",
  SwarmsResume: "swarms:resume",
  SwarmsFork: "swarms:fork",
  PolicyRead: "policy:read",
  PolicyWrite: "policy:write",
  StorageArchive: "storage:archive",
  StorageRestore: "storage:restore",
  KeysRead: "keys:read",
  KeysWrite: "keys:write",
  KeysRevoke: "keys:revoke",
  MfaManage: "mfa:manage",
  AuditRead: "audit:read",
  FleetManage: "fleet:manage",
} as const;

export type Permission = (typeof Permission)[keyof typeof Permission];

export const ALL_PERMISSIONS: Set<Permission> = new Set(Object.values(Permission));

export const ROLE_PERMISSIONS: Record<Role, Set<Permission>> = {
  [Role.Admin]: ALL_PERMISSIONS,
  [Role.Operator]: new Set([
    Permission.SwarmsRead,
    Permission.SwarmsRun,
    Permission.SwarmsTrip,
    Permission.SwarmsResume,
    Permission.SwarmsFork,
    Permission.PolicyRead,
    Permission.StorageArchive,
    Permission.StorageRestore,
    Permission.KeysRead,
    Permission.FleetManage,
  ]),
  [Role.Auditor]: new Set([
    Permission.SwarmsRead,
    Permission.PolicyRead,
    Permission.KeysRead,
    Permission.AuditRead,
  ]),
  [Role.Viewer]: new Set([
    Permission.SwarmsRead,
    Permission.PolicyRead,
  ]),
};

export function hasPermission(role: Role, permission: Permission): boolean {
  const perms = ROLE_PERMISSIONS[role];
  return perms ? perms.has(permission) : false;
}

export interface UserAccount {
  id: string;
  username: string;
  email: string;
  role: Role;
  mfa_enabled: boolean;
  totp_secret?: string | null;
  backup_codes: string[]; // SHA-256 hashes
  created_at: string;
}

const RFC4648_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

export function base32Encode(buffer: Buffer): string {
  let bits = 0;
  let value = 0;
  let output = "";

  for (let i = 0; i < buffer.length; i++) {
    value = (value << 8) | buffer[i];
    bits += 8;
    while (bits >= 5) {
      output += RFC4648_ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }

  if (bits > 0) {
    output += RFC4648_ALPHABET[(value << (5 - bits)) & 31];
  }

  return output;
}

export function base32Decode(base32Str: string): Buffer {
  const clean = base32Str.toUpperCase().replace(/[=\s]/g, "");
  let bits = 0;
  let value = 0;
  const bytes: number[] = [];

  for (let i = 0; i < clean.length; i++) {
    const char = clean[i];
    const index = RFC4648_ALPHABET.indexOf(char);
    if (index === -1) {
      throw new Error(`Invalid Base32 character: ${char}`);
    }
    value = (value << 5) | index;
    bits += 5;
    if (bits >= 8) {
      bytes.push((value >>> (bits - 8)) & 255);
      bits -= 8;
    }
  }

  return Buffer.from(bytes);
}

export class TotpEngine {
  public static readonly TIME_STEP_SECONDS = 30;
  public static readonly CODE_DIGITS = 6;

  public static generateSecret(byteLength: number = 20): string {
    const raw = crypto.randomBytes(byteLength);
    return base32Encode(raw);
  }

  public static generateTotp(secret: string, timeStep?: number): string {
    const step = timeStep !== undefined ? timeStep : Math.floor(Date.now() / 1000 / TotpEngine.TIME_STEP_SECONDS);
    const keyBuf = base32Decode(secret);

    const counterBuf = Buffer.alloc(8);
    counterBuf.writeBigUInt64BE(BigInt(step));

    const hmac = crypto.createHmac("sha1", keyBuf).update(counterBuf).digest();
    const offset = hmac[hmac.length - 1] & 0x0f;

    const codeInt =
      (((hmac[offset] & 0x7f) << 24) |
        ((hmac[offset + 1] & 0xff) << 16) |
        ((hmac[offset + 2] & 0xff) << 8) |
        (hmac[offset + 3] & 0xff)) %
      1000000;

    return String(codeInt).padStart(TotpEngine.CODE_DIGITS, "0");
  }

  public static verifyTotp(code: string, secret: string, window: number = 1): boolean {
    if (!code || code.trim().length !== TotpEngine.CODE_DIGITS) {
      return false;
    }

    const cleanCode = code.trim();
    const currentStep = Math.floor(Date.now() / 1000 / TotpEngine.TIME_STEP_SECONDS);

    for (let offset = -window; offset <= window; offset++) {
      const expected = TotpEngine.generateTotp(secret, currentStep + offset);
      const bufA = Buffer.from(cleanCode, "utf8");
      const bufB = Buffer.from(expected, "utf8");
      if (bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB)) {
        return true;
      }
    }

    return false;
  }

  public static generateBackupCodes(count: number = 8): [string[], string[]] {
    const rawCodes: string[] = [];
    const hashedCodes: string[] = [];

    for (let i = 0; i < count; i++) {
      const p1 = crypto.randomBytes(2).toString("hex").toUpperCase();
      const p2 = crypto.randomBytes(2).toString("hex").toUpperCase();
      const raw = `${p1}-${p2}`;
      rawCodes.push(raw);

      const canonical = raw.replace(/-/g, "").trim().toUpperCase();
      const hash = crypto.createHash("sha256").update(canonical, "utf8").digest("hex");
      hashedCodes.append ? hashedCodes.push(hash) : hashedCodes.push(hash);
    }

    return [rawCodes, hashedCodes];
  }

  public static verifyAndConsumeBackupCode(
    rawCode: string,
    hashedCodes: string[]
  ): [boolean, string[]] {
    if (!rawCode) {
      return [false, [...hashedCodes]];
    }

    const canonical = rawCode.replace(/-/g, "").trim().toUpperCase();
    const testHash = crypto.createHash("sha256").update(canonical, "utf8").digest("hex");

    const remaining: string[] = [];
    let matched = false;

    for (const stored of hashedCodes) {
      const bufA = Buffer.from(testHash, "utf8");
      const bufB = Buffer.from(stored, "utf8");
      if (!matched && bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB)) {
        matched = true;
        continue;
      }
      remaining.push(stored);
    }

    return [matched, remaining];
  }

  public static buildOtpauthUri(username: string, secret: string, issuer: string = "SwarmWatch"): string {
    const safeIssuer = encodeURIComponent(issuer);
    const safeUser = encodeURIComponent(username);
    return `otpauth://totp/${safeIssuer}:${safeUser}?secret=${secret}&issuer=${safeIssuer}&algorithm=SHA1&digits=6&period=30`;
  }
}
