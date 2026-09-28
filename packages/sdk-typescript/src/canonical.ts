/**
 * Canonical Causal Encoding & Byte-Deterministic Contract Engine (Seam 50 / ADR 0061).
 * Conforms strictly to RFC 8785 (JSON Canonicalization Scheme - JCS) principles to guarantee
 * 100% byte-for-byte identical state hashes and Merkle leaves across Python and TypeScript.
 */

import * as crypto from "node:crypto";

export const CanonicalDomainTag = {
  LEAF: "sw:leaf:v1",
  SNAPSHOT: "sw:snap:v1",
  RECEIPT: "sw:rcpt:v1",
  PERMIT: "sw:pmt:v1",
  EFFECT: "sw:eff:v1",
  TOPOLOGY: "sw:topo:v1",
  STATE: "sw:state:v1",
  METRIC: "sw:metric:v1",
  REMEDIATION: "sw:rem:v1",
  RECONCILER: "sw:rec:v1",
  PRIVACY: "sw:priv:v1",
  EVOLUTION: "sw:evo:v1",
  ACCEPTANCE: "sw:acc:v1",
  EVIDENCE: "sw:evid:v1",
} as const;

export type CanonicalDomainTagEnum = typeof CanonicalDomainTag[keyof typeof CanonicalDomainTag];

export class CanonicalEncodingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CanonicalEncodingError";
  }
}

export class CanonicalCodec {
  public static readonly SUPPORTED_VERSIONS = new Set<number>([1]);

  /**
   * Serializes an arbitrary JavaScript object or primitive into a canonical JSON string (RFC 8785).
   * Enforces lexicographical key sorting by UTF-16 code units, whitespace elimination,
   * negative zero normalization, and unescaped UTF-8 string representation.
   */
  public static canonicalize(obj: unknown, version: number = 1): string {
    if (!this.SUPPORTED_VERSIONS.has(version)) {
      throw new CanonicalEncodingError(`Unsupported canonical encoding version: ${version}`);
    }

    const seen = new Set<object>();

    function encodeVal(val: unknown): string {
      if (val === null) {
        return "null";
      }
      if (typeof val === "boolean") {
        return val ? "true" : "false";
      }
      if (typeof val === "number") {
        if (!Number.isFinite(val)) {
          throw new CanonicalEncodingError("Non-finite numbers (NaN, Infinity) are prohibited in canonical encoding");
        }
        if (Object.is(val, -0) || val === 0) {
          return "0";
        }
        return val.toString();
      }
      if (typeof val === "string") {
        return JSON.stringify(val);
      }
      if (typeof val === "bigint") {
        return val.toString();
      }
      if (typeof val === "object") {
        // Handle unwrapping of custom toJSON / toDict methods
        const objAny = val as Record<string, unknown>;
        if (typeof objAny.toJSON === "function") {
          return encodeVal(objAny.toJSON());
        }
        if (typeof objAny.toDict === "function") {
          return encodeVal(objAny.toDict());
        }

        if (seen.has(val)) {
          throw new CanonicalEncodingError("Circular reference detected in data structure");
        }
        seen.add(val);

        try {
          if (Array.isArray(val)) {
            const items = val.map((item) => {
              if (item === undefined || typeof item === "function" || typeof item === "symbol") {
                return "null";
              }
              return encodeVal(item);
            });
            return `[${items.join(",")}]`;
          }

          // Plain object dictionary: sort keys strictly by UTF-16 code units
          const keys = Object.keys(objAny).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
          const pairs: string[] = [];

          for (const key of keys) {
            const propVal = objAny[key];
            if (propVal === undefined || typeof propVal === "function" || typeof propVal === "symbol") {
              continue; // Omit undefined/function properties per standard JSON serialization
            }
            const encodedKey = JSON.stringify(key);
            const encodedVal = encodeVal(propVal);
            pairs.push(`${encodedKey}:${encodedVal}`);
          }

          return `{${pairs.join(",")}}`;
        } finally {
          seen.delete(val);
        }
      }

      throw new CanonicalEncodingError(`Unsupported type for canonical encoding: ${typeof val}`);
    }

    if (obj === undefined || typeof obj === "function" || typeof obj === "symbol") {
      throw new CanonicalEncodingError(`Top-level ${typeof obj} cannot be canonically encoded`);
    }

    return encodeVal(obj);
  }

  /**
   * Serializes value to canonical UTF-8 bytes (Buffer).
   */
  public static encode(obj: unknown, version: number = 1): Buffer {
    const canonicalStr = this.canonicalize(obj, version);
    return Buffer.from(canonicalStr, "utf8");
  }

  /**
   * Parses canonical JSON string or Buffer back to JavaScript value.
   */
  public static decode(payload: Buffer | string): unknown {
    const str = typeof payload === "string" ? payload : payload.toString("utf8");
    return JSON.parse(str);
  }

  /**
   * Computes domain-separated SHA-256 digest over canonical byte representation.
   * H_v(domain_tag, x) = SHA256(domain_tag || 0x00 || C_v(x))
   */
  public static hash(obj: unknown, domainTag: string = "", version: number = 1): string {
    const canonicalBytes = this.encode(obj, version);
    const hash = crypto.createHash("sha256");
    if (domainTag) {
      hash.update(Buffer.from(`${domainTag}\0`, "utf8"));
    }
    hash.update(canonicalBytes);
    return hash.digest("hex");
  }

  /**
   * Validates whether an object is canonically encodable without throwing exceptions.
   */
  public static validate(obj: unknown, version: number = 1): { isValid: boolean; error: string | null } {
    try {
      this.canonicalize(obj, version);
      return { isValid: true, error: null };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return { isValid: false, error: msg };
    }
  }
}

export const globalCanonicalCodec = new CanonicalCodec();
