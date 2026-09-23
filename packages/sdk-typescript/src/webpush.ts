/**
 * SwarmWatch WebPush & VAPID Cryptographic Engine (Seam 28 / ADR 0039).
 *
 * Implements standard Voluntary Application Server Identification (VAPID) for Web Push (RFC 8292)
 * using Node.js built-in node:crypto with zero external dependencies.
 */

import * as crypto from "node:crypto";
import { URL } from "node:url";

export interface VapidKeys {
  publicKey: string;
  privateKey: string;
}

export interface PushSubscriptionKeys {
  p256dh: string;
  auth: string;
}

export interface PushSubscription {
  id?: string;
  projectId?: string;
  endpoint: string;
  keys: PushSubscriptionKeys;
  userId?: string;
  topics?: string[];
  createdAt?: string;
}

export interface PushMessage {
  title: string;
  body: string;
  icon?: string;
  badge?: string;
  tag?: string;
  data?: Record<string, any>;
  urgency?: "very-low" | "low" | "normal" | "high";
  ttl?: number;
}

function base64UrlEncode(data: Buffer | string): string {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data, "utf8");
  return buf.toString("base64url");
}

/**
 * Enterprise VAPID Key Generator and JWT Signer (RFC 8292).
 */
export class VapidSigner {
  public readonly publicKey: string;
  public readonly privateKey: string;
  private privateKeyPem: string;

  constructor(keys?: Partial<VapidKeys>) {
    if (keys && keys.publicKey && keys.privateKey) {
      this.publicKey = keys.publicKey;
      this.privateKey = keys.privateKey;
      // In constructor with custom keys, privateKey can be a PEM or Base64 PKCS#8
      this.privateKeyPem = keys.privateKey;
    } else {
      // Generate new EC prime256v1 (NIST P-256) keypair
      const keyPair = crypto.generateKeyPairSync("ec", {
        namedCurve: "prime256v1",
        publicKeyEncoding: { type: "spki", format: "der" },
        privateKeyEncoding: { type: "pkcs8", format: "pem" },
      });

      // Extract raw 65-byte uncompressed public key (last 65 bytes of SPKI DER, starts with 0x04)
      const rawPub = keyPair.publicKey.subarray(keyPair.publicKey.length - 65);
      this.publicKey = base64UrlEncode(rawPub);
      this.privateKeyPem = keyPair.privateKey;
      this.privateKey = base64UrlEncode(keyPair.privateKey);
    }
  }

  public static generate(): VapidSigner {
    return new VapidSigner();
  }

  /**
   * Mints an RFC 8292 compliant VAPID JWT token signed with ES256 (SHA-256 + IEEE P1363).
   */
  public mintVapidToken(
    audience: string,
    subject: string = "mailto:admin@swarmwatch.local",
    expirationSeconds: number = 86400
  ): string {
    const now = Math.floor(Date.now() / 1000);
    const header = { typ: "JWT", alg: "ES256" };
    const claims = {
      aud: audience,
      exp: now + expirationSeconds,
      sub: subject,
    };

    const hdrB64 = base64UrlEncode(JSON.stringify(header));
    const claimsB64 = base64UrlEncode(JSON.stringify(claims));
    const signingInput = `${hdrB64}.${claimsB64}`;

    const sign = crypto.createSign("SHA256");
    sign.update(signingInput);
    const signature = sign.sign({
      key: this.privateKeyPem,
      dsaEncoding: "ieee-p1363",
    });

    return `${signingInput}.${base64UrlEncode(signature)}`;
  }

  /**
   * Generates standard WebPush HTTP headers for outbound push delivery.
   */
  public buildAuthHeaders(
    endpoint: string,
    subject: string = "mailto:admin@swarmwatch.local"
  ): Record<string, string> {
    const parsed = new URL(endpoint);
    const audience = `${parsed.protocol}//${parsed.host}`;
    const token = this.mintVapidToken(audience, subject);

    return {
      Authorization: `vapid t=${token}, k=${this.publicKey}`,
      "Crypto-Key": `p256ecdsa=${this.publicKey}`,
    };
  }
}
