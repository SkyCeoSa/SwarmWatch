import test from "node:test";
import assert from "node:assert/strict";
import {
  Role,
  Permission,
  hasPermission,
  TotpEngine,
  base32Encode,
  base32Decode,
} from "../src/index.ts";

test("Enterprise MFA / TOTP & RBAC Engine (TypeScript / ADR 0037)", async (t) => {
  await t.test("Base32 encoding and decoding byte-exact roundtrip", () => {
    const original = Buffer.from("SwarmWatch-Secret-Bytes-12345!@#", "utf8");
    const encoded = base32Encode(original);
    assert.ok(encoded.length > 0);
    const decoded = base32Decode(encoded);
    assert.deepEqual(decoded, original);
  });

  await t.test("TotpEngine secret generation and RFC 6238 OTPAuth URI formatting", () => {
    const secret = TotpEngine.generateSecret(20);
    assert.ok(typeof secret === "string");
    assert.ok(secret.length >= 26); // 20 bytes Base32 is 32 chars without padding

    const uri = TotpEngine.buildOtpauthUri("alice@corp.internal", secret, "SwarmWatch Cluster");
    assert.ok(uri.startsWith("otpauth://totp/SwarmWatch%20Cluster:alice%40corp.internal?secret="));
    assert.ok(uri.includes("algorithm=SHA1"));
    assert.ok(uri.includes("digits=6"));
    assert.ok(uri.includes("period=30"));
  });

  await t.test("TotpEngine generates 6-digit deterministic TOTP codes and verifies drift window", () => {
    const secret = TotpEngine.generateSecret();
    const currentStep = Math.floor(Date.now() / 1000 / 30);

    const code = TotpEngine.generateTotp(secret, currentStep);
    assert.equal(code.length, 6);
    assert.match(code, /^\d{6}$/);

    // Current code verifies
    assert.equal(TotpEngine.verifyTotp(code, secret, 1), true);

    // Code at step - 1 verifies within window 1
    const prevCode = TotpEngine.generateTotp(secret, currentStep - 1);
    assert.equal(TotpEngine.verifyTotp(prevCode, secret, 1), true);

    // Code at step + 1 verifies within window 1
    const nextCode = TotpEngine.generateTotp(secret, currentStep + 1);
    assert.equal(TotpEngine.verifyTotp(nextCode, secret, 1), true);

    // Code at step - 5 fails (outside window 1)
    const oldCode = TotpEngine.generateTotp(secret, currentStep - 5);
    assert.equal(TotpEngine.verifyTotp(oldCode, secret, 1), false);

    // Bogus code fails
    assert.equal(TotpEngine.verifyTotp("999999", secret, 0), false);
    assert.equal(TotpEngine.verifyTotp("", secret), false);
  });

  await t.test("TotpEngine single-use backup recovery codes generation and burn consumption", () => {
    const [rawCodes, hashedCodes] = TotpEngine.generateBackupCodes(8);
    assert.equal(rawCodes.length, 8);
    assert.equal(hashedCodes.length, 8);

    const codeToBurn = rawCodes[2];

    // First consumption succeeds and burns code
    const [valid1, remaining1] = TotpEngine.verifyAndConsumeBackupCode(codeToBurn, hashedCodes);
    assert.equal(valid1, true);
    assert.equal(remaining1.length, 7);

    // Attempting to reuse burned code fails
    const [valid2, remaining2] = TotpEngine.verifyAndConsumeBackupCode(codeToBurn, remaining1);
    assert.equal(valid2, false);
    assert.equal(remaining2.length, 7);

    // Invalid code fails
    const [validBogus, remainingBogus] = TotpEngine.verifyAndConsumeBackupCode("BOGU-S999", remaining1);
    assert.equal(validBogus, false);
    assert.equal(remainingBogus.length, 7);
  });

  await t.test("RBAC matrix permissions: Admin has full authority", () => {
    assert.equal(hasPermission(Role.Admin, Permission.SwarmsRun), true);
    assert.equal(hasPermission(Role.Admin, Permission.SwarmsTrip), true);
    assert.equal(hasPermission(Role.Admin, Permission.PolicyWrite), true);
    assert.equal(hasPermission(Role.Admin, Permission.KeysRevoke), true);
    assert.equal(hasPermission(Role.Admin, Permission.MfaManage), true);
    assert.equal(hasPermission(Role.Admin, Permission.AuditRead), true);
  });

  await t.test("RBAC matrix permissions: Operator can trip and run, but cannot write policies or revoke keys", () => {
    assert.equal(hasPermission(Role.Operator, Permission.SwarmsRun), true);
    assert.equal(hasPermission(Role.Operator, Permission.SwarmsTrip), true);
    assert.equal(hasPermission(Role.Operator, Permission.StorageArchive), true);
    assert.equal(hasPermission(Role.Operator, Permission.PolicyWrite), false);
    assert.equal(hasPermission(Role.Operator, Permission.KeysRevoke), false);
    assert.equal(hasPermission(Role.Operator, Permission.MfaManage), false);
  });

  await t.test("RBAC matrix permissions: Auditor has read-only access to audit, keys, spans, but no mutation", () => {
    assert.equal(hasPermission(Role.Auditor, Permission.AuditRead), true);
    assert.equal(hasPermission(Role.Auditor, Permission.KeysRead), true);
    assert.equal(hasPermission(Role.Auditor, Permission.SwarmsRead), true);
    assert.equal(hasPermission(Role.Auditor, Permission.SwarmsTrip), false);
    assert.equal(hasPermission(Role.Auditor, Permission.SwarmsRun), false);
    assert.equal(hasPermission(Role.Auditor, Permission.PolicyWrite), false);
  });

  await t.test("RBAC matrix permissions: Viewer has strictly minimal read-only visibility", () => {
    assert.equal(hasPermission(Role.Viewer, Permission.SwarmsRead), true);
    assert.equal(hasPermission(Role.Viewer, Permission.PolicyRead), true);
    assert.equal(hasPermission(Role.Viewer, Permission.KeysRead), false);
    assert.equal(hasPermission(Role.Viewer, Permission.AuditRead), false);
    assert.equal(hasPermission(Role.Viewer, Permission.SwarmsTrip), false);
  });
});
