/**
 * Acceptance Harness & Multi-Dimensional Release Gate Tests (Seam 60 / ADR 0071).
 * Verifies multi-gate release certification, physical artifact craft auditing,
 * and immutable cryptographic attestations in pure Node 22 TypeScript/JavaScript.
 */

import { test } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

import {
  AcceptanceGateName,
  AcceptanceStatus,
  AcceptanceProfile,
  AcceptanceError,
  AttestationVerificationError,
  AcceptanceHarness,
  globalAcceptanceHarness,
} from "../src/acceptance.ts";

test("AcceptanceHarness executes all 6 gates and certifies clean candidate", () => {
  const harness = new AcceptanceHarness();
  const report = harness.run(AcceptanceProfile.PRODUCTION);

  assert.ok(report.reportId.startsWith("acc-rep-"));
  assert.equal(report.status, AcceptanceStatus.CERTIFIED);
  assert.equal(report.overallScore, 1.0);
  assert.ok(report.durationMs >= 0);
  assert.equal(report.reportHash.length, 64);

  // All 6 gate dimensions must be present and passed
  for (const gateName of AcceptanceGateName.ALL_GATES) {
    assert.ok(gateName in report.gates, `Missing gate: ${gateName}`);
    const gSum = report.gates[gateName];
    assert.equal(gSum.passed, true);
    assert.equal(gSum.score, 1.0);
    assert.equal(gSum.violations.length, 0);
  }
});

test("AcceptanceHarness attests certified report and verifies cryptographic signature", () => {
  const harness = new AcceptanceHarness();
  const report = harness.run();

  const secretKey = "test-enterprise-audit-secret-key";
  const attestation = harness.attest(report, "Security Officer Alpha", secretKey);

  assert.ok(attestation.attestationId.startsWith("acc-attest-"));
  assert.equal(attestation.reportId, report.reportId);
  assert.equal(attestation.status, AcceptanceStatus.CERTIFIED);
  assert.equal(attestation.attestationHash.length, 64);
  assert.equal(attestation.signature.length, 64);

  // Authentic attestation must verify
  const isValid = harness.verifyAttestation(attestation, secretKey);
  assert.equal(isValid, true);

  // Forged signature must fail closed
  const tampered = harness.attest(report, "Security Officer Alpha", secretKey);
  tampered.signature = "0".repeat(64);
  assert.throws(
    () => harness.verifyAttestation(tampered, secretKey),
    (err) => err instanceof AttestationVerificationError
  );

  // Wrong secret key must fail closed
  assert.throws(
    () => harness.verifyAttestation(attestation, "wrong-secret-key"),
    (err) => err instanceof AttestationVerificationError
  );
});

test("AcceptanceHarness audits physical dashboard.html artifact successfully", () => {
  const harness = new AcceptanceHarness();
  const auditRes = harness.auditDashboardArtifact();

  assert.equal(auditRes.compliant, true);
  assert.equal(auditRes.violations.length, 0);
  assert.ok(auditRes.total_badges > 0);

  // Every single fleet badge must conform to craft floor
  for (const b of auditRes.badges_checked) {
    assert.ok(b.length <= 30, `Badge exceeded 30 chars: ${b.text}`);
    assert.equal(b.is_upper, true, `Badge is not uppercase: ${b.text}`);
    assert.equal(b.valid, true);
  }
});

test("AcceptanceHarness detects craft floor violations in malformed HTML", () => {
  const harness = new AcceptanceHarness();

  const badHtml = `
  <html>
    <head>
      <style>
        body { background: #FFFFFF; } /* missing base canvas */
        .glow { box-shadow: 0 0 25px rgba(255, 0, 0, 1); } /* neon glow */
      </style>
    </head>
    <body>
      <div class="fleet-badge">
        <span>🚨 ALERT BADGE HAS LOWERCASE AND IS WAY TOO LONG BEYOND THIRTY CHARACTERS</span>
      </div>
    </body>
  </html>
  `;

  const tmpPath = path.join(os.tmpdir(), `bad-dash-${Date.now()}.html`);
  fs.writeFileSync(tmpPath, badHtml, "utf-8");

  try {
    const auditRes = harness.auditDashboardArtifact(tmpPath);
    assert.equal(auditRes.compliant, false);
    const violationsStr = auditRes.violations.join(" ");
    assert.ok(violationsStr.includes("Palette invariant"));
    assert.ok(violationsStr.includes("Emoji invariant") || violationsStr.includes("neon glow"));
    assert.ok(violationsStr.includes("exceeded 30 chars") || violationsStr.includes("uppercase"));
  } finally {
    if (fs.existsSync(tmpPath)) {
      fs.unlinkSync(tmpPath);
    }
  }
});

test("AcceptanceHarness fails closed when attempting to attest rejected report", () => {
  const harness = new AcceptanceHarness();
  const report = harness.run();
  report.status = AcceptanceStatus.REJECTED;

  assert.throws(
    () => harness.attest(report),
    (err) => err instanceof AcceptanceError && err.message.includes("Cannot attest rejected")
  );
});

test("AcceptanceHarness supports filtering by custom gates and checks", () => {
  const harness = new AcceptanceHarness();
  const report = harness.run(AcceptanceProfile.CANARY, ["sec-01", "iop-01"]);

  assert.equal(report.checks.length, 2);
  assert.equal(report.status, AcceptanceStatus.CERTIFIED);
  assert.equal(Object.keys(report.gates).length, 2);
  assert.ok("security" in report.gates);
  assert.ok("interoperability" in report.gates);
});

test("AcceptanceHarness tracks history and resets metrics cleanly", () => {
  const harness = new AcceptanceHarness();
  harness.reset();

  assert.equal(harness.status().total_runs, 0);

  const report = harness.run();
  harness.attest(report);

  const st = harness.status();
  assert.equal(st.total_runs, 1);
  assert.equal(st.certified_runs, 1);
  assert.equal(st.rejected_runs, 0);
  assert.equal(st.status, "CERTIFIED");
  assert.equal(st.latest_report_id, report.reportId);

  harness.reset();
  assert.equal(harness.status().total_runs, 0);
  assert.equal(harness.status().status, "NOMINAL");
});
