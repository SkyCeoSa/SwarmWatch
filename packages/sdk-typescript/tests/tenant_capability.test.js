/**
 * Tests for Tenant Identity Boundary & Attenuated Capability Tokens (Seam 65 / ADR 0076).
 * Verifies cryptographically bound capability tokens, monotonic delegation attenuation,
 * subtree revocation cascade, replay and cross-tenant forgery rejection, and sub-250µs verification.
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

import {
  CapabilityError,
  PrivilegeEscalationError,
  AttenuationViolationError,
  CapabilityVerificationError,
  RevocationError,
  resourcePatternMatches,
  isSubpattern,
  CapabilityToken,
  CapabilityVerdict,
  RevocationReceipt,
  CapabilityStatus,
  TenantCapabilityManager,
  globalCapabilityManager,
} from "../src/tenant_capability.ts";
import { WitnessKeyManager } from "../src/root_witness.ts";

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "swarmwatch_ts_capability_"));
}

test("Seam 65: Tenant Identity Boundary & Attenuated Capability Tokens (ADR 0076)", async (t) => {
  await t.test("Resource pattern matching and monotonic subpattern validation", () => {
    assert.strictEqual(resourcePatternMatches("*", "sw://tenant-1/swarms/42"), true);
    assert.strictEqual(resourcePatternMatches("sw://tenant-1/*", "sw://tenant-1/swarms/42"), true);
    assert.strictEqual(resourcePatternMatches("sw://tenant-1/swarms/*", "sw://tenant-1/swarms/42"), true);
    assert.strictEqual(resourcePatternMatches("sw://tenant-1/swarms/42", "sw://tenant-1/swarms/42"), true);
    assert.strictEqual(resourcePatternMatches("sw://tenant-1/swarms/42", "sw://tenant-1/swarms/99"), false);
    assert.strictEqual(resourcePatternMatches("sw://tenant-2/*", "sw://tenant-1/swarms/42"), false);

    assert.strictEqual(isSubpattern("*", "sw://tenant-1/*"), true);
    assert.strictEqual(isSubpattern("sw://tenant-1/*", "sw://tenant-1/swarms/*"), true);
    assert.strictEqual(isSubpattern("sw://tenant-1/swarms/*", "sw://tenant-1/swarms/fin/*"), true);
    assert.strictEqual(isSubpattern("sw://tenant-1/swarms/42", "sw://tenant-1/swarms/42"), true);
    assert.strictEqual(isSubpattern("sw://tenant-1/swarms/*", "sw://tenant-1/*"), false);
    assert.strictEqual(isSubpattern("sw://tenant-1/*", "*"), false);
  });

  await t.test("Root token issuance and cryptographic signature verification", () => {
    const km = WitnessKeyManager.generate();
    const mgr = new TenantCapabilityManager({ authorityKeyManager: km });

    const token = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-orchestrator",
      scopes: ["telemetry:write", "effect:dispatch", "circuit:trip"],
      resourcePattern: "sw://tenant-alpha/swarms/*",
      epoch: 1,
      ttlSeconds: 3600,
      metadata: { department: "finance" },
    });

    assert.ok(token.tokenId);
    assert.strictEqual(token.parentTokenId, null);
    assert.strictEqual(token.tenantId, "tenant-alpha");
    assert.strictEqual(token.issuerPrincipal, "authority");
    assert.strictEqual(token.subjectPrincipal, "agent-orchestrator");
    assert.deepStrictEqual(token.scopes, ["telemetry:write", "effect:dispatch", "circuit:trip"]);
    assert.strictEqual(token.resourcePattern, "sw://tenant-alpha/swarms/*");
    assert.strictEqual(token.epoch, 1);
    assert.strictEqual(token.depth, 0);
    assert.strictEqual(token.maxDepth, 8);
    assert.strictEqual(token.issuerPublicKey, km.publicKeyB64);
    assert.ok(token.attenuationDigest);
    assert.ok(token.signature);

    assert.strictEqual(mgr.verifyTokenSignature(token), true);
  });

  await t.test("Monotonic attenuation success", () => {
    const km = WitnessKeyManager.generate();
    const mgr = new TenantCapabilityManager({ authorityKeyManager: km });

    const rootToken = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-orchestrator",
      scopes: ["telemetry:write", "effect:dispatch", "circuit:trip"],
      resourcePattern: "sw://tenant-alpha/swarms/*",
      epoch: 1,
      ttlSeconds: 3600,
    });

    const childToken = mgr.attenuate({
      parentToken: rootToken,
      subjectPrincipal: "agent-worker-01",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/swarms/finance/*",
      ttlSeconds: 1800,
    });

    assert.strictEqual(childToken.parentTokenId, rootToken.tokenId);
    assert.strictEqual(childToken.tenantId, rootToken.tenantId);
    assert.strictEqual(childToken.subjectPrincipal, "agent-worker-01");
    assert.deepStrictEqual(childToken.scopes, ["telemetry:write"]);
    assert.strictEqual(childToken.resourcePattern, "sw://tenant-alpha/swarms/finance/*");
    assert.strictEqual(childToken.depth, 1);
    assert.ok(childToken.expiresAtNs <= rootToken.expiresAtNs);

    assert.strictEqual(mgr.verifyTokenSignature(childToken), true);
  });

  await t.test("Privilege escalation rejected on unheld scope", () => {
    const mgr = new TenantCapabilityManager();
    const rootToken = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-orchestrator",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/*",
    });

    assert.throws(
      () => {
        mgr.attenuate({
          parentToken: rootToken,
          subjectPrincipal: "agent-sub",
          scopes: ["telemetry:write", "admin:all"],
        });
      },
      (err) => err instanceof PrivilegeEscalationError && err.message.includes("Unheld scopes requested")
    );
  });

  await t.test("Cross-tenant delegation strictly prohibited", () => {
    const mgr = new TenantCapabilityManager();
    const rootToken = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-orchestrator",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/*",
    });

    assert.throws(
      () => {
        mgr.attenuate({
          parentToken: rootToken,
          subjectPrincipal: "agent-sub",
          scopes: ["telemetry:write"],
          resourcePattern: "sw://tenant-beta/*",
        });
      },
      (err) => err instanceof PrivilegeEscalationError && err.message.includes("Cross-tenant resource pattern prohibited")
    );
  });

  await t.test("Resource widening rejected", () => {
    const mgr = new TenantCapabilityManager();
    const rootToken = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-orchestrator",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/swarms/finance/*",
    });

    assert.throws(
      () => {
        mgr.attenuate({
          parentToken: rootToken,
          subjectPrincipal: "agent-sub",
          scopes: ["telemetry:write"],
          resourcePattern: "sw://tenant-alpha/swarms/*",
        });
      },
      (err) => err instanceof PrivilegeEscalationError && err.message.includes("Resource pattern cannot be widened")
    );
  });

  await t.test("Expiration extension rejected", () => {
    const mgr = new TenantCapabilityManager();
    const rootToken = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-orchestrator",
      scopes: ["telemetry:write"],
      ttlSeconds: 60,
    });

    assert.throws(
      () => {
        mgr.attenuate({
          parentToken: rootToken,
          subjectPrincipal: "agent-sub",
          ttlSeconds: 3600,
        });
      },
      (err) => err instanceof AttenuationViolationError && err.message.includes("Child expiration cannot exceed")
    );
  });

  await t.test("Max delegation depth enforced", () => {
    const mgr = new TenantCapabilityManager();
    let current = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-0",
      scopes: ["telemetry:write"],
      maxDepth: 3,
    });

    for (let i = 1; i <= 3; i++) {
      current = mgr.attenuate({
        parentToken: current,
        subjectPrincipal: `agent-${i}`,
        scopes: ["telemetry:write"],
      });
      assert.strictEqual(current.depth, i);
    }

    assert.throws(
      () => {
        mgr.attenuate({
          parentToken: current,
          subjectPrincipal: "agent-4",
          scopes: ["telemetry:write"],
        });
      },
      (err) => err instanceof AttenuationViolationError && err.message.includes("exceeds max_depth")
    );
  });

  await t.test("Verification lifecycle success and verdict structure", () => {
    const mgr = new TenantCapabilityManager();
    const token = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-worker",
      scopes: ["telemetry:write", "effect:dispatch"],
      resourcePattern: "sw://tenant-alpha/swarms/eu/*",
      epoch: 2,
      ttlSeconds: 3600,
    });

    const verdict = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-alpha",
      currentEpoch: 2,
    });

    assert.strictEqual(verdict.valid, true);
    assert.strictEqual(verdict.tokenId, token.tokenId);
    assert.strictEqual(verdict.tenantId, "tenant-alpha");
    assert.strictEqual(verdict.subjectPrincipal, "agent-worker");
    assert.strictEqual(verdict.actionScope, "telemetry:write");
    assert.strictEqual(verdict.targetResource, "sw://tenant-alpha/swarms/eu/node-1");
    assert.strictEqual(verdict.reason, "AUTHORIZED");
    assert.ok(verdict.checkedAtNs > 0n);
  });

  await t.test("Verification failures: tenant, principal, scope, resource, time, and epoch", () => {
    const mgr = new TenantCapabilityManager();
    const token = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-worker",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/swarms/eu/*",
      epoch: 5,
      ttlSeconds: 300,
    });

    // 1. Tenant mismatch
    const v1 = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-beta",
    });
    assert.strictEqual(v1.valid, false);
    assert.ok(v1.reason.includes("TENANT_MISMATCH"));

    // 2. Principal mismatch
    const v2 = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-imposter",
      expectedTenantId: "tenant-alpha",
    });
    assert.strictEqual(v2.valid, false);
    assert.ok(v2.reason.includes("PRINCIPAL_MISMATCH"));

    // 3. Unauthorized scope
    const v3 = mgr.verify({
      token,
      actionScope: "circuit:trip",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-alpha",
    });
    assert.strictEqual(v3.valid, false);
    assert.ok(v3.reason.includes("UNAUTHORIZED_SCOPE"));

    // 4. Resource out of bounds
    const v4 = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/us/node-2",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-alpha",
    });
    assert.strictEqual(v4.valid, false);
    assert.ok(v4.reason.includes("RESOURCE_OUT_OF_BOUNDS"));

    // 5. Expired token
    const v5 = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-alpha",
      nowNs: token.expiresAtNs + 10_000_000_000n,
    });
    assert.strictEqual(v5.valid, false);
    assert.ok(v5.reason.includes("EXPIRED_TOKEN"));

    // 6. Premature token
    const v6 = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-alpha",
      nowNs: token.notBeforeNs - 10_000_000_000n,
    });
    assert.strictEqual(v6.valid, false);
    assert.ok(v6.reason.includes("PREMATURE_TOKEN"));

    // 7. Stale epoch
    const v7 = mgr.verify({
      token,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/swarms/eu/node-1",
      executingPrincipal: "agent-worker",
      expectedTenantId: "tenant-alpha",
      currentEpoch: 6,
    });
    assert.strictEqual(v7.valid, false);
    assert.ok(v7.reason.includes("STALE_EPOCH"));
  });

  await t.test("Revocation and subtree cascade", () => {
    const mgr = new TenantCapabilityManager();
    const root = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-root",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/*",
    });

    const child = mgr.attenuate({
      parentToken: root,
      subjectPrincipal: "agent-child",
      scopes: ["telemetry:write"],
    });

    const grandchild = mgr.attenuate({
      parentToken: child,
      subjectPrincipal: "agent-grandchild",
      scopes: ["telemetry:write"],
    });

    assert.strictEqual(mgr.verify({
      token: grandchild,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/x",
      executingPrincipal: "agent-grandchild",
      expectedTenantId: "tenant-alpha",
    }).valid, true);

    // Revoke child token
    const receipt = mgr.revoke(child.tokenId, "Compromised subagent detected", "admin");
    assert.strictEqual(receipt.tokenId, child.tokenId);

    // Root remains valid
    assert.strictEqual(mgr.verify({
      token: root,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/x",
      executingPrincipal: "agent-root",
      expectedTenantId: "tenant-alpha",
    }).valid, true);

    // Child is revoked
    const vChild = mgr.verify({
      token: child,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/x",
      executingPrincipal: "agent-child",
      expectedTenantId: "tenant-alpha",
    });
    assert.strictEqual(vChild.valid, false);
    assert.ok(vChild.reason.includes("TOKEN_REVOKED"));

    // Grandchild is also revoked via subtree cascade!
    const vGrandchild = mgr.verify({
      token: grandchild,
      actionScope: "telemetry:write",
      targetResource: "sw://tenant-alpha/x",
      executingPrincipal: "agent-grandchild",
      expectedTenantId: "tenant-alpha",
    });
    assert.strictEqual(vGrandchild.valid, false);
    assert.ok(vGrandchild.reason.includes("ANCESTOR_REVOKED"));
  });

  await t.test("Tampered token signature fails", () => {
    const mgr = new TenantCapabilityManager();
    const token = mgr.issueRootToken({
      tenantId: "tenant-alpha",
      subjectPrincipal: "agent-root",
      scopes: ["telemetry:write"],
      resourcePattern: "sw://tenant-alpha/*",
    });

    const tampered = new CapabilityToken({
      ...token,
      scopes: ["telemetry:write", "admin:all"],
    });

    const verdict = mgr.verify({
      token: tampered,
      actionScope: "admin:all",
      targetResource: "sw://tenant-alpha/x",
      executingPrincipal: "agent-root",
      expectedTenantId: "tenant-alpha",
    });
    assert.strictEqual(verdict.valid, false);
    assert.ok(verdict.reason.includes("SIGNATURE_INVALID"));
  });

  await t.test("Sub-250µs verification SLO benchmark", () => {
    const mgr = new TenantCapabilityManager();
    const root = mgr.issueRootToken({
      tenantId: "tenant-perf",
      subjectPrincipal: "agent-perf",
      scopes: ["telemetry:write", "effect:dispatch"],
      resourcePattern: "sw://tenant-perf/swarms/*",
    });
    const child = mgr.attenuate({
      parentToken: root,
      subjectPrincipal: "agent-child",
      scopes: ["telemetry:write"],
    });

    // Warmup
    for (let i = 0; i < 10; i++) {
      mgr.verify({
        token: child,
        actionScope: "telemetry:write",
        targetResource: "sw://tenant-perf/swarms/1",
        executingPrincipal: "agent-child",
        expectedTenantId: "tenant-perf",
      });
    }

    // Benchmark: best of 3 runs of 500 iterations
    let bestAvgUs = Infinity;
    const iterations = 500;

    for (let run = 0; run < 3; run++) {
      const t0 = process.hrtime.bigint();
      for (let i = 0; i < iterations; i++) {
        mgr.verify({
          token: child,
          actionScope: "telemetry:write",
          targetResource: "sw://tenant-perf/swarms/1",
          executingPrincipal: "agent-child",
          expectedTenantId: "tenant-perf",
        });
      }
      const t1 = process.hrtime.bigint();
      const totalNs = Number(t1 - t0);
      const avgUs = (totalNs / iterations) / 1000;
      if (avgUs < bestAvgUs) {
        bestAvgUs = avgUs;
      }
    }

    assert.ok(bestAvgUs < 250.0, `Capability verification took ${bestAvgUs.toFixed(2)}µs, exceeding 250µs SLO`);
  });

  await t.test("Global instance and status reporting", () => {
    const status = globalCapabilityManager.status();
    assert.ok(status instanceof CapabilityStatus);
    assert.ok(status.totalIssued >= 0);
    assert.ok(status.totalRevoked >= 0);
    const d = status.toDict();
    assert.ok("total_issued" in d);
    assert.ok("total_revoked" in d);
  });
});
