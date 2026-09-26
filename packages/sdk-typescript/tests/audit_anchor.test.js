import { test, describe, beforeEach } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

import {
  AuditAnchor,
  AuditIntegrityError,
  RootEpoch,
  MerkleAuditReceipt,
  VerificationResult,
  globalAuditAnchor,
  sha256Digest,
} from "../src/audit_anchor.ts";

describe("Seam 54: Durable Audit Anchor & Merkle Audit Receipt Engine (ADR 0065)", () => {
  beforeEach(() => {
    globalAuditAnchor.reset(false);
  });

  test("Genesis initialization creates deterministic epoch 0", () => {
    const anchor = new AuditAnchor(":memory:");
    assert.equal(anchor.totalEpochs, 1);
    assert.equal(anchor.latestEpoch, 0);

    const st = anchor.status();
    assert.equal(st.status, "ANCHORED");
    assert.equal(st.total_epochs, 1);
    assert.equal(st.latest_epoch, 0);
    assert.equal(st.genesis_hash.length, 64);
    assert.equal(st.latest_digest.length, 64);

    const audit = anchor.auditChain();
    assert.equal(audit.valid, true);
    assert.equal(audit.errors.length, 0);
  });

  test("RootEpoch and MerkleAuditReceipt serialization round-trip", () => {
    const epoch = new RootEpoch({
      epoch: 5,
      rootHash: sha256Digest("test_root"),
      prevChainDigest: sha256Digest("prev_digest"),
      chainDigest: sha256Digest("curr_digest"),
      tenantId: "tenant-serialization",
      anchoredAt: "2026-09-26T12:00:00Z",
      signature: "sig123",
      metadata: { cluster: "east-1" },
    });

    const epDict = epoch.toDict();
    assert.equal(epDict.epoch, 5);
    assert.equal(epDict.tenant_id, "tenant-serialization");

    const epRestored = RootEpoch.fromDict(epDict);
    assert.equal(epRestored.epoch, 5);
    assert.equal(epRestored.rootHash, epoch.rootHash);
    assert.equal(epRestored.signature, "sig123");

    const receipt = new MerkleAuditReceipt({
      receiptId: "rcpt_abc123",
      epoch: 5,
      tenantId: "tenant-serialization",
      leafHash: sha256Digest("leaf_x"),
      auditPath: [["left", sha256Digest("sib_1")]],
      rootHash: epoch.rootHash,
      chainDigest: epoch.chainDigest,
      signature: "receipt_sig_456",
      issuedAt: "2026-09-26T12:01:00Z",
    });

    const rcptDict = receipt.toDict();
    assert.equal(rcptDict.receipt_id, "rcpt_abc123");
    const rcptRestored = MerkleAuditReceipt.fromDict(rcptDict);
    assert.equal(rcptRestored.receiptId, "rcpt_abc123");
    assert.equal(rcptRestored.auditPath.length, 1);
    assert.equal(rcptRestored.auditPath[0][0], "left");
  });

  test("Monotonic append-root and chain digest calculation", () => {
    const anchor = new AuditAnchor(":memory:");

    const r1 = sha256Digest("session_root_1");
    const ep1 = anchor.appendRoot(r1, "tenant-1");
    assert.equal(ep1.epoch, 1);
    assert.equal(ep1.rootHash, r1);
    assert.equal(ep1.tenantId, "tenant-1");
    assert.equal(ep1.signature.length, 64);

    const r2 = sha256Digest("session_root_2");
    const ep2 = anchor.appendRoot(r2, "tenant-2");
    assert.equal(ep2.epoch, 2);
    assert.equal(ep2.prevChainDigest, ep1.chainDigest);

    assert.equal(anchor.totalEpochs, 3);
    assert.equal(anchor.latestEpoch, 2);
    assert.equal(anchor.latestDigest, ep2.chainDigest);

    // Verify expected chain digest formula
    const expectedDigest2 = sha256Digest(`${ep1.chainDigest}:2:${r2}:tenant-2`);
    assert.equal(ep2.chainDigest, expectedDigest2);
  });

  test("Merkle audit receipt issuance and independent verification", () => {
    const anchor = new AuditAnchor(":memory:");

    const leafA = sha256Digest("leaf_alpha");
    const leafB = sha256Digest("leaf_beta");
    const rootAB = sha256Digest(leafA + leafB);

    const ep = anchor.appendRoot(rootAB, "tenant-audit");

    // Path for leafA: sibling leafB is to the right
    const receipt = anchor.issueReceipt(ep.epoch, leafA, [["right", leafB]], "tenant-audit");
    assert.equal(receipt.epoch, ep.epoch);
    assert.equal(receipt.leafHash, leafA);
    assert.equal(receipt.rootHash, rootAB);
    assert.equal(receipt.signature.length, 64);

    const result = anchor.verifyReceipt(receipt);
    assert.equal(result.valid, true);
    assert.equal(result.epoch, ep.epoch);
    assert.equal(result.leafHash, leafA);
    assert.equal(result.rootHash, rootAB);
    assert.equal(result.reason, null);
  });

  test("Tampered receipt rejection", () => {
    const anchor = new AuditAnchor(":memory:");

    const leafA = sha256Digest("leaf_orig_a");
    const leafB = sha256Digest("leaf_orig_b");
    const rootAB = sha256Digest(leafA + leafB);
    const ep = anchor.appendRoot(rootAB, "tenant-tamper");

    const receipt = anchor.issueReceipt(ep.epoch, leafA, [["right", leafB]], "tenant-tamper");

    // 1. Tampered leaf hash
    const tampered1 = MerkleAuditReceipt.fromDict(receipt.toDict());
    tampered1.leafHash = sha256Digest("forged_leaf");
    const res1 = anchor.verifyReceipt(tampered1);
    assert.equal(res1.valid, false);
    assert.match(res1.reason || "", /Merkle path reconstruction/);

    // 2. Tampered signature
    const tampered2 = MerkleAuditReceipt.fromDict(receipt.toDict());
    tampered2.signature = "f".repeat(64);
    const res2 = anchor.verifyReceipt(tampered2);
    assert.equal(res2.valid, false);
    assert.match(res2.reason || "", /Invalid cryptographic HMAC/);

    // 3. Unknown epoch
    const tampered3 = MerkleAuditReceipt.fromDict(receipt.toDict());
    tampered3.epoch = 8888;
    const res3 = anchor.verifyReceipt(tampered3);
    assert.equal(res3.valid, false);
    assert.match(res3.reason || "", /not found in durable audit anchor/);
  });

  test("Post-crash chain of custody persistence and fail-closed integrity", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "sw-audit-ts-"));
    const storageFile = path.join(tmpDir, "audit_ledger.jsonl");

    try {
      // 1. First lifecycle: commit genesis + 2 roots
      const anchor1 = new AuditAnchor(storageFile);
      const r1 = sha256Digest("first_ts_commit");
      anchor1.appendRoot(r1, "tenant-ts-1");
      const r2 = sha256Digest("second_ts_commit");
      anchor1.appendRoot(r2, "tenant-ts-2");
      assert.equal(anchor1.totalEpochs, 3);

      // 2. Process restart: instantiate new anchor from existing durable file
      const anchorRestart = new AuditAnchor(storageFile);
      assert.equal(anchorRestart.totalEpochs, 3);
      assert.equal(anchorRestart.latestEpoch, 2);
      assert.equal(anchorRestart.latestDigest, anchor1.latestDigest);
      assert.equal(anchorRestart.auditChain().valid, true);

      // Append epoch 3
      const r3 = sha256Digest("third_ts_commit");
      const ep3 = anchorRestart.appendRoot(r3, "tenant-ts-3");
      assert.equal(ep3.epoch, 3);

      // 3. Tamper with historical epoch 1 on disk
      const lines = fs.readFileSync(storageFile, "utf-8").split("\n");
      const record1 = JSON.parse(lines[1]);
      record1.root_hash = sha256Digest("tampered_ts_root");
      lines[1] = JSON.stringify(record1);
      fs.writeFileSync(storageFile, lines.join("\n"), "utf-8");

      // 4. Instantiating new anchor must fail closed and throw AuditIntegrityError
      assert.throws(
        () => {
          new AuditAnchor(storageFile);
        },
        (err) => {
          return (
            err instanceof AuditIntegrityError &&
            (err.message.includes("Chain digest mismatch") ||
              err.message.includes("History rewriting detected"))
          );
        }
      );
    } finally {
      try {
        fs.rmSync(tmpDir, { recursive: true, force: true });
      } catch (e) {}
    }
  });

  test("Full auditChain check reports valid across chained epochs", () => {
    const anchor = new AuditAnchor(":memory:");
    for (let i = 0; i < 5; i++) {
      anchor.appendRoot(sha256Digest(`root_${i}`), `tenant_${i}`);
    }

    assert.equal(anchor.totalEpochs, 6); // 1 genesis + 5 epochs
    const audit = anchor.auditChain();
    assert.equal(audit.valid, true);
    assert.equal(audit.total_epochs, 6);
    assert.equal(audit.latest_epoch, 5);
    assert.equal(audit.errors.length, 0);
  });
});
