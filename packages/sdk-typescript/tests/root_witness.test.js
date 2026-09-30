/**
 * Tests for Independent Root Witness & Public-Key Chains (Seam 64 / ADR 0075).
 * Verifies asymmetric ECDSA (NIST P-256 / ES256) signing, RFC 6962 Certificate
 * Transparency logarithmic consistency proofs, rollback and whole-chain replacement
 * attack detection, NIST SP 800-57 verifiable key rotation logs, and sub-250µs verification.
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

import {
  WitnessError,
  RollbackAttackError,
  ConsistencyVerificationError,
  SplitBrainForkError,
  KeyRotationError,
  WitnessKeyManager,
  LeafReceipt,
  SignedCheckpoint,
  ConsistencyProof,
  KeyRotationRecord,
  WitnessStatus,
  RootWitness,
  globalRootWitness,
} from "../src/root_witness.ts";

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "swarmwatch_ts_witness_"));
}

test("Seam 64: Independent Root Witness & Public-Key Chains (ADR 0075)", async (t) => {
  await t.test("Witness key generation, export, and re-import", () => {
    const km = WitnessKeyManager.generate();
    assert.ok(km.publicKeyB64);
    assert.ok(km.privateKeyB64);

    const rawPub = km.getRawPublicKey();
    assert.strictEqual(rawPub.length, 65);
    assert.strictEqual(rawPub[0], 0x04);

    const verifier = WitnessKeyManager.fromPublicKey(km.publicKeyB64);
    assert.strictEqual(verifier.publicKeyB64, km.publicKeyB64);
    assert.deepStrictEqual(verifier.getRawPublicKey(), rawPub);
  });

  await t.test("Asymmetric signing and verification", () => {
    const km = WitnessKeyManager.generate();
    const msg = "SwarmWatch Checkpoint Root Anchor";

    const sig = km.sign(msg);
    assert.strictEqual(sig.length, 64); // IEEE P1363

    assert.strictEqual(km.verify(msg, sig), true);
    assert.strictEqual(km.verify("Tampered message", sig), false);

    const verifier = WitnessKeyManager.fromPublicKey(km.publicKeyB64);
    assert.strictEqual(verifier.verify(msg, sig), true);
    assert.strictEqual(verifier.verify("Tampered message", sig), false);

    const otherKm = WitnessKeyManager.generate();
    assert.strictEqual(otherKm.verify(msg, sig), false);
  });

  await t.test("Leaf submission and receipt", () => {
    const witness = new RootWitness();
    assert.strictEqual(witness.currentTreeSize, 0);

    const r1 = witness.submit("span-entropy-001");
    assert.ok(r1 instanceof LeafReceipt);
    assert.strictEqual(r1.leafIndex, 0);
    assert.strictEqual(r1.leafHash.length, 64);
    assert.strictEqual(witness.currentTreeSize, 1);

    const r2 = witness.submit(Buffer.from("span-trip-002"));
    assert.strictEqual(r2.leafIndex, 1);
    assert.notStrictEqual(r2.leafHash, r1.leafHash);
    assert.strictEqual(witness.currentTreeSize, 2);
  });

  await t.test("Signed checkpoint creation, monotonic epochs, and hash chain", () => {
    const witness = new RootWitness();

    const cp0 = witness.checkpoint({ description: "Genesis Checkpoint" });
    assert.ok(cp0 instanceof SignedCheckpoint);
    assert.strictEqual(cp0.epoch, 0);
    assert.strictEqual(cp0.treeSize, 0);
    assert.strictEqual(cp0.prevCheckpointHash, "0".repeat(64));
    assert.strictEqual(cp0.checkpointHash.length, 64);
    assert.ok(cp0.signature.length > 0);
    assert.strictEqual(RootWitness.verifyCheckpoint(cp0), true);

    witness.submit("leaf-1");
    witness.submit("leaf-2");
    const cp1 = witness.checkpoint({ reason: "Epoch 1 batch" });
    assert.strictEqual(cp1.epoch, 1);
    assert.strictEqual(cp1.treeSize, 2);
    assert.strictEqual(cp1.prevCheckpointHash, cp0.checkpointHash);
    assert.strictEqual(RootWitness.verifyCheckpoint(cp1), true);

    witness.submit("leaf-3");
    const cp2 = witness.checkpoint();
    assert.strictEqual(cp2.epoch, 2);
    assert.strictEqual(cp2.treeSize, 3);
    assert.strictEqual(cp2.prevCheckpointHash, cp1.checkpointHash);
    assert.strictEqual(RootWitness.verifyCheckpoint(cp2), true);
  });

  await t.test("RFC 6962 logarithmic consistency proof generation and verification", () => {
    const witness = new RootWitness();

    for (let i = 0; i < 15; i++) {
      witness.submit(`tx-leaf-${i}`);
    }
    const cpSmall = witness.checkpoint(); // epoch 0, treeSize 15

    for (let i = 15; i < 35; i++) {
      witness.submit(`tx-leaf-${i}`);
    }
    const cpLarge = witness.checkpoint(); // epoch 1, treeSize 35

    const proof = witness.getConsistencyProof(cpSmall.treeSize, cpLarge.treeSize);
    assert.ok(proof instanceof ConsistencyProof);
    assert.strictEqual(proof.oldSize, 15);
    assert.strictEqual(proof.newSize, 35);
    assert.strictEqual(proof.oldRoot, cpSmall.rootHash);
    assert.strictEqual(proof.newRoot, cpLarge.rootHash);
    assert.ok(proof.proofHashes.length > 0);

    const valid = RootWitness.verifyConsistency(cpSmall, cpLarge, proof);
    assert.strictEqual(valid, true);

    const sameProof = witness.getConsistencyProof(cpLarge.treeSize, cpLarge.treeSize);
    assert.strictEqual(sameProof.proofHashes.length, 0);
    assert.strictEqual(RootWitness.verifyConsistency(cpLarge, cpLarge, sameProof), true);

    const tamperedHashes = [...proof.proofHashes];
    tamperedHashes[0] = "f".repeat(64);
    const tamperedProof = new ConsistencyProof({
      oldSize: proof.oldSize,
      newSize: proof.newSize,
      oldRoot: proof.oldRoot,
      newRoot: proof.newRoot,
      proofHashes: tamperedHashes,
    });

    assert.throws(
      () => RootWitness.verifyConsistency(cpSmall, cpLarge, tamperedProof),
      ConsistencyVerificationError
    );
  });

  await t.test("Rollback attack detection", () => {
    const witness = new RootWitness();
    witness.submit("data-1");
    const cp1 = witness.checkpoint(); // epoch 0, size 1

    witness.submit("data-2");
    const cp2 = witness.checkpoint(); // epoch 1, size 2

    assert.throws(
      () => witness.ingestExternalCheckpoint(cp1),
      RollbackAttackError
    );

    const nowNs = Date.now() * 1_000_000;
    const forgedHash = SignedCheckpoint.computeCheckpointHash(
      2,
      1, // Truncated size!
      cp1.rootHash,
      cp2.checkpointHash,
      nowNs,
      witness.keyManager.publicKeyB64
    );
    const forgedSig = witness.keyManager.signB64(Buffer.from(forgedHash, "utf-8"));
    const forgedCp = new SignedCheckpoint({
      epoch: 2,
      treeSize: 1,
      rootHash: cp1.rootHash,
      prevCheckpointHash: cp2.checkpointHash,
      checkpointHash: forgedHash,
      witnessPublicKey: witness.keyManager.publicKeyB64,
      signature: forgedSig,
      timestampNs: nowNs,
    });

    assert.throws(
      () => witness.ingestExternalCheckpoint(forgedCp),
      RollbackAttackError
    );
  });

  await t.test("Split-brain fork attack detection", () => {
    const witness1 = new RootWitness();
    witness1.submit("branch-a-1");
    witness1.checkpoint(); // epoch 0

    const witness2 = new RootWitness();
    witness2.submit("branch-b-0");
    witness2.checkpoint(); // epoch 0
    witness2.submit("branch-b-1");
    const cp2Divergent = witness2.checkpoint(); // epoch 1

    assert.throws(
      () => witness1.ingestExternalCheckpoint(cp2Divergent),
      SplitBrainForkError
    );
  });

  await t.test("Public checkpoint verification with zero knowledge of private key", () => {
    const witness = new RootWitness();
    witness.submit("public-evidence-data");
    const cp = witness.checkpoint();

    const pubKey = cp.witnessPublicKey;
    assert.strictEqual(RootWitness.verifyCheckpoint(cp, pubKey), true);

    const d1 = cp.toDict();
    d1.root_hash = "e".repeat(64);
    const tamperedRoot = SignedCheckpoint.fromDict(d1);
    assert.strictEqual(RootWitness.verifyCheckpoint(tamperedRoot, pubKey), false);

    const d2 = cp.toDict();
    d2.epoch = 999;
    const tamperedEpoch = SignedCheckpoint.fromDict(d2);
    assert.strictEqual(RootWitness.verifyCheckpoint(tamperedEpoch, pubKey), false);
  });

  await t.test("NIST SP 800-57 verifiable key rotation log", () => {
    const witness = new RootWitness();
    const initialPub = witness.keyManager.publicKeyB64;

    witness.submit("leaf-before-rotation");
    const cp1 = witness.checkpoint();

    const newKm = WitnessKeyManager.generate();
    const newPub = newKm.publicKeyB64;

    const rotationRec = witness.rotateKey(newKm);
    assert.ok(rotationRec instanceof KeyRotationRecord);
    assert.strictEqual(rotationRec.oldPublicKey, initialPub);
    assert.strictEqual(rotationRec.newPublicKey, newPub);
    assert.strictEqual(rotationRec.rotationEpoch, cp1.epoch + 1);

    assert.strictEqual(RootWitness.verifyRotationRecord(rotationRec), true);

    witness.submit("leaf-after-rotation");
    const cp2 = witness.checkpoint();
    assert.strictEqual(cp2.witnessPublicKey, newPub);
    assert.strictEqual(RootWitness.verifyCheckpoint(cp2), true);

    assert.strictEqual(witness.verifyRotationLog(initialPub), true);

    const badRecord = KeyRotationRecord.fromDict(rotationRec.toDict());
    badRecord.newPublicKey = WitnessKeyManager.generate().publicKeyB64;
    assert.throws(
      () => RootWitness.verifyRotationRecord(badRecord),
      KeyRotationError
    );
  });

  await t.test("Witness durability and checkpoint journal reload", () => {
    const tempDir = createTempDir();
    const storagePath = path.join(tempDir, "witness_checkpoints.jsonl");

    const w1 = new RootWitness({ storagePath });
    w1.submit("record-alpha");
    w1.checkpoint();
    w1.submit("record-beta");
    const cp1 = w1.checkpoint();
    assert.strictEqual(w1.totalCheckpoints, 2);

    const w2 = new RootWitness({ storagePath, keyManager: w1.keyManager });
    assert.strictEqual(w2.totalCheckpoints, 2);
    assert.strictEqual(w2.latestEpoch, 1);
    assert.strictEqual(w2.currentTreeSize, 2);
    assert.strictEqual(w2.getLatestCheckpoint()?.checkpointHash, cp1.checkpointHash);

    w2.submit("record-gamma");
    const cp2 = w2.checkpoint();
    assert.strictEqual(cp2.epoch, 2);
    assert.strictEqual(cp2.prevCheckpointHash, cp1.checkpointHash);

    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  await t.test("Sub-250 microsecond consistency verification SLO", () => {
    const witness = new RootWitness();
    for (let i = 0; i < 64; i++) {
      witness.submit(`perf-leaf-${i}`);
    }

    const proof = witness.getConsistencyProof(32, 64);
    const cp1 = new SignedCheckpoint({
      epoch: 0,
      treeSize: 32,
      rootHash: proof.oldRoot,
      prevCheckpointHash: "",
      checkpointHash: "",
      witnessPublicKey: "",
      signature: "",
      timestampNs: 0,
    });
    const cp2 = new SignedCheckpoint({
      epoch: 1,
      treeSize: 64,
      rootHash: proof.newRoot,
      prevCheckpointHash: "",
      checkpointHash: "",
      witnessPublicKey: "",
      signature: "",
      timestampNs: 0,
    });

    // Warmup
    for (let i = 0; i < 10; i++) {
      RootWitness.verifyConsistency(cp1, cp2, proof);
    }

    let bestAvgUs = Infinity;
    const iterations = 500;

    for (let r = 0; r < 3; r++) {
      const t0 = process.hrtime.bigint();
      for (let i = 0; i < iterations; i++) {
        RootWitness.verifyConsistency(cp1, cp2, proof);
      }
      const t1 = process.hrtime.bigint();
      const avgUs = Number(t1 - t0) / (iterations * 1000);
      if (avgUs < bestAvgUs) {
        bestAvgUs = avgUs;
      }
    }

    assert.ok(
      bestAvgUs < 250.0,
      `Consistency verification took ${bestAvgUs.toFixed(2)}µs, exceeding 250µs SLO`
    );
  });

  await t.test("Operational status and global singleton", () => {
    const status = globalRootWitness.status();
    assert.ok(status instanceof WitnessStatus);
    assert.ok(status.latestEpoch >= -1);
    assert.ok(status.activePublicKey.length > 0);

    const d = status.toDict();
    assert.ok("active_public_key" in d);
    assert.ok("latest_epoch" in d);
    assert.ok("current_tree_size" in d);
  });
});
