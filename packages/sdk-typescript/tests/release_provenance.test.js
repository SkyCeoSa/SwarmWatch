/**
 * Unit and integration test suite for Verifiable Release Provenance & Signed Artifact DAG (ADR 0080 / Seam 69).
 * Plain ES module for Node.js native execution.
 */

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  ArtifactDigest,
  ProvenanceStep,
  ProvenanceStepType,
  ProvenanceDag,
  SignedProvenanceManifest,
  ProvenanceVerificationResult,
  ProvenanceCyclicDependencyError,
  ProvenanceError,
  ReleaseProvenance,
  globalReleaseProvenance,
} from "../src/release_provenance.ts";
import { WitnessKeyManager } from "../src/root_witness.ts";

test("ArtifactDigest creation, hashing, and serialization", () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "sw-prov-test-"));
  const tempFile = path.join(tempDir, "sample-artifact.txt");
  fs.writeFileSync(tempFile, "Hello SwarmWatch SLSA Level 3 Provenance (TS)");

  try {
    const digest = ArtifactDigest.fromFile(tempFile);
    assert.equal(digest.byte_size, Buffer.byteLength("Hello SwarmWatch SLSA Level 3 Provenance (TS)"));
    assert.equal(digest.sha256.length, 64);
    assert.equal(digest.mime_type, "application/octet-stream");

    const dict = digest.toDict();
    const restored = ArtifactDigest.fromDict(dict);
    assert.equal(restored.path, digest.path);
    assert.equal(restored.sha256, digest.sha256);
    assert.equal(restored.byte_size, digest.byte_size);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test("ProvenanceStep digest deterministic calculation", () => {
  const step1 = new ProvenanceStep(
    "step-source-01",
    ProvenanceStepType.SOURCE_CHECKOUT,
    [],
    [],
    [],
    { git_branch: "main", clean: true }
  );
  assert.equal(step1.step_digest.length, 64);
  assert.equal(step1.step_digest, step1.computeDigest());

  const step2 = new ProvenanceStep(
    "step-source-01",
    ProvenanceStepType.SOURCE_CHECKOUT,
    [],
    [],
    [],
    { clean: true, git_branch: "main" }
  );
  assert.equal(step1.step_digest, step2.step_digest);
});

test("ProvenanceDag topological sort and DAG hash", () => {
  const dag = new ProvenanceDag();

  const s1 = new ProvenanceStep("s1", ProvenanceStepType.SOURCE_CHECKOUT);
  const s2 = new ProvenanceStep("s2", ProvenanceStepType.ENVIRONMENT_LOCK, ["s1"]);
  const s3 = new ProvenanceStep("s3", ProvenanceStepType.TEST_EXECUTION, ["s2"]);
  const s4 = new ProvenanceStep("s4", ProvenanceStepType.BUILD_PACKAGING, ["s3"]);

  dag.addStep(s1);
  dag.addStep(s2);
  dag.addStep(s3);
  dag.addStep(s4);

  const order = dag.topologicalSort();
  assert.deepEqual(order.map((s) => s.step_id), ["s1", "s2", "s3", "s4"]);

  const dagHash = dag.computeDagHash();
  assert.equal(dagHash.length, 64);
});

test("ProvenanceDag cyclic dependency rejection", () => {
  const dag = new ProvenanceDag();
  const s1 = new ProvenanceStep("s1", ProvenanceStepType.SOURCE_CHECKOUT);
  const s2 = new ProvenanceStep("s2", ProvenanceStepType.BUILD_PACKAGING, ["s1"]);
  dag.addStep(s1);
  dag.addStep(s2);

  const s3 = new ProvenanceStep("s3", ProvenanceStepType.TEST_EXECUTION, ["s2"]);
  dag.addStep(s3);

  // Inject cycle into parent_step_ids
  s2.parent_step_ids.push("s3");
  assert.throws(() => {
    dag.topologicalSort();
  }, ProvenanceCyclicDependencyError);
});

test("ProvenanceDag missing parent rejection", () => {
  const dag = new ProvenanceDag();
  const sOrphan = new ProvenanceStep("s_orphan", ProvenanceStepType.BUILD_PACKAGING, ["missing-parent"]);
  assert.throws(() => {
    dag.addStep(sOrphan);
  }, ProvenanceError);
});

test("generateManifest and verifyManifest signature pass", () => {
  const prov = new ReleaseProvenance();

  const s1 = prov.recordStep({
    step_type: ProvenanceStepType.SOURCE_CHECKOUT,
    metadata: { commit: "abc1234", branch: "main" },
  });
  const s2 = prov.recordStep({
    step_type: ProvenanceStepType.TEST_EXECUTION,
    parent_step_ids: [s1.step_id],
    metadata: { tests_passed: 1051, duration_s: 387.2 },
  });
  prov.recordStep({
    step_type: ProvenanceStepType.BUILD_PACKAGING,
    parent_step_ids: [s2.step_id],
    outputs: [new ArtifactDigest("dist/swarmwatch-0.1.0-py3-none-any.whl", "a".repeat(64), 489003, "application/x-wheel+zip")],
  });

  const manifest = prov.generateManifest({
    git_commit: "abc1234",
    git_branch: "main",
  });

  assert.equal(manifest.git_commit, "abc1234");
  assert.notEqual(manifest.signature, "");
  assert.equal(manifest.public_key_b64, prov.keyManager.publicKeyB64);
  assert.equal(manifest.steps.length, 3);
  assert.equal(manifest.products.length, 1);

  // Verify manifest
  const result = prov.verifyManifest({ manifest });
  assert.equal(result.is_valid, true);
  assert.equal(result.errors.length, 0);
  assert.equal(result.verified_steps_count, 3);
});

test("Tamper detection on manifest git_commit", () => {
  const prov = new ReleaseProvenance();
  prov.recordStep({ step_type: ProvenanceStepType.SOURCE_CHECKOUT });
  const manifest = prov.generateManifest({ git_commit: "clean-sha-01" });

  const tamperedDict = manifest.toDict();
  tamperedDict.git_commit = "tampered-sha-02";

  const result = prov.verifyManifest({ manifest: tamperedDict });
  assert.equal(result.is_valid, false);
  assert.ok(result.errors.some((e) => e.toLowerCase().includes("signature")));
});

test("Tamper detection on step digest corruption", () => {
  const prov = new ReleaseProvenance();
  prov.recordStep({
    step_type: ProvenanceStepType.SOURCE_CHECKOUT,
    metadata: { clean: true },
  });
  const manifest = prov.generateManifest({ git_commit: "sha-001" });

  const tamperedDict = manifest.toDict();
  tamperedDict.steps[0].metadata.clean = false;

  const result = prov.verifyManifest({ manifest: tamperedDict });
  assert.equal(result.is_valid, false);
  assert.ok(result.errors.some((e) => e.includes("Step digest corruption") || e.toLowerCase().includes("signature")));
});

test("Tamper detection on product file checksum on disk", () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "sw-prov-prod-"));
  const productPath = path.join(tempDir, "mock-package.whl");
  fs.writeFileSync(productPath, "Clean Build Bytes v0.1.0");

  try {
    const prov = new ReleaseProvenance();
    const prodDigest = ArtifactDigest.fromFile(productPath, tempDir);
    prov.recordStep({
      step_type: ProvenanceStepType.BUILD_PACKAGING,
      outputs: [prodDigest],
    });
    const manifest = prov.generateManifest({ git_commit: "build-sha-1" });

    // Clean verification
    const resClean = prov.verifyManifest({
      manifest,
      verifyDiskFiles: true,
      baseDir: tempDir,
    });
    assert.equal(resClean.is_valid, true);
    assert.equal(resClean.verified_products_count, 1);

    // Tamper with file
    fs.writeFileSync(productPath, "Malicious Injected Bytes");
    const resTampered = prov.verifyManifest({
      manifest,
      verifyDiskFiles: true,
      baseDir: tempDir,
    });
    assert.equal(resTampered.is_valid, false);
    assert.ok(resTampered.errors.some((e) => e.includes("checksum tampering")));
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test("Cross-key signature rejection", () => {
  const prov1 = new ReleaseProvenance();
  prov1.recordStep({ step_type: ProvenanceStepType.SOURCE_CHECKOUT });
  const manifest = prov1.generateManifest({ git_commit: "sha-key-test" });

  const alienKey = new WitnessKeyManager();
  const alienPub = alienKey.publicKeyB64;

  const res = prov1.verifyManifest({ manifest, publicKeyB64: alienPub });
  assert.equal(res.is_valid, false);
  assert.ok(res.errors.some((e) => e.toLowerCase().includes("signature mismatch")));
});

test("Sub-250us provenance verification SLO benchmark", () => {
  const prov = new ReleaseProvenance();
  const s1 = prov.recordStep({ step_type: ProvenanceStepType.SOURCE_CHECKOUT });
  const s2 = prov.recordStep({ step_type: ProvenanceStepType.TEST_EXECUTION, parent_step_ids: [s1.step_id] });
  prov.recordStep({ step_type: ProvenanceStepType.BUILD_PACKAGING, parent_step_ids: [s2.step_id] });
  const manifest = prov.generateManifest({ git_commit: "bench-sha-01" });

  // Warmup
  for (let i = 0; i < 5; i++) {
    prov.verifyManifest({ manifest });
  }

  // Benchmark: 500 iterations of full verification
  const iterations = 500;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const res = prov.verifyManifest({ manifest });
    assert.equal(res.is_valid, true);
  }
  const t1 = performance.now();

  const avgUs = ((t1 - t0) / iterations) * 1000;
  assert.ok(avgUs < 250.0, `Provenance verification took ${avgUs.toFixed(2)}µs, exceeding 250µs SLO`);
});

test("Global release provenance status and reset", () => {
  const st = globalReleaseProvenance.status();
  assert.ok("active_dag_step_count" in st);
  assert.equal(st.slsa_level, "SLSA_BUILD_LEVEL_3");

  globalReleaseProvenance.recordStep({ step_type: ProvenanceStepType.SOURCE_CHECKOUT });
  assert.ok(Number(globalReleaseProvenance.status().active_dag_step_count) >= 1);

  globalReleaseProvenance.reset();
  assert.equal(globalReleaseProvenance.status().active_dag_step_count, 0);
});
