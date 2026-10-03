/**
 * Test Suite for SwarmWatch Multi-Modal Cognitive Grounding & Vector Evidence Fusion (Seam 73 / ADR 0084).
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";
import {
  CognitiveGroundingEngine,
  CognitiveGroundingError,
  EvidenceModality,
  EvidenceNotFoundError,
  GroundingCircuitTrippedError,
  GroundingModalityMismatchError,
  GroundingReceipt,
  GroundingVerdict,
  UngroundedInferenceError,
  VectorEvidence,
  VectorEvidenceStore,
  computeCanonicalGroundingHash,
  computeDeterministicEmbedding,
  cosineSimilarity,
} from "../src/cognitive_grounding.ts";

test("Deterministic embedding properties and L2 norm", () => {
  const text = "The user has authorized a payment transfer of $500 for invoice #8921";
  const vec1 = computeDeterministicEmbedding(text, 64);
  const vec2 = computeDeterministicEmbedding(text, 64);

  assert.equal(vec1.length, 64);
  assert.deepEqual(vec1, vec2, "Embedding must be 100% deterministic");

  let normSq = 0;
  for (const x of vec1) {
    normSq += x * x;
  }
  const norm = Math.sqrt(normSq);
  assert.ok(Math.abs(norm - 1.0) < 1e-5, "Non-empty embedding must be L2-normalized to unit sphere");

  const emptyVec = computeDeterministicEmbedding("", 64);
  assert.ok(emptyVec.every((x) => x === 0.0));
});

test("Cosine similarity bounds and self-similarity", () => {
  const vecA = computeDeterministicEmbedding("Autonomous agent swarm safety verification", 64);
  const vecB = computeDeterministicEmbedding("Autonomous agent swarm safety verification", 64);
  const vecC = computeDeterministicEmbedding("Completely unrelated botanical biology photosynthesis cell", 64);

  const simSelf = cosineSimilarity(vecA, vecB);
  assert.ok(Math.abs(simSelf - 1.0) < 1e-5, `Self-similarity must be ~1.0, got ${simSelf}`);

  const simDiff = cosineSimilarity(vecA, vecC);
  assert.ok(simDiff >= -1.0 && simDiff <= 1.0);
  assert.ok(simDiff < 0.40, `Unrelated text should have low similarity, got ${simDiff}`);
});

test("Canonical grounding hash computation", () => {
  const h = computeCanonicalGroundingHash({
    receiptId: "r1",
    claimId: "c1",
    tenantId: "t1",
    verdict: "GROUNDED",
    overallScore: 0.9058,
    matchedEvidenceIds: ["e1"],
  });
  assert.equal(h, "sw:ground:v1:df2a70fbfbda1d432192ee192aab7f66d4993e5f34022aabad1b04f123a29f47");
});

test("VectorEvidenceStore CRUD operations", () => {
  const store = new VectorEvidenceStore(64);
  assert.equal(store.count(), 0);

  const ev1 = new VectorEvidence({
    evidenceId: "ev-1",
    tenantId: "tenant-01",
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Ground truth text assertion",
    embedding: computeDeterministicEmbedding("Ground truth text assertion", 64),
    sourceReference: "snap-1",
  });
  const ev2 = new VectorEvidence({
    evidenceId: "ev-2",
    tenantId: "tenant-01",
    modality: EvidenceModality.ACTION_RECEIPT,
    content: "Action receipt payload",
    embedding: computeDeterministicEmbedding("Action receipt payload", 64),
    sourceReference: "rcpt-1",
  });
  const evOther = new VectorEvidence({
    evidenceId: "ev-3",
    tenantId: "tenant-02",
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Other tenant evidence",
    embedding: computeDeterministicEmbedding("Other tenant evidence", 64),
    sourceReference: "snap-2",
  });

  store.add(ev1);
  store.add(ev2);
  store.add(evOther);

  assert.equal(store.count("tenant-01"), 2);
  assert.equal(store.count("tenant-02"), 1);
  assert.equal(store.count(), 3);

  const textItems = store.listAll("tenant-01", EvidenceModality.TEXT_CLAIM);
  assert.equal(textItems.length, 1);
  assert.equal(textItems[0].evidenceId, "ev-1");

  assert.equal(store.remove("ev-1", "tenant-01"), true);
  assert.equal(store.get("ev-1", "tenant-01"), undefined);
  assert.equal(store.count("tenant-01"), 1);

  store.clear("tenant-01");
  assert.equal(store.count("tenant-01"), 0);
  assert.equal(store.count("tenant-02"), 1);
});

test("Evidence registration across all modalities", () => {
  const engine = new CognitiveGroundingEngine();

  const evText = engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Current database replica sync lag is 12ms",
    sourceReference: "doc-health",
  });
  const evVis = engine.registerEvidence({
    modality: EvidenceModality.VISUAL_ARTIFACT,
    content: "ui_login_screenshot_artifact_sha256_watermark_89a1b2",
    sourceReference: "art-01",
  });
  const evAct = engine.registerEvidence({
    modality: EvidenceModality.ACTION_RECEIPT,
    content: "Action execution receipt for payment refund #440",
    sourceReference: "rcpt-440",
  });
  const evWit = engine.registerEvidence({
    modality: EvidenceModality.ROOT_WITNESS,
    content: "Root witness log-free signed checkpoint epoch 42",
    sourceReference: "wit-42",
  });
  const evTel = engine.registerEvidence({
    modality: EvidenceModality.STRUCTURED_TELEMETRY,
    content: "Telemetry span status=OK duration=45us turns=3",
    sourceReference: "span-99",
  });

  assert.equal(evText.modality, EvidenceModality.TEXT_CLAIM);
  assert.equal(evVis.modality, EvidenceModality.VISUAL_ARTIFACT);
  assert.equal(evAct.modality, EvidenceModality.ACTION_RECEIPT);
  assert.equal(evWit.modality, EvidenceModality.ROOT_WITNESS);
  assert.equal(evTel.modality, EvidenceModality.STRUCTURED_TELEMETRY);

  assert.equal(engine.store.count(), 5);
});

test("Verify grounding verdicts", () => {
  const engine = new CognitiveGroundingEngine({
    defaultThreshold: 0.70,
    partialThreshold: 0.45,
    driftThreshold: 0.15,
  });

  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "The server cluster has 16 worker nodes online across us-east and eu-west.",
    sourceReference: "cluster-inv",
  });

  // 1. Exact / near match -> GROUNDED
  const rcptGrounded = engine.verifyGrounding({
    claim: "16 worker nodes online across us-east and eu-west",
  });
  assert.equal(rcptGrounded.verdict, GroundingVerdict.GROUNDED);
  assert.ok(rcptGrounded.overallScore >= 0.70);
  assert.ok(rcptGrounded.canonicalHash.startsWith("sw:ground:v1:"));

  // 2. Moderate overlap -> PARTIALLY_GROUNDED
  const rcptPartial = engine.verifyGrounding({
    claim: "cluster worker nodes online",
  });
  assert.ok(
    rcptPartial.verdict === GroundingVerdict.GROUNDED ||
    rcptPartial.verdict === GroundingVerdict.PARTIALLY_GROUNDED
  );
  assert.ok(rcptPartial.overallScore >= 0.45);

  // 3. Completely unrelated -> UNGROUNDED or DRIFT_DETECTED
  const rcptUnrelated = engine.verifyGrounding({
    claim: "quantum photosynthesis chlorophyll in tropical rainforest flora",
  });
  assert.ok(
    rcptUnrelated.verdict === GroundingVerdict.UNGROUNDED ||
    rcptUnrelated.verdict === GroundingVerdict.DRIFT_DETECTED
  );
  assert.ok(rcptUnrelated.overallScore < 0.45);
});

test("Required modalities filtering and mismatch handling", () => {
  const engine = new CognitiveGroundingEngine();
  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Policy rule restricts API calls to read-only endpoints.",
    sourceReference: "pol-01",
  });

  const rcpt = engine.verifyGrounding({
    claim: "Policy rule restricts API calls",
    requiredModalities: [EvidenceModality.VISUAL_ARTIFACT],
    strict: false,
  });
  assert.equal(rcpt.verdict, GroundingVerdict.UNGROUNDED);

  assert.throws(
    () => {
      engine.verifyGrounding({
        claim: "Policy rule restricts API calls",
        requiredModalities: [EvidenceModality.VISUAL_ARTIFACT],
        strict: true,
      });
    },
    GroundingModalityMismatchError
  );
});

test("Strict mode rejection on ungrounded inference", () => {
  const engine = new CognitiveGroundingEngine();
  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "System memory limit is 8192 MB.",
    sourceReference: "sys-cfg",
  });

  assert.throws(
    () => {
      engine.verifyGrounding({
        claim: "User has authorized transferring 50 bitcoins to unknown wallet",
        strict: true,
      });
    },
    UngroundedInferenceError
  );
});

test("Multi-modal evidence fusion with custom weights", () => {
  const engine = new CognitiveGroundingEngine();
  const ev1 = engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Invoice #1001 total is $450.00",
    sourceReference: "doc-inv",
  });
  const ev2 = engine.registerEvidence({
    modality: EvidenceModality.ACTION_RECEIPT,
    content: "Credit card charge authorized for invoice #1001 amount $450.00",
    sourceReference: "rcpt-1001",
  });

  const rcpt = engine.fuseEvidence({
    claim: "Invoice #1001 total is $450.00 credit card authorized",
    evidenceIds: [ev1.evidenceId, ev2.evidenceId],
    modalityWeights: {
      TEXT_CLAIM: 0.4,
      ACTION_RECEIPT: 0.6,
    },
  });

  assert.equal(rcpt.verdict, GroundingVerdict.GROUNDED);
  assert.ok(rcpt.overallScore > 0.70);
  assert.equal(rcpt.matchedEvidenceIds.length, 2);
});

test("Fail-closed safety circuit coupling", () => {
  const engine = new CognitiveGroundingEngine();
  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Server health is healthy",
    sourceReference: "doc-1",
  });

  const rcpt = engine.verifyGrounding({ claim: "Server health is healthy" });
  assert.equal(rcpt.verdict, GroundingVerdict.GROUNDED);

  engine.tripCircuit();
  assert.equal(engine.isCircuitTripped(), true);

  assert.throws(() => {
    engine.verifyGrounding({ claim: "Server health is healthy" });
  }, GroundingCircuitTrippedError);

  assert.throws(() => {
    engine.fuseEvidence({
      claim: "Server health is healthy",
      evidenceIds: [engine.store.listAll()[0].evidenceId],
    });
  }, GroundingCircuitTrippedError);

  engine.resetCircuit();
  assert.equal(engine.isCircuitTripped(), false);

  const rcptReset = engine.verifyGrounding({ claim: "Server health is healthy" });
  assert.equal(rcptReset.verdict, GroundingVerdict.GROUNDED);
});

test("Consecutive drift detection auto-trips circuit", () => {
  const engine = new CognitiveGroundingEngine({
    maxDriftCountBeforeTrip: 3,
    defaultThreshold: 0.70,
    partialThreshold: 0.45,
    driftThreshold: 0.15,
  });

  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Kubernetes cluster nodes are running version 1.30",
    sourceReference: "k8s-spec",
  });

  assert.equal(engine.isCircuitTripped(), false);

  engine.verifyGrounding({ claim: "astrophysics planetary orbits around betelgeuse" });
  assert.equal(engine.isCircuitTripped(), false);

  engine.verifyGrounding({ claim: "ancient mesopotamian cuneiform tablets translation" });
  assert.equal(engine.isCircuitTripped(), false);

  engine.verifyGrounding({ claim: "deep sea hydrothermal vent marine microorganisms" });
  assert.equal(engine.isCircuitTripped(), true);

  assert.throws(() => {
    engine.verifyGrounding({ claim: "Kubernetes cluster nodes are running version 1.30" });
  }, GroundingCircuitTrippedError);
});

test("Receipts audit and status reporting", () => {
  const engine = new CognitiveGroundingEngine();
  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Database credentials rotated successfully",
    sourceReference: "sec-audit",
  });

  const rcpt = engine.verifyGrounding({ claim: "Database credentials rotated" });
  assert.equal(engine.getReceipts().length, 1);

  const fetched = engine.getReceipt(rcpt.receiptId);
  assert.ok(fetched !== undefined);
  assert.equal(fetched.canonicalHash, rcpt.canonicalHash);

  const st = engine.status();
  assert.equal(st.total_evidence_count, 1);
  assert.equal(st.total_verifications, 1);
  assert.equal(st.circuit_tripped, false);
  assert.equal(st.status_badge, "GROUNDING: FUSION OK");
});

test("Sub-500us execution benchmark", () => {
  const engine = new CognitiveGroundingEngine();
  for (let i = 0; i < 10; i++) {
    engine.registerEvidence({
      modality: EvidenceModality.TEXT_CLAIM,
      content: `Telemetry observation index ${i} node status OK memory ${i * 100}MB`,
      sourceReference: `obs-${i}`,
    });
  }

  const durationsUs = [];
  for (let i = 0; i < 100; i++) {
    const t0 = performance.now();
    engine.verifyGrounding({ claim: "Telemetry observation index 5 node status OK" });
    const elapsedUs = (performance.now() - t0) * 1000.0;
    durationsUs.push(elapsedUs);
  }

  const mean = durationsUs.reduce((a, b) => a + b, 0) / durationsUs.length;
  assert.ok(mean < 500.0, `Expected <500us execution latency, got ${mean.toFixed(2)}us`);
});
