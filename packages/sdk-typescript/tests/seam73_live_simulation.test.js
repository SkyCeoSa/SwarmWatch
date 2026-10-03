/**
 * Automated Test Harness for Seam 73 Live Cognitive Grounding Simulation (Node.js Native ESM).
 * Validates dual-SDK parity for all 8 operational phases.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  CognitiveGroundingEngine,
  EvidenceModality,
  GroundingVerdict,
  computeDeterministicEmbedding,
  cosineSimilarity,
  computeCanonicalGroundingHash,
  UngroundedInferenceError,
  GroundingModalityMismatchError,
  GroundingCircuitTrippedError,
} from "../src/cognitive_grounding.ts";

test("Seam 73 Live Cognitive Grounding & Vector Fusion Full Lifecycle Simulation", () => {
  const engine = new CognitiveGroundingEngine({
    defaultThreshold: 0.70,
    partialThreshold: 0.45,
    driftThreshold: 0.15,
    maxDriftCountBeforeTrip: 3,
  });

  // --------------------------------------------------------------------------
  // Phase 1: Multi-Modal Evidence Registration (5 discrete modalities)
  // --------------------------------------------------------------------------
  const evText = engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Database schema migration completed successfully for cluster-us-east-1 table shards",
    sourceReference: "migration-log-9021",
  });
  assert.equal(evText.modality, EvidenceModality.TEXT_CLAIM);
  assert.equal(evText.embedding.length, 64);

  const evVisual = engine.registerEvidence({
    modality: EvidenceModality.VISUAL_ARTIFACT,
    content: "dashboard_screenshot_2026_q3_cluster_health_green_metrics_panel",
    sourceReference: "s3://audit-bucket/screenshots/dash-q3.png",
  });
  assert.equal(evVisual.modality, EvidenceModality.VISUAL_ARTIFACT);

  const evAction = engine.registerEvidence({
    modality: EvidenceModality.ACTION_RECEIPT,
    content: "POST /api/v1/shards/migrate response 200 OK duration 412ms commit-hash 9fbc012",
    sourceReference: "audit-receipt-8841",
  });
  assert.equal(evAction.modality, EvidenceModality.ACTION_RECEIPT);

  const evWitness = engine.registerEvidence({
    modality: EvidenceModality.ROOT_WITNESS,
    content: "Raft consensus quorum commit at index 104291 leader-node-4 ed25519-sig-valid",
    sourceReference: "raft-commit-sig-104291",
  });
  assert.equal(evWitness.modality, EvidenceModality.ROOT_WITNESS);

  const evTelemetry = engine.registerEvidence({
    modality: EvidenceModality.STRUCTURED_TELEMETRY,
    content: "node_cpu_utilization 0.24 node_memory_rss_bytes 4294967296 error_rate 0.000",
    sourceReference: "otel-metric-stream-01",
  });
  assert.equal(evTelemetry.modality, EvidenceModality.STRUCTURED_TELEMETRY);

  const allEvidence = engine.store.listAll();
  assert.equal(allEvidence.length, 5);

  // --------------------------------------------------------------------------
  // Phase 2: Deterministic In-Process Vector Embedding (Zero-Token)
  // --------------------------------------------------------------------------
  const query = "Database schema migration completed successfully for cluster-us-east-1 table shards";
  const vec1 = computeDeterministicEmbedding(query, 64);
  const vec2 = computeDeterministicEmbedding(query, 64);
  assert.deepEqual(vec1, vec2);
  assert.equal(vec1.length, 64);

  // Unit vector norm check
  const normSq = vec1.reduce((sum, v) => sum + v * v, 0);
  assert.ok(Math.abs(normSq - 1.0) < 1e-4);

  // Cosine similarity self-match
  const selfSim = cosineSimilarity(vec1, vec2);
  assert.ok(Math.abs(selfSim - 1.0) < 1e-4);

  // --------------------------------------------------------------------------
  // Phase 3: Exact Cosine Metric Space & Grounding Verdict Lattice
  // --------------------------------------------------------------------------
  const receipt1 = engine.verifyGrounding({
    claim: "Database schema migration completed successfully for cluster-us-east-1 table shards",
  });
  assert.equal(receipt1.verdict, GroundingVerdict.GROUNDED);
  assert.ok(receipt1.overallScore >= 0.70);
  assert.ok(receipt1.receiptId.startsWith("rcpt-ground-"));
  assert.ok(receipt1.canonicalHash.startsWith("sw:ground:v1:"));

  const receipt2 = engine.verifyGrounding({
    claim: "Database migration finished for some cluster shards in east",
  });
  assert.equal(receipt2.verdict, GroundingVerdict.PARTIALLY_GROUNDED);
  assert.ok(receipt2.overallScore >= 0.45 && receipt2.overallScore < 0.70);

  const receipt3 = engine.verifyGrounding({
    claim: "Quantum teleportation protocol synthesized artificial gravitational wave",
  });
  assert.ok(
    receipt3.verdict === GroundingVerdict.UNGROUNDED ||
    receipt3.verdict === GroundingVerdict.DRIFT_DETECTED
  );
  assert.ok(receipt3.overallScore < 0.45);

  // --------------------------------------------------------------------------
  // Phase 4: Cross-Modal Evidentiary Fusion Algebra
  // --------------------------------------------------------------------------
  const fusedReceipt = engine.fuseEvidence({
    claim: "Database schema migration completed with Raft quorum commit and 200 OK receipt",
    evidenceIds: [evText.evidenceId, evAction.evidenceId, evWitness.evidenceId],
    modalityWeights: {
      [EvidenceModality.TEXT_CLAIM]: 0.4,
      [EvidenceModality.ACTION_RECEIPT]: 0.3,
      [EvidenceModality.ROOT_WITNESS]: 0.3,
    },
  });
  assert.ok(fusedReceipt.overallScore >= 0.40);
  assert.ok(fusedReceipt.canonicalHash.startsWith("sw:ground:v1:"));

  // --------------------------------------------------------------------------
  // Phase 5: Strict Mode & Required Modality Fencing
  // --------------------------------------------------------------------------
  engine.resetCircuit();

  assert.throws(
    () => engine.verifyGrounding({
      claim: "Quantum teleportation alien spacecraft detected",
      strict: true,
    }),
    (err) => err instanceof UngroundedInferenceError
  );

  engine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Account #4109 credit limit verified",
    sourceReference: "doc-credit",
    tenantId: "tenant-beta",
  });

  assert.throws(
    () => engine.verifyGrounding({
      claim: "Account #4109 credit limit",
      requiredModalities: [EvidenceModality.VISUAL_ARTIFACT],
      tenantId: "tenant-beta",
      strict: true,
    }),
    (err) => err instanceof GroundingModalityMismatchError
  );

  // --------------------------------------------------------------------------
  // Phase 6: Runaway Cognitive Drift Detection & Auto-Trip Governor
  // --------------------------------------------------------------------------
  const driftEngine = new CognitiveGroundingEngine({
    defaultThreshold: 0.70,
    partialThreshold: 0.45,
    driftThreshold: 0.15,
    maxDriftCountBeforeTrip: 3,
  });
  driftEngine.registerEvidence({
    modality: EvidenceModality.TEXT_CLAIM,
    content: "Fixed production config",
    sourceReference: "ref-0",
  });

  driftEngine.verifyGrounding({ claim: "Martian colony oxygen generator malfunction" });
  assert.equal(driftEngine.isCircuitTripped(), false);

  driftEngine.verifyGrounding({ claim: "Sub-atomic tachyon particle communication beam" });
  assert.equal(driftEngine.isCircuitTripped(), false);

  driftEngine.verifyGrounding({ claim: "Interdimensional warp drive engaged at hyperspace" });
  assert.equal(driftEngine.isCircuitTripped(), true);

  // --------------------------------------------------------------------------
  // Phase 7: Fail-Closed Safety Circuit Coupling
  // --------------------------------------------------------------------------
  assert.throws(
    () => driftEngine.verifyGrounding({ claim: "Any further claim after circuit trip" }),
    (err) => err instanceof GroundingCircuitTrippedError
  );

  driftEngine.resetCircuit();
  assert.equal(driftEngine.isCircuitTripped(), false);

  // --------------------------------------------------------------------------
  // Phase 8: Microsecond Precision Latency Benchmark & Receipts Generation
  // --------------------------------------------------------------------------
  const benchEngine = new CognitiveGroundingEngine();
  for (let i = 0; i < 20; i++) {
    benchEngine.registerEvidence({
      modality: EvidenceModality.TEXT_CLAIM,
      content: `Telemetry observation ${i}: node operational latency ${i * 5}us CPU load ${i * 2}%`,
      sourceReference: `span-${i}`,
    });
  }

  const iterations = 500;
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const r = benchEngine.verifyGrounding({
      claim: `Telemetry observation ${i % 20}: node operational latency`,
    });
    assert.ok(r.canonicalHash.startsWith("sw:ground:v1:"));
  }
  const t1 = performance.now();
  const totalMs = t1 - t0;
  const avgUs = (totalMs / iterations) * 1000.0;

  assert.ok(avgUs < 250.0, `Grounding verification latency exceeded 250µs SLO: ${avgUs.toFixed(2)}µs`);

  const st = benchEngine.status();
  assert.equal(st.total_evidence_count, 20);
  assert.equal(st.total_verifications, iterations);
  assert.equal(st.circuit_tripped, false);
});
