import { test } from "node:test";
import assert from "node:assert/strict";

import {
  MinHashSigner,
  SemanticDriftDetector,
  DivergenceStatus,
} from "../src/drift.ts";

test("MinHashSigner computes deterministic 64-element signatures with universal permutations", () => {
  const signer = new MinHashSigner();
  const text = "The quick brown fox jumps over the lazy dog";
  const shingles = signer.extractShingles(text);

  assert.ok(shingles.size > 0);
  assert.ok(Array.from(shingles).some((s) => s.startsWith("w1:")));
  assert.ok(Array.from(shingles).some((s) => s.startsWith("w2:")));

  const sig1 = signer.computeSignature(text);
  const sig2 = signer.computeSignature(text);

  assert.equal(sig1.length, 64);
  assert.equal(sig2.length, 64);
  for (let i = 0; i < 64; i++) {
    assert.equal(sig1[i], sig2[i]);
  }
  assert.equal(signer.jaccardSimilarity(sig1, sig2), 1.0);
});

test("MinHashSigner estimates higher similarity for topical overlap and lower for disjoint topics", () => {
  const signer = new MinHashSigner();
  const anchor = "Implement a secure PostgreSQL database schema with migration scripts";
  const similar = "Design an encrypted PostgreSQL relational schema and write SQL migration files";
  const unrelated = "Astronauts discovered ancient lunar rocks containing rare extraterrestrial crystals";

  const sigAnchor = signer.computeSignature(anchor);
  const sigSimilar = signer.computeSignature(similar);
  const sigUnrelated = signer.computeSignature(unrelated);

  const simHigh = signer.jaccardSimilarity(sigAnchor, sigSimilar);
  const simLow = signer.jaccardSimilarity(sigAnchor, sigUnrelated);

  assert.ok(simHigh > simLow);
  assert.ok(simLow < 0.15);
});

test("MinHashSigner computes accurate Vocabulary Compression Ratio (VCR)", () => {
  const variedText = "The architect designed scalable microservices for distributed cloud deployment across multiple regions";
  const { vcr: vcrHigh } = MinHashSigner.computeVcr(variedText);
  assert.ok(vcrHigh > 0.80);

  const echoText = "loop error failed retry loop error failed retry loop error failed retry loop error failed retry";
  const { vcr: vcrLow } = MinHashSigner.computeVcr(echoText);
  assert.ok(vcrLow < 0.35);
});

test("SemanticDriftDetector tracks turn progression and detects topic divergence", () => {
  const detector = new SemanticDriftDetector({ consecutiveDivergentLimit: 2 });
  const swarmId = "ts-drift-swarm-01";

  // Turn 1: Sets anchor
  const t1 = detector.evaluateTurn(
    swarmId,
    "Architect",
    "Build a high-performance Python FastAPI service with Redis caching and JWT authentication"
  );
  assert.equal(t1.turn_index, 1);
  assert.equal(t1.drift_score, 0.0);
  assert.equal(t1.divergence_status, "nominal");
  assert.equal(t1.is_circuit_broken, false);

  // Turn 2: Coherent follow-up
  const t2 = detector.evaluateTurn(
    swarmId,
    "BackendEngineer",
    "Configured FastAPI application with Redis connection pool and JWT bearer token middleware"
  );
  assert.equal(t2.turn_index, 2);
  assert.ok(t2.drift_score < 0.50);
  assert.ok(t2.divergence_status === "nominal" || t2.divergence_status === "warning");
  assert.equal(t2.is_circuit_broken, false);

  // Turn 3: Severe topic derailment
  const t3 = detector.evaluateTurn(
    swarmId,
    "RogueAgent",
    "Let us write poetry about medieval castles and knights defending the ancient fortress against dragons"
  );
  assert.equal(t3.turn_index, 3);
  assert.ok(t3.drift_score >= 0.70);
  assert.equal(t3.divergence_status, "divergent");
  assert.equal(t3.is_circuit_broken, false);

  // Turn 4: Second consecutive divergent turn -> Trips circuit!
  const t4 = detector.evaluateTurn(
    swarmId,
    "RogueAgent",
    "The wizard cast a powerful lightning spell illuminating the enchanted forest in mystical colors"
  );
  assert.equal(t4.turn_index, 4);
  assert.equal(t4.divergence_status, "tripped");
  assert.equal(t4.is_circuit_broken, true);
  assert.ok(t4.trip_reason?.includes("Semantic drift threshold breached"));
});

test("SemanticDriftDetector detects echo chamber collapse and halts execution", () => {
  const detector = new SemanticDriftDetector({
    minVcrThreshold: 0.25,
    vcrMinTokenLength: 20,
    consecutiveDivergentLimit: 1,
  });
  const swarmId = "ts-echo-swarm-01";

  detector.setAnchor(swarmId, "Analyze financial trading trends and arbitrage opportunities");

  const echoPayload = (
    "arbitrage trade error trade error trade error trade error " +
    "trade error trade error trade error trade error trade error trade error"
  );

  const turn = detector.evaluateTurn(swarmId, "TraderBot", echoPayload);
  assert.equal(turn.divergence_status, "tripped");
  assert.equal(turn.is_circuit_broken, true);
  assert.ok(turn.trip_reason?.toLowerCase().includes("echo chamber"));
});
