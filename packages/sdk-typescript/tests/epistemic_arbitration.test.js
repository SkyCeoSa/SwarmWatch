/**
 * Test Suite for SwarmWatch Multi-Cluster Epistemic Proof Verification & Automated Cross-Swarm Dispute Resolution (Seam 76 / ADR 0087).
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  EpistemicModality,
  ClaimPolarity,
  DeductionRule,
  DisputeStatus,
  ArbitrationVerdict,
  EpistemicProofDAG,
  EpistemicArbitrationEngine,
  EpistemicArbitrationError,
  DisputeCircuitTrippedError,
  InvalidEpistemicProofError,
  EpistemicCircularityError,
  EpistemicDeadlockError,
  EpistemicContradictionCircuitError,
  sha256Hex,
} from "../src/epistemic_arbitration.ts";

test("EpistemicProofDAG: Construction, acyclicity validation, and depth computation", () => {
  const dag = new EpistemicProofDAG({
    dag_id: "dag_001",
    root_node_id: "root",
    max_depth: 10,
  });

  // Leaves
  dag.addNode({
    node_id: "leaf1",
    rule: DeductionRule.AXIOM,
    premise_ids: [],
    conclusion: "fact_1",
    evidence_hash: sha256Hex("data_1"),
    evidence_weight: 0.9,
    is_valid: true,
  });
  dag.addNode({
    node_id: "leaf2",
    rule: DeductionRule.EVIDENTIARY_GROUNDING,
    premise_ids: [],
    conclusion: "fact_2",
    evidence_hash: sha256Hex("data_2"),
    evidence_weight: 0.85,
    is_valid: true,
  });

  // Intermediate premise
  dag.addNode({
    node_id: "step1",
    rule: DeductionRule.CONJUNCTION,
    premise_ids: ["leaf1", "leaf2"],
    conclusion: "fact_1_and_fact_2",
    evidence_weight: 0.0,
    is_valid: true,
  });

  // Root
  dag.addNode({
    node_id: "root",
    rule: DeductionRule.MODUS_PONENS,
    premise_ids: ["step1"],
    conclusion: "final_claim",
    evidence_weight: 0.0,
    is_valid: true,
  });

  assert.equal(dag.validateAcyclicity(), true);
  // Leaf (1) -> step1 (2) -> root (3)
  assert.equal(dag.computeDepth(), 3);
});

test("EpistemicProofDAG: Circularity detection catches cycles and throws EpistemicCircularityError", () => {
  const dag = new EpistemicProofDAG({
    dag_id: "dag_cycle",
    root_node_id: "node_a",
  });

  dag.addNode({
    node_id: "node_a",
    rule: DeductionRule.MODUS_PONENS,
    premise_ids: ["node_b"],
    conclusion: "A",
    evidence_weight: 0.5,
    is_valid: true,
  });
  dag.addNode({
    node_id: "node_b",
    rule: DeductionRule.MODUS_PONENS,
    premise_ids: ["node_c"],
    conclusion: "B",
    evidence_weight: 0.5,
    is_valid: true,
  });
  dag.addNode({
    node_id: "node_c",
    rule: DeductionRule.MODUS_PONENS,
    premise_ids: ["node_a"], // Circular reference
    conclusion: "C",
    evidence_weight: 0.5,
    is_valid: true,
  });

  assert.throws(
    () => dag.validateAcyclicity(),
    (err) => err instanceof EpistemicCircularityError && err.message.includes("Circular reasoning detected")
  );
});

test("EpistemicProofDAG: Max depth violation throws InvalidEpistemicProofError", () => {
  const dag = new EpistemicProofDAG({
    dag_id: "dag_deep",
    root_node_id: "n3",
    max_depth: 2,
  });

  dag.addNode({ node_id: "n1", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "1", evidence_weight: 1.0, is_valid: true });
  dag.addNode({ node_id: "n2", rule: DeductionRule.MODUS_PONENS, premise_ids: ["n1"], conclusion: "2", evidence_weight: 0.0, is_valid: true });
  dag.addNode({ node_id: "n3", rule: DeductionRule.MODUS_PONENS, premise_ids: ["n2"], conclusion: "3", evidence_weight: 0.0, is_valid: true });

  assert.throws(
    () => dag.validateAcyclicity(),
    (err) => err instanceof InvalidEpistemicProofError && err.message.includes("exceeded maximum deduction depth")
  );
});

test("EpistemicProofDAG: Leaf weight average and deduction ratio evaluation", () => {
  const dag = new EpistemicProofDAG({
    dag_id: "dag_eval",
    root_node_id: "root",
  });

  dag.addNode({
    node_id: "l1",
    rule: DeductionRule.AXIOM,
    premise_ids: [],
    conclusion: "L1",
    evidence_weight: 0.8,
    is_valid: true,
  });
  dag.addNode({
    node_id: "l2",
    rule: DeductionRule.EVIDENTIARY_GROUNDING,
    premise_ids: [],
    conclusion: "L2",
    evidence_weight: 1.0,
    is_valid: true,
  });
  dag.addNode({
    node_id: "root",
    rule: DeductionRule.CONJUNCTION,
    premise_ids: ["l1", "l2"],
    conclusion: "L1 and L2",
    evidence_weight: 0.0,
    is_valid: true,
  });

  const res = dag.evaluateDag();
  assert.equal(res.isValid, true);
  assert.ok(Math.abs(res.avgLeafWeight - 0.9) < 0.001);
  assert.ok(Math.abs(res.deductionRatio - 1.0) < 0.001);
});

test("EpistemicArbitrationEngine: Claim registration and formula scoring", () => {
  const engine = new EpistemicArbitrationEngine();

  const dag = new EpistemicProofDAG({ dag_id: "dag_sc", root_node_id: "root" });
  dag.addNode({ node_id: "l1", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "L1", evidence_weight: 0.9, is_valid: true });
  dag.addNode({ node_id: "root", rule: DeductionRule.EPISTEMIC_NECESSITATION, premise_ids: ["l1"], conclusion: "Claim", evidence_weight: 0.0, is_valid: true });

  const claim = engine.registerClaim({
    swarmId: "swarm_alpha",
    agentId: "agent_1",
    subject: "ledger_entry_42",
    predicate: "is_committed",
    polarity: ClaimPolarity.TRUE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.95,
    evidenceHashes: [sha256Hex("raw_tx_bytes")],
    proofDag: dag,
  });

  assert.ok(claim.claim_id.startsWith("claim_"));
  assert.equal(claim.subject, "ledger_entry_42");
  assert.equal(claim.digest.length, 64);

  const { isValid, score } = engine.computeClaimScore(claim);
  assert.equal(isValid, true);
  assert.ok(score > 0.80);
});

test("EpistemicArbitrationEngine: Dispute resolution with prevailing claimant and receipt verification", () => {
  const engine = new EpistemicArbitrationEngine({ disputeMarginThreshold: 0.10 });

  // Claimant
  const dagClaimant = new EpistemicProofDAG({ dag_id: "dag_c", root_node_id: "rc" });
  dagClaimant.addNode({ node_id: "lc", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "C", evidence_weight: 0.95, is_valid: true });
  dagClaimant.addNode({ node_id: "rc", rule: DeductionRule.MODUS_PONENS, premise_ids: ["lc"], conclusion: "Claim", evidence_weight: 0.0, is_valid: true });

  const claimantClaim = engine.registerClaim({
    swarmId: "swarm_alpha",
    agentId: "agent_c",
    subject: "tx_999",
    predicate: "finalized",
    polarity: ClaimPolarity.TRUE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.95,
    evidenceHashes: [sha256Hex("evidence_c")],
    proofDag: dagClaimant,
  });

  // Respondent
  const dagRespondent = new EpistemicProofDAG({ dag_id: "dag_r", root_node_id: "rr" });
  dagRespondent.addNode({ node_id: "lr", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "R", evidence_weight: 0.20, is_valid: true });
  dagRespondent.addNode({ node_id: "rr", rule: DeductionRule.MODUS_PONENS, premise_ids: ["lr"], conclusion: "Not Claim", evidence_weight: 0.0, is_valid: true });

  const respondentClaim = engine.registerClaim({
    swarmId: "swarm_beta",
    agentId: "agent_r",
    subject: "tx_999",
    predicate: "finalized",
    polarity: ClaimPolarity.FALSE,
    modality: EpistemicModality.BELIEVES,
    confidence: 0.30,
    evidenceHashes: [sha256Hex("evidence_r")],
    proofDag: dagRespondent,
  });

  const dispute = engine.openDispute({
    claimantClaimId: claimantClaim.claim_id,
    respondentClaimId: respondentClaim.claim_id,
  });
  assert.equal(dispute.status, DisputeStatus.OPEN);

  const receipt = engine.resolveDispute(dispute.dispute_id);
  assert.equal(receipt.verdict, ArbitrationVerdict.CLAIMANT_PREVAILS);
  assert.equal(receipt.prevailing_swarm_id, "swarm_alpha");
  assert.equal(receipt.prevailing_claim_id, claimantClaim.claim_id);
  assert.ok(receipt.receipt_hash.startsWith("sw:dispute:v1:"));
  assert.ok(receipt.score_delta > 0.10);

  // Verify receipt authenticity
  const { isValid, errorReason } = engine.verifyResolutionReceipt(receipt);
  assert.equal(isValid, true);
  assert.equal(errorReason, null);
});

test("EpistemicArbitrationEngine: Dispute resolution with mutual invalidation", () => {
  const engine = new EpistemicArbitrationEngine();

  // Claimant has invalid node
  const dagC = new EpistemicProofDAG({ dag_id: "dag_c", root_node_id: "rc" });
  dagC.addNode({ node_id: "lc", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "C", evidence_weight: 0.8, is_valid: false });
  dagC.addNode({ node_id: "rc", rule: DeductionRule.MODUS_PONENS, premise_ids: ["lc"], conclusion: "Root", evidence_weight: 0.0, is_valid: true });

  const cClaim = engine.registerClaim({
    swarmId: "swarm_alpha",
    agentId: "agent_c",
    subject: "state_conflict",
    predicate: "valid",
    polarity: ClaimPolarity.TRUE,
    modality: EpistemicModality.JUSTIFIED,
    confidence: 0.8,
    evidenceHashes: [],
    proofDag: dagC,
  });

  // Respondent has empty DAG
  const dagR = new EpistemicProofDAG({ dag_id: "dag_r", root_node_id: "rr" });
  const rClaim = engine.registerClaim({
    swarmId: "swarm_beta",
    agentId: "agent_r",
    subject: "state_conflict",
    predicate: "valid",
    polarity: ClaimPolarity.FALSE,
    modality: EpistemicModality.JUSTIFIED,
    confidence: 0.8,
    evidenceHashes: [],
    proofDag: dagR,
  });

  const dispute = engine.openDispute({
    claimantClaimId: cClaim.claim_id,
    respondentClaimId: rClaim.claim_id,
  });
  const receipt = engine.resolveDispute(dispute.dispute_id);

  assert.equal(receipt.verdict, ArbitrationVerdict.MUTUAL_INVALIDATION);
  assert.equal(receipt.prevailing_swarm_id, null);
  assert.equal(receipt.prevailing_claim_id, null);
  assert.ok(receipt.receipt_hash.startsWith("sw:dispute:v1:"));
});

test("EpistemicArbitrationEngine: Deadlocked dispute and fail-closed safety circuit trip", () => {
  const engine = new EpistemicArbitrationEngine({ disputeMarginThreshold: 0.15 });

  // Both parties submit identical strength proofs
  const dagC = new EpistemicProofDAG({ dag_id: "dag_c", root_node_id: "rc" });
  dagC.addNode({ node_id: "lc", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "C", evidence_weight: 0.85, is_valid: true });
  dagC.addNode({ node_id: "rc", rule: DeductionRule.MODUS_PONENS, premise_ids: ["lc"], conclusion: "Claim", evidence_weight: 0.0, is_valid: true });

  const dagR = new EpistemicProofDAG({ dag_id: "dag_r", root_node_id: "rr" });
  dagR.addNode({ node_id: "lr", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "R", evidence_weight: 0.85, is_valid: true });
  dagR.addNode({ node_id: "rr", rule: DeductionRule.MODUS_PONENS, premise_ids: ["lr"], conclusion: "Not Claim", evidence_weight: 0.0, is_valid: true });

  const cClaim = engine.registerClaim({
    swarmId: "swarm_alpha",
    agentId: "agent_c",
    subject: "high_stakes_key",
    predicate: "status_ok",
    polarity: ClaimPolarity.TRUE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.90,
    evidenceHashes: [sha256Hex("ev1")],
    proofDag: dagC,
  });

  const rClaim = engine.registerClaim({
    swarmId: "swarm_beta",
    agentId: "agent_r",
    subject: "high_stakes_key",
    predicate: "status_ok",
    polarity: ClaimPolarity.FALSE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.90,
    evidenceHashes: [sha256Hex("ev2")],
    proofDag: dagR,
  });

  const dispute = engine.openDispute({
    claimantClaimId: cClaim.claim_id,
    respondentClaimId: rClaim.claim_id,
  });
  const receipt = engine.resolveDispute(dispute.dispute_id);

  assert.ok(
    receipt.verdict === ArbitrationVerdict.DEADLOCKED ||
    receipt.verdict === ArbitrationVerdict.EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP
  );
  assert.ok(receipt.receipt_hash.startsWith("sw:dispute:v1:"));

  // Explicit circuit trip
  engine.tripCircuit("Irreconcilable epistemic deadlock on high_stakes_key");
  const status = engine.getStatus();
  assert.equal(status.circuit_tripped, true);

  // Subsequent claims must be rejected with DisputeCircuitTrippedError
  assert.throws(
    () => {
      engine.registerClaim({
        swarmId: "swarm_gamma",
        agentId: "agent_g",
        subject: "tx_any",
        predicate: "any",
        polarity: ClaimPolarity.TRUE,
        modality: EpistemicModality.BELIEVES,
        confidence: 0.5,
        evidenceHashes: [],
        proofDag: dagC,
      });
    },
    (err) => err instanceof DisputeCircuitTrippedError
  );
});
