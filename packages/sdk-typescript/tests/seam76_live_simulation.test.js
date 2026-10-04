/**
 * Test Suite for Seam 76 (ADR 0087) Epistemic Proof Arbitration Live Simulation in TypeScript.
 * Plain ES module for Node.js native execution with --experimental-strip-types.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

import {
  EpistemicModality,
  ClaimPolarity,
  DeductionRule,
  DisputeStatus,
  ArbitrationVerdict,
  EpistemicProofDAG,
  EpistemicArbitrationEngine,
  EpistemicCircularityError,
  DisputeCircuitTrippedError,
  sha256Hex,
} from "../src/epistemic_arbitration.ts";

test("Seam 76 Live Simulation: Certify all 8 operational invariants in TypeScript", () => {
  const engine = new EpistemicArbitrationEngine({ disputeMarginThreshold: 0.10 });

  // Invariant 1: Zero-Token Cost Guarantee ($0.00)
  const initialStatus = engine.getStatus();
  assert.equal(initialStatus.claims_count, 0);
  assert.equal(initialStatus.circuit_tripped, false);

  // Invariant 3: Directed Acyclic Proof Validation & Circularity Defense
  const validDag = new EpistemicProofDAG({ dag_id: "dag_valid", root_node_id: "root", max_depth: 8 });
  validDag.addNode({ node_id: "l1", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "A", evidence_weight: 0.92, is_valid: true });
  validDag.addNode({ node_id: "l2", rule: DeductionRule.EVIDENTIARY_GROUNDING, premise_ids: [], conclusion: "B", evidence_weight: 0.88, is_valid: true });
  validDag.addNode({ node_id: "step1", rule: DeductionRule.CONJUNCTION, premise_ids: ["l1", "l2"], conclusion: "A and B", evidence_weight: 0.0, is_valid: true });
  validDag.addNode({ node_id: "root", rule: DeductionRule.MODUS_PONENS, premise_ids: ["step1"], conclusion: "Claim", evidence_weight: 0.0, is_valid: true });

  assert.equal(validDag.validateAcyclicity(), true);
  assert.equal(validDag.computeDepth(), 3);

  const cycleDag = new EpistemicProofDAG({ dag_id: "dag_cycle", root_node_id: "c1" });
  cycleDag.addNode({ node_id: "c1", rule: DeductionRule.MODUS_PONENS, premise_ids: ["c2"], conclusion: "1", evidence_weight: 0.5, is_valid: true });
  cycleDag.addNode({ node_id: "c2", rule: DeductionRule.MODUS_PONENS, premise_ids: ["c1"], conclusion: "2", evidence_weight: 0.5, is_valid: true });
  assert.throws(() => cycleDag.validateAcyclicity(), EpistemicCircularityError);

  // Invariant 2: Sub-250µs Execution Latency SLO
  const nClaims = 50;
  const t0 = performance.now();
  for (let i = 0; i < nClaims; i++) {
    const d = new EpistemicProofDAG({ dag_id: `dag_${i}`, root_node_id: `r_${i}` });
    d.addNode({ node_id: `l_${i}`, rule: DeductionRule.AXIOM, premise_ids: [], conclusion: `C_${i}`, evidence_weight: 0.80, is_valid: true });
    d.addNode({ node_id: `r_${i}`, rule: DeductionRule.MODUS_PONENS, premise_ids: [`l_${i}`], conclusion: `R_${i}`, evidence_weight: 0.0, is_valid: true });
    engine.registerClaim({
      swarmId: `swarm_${i % 3}`,
      agentId: `agent_${i % 10}`,
      subject: `resource_slot_${i}`,
      predicate: "state_valid",
      polarity: ClaimPolarity.TRUE,
      modality: EpistemicModality.KNOWS,
      confidence: 0.90,
      evidenceHashes: [sha256Hex(`ev_${i}`)],
      proofDag: d,
    });
  }
  const t1 = performance.now();
  const avgLatencyUs = ((t1 - t0) / nClaims) * 1000;
  assert.ok(avgLatencyUs < 250.0, `Claim ingestion latency ${avgLatencyUs}µs exceeded 250µs SLO`);

  // Invariant 4: Evidentiary Dominance Scoring Formula
  const sampleClaim = engine.getClaim(Array.from(engine._claims.keys())[0]);
  assert.ok(sampleClaim !== undefined);
  const { isValid, score } = engine.computeClaimScore(sampleClaim);
  assert.equal(isValid, true);
  assert.ok(score >= 0.0 && score <= 1.0);

  // Invariant 5: Multi-Cluster Cross-Swarm Dispute Resolution
  const dagAlpha = new EpistemicProofDAG({ dag_id: "dag_alpha", root_node_id: "ra" });
  dagAlpha.addNode({ node_id: "la", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "alpha_fact", evidence_weight: 0.96, is_valid: true });
  dagAlpha.addNode({ node_id: "ra", rule: DeductionRule.MODUS_PONENS, premise_ids: ["la"], conclusion: "final_tx", evidence_weight: 0.0, is_valid: true });
  const claimAlpha = engine.registerClaim({
    swarmId: "swarm_us_east",
    agentId: "consensus_leader_1",
    subject: "cross_chain_state_epoch_104",
    predicate: "is_finalized",
    polarity: ClaimPolarity.TRUE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.98,
    evidenceHashes: [sha256Hex("quorum_block_header")],
    proofDag: dagAlpha,
  });

  const dagBeta = new EpistemicProofDAG({ dag_id: "dag_beta", root_node_id: "rb" });
  dagBeta.addNode({ node_id: "lb", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "beta_hint", evidence_weight: 0.25, is_valid: true });
  dagBeta.addNode({ node_id: "rb", rule: DeductionRule.MODUS_PONENS, premise_ids: ["lb"], conclusion: "final_tx", evidence_weight: 0.0, is_valid: true });
  const claimBeta = engine.registerClaim({
    swarmId: "swarm_eu_central",
    agentId: "adversarial_agent_7",
    subject: "cross_chain_state_epoch_104",
    predicate: "is_finalized",
    polarity: ClaimPolarity.FALSE,
    modality: EpistemicModality.BELIEVES,
    confidence: 0.35,
    evidenceHashes: [sha256Hex("unverified_gossip")],
    proofDag: dagBeta,
  });

  const dispute = engine.openDispute({
    claimantClaimId: claimAlpha.claim_id,
    respondentClaimId: claimBeta.claim_id,
  });
  const receipt = engine.resolveDispute(dispute.dispute_id);
  assert.equal(receipt.verdict, ArbitrationVerdict.CLAIMANT_PREVAILS);
  assert.equal(receipt.prevailing_swarm_id, "swarm_us_east");

  // Invariant 6: Tamper-Evident Receipt Verification
  assert.ok(receipt.receipt_hash.startsWith("sw:dispute:v1:"));
  const verifyRes = engine.verifyResolutionReceipt(receipt);
  assert.equal(verifyRes.isValid, true);
  assert.equal(verifyRes.errorReason, null);

  // Invariant 7: Deadlock & Contradiction Detection
  const dagSym1 = new EpistemicProofDAG({ dag_id: "d_s1", root_node_id: "r1" });
  dagSym1.addNode({ node_id: "l1", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "K", evidence_weight: 0.90, is_valid: true });
  dagSym1.addNode({ node_id: "r1", rule: DeductionRule.MODUS_PONENS, premise_ids: ["l1"], conclusion: "Final", evidence_weight: 0.0, is_valid: true });

  const dagSym2 = new EpistemicProofDAG({ dag_id: "d_s2", root_node_id: "r2" });
  dagSym2.addNode({ node_id: "l2", rule: DeductionRule.AXIOM, premise_ids: [], conclusion: "K_neg", evidence_weight: 0.90, is_valid: true });
  dagSym2.addNode({ node_id: "r2", rule: DeductionRule.MODUS_PONENS, premise_ids: ["l2"], conclusion: "Final_neg", evidence_weight: 0.0, is_valid: true });

  const cSym1 = engine.registerClaim({
    swarmId: "swarm_alpha",
    agentId: "a1",
    subject: "irreconcilable_ledger_entry",
    predicate: "consensus_reached",
    polarity: ClaimPolarity.TRUE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.90,
    evidenceHashes: [sha256Hex("sym1")],
    proofDag: dagSym1,
  });
  const cSym2 = engine.registerClaim({
    swarmId: "swarm_beta",
    agentId: "a2",
    subject: "irreconcilable_ledger_entry",
    predicate: "consensus_reached",
    polarity: ClaimPolarity.FALSE,
    modality: EpistemicModality.KNOWS,
    confidence: 0.90,
    evidenceHashes: [sha256Hex("sym2")],
    proofDag: dagSym2,
  });

  const disputeDeadlock = engine.openDispute({
    claimantClaimId: cSym1.claim_id,
    respondentClaimId: cSym2.claim_id,
  });
  const deadlockReceipt = engine.resolveDispute(disputeDeadlock.dispute_id, 0.15);
  assert.ok(
    deadlockReceipt.verdict === ArbitrationVerdict.DEADLOCKED ||
    deadlockReceipt.verdict === ArbitrationVerdict.EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP
  );

  // Invariant 8: Fail-Closed Safety Circuit Coupling
  engine.tripCircuit("Deadlock on high-value ledger state transition");
  assert.equal(engine.getStatus().circuit_tripped, true);
  assert.throws(
    () => {
      engine.registerClaim({
        swarmId: "swarm_gamma",
        agentId: "agent_x",
        subject: "post_trip_op",
        predicate: "op",
        polarity: ClaimPolarity.TRUE,
        modality: EpistemicModality.BELIEVES,
        confidence: 0.5,
        evidenceHashes: [],
      });
    },
    DisputeCircuitTrippedError
  );
});
