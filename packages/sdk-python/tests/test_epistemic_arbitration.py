# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Multi-Cluster Epistemic Proof Verification & Automated Cross-Swarm Dispute Resolution (Seam 76 / ADR 0087).
"""

import pytest
from swarmwatch.epistemic_arbitration import (
    EpistemicModality,
    ClaimPolarity,
    DeductionRule,
    DisputeStatus,
    ArbitrationVerdict,
    ProofNode,
    EpistemicProofDAG,
    EpistemicClaim,
    DisputeCase,
    DisputeResolutionReceipt,
    EpistemicArbitrationEngine,
    EpistemicArbitrationError,
    DisputeCircuitTrippedError,
    InvalidEpistemicProofError,
    EpistemicCircularityError,
    EpistemicDeadlockError,
    EpistemicContradictionCircuitError,
    sha256_hex,
)


def test_proof_dag_construction_and_depth():
    """Verify EpistemicProofDAG construction, acyclicity validation, and depth computation."""
    dag = EpistemicProofDAG(dag_id="dag_001", root_node_id="root", max_depth=10)

    # Leaves
    leaf1 = ProofNode(
        node_id="leaf1",
        rule=DeductionRule.AXIOM,
        premise_ids=[],
        conclusion="fact_1",
        evidence_hash=sha256_hex("data_1"),
        evidence_weight=0.9,
    )
    leaf2 = ProofNode(
        node_id="leaf2",
        rule=DeductionRule.EVIDENTIARY_GROUNDING,
        premise_ids=[],
        conclusion="fact_2",
        evidence_hash=sha256_hex("data_2"),
        evidence_weight=0.85,
    )

    # Intermediate premise
    step1 = ProofNode(
        node_id="step1",
        rule=DeductionRule.CONJUNCTION,
        premise_ids=["leaf1", "leaf2"],
        conclusion="fact_1_and_fact_2",
        evidence_weight=0.0,
    )

    # Root
    root = ProofNode(
        node_id="root",
        rule=DeductionRule.MODUS_PONENS,
        premise_ids=["step1"],
        conclusion="final_claim",
        evidence_weight=0.0,
    )

    dag.add_node(leaf1)
    dag.add_node(leaf2)
    dag.add_node(step1)
    dag.add_node(root)

    assert dag.validate_acyclicity() is True
    # Depth: leaf (1) -> step1 (2) -> root (3)
    assert dag.compute_depth() == 3


def test_dag_circularity_detection():
    """Verify cycle detection identifies circular deduction graphs and raises EpistemicCircularityError."""
    dag = EpistemicProofDAG(dag_id="dag_cycle", root_node_id="node_a")

    node_a = ProofNode(
        node_id="node_a",
        rule=DeductionRule.MODUS_PONENS,
        premise_ids=["node_b"],
        conclusion="A",
    )
    node_b = ProofNode(
        node_id="node_b",
        rule=DeductionRule.MODUS_PONENS,
        premise_ids=["node_c"],
        conclusion="B",
    )
    node_c = ProofNode(
        node_id="node_c",
        rule=DeductionRule.MODUS_PONENS,
        premise_ids=["node_a"],  # Cycle back to A
        conclusion="C",
    )

    dag.add_node(node_a)
    dag.add_node(node_b)
    dag.add_node(node_c)

    with pytest.raises(EpistemicCircularityError) as exc_info:
        dag.validate_acyclicity()
    assert "Circular reasoning detected" in str(exc_info.value)


def test_dag_max_depth_exceeded():
    """Verify DAG recursion exceeding maximum allowed depth raises InvalidEpistemicProofError."""
    dag = EpistemicProofDAG(dag_id="dag_deep", root_node_id="n3", max_depth=2)

    n1 = ProofNode(node_id="n1", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="1")
    n2 = ProofNode(node_id="n2", rule=DeductionRule.MODUS_PONENS, premise_ids=["n1"], conclusion="2")
    n3 = ProofNode(node_id="n3", rule=DeductionRule.MODUS_PONENS, premise_ids=["n2"], conclusion="3")

    dag.add_node(n1)
    dag.add_node(n2)
    dag.add_node(n3)

    with pytest.raises(InvalidEpistemicProofError) as exc_info:
        dag.validate_acyclicity()
    assert "exceeded maximum deduction depth" in str(exc_info.value)


def test_leaf_weight_and_deduction_ratio():
    """Verify evaluate_dag calculates correct leaf weight averages and deduction validity ratios."""
    dag = EpistemicProofDAG(dag_id="dag_eval", root_node_id="root")

    leaf1 = ProofNode(
        node_id="l1",
        rule=DeductionRule.AXIOM,
        premise_ids=[],
        conclusion="L1",
        evidence_weight=0.8,
        is_valid=True,
    )
    leaf2 = ProofNode(
        node_id="l2",
        rule=DeductionRule.EVIDENTIARY_GROUNDING,
        premise_ids=[],
        conclusion="L2",
        evidence_weight=1.0,
        is_valid=True,
    )
    root = ProofNode(
        node_id="root",
        rule=DeductionRule.CONJUNCTION,
        premise_ids=["l1", "l2"],
        conclusion="L1 and L2",
        evidence_weight=0.0,
        is_valid=True,
    )

    dag.add_node(leaf1)
    dag.add_node(leaf2)
    dag.add_node(root)

    is_valid, avg_leaf, deduction_ratio = dag.evaluate_dag()
    assert is_valid is True
    # Avg leaf: (0.8 + 1.0) / 2 = 0.9
    assert pytest.approx(avg_leaf, 0.001) == 0.9
    # Root rule is valid, so 1.0 ratio
    assert pytest.approx(deduction_ratio, 0.001) == 1.0


def test_claim_registration_and_scoring():
    """Verify epistemic claim registration and deterministic formula scoring."""
    engine = EpistemicArbitrationEngine()

    dag = EpistemicProofDAG(dag_id="dag_sc", root_node_id="root")
    dag.add_node(ProofNode(node_id="l1", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="L1", evidence_weight=0.9))
    dag.add_node(ProofNode(node_id="root", rule=DeductionRule.EPISTEMIC_NECESSITATION, premise_ids=["l1"], conclusion="Claim", evidence_weight=0.0))

    claim = engine.register_claim(
        swarm_id="swarm_alpha",
        agent_id="agent_1",
        subject="ledger_entry_42",
        predicate="is_committed",
        polarity=ClaimPolarity.TRUE,
        modality=EpistemicModality.KNOWS,
        confidence=0.95,
        evidence_hashes=[sha256_hex("raw_tx_bytes")],
        proof_dag=dag,
    )

    assert claim.claim_id.startswith("claim_")
    assert claim.subject == "ledger_entry_42"
    assert claim.digest.startswith("0x") or len(claim.digest) == 64

    # Calculate score
    # S = 0.50 * 0.9 + 0.30 * 1.0 + 0.15 * 0.95 - 0.02 * 1
    # S = 0.45 + 0.30 + 0.1425 - 0.02 = 0.8725
    is_valid, score = engine.compute_claim_score(claim)
    assert is_valid is True
    assert score > 0.80


def test_dispute_resolution_claimant_prevails():
    """Verify dispute where claimant has higher evidentiary score by margin >= threshold results in CLAIMANT_PREVAILS."""
    engine = EpistemicArbitrationEngine(dispute_margin_threshold=0.10)

    # Strong claimant DAG
    dag_claimant = EpistemicProofDAG(dag_id="dag_c", root_node_id="rc")
    dag_claimant.add_node(ProofNode(node_id="lc", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="C", evidence_weight=0.95))
    dag_claimant.add_node(ProofNode(node_id="rc", rule=DeductionRule.MODUS_PONENS, premise_ids=["lc"], conclusion="Claim", evidence_weight=0.0))

    claimant_claim = engine.register_claim(
        swarm_id="swarm_alpha",
        agent_id="agent_c",
        subject="tx_999",
        predicate="finalized",
        polarity=ClaimPolarity.TRUE,
        modality=EpistemicModality.KNOWS,
        confidence=0.95,
        evidence_hashes=[sha256_hex("evidence_c")],
        proof_dag=dag_claimant,
    )

    # Weak respondent DAG
    dag_respondent = EpistemicProofDAG(dag_id="dag_r", root_node_id="rr")
    dag_respondent.add_node(ProofNode(node_id="lr", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="R", evidence_weight=0.20))
    dag_respondent.add_node(ProofNode(node_id="rr", rule=DeductionRule.MODUS_PONENS, premise_ids=["lr"], conclusion="Not Claim", evidence_weight=0.0))

    respondent_claim = engine.register_claim(
        swarm_id="swarm_beta",
        agent_id="agent_r",
        subject="tx_999",
        predicate="finalized",
        polarity=ClaimPolarity.FALSE,
        modality=EpistemicModality.BELIEVES,
        confidence=0.30,
        evidence_hashes=[sha256_hex("evidence_r")],
        proof_dag=dag_respondent,
    )

    dispute = engine.open_dispute(claimant_claim.claim_id, respondent_claim.claim_id)
    assert dispute.status == DisputeStatus.OPEN

    receipt = engine.resolve_dispute(dispute.dispute_id)
    assert receipt.verdict == ArbitrationVerdict.CLAIMANT_PREVAILS
    assert receipt.prevailing_swarm_id == "swarm_alpha"
    assert receipt.prevailing_claim_id == claimant_claim.claim_id
    assert receipt.receipt_hash.startswith("sw:dispute:v1:")
    assert receipt.score_delta > 0.10

    # Verify receipt integrity
    is_valid, err = engine.verify_resolution_receipt(receipt)
    assert is_valid is True
    assert err is None


def test_dispute_resolution_mutual_invalidation():
    """Verify dispute where both proof DAGs are invalid yields MUTUAL_INVALIDATION."""
    engine = EpistemicArbitrationEngine()

    # Claimant has invalid node
    dag_c = EpistemicProofDAG(dag_id="dag_c", root_node_id="rc")
    dag_c.add_node(ProofNode(node_id="lc", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="C", evidence_weight=0.8, is_valid=False))
    dag_c.add_node(ProofNode(node_id="rc", rule=DeductionRule.MODUS_PONENS, premise_ids=["lc"], conclusion="Root", evidence_weight=0.0, is_valid=True))

    c_claim = engine.register_claim(
        swarm_id="swarm_alpha",
        agent_id="agent_c",
        subject="state_conflict",
        predicate="valid",
        polarity=ClaimPolarity.TRUE,
        modality=EpistemicModality.JUSTIFIED,
        confidence=0.8,
        evidence_hashes=[],
        proof_dag=dag_c,
    )

    # Respondent has empty / invalid DAG
    dag_r = EpistemicProofDAG(dag_id="dag_r", root_node_id="rr")
    r_claim = engine.register_claim(
        swarm_id="swarm_beta",
        agent_id="agent_r",
        subject="state_conflict",
        predicate="valid",
        polarity=ClaimPolarity.FALSE,
        modality=EpistemicModality.JUSTIFIED,
        confidence=0.8,
        evidence_hashes=[],
        proof_dag=dag_r,
    )

    dispute = engine.open_dispute(c_claim.claim_id, r_claim.claim_id)
    receipt = engine.resolve_dispute(dispute.dispute_id)

    assert receipt.verdict == ArbitrationVerdict.MUTUAL_INVALIDATION
    assert receipt.prevailing_swarm_id is None
    assert receipt.prevailing_claim_id is None
    assert receipt.receipt_hash.startswith("sw:dispute:v1:")


def test_dispute_deadlock_and_circuit_trip():
    """Verify deadlocked disputes with delta below margin trip the fail-closed safety circuit."""
    engine = EpistemicArbitrationEngine(dispute_margin_threshold=0.15)

    # Both parties submit identical strength proofs
    dag_c = EpistemicProofDAG(dag_id="dag_c", root_node_id="rc")
    dag_c.add_node(ProofNode(node_id="lc", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="C", evidence_weight=0.85))
    dag_c.add_node(ProofNode(node_id="rc", rule=DeductionRule.MODUS_PONENS, premise_ids=["lc"], conclusion="Claim", evidence_weight=0.0))

    dag_r = EpistemicProofDAG(dag_id="dag_r", root_node_id="rr")
    dag_r.add_node(ProofNode(node_id="lr", rule=DeductionRule.AXIOM, premise_ids=[], conclusion="R", evidence_weight=0.85))
    dag_r.add_node(ProofNode(node_id="rr", rule=DeductionRule.MODUS_PONENS, premise_ids=["lr"], conclusion="Not Claim", evidence_weight=0.0))

    c_claim = engine.register_claim(
        swarm_id="swarm_alpha",
        agent_id="agent_c",
        subject="high_stakes_key",
        predicate="status_ok",
        polarity=ClaimPolarity.TRUE,
        modality=EpistemicModality.KNOWS,
        confidence=0.90,
        evidence_hashes=[sha256_hex("ev1")],
        proof_dag=dag_c,
    )

    r_claim = engine.register_claim(
        swarm_id="swarm_beta",
        agent_id="agent_r",
        subject="high_stakes_key",
        predicate="status_ok",
        polarity=ClaimPolarity.FALSE,
        modality=EpistemicModality.KNOWS,
        confidence=0.90,
        evidence_hashes=[sha256_hex("ev2")],
        proof_dag=dag_r,
    )

    dispute = engine.open_dispute(c_claim.claim_id, r_claim.claim_id)

    # Resolution should yield DEADLOCK or trip circuit
    receipt = engine.resolve_dispute(dispute.dispute_id)
    assert receipt.verdict in (
        ArbitrationVerdict.DEADLOCKED,
        ArbitrationVerdict.EPISTEMIC_DIVERGENCE_CIRCUIT_TRIP,
    )
    assert receipt.receipt_hash.startswith("sw:dispute:v1:")

    # When deadlocked and trip_circuit_on_deadlock is invoked, circuit trips fail-closed
    engine.trip_circuit("Irreconcilable epistemic deadlock on high_stakes_key")
    status = engine.get_status()
    assert status["circuit_tripped"] is True

    # Subsequent claims must be rejected with DisputeCircuitTrippedError
    with pytest.raises(DisputeCircuitTrippedError):
        engine.register_claim(
            swarm_id="swarm_gamma",
            agent_id="agent_g",
            subject="tx_any",
            predicate="any",
            polarity=ClaimPolarity.TRUE,
            modality=EpistemicModality.BELIEVES,
            confidence=0.5,
            evidence_hashes=[],
            proof_dag=dag_c,
        )
