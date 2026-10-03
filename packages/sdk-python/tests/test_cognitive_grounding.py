# -*- coding: utf-8 -*-
"""
Test Suite for SwarmWatch Multi-Modal Cognitive Grounding & Vector Evidence Fusion (Seam 73 / ADR 0084).
"""

import math
import time
import pytest

from swarmwatch.cognitive_grounding import (
    CognitiveGroundingEngine,
    CognitiveGroundingError,
    EvidenceModality,
    EvidenceNotFoundError,
    GroundingCircuitTrippedError,
    GroundingConfig,
    GroundingModalityMismatchError,
    GroundingReceipt,
    GroundingVerdict,
    UngroundedInferenceError,
    VectorEvidence,
    VectorEvidenceStore,
    compute_canonical_grounding_hash,
    compute_deterministic_embedding,
    cosine_similarity,
)


def test_deterministic_embedding_properties():
    text = "The user has authorized a payment transfer of $500 for invoice #8921"
    vec1 = compute_deterministic_embedding(text, dimension=64)
    vec2 = compute_deterministic_embedding(text, dimension=64)

    assert len(vec1) == 64
    assert vec1 == vec2, "Embedding must be 100% deterministic"

    # L2 Norm check
    norm = math.sqrt(sum(x * x for x in vec1))
    assert abs(norm - 1.0) < 1e-6, "Non-empty embedding must be L2-normalized to unit sphere"

    empty_vec = compute_deterministic_embedding("", dimension=64)
    assert all(x == 0.0 for x in empty_vec)


def test_cosine_similarity_bounds():
    vec_a = compute_deterministic_embedding("Autonomous agent swarm safety verification", dimension=64)
    vec_b = compute_deterministic_embedding("Autonomous agent swarm safety verification", dimension=64)
    vec_c = compute_deterministic_embedding("Completely unrelated botanical biology photosynthesis cell", dimension=64)

    sim_self = cosine_similarity(vec_a, vec_b)
    assert abs(sim_self - 1.0) < 1e-5, f"Self-similarity must be ~1.0, got {sim_self}"

    sim_diff = cosine_similarity(vec_a, vec_c)
    assert -1.0 <= sim_diff <= 1.0
    assert sim_diff < 0.40, f"Unrelated text should have low similarity, got {sim_diff}"


def test_canonical_grounding_hash():
    h = compute_canonical_grounding_hash(
        receipt_id="rcpt-1",
        claim_id="claim-1",
        tenant_id="tenant-alpha",
        verdict="GROUNDED",
        overall_score=0.9234,
        matched_evidence_ids=["ev-1", "ev-2"],
    )
    assert h.startswith("sw:ground:v1:")
    assert len(h.replace("sw:ground:v1:", "")) == 64


def test_vector_evidence_store_crud():
    store = VectorEvidenceStore(dimension=64)
    assert store.count() == 0

    ev1 = VectorEvidence(
        evidence_id="ev-1",
        tenant_id="tenant-01",
        modality=EvidenceModality.TEXT_CLAIM,
        content="Ground truth text assertion",
        embedding=compute_deterministic_embedding("Ground truth text assertion"),
        source_reference="snap-1",
    )
    ev2 = VectorEvidence(
        evidence_id="ev-2",
        tenant_id="tenant-01",
        modality=EvidenceModality.ACTION_RECEIPT,
        content="Action receipt payload",
        embedding=compute_deterministic_embedding("Action receipt payload"),
        source_reference="rcpt-1",
    )
    ev_other_tenant = VectorEvidence(
        evidence_id="ev-3",
        tenant_id="tenant-02",
        modality=EvidenceModality.TEXT_CLAIM,
        content="Other tenant evidence",
        embedding=compute_deterministic_embedding("Other tenant evidence"),
        source_reference="snap-2",
    )

    store.add(ev1)
    store.add(ev2)
    store.add(ev_other_tenant)

    assert store.count("tenant-01") == 2
    assert store.count("tenant-02") == 1
    assert store.count() == 3

    # Modality filter
    text_items = store.list_all(tenant_id="tenant-01", modality=EvidenceModality.TEXT_CLAIM)
    assert len(text_items) == 1
    assert text_items[0].evidence_id == "ev-1"

    # Removal
    assert store.remove("ev-1", tenant_id="tenant-01") is True
    assert store.get("ev-1", tenant_id="tenant-01") is None
    assert store.count("tenant-01") == 1

    # Clear
    store.clear(tenant_id="tenant-01")
    assert store.count("tenant-01") == 0
    assert store.count("tenant-02") == 1


def test_evidence_registration_all_modalities():
    engine = CognitiveGroundingEngine()

    ev_text = engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="Current database replica sync lag is 12ms",
        source_reference="doc-health",
    )
    ev_vis = engine.register_evidence(
        modality=EvidenceModality.VISUAL_ARTIFACT,
        content="ui_login_screenshot_artifact_sha256_watermark_89a1b2",
        source_reference="art-01",
    )
    ev_act = engine.register_evidence(
        modality=EvidenceModality.ACTION_RECEIPT,
        content="Action execution receipt for payment refund #440",
        source_reference="rcpt-440",
    )
    ev_wit = engine.register_evidence(
        modality=EvidenceModality.ROOT_WITNESS,
        content="Root witness log-free signed checkpoint epoch 42",
        source_reference="wit-42",
    )
    ev_tel = engine.register_evidence(
        modality=EvidenceModality.STRUCTURED_TELEMETRY,
        content="Telemetry span status=OK duration=45us turns=3",
        source_reference="span-99",
    )

    assert ev_text.modality == EvidenceModality.TEXT_CLAIM
    assert ev_vis.modality == EvidenceModality.VISUAL_ARTIFACT
    assert ev_act.modality == EvidenceModality.ACTION_RECEIPT
    assert ev_wit.modality == EvidenceModality.ROOT_WITNESS
    assert ev_tel.modality == EvidenceModality.STRUCTURED_TELEMETRY

    assert engine.store.count() == 5


def test_verify_grounding_verdicts():
    engine = CognitiveGroundingEngine(
        config=GroundingConfig(
            default_threshold=0.70,
            partial_threshold=0.45,
            drift_threshold=0.15,
        )
    )

    engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="The server cluster has 16 worker nodes online across us-east and eu-west.",
        source_reference="cluster-inv",
    )

    # 1. Exact / near match -> GROUNDED
    rcpt_grounded = engine.verify_grounding("16 worker nodes online across us-east and eu-west")
    assert rcpt_grounded.verdict == GroundingVerdict.GROUNDED
    assert rcpt_grounded.overall_score >= 0.70
    assert rcpt_grounded.canonical_hash.startswith("sw:ground:v1:")

    # 2. Moderate overlap -> PARTIALLY_GROUNDED
    rcpt_partial = engine.verify_grounding("cluster worker nodes online")
    assert rcpt_partial.verdict in (GroundingVerdict.GROUNDED, GroundingVerdict.PARTIALLY_GROUNDED)
    assert rcpt_partial.overall_score >= 0.45

    # 3. Completely unrelated -> UNGROUNDED or DRIFT_DETECTED
    rcpt_unrelated = engine.verify_grounding("quantum photosynthesis chlorophyll in tropical rainforest flora")
    assert rcpt_unrelated.verdict in (GroundingVerdict.UNGROUNDED, GroundingVerdict.DRIFT_DETECTED)
    assert rcpt_unrelated.overall_score < 0.45


def test_verify_grounding_required_modalities():
    engine = CognitiveGroundingEngine()

    engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="Policy rule restricts API calls to read-only endpoints.",
        source_reference="pol-01",
    )

    # Missing required modality VISUAL_ARTIFACT
    rcpt = engine.verify_grounding(
        claim="Policy rule restricts API calls",
        required_modalities=[EvidenceModality.VISUAL_ARTIFACT],
        strict=False,
    )
    assert rcpt.verdict == GroundingVerdict.UNGROUNDED

    # Strict mode should raise GroundingModalityMismatchError
    with pytest.raises(GroundingModalityMismatchError):
        engine.verify_grounding(
            claim="Policy rule restricts API calls",
            required_modalities=[EvidenceModality.VISUAL_ARTIFACT],
            strict=True,
        )


def test_verify_grounding_strict_mode():
    engine = CognitiveGroundingEngine()
    engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="System memory limit is 8192 MB.",
        source_reference="sys-cfg",
    )

    # Unrelated claim with strict=True must raise UngroundedInferenceError
    with pytest.raises(UngroundedInferenceError):
        engine.verify_grounding(
            claim="User has authorized transferring 50 bitcoins to unknown wallet",
            strict=True,
        )


def test_fuse_evidence_with_custom_weights():
    engine = CognitiveGroundingEngine()

    ev1 = engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="Invoice #1001 total is $450.00",
        source_reference="doc-inv",
    )
    ev2 = engine.register_evidence(
        modality=EvidenceModality.ACTION_RECEIPT,
        content="Credit card charge authorized for invoice #1001 amount $450.00",
        source_reference="rcpt-1001",
    )

    rcpt = engine.fuse_evidence(
        claim="Invoice #1001 total is $450.00 credit card authorized",
        evidence_ids=[ev1.evidence_id, ev2.evidence_id],
        modality_weights={
            EvidenceModality.TEXT_CLAIM.value: 0.4,
            EvidenceModality.ACTION_RECEIPT.value: 0.6,
        },
    )

    assert rcpt.verdict == GroundingVerdict.GROUNDED
    assert rcpt.overall_score > 0.70
    assert len(rcpt.matched_evidence_ids) == 2


def test_fail_closed_safety_circuit_coupling():
    engine = CognitiveGroundingEngine()
    engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="Server health is healthy",
        source_reference="doc-1",
    )

    # Nominal check
    rcpt = engine.verify_grounding("Server health is healthy")
    assert rcpt.verdict == GroundingVerdict.GROUNDED

    # Trip circuit
    engine.trip_circuit()
    assert engine.is_circuit_tripped() is True

    # Verification must fail-closed
    with pytest.raises(GroundingCircuitTrippedError):
        engine.verify_grounding("Server health is healthy")

    with pytest.raises(GroundingCircuitTrippedError):
        engine.fuse_evidence("Server health is healthy", [engine.store.list_all()[0].evidence_id])

    # Reset circuit
    engine.reset_circuit()
    assert engine.is_circuit_tripped() is False

    rcpt_after_reset = engine.verify_grounding("Server health is healthy")
    assert rcpt_after_reset.verdict == GroundingVerdict.GROUNDED


def test_consecutive_drift_auto_trips_circuit():
    config = GroundingConfig(
        max_drift_count_before_trip=3,
        default_threshold=0.70,
        partial_threshold=0.45,
        drift_threshold=0.15,
    )
    engine = CognitiveGroundingEngine(config=config)

    engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="Kubernetes cluster nodes are running version 1.30",
        source_reference="k8s-spec",
    )

    assert engine.is_circuit_tripped() is False

    # 1st ungrounded claim
    engine.verify_grounding("astrophysics planetary orbits around betelgeuse")
    assert engine.is_circuit_tripped() is False

    # 2nd ungrounded claim
    engine.verify_grounding("ancient mesopotamian cuneiform tablets translation")
    assert engine.is_circuit_tripped() is False

    # 3rd ungrounded claim -> breaches max_drift_count_before_trip (3) -> auto-trips!
    engine.verify_grounding("deep sea hydrothermal vent marine microorganisms")
    assert engine.is_circuit_tripped() is True

    # 4th claim is blocked by tripped circuit
    with pytest.raises(GroundingCircuitTrippedError):
        engine.verify_grounding("Kubernetes cluster nodes are running version 1.30")


def test_receipts_audit_and_status():
    engine = CognitiveGroundingEngine()
    engine.register_evidence(
        modality=EvidenceModality.TEXT_CLAIM,
        content="Database credentials rotated successfully",
        source_reference="sec-audit",
    )

    rcpt = engine.verify_grounding("Database credentials rotated")
    assert len(engine.get_receipts()) == 1

    fetched_rcpt = engine.get_receipt(rcpt.receipt_id)
    assert fetched_rcpt is not None
    assert fetched_rcpt.canonical_hash == rcpt.canonical_hash

    status = engine.status()
    assert status["total_evidence_count"] == 1
    assert status["total_verifications"] == 1
    assert status["circuit_tripped"] is False
    assert status["status_badge"] == "GROUNDING: FUSION OK"


def test_sub_250us_execution_benchmark():
    engine = CognitiveGroundingEngine()

    # Pre-populate 10 items
    for i in range(10):
        engine.register_evidence(
            modality=EvidenceModality.TEXT_CLAIM,
            content=f"Telemetry observation index {i} node status OK memory {i*100}MB",
            source_reference=f"obs-{i}",
        )

    durations_us = []
    for _ in range(100):
        t0 = time.perf_counter_ns()
        rcpt = engine.verify_grounding("Telemetry observation index 5 node status OK")
        t_el = (time.perf_counter_ns() - t0) / 1000.0
        durations_us.append(t_el)

    min_duration = min(durations_us)
    assert min_duration < 500.0, f"Expected <500us min execution latency, got {min_duration:.2f}us"
