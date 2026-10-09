"""
Tests for SwarmWatch Real-Time Epistemic Dispute Resolution & Dynamic Slashing Escalation Tribunal (Seam 97 / ADR 0108).
Verifies TRB1 (Multi-Tiered Dispute Escalation), TRB2 (Severity-Graded Slashing Matrix),
TRB3 (Super-Majority Arbiter Finality), TRB4 (Fail-Closed Safety Circuit Latching),
and sub-250 microsecond execution latency SLO.
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload
from swarmwatch.slashing_tribunal import (
    SlashingEscalationTribunal,
    DisputeClaim,
    TribunalArbiter,
    TribunalVerdictReceipt,
    DisputeNotFoundError,
    InvalidDisputeTierError,
    InsufficientArbiterQuorumError,
    TribunalCircuitTrippedError,
    compute_verdict_digest,
)


@pytest.fixture
def clean_tribunal():
    bus = SafetyCircuitBus()
    tribunal = SlashingEscalationTribunal(
        tribunal_id="test-tribunal",
        surge_threshold=3,
        circuit_bus=bus,
    )
    # Register 3 active arbiters (quorum = 2*3//3 + 1 = 3)
    priv1, pub1 = generate_node_keypair()
    priv2, pub2 = generate_node_keypair()
    priv3, pub3 = generate_node_keypair()
    arb1 = TribunalArbiter(arbiter_id="arb-01", public_key=pub1, bonded_stake=1000.0, is_active=True)
    arb2 = TribunalArbiter(arbiter_id="arb-02", public_key=pub2, bonded_stake=1000.0, is_active=True)
    arb3 = TribunalArbiter(arbiter_id="arb-03", public_key=pub3, bonded_stake=1000.0, is_active=True)
    tribunal.register_arbiter(arb1)
    tribunal.register_arbiter(arb2)
    tribunal.register_arbiter(arb3)
    keys = {"arb-01": priv1, "arb-02": priv2, "arb-03": priv3}
    return tribunal, bus, keys


def make_signed_votes(tribunal: SlashingEscalationTribunal, dispute_id: str, ruling: str, slashed_amount: float, recipient_id: str, hlc_ts: str, arbiter_ids: list[str], keys: dict[str, str]) -> dict[str, str]:
    claim = tribunal.get_dispute(dispute_id)
    assert claim is not None
    digest = compute_verdict_digest(
        dispute_id=dispute_id,
        tier=claim.tier,
        verdict=ruling,
        slashed_amount=slashed_amount,
        recipient_id=recipient_id,
        hlc_ts=hlc_ts,
    )
    votes = {}
    for arb_id in arbiter_ids:
        priv = keys[arb_id]
        sig = sign_bft_payload(priv, digest)
        votes[arb_id] = sig
    return votes


def test_dispute_lifecycle_and_monotonic_escalation_trb1(clean_tribunal):
    tribunal, bus, keys = clean_tribunal

    # 1. File Tier 1 Dispute
    claim = tribunal.file_dispute(
        claimant_id="agent-alice",
        defendant_id="agent-bob",
        severity="MEDIUM",
        evidence_digest="e" * 64,
        dispute_bond=20.0,
    )
    assert claim.tier == 1
    assert claim.dispute_bond == 20.0
    assert claim.status == "OPEN"

    # 2. Escalate to Tier 2 (Committee)
    claim2 = tribunal.escalate_dispute(claim.dispute_id, additional_bond=20.0)
    assert claim2.tier == 2
    assert claim2.dispute_bond == 40.0
    assert claim2.status == "ESCALATED"

    # 3. Escalate to Tier 3 (Tribunal Appellate)
    claim3 = tribunal.escalate_dispute(claim.dispute_id, additional_bond=40.0)
    assert claim3.tier == 3
    assert claim3.dispute_bond == 80.0

    # 4. Attempt invalid escalation beyond Tier 3
    with pytest.raises(InvalidDisputeTierError):
        tribunal.escalate_dispute(claim.dispute_id, additional_bond=80.0)

    # 5. Non-existent dispute escalation raises DisputeNotFoundError
    with pytest.raises(DisputeNotFoundError):
        tribunal.escalate_dispute("non-existent-dispute", additional_bond=50.0)


def test_severity_graded_dynamic_slashing_trb2(clean_tribunal):
    tribunal, bus, keys = clean_tribunal

    # Test LOW severity: 5% slash
    d_low = tribunal.file_dispute("alice", "bob", severity="LOW", dispute_bond=10.0)
    hlc_ts = str(tribunal._hlc.now())
    votes_low = make_signed_votes(tribunal, d_low.dispute_id, "RESOLVED_CLAIMANT", 5.0, "alice", hlc_ts, ["arb-01", "arb-02", "arb-03"], keys)
    rec_low = tribunal.adjudicate_dispute(d_low.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=votes_low, hlc_timestamp=hlc_ts)
    assert rec_low.slashed_amount == 5.0  # 5% of 100.0
    assert rec_low.recipient_id == "alice"

    # Test MEDIUM severity: 25% slash
    d_med = tribunal.file_dispute("charlie", "dave", severity="MEDIUM", dispute_bond=10.0)
    hlc_ts_med = str(tribunal._hlc.now())
    votes_med = make_signed_votes(tribunal, d_med.dispute_id, "RESOLVED_CLAIMANT", 25.0, "charlie", hlc_ts_med, ["arb-01", "arb-02", "arb-03"], keys)
    rec_med = tribunal.adjudicate_dispute(d_med.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=votes_med, hlc_timestamp=hlc_ts_med)
    assert rec_med.slashed_amount == 25.0  # 25% of 100.0

    # Test CRITICAL severity: 100% slash
    d_crit = tribunal.file_dispute("eve", "mallory", severity="CRITICAL", dispute_bond=10.0)
    hlc_ts_crit = str(tribunal._hlc.now())
    votes_crit = make_signed_votes(tribunal, d_crit.dispute_id, "RESOLVED_CLAIMANT", 100.0, "eve", hlc_ts_crit, ["arb-01", "arb-02", "arb-03"], keys)
    rec_crit = tribunal.adjudicate_dispute(d_crit.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=votes_crit, hlc_timestamp=hlc_ts_crit)
    assert rec_crit.slashed_amount == 100.0  # 100% of 100.0

    # Test Defendant wins: 0% slash
    d_def = tribunal.file_dispute("frank", "grace", severity="CRITICAL", dispute_bond=10.0)
    hlc_ts_def = str(tribunal._hlc.now())
    votes_def = make_signed_votes(tribunal, d_def.dispute_id, "RESOLVED_DEFENDANT", 0.0, "grace", hlc_ts_def, ["arb-01", "arb-02", "arb-03"], keys)
    rec_def = tribunal.adjudicate_dispute(d_def.dispute_id, "RESOLVED_DEFENDANT", defendant_collateral=100.0, arbiter_votes=votes_def, hlc_timestamp=hlc_ts_def)
    assert rec_def.slashed_amount == 0.0
    assert rec_def.recipient_id == "grace"


def test_super_majority_arbiter_quorum_trb3(clean_tribunal):
    tribunal, bus, keys = clean_tribunal
    d = tribunal.file_dispute("user1", "user2", severity="MEDIUM", dispute_bond=10.0)

    # Only 1 out of 3 votes provided (quorum requires 3)
    hlc_ts = str(tribunal._hlc.now())
    votes_insufficient = make_signed_votes(tribunal, d.dispute_id, "RESOLVED_CLAIMANT", 25.0, "user1", hlc_ts, ["arb-01"], keys)
    with pytest.raises(InsufficientArbiterQuorumError):
        tribunal.adjudicate_dispute(d.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=votes_insufficient, hlc_timestamp=hlc_ts)

    # 3 out of 3 votes succeeds
    votes_full = make_signed_votes(tribunal, d.dispute_id, "RESOLVED_CLAIMANT", 25.0, "user1", hlc_ts, ["arb-01", "arb-02", "arb-03"], keys)
    receipt = tribunal.adjudicate_dispute(d.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=votes_full, hlc_timestamp=hlc_ts)
    assert receipt.verdict == "RESOLVED_CLAIMANT"
    assert len(receipt.arbiter_signatures) == 3


def test_fail_closed_surge_circuit_latching_trb4(clean_tribunal):
    tribunal, bus, keys = clean_tribunal
    assert not bus.is_tripped()

    # Surge threshold is 3 critical disputes
    tribunal.file_dispute("c1", "d1", severity="CRITICAL", dispute_bond=10.0)
    tribunal.file_dispute("c2", "d2", severity="CRITICAL", dispute_bond=10.0)
    assert not bus.is_tripped()

    # Third critical dispute triggers surge trip
    tribunal.file_dispute("c3", "d3", severity="CRITICAL", dispute_bond=10.0)
    assert bus.is_tripped()
    assert bus.get_status().last_trip_event.reason == "TRIBUNAL_SYSTEMIC_DISPUTE_SURGE"

    # Subsequent dispute filing fails fail-closed
    with pytest.raises(TribunalCircuitTrippedError):
        tribunal.file_dispute("c4", "d4", severity="LOW", dispute_bond=10.0)


def test_adversarial_forged_tribunal_signatures_rejected(clean_tribunal):
    tribunal, bus, keys = clean_tribunal
    d = tribunal.file_dispute("adv_c", "adv_d", severity="MEDIUM", dispute_bond=10.0)
    hlc_ts = str(tribunal._hlc.now())
    digest = compute_verdict_digest(d.dispute_id, d.tier, "RESOLVED_CLAIMANT", 25.0, "adv_c", hlc_ts)

    # 1. Test literal string pattern
    forged_literal = {
        "arb-01": f"sig:{tribunal._arbiters['arb-01'].public_key}:{digest[:16]}",
        "arb-02": f"sig:{tribunal._arbiters['arb-02'].public_key}:{digest[:16]}",
        "arb-03": f"sig:{tribunal._arbiters['arb-03'].public_key}:{digest[:16]}",
    }
    with pytest.raises(InsufficientArbiterQuorumError):
        tribunal.adjudicate_dispute(d.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=forged_literal, hlc_timestamp=hlc_ts)

    # 2. Corrupted signature
    real_sig = sign_bft_payload(keys["arb-01"], digest)
    corrupted_sig = "ff" + real_sig[2:]
    forged_corrupted = {
        "arb-01": corrupted_sig,
        "arb-02": corrupted_sig,
        "arb-03": corrupted_sig,
    }
    with pytest.raises(InsufficientArbiterQuorumError):
        tribunal.adjudicate_dispute(d.dispute_id, "RESOLVED_CLAIMANT", defendant_collateral=100.0, arbiter_votes=forged_corrupted, hlc_timestamp=hlc_ts)


def test_performance_sub_250us_slo_and_zero_tokens(clean_tribunal):
    tribunal, bus, keys = clean_tribunal

    # Benchmark dispute filing and escalation
    t0 = time.perf_counter_ns()
    d = tribunal.file_dispute("alice", "bob", severity="LOW", dispute_bond=10.0)
    d2 = tribunal.escalate_dispute(d.dispute_id, additional_bond=10.0)
    t1 = time.perf_counter_ns()

    elapsed_us = (t1 - t0) / 1000.0
    assert elapsed_us < 250.0, f"Execution latency {elapsed_us:.2f}us exceeded 250us SLO."

    status = tribunal.get_status()
    assert status["total_disputes_count"] == 1
    assert status["open_disputes_count"] == 1
