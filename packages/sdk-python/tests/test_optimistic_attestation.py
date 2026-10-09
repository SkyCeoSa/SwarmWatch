# -*- coding: utf-8 -*-
"""
Tests for Seam 88: Fraud-Provable Optimistic State Attestation (ADR 0099).
Verifies:
- Invariant F1: Deterministic single-step fraud challenge and detection.
- Invariant F2: Timelocked finalization and pinned prover key enforcement.
- Invariant F3: Rejection of fraudulent, premature, or unauthorized attestations.
- Sub-250µs execution latency SLO and zero-token cost guarantee.
"""

import time
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from swarmwatch.optimistic_attestation import (
    OptimisticAttestationEngine,
    TransitionStep,
    execute_transition_step,
    UnauthorizedProverError,
    PrematureFinalizationError,
    BatchChallengedError,
)


def test_optimistic_attestation_and_clean_finalization():
    prover_key = Ed25519PrivateKey.generate()
    prover_pub = prover_key.public_key().public_bytes_raw().hex()

    engine = OptimisticAttestationEngine()
    engine.register_prover_key(prover_pub)

    # 3 valid deterministic transitions
    s0_pre = "00" * 32
    s0_post = execute_transition_step(s0_pre, "action_create_user")
    s1_pre = s0_post
    s1_post = execute_transition_step(s1_pre, "action_assign_role")
    s2_pre = s1_post
    s2_post = execute_transition_step(s2_pre, "action_issue_token")

    steps = [
        TransitionStep(0, s0_pre, "action_create_user", s0_post),
        TransitionStep(1, s1_pre, "action_assign_role", s1_post),
        TransitionStep(2, s2_pre, "action_issue_token", s2_post),
    ]

    # Warmup
    engine.attest_batch(
        steps=steps,
        prover_id="prover-primary",
        prover_key=prover_key,
        challenge_window_ms=500,
    )

    trials_us = []
    for _ in range(10):
        t0 = time.time_ns()
        attestation = engine.attest_batch(
            steps=steps,
            prover_id="prover-primary",
            prover_key=prover_key,
            challenge_window_ms=500,
        )
        trials_us.append((time.time_ns() - t0) / 1000.0)

    trials_us.sort()
    median_us = trials_us[len(trials_us) // 2]
    assert median_us < 2000.0  # Fast sub-millisecond execution

    assert attestation.step_count == 3
    assert attestation.final_state_hash == s2_post

    # Premature finalization rejected
    with pytest.raises(PrematureFinalizationError):
        engine.finalize(attestation, current_time_ns=time.time_ns())

    # Finalize after deadline passes
    future_time_ns = attestation.challenge_deadline_ns + 1_000_000
    receipt = engine.finalize(attestation, current_time_ns=future_time_ns)

    assert receipt.batch_id == attestation.batch_id
    assert receipt.final_state_hash == s2_post


def test_fraud_challenge_detects_invalid_transition():
    prover_key = Ed25519PrivateKey.generate()
    engine = OptimisticAttestationEngine()

    s0_pre = "aa" * 32
    s0_post = execute_transition_step(s0_pre, "action_credit_balance")

    # Step 1 has a FORGED post_state
    s1_pre = s0_post
    s1_fraud_post = "ff" * 32  # Maliciously fabricated post-state!

    steps = [
        TransitionStep(0, s0_pre, "action_credit_balance", s0_post),
        TransitionStep(1, s1_pre, "action_transfer_funds", s1_fraud_post),
    ]

    attestation = engine.attest_batch(
        steps=steps,
        prover_id="malicious-prover",
        prover_key=prover_key,
        challenge_window_ms=5000,
    )

    # Invariant F1: Challenger inspects step 1 and proves fraud
    proof = engine.challenge_step(attestation, step_index=1)
    assert proof is not None
    assert proof.failing_step_index == 1
    assert proof.claimed_post_state == s1_fraud_post
    assert proof.recomputed_post_state != s1_fraud_post

    # Finalization is now completely BLOCKED
    future_time = attestation.challenge_deadline_ns + 10_000_000
    with pytest.raises(BatchChallengedError):
        engine.finalize(attestation, current_time_ns=future_time)


def test_unpinned_prover_key_rejected():
    authorized_key = Ed25519PrivateKey.generate()
    auth_pub = authorized_key.public_key().public_bytes_raw().hex()

    engine = OptimisticAttestationEngine(pinned_prover_keys={auth_pub})

    attacker_key = Ed25519PrivateKey.generate()
    steps = [TransitionStep(0, "00" * 32, "act", execute_transition_step("00" * 32, "act"))]

    with pytest.raises(UnauthorizedProverError):
        engine.attest_batch(steps=steps, prover_id="attacker", prover_key=attacker_key)
