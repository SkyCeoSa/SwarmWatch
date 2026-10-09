"""
Tests for SwarmWatch Autonomous Agent Capability Attestation & Hardware Secure Enclave (SGX/TEE) Remote Witness (Seam 98 / ADR 0109).
Verifies TEE1 (Measurement Integrity), TEE2 (Nonce Freshness & Anti-Replay),
TEE3 (Lease-Bounded Capability Delegation), TEE4 (Enclave Quarantine & Safety Circuit Latching),
and sub-250 microsecond execution latency SLO.
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload
from swarmwatch.enclave_attestation import (
    EnclaveAttestationCoordinator,
    EnclaveMeasurement,
    AttestationQuote,
    EnclaveCapabilityToken,
    EnclaveMeasurementMismatchError,
    StaleAttestationReportError,
    UnauthorizedCapabilityError,
    EnclaveQuarantinedError,
    EnclaveCircuitTrippedError,
    EnclaveAttestationError,
    compute_enclave_measurement_hash,
    compute_quote_digest,
)


@pytest.fixture
def clean_enclave():
    bus = SafetyCircuitBus()
    coord = EnclaveAttestationCoordinator(
        coordinator_id="test-enclave-coord",
        failure_threshold=3,
        challenge_ttl_sec=5.0,
        default_lease_sec=60.0,
        circuit_bus=bus,
    )
    meas = EnclaveMeasurement(
        enclave_id="enc-01",
        tee_type="SGX",
        code_hash="c" * 64,
        config_hash="f" * 64,
        signer_id="intel-ias-signer",
        is_approved=True,
    )
    mhash = coord.register_approved_measurement(meas)
    privkey, pubkey = generate_node_keypair()
    return coord, bus, mhash, privkey, pubkey


def make_signed_quote(
    enclave_id: str,
    tee_type: str,
    measurement_hash: str,
    nonce: str,
    privkey: str,
    pubkey: str,
    timestamp_ns: int,
    user_data: str = "udata",
) -> AttestationQuote:
    digest = compute_quote_digest(
        enclave_id=enclave_id,
        tee_type=tee_type,
        measurement_hash=measurement_hash,
        nonce=nonce,
        user_data=user_data,
        timestamp_ns=timestamp_ns,
    )
    sig = sign_bft_payload(privkey, digest)
    return AttestationQuote(
        quote_id=f"q-{nonce[:8]}",
        enclave_id=enclave_id,
        tee_type=tee_type,
        measurement_hash=measurement_hash,
        challenge_nonce=nonce,
        public_key=pubkey,
        signature=sig,
        timestamp_ns=timestamp_ns,
        user_data_digest=user_data,
    )


def test_measurement_registration_and_quote_verification_tee1(clean_enclave):
    coord, bus, mhash, privkey, pubkey = clean_enclave

    chal = coord.issue_challenge("enc-01")
    now_ns = time.time_ns()
    quote = make_signed_quote("enc-01", "SGX", mhash, chal.nonce, privkey, pubkey, now_ns)

    token = coord.verify_and_attest(quote, "agent-007", ["TASK_EXECUTE", "SETTLEMENT_READ"])
    assert token.enclave_id == "enc-01"
    assert token.agent_id == "agent-007"
    assert "TASK_EXECUTE" in token.capabilities

    # Unapproved / altered measurement fails fail-closed (TEE1)
    chal2 = coord.issue_challenge("enc-01")
    bad_mhash = "0" * 64
    quote_bad_meas = make_signed_quote("enc-01", "SGX", bad_mhash, chal2.nonce, privkey, pubkey, now_ns)
    with pytest.raises(EnclaveMeasurementMismatchError):
        coord.verify_and_attest(quote_bad_meas, "agent-007", ["TASK_EXECUTE"])


def test_challenge_nonce_freshness_and_anti_replay_tee2(clean_enclave):
    coord, bus, mhash, privkey, pubkey = clean_enclave

    chal = coord.issue_challenge("enc-01")
    now_ns = time.time_ns()
    quote = make_signed_quote("enc-01", "SGX", mhash, chal.nonce, privkey, pubkey, now_ns)

    # First verification succeeds
    coord.verify_and_attest(quote, "agent-007", ["TASK_EXECUTE"])

    # Replay of the same quote with consumed nonce fails (TEE2)
    with pytest.raises(StaleAttestationReportError):
        coord.verify_and_attest(quote, "agent-007", ["TASK_EXECUTE"])

    # Unknown challenge nonce fails
    quote_fake_nonce = make_signed_quote("enc-01", "SGX", mhash, "fake-nonce" * 4, privkey, pubkey, now_ns)
    with pytest.raises(StaleAttestationReportError):
        coord.verify_and_attest(quote_fake_nonce, "agent-007", ["TASK_EXECUTE"])


def test_lease_bounded_capability_delegation_tee3(clean_enclave):
    coord, bus, mhash, privkey, pubkey = clean_enclave

    chal = coord.issue_challenge("enc-01")
    now_ns = time.time_ns()
    quote = make_signed_quote("enc-01", "SGX", mhash, chal.nonce, privkey, pubkey, now_ns)

    token = coord.verify_and_attest(quote, "agent-007", ["TASK_EXECUTE", "FUNDS_TRANSFER"])

    # Valid capability succeeds
    assert coord.validate_capability(token, "TASK_EXECUTE") is True
    assert coord.validate_capability(token, "FUNDS_TRANSFER") is True

    # Unauthorized capability raises UnauthorizedCapabilityError
    with pytest.raises(UnauthorizedCapabilityError):
        coord.validate_capability(token, "ADMIN_CLUSTER_REARM")

    # Expired token raises UnauthorizedCapabilityError
    token.expires_at_ns = time.time_ns() - 1000  # simulate past expiration
    with pytest.raises(UnauthorizedCapabilityError):
        coord.validate_capability(token, "TASK_EXECUTE")


def test_consecutive_failures_quarantine_and_circuit_latching_tee4(clean_enclave):
    coord, bus, mhash, privkey, pubkey = clean_enclave
    assert not bus.is_tripped()

    # Repeated signature validation failures (threshold = 3)
    for i in range(3):
        chal = coord.issue_challenge("enc-01")
        now_ns = time.time_ns()
        # Bad signature
        bad_quote = AttestationQuote(
            quote_id=f"q-bad-{i}",
            enclave_id="enc-01",
            tee_type="SGX",
            measurement_hash=mhash,
            challenge_nonce=chal.nonce,
            public_key=pubkey,
            signature="bad-signature" * 5,
            timestamp_ns=now_ns,
            user_data_digest="",
        )
        try:
            coord.verify_and_attest(bad_quote, "agent-bad", ["READ"])
        except Exception:
            pass

    # Safety circuit is now tripped and enclave quarantined
    assert bus.is_tripped()
    assert bus.get_status().last_trip_event.reason == "TEE_HARDWARE_ATTESTATION_BREACH"

    # Subsequent challenge issuance fails fail-closed
    with pytest.raises(EnclaveCircuitTrippedError):
        coord.issue_challenge("enc-01")


def test_adversarial_forged_enclave_signatures_rejected(clean_enclave):
    coord, bus, mhash, privkey, pubkey = clean_enclave
    chal = coord.issue_challenge("enc-01")
    now_ns = time.time_ns()
    digest = compute_quote_digest("enc-01", "SGX", mhash, chal.nonce, "udata", now_ns)

    # 1. Literal test string format
    quote_literal = AttestationQuote(
        quote_id="q-adv-1",
        enclave_id="enc-01",
        tee_type="SGX",
        measurement_hash=mhash,
        challenge_nonce=chal.nonce,
        public_key=pubkey,
        signature=f"sig:{pubkey}:{digest[:16]}",
        timestamp_ns=now_ns,
        user_data_digest="udata",
    )
    with pytest.raises(EnclaveAttestationError):
        coord.verify_and_attest(quote_literal, "agent-adv", ["READ"])

    # 2. Corrupted token signature in capability verification
    valid_chal = coord.issue_challenge("enc-01")
    valid_quote = make_signed_quote("enc-01", "SGX", mhash, valid_chal.nonce, privkey, pubkey, now_ns)
    token = coord.verify_and_attest(valid_quote, "agent-adv", ["READ"])
    token.token_signature = "ff" + token.token_signature[2:]
    with pytest.raises(UnauthorizedCapabilityError):
        coord.validate_capability(token, "READ")


def test_performance_sub_250us_slo_and_zero_tokens(clean_enclave):
    coord, bus, mhash, privkey, pubkey = clean_enclave

    chal = coord.issue_challenge("enc-01")
    now_ns = time.time_ns()
    quote = make_signed_quote("enc-01", "SGX", mhash, chal.nonce, privkey, pubkey, now_ns)
    token = coord.verify_and_attest(quote, "agent-perf", ["EXECUTE"])

    # Warmup
    coord.validate_capability(token, "EXECUTE")

    latencies_us = []
    for _ in range(20):
        t0 = time.perf_counter_ns()
        coord.validate_capability(token, "EXECUTE")
        latencies_us.append((time.perf_counter_ns() - t0) / 1000.0)

    latencies_us.sort()
    median_us = latencies_us[len(latencies_us) // 2]
    assert median_us < 250.0, f"Execution latency {median_us:.2f}us exceeded 250us SLO."

    status = coord.get_status()
    assert status["total_attestations"] == 1
    assert status["active_capability_tokens"] == 1
    assert status["circuit_tripped"] is False
