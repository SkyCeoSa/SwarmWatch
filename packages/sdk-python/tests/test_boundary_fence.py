"""
Tests for SwarmWatch Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault (Seam 99 / ADR 0110).
Verifies BND1 (Geographic Residency Policy Constraint), BND2 (Zero-Egress Vault Encryption),
BND3 (Cryptographic Egress Export Passport), BND4 (Breach Containment & Safety Circuit Latching),
and sub-250 microsecond execution latency SLO.
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.boundary_fence import (
    DataBoundaryFence,
    ResidencyZone,
    EgressTreaty,
    VaultEnvelope,
    EgressExportPassport,
    CrossBorderResidencyViolationError,
    InvalidEgressPassportError,
    FenceZoneQuarantinedError,
    FenceCircuitTrippedError,
)


@pytest.fixture
def clean_fence():
    bus = SafetyCircuitBus()
    fence = DataBoundaryFence(
        fence_id="test-boundary-fence",
        breach_threshold=3,
        passport_ttl_sec=30.0,
        circuit_bus=bus,
    )
    z_eu = ResidencyZone(zone_id="EU_DE", jurisdiction="Germany", compliance_tags=["GDPR"])
    z_us = ResidencyZone(zone_id="US_EAST", jurisdiction="United States", compliance_tags=["SOC2"])
    z_ch = ResidencyZone(zone_id="CH_ZH", jurisdiction="Switzerland", compliance_tags=["FADP"])
    fence.register_residency_zone(z_eu)
    fence.register_residency_zone(z_us)
    fence.register_residency_zone(z_ch)
    return fence, bus


def test_geofenced_residency_and_treaty_enforcement_bnd1(clean_fence):
    fence, bus = clean_fence

    # 1. Intra-zone transfer needs no treaty or passport
    env_local = fence.seal_data("tenant-acme", "EU_DE", "local-sensitive-payload")
    res_local = fence.inspect_egress(env_local, "EU_DE")
    assert res_local.allowed is True
    assert res_local.reason == "LOCAL_INTRA_ZONE_TRANSFER"

    # 2. Cross-border transfer without active treaty raises CrossBorderResidencyViolationError (BND1)
    with pytest.raises(CrossBorderResidencyViolationError):
        fence.issue_egress_passport("tenant-acme", "EU_DE", "US_EAST", env_local.data_digest)

    # 3. Register treaty EU_DE -> US_EAST
    treaty = EgressTreaty(
        treaty_id="trt-eu-us",
        source_zone="EU_DE",
        dest_zone="US_EAST",
        is_bidirectional=True,
        is_active=True,
    )
    fence.register_egress_treaty(treaty)

    # 4. Now passport issuance succeeds
    passport = fence.issue_egress_passport("tenant-acme", "EU_DE", "US_EAST", env_local.data_digest)
    assert passport.source_zone == "EU_DE"
    assert passport.dest_zone == "US_EAST"


def test_zero_egress_vault_envelope_encryption_and_integrity_bnd2(clean_fence):
    fence, bus = clean_fence

    raw_data = "sensitive-customer-pii-record-48291"
    envelope = fence.seal_data("tenant-alpha", "EU_DE", raw_data)
    assert envelope.origin_zone == "EU_DE"
    assert envelope.tenant_id == "tenant-alpha"
    assert len(envelope.ciphertext_hex) > 0

    # Unsealing recovers plaintext bitwise
    recovered = fence.unseal_data(envelope)
    assert recovered.decode("utf-8") == raw_data

    # Tampered envelope fails decryption/authentication
    tampered_env = VaultEnvelope(
        envelope_id=envelope.envelope_id,
        tenant_id=envelope.tenant_id,
        origin_zone=envelope.origin_zone,
        ciphertext_hex="ff" * len(bytes.fromhex(envelope.ciphertext_hex)),
        tag_hex=envelope.tag_hex,
        iv_hex=envelope.iv_hex,
        data_digest=envelope.data_digest,
        created_at_ns=envelope.created_at_ns,
    )
    with pytest.raises(Exception):
        fence.unseal_data(tampered_env)


def test_cryptographic_egress_passport_validation_and_anti_replay_bnd3(clean_fence):
    fence, bus = clean_fence
    treaty = EgressTreaty(treaty_id="t1", source_zone="EU_DE", dest_zone="CH_ZH", is_active=True)
    fence.register_egress_treaty(treaty)

    env = fence.seal_data("tenant-bank", "EU_DE", "swiss-banking-telemetry")
    passport = fence.issue_egress_passport("tenant-bank", "EU_DE", "CH_ZH", env.data_digest)

    # First inspection succeeds and consumes passport
    res = fence.inspect_egress(env, "CH_ZH", passport)
    assert res.allowed is True
    assert res.reason == "CROSS_BORDER_PASSPORT_VERIFIED"

    # Replay of the consumed passport fails fail-closed (BND3)
    with pytest.raises(InvalidEgressPassportError):
        fence.inspect_egress(env, "CH_ZH", passport)

    # Route mismatch passport fails
    passport2 = fence.issue_egress_passport("tenant-bank", "EU_DE", "CH_ZH", env.data_digest)
    passport2.dest_zone = "US_EAST"  # mismatch
    with pytest.raises(InvalidEgressPassportError):
        fence.inspect_egress(env, "CH_ZH", passport2)


def test_breach_containment_quarantine_and_safety_circuit_latching_bnd4(clean_fence):
    fence, bus = clean_fence
    assert not bus.is_tripped()

    env = fence.seal_data("tenant-bad", "EU_DE", "data-payload")

    # 3 consecutive breaches on EU_DE (breach threshold = 3)
    for _ in range(3):
        try:
            # Transfer without passport
            fence.inspect_egress(env, "US_EAST", passport=None)
        except InvalidEgressPassportError:
            pass

    # Safety circuit is now tripped and zone quarantined
    assert bus.is_tripped()
    assert bus.get_status().last_trip_event.reason == "BOUNDARY_FENCE_EGRESS_BREACH"
    assert fence._zones["EU_DE"].is_quarantined is True

    # Subsequent operations on quarantined zone fail
    with pytest.raises(FenceCircuitTrippedError):
        fence.seal_data("tenant-any", "EU_DE", "any-data")


def test_performance_sub_250us_slo_and_zero_tokens(clean_fence):
    fence, bus = clean_fence
    treaty = EgressTreaty(treaty_id="t-perf", source_zone="EU_DE", dest_zone="US_EAST", is_active=True)
    fence.register_egress_treaty(treaty)

    env = fence.seal_data("tenant-perf", "EU_DE", "telemetry-span-record")
    passport = fence.issue_egress_passport("tenant-perf", "EU_DE", "US_EAST", env.data_digest)

    # Warmup
    fence.inspect_egress(env, "US_EAST", passport)

    # Benchmark hot-path perimeter inspection with median filtering against OS scheduler jitter
    latencies_us = []
    for _ in range(20):
        passport.is_consumed = False
        t0 = time.perf_counter_ns()
        res = fence.inspect_egress(env, "US_EAST", passport)
        latencies_us.append((time.perf_counter_ns() - t0) / 1000.0)

    latencies_us.sort()
    median_us = latencies_us[len(latencies_us) // 2]
    assert median_us < 250.0, f"Execution latency {median_us:.2f}us exceeded 250us SLO."

    status = fence.get_status()
    assert status["total_sealed_envelopes"] == 1
    assert status["total_inspections"] >= 1
    assert status["circuit_tripped"] is False


def test_adversarial_forged_passport_signatures_rejected(clean_fence):
    fence, bus = clean_fence
    treaty = EgressTreaty(treaty_id="t-adv", source_zone="EU_DE", dest_zone="US_EAST", is_active=True)
    fence.register_egress_treaty(treaty)

    env = fence.seal_data("tenant-adv", "EU_DE", "confidential-payload")
    passport = fence.issue_egress_passport("tenant-adv", "EU_DE", "US_EAST", env.data_digest)

    # 1. Attacker attempts to forge signature with string template
    passport.passport_signature = f"sig:{fence.fence_id}:{passport.payload_digest[:16]}"
    with pytest.raises(InvalidEgressPassportError):
        fence.inspect_egress(env, "US_EAST", passport)

    # 2. Attacker attempts random hex signature
    passport.passport_signature = "baddecaf" * 8
    with pytest.raises(InvalidEgressPassportError):
        fence.inspect_egress(env, "US_EAST", passport)

