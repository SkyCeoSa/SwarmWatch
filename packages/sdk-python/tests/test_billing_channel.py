"""
Tests for SwarmWatch Autonomous Cloud SaaS Billing, Usage Metering & Micro-Payment Channel Ledger (Seam 95 / ADR 0106).
Verifies MPC1 (Balance Conservation), MPC2 (Monotonic Nonces & Signatures),
MPC3 (Dispute Window & Challenge Slashing), MPC4 (Usage Metering & Circuit Latching),
and sub-250 microsecond execution latency SLO.
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.hlc import HybridLogicalClock
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload
from swarmwatch.billing_channel import (
    MicroPaymentChannelLedger,
    ChannelStateCommitment,
    ChannelBalanceConservationError,
    StaleChannelNonceError,
    InvalidChannelSignatureError,
    ChannelDisputeActiveError,
    BillingChannelExhaustedError,
    compute_channel_commitment_digest,
)


@pytest.fixture
def clean_ledger():
    bus = SafetyCircuitBus()
    ledger = MicroPaymentChannelLedger(ledger_id="test-billing-ledger", circuit_bus=bus)
    return ledger, bus


def make_signed_commitment(
    channel_id: str,
    nonce: int,
    agent_balance: float,
    provider_balance: float,
    agent_priv: str,
    provider_priv: str,
    hlc_ts: str = "hlc:100:0:node",
) -> ChannelStateCommitment:
    digest = compute_channel_commitment_digest(
        channel_id=channel_id,
        nonce=nonce,
        agent_balance=agent_balance,
        provider_balance=provider_balance,
        hlc_timestamp=hlc_ts,
    )
    agent_sig = sign_bft_payload(agent_priv, digest)
    provider_sig = sign_bft_payload(provider_priv, digest)
    return ChannelStateCommitment(
        channel_id=channel_id,
        nonce=nonce,
        agent_balance=agent_balance,
        provider_balance=provider_balance,
        commitment_digest=digest,
        agent_signature=agent_sig,
        provider_signature=provider_sig,
        hlc_timestamp=hlc_ts,
    )


def test_channel_opening_and_balance_conservation_mpc1(clean_ledger):
    ledger, bus = clean_ledger
    agent_priv, agent_pub = generate_node_keypair()
    provider_priv, provider_pub = generate_node_keypair()

    # Reject non-positive deposit
    with pytest.raises(ChannelBalanceConservationError):
        ledger.open_channel("tenant-alpha", agent_pub, provider_pub, initial_deposit=-10.0)

    channel = ledger.open_channel(
        tenant_id="tenant-alpha",
        agent_pubkey=agent_pub,
        provider_pubkey=provider_pub,
        initial_deposit=100.0,
        reserve_threshold=5.0,
        channel_id="chan-001",
    )
    assert channel.channel_id == "chan-001"
    assert channel.agent_balance == 100.0
    assert channel.provider_balance == 0.0
    assert channel.status == "OPEN"

    # Valid update conserving 100.0
    comm = make_signed_commitment("chan-001", 1, 80.0, 20.0, agent_priv, provider_priv)
    updated = ledger.submit_state_update(comm)
    assert updated.nonce == 1
    assert channel.agent_balance == 80.0
    assert channel.provider_balance == 20.0

    # Breach balance conservation: 80 + 30 != 100
    breach = make_signed_commitment("chan-001", 2, 80.0, 30.0, agent_priv, provider_priv)
    with pytest.raises(ChannelBalanceConservationError):
        ledger.submit_state_update(breach)


def test_monotonic_nonces_and_signatures_mpc2(clean_ledger):
    ledger, bus = clean_ledger
    agent_priv, agent_pub = generate_node_keypair()
    provider_priv, provider_pub = generate_node_keypair()

    ledger.open_channel("tenant-beta", agent_pub, provider_pub, 50.0, channel_id="chan-002")

    comm1 = make_signed_commitment("chan-002", 5, 45.0, 5.0, agent_priv, provider_priv)
    ledger.submit_state_update(comm1)

    # Stale nonce: lower or equal
    stale = make_signed_commitment("chan-002", 5, 40.0, 10.0, agent_priv, provider_priv)
    with pytest.raises(StaleChannelNonceError):
        ledger.submit_state_update(stale)

    stale_lower = make_signed_commitment("chan-002", 3, 40.0, 10.0, agent_priv, provider_priv)
    with pytest.raises(StaleChannelNonceError):
        ledger.submit_state_update(stale_lower)

    # Invalid signature
    forged = make_signed_commitment("chan-002", 6, 40.0, 10.0, agent_priv, provider_priv)
    forged.agent_signature = "00" * 64
    with pytest.raises(InvalidChannelSignatureError):
        ledger.submit_state_update(forged)


def test_dispute_window_and_adversarial_challenge_slashing_mpc3(clean_ledger):
    ledger, bus = clean_ledger
    agent_priv, agent_pub = generate_node_keypair()
    provider_priv, provider_pub = generate_node_keypair()

    ledger.open_channel("tenant-gamma", agent_pub, provider_pub, 200.0, channel_id="chan-003")

    # Honest progress up to nonce 10
    comm10 = make_signed_commitment("chan-003", 10, 150.0, 50.0, agent_priv, provider_priv)
    ledger.submit_state_update(comm10)

    # Initiates closure
    closing = ledger.initiate_closure("chan-003")
    assert closing.status == "CLOSING"

    # Further normal state update rejected during CLOSING
    normal_comm = make_signed_commitment("chan-003", 11, 140.0, 60.0, agent_priv, provider_priv)
    with pytest.raises(ChannelDisputeActiveError):
        ledger.submit_state_update(normal_comm)

    # Counterparty reveals higher signed state from offline communication (nonce 15)
    higher_comm = make_signed_commitment("chan-003", 15, 100.0, 100.0, agent_priv, provider_priv)
    slash_cert = ledger.challenge_closure("chan-003", higher_comm)

    assert slash_cert.settlement_type == "CHEATER_SLASHED"
    assert slash_cert.final_agent_balance == 0.0
    assert slash_cert.final_provider_balance == 200.0  # Full deposit forfeited
    chan = ledger.get_channel("chan-003")
    assert chan.status == "SLASHED"


def test_realtime_usage_metering_and_circuit_latching_mpc4(clean_ledger):
    ledger, bus = clean_ledger
    agent_priv, agent_pub = generate_node_keypair()
    provider_priv, provider_pub = generate_node_keypair()

    ledger.open_channel(
        tenant_id="tenant-delta",
        agent_pubkey=agent_pub,
        provider_pubkey=provider_pub,
        initial_deposit=10.0,
        reserve_threshold=3.0,
        channel_id="chan-004",
    )

    # 1st meter event: 5000 spans at $0.001 = $5.0
    evt1 = ledger.record_meter_event("chan-004", "TELEMETRY_SPAN", 5000, 0.001)
    assert evt1.total_charge == 5.0
    chan = ledger.get_channel("chan-004")
    assert chan.agent_balance == 5.0
    assert not bus.is_tripped()

    # 2nd meter event: 3000 spans at $0.001 = $3.0
    # Balance drops to 2.0 (< reserve_threshold 3.0) -> trips SafetyCircuitBus
    evt2 = ledger.record_meter_event("chan-004", "TELEMETRY_SPAN", 3000, 0.001)
    assert evt2.total_charge == 3.0
    assert chan.agent_balance == 2.0
    assert bus.is_tripped()
    trip_event = bus.get_status().last_trip_event
    assert trip_event.reason == "BILLING_CHANNEL_DEPLETED"


def test_adversarial_forged_channel_signatures_rejected(clean_ledger):
    import hmac, hashlib
    ledger, bus = clean_ledger
    agent_priv, agent_pub = generate_node_keypair()
    provider_priv, provider_pub = generate_node_keypair()

    ledger.open_channel("tenant-adv", agent_pub, provider_pub, 100.0, channel_id="chan-adv")

    digest = compute_channel_commitment_digest("chan-adv", 1, 90.0, 10.0, "hlc:1:0:node")

    # Attempt 1: Forged HMAC of pubkey is rejected
    forged_hmac = hmac.new(agent_pub.encode(), digest.encode(), hashlib.sha256).hexdigest()
    comm_hmac = ChannelStateCommitment(
        channel_id="chan-adv", nonce=1, agent_balance=90.0, provider_balance=10.0,
        commitment_digest=digest, agent_signature=forged_hmac,
        provider_signature=sign_bft_payload(provider_priv, digest), hlc_timestamp="hlc:1:0:node"
    )
    with pytest.raises(InvalidChannelSignatureError):
        ledger.submit_state_update(comm_hmac)

    # Attempt 2: Forged test string format "sig:<pubkey>:<digest>" is rejected
    forged_direct = f"sig:{agent_pub}:{digest[:16]}"
    comm_direct = ChannelStateCommitment(
        channel_id="chan-adv", nonce=1, agent_balance=90.0, provider_balance=10.0,
        commitment_digest=digest, agent_signature=forged_direct,
        provider_signature=sign_bft_payload(provider_priv, digest), hlc_timestamp="hlc:1:0:node"
    )
    with pytest.raises(InvalidChannelSignatureError):
        ledger.submit_state_update(comm_direct)


def test_sub_250_microsecond_latency_slo(clean_ledger):
    ledger, bus = clean_ledger
    agent_priv, agent_pub = generate_node_keypair()
    provider_priv, provider_pub = generate_node_keypair()

    ledger.open_channel("tenant-perf", agent_pub, provider_pub, 10000.0, reserve_threshold=1.0, channel_id="chan-perf")

    n_ops = 100
    start = time.perf_counter()
    for i in range(1, n_ops + 1):
        ledger.record_meter_event("chan-perf", "METER_UNIT", 1, 0.0001)
    elapsed = time.perf_counter() - start

    avg_us = (elapsed / n_ops) * 1_000_000
    assert avg_us < 250.0, f"Average execution latency {avg_us:.2f}µs exceeded 250µs SLO"
