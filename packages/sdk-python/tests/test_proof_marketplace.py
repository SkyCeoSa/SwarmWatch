"""
Unit & Invariant Tests for Seam 93: Decentralized Cryptographic Proof Marketplace (ADR 0104)

Verifies:
- Invariant CPM1: Collateral Staking & Slashing
- Invariant CPM2: Double-Auction Proof Matching
- Invariant CPM3: K-of-N Cryptographic Quorum Verification
- Invariant CPM4: Fail-Closed Safety Circuit Bus Latching
- Sub-250µs execution latency SLO
"""

import time
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.proof_marketplace import (
    ProofMarketplaceCoordinator,
    VerifierNode,
    ProofBounty,
    VerifierBid,
    ProofVerificationReceipt,
    InsufficientStakeError,
    VerifierSlashedError,
    InvalidReceiptSignatureError,
    InsufficientVerificationQuorumError,
    compute_bid_digest,
    compute_receipt_digest,
)


def _generate_test_verifier(verifier_id: str, stake: float = 500.0, min_stake: float = 100.0):
    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    pub_hex = pub.public_bytes_raw().hex()
    node = VerifierNode(
        verifier_id=verifier_id,
        public_key=pub_hex,
        bonded_stake=stake,
        min_stake=min_stake,
    )
    return node, priv


def test_verifier_staking_and_min_stake_cpm1():
    coordinator = ProofMarketplaceCoordinator("market-test")
    node, _ = _generate_test_verifier("v-01", stake=500.0, min_stake=100.0)
    coordinator.register_verifier(node)

    assert coordinator.get_verifier("v-01") is not None
    assert node.bonded_stake == 500.0

    # Bond additional stake
    coordinator.bond_stake("v-01", 200.0)
    assert node.bonded_stake == 700.0

    # Unbond stake safely
    coordinator.unbond_stake("v-01", 500.0)
    assert node.bonded_stake == 200.0

    # Unbonding below min_stake fails
    with pytest.raises(InsufficientStakeError):
        coordinator.unbond_stake("v-01", 150.0)

    # Registering with insufficient stake fails
    poor_node, _ = _generate_test_verifier("v-poor", stake=50.0, min_stake=100.0)
    with pytest.raises(InsufficientStakeError):
        coordinator.register_verifier(poor_node)


def test_bounty_creation_and_double_auction_matching_cpm2():
    coordinator = ProofMarketplaceCoordinator("market-test")

    # Register 3 verifiers
    v1, priv1 = _generate_test_verifier("v-1", stake=300.0)
    v2, priv2 = _generate_test_verifier("v-2", stake=400.0)
    v3, priv3 = _generate_test_verifier("v-3", stake=500.0)
    coordinator.register_verifier(v1)
    coordinator.register_verifier(v2)
    coordinator.register_verifier(v3)

    bounty = ProofBounty(
        bounty_id="bounty-100",
        proof_type="ZK_SNARK_ROLLUP",
        proof_digest="a" * 64,
        max_bounty=50.0,
        required_quorum=2,
    )
    coordinator.create_bounty(bounty)

    # Submit valid bids
    digest1 = compute_bid_digest("bounty-100", "v-1", 25.0)
    sig1 = priv1.sign(bytes.fromhex(digest1)).hex()
    coordinator.submit_bid(VerifierBid(
        bid_id="bid-1",
        bounty_id="bounty-100",
        verifier_id="v-1",
        fee_bid=25.0,
        staked_collateral=300.0,
        signature=sig1,
    ))

    digest2 = compute_bid_digest("bounty-100", "v-2", 20.0)
    sig2 = priv2.sign(bytes.fromhex(digest2)).hex()
    coordinator.submit_bid(VerifierBid(
        bid_id="bid-2",
        bounty_id="bounty-100",
        verifier_id="v-2",
        fee_bid=20.0,
        staked_collateral=400.0,
        signature=sig2,
    ))

    # Forged signature rejected
    digest3 = compute_bid_digest("bounty-100", "v-3", 15.0)
    with pytest.raises(InvalidReceiptSignatureError):
        coordinator.submit_bid(VerifierBid(
            bid_id="bid-3",
            bounty_id="bounty-100",
            verifier_id="v-3",
            fee_bid=15.0,
            staked_collateral=500.0,
            signature="ff" * 64,
        ))

    # Match bounty: selects top 2 lowest fee bids (v-2 with 20.0, v-1 with 25.0)
    matched = coordinator.match_bounty("bounty-100")
    assert len(matched) == 2
    assert matched == ["v-2", "v-1"]
    assert bounty.status == "MATCHED"


def test_k_of_n_quorum_settlement_and_payout_cpm3():
    coordinator = ProofMarketplaceCoordinator("market-test")

    nodes = []
    privs = []
    for i in range(3):
        n, p = _generate_test_verifier(f"node-{i}", stake=300.0)
        coordinator.register_verifier(n)
        nodes.append(n)
        privs.append(p)

    bounty = ProofBounty(
        bounty_id="bounty-200",
        proof_type="EPISTEMIC_DISPUTE_PROOF",
        proof_digest="b" * 64,
        max_bounty=60.0,
        required_quorum=3,
    )
    coordinator.create_bounty(bounty)

    # Submit bids and receipts
    for i in range(3):
        b_dig = compute_bid_digest("bounty-200", f"node-{i}", 15.0)
        b_sig = privs[i].sign(bytes.fromhex(b_dig)).hex()
        coordinator.submit_bid(VerifierBid(
            bid_id=f"bid-{i}",
            bounty_id="bounty-200",
            verifier_id=f"node-{i}",
            fee_bid=15.0,
            staked_collateral=300.0,
            signature=b_sig,
        ))

    coordinator.match_bounty("bounty-200")

    # Submit 3 matching True receipts
    for i in range(3):
        r_dig = compute_receipt_digest("bounty-200", f"node-{i}", True, "b" * 64)
        r_sig = privs[i].sign(bytes.fromhex(r_dig)).hex()
        coordinator.submit_receipt(ProofVerificationReceipt(
            receipt_id=f"rcpt-{i}",
            bounty_id="bounty-200",
            verifier_id=f"node-{i}",
            verdict=True,
            proof_hash="b" * 64,
            signature=r_sig,
        ))

    certificate = coordinator.settle_bounty("bounty-200")
    assert certificate.finalized_verdict is True
    assert certificate.bounty_id == "bounty-200"
    assert len(certificate.participating_verifiers) == 3
    assert len(certificate.slashed_verifiers) == 0
    assert certificate.cleared_fee == 15.0
    assert certificate.payout_distributions["node-0"] == 5.0
    assert bounty.status == "FINALIZED"


def test_equivocation_and_fraud_slashing_cpm1():
    coordinator = ProofMarketplaceCoordinator("market-test")

    n1, p1 = _generate_test_verifier("v-good-1", stake=400.0)
    n2, p2 = _generate_test_verifier("v-good-2", stake=400.0)
    n3, p3 = _generate_test_verifier("v-malicious", stake=500.0)
    coordinator.register_verifier(n1)
    coordinator.register_verifier(n2)
    coordinator.register_verifier(n3)

    bounty = ProofBounty(
        bounty_id="bounty-300",
        proof_type="CAUSAL_STATE_PROOF",
        proof_digest="c" * 64,
        max_bounty=100.0,
        required_quorum=2,
    )
    coordinator.create_bounty(bounty)

    # Submit valid receipts: v-good-1 and v-good-2 vote True, v-malicious votes False
    r_dig1 = compute_receipt_digest("bounty-300", "v-good-1", True, "c" * 64)
    coordinator.submit_receipt(ProofVerificationReceipt(
        receipt_id="r-1",
        bounty_id="bounty-300",
        verifier_id="v-good-1",
        verdict=True,
        proof_hash="c" * 64,
        signature=p1.sign(bytes.fromhex(r_dig1)).hex(),
    ))

    r_dig2 = compute_receipt_digest("bounty-300", "v-good-2", True, "c" * 64)
    coordinator.submit_receipt(ProofVerificationReceipt(
        receipt_id="r-2",
        bounty_id="bounty-300",
        verifier_id="v-good-2",
        verdict=True,
        proof_hash="c" * 64,
        signature=p2.sign(bytes.fromhex(r_dig2)).hex(),
    ))

    r_dig3 = compute_receipt_digest("bounty-300", "v-malicious", False, "c" * 64)
    coordinator.submit_receipt(ProofVerificationReceipt(
        receipt_id="r-3",
        bounty_id="bounty-300",
        verifier_id="v-malicious",
        verdict=False,
        proof_hash="c" * 64,
        signature=p3.sign(bytes.fromhex(r_dig3)).hex(),
    ))

    cert = coordinator.settle_bounty("bounty-300")
    assert cert.finalized_verdict is True
    assert "v-malicious" in cert.slashed_verifiers

    # Check that v-malicious was permanently slashed
    assert n3.is_slashed is True
    assert n3.is_active is False
    assert n3.bonded_stake == 0.0

    # Slashed verifier cannot bond stake or submit bids
    with pytest.raises(VerifierSlashedError):
        coordinator.bond_stake("v-malicious", 100.0)


def test_solvency_breach_trips_safety_circuit_bus_cpm4():
    circuit_bus = SafetyCircuitBus()
    # Pool with high minimum solvency requirement (1000.0)
    coordinator = ProofMarketplaceCoordinator(
        marketplace_id="market-solvent",
        min_solvency_threshold=1000.0,
        circuit_bus=circuit_bus,
    )

    n1, _ = _generate_test_verifier("v-node-1", stake=600.0)
    n2, _ = _generate_test_verifier("v-node-2", stake=600.0)
    coordinator.register_verifier(n1)
    coordinator.register_verifier(n2)

    assert circuit_bus.is_tripped() is False

    # Slashing node 1 leaves only 600.0 stake (< 1000.0 required solvency)
    coordinator.slash_verifier("v-node-1", penalty_fraction=1.0, reason="Equivocation")

    assert circuit_bus.is_tripped() is True
    status = circuit_bus.get_status()
    assert status.last_trip_event.reason == "PROOF_MARKETPLACE_SOLVENCY_BREACH"


def test_sub_250_microsecond_latency_slo():
    coordinator = ProofMarketplaceCoordinator("market-bench")
    node, _ = _generate_test_verifier("v-bench", stake=1000.0)
    coordinator.register_verifier(node)

    # Benchmark: 200 iterations of bounty creation and lookup
    start = time.perf_counter()
    iterations = 200
    for i in range(iterations):
        b = ProofBounty(
            bounty_id=f"b-bench-{i}",
            proof_type="ZK_PROOF",
            proof_digest="0" * 64,
            max_bounty=10.0,
        )
        coordinator.create_bounty(b)
    elapsed = time.perf_counter() - start

    avg_latency_us = (elapsed / iterations) * 1_000_000
    assert avg_latency_us < 250.0, f"Average latency {avg_latency_us:.2f}µs exceeds 250µs SLO"
