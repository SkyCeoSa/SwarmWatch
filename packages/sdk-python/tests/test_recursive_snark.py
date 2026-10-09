"""
Unit & Invariant Tests for Seam 94: Recursive SNARK Verification Gateway & Aggregate Attestation (ADR 0105)

Verifies:
- Invariant RZK1: Constant-Size O(1) Succinct Recursive Proof Compression
- Invariant RZK2: Recursive Soundness & Non-Malleability (Tainted Proof Rejection)
- Invariant RZK3: Deterministic Binary Hierarchical Tree Folding
- Invariant RZK4: Fail-Closed Safety Circuit Bus Latching on Verification Failure
- Sub-250µs execution latency SLO
"""

import time
import pytest

from swarmwatch.circuit_bus import SafetyCircuitBus
from swarmwatch.recursive_snark import (
    RecursiveSnarkGateway,
    ConstituentProof,
    AggregateSnarkReceipt,
    EmptyProofBatchError,
    RecursiveProofSoundnessBreachError,
    RecursiveVerificationFailedError,
    compute_recursive_proof_bytes,
)


def _generate_proof(proof_id: str, epoch: int = 1, is_valid: bool = True) -> ConstituentProof:
    return ConstituentProof(
        proof_id=proof_id,
        batch_epoch=epoch,
        state_digest=f"digest-{proof_id}" * 4,
        proof_witness=f"witness-{proof_id}" * 4,
        is_valid=is_valid,
    )


def test_binary_tree_folding_and_succinct_compression_rzk1_rzk3():
    gateway = RecursiveSnarkGateway("gateway-test")

    # Test folding with various batch sizes: 1, 2, 4, 7
    for count in [1, 2, 4, 7]:
        batch = [_generate_proof(f"p-{count}-{i}") for i in range(count)]
        receipt = gateway.fold_batch(batch)

        assert receipt.constituent_count == count
        assert len(receipt.aggregate_root) == 64
        # Constant O(1) size: exactly 64 hex characters (32 bytes)
        assert len(receipt.recursive_proof_bytes) == 64
        assert receipt.verified is True

        # Check idempotence / deterministic tree derivation
        receipt_repeat = gateway.fold_batch(batch)
        assert receipt.aggregate_root == receipt_repeat.aggregate_root
        assert receipt.recursive_proof_bytes == receipt_repeat.recursive_proof_bytes


def test_recursive_soundness_rejection_rzk2():
    gateway = RecursiveSnarkGateway("gateway-test")

    # Empty batch rejected
    with pytest.raises(EmptyProofBatchError):
        gateway.fold_batch([])

    # Batch with tainted proof rejected fail-closed
    good1 = _generate_proof("good-1")
    bad = _generate_proof("bad-proof", is_valid=False)
    good2 = _generate_proof("good-2")

    with pytest.raises(RecursiveProofSoundnessBreachError):
        gateway.fold_batch([good1, bad, good2])

    # Proof with empty witness rejected
    empty_wit = ConstituentProof("empty-wit", 1, "digest-a", "")
    with pytest.raises(RecursiveProofSoundnessBreachError):
        gateway.fold_batch([good1, empty_wit])


def test_constant_time_aggregate_verification_rzk1():
    gateway = RecursiveSnarkGateway("gateway-test")
    batch = [_generate_proof(f"v-proof-{i}") for i in range(8)]
    receipt = gateway.fold_batch(batch)

    # Verification must succeed
    assert gateway.verify_aggregate_receipt(receipt) is True


def test_forged_proof_trips_safety_circuit_bus_rzk4():
    circuit_bus = SafetyCircuitBus()
    gateway = RecursiveSnarkGateway("gateway-test", circuit_bus=circuit_bus)

    batch = [_generate_proof(f"proof-{i}") for i in range(4)]
    receipt = gateway.fold_batch(batch)

    assert circuit_bus.is_tripped() is False

    # Tamper with recursive proof bytes (forgery attack)
    tampered_receipt = AggregateSnarkReceipt(
        aggregate_id=receipt.aggregate_id,
        constituent_count=receipt.constituent_count,
        aggregate_root=receipt.aggregate_root,
        recursive_proof_bytes="00" * 32,  # Tampered proof
        verified=True,
        created_at_hlc=receipt.created_at_hlc,
    )

    with pytest.raises(RecursiveVerificationFailedError):
        gateway.verify_aggregate_receipt(tampered_receipt)

    assert circuit_bus.is_tripped() is True
    status = circuit_bus.get_status()
    assert status.last_trip_event.reason == "RECURSIVE_SNARK_VERIFICATION_FAILURE"


def test_sub_250_microsecond_latency_slo():
    gateway = RecursiveSnarkGateway("gateway-bench")
    batch = [_generate_proof(f"bench-p-{i}") for i in range(4)]

    # Benchmark: 200 iterations of recursive batch folding and verification
    start = time.perf_counter()
    iterations = 200
    for _ in range(iterations):
        rcpt = gateway.fold_batch(batch)
        gateway.verify_aggregate_receipt(rcpt)
    elapsed = time.perf_counter() - start

    avg_latency_us = (elapsed / iterations) * 1_000_000
    assert avg_latency_us < 250.0, f"Average latency {avg_latency_us:.2f}µs exceeds 250µs SLO"
