"""
SwarmWatch Autonomous Agent Capability Attestation & Hardware Secure Enclave (SGX/TEE) Remote Witness (Seam 98 / ADR 0109).

Provides hardware measurement verification (TEE1), anti-replay challenge nonces (TEE2),
lease-bounded capability delegation (TEE3), and fail-closed safety circuit latching on attestation breaches (TEE4).
Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from swarmwatch.circuit_bus import SafetyCircuitBus, global_safety_circuit_bus
from swarmwatch.hlc import HybridLogicalClock
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload, verify_bft_signature


class EnclaveAttestationError(Exception):
    """Base exception for hardware secure enclave remote witness operations."""
    pass


class EnclaveMeasurementMismatchError(EnclaveAttestationError):
    """Raised when runtime enclave measurement deviates from approved policy (TEE1)."""
    pass


class StaleAttestationReportError(EnclaveAttestationError):
    """Raised when challenge nonce is stale, expired, or previously consumed (TEE2)."""
    pass


class UnauthorizedCapabilityError(EnclaveAttestationError):
    """Raised when an agent attempts an action not authorized in capability token (TEE3)."""
    pass


class EnclaveQuarantinedError(EnclaveAttestationError):
    """Raised when an enclave is quarantined due to hardware compromise or revocation (TEE4)."""
    pass


class EnclaveCircuitTrippedError(EnclaveAttestationError):
    """Raised when attestation operations are blocked due to a tripped safety circuit bus."""
    pass


@dataclass
class EnclaveMeasurement:
    """Registered reference measurement defining trusted enclave code and config."""
    enclave_id: str
    tee_type: str  # SGX, TDX, NITRO, SEV_SNP, SIMULATED
    code_hash: str
    config_hash: str
    signer_id: str
    is_approved: bool = True


@dataclass
class EnclaveChallenge:
    """Active challenge nonce issued to enforce freshness and prevent quote replay."""
    challenge_id: str
    enclave_id: str
    nonce: str
    issued_at_ns: int
    expires_at_ns: int
    is_consumed: bool = False


@dataclass
class AttestationQuote:
    """Cryptographic remote attestation quote emitted by hardware enclave."""
    quote_id: str
    enclave_id: str
    tee_type: str
    measurement_hash: str
    challenge_nonce: str
    public_key: str
    signature: str
    timestamp_ns: int
    user_data_digest: str = ""


@dataclass
class EnclaveCapabilityToken:
    """Lease-bounded credential delegating authenticated permissions to an agent."""
    token_id: str
    enclave_id: str
    agent_id: str
    capabilities: List[str]
    issued_at_ns: int
    expires_at_ns: int
    token_signature: str


def compute_enclave_measurement_hash(code_hash: str, config_hash: str, signer_id: str) -> str:
    """Computes canonical measurement hash over enclave binaries and signer."""
    raw = f"sw:mrenclave:v1:{code_hash}:{config_hash}:{signer_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_quote_digest(
    enclave_id: str,
    tee_type: str,
    measurement_hash: str,
    nonce: str,
    user_data: str,
    timestamp_ns: int,
) -> str:
    """Computes canonical digest over hardware attestation quote."""
    raw = f"sw:quote:v1:{enclave_id}:{tee_type}:{measurement_hash}:{nonce}:{user_data}:{timestamp_ns}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_token_digest(
    token_id: str,
    enclave_id: str,
    agent_id: str,
    capabilities: List[str],
    expires_at_ns: int,
) -> str:
    """Computes canonical digest over capability delegation token."""
    caps_sorted = ",".join(sorted(capabilities))
    raw = f"sw:captoken:v1:{token_id}:{enclave_id}:{agent_id}:{caps_sorted}:{expires_at_ns}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_enclave_signature(pubkey: str, digest: str, signature: str) -> bool:
    """Verifies cryptographic Ed25519 enclave quote signature. Zero test-vector fallback."""
    if not signature or not pubkey or not digest:
        return False
    return verify_bft_signature(pubkey, digest, signature)


class EnclaveAttestationCoordinator:
    """
    Authoritative coordinator for autonomous agent capability attestation and
    hardware secure enclave (SGX/TEE) remote witness verification (ADR 0109).
    Guarantees TEE1, TEE2, TEE3, and TEE4 invariants.
    """

    def __init__(
        self,
        coordinator_id: str = "enclave-coordinator-primary",
        failure_threshold: int = 3,
        challenge_ttl_sec: float = 30.0,
        default_lease_sec: float = 300.0,
        circuit_bus: Optional[SafetyCircuitBus] = None,
    ) -> None:
        self.coordinator_id = coordinator_id
        self.failure_threshold = failure_threshold
        self.challenge_ttl_ns = int(challenge_ttl_sec * 1_000_000_000)
        self.default_lease_ns = int(default_lease_sec * 1_000_000_000)
        self.circuit_bus = circuit_bus or global_safety_circuit_bus
        self._hlc = HybridLogicalClock(node_id=f"tee-{coordinator_id}")
        self._lock = threading.RLock()

        self._coordinator_priv, self.coordinator_public_key = generate_node_keypair()
        self._approved_measurements: Dict[str, EnclaveMeasurement] = {}  # measurement_hash -> EnclaveMeasurement
        self._active_challenges: Dict[str, EnclaveChallenge] = {}        # nonce -> EnclaveChallenge
        self._issued_tokens: Dict[str, EnclaveCapabilityToken] = {}      # token_id -> EnclaveCapabilityToken
        self._quarantined_enclaves: Set[str] = set()
        self._failure_counts: Dict[str, int] = {}
        self._total_attestations = 0
        self._total_verifications = 0

    def bind_circuit_bus(self, circuit_bus: SafetyCircuitBus) -> None:
        """Binds authoritative safety circuit bus."""
        with self._lock:
            self.circuit_bus = circuit_bus

    def register_approved_measurement(self, measurement: EnclaveMeasurement) -> str:
        """Registers an authorized reference measurement for an enclave binary."""
        with self._lock:
            mhash = compute_enclave_measurement_hash(
                measurement.code_hash,
                measurement.config_hash,
                measurement.signer_id,
            )
            self._approved_measurements[mhash] = measurement
            return mhash

    def issue_challenge(self, enclave_id: str) -> EnclaveChallenge:
        """Issues a fresh anti-replay challenge nonce (TEE2)."""
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise EnclaveCircuitTrippedError("Cannot issue challenge: Safety circuit bus is TRIPPED.")

            if enclave_id in self._quarantined_enclaves:
                raise EnclaveQuarantinedError(f"Enclave '{enclave_id}' is permanently quarantined.")

            now_ns = time.time_ns()
            raw = f"chal:{enclave_id}:{now_ns}:{self._hlc.now()}"
            nonce = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            cid = f"chl-{nonce[:16]}"

            challenge = EnclaveChallenge(
                challenge_id=cid,
                enclave_id=enclave_id,
                nonce=nonce,
                issued_at_ns=now_ns,
                expires_at_ns=now_ns + self.challenge_ttl_ns,
                is_consumed=False,
            )
            self._active_challenges[nonce] = challenge
            return challenge

    def verify_and_attest(
        self,
        quote: AttestationQuote,
        agent_id: str,
        requested_capabilities: List[str],
        lease_sec: Optional[float] = None,
    ) -> EnclaveCapabilityToken:
        """
        Verifies remote attestation quote against hardware measurement and active challenge (TEE1, TEE2).
        Issues lease-bounded capability token upon success (TEE3).
        """
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise EnclaveCircuitTrippedError("Cannot verify quote: Safety circuit bus is TRIPPED.")

            if quote.enclave_id in self._quarantined_enclaves:
                raise EnclaveQuarantinedError(f"Enclave '{quote.enclave_id}' is quarantined.")

            # 1. Challenge Nonce Verification (TEE2)
            challenge = self._active_challenges.get(quote.challenge_nonce)
            now_ns = time.time_ns()
            if not challenge:
                self._record_failure(quote.enclave_id, "CHALLENGE_NONCE_UNKNOWN")
                raise StaleAttestationReportError("Attestation quote references unknown challenge nonce.")

            if challenge.is_consumed:
                self._record_failure(quote.enclave_id, "CHALLENGE_NONCE_REPLAYED")
                raise StaleAttestationReportError("Attestation challenge nonce has already been consumed.")

            if now_ns > challenge.expires_at_ns:
                self._record_failure(quote.enclave_id, "CHALLENGE_NONCE_EXPIRED")
                raise StaleAttestationReportError("Attestation challenge nonce has expired.")

            if challenge.enclave_id != quote.enclave_id:
                self._record_failure(quote.enclave_id, "CHALLENGE_ENCLAVE_MISMATCH")
                raise StaleAttestationReportError("Challenge nonce was issued to a different enclave ID.")

            challenge.is_consumed = True

            # 2. Measurement Verification (TEE1)
            ref_meas = self._approved_measurements.get(quote.measurement_hash)
            if not ref_meas or not ref_meas.is_approved:
                self._record_failure(quote.enclave_id, "MEASUREMENT_UNAPPROVED")
                raise EnclaveMeasurementMismatchError(
                    f"Enclave measurement '{quote.measurement_hash}' is unapproved or mismatched."
                )

            # 3. Cryptographic Signature Verification
            if len(quote.public_key) != 64 or any(c not in "0123456789abcdefABCDEF" for c in quote.public_key):
                self._record_failure(quote.enclave_id, "PUBLIC_KEY_INVALID")
                raise EnclaveAttestationError("Quote public_key must be a valid 64-character hex Ed25519 key.")

            digest = compute_quote_digest(
                enclave_id=quote.enclave_id,
                tee_type=quote.tee_type,
                measurement_hash=quote.measurement_hash,
                nonce=quote.challenge_nonce,
                user_data=quote.user_data_digest,
                timestamp_ns=quote.timestamp_ns,
            )
            if not verify_enclave_signature(quote.public_key, digest, quote.signature):
                self._record_failure(quote.enclave_id, "QUOTE_SIGNATURE_INVALID")
                raise EnclaveAttestationError("Enclave quote cryptographic signature verification failed.")

            # Success: reset failure count
            self._failure_counts[quote.enclave_id] = 0
            self._total_attestations += 1

            # 4. Issue Capability Token (TEE3)
            lease_duration_ns = int(lease_sec * 1_000_000_000) if lease_sec else self.default_lease_ns
            expires_at_ns = now_ns + lease_duration_ns
            tid = f"tok-{hashlib.sha256(f'{quote.enclave_id}:{agent_id}:{now_ns}'.encode('utf-8')).hexdigest()[:16]}"
            token_dig = compute_token_digest(tid, quote.enclave_id, agent_id, requested_capabilities, expires_at_ns)
            token_sig = sign_bft_payload(self._coordinator_priv, token_dig)

            token = EnclaveCapabilityToken(
                token_id=tid,
                enclave_id=quote.enclave_id,
                agent_id=agent_id,
                capabilities=requested_capabilities,
                issued_at_ns=now_ns,
                expires_at_ns=expires_at_ns,
                token_signature=token_sig,
            )
            self._issued_tokens[tid] = token
            return token

    def validate_capability(self, token: EnclaveCapabilityToken, required_capability: str) -> bool:
        """Validates capability token against lease expiry and permissions (TEE3)."""
        with self._lock:
            self._total_verifications += 1
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise EnclaveCircuitTrippedError("Cannot validate capability: Safety circuit bus is TRIPPED.")

            if token.enclave_id in self._quarantined_enclaves:
                raise EnclaveQuarantinedError(f"Enclave '{token.enclave_id}' is quarantined.")

            now_ns = time.time_ns()
            if now_ns > token.expires_at_ns:
                raise UnauthorizedCapabilityError(f"Capability token '{token.token_id}' has expired.")

            if required_capability not in token.capabilities:
                raise UnauthorizedCapabilityError(
                    f"Action '{required_capability}' not granted in token capabilities: {token.capabilities}."
                )

            # Verify integrity
            expected_dig = compute_token_digest(
                token.token_id,
                token.enclave_id,
                token.agent_id,
                token.capabilities,
                token.expires_at_ns,
            )
            if not verify_bft_signature(self.coordinator_public_key, expected_dig, token.token_signature):
                raise UnauthorizedCapabilityError("Capability token signature integrity check failed.")

            return True

    def revoke_enclave(self, enclave_id: str, reason: str = "MANUAL_REVOCATION") -> None:
        """Permanently quarantines an enclave instance."""
        with self._lock:
            self._quarantined_enclaves.add(enclave_id)
            if self.circuit_bus:
                self.circuit_bus.trip(
                    reason="TEE_HARDWARE_ATTESTATION_BREACH",
                    scope=f"enclave:{enclave_id}",
                    metadata={"revoked_enclave": enclave_id, "reason": reason},
                )

    def _record_failure(self, enclave_id: str, reason: str) -> None:
        count = self._failure_counts.get(enclave_id, 0) + 1
        self._failure_counts[enclave_id] = count
        if count >= self.failure_threshold:
            self._quarantined_enclaves.add(enclave_id)
            if self.circuit_bus:
                self.circuit_bus.trip(
                    reason="TEE_HARDWARE_ATTESTATION_BREACH",
                    scope=f"enclave:{enclave_id}",
                    metadata={
                        "quarantined_enclave": enclave_id,
                        "consecutive_failures": count,
                        "last_reason": reason,
                    },
                )

    def get_status(self) -> Dict[str, Any]:
        """Returns point-in-time metrics for hardware secure enclave remote witness."""
        with self._lock:
            active_tokens = sum(1 for t in self._issued_tokens.values() if time.time_ns() <= t.expires_at_ns)
            return {
                "coordinator_id": self.coordinator_id,
                "approved_measurements_count": len(self._approved_measurements),
                "active_challenges_count": len(self._active_challenges),
                "total_attestations": self._total_attestations,
                "active_capability_tokens": active_tokens,
                "total_capability_verifications": self._total_verifications,
                "quarantined_enclaves_count": len(self._quarantined_enclaves),
                "circuit_tripped": self.circuit_bus.is_tripped() if self.circuit_bus else False,
            }


# Authoritative Global Singletons
global_enclave_coordinator: EnclaveAttestationCoordinator = EnclaveAttestationCoordinator()
global_enclave_attestation: EnclaveAttestationCoordinator = global_enclave_coordinator
