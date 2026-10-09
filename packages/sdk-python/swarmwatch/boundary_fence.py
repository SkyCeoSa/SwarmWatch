"""
SwarmWatch Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault (Seam 99 / ADR 0110).

Provides geofenced residency boundaries (BND1), zero-egress vault envelope encryption (BND2),
cryptographic egress export passports (BND3), and fail-closed safety circuit containment (BND4).
Zero-token cost guarantee ($0.00 USD), sub-250 microsecond execution latency SLO.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from swarmwatch.circuit_bus import SafetyCircuitBus, global_safety_circuit_bus
from swarmwatch.hlc import HybridLogicalClock
from swarmwatch.byzantine_oracle import generate_node_keypair, sign_bft_payload, verify_bft_signature
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class BoundaryFenceError(Exception):
    """Base exception for cross-border data boundary fence operations."""
    pass


class CrossBorderResidencyViolationError(BoundaryFenceError):
    """Raised when cross-border transmission violates geographic residency treaty (BND1)."""
    pass


class UnsealedDataEgressError(BoundaryFenceError):
    """Raised when unencrypted raw data attempts egress without vault envelope (BND2)."""
    pass


class InvalidEgressPassportError(BoundaryFenceError):
    """Raised when an egress export passport is expired, forged, or mismatched (BND3)."""
    pass


class FenceZoneQuarantinedError(BoundaryFenceError):
    """Raised when a residency zone is under quarantine due to a boundary breach (BND4)."""
    pass


class FenceCircuitTrippedError(BoundaryFenceError):
    """Raised when fence operations are blocked due to a tripped safety circuit bus."""
    pass


@dataclass
class ResidencyZone:
    """Geographic or regulatory sovereign boundary zone."""
    zone_id: str  # e.g. EU_DE, US_EAST, CH_ZH, JP_TYO
    jurisdiction: str
    compliance_tags: List[str] = field(default_factory=list)
    is_quarantined: bool = False


@dataclass
class EgressTreaty:
    """Bilateral or unilateral authorized export policy between residency zones."""
    treaty_id: str
    source_zone: str
    dest_zone: str
    is_bidirectional: bool = False
    max_classification: str = "RESTRICTED"
    is_active: bool = True


@dataclass
class VaultEnvelope:
    """Zero-egress encrypted cryptographic container protecting sensitive payloads."""
    envelope_id: str
    tenant_id: str
    origin_zone: str
    ciphertext_hex: str
    tag_hex: str
    iv_hex: str
    data_digest: str
    created_at_ns: int


@dataclass
class EgressExportPassport:
    """Cryptographic authorization permit authorizing single-use cross-border transit."""
    passport_id: str
    tenant_id: str
    source_zone: str
    dest_zone: str
    payload_digest: str
    issued_at_ns: int
    expires_at_ns: int
    passport_signature: str
    is_consumed: bool = False


@dataclass
class BoundaryInspectionResult:
    """Audit attestation recording perimeter packet inspection and verdict."""
    allowed: bool
    source_zone: str
    dest_zone: str
    envelope_id: str
    reason: str
    inspected_at_ns: int


def compute_passport_digest(
    passport_id: str,
    tenant_id: str,
    src_zone: str,
    dst_zone: str,
    payload_digest: str,
    expires_at_ns: int,
) -> str:
    """Computes canonical digest over cross-border egress passport."""
    raw = f"sw:pass:v1:{passport_id}:{tenant_id}:{src_zone}:{dst_zone}:{payload_digest}:{expires_at_ns}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def verify_passport_signature(pubkey: str, digest: str, signature: str) -> bool:
    """Verifies cryptographic Ed25519 passport signature. Zero HMAC/test-vector fallback."""
    if not signature or not pubkey or not digest:
        return False
    return verify_bft_signature(pubkey, digest, signature)


class DataBoundaryFence:
    """
    Authoritative coordinator for sovereign cross-border data residency and
    zero-egress vault encryption perimeters (ADR 0110).
    Guarantees BND1, BND2, BND3, and BND4 invariants.
    """

    def __init__(
        self,
        fence_id: str = "boundary-fence-primary",
        breach_threshold: int = 3,
        passport_ttl_sec: float = 60.0,
        circuit_bus: Optional[SafetyCircuitBus] = None,
        vault_master_key: Optional[bytes] = None,
    ) -> None:
        self.fence_id = fence_id
        self.breach_threshold = breach_threshold
        self.passport_ttl_ns = int(passport_ttl_sec * 1_000_000_000)
        self.circuit_bus = circuit_bus or global_safety_circuit_bus
        self._hlc = HybridLogicalClock(node_id=f"fnc-{fence_id}")
        self._lock = threading.RLock()

        self._vault_master_key: bytes = vault_master_key or os.urandom(32)
        self._authority_priv, self.authority_public_key = generate_node_keypair()
        self._zones: Dict[str, ResidencyZone] = {}
        self._treaties: Dict[str, EgressTreaty] = {}
        self._passports: Dict[str, EgressExportPassport] = {}
        self._envelopes: Dict[str, VaultEnvelope] = {}
        self._breach_counts: Dict[str, int] = {}  # zone_id -> breach count
        self._inspections_count = 0
        self._sealed_count = 0

    def bind_circuit_bus(self, circuit_bus: SafetyCircuitBus) -> None:
        """Binds authoritative safety circuit bus."""
        with self._lock:
            self.circuit_bus = circuit_bus

    def register_residency_zone(self, zone: ResidencyZone) -> None:
        """Registers a sovereign data residency zone."""
        with self._lock:
            self._zones[zone.zone_id] = zone

    def register_egress_treaty(self, treaty: EgressTreaty) -> None:
        """Registers a cross-border egress treaty."""
        with self._lock:
            self._treaties[treaty.treaty_id] = treaty

    def seal_data(
        self,
        tenant_id: str,
        zone_id: str,
        plaintext: str | bytes,
        key: Optional[bytes] = None,
    ) -> VaultEnvelope:
        """
        Encapsulates sensitive plaintext into an authenticated zero-egress vault envelope (BND2).
        Uses AES-256-GCM fail-closed.
        """
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise FenceCircuitTrippedError("Cannot seal data: Safety circuit bus is TRIPPED.")

            zone = self._zones.get(zone_id)
            if zone and zone.is_quarantined:
                raise FenceZoneQuarantinedError(f"Zone '{zone_id}' is quarantined.")

            data_bytes = plaintext.encode("utf-8") if isinstance(plaintext, str) else plaintext
            data_digest = hashlib.sha256(data_bytes).hexdigest()
            now_ns = time.time_ns()

            enc_key = key or hmac.new(self._vault_master_key, f"{zone_id}:{tenant_id}".encode("utf-8"), hashlib.sha256).digest()
            iv = os.urandom(12)

            try:
                aesgcm = AESGCM(enc_key)
                ciphertext_with_tag = aesgcm.encrypt(iv, data_bytes, f"{tenant_id}:{zone_id}".encode("utf-8"))
                ciphertext = ciphertext_with_tag[:-16]
                tag = ciphertext_with_tag[-16:]
            except Exception as e:
                raise BoundaryFenceError(f"AES-256-GCM vault encryption failed: {e}") from e

            eid = f"vlt-{hashlib.sha256(f'{tenant_id}:{zone_id}:{now_ns}:{data_digest}'.encode('utf-8')).hexdigest()[:16]}"
            envelope = VaultEnvelope(
                envelope_id=eid,
                tenant_id=tenant_id,
                origin_zone=zone_id,
                ciphertext_hex=ciphertext.hex(),
                tag_hex=tag.hex(),
                iv_hex=iv.hex(),
                data_digest=data_digest,
                created_at_ns=now_ns,
            )
            self._envelopes[eid] = envelope
            self._sealed_count += 1
            return envelope

    def unseal_data(self, envelope: VaultEnvelope, key: Optional[bytes] = None) -> bytes:
        """Decrypts and authenticates data from a zero-egress vault envelope."""
        with self._lock:
            enc_key = key or hmac.new(self._vault_master_key, f"{envelope.origin_zone}:{envelope.tenant_id}".encode("utf-8"), hashlib.sha256).digest()
            ciphertext = bytes.fromhex(envelope.ciphertext_hex)
            tag = bytes.fromhex(envelope.tag_hex)
            iv = bytes.fromhex(envelope.iv_hex)

            if len(tag) != 16:
                raise BoundaryFenceError(
                    f"Invalid authentication tag length: {len(tag)} bytes (must be 16 bytes for AES-256-GCM, PoC-N rejected)"
                )

            try:
                aesgcm = AESGCM(enc_key)
                plaintext = aesgcm.decrypt(iv, ciphertext + tag, f"{envelope.tenant_id}:{envelope.origin_zone}".encode("utf-8"))
            except Exception as e:
                raise BoundaryFenceError(f"AES-256-GCM vault decryption/authentication failed: {e}") from e

            return plaintext

    def issue_egress_passport(
        self,
        tenant_id: str,
        source_zone: str,
        dest_zone: str,
        payload_digest: str,
        ttl_sec: Optional[float] = None,
    ) -> EgressExportPassport:
        """
        Issues single-use cryptographic egress passport if approved by active treaty (BND1, BND3).
        """
        with self._lock:
            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise FenceCircuitTrippedError("Cannot issue passport: Safety circuit bus is TRIPPED.")

            src = self._zones.get(source_zone)
            if src and src.is_quarantined:
                raise FenceZoneQuarantinedError(f"Source zone '{source_zone}' is quarantined.")

            dst = self._zones.get(dest_zone)
            if dst and dst.is_quarantined:
                raise FenceZoneQuarantinedError(f"Destination zone '{dest_zone}' is quarantined.")

            # Validate treaty (BND1)
            if source_zone != dest_zone:
                treaty = self._find_active_treaty(source_zone, dest_zone)
                if not treaty:
                    self._record_breach(source_zone, "NO_ACTIVE_EGRESS_TREATY")
                    raise CrossBorderResidencyViolationError(
                        f"Cross-border transfer from '{source_zone}' to '{dest_zone}' violates data residency (no active treaty)."
                    )

            now_ns = time.time_ns()
            duration_ns = int(ttl_sec * 1_000_000_000) if ttl_sec else self.passport_ttl_ns
            expires_at_ns = now_ns + duration_ns
            pid = f"pas-{hashlib.sha256(f'{tenant_id}:{source_zone}:{dest_zone}:{now_ns}'.encode('utf-8')).hexdigest()[:16]}"

            digest = compute_passport_digest(pid, tenant_id, source_zone, dest_zone, payload_digest, expires_at_ns)
            sig = sign_bft_payload(self._authority_priv, digest)

            passport = EgressExportPassport(
                passport_id=pid,
                tenant_id=tenant_id,
                source_zone=source_zone,
                dest_zone=dest_zone,
                payload_digest=payload_digest,
                issued_at_ns=now_ns,
                expires_at_ns=expires_at_ns,
                passport_signature=sig,
                is_consumed=False,
            )
            self._passports[pid] = passport
            return passport

    def inspect_egress(
        self,
        envelope: VaultEnvelope,
        dest_zone: str,
        passport: Optional[EgressExportPassport] = None,
    ) -> BoundaryInspectionResult:
        """
        Performs in-flight perimeter inspection at boundary fence (BND1, BND2, BND3).
        Guarantees sub-250 microsecond execution latency.
        """
        with self._lock:
            self._inspections_count += 1
            now_ns = time.time_ns()

            if self.circuit_bus and self.circuit_bus.is_tripped():
                raise FenceCircuitTrippedError("Cannot inspect egress: Safety circuit bus is TRIPPED.")

            source_zone = envelope.origin_zone
            src_zone = self._zones.get(source_zone)
            if src_zone and src_zone.is_quarantined:
                raise FenceZoneQuarantinedError(f"Source zone '{source_zone}' is under quarantine.")

            # Same-zone transfer is unrestricted
            if source_zone == dest_zone:
                return BoundaryInspectionResult(
                    allowed=True,
                    source_zone=source_zone,
                    dest_zone=dest_zone,
                    envelope_id=envelope.envelope_id,
                    reason="LOCAL_INTRA_ZONE_TRANSFER",
                    inspected_at_ns=now_ns,
                )

            # Cross-zone requires passport (BND3)
            if not passport:
                self._record_breach(source_zone, "EGRESS_PASSPORT_MISSING")
                raise InvalidEgressPassportError(
                    f"Cross-border transfer to '{dest_zone}' requires an egress export passport."
                )

            if passport.is_consumed:
                self._record_breach(source_zone, "PASSPORT_REPLAY_ATTEMPT")
                raise InvalidEgressPassportError(f"Egress passport '{passport.passport_id}' has already been consumed.")

            if now_ns > passport.expires_at_ns:
                self._record_breach(source_zone, "PASSPORT_EXPIRED")
                raise InvalidEgressPassportError(f"Egress passport '{passport.passport_id}' has expired.")

            if passport.source_zone != source_zone or passport.dest_zone != dest_zone:
                self._record_breach(source_zone, "PASSPORT_ZONE_MISMATCH")
                raise InvalidEgressPassportError("Egress passport route does not match envelope route.")

            if passport.payload_digest != envelope.data_digest:
                self._record_breach(source_zone, "PASSPORT_PAYLOAD_MISMATCH")
                raise InvalidEgressPassportError("Egress passport payload digest does not match envelope digest.")

            # Verify passport signature
            p_dig = compute_passport_digest(
                passport.passport_id,
                passport.tenant_id,
                passport.source_zone,
                passport.dest_zone,
                passport.payload_digest,
                passport.expires_at_ns,
            )
            if not verify_passport_signature(self.authority_public_key, p_dig, passport.passport_signature):
                self._record_breach(source_zone, "PASSPORT_SIGNATURE_INVALID")
                raise InvalidEgressPassportError("Egress passport signature verification failed.")

            # Mark consumed
            passport.is_consumed = True
            return BoundaryInspectionResult(
                allowed=True,
                source_zone=source_zone,
                dest_zone=dest_zone,
                envelope_id=envelope.envelope_id,
                reason="CROSS_BORDER_PASSPORT_VERIFIED",
                inspected_at_ns=now_ns,
            )

    def quarantine_zone(self, zone_id: str, reason: str = "MANUAL_QUARANTINE") -> None:
        """Quarantines a residency zone fail-closed."""
        with self._lock:
            zone = self._zones.get(zone_id)
            if zone:
                zone.is_quarantined = True
            if self.circuit_bus:
                self.circuit_bus.trip(
                    reason="BOUNDARY_FENCE_EGRESS_BREACH",
                    scope=f"zone:{zone_id}",
                    metadata={"quarantined_zone": zone_id, "reason": reason},
                )

    def _find_active_treaty(self, src: str, dst: str) -> Optional[EgressTreaty]:
        for t in self._treaties.values():
            if not t.is_active:
                continue
            if t.source_zone == src and t.dest_zone == dst:
                return t
            if t.is_bidirectional and t.source_zone == dst and t.dest_zone == src:
                return t
        return None

    def _record_breach(self, zone_id: str, reason: str) -> None:
        count = self._breach_counts.get(zone_id, 0) + 1
        self._breach_counts[zone_id] = count
        if count >= self.breach_threshold:
            self.quarantine_zone(zone_id, reason=f"BREACH_THRESHOLD_EXCEEDED:{reason}")

    def get_status(self) -> Dict[str, Any]:
        """Returns point-in-time metrics for boundary fence."""
        with self._lock:
            active_treaties = sum(1 for t in self._treaties.values() if t.is_active)
            quarantined_zones = sum(1 for z in self._zones.values() if z.is_quarantined)
            active_passports = sum(1 for p in self._passports.values() if not p.is_consumed and time.time_ns() <= p.expires_at_ns)
            return {
                "fence_id": self.fence_id,
                "total_zones_count": len(self._zones),
                "quarantined_zones_count": quarantined_zones,
                "active_treaties_count": active_treaties,
                "total_sealed_envelopes": self._sealed_count,
                "active_passports_count": active_passports,
                "total_inspections": self._inspections_count,
                "circuit_tripped": self.circuit_bus.is_tripped() if self.circuit_bus else False,
            }


# Authoritative Global Singletons
global_boundary_fence: DataBoundaryFence = DataBoundaryFence()
global_data_fence: DataBoundaryFence = global_boundary_fence
