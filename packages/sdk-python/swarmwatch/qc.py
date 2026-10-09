# -*- coding: utf-8 -*-
"""
SwarmWatch Committee Key Ceremony & Threshold Quorum Certificates (Seam 81 / ADR 0092).

Implements threshold multi-party quorum certificates (QCs), certified epoch key ceremonies,
Ed25519 cryptographic signatures, deterministic Merkle committee roots, domain-separated
attestations, and fail-closed Byzantine verification across distributed seams.

Invariants:
- QC1: QuorumCertificate is valid iff it carries >= ceil((N+f+1)/2) distinct Ed25519 signatures
  over H(domain || epoch || digest || committee_root) from keys in CommitteeRoot(epoch).
- QC2: Committee rotation to epoch e+1 is strictly certified by QC(e) committing to the next CommitteeRoot.
- QC3: Domain-separated tags ('sw:qc:v1:<domain>') and monotonic epoch prevent replay attacks.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
import math
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class QuorumCertificateError(Exception):
    """Base exception for all threshold quorum certificate failures."""


class InsufficientQuorumError(QuorumCertificateError):
    """Raised when signatures do not meet the Byzantine quorum threshold."""


class InvalidCommitteeRootError(QuorumCertificateError):
    """Raised when a certificate's committee root does not match the pinned epoch root."""


class ForgedCertificateSignatureError(QuorumCertificateError):
    """Raised when any Ed25519 signature fails verification or is unkeyed/bogus."""


class UnauthorizedSignerError(QuorumCertificateError):
    """Raised when a signer is not a recognized member of the epoch committee roster."""


class KeyRotationViolationError(QuorumCertificateError):
    """Raised when an uncertified or regressive committee rotation is attempted."""


# ==============================================================================
# 2. Domain Types & Enums
# ==============================================================================

@dataclass(frozen=True)
class CommitteeMember:
    """Immutable validator or operator member of an epoch committee."""
    member_id: str
    public_key_hex: str
    role: str = "VALIDATOR"


@dataclass(frozen=True)
class CommitteeRoster:
    """Certified membership roster for a specific epoch."""
    epoch: int
    members: Dict[str, CommitteeMember]
    committee_root: str
    quorum_threshold: int
    fault_tolerance_f: int
    created_at_ns: int = field(default_factory=time.time_ns)

    @property
    def total_members(self) -> int:
        return len(self.members)


@dataclass
class ThresholdQuorumCertificate:
    """Cryptographic multi-party proof of execution or state cutover."""
    cert_id: str
    domain: str
    epoch: int
    digest: str
    committee_root: str
    quorum_required: int
    signatures: Dict[str, str]  # member_id -> signature_hex
    signers: List[str]          # sorted list of participating member IDs
    issued_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cert_id": self.cert_id,
            "domain": self.domain,
            "epoch": self.epoch,
            "digest": self.digest,
            "committee_root": self.committee_root,
            "quorum_required": self.quorum_required,
            "signatures": dict(sorted(self.signatures.items())),
            "signers": list(self.signers),
            "issued_at_ns": self.issued_at_ns,
        }


@dataclass(frozen=True)
class QuorumVerdict:
    """Outcome of threshold quorum certificate verification."""
    is_valid: bool
    verified_signers: List[str]
    threshold_met: bool
    error_reason: Optional[str] = None


# ==============================================================================
# 3. Cryptographic Math & Helpers
# ==============================================================================

def canonical_json_bytes(data: Dict[str, Any]) -> bytes:
    """Produces canonical, sorted, deterministic JSON bytes."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def compute_byzantine_quorum(n: int) -> Tuple[int, int]:
    """
    Computes fault tolerance f and threshold quorum Q for N committee members.
    f = floor((N - 1) / 3)
    Q = ceil((N + f + 1) / 2)
    For small committees (N < 4), f=0 and Q=ceil((N+1)/2) with a minimum of 1.
    """
    if n <= 0:
        return 0, 0
    if n < 4:
        f = 0
        q = max(1, math.ceil((n + 1) / 2))
        return f, q
    f = (n - 1) // 3
    q = math.ceil((n + f + 1) / 2)
    return f, q


def compute_merkle_root(leaf_hashes: List[str]) -> str:
    """
    Computes a deterministic binary Merkle tree root over sorted leaf hex hashes.
    Guarantees balanced tree construction with odd-leaf duplicate hashing.
    """
    if not leaf_hashes:
        return hashlib.sha256(b"sw:merkle:empty").hexdigest()
    
    current_level = sorted(leaf_hashes)
    while len(current_level) > 1:
        next_level = []
        for i in range(0, len(current_level), 2):
            left = current_level[i]
            right = current_level[i + 1] if i + 1 < len(current_level) else current_level[i]
            node_hash = hashlib.sha256(f"{left}:{right}".encode("utf-8")).hexdigest()
            next_level.append(node_hash)
        current_level = next_level
    return current_level[0]


def compute_committee_root(members: List[CommitteeMember]) -> str:
    """Computes deterministic Merkle root from sorted member public key digests."""
    leaf_digests = [
        hashlib.sha256(f"{m.member_id}:{m.public_key_hex}".encode("utf-8")).hexdigest()
        for m in sorted(members, key=lambda m: m.member_id)
    ]
    return compute_merkle_root(leaf_digests)


def format_attestation_payload(domain: str, epoch: int, digest: str, committee_root: str) -> bytes:
    """Builds canonical domain-separated signing message."""
    domain_tag = f"sw:qc:v1:{domain}"
    raw = f"{domain_tag}:{epoch}:{digest}:{committee_root}".encode("utf-8")
    return hashlib.sha256(raw).digest()


# ==============================================================================
# 4. Committee Key Ceremony & Quorum Attestation Kernel
# ==============================================================================

def create_committee_roster(epoch: int, members: List[CommitteeMember]) -> CommitteeRoster:
    """
    Executes a committee initialization ceremony for a given epoch.
    Calculates Byzantine threshold and Merkle root deterministically.
    """
    if not members:
        raise ValueError("Committee members list cannot be empty")
    
    member_dict = {m.member_id: m for m in members}
    if len(member_dict) != len(members):
        raise ValueError("Duplicate member IDs found in committee ceremony")
    
    f, q = compute_byzantine_quorum(len(members))
    root = compute_committee_root(members)
    
    return CommitteeRoster(
        epoch=epoch,
        members=member_dict,
        committee_root=root,
        quorum_threshold=q,
        fault_tolerance_f=f,
    )


def attest_quorum(
    roster: CommitteeRoster,
    domain: str,
    digest: str,
    signer_keys: Dict[str, Ed25519PrivateKey],
) -> ThresholdQuorumCertificate:
    """
    Produces a ThresholdQuorumCertificate by collecting Ed25519 signatures from committee members.
    Fails closed if the collected signatures do not meet roster.quorum_threshold.
    """
    if not signer_keys:
        raise InsufficientQuorumError("Cannot attest quorum with zero signer keys")
    
    payload = format_attestation_payload(domain, roster.epoch, digest, roster.committee_root)
    signatures: Dict[str, str] = {}
    signers: List[str] = []
    
    for member_id, private_key in signer_keys.items():
        if member_id not in roster.members:
            raise UnauthorizedSignerError(f"Signer '{member_id}' is not in epoch {roster.epoch} roster")
        
        # Verify the private key corresponds to the roster's declared public key
        pub_bytes = private_key.public_key().public_bytes_raw()
        expected_pub_hex = roster.members[member_id].public_key_hex
        if pub_bytes.hex() != expected_pub_hex:
            raise UnauthorizedSignerError(f"Private key for '{member_id}' does not match registered public key")
        
        sig_bytes = private_key.sign(payload)
        signatures[member_id] = sig_bytes.hex()
        signers.append(member_id)
    
    signers.sort()
    
    if len(signatures) < roster.quorum_threshold:
        raise InsufficientQuorumError(
            f"Collected {len(signatures)} signatures, required quorum is {roster.quorum_threshold}"
        )
    
    cert_id_material = f"{domain}:{roster.epoch}:{digest}:{roster.committee_root}:{':'.join(signers)}"
    cert_id = hashlib.sha256(cert_id_material.encode("utf-8")).hexdigest()
    
    return ThresholdQuorumCertificate(
        cert_id=cert_id,
        domain=domain,
        epoch=roster.epoch,
        digest=digest,
        committee_root=roster.committee_root,
        quorum_required=roster.quorum_threshold,
        signatures=signatures,
        signers=signers,
    )


def verify_quorum_certificate(
    cert: ThresholdQuorumCertificate,
    roster: Optional[CommitteeRoster] = None,
    expected_root: Optional[str] = None,
    expected_epoch: Optional[int] = None,
) -> QuorumVerdict:
    """
    Verifies a ThresholdQuorumCertificate against the committee's Merkle root and Ed25519 public keys.
    Fails closed if signatures are missing, unkeyed, forged, or under quorum.
    """
    if expected_epoch is not None and cert.epoch != expected_epoch:
        return QuorumVerdict(
            is_valid=False,
            verified_signers=[],
            threshold_met=False,
            error_reason=f"Certificate epoch {cert.epoch} does not match expected {expected_epoch}",
        )
    
    if expected_root is not None and cert.committee_root != expected_root:
        return QuorumVerdict(
            is_valid=False,
            verified_signers=[],
            threshold_met=False,
            error_reason=f"Certificate committee root {cert.committee_root} does not match expected {expected_root}",
        )
    
    # If roster provided, verify the certificate's committee root matches the roster's root
    if roster is not None:
        if cert.committee_root != roster.committee_root:
            return QuorumVerdict(
                is_valid=False,
                verified_signers=[],
                threshold_met=False,
                error_reason="Certificate committee root does not match provided roster root",
            )
        effective_threshold = roster.quorum_threshold
    else:
        # Without a roster, cannot verify individual public keys safely unless pinned
        if not expected_root or cert.committee_root != expected_root:
            return QuorumVerdict(
                is_valid=False,
                verified_signers=[],
                threshold_met=False,
                error_reason="Cannot verify certificate without trusted roster or matching expected_root",
            )
        effective_threshold = cert.quorum_required
    
    if not cert.signatures:
        return QuorumVerdict(
            is_valid=False,
            verified_signers=[],
            threshold_met=False,
            error_reason="Certificate contains no cryptographic signatures",
        )
    
    payload = format_attestation_payload(cert.domain, cert.epoch, cert.digest, cert.committee_root)
    verified_signers: List[str] = []
    
    for member_id, sig_hex in cert.signatures.items():
        if roster is not None:
            if member_id not in roster.members:
                continue
            pub_hex = roster.members[member_id].public_key_hex
        else:
            # Cannot verify without public key knowledge
            return QuorumVerdict(
                is_valid=False,
                verified_signers=[],
                threshold_met=False,
                error_reason="Missing public key material for member verification",
            )
        
        try:
            pub_bytes = bytes.fromhex(pub_hex)
            sig_bytes = bytes.fromhex(sig_hex)
            pub_key = Ed25519PublicKey.from_public_bytes(pub_bytes)
            pub_key.verify(sig_bytes, payload)
            verified_signers.append(member_id)
        except (ValueError, InvalidSignature):
            continue
    
    threshold_met = len(verified_signers) >= effective_threshold
    is_valid = threshold_met and (len(verified_signers) >= cert.quorum_required)
    
    return QuorumVerdict(
        is_valid=is_valid,
        verified_signers=verified_signers,
        threshold_met=threshold_met,
        error_reason=None if is_valid else f"Quorum threshold not met: {len(verified_signers)}/{effective_threshold}",
    )


def rotate_committee(
    current_roster: CommitteeRoster,
    new_members: List[CommitteeMember],
    signer_keys: Dict[str, Ed25519PrivateKey],
) -> Tuple[CommitteeRoster, ThresholdQuorumCertificate]:
    """
    Executes a certified committee rotation ceremony to epoch e+1.
    The current committee signs a QC committing to the next CommitteeRoot.
    """
    next_epoch = current_roster.epoch + 1
    next_roster = create_committee_roster(next_epoch, new_members)
    
    # Attestation digest is the next committee root
    rotation_digest = next_roster.committee_root
    rotation_qc = attest_quorum(
        roster=current_roster,
        domain="committee:rotation",
        digest=rotation_digest,
        signer_keys=signer_keys,
    )
    
    return next_roster, rotation_qc
