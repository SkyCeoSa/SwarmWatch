# -*- coding: utf-8 -*-
"""
SwarmWatch BFT View-Change & Accountable Primary Rotation (Seam 87 / ADR 0098).

Implements decentralized PBFT view-change liveness (V1), third-party verifiable equivocation
slashing proofs (V2), and deterministic primary assignment (V3).

Invariants:
- V1: New view installation requires >= 2f+1 signed ViewChangeVotes carrying prepared proofs.
- V2: Conflicting signatures for the same view and sequence produce an incontrovertible SlashingProof.
- V3: Primary for view v is deterministically sorted_members[v % N].
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from swarmwatch.qc import (
    CommitteeMember,
    CommitteeRoster,
    compute_byzantine_quorum,
)


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class BftViewError(Exception):
    """Base exception for BFT view change operations."""


class InsufficientViewVotesError(BftViewError):
    """Raised when view change votes do not reach 2f+1 quorum."""


class InvalidVoteSignatureError(BftViewError):
    """Raised when a view change vote signature fails cryptographic verification."""


class InvalidSlashingProofError(BftViewError):
    """Raised when a slashing proof does not establish true cryptographic equivocation."""


# ==============================================================================
# 2. Domain Types
# ==============================================================================

@dataclass(frozen=True)
class ViewChangeVote:
    """Signed validator vote proposing transition to view v+1."""
    voter_id: str
    target_view: int
    highest_prepared_seq: int
    highest_prepared_digest: str
    signature_hex: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "voter_id": self.voter_id,
            "target_view": self.target_view,
            "highest_prepared_seq": self.highest_prepared_seq,
            "highest_prepared_digest": self.highest_prepared_digest,
            "signature_hex": self.signature_hex,
        }


@dataclass(frozen=True)
class NewViewCertificate:
    """Certified proof that view v has been legally installed by committee quorum."""
    view: int
    primary_id: str
    votes: List[ViewChangeVote]
    proof_hash: str
    installed_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "view": self.view,
            "primary_id": self.primary_id,
            "votes": [v.to_dict() for v in self.votes],
            "proof_hash": self.proof_hash,
            "installed_at_ns": self.installed_at_ns,
        }


@dataclass(frozen=True)
class SlashingProof:
    """Self-contained, third-party verifiable evidence of equivocation (Invariant V2)."""
    slashed_node_id: str
    view: int
    sequence: int
    digest_a: str
    sig_a_hex: str
    digest_b: str
    sig_b_hex: str
    proven_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slashed_node_id": self.slashed_node_id,
            "view": self.view,
            "sequence": self.sequence,
            "digest_a": self.digest_a,
            "sig_a_hex": self.sig_a_hex,
            "digest_b": self.digest_b,
            "sig_b_hex": self.sig_b_hex,
            "proven_at_ns": self.proven_at_ns,
        }


# ==============================================================================
# 3. Helpers & BFT View Coordinator
# ==============================================================================

def get_primary(view: int, roster: CommitteeRoster) -> str:
    """
    Deterministically computes the designated primary for view v (Invariant V3).
    Primary = sorted_member_ids[view % total_members].
    """
    sorted_ids = sorted(roster.members.keys())
    if not sorted_ids:
        raise ValueError("Cannot select primary from empty committee roster")
    return sorted_ids[view % len(sorted_ids)]


def format_view_vote_payload(target_view: int, seq: int, digest: str) -> bytes:
    """Canonical domain-separated byte payload for view change votes."""
    raw = f"sw:bft:view_change:v1:{target_view}:{seq}:{digest}".encode("utf-8")
    return hashlib.sha256(raw).digest()


class BftViewCoordinator:
    """
    Manages view advancement, vote collection, and primary rotation across BFT replicas.
    """

    def __init__(self, initial_view: int = 0) -> None:
        self._lock = threading.RLock()
        self._current_view: int = int(initial_view)
        self._latest_certificate: Optional[NewViewCertificate] = None
        self._slashed_nodes: Set[str] = set()

    @property
    def current_view(self) -> int:
        with self._lock:
            return self._current_view

    def create_vote(
        self,
        target_view: int,
        highest_prepared_seq: int,
        highest_prepared_digest: str,
        voter_id: str,
        voter_key: Ed25519PrivateKey,
    ) -> ViewChangeVote:
        """Constructs and signs a ViewChangeVote."""
        payload = format_view_vote_payload(target_view, highest_prepared_seq, highest_prepared_digest)
        sig = voter_key.sign(payload)
        return ViewChangeVote(
            voter_id=voter_id,
            target_view=target_view,
            highest_prepared_seq=highest_prepared_seq,
            highest_prepared_digest=highest_prepared_digest,
            signature_hex=sig.hex(),
        )

    def install_new_view(
        self,
        target_view: int,
        votes: List[ViewChangeVote],
        roster: CommitteeRoster,
    ) -> NewViewCertificate:
        """
        Installs a new view if >= 2f+1 verified votes are present (Invariant V1).
        Assigns the deterministic primary (Invariant V3).
        """
        with self._lock:
            if target_view <= self._current_view:
                raise BftViewError(
                    f"Target view {target_view} must be strictly greater than current view {self._current_view}"
                )

            # Quorum required is >= 2f + 1 (roster.quorum_threshold)
            required_quorum = roster.quorum_threshold
            valid_votes: List[ViewChangeVote] = []
            seen_voters: Set[str] = set()

            for vote in votes:
                if vote.target_view != target_view:
                    continue
                if vote.voter_id in seen_voters:
                    continue
                if vote.voter_id not in roster.members:
                    continue
                if vote.voter_id in self._slashed_nodes:
                    continue  # Slashed nodes are disqualified

                # Verify Ed25519 signature
                pub_hex = roster.members[vote.voter_id].public_key_hex
                payload = format_view_vote_payload(
                    vote.target_view,
                    vote.highest_prepared_seq,
                    vote.highest_prepared_digest,
                )
                try:
                    pub = Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_hex))
                    pub.verify(bytes.fromhex(vote.signature_hex), payload)
                    valid_votes.append(vote)
                    seen_voters.add(vote.voter_id)
                except (ValueError, InvalidSignature):
                    continue

            if len(valid_votes) < required_quorum:
                raise InsufficientViewVotesError(
                    f"Collected {len(valid_votes)} valid votes, required {required_quorum} (2f+1)"
                )

            primary_id = get_primary(target_view, roster)
            proof_material = f"{target_view}:{primary_id}:{':'.join(sorted(seen_voters))}"
            proof_hash = hashlib.sha256(proof_material.encode("utf-8")).hexdigest()

            cert = NewViewCertificate(
                view=target_view,
                primary_id=primary_id,
                votes=valid_votes,
                proof_hash=proof_hash,
            )

            self._current_view = target_view
            self._latest_certificate = cert
            return cert

    def verify_and_apply_slashing(
        self,
        proof: SlashingProof,
        roster: CommitteeRoster,
    ) -> bool:
        """
        Verifies third-party accountable slashing proof (Invariant V2).
        If both conflicting signatures verify under the node's public key,
        the node is permanently added to the slashed set.
        """
        with self._lock:
            if proof.slashed_node_id not in roster.members:
                raise InvalidSlashingProofError(f"Node '{proof.slashed_node_id}' is not in the committee roster")

            if proof.digest_a == proof.digest_b:
                raise InvalidSlashingProofError("Equivocation requires two different digests for the same view/seq")

            pub_hex = roster.members[proof.slashed_node_id].public_key_hex
            pub = Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_hex))

            payload_a = f"sw:bft:msg:{proof.view}:{proof.sequence}:{proof.digest_a}".encode("utf-8")
            payload_b = f"sw:bft:msg:{proof.view}:{proof.sequence}:{proof.digest_b}".encode("utf-8")

            try:
                pub.verify(bytes.fromhex(proof.sig_a_hex), payload_a)
                pub.verify(bytes.fromhex(proof.sig_b_hex), payload_b)
            except (ValueError, InvalidSignature) as e:
                raise InvalidSlashingProofError(f"Cryptographic verification of equivocation failed: {e}")

            self._slashed_nodes.add(proof.slashed_node_id)
            return True

    def is_slashed(self, node_id: str) -> bool:
        with self._lock:
            return node_id in self._slashed_nodes
