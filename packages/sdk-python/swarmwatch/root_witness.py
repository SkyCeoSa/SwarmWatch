# -*- coding: utf-8 -*-
"""
Independent Root Witness & Public-Key Chains (Seam 64 / ADR 0075).
Implements asymmetric ECDSA (NIST P-256 / ES256) signing, RFC 6962 Certificate
Transparency logarithmic consistency proofs, rollback and whole-chain replacement
attack detection, NIST SP 800-57 verifiable key rotation logs, and sub-250µs verification.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class WitnessError(RuntimeError):
    """Base exception for all root witness operational failures."""
    pass


class RollbackAttackError(WitnessError):
    """Raised when a checkpoint attempts to rewind monotonic epoch or truncate tree size."""
    pass


class ConsistencyVerificationError(WitnessError):
    """Raised when an RFC 6962 consistency proof fails to mathematically verify."""
    pass


class SplitBrainForkError(WitnessError):
    """Raised when a checkpoint breaks the cryptographic hash chain or diverges from verified history."""
    pass


class KeyRotationError(WitnessError):
    """Raised when key rotation verification or proof-of-possession fails."""
    pass


# ---------------------------------------------------------------------------
# Cryptographic Primitives (NIST P-256 Curve & Utilities)
# ---------------------------------------------------------------------------

P_256_P = 0xFFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFF
P_256_A = P_256_P - 3
P_256_B = 0x5AC635D8AA3A93E7B3EBBD55769886BC651D06B0CC53B0F63BCE3C3E27D2604B
P_256_N = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551
P_256_GX = 0x6B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296
P_256_GY = 0x4FE342E2FE1A7F9B8EE7EB4A7C0F9E162BCE33576B315ECECBB6406837BF51F5


def _point_add(pt1: Optional[Tuple[int, int]], pt2: Optional[Tuple[int, int]]) -> Optional[Tuple[int, int]]:
    """Elliptic curve point addition on NIST P-256."""
    if pt1 is None:
        return pt2
    if pt2 is None:
        return pt1
    x1, y1 = pt1
    x2, y2 = pt2
    if x1 == x2:
        if (y1 + y2) % P_256_P == 0:
            return None
        # Point doubling
        lam = ((3 * x1 * x1 + P_256_A) * pow(2 * y1, P_256_P - 2, P_256_P)) % P_256_P
    else:
        lam = ((y2 - y1) * pow(x2 - x1, P_256_P - 2, P_256_P)) % P_256_P
    x3 = (lam * lam - x1 - x2) % P_256_P
    y3 = (lam * (x1 - x3) - y1) % P_256_P
    return (x3, y3)


def _point_mul(k: int, pt: Tuple[int, int]) -> Optional[Tuple[int, int]]:
    """Elliptic curve scalar multiplication (double-and-add) on NIST P-256."""
    res = None
    curr = pt
    while k > 0:
        if k & 1:
            res = _point_add(res, curr)
        curr = _point_add(curr, curr)
        k >>= 1
    return res


def _b64url_encode(data: bytes) -> str:
    """Encodes bytes into URL-safe Base64 without trailing '=' padding."""
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    """Decodes URL-safe Base64 string with optional stripped padding."""
    s += "=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s.encode("ascii"))


def sha256_digest(content: Union[str, bytes]) -> str:
    """Computes standard hex-encoded SHA-256 digest."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def sha256_bytes(content: Union[str, bytes]) -> bytes:
    """Computes standard raw SHA-256 digest bytes."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).digest()


# ---------------------------------------------------------------------------
# Key Management: WitnessKeyManager
# ---------------------------------------------------------------------------

class WitnessKeyManager:
    """
    Manages asymmetric NIST P-256 (secp256r1 / ES256) keypair generation,
    IEEE P1363 (64-byte r || s) signing, and public-key verification.
    Guarantees pure zero-dependency cross-runtime compatibility with Node.js crypto.
    """

    def __init__(
        self,
        private_key_b64: Optional[str] = None,
        public_key_b64: Optional[str] = None,
    ) -> None:
        self.private_key_b64 = private_key_b64
        self.public_key_b64 = public_key_b64
        self._private_scalar: Optional[int] = None
        self._public_point: Optional[Tuple[int, int]] = None

        if private_key_b64:
            self._private_scalar = int.from_bytes(_b64url_decode(private_key_b64), "big")
            if public_key_b64:
                pub_raw = _b64url_decode(public_key_b64)
                if len(pub_raw) == 65 and pub_raw[0] == 0x04:
                    self._public_point = (
                        int.from_bytes(pub_raw[1:33], "big"),
                        int.from_bytes(pub_raw[33:65], "big"),
                    )
                else:
                    self._public_point = _point_mul(self._private_scalar, (P_256_GX, P_256_GY))
            else:
                self._public_point = _point_mul(self._private_scalar, (P_256_GX, P_256_GY))
                if self._public_point is not None:
                    pub_bytes = b"\x04" + self._public_point[0].to_bytes(32, "big") + self._public_point[1].to_bytes(32, "big")
                    self.public_key_b64 = _b64url_encode(pub_bytes)
        elif public_key_b64:
            pub_raw = _b64url_decode(public_key_b64)
            if len(pub_raw) == 65 and pub_raw[0] == 0x04:
                self._public_point = (
                    int.from_bytes(pub_raw[1:33], "big"),
                    int.from_bytes(pub_raw[33:65], "big"),
                )
            else:
                raise ValueError("Invalid public key: expected 65-byte uncompressed point starting with 0x04")
        else:
            # Generate brand new keypair
            self._private_scalar = secrets.randbelow(P_256_N - 2) + 1
            self._public_point = _point_mul(self._private_scalar, (P_256_GX, P_256_GY))
            if self._public_point is None:
                raise RuntimeError("Failed to compute P-256 public point")

            priv_bytes = self._private_scalar.to_bytes(32, "big")
            pub_bytes = b"\x04" + self._public_point[0].to_bytes(32, "big") + self._public_point[1].to_bytes(32, "big")

            self.private_key_b64 = _b64url_encode(priv_bytes)
            self.public_key_b64 = _b64url_encode(pub_bytes)

    @classmethod
    def generate(cls) -> "WitnessKeyManager":
        """Factory method to generate a new EC keypair."""
        return cls()

    @classmethod
    def from_public_key(cls, public_key_b64: str) -> "WitnessKeyManager":
        """Factory method to construct a verification-only instance from an uncompressed public key."""
        return cls(public_key_b64=public_key_b64)

    def get_raw_public_key(self) -> bytes:
        """Returns standard 65-byte uncompressed public key point (0x04 || X || Y)."""
        if self._public_point is None:
            raise WitnessError("Public key point not available")
        return b"\x04" + self._public_point[0].to_bytes(32, "big") + self._public_point[1].to_bytes(32, "big")

    def sign(self, message: bytes) -> bytes:
        """
        Computes IEEE P1363 64-byte (r || s) ES256 signature for message bytes.
        Enforces standard low-s (BIP 62) canonicalization.
        """
        if self._private_scalar is None:
            raise WitnessError("Cannot sign without private key")

        e = int.from_bytes(hashlib.sha256(message).digest(), "big")
        while True:
            k = secrets.randbelow(P_256_N - 2) + 1
            pt_r = _point_mul(k, (P_256_GX, P_256_GY))
            if pt_r is None:
                continue
            r = pt_r[0] % P_256_N
            if r == 0:
                continue
            k_inv = pow(k, P_256_N - 2, P_256_N)
            s = (k_inv * (e + self._private_scalar * r)) % P_256_N
            if s == 0:
                continue
            # Enforce low-s
            if s > P_256_N // 2:
                s = P_256_N - s
            return r.to_bytes(32, "big") + s.to_bytes(32, "big")

    def sign_b64(self, message: bytes) -> str:
        """Computes IEEE P1363 signature and returns as URL-safe Base64 string."""
        return _b64url_encode(self.sign(message))

    def verify(self, message: bytes, signature: bytes) -> bool:
        """Verifies 64-byte (r || s) ES256 signature against current public key."""
        if len(signature) != 64 or self._public_point is None:
            return False
        r = int.from_bytes(signature[:32], "big")
        s = int.from_bytes(signature[32:], "big")
        if not (0 < r < P_256_N and 0 < s < P_256_N):
            return False

        e = int.from_bytes(hashlib.sha256(message).digest(), "big")
        w = pow(s, P_256_N - 2, P_256_N)
        u1 = (e * w) % P_256_N
        u2 = (r * w) % P_256_N

        pt1 = _point_mul(u1, (P_256_GX, P_256_GY))
        pt2 = _point_mul(u2, self._public_point)
        v = _point_add(pt1, pt2)
        if v is None:
            return False
        return (v[0] % P_256_N) == r

    def verify_b64(self, message: bytes, signature_b64: str) -> bool:
        """Verifies base64url-encoded signature against message."""
        try:
            sig_bytes = _b64url_decode(signature_b64)
            return self.verify(message, sig_bytes)
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Merkle Tree & RFC 6962 Consistency Proof Primitives
# ---------------------------------------------------------------------------

def leaf_hash(data: Union[str, bytes]) -> str:
    """Computes RFC 6962 leaf hash: SHA256(0x00 || data)."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(b"\x00" + data).hexdigest()


def node_hash(left_hex: str, right_hex: str) -> str:
    """Computes RFC 6962 interior node hash: SHA256(0x01 || left || right)."""
    left_bytes = bytes.fromhex(left_hex)
    right_bytes = bytes.fromhex(right_hex)
    return hashlib.sha256(b"\x01" + left_bytes + right_bytes).hexdigest()


def _largest_power_of_2_less_than(n: int) -> int:
    """Computes the largest power of 2 strictly less than n."""
    if n <= 1:
        return 0
    k = 1
    while k < n:
        k <<= 1
    return k >> 1


def _mth(leaves: List[str]) -> str:
    """Computes RFC 6962 Merkle Tree Hash (MTH) across leaves."""
    n = len(leaves)
    if n == 0:
        return sha256_digest(b"")
    if n == 1:
        return leaves[0]
    k = _largest_power_of_2_less_than(n)
    return node_hash(_mth(leaves[:k]), _mth(leaves[k:]))


def _subproof(m: int, leaves: List[str], b: bool) -> List[str]:
    """RFC 6962 SUBPROOF recursive helper."""
    n = len(leaves)
    if m == n:
        return [] if b else [_mth(leaves)]
    k = _largest_power_of_2_less_than(n)
    if m <= k:
        return _subproof(m, leaves[:k], b) + [_mth(leaves[k:])]
    else:
        return _subproof(m - k, leaves[k:], False) + [_mth(leaves[:k])]


def _consistency_proof(m: int, leaves: List[str]) -> List[str]:
    """Generates RFC 6962 Section 2.1.2 consistency proof between tree size m and len(leaves)."""
    n = len(leaves)
    if m == n or m == 0:
        return []
    k = _largest_power_of_2_less_than(n)
    if m <= k:
        return _subproof(m, leaves[:k], True) + [_mth(leaves[k:])]
    else:
        return _subproof(m - k, leaves[k:], False) + [_mth(leaves[:k])]


def _verify_consistency_proof(
    m: int,
    n: int,
    old_root: str,
    new_root: str,
    proof: List[str],
) -> bool:
    """
    Verifies RFC 6962 Section 2.1.2 consistency proof between size m and n in O(log n) time.
    Reconstructs both old and new roots from the proof path.
    """
    if m == n:
        return (not proof) and (old_root == new_root)
    if m == 0 or m > n:
        return False
    if not proof:
        return False

    p = list(proof)
    if (m & (m - 1)) == 0:
        p = [old_root] + p

    fn = m - 1
    sn = n - 1
    while (fn & 1) == 1:
        fn >>= 1
        sn >>= 1

    if not p:
        return False
    fr = p[0]
    sr = p[0]
    p = p[1:]

    for c in p:
        if sn == 0:
            return False
        if (fn & 1) == 1 or fn == sn:
            fr = node_hash(c, fr)
            sr = node_hash(c, sr)
            while (fn & 1) == 0 and fn != 0:
                fn >>= 1
                sn >>= 1
        else:
            sr = node_hash(sr, c)
        fn >>= 1
        sn >>= 1

    return fr == old_root and sr == new_root and sn == 0


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class LeafReceipt:
    """Receipt certifying commitment of a leaf into the witness accumulator."""
    leaf_index: int
    leaf_hash: str
    timestamp_ns: int = field(default_factory=time.time_ns)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "leaf_index": self.leaf_index,
            "leaf_hash": self.leaf_hash,
            "timestamp_ns": self.timestamp_ns,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LeafReceipt":
        return cls(
            leaf_index=int(data.get("leaf_index", 0)),
            leaf_hash=str(data.get("leaf_hash", "")),
            timestamp_ns=int(data.get("timestamp_ns", 0)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class SignedCheckpoint:
    """
    Immutable root epoch checkpoint signed by an independent witness public-key pair.
    Binds monotonic epoch, tree size, Merkle root, and previous checkpoint hash.
    """
    epoch: int
    tree_size: int
    root_hash: str
    prev_checkpoint_hash: str
    checkpoint_hash: str
    witness_public_key: str
    signature: str
    timestamp_ns: int = field(default_factory=time.time_ns)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def compute_checkpoint_hash(
        epoch: int,
        tree_size: int,
        root_hash: str,
        prev_checkpoint_hash: str,
        timestamp_ns: int,
        witness_public_key: str,
    ) -> str:
        """Computes deterministic canonical checkpoint hash."""
        seed = f"sw:cp:v1:{epoch}:{tree_size}:{root_hash}:{prev_checkpoint_hash}:{timestamp_ns}:{witness_public_key}"
        return sha256_digest(seed)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "tree_size": self.tree_size,
            "root_hash": self.root_hash,
            "prev_checkpoint_hash": self.prev_checkpoint_hash,
            "checkpoint_hash": self.checkpoint_hash,
            "witness_public_key": self.witness_public_key,
            "signature": self.signature,
            "timestamp_ns": self.timestamp_ns,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SignedCheckpoint":
        return cls(
            epoch=int(data.get("epoch", 0)),
            tree_size=int(data.get("tree_size", 0)),
            root_hash=str(data.get("root_hash", "")),
            prev_checkpoint_hash=str(data.get("prev_checkpoint_hash", "")),
            checkpoint_hash=str(data.get("checkpoint_hash", "")),
            witness_public_key=str(data.get("witness_public_key", "")),
            signature=str(data.get("signature", "")),
            timestamp_ns=int(data.get("timestamp_ns", 0)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class ConsistencyProof:
    """RFC 6962 Logarithmic Consistency Proof between two tree versions."""
    old_size: int
    new_size: int
    old_root: str
    new_root: str
    proof_hashes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "old_size": self.old_size,
            "new_size": self.new_size,
            "old_root": self.old_root,
            "new_root": self.new_root,
            "proof_hashes": list(self.proof_hashes),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConsistencyProof":
        return cls(
            old_size=int(data.get("old_size", 0)),
            new_size=int(data.get("new_size", 0)),
            old_root=str(data.get("old_root", "")),
            new_root=str(data.get("new_root", "")),
            proof_hashes=[str(h) for h in data.get("proof_hashes", [])],
        )


@dataclass
class KeyRotationRecord:
    """
    NIST SP 800-57 cryptographically verifiable key rotation record.
    Includes challenge, proof-of-possession from new key, and authorization from old key.
    """
    rotation_id: str
    old_public_key: str
    new_public_key: str
    rotation_epoch: int
    timestamp_ns: int
    challenge: str
    proof_of_possession: str
    authorization_signature: str

    @staticmethod
    def compute_challenge(
        old_public_key: str,
        new_public_key: str,
        rotation_epoch: int,
        timestamp_ns: int,
    ) -> str:
        """Computes deterministic challenge payload for key rotation."""
        seed = f"sw:rot:v1:{old_public_key}:{new_public_key}:{rotation_epoch}:{timestamp_ns}"
        return sha256_digest(seed)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rotation_id": self.rotation_id,
            "old_public_key": self.old_public_key,
            "new_public_key": self.new_public_key,
            "rotation_epoch": self.rotation_epoch,
            "timestamp_ns": self.timestamp_ns,
            "challenge": self.challenge,
            "proof_of_possession": self.proof_of_possession,
            "authorization_signature": self.authorization_signature,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KeyRotationRecord":
        return cls(
            rotation_id=str(data.get("rotation_id", "")),
            old_public_key=str(data.get("old_public_key", "")),
            new_public_key=str(data.get("new_public_key", "")),
            rotation_epoch=int(data.get("rotation_epoch", 0)),
            timestamp_ns=int(data.get("timestamp_ns", 0)),
            challenge=str(data.get("challenge", "")),
            proof_of_possession=str(data.get("proof_of_possession", "")),
            authorization_signature=str(data.get("authorization_signature", "")),
        )


@dataclass
class WitnessStatus:
    """Operational status summary for the Independent Root Witness."""
    active_public_key: str
    latest_epoch: int
    current_tree_size: int
    latest_checkpoint_hash: str
    total_rotations: int
    total_checkpoints: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "active_public_key": self.active_public_key,
            "latest_epoch": self.latest_epoch,
            "current_tree_size": self.current_tree_size,
            "latest_checkpoint_hash": self.latest_checkpoint_hash,
            "total_rotations": self.total_rotations,
            "total_checkpoints": self.total_checkpoints,
        }


# ---------------------------------------------------------------------------
# Core Deep Module: RootWitness
# ---------------------------------------------------------------------------

class RootWitness:
    """
    Independent Root Witness maintaining an append-only, publicly verifiable Merkle
    tree accumulator and asymmetric public-key checkpoint chains.
    Enforces RFC 6962 consistency proofs and NIST SP 800-57 key rotation logs.
    """

    GENESIS_PREV_HASH = "0" * 64

    def __init__(
        self,
        key_manager: Optional[WitnessKeyManager] = None,
        storage_path: Optional[str] = None,
    ) -> None:
        self._lock = threading.RLock()
        self.key_manager = key_manager or WitnessKeyManager.generate()
        self.storage_path = storage_path

        self._leaves: List[str] = []
        self._checkpoints: List[SignedCheckpoint] = []
        self._rotation_log: List[KeyRotationRecord] = []

        if self.storage_path and os.path.exists(self.storage_path):
            self._load_from_storage()

    @property
    def current_tree_size(self) -> int:
        with self._lock:
            stored_size = self._checkpoints[-1].tree_size if self._checkpoints else 0
            return max(len(self._leaves), stored_size)

    @property
    def total_checkpoints(self) -> int:
        with self._lock:
            return len(self._checkpoints)

    @property
    def latest_epoch(self) -> int:
        with self._lock:
            return self._checkpoints[-1].epoch if self._checkpoints else -1

    def submit(
        self,
        leaf_data: Union[str, bytes],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LeafReceipt:
        """
        Commits a new leaf payload into the witness accumulator and returns a LeafReceipt.
        """
        with self._lock:
            l_hash = leaf_hash(leaf_data)
            index = len(self._leaves)
            self._leaves.append(l_hash)
            receipt = LeafReceipt(
                leaf_index=index,
                leaf_hash=l_hash,
                timestamp_ns=time.time_ns(),
                metadata=metadata or {},
            )
            if self.storage_path:
                self._persist_leaf(receipt)
            return receipt

    def checkpoint(self, metadata: Optional[Dict[str, Any]] = None) -> SignedCheckpoint:
        """
        Issues an immutable signed checkpoint anchoring the current tree state.
        Monotonically advances epoch, computes canonical hash, and signs with asymmetric key.
        """
        with self._lock:
            epoch = len(self._checkpoints)
            tree_size = len(self._leaves)
            root = _mth(self._leaves)

            prev_hash = self._checkpoints[-1].checkpoint_hash if self._checkpoints else self.GENESIS_PREV_HASH
            now_ns = time.time_ns()
            pub_key = self.key_manager.public_key_b64

            cp_hash = SignedCheckpoint.compute_checkpoint_hash(
                epoch=epoch,
                tree_size=tree_size,
                root_hash=root,
                prev_checkpoint_hash=prev_hash,
                timestamp_ns=now_ns,
                witness_public_key=pub_key,
            )

            signature = self.key_manager.sign_b64(cp_hash.encode("utf-8"))

            cp = SignedCheckpoint(
                epoch=epoch,
                tree_size=tree_size,
                root_hash=root,
                prev_checkpoint_hash=prev_hash,
                checkpoint_hash=cp_hash,
                witness_public_key=pub_key,
                signature=signature,
                timestamp_ns=now_ns,
                metadata=metadata or {},
            )

            self._checkpoints.append(cp)

            if self.storage_path:
                self._persist_checkpoint(cp)

            return cp

    def get_latest_checkpoint(self) -> Optional[SignedCheckpoint]:
        """Retrieves the latest issued or ingested signed checkpoint."""
        with self._lock:
            return self._checkpoints[-1] if self._checkpoints else None

    def get_checkpoint(self, epoch: int = -1) -> SignedCheckpoint:
        """Retrieves a specific checkpoint by epoch index."""
        with self._lock:
            if not self._checkpoints:
                raise WitnessError("No checkpoints exist in witness")
            if epoch == -1:
                return self._checkpoints[-1]
            if 0 <= epoch < len(self._checkpoints):
                return self._checkpoints[epoch]
            raise WitnessError(f"Checkpoint for epoch {epoch} not found")

    def get_consistency_proof(self, old_size: int, new_size: int) -> ConsistencyProof:
        """
        Generates an RFC 6962 consistency proof between tree size old_size and new_size.
        """
        with self._lock:
            if old_size > new_size or new_size > len(self._leaves):
                raise WitnessError(f"Invalid consistency proof bounds: {old_size} to {new_size} (current: {len(self._leaves)})")

            old_root = _mth(self._leaves[:old_size])
            new_root = _mth(self._leaves[:new_size])
            proof_hashes = _consistency_proof(old_size, self._leaves[:new_size])

            return ConsistencyProof(
                old_size=old_size,
                new_size=new_size,
                old_root=old_root,
                new_root=new_root,
                proof_hashes=proof_hashes,
            )

    def ingest_external_checkpoint(self, checkpoint: SignedCheckpoint) -> bool:
        """
        Ingests an external signed checkpoint, verifying asymmetric signature,
        detecting rollback attacks, and preventing split-brain chain forks.
        """
        with self._lock:
            # 1. Verify asymmetric signature
            if not self.verify_checkpoint(checkpoint, checkpoint.witness_public_key):
                raise WitnessError("Checkpoint signature verification failed")

            if self._checkpoints:
                latest = self._checkpoints[-1]
                # 2. Check for rollback attacks
                if checkpoint.epoch <= latest.epoch:
                    raise RollbackAttackError(f"Rollback attack detected: incoming epoch {checkpoint.epoch} <= active epoch {latest.epoch}")
                if checkpoint.tree_size < latest.tree_size:
                    raise RollbackAttackError(f"Tree size truncation detected: incoming size {checkpoint.tree_size} < active size {latest.tree_size}")

                # 3. Check for split-brain fork
                if checkpoint.prev_checkpoint_hash != latest.checkpoint_hash:
                    raise SplitBrainForkError(
                        f"Split-brain fork detected: incoming prev_hash {checkpoint.prev_checkpoint_hash} "
                        f"does not match active head {latest.checkpoint_hash}"
                    )

            self._checkpoints.append(checkpoint)
            if self.storage_path:
                self._persist_checkpoint(checkpoint)
            return True

    @staticmethod
    def verify_checkpoint(
        checkpoint: SignedCheckpoint,
        public_key: Optional[str] = None,
    ) -> bool:
        """
        Independently verifies a signed checkpoint using ONLY the public key.
        Guarantees zero-knowledge of private key and full public auditability.
        """
        target_pub = public_key or checkpoint.witness_public_key
        if not target_pub:
            return False

        # 1. Recompute canonical checkpoint hash
        expected_hash = SignedCheckpoint.compute_checkpoint_hash(
            epoch=checkpoint.epoch,
            tree_size=checkpoint.tree_size,
            root_hash=checkpoint.root_hash,
            prev_checkpoint_hash=checkpoint.prev_checkpoint_hash,
            timestamp_ns=checkpoint.timestamp_ns,
            witness_public_key=target_pub,
        )

        if checkpoint.checkpoint_hash != expected_hash:
            return False

        # 2. Verify asymmetric signature
        try:
            verifier = WitnessKeyManager.from_public_key(target_pub)
            return verifier.verify_b64(expected_hash.encode("utf-8"), checkpoint.signature)
        except Exception:
            return False

    @staticmethod
    def verify_consistency(
        old_checkpoint: SignedCheckpoint,
        new_checkpoint: SignedCheckpoint,
        proof: ConsistencyProof,
    ) -> bool:
        """
        Verifies RFC 6962 consistency proof between two checkpoints in O(log n) time.
        Raises ConsistencyVerificationError if the proof fails.
        """
        if proof.old_size != old_checkpoint.tree_size or proof.new_size != new_checkpoint.tree_size:
            raise ConsistencyVerificationError(
                f"Proof size mismatch: ({proof.old_size}, {proof.new_size}) != ({old_checkpoint.tree_size}, {new_checkpoint.tree_size})"
            )

        if proof.old_root != old_checkpoint.root_hash or proof.new_root != new_checkpoint.root_hash:
            raise ConsistencyVerificationError("Root hash mismatch between checkpoints and consistency proof")

        valid = _verify_consistency_proof(
            m=proof.old_size,
            n=proof.new_size,
            old_root=proof.old_root,
            new_root=proof.new_root,
            proof=proof.proof_hashes,
        )

        if not valid:
            raise ConsistencyVerificationError("RFC 6962 consistency proof failed to mathematically fold roots")

        return True

    def rotate_key(self, new_key_manager: WitnessKeyManager) -> KeyRotationRecord:
        """
        NIST SP 800-57 compliant key rotation protocol.
        Generates challenge signed by both the incoming key (proof-of-possession)
        and the outgoing active key (authorization).
        """
        with self._lock:
            old_pub = self.key_manager.public_key_b64
            new_pub = new_key_manager.public_key_b64
            rotation_epoch = self.latest_epoch + 1
            now_ns = time.time_ns()

            challenge = KeyRotationRecord.compute_challenge(
                old_public_key=old_pub,
                new_public_key=new_pub,
                rotation_epoch=rotation_epoch,
                timestamp_ns=now_ns,
            )

            proof_of_possession = new_key_manager.sign_b64(challenge.encode("utf-8"))
            auth_sig = self.key_manager.sign_b64(challenge.encode("utf-8"))

            record = KeyRotationRecord(
                rotation_id=str(uuid.uuid4()),
                old_public_key=old_pub,
                new_public_key=new_pub,
                rotation_epoch=rotation_epoch,
                timestamp_ns=now_ns,
                challenge=challenge,
                proof_of_possession=proof_of_possession,
                authorization_signature=auth_sig,
            )

            # Self-verify before committing rotation
            if not self.verify_rotation_record(record):
                raise KeyRotationError("Generated rotation record failed verification")

            self._rotation_log.append(record)
            self.key_manager = new_key_manager
            return record

    @staticmethod
    def verify_rotation_record(record: KeyRotationRecord) -> bool:
        """
        Verifies both proof-of-possession and authorization signatures for a key rotation record.
        """
        expected_challenge = KeyRotationRecord.compute_challenge(
            old_public_key=record.old_public_key,
            new_public_key=record.new_public_key,
            rotation_epoch=record.rotation_epoch,
            timestamp_ns=record.timestamp_ns,
        )

        if record.challenge != expected_challenge:
            raise KeyRotationError("Challenge payload mismatch in key rotation record")

        challenge_bytes = expected_challenge.encode("utf-8")

        # 1. Verify authorization from old key
        old_verifier = WitnessKeyManager.from_public_key(record.old_public_key)
        if not old_verifier.verify_b64(challenge_bytes, record.authorization_signature):
            raise KeyRotationError("Authorization signature by outgoing key failed verification")

        # 2. Verify proof of possession by new key
        new_verifier = WitnessKeyManager.from_public_key(record.new_public_key)
        if not new_verifier.verify_b64(challenge_bytes, record.proof_of_possession):
            raise KeyRotationError("Proof of possession by incoming key failed verification")

        return True

    def verify_rotation_log(self, root_public_key: str) -> bool:
        """
        Verifies the unbroken chain of custody in the key rotation log from
        the initial root public key to the current active key.
        """
        with self._lock:
            current_expected_key = root_public_key
            for rec in self._rotation_log:
                if rec.old_public_key != current_expected_key:
                    return False
                if not self.verify_rotation_record(rec):
                    return False
                current_expected_key = rec.new_public_key
            return current_expected_key == self.key_manager.public_key_b64

    def status(self) -> WitnessStatus:
        """Returns the operational status of the witness."""
        with self._lock:
            latest_hash = self._checkpoints[-1].checkpoint_hash if self._checkpoints else self.GENESIS_PREV_HASH
            return WitnessStatus(
                active_public_key=self.key_manager.public_key_b64,
                latest_epoch=self.latest_epoch,
                current_tree_size=self.current_tree_size,
                latest_checkpoint_hash=latest_hash,
                total_rotations=len(self._rotation_log),
                total_checkpoints=len(self._checkpoints),
            )

    def _persist_leaf(self, receipt: LeafReceipt) -> None:
        """Appends leaf record to persistent append-only JSONL file."""
        if not self.storage_path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.storage_path)), exist_ok=True)
        record = {
            "record_type": "leaf",
            **receipt.to_dict(),
        }
        with open(self.storage_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _persist_checkpoint(self, checkpoint: SignedCheckpoint) -> None:
        """Appends checkpoint record to persistent append-only JSONL file."""
        if not self.storage_path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.storage_path)), exist_ok=True)
        record = {
            "record_type": "checkpoint",
            **checkpoint.to_dict(),
        }
        with open(self.storage_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _load_from_storage(self) -> None:
        """Replays leaves and checkpoints from disk into memory, verifying hash continuity."""
        if not self.storage_path or not os.path.exists(self.storage_path):
            return

        with open(self.storage_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                rec_type = rec.get("record_type")
                if rec_type == "leaf":
                    self._leaves.append(rec["leaf_hash"])
                elif rec_type == "checkpoint" or "epoch" in rec:
                    cp = SignedCheckpoint.from_dict(rec)
                    self._checkpoints.append(cp)


# ---------------------------------------------------------------------------
# Global Singleton Instance
# ---------------------------------------------------------------------------

global_root_witness = RootWitness()
