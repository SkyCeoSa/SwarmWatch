# -*- coding: utf-8 -*-
"""
Durable Audit Anchor & Merkle Audit Receipt Engine (Seam 54 / ADR 0065).
Enforces append-only root commitment chains, post-crash chain of custody,
and verifiable Merkle audit receipts surviving system restarts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid


class AuditIntegrityError(RuntimeError):
    """Raised when the cryptographic audit chain or post-crash chain of custody is violated."""
    pass


def sha256_digest(content: str | bytes) -> str:
    """Computes standard hex-encoded SHA-256 digest."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


@dataclass
class RootEpoch:
    """
    Immutable root epoch recording a committed Merkle root within the append-only audit chain.
    """
    epoch: int
    root_hash: str
    prev_chain_digest: str
    chain_digest: str
    tenant_id: str
    anchored_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    signature: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "root_hash": self.root_hash,
            "prev_chain_digest": self.prev_chain_digest,
            "chain_digest": self.chain_digest,
            "tenant_id": self.tenant_id,
            "anchored_at": self.anchored_at,
            "signature": self.signature,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RootEpoch":
        return cls(
            epoch=int(data.get("epoch", 0)),
            root_hash=str(data.get("root_hash", "")),
            prev_chain_digest=str(data.get("prev_chain_digest", "")),
            chain_digest=str(data.get("chain_digest", "")),
            tenant_id=str(data.get("tenant_id", "system")),
            anchored_at=str(data.get("anchored_at") or datetime.now(timezone.utc).isoformat()),
            signature=str(data.get("signature", "")),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class MerkleAuditReceipt:
    """
    Verifiable Merkle audit receipt proving mathematical inclusion of a leaf in an anchored epoch root.
    Note: Explicitly designated as a Merkle audit receipt, not a Zero-Knowledge proof.
    """
    receipt_id: str
    epoch: int
    tenant_id: str
    leaf_hash: str
    audit_path: List[Tuple[str, str]]  # List of ("left" | "right", sibling_hash)
    root_hash: str
    chain_digest: str
    signature: str = ""
    issued_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "epoch": self.epoch,
            "tenant_id": self.tenant_id,
            "leaf_hash": self.leaf_hash,
            "audit_path": [list(item) for item in self.audit_path],
            "root_hash": self.root_hash,
            "chain_digest": self.chain_digest,
            "signature": self.signature,
            "issued_at": self.issued_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MerkleAuditReceipt":
        raw_path = data.get("audit_path", [])
        parsed_path: List[Tuple[str, str]] = []
        for step in raw_path:
            if isinstance(step, (list, tuple)) and len(step) >= 2:
                parsed_path.append((str(step[0]), str(step[1])))

        return cls(
            receipt_id=str(data.get("receipt_id", "")),
            epoch=int(data.get("epoch", 0)),
            tenant_id=str(data.get("tenant_id", "default-tenant")),
            leaf_hash=str(data.get("leaf_hash", "")),
            audit_path=parsed_path,
            root_hash=str(data.get("root_hash", "")),
            chain_digest=str(data.get("chain_digest", "")),
            signature=str(data.get("signature", "")),
            issued_at=str(data.get("issued_at") or datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class VerificationResult:
    """Outcome of an independent Merkle audit receipt verification."""
    valid: bool
    epoch: int
    leaf_hash: str
    root_hash: str
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "epoch": self.epoch,
            "leaf_hash": self.leaf_hash,
            "root_hash": self.root_hash,
            "reason": self.reason,
        }


class AuditAnchor:
    """
    Append-only durable audit anchor maintaining an immutable hash-chained epoch ledger.
    Guarantees post-crash chain of custody and fail-closed verification against history rewriting.
    """

    GENESIS_PAYLOAD = "GENESIS_SWARMWATCH_AUDIT_ANCHOR"

    def __init__(
        self,
        storage_path: Optional[str] = None,
        secret_key: Optional[str] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._secret_key = secret_key or os.environ.get("SWARMWATCH_AUDIT_SECRET_KEY", "sw_audit_anchor_secret_key_v1")
        
        # Determine persistent storage path
        if storage_path is not None:
            self._storage_path = storage_path
        else:
            self._storage_path = os.environ.get("SWARMWATCH_AUDIT_STORAGE_PATH", "audit_anchor_epochs.jsonl")

        self._epochs: List[RootEpoch] = []
        self._epochs_by_num: Dict[int, RootEpoch] = {}
        self._load_or_initialize()

    @property
    def total_epochs(self) -> int:
        with self._lock:
            return len(self._epochs)

    @property
    def latest_epoch(self) -> int:
        with self._lock:
            return self._epochs[-1].epoch if self._epochs else 0

    @property
    def latest_digest(self) -> str:
        with self._lock:
            return self._epochs[-1].chain_digest if self._epochs else ""

    def _compute_chain_digest(self, prev_digest: str, epoch: int, root_hash: str, tenant_id: str) -> str:
        seed = f"{prev_digest}:{epoch}:{root_hash}:{tenant_id}"
        return sha256_digest(seed)

    def _compute_epoch_signature(self, epoch: int, root_hash: str, chain_digest: str) -> str:
        payload = f"{epoch}:{root_hash}:{chain_digest}"
        return hmac.new(self._secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _compute_receipt_signature(self, receipt_id: str, epoch: int, tenant_id: str, leaf_hash: str, root_hash: str, chain_digest: str) -> str:
        payload = f"{receipt_id}:{epoch}:{tenant_id}:{leaf_hash}:{root_hash}:{chain_digest}"
        return hmac.new(self._secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _create_genesis_epoch(self) -> RootEpoch:
        genesis_root = sha256_digest(self.GENESIS_PAYLOAD)
        genesis_chain_digest = sha256_digest(f"0:{genesis_root}")
        genesis_sig = self._compute_epoch_signature(0, genesis_root, genesis_chain_digest)
        return RootEpoch(
            epoch=0,
            root_hash=genesis_root,
            prev_chain_digest="0" * 64,
            chain_digest=genesis_chain_digest,
            tenant_id="system",
            anchored_at=datetime.now(timezone.utc).isoformat(),
            signature=genesis_sig,
            metadata={"description": "Genesis Audit Anchor Root"},
        )

    def _load_or_initialize(self) -> None:
        """Loads durable epochs and verifies post-crash chain of custody, or initializes Genesis."""
        with self._lock:
            self._epochs.clear()
            self._epochs_by_num.clear()

            if self._storage_path != ":memory:" and os.path.exists(self._storage_path):
                loaded_epochs: List[RootEpoch] = []
                with open(self._storage_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                            epoch_obj = RootEpoch.from_dict(record)
                            loaded_epochs.append(epoch_obj)
                        except Exception as e:
                            raise AuditIntegrityError(f"Corrupted record at line {line_num} in {self._storage_path}: {e}")

                if loaded_epochs:
                    self._epochs = loaded_epochs
                    for ep in self._epochs:
                        self._epochs_by_num[ep.epoch] = ep
                    # Enforce strict post-crash chain of custody
                    self._verify_chain_integrity()
                    return

            # If no existing durable records, anchor Genesis
            genesis = self._create_genesis_epoch()
            self._epochs.append(genesis)
            self._epochs_by_num[genesis.epoch] = genesis
            if self._storage_path != ":memory:":
                dir_name = os.path.dirname(self._storage_path)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                with open(self._storage_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(genesis.to_dict()) + "\n")
                    f.flush()

    def _verify_chain_integrity(self) -> None:
        """
        Validates hash-chain continuity from Genesis (e=0) up to latest epoch.
        Fail-closed: raises AuditIntegrityError if any link has been broken or tampered with.
        """
        if not self._epochs:
            raise AuditIntegrityError("Empty audit epoch history; Genesis is required.")

        # 1. Verify Genesis
        genesis = self._epochs[0]
        if genesis.epoch != 0:
            raise AuditIntegrityError(f"First epoch must be 0 (Genesis), found {genesis.epoch}.")
        expected_gen_root = sha256_digest(self.GENESIS_PAYLOAD)
        if not hmac.compare_digest(genesis.root_hash, expected_gen_root):
            raise AuditIntegrityError("Genesis root hash does not match canonical Genesis payload.")
        expected_gen_digest = sha256_digest(f"0:{expected_gen_root}")
        if not hmac.compare_digest(genesis.chain_digest, expected_gen_digest):
            raise AuditIntegrityError("Genesis chain digest does not match expected canonical digest.")
        expected_gen_sig = self._compute_epoch_signature(0, expected_gen_root, expected_gen_digest)
        if not hmac.compare_digest(genesis.signature, expected_gen_sig):
            raise AuditIntegrityError("Invalid cryptographic HMAC signature on Genesis epoch.")

        # 2. Verify all subsequent epochs in strict monotonic succession
        for i in range(1, len(self._epochs)):
            prev_ep = self._epochs[i - 1]
            curr_ep = self._epochs[i]

            if curr_ep.epoch != prev_ep.epoch + 1:
                raise AuditIntegrityError(
                    f"Epoch discontinuity detected: epoch {curr_ep.epoch} follows {prev_ep.epoch}."
                )

            if not hmac.compare_digest(curr_ep.prev_chain_digest, prev_ep.chain_digest):
                raise AuditIntegrityError(
                    f"Chain digest link broken between epoch {prev_ep.epoch} and {curr_ep.epoch}."
                )

            expected_digest = self._compute_chain_digest(
                prev_digest=curr_ep.prev_chain_digest,
                epoch=curr_ep.epoch,
                root_hash=curr_ep.root_hash,
                tenant_id=curr_ep.tenant_id,
            )
            if not hmac.compare_digest(curr_ep.chain_digest, expected_digest):
                raise AuditIntegrityError(
                    f"Chain digest mismatch at epoch {curr_ep.epoch}. History rewriting detected!"
                )

            expected_sig = self._compute_epoch_signature(
                curr_ep.epoch, curr_ep.root_hash, curr_ep.chain_digest
            )
            if not hmac.compare_digest(curr_ep.signature, expected_sig):
                raise AuditIntegrityError(
                    f"Invalid cryptographic HMAC signature at epoch {curr_ep.epoch}."
                )

    def append_root(
        self,
        root_hash: str,
        tenant_id: str = "default-tenant",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> RootEpoch:
        """
        Appends a newly committed Merkle root into the append-only epoch chain.
        Persists immediately to durable storage.
        """
        if not root_hash:
            raise ValueError("root_hash cannot be empty")

        with self._lock:
            next_epoch_num = len(self._epochs)
            prev_digest = self._epochs[-1].chain_digest

            new_chain_digest = self._compute_chain_digest(
                prev_digest=prev_digest,
                epoch=next_epoch_num,
                root_hash=root_hash,
                tenant_id=tenant_id,
            )
            sig = self._compute_epoch_signature(next_epoch_num, root_hash, new_chain_digest)

            epoch_record = RootEpoch(
                epoch=next_epoch_num,
                root_hash=root_hash,
                prev_chain_digest=prev_digest,
                chain_digest=new_chain_digest,
                tenant_id=tenant_id,
                anchored_at=datetime.now(timezone.utc).isoformat(),
                signature=sig,
                metadata=metadata or {},
            )

            self._epochs.append(epoch_record)
            self._epochs_by_num[epoch_record.epoch] = epoch_record

            # Persist to durable storage
            if self._storage_path != ":memory:":
                dir_name = os.path.dirname(self._storage_path)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                with open(self._storage_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(epoch_record.to_dict()) + "\n")
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except OSError:
                        pass

            return epoch_record

    def issue_receipt(
        self,
        epoch: int,
        leaf_hash: str,
        audit_path: Union[List[Tuple[str, str]], List[List[str]]],
        tenant_id: str = "default-tenant",
    ) -> MerkleAuditReceipt:
        """
        Issues an authentic MerkleAuditReceipt for a leaf verified against an anchored epoch.
        """
        with self._lock:
            if epoch not in self._epochs_by_num:
                raise KeyError(f"Epoch {epoch} does not exist in audit anchor ledger.")

            anchored_epoch = self._epochs_by_num[epoch]
            target_root = anchored_epoch.root_hash

            # Reconstruct and validate Merkle folding
            parsed_path: List[Tuple[str, str]] = []
            current_hash = leaf_hash
            for step in audit_path:
                direction, sibling = str(step[0]), str(step[1])
                parsed_path.append((direction, sibling))
                if direction == "left":
                    current_hash = sha256_digest(sibling + current_hash)
                else:
                    current_hash = sha256_digest(current_hash + sibling)

            if not hmac.compare_digest(current_hash, target_root):
                raise ValueError(
                    f"Supplied Merkle audit path folds to '{current_hash}', which does not match epoch {epoch} root '{target_root}'."
                )

            receipt_id = f"rcpt_{uuid.uuid4().hex[:12]}"
            sig = self._compute_receipt_signature(
                receipt_id=receipt_id,
                epoch=epoch,
                tenant_id=tenant_id,
                leaf_hash=leaf_hash,
                root_hash=target_root,
                chain_digest=anchored_epoch.chain_digest,
            )

            return MerkleAuditReceipt(
                receipt_id=receipt_id,
                epoch=epoch,
                tenant_id=tenant_id,
                leaf_hash=leaf_hash,
                audit_path=parsed_path,
                root_hash=target_root,
                chain_digest=anchored_epoch.chain_digest,
                signature=sig,
                issued_at=datetime.now(timezone.utc).isoformat(),
            )

    def verify_receipt(
        self,
        receipt: Union[MerkleAuditReceipt, Dict[str, Any]],
    ) -> VerificationResult:
        """
        Independently verifies a MerkleAuditReceipt against the durable epoch ledger.
        """
        if isinstance(receipt, dict):
            rcpt = MerkleAuditReceipt.from_dict(receipt)
        else:
            rcpt = receipt

        with self._lock:
            # 1. Merkle path folding
            current_hash = rcpt.leaf_hash
            for direction, sibling in rcpt.audit_path:
                if direction == "left":
                    current_hash = sha256_digest(sibling + current_hash)
                else:
                    current_hash = sha256_digest(current_hash + sibling)

            if not hmac.compare_digest(current_hash, rcpt.root_hash):
                return VerificationResult(
                    valid=False,
                    epoch=rcpt.epoch,
                    leaf_hash=rcpt.leaf_hash,
                    root_hash=rcpt.root_hash,
                    reason=f"Merkle path reconstruction resulted in '{current_hash}', mismatching receipt root '{rcpt.root_hash}'.",
                )

            # 2. Check epoch existence in anchored history
            if rcpt.epoch not in self._epochs_by_num:
                return VerificationResult(
                    valid=False,
                    epoch=rcpt.epoch,
                    leaf_hash=rcpt.leaf_hash,
                    root_hash=rcpt.root_hash,
                    reason=f"Epoch {rcpt.epoch} not found in durable audit anchor.",
                )

            anchored_epoch = self._epochs_by_num[rcpt.epoch]

            # 3. Check anchored root
            if not hmac.compare_digest(rcpt.root_hash, anchored_epoch.root_hash):
                return VerificationResult(
                    valid=False,
                    epoch=rcpt.epoch,
                    leaf_hash=rcpt.leaf_hash,
                    root_hash=rcpt.root_hash,
                    reason=f"Receipt root_hash does not match anchored root for epoch {rcpt.epoch}.",
                )

            # 4. Check chain digest continuity
            if not hmac.compare_digest(rcpt.chain_digest, anchored_epoch.chain_digest):
                return VerificationResult(
                    valid=False,
                    epoch=rcpt.epoch,
                    leaf_hash=rcpt.leaf_hash,
                    root_hash=rcpt.root_hash,
                    reason=f"Receipt chain_digest does not match anchored chain_digest for epoch {rcpt.epoch}.",
                )

            # 5. Check receipt HMAC signature
            expected_sig = self._compute_receipt_signature(
                receipt_id=rcpt.receipt_id,
                epoch=rcpt.epoch,
                tenant_id=rcpt.tenant_id,
                leaf_hash=rcpt.leaf_hash,
                root_hash=rcpt.root_hash,
                chain_digest=rcpt.chain_digest,
            )
            if not hmac.compare_digest(rcpt.signature, expected_sig):
                return VerificationResult(
                    valid=False,
                    epoch=rcpt.epoch,
                    leaf_hash=rcpt.leaf_hash,
                    root_hash=rcpt.root_hash,
                    reason="Invalid cryptographic HMAC receipt signature.",
                )

            return VerificationResult(
                valid=True,
                epoch=rcpt.epoch,
                leaf_hash=rcpt.leaf_hash,
                root_hash=rcpt.root_hash,
                reason=None,
            )

    def audit_chain(self, since_epoch: int = 0) -> Dict[str, Any]:
        """
        Performs an end-to-end audit check of the entire chain from Genesis or since_epoch.
        """
        with self._lock:
            errors: List[str] = []
            try:
                self._verify_chain_integrity()
                is_valid = True
            except AuditIntegrityError as err:
                is_valid = False
                errors.append(str(err))

            return {
                "valid": is_valid,
                "total_epochs": len(self._epochs),
                "since_epoch": since_epoch,
                "genesis_hash": self._epochs[0].root_hash if self._epochs else "",
                "latest_epoch": self.latest_epoch,
                "latest_digest": self.latest_digest,
                "errors": errors,
            }

    def status(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Returns diagnostic status of the durable audit anchor."""
        with self._lock:
            latest = self._epochs[-1] if self._epochs else None
            return {
                "status": "ANCHORED",
                "total_epochs": len(self._epochs),
                "latest_epoch": latest.epoch if latest else 0,
                "latest_digest": latest.chain_digest if latest else "",
                "latest_root": latest.root_hash if latest else "",
                "genesis_hash": self._epochs[0].root_hash if self._epochs else "",
                "storage_path": self._storage_path,
                "tenant_id": tenant_id or "all",
            }

    def reset(self, clear_storage: bool = False) -> None:
        """Resets anchor state and re-initializes Genesis."""
        with self._lock:
            self._epochs.clear()
            self._epochs_by_num.clear()
            if clear_storage and self._storage_path != ":memory:" and os.path.exists(self._storage_path):
                try:
                    os.remove(self._storage_path)
                except OSError:
                    pass
            self._load_or_initialize()


# Global singleton instance
global_audit_anchor = AuditAnchor()
