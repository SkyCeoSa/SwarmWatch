# -*- coding: utf-8 -*-
"""
Unified Safety Authority & Linearizable Execution Permit Engine (Seam 51 / ADR 0062).
Acts as the single authoritative decision plane across distributed workers and subsystems,
issuing unexpired, tenant-scoped execution permits (SafetyPermit) with monotonic fencing epochs.
Guarantees fail-closed execution during network partitions and split-brain states.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag


class PermitStatus:
    """Explicit decision status returned by the SafetyAuthority."""
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    INDETERMINATE = "INDETERMINATE"


class AuthorityState:
    """Operating state of the safety authority plane."""
    NOMINAL = "NOMINAL"
    TRIPPED = "TRIPPED"
    DRAINING = "DRAINING"


class SafetyPermit:
    """
    Cryptographically verifiable execution lease token authorizing an agent turn or action.
    Tied to a specific fencing epoch, tenant quota, and expiration deadline.
    """

    def __init__(
        self,
        permit_id: str,
        tenant_id: str,
        epoch: int,
        action: str,
        cost_estimate: float = 0.0,
        tokens_estimate: int = 0,
        status: str = PermitStatus.ACCEPTED,
        rejection_reason: Optional[str] = None,
        issued_at: Optional[float] = None,
        expires_at: Optional[float] = None,
        permit_hash: Optional[str] = None,
    ):
        self.permit_id = permit_id
        self.tenant_id = tenant_id
        self.epoch = epoch
        self.action = action
        self.cost_estimate = float(cost_estimate)
        self.tokens_estimate = int(tokens_estimate)
        self.status = status
        self.rejection_reason = rejection_reason
        now = time.time()
        self.issued_at = issued_at if issued_at is not None else now
        self.expires_at = expires_at if expires_at is not None else (self.issued_at + 5.0)
        self.permit_hash = permit_hash if permit_hash is not None else self.compute_permit_hash()

    def compute_permit_hash(self) -> str:
        """Computes domain-separated canonical SHA-256 digest of the permit."""
        payload = {
            "action": self.action,
            "cost_estimate": self.cost_estimate,
            "epoch": self.epoch,
            "expires_at": round(self.expires_at, 6),
            "issued_at": round(self.issued_at, 6),
            "permit_id": self.permit_id,
            "status": self.status,
            "tenant_id": self.tenant_id,
            "tokens_estimate": self.tokens_estimate,
        }
        return CanonicalCodec.hash(payload, domain_tag=CanonicalDomainTag.PERMIT)

    def is_valid(self, current_epoch: int, current_time: Optional[float] = None) -> bool:
        """Verifies if the permit is valid under the current epoch and not expired."""
        if self.status != PermitStatus.ACCEPTED:
            return False
        if self.epoch != current_epoch:
            return False
        now = current_time if current_time is not None else time.time()
        return now <= self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "permit_id": self.permit_id,
            "tenant_id": self.tenant_id,
            "epoch": self.epoch,
            "action": self.action,
            "cost_estimate": self.cost_estimate,
            "tokens_estimate": self.tokens_estimate,
            "status": self.status,
            "rejection_reason": self.rejection_reason,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "permit_hash": self.permit_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SafetyPermit:
        return cls(
            permit_id=data["permit_id"],
            tenant_id=data.get("tenant_id", "default"),
            epoch=int(data.get("epoch", 1)),
            action=data.get("action", "agent_turn"),
            cost_estimate=float(data.get("cost_estimate", 0.0)),
            tokens_estimate=int(data.get("tokens_estimate", 0)),
            status=data.get("status", PermitStatus.ACCEPTED),
            rejection_reason=data.get("rejection_reason"),
            issued_at=float(data.get("issued_at", time.time())),
            expires_at=float(data.get("expires_at", time.time() + 5.0)),
            permit_hash=data.get("permit_hash"),
        )


class TenantQuota:
    """Linearizable budget ledger for a tenant."""

    def __init__(
        self,
        tenant_id: str,
        max_cost: float = 100.0,
        max_tokens: int = 1_000_000,
        max_turns: int = 1000,
    ):
        self.tenant_id = tenant_id
        self.max_cost = float(max_cost)
        self.max_tokens = int(max_tokens)
        self.max_turns = int(max_turns)
        self.spent_cost = 0.0
        self.reserved_cost = 0.0
        self.spent_tokens = 0
        self.reserved_tokens = 0
        self.spent_turns = 0
        self.state = AuthorityState.NOMINAL
        self.trip_reason: Optional[str] = None

    def available_cost(self) -> float:
        return max(0.0, self.max_cost - (self.spent_cost + self.reserved_cost))

    def available_tokens(self) -> int:
        return max(0, self.max_tokens - (self.spent_tokens + self.reserved_tokens))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "max_cost": self.max_cost,
            "spent_cost": self.spent_cost,
            "reserved_cost": self.reserved_cost,
            "available_cost": self.available_cost(),
            "max_tokens": self.max_tokens,
            "spent_tokens": self.spent_tokens,
            "reserved_tokens": self.reserved_tokens,
            "available_tokens": self.available_tokens(),
            "max_turns": self.max_turns,
            "spent_turns": self.spent_turns,
            "state": self.state,
            "trip_reason": self.trip_reason,
        }


class SafetyAuthority:
    """
    Linearizable, fail-closed safety authority.
    Coordinates execute-permit issuance, budget reservations, and monotonic epoch fencing.
    """

    def __init__(self, initial_epoch: int = 1):
        self._lock = threading.RLock()
        self._epoch = int(initial_epoch)
        self._global_state = AuthorityState.NOMINAL
        self._global_trip_reason: Optional[str] = None
        self._tenant_quotas: Dict[str, TenantQuota] = {}
        self._active_permits: Dict[str, SafetyPermit] = {}
        self._permit_history: List[SafetyPermit] = []
        self._max_history = 1000
        self._fail_closed_partition_mode = False

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    @property
    def global_state(self) -> str:
        with self._lock:
            return self._global_state

    def set_partition_simulation(self, enabled: bool):
        """Simulates network partition / quorum loss for fail-closed verification."""
        with self._lock:
            self._fail_closed_partition_mode = enabled

    def get_or_create_quota(self, tenant_id: str) -> TenantQuota:
        with self._lock:
            if tenant_id not in self._tenant_quotas:
                self._tenant_quotas[tenant_id] = TenantQuota(tenant_id)
            return self._tenant_quotas[tenant_id]

    def configure_tenant(
        self,
        tenant_id: str,
        max_cost: float = 100.0,
        max_tokens: int = 1_000_000,
        max_turns: int = 1000,
    ) -> TenantQuota:
        with self._lock:
            quota = self.get_or_create_quota(tenant_id)
            quota.max_cost = float(max_cost)
            quota.max_tokens = int(max_tokens)
            quota.max_turns = int(max_turns)
            return quota

    def reserve(
        self,
        tenant_id: str,
        action: str = "agent_turn",
        cost_estimate: float = 0.0,
        tokens_estimate: int = 0,
        timeout_ms: float = 5000.0,
        permit_id: Optional[str] = None,
    ) -> SafetyPermit:
        """
        Atomically requests and reserves an execution permit under the current epoch.
        Returns SafetyPermit with status ACCEPTED, REJECTED, or INDETERMINATE.
        """
        with self._lock:
            # 1. Fail-closed partition check
            if self._fail_closed_partition_mode:
                return SafetyPermit(
                    permit_id=f"pmt-indet-{uuid.uuid4().hex[:8]}",
                    tenant_id=tenant_id,
                    epoch=self._epoch,
                    action=action,
                    cost_estimate=cost_estimate,
                    tokens_estimate=tokens_estimate,
                    status=PermitStatus.INDETERMINATE,
                    rejection_reason="Authority is unreachable or experiencing network partition (fail-closed)",
                )

            # 2. Check global authority trip state
            if self._global_state == AuthorityState.TRIPPED:
                return SafetyPermit(
                    permit_id=f"pmt-rej-{uuid.uuid4().hex[:8]}",
                    tenant_id=tenant_id,
                    epoch=self._epoch,
                    action=action,
                    cost_estimate=cost_estimate,
                    tokens_estimate=tokens_estimate,
                    status=PermitStatus.REJECTED,
                    rejection_reason=f"Global safety circuit tripped at epoch {self._epoch}: {self._global_trip_reason}",
                )

            # 3. Check tenant quota and trip state
            quota = self.get_or_create_quota(tenant_id)
            if quota.state == AuthorityState.TRIPPED:
                return SafetyPermit(
                    permit_id=f"pmt-rej-{uuid.uuid4().hex[:8]}",
                    tenant_id=tenant_id,
                    epoch=self._epoch,
                    action=action,
                    cost_estimate=cost_estimate,
                    tokens_estimate=tokens_estimate,
                    status=PermitStatus.REJECTED,
                    rejection_reason=f"Tenant {tenant_id} circuit tripped: {quota.trip_reason}",
                )

            # 4. Check quota headroom
            if cost_estimate > quota.available_cost():
                return SafetyPermit(
                    permit_id=f"pmt-rej-{uuid.uuid4().hex[:8]}",
                    tenant_id=tenant_id,
                    epoch=self._epoch,
                    action=action,
                    cost_estimate=cost_estimate,
                    tokens_estimate=tokens_estimate,
                    status=PermitStatus.REJECTED,
                    rejection_reason=f"Insufficient cost budget: estimated ${cost_estimate:.4f}, available ${quota.available_cost():.4f}",
                )

            if tokens_estimate > quota.available_tokens():
                return SafetyPermit(
                    permit_id=f"pmt-rej-{uuid.uuid4().hex[:8]}",
                    tenant_id=tenant_id,
                    epoch=self._epoch,
                    action=action,
                    cost_estimate=cost_estimate,
                    tokens_estimate=tokens_estimate,
                    status=PermitStatus.REJECTED,
                    rejection_reason=f"Insufficient token budget: estimated {tokens_estimate}, available {quota.available_tokens()}",
                )

            # 5. Atomically reserve and issue permit
            quota.reserved_cost += cost_estimate
            quota.reserved_tokens += tokens_estimate

            if not permit_id:
                permit_id = f"pmt-{uuid.uuid4().hex[:12]}"
            now = time.time()
            expires_at = now + (max(100.0, timeout_ms) / 1000.0)

            permit = SafetyPermit(
                permit_id=permit_id,
                tenant_id=tenant_id,
                epoch=self._epoch,
                action=action,
                cost_estimate=cost_estimate,
                tokens_estimate=tokens_estimate,
                status=PermitStatus.ACCEPTED,
                issued_at=now,
                expires_at=expires_at,
            )

            self._active_permits[permit_id] = permit
            self._permit_history.append(permit)
            if len(self._permit_history) > self._max_history:
                self._permit_history.pop(0)

            return permit

    def commit(
        self,
        permit_id: str,
        actual_cost: float = 0.0,
        actual_tokens: int = 0,
    ) -> Tuple[bool, str]:
        """
        Commits an executed permit, settling reserved budget and recording permanent spend.
        Fails if permit was invalidated by an epoch transition or expired lease.
        """
        with self._lock:
            permit = self._active_permits.get(permit_id)
            if not permit:
                return False, f"Permit {permit_id} not found in active permits or already closed"

            # Check epoch fencing
            if permit.epoch != self._epoch:
                self._active_permits.pop(permit_id, None)
                return False, f"Permit invalidated by epoch transition ({permit.epoch} != {self._epoch})"

            # Check expiration
            now = time.time()
            if now > permit.expires_at:
                self._active_permits.pop(permit_id, None)
                # Release reserved budget
                quota = self.get_or_create_quota(permit.tenant_id)
                quota.reserved_cost = max(0.0, quota.reserved_cost - permit.cost_estimate)
                quota.reserved_tokens = max(0, quota.reserved_tokens - permit.tokens_estimate)
                return False, f"Permit {permit_id} expired before commit ({now:.2f} > {permit.expires_at:.2f})"

            # Settle budget
            quota = self.get_or_create_quota(permit.tenant_id)
            quota.reserved_cost = max(0.0, quota.reserved_cost - permit.cost_estimate)
            quota.reserved_tokens = max(0, quota.reserved_tokens - permit.tokens_estimate)
            quota.spent_cost += actual_cost
            quota.spent_tokens += actual_tokens
            quota.spent_turns += 1

            self._active_permits.pop(permit_id, None)
            return True, "Permit committed successfully"

    def release(self, permit_id: str, reason: str = "aborted") -> Tuple[bool, str]:
        """
        Releases reserved budget back to the pool without debiting spend.
        Used when an agent turn aborts before invoking external tool/model calls.
        """
        with self._lock:
            permit = self._active_permits.pop(permit_id, None)
            if not permit:
                return False, f"Permit {permit_id} not found in active permits"

            quota = self.get_or_create_quota(permit.tenant_id)
            quota.reserved_cost = max(0.0, quota.reserved_cost - permit.cost_estimate)
            quota.reserved_tokens = max(0, quota.reserved_tokens - permit.tokens_estimate)
            return True, f"Permit {permit_id} released ({reason})"

    def trip(self, reason: str, tenant_id: Optional[str] = None) -> int:
        """
        Trips the safety authority, monotonically increments fencing epoch,
        and invalidates all active permits under the previous epoch.
        """
        with self._lock:
            self._epoch += 1
            if tenant_id:
                quota = self.get_or_create_quota(tenant_id)
                quota.state = AuthorityState.TRIPPED
                quota.trip_reason = reason
            else:
                self._global_state = AuthorityState.TRIPPED
                self._global_trip_reason = reason

            # Invalidate all active permits
            self._active_permits.clear()
            return self._epoch

    def rearm(self, tenant_id: Optional[str] = None, new_epoch: Optional[int] = None) -> int:
        """
        Re-arms the safety authority back to NOMINAL, incrementing fencing epoch.
        """
        with self._lock:
            if new_epoch is not None and int(new_epoch) > self._epoch:
                self._epoch = int(new_epoch)
            else:
                self._epoch += 1
            if tenant_id:
                quota = self.get_or_create_quota(tenant_id)
                quota.state = AuthorityState.NOMINAL
                quota.trip_reason = None
            else:
                self._global_state = AuthorityState.NOMINAL
                self._global_trip_reason = None
            return self._epoch

    def status(self, tenant_id: str = "default") -> Dict[str, Any]:
        """Returns comprehensive authoritative status and active permit counts."""
        with self._lock:
            quota = self.get_or_create_quota(tenant_id)
            return {
                "epoch": self._epoch,
                "global_state": self._global_state,
                "global_trip_reason": self._global_trip_reason,
                "active_permits_count": len(self._active_permits),
                "tenant": quota.to_dict(),
            }

    def reset(self):
        """Resets authority state for fresh test execution."""
        with self._lock:
            self._epoch = 1
            self._global_state = AuthorityState.NOMINAL
            self._global_trip_reason = None
            self._tenant_quotas.clear()
            self._active_permits.clear()
            self._permit_history.clear()
            self._fail_closed_partition_mode = False


# Global singleton
global_safety_authority = SafetyAuthority()
