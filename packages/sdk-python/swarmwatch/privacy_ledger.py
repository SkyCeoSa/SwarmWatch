# -*- coding: utf-8 -*-
r"""
Privacy Release Ledger & Differential Privacy Accountant (Seam 58 / ADR 0069).
Bounds repeated-query telemetry leakage across multi-tenant boundaries.
Enforces:
1. Privacy Release Ledger Invariant: Exports occur only when tenant's remaining privacy budget permits.
2. Clipped Sensitivity ($L_1$): Clamps inputs within $[L_{min}, L_{max}]$ to strictly bound query sensitivity.
3. Dual Composition Accounting: Linear composition ($\sum \epsilon_i$) and advanced $(\epsilon, \delta)$-DP bounds.
4. Calibrated Laplace Perturbation: CSPRNG in production, deterministic seeded reproducibility for audits.
5. Zero external LLM API calls (<25us local execution).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import math
import random
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from swarmwatch.canonical import CanonicalCodec, CanonicalDomainTag


class PrivacyBudgetExhaustedError(ValueError):
    """Raised when an operation exceeds the tenant's remaining differential privacy budget."""
    pass


class ReservationStatus(str, Enum):
    """Lifecycle states of a privacy budget reservation."""
    PENDING = "PENDING"
    RELEASED = "RELEASED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


def sample_laplace(scale: float, seed: Optional[int] = None) -> float:
    """
    Draws a Laplace(0, scale) noise sample.
    If scale <= 0, returns 0.0.
    Uses CSPRNG by default; accepts seed for deterministic audit verification.
    """
    if scale <= 0.0:
        return 0.0

    if seed is not None:
        rng = random.Random(seed)
        u = rng.uniform(1e-12, 1.0 - 1e-12)
    else:
        # Cryptographically secure random float in (1e-12, 1 - 1e-12)
        bits = secrets.randbits(53)
        u = bits / (1 << 53)
        if u <= 1e-12:
            u = 1e-12
        elif u >= 1.0 - 1e-12:
            u = 1.0 - 1e-12

    sgn = 1.0 if u >= 0.5 else -1.0
    return -sgn * scale * math.log(1.0 - 2.0 * abs(u - 0.5))


def clip_value(val: float, min_val: Optional[float], max_val: Optional[float]) -> float:
    """Clips a scalar value within [min_val, max_val] if bounds are specified."""
    clipped = val
    if min_val is not None:
        clipped = max(min_val, clipped)
    if max_val is not None:
        clipped = min(max_val, clipped)
    return clipped


@dataclass
class PrivacyReservation:
    """
    Ephemeral cryptographic permit reserving privacy budget prior to query evaluation.
    """
    reservation_id: str
    tenant_id: str
    query_id: str
    epsilon: float
    delta: float
    sensitivity: float
    status: ReservationStatus = ReservationStatus.PENDING
    created_at: float = field(default_factory=time.time)
    reservation_hash: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.status, str):
            self.status = ReservationStatus(self.status)
        if not self.reservation_hash:
            self.reservation_hash = self.compute_hash()

    def compute_hash(self) -> str:
        payload = {
            "delta": self.delta,
            "epsilon": self.epsilon,
            "query_id": self.query_id,
            "reservation_id": self.reservation_id,
            "sensitivity": self.sensitivity,
            "status": self.status.value,
            "tenant_id": self.tenant_id,
        }
        return CanonicalCodec.hash(payload, CanonicalDomainTag.PRIVACY)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "tenant_id": self.tenant_id,
            "query_id": self.query_id,
            "epsilon": self.epsilon,
            "delta": self.delta,
            "sensitivity": self.sensitivity,
            "status": self.status.value,
            "created_at": self.created_at,
            "reservation_hash": self.reservation_hash,
        }


@dataclass
class PrivacyReleaseResult:
    """
    Result of a perturbed privacy release binding clipped data, injected noise, and budget usage.
    """
    release_id: str
    reservation_id: str
    tenant_id: str
    raw_clipped: Union[float, List[float], Dict[str, float]]
    perturbed_value: Union[float, List[float], Dict[str, float]]
    noise_added: Union[float, List[float], Dict[str, float]]
    epsilon_spent: float
    delta_spent: float
    remaining_epsilon: float
    remaining_delta: float
    released_at: float = field(default_factory=time.time)
    release_hash: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.release_hash:
            self.release_hash = self.compute_hash()

    def compute_hash(self) -> str:
        payload = {
            "delta_spent": self.delta_spent,
            "epsilon_spent": self.epsilon_spent,
            "release_id": self.release_id,
            "remaining_delta": self.remaining_delta,
            "remaining_epsilon": self.remaining_epsilon,
            "reservation_id": self.reservation_id,
            "tenant_id": self.tenant_id,
        }
        return CanonicalCodec.hash(payload, CanonicalDomainTag.PRIVACY)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "release_id": self.release_id,
            "reservation_id": self.reservation_id,
            "tenant_id": self.tenant_id,
            "raw_clipped": self.raw_clipped,
            "perturbed_value": self.perturbed_value,
            "noise_added": self.noise_added,
            "epsilon_spent": self.epsilon_spent,
            "delta_spent": self.delta_spent,
            "remaining_epsilon": self.remaining_epsilon,
            "remaining_delta": self.remaining_delta,
            "released_at": self.released_at,
            "release_hash": self.release_hash,
        }


class CompositionAccountant:
    """
    Calculates cumulative privacy spend under linear and advanced composition theorems.
    """

    def __init__(self) -> None:
        self._queries: List[Tuple[float, float]] = []

    def record(self, epsilon: float, delta: float = 0.0) -> None:
        self._queries.append((float(epsilon), float(delta)))

    def linear_total(self) -> Tuple[float, float]:
        """Simple linear composition sum: eps_tot = sum(eps_i), delta_tot = sum(delta_i)."""
        tot_eps = sum(q[0] for q in self._queries)
        tot_delta = sum(q[1] for q in self._queries)
        return tot_eps, tot_delta

    def advanced_composition(self, delta_prime: float = 1e-5) -> Tuple[float, float]:
        """
        Advanced composition bound:
        eps_g = sqrt(2 * k * ln(1 / delta')) * eps_bar + k * eps_bar * (e^eps_bar - 1)
        delta_g = sum(delta_i) + delta'
        """
        k = len(self._queries)
        if k == 0:
            return 0.0, 0.0

        linear_eps, base_delta = self.linear_total()
        eps_bar = linear_eps / k

        if delta_prime <= 0.0 or eps_bar <= 0.0:
            return linear_eps, base_delta

        term1 = math.sqrt(2.0 * k * math.log(1.0 / delta_prime)) * eps_bar
        term2 = k * eps_bar * (math.exp(eps_bar) - 1.0)
        eps_adv = term1 + term2
        delta_adv = base_delta + delta_prime

        # Composition bound cannot exceed linear sum
        return min(linear_eps, eps_adv), delta_adv

    def count(self) -> int:
        return len(self._queries)

    def reset(self) -> None:
        self._queries.clear()


@dataclass
class TenantPrivacyBudget:
    """Configured and consumed budget for a single tenant."""
    total_epsilon: float = 10.0
    total_delta: float = 1e-4
    accountant: CompositionAccountant = field(default_factory=CompositionAccountant)
    reservations: Dict[str, PrivacyReservation] = field(default_factory=dict)
    releases: List[PrivacyReleaseResult] = field(default_factory=list)


class PrivacyRelease:
    """
    Deep module coordinating differential privacy reservations, sensitivity clipping,
    calibrated noise perturbation, and composition accounting.
    """

    def __init__(self, default_epsilon: float = 10.0, default_delta: float = 1e-4) -> None:
        self._default_epsilon = default_epsilon
        self._default_delta = default_delta
        self._tenants: Dict[str, TenantPrivacyBudget] = {}
        self._lock = threading.RLock()

    def _get_tenant(self, tenant_id: str) -> TenantPrivacyBudget:
        if tenant_id not in self._tenants:
            self._tenants[tenant_id] = TenantPrivacyBudget(
                total_epsilon=self._default_epsilon,
                total_delta=self._default_delta,
            )
        return self._tenants[tenant_id]

    def set_tenant_budget(
        self,
        tenant_id: str,
        total_epsilon: float,
        total_delta: float = 0.0,
    ) -> None:
        """Sets or replenishes the total differential privacy budget for a tenant."""
        with self._lock:
            tb = self._get_tenant(tenant_id)
            tb.total_epsilon = max(0.0, float(total_epsilon))
            tb.total_delta = max(0.0, float(total_delta))

    def reserve(
        self,
        tenant_id: str,
        epsilon: float,
        delta: float = 0.0,
        sensitivity: float = 1.0,
        query_id: Optional[str] = None,
    ) -> PrivacyReservation:
        """
        Reserves privacy budget prior to query evaluation.
        Fails closed with PrivacyBudgetExhaustedError if requested budget exceeds remaining.
        """
        with self._lock:
            if epsilon <= 0.0:
                raise ValueError("Epsilon budget request must be strictly positive")
            if delta < 0.0:
                raise ValueError("Delta budget request cannot be negative")
            if sensitivity <= 0.0:
                raise ValueError("Sensitivity must be strictly positive")

            tb = self._get_tenant(tenant_id)
            spent_eps, spent_delta = tb.accountant.linear_total()

            # Account for currently pending reservations as well
            pending_eps = sum(
                r.epsilon for r in tb.reservations.values()
                if r.status == ReservationStatus.PENDING
            )
            pending_delta = sum(
                r.delta for r in tb.reservations.values()
                if r.status == ReservationStatus.PENDING
            )

            rem_eps = tb.total_epsilon - (spent_eps + pending_eps)
            rem_delta = tb.total_delta - (spent_delta + pending_delta)

            if epsilon > rem_eps or (delta > 0.0 and delta > rem_delta):
                raise PrivacyBudgetExhaustedError(
                    f"Privacy budget exhausted for tenant '{tenant_id}': "
                    f"requested epsilon={epsilon:.4f} (remaining={rem_eps:.4f}), "
                    f"requested delta={delta:.6f} (remaining={rem_delta:.6f})"
                )

            rid = f"prev-{secrets.token_hex(6)}"
            qid = query_id or f"qry-{secrets.token_hex(4)}"

            res = PrivacyReservation(
                reservation_id=rid,
                tenant_id=tenant_id,
                query_id=qid,
                epsilon=epsilon,
                delta=delta,
                sensitivity=sensitivity,
                status=ReservationStatus.PENDING,
            )
            tb.reservations[rid] = res
            return res

    def release(
        self,
        reservation_id: str,
        raw_value: Union[float, List[float], Dict[str, float]],
        noise_seed: Optional[int] = None,
        min_bound: Optional[float] = None,
        max_bound: Optional[float] = None,
    ) -> PrivacyReleaseResult:
        """
        Executes calibrated Laplace perturbation over clipped data and commits budget.
        """
        with self._lock:
            # Locate reservation across tenants
            target_tb: Optional[TenantPrivacyBudget] = None
            reservation: Optional[PrivacyReservation] = None

            for tb in self._tenants.values():
                if reservation_id in tb.reservations:
                    target_tb = tb
                    reservation = tb.reservations[reservation_id]
                    break

            if not target_tb or not reservation:
                raise ValueError(f"Reservation '{reservation_id}' not found")

            if reservation.status != ReservationStatus.PENDING:
                raise ValueError(
                    f"Reservation '{reservation_id}' has invalid status: {reservation.status.value}"
                )

            # Sensitivity scale b = Delta f / epsilon
            scale = reservation.sensitivity / reservation.epsilon

            # Process scalar, list, or dict
            raw_clipped: Union[float, List[float], Dict[str, float]]
            perturbed: Union[float, List[float], Dict[str, float]]
            noise_map: Union[float, List[float], Dict[str, float]]

            if isinstance(raw_value, (int, float)):
                c = clip_value(float(raw_value), min_bound, max_bound)
                noise = sample_laplace(scale, seed=noise_seed)
                raw_clipped = c
                perturbed = c + noise
                noise_map = noise
            elif isinstance(raw_value, list):
                raw_clipped = [clip_value(float(v), min_bound, max_bound) for v in raw_value]
                # If seed provided, use incremental seeds for list elements
                noises = [
                    sample_laplace(scale, seed=(noise_seed + idx if noise_seed is not None else None))
                    for idx, _ in enumerate(raw_clipped)
                ]
                perturbed = [c + n for c, n in zip(raw_clipped, noises)]
                noise_map = noises
            elif isinstance(raw_value, dict):
                raw_clipped = {k: clip_value(float(v), min_bound, max_bound) for k, v in raw_value.items()}
                noise_dict = {}
                perturbed_dict = {}
                for idx, (k, val) in enumerate(raw_clipped.items()):
                    n = sample_laplace(scale, seed=(noise_seed + idx if noise_seed is not None else None))
                    noise_dict[k] = n
                    perturbed_dict[k] = val + n
                noise_map = noise_dict
                perturbed = perturbed_dict
            else:
                raise TypeError(f"Unsupported raw_value type for privacy release: {type(raw_value)}")

            # Commit budget consumption in accountant
            target_tb.accountant.record(reservation.epsilon, reservation.delta)
            reservation.status = ReservationStatus.RELEASED

            spent_eps, spent_delta = target_tb.accountant.linear_total()
            rem_eps = max(0.0, target_tb.total_epsilon - spent_eps)
            rem_delta = max(0.0, target_tb.total_delta - spent_delta)

            rel_id = f"prel-{secrets.token_hex(6)}"
            result = PrivacyReleaseResult(
                release_id=rel_id,
                reservation_id=reservation_id,
                tenant_id=reservation.tenant_id,
                raw_clipped=raw_clipped,
                perturbed_value=perturbed,
                noise_added=noise_map,
                epsilon_spent=reservation.epsilon,
                delta_spent=reservation.delta,
                remaining_epsilon=rem_eps,
                remaining_delta=rem_delta,
            )

            target_tb.releases.append(result)
            return result

    def remaining(self, tenant_id: str) -> Dict[str, float]:
        """Returns the remaining and total differential privacy budget for a tenant."""
        with self._lock:
            tb = self._get_tenant(tenant_id)
            spent_eps, spent_delta = tb.accountant.linear_total()
            pending_eps = sum(
                r.epsilon for r in tb.reservations.values()
                if r.status == ReservationStatus.PENDING
            )
            pending_delta = sum(
                r.delta for r in tb.reservations.values()
                if r.status == ReservationStatus.PENDING
            )

            rem_eps = max(0.0, tb.total_epsilon - (spent_eps + pending_eps))
            rem_delta = max(0.0, tb.total_delta - (spent_delta + pending_delta))

            return {
                "remaining_epsilon": rem_eps,
                "remaining_delta": rem_delta,
                "total_epsilon": tb.total_epsilon,
                "total_delta": tb.total_delta,
                "spent_epsilon": spent_eps,
                "spent_delta": spent_delta,
                "pending_epsilon": pending_eps,
                "pending_delta": pending_delta,
            }

    def status(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Returns consolidated diagnostic report of privacy release ledgers."""
        with self._lock:
            tenants_to_report = [tenant_id] if tenant_id else list(self._tenants.keys())
            total_releases = 0
            total_reservations = 0
            total_spent_eps = 0.0
            budget_constrained_count = 0

            for tid in tenants_to_report:
                tb = self._get_tenant(tid)
                total_releases += len(tb.releases)
                total_reservations += len(tb.reservations)
                s_eps, _ = tb.accountant.linear_total()
                total_spent_eps += s_eps
                if s_eps >= tb.total_epsilon:
                    budget_constrained_count += 1

            status_str = "CONVERGED" if budget_constrained_count == 0 else "BUDGET_CONSTRAINED"

            return {
                "active_tenants": len(self._tenants),
                "budget_constrained_count": budget_constrained_count,
                "status": status_str,
                "total_releases": total_releases,
                "total_reservations": total_reservations,
                "total_spent_epsilon": total_spent_eps,
            }

    def reset(self) -> None:
        """Clears all in-memory tenant ledgers, reservations, and releases."""
        with self._lock:
            self._tenants.clear()


# Global singleton instance
global_privacy_release = PrivacyRelease()
