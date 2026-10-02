# -*- coding: utf-8 -*-
"""
Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66 / ADR 0077).
Implements formal action classification, reversibility lattice join composition,
deterministic reverse-ordered compensation planning, and fail-closed human escalation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
import hashlib
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union
import uuid


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class CompensationError(RuntimeError):
    """Base exception for all compensation engine errors."""
    pass


class IrreversibleActionError(CompensationError):
    """Raised when automated rollback is attempted on an irreversible action without human authorization."""
    pass


class UnregisteredActionError(CompensationError):
    """Raised when an unregistered action type is strictly prohibited in strict validation mode."""
    pass


class EscalationPendingError(CompensationError):
    """Raised when a compensation plan is blocked awaiting human escalation review."""
    pass


# ---------------------------------------------------------------------------
# Reversibility Lattice & Ordering
# ---------------------------------------------------------------------------

class ReversibilityClass(IntEnum):
    """
    Formal action reversibility classification forming a bounded total order (lattice):
    REVERSIBLE (0) < COMPENSATABLE (1) < IRREVERSIBLE (2)
    """
    REVERSIBLE = 0
    COMPENSATABLE = 1
    IRREVERSIBLE = 2

    @classmethod
    def from_str(cls, val: Union[str, ReversibilityClass]) -> "ReversibilityClass":
        if isinstance(val, ReversibilityClass):
            return val
        clean = str(val).upper().strip()
        if clean == "REVERSIBLE":
            return cls.REVERSIBLE
        elif clean == "COMPENSATABLE":
            return cls.COMPENSATABLE
        elif clean == "IRREVERSIBLE":
            return cls.IRREVERSIBLE
        raise ValueError(f"Unknown reversibility class: {val}")

    @classmethod
    def join(cls, classes: Sequence[Union[ReversibilityClass, str]]) -> "ReversibilityClass":
        """
        Computes the lattice join (least upper bound / max) over a sequence of classes.
        If empty, returns REVERSIBLE (neutral element).
        """
        if not classes:
            return cls.REVERSIBLE
        parsed = [cls.from_str(c) for c in classes]
        return max(parsed)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

def sha256_digest(content: Union[str, bytes]) -> str:
    """Computes standard hex SHA-256 digest."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


@dataclass
class ActionExecution:
    """Immutable audit record of an executed action side-effect."""
    action_id: str
    action_type: str
    tenant_id: str
    principal: str
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp_ns: int = field(default_factory=time.time_ns)
    idempotency_key: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_id": self.action_id,
            "action_type": self.action_type,
            "tenant_id": self.tenant_id,
            "principal": self.principal,
            "payload": dict(self.payload),
            "timestamp_ns": self.timestamp_ns,
            "idempotency_key": self.idempotency_key,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ActionExecution":
        return cls(
            action_id=str(data.get("action_id", "")),
            action_type=str(data.get("action_type", "")),
            tenant_id=str(data.get("tenant_id", "")),
            principal=str(data.get("principal", "")),
            payload=dict(data.get("payload", {})),
            timestamp_ns=int(data.get("timestamp_ns", 0)),
            idempotency_key=str(data.get("idempotency_key")) if data.get("idempotency_key") else None,
        )


@dataclass
class ActionContract:
    """Registered semantic reversibility contract for an action type."""
    action_type: str
    reversibility_class: ReversibilityClass
    description: str = ""
    inverse_handler: Optional[Callable[[ActionExecution], bool]] = None
    compensation_handler: Optional[Callable[[ActionExecution], bool]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_type": self.action_type,
            "reversibility_class": self.reversibility_class.name,
            "description": self.description,
            "has_inverse_handler": self.inverse_handler is not None,
            "has_compensation_handler": self.compensation_handler is not None,
        }


@dataclass
class CompositeClassification:
    """Result of lattice join evaluation across a sequence of actions."""
    reversibility_class: ReversibilityClass
    total_actions: int
    reversible_count: int
    compensatable_count: int
    irreversible_count: int
    irreversible_action_ids: List[str]

    @property
    def is_fully_reversible(self) -> bool:
        return self.reversibility_class == ReversibilityClass.REVERSIBLE

    @property
    def is_compensatable(self) -> bool:
        return self.reversibility_class <= ReversibilityClass.COMPENSATABLE

    @property
    def has_irreversible_steps(self) -> bool:
        return self.reversibility_class == ReversibilityClass.IRREVERSIBLE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reversibility_class": self.reversibility_class.name,
            "total_actions": self.total_actions,
            "reversible_count": self.reversible_count,
            "compensatable_count": self.compensatable_count,
            "irreversible_count": self.irreversible_count,
            "irreversible_action_ids": list(self.irreversible_action_ids),
            "is_fully_reversible": self.is_fully_reversible,
            "is_compensatable": self.is_compensatable,
            "has_irreversible_steps": self.has_irreversible_steps,
        }


@dataclass
class CompensationStep:
    """An individual compensation instruction targeting a prior executed action."""
    step_index: int
    target_action_id: str
    action_type: str
    reversibility_class: ReversibilityClass
    handler_type: str  # "inverse" | "compensation" | "human_escalation"
    idempotency_key: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_index": self.step_index,
            "target_action_id": self.target_action_id,
            "action_type": self.action_type,
            "reversibility_class": self.reversibility_class.name,
            "handler_type": self.handler_type,
            "idempotency_key": self.idempotency_key,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompensationStep":
        return cls(
            step_index=int(data.get("step_index", 0)),
            target_action_id=str(data.get("target_action_id", "")),
            action_type=str(data.get("action_type", "")),
            reversibility_class=ReversibilityClass.from_str(data.get("reversibility_class", "IRREVERSIBLE")),
            handler_type=str(data.get("handler_type", "human_escalation")),
            idempotency_key=str(data.get("idempotency_key", "")),
        )


@dataclass
class CompensationPlan:
    """Ordered sequence of inverse and compensation steps in strict reverse execution order."""
    plan_id: str
    composite_class: ReversibilityClass
    steps: List[CompensationStep]
    created_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "composite_class": self.composite_class.name,
            "steps": [s.to_dict() for s in self.steps],
            "created_at_ns": self.created_at_ns,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompensationPlan":
        steps_raw = data.get("steps", [])
        return cls(
            plan_id=str(data.get("plan_id", "")),
            composite_class=ReversibilityClass.from_str(data.get("composite_class", "IRREVERSIBLE")),
            steps=[CompensationStep.from_dict(s) for s in steps_raw],
            created_at_ns=int(data.get("created_at_ns", 0)),
        )


@dataclass
class CompensationResult:
    """Outcome of executing a compensation plan."""
    plan_id: str
    success: bool
    steps_executed: int
    steps_failed: int
    escalations_resolved: int
    errors: List[str] = field(default_factory=list)
    executed_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "success": self.success,
            "steps_executed": self.steps_executed,
            "steps_failed": self.steps_failed,
            "escalations_resolved": self.escalations_resolved,
            "errors": list(self.errors),
            "executed_at_ns": self.executed_at_ns,
        }


@dataclass
class HumanEscalationReceipt:
    """Immutable audit record for an irreversible action requiring human authorization."""
    escalation_id: str
    action_id: str
    action_type: str
    tenant_id: str
    principal: str
    reason: str
    status: str  # "PENDING" | "RESOLVED" | "REJECTED"
    escalation_hash: str
    created_at_ns: int = field(default_factory=time.time_ns)

    @staticmethod
    def compute_hash(
        escalation_id: str,
        action_id: str,
        action_type: str,
        tenant_id: str,
        principal: str,
        reason: str,
    ) -> str:
        seed = f"sw:esc:v1:{escalation_id}:{action_id}:{action_type}:{tenant_id}:{principal}:{reason}"
        return sha256_digest(seed)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "escalation_id": self.escalation_id,
            "action_id": self.action_id,
            "action_type": self.action_type,
            "tenant_id": self.tenant_id,
            "principal": self.principal,
            "reason": self.reason,
            "status": self.status,
            "escalation_hash": self.escalation_hash,
            "created_at_ns": self.created_at_ns,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HumanEscalationReceipt":
        return cls(
            escalation_id=str(data.get("escalation_id", "")),
            action_id=str(data.get("action_id", "")),
            action_type=str(data.get("action_type", "")),
            tenant_id=str(data.get("tenant_id", "")),
            principal=str(data.get("principal", "")),
            reason=str(data.get("reason", "")),
            status=str(data.get("status", "PENDING")),
            escalation_hash=str(data.get("escalation_hash", "")),
            created_at_ns=int(data.get("created_at_ns", 0)),
        )


@dataclass
class EscalationResolution:
    """Cryptographic authorization record resolving a human escalation."""
    resolution_id: str
    escalation_id: str
    authorizer_principal: str
    decision: str  # "APPROVED_BYPASS" | "MANUAL_REMEDIATED" | "ABORTED"
    remediation_notes: str
    resolved_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resolution_id": self.resolution_id,
            "escalation_id": self.escalation_id,
            "authorizer_principal": self.authorizer_principal,
            "decision": self.decision,
            "remediation_notes": self.remediation_notes,
            "resolved_at_ns": self.resolved_at_ns,
        }


@dataclass
class CompensationStatus:
    """Operational diagnostics for the compensation engine."""
    total_registered_contracts: int
    total_plans_generated: int
    total_compensations_executed: int
    total_escalations_pending: int
    total_escalations_resolved: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_registered_contracts": self.total_registered_contracts,
            "total_plans_generated": self.total_plans_generated,
            "total_compensations_executed": self.total_compensations_executed,
            "total_escalations_pending": self.total_escalations_pending,
            "total_escalations_resolved": self.total_escalations_resolved,
        }


# ---------------------------------------------------------------------------
# Core Deep Module: CompensationEngine
# ---------------------------------------------------------------------------

class CompensationEngine:
    """
    Governs effect-aware action classification, reversibility lattice join composition,
    reverse-ordered compensation planning, and fail-closed human escalation.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._contracts: Dict[str, ActionContract] = {}
        self._actions_by_id: Dict[str, ActionExecution] = {}
        self._escalations: Dict[str, HumanEscalationReceipt] = {}
        self._resolutions: Dict[str, EscalationResolution] = {}

        self._total_plans_generated = 0
        self._total_compensations_executed = 0

    def register_contract(self, contract: ActionContract) -> None:
        """Registers an action contract defining its reversibility classification and handlers."""
        with self._lock:
            self._contracts[contract.action_type] = contract

    def get_contract(self, action_type: str) -> Optional[ActionContract]:
        """Returns the registered contract for an action type if present."""
        with self._lock:
            return self._contracts.get(action_type)

    def classify(self, actions: Sequence[ActionExecution]) -> CompositeClassification:
        """
        Classifies a sequence of action executions using the reversibility lattice join.
        Unregistered action types strictly default to IRREVERSIBLE (fail-closed).
        """
        with self._lock:
            rev_count = 0
            comp_count = 0
            irr_count = 0
            irreversible_ids: List[str] = []
            classes: List[ReversibilityClass] = []

            for a in actions:
                self._actions_by_id[a.action_id] = a
                contract = self._contracts.get(a.action_type)
                if contract is None:
                    # Fail-closed default: unregistered actions are IRREVERSIBLE
                    cls_val = ReversibilityClass.IRREVERSIBLE
                else:
                    cls_val = contract.reversibility_class

                classes.append(cls_val)
                if cls_val == ReversibilityClass.REVERSIBLE:
                    rev_count += 1
                elif cls_val == ReversibilityClass.COMPENSATABLE:
                    comp_count += 1
                else:
                    irr_count += 1
                    irreversible_ids.append(a.action_id)

            composite = ReversibilityClass.join(classes)

            return CompositeClassification(
                reversibility_class=composite,
                total_actions=len(actions),
                reversible_count=rev_count,
                compensatable_count=comp_count,
                irreversible_count=irr_count,
                irreversible_action_ids=irreversible_ids,
            )

    def build_compensation_plan(self, actions: Sequence[ActionExecution]) -> CompensationPlan:
        """
        Builds a step-by-step compensation plan reversing the executed action sequence.
        Steps are ordered strictly in REVERSE order: (A_k, A_{k-1}, ..., A_1).
        """
        with self._lock:
            classification = self.classify(actions)
            plan_id = str(uuid.uuid4())
            steps: List[CompensationStep] = []

            # Reverse the sequence for compensation
            reversed_actions = list(reversed(actions))

            for idx, a in enumerate(reversed_actions):
                contract = self._contracts.get(a.action_type)
                r_class = contract.reversibility_class if contract else ReversibilityClass.IRREVERSIBLE

                if r_class == ReversibilityClass.REVERSIBLE:
                    h_type = "inverse"
                elif r_class == ReversibilityClass.COMPENSATABLE:
                    h_type = "compensation"
                else:
                    h_type = "human_escalation"

                step_key = sha256_digest(f"sw:comp:{plan_id}:{a.action_id}:{idx}")
                steps.append(CompensationStep(
                    step_index=idx,
                    target_action_id=a.action_id,
                    action_type=a.action_type,
                    reversibility_class=r_class,
                    handler_type=h_type,
                    idempotency_key=step_key,
                ))

            self._total_plans_generated += 1

            return CompensationPlan(
                plan_id=plan_id,
                composite_class=classification.reversibility_class,
                steps=steps,
            )

    def execute_compensation(
        self,
        plan: CompensationPlan,
        allow_authorized_escalations: bool = False,
    ) -> CompensationResult:
        """
        Executes inverse and compensation handlers for a plan in order.
        Strictly halts fail-closed on unapproved irreversible actions with IrreversibleActionError.
        """
        with self._lock:
            # 1. Immediate fail-closed check if plan is irreversible and authorized escalations not allowed
            if plan.composite_class == ReversibilityClass.IRREVERSIBLE and not allow_authorized_escalations:
                raise IrreversibleActionError(
                    f"Automated rollback prohibited for irreversible action in plan '{plan.plan_id}'"
                )

            executed = 0
            failed = 0
            escalations_resolved = 0
            errors: List[str] = []

            for step in plan.steps:
                action = self._actions_by_id.get(step.target_action_id)
                if action is None:
                    action = ActionExecution(
                        action_id=step.target_action_id,
                        action_type=step.action_type,
                        tenant_id="unknown",
                        principal="unknown",
                    )

                if step.reversibility_class == ReversibilityClass.IRREVERSIBLE:
                    # Check if human escalation was authorized
                    is_authorized = False
                    for esc in self._escalations.values():
                        if esc.action_id == step.target_action_id and esc.status == "RESOLVED":
                            if esc.escalation_id in self._resolutions:
                                is_authorized = True
                                break

                    if not is_authorized:
                        raise IrreversibleActionError(
                            f"Action '{step.target_action_id}' is irreversible and lacks approved human escalation"
                        )
                    escalations_resolved += 1
                    executed += 1
                    continue

                contract = self._contracts.get(step.action_type)
                success = True

                if step.reversibility_class == ReversibilityClass.REVERSIBLE:
                    if contract and contract.inverse_handler:
                        try:
                            success = bool(contract.inverse_handler(action))
                        except Exception as e:
                            success = False
                            errors.append(f"Step {step.step_index} inverse handler error: {e}")
                elif step.reversibility_class == ReversibilityClass.COMPENSATABLE:
                    if contract and contract.compensation_handler:
                        try:
                            success = bool(contract.compensation_handler(action))
                        except Exception as e:
                            success = False
                            errors.append(f"Step {step.step_index} compensation handler error: {e}")

                if success:
                    executed += 1
                else:
                    failed += 1

            self._total_compensations_executed += 1

            return CompensationResult(
                plan_id=plan.plan_id,
                success=(failed == 0),
                steps_executed=executed,
                steps_failed=failed,
                escalations_resolved=escalations_resolved,
                errors=errors,
            )

    def escalate(self, action: ActionExecution, reason: str) -> HumanEscalationReceipt:
        """Generates an immutable audit receipt requesting human review for an irreversible action."""
        with self._lock:
            self._actions_by_id[action.action_id] = action
            escalation_id = str(uuid.uuid4())
            digest = HumanEscalationReceipt.compute_hash(
                escalation_id=escalation_id,
                action_id=action.action_id,
                action_type=action.action_type,
                tenant_id=action.tenant_id,
                principal=action.principal,
                reason=reason,
            )
            receipt = HumanEscalationReceipt(
                escalation_id=escalation_id,
                action_id=action.action_id,
                action_type=action.action_type,
                tenant_id=action.tenant_id,
                principal=action.principal,
                reason=reason,
                status="PENDING",
                escalation_hash=digest,
            )
            self._escalations[escalation_id] = receipt
            return receipt

    def authorize_escalation(
        self,
        escalation_id: str,
        authorizer_principal: str,
        decision: str = "MANUAL_REMEDIATED",
        remediation_notes: str = "",
    ) -> EscalationResolution:
        """Records human operator approval resolving a pending escalation."""
        with self._lock:
            esc = self._escalations.get(escalation_id)
            if esc is None:
                raise CompensationError(f"Escalation '{escalation_id}' not found")

            clean_decision = str(decision).upper().strip()
            if clean_decision not in ("APPROVED_BYPASS", "MANUAL_REMEDIATED", "ABORTED"):
                raise ValueError(f"Invalid escalation decision: {decision}")

            esc.status = "RESOLVED"
            resolution = EscalationResolution(
                resolution_id=str(uuid.uuid4()),
                escalation_id=escalation_id,
                authorizer_principal=authorizer_principal,
                decision=clean_decision,
                remediation_notes=remediation_notes,
            )
            self._resolutions[escalation_id] = resolution
            return resolution

    def status(self) -> CompensationStatus:
        """Returns the operational diagnostics of the compensation engine."""
        with self._lock:
            pending = sum(1 for e in self._escalations.values() if e.status == "PENDING")
            resolved = len(self._resolutions)
            return CompensationStatus(
                total_registered_contracts=len(self._contracts),
                total_plans_generated=self._total_plans_generated,
                total_compensations_executed=self._total_compensations_executed,
                total_escalations_pending=pending,
                total_escalations_resolved=resolved,
            )

    def reset(self) -> None:
        """Resets engine state for testing."""
        with self._lock:
            self._contracts.clear()
            self._actions_by_id.clear()
            self._escalations.clear()
            self._resolutions.clear()
            self._total_plans_generated = 0
            self._total_compensations_executed = 0


# ---------------------------------------------------------------------------
# Global Singleton Instance
# ---------------------------------------------------------------------------

global_compensation_engine = CompensationEngine()
