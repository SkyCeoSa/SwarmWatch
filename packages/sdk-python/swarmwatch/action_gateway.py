# -*- coding: utf-8 -*-
"""
SwarmWatch Autonomous Action Gateway & Governed Downstream Agent Execution (Seam 71 / ADR 0082).

Implements capability-gated downstream agent execution, multi-party human quorum authorization,
reversibility and risk classification, fail-closed circuit synchronization, and
cryptographically verifiable action execution receipts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import uuid


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class ActionGatewayError(RuntimeError):
    """Base exception for all action gateway governance errors."""
    pass


class UnauthorizedActionError(ActionGatewayError):
    """Raised when an action request lacks valid capability tokens or crosses tenant boundaries."""
    pass


class ActionQuorumPendingError(ActionGatewayError):
    """Raised when an action requires human operator quorum approval before execution."""
    pass


class ActionCircuitBlockedError(ActionGatewayError):
    """Raised when execution is halted because the safety circuit breaker is tripped."""
    pass


class ActionExecutionFailedError(ActionGatewayError):
    """Raised when the downstream action handler fails during execution."""
    pass


# ==============================================================================
# 2. Enums
# ==============================================================================

class ActionRiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ActionApprovalStatus(str, Enum):
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTED = "EXECUTED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# ==============================================================================
# 3. Models & Data Structures
# ==============================================================================

def sha256_digest(data: str | bytes) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


@dataclass
class ActionRegistration:
    """Registered executor and baseline governance policy for an action."""
    action_name: str
    handler: Callable[[Dict[str, Any]], Any]
    default_risk: ActionRiskLevel = ActionRiskLevel.LOW
    default_reversibility: str = "REVERSIBLE"
    required_quorum_count: int = 1
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_name": self.action_name,
            "default_risk": self.default_risk.value,
            "default_reversibility": self.default_reversibility,
            "required_quorum_count": self.required_quorum_count,
            "description": self.description,
        }


@dataclass
class ActionApproval:
    """Cryptographic authorization record from a human or authorized principal."""
    approval_id: str
    request_id: str
    approver_principal: str
    decision: str  # "APPROVED" or "REJECTED"
    comment: str = ""
    timestamp_ns: int = field(default_factory=time.time_ns)
    signature: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "request_id": self.request_id,
            "approver_principal": self.approver_principal,
            "decision": self.decision,
            "comment": self.comment,
            "timestamp_ns": self.timestamp_ns,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ActionApproval:
        return cls(
            approval_id=data["approval_id"],
            request_id=data["request_id"],
            approver_principal=data["approver_principal"],
            decision=data["decision"],
            comment=data.get("comment", ""),
            timestamp_ns=data.get("timestamp_ns", time.time_ns()),
            signature=data.get("signature", ""),
        )


@dataclass
class ActionRequest:
    """A governed action request waiting for or granted execution admission."""
    request_id: str
    tenant_id: str
    agent_id: str
    action_name: str
    target_resource: str
    parameters: Dict[str, Any]
    capability_token: Optional[str] = None
    estimated_cost_usd: float = 0.0
    reversibility_class: str = "REVERSIBLE"
    risk_level: ActionRiskLevel = ActionRiskLevel.LOW
    status: ActionApprovalStatus = ActionApprovalStatus.PENDING_APPROVAL
    required_quorum_count: int = 0
    created_at_ns: int = field(default_factory=time.time_ns)
    approvals: List[ActionApproval] = field(default_factory=list)
    rejection_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "action_name": self.action_name,
            "target_resource": self.target_resource,
            "parameters": self.parameters,
            "capability_token": self.capability_token,
            "estimated_cost_usd": self.estimated_cost_usd,
            "reversibility_class": self.reversibility_class,
            "risk_level": self.risk_level.value,
            "status": self.status.value,
            "required_quorum_count": self.required_quorum_count,
            "created_at_ns": self.created_at_ns,
            "approvals": [a.to_dict() for a in self.approvals],
            "rejection_reason": self.rejection_reason,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ActionRequest:
        approvals = [ActionApproval.from_dict(a) for a in data.get("approvals", [])]
        return cls(
            request_id=data["request_id"],
            tenant_id=data["tenant_id"],
            agent_id=data["agent_id"],
            action_name=data["action_name"],
            target_resource=data["target_resource"],
            parameters=data.get("parameters", {}),
            capability_token=data.get("capability_token"),
            estimated_cost_usd=float(data.get("estimated_cost_usd", 0.0)),
            reversibility_class=data.get("reversibility_class", "REVERSIBLE"),
            risk_level=ActionRiskLevel(data.get("risk_level", "LOW")),
            status=ActionApprovalStatus(data.get("status", "PENDING_APPROVAL")),
            required_quorum_count=int(data.get("required_quorum_count", 0)),
            created_at_ns=int(data.get("created_at_ns", time.time_ns())),
            approvals=approvals,
            rejection_reason=data.get("rejection_reason"),
        )


@dataclass
class ActionExecutionReceipt:
    """Non-repudiable audit receipt certifying downstream action execution."""
    receipt_id: str
    request_id: str
    tenant_id: str
    agent_id: str
    action_name: str
    status: str  # "SUCCESS" or "FAILED"
    result_summary: Dict[str, Any]
    executed_at_ns: int
    duration_us: float
    receipt_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "request_id": self.request_id,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "action_name": self.action_name,
            "status": self.status,
            "result_summary": self.result_summary,
            "executed_at_ns": self.executed_at_ns,
            "duration_us": self.duration_us,
            "receipt_hash": self.receipt_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ActionExecutionReceipt:
        return cls(
            receipt_id=data["receipt_id"],
            request_id=data["request_id"],
            tenant_id=data["tenant_id"],
            agent_id=data["agent_id"],
            action_name=data["action_name"],
            status=data["status"],
            result_summary=data.get("result_summary", {}),
            executed_at_ns=int(data["executed_at_ns"]),
            duration_us=float(data["duration_us"]),
            receipt_hash=data["receipt_hash"],
        )


# ==============================================================================
# 4. Core Engine: ActionGateway
# ==============================================================================

class ActionGateway:
    """
    Autonomous Action Gateway coordinating capability verification, multi-party quorum
    governance, downstream execution dispatch, and tamper-evident receipt minting.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._registry: Dict[str, ActionRegistration] = {}
        self._requests: Dict[str, ActionRequest] = {}
        self._receipts: Dict[str, ActionExecutionReceipt] = {}
        self._circuit_tripped: bool = False
        self._require_capability_tokens: bool = False
        self._capability_validator: Optional[Callable[[str, str, str, str], bool]] = None
        self._auto_register_defaults()

    def _auto_register_defaults(self) -> None:
        """Registers canonical default mock actions for testing and baseline operation."""
        self.register_executor(
            action_name="db:query",
            handler=lambda params: {"rows_returned": 42, "query": params.get("sql", "")},
            default_risk=ActionRiskLevel.LOW,
            default_reversibility="REVERSIBLE",
            required_quorum_count=0,
            description="Read-only SQL query execution",
        )
        self.register_executor(
            action_name="api:webhook_call",
            handler=lambda params: {"status": 200, "delivered": True, "target": params.get("url", "")},
            default_risk=ActionRiskLevel.MEDIUM,
            default_reversibility="COMPENSATABLE",
            required_quorum_count=1,
            description="Outbound webhook HTTP invocation",
        )
        self.register_executor(
            action_name="db:drop_table",
            handler=lambda params: {"dropped": True, "table": params.get("table", "")},
            default_risk=ActionRiskLevel.CRITICAL,
            default_reversibility="IRREVERSIBLE",
            required_quorum_count=2,
            description="Destructive table schema drop",
        )
        self.register_executor(
            action_name="finance:refund",
            handler=lambda params: {"refund_id": f"ref-{uuid.uuid4().hex[:8]}", "amount": params.get("amount", 0)},
            default_risk=ActionRiskLevel.HIGH,
            default_reversibility="IRREVERSIBLE",
            required_quorum_count=1,
            description="Customer payment refund transaction",
        )

    def set_capability_validator(
        self,
        validator: Optional[Callable[[str, str, str, str], bool]],
        require_tokens: bool = True,
    ) -> None:
        """Configures external capability validator: validator(token, action, resource, tenant_id) -> bool."""
        with self._lock:
            self._capability_validator = validator
            self._require_capability_tokens = require_tokens

    def set_circuit_tripped(self, tripped: bool) -> None:
        """Sets circuit breaker state, halting downstream execution when tripped."""
        with self._lock:
            self._circuit_tripped = tripped

    def register_executor(
        self,
        action_name: str,
        handler: Callable[[Dict[str, Any]], Any],
        default_risk: ActionRiskLevel = ActionRiskLevel.LOW,
        default_reversibility: str = "REVERSIBLE",
        required_quorum_count: int = 1,
        description: str = "",
    ) -> None:
        """Registers an action executor with default governance rules."""
        with self._lock:
            self._registry[action_name] = ActionRegistration(
                action_name=action_name,
                handler=handler,
                default_risk=default_risk,
                default_reversibility=default_reversibility,
                required_quorum_count=required_quorum_count,
                description=description,
            )

    def evaluate_risk(
        self,
        action_name: str,
        target_resource: str,
        reversibility: str,
        estimated_cost_usd: float,
    ) -> Tuple[ActionRiskLevel, int]:
        """
        Calculates risk tier and required human quorum count using multi-criteria lattice.
        """
        reg = self._registry.get(action_name)
        base_quorum = reg.required_quorum_count if reg else 1

        # 1. Critical actions: destructive or high-cost
        if (
            reversibility == "IRREVERSIBLE"
            and (
                estimated_cost_usd > 50.0
                or any(k in action_name.lower() for k in ("drop", "delete", "destroy", "purge"))
            )
        ):
            return ActionRiskLevel.CRITICAL, max(2, base_quorum)

        # 2. High-risk actions: irreversible or significant cost
        if reversibility == "IRREVERSIBLE" or estimated_cost_usd > 50.0:
            return ActionRiskLevel.HIGH, max(1, base_quorum)

        # 3. Medium-risk actions: compensatable or moderate cost
        if reversibility == "COMPENSATABLE" or estimated_cost_usd > 1.0:
            return ActionRiskLevel.MEDIUM, max(1, base_quorum)

        # 4. Low-risk actions: reversible and low cost
        return ActionRiskLevel.LOW, 0

    def request_action(
        self,
        tenant_id: str,
        agent_id: str,
        action_name: str,
        target_resource: str,
        parameters: Dict[str, Any],
        capability_token: Optional[str] = None,
        estimated_cost_usd: float = 0.0,
        reversibility_class: Optional[str] = None,
        explicit_request_id: Optional[str] = None,
    ) -> ActionRequest:
        """
        Submits and evaluates an action request. Auto-approves LOW risk actions.
        Fails closed on circuit trips or capability validation failure.
        """
        with self._lock:
            # 1. Circuit breaker check
            if self._circuit_tripped:
                raise ActionCircuitBlockedError("Action Gateway halted: safety circuit breaker is tripped")

            # 2. Capability verification
            if self._require_capability_tokens:
                if not capability_token:
                    raise UnauthorizedActionError(f"Action '{action_name}' requires capability token")
                if self._capability_validator:
                    is_valid = self._capability_validator(capability_token, action_name, target_resource, tenant_id)
                    if not is_valid:
                        raise UnauthorizedActionError(
                            f"Capability token rejected for action '{action_name}' on '{target_resource}'"
                        )

            # 3. Resolve reversibility and risk
            reg = self._registry.get(action_name)
            rev = reversibility_class or (reg.default_reversibility if reg else "REVERSIBLE")
            risk_level, required_quorum = self.evaluate_risk(
                action_name=action_name,
                target_resource=target_resource,
                reversibility=rev,
                estimated_cost_usd=estimated_cost_usd,
            )

            # 4. Determine initial approval status
            status = ActionApprovalStatus.APPROVED if required_quorum == 0 else ActionApprovalStatus.PENDING_APPROVAL

            req_id = explicit_request_id or f"act-req-{uuid.uuid4().hex[:12]}"
            request = ActionRequest(
                request_id=req_id,
                tenant_id=tenant_id,
                agent_id=agent_id,
                action_name=action_name,
                target_resource=target_resource,
                parameters=parameters,
                capability_token=capability_token,
                estimated_cost_usd=estimated_cost_usd,
                reversibility_class=rev,
                risk_level=risk_level,
                status=status,
                required_quorum_count=required_quorum,
                created_at_ns=time.time_ns(),
            )

            self._requests[req_id] = request
            return request

    def approve_action(
        self,
        request_id: str,
        approver_principal: str,
        decision: str = "APPROVED",
        comment: str = "",
        signature: str = "",
    ) -> ActionRequest:
        """
        Records human operator approval or rejection for a pending action request.
        """
        with self._lock:
            request = self._requests.get(request_id)
            if not request:
                raise ActionGatewayError(f"Action request '{request_id}' not found")

            if request.status not in (ActionApprovalStatus.PENDING_APPROVAL, ActionApprovalStatus.APPROVED):
                raise ActionGatewayError(
                    f"Cannot approve action request '{request_id}' in state '{request.status.value}'"
                )

            approval = ActionApproval(
                approval_id=f"appr-{uuid.uuid4().hex[:8]}",
                request_id=request_id,
                approver_principal=approver_principal,
                decision=decision.upper(),
                comment=comment,
                timestamp_ns=time.time_ns(),
                signature=signature,
            )
            request.approvals.append(approval)

            if decision.upper() == "REJECTED":
                request.status = ActionApprovalStatus.REJECTED
                request.rejection_reason = comment or f"Rejected by {approver_principal}"
                return request

            # Count valid positive approvals
            positive_approvals = sum(1 for a in request.approvals if a.decision == "APPROVED")
            if positive_approvals >= request.required_quorum_count:
                request.status = ActionApprovalStatus.APPROVED

            return request

    def execute_action(self, request_id: str) -> ActionExecutionReceipt:
        """
        Dispatches an approved action to its registered executor and mints a verifiable execution receipt.
        Fails closed with ActionQuorumPendingError if quorum is not reached.
        """
        with self._lock:
            # 1. Circuit breaker check
            if self._circuit_tripped:
                raise ActionCircuitBlockedError("Action Gateway execution blocked: safety circuit breaker is tripped")

            # 2. Request retrieval
            request = self._requests.get(request_id)
            if not request:
                raise ActionGatewayError(f"Action request '{request_id}' not found")

            if request.status == ActionApprovalStatus.PENDING_APPROVAL:
                positive = sum(1 for a in request.approvals if a.decision == "APPROVED")
                raise ActionQuorumPendingError(
                    f"Action request '{request_id}' requires {request.required_quorum_count} approvals; has {positive}"
                )

            if request.status == ActionApprovalStatus.REJECTED:
                raise ActionGatewayError(f"Action request '{request_id}' was rejected: {request.rejection_reason}")

            if request.status == ActionApprovalStatus.EXECUTED:
                raise ActionGatewayError(f"Action request '{request_id}' has already been executed")

            reg = self._registry.get(request.action_name)
            if not reg:
                raise ActionGatewayError(f"No executor registered for action '{request.action_name}'")

        # 3. Execution outside main lock to allow concurrency
        t0 = time.perf_counter()
        exec_status = "SUCCESS"
        result_summary: Dict[str, Any] = {}

        try:
            handler_output = reg.handler(request.parameters)
            if isinstance(handler_output, dict):
                result_summary = handler_output
            else:
                result_summary = {"result": handler_output}
        except Exception as e:
            exec_status = "FAILED"
            result_summary = {"error": str(e), "error_type": type(e).__name__}

        t1 = time.perf_counter()
        duration_us = (t1 - t0) * 1_000_000
        exec_at_ns = time.time_ns()
        receipt_id = f"rcpt-act-{uuid.uuid4().hex[:12]}"

        # 4. Mint canonical cryptographic receipt hash
        canonical_dict = {
            "receipt_id": receipt_id,
            "request_id": request_id,
            "tenant_id": request.tenant_id,
            "agent_id": request.agent_id,
            "action_name": request.action_name,
            "status": exec_status,
            "executed_at_ns": exec_at_ns,
            "duration_us": round(duration_us, 2),
            "result_summary": result_summary,
        }
        encoded = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"))
        receipt_hash = sha256_digest(f"sw:act:v1:{encoded}")

        receipt = ActionExecutionReceipt(
            receipt_id=receipt_id,
            request_id=request_id,
            tenant_id=request.tenant_id,
            agent_id=request.agent_id,
            action_name=request.action_name,
            status=exec_status,
            result_summary=result_summary,
            executed_at_ns=exec_at_ns,
            duration_us=duration_us,
            receipt_hash=receipt_hash,
        )

        with self._lock:
            request.status = ActionApprovalStatus.EXECUTED if exec_status == "SUCCESS" else ActionApprovalStatus.FAILED
            self._receipts[receipt_id] = receipt

        if exec_status == "FAILED":
            raise ActionExecutionFailedError(
                f"Action '{request.action_name}' execution failed: {result_summary.get('error')}"
            )

        return receipt

    def get_pending_approvals(self, tenant_id: Optional[str] = None) -> List[ActionRequest]:
        """Lists pending action requests awaiting operator approval."""
        with self._lock:
            pending = [
                r for r in self._requests.values()
                if r.status == ActionApprovalStatus.PENDING_APPROVAL
            ]
            if tenant_id:
                pending = [r for r in pending if r.tenant_id == tenant_id]
            return sorted(pending, key=lambda x: x.created_at_ns)

    def get_receipt(self, receipt_id: str) -> Optional[ActionExecutionReceipt]:
        """Retrieves an immutable execution receipt by ID."""
        with self._lock:
            return self._receipts.get(receipt_id)

    def get_request(self, request_id: str) -> Optional[ActionRequest]:
        """Retrieves an action request by ID."""
        with self._lock:
            return self._requests.get(request_id)

    def get_receipts(self, tenant_id: Optional[str] = None) -> List[ActionExecutionReceipt]:
        """Returns all recorded execution receipts."""
        with self._lock:
            receipts = list(self._receipts.values())
            if tenant_id:
                receipts = [r for r in receipts if r.tenant_id == tenant_id]
            return sorted(receipts, key=lambda x: x.executed_at_ns)

    def status(self) -> Dict[str, Any]:
        """Returns live operational diagnostics of the Action Gateway."""
        with self._lock:
            executed_count = sum(1 for r in self._receipts.values() if r.status == "SUCCESS")
            failed_count = sum(1 for r in self._receipts.values() if r.status == "FAILED")
            pending_count = sum(1 for r in self._requests.values() if r.status == ActionApprovalStatus.PENDING_APPROVAL)
            return {
                "circuit_tripped": self._circuit_tripped,
                "registered_executors_count": len(self._registry),
                "total_requests": len(self._requests),
                "pending_approvals_count": pending_count,
                "executed_receipts_count": executed_count,
                "failed_receipts_count": failed_count,
                "require_capability_tokens": self._require_capability_tokens,
            }

    def reset(self) -> None:
        """Resets transient state, clearing requests and receipts while preserving registered executors."""
        with self._lock:
            self._requests.clear()
            self._receipts.clear()
            self._circuit_tripped = False
            self._require_capability_tokens = False
            self._capability_validator = None
            self._registry.clear()
            self._auto_register_defaults()


# ==============================================================================
# 5. Global Singleton
# ==============================================================================

global_action_gateway = ActionGateway()
