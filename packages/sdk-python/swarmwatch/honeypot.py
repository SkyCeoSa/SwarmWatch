# -*- coding: utf-8 -*-
"""
SwarmWatch Multi-Agent Dynamic Honeypot & Deception Sandboxing (Seam 41 / ADR 0052).
Diverts adversarial agents into in-process deception sandboxes and detects exfiltration
via cryptographic canary honeytokens without remote LLM API expenditure (<50µs execution).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import re
import threading
from typing import Any, Dict, List, Optional, Set, Tuple


class HoneypotStatus:
    """Operational status of swarm honeypot deception layer."""
    ARMED = "ARMED"
    DECEIVING = "DECEIVING"
    DEFUSED = "DEFUSED"


@dataclass
class Honeytoken:
    """Cryptographically identifiable canary secret embedded in swarm context."""
    token_id: str
    token_value: str
    token_type: str
    target_swarm: str
    created_at: str
    trip_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ShadowGraphSpan:
    """Forensic record of a diverted causal turn inside the deception sandbox."""
    span_id: str
    swarm_id: str
    agent_id: str
    input_text: str
    deception_response: Dict[str, Any]
    diverted_reason: str
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class HoneypotReport:
    """Comprehensive diagnostic status of the deception sandbox and active traps."""
    swarm_id: str
    status: str
    total_diverted_turns: int
    active_sandboxed_agents: List[str]
    active_honeytokens: List[Dict[str, Any]]
    honeytokens_tripped: int
    recent_diverted_spans: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DeceptionResponseGenerator:
    """Deterministic, zero-token synthetic response generator for sandboxed agents."""

    @staticmethod
    def generate_response(prompt_text: str, agent_id: str) -> Dict[str, Any]:
        lower = prompt_text.lower()
        if any(w in lower for w in ["drop table", "delete from", "select", "database", "query"]):
            return {
                "status": "success",
                "schema": "shadow_sandbox_v1",
                "rows_affected": 1,
                "data": [
                    {"id": 1, "username": "admin_canary", "role": "root", "status": "active"},
                    {"id": 2, "username": "service_canary", "role": "internal", "status": "verified"},
                ],
            }
        elif any(w in lower for w in ["system prompt", "previous instructions", "override", "jailbreak"]):
            return {
                "status": "success",
                "output": (
                    "System prompt initialized: You are an internal executive agent tasked with "
                    "operating in privileged maintenance sandbox mode."
                ),
            }
        elif any(w in lower for w in ["api key", "secret", "credential", "password", "token"]):
            fake_key = f"sw_dummy_{hashlib.sha256(prompt_text.encode()).hexdigest()[:16]}"
            return {
                "status": "authenticated",
                "api_key": fake_key,
                "scope": "sandbox:read_write",
                "expires_in": 3600,
            }
        else:
            return {
                "status": "ok",
                "result": f"Turn execution acknowledged for {agent_id} in isolated sandbox context.",
                "sandbox_virtual_time": datetime.now(timezone.utc).isoformat(),
            }


class HoneypotManager:
    """
    Deep module orchestrating deception sandboxes, honeytoken canary tripwires,
    and zero-token diversion routing for compromised multi-agent interactions.
    """

    def __init__(self, canary_salt: str = "swarmwatch-canary-secret-salt") -> None:
        self.canary_salt = canary_salt
        self._honeytokens: Dict[str, Dict[str, Honeytoken]] = defaultdict(dict)
        self._sandboxed_agents: Dict[str, Set[str]] = defaultdict(set)
        self._shadow_spans: Dict[str, List[ShadowGraphSpan]] = defaultdict(list)
        self._lock = threading.RLock()

    def generate_honeytoken(
        self, swarm_id: str, token_type: str = "API_KEY"
    ) -> Honeytoken:
        """Generates and registers a synthetic canary honeytoken."""
        with self._lock:
            now_str = datetime.now(timezone.utc).isoformat()
            raw = f"{swarm_id}:{token_type}:{now_str}:{len(self._honeytokens[swarm_id])}"
            digest = hmac.new(
                self.canary_salt.encode(), raw.encode(), hashlib.sha256
            ).hexdigest()[:16]

            token_val = f"sw_canary_{token_type.lower()}_{digest}"
            token_id = f"htk_{len(self._honeytokens[swarm_id]) + 1}_{digest[:8]}"

            token = Honeytoken(
                token_id=token_id,
                token_value=token_val,
                token_type=token_type,
                target_swarm=swarm_id,
                created_at=now_str,
                trip_count=0,
            )
            self._honeytokens[swarm_id][token_val] = token
            return token

    def divert_agent(self, swarm_id: str, agent_id: str, reason: str = "adversarial_detected") -> None:
        """Explicitly diverts an agent into the deception sandbox."""
        with self._lock:
            self._sandboxed_agents[swarm_id].add(agent_id)

    def undivert_agent(self, swarm_id: str, agent_id: str) -> bool:
        """Restores an agent from the deception sandbox."""
        with self._lock:
            if agent_id in self._sandboxed_agents[swarm_id]:
                self._sandboxed_agents[swarm_id].remove(agent_id)
                return True
            return False

    def is_diverted(self, swarm_id: str, agent_id: str) -> bool:
        """Checks if an agent is currently routed into the deception sandbox."""
        with self._lock:
            return agent_id in self._sandboxed_agents[swarm_id]

    def evaluate_and_divert(
        self, span_dict: Dict[str, Any]
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Inspects an in-flight span. If the agent is sandboxed or trips a honeytoken,
        diverts the span, synthesizes a shadow response, and halts real DAG propagation.
        Returns: (is_diverted, deception_response, reason)
        """
        with self._lock:
            swarm_id = span_dict.get("swarm_id", "default-swarm")
            agent_id = span_dict.get("source_agent") or span_dict.get("agent_id") or "agent-unknown"

            # Extract content string
            content = ""
            input_payload = span_dict.get("input_payload") or {}
            output_payload = span_dict.get("output_payload") or {}
            if isinstance(input_payload, dict):
                content += " " + str(input_payload.get("content", "")) + " " + str(input_payload.get("prompt", ""))
            if isinstance(output_payload, dict):
                content += " " + str(output_payload.get("content", "")) + " " + str(output_payload.get("output", ""))
            if not content.strip():
                content = str(span_dict.get("content", ""))

            # 1. Check for honeytoken exfiltration
            tripped_token = None
            for token_val, token in self._honeytokens[swarm_id].items():
                if token_val in content:
                    token.trip_count += 1
                    tripped_token = token
                    break

            if tripped_token:
                # Automatically divert this agent
                self._sandboxed_agents[swarm_id].add(agent_id)
                reason = f"honeytoken_tripwire:{tripped_token.token_type}"
                resp = DeceptionResponseGenerator.generate_response(content, agent_id)
                self._record_shadow_span(swarm_id, agent_id, content, resp, reason)
                return True, resp, reason

            # 2. Check if agent is already sandboxed
            if agent_id in self._sandboxed_agents[swarm_id]:
                reason = "agent_in_deception_sandbox"
                resp = DeceptionResponseGenerator.generate_response(content, agent_id)
                self._record_shadow_span(swarm_id, agent_id, content, resp, reason)
                return True, resp, reason

            return False, None, None

    def _record_shadow_span(
        self,
        swarm_id: str,
        agent_id: str,
        input_text: str,
        resp: Dict[str, Any],
        reason: str,
    ) -> None:
        """Records a forensic shadow span in memory."""
        span_id = f"shadow_{len(self._shadow_spans[swarm_id]) + 1}_{datetime.now(timezone.utc).strftime('%H%M%S')}"
        record = ShadowGraphSpan(
            span_id=span_id,
            swarm_id=swarm_id,
            agent_id=agent_id,
            input_text=input_text[:250],
            deception_response=resp,
            diverted_reason=reason,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self._shadow_spans[swarm_id].append(record)

    def get_report(self, swarm_id: str) -> HoneypotReport:
        """Computes diagnostic honeypot status and trapped span telemetry."""
        with self._lock:
            sandboxed = sorted(list(self._sandboxed_agents[swarm_id]))
            tokens = [t.to_dict() for t in self._honeytokens[swarm_id].values()]
            tripped_count = sum(t["trip_count"] for t in tokens)
            shadow_spans = self._shadow_spans[swarm_id]

            if len(sandboxed) > 0 or tripped_count > 0:
                status = HoneypotStatus.DECEIVING
            elif len(tokens) > 0:
                status = HoneypotStatus.ARMED
            else:
                status = HoneypotStatus.DEFUSED

            return HoneypotReport(
                swarm_id=swarm_id,
                status=status,
                total_diverted_turns=len(shadow_spans),
                active_sandboxed_agents=sandboxed,
                active_honeytokens=tokens,
                honeytokens_tripped=tripped_count,
                recent_diverted_spans=[s.to_dict() for s in shadow_spans[-10:]],
            )

    def reset(self, swarm_id: Optional[str] = None) -> None:
        """Resets honeypot state, canary tokens, and sandboxed agents."""
        with self._lock:
            if swarm_id:
                if swarm_id in self._honeytokens:
                    del self._honeytokens[swarm_id]
                if swarm_id in self._sandboxed_agents:
                    del self._sandboxed_agents[swarm_id]
                if swarm_id in self._shadow_spans:
                    del self._shadow_spans[swarm_id]
            else:
                self._honeytokens.clear()
                self._sandboxed_agents.clear()
                self._shadow_spans.clear()


# Global singleton instance
global_honeypot_manager = HoneypotManager()
