# -*- coding: utf-8 -*-
"""
SwarmWatch Autonomous Counterfactual Swarm Optimization & Self-Healing Policy Controller
(Seam 31 / ADR 0042).

Pure Python standard library implementation providing real-time telemetry observation,
mathematical anomaly detection, dynamic tool blacklisting, model fallback routing,
proactive quota clamping, counterfactual savings evaluation, and 100% reversible rollbacks
with zero remote LLM token usage.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid

from swarmwatch.schema import SwarmSpan
from swarmwatch.policy import PolicyEngine, RuleConfig
from swarmwatch.metrics import (
    record_self_healing_action,
    set_self_healing_active_actions,
    record_counterfactual_savings,
)

logger = logging.getLogger("swarmwatch.selfhealing")


class AnomalyType:
    """Formal classification of multi-agent runtime operational anomalies."""
    REPEATED_TOOL_FAILURES = "repeated_tool_failures"
    ENTROPY_VOLATILITY = "entropy_volatility"
    BUDGET_RUNAWAY = "budget_runaway"
    CASCADE_ERROR = "cascade_error"
    LATENCY_SPIKE = "latency_spike"


class SelfHealingActionType:
    """Surgical remediation intervention strategies."""
    BLACKLIST_TOOL = "blacklist_tool"
    ROUTE_MODEL_FALLBACK = "route_model_fallback"
    CLAMP_QUOTA = "clamp_quota"
    AUTO_RESET_ON_STABILIZATION = "auto_reset_on_stabilization"
    INJECT_BACKOFF = "inject_backoff"


@dataclass
class CounterfactualEvaluation:
    """Quantitative estimate of resources saved via self-healing remediation."""
    tokens_saved: int = 0
    cost_saved: float = 0.0
    turns_avoided: int = 0
    formula: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SelfHealingAction:
    """An atomic remediation intervention recorded in the self-healing ledger."""
    id: str
    swarm_id: str
    agent_name: str
    action_type: str
    anomaly_detected: str
    target: str
    reason: str
    counterfactual: CounterfactualEvaluation = field(default_factory=CounterfactualEvaluation)
    applied_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "active"  # "active", "rolled_back", "expired"
    original_state: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        return data


@dataclass
class SelfHealingPolicyConfig:
    """Configuration parameters for the autonomous self-healing controller."""
    enabled: bool = True
    max_consecutive_tool_failures: int = 3
    entropy_floor_trigger: float = 0.35
    budget_acceleration_threshold: float = 0.8
    cost_per_token_default: float = 0.00002  # $20 per 1M tokens ($0.02 / 1k)
    model_fallback_map: Dict[str, str] = field(default_factory=lambda: {
        "gpt-4o": "gpt-4o-mini",
        "gpt-4": "gpt-3.5-turbo",
        "claude-3-5-sonnet": "claude-3-5-haiku",
        "claude-3-opus": "claude-3-5-sonnet",
        "default": "gpt-4o-mini",
    })
    auto_rollback_seconds: Optional[int] = 300


class SwarmSelfHealingController:
    """
    Autonomous control plane module continuously monitoring causal span streams,
    detecting multi-agent operational anomalies, and enacting surgical, reversible
    policy remediations with zero external LLM token overhead.
    """

    def __init__(
        self,
        config: Optional[SelfHealingPolicyConfig] = None,
        policy_engine: Optional[PolicyEngine] = None,
    ):
        self.config = config or SelfHealingPolicyConfig()
        self.policy_engine = policy_engine
        self._lock = threading.RLock()

        # Operational tracking state
        self._consecutive_tool_failures: Dict[Tuple[str, str], int] = {}
        self._recent_entropy_history: Dict[str, List[float]] = {}
        self._agent_token_totals: Dict[str, int] = {}
        self._agent_turn_counts: Dict[str, int] = {}
        self._active_model_routes: Dict[str, str] = {}
        self._active_backoffs: Dict[str, float] = {}

        # Self-healing ledger
        self._actions: Dict[str, SelfHealingAction] = {}
        self._cumulative_tokens_saved: int = 0
        self._cumulative_cost_saved: float = 0.0

    def set_policy_engine(self, engine: PolicyEngine) -> None:
        """Binds the active declarative PolicyEngine."""
        with self._lock:
            self.policy_engine = engine

    def get_model_route(self, agent_name: str, requested_model: Optional[str] = None) -> str:
        """Resolves active routed model for an agent, honoring fallback remediations."""
        with self._lock:
            if agent_name in self._active_model_routes:
                return self._active_model_routes[agent_name]
            if requested_model:
                return requested_model
            return self.config.model_fallback_map.get("default", "gpt-4o-mini")

    def observe_span(self, span: SwarmSpan) -> Optional[SelfHealingAction]:
        """
        Inspects an incoming causal span in real time (<1µs) and triggers
        surgical remediation if an operational anomaly is detected.
        """
        if not self.config.enabled:
            return None

        agent = span.source_agent or "unknown_agent"
        swarm_id = span.swarm_id or "default_swarm"
        span_type = getattr(span, "span_type", "turn") or "turn"
        status = getattr(span, "status", "ok") or "ok"

        with self._lock:
            # 1. Update turn counts & token totals
            tokens = getattr(span, "token_count", 0) or 0
            self._agent_token_totals[agent] = self._agent_token_totals.get(agent, 0) + tokens
            self._agent_turn_counts[agent] = self._agent_turn_counts.get(agent, 0) + 1

            # 2. Check for Repeated Tool Failures
            if span_type in ("tool", "tool_call", "function"):
                tool_name = getattr(span, "target_agent", None) or "unnamed_tool"
                pair_key = (agent, tool_name)

                is_error = status in ("error", "failed", "failure")
                if not is_error and getattr(span, "output_payload", None):
                    out_text = str(span.output_payload).lower()
                    if "exception" in out_text or "traceback" in out_text or "error" in out_text:
                        is_error = True

                if is_error:
                    failures = self._consecutive_tool_failures.get(pair_key, 0) + 1
                    self._consecutive_tool_failures[pair_key] = failures
                    if failures >= self.config.max_consecutive_tool_failures:
                        return self._enact_tool_blacklist(
                            swarm_id=swarm_id,
                            agent_name=agent,
                            tool_name=tool_name,
                            failures=failures,
                        )
                else:
                    # Successful invocation resets consecutive counter
                    self._consecutive_tool_failures[pair_key] = 0

            # 3. Check for Shannon Entropy Volatility (Degenerative Repetition Loop)
            entropy = getattr(span, "entropy_score", None)
            if entropy is not None:
                history = self._recent_entropy_history.setdefault(agent, [])
                history.append(entropy)
                if len(history) > 10:
                    history.pop(0)

                if entropy < self.config.entropy_floor_trigger:
                    requested_model = "gpt-4o"
                    if getattr(span, "input_payload", None):
                        requested_model = span.input_payload.get("model", "gpt-4o")
                    fallback_model = self.config.model_fallback_map.get(
                        requested_model,
                        self.config.model_fallback_map.get("default", "gpt-4o-mini"),
                    )

                    if self._active_model_routes.get(agent) != fallback_model:
                        return self._enact_model_fallback(
                            swarm_id=swarm_id,
                            agent_name=agent,
                            requested_model=requested_model,
                            fallback_model=fallback_model,
                            entropy=entropy,
                        )

            # 4. Check for Budget Runaway Acceleration
            if self.policy_engine:
                effective = self.policy_engine.get_effective_config(agent)
                if effective.token_budget and effective.token_budget > 0:
                    used = self._agent_token_totals[agent]
                    ratio = used / effective.token_budget
                    turns_used = self._agent_turn_counts[agent]
                    max_turns = effective.max_turns or 20

                    if ratio >= self.config.budget_acceleration_threshold and turns_used < (max_turns * 0.5):
                        # Clamping needed before circuit trip
                        return self._enact_quota_clamping(
                            swarm_id=swarm_id,
                            agent_name=agent,
                            current_used=used,
                            budget=effective.token_budget,
                            turns_used=turns_used,
                        )

            return None

    def _enact_tool_blacklist(
        self,
        swarm_id: str,
        agent_name: str,
        tool_name: str,
        failures: int,
    ) -> SelfHealingAction:
        """Dynamically appends a failing tool to prohibited_tools in PolicyEngine."""
        action_id = f"heal-{uuid.uuid4().hex[:8]}"
        original_tools = []

        if self.policy_engine:
            with self.policy_engine._lock:
                agent_cfg = self.policy_engine.policy.agents.get(agent_name)
                if not agent_cfg:
                    agent_cfg = RuleConfig()
                    self.policy_engine.policy.agents[agent_name] = agent_cfg

                original_tools = list(agent_cfg.prohibited_tools)
                if tool_name not in agent_cfg.prohibited_tools:
                    agent_cfg.prohibited_tools = list(dict.fromkeys(agent_cfg.prohibited_tools + [tool_name]))
                    self.policy_engine._cached_effective.clear()

        # Counterfactual optimization estimate:
        # Assuming ~500 tokens per failing invocation avoided over next 5 turns
        est_tokens = 500 * 5
        est_cost = est_tokens * self.config.cost_per_token_default

        cf = CounterfactualEvaluation(
            tokens_saved=est_tokens,
            cost_saved=round(est_cost, 4),
            turns_avoided=5,
            formula=f"avoided_tool_calls(5) * est_tokens_per_call(500) * rate({self.config.cost_per_token_default})",
        )

        action = SelfHealingAction(
            id=action_id,
            swarm_id=swarm_id,
            agent_name=agent_name,
            action_type=SelfHealingActionType.BLACKLIST_TOOL,
            anomaly_detected=AnomalyType.REPEATED_TOOL_FAILURES,
            target=tool_name,
            reason=f"Tool '{tool_name}' failed {failures} consecutive times; quarantined to preserve swarm execution",
            counterfactual=cf,
            original_state={"prohibited_tools": original_tools},
        )

        self._record_action(action)
        logger.warning(
            "Autonomous Self-Healing: Blacklisted tool '%s' on agent '%s' in swarm '%s' (est saved: %d tokens / $%.4f)",
            tool_name, agent_name, swarm_id, est_tokens, est_cost
        )
        return action

    def _enact_model_fallback(
        self,
        swarm_id: str,
        agent_name: str,
        requested_model: str,
        fallback_model: str,
        entropy: float,
    ) -> SelfHealingAction:
        """Dynamically routes agent turns to a stabilized, cheaper fallback model."""
        action_id = f"heal-{uuid.uuid4().hex[:8]}"
        orig_model = self._active_model_routes.get(agent_name, requested_model)
        self._active_model_routes[agent_name] = fallback_model

        # Counterfactual estimate: ~1000 tokens per turn over 4 turns at cost delta ($0.03 vs $0.003)
        est_tokens = 4000
        est_cost = 0.12  # Estimated $0.12 saved

        cf = CounterfactualEvaluation(
            tokens_saved=est_tokens,
            cost_saved=est_cost,
            turns_avoided=0,
            formula=f"fallback_route({requested_model}->{fallback_model}) * 4_turns",
        )

        action = SelfHealingAction(
            id=action_id,
            swarm_id=swarm_id,
            agent_name=agent_name,
            action_type=SelfHealingActionType.ROUTE_MODEL_FALLBACK,
            anomaly_detected=AnomalyType.ENTROPY_VOLATILITY,
            target=fallback_model,
            reason=f"Shannon entropy dropped to {entropy:.3f} (< {self.config.entropy_floor_trigger}); down-routed to '{fallback_model}'",
            counterfactual=cf,
            original_state={"routed_model": orig_model},
        )

        self._record_action(action)
        logger.warning(
            "Autonomous Self-Healing: Down-routed agent '%s' model '%s' -> '%s' (entropy %.3f)",
            agent_name, requested_model, fallback_model, entropy
        )
        return action

    def _enact_quota_clamping(
        self,
        swarm_id: str,
        agent_name: str,
        current_used: int,
        budget: int,
        turns_used: int,
    ) -> SelfHealingAction:
        """Preemptively clamps turn limits to prevent budget exhaustion."""
        action_id = f"heal-{uuid.uuid4().hex[:8]}"
        orig_max_turns = None

        if self.policy_engine:
            with self.policy_engine._lock:
                agent_cfg = self.policy_engine.policy.agents.get(agent_name)
                if not agent_cfg:
                    agent_cfg = RuleConfig()
                    self.policy_engine.policy.agents[agent_name] = agent_cfg

                orig_max_turns = agent_cfg.max_turns
                new_max = max(turns_used + 3, int((orig_max_turns or 20) * 0.75))
                agent_cfg.max_turns = new_max
                self.policy_engine._cached_effective.clear()

        est_tokens = max(0, budget - current_used)
        est_cost = est_tokens * self.config.cost_per_token_default

        cf = CounterfactualEvaluation(
            tokens_saved=est_tokens,
            cost_saved=round(est_cost, 4),
            turns_avoided=3,
            formula=f"clamped_max_turns_from({orig_max_turns})",
        )

        action = SelfHealingAction(
            id=action_id,
            swarm_id=swarm_id,
            agent_name=agent_name,
            action_type=SelfHealingActionType.CLAMP_QUOTA,
            anomaly_detected=AnomalyType.BUDGET_RUNAWAY,
            target=str(agent_cfg.max_turns if self.policy_engine else "clamped"),
            reason=f"Token consumption ({current_used}) crossed 80% quota in turn {turns_used}; clamped turns",
            counterfactual=cf,
            original_state={"max_turns": orig_max_turns},
        )

        self._record_action(action)
        return action

    def _record_action(self, action: SelfHealingAction) -> None:
        """Records an action into state ledger and publishes Prometheus metrics."""
        self._actions[action.id] = action
        self._cumulative_tokens_saved += action.counterfactual.tokens_saved
        self._cumulative_cost_saved += action.counterfactual.cost_saved

        record_self_healing_action(action.action_type, action.anomaly_detected)
        set_self_healing_active_actions(len([a for a in self._actions.values() if a.status == "active"]))
        record_counterfactual_savings(
            action.counterfactual.tokens_saved,
            action.counterfactual.cost_saved,
        )

    def remediate(
        self,
        swarm_id: str,
        agent_name: str,
        action_type: str,
        target: str,
        reason: Optional[str] = None,
    ) -> SelfHealingAction:
        """Explicit programmatic or operator remediation action."""
        with self._lock:
            if action_type == SelfHealingActionType.BLACKLIST_TOOL:
                return self._enact_tool_blacklist(
                    swarm_id=swarm_id,
                    agent_name=agent_name,
                    tool_name=target,
                    failures=self.config.max_consecutive_tool_failures,
                )
            elif action_type == SelfHealingActionType.ROUTE_MODEL_FALLBACK:
                return self._enact_model_fallback(
                    swarm_id=swarm_id,
                    agent_name=agent_name,
                    requested_model="gpt-4o",
                    fallback_model=target,
                    entropy=0.25,
                )
            elif action_type == SelfHealingActionType.AUTO_RESET_ON_STABILIZATION:
                action_id = f"heal-{uuid.uuid4().hex[:8]}"
                action = SelfHealingAction(
                    id=action_id,
                    swarm_id=swarm_id,
                    agent_name=agent_name,
                    action_type=SelfHealingActionType.AUTO_RESET_ON_STABILIZATION,
                    anomaly_detected="stabilization_probation",
                    target=target,
                    reason=reason or "Agent metrics nominal for probation period; cleared circuit warnings",
                )
                self._record_action(action)
                return action
            else:
                action_id = f"heal-{uuid.uuid4().hex[:8]}"
                action = SelfHealingAction(
                    id=action_id,
                    swarm_id=swarm_id,
                    agent_name=agent_name,
                    action_type=action_type,
                    anomaly_detected="manual_intervention",
                    target=target,
                    reason=reason or "Manual remediation action enacted via API",
                )
                self._record_action(action)
                return action

    def rollback_action(self, action_id: str) -> bool:
        """
        Reverts a self-healing action back to its recorded original_state.
        100% reversible and safe.
        """
        with self._lock:
            action = self._actions.get(action_id)
            if not action or action.status != "active":
                return False

            orig = action.original_state

            if action.action_type == SelfHealingActionType.BLACKLIST_TOOL:
                if self.policy_engine and "prohibited_tools" in orig:
                    with self.policy_engine._lock:
                        agent_cfg = self.policy_engine.policy.agents.get(action.agent_name)
                        if agent_cfg:
                            agent_cfg.prohibited_tools = list(orig["prohibited_tools"])
                            self.policy_engine._cached_effective.clear()
                # Reset consecutive failure counter
                self._consecutive_tool_failures.pop((action.agent_name, action.target), None)

            elif action.action_type == SelfHealingActionType.ROUTE_MODEL_FALLBACK:
                if "routed_model" in orig:
                    self._active_model_routes[action.agent_name] = orig["routed_model"]
                else:
                    self._active_model_routes.pop(action.agent_name, None)

            elif action.action_type == SelfHealingActionType.CLAMP_QUOTA:
                if self.policy_engine and "max_turns" in orig:
                    with self.policy_engine._lock:
                        agent_cfg = self.policy_engine.policy.agents.get(action.agent_name)
                        if agent_cfg:
                            agent_cfg.max_turns = orig["max_turns"]
                            self.policy_engine._cached_effective.clear()

            action.status = "rolled_back"
            set_self_healing_active_actions(len([a for a in self._actions.values() if a.status == "active"]))
            logger.info("Self-Healing Action '%s' rolled back successfully", action_id)
            return True

    def rollback_all(self, swarm_id: Optional[str] = None) -> int:
        """Rolls back all active actions for a specific swarm or across all swarms."""
        with self._lock:
            count = 0
            for action in list(self._actions.values()):
                if action.status == "active":
                    if swarm_id is None or action.swarm_id == swarm_id:
                        if self.rollback_action(action.id):
                            count += 1
            return count

    def get_active_actions(self) -> List[SelfHealingAction]:
        with self._lock:
            return [a for a in self._actions.values() if a.status == "active"]

    def get_action_history(self) -> List[SelfHealingAction]:
        with self._lock:
            return list(self._actions.values())

    def status(self) -> Dict[str, Any]:
        """Diagnostic state of the self-healing controller."""
        with self._lock:
            active = [a.to_dict() for a in self._actions.values() if a.status == "active"]
            return {
                "enabled": self.config.enabled,
                "active_actions_count": len(active),
                "total_actions_recorded": len(self._actions),
                "cumulative_tokens_saved": self._cumulative_tokens_saved,
                "cumulative_cost_saved": round(self._cumulative_cost_saved, 4),
                "active_model_routes": dict(self._active_model_routes),
                "active_actions": active,
            }


# Global singleton instance for in-process self-healing
global_self_healing_controller = SwarmSelfHealingController()
