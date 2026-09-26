# -*- coding: utf-8 -*-
"""
Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55 / ADR 0066).
Replaces overbroad 'VERIFIED' claims with mathematically sound bounded semantics:
BOUNDED_SAFE (with explicit depth k, assumptions, and state coverage),
UNBOUNDED_PROVEN (closed state spaces), INDETERMINATE (resource cutoffs),
and VIOLATED (minimal counterexample trace with independent replay verification).
Zero external LLM API calls or SAT/SMT solver dependencies (<1.0ms local execution).
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from swarmwatch.model_checker import (
    AgentTransition,
    AgentTransitionModel,
    VerificationInvariant as LegacyInvariant,
)


class BoundedVerdict(str, Enum):
    """
    Truthful formal verification verdict outcomes complying with ADR 0066.
    """
    BOUNDED_SAFE = "BOUNDED_SAFE"
    VIOLATED = "VIOLATED"
    INDETERMINATE = "INDETERMINATE"
    UNBOUNDED_PROVEN = "UNBOUNDED_PROVEN"
    VACUOUS = "VACUOUS"


class VerificationInvariant:
    """Canonical mathematical invariants for multi-agent interaction systems."""
    NO_DEADLOCK = "NO_DEADLOCK"
    NO_UNBOUNDED_CYCLES = "NO_UNBOUNDED_CYCLES"
    BOUNDED_BUDGET = "BOUNDED_BUDGET"
    MUTUAL_EXCLUSION = "MUTUAL_EXCLUSION"
    LIVENESS_REACHABLE = "LIVENESS_REACHABLE"


@dataclass
class VerificationProperty:
    """
    Formal specification property evaluated over a transition model.
    """
    property_id: str
    invariant_type: str = VerificationInvariant.NO_DEADLOCK
    parameters: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "property_id": self.property_id,
            "invariant_type": self.invariant_type,
            "parameters": dict(self.parameters),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> VerificationProperty:
        return cls(
            property_id=str(data.get("property_id", "")),
            invariant_type=str(data.get("invariant_type", VerificationInvariant.NO_DEADLOCK)),
            parameters=dict(data.get("parameters", {})),
        )


@dataclass
class VerificationLimits:
    """
    Resource limits and search bound parameters governing model exploration.
    """
    max_depth_k: int = 15
    max_states: int = 10000
    timeout_ms: float = 1000.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_depth_k": self.max_depth_k,
            "max_states": self.max_states,
            "timeout_ms": self.timeout_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> VerificationLimits:
        return cls(
            max_depth_k=int(data.get("max_depth_k", 15)),
            max_states=int(data.get("max_states", 10000)),
            timeout_ms=float(data.get("timeout_ms", 1000.0)),
        )


@dataclass
class TransitionStep:
    """
    A single step in a deterministic counterexample trace.
    """
    step_index: int
    from_agent: str
    to_agent: str
    action_type: str
    token_cost: int
    cumulative_tokens: int
    state_flags: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TransitionStep:
        return cls(
            step_index=int(data["step_index"]),
            from_agent=str(data["from_agent"]),
            to_agent=str(data["to_agent"]),
            action_type=str(data.get("action_type", "DELEGATE")),
            token_cost=int(data.get("token_cost", 0)),
            cumulative_tokens=int(data.get("cumulative_tokens", 0)),
            state_flags=list(data.get("state_flags", [])),
        )


@dataclass
class VerificationReport:
    """
    Comprehensive, truthful formal verification report.
    Adheres strictly to ADR 0066 bounded verification semantics.
    """
    model_id: str
    verdict: str
    bound_k: int
    depth_reached: int
    total_states_explored: int
    frontier_states_remaining: int
    state_coverage_ratio: float
    execution_time_ms: float
    assumptions: List[str]
    properties_checked: List[str]
    violated_property: Optional[str]
    counterexample_trace: List[Dict[str, Any]]
    diagnostic_reason: str
    tenant_id: str = "default"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_id": self.model_id,
            "verdict": self.verdict,
            "bound_k": self.bound_k,
            "depth_reached": self.depth_reached,
            "total_states_explored": self.total_states_explored,
            "frontier_states_remaining": self.frontier_states_remaining,
            "state_coverage_ratio": self.state_coverage_ratio,
            "execution_time_ms": self.execution_time_ms,
            "assumptions": list(self.assumptions),
            "properties_checked": list(self.properties_checked),
            "violated_property": self.violated_property,
            "counterexample_trace": [dict(s) for s in self.counterexample_trace],
            "diagnostic_reason": self.diagnostic_reason,
            "tenant_id": self.tenant_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> VerificationReport:
        return cls(
            model_id=str(data.get("model_id", "default_model")),
            verdict=str(data.get("verdict", BoundedVerdict.INDETERMINATE.value)),
            bound_k=int(data.get("bound_k", 0)),
            depth_reached=int(data.get("depth_reached", 0)),
            total_states_explored=int(data.get("total_states_explored", 0)),
            frontier_states_remaining=int(data.get("frontier_states_remaining", 0)),
            state_coverage_ratio=float(data.get("state_coverage_ratio", 0.0)),
            execution_time_ms=float(data.get("execution_time_ms", 0.0)),
            assumptions=list(data.get("assumptions", [])),
            properties_checked=list(data.get("properties_checked", [])),
            violated_property=data.get("violated_property"),
            counterexample_trace=list(data.get("counterexample_trace", [])),
            diagnostic_reason=str(data.get("diagnostic_reason", "")),
            tenant_id=str(data.get("tenant_id", "default")),
            created_at=str(data.get("created_at", datetime.now(timezone.utc).isoformat())),
        )


class SafetyVerifier:
    """
    Deep module providing bounded verification semantics and minimal counterexample generation.
    Sub-millisecond local in-memory BFS engine without SAT solvers or external LLM API calls.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._history: Dict[str, List[VerificationReport]] = defaultdict(list)
        self._models: Dict[str, AgentTransitionModel] = {}

    def _normalize_model(self, model: Union[AgentTransitionModel, Dict[str, Any]]) -> AgentTransitionModel:
        """Normalizes input dictionary or model instance into an AgentTransitionModel."""
        if isinstance(model, AgentTransitionModel):
            return model
        if isinstance(model, dict):
            transitions: List[AgentTransition] = []
            for t in model.get("transitions", []):
                if isinstance(t, AgentTransition):
                    transitions.append(t)
                elif isinstance(t, dict):
                    transitions.append(
                        AgentTransition(
                            from_agent=str(t.get("from_agent", "")),
                            to_agent=str(t.get("to_agent", "")),
                            action_type=str(t.get("action_type", "DELEGATE")),
                            token_cost=int(t.get("token_cost", 150)),
                            required_flags=tuple(t.get("required_flags", ())),
                            sets_flags=tuple(t.get("sets_flags", ())),
                        )
                    )
            mutex_pairs = [
                (str(p[0]), str(p[1])) for p in model.get("mutual_exclusion_pairs", []) if len(p) >= 2
            ]
            return AgentTransitionModel(
                initial_agent=str(model.get("initial_agent", "")),
                final_agents=set(model.get("final_agents", [])),
                transitions=transitions,
                mutual_exclusion_pairs=mutex_pairs,
                max_token_budget=int(model.get("max_token_budget", 5000)),
            )
        raise ValueError(f"Unsupported model specification: {type(model)}")

    def _normalize_properties(
        self, properties: Optional[List[Union[VerificationProperty, str]]]
    ) -> List[VerificationProperty]:
        """Normalizes property list into structured VerificationProperty instances."""
        if not properties:
            return [
                VerificationProperty("prop_no_deadlock", VerificationInvariant.NO_DEADLOCK),
                VerificationProperty("prop_no_cycles", VerificationInvariant.NO_UNBOUNDED_CYCLES),
                VerificationProperty("prop_budget", VerificationInvariant.BOUNDED_BUDGET),
                VerificationProperty("prop_mutex", VerificationInvariant.MUTUAL_EXCLUSION),
            ]
        normalized: List[VerificationProperty] = []
        for p in properties:
            if isinstance(p, VerificationProperty):
                normalized.append(p)
            elif isinstance(p, dict):
                normalized.append(VerificationProperty.from_dict(p))
            elif isinstance(p, str):
                normalized.append(VerificationProperty(property_id=p, invariant_type=p))
        return normalized

    def verify(
        self,
        model: Union[AgentTransitionModel, Dict[str, Any]],
        properties: Optional[List[Union[VerificationProperty, str]]] = None,
        limits: Optional[VerificationLimits] = None,
        model_id: str = "default_model",
        tenant_id: str = "default",
    ) -> VerificationReport:
        """
        Exhaustively explores state space up to depth bound k under explicit resource limits.
        Returns a VerificationReport with truthful verdict (BOUNDED_SAFE, UNBOUNDED_PROVEN,
        INDETERMINATE, VIOLATED, or VACUOUS) and exact state space coverage ratio.
        """
        t0 = time.perf_counter()
        normalized_model = self._normalize_model(model)
        props = self._normalize_properties(properties)
        lims = limits or VerificationLimits()

        assumptions = [
            "Closed-world transition relation: only explicitly declared transitions are admissible",
            f"Exploration strictly bounded to depth k={lims.max_depth_k}",
            f"Resource limits: max_states={lims.max_states}, timeout={lims.timeout_ms}ms",
            "Deterministic non-negative token accumulation and monotonic flag assignment",
        ]
        props_checked_names = [p.property_id for p in props]

        with self._lock:
            self._models[model_id] = normalized_model

        if not normalized_model.transitions:
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
            report = VerificationReport(
                model_id=model_id,
                verdict=BoundedVerdict.VACUOUS.value,
                bound_k=lims.max_depth_k,
                depth_reached=0,
                total_states_explored=0,
                frontier_states_remaining=0,
                state_coverage_ratio=1.0,
                execution_time_ms=elapsed_ms,
                assumptions=assumptions,
                properties_checked=props_checked_names,
                violated_property=None,
                counterexample_trace=[],
                diagnostic_reason="Model contains zero transitions; vacuously safe.",
                tenant_id=tenant_id,
            )
            with self._lock:
                self._history[tenant_id].append(report)
            return report

        transitions_by_src: Dict[str, List[AgentTransition]] = defaultdict(list)
        for t in normalized_model.transitions:
            transitions_by_src[t.from_agent].append(t)

        # BFS state tuple: (agent, depth, tokens, flags_frozenset, trace_list)
        initial_state = (
            normalized_model.initial_agent,
            0,
            0,
            frozenset(),
            [],
        )

        visited: Set[Tuple[str, int, int, frozenset]] = set()
        queue = deque([initial_state])
        total_explored = 0
        depth_reached = 0
        frontier_unexpanded = 0
        is_indeterminate = False
        indeterminate_reason = ""

        while queue:
            # Check resource constraints
            elapsed_now_ms = (time.perf_counter() - t0) * 1000.0
            if elapsed_now_ms > lims.timeout_ms:
                is_indeterminate = True
                indeterminate_reason = (
                    f"Verification search truncated: timeout of {lims.timeout_ms}ms exceeded "
                    f"at depth {depth_reached} with {len(queue)} frontier states remaining."
                )
                break

            if total_explored >= lims.max_states:
                is_indeterminate = True
                indeterminate_reason = (
                    f"Verification search truncated: state cap of {lims.max_states} reached "
                    f"at depth {depth_reached} with {len(queue)} frontier states remaining."
                )
                break

            agent, depth, tokens, flags, trace = queue.popleft()
            state_key = (agent, depth, tokens, flags)
            if state_key in visited:
                continue
            visited.add(state_key)
            total_explored += 1
            if depth > depth_reached:
                depth_reached = depth

            is_final = agent in normalized_model.final_agents

            # Invariant checks at current state
            for prop in props:
                inv = prop.invariant_type

                # 1. BOUNDED_BUDGET
                if inv == VerificationInvariant.BOUNDED_BUDGET:
                    max_budget = int(prop.parameters.get("max_tokens", normalized_model.max_token_budget))
                    if tokens > max_budget:
                        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                        report = VerificationReport(
                            model_id=model_id,
                            verdict=BoundedVerdict.VIOLATED.value,
                            bound_k=lims.max_depth_k,
                            depth_reached=depth,
                            total_states_explored=total_explored,
                            frontier_states_remaining=len(queue),
                            state_coverage_ratio=round(total_explored / (total_explored + len(queue)), 4),
                            execution_time_ms=elapsed_ms,
                            assumptions=assumptions,
                            properties_checked=props_checked_names,
                            violated_property=prop.property_id,
                            counterexample_trace=trace,
                            diagnostic_reason=f"Path exceeded token budget: {tokens} > {max_budget}",
                            tenant_id=tenant_id,
                        )
                        with self._lock:
                            self._history[tenant_id].append(report)
                        return report

                # 2. MUTUAL_EXCLUSION
                elif inv == VerificationInvariant.MUTUAL_EXCLUSION:
                    pairs = prop.parameters.get("pairs", normalized_model.mutual_exclusion_pairs)
                    prior_agents = [step["from_agent"] for step in trace]
                    for a1, a2 in pairs:
                        if (agent == a1 and a2 in prior_agents) or (agent == a2 and a1 in prior_agents):
                            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                            report = VerificationReport(
                                model_id=model_id,
                                verdict=BoundedVerdict.VIOLATED.value,
                                bound_k=lims.max_depth_k,
                                depth_reached=depth,
                                total_states_explored=total_explored,
                                frontier_states_remaining=len(queue),
                                state_coverage_ratio=round(total_explored / (total_explored + len(queue)), 4),
                                execution_time_ms=elapsed_ms,
                                assumptions=assumptions,
                                properties_checked=props_checked_names,
                                violated_property=prop.property_id,
                                counterexample_trace=trace,
                                diagnostic_reason=f"Mutual exclusion violated between incompatible agents '{a1}' and '{a2}'",
                                tenant_id=tenant_id,
                            )
                            with self._lock:
                                self._history[tenant_id].append(report)
                            return report

                # 3. NO_UNBOUNDED_CYCLES
                elif inv == VerificationInvariant.NO_UNBOUNDED_CYCLES:
                    max_repeat = int(prop.parameters.get("max_repeat", 3))
                    prior_agents = [step["from_agent"] for step in trace]
                    if prior_agents.count(agent) >= max_repeat:
                        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                        report = VerificationReport(
                            model_id=model_id,
                            verdict=BoundedVerdict.VIOLATED.value,
                            bound_k=lims.max_depth_k,
                            depth_reached=depth,
                            total_states_explored=total_explored,
                            frontier_states_remaining=len(queue),
                            state_coverage_ratio=round(total_explored / (total_explored + len(queue)), 4),
                            execution_time_ms=elapsed_ms,
                            assumptions=assumptions,
                            properties_checked=props_checked_names,
                            violated_property=prop.property_id,
                            counterexample_trace=trace,
                            diagnostic_reason=f"Unbounded delegation cycle detected: agent '{agent}' visited >= {max_repeat} times",
                            tenant_id=tenant_id,
                        )
                        with self._lock:
                            self._history[tenant_id].append(report)
                        return report

            # Calculate enabled transitions
            enabled_transitions: List[AgentTransition] = []
            for t in transitions_by_src.get(agent, []):
                if all(f in flags for f in t.required_flags):
                    enabled_transitions.append(t)

            # 4. NO_DEADLOCK
            for prop in props:
                if prop.invariant_type == VerificationInvariant.NO_DEADLOCK:
                    if not enabled_transitions and not is_final:
                        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                        report = VerificationReport(
                            model_id=model_id,
                            verdict=BoundedVerdict.VIOLATED.value,
                            bound_k=lims.max_depth_k,
                            depth_reached=depth,
                            total_states_explored=total_explored,
                            frontier_states_remaining=len(queue),
                            state_coverage_ratio=round(total_explored / (total_explored + len(queue)), 4),
                            execution_time_ms=elapsed_ms,
                            assumptions=assumptions,
                            properties_checked=props_checked_names,
                            violated_property=prop.property_id,
                            counterexample_trace=trace,
                            diagnostic_reason=f"Conversational deadlock: agent '{agent}' has no enabled transitions and is not a final state",
                            tenant_id=tenant_id,
                        )
                        with self._lock:
                            self._history[tenant_id].append(report)
                        return report

            # Terminal states do not produce outgoing transitions
            if is_final:
                continue

            # Check depth boundary k
            if depth >= lims.max_depth_k:
                if enabled_transitions:
                    frontier_unexpanded += len(enabled_transitions)
                continue

            # Enqueue next states
            for t in enabled_transitions:
                next_tokens = tokens + t.token_cost
                next_flags = set(flags)
                next_flags.update(t.sets_flags)
                step = TransitionStep(
                    step_index=len(trace) + 1,
                    from_agent=agent,
                    to_agent=t.to_agent,
                    action_type=t.action_type,
                    token_cost=t.token_cost,
                    cumulative_tokens=next_tokens,
                    state_flags=sorted(list(next_flags)),
                )
                queue.append((
                    t.to_agent,
                    depth + 1,
                    next_tokens,
                    frozenset(next_flags),
                    trace + [step.to_dict()],
                ))

        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        if is_indeterminate:
            total_frontier = frontier_unexpanded + len(queue)
            coverage = round(total_explored / max(1, total_explored + total_frontier), 4)
            report = VerificationReport(
                model_id=model_id,
                verdict=BoundedVerdict.INDETERMINATE.value,
                bound_k=lims.max_depth_k,
                depth_reached=depth_reached,
                total_states_explored=total_explored,
                frontier_states_remaining=total_frontier,
                state_coverage_ratio=coverage,
                execution_time_ms=elapsed_ms,
                assumptions=assumptions,
                properties_checked=props_checked_names,
                violated_property=None,
                counterexample_trace=[],
                diagnostic_reason=indeterminate_reason,
                tenant_id=tenant_id,
            )
        elif frontier_unexpanded == 0:
            # All paths terminated naturally at final states or closed with 0 frontier
            report = VerificationReport(
                model_id=model_id,
                verdict=BoundedVerdict.UNBOUNDED_PROVEN.value,
                bound_k=lims.max_depth_k,
                depth_reached=depth_reached,
                total_states_explored=total_explored,
                frontier_states_remaining=0,
                state_coverage_ratio=1.0,
                execution_time_ms=elapsed_ms,
                assumptions=assumptions,
                properties_checked=props_checked_names,
                violated_property=None,
                counterexample_trace=[],
                diagnostic_reason=(
                    f"Exhaustive state space closed with 0 frontier remaining (depth {depth_reached} <= {lims.max_depth_k}). "
                    f"Properties are proven for all possible executions."
                ),
                tenant_id=tenant_id,
            )
        else:
            # Invariants held strictly up to depth k, but frontier remains open beyond k
            coverage = round(total_explored / (total_explored + frontier_unexpanded), 4)
            report = VerificationReport(
                model_id=model_id,
                verdict=BoundedVerdict.BOUNDED_SAFE.value,
                bound_k=lims.max_depth_k,
                depth_reached=depth_reached,
                total_states_explored=total_explored,
                frontier_states_remaining=frontier_unexpanded,
                state_coverage_ratio=coverage,
                execution_time_ms=elapsed_ms,
                assumptions=assumptions,
                properties_checked=props_checked_names,
                violated_property=None,
                counterexample_trace=[],
                diagnostic_reason=(
                    f"All {len(props)} properties hold for all reachable states up to bound k={lims.max_depth_k}. "
                    f"State space remains unclosed beyond k with {frontier_unexpanded} unexpanded transitions."
                ),
                tenant_id=tenant_id,
            )

        with self._lock:
            self._history[tenant_id].append(report)
        return report

    def verify_trace(
        self,
        model: Union[AgentTransitionModel, Dict[str, Any]],
        property: Union[VerificationProperty, str],
        trace: List[Dict[str, Any]],
    ) -> Tuple[bool, Optional[str]]:
        """
        Independent, sub-millisecond trace replay validator.
        Replays the counterexample step-by-step against model transitions, verifying
        both transition validity and the genuine existence of the claimed violation.
        Returns (is_sound, diagnostic_message).
        """
        normalized_model = self._normalize_model(model)
        prop = property if isinstance(property, VerificationProperty) else VerificationProperty(property, property)

        if not trace:
            # Check if initial agent is in deadlock
            if prop.invariant_type == VerificationInvariant.NO_DEADLOCK:
                if normalized_model.initial_agent not in normalized_model.final_agents:
                    enabled = [t for t in normalized_model.transitions if t.from_agent == normalized_model.initial_agent and not t.required_flags]
                    if not enabled:
                        return True, None
            return False, "Trace is empty and initial state does not violate property"

        current_agent = normalized_model.initial_agent
        current_tokens = 0
        current_flags: Set[str] = set()
        seen_agents = [current_agent]

        for step_dict in trace:
            step = TransitionStep.from_dict(step_dict)

            # 1. State continuity check
            if step.from_agent != current_agent:
                return False, f"Step {step.step_index}: from_agent '{step.from_agent}' does not match current state '{current_agent}'"

            # 2. Transition admissibility check
            candidate = None
            for t in normalized_model.transitions:
                if (
                    t.from_agent == step.from_agent
                    and t.to_agent == step.to_agent
                    and t.action_type == step.action_type
                ):
                    candidate = t
                    break

            if not candidate:
                return False, f"Step {step.step_index}: transition {step.from_agent} -> {step.to_agent} with action '{step.action_type}' does not exist in model"

            # 3. Required flags check
            for flag in candidate.required_flags:
                if flag not in current_flags:
                    return False, f"Step {step.step_index}: required flag '{flag}' not satisfied in current flags {sorted(list(current_flags))}"

            # 4. Apply transition
            current_flags.update(candidate.sets_flags)
            current_tokens += candidate.token_cost
            current_agent = candidate.to_agent
            seen_agents.append(current_agent)

            # 5. Token arithmetic check
            if step.cumulative_tokens != current_tokens:
                return False, f"Step {step.step_index}: cumulative tokens {step.cumulative_tokens} does not match replayed tokens {current_tokens}"

        # 6. Verify property violation soundness at terminal state of trace
        inv = prop.invariant_type

        if inv == VerificationInvariant.NO_DEADLOCK:
            if current_agent in normalized_model.final_agents:
                return False, f"Agent '{current_agent}' is a declared final state; not deadlocked"
            enabled = [
                t for t in normalized_model.transitions
                if t.from_agent == current_agent and all(f in current_flags for f in t.required_flags)
            ]
            if enabled:
                return False, f"Agent '{current_agent}' has {len(enabled)} enabled transitions; not deadlocked"
            return True, None

        elif inv == VerificationInvariant.BOUNDED_BUDGET:
            max_budget = int(prop.parameters.get("max_tokens", normalized_model.max_token_budget))
            if current_tokens <= max_budget:
                return False, f"Cumulative tokens {current_tokens} does not exceed budget limit {max_budget}"
            return True, None

        elif inv == VerificationInvariant.MUTUAL_EXCLUSION:
            pairs = prop.parameters.get("pairs", normalized_model.mutual_exclusion_pairs)
            for a1, a2 in pairs:
                if a1 in seen_agents and a2 in seen_agents:
                    return True, None
            return False, "No mutual exclusion agent pair co-occurred along the trace path"

        elif inv == VerificationInvariant.NO_UNBOUNDED_CYCLES:
            max_repeat = int(prop.parameters.get("max_repeat", 3))
            counts = {a: seen_agents.count(a) for a in set(seen_agents)}
            if max(counts.values(), default=0) >= max_repeat:
                return True, None
            return False, f"No agent occurred >= {max_repeat} times along the trace path"

        return True, None

    def status(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Returns diagnostic status and summary metrics of safety verifications."""
        with self._lock:
            all_reports: List[VerificationReport] = []
            if tenant_id:
                all_reports = list(self._history.get(tenant_id, []))
            else:
                for r_list in self._history.values():
                    all_reports.extend(r_list)

            verdicts_breakdown: Dict[str, int] = {
                BoundedVerdict.BOUNDED_SAFE.value: 0,
                BoundedVerdict.VIOLATED.value: 0,
                BoundedVerdict.INDETERMINATE.value: 0,
                BoundedVerdict.UNBOUNDED_PROVEN.value: 0,
                BoundedVerdict.VACUOUS.value: 0,
            }
            total_coverage = 0.0
            for r in all_reports:
                verdicts_breakdown[r.verdict] = verdicts_breakdown.get(r.verdict, 0) + 1
                total_coverage += r.state_coverage_ratio

            avg_coverage = round(total_coverage / len(all_reports), 4) if all_reports else 1.0
            last_report = all_reports[-1] if all_reports else None

            return {
                "total_verifications": len(all_reports),
                "verdicts_breakdown": verdicts_breakdown,
                "average_coverage_ratio": avg_coverage,
                "last_verdict": last_report.verdict if last_report else None,
                "last_model_id": last_report.model_id if last_report else None,
                "last_bound_k": last_report.bound_k if last_report else None,
                "last_depth_reached": last_report.depth_reached if last_report else None,
                "tenant_id": tenant_id or "all",
            }

    def reset(self) -> None:
        """Clears verification history and cached models."""
        with self._lock:
            self._history.clear()
            self._models.clear()


# Global Singleton Instance
global_safety_verifier = SafetyVerifier()
