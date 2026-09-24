# -*- coding: utf-8 -*-
"""
SwarmWatch Autonomous Dynamic Topology Healing & Auto-Patching (Seam 43 / ADR 0054).
Deep module synthesizing, applying, and reverifying graph-theoretic patches
to automatically resolve deadlocks, unbounded cycles, budget breaches,
and mutual exclusion conflicts with zero remote LLM API calls.
"""

from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
import threading
import time
import uuid
from typing import Dict, Any, List, Optional, Set, Tuple

from swarmwatch.model_checker import (
    VerificationInvariant,
    VerificationVerdict,
    AgentTransition,
    AgentTransitionModel,
    VerificationResult,
    BoundedModelChecker,
    global_model_checker,
)


class PatchAction(str, Enum):
    """Canonical graph repair action."""
    FALLBACK_REDIRECT = "FALLBACK_REDIRECT"
    CYCLE_GATE_BREAK = "CYCLE_GATE_BREAK"
    BUDGET_REWIRE = "BUDGET_REWIRE"
    MUTEX_MEDIATE = "MUTEX_MEDIATE"


@dataclass
class TopologyPatch:
    """An atomic graph mutation patch applied to an AgentTransitionModel."""
    patch_id: str
    swarm_id: str
    action: str
    target_agent: str
    rewired_to: str
    synthetic_transitions: List[AgentTransition] = field(default_factory=list)
    removed_transitions: List[AgentTransition] = field(default_factory=list)
    rationale: str = ""
    applied_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "ACTIVE"  # ACTIVE, REVERTED, PENDING

    def to_dict(self) -> Dict[str, Any]:
        return {
            "patch_id": self.patch_id,
            "swarm_id": self.swarm_id,
            "action": self.action,
            "target_agent": self.target_agent,
            "rewired_to": self.rewired_to,
            "synthetic_transitions": [asdict(t) for t in self.synthetic_transitions],
            "removed_transitions": [asdict(t) for t in self.removed_transitions],
            "rationale": self.rationale,
            "applied_at": self.applied_at,
            "status": self.status,
        }


@dataclass
class HealedModelReport:
    """Audit record certifying the synthesized patch and secondary BMC reverification."""
    swarm_id: str
    original_verdict: str
    violated_invariant: Optional[str]
    applied_patch: Optional[TopologyPatch]
    patched_model: Optional[Dict[str, Any]]
    healed_verdict: str
    execution_time_ms: float
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "swarm_id": self.swarm_id,
            "original_verdict": self.original_verdict,
            "violated_invariant": self.violated_invariant,
            "applied_patch": self.applied_patch.to_dict() if self.applied_patch else None,
            "patched_model": self.patched_model,
            "healed_verdict": self.healed_verdict,
            "execution_time_ms": self.execution_time_ms,
            "summary": self.summary,
        }


class TopologyAutoHealer:
    """
    Deep module that analyzes BMC invariant violations, synthesizes deterministic
    graph mutations (TopologyPatch), and proves safety through secondary BMC verification.
    """

    def __init__(self, checker: Optional[BoundedModelChecker] = None) -> None:
        self._checker = checker or global_model_checker
        self._patches: Dict[str, List[TopologyPatch]] = defaultdict(list)
        self._original_models: Dict[str, AgentTransitionModel] = {}
        self._healed_models: Dict[str, AgentTransitionModel] = {}
        self._reports: Dict[str, List[HealedModelReport]] = defaultdict(list)
        self._lock = threading.RLock()

    def synthesize_patch(
        self,
        model: AgentTransitionModel,
        verification: VerificationResult,
    ) -> Optional[TopologyPatch]:
        """
        Synthesizes an optimal, deterministic TopologyPatch addressing the specific
        invariant failure identified by the BoundedModelChecker.
        """
        if verification.verdict != VerificationVerdict.VIOLATED:
            return None

        swarm_id = verification.swarm_id
        patch_id = f"patch-{uuid.uuid4().hex[:8]}"
        violation = verification.violated_invariant

        # Default recovery destination
        default_target = (
            sorted(list(model.final_agents))[0]
            if model.final_agents
            else "agent-supervisor"
        )

        # 1. NO_DEADLOCK: deadlocked agent has no transitions and is not final
        if violation == VerificationInvariant.NO_DEADLOCK:
            deadlocked_agent = model.initial_agent
            if verification.counterexample:
                deadlocked_agent = verification.counterexample[-1].get("to_agent") or model.initial_agent

            synthetic_edge = AgentTransition(
                from_agent=deadlocked_agent,
                to_agent=default_target,
                action_type="FALLBACK_RECOVERY",
                token_cost=50,
            )
            return TopologyPatch(
                patch_id=patch_id,
                swarm_id=swarm_id,
                action=PatchAction.FALLBACK_REDIRECT,
                target_agent=deadlocked_agent,
                rewired_to=default_target,
                synthetic_transitions=[synthetic_edge],
                rationale=f"Injected fallback recovery edge from deadlocked '{deadlocked_agent}' to terminal '{default_target}'.",
            )

        # 2. NO_UNBOUNDED_CYCLES: circular delegation loop detected
        elif violation == VerificationInvariant.NO_UNBOUNDED_CYCLES:
            cyclic_agent = model.initial_agent
            if verification.counterexample:
                cyclic_agent = verification.counterexample[-1].get("to_agent") or model.initial_agent

            # Inject direct exit edge to terminal agent with progress counter flag
            exit_edge = AgentTransition(
                from_agent=cyclic_agent,
                to_agent=default_target,
                action_type="CYCLE_BREAK_EXIT",
                token_cost=40,
                sets_flags=["CYCLE_TERMINATED"],
            )
            # Find and prune or decouple the recursive return edge
            removed = []
            for t in model.transitions:
                if t.from_agent == cyclic_agent and t.to_agent != default_target:
                    removed.append(t)
                    break

            return TopologyPatch(
                patch_id=patch_id,
                swarm_id=swarm_id,
                action=PatchAction.CYCLE_GATE_BREAK,
                target_agent=cyclic_agent,
                rewired_to=default_target,
                synthetic_transitions=[exit_edge],
                removed_transitions=removed,
                rationale=f"Decoupled cyclic delegation at '{cyclic_agent}' and injected termination exit to '{default_target}'.",
            )

        # 3. BOUNDED_BUDGET: cumulative tokens exceed budget
        elif violation == VerificationInvariant.BOUNDED_BUDGET:
            # Locate the greedy transition along counterexample that breached quota
            breach_step = verification.counterexample[-1] if verification.counterexample else None
            greedy_src = breach_step.get("from_agent") if breach_step else model.initial_agent
            greedy_dst = breach_step.get("to_agent") if breach_step else default_target

            # Replace greedy transition with a compressed summary transition
            compressed_edge = AgentTransition(
                from_agent=greedy_src,
                to_agent=greedy_dst,
                action_type="COMPRESSED_SUMMARY",
                token_cost=max(10, model.max_token_budget // 10),
            )
            removed = [
                t for t in model.transitions
                if t.from_agent == greedy_src and t.to_agent == greedy_dst
            ]

            return TopologyPatch(
                patch_id=patch_id,
                swarm_id=swarm_id,
                action=PatchAction.BUDGET_REWIRE,
                target_agent=greedy_src,
                rewired_to=greedy_dst,
                synthetic_transitions=[compressed_edge],
                removed_transitions=removed,
                rationale=f"Throttled token consumption between '{greedy_src}' and '{greedy_dst}' with compressed summary.",
            )

        # 4. MUTUAL_EXCLUSION: incompatible agents executed concurrently
        elif violation == VerificationInvariant.MUTUAL_EXCLUSION:
            # Find conflicting agent pair
            pair = model.mutual_exclusion_pairs[0] if model.mutual_exclusion_pairs else ("executor", "verifier")
            a1, a2 = pair
            mediator_node = f"agent-mediator-{a1[:4]}"

            # Decouple direct edge between conflicting pair with an air-gapped mediator
            synthetic_edges = [
                AgentTransition(from_agent=a1, to_agent=mediator_node, action_type="DELEGATE_MEDIATED", token_cost=30),
                AgentTransition(from_agent=mediator_node, to_agent=default_target, action_type="MEDIATED_FINISH", token_cost=30),
            ]
            removed = [
                t for t in model.transitions
                if (t.from_agent == a1 and t.to_agent == a2) or (t.from_agent == a2 and t.to_agent == a1)
            ]

            return TopologyPatch(
                patch_id=patch_id,
                swarm_id=swarm_id,
                action=PatchAction.MUTEX_MEDIATE,
                target_agent=a1,
                rewired_to=mediator_node,
                synthetic_transitions=synthetic_edges,
                removed_transitions=removed,
                rationale=f"Air-gapped conflicting role pair ('{a1}', '{a2}') via intermediate '{mediator_node}'.",
            )

        return None

    def apply_patch(
        self,
        model: AgentTransitionModel,
        patch: TopologyPatch,
    ) -> AgentTransitionModel:
        """Applies a TopologyPatch to an AgentTransitionModel, returning a mutated clone."""
        new_transitions: List[AgentTransition] = []

        # Filter out removed transitions
        removed_keys = {(t.from_agent, t.to_agent, t.action_type) for t in patch.removed_transitions}
        for t in model.transitions:
            key = (t.from_agent, t.to_agent, t.action_type)
            if key not in removed_keys:
                new_transitions.append(t)

        # Add synthetic transitions
        for st in patch.synthetic_transitions:
            new_transitions.append(st)

        # Ensure final agents include rewired targets if they were intended as terminal
        new_final = set(model.final_agents)
        if patch.rewired_to not in new_final and patch.action in (PatchAction.FALLBACK_REDIRECT, PatchAction.CYCLE_GATE_BREAK):
            new_final.add(patch.rewired_to)

        # Clone and return
        return AgentTransitionModel(
            initial_agent=model.initial_agent,
            final_agents=new_final,
            transitions=new_transitions,
            max_token_budget=model.max_token_budget,
            mutual_exclusion_pairs=list(model.mutual_exclusion_pairs),
        )

    def heal_and_verify(
        self,
        model: AgentTransitionModel,
        bound_k: int = 10,
        swarm_id: str = "default-swarm",
    ) -> HealedModelReport:
        """
        Executes the complete self-healing cycle:
        1. Runs BMC verification on original model.
        2. If violated, synthesizes optimal graph patch.
        3. Applies patch and runs secondary BMC verification to certify compliance.
        4. Records patch and audit report.
        """
        t0 = time.perf_counter()

        with self._lock:
            self._original_models[swarm_id] = model

        # Step 1: Initial verification
        initial_res = self._checker.verify(model, bound_k=bound_k, swarm_id=swarm_id)

        if initial_res.verdict == VerificationVerdict.VERIFIED:
            elapsed = (time.perf_counter() - t0) * 1000.0
            report = HealedModelReport(
                swarm_id=swarm_id,
                original_verdict=initial_res.verdict,
                violated_invariant=None,
                applied_patch=None,
                patched_model=model.to_dict(),
                healed_verdict=VerificationVerdict.VERIFIED,
                execution_time_ms=round(elapsed, 3),
                summary="Model topology is already compliant; zero auto-patches required.",
            )
            with self._lock:
                self._reports[swarm_id].append(report)
            return report

        # Step 2: Synthesize patch
        patch = self.synthesize_patch(model, initial_res)
        if not patch:
            elapsed = (time.perf_counter() - t0) * 1000.0
            report = HealedModelReport(
                swarm_id=swarm_id,
                original_verdict=initial_res.verdict,
                violated_invariant=initial_res.violated_invariant,
                applied_patch=None,
                patched_model=None,
                healed_verdict=VerificationVerdict.VIOLATED,
                execution_time_ms=round(elapsed, 3),
                summary=f"Failed to synthesize patch for invariant: {initial_res.violated_invariant}",
            )
            with self._lock:
                self._reports[swarm_id].append(report)
            return report

        # Step 3: Apply patch
        healed_model = self.apply_patch(model, patch)

        # Step 4: Secondary verification
        healed_res = self._checker.verify(healed_model, bound_k=bound_k, swarm_id=swarm_id)
        elapsed = (time.perf_counter() - t0) * 1000.0

        report = HealedModelReport(
            swarm_id=swarm_id,
            original_verdict=initial_res.verdict,
            violated_invariant=initial_res.violated_invariant,
            applied_patch=patch,
            patched_model=healed_model.to_dict(),
            healed_verdict=healed_res.verdict,
            execution_time_ms=round(elapsed, 3),
            summary=(
                f"Auto-healed {initial_res.violated_invariant} via {patch.action}. "
                f"Secondary BMC verification: {healed_res.verdict}."
            ),
        )

        with self._lock:
            self._patches[swarm_id].append(patch)
            self._healed_models[swarm_id] = healed_model
            self._reports[swarm_id].append(report)

        return report

    def revert_patch(self, swarm_id: str, patch_id: Optional[str] = None) -> bool:
        """Reverts active patches for the specified swarm."""
        with self._lock:
            patches = self._patches.get(swarm_id, [])
            if not patches:
                return False

            if patch_id:
                for p in patches:
                    if p.patch_id == patch_id and p.status == "ACTIVE":
                        p.status = "REVERTED"
                        return True
                return False
            else:
                for p in patches:
                    p.status = "REVERTED"
                if swarm_id in self._original_models:
                    self._healed_models[swarm_id] = self._original_models[swarm_id]
                return True

    def get_active_patches(self, swarm_id: str) -> List[TopologyPatch]:
        """Returns all currently active patches for a swarm."""
        with self._lock:
            return [p for p in self._patches.get(swarm_id, []) if p.status == "ACTIVE"]

    def get_latest_report(self, swarm_id: str) -> Optional[HealedModelReport]:
        """Returns the most recent healing report for a swarm."""
        with self._lock:
            history = self._reports.get(swarm_id, [])
            return history[-1] if history else None

    def reset(self, swarm_id: Optional[str] = None) -> None:
        """Clears healing state, patches, and reports."""
        with self._lock:
            if swarm_id:
                if swarm_id in self._patches:
                    del self._patches[swarm_id]
                if swarm_id in self._original_models:
                    del self._original_models[swarm_id]
                if swarm_id in self._healed_models:
                    del self._healed_models[swarm_id]
                if swarm_id in self._reports:
                    del self._reports[swarm_id]
            else:
                self._patches.clear()
                self._original_models.clear()
                self._healed_models.clear()
                self._reports.clear()


# Global singleton instance
global_topology_healer = TopologyAutoHealer()
