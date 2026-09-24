# -*- coding: utf-8 -*-
"""
SwarmWatch Adaptive Dynamic Quota Re-balancing Engine.
Continuously evaluates multi-agent utility combining semantic convergence,
lexical diversity, Shannon entropy stability, and fault resilience to dynamically
re-allocate token and turn quotas across distributed swarms (ADR 0046).
"""

from datetime import datetime, timezone
from enum import Enum
import math
import threading
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field


class AgentTier(str, Enum):
    """Operational classification tiers governing agent quota eligibility."""
    BOOSTED = "boosted"
    NOMINAL = "nominal"
    THROTTLED = "throttled"
    QUARANTINED = "quarantined"


class RebalancePolicy(BaseModel):
    """Configuration parameters governing utility calculation and rebalancing dynamics."""
    weight_drift: float = Field(default=0.45, ge=0.0, le=1.0)
    weight_vcr: float = Field(default=0.25, ge=0.0, le=1.0)
    weight_entropy: float = Field(default=0.20, ge=0.0, le=1.0)
    weight_fault: float = Field(default=0.10, ge=0.0, le=1.0)
    target_entropy: float = Field(default=0.75, ge=0.1, le=1.0)
    sensitivity_exponent: float = Field(default=1.5, ge=1.0, le=3.0)
    safety_floor_tokens: int = Field(default=500, ge=0)
    surge_threshold_ratio: float = Field(default=0.85, ge=0.5, le=0.99)
    epoch_interval_turns: int = Field(default=5, ge=1)


class AgentState(BaseModel):
    """Internal mutable tracking state for an individual agent in a swarm."""
    agent_id: str
    swarm_id: str
    total_tokens: int = 0
    total_turns: int = 0
    total_faults: int = 0
    last_drift_score: float = 0.0
    last_vcr: float = 1.0
    last_entropy: float = 0.75
    utility_score: float = 1.0
    tier: AgentTier = AgentTier.NOMINAL
    allocated_tokens: int = 0
    consumed_tokens: int = 0
    surge_granted_tokens: int = 0
    is_throttled: bool = False
    is_quarantined: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AgentAllocation(BaseModel):
    """Immutable public view of an agent's current allocation and operational tier."""
    agent_id: str
    tier: AgentTier
    utility_score: float
    allocated_tokens: int
    consumed_tokens: int
    remaining_tokens: int
    surge_granted_tokens: int
    is_throttled: bool
    is_quarantined: bool
    utilization_ratio: float


class RebalancePlan(BaseModel):
    """An immutable allocation specification generated during a rebalancing epoch."""
    epoch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    swarm_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_fleet_quota: int
    total_consumed_tokens: int
    reclaimed_buffer_tokens: int
    allocations: Dict[str, AgentAllocation]
    justification: str


class AdaptiveQuotaRebalancer:
    """
    In-process, zero-external-dependency, zero-token-cost mathematical engine
    evaluating agent utility and executing adaptive dynamic quota re-allocation
    across distributed multi-agent swarms.
    """

    def __init__(self, policy: Optional[RebalancePolicy] = None):
        self.policy = policy or RebalancePolicy()
        self._lock = threading.RLock()
        # swarm_id -> agent_id -> AgentState
        self._swarms: Dict[str, Dict[str, AgentState]] = {}
        # swarm_id -> total fleet quota
        self._fleet_quotas: Dict[str, int] = {}
        # swarm_id -> reclaimed buffer
        self._reclaimed_buffers: Dict[str, int] = {}
        # swarm_id -> latest RebalancePlan
        self._latest_plans: Dict[str, RebalancePlan] = {}
        # swarm_id -> turn counter for epoch triggering
        self._swarm_turns: Dict[str, int] = {}

    def set_fleet_quota(self, swarm_id: str, total_tokens: int) -> None:
        """Sets the baseline global token quota for a swarm."""
        with self._lock:
            self._fleet_quotas[swarm_id] = total_tokens
            if swarm_id not in self._reclaimed_buffers:
                self._reclaimed_buffers[swarm_id] = 0
            if swarm_id not in self._swarm_turns:
                self._swarm_turns[swarm_id] = 0

    def register_agent(
        self,
        swarm_id: str,
        agent_id: str,
        initial_tokens: Optional[int] = None,
    ) -> AgentAllocation:
        """Registers a new agent within a swarm with initial baseline quota."""
        with self._lock:
            agents = self._swarms.setdefault(swarm_id, {})
            if agent_id not in agents:
                init_quota = (
                    initial_tokens
                    if initial_tokens is not None
                    else self.policy.safety_floor_tokens * 4
                )
                state = AgentState(
                    agent_id=agent_id,
                    swarm_id=swarm_id,
                    allocated_tokens=init_quota,
                )
                agents[agent_id] = state

            return self._to_allocation(agents[agent_id])

    def record_turn(
        self,
        swarm_id: str,
        agent_id: str,
        tokens: int,
        drift_score: float = 0.0,
        vcr: float = 1.0,
        entropy: float = 0.75,
        is_fault: bool = False,
    ) -> AgentAllocation:
        """
        Records turn execution metrics for an agent, evaluates real-time utility,
        and optionally triggers an epoch rebalance if the turn interval is met.
        """
        with self._lock:
            agents = self._swarms.setdefault(swarm_id, {})
            state = agents.get(agent_id)
            if not state:
                state = AgentState(
                    agent_id=agent_id,
                    swarm_id=swarm_id,
                    allocated_tokens=self.policy.safety_floor_tokens * 4,
                )
                agents[agent_id] = state

            state.total_tokens += tokens
            state.consumed_tokens += tokens
            state.total_turns += 1
            if is_fault:
                state.total_faults += 1

            state.last_drift_score = max(0.0, min(1.0, drift_score))
            state.last_vcr = max(0.0, min(1.0, vcr))
            state.last_entropy = max(0.0, min(2.5, entropy))
            state.updated_at = datetime.now(timezone.utc)

            # Recalculate utility and tier
            state.utility_score = self.evaluate_utility(
                drift_score=state.last_drift_score,
                vcr=state.last_vcr,
                entropy=state.last_entropy,
                fault_rate=(state.total_faults / max(1, state.total_turns)),
            )
            state.tier = self._classify_tier(state.utility_score)
            state.is_throttled = state.tier in (AgentTier.THROTTLED, AgentTier.QUARANTINED)
            state.is_quarantined = state.tier == AgentTier.QUARANTINED

            # Check if surge is needed for boosted agent approaching limit
            if (
                state.tier == AgentTier.BOOSTED
                and state.allocated_tokens > 0
                and (state.consumed_tokens / state.allocated_tokens) >= self.policy.surge_threshold_ratio
            ):
                self.request_surge(swarm_id, agent_id)

            # Track turn counter for epoch rebalancing
            current_turns = self._swarm_turns.get(swarm_id, 0) + 1
            self._swarm_turns[swarm_id] = current_turns
            if current_turns % self.policy.epoch_interval_turns == 0:
                self.compute_rebalance(swarm_id)

            return self._to_allocation(state)

    def evaluate_utility(
        self,
        drift_score: float,
        vcr: float,
        entropy: float,
        fault_rate: float,
    ) -> float:
        """
        Pure mathematical composite utility function U_a in [0.0, 1.0].
        U_a = w_d*(1 - drift) + w_v*VCR + w_e*E - w_f*fault
        """
        drift_inv = 1.0 - max(0.0, min(1.0, drift_score))
        vcr_term = max(0.0, min(1.0, vcr))

        # Normalized entropy alignment score centered around target_entropy
        diff = abs(entropy - self.policy.target_entropy)
        entropy_term = max(0.0, 1.0 - (diff / max(0.1, self.policy.target_entropy)))

        fault_term = max(0.0, min(1.0, fault_rate))

        utility = (
            self.policy.weight_drift * drift_inv
            + self.policy.weight_vcr * vcr_term
            + self.policy.weight_entropy * entropy_term
            - self.policy.weight_fault * fault_term
        )
        return max(0.0, min(1.0, utility))

    def _classify_tier(self, utility: float) -> AgentTier:
        """Classifies agent into one of 4 operational tiers based on utility score."""
        if utility >= 0.70:
            return AgentTier.BOOSTED
        elif utility >= 0.40:
            return AgentTier.NOMINAL
        elif utility >= 0.20:
            return AgentTier.THROTTLED
        else:
            return AgentTier.QUARANTINED

    def compute_rebalance(
        self,
        swarm_id: str,
        justification: Optional[str] = None,
    ) -> RebalancePlan:
        """
        Executes a dynamic quota rebalancing epoch across all active agents in a swarm:
        1. Classifies each agent into operational tiers.
        2. Reclaims unspent quota from throttled and quarantined agents into reclaimed_buffer.
        3. Re-distributes remaining budget proportionally based on non-linear utility weights (U_a^kappa).
        4. Guarantees safety floor tokens for active non-quarantined agents.
        """
        with self._lock:
            agents = self._swarms.get(swarm_id, {})
            fleet_quota = self._fleet_quotas.get(swarm_id, 100_000)
            reclaimed_buf = self._reclaimed_buffers.get(swarm_id, 0)

            if not agents:
                plan = RebalancePlan(
                    swarm_id=swarm_id,
                    total_fleet_quota=fleet_quota,
                    total_consumed_tokens=0,
                    reclaimed_buffer_tokens=reclaimed_buf,
                    allocations={},
                    justification=justification or "Empty swarm rebalance",
                )
                self._latest_plans[swarm_id] = plan
                return plan

            total_consumed = sum(a.consumed_tokens for a in agents.values())
            remaining_fleet = max(0, fleet_quota - total_consumed)

            # Step 1: Update tiers and reclaim from throttled/quarantined
            active_candidates: List[AgentState] = []
            for agent in agents.values():
                agent.utility_score = self.evaluate_utility(
                    drift_score=agent.last_drift_score,
                    vcr=agent.last_vcr,
                    entropy=agent.last_entropy,
                    fault_rate=(agent.total_faults / max(1, agent.total_turns)),
                )
                agent.tier = self._classify_tier(agent.utility_score)
                agent.is_throttled = agent.tier in (AgentTier.THROTTLED, AgentTier.QUARANTINED)
                agent.is_quarantined = agent.tier == AgentTier.QUARANTINED

                if agent.is_quarantined:
                    # Reclaim any unconsumed allocation
                    unspent = max(0, agent.allocated_tokens - agent.consumed_tokens)
                    if unspent > 0:
                        reclaimed_buf += unspent
                        agent.allocated_tokens = agent.consumed_tokens
                elif agent.tier == AgentTier.THROTTLED:
                    # Cap at safety floor + consumed tokens
                    target_cap = agent.consumed_tokens + self.policy.safety_floor_tokens
                    if agent.allocated_tokens > target_cap:
                        reclaimed_buf += (agent.allocated_tokens - target_cap)
                        agent.allocated_tokens = target_cap
                else:
                    active_candidates.append(agent)

            # Step 2: Calculate distributable pool
            # Combine remaining fleet quota and reclaimed buffer
            available_pool = remaining_fleet + reclaimed_buf
            safety_total = len(active_candidates) * self.policy.safety_floor_tokens
            distributable = max(0, available_pool - safety_total)

            # Step 3: Compute non-linear weights (U_a^kappa)
            kappa = self.policy.sensitivity_exponent
            weights: Dict[str, float] = {}
            for agent in active_candidates:
                weights[agent.agent_id] = math.pow(max(0.001, agent.utility_score), kappa)

            sum_weights = sum(weights.values()) or 1.0

            # Step 4: Re-allocate to active candidates
            allocated_from_dist = 0
            for agent in active_candidates:
                fraction = weights[agent.agent_id] / sum_weights
                share = int(distributable * fraction)
                new_future_tokens = self.policy.safety_floor_tokens + share
                agent.allocated_tokens = agent.consumed_tokens + new_future_tokens
                allocated_from_dist += share

            # Remaining unallocated fractional tokens stay in buffer
            reclaimed_buf = max(0, distributable - allocated_from_dist)
            self._reclaimed_buffers[swarm_id] = reclaimed_buf

            # Build Plan
            allocations = {
                aid: self._to_allocation(state)
                for aid, state in agents.items()
            }
            plan = RebalancePlan(
                swarm_id=swarm_id,
                total_fleet_quota=fleet_quota,
                total_consumed_tokens=total_consumed,
                reclaimed_buffer_tokens=reclaimed_buf,
                allocations=allocations,
                justification=justification or f"Adaptive epoch rebalance across {len(agents)} agents",
            )
            self._latest_plans[swarm_id] = plan
            return plan

    def request_surge(
        self,
        swarm_id: str,
        agent_id: str,
        requested_tokens: Optional[int] = None,
    ) -> int:
        """
        Grants a temporary surge slice to a converging BOOSTED agent from the
        reclaimed buffer without expanding the global fleet limit.
        Returns the number of tokens granted.
        """
        with self._lock:
            agents = self._swarms.get(swarm_id, {})
            agent = agents.get(agent_id)
            if not agent or agent.tier != AgentTier.BOOSTED:
                return 0

            reclaimed = self._reclaimed_buffers.get(swarm_id, 0)
            if reclaimed <= 0:
                return 0

            # Default surge is 50% of available buffer or requested
            ask = (
                requested_tokens
                if requested_tokens is not None
                else max(self.policy.safety_floor_tokens, reclaimed // 2)
            )
            grant = min(ask, reclaimed)

            if grant > 0:
                agent.allocated_tokens += grant
                agent.surge_granted_tokens += grant
                self._reclaimed_buffers[swarm_id] = reclaimed - grant

            return grant

    def get_plan(self, swarm_id: str) -> Optional[RebalancePlan]:
        """Returns the most recent rebalance plan for a swarm."""
        with self._lock:
            return self._latest_plans.get(swarm_id)

    def get_allocations(self, swarm_id: str) -> Dict[str, AgentAllocation]:
        """Returns the current allocation state for all agents in a swarm."""
        with self._lock:
            agents = self._swarms.get(swarm_id, {})
            return {
                aid: self._to_allocation(state)
                for aid, state in agents.items()
            }

    def get_agent_quota(self, swarm_id: str, agent_id: str) -> Optional[AgentAllocation]:
        """Returns allocation details for a specific agent."""
        with self._lock:
            agents = self._swarms.get(swarm_id, {})
            state = agents.get(agent_id)
            if state:
                return self._to_allocation(state)
            return None

    def get_aggregate_stats(self, swarm_id: str) -> Dict[str, Any]:
        """Returns summary statistics for the rebalancer dashboard and API."""
        with self._lock:
            agents = self._swarms.get(swarm_id, {})
            fleet_quota = self._fleet_quotas.get(swarm_id, 100_000)
            reclaimed_buf = self._reclaimed_buffers.get(swarm_id, 0)
            plan = self._latest_plans.get(swarm_id)

            tier_counts = {
                AgentTier.BOOSTED.value: 0,
                AgentTier.NOMINAL.value: 0,
                AgentTier.THROTTLED.value: 0,
                AgentTier.QUARANTINED.value: 0,
            }
            total_consumed = 0
            for a in agents.values():
                tier_counts[a.tier.value] += 1
                total_consumed += a.consumed_tokens

            return {
                "swarm_id": swarm_id,
                "total_agents": len(agents),
                "fleet_quota_tokens": fleet_quota,
                "total_consumed_tokens": total_consumed,
                "reclaimed_buffer_tokens": reclaimed_buf,
                "tier_distribution": tier_counts,
                "last_epoch_id": plan.epoch_id if plan else None,
                "last_epoch_timestamp": plan.timestamp.isoformat() if plan else None,
            }

    def reset(self, swarm_id: str) -> None:
        """Resets rebalancer state for a swarm."""
        with self._lock:
            self._swarms.pop(swarm_id, None)
            self._fleet_quotas.pop(swarm_id, None)
            self._reclaimed_buffers.pop(swarm_id, None)
            self._latest_plans.pop(swarm_id, None)
            self._swarm_turns.pop(swarm_id, None)

    def _to_allocation(self, state: AgentState) -> AgentAllocation:
        remaining = max(0, state.allocated_tokens - state.consumed_tokens)
        ratio = (
            state.consumed_tokens / state.allocated_tokens
            if state.allocated_tokens > 0
            else 1.0
        )
        return AgentAllocation(
            agent_id=state.agent_id,
            tier=state.tier,
            utility_score=round(state.utility_score, 4),
            allocated_tokens=state.allocated_tokens,
            consumed_tokens=state.consumed_tokens,
            remaining_tokens=remaining,
            surge_granted_tokens=state.surge_granted_tokens,
            is_throttled=state.is_throttled,
            is_quarantined=state.is_quarantined,
            utilization_ratio=round(ratio, 4),
        )


# Global singleton instance
global_rebalancer = AdaptiveQuotaRebalancer()
