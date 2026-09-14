# -*- coding: utf-8 -*-
"""
SwarmWatch Distributed Fleet Circuit Coordinator.
Enables cross-worker state synchronization, fleet-wide token and turn quota aggregation,
and instantaneous halt broadcasting across multi-worker multi-agent swarms (ADR 0011).
"""

from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
import threading
from typing import Callable, Dict, List, Optional, Set
import uuid
from pydantic import BaseModel, Field

from swarmwatch.circuit import CircuitPolicy, SwarmCircuitBreakException

logger = logging.getLogger("swarmwatch.fleet")


class FleetMetrics(BaseModel):
    swarm_id: str
    total_tokens: int = 0
    total_cost: float = 0.0
    total_turns: int = 0
    active_workers: Set[str] = Field(default_factory=set)
    is_tripped: bool = False
    trip_reason: Optional[str] = None
    tripped_by: Optional[str] = None
    tripped_at: Optional[datetime] = None


class FleetEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    swarm_id: str
    event_type: str  # "metric_update" | "trip_broadcast" | "worker_joined" | "worker_left"
    worker_id: str
    payload: Dict = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FleetCircuitCoordinator:
    """
    Fleet-wide Circuit Coordinator for multi-process and distributed agent swarms.
    Aggregates metrics across N workers and instantaneously halts all fleet workers
    when global budget ceilings or entropy anomalies are detected.
    """

    def __init__(self, policy: Optional[CircuitPolicy] = None):
        self.policy = policy or CircuitPolicy()
        self._lock = threading.RLock()
        self._fleet_metrics: Dict[str, FleetMetrics] = {}
        self._worker_metrics: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self._subscribers: List[Callable[[FleetEvent], None]] = []

    def register_worker(self, worker_id: str, swarm_id: str) -> None:
        with self._lock:
            if swarm_id not in self._fleet_metrics:
                self._fleet_metrics[swarm_id] = FleetMetrics(swarm_id=swarm_id)
            self._fleet_metrics[swarm_id].active_workers.add(worker_id)
            self._emit_event(FleetEvent(
                swarm_id=swarm_id,
                event_type="worker_joined",
                worker_id=worker_id,
            ))

    def unregister_worker(self, worker_id: str, swarm_id: str) -> None:
        with self._lock:
            if swarm_id in self._fleet_metrics:
                self._fleet_metrics[swarm_id].active_workers.discard(worker_id)
            self._emit_event(FleetEvent(
                swarm_id=swarm_id,
                event_type="worker_left",
                worker_id=worker_id,
            ))

    def record_turn(
        self,
        worker_id: str,
        swarm_id: str,
        tokens: int = 0,
        cost: Optional[float] = None,
        turns: int = 1,
    ) -> FleetMetrics:
        """
        Records worker turn execution, updates cumulative fleet metrics,
        and evaluates fleet quota thresholds. Raises SwarmCircuitBreakException
        if the fleet-wide budget or turn limit is breached.
        """
        with self._lock:
            metrics = self._fleet_metrics.setdefault(swarm_id, FleetMetrics(swarm_id=swarm_id))
            metrics.active_workers.add(worker_id)

            if metrics.is_tripped:
                raise SwarmCircuitBreakException(
                    reason=f"Fleet Circuit Breaker TRIPPED for swarm '{swarm_id}': {metrics.trip_reason} (tripped by {metrics.tripped_by})",
                    metric_val=float(metrics.total_turns),
                    agent_id=worker_id,
                    swarm_id=swarm_id,
                )

            # Compute turn cost
            turn_cost = cost if cost is not None else (tokens * self.policy.cost_per_token)

            # Update worker cumulative stats
            worker_stat = self._worker_metrics[swarm_id].setdefault(worker_id, {"tokens": 0, "cost": 0.0, "turns": 0})
            worker_stat["tokens"] += tokens
            worker_stat["cost"] += turn_cost
            worker_stat["turns"] += turns

            # Update fleet aggregates
            metrics.total_tokens += tokens
            metrics.total_cost += turn_cost
            metrics.total_turns += turns

            # Check fleet-wide ceilings
            if self.policy.max_turns is not None and metrics.total_turns >= self.policy.max_turns:
                self.trip(
                    swarm_id=swarm_id,
                    reason=f"Fleet max turns quota reached ({metrics.total_turns}/{self.policy.max_turns})",
                    tripped_by=worker_id
                )
                raise SwarmCircuitBreakException(
                    reason=f"Fleet Circuit Breaker TRIPPED: max turns quota reached ({metrics.total_turns}/{self.policy.max_turns})",
                    metric_val=float(metrics.total_turns),
                    agent_id=worker_id,
                    swarm_id=swarm_id,
                )

            if self.policy.token_budget is not None and metrics.total_tokens >= self.policy.token_budget:
                self.trip(
                    swarm_id=swarm_id,
                    reason=f"Fleet token budget exceeded ({metrics.total_tokens}/{self.policy.token_budget})",
                    tripped_by=worker_id
                )
                raise SwarmCircuitBreakException(
                    reason=f"Fleet Circuit Breaker TRIPPED: token budget exceeded ({metrics.total_tokens}/{self.policy.token_budget})",
                    metric_val=float(metrics.total_tokens),
                    agent_id=worker_id,
                    swarm_id=swarm_id,
                )

            if self.policy.cost_budget is not None and metrics.total_cost >= self.policy.cost_budget:
                self.trip(
                    swarm_id=swarm_id,
                    reason=f"Fleet cost budget exceeded (${metrics.total_cost:.4f}/${self.policy.cost_budget:.4f})",
                    tripped_by=worker_id
                )
                raise SwarmCircuitBreakException(
                    reason=f"Fleet Circuit Breaker TRIPPED: cost budget exceeded (${metrics.total_cost:.4f}/${self.policy.cost_budget:.4f})",
                    metric_val=float(metrics.total_cost),
                    agent_id=worker_id,
                    swarm_id=swarm_id,
                )

            self._emit_event(FleetEvent(
                swarm_id=swarm_id,
                event_type="metric_update",
                worker_id=worker_id,
                payload={
                    "total_tokens": metrics.total_tokens,
                    "total_cost": metrics.total_cost,
                    "total_turns": metrics.total_turns,
                }
            ))

            return metrics

    def trip(self, swarm_id: str, reason: str, tripped_by: str) -> None:
        """Broadcasts immediate trip across all workers in the swarm fleet."""
        with self._lock:
            metrics = self._fleet_metrics.setdefault(swarm_id, FleetMetrics(swarm_id=swarm_id))
            if not metrics.is_tripped:
                metrics.is_tripped = True
                metrics.trip_reason = reason
                metrics.tripped_by = tripped_by
                metrics.tripped_at = datetime.now(timezone.utc)
                self._emit_event(FleetEvent(
                    swarm_id=swarm_id,
                    event_type="trip_broadcast",
                    worker_id=tripped_by,
                    payload={"reason": reason, "tripped_at": metrics.tripped_at.isoformat()}
                ))

    def reset_swarm(self, swarm_id: str) -> None:
        with self._lock:
            if swarm_id in self._fleet_metrics:
                del self._fleet_metrics[swarm_id]
            if swarm_id in self._worker_metrics:
                del self._worker_metrics[swarm_id]

    def is_tripped(self, swarm_id: str) -> bool:
        with self._lock:
            metrics = self._fleet_metrics.get(swarm_id)
            return metrics.is_tripped if metrics else False

    def get_metrics(self, swarm_id: str) -> Optional[FleetMetrics]:
        with self._lock:
            metrics = self._fleet_metrics.get(swarm_id)
            if metrics:
                return metrics.model_copy()
            return None

    def subscribe(self, callback: Callable[[FleetEvent], None]) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def _emit_event(self, event: FleetEvent) -> None:
        for cb in self._subscribers:
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Error in fleet subscriber callback: {e}")
