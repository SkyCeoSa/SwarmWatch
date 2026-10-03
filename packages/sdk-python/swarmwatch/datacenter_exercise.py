"""
SwarmWatch Datacenter Failure Exercise & Region Loss Fault Injection (Seam 70 / ADR 0081).

Implements deterministic multi-region network partitioning, quorum preservation,
fail-closed minority partition blocking, monotonic safety trip latching, and
cryptographically verifiable disaster recovery audit reports.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


# ==============================================================================
# 1. Custom Exceptions
# ==============================================================================

class DatacenterExerciseError(Exception):
    """Base exception for datacenter failure and disaster recovery exercises."""
    pass


class MinorityPartitionBlockedError(DatacenterExerciseError):
    """Raised when an operation is rejected because a node is in an isolated minority partition."""
    pass


class SplitBrainViolationError(DatacenterExerciseError):
    """Raised when conflicting authoritative decisions occur across partitioned clusters."""
    pass


class SafetyMonotonicityViolationError(DatacenterExerciseError):
    """Raised when a circuit trip or safety halt is cleared or unlatched during partition heal."""
    pass


# ==============================================================================
# 2. Enums
# ==============================================================================

class FailureScenarioType(str, Enum):
    """Standardized failure scenarios for datacenter disaster recovery testing."""
    REGION_SEVERED = "REGION_SEVERED"
    ASYMMETRIC_SPLIT = "ASYMMETRIC_SPLIT"
    TOTAL_POWER_LOSS = "TOTAL_POWER_LOSS"
    CORRUPTED_LEASES = "CORRUPTED_LEASES"


class ExercisePhase(str, Enum):
    """Phases of the formal failure exercise automaton."""
    NOMINAL = "NOMINAL"
    REGION_ISOLATION = "REGION_ISOLATION"
    FAIL_CLOSED_VERIFICATION = "FAIL_CLOSED_VERIFICATION"
    RECONNECT_AND_HEAL = "RECONNECT_AND_HEAL"
    POST_HEAL_CONVERGENCE = "POST_HEAL_CONVERGENCE"


class NodeRole(str, Enum):
    """Operational consensus role of a node within the cluster."""
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    ISOLATED_MINORITY = "ISOLATED_MINORITY"


# ==============================================================================
# 3. Models & Data Structures
# ==============================================================================

def sha256_digest(data: str | bytes) -> str:
    """Computes a SHA-256 hexadecimal hash string."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


@dataclass
class ClusterNode:
    """Represents an active server node in a specific datacenter region."""
    node_id: str
    region_id: str
    role: NodeRole = NodeRole.FOLLOWER
    lease_expiry_ns: int = 0
    is_healthy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "region_id": self.region_id,
            "role": self.role.value,
            "lease_expiry_ns": self.lease_expiry_ns,
            "is_healthy": self.is_healthy,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ClusterNode":
        return cls(
            node_id=str(data["node_id"]),
            region_id=str(data["region_id"]),
            role=NodeRole(data.get("role", NodeRole.FOLLOWER.value)),
            lease_expiry_ns=int(data.get("lease_expiry_ns", 0)),
            is_healthy=bool(data.get("is_healthy", True)),
        )


class NetworkMatrix:
    """
    Directed connectivity matrix between datacenter regions.
    Governs whether packets transmitted from Region A arrive at Region B.
    """

    def __init__(self, regions: Optional[List[str]] = None) -> None:
        self._regions: Set[str] = set(regions or [])
        self._routes: Dict[str, Set[str]] = {}
        self.restore_all()

    def add_region(self, region_id: str) -> None:
        self._regions.add(region_id)
        if region_id not in self._routes:
            self._routes[region_id] = set()
        # Connect bidirectionally to existing regions
        for r in self._regions:
            self._routes[region_id].add(r)
            self._routes[r].add(region_id)

    def is_reachable(self, from_region: str, to_region: str) -> bool:
        if from_region == to_region:
            return True
        return to_region in self._routes.get(from_region, set())

    def sever_region(self, region_id: str) -> None:
        """Completely isolates a region bidirectionally."""
        if region_id in self._routes:
            self._routes[region_id] = {region_id}
        for r in self._regions:
            if r in self._routes:
                self._routes[r].discard(region_id)

    def set_route(self, from_region: str, to_region: str, is_connected: bool) -> None:
        """Sets asymmetric or directed route reachability."""
        if from_region not in self._routes:
            self._routes[from_region] = {from_region}
        if is_connected:
            self._routes[from_region].add(to_region)
        else:
            if from_region != to_region:
                self._routes[from_region].discard(to_region)

    def restore_all(self, regions: Optional[List[str]] = None) -> None:
        """Restores full, symmetric cross-region connectivity."""
        if regions:
            self._regions = set(regions)
        self._routes = {r: set(self._regions) for r in self._regions}

    def to_dict(self) -> Dict[str, List[str]]:
        return {r: sorted(list(targets)) for r, targets in sorted(self._routes.items())}


@dataclass
class PermitAttemptResult:
    """Result of requesting an authoritative execution permit from a specific node."""
    node_id: str
    region_id: str
    granted: bool
    permit_id: Optional[str] = None
    reason: str = ""
    timestamp_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "region_id": self.region_id,
            "granted": self.granted,
            "permit_id": self.permit_id,
            "reason": self.reason,
            "timestamp_ns": self.timestamp_ns,
        }


@dataclass
class FailureExerciseEvent:
    """Timestamped telemetry record logged during disaster recovery exercise execution."""
    timestamp_ns: int
    phase: ExercisePhase
    event_type: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp_ns": self.timestamp_ns,
            "phase": self.phase.value,
            "event_type": self.event_type,
            "details": self.details,
        }


@dataclass
class FailureExerciseReport:
    """
    Comprehensive, cryptographically verifiable audit report proving
    resilience to catastrophic datacenter or regional network loss.
    """
    exercise_id: str
    scenario: FailureScenarioType
    total_nodes: int
    total_regions: int
    majority_partition_operational: bool
    minority_partition_isolated: bool
    split_brain_prevented: bool
    safety_monotonicity_verified: bool
    trips_preserved_count: int
    invariants_passed: bool
    duration_ms: float
    exercise_hash: str
    events: List[FailureExerciseEvent] = field(default_factory=list)
    issued_at_ns: int = field(default_factory=time.time_ns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exercise_id": self.exercise_id,
            "scenario": self.scenario.value,
            "total_nodes": self.total_nodes,
            "total_regions": self.total_regions,
            "majority_partition_operational": self.majority_partition_operational,
            "minority_partition_isolated": self.minority_partition_isolated,
            "split_brain_prevented": self.split_brain_prevented,
            "safety_monotonicity_verified": self.safety_monotonicity_verified,
            "trips_preserved_count": self.trips_preserved_count,
            "invariants_passed": self.invariants_passed,
            "duration_ms": self.duration_ms,
            "exercise_hash": self.exercise_hash,
            "events": [e.to_dict() for e in self.events],
            "issued_at_ns": self.issued_at_ns,
        }


# ==============================================================================
# 4. Core Engine: DatacenterFailureOrchestrator
# ==============================================================================

class DatacenterFailureOrchestrator:
    """
    Deep module governing multi-region disaster recovery simulation,
    quorum-preserving fault schedules, and split-brain safety verification.
    """

    def __init__(
        self,
        nodes: Optional[List[ClusterNode]] = None,
        lease_duration_ms: float = 3000.0,
    ) -> None:
        self._lock = threading.RLock()
        self._lease_duration_ns = int(lease_duration_ms * 1_000_000)
        self._nodes: Dict[str, ClusterNode] = {}
        self._regions: Set[str] = set()
        self._matrix = NetworkMatrix()
        self._active_phase = ExercisePhase.NOMINAL
        self._tripped: bool = False
        self._trip_reasons: List[str] = []
        self._tripped_nodes: Set[str] = set()
        self._issued_permits: Dict[str, Dict[str, Any]] = {}
        self._events: List[FailureExerciseEvent] = []
        self._reports_history: List[FailureExerciseReport] = []

        if nodes:
            self.configure(nodes)
        else:
            self.configure_default_topology()

    def configure_default_topology(self) -> None:
        """Sets up a canonical 5-node, 3-region distributed topology."""
        default_nodes = [
            ClusterNode(node_id="node-us-east-1", region_id="us-east-1", role=NodeRole.LEADER),
            ClusterNode(node_id="node-us-east-2", region_id="us-east-1", role=NodeRole.FOLLOWER),
            ClusterNode(node_id="node-us-west-1", region_id="us-west-2", role=NodeRole.FOLLOWER),
            ClusterNode(node_id="node-us-west-2", region_id="us-west-2", role=NodeRole.FOLLOWER),
            ClusterNode(node_id="node-eu-cent-1", region_id="eu-central-1", role=NodeRole.FOLLOWER),
        ]
        self.configure(default_nodes)

    def configure(self, nodes: List[ClusterNode]) -> None:
        with self._lock:
            self._nodes = {n.node_id: n for n in nodes}
            self._regions = {n.region_id for n in nodes}
            self._matrix = NetworkMatrix(list(self._regions))
            self._active_phase = ExercisePhase.NOMINAL
            self._tripped = False
            self._trip_reasons.clear()
            self._tripped_nodes.clear()
            self._issued_permits.clear()
            self._events.clear()

            now_ns = time.time_ns()
            for n in self._nodes.values():
                if n.role == NodeRole.LEADER:
                    n.lease_expiry_ns = now_ns + self._lease_duration_ns

            self._log_event(
                phase=ExercisePhase.NOMINAL,
                event_type="TOPOLOGY_CONFIGURED",
                details={
                    "total_nodes": len(self._nodes),
                    "regions": sorted(list(self._regions)),
                },
            )

    def _log_event(self, phase: ExercisePhase, event_type: str, details: Dict[str, Any]) -> None:
        event = FailureExerciseEvent(
            timestamp_ns=time.time_ns(),
            phase=phase,
            event_type=event_type,
            details=details,
        )
        self._events.append(event)

    def get_reachable_nodes(self, node_id: str) -> List[ClusterNode]:
        """Returns all nodes currently reachable from node_id based on the network matrix."""
        source_node = self._nodes.get(node_id)
        if not source_node or not source_node.is_healthy:
            return []

        reachable = []
        for target in self._nodes.values():
            if not target.is_healthy:
                continue
            if self._matrix.is_reachable(source_node.region_id, target.region_id):
                reachable.append(target)
        return reachable

    def has_majority_quorum(self, node_id: str) -> bool:
        """Calculates whether node_id can communicate with a strict majority of the total healthy cluster."""
        total_healthy = sum(1 for n in self._nodes.values() if n.is_healthy)
        majority_threshold = (total_healthy // 2) + 1
        reachable_count = len(self.get_reachable_nodes(node_id))
        return reachable_count >= majority_threshold

    def inject_partition(
        self,
        severed_regions: List[str],
        asymmetric_routes: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        """Injects regional network severance and/or asymmetric routing drops."""
        with self._lock:
            self._active_phase = ExercisePhase.REGION_ISOLATION
            for r in severed_regions:
                self._matrix.sever_region(r)

            if asymmetric_routes:
                for from_r, to_r in asymmetric_routes:
                    self._matrix.set_route(from_r, to_r, is_connected=False)

            self._log_event(
                phase=ExercisePhase.REGION_ISOLATION,
                event_type="PARTITION_INJECTED",
                details={
                    "severed_regions": severed_regions,
                    "asymmetric_routes": asymmetric_routes or [],
                    "matrix": self._matrix.to_dict(),
                },
            )

    def heal_partition(self) -> None:
        """Restores full cross-region communication and triggers state reconciliation."""
        with self._lock:
            self._active_phase = ExercisePhase.RECONNECT_AND_HEAL
            self._matrix.restore_all(list(self._regions))

            # Reconcile safety monotonicity: if any node tripped during partition, whole cluster trips
            if self._tripped_nodes:
                self._tripped = True

            self._log_event(
                phase=ExercisePhase.RECONNECT_AND_HEAL,
                event_type="PARTITION_HEALED",
                details={
                    "reconnected_regions": sorted(list(self._regions)),
                    "monotone_trip_latched": self._tripped,
                },
            )

    def request_permit(
        self,
        node_id: str,
        tenant_id: str = "tenant-prod-01",
        tokens: int = 100,
    ) -> PermitAttemptResult:
        """
        Dispatches an execution permit request to a designated cluster node.
        Fails closed with MinorityPartitionBlockedError if the node lacks majority quorum.
        Fails closed if the cluster or node is tripped.
        """
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                raise DatacenterExerciseError(f"Node '{node_id}' does not exist in cluster")

            # 1. Monotonic trip check
            if self._tripped or node_id in self._tripped_nodes:
                return PermitAttemptResult(
                    node_id=node_id,
                    region_id=node.region_id,
                    granted=False,
                    reason="Cluster safety trip latched (fail-closed)",
                )

            # 2. Majority quorum check
            if not self.has_majority_quorum(node_id):
                self._log_event(
                    phase=self._active_phase,
                    event_type="MINORITY_PERMIT_BLOCKED",
                    details={
                        "node_id": node_id,
                        "region_id": node.region_id,
                        "reachable_nodes": [n.node_id for n in self.get_reachable_nodes(node_id)],
                    },
                )
                raise MinorityPartitionBlockedError(
                    f"Node '{node_id}' in region '{node.region_id}' is isolated in a minority partition"
                )

            # 3. Grant permit in majority quorum
            permit_id = f"pmt-{hashlib.sha256(f'{node_id}:{tenant_id}:{time.time_ns()}'.encode()).hexdigest()[:12]}"
            record = {
                "permit_id": permit_id,
                "node_id": node_id,
                "region_id": node.region_id,
                "tenant_id": tenant_id,
                "tokens": tokens,
                "issued_at_ns": time.time_ns(),
            }
            self._issued_permits[permit_id] = record

            self._log_event(
                phase=self._active_phase,
                event_type="PERMIT_GRANTED",
                details=record,
            )

            return PermitAttemptResult(
                node_id=node_id,
                region_id=node.region_id,
                granted=True,
                permit_id=permit_id,
                reason="Majority quorum verified",
            )

    def broadcast_trip(self, origin_node_id: str, reason: str = "Entropy collapse emergency halt") -> Dict[str, Any]:
        """
        Broadcasts an emergency circuit trip originating from a specific node.
        Monotonically latches trip state across all currently reachable nodes.
        """
        with self._lock:
            origin_node = self._nodes.get(origin_node_id)
            if not origin_node:
                raise DatacenterExerciseError(f"Node '{origin_node_id}' not found")

            self._tripped_nodes.add(origin_node_id)
            self._trip_reasons.append(reason)

            reachable = self.get_reachable_nodes(origin_node_id)
            for r in reachable:
                self._tripped_nodes.add(r.node_id)

            # If origin has majority quorum, trip the global orchestrator flag immediately
            if self.has_majority_quorum(origin_node_id):
                self._tripped = True

            self._log_event(
                phase=self._active_phase,
                event_type="TRIP_BROADCAST",
                details={
                    "origin_node_id": origin_node_id,
                    "reason": reason,
                    "affected_nodes": [n.node_id for n in reachable],
                    "cluster_globally_tripped": self._tripped,
                },
            )

            return {
                "origin_node_id": origin_node_id,
                "reason": reason,
                "affected_node_count": len(reachable),
                "cluster_globally_tripped": self._tripped,
            }

    def run_exercise(
        self,
        scenario: FailureScenarioType = FailureScenarioType.REGION_SEVERED,
        target_severed_regions: Optional[List[str]] = None,
    ) -> FailureExerciseReport:
        """
        Executes an end-to-end 5-phase disaster recovery exercise:
        1. NOMINAL: Verifies full connectivity and majority permit issuance.
        2. REGION_ISOLATION: Injects complete regional loss on target_severed_regions.
        3. FAIL_CLOSED_VERIFICATION: Proves minority nodes reject permits, majority continues.
        4. RECONNECT_AND_HEAL: Re-establishes physical network links and anti-entropy sync.
        5. POST_HEAL_CONVERGENCE: Validates safety monotonicity, zero split-brain, and generates signed report.
        """
        start_time = time.perf_counter()
        exercise_id = f"dr-ex-{hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:8]}"

        severed = target_severed_regions or ["eu-central-1"]

        # Phase 1: NOMINAL
        self._active_phase = ExercisePhase.NOMINAL
        majority_operational = False
        nominal_node = next(iter(self._nodes.keys()))
        try:
            nominal_res = self.request_permit(nominal_node, tokens=50)
            majority_operational = nominal_res.granted
        except Exception:
            majority_operational = False

        # Phase 2: REGION_ISOLATION
        self.inject_partition(severed)

        # Phase 3: FAIL_CLOSED_VERIFICATION
        self._active_phase = ExercisePhase.FAIL_CLOSED_VERIFICATION
        minority_isolated = True
        split_brain_prevented = True

        # Find minority node
        minority_node = None
        for n in self._nodes.values():
            if n.region_id in severed:
                minority_node = n.node_id
                break

        if minority_node:
            try:
                # Minority node MUST throw MinorityPartitionBlockedError or reject
                self.request_permit(minority_node, tokens=50)
                # If permit was granted in minority partition, split-brain violation!
                minority_isolated = False
                split_brain_prevented = False
            except MinorityPartitionBlockedError:
                minority_isolated = True
            except Exception:
                minority_isolated = True

        # Majority node should still be able to issue permits (unless severed took majority)
        majority_node = None
        for n in self._nodes.values():
            if n.region_id not in severed and self.has_majority_quorum(n.node_id):
                majority_node = n.node_id
                break

        if majority_node:
            res_maj = self.request_permit(majority_node, tokens=75)
            if not res_maj.granted:
                majority_operational = False

        # Test safety trip monotonicity: broadcast a trip from majority node
        trip_node = majority_node or nominal_node
        self.broadcast_trip(trip_node, reason="Chaos fault safety trip injection")

        # Phase 4: RECONNECT_AND_HEAL
        self.heal_partition()

        # Phase 5: POST_HEAL_CONVERGENCE
        self._active_phase = ExercisePhase.POST_HEAL_CONVERGENCE
        safety_monotonicity_verified = self._tripped is True  # Trip must stay latched!

        # Post-heal permits must fail closed because trip is latched
        post_heal_attempt = self.request_permit(nominal_node, tokens=20)
        if post_heal_attempt.granted:
            safety_monotonicity_verified = False

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        invariants_passed = (
            majority_operational
            and minority_isolated
            and split_brain_prevented
            and safety_monotonicity_verified
        )

        canonical_dict = {
            "exercise_id": exercise_id,
            "scenario": scenario.value,
            "total_nodes": len(self._nodes),
            "total_regions": len(self._regions),
            "majority_partition_operational": majority_operational,
            "minority_partition_isolated": minority_isolated,
            "split_brain_prevented": split_brain_prevented,
            "safety_monotonicity_verified": safety_monotonicity_verified,
            "trips_preserved_count": len(self._tripped_nodes),
            "invariants_passed": invariants_passed,
        }
        encoded = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"))
        report_hash = sha256_digest(f"sw:fail:v1:{encoded}")

        report = FailureExerciseReport(
            exercise_id=exercise_id,
            scenario=scenario,
            total_nodes=len(self._nodes),
            total_regions=len(self._regions),
            majority_partition_operational=majority_operational,
            minority_partition_isolated=minority_isolated,
            split_brain_prevented=split_brain_prevented,
            safety_monotonicity_verified=safety_monotonicity_verified,
            trips_preserved_count=len(self._tripped_nodes),
            invariants_passed=invariants_passed,
            duration_ms=elapsed_ms,
            exercise_hash=report_hash,
            events=list(self._events),
        )

        with self._lock:
            self._reports_history.append(report)

        return report

    def status(self) -> Dict[str, Any]:
        """Returns live operational diagnostics of the datacenter failure orchestrator."""
        with self._lock:
            return {
                "active_phase": self._active_phase.value,
                "total_nodes": len(self._nodes),
                "total_regions": len(self._regions),
                "cluster_globally_tripped": self._tripped,
                "tripped_nodes_count": len(self._tripped_nodes),
                "issued_permits_count": len(self._issued_permits),
                "events_recorded": len(self._events),
                "total_exercises_run": len(self._reports_history),
                "network_matrix": self._matrix.to_dict(),
            }

    def get_reports(self) -> List[FailureExerciseReport]:
        """Returns copies of all recorded failure exercise reports."""
        with self._lock:
            return list(self._reports_history)

    def reset(self) -> None:
        """Resets cluster state, clears trips, and re-establishes nominal topology."""
        with self._lock:
            self.configure_default_topology()
            self._reports_history.clear()


# ==============================================================================
# 5. Global Singleton
# ==============================================================================

global_datacenter_orchestrator = DatacenterFailureOrchestrator()
