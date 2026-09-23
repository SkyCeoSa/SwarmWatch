# -*- coding: utf-8 -*-
"""
SwarmWatch Multi-Cluster High-Availability Raft Consensus Engine (Seam 30 / ADR 0041).

Pure Python standard library implementation of the Raft distributed consensus protocol
(Ongaro & Ousterhout) providing leader election, replicated state machine logs,
atomic fleet circuit trips, and quorum commits ($N/2 + 1$) without external Redis.
"""

import asyncio
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import json
import logging
import random
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from swarmwatch.metrics import (
    set_raft_current_term,
    set_raft_commit_index,
    set_raft_node_role,
    record_raft_election,
)

logger = logging.getLogger("swarmwatch.raft")


class RaftNodeRole:
    """Operational state of a Raft consensus cluster member."""
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"


@dataclass
class RaftLogEntry:
    """An immutable entry within the replicated Raft log."""
    term: int
    index: int
    command: str
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RaftLogEntry":
        return cls(
            term=int(data.get("term", 0)),
            index=int(data.get("index", 0)),
            command=data.get("command", ""),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class RequestVoteRpc:
    """Solicit votes from cluster peers during leader election."""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RequestVoteRpc":
        return cls(
            term=int(data.get("term", 0)),
            candidate_id=str(data.get("candidate_id", "")),
            last_log_index=int(data.get("last_log_index", 0)),
            last_log_term=int(data.get("last_log_term", 0)),
        )


@dataclass
class RequestVoteResponse:
    """Response returned by a voter peer."""
    term: int
    vote_granted: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AppendEntriesRpc:
    """Heartbeat and log replication RPC dispatched by the leader."""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict[str, Any]] = field(default_factory=list)
    leader_commit: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AppendEntriesRpc":
        return cls(
            term=int(data.get("term", 0)),
            leader_id=str(data.get("leader_id", "")),
            prev_log_index=int(data.get("prev_log_index", 0)),
            prev_log_term=int(data.get("prev_log_term", 0)),
            entries=data.get("entries", []),
            leader_commit=int(data.get("leader_commit", 0)),
        )


@dataclass
class AppendEntriesResponse:
    """Response returned by a follower replicating log entries."""
    term: int
    success: bool
    match_index: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SwarmRaftStateMachine:
    """
    Deterministic replicated state machine applying committed Raft log entries.
    Tracks atomic swarm circuit trips, quota counters, and worker pod registrations.
    """

    def __init__(self):
        self._swarms: Dict[str, Dict[str, Any]] = {}

    def apply_entry(self, entry: RaftLogEntry) -> Dict[str, Any]:
        """Applies a committed log entry deterministically."""
        cmd = entry.command.lower()
        payload = entry.payload
        swarm_id = payload.get("swarm_id", "default-swarm")

        if swarm_id not in self._swarms:
            self._swarms[swarm_id] = {
                "swarm_id": swarm_id,
                "total_turns": 0,
                "total_tokens": 0,
                "total_cost": 0.0,
                "is_tripped": False,
                "trip_reason": None,
                "tripped_by": None,
                "active_workers": set(),
                "last_updated": entry.timestamp,
            }

        state = self._swarms[swarm_id]
        state["last_updated"] = entry.timestamp

        if cmd in ("trip", "circuit_trip", "fleet_trip"):
            state["is_tripped"] = True
            state["trip_reason"] = payload.get("reason", "Raft consensus circuit trip")
            state["tripped_by"] = payload.get("tripped_by", "raft-cluster")
            logger.info("Raft StateMachine applied circuit trip to swarm '%s': %s", swarm_id, state["trip_reason"])
        elif cmd in ("reset", "circuit_reset", "fleet_reset"):
            state["is_tripped"] = False
            state["trip_reason"] = None
            state["tripped_by"] = None
            state["total_turns"] = 0
            state["total_tokens"] = 0
            state["total_cost"] = 0.0
            logger.info("Raft StateMachine applied circuit reset to swarm '%s'", swarm_id)
        elif cmd in ("quota_inc", "update_quota"):
            state["total_turns"] += int(payload.get("turns", 0))
            state["total_tokens"] += int(payload.get("tokens", 0))
            state["total_cost"] += float(payload.get("cost", 0.0))
            worker_id = payload.get("worker_id")
            if worker_id:
                state["active_workers"].add(worker_id)
        elif cmd in ("register_worker",):
            worker_id = payload.get("worker_id")
            if worker_id:
                state["active_workers"].add(worker_id)

        res = dict(state)
        res["active_workers"] = list(state["active_workers"])
        return res

    def get_swarm_state(self, swarm_id: str) -> Optional[Dict[str, Any]]:
        state = self._swarms.get(swarm_id)
        if not state:
            return None
        res = dict(state)
        res["active_workers"] = list(state["active_workers"])
        return res

    def list_swarms(self) -> List[Dict[str, Any]]:
        results = []
        for s in self._swarms.values():
            res = dict(s)
            res["active_workers"] = list(s["active_workers"])
            results.append(res)
        return results


class RaftConsensusEngine:
    """
    Raft consensus coordinator managing term transitions, candidate elections,
    periodic heartbeats, log replication, and quorum calculations.
    """

    def __init__(
        self,
        node_id: str = "node-01",
        peers: Optional[List[str]] = None,
        rpc_sender: Optional[Callable[[str, str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
        election_timeout_range: Tuple[float, float] = (0.15, 0.30),
        heartbeat_interval: float = 0.05,
    ):
        self.node_id = node_id
        self.peers = peers or []
        self._rpc_sender = rpc_sender
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval

        # Persistent state on all nodes
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        # 1-indexed log with sentinel dummy entry at index 0
        self.log: List[RaftLogEntry] = [
            RaftLogEntry(term=0, index=0, command="SENTINEL", payload={})
        ]

        # Volatile state on all nodes
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: str = RaftNodeRole.FOLLOWER
        self.leader_id: Optional[str] = None

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self.state_machine = SwarmRaftStateMachine()
        self._lock = asyncio.Lock()
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._last_heartbeat_received: float = time.monotonic()

    @property
    def is_leader(self) -> bool:
        return self.role == RaftNodeRole.LEADER

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def quorum_size(self) -> int:
        return (self.cluster_size // 2) + 1

    def handle_request_vote(self, rpc: RequestVoteRpc) -> RequestVoteResponse:
        """Evaluates incoming RequestVote RPC from a candidate."""
        # 1. If term < current_term, reject vote
        if rpc.term < self.current_term:
            return RequestVoteResponse(term=self.current_term, vote_granted=False)

        # 2. If term > current_term, update term and step down to follower
        if rpc.term > self.current_term:
            self.current_term = rpc.term
            self.role = RaftNodeRole.FOLLOWER
            self.voted_for = None
            self.leader_id = None
            set_raft_current_term(self.current_term)
            set_raft_node_role(self.role)

        # 3. Check if we already voted for someone else in this term
        can_vote = (self.voted_for is None or self.voted_for == rpc.candidate_id)

        # 4. Check candidate's log completeness (Election Safety)
        our_last_index = self.last_log_index
        our_last_term = self.last_log_term
        log_ok = (
            rpc.last_log_term > our_last_term
            or (rpc.last_log_term == our_last_term and rpc.last_log_index >= our_last_index)
        )

        if can_vote and log_ok:
            self.voted_for = rpc.candidate_id
            self._last_heartbeat_received = time.monotonic()
            return RequestVoteResponse(term=self.current_term, vote_granted=True)

        return RequestVoteResponse(term=self.current_term, vote_granted=False)

    def handle_append_entries(self, rpc: AppendEntriesRpc) -> AppendEntriesResponse:
        """Evaluates incoming AppendEntries RPC (heartbeat or log replication) from leader."""
        # 1. Reply false if term < current_term
        if rpc.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False, match_index=self.last_log_index)

        # 2. Recognize leader and refresh heartbeat timer
        self._last_heartbeat_received = time.monotonic()
        if rpc.term > self.current_term or self.role == RaftNodeRole.CANDIDATE:
            self.current_term = rpc.term
            self.role = RaftNodeRole.FOLLOWER
            self.voted_for = None
            set_raft_current_term(self.current_term)
            set_raft_node_role(self.role)

        self.leader_id = rpc.leader_id

        # 3. Reply false if log doesn't contain an entry at prev_log_index matching prev_log_term
        if rpc.prev_log_index >= len(self.log):
            return AppendEntriesResponse(term=self.current_term, success=False, match_index=self.last_log_index)

        if rpc.prev_log_index > 0 and self.log[rpc.prev_log_index].term != rpc.prev_log_term:
            # Drop uncommitted conflicting entries
            self.log = self.log[: rpc.prev_log_index]
            return AppendEntriesResponse(term=self.current_term, success=False, match_index=self.last_log_index)

        # 4. Append any new entries not already in the log
        for i, entry_dict in enumerate(rpc.entries):
            entry = RaftLogEntry.from_dict(entry_dict) if isinstance(entry_dict, dict) else entry_dict
            idx = rpc.prev_log_index + 1 + i
            if idx < len(self.log):
                if self.log[idx].term != entry.term:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        # 5. If leader_commit > commit_index, advance commit_index and apply to state machine
        if rpc.leader_commit > self.commit_index:
            self.commit_index = min(rpc.leader_commit, self.last_log_index)
            set_raft_commit_index(self.commit_index)
            self._apply_committed_entries()

        return AppendEntriesResponse(term=self.current_term, success=True, match_index=self.last_log_index)

    def _apply_committed_entries(self) -> None:
        """Applies newly committed log entries up to commit_index."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                if entry.command != "SENTINEL":
                    self.state_machine.apply_entry(entry)

    async def start_election(self) -> bool:
        """Transitions node to CANDIDATE and solicits quorum votes."""
        record_raft_election()
        self.current_term += 1
        self.role = RaftNodeRole.CANDIDATE
        self.voted_for = self.node_id
        self.leader_id = None
        set_raft_current_term(self.current_term)
        set_raft_node_role(self.role)

        votes_granted = 1  # Vote for self
        logger.info("Node %s starting election for term %d (quorum needed: %d)", self.node_id, self.current_term, self.quorum_size)

        # If single-node cluster, immediately become leader
        if self.cluster_size <= 1:
            self.become_leader()
            return True

        if not self._rpc_sender:
            return False

        rpc = RequestVoteRpc(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.last_log_index,
            last_log_term=self.last_log_term,
        )

        tasks = [
            self._rpc_sender(peer, "request_vote", rpc.to_dict())
            for peer in self.peers
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, dict):
                resp = RequestVoteResponse(**res)
                if resp.term > self.current_term:
                    self.step_down(resp.term)
                    return False
                if resp.vote_granted:
                    votes_granted += 1

        if votes_granted >= self.quorum_size and self.role == RaftNodeRole.CANDIDATE:
            self.become_leader()
            return True

        return False

    def become_leader(self) -> None:
        """Assumes leadership role upon winning election."""
        self.role = RaftNodeRole.LEADER
        self.leader_id = self.node_id
        set_raft_node_role(self.role)
        logger.info("Node %s elected LEADER for term %d", self.node_id, self.current_term)

        # Re-initialize next_index and match_index for peers
        for peer in self.peers:
            self.next_index[peer] = self.last_log_index + 1
            self.match_index[peer] = 0

    async def broadcast_heartbeats(self) -> None:
        """Dispatches an empty AppendEntries RPC to all peers to assert leadership."""
        if not self.is_leader or not self._rpc_sender:
            return
        tasks = []
        for peer in self.peers:
            rpc = AppendEntriesRpc(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=self.last_log_index,
                prev_log_term=self.last_log_term,
                entries=[],
                leader_commit=self.commit_index,
            )
            tasks.append(self._rpc_sender(peer, "append_entries", rpc.to_dict()))
        await asyncio.gather(*tasks, return_exceptions=True)

    def step_down(self, new_term: Optional[int] = None) -> None:
        """Voluntarily relinquishes leadership or candidate state."""
        if new_term is not None and new_term > self.current_term:
            self.current_term = new_term
            set_raft_current_term(self.current_term)

        self.role = RaftNodeRole.FOLLOWER
        self.voted_for = None
        self.leader_id = None
        set_raft_node_role(self.role)
        logger.info("Node %s stepped down to FOLLOWER in term %d", self.node_id, self.current_term)

    async def propose(self, command: str, payload: Dict[str, Any]) -> Tuple[bool, Optional[RaftLogEntry], Optional[str]]:
        """
        Submits a client proposal to the cluster leader.
        Returns: (success, committed_entry, leader_id_if_not_leader)
        """
        if not self.is_leader:
            return False, None, self.leader_id

        async with self._lock:
            entry = RaftLogEntry(
                term=self.current_term,
                index=self.last_log_index + 1,
                command=command,
                payload=payload,
            )
            self.log.append(entry)

            # Single-node cluster commit
            if self.cluster_size <= 1:
                self.commit_index = entry.index
                set_raft_commit_index(self.commit_index)
                self._apply_committed_entries()
                return True, entry, None

            # Multi-node replication
            if not self._rpc_sender:
                return False, entry, None

            # Replicate to peers
            replication_successes = 1  # Self committed locally
            tasks = []
            for peer in self.peers:
                prev_idx = entry.index - 1
                prev_term = self.log[prev_idx].term if prev_idx < len(self.log) else 0
                rpc = AppendEntriesRpc(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=[entry.to_dict()],
                    leader_commit=self.commit_index,
                )
                tasks.append(self._rpc_sender(peer, "append_entries", rpc.to_dict()))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, dict):
                    resp = AppendEntriesResponse(**res)
                    if resp.term > self.current_term:
                        self.step_down(resp.term)
                        return False, None, None
                    if resp.success:
                        replication_successes += 1

            if replication_successes >= self.quorum_size:
                self.commit_index = entry.index
                set_raft_commit_index(self.commit_index)
                self._apply_committed_entries()
                return True, entry, None

            return False, entry, None

    def status(self) -> Dict[str, Any]:
        """Diagnostic state of this Raft consensus node."""
        set_raft_current_term(self.current_term)
        set_raft_commit_index(self.commit_index)
        set_raft_node_role(self.role)
        return {
            "node_id": self.node_id,
            "role": self.role,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_log_index": self.last_log_index,
            "peers": self.peers,
            "cluster_size": self.cluster_size,
            "quorum_size": self.quorum_size,
        }


class MockRaftCluster:
    """
    In-memory multi-node Raft cluster simulator for deterministic tests (<50ms).
    Directly routes async RPCs between node instances without socket overhead.
    """

    def __init__(self, node_count: int = 3):
        self.node_ids = [f"node-{i:02d}" for i in range(1, node_count + 1)]
        self.nodes: Dict[str, RaftConsensusEngine] = {}

        # Instantiate nodes
        for nid in self.node_ids:
            peers = [p for p in self.node_ids if p != nid]
            engine = RaftConsensusEngine(
                node_id=nid,
                peers=peers,
                rpc_sender=self._make_sender(nid),
            )
            self.nodes[nid] = engine

    def _make_sender(self, sender_id: str):
        async def send(target_id: str, rpc_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
            target = self.nodes.get(target_id)
            if not target:
                raise ValueError(f"Peer node '{target_id}' not found in cluster")

            if rpc_type == "request_vote":
                rpc = RequestVoteRpc.from_dict(data)
                return target.handle_request_vote(rpc).to_dict()
            elif rpc_type == "append_entries":
                rpc = AppendEntriesRpc.from_dict(data)
                return target.handle_append_entries(rpc).to_dict()
            raise ValueError(f"Unknown RPC type: {rpc_type}")

        return send

    async def elect_leader(self, preferred_candidate: Optional[str] = None) -> RaftConsensusEngine:
        """Runs an election round on the preferred candidate or node-01."""
        candidate_id = preferred_candidate or self.node_ids[0]
        candidate = self.nodes[candidate_id]
        won = await candidate.start_election()
        if not won:
            raise RuntimeError(f"Candidate '{candidate_id}' failed to win Raft election")
        await candidate.broadcast_heartbeats()
        return candidate

    async def propose_command(
        self,
        command: str,
        payload: Dict[str, Any],
        leader_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[RaftLogEntry]]:
        """Submits a command proposal to the active cluster leader."""
        leader = self.nodes.get(leader_id) if leader_id else next((n for n in self.nodes.values() if n.is_leader), None)
        if not leader:
            raise RuntimeError("No active Raft leader found in cluster to receive proposal")

        success, entry, _ = await leader.propose(command, payload)
        return success, entry


# Global singleton instance for single-server standalone or leader
global_raft_engine = RaftConsensusEngine(
    node_id="local-leader-01",
    peers=[],
)
