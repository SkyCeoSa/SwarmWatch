# -*- coding: utf-8 -*-
"""
SwarmWatch Distributed Redis Lock & Lease-Based Leader Election (Seam 24 / ADR 0035).
Provides atomic mutual exclusion, lease-based leader election, automatic heartbeat renewal,
and zero-downtime standby failover for multi-server SwarmWatch deployments.
"""

from enum import Enum
import json
import logging
import os
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import uuid

from pydantic import BaseModel, Field

from swarmwatch.redis_circuit import RedisClientInterface, MockRedisClient

logger = logging.getLogger("swarmwatch.redis_lock")

# Standard Lua script for atomic Compare-And-Swap (CAS) release:
# Only deletes the lock key if the stored value matches the caller's unique token.
LUA_RELEASE_LOCK = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Standard Lua script for atomic lease renewal:
# Only extends TTL if the stored value matches the caller's unique token.
LUA_EXTEND_LEASE = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""


class ServerNodeRole(str, Enum):
    """The explicit operational role of a SwarmWatch server instance in the cluster."""
    LEADER = "LEADER"
    STANDBY = "STANDBY"
    STEPPED_DOWN = "STEPPED_DOWN"


class ClusterNodeInfo(BaseModel):
    """Metadata describing a registered SwarmWatch server node in the cluster roster."""
    node_id: str
    role: ServerNodeRole = ServerNodeRole.STANDBY
    hostname: str = Field(default_factory=lambda: socket.gethostname())
    pid: int = Field(default_factory=os.getpid)
    last_heartbeat: float = Field(default_factory=time.time)
    started_at: float = Field(default_factory=time.time)

    @property
    def is_alive(self) -> bool:
        """Node is considered alive if a heartbeat was registered within the last 10 seconds."""
        return (time.time() - self.last_heartbeat) < 10.0


class ClusterLeaderStatus(BaseModel):
    """Comprehensive cluster leadership and active roster summary."""
    leader_node_id: Optional[str] = None
    is_leader: bool = False
    lease_expires_in_ms: Optional[int] = None
    local_node_id: str
    local_role: ServerNodeRole
    nodes_count: int = 1
    roster: List[ClusterNodeInfo] = Field(default_factory=list)


class RedisDistributedLock:
    """
    Atomic distributed mutual exclusion primitive adhering to Redlock single-instance CAS semantics.
    Guarantees mutual exclusion, automatic lease expiration, and safe token-verified release.
    """

    def __init__(
        self,
        client: RedisClientInterface,
        resource_name: str,
        ttl_ms: int = 5000,
        retry_interval_ms: int = 50,
    ):
        self.client = client
        self.resource_name = resource_name
        self.ttl_ms = ttl_ms
        self.retry_interval_ms = retry_interval_ms
        self._token: Optional[str] = None
        self._acquired_at: float = 0.0

    @property
    def token(self) -> Optional[str]:
        return self._token

    @property
    def is_held(self) -> bool:
        return self._token is not None

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Attempts to acquire the distributed mutex.
        If blocking=True, retries until timeout (or forever if timeout is None).
        """
        start = time.time()
        token = str(uuid.uuid4())

        while True:
            # Redis SET resource token NX PX ttl_ms
            success = self.client.set(self.resource_name, token, px=self.ttl_ms, nx=True)
            if success:
                self._token = token
                self._acquired_at = time.time()
                return True

            if not blocking:
                return False

            if timeout is not None and (time.time() - start) >= timeout:
                return False

            time.sleep(self.retry_interval_ms / 1000.0)

    def release(self) -> bool:
        """
        Atomically releases the lock using CAS verification to prevent releasing
        locks that have expired and been acquired by another node.
        """
        if not self._token:
            return False

        try:
            res = self.client.eval_script(LUA_RELEASE_LOCK, [self.resource_name], [self._token])
            released = bool(res and int(res) == 1)
        except Exception as e:
            logger.error(f"Error releasing distributed lock '{self.resource_name}': {e}")
            released = False
        finally:
            self._token = None
        return released

    def extend(self, additional_ttl_ms: Optional[int] = None) -> bool:
        """
        Atomically extends the lock lease TTL for the current owner.
        """
        if not self._token:
            return False

        ms = additional_ttl_ms or self.ttl_ms
        try:
            res = self.client.eval_script(LUA_EXTEND_LEASE, [self.resource_name], [self._token, ms])
            return bool(res and int(res) == 1)
        except Exception as e:
            logger.error(f"Error extending distributed lock '{self.resource_name}': {e}")
            return False

    def is_locked(self) -> bool:
        """Checks if the resource is currently held by any node."""
        val = self.client.get(self.resource_name)
        return val is not None

    def __enter__(self):
        acquired = self.acquire(blocking=True)
        if not acquired:
            raise TimeoutError(f"Failed to acquire distributed lock for '{self.resource_name}'")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class RedisLeaderElector:
    """
    Coordination manager orchestrating lease-based leader election, periodic heartbeats,
    and automatic standby failover across multi-instance SwarmWatch deployments (Seam 24).
    """

    def __init__(
        self,
        client: Optional[RedisClientInterface] = None,
        node_id: Optional[str] = None,
        election_key: str = "swarmwatch:leader:lock",
        roster_key: str = "swarmwatch:cluster:nodes",
        lease_ttl_ms: int = 5000,
        heartbeat_interval_s: float = 1.5,
    ):
        self.client = client or MockRedisClient()
        self.node_id = node_id or f"srv-node-{uuid.uuid4().hex[:8]}"
        self.election_key = election_key
        self.roster_key = roster_key
        self.lease_ttl_ms = lease_ttl_ms
        self.heartbeat_interval_s = heartbeat_interval_s

        self._lock = threading.RLock()
        self._role = ServerNodeRole.STANDBY
        self._running = False
        self._daemon_thread: Optional[threading.Thread] = None

        self._on_elected_callbacks: List[Callable[[], None]] = []
        self._on_stepped_down_callbacks: List[Callable[[], None]] = []
        self._stepped_down_at: float = 0.0

        self._info = ClusterNodeInfo(
            node_id=self.node_id,
            role=self._role,
            last_heartbeat=time.time(),
            started_at=time.time(),
        )

    @property
    def role(self) -> ServerNodeRole:
        with self._lock:
            return self._role

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self._role == ServerNodeRole.LEADER

    def on_elected(self, callback: Callable[[], None]) -> None:
        """Registers a callback executed when this node is elected LEADER."""
        with self._lock:
            self._on_elected_callbacks.append(callback)

    def on_stepped_down(self, callback: Callable[[], None]) -> None:
        """Registers a callback executed when this node steps down or loses leadership."""
        with self._lock:
            self._on_stepped_down_callbacks.append(callback)

    def start(self) -> None:
        """Starts the background election and heartbeat daemon thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._daemon_thread = threading.Thread(
                target=self._election_loop,
                daemon=True,
                name=f"LeaderElector-{self.node_id}",
            )
            self._daemon_thread.start()
        logger.info(f"LeaderElector started for node '{self.node_id}'")

    def stop(self) -> None:
        """Stops the daemon thread and steps down if currently LEADER."""
        with self._lock:
            if not self._running:
                return
            self._running = False

        if self.is_leader:
            self.step_down()

        # Remove self from roster
        try:
            self.client.srem(self.roster_key, self.node_id)
            self.client.del_key(f"{self.roster_key}:{self.node_id}")
        except Exception:
            pass

        logger.info(f"LeaderElector stopped for node '{self.node_id}'")

    def step_down(self) -> bool:
        """Voluntarily relinquishes leadership, immediately triggering standby failover."""
        with self._lock:
            if self._role != ServerNodeRole.LEADER:
                return False

            self._role = ServerNodeRole.STEPPED_DOWN
            self._info.role = ServerNodeRole.STEPPED_DOWN
            self._stepped_down_at = time.time()

        # Release lock key in Redis
        try:
            res = self.client.eval_script(LUA_RELEASE_LOCK, [self.election_key], [self.node_id])
            released = bool(res and int(res) == 1)
        except Exception as e:
            logger.error(f"Error stepping down leader '{self.node_id}': {e}")
            released = False

        # Fire callbacks
        self._notify_stepped_down()
        logger.info(f"Node '{self.node_id}' stepped down from LEADER role")
        return released

    def _election_loop(self) -> None:
        """Continuous background loop maintaining heartbeats and participating in election."""
        while self._running:
            try:
                self._record_heartbeat()

                with self._lock:
                    current_role = self._role

                if current_role == ServerNodeRole.LEADER:
                    # Renew lease
                    res = self.client.eval_script(
                        LUA_EXTEND_LEASE,
                        [self.election_key],
                        [self.node_id, self.lease_ttl_ms],
                    )
                    renewed = bool(res and int(res) == 1)
                    if not renewed:
                        logger.warning(f"Node '{self.node_id}' failed to renew lease; stepping down to STANDBY")
                        with self._lock:
                            self._role = ServerNodeRole.STANDBY
                            self._info.role = ServerNodeRole.STANDBY
                        self._notify_stepped_down()
                elif current_role == ServerNodeRole.STEPPED_DOWN:
                    # Yield to standby nodes; transition back to STANDBY once another node is elected or grace period expires
                    current_leader = self.client.get(self.election_key)
                    if current_leader and current_leader != self.node_id:
                        with self._lock:
                            self._role = ServerNodeRole.STANDBY
                            self._info.role = ServerNodeRole.STANDBY
                    elif (time.time() - self._stepped_down_at) > (self.lease_ttl_ms / 1000.0):
                        with self._lock:
                            self._role = ServerNodeRole.STANDBY
                            self._info.role = ServerNodeRole.STANDBY
                else:
                    # STANDBY: Attempt to acquire lease if absent or expired
                    acquired = self.client.set(
                        self.election_key,
                        self.node_id,
                        px=self.lease_ttl_ms,
                        nx=True,
                    )
                    if acquired:
                        with self._lock:
                            self._role = ServerNodeRole.LEADER
                            self._info.role = ServerNodeRole.LEADER
                        logger.info(f"Node '{self.node_id}' elected LEADER")
                        self._notify_elected()
                    else:
                        # Confirm who the leader is
                        current_leader = self.client.get(self.election_key)
                        if current_leader == self.node_id:
                            # Re-assert leader role if our ID holds the key
                            with self._lock:
                                self._role = ServerNodeRole.LEADER
                                self._info.role = ServerNodeRole.LEADER
                        else:
                            with self._lock:
                                if self._role != ServerNodeRole.STANDBY:
                                    self._role = ServerNodeRole.STANDBY
                                    self._info.role = ServerNodeRole.STANDBY
            except Exception as e:
                logger.error(f"Error in election loop for node '{self.node_id}': {e}")

            time.sleep(self.heartbeat_interval_s)

    def _record_heartbeat(self) -> None:
        """Updates this node's heartbeat timestamp and metadata in Redis."""
        now = time.time()
        self._info.last_heartbeat = now
        node_key = f"{self.roster_key}:{self.node_id}"

        # Update node data
        data_str = json.dumps(self._info.model_dump(mode="json"))
        self.client.set(node_key, data_str, px=int(self.heartbeat_interval_s * 4 * 1000))
        self.client.sadd(self.roster_key, self.node_id)

    def _notify_elected(self) -> None:
        for cb in list(self._on_elected_callbacks):
            try:
                cb()
            except Exception as e:
                logger.error(f"Error in on_elected callback: {e}")

    def _notify_stepped_down(self) -> None:
        for cb in list(self._on_stepped_down_callbacks):
            try:
                cb()
            except Exception as e:
                logger.error(f"Error in on_stepped_down callback: {e}")

    def get_leader_status(self) -> ClusterLeaderStatus:
        """Inspects Redis to return the active leader node and cluster roster."""
        leader_id = self.client.get(self.election_key)
        all_node_ids = self.client.smembers(self.roster_key)

        roster: List[ClusterNodeInfo] = []
        dead_nodes: List[str] = []

        for nid in all_node_ids:
            node_key = f"{self.roster_key}:{nid}"
            raw = self.client.get(node_key)
            if raw:
                try:
                    info_dict = json.loads(raw)
                    node_info = ClusterNodeInfo(**info_dict)
                    if node_info.is_alive:
                        # Re-align role with actual leader key
                        if leader_id and node_info.node_id == leader_id:
                            node_info.role = ServerNodeRole.LEADER
                        roster.append(node_info)
                    else:
                        dead_nodes.append(nid)
                except Exception:
                    dead_nodes.append(nid)
            else:
                dead_nodes.append(nid)

        # Cleanup dead nodes from set
        if dead_nodes:
            self.client.srem(self.roster_key, *dead_nodes)

        # If roster is empty (e.g. mock test), ensure local node is listed
        if not any(n.node_id == self.node_id for n in roster):
            roster.append(self._info)

        # Sort roster so LEADER is first, then alphabetical by node_id
        roster.sort(key=lambda n: (0 if n.role == ServerNodeRole.LEADER else 1, n.node_id))

        with self._lock:
            local_role = self._role

        return ClusterLeaderStatus(
            leader_node_id=leader_id,
            is_leader=(leader_id == self.node_id),
            lease_expires_in_ms=self.lease_ttl_ms if leader_id else 0,
            local_node_id=self.node_id,
            local_role=local_role,
            nodes_count=len(roster),
            roster=roster,
        )
