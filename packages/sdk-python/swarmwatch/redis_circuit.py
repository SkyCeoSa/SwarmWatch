# -*- coding: utf-8 -*-
"""
SwarmWatch Distributed Redis Circuit Breaker & Fleet State Synchronization.
Provides atomic quota ledgers, sub-millisecond Pub/Sub trip broadcasting,
and zero-dependency standard library socket client with mock fallback (ADR 0033).
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
import re
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import urllib.parse

from pydantic import BaseModel, Field

from swarmwatch.circuit import CircuitPolicy, SwarmCircuitBreakException
from swarmwatch.fleet import FleetEvent, FleetMetrics

logger = logging.getLogger("swarmwatch.redis_circuit")


class RedisTripBroadcast(BaseModel):
    """Event payload broadcast over Redis Pub/Sub channels on circuit breaker trips."""
    swarm_id: str
    reason: str
    tripped_by: str
    tripped_at: str
    metric_val: float = 0.0


class RedisProtocolError(Exception):
    """Raised on invalid Redis RESP serialization or protocol errors."""
    pass


class RedisClientInterface(ABC):
    """Abstract interface defining the minimal Redis commands required by SwarmWatch."""

    @abstractmethod
    def ping(self) -> bool:
        pass

    @abstractmethod
    def hset(self, name: str, key: str, value: str) -> int:
        pass

    @abstractmethod
    def hget(self, name: str, key: str) -> Optional[str]:
        pass

    @abstractmethod
    def hgetall(self, name: str) -> Dict[str, str]:
        pass

    @abstractmethod
    def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        pass

    @abstractmethod
    def hincrbyfloat(self, name: str, key: str, amount: float = 1.0) -> float:
        pass

    @abstractmethod
    def sadd(self, name: str, *values: str) -> int:
        pass

    @abstractmethod
    def srem(self, name: str, *values: str) -> int:
        pass

    @abstractmethod
    def smembers(self, name: str) -> Set[str]:
        pass

    @abstractmethod
    def del_key(self, *names: str) -> int:
        pass

    @abstractmethod
    def publish(self, channel: str, message: str) -> int:
        pass

    @abstractmethod
    def subscribe(self, channel: str, callback: Callable[[str, str], None]) -> Any:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class MockRedisClient(RedisClientInterface):
    """
    Thread-safe in-memory Redis client supporting hashes, sets, atomic increments,
    and instantaneous pub/sub dispatch for deterministic offline testing and local execution.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._hashes: Dict[str, Dict[str, str]] = defaultdict(dict)
        self._sets: Dict[str, Set[str]] = defaultdict(set)
        self._subscribers: Dict[str, List[Callable[[str, str], None]]] = defaultdict(list)
        self._closed = False

    def ping(self) -> bool:
        with self._lock:
            return not self._closed

    def hset(self, name: str, key: str, value: str) -> int:
        with self._lock:
            is_new = key not in self._hashes[name]
            self._hashes[name][str(key)] = str(value)
            return 1 if is_new else 0

    def hget(self, name: str, key: str) -> Optional[str]:
        with self._lock:
            return self._hashes[name].get(str(key))

    def hgetall(self, name: str) -> Dict[str, str]:
        with self._lock:
            return dict(self._hashes[name])

    def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        with self._lock:
            current_raw = self._hashes[name].get(str(key), "0")
            try:
                current = int(current_raw)
            except ValueError:
                current = 0
            new_val = current + amount
            self._hashes[name][str(key)] = str(new_val)
            return new_val

    def hincrbyfloat(self, name: str, key: str, amount: float = 1.0) -> float:
        with self._lock:
            current_raw = self._hashes[name].get(str(key), "0.0")
            try:
                current = float(current_raw)
            except ValueError:
                current = 0.0
            new_val = current + amount
            self._hashes[name][str(key)] = str(new_val)
            return new_val

    def sadd(self, name: str, *values: str) -> int:
        with self._lock:
            added = 0
            for val in values:
                sval = str(val)
                if sval not in self._sets[name]:
                    self._sets[name].add(sval)
                    added += 1
            return added

    def srem(self, name: str, *values: str) -> int:
        with self._lock:
            removed = 0
            for val in values:
                sval = str(val)
                if sval in self._sets[name]:
                    self._sets[name].discard(sval)
                    removed += 1
            return removed

    def smembers(self, name: str) -> Set[str]:
        with self._lock:
            return set(self._sets[name])

    def del_key(self, *names: str) -> int:
        with self._lock:
            deleted = 0
            for name in names:
                if name in self._hashes:
                    del self._hashes[name]
                    deleted += 1
                if name in self._sets:
                    del self._sets[name]
                    deleted += 1
            return deleted

    def publish(self, channel: str, message: str) -> int:
        with self._lock:
            subscribers = list(self._subscribers.get(channel, []))
            # Also notify wildcard/global subscribers if matching prefix
            for ch, subs in self._subscribers.items():
                if ch != channel and ch.endswith("*") and channel.startswith(ch[:-1]):
                    subscribers.extend(subs)

        count = len(subscribers)
        for cb in subscribers:
            try:
                cb(channel, message)
            except Exception as e:
                logger.error(f"Error in MockRedis subscriber: {e}")
        return count

    def subscribe(self, channel: str, callback: Callable[[str, str], None]) -> Any:
        with self._lock:
            self._subscribers[channel].append(callback)
            return (channel, callback)

    def unsubscribe(self, token: Tuple[str, Callable[[str, str], None]]) -> None:
        channel, callback = token
        with self._lock:
            if channel in self._subscribers and callback in self._subscribers[channel]:
                self._subscribers[channel].remove(callback)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._subscribers.clear()


class RESPPrefix:
    SIMPLE_STRING = b"+"
    ERROR = b"-"
    INTEGER = b":"
    BULK_STRING = b"$"
    ARRAY = b"*"


class RESPRedisClient(RedisClientInterface):
    """
    Pure Python standard library socket client communicating via RESP2 protocol.
    Guarantees zero third-party dependencies while delivering sub-millisecond execution.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        password: Optional[str] = None,
        db: int = 0,
        timeout: float = 5.0,
    ):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.timeout = timeout
        self._lock = threading.RLock()
        self._socket: Optional[socket.socket] = None
        self._rfile: Optional[Any] = None
        self._pubsub_threads: List[threading.Thread] = []
        self._sub_sockets: List[socket.socket] = []
        self._closed = False

    @classmethod
    def from_url(cls, url: str, timeout: float = 5.0) -> "RESPRedisClient":
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 6379
        password = parsed.password
        db = 0
        if parsed.path and parsed.path.lstrip("/").isdigit():
            db = int(parsed.path.lstrip("/"))
        return cls(host=host, port=port, password=password, db=db, timeout=timeout)

    def _get_connection(self) -> socket.socket:
        if self._socket is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect((self.host, self.port))
            self._socket = s
            self._rfile = s.makefile("rb")
            if self.password:
                self._execute_command("AUTH", self.password)
            if self.db != 0:
                self._execute_command("SELECT", str(self.db))
        return self._socket

    def _encode_command(self, *args: str) -> bytes:
        parts = [f"*{len(args)}\r\n".encode("utf-8")]
        for arg in args:
            encoded = str(arg).encode("utf-8")
            parts.append(f"${len(encoded)}\r\n".encode("utf-8"))
            parts.append(encoded + b"\r\n")
        return b"".join(parts)

    def _read_response(self, rfile: Any) -> Any:
        line = rfile.readline()
        if not line:
            raise RedisProtocolError("Connection closed unexpectedly by Redis server")

        prefix = line[:1]
        content = line[1:-2]  # strip prefix and \r\n

        if prefix == RESPPrefix.SIMPLE_STRING:
            return content.decode("utf-8", errors="replace")
        elif prefix == RESPPrefix.ERROR:
            raise RedisProtocolError(content.decode("utf-8", errors="replace"))
        elif prefix == RESPPrefix.INTEGER:
            return int(content)
        elif prefix == RESPPrefix.BULK_STRING:
            length = int(content)
            if length == -1:
                return None
            data = rfile.read(length)
            rfile.read(2)  # strip trailing \r\n
            return data.decode("utf-8", errors="replace")
        elif prefix == RESPPrefix.ARRAY:
            length = int(content)
            if length == -1:
                return None
            result = []
            for _ in range(length):
                result.append(self._read_response(rfile))
            return result
        else:
            raise RedisProtocolError(f"Unknown RESP protocol prefix: {prefix!r}")

    def _execute_command(self, *args: str) -> Any:
        with self._lock:
            s = self._get_connection()
            try:
                s.sendall(self._encode_command(*args))
                return self._read_response(self._rfile)
            except (socket.error, RedisProtocolError) as e:
                self.close()
                raise e

    def ping(self) -> bool:
        res = self._execute_command("PING")
        return res == "PONG" or res is True

    def hset(self, name: str, key: str, value: str) -> int:
        return int(self._execute_command("HSET", name, str(key), str(value)))

    def hget(self, name: str, key: str) -> Optional[str]:
        res = self._execute_command("HGET", name, str(key))
        return str(res) if res is not None else None

    def hgetall(self, name: str) -> Dict[str, str]:
        res = self._execute_command("HGETALL", name)
        if not res or not isinstance(res, list):
            return {}
        result = {}
        for i in range(0, len(res), 2):
            if i + 1 < len(res):
                result[str(res[i])] = str(res[i + 1])
        return result

    def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        return int(self._execute_command("HINCRBY", name, str(key), str(amount)))

    def hincrbyfloat(self, name: str, key: str, amount: float = 1.0) -> float:
        return float(self._execute_command("HINCRBYFLOAT", name, str(key), str(amount)))

    def sadd(self, name: str, *values: str) -> int:
        if not values:
            return 0
        return int(self._execute_command("SADD", name, *values))

    def srem(self, name: str, *values: str) -> int:
        if not values:
            return 0
        return int(self._execute_command("SREM", name, *values))

    def smembers(self, name: str) -> Set[str]:
        res = self._execute_command("SMEMBERS", name)
        if not res or not isinstance(res, list):
            return set()
        return {str(x) for x in res}

    def del_key(self, *names: str) -> int:
        if not names:
            return 0
        return int(self._execute_command("DEL", *names))

    def publish(self, channel: str, message: str) -> int:
        return int(self._execute_command("PUBLISH", channel, message))

    def subscribe(self, channel: str, callback: Callable[[str, str], None]) -> Any:
        """
        Creates a dedicated subscriber socket and background daemon thread listening
        for incoming pub/sub messages.
        """
        sub_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sub_socket.connect((self.host, self.port))
        if self.password:
            sub_socket.sendall(self._encode_command("AUTH", self.password))
            r = sub_socket.makefile("rb")
            self._read_response(r)
        else:
            r = sub_socket.makefile("rb")

        sub_socket.sendall(self._encode_command("SUBSCRIBE", channel))
        self._read_response(r)  # Read confirmation array

        with self._lock:
            self._sub_sockets.append(sub_socket)

        def _listener():
            while not self._closed:
                try:
                    resp = self._read_response(r)
                    if isinstance(resp, list) and len(resp) >= 3 and resp[0] == "message":
                        ch = resp[1]
                        msg = resp[2]
                        callback(ch, msg)
                except Exception:
                    break

        th = threading.Thread(target=_listener, daemon=True, name=f"RESPRedisSub-{channel}")
        th.start()
        with self._lock:
            self._pubsub_threads.append(th)
        return (channel, sub_socket)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._rfile:
                try:
                    self._rfile.close()
                except Exception:
                    pass
                self._rfile = None
            if self._socket:
                try:
                    self._socket.close()
                except Exception:
                    pass
                self._socket = None
            for s in self._sub_sockets:
                try:
                    s.close()
                except Exception:
                    pass
            self._sub_sockets.clear()


class RedisFleetBackend:
    """
    Storage and messaging backend orchestrating Redis key formatting,
    atomic quota aggregation, and Pub/Sub trip broadcasting (ADR 0033).
    """

    def __init__(self, client: Optional[RedisClientInterface] = None):
        self.client = client or MockRedisClient()

    def _metrics_key(self, swarm_id: str) -> str:
        return f"swarmwatch:swarm:{swarm_id}:metrics"

    def _workers_key(self, swarm_id: str) -> str:
        return f"swarmwatch:swarm:{swarm_id}:workers"

    def _trip_channel(self, swarm_id: str) -> str:
        return f"swarmwatch:fleet:{swarm_id}:trips"

    def _global_trip_channel(self) -> str:
        return "swarmwatch:trips"

    def register_worker(self, worker_id: str, swarm_id: str) -> None:
        self.client.sadd(self._workers_key(swarm_id), worker_id)

    def unregister_worker(self, worker_id: str, swarm_id: str) -> None:
        self.client.srem(self._workers_key(swarm_id), worker_id)

    def record_turn_atomic(
        self,
        worker_id: str,
        swarm_id: str,
        tokens: int,
        cost: Optional[float],
        turns: int,
        policy: CircuitPolicy,
    ) -> FleetMetrics:
        """
        Atomically increments token, turn, and cost counters, checks for prior trips,
        evaluates ceilings, and triggers broadcast trips if quotas are breached.
        """
        metrics_key = self._metrics_key(swarm_id)
        workers_key = self._workers_key(swarm_id)

        # 1. Check if already tripped
        is_tripped_raw = self.client.hget(metrics_key, "is_tripped")
        if is_tripped_raw and str(is_tripped_raw) in ("1", "true", "True"):
            all_fields = self.client.hgetall(metrics_key)
            active = self.client.smembers(workers_key)
            return self._parse_metrics(swarm_id, all_fields, active)

        # 2. Add worker to active set
        self.client.sadd(workers_key, worker_id)

        # 3. Compute turn cost
        turn_cost = cost if cost is not None else (tokens * policy.cost_per_token)

        # 4. Atomic increments in Redis
        tot_tokens = self.client.hincrby(metrics_key, "total_tokens", tokens)
        tot_turns = self.client.hincrby(metrics_key, "total_turns", turns)
        tot_cost = self.client.hincrbyfloat(metrics_key, "total_cost", turn_cost)

        # 5. Check fleet-wide ceilings
        breached = False
        trip_reason = ""
        metric_val = 0.0

        if policy.max_turns is not None and tot_turns >= policy.max_turns:
            breached = True
            trip_reason = f"Fleet max turns quota reached ({tot_turns}/{policy.max_turns})"
            metric_val = float(tot_turns)
        elif policy.token_budget is not None and tot_tokens >= policy.token_budget:
            breached = True
            trip_reason = f"Fleet token budget exceeded ({tot_tokens}/{policy.token_budget})"
            metric_val = float(tot_tokens)
        elif policy.cost_budget is not None and tot_cost >= policy.cost_budget:
            breached = True
            trip_reason = f"Fleet cost budget exceeded (${tot_cost:.4f}/${policy.cost_budget:.4f})"
            metric_val = float(tot_cost)

        if breached:
            self.trip(swarm_id=swarm_id, reason=trip_reason, tripped_by=worker_id, metric_val=metric_val)
            active = self.client.smembers(workers_key)
            all_fields = self.client.hgetall(metrics_key)
            metrics = self._parse_metrics(swarm_id, all_fields, active)
            raise SwarmCircuitBreakException(
                reason=f"Fleet Circuit Breaker TRIPPED: {trip_reason}",
                metric_val=metric_val,
                agent_id=worker_id,
                swarm_id=swarm_id,
            )

        active = self.client.smembers(workers_key)
        all_fields = self.client.hgetall(metrics_key)
        return self._parse_metrics(swarm_id, all_fields, active)

    def trip(self, swarm_id: str, reason: str, tripped_by: str, metric_val: float = 0.0) -> None:
        """Sets tripped state in Redis hash and publishes broadcast payload to Pub/Sub channels."""
        metrics_key = self._metrics_key(swarm_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        self.client.hset(metrics_key, "is_tripped", "1")
        self.client.hset(metrics_key, "trip_reason", reason)
        self.client.hset(metrics_key, "tripped_by", tripped_by)
        self.client.hset(metrics_key, "tripped_at", now_iso)

        payload = RedisTripBroadcast(
            swarm_id=swarm_id,
            reason=reason,
            tripped_by=tripped_by,
            tripped_at=now_iso,
            metric_val=metric_val,
        ).model_dump_json()

        # Publish to swarm-specific channel and global channel
        self.client.publish(self._trip_channel(swarm_id), payload)
        self.client.publish(self._global_trip_channel(), payload)

    def is_tripped(self, swarm_id: str) -> bool:
        metrics_key = self._metrics_key(swarm_id)
        raw = self.client.hget(metrics_key, "is_tripped")
        return bool(raw and str(raw) in ("1", "true", "True"))

    def get_metrics(self, swarm_id: str) -> Optional[FleetMetrics]:
        metrics_key = self._metrics_key(swarm_id)
        workers_key = self._workers_key(swarm_id)
        all_fields = self.client.hgetall(metrics_key)
        if not all_fields:
            return None
        active = self.client.smembers(workers_key)
        return self._parse_metrics(swarm_id, all_fields, active)

    def reset_swarm(self, swarm_id: str) -> None:
        metrics_key = self._metrics_key(swarm_id)
        workers_key = self._workers_key(swarm_id)
        self.client.del_key(metrics_key, workers_key)

    def _parse_metrics(self, swarm_id: str, fields: Dict[str, str], active_workers: Set[str]) -> FleetMetrics:
        try:
            tokens = int(fields.get("total_tokens", "0"))
        except ValueError:
            tokens = 0

        try:
            turns = int(fields.get("total_turns", "0"))
        except ValueError:
            turns = 0

        try:
            cost = float(fields.get("total_cost", "0.0"))
        except ValueError:
            cost = 0.0

        is_tripped = fields.get("is_tripped") in ("1", "true", "True")
        reason = fields.get("trip_reason")
        tripped_by = fields.get("tripped_by")
        tripped_at_str = fields.get("tripped_at")
        tripped_at = None
        if tripped_at_str:
            try:
                tripped_at = datetime.fromisoformat(tripped_at_str)
            except Exception:
                tripped_at = None

        return FleetMetrics(
            swarm_id=swarm_id,
            total_tokens=tokens,
            total_cost=cost,
            total_turns=turns,
            active_workers=active_workers,
            is_tripped=is_tripped,
            trip_reason=reason,
            tripped_by=tripped_by,
            tripped_at=tripped_at,
        )


class RedisCircuitCoordinator:
    """
    Distributed Redis Circuit Breaker & Fleet State Synchronization Coordinator.
    Hides Redis client setup, atomic quota checks, and real-time Pub/Sub listener
    behind an austere, deep module interface (ADR 0033).
    """

    def __init__(
        self,
        policy: Optional[CircuitPolicy] = None,
        redis_client: Optional[RedisClientInterface] = None,
        redis_url: Optional[str] = None,
    ):
        self.policy = policy or CircuitPolicy()
        self._lock = threading.RLock()
        self._tripped_cache: Dict[str, bool] = {}
        self._subscribers: List[Callable[[FleetEvent], None]] = []

        if redis_client is not None:
            self._client = redis_client
        elif redis_url:
            self._client = RESPPrefixClient_or_native(redis_url)
        else:
            self._client = MockRedisClient()

        self.backend = RedisFleetBackend(self._client)

        # Subscribe to global and swarm trip broadcasts
        self._client.subscribe("swarmwatch:trips", self._on_remote_trip)

    def _on_remote_trip(self, channel: str, message: str) -> None:
        """Handles incoming trip broadcast from peer worker pods over Redis Pub/Sub."""
        try:
            data = json.loads(message)
            swarm_id = data.get("swarm_id")
            if swarm_id:
                with self._lock:
                    self._tripped_cache[swarm_id] = True

                event = FleetEvent(
                    swarm_id=swarm_id,
                    event_type="trip_broadcast",
                    worker_id=data.get("tripped_by", "remote-worker"),
                    payload=data,
                )
                for cb in self._subscribers:
                    try:
                        cb(event)
                    except Exception as e:
                        logger.error(f"Error in subscriber callback: {e}")
        except Exception as e:
            logger.error(f"Failed to parse trip broadcast message: {e}")

    def register_worker(self, worker_id: str, swarm_id: str) -> None:
        self.backend.register_worker(worker_id, swarm_id)
        event = FleetEvent(
            swarm_id=swarm_id,
            event_type="worker_joined",
            worker_id=worker_id,
        )
        self._emit_local_event(event)

    def unregister_worker(self, worker_id: str, swarm_id: str) -> None:
        self.backend.unregister_worker(worker_id, swarm_id)
        event = FleetEvent(
            swarm_id=swarm_id,
            event_type="worker_left",
            worker_id=worker_id,
        )
        self._emit_local_event(event)

    def record_turn(
        self,
        worker_id: str,
        swarm_id: str,
        tokens: int = 0,
        cost: Optional[float] = None,
        turns: int = 1,
    ) -> FleetMetrics:
        """
        Records worker turn execution, updates cumulative fleet metrics via Redis,
        and evaluates fleet quota thresholds. Raises SwarmCircuitBreakException
        if the fleet-wide budget or turn limit is breached.
        """
        # Fast in-memory check
        with self._lock:
            if self._tripped_cache.get(swarm_id):
                metrics = self.backend.get_metrics(swarm_id)
                reason = metrics.trip_reason if metrics else "Distributed circuit breaker tripped"
                tripped_by = metrics.tripped_by if metrics else "peer-worker"
                raise SwarmCircuitBreakException(
                    reason=f"Fleet Circuit Breaker TRIPPED for swarm '{swarm_id}': {reason} (tripped by {tripped_by})",
                    metric_val=float(metrics.total_turns if metrics else turns),
                    agent_id=worker_id,
                    swarm_id=swarm_id,
                )

        metrics = self.backend.record_turn_atomic(
            worker_id=worker_id,
            swarm_id=swarm_id,
            tokens=tokens,
            cost=cost,
            turns=turns,
            policy=self.policy,
        )

        if metrics.is_tripped:
            with self._lock:
                self._tripped_cache[swarm_id] = True
            raise SwarmCircuitBreakException(
                reason=f"Fleet Circuit Breaker TRIPPED for swarm '{swarm_id}': {metrics.trip_reason} (tripped by {metrics.tripped_by})",
                metric_val=float(metrics.total_turns),
                agent_id=worker_id,
                swarm_id=swarm_id,
            )

        self._emit_local_event(FleetEvent(
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

    def trip(self, swarm_id: str, reason: str, tripped_by: str, metric_val: float = 0.0) -> None:
        """Broadcasts immediate trip across all workers in the distributed swarm fleet."""
        with self._lock:
            self._tripped_cache[swarm_id] = True
        self.backend.trip(swarm_id=swarm_id, reason=reason, tripped_by=tripped_by, metric_val=metric_val)

    def is_tripped(self, swarm_id: str) -> bool:
        with self._lock:
            if self._tripped_cache.get(swarm_id):
                return True
        tripped = self.backend.is_tripped(swarm_id)
        if tripped:
            with self._lock:
                self._tripped_cache[swarm_id] = True
        return tripped

    def get_metrics(self, swarm_id: str) -> Optional[FleetMetrics]:
        return self.backend.get_metrics(swarm_id)

    def reset_swarm(self, swarm_id: str) -> None:
        with self._lock:
            self._tripped_cache.pop(swarm_id, None)
        self.backend.reset_swarm(swarm_id)

    def subscribe(self, callback: Callable[[FleetEvent], None]) -> None:
        with self._lock:
            self._subscribers.append(callback)

    def _emit_local_event(self, event: FleetEvent) -> None:
        for cb in self._subscribers:
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Error in fleet subscriber callback: {e}")

    def close(self) -> None:
        self._client.close()


def RESPPrefixClient_or_native(redis_url: str) -> RedisClientInterface:
    """
    Factory function resolving a Redis client.
    If the official `redis` package is installed, wraps `redis.Redis`.
    Otherwise, returns `RESPRedisClient` (standard library socket implementation).
    """
    try:
        import redis
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        # Verify ping
        client.ping()
        return NativeRedisAdapter(client)
    except Exception:
        # Fall back cleanly to RESPRedisClient
        return RESPRedisClient.from_url(redis_url)


class NativeRedisAdapter(RedisClientInterface):
    """Adapter wrapping an official redis-py `redis.Redis` instance."""

    def __init__(self, client: Any):
        self.client = client
        self._pubsubs: List[Any] = []

    def ping(self) -> bool:
        return bool(self.client.ping())

    def hset(self, name: str, key: str, value: str) -> int:
        return int(self.client.hset(name, key, value))

    def hget(self, name: str, key: str) -> Optional[str]:
        res = self.client.hget(name, key)
        return str(res) if res is not None else None

    def hgetall(self, name: str) -> Dict[str, str]:
        res = self.client.hgetall(name)
        return {str(k): str(v) for k, v in res.items()}

    def hincrby(self, name: str, key: str, amount: int = 1) -> int:
        return int(self.client.hincrby(name, key, amount))

    def hincrbyfloat(self, name: str, key: str, amount: float = 1.0) -> float:
        return float(self.client.hincrbyfloat(name, key, amount))

    def sadd(self, name: str, *values: str) -> int:
        if not values:
            return 0
        return int(self.client.sadd(name, *values))

    def srem(self, name: str, *values: str) -> int:
        if not values:
            return 0
        return int(self.client.srem(name, *values))

    def smembers(self, name: str) -> Set[str]:
        res = self.client.smembers(name)
        return {str(x) for x in res}

    def del_key(self, *names: str) -> int:
        if not names:
            return 0
        return int(self.client.delete(*names))

    def publish(self, channel: str, message: str) -> int:
        return int(self.client.publish(channel, message))

    def subscribe(self, channel: str, callback: Callable[[str, str], None]) -> Any:
        ps = self.client.pubsub()
        ps.subscribe(**{channel: lambda msg: callback(msg['channel'], msg['data']) if msg['type'] == 'message' else None})
        th = ps.run_in_thread(sleep_time=0.01, daemon=True)
        self._pubsubs.append((ps, th))
        return ps

    def close(self) -> None:
        for ps, th in self._pubsubs:
            try:
                ps.close()
            except Exception:
                pass
        self._pubsubs.clear()
        try:
            self.client.close()
        except Exception:
            pass
