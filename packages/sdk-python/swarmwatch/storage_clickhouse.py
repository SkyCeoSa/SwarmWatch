# -*- coding: utf-8 -*-
"""
SwarmWatch ClickHouse Columnar Storage Adapter.
Provides high-throughput batch ingestion for multi-agent swarm telemetry at enterprise scale.
Conforms strictly to Seam 4 (StorageAdapter).
"""
import asyncio
import base64
from datetime import datetime, timezone
import gzip
import json
import logging
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.parse

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent
from swarmwatch.storage import StorageAdapter

logger = logging.getLogger("swarmwatch.storage.clickhouse")


class ClickHouseStorageAdapter(StorageAdapter):
    """
    Enterprise-scale columnar storage adapter for ClickHouse.
    Supports high-volume span batching, compressed snapshot storage,
    and instantaneous analytical queries across millions of multi-agent turns.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        user: str = "default",
        password: str = "",
        database: str = "swarmwatch",
        secure: bool = False,
        timeout: float = 10.0,
        use_mock: bool = False,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.secure = secure
        self.timeout = timeout
        self.use_mock = use_mock
        self._initialized = False

        # In-memory mock storage for local testing & development environments
        self._mock_spans: Dict[str, SwarmSpan] = {}
        self._mock_snapshots: Dict[str, StateSnapshot] = {}
        self._mock_events: List[CircuitEvent] = []

        protocol = "https" if self.secure else "http"
        self._base_url = f"{protocol}://{self.host}:{self.port}"

    def _execute_http_query(self, query: str, data: Optional[bytes] = None) -> str:
        """Executes a SQL statement or query against ClickHouse HTTP interface."""
        if self.use_mock:
            return ""

        params = {"database": self.database}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        url = f"{self._base_url}/?{urllib.parse.urlencode(params)}"
        req_data = data if data is not None else query.encode("utf-8")
        req = urllib.request.Request(url, data=req_data, method="POST")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                return response.read().decode("utf-8")
        except Exception as e:
            logger.error("ClickHouse query error: %s (query: %s)", e, query[:120])
            raise

    async def initialize(self) -> None:
        """Initializes database and columnar tables if they do not exist."""
        if self.use_mock:
            self._initialized = True
            return

        def _init():
            # 1. Create database
            self._execute_http_query(f"CREATE DATABASE IF NOT EXISTS {self.database};")

            # 2. Create spans table with ReplacingMergeTree
            self._execute_http_query(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.spans (
                    id String,
                    swarm_id String,
                    branch_id String DEFAULT 'main',
                    parent_span_id Nullable(String),
                    source_agent String,
                    target_agent String,
                    span_type LowCardinality(String),
                    input_payload String,
                    output_payload String,
                    token_count UInt32 DEFAULT 0,
                    entropy_score Float32 DEFAULT 1.0,
                    start_time DateTime64(3, 'UTC'),
                    end_time DateTime64(3, 'UTC'),
                    duration_ms Float32 DEFAULT 0.0,
                    created_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(start_time)
                ORDER BY (swarm_id, branch_id, start_time, id);
            """)

            # 3. Create snapshots table
            self._execute_http_query(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.snapshots (
                    id String,
                    span_id String,
                    branch_id String DEFAULT 'main',
                    state_hash String,
                    state_data_compressed String,
                    created_at DateTime64(3, 'UTC')
                ) ENGINE = ReplacingMergeTree()
                ORDER BY (branch_id, span_id, id);
            """)

            # 4. Create circuit_events table
            self._execute_http_query(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.circuit_events (
                    id String,
                    swarm_id String,
                    span_id Nullable(String),
                    event_type LowCardinality(String),
                    reason String,
                    metric_val Float32,
                    created_at DateTime64(3, 'UTC')
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(created_at)
                ORDER BY (swarm_id, created_at, id);
            """)

        await asyncio.to_thread(_init)
        self._initialized = True

    def save_spans_sync(self, spans: List[SwarmSpan]) -> None:
        """Synchronously persists a batch of spans."""
        if not spans:
            return

        if self.use_mock:
            for s in spans:
                self._mock_spans[s.id] = s
            return

        rows = []
        for s in spans:
            rows.append(json.dumps({
                "id": s.id,
                "swarm_id": s.swarm_id,
                "branch_id": s.branch_id,
                "parent_span_id": s.parent_span_id,
                "source_agent": s.source_agent,
                "target_agent": s.target_agent,
                "span_type": s.span_type,
                "input_payload": json.dumps(s.input_payload),
                "output_payload": json.dumps(s.output_payload),
                "token_count": s.token_count,
                "entropy_score": float(s.entropy_score),
                "start_time": s.start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "end_time": s.end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "duration_ms": float(s.duration_ms),
            }))

        payload = "\n".join(rows).encode("utf-8")
        query = f"INSERT INTO {self.database}.spans FORMAT JSONEachRow"
        params = {"database": self.database, "query": query}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        url = f"{self._base_url}/?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            response.read()

    async def save_spans(self, spans: List[SwarmSpan]) -> None:
        await asyncio.to_thread(self.save_spans_sync, spans)

    def get_swarm_spans_sync(self, swarm_id: str, branch_id: str = "main") -> List[SwarmSpan]:
        """Synchronously retrieves spans for a given swarm and branch."""
        if self.use_mock:
            matches = [
                s for s in self._mock_spans.values()
                if s.swarm_id == swarm_id and s.branch_id == branch_id
            ]
            matches.sort(key=lambda s: s.start_time)
            return matches

        query = f"""
            SELECT * FROM {self.database}.spans
            WHERE swarm_id = '{swarm_id}' AND branch_id = '{branch_id}'
            ORDER BY start_time ASC
            FORMAT JSONEachRow
        """
        output = self._execute_http_query(query)
        spans = []
        for line in output.strip().split("\n"):
            if not line.strip():
                continue
            r = json.loads(line)
            spans.append(SwarmSpan(
                id=r["id"],
                swarm_id=r["swarm_id"],
                branch_id=r["branch_id"],
                parent_span_id=r["parent_span_id"],
                source_agent=r["source_agent"],
                target_agent=r["target_agent"],
                span_type=r["span_type"],
                input_payload=json.loads(r["input_payload"]) if r.get("input_payload") else {},
                output_payload=json.loads(r["output_payload"]) if r.get("output_payload") else {},
                token_count=r.get("token_count", 0),
                entropy_score=r.get("entropy_score", 1.0),
                start_time=datetime.fromisoformat(r["start_time"].replace(" ", "T")),
                end_time=datetime.fromisoformat(r["end_time"].replace(" ", "T")),
                duration_ms=r.get("duration_ms", 0.0),
            ))
        return spans

    async def get_swarm_spans(self, swarm_id: str, branch_id: str = "main") -> List[SwarmSpan]:
        return await asyncio.to_thread(self.get_swarm_spans_sync, swarm_id, branch_id)

    def save_snapshot_sync(self, snapshot: StateSnapshot) -> None:
        """Persists a compressed state snapshot."""
        if self.use_mock:
            self._mock_snapshots[snapshot.span_id] = snapshot
            return

        raw_json = json.dumps(snapshot.state_data).encode("utf-8")
        compressed = gzip.compress(raw_json)
        b64_compressed = base64.b64encode(compressed).decode("ascii")

        row = json.dumps({
            "id": snapshot.id,
            "span_id": snapshot.span_id,
            "branch_id": snapshot.branch_id,
            "state_hash": snapshot.state_hash,
            "state_data_compressed": b64_compressed,
            "created_at": snapshot.created_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        })

        query = f"INSERT INTO {self.database}.snapshots FORMAT JSONEachRow"
        params = {"database": self.database, "query": query}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        url = f"{self._base_url}/?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, data=row.encode("utf-8"), method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            response.read()

    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        await asyncio.to_thread(self.save_snapshot_sync, snapshot)

    def get_snapshot_sync(self, span_id: str) -> Optional[StateSnapshot]:
        """Retrieves and decompresses state snapshot for span."""
        if self.use_mock:
            return self._mock_snapshots.get(span_id)

        query = f"""
            SELECT * FROM {self.database}.snapshots
            WHERE span_id = '{span_id}'
            LIMIT 1
            FORMAT JSONEachRow
        """
        output = self._execute_http_query(query)
        line = output.strip().split("\n")[0] if output.strip() else ""
        if not line:
            return None

        r = json.loads(line)
        compressed = base64.b64decode(r["state_data_compressed"])
        state_data = json.loads(gzip.decompress(compressed).decode("utf-8"))

        return StateSnapshot(
            id=r["id"],
            span_id=r["span_id"],
            branch_id=r["branch_id"],
            state_hash=r["state_hash"],
            state_data=state_data,
            created_at=datetime.fromisoformat(r["created_at"].replace(" ", "T")),
        )

    async def get_snapshot(self, span_id: str) -> Optional[StateSnapshot]:
        return await asyncio.to_thread(self.get_snapshot_sync, span_id)

    def record_circuit_event_sync(self, event: CircuitEvent) -> None:
        """Records a circuit warning or halt event."""
        if self.use_mock:
            self._mock_events.append(event)
            return

        row = json.dumps({
            "id": event.id,
            "swarm_id": event.swarm_id,
            "span_id": event.span_id,
            "event_type": event.event_type,
            "reason": event.reason,
            "metric_val": float(event.metric_val),
            "created_at": event.created_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        })

        query = f"INSERT INTO {self.database}.circuit_events FORMAT JSONEachRow"
        params = {"database": self.database, "query": query}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        url = f"{self._base_url}/?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, data=row.encode("utf-8"), method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            response.read()

    async def record_circuit_event(self, event: CircuitEvent) -> None:
        await asyncio.to_thread(self.record_circuit_event_sync, event)

    def list_branches_sync(self, swarm_id: str) -> List[str]:
        """Lists distinct execution branches for a swarm."""
        if self.use_mock:
            branches = set()
            for s in self._mock_spans.values():
                if s.swarm_id == swarm_id:
                    branches.add(s.branch_id)
            return sorted(list(branches))

        query = f"""
            SELECT DISTINCT branch_id FROM {self.database}.spans
            WHERE swarm_id = '{swarm_id}'
            FORMAT JSONEachRow
        """
        output = self._execute_http_query(query)
        branches = []
        for line in output.strip().split("\n"):
            if not line.strip():
                continue
            r = json.loads(line)
            branches.append(r["branch_id"])
        return branches

    async def list_branches(self, swarm_id: str) -> List[str]:
        return await asyncio.to_thread(self.list_branches_sync, swarm_id)

    def close_sync(self) -> None:
        if self.use_mock:
            self._mock_spans.clear()
            self._mock_snapshots.clear()
            self._mock_events.clear()

    async def close(self) -> None:
        await asyncio.to_thread(self.close_sync)
