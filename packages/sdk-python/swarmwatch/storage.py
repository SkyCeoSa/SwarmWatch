# -*- coding: utf-8 -*-
import abc
import asyncio
import gzip
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent


class StorageAdapter(abc.ABC):
    """
    Deep Module Storage Interface.
    Decouples ingestion and SDK from underlying persistent storage (SQLite vs ClickHouse).
    """

    @abc.abstractmethod
    async def initialize(self) -> None:
        """Initialize connections, schemas, and pragmas."""
        pass

    @abc.abstractmethod
    async def save_spans(self, spans: List[SwarmSpan]) -> None:
        """Persist a batch of spans atomically."""
        pass

    @abc.abstractmethod
    async def get_swarm_spans(self, swarm_id: str, branch_id: str = "main") -> List[SwarmSpan]:
        """Fetch all spans for a specific swarm and branch ordered chronologically."""
        pass

    @abc.abstractmethod
    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        """Save a state snapshot."""
        pass

    @abc.abstractmethod
    async def get_snapshot(self, span_id: str) -> Optional[StateSnapshot]:
        """Retrieve state snapshot associated with a span."""
        pass

    @abc.abstractmethod
    async def record_circuit_event(self, event: CircuitEvent) -> None:
        """Record a circuit breaker warning or trip event."""
        pass

    @abc.abstractmethod
    async def list_branches(self, swarm_id: str) -> List[str]:
        """List all execution branches for a swarm."""
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        """Close storage resources."""
        pass


class SQLiteStorageAdapter(StorageAdapter):
    """
    High-performance zero-docker local storage adapter.
    Enforces PRAGMA journal_mode=WAL and PRAGMA synchronous=NORMAL for concurrent async writes.
    Uses thread-local connections to safely exploit SQLite WAL concurrency across worker threads.
    """

    def __init__(self, db_path: str = "swarmwatch.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._initialized = False
        self._init_lock = threading.Lock()

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            conn.row_factory = sqlite3.Row
            # Enable Foreign Keys and Performance Pragmas
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            with self._init_lock:
                self._ensure_schema(conn)
            self._local.conn = conn
        return self._local.conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        if self._initialized:
            return
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS swarms (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS spans (
                    id TEXT PRIMARY KEY,
                    swarm_id TEXT NOT NULL,
                    branch_id TEXT NOT NULL DEFAULT 'main',
                    parent_span_id TEXT,
                    source_agent TEXT NOT NULL,
                    target_agent TEXT NOT NULL,
                    span_type TEXT NOT NULL,
                    input_payload TEXT,
                    output_payload TEXT,
                    token_count INTEGER DEFAULT 0,
                    entropy_score REAL DEFAULT 1.0,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    duration_ms REAL DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_spans_swarm_branch ON spans(swarm_id, branch_id, start_time);
                CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans(parent_span_id);

                CREATE TABLE IF NOT EXISTS snapshots (
                    id TEXT PRIMARY KEY,
                    span_id TEXT NOT NULL UNIQUE,
                    branch_id TEXT NOT NULL DEFAULT 'main',
                    state_hash TEXT NOT NULL,
                    state_data_compressed BLOB NOT NULL,
                    created_at TIMESTAMP NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_snapshots_branch_span ON snapshots(branch_id, span_id);

                CREATE TABLE IF NOT EXISTS circuit_events (
                    id TEXT PRIMARY KEY,
                    swarm_id TEXT NOT NULL,
                    span_id TEXT,
                    event_type TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    metric_val REAL NOT NULL,
                    created_at TIMESTAMP NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_circuit_events_swarm ON circuit_events(swarm_id, created_at);
            """)
        self._initialized = True

    async def get_journal_mode(self) -> str:
        conn = self._get_connection()
        cursor = conn.execute("PRAGMA journal_mode;")
        row = cursor.fetchone()
        return row[0] if row else "unknown"

    async def initialize(self) -> None:
        conn = self._get_connection()
        self._ensure_schema(conn)

    def save_spans_sync(self, spans: List[SwarmSpan]) -> None:
        if not spans:
            return
        conn = self._get_connection()
        records = []
        for s in spans:
            records.append((
                s.id,
                s.swarm_id,
                s.branch_id,
                s.parent_span_id,
                s.source_agent,
                s.target_agent,
                s.span_type,
                json.dumps(s.input_payload),
                json.dumps(s.output_payload),
                s.token_count,
                s.entropy_score,
                s.start_time.isoformat(),
                s.end_time.isoformat(),
                s.duration_ms
            ))
        with conn:
            conn.executemany("""
                INSERT OR REPLACE INTO spans (
                    id, swarm_id, branch_id, parent_span_id,
                    source_agent, target_agent, span_type,
                    input_payload, output_payload, token_count,
                    entropy_score, start_time, end_time, duration_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)

    async def save_spans(self, spans: List[SwarmSpan]) -> None:
        self.save_spans_sync(spans)

    def query_spans(self, swarm_id: str, branch_id: str = "main") -> List[SwarmSpan]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM spans
            WHERE swarm_id = ? AND branch_id = ?
            ORDER BY start_time ASC
        """, (swarm_id, branch_id))
        rows = cursor.fetchall()
        spans = []
        for r in rows:
            spans.append(SwarmSpan(
                id=r["id"],
                swarm_id=r["swarm_id"],
                branch_id=r["branch_id"],
                parent_span_id=r["parent_span_id"],
                source_agent=r["source_agent"],
                target_agent=r["target_agent"],
                span_type=r["span_type"],
                input_payload=json.loads(r["input_payload"]) if r["input_payload"] else {},
                output_payload=json.loads(r["output_payload"]) if r["output_payload"] else {},
                token_count=r["token_count"],
                entropy_score=r["entropy_score"],
                start_time=datetime.fromisoformat(r["start_time"]),
                end_time=datetime.fromisoformat(r["end_time"]),
                duration_ms=r["duration_ms"]
            ))
        return spans

    async def get_swarm_spans(self, swarm_id: str, branch_id: str = "main") -> List[SwarmSpan]:
        return self.query_spans(swarm_id=swarm_id, branch_id=branch_id)

    def save_snapshot_sync(self, snapshot: StateSnapshot) -> None:
        conn = self._get_connection()
        raw_json = json.dumps(snapshot.state_data).encode("utf-8")
        compressed_bytes = gzip.compress(raw_json)
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO snapshots (
                    id, span_id, branch_id, state_hash, state_data_compressed, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                snapshot.id,
                snapshot.span_id,
                snapshot.branch_id,
                snapshot.state_hash,
                compressed_bytes,
                snapshot.created_at.isoformat()
            ))

    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        self.save_snapshot_sync(snapshot)

    def _unused_async_save_snapshot(self, snapshot: StateSnapshot) -> None:
        conn = self._get_connection()
        # Compress state payload using gzip
        raw_json = json.dumps(snapshot.state_data).encode("utf-8")
        compressed_bytes = gzip.compress(raw_json)

        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO snapshots (
                    id, span_id, branch_id, state_hash, state_data_compressed, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                snapshot.id,
                snapshot.span_id,
                snapshot.branch_id,
                snapshot.state_hash,
                compressed_bytes,
                snapshot.created_at.isoformat()
            ))

    async def get_snapshot(self, span_id: str) -> Optional[StateSnapshot]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM snapshots WHERE span_id = ?
        """, (span_id,))
        row = cursor.fetchone()
        if not row:
            return None

        decompressed_json = gzip.decompress(row["state_data_compressed"]).decode("utf-8")
        state_data = json.loads(decompressed_json)

        return StateSnapshot(
            id=row["id"],
            span_id=row["span_id"],
            branch_id=row["branch_id"],
            state_hash=row["state_hash"],
            state_data=state_data,
            created_at=datetime.fromisoformat(row["created_at"])
        )

    async def record_circuit_event(self, event: CircuitEvent) -> None:
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT INTO circuit_events (
                    id, swarm_id, span_id, event_type, reason, metric_val, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                event.id,
                event.swarm_id,
                event.span_id,
                event.event_type,
                event.reason,
                event.metric_val,
                event.created_at.isoformat()
            ))

    async def list_branches(self, swarm_id: str) -> List[str]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT DISTINCT branch_id FROM spans WHERE swarm_id = ?
        """, (swarm_id,))
        rows = cursor.fetchall()
        return [r["branch_id"] for r in rows]

    def close_sync(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    async def close(self) -> None:
        self.close_sync()
