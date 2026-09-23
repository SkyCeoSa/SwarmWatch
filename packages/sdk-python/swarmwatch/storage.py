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
import uuid

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent, CounterfactualSavings
from swarmwatch.tenant import Organization, Project, ApiKey
from swarmwatch.rbac import UserAccount, Role


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
    async def get_swarm_spans(
        self,
        swarm_id: str,
        branch_id: str = "main",
        project_id: Optional[str] = None
    ) -> List[SwarmSpan]:
        """Fetch all spans for a specific swarm and branch ordered chronologically."""
        pass

    @abc.abstractmethod
    async def save_snapshot(self, snapshot: StateSnapshot) -> None:
        """Save a state snapshot."""
        pass

    @abc.abstractmethod
    async def get_snapshot(self, span_id: str, project_id: Optional[str] = None) -> Optional[StateSnapshot]:
        """Retrieve state snapshot associated with a span."""
        pass

    @abc.abstractmethod
    async def record_circuit_event(self, event: CircuitEvent) -> None:
        """Record a circuit breaker warning or trip event."""
        pass

    @abc.abstractmethod
    async def list_branches(self, swarm_id: str, project_id: Optional[str] = None) -> List[str]:
        """List all execution branches for a swarm."""
        pass

    async def get_snapshots_for_swarm(
        self,
        swarm_id: str,
        branch_id: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> List[StateSnapshot]:
        """Fetch all state snapshots for a swarm across branches."""
        return []

    async def create_organization(self, org: Organization) -> Organization:
        """Create a new tenant organization."""
        return org

    async def get_organization(self, org_id: str) -> Optional[Organization]:
        """Fetch organization by ID or slug."""
        return None

    async def list_organizations(self) -> List[Organization]:
        """List all organizations."""
        return []

    async def create_project(self, project: Project) -> Project:
        """Create a new project within an organization."""
        return project

    async def get_project(self, project_id: str) -> Optional[Project]:
        """Fetch project by ID or slug."""
        return None

    async def list_projects(self, org_id: Optional[str] = None) -> List[Project]:
        """List projects, optionally filtered by org_id."""
        return []

    async def save_api_key(self, api_key: ApiKey) -> None:
        """Save a new cryptographic API key record."""
        pass

    async def get_api_key_by_hash(self, key_hash: str) -> Optional[ApiKey]:
        """Retrieve an active API key record by its SHA-256 hash."""
        return None

    async def list_api_keys(self, project_id: str) -> List[ApiKey]:
        """List API keys for a specific project."""
        return []

    async def revoke_api_key(self, key_id: str) -> bool:
        """Revoke an API key by setting its revoked_at timestamp."""
        return False

    async def list_swarms(self, project_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """List active swarms with turn metrics, branch info, and circuit status."""
        return []

    async def save_user(self, user: UserAccount) -> None:
        """Create or update a user account record."""
        pass

    async def get_user_by_username(self, username: str) -> Optional[UserAccount]:
        """Fetch user account by username."""
        return None

    async def get_user(self, user_id: str) -> Optional[UserAccount]:
        """Fetch user account by ID."""
        return None

    async def list_users(self) -> List[UserAccount]:
        """List all user accounts."""
        return []

    async def record_audit_event(
        self,
        action: str,
        resource: str,
        status: str,
        user_id: Optional[str] = None,
        username: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record an enterprise security audit event."""
        pass

    async def list_audit_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List security audit trail events."""
        return []

    async def save_push_subscription(self, subscription: Any) -> None:
        """Register or update a WebPush browser/device subscription."""
        pass

    async def delete_push_subscription(self, endpoint: str) -> bool:
        """Unregister a WebPush subscription by its endpoint URI."""
        return False

    async def get_push_subscription(self, endpoint: str) -> Optional[Any]:
        """Fetch a WebPush subscription by its endpoint URI."""
        return None

    async def list_push_subscriptions(self, project_id: Optional[str] = None) -> List[Any]:
        """List active WebPush subscriptions for a tenant project."""
        return []

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
                CREATE TABLE IF NOT EXISTS organizations (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    slug TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    org_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (org_id) REFERENCES organizations(id) ON DELETE CASCADE,
                    UNIQUE(org_id, slug)
                );

                CREATE TABLE IF NOT EXISTS api_keys (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    key_prefix TEXT NOT NULL,
                    key_hash TEXT UNIQUE NOT NULL,
                    scopes TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    revoked_at TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS swarms (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL DEFAULT 'default-project',
                    name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS spans (
                    id TEXT PRIMARY KEY,
                    swarm_id TEXT NOT NULL,
                    project_id TEXT NOT NULL DEFAULT 'default-project',
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

                CREATE TABLE IF NOT EXISTS snapshots (
                    id TEXT PRIMARY KEY,
                    span_id TEXT NOT NULL UNIQUE,
                    project_id TEXT NOT NULL DEFAULT 'default-project',
                    branch_id TEXT NOT NULL DEFAULT 'main',
                    state_hash TEXT NOT NULL,
                    state_data_compressed BLOB NOT NULL,
                    created_at TIMESTAMP NOT NULL
                );

                CREATE TABLE IF NOT EXISTS circuit_events (
                    id TEXT PRIMARY KEY,
                    swarm_id TEXT NOT NULL,
                    project_id TEXT NOT NULL DEFAULT 'default-project',
                    span_id TEXT,
                    event_type TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    metric_val REAL NOT NULL,
                    savings_json TEXT,
                    created_at TIMESTAMP NOT NULL
                );

                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT,
                    role TEXT NOT NULL DEFAULT 'viewer',
                    mfa_enabled INTEGER NOT NULL DEFAULT 0,
                    totp_secret TEXT,
                    backup_codes_json TEXT DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    id TEXT PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_id TEXT,
                    username TEXT,
                    action TEXT NOT NULL,
                    resource TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT
                );

                CREATE TABLE IF NOT EXISTS push_subscriptions (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL DEFAULT 'default-project',
                    endpoint TEXT UNIQUE NOT NULL,
                    p256dh TEXT NOT NULL,
                    auth TEXT NOT NULL,
                    user_id TEXT,
                    topics_json TEXT DEFAULT '["circuit_trip","circuit_warning","fleet_trip"]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Auto-migrate existing tables if missing project_id or savings_json column
            for table in ["swarms", "spans", "snapshots", "circuit_events"]:
                try:
                    cursor = conn.execute(f"PRAGMA table_info({table});")
                    cols = [row[1] for row in cursor.fetchall()]
                    if cols and "project_id" not in cols:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN project_id TEXT NOT NULL DEFAULT 'default-project';")
                    if table == "circuit_events" and cols and "savings_json" not in cols:
                        conn.execute("ALTER TABLE circuit_events ADD COLUMN savings_json TEXT;")
                except Exception:
                    pass

            conn.executescript("""
                CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
                CREATE INDEX IF NOT EXISTS idx_api_keys_project ON api_keys(project_id);
                CREATE INDEX IF NOT EXISTS idx_projects_org ON projects(org_id);
                CREATE INDEX IF NOT EXISTS idx_spans_swarm_branch ON spans(swarm_id, branch_id, start_time);
                CREATE INDEX IF NOT EXISTS idx_spans_project_swarm ON spans(project_id, swarm_id, branch_id, start_time);
                CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans(parent_span_id);
                CREATE INDEX IF NOT EXISTS idx_snapshots_branch_span ON snapshots(branch_id, span_id);
                CREATE INDEX IF NOT EXISTS idx_circuit_events_swarm ON circuit_events(swarm_id, created_at);
            """)

            # Seed default organization and default project
            conn.execute("""
                INSERT OR IGNORE INTO organizations (id, name, slug)
                VALUES ('default-org', 'Default Organization', 'default-org');
            """)
            conn.execute("""
                INSERT OR IGNORE INTO projects (id, org_id, name, slug)
                VALUES ('default-project', 'default-org', 'Default Project', 'default-project');
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

    # Multi-Tenant Organization CRUD
    def create_organization_sync(self, org: Organization) -> Organization:
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO organizations (id, name, slug, created_at)
                VALUES (?, ?, ?, ?)
            """, (org.id, org.name, org.slug, org.created_at.isoformat()))
        return org

    async def create_organization(self, org: Organization) -> Organization:
        return self.create_organization_sync(org)

    def get_organization_sync(self, org_id: str) -> Optional[Organization]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM organizations WHERE id = ? OR slug = ?
        """, (org_id, org_id))
        row = cursor.fetchone()
        if not row:
            return None
        return Organization(
            id=row["id"],
            name=row["name"],
            slug=row["slug"],
            created_at=datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"]
        )

    async def get_organization(self, org_id: str) -> Optional[Organization]:
        return self.get_organization_sync(org_id)

    def list_organizations_sync(self) -> List[Organization]:
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM organizations ORDER BY created_at ASC")
        rows = cursor.fetchall()
        orgs = []
        for r in rows:
            created_at = datetime.fromisoformat(r["created_at"]) if isinstance(r["created_at"], str) else r["created_at"]
            orgs.append(Organization(id=r["id"], name=r["name"], slug=r["slug"], created_at=created_at))
        return orgs

    async def list_organizations(self) -> List[Organization]:
        return self.list_organizations_sync()

    # Multi-Tenant Project CRUD
    def create_project_sync(self, project: Project) -> Project:
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO projects (id, org_id, name, slug, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (project.id, project.org_id, project.name, project.slug, project.created_at.isoformat()))
        return project

    async def create_project(self, project: Project) -> Project:
        return self.create_project_sync(project)

    def get_project_sync(self, project_id: str) -> Optional[Project]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM projects WHERE id = ? OR slug = ?
        """, (project_id, project_id))
        row = cursor.fetchone()
        if not row:
            return None
        created_at = datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"]
        return Project(id=row["id"], org_id=row["org_id"], name=row["name"], slug=row["slug"], created_at=created_at)

    async def get_project(self, project_id: str) -> Optional[Project]:
        return self.get_project_sync(project_id)

    def list_projects_sync(self, org_id: Optional[str] = None) -> List[Project]:
        conn = self._get_connection()
        if org_id:
            cursor = conn.execute("SELECT * FROM projects WHERE org_id = ? ORDER BY created_at ASC", (org_id,))
        else:
            cursor = conn.execute("SELECT * FROM projects ORDER BY created_at ASC")
        rows = cursor.fetchall()
        projects = []
        for r in rows:
            created_at = datetime.fromisoformat(r["created_at"]) if isinstance(r["created_at"], str) else r["created_at"]
            projects.append(Project(id=r["id"], org_id=r["org_id"], name=r["name"], slug=r["slug"], created_at=created_at))
        return projects

    async def list_projects(self, org_id: Optional[str] = None) -> List[Project]:
        return self.list_projects_sync(org_id)

    # API Key CRUD
    def save_api_key_sync(self, api_key: ApiKey) -> None:
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO api_keys (
                    id, project_id, name, key_prefix, key_hash, scopes, created_at, revoked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                api_key.id,
                api_key.project_id,
                api_key.name,
                api_key.key_prefix,
                api_key.key_hash,
                json.dumps(api_key.scopes),
                api_key.created_at.isoformat(),
                api_key.revoked_at.isoformat() if api_key.revoked_at else None
            ))

    async def save_api_key(self, api_key: ApiKey) -> None:
        self.save_api_key_sync(api_key)

    def get_api_key_by_hash_sync(self, key_hash: str) -> Optional[ApiKey]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM api_keys WHERE key_hash = ? AND revoked_at IS NULL
        """, (key_hash,))
        row = cursor.fetchone()
        if not row:
            return None
        created_at = datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"]
        revoked_at = datetime.fromisoformat(row["revoked_at"]) if row["revoked_at"] else None
        return ApiKey(
            id=row["id"],
            project_id=row["project_id"],
            name=row["name"],
            key_prefix=row["key_prefix"],
            key_hash=row["key_hash"],
            scopes=json.loads(row["scopes"]) if row["scopes"] else [],
            created_at=created_at,
            revoked_at=revoked_at,
        )

    async def get_api_key_by_hash(self, key_hash: str) -> Optional[ApiKey]:
        return self.get_api_key_by_hash_sync(key_hash)

    def list_api_keys_sync(self, project_id: str) -> List[ApiKey]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM api_keys WHERE project_id = ? ORDER BY created_at DESC
        """, (project_id,))
        rows = cursor.fetchall()
        keys = []
        for r in rows:
            created_at = datetime.fromisoformat(r["created_at"]) if isinstance(r["created_at"], str) else r["created_at"]
            revoked_at = datetime.fromisoformat(r["revoked_at"]) if r["revoked_at"] else None
            keys.append(ApiKey(
                id=r["id"],
                project_id=r["project_id"],
                name=r["name"],
                key_prefix=r["key_prefix"],
                key_hash=r["key_hash"],
                scopes=json.loads(r["scopes"]) if r["scopes"] else [],
                created_at=created_at,
                revoked_at=revoked_at,
            ))
        return keys

    async def list_api_keys(self, project_id: str) -> List[ApiKey]:
        return self.list_api_keys_sync(project_id)

    def revoke_api_key_sync(self, key_id: str) -> bool:
        conn = self._get_connection()
        with conn:
            cursor = conn.execute("""
                UPDATE api_keys SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL
            """, (datetime.now(timezone.utc).isoformat(), key_id))
            return cursor.rowcount > 0

    async def revoke_api_key(self, key_id: str) -> bool:
        return self.revoke_api_key_sync(key_id)

    def save_spans_sync(self, spans: List[SwarmSpan]) -> None:
        if not spans:
            return
        conn = self._get_connection()
        records = []
        for s in spans:
            records.append((
                s.id,
                s.swarm_id,
                getattr(s, "project_id", "default-project"),
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
                    id, swarm_id, project_id, branch_id, parent_span_id,
                    source_agent, target_agent, span_type,
                    input_payload, output_payload, token_count,
                    entropy_score, start_time, end_time, duration_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)

    async def save_spans(self, spans: List[SwarmSpan]) -> None:
        self.save_spans_sync(spans)

    def query_spans(
        self,
        swarm_id: str,
        branch_id: str = "main",
        project_id: Optional[str] = None
    ) -> List[SwarmSpan]:
        conn = self._get_connection()
        if project_id:
            cursor = conn.execute("""
                SELECT * FROM spans
                WHERE swarm_id = ? AND branch_id = ? AND project_id = ?
                ORDER BY start_time ASC
            """, (swarm_id, branch_id, project_id))
        else:
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
                project_id=r["project_id"] if "project_id" in r.keys() else "default-project",
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

    async def get_swarm_spans(
        self,
        swarm_id: str,
        branch_id: str = "main",
        project_id: Optional[str] = None
    ) -> List[SwarmSpan]:
        return self.query_spans(swarm_id=swarm_id, branch_id=branch_id, project_id=project_id)

    def save_snapshot_sync(self, snapshot: StateSnapshot) -> None:
        conn = self._get_connection()
        raw_json = json.dumps(snapshot.state_data).encode("utf-8")
        compressed_bytes = gzip.compress(raw_json)
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO snapshots (
                    id, span_id, project_id, branch_id, state_hash, state_data_compressed, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot.id,
                snapshot.span_id,
                getattr(snapshot, "project_id", "default-project"),
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
                    id, span_id, project_id, branch_id, state_hash, state_data_compressed, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot.id,
                snapshot.span_id,
                getattr(snapshot, "project_id", "default-project"),
                snapshot.branch_id,
                snapshot.state_hash,
                compressed_bytes,
                snapshot.created_at.isoformat()
            ))

    async def get_snapshot(self, span_id: str, project_id: Optional[str] = None) -> Optional[StateSnapshot]:
        conn = self._get_connection()
        if project_id is not None:
            cursor = conn.execute("""
                SELECT * FROM snapshots WHERE span_id = ? AND project_id = ?
            """, (span_id, project_id))
        else:
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
            project_id=row["project_id"] if "project_id" in row.keys() else "default-project",
            branch_id=row["branch_id"],
            state_hash=row["state_hash"],
            state_data=state_data,
            created_at=datetime.fromisoformat(row["created_at"])
        )

    def record_circuit_event_sync(self, event: CircuitEvent) -> None:
        conn = self._get_connection()
        savings_str = event.counterfactual_savings.model_dump_json() if event.counterfactual_savings else None
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO circuit_events (
                    id, swarm_id, project_id, span_id, event_type, reason, metric_val, savings_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.id,
                event.swarm_id,
                getattr(event, "project_id", "default-project"),
                event.span_id,
                event.event_type,
                event.reason,
                event.metric_val,
                savings_str,
                event.created_at.isoformat()
            ))

    async def record_circuit_event(self, event: CircuitEvent) -> None:
        self.record_circuit_event_sync(event)

    def get_circuit_events_sync(self, swarm_id: str, project_id: Optional[str] = None) -> List[CircuitEvent]:
        conn = self._get_connection()
        if project_id:
            cursor = conn.execute("""
                SELECT * FROM circuit_events
                WHERE swarm_id = ? AND project_id = ?
                ORDER BY created_at ASC
            """, (swarm_id, project_id))
        else:
            cursor = conn.execute("""
                SELECT * FROM circuit_events
                WHERE swarm_id = ?
                ORDER BY created_at ASC
            """, (swarm_id,))
        rows = cursor.fetchall()
        events = []
        for r in rows:
            savings = None
            if "savings_json" in r.keys() and r["savings_json"]:
                try:
                    savings = CounterfactualSavings.model_validate_json(r["savings_json"])
                except Exception:
                    pass
            events.append(CircuitEvent(
                id=r["id"],
                swarm_id=r["swarm_id"],
                project_id=r["project_id"] if "project_id" in r.keys() else "default-project",
                span_id=r["span_id"],
                event_type=r["event_type"],
                reason=r["reason"],
                metric_val=r["metric_val"],
                counterfactual_savings=savings,
                created_at=datetime.fromisoformat(r["created_at"])
            ))
        return events

    async def get_circuit_events(self, swarm_id: str, project_id: Optional[str] = None) -> List[CircuitEvent]:
        return self.get_circuit_events_sync(swarm_id=swarm_id, project_id=project_id)

    def get_spans(
        self,
        swarm_id: str,
        branch_id: str = "main",
        project_id: Optional[str] = None
    ) -> List[SwarmSpan]:
        """Convenience sync getter for spans, matching query_spans."""
        return self.query_spans(swarm_id=swarm_id, branch_id=branch_id, project_id=project_id)

    async def list_branches(self, swarm_id: str, project_id: Optional[str] = None) -> List[str]:
        conn = self._get_connection()
        if project_id:
            cursor = conn.execute("""
                SELECT DISTINCT branch_id FROM spans WHERE swarm_id = ? AND project_id = ?
                UNION
                SELECT DISTINCT branch_id FROM snapshots WHERE branch_id IS NOT NULL AND branch_id != ''
            """, (swarm_id, project_id))
        else:
            cursor = conn.execute("""
                SELECT DISTINCT branch_id FROM spans WHERE swarm_id = ?
                UNION
                SELECT DISTINCT branch_id FROM snapshots WHERE branch_id IS NOT NULL AND branch_id != ''
            """, (swarm_id,))
        rows = cursor.fetchall()
        branches = [r["branch_id"] for r in rows if r["branch_id"]]
        if "main" not in branches:
            branches.insert(0, "main")
        return branches

    async def get_snapshots_for_swarm(
        self,
        swarm_id: str,
        branch_id: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> List[StateSnapshot]:
        conn = self._get_connection()
        query = """
            SELECT s.* FROM snapshots s
            JOIN spans sp ON s.span_id = sp.id
            WHERE sp.swarm_id = ?
        """
        params = [swarm_id]
        if branch_id:
            query += " AND s.branch_id = ?"
            params.append(branch_id)
        if project_id:
            query += " AND s.project_id = ?"
            params.append(project_id)
        query += " ORDER BY s.created_at ASC"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        snaps = []
        for row in rows:
            try:
                decompressed_json = gzip.decompress(row["state_data_compressed"]).decode("utf-8")
                state_data = json.loads(decompressed_json)
                snaps.append(StateSnapshot(
                    id=row["id"],
                    span_id=row["span_id"],
                    project_id=row["project_id"] if "project_id" in row.keys() else "default-project",
                    branch_id=row["branch_id"],
                    state_hash=row["state_hash"],
                    state_data=state_data,
                    created_at=datetime.fromisoformat(row["created_at"])
                ))
            except Exception:
                continue
        return snaps

    def list_swarms_sync(self, project_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        if project_id:
            cursor = conn.execute("""
                SELECT swarm_id, project_id, branch_id,
                       COUNT(id) as turn_count,
                       MAX(start_time) as last_activity,
                       AVG(entropy_score) as avg_entropy,
                       MIN(entropy_score) as min_entropy
                FROM spans
                WHERE project_id = ?
                GROUP BY swarm_id, project_id, branch_id
                ORDER BY last_activity DESC
                LIMIT ?
            """, (project_id, limit))
        else:
            cursor = conn.execute("""
                SELECT swarm_id, project_id, branch_id,
                       COUNT(id) as turn_count,
                       MAX(start_time) as last_activity,
                       AVG(entropy_score) as avg_entropy,
                       MIN(entropy_score) as min_entropy
                FROM spans
                GROUP BY swarm_id, project_id, branch_id
                ORDER BY last_activity DESC
                LIMIT ?
            """, (limit,))
        rows = cursor.fetchall()
        swarms = []
        for r in rows:
            cur_trip = conn.execute(
                "SELECT reason, metric_val FROM circuit_events WHERE swarm_id = ? AND event_type = 'trip' ORDER BY created_at DESC LIMIT 1",
                (r["swarm_id"],)
            ).fetchone()
            swarms.append({
                "swarm_id": r["swarm_id"],
                "project_id": r["project_id"] if "project_id" in r.keys() else "default-project",
                "branch_id": r["branch_id"],
                "turn_count": r["turn_count"],
                "last_activity": str(r["last_activity"]) if r["last_activity"] else "",
                "avg_entropy": round(float(r["avg_entropy"]), 2) if r["avg_entropy"] is not None else 1.0,
                "min_entropy": round(float(r["min_entropy"]), 2) if r["min_entropy"] is not None else 1.0,
                "status": "HALTED" if cur_trip else "RUNNING",
                "halt_reason": cur_trip["reason"] if cur_trip else None,
            })
        return swarms

    async def list_swarms(self, project_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return self.list_swarms_sync(project_id=project_id, limit=limit)

    # Enterprise User & MFA CRUD
    def save_user_sync(self, user: UserAccount) -> None:
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO users (
                    id, username, email, role, mfa_enabled, totp_secret, backup_codes_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user.id,
                user.username,
                user.email,
                user.role.value if isinstance(user.role, Role) else str(user.role),
                1 if user.mfa_enabled else 0,
                user.totp_secret,
                json.dumps(user.backup_codes),
                user.created_at.isoformat() if isinstance(user.created_at, datetime) else str(user.created_at)
            ))

    async def save_user(self, user: UserAccount) -> None:
        self.save_user_sync(user)

    def get_user_by_username_sync(self, username: str) -> Optional[UserAccount]:
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_user(row)

    async def get_user_by_username(self, username: str) -> Optional[UserAccount]:
        return self.get_user_by_username_sync(username)

    def get_user_sync(self, user_id: str) -> Optional[UserAccount]:
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_user(row)

    async def get_user(self, user_id: str) -> Optional[UserAccount]:
        return self.get_user_sync(user_id)

    def list_users_sync(self) -> List[UserAccount]:
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM users ORDER BY created_at ASC")
        rows = cursor.fetchall()
        return [self._row_to_user(r) for r in rows]

    async def list_users(self) -> List[UserAccount]:
        return self.list_users_sync()

    def _row_to_user(self, r: sqlite3.Row) -> UserAccount:
        created_at = datetime.fromisoformat(r["created_at"]) if isinstance(r["created_at"], str) else r["created_at"]
        backup_codes = json.loads(r["backup_codes_json"]) if r["backup_codes_json"] else []
        role_val = Role(r["role"]) if r["role"] in [e.value for e in Role] else Role.VIEWER
        return UserAccount(
            id=r["id"],
            username=r["username"],
            email=r["email"],
            role=role_val,
            mfa_enabled=bool(r["mfa_enabled"]),
            totp_secret=r["totp_secret"],
            backup_codes=backup_codes,
            created_at=created_at,
        )

    # Enterprise Audit Trail
    def record_audit_event_sync(
        self,
        action: str,
        resource: str,
        status: str,
        user_id: Optional[str] = None,
        username: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        conn = self._get_connection()
        import uuid
        with conn:
            conn.execute("""
                INSERT INTO audit_events (
                    id, timestamp, user_id, username, action, resource, status, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()),
                datetime.now(timezone.utc).isoformat(),
                user_id,
                username,
                action,
                resource,
                status,
                json.dumps(details) if details else "{}"
            ))

    async def record_audit_event(
        self,
        action: str,
        resource: str,
        status: str,
        user_id: Optional[str] = None,
        username: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.record_audit_event_sync(action, resource, status, user_id, username, details)

    def list_audit_events_sync(self, limit: int = 100) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT * FROM audit_events ORDER BY timestamp DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        events = []
        for r in rows:
            events.append({
                "id": r["id"],
                "timestamp": str(r["timestamp"]),
                "user_id": r["user_id"],
                "username": r["username"],
                "action": r["action"],
                "resource": r["resource"],
                "status": r["status"],
                "details": json.loads(r["details_json"]) if r["details_json"] else {},
            })
        return events

    async def list_audit_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.list_audit_events_sync(limit=limit)

    def save_push_subscription_sync(self, sub: Any) -> None:
        conn = self._get_connection()
        topics = getattr(sub, 'topics', ['circuit_trip', 'circuit_warning', 'fleet_trip'])
        topics_json = json.dumps(topics)
        keys = getattr(sub, 'keys', None)
        p256dh = getattr(keys, 'p256dh', '') if keys else ''
        auth = getattr(keys, 'auth', '') if keys else ''
        sub_id = getattr(sub, 'id', f"sub-{uuid.uuid4().hex[:12]}")
        project_id = getattr(sub, 'project_id', 'default-project')
        endpoint = getattr(sub, 'endpoint', '')
        user_id = getattr(sub, 'user_id', None)
        with conn:
            conn.execute("""
                INSERT INTO push_subscriptions (id, project_id, endpoint, p256dh, auth, user_id, topics_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(endpoint) DO UPDATE SET
                    p256dh = excluded.p256dh,
                    auth = excluded.auth,
                    user_id = excluded.user_id,
                    topics_json = excluded.topics_json,
                    project_id = excluded.project_id
            """, (
                sub_id,
                project_id,
                endpoint,
                p256dh,
                auth,
                user_id,
                topics_json,
                datetime.now(timezone.utc).isoformat()
            ))

    async def save_push_subscription(self, sub: Any) -> None:
        self.save_push_subscription_sync(sub)

    def delete_push_subscription_sync(self, endpoint: str) -> bool:
        conn = self._get_connection()
        with conn:
            cursor = conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
            return cursor.rowcount > 0

    async def delete_push_subscription(self, endpoint: str) -> bool:
        return self.delete_push_subscription_sync(endpoint)

    def get_push_subscription_sync(self, endpoint: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        r = cursor.fetchone()
        if not r:
            return None
        return {
            "id": r["id"],
            "project_id": r["project_id"],
            "endpoint": r["endpoint"],
            "keys": {"p256dh": r["p256dh"], "auth": r["auth"]},
            "user_id": r["user_id"],
            "topics": json.loads(r["topics_json"]) if r["topics_json"] else [],
            "created_at": str(r["created_at"]),
        }

    async def get_push_subscription(self, endpoint: str) -> Optional[Dict[str, Any]]:
        return self.get_push_subscription_sync(endpoint)

    def list_push_subscriptions_sync(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        if project_id and project_id != 'default-project':
            cursor = conn.execute("SELECT * FROM push_subscriptions WHERE project_id = ? ORDER BY created_at DESC", (project_id,))
        else:
            cursor = conn.execute("SELECT * FROM push_subscriptions ORDER BY created_at DESC")
        rows = cursor.fetchall()
        subs = []
        for r in rows:
            subs.append({
                "id": r["id"],
                "project_id": r["project_id"],
                "endpoint": r["endpoint"],
                "keys": {"p256dh": r["p256dh"], "auth": r["auth"]},
                "user_id": r["user_id"],
                "topics": json.loads(r["topics_json"]) if r["topics_json"] else [],
                "created_at": str(r["created_at"]),
            })
        return subs

    async def list_push_subscriptions(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.list_push_subscriptions_sync(project_id=project_id)

    def close_sync(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    async def close(self) -> None:
        self.close_sync()


# Export ClickHouseStorageAdapter for seamless access via swarmwatch.storage
from swarmwatch.storage_clickhouse import ClickHouseStorageAdapter  # noqa: E402

__all__ = ["StorageAdapter", "SQLiteStorageAdapter", "ClickHouseStorageAdapter"]
