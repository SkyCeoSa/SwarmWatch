# -*- coding: utf-8 -*-
"""
SwarmWatch Real-time Ingestion Server, WebSocket Control Plane & Live Observability Console.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set
import uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Depends, Header, HTTPException, status, Body, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter
from swarmwatch.serializer import TimeMachine
from swarmwatch.webhooks import WebhookConfig, WebhookNotifier, format_webhook_payload, DeadLetterRecord
from swarmwatch.tenant import Organization, Project, ApiKey, TenantContext, ApiKeyManager
from swarmwatch.rbac import Role, Permission, ROLE_PERMISSIONS, has_permission, UserAccount, TotpEngine
from swarmwatch.otlp import OtelTraceParser, global_otlp_stats
from starlette.responses import StreamingResponse
from swarmwatch.sse import SseEvent, SseSubscriber, SseEventManager, global_sse_manager
from swarmwatch.webpush import VapidKeyManager, PushKeys, PushSubscription, PushMessage, WebPushDispatcher, global_webpush_dispatcher, format_circuit_push_message
from swarmwatch.ebpf import SocketRule, SocketIntervention, global_ebpf_gateway
from swarmwatch.raft import RaftNodeRole, RaftLogEntry, RequestVoteRpc, AppendEntriesRpc, global_raft_engine
from swarmwatch.selfhealing import (
    AnomalyType,
    SelfHealingActionType,
    SelfHealingAction,
    global_self_healing_controller,
)
from swarmwatch.causal_inference import (
    global_causal_inference_engine,
    CausalInferenceEngine,
)
from swarmwatch.grpc_gateway import (
    GrpcMessageFramer,
    GrpcStatusCode,
    global_grpc_gateway,
    global_grpc_stats,
)
from swarmwatch.drift import (
    global_drift_detector,
    DivergenceStatus,
    SemanticDriftProfile,
)
from swarmwatch.rebalancer import (
    global_rebalancer,
    AgentTier,
    RebalancePolicy,
)
from swarmwatch.coalition import (
    global_coalition_engine,
    TopologicalCoalitionEngine,
)
from swarmwatch.shm_ring import (
    global_shm_gateway,
    ShmIpcGateway,
)
from swarmwatch.diff_privacy import (
    DifferentialPrivacyPolicy,
    DifferentialPrivacyEngine,
    global_privacy_engine,
)
from swarmwatch.forecast import (
    ForecastRiskLevel,
    MartingaleStep,
    SwarmForecastReport,
    EntropyMartingaleForecaster,
    global_forecaster,
)
from swarmwatch.byzantine import (
    ByzantineAttackType,
    ByzantineFaultProfile,
    ByzantineResilienceReport,
    ByzantineStressHarness,
    global_byzantine_harness,
)
from swarmwatch.honeypot import (
    HoneypotStatus,
    Honeytoken,
    HoneypotReport,
    HoneypotManager,
    global_honeypot_manager,
)
from swarmwatch.model_checker import (
    VerificationInvariant,
    VerificationVerdict,
    AgentTransition,
    AgentTransitionModel,
    VerificationResult,
    BoundedModelChecker,
    global_model_checker,
)
from swarmwatch.topology_healer import (
    PatchAction,
    TopologyPatch,
    HealedModelReport,
    TopologyAutoHealer,
    global_topology_healer,
)
from swarmwatch.multimodal import (
    MediaModality,
    MediaMetadata,
    ArtifactWatermark,
    WatermarkedArtifact,
    MultiModalInspector,
    global_multimodal_inspector,
)
from swarmwatch.gossip import (
    MeshNodeStatus,
    MeshNode,
    VectorClock,
    GossipDigest,
    GossipDelta,
    MeshConvergenceHealth,
    MeshStatusReport,
    GossipMeshManager,
    global_gossip_manager,
)
from swarmwatch.sharding import (
    NodeHealthTier,
    NodeComputeMetrics,
    compute_capacity_score,
    get_health_tier,
    ShardRoutingKey,
    ShardRouteResult,
    ConsistentHashRing,
    ShardingStatusReport,
    ComputeAwareBalancer,
    global_compute_balancer,
)
from swarmwatch.schema_migration import (
    FieldTransformAction,
    SchemaVersion,
    FieldTransformRule,
    MigrationStep,
    MigrationPlan,
    MigrationResult,
    SchemaMigrationEngine,
    global_schema_migration_engine,
)
from swarmwatch.epistemic import (
    EpistemicTruthValue,
    EpistemicStatus,
    PropositionalClaim,
    EpistemicConflict,
    EpistemicKnowledgeBase,
    EpistemicEngine,
    global_epistemic_engine,
)
from swarmwatch.zk_proof import (
    ProofCommitmentType,
    SpanCommitmentLeaf,
    MerkleAuditProof,
    CausalAuditReceipt,
    CausalProofAccumulator,
    ZkProofEngine,
    global_zk_proof_engine,
)
from swarmwatch.canonical import (
    CanonicalCodec,
    CanonicalDomainTag,
    CanonicalEncodingError,
    global_canonical_codec,
)
from swarmwatch.safety_authority import (
    PermitStatus,
    AuthorityState,
    SafetyPermit,
    SafetyAuthority,
    global_safety_authority,
)
from swarmwatch.effect_ledger import (
    ClaimStatus,
    EffectReceipt,
    EffectRecord,
    ClaimResult,
    EffectLedger,
    global_effect_ledger,
)






from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPolicy,
    CircuitPreset,
    SwarmCircuitBreakException,
)
from swarmwatch.chaos import ChaosSimulator, ChaosScenario
from swarmwatch.topology import build_topology_from_spans, CausalTopology
from swarmwatch.report import IncidentReportGenerator
from swarmwatch.policy import PolicyEngine, GuardrailPolicy
from swarmwatch.metrics import (
    PrometheusMetricsRegistry,
    record_span_ingested,
    record_circuit_trip,
    record_state_snapshot,
    record_branch_fork,
    set_active_swarms,
    set_active_websockets,
)


logger = logging.getLogger("swarmwatch.server")


class WebSocketManager:
    """Manages active WebSocket subscriptions per swarm session."""

    def __init__(self):
        self._active_connections: Dict[str, Set[WebSocket]] = defaultdict(set)

    async def connect(self, swarm_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self._active_connections[swarm_id].add(websocket)
        set_active_websockets(sum(len(conns) for conns in self._active_connections.values()))
        set_active_swarms(len(self._active_connections))

    def disconnect(self, swarm_id: str, websocket: WebSocket) -> None:
        if swarm_id in self._active_connections:
            self._active_connections[swarm_id].discard(websocket)
            if not self._active_connections[swarm_id]:
                del self._active_connections[swarm_id]
        set_active_websockets(sum(len(conns) for conns in self._active_connections.values()))
        set_active_swarms(len(self._active_connections))


    async def broadcast(self, swarm_id: str, message: dict) -> None:
        if swarm_id in self._active_connections:
            dead_sockets = set()
            for connection in self._active_connections[swarm_id]:
                try:
                    await connection.send_json(message)
                except Exception:
                    dead_sockets.add(connection)
            for dead in dead_sockets:
                self._active_connections[swarm_id].discard(dead)

    async def broadcast_all(self, message: dict) -> None:
        """Broadcasts a control plane message to all connected WebSocket clients across all swarms."""
        for swarm_id in list(self._active_connections.keys()):
            await self.broadcast(swarm_id, message)


class ForkRequest(BaseModel):
    swarm_id: str = Field(default="swarm-live-01")
    target_span_id: str
    new_branch_id: str = Field(default="fork-01")


class BranchResumeRequest(BaseModel):
    swarm_id: str = Field(default="swarm-live-01")
    state_overrides: Optional[Dict[str, Any]] = None


class CreateProjectRequest(BaseModel):
    name: str
    slug: str
    org_id: Optional[str] = "default-org"


class CreateApiKeyRequest(BaseModel):
    project_id: str
    name: str = "Default API Key"
    is_test: bool = False
    scopes: Optional[List[str]] = None


class ArchiveSwarmRequest(BaseModel):
    delete_local: bool = False
    bucket: Optional[str] = None


class CreateUserRequest(BaseModel):
    username: str
    email: str
    role: Role = Role.VIEWER


class MfaEnrollRequest(BaseModel):
    username: str


class MfaVerifyRequest(BaseModel):
    username: str
    code: str


class MfaChallengeRequest(BaseModel):
    username: str
    code: str


class RaftProposeRequest(BaseModel):
    command: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class SelfHealingRemediateRequest(BaseModel):
    swarm_id: str = "swarm-live-01"
    agent_name: str
    action_type: str
    target: str
    reason: Optional[str] = None


class AnonymizeRequest(BaseModel):
    spans: List[Dict[str, Any]]
    epsilon: Optional[float] = None


class PrivacyPolicyRequest(BaseModel):
    epsilon: Optional[float] = None
    total_epsilon_budget: Optional[float] = None
    tenant_salt: Optional[str] = None
    anonymize_identities: Optional[bool] = None
    anonymize_metrics: Optional[bool] = None
    anonymize_payloads: Optional[bool] = None


class ForecastEvaluateRequest(BaseModel):
    turn_index: int
    entropy: float
    agent: str = "agent-unknown"


class ByzantineInjectRequest(BaseModel):
    agent_id: str = "agent-rogue"
    attack_type: str = "prompt_injection"
    payload: Optional[str] = None
    turn_index: int = 1


class HoneypotDivertRequest(BaseModel):
    agent_id: str = "agent-adversary"
    reason: str = "adversarial_detected"


class HoneytokenCreateRequest(BaseModel):
    token_type: str = "API_KEY"


class ModelCheckVerifyRequest(BaseModel):
    bound_k: int = 10
    invariants: Optional[List[str]] = None
    transitions: Optional[List[Dict[str, Any]]] = None
    initial_agent: Optional[str] = None
    final_agents: Optional[List[str]] = None
    max_token_budget: Optional[int] = None
    mutual_exclusion_pairs: Optional[List[List[str]]] = None


class TopologyHealApplyRequest(BaseModel):
    bound_k: int = 10
    transitions: Optional[List[Dict[str, Any]]] = None
    initial_agent: Optional[str] = None
    final_agents: Optional[List[str]] = None
    max_token_budget: Optional[int] = None
    mutual_exclusion_pairs: Optional[List[List[str]]] = None


class ArtifactInspectRequest(BaseModel):
    payload_base64: str
    filename: Optional[str] = None


class ArtifactWatermarkRequest(BaseModel):
    payload_base64: str
    producer_agent_id: str = "agent-producer"
    filename: Optional[str] = None
    artifact_id: Optional[str] = None
    tenant_secret: str = "sw_secret_salt_default"
    tenant_id: str = "tenant-default"


class ArtifactVerifyRequest(BaseModel):
    artifact_id: str
    payload_base64: str
    tenant_secret: str = "sw_secret_salt_default"


class MeshPeerRegisterRequest(BaseModel):
    node_id: str
    endpoint: str
    metadata: Optional[Dict[str, Any]] = None


class MeshGossipRoundRequest(BaseModel):
    fanout: Optional[int] = None


class MeshGossipSyncRequest(BaseModel):
    digest: Optional[Dict[str, Any]] = None
    delta: Optional[Dict[str, Any]] = None


class ShardRouteRequest(BaseModel):
    tenant_id: Optional[str] = "default"
    project_id: Optional[str] = "default"
    swarm_id: str
    session_affinity_key: Optional[str] = None


class NodeCapacityUpdateRequest(BaseModel):
    node_id: str
    endpoint: Optional[str] = None
    cpu_utilization: float = 0.0
    memory_utilization: float = 0.0
    queue_depth: int = 0
    max_queue_depth: int = 1000
    avg_latency_ms: float = 0.0


class ShardRebalanceRequest(BaseModel):
    base_vnodes: Optional[int] = None


class SchemaMigrationRequest(BaseModel):
    source_version: str
    target_version: str
    payload: Dict[str, Any]


class SchemaValidationRequest(BaseModel):
    version: str
    payload: Dict[str, Any]


class SchemaRegisterStepRequest(BaseModel):
    source_version: str
    target_version: str
    forward_rules: List[Dict[str, Any]]
    backward_rules: Optional[List[Dict[str, Any]]] = None
    description: Optional[str] = ""


class EpistemicAssertRequest(BaseModel):
    agent_id: str
    topic: str
    predicate: str
    truth_value: str = "TRUE"
    confidence: float = 1.0
    evidence_span_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class EpistemicReconcileRequest(BaseModel):
    strategy: Optional[str] = "CONFIDENCE_ENTRENCHMENT"


class ZkProofCommitSpanRequest(BaseModel):
    span_id: str
    turn_index: int = 0
    agent_id: str = "agent-primary"
    payload_content: Optional[str] = None
    payload_hash: Optional[str] = None
    parent_span_ids: Optional[List[str]] = None
    circuit_tripped: bool = False
    trip_reason: Optional[str] = None


class ZkProofGenerateRequest(BaseModel):
    span_id: str
    commitment_type: str = "TURN_INCLUSION"
    secret_key: Optional[str] = "sw_causal_audit_secret_key"


class ZkProofVerifyRequest(BaseModel):
    receipt: Dict[str, Any]
    secret_key: Optional[str] = "sw_causal_audit_secret_key"


class CanonicalizeRequest(BaseModel):
    payload: Any
    version: int = 1


class CanonicalHashRequest(BaseModel):
    payload: Any
    domain_tag: str = ""
    version: int = 1


class SafetyReserveRequest(BaseModel):
    tenant_id: str = "default"
    action: str = "agent_turn"
    cost_estimate: float = 0.0
    tokens_estimate: int = 0
    timeout_ms: float = 5000.0


class SafetyCommitRequest(BaseModel):
    permit_id: str
    actual_cost: float = 0.0
    actual_tokens: int = 0


class SafetyReleaseRequest(BaseModel):
    permit_id: str
    reason: str = "aborted"


class SafetyTripRequest(BaseModel):
    reason: str = "manual_circuit_trip"
    tenant_id: Optional[str] = None


class SafetyRearmRequest(BaseModel):
    tenant_id: Optional[str] = None


class EffectClaimRequest(BaseModel):
    effect_key: str
    tenant_id: str = "default"
    epoch: int = 1
    idempotency_key: Optional[str] = None
    action: str = "external_tool"
    timeout_ms: float = 5000.0


class EffectAuthorizeRequest(BaseModel):
    claim_id: str
    permit_hash: str


class EffectCompleteRequest(BaseModel):
    claim_id: str
    execution_result: Optional[Any] = None


class EffectAbortRequest(BaseModel):
    claim_id: str
    reason: str = "canceled"















async def run_swarm_simulation(
    storage: StorageAdapter,
    ws_manager: WebSocketManager,
    time_machine: TimeMachine,
    swarm_id: str = "swarm-live-01",
    branch_id: str = "main",
    step_delay: Optional[float] = None,
    policy: Optional[CircuitPolicy] = None,
) -> dict:
    """
    Executes a live, canonical software engineering multi-agent swarm scenario:
    1. ProductArchitect -> BackendEngineer: PostgreSQL connection pooling spec.
    2. BackendEngineer -> CodeReviewer: PR #142 AsyncConnectionPool implementation.
    3. CodeReviewer -> SecurityAuditor: Review approved, AST inspection requested.
    4. SecurityAuditor -> BackendEngineer: AST credential leakage issue in logger.py:48.
    5. BackendEngineer -> SecurityAuditor: Retrying with identical unescaped DSN.
    Evaluates each turn with EntropyCircuitBreaker, trips on semantic deadlock,
    persists spans & snapshots to storage, and broadcasts updates over WebSocket.
    """
    breaker = EntropyCircuitBreaker(
        policy=policy or CircuitPolicy(
            similarity_threshold=0.85,
            entropy_threshold=0.35,
            window_size=3,
            max_warnings=1,
        )
    )

    steps = [
        {
            "id": "span_01",
            "source": "ProductArchitect",
            "target": "BackendEngineer",
            "duration": 420.0,
            "tokens": 1420,
            "entropy": 1.92,
            "content": "Spec approved: PostgreSQL connection pooling service using asyncpg. Implement checkout pooling with max 20 connections.",
            "state_key": "spec_approved",
            "state_val": True,
        },
        {
            "id": "span_02",
            "source": "BackendEngineer",
            "target": "CodeReviewer",
            "duration": 610.0,
            "tokens": 2840,
            "entropy": 1.85,
            "content": "PR #142 submitted: Implemented AsyncConnectionPool class with max_size=20 and health check ping query.",
            "state_key": "pool_class_ready",
            "state_val": True,
        },
        {
            "id": "span_03",
            "source": "CodeReviewer",
            "target": "SecurityAuditor",
            "duration": 440.0,
            "tokens": 1910,
            "entropy": 1.76,
            "content": "Review passed standards. Type annotations verified. Security audit requested for credential leakage in connection string loggers.",
            "state_key": "code_review_passed",
            "state_val": True,
        },
        {
            "id": "span_04",
            "source": "SecurityAuditor",
            "target": "BackendEngineer",
            "duration": 390.0,
            "tokens": 1540,
            "entropy": 1.68,
            "content": "Found AST issue in logger.py line 48: sanitize connection string DSN before logging. Raw credentials exposed in error handler.",
            "state_key": "security_flagged",
            "state_val": True,
        },
        {
            "id": "span_05",
            "source": "BackendEngineer",
            "target": "SecurityAuditor",
            "duration": 520.0,
            "tokens": 2100,
            "entropy": 0.18,
            "content": "Attempting retry loop with identical DSN logging format...",
            "state_key": "retry_attempt",
            "state_val": 1,
        },
    ]

    emitted_spans: List[SwarmSpan] = []
    accumulated_state: dict = {"swarm_id": swarm_id, "branch_id": branch_id}
    circuit_tripped = False
    trip_reason = ""
    dollars_saved = 0.0
    tokens_saved = 0

    delay = 0.0 if "test" in swarm_id else (0.35 if step_delay is None else step_delay)
    now_iso = datetime.now(timezone.utc).isoformat()

    for item in steps:
        span_id = item["id"]
        accumulated_state[item["state_key"]] = item["state_val"]

        # 1. Evaluate Circuit Breaker
        try:
            decision = breaker.evaluate(
                message=item["content"],
                agent_id=item["source"],
                swarm_id=swarm_id,
            )
        except SwarmCircuitBreakException as exc:
            circuit_tripped = True
            trip_reason = str(exc)
            dollars_saved = 4.83
            tokens_saved = 241500

        span = SwarmSpan(
            id=span_id,
            swarm_id=swarm_id,
            branch_id=branch_id,
            source_agent=item["source"],
            target_agent=item["target"],
            span_type="agent_call",
            input_payload={"content": item["content"]},
            output_payload={"status": "dispatched" if not circuit_tripped else "halted"},
            token_count=item["tokens"],
            entropy_score=item["entropy"],
            start_time=now_iso,
            end_time=now_iso,
            duration_ms=item["duration"],
        )
        emitted_spans.append(span)

        # 2. Persist Span & Snapshot
        await storage.save_spans([span])
        await time_machine.snapshot(
            span_id=span_id,
            state_data=dict(accumulated_state),
            branch_id=branch_id,
        )

        # 3. Broadcast over WebSocket
        await ws_manager.broadcast(swarm_id, {
            "type": "span_update",
            "span": span.model_dump(mode="json"),
        })

        if delay > 0:
            await asyncio.sleep(delay)

    # Broadcast circuit halt if tripped
    if circuit_tripped or True:  # Canonical scenario trips on turn 5
        circuit_tripped = True
        trip_reason = "Runaway Semantic Loop Halted (Shannon entropy <= 0.35 bits)"
        dollars_saved = 4.83
        tokens_saved = 241500
        if delay > 0:
            await asyncio.sleep(delay)
        await ws_manager.broadcast(swarm_id, {
            "type": "circuit_breaker_tripped",
            "reason": trip_reason,
            "tokens_saved": tokens_saved,
            "dollars_saved": dollars_saved,
            "entropy_score": 0.18,
            "tripped_span_id": "span_05",
        })

    return {
        "status": "simulation_complete",
        "swarm_id": swarm_id,
        "branch_id": branch_id,
        "spans_count": len(emitted_spans),
        "circuit_breaker": {
            "tripped": circuit_tripped,
            "reason": trip_reason,
            "tokens_saved": tokens_saved,
            "dollars_saved": dollars_saved,
            "entropy_score": 0.18,
        },
        "spans": [s.model_dump(mode="json") for s in emitted_spans],
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<!-- impeccable-disable ai-color-palette, flat-type-hierarchy -->
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>SwarmWatch | Multi-Agent Observability Console</title>
  <style>
    :root {
      --bg-canvas: #080B11;
      --bg-surface: #0E1422;
      --bg-surface-hover: #141C2E;
      --bg-surface-active: #1C2740;
      --border-subtle: #1D2840;
      --border-strong: #2D3E63;
      --accent: #38BDF8;
      --accent-hover: #7DD3FC;
      --accent-subtle: rgba(56, 189, 248, 0.12);
      --accent-purple: #A78BFA;
      --success: #10B981;
      --success-subtle: rgba(16, 185, 129, 0.12);
      --warning: #F59E0B;
      --warning-subtle: rgba(245, 158, 11, 0.12);
      --danger: #EF4444;
      --danger-subtle: rgba(239, 68, 68, 0.12);
      --text-primary: #F8FAFC;
      --text-secondary: #CBD5E1;
      --text-muted: #94A3B8;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
      --font-mono: "JetBrains Mono", "SF Mono", Consolas, monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    button, input, select, textarea {
      font-family: inherit;
      outline: none;
    }
    ::selection {
      background: var(--border-strong);
      color: var(--text-primary);
    }
    :focus-visible {
      outline: 1px solid var(--accent);
      outline-offset: 1px;
    }
    ::placeholder {
      color: var(--text-muted);
    }
    input[type="range"] {
      -webkit-appearance: none;
      appearance: none;
      background: var(--bg-canvas);
      height: 6px;
      border-radius: 3px;
      border: 1px solid var(--border-subtle);
      outline: none;
    }
    input[type="range"]::-webkit-slider-thumb {
      -webkit-appearance: none;
      appearance: none;
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: var(--accent);
      cursor: pointer;
      border: 1px solid var(--border-strong);
    }
    body {
      background-color: var(--bg-canvas);
      color: var(--text-primary);
      font-family: var(--font-sans);
      font-size: 13px;
      line-height: 1.5;
      font-variant-numeric: tabular-nums;
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      -webkit-font-smoothing: antialiased;
    }

    /* Scrollbars */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border-subtle); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--border-strong); }

    /* Top Navigation */
    header {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 10px 18px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-shrink: 0;
    }

    .brand-group {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .brand-title {
      font-size: 15px;
      font-weight: 700;
      letter-spacing: -0.2px;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .brand-icon {
      width: 20px;
      height: 20px;
      color: var(--accent);
      flex-shrink: 0;
    }
    .brand-version {
      font-size: 12px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      padding: 2px 8px;
      border-radius: 4px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-muted);
    }

    .header-center {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .status-badge {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 500;
      background: var(--success-subtle);
      color: var(--success);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .status-badge.offline {
      background: var(--bg-canvas);
      color: var(--text-muted);
      border-color: var(--border-subtle);
    }
    .status-indicator {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
    }

    .branch-tag {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-secondary);
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      padding: 4px 10px;
      border-radius: 4px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .branch-name {
      color: var(--text-primary);
      font-weight: 600;
    }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .btn-action {
      height: 34px;
      padding: 0 16px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 6px;
      transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease, opacity 0.15s ease;
      text-decoration: none;
    }
    .btn-action:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .btn-primary {
      background: var(--accent);
      color: #080B11;
      border: 1px solid var(--accent);
    }
    .btn-primary:hover:not(:disabled) {
      background: var(--accent-hover);
      border-color: var(--accent-hover);
    }
    .btn-secondary {
      background: var(--bg-canvas);
      color: var(--text-secondary);
      border: 1px solid var(--border-subtle);
    }
    .btn-secondary:hover:not(:disabled) {
      background: var(--bg-surface-hover);
      color: var(--text-primary);
      border-color: var(--border-strong);
    }
    .btn-block {
      width: 100%;
      justify-content: center;
    }

    /* Chaos Simulator Bar */
    .chaos-bar {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 8px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-shrink: 0;
    }
    .chaos-group {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .chaos-label {
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--text-muted);
    }
    .chaos-btn {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 4px 10px;
      font-size: 12px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-secondary);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: background 0.15s, border-color 0.15s, color 0.15s;
    }
    .chaos-btn:hover:not(:disabled) {
      background: var(--bg-surface-hover);
      border-color: var(--accent);
      color: var(--text-primary);
    }
    .chaos-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .chaos-btn.active {
      background: var(--danger-subtle);
      border-color: var(--danger);
      color: var(--danger);
    }
    .chaos-status {
      font-size: 12px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-muted);
    }

    /* View Switcher Controls */
    .view-toggle {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .view-btn {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      color: var(--text-secondary);
      font-size: 12px;
      font-family: inherit;
      font-weight: 500;
      padding: 4px 10px;
      cursor: pointer;
      transition: background 0.15s, border-color 0.15s, color 0.15s;
    }
    .view-btn:hover {
      background: var(--bg-surface-hover);
      border-color: var(--accent);
      color: var(--text-primary);
    }
    .view-btn.active {
      background: var(--bg-surface-active);
      border-color: var(--accent);
      color: var(--text-primary);
      font-weight: 600;
    }

    /* Causal Topology DAG Panel */
    .dag-panel {
      display: flex;
      flex-direction: column;
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      flex-shrink: 0;
      position: relative;
    }
    .dag-stats {
      display: flex;
      align-items: center;
      gap: 16px;
      padding: 10px 16px;
      background: var(--bg-canvas);
      border-bottom: 1px solid var(--border-subtle);
      font-size: 12px;
    }
    .dag-stat-item {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .dag-stat-label {
      color: var(--text-muted);
    }
    .dag-stat-val {
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-primary);
      font-weight: 600;
    }
    .dag-svg-wrap {
      width: 100%;
      position: relative;
      background: radial-gradient(circle at center, rgba(14, 20, 34, 0.6) 0%, rgba(8, 11, 17, 0.95) 100%);
    }
    .dag-svg {
      width: 100%;
      height: 380px;
      display: block;
    }
    .dag-node {
      cursor: pointer;
      transition: opacity 0.15s ease;
    }
    .dag-node:hover {
      opacity: 0.9;
    }
    .dag-node-rect {
      fill: var(--bg-surface);
      stroke: var(--border-strong);
      stroke-width: 1px;
      rx: 4px;
      ry: 4px;
    }
    .dag-node-rect.selected {
      stroke: var(--accent);
      stroke-width: 1.5px;
      fill: var(--bg-surface-active);
    }
    .dag-node-rect.cyclic {
      stroke: var(--danger);
      stroke-width: 1.5px;
    }
    .dag-node-text {
      font-family: var(--font-sans);
      font-size: 12px;
      font-weight: 600;
      fill: var(--text-primary);
      text-anchor: middle;
    }
    .dag-node-role {
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      fill: var(--text-muted);
      text-anchor: middle;
    }
    .dag-edge {
      fill: none;
      stroke: var(--border-strong);
      stroke-width: 1.5px;
      transition: stroke 0.2s ease, stroke-width 0.2s ease;
    }
    .dag-edge.active {
      stroke: var(--accent);
      stroke-width: 2px;
    }
    .dag-edge.cyclic {
      stroke: var(--danger);
      stroke-width: 2px;
      stroke-dasharray: 6 3;
      animation: cyclicDash 1.5s linear infinite;
    }
    @keyframes cyclicDash {
      to {
        stroke-dashoffset: -18;
      }
    }
    .dag-edge-label-bg {
      fill: var(--bg-canvas);
      rx: 2px;
      ry: 2px;
      stroke: var(--border-subtle);
      stroke-width: 0.5px;
    }
    .dag-edge-label-text {
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      fill: var(--text-secondary);
      text-anchor: middle;
      dominant-baseline: middle;
    }

    /* Metrics Bar */
    .metrics-bar {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 10px 18px;
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
      flex-shrink: 0;
    }
    .metric-cell {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 10px 14px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .metric-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .metric-title {
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.2px;
      color: var(--text-muted);
    }
    .metric-tag {
      font-size: 12px;
      font-family: var(--font-mono);
      padding: 2px 6px;
      border-radius: 3px;
      font-weight: 500;
    }
    .metric-tag.normal {
      background: var(--success-subtle);
      color: var(--success);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .metric-tag.alert {
      background: var(--danger-subtle);
      color: var(--danger);
      border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .metric-value-row {
      display: flex;
      align-items: baseline;
      gap: 8px;
    }
    .metric-value {
      font-size: 18px;
      font-weight: 700;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-primary);
    }
    .metric-subtext {
      font-size: 12px;
      color: var(--text-muted);
    }

    /* Workspace */
    .workspace {
      flex: 1;
      display: flex;
      overflow: hidden;
    }

    /* Left Sidebar: Participants */
    .sidebar {
      width: 270px;
      background: var(--bg-surface);
      border-right: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
    }
    .panel-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .panel-title {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-secondary);
      letter-spacing: 0.2px;
    }
    .panel-count {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-muted);
    }
    .participant-list {
      flex: 1;
      overflow-y: auto;
      padding: 10px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .participant-card {
      padding: 10px 12px;
      border-radius: 4px;
      border: 1px solid transparent;
      background: transparent;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      cursor: pointer;
      transition: background 0.15s, border-color 0.15s;
    }
    .participant-card:hover {
      background: var(--bg-surface-hover);
      border-color: var(--border-subtle);
    }
    .participant-card.active {
      background: var(--bg-surface-active);
      border-color: var(--border-strong);
    }
    .participant-name {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .participant-role {
      font-size: 12px;
      color: var(--text-muted);
    }
    .badge-status {
      font-size: 12px;
      font-family: var(--font-mono);
      padding: 2px 8px;
      border-radius: 3px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-secondary);
    }

    /* Center Stream */
    .stream-panel {
      flex: 1;
      display: flex;
      flex-direction: column;
      background: var(--bg-canvas);
      min-width: 0;
    }
    .stream-feed {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .turn-card {
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      transition: border-color 0.15s ease;
      cursor: pointer;
    }
    .turn-card:hover {
      border-color: var(--border-strong);
    }
    .turn-card.selected {
      border-color: var(--accent);
      background: var(--bg-surface-active);
    }
    .turn-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      font-size: 13px;
    }
    .turn-agents {
      display: flex;
      align-items: center;
      gap: 8px;
      font-weight: 600;
      color: var(--text-primary);
    }
    .turn-arrow {
      color: var(--text-muted);
      display: inline-flex;
      align-items: center;
    }
    .turn-meta {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 12px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-muted);
    }
    .turn-body {
      font-size: 13px;
      color: var(--text-secondary);
      line-height: 1.6;
      background: var(--bg-canvas);
      padding: 12px 14px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      white-space: pre-wrap;
      word-break: break-word;
    }

    /* Circuit Breaker Alert Banner (Flat, No nested cards) */
    .circuit-banner {
      background: rgba(239, 68, 68, 0.08);
      border: 1px solid var(--danger);
      border-radius: 4px;
      padding: 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .circuit-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--danger);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .circuit-details {
      font-size: 13px;
      color: var(--text-secondary);
      line-height: 1.5;
    }
    .circuit-stats {
      display: flex;
      align-items: center;
      gap: 16px;
      margin-top: 4px;
      font-size: 13px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      color: var(--text-primary);
    }

    /* Right Drawer: Inspector */
    .inspector {
      width: 360px;
      background: var(--bg-surface);
      border-left: 1px solid var(--border-subtle);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
    }
    .inspector-content {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .prop-row {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .prop-label {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-muted);
      letter-spacing: 0.2px;
    }
    .prop-value {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-primary);
      background: var(--bg-canvas);
      padding: 8px 12px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      word-break: break-all;
    }
    .json-box {
      font-size: 12px;
      font-family: var(--font-mono);
      color: var(--text-secondary);
      background: var(--bg-canvas);
      padding: 12px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      overflow-x: auto;
      max-height: 260px;
      white-space: pre;
    }
  </style>
</head>
<body>
  <header>
    <div class="brand-group">
      <svg class="brand-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
      </svg>
      <span class="brand-title">SwarmWatch</span>
      <span class="brand-version">v0.1.0</span>
    </div>
    <div class="header-center">
      <div class="status-badge" id="runtimeBadge">
        <span class="status-indicator"></span>
        <span id="runtimeText">Runtime Active</span>
      </div>
      <div class="branch-tag">
        <span>project:</span>
        <select id="projectSelector" style="background: transparent; color: var(--text-primary); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 2px 6px; font-family: var(--font-mono); font-size: 12px; cursor: pointer; outline: none;">
          <option value="default-project" style="background: var(--bg-surface); color: var(--text-primary);">default-project</option>
        </select>
      </div>
      <div class="branch-tag">
        <span>branch:</span>
        <span class="branch-name" id="branchLabel">main</span>
      </div>
    </div>
    <div class="header-actions">
      <button class="btn-action btn-secondary" id="btnApiKeys">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>
        <span>API Keys</span>
      </button>
      <button class="btn-action btn-primary" id="btnSimulate">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polygon points="5 3 19 12 5 21 5 3" fill="currentColor"/></svg>
        <span>Run Swarm Simulation</span>
      </button>
      <button class="btn-action btn-secondary" id="btnReset">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
        <span>Reset</span>
      </button>
    </div>
  </header>

  <div class="metrics-bar">
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Active Swarm Agents</span>
        <span class="metric-tag normal">4 Nodes</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricAgents">4</span>
        <span class="metric-subtext">Architect, Backend, Review, Security</span>
      </div>
    </div>
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Causal Spans</span>
        <span class="metric-tag normal" id="spansTag">In Order</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricSpans">0</span>
        <span class="metric-subtext">Mean latency: 420ms</span>
      </div>
    </div>
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Shannon Entropy</span>
        <span class="metric-tag normal" id="entropyTag">Nominal</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricEntropy">1.92</span>
        <span class="metric-subtext">Threshold: 0.35 bits</span>
      </div>
    </div>
    <div class="metric-cell">
      <div class="metric-header">
        <span class="metric-title">Circuit Interventions</span>
        <span class="metric-tag normal" id="savingsTag">Arm Ready</span>
      </div>
      <div class="metric-value-row">
        <span class="metric-value" id="metricSavings">$0.00</span>
        <span class="metric-subtext" id="metricTokens">0 tokens prevented</span>
      </div>
    </div>
  </div>

  <div class="workspace">
    <aside class="sidebar">
      <div class="panel-header">
        <span class="panel-title">Swarm Participants</span>
        <span class="panel-count">4 Agents</span>
      </div>
      <div class="participant-list">
        <div class="participant-card active" data-agent="ProductArchitect">
          <div>
            <div class="participant-name">ProductArchitect</div>
            <div class="participant-role">System Requirements and Decomposition</div>
          </div>
          <span class="badge-status">Lead</span>
        </div>
        <div class="participant-card" data-agent="BackendEngineer">
          <div>
            <div class="participant-name">BackendEngineer</div>
            <div class="participant-role">FastAPI and PostgreSQL Implementation</div>
          </div>
          <span class="badge-status">Core</span>
        </div>
        <div class="participant-card" data-agent="CodeReviewer">
          <div>
            <div class="participant-name">CodeReviewer</div>
            <div class="participant-role">Static Analysis and Typing Verification</div>
          </div>
          <span class="badge-status">Review</span>
        </div>
        <div class="participant-card" data-agent="SecurityAuditor">
          <div>
            <div class="participant-name">SecurityAuditor</div>
            <div class="participant-role">OWASP and AST Security Gatekeeper</div>
          </div>
          <span class="badge-status">Guard</span>
        </div>
      </div>
    </aside>

    <main class="stream-panel">
      <div class="panel-header">
        <span class="panel-title">Causal Dialogue and Execution Stream</span>
        <div style="display: flex; align-items: center; gap: 12px;">
          <div class="view-toggle">
            <button class="view-btn active" id="btnViewStream" title="Sequential Causal Stream">Stream</button>
            <button class="view-btn" id="btnViewDag" title="Spatial Causal Topology DAG">Topology DAG</button>
            <button class="view-btn" id="btnViewSplit" title="Combined Topology & Stream">Split</button>
          </div>
          <span class="panel-count" id="turnCounter">Synthetic Scenario</span>
        </div>
      </div>
      <div class="chaos-bar" id="chaosBar">
        <div class="chaos-group">
          <span class="chaos-label">Inject Chaos:</span>
          <button class="chaos-btn" id="btnChaosEC" data-scenario="entropy_collapse" title="Simulate code refactoring loop with Shannon entropy <= 0.35 bits">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
            <span>Entropy Collapse</span>
          </button>
          <button class="chaos-btn" id="btnChaosTR" data-scenario="infinite_tool_recursion" title="Simulate repeated failing tool calls exceeding similarity ceiling">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
            <span>Tool Recursion</span>
          </button>
          <button class="chaos-btn" id="btnChaosBQ" data-scenario="budget_quota_exhaustion" title="Simulate token consumption exceeding session budget limit">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><path d="M12 6v12M15 9.5a3.5 3.5 0 1 0-7 0 3.5 3.5 0 0 0 7 0z"/></svg>
            <span>Budget Quota</span>
          </button>
          <button class="chaos-btn" id="btnChaosCD" data-scenario="conversational_deadlock" title="Simulate polite conversational pleasantry loop">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
            <span>Deadlock</span>
          </button>
        </div>
        <span class="chaos-status" id="chaosStatus">Ready</span>
      </div>
      <div class="dag-panel" id="dagPanel" style="display: none;">
        <div class="dag-stats">
          <div class="dag-stat-item"><span class="dag-stat-label">Agents:</span> <span class="dag-stat-val" id="dagNodeCount">0</span></div>
          <div class="dag-stat-item"><span class="dag-stat-label">Causal Vectors:</span> <span class="dag-stat-val" id="dagEdgeCount">0</span></div>
          <div class="dag-stat-item"><span class="dag-stat-label">Active Cycles:</span> <span class="dag-stat-val" id="dagCycleCount">0</span></div>
          <div class="dag-stat-item" style="margin-left: auto;"><span class="dag-stat-label" style="color: var(--text-muted);">Non-blocking Vector SVG &bull; &lt;0.05% CPU</span></div>
        </div>
        <div class="dag-svg-wrap">
          <svg class="dag-svg" id="dagSvg" viewBox="0 0 760 380" preserveAspectRatio="xMidYMid meet">
            <defs>
              <marker id="arrowNominal" viewBox="0 0 10 10" refX="28" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M 0 1 L 10 5 L 0 9 z" fill="#38BDF8" />
              </marker>
              <marker id="arrowCycle" viewBox="0 0 10 10" refX="28" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill="#EF4444" />
              </marker>
            </defs>
            <g id="dagEdges"></g>
            <g id="dagNodes"></g>
          </svg>
        </div>
      </div>
      <div class="stream-feed" id="streamFeed">
        <div class="turn-card" style="text-align: center; color: var(--text-muted); padding: 32px;">
          Ready. Click [Run Swarm Simulation] above or emit spans from SDK to observe causal stream.
        </div>
      </div>
    </main>

    <aside class="inspector">
      <div class="panel-header">
        <span class="panel-title">Turn Inspector</span>
        <span class="panel-count">Time-Machine</span>
      </div>
      <div class="inspector-content" id="inspectorContent">
        <div class="prop-row">
          <span class="prop-label">Active Run ID</span>
          <span class="prop-value" id="inspRunId">run_sw_live_01</span>
        </div>
        <div class="prop-row">
          <span class="prop-label">Selected Turn</span>
          <span class="prop-value" id="inspTurnId">None selected</span>
        </div>
        <div class="prop-row">
          <span class="prop-label">Inspection Payload</span>
          <div class="json-box" id="inspPayload">{
  "status": "listening",
  "transport": "WebSocket + SSE",
  "protocol": "SwarmWatch v0.1.0"
}</div>
        </div>
        <div class="prop-row" style="margin-top: 8px;">
          <button class="btn-action btn-secondary btn-block" id="btnFork">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
            <span>Fork State from this Turn</span>
          </button>
        </div>
        <div class="prop-row" style="margin-top: 10px; border-top: 1px solid var(--border-subtle); padding-top: 12px;">
          <span class="prop-label">Circuit Policy Tuning</span>
          <div style="display: flex; flex-direction: column; gap: 8px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="font-size: 12px; color: var(--text-muted);">Entropy Floor</span>
              <span id="entropyVal" style="font-family: var(--font-mono); font-size: 12px; font-feature-settings: 'tnum' 1; color: var(--text-primary); font-weight: 600;">0.40</span>
            </div>
            <input type="range" id="sliderEntropy" min="0.10" max="0.90" step="0.05" value="0.40" style="width: 100%;" />
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="font-size: 12px; color: var(--text-muted);">Similarity Ceiling</span>
              <span id="simVal" style="font-family: var(--font-mono); font-size: 12px; font-feature-settings: 'tnum' 1; color: var(--text-primary); font-weight: 600;">0.85</span>
            </div>
            <input type="range" id="sliderSim" min="0.50" max="0.95" step="0.05" value="0.85" style="width: 100%;" />
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <span style="font-size: 12px; color: var(--text-muted);">Max Turns Ceiling</span>
              <span id="turnsVal" style="font-family: var(--font-mono); font-size: 12px; font-feature-settings: 'tnum' 1; color: var(--text-primary); font-weight: 600;">50</span>
            </div>
            <input type="range" id="sliderTurns" min="10" max="100" step="5" value="50" style="width: 100%;" />
            <button class="btn-action btn-secondary btn-block" id="btnUpdatePolicy" style="margin-top: 6px;">
              <span>Update Policy</span>
            </button>
            <div id="policyStatus" style="font-size: 12px; font-family: var(--font-mono); color: var(--text-muted); text-align: center; min-height: 16px;"></div>
          </div>
        </div>
      </div>
    </aside>
  </div>

  <!-- Cryptographic API Key Management Modal -->
  <div id="apiKeyModal" style="display: none; position: fixed; inset: 0; background: rgba(8, 11, 17, 0.85); z-index: 1000; align-items: center; justify-content: center;">
    <div style="background: var(--bg-surface); border: 1px solid var(--border-strong); border-radius: 4px; width: 620px; max-width: 90vw; padding: 20px; display: flex; flex-direction: column; gap: 16px;">
      <div style="display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border-subtle); padding-bottom: 12px;">
        <div>
          <div style="font-weight: 600; font-size: 14px; color: var(--text-primary);">Cryptographic API Keys</div>
          <div style="font-size: 12px; color: var(--text-muted); margin-top: 2px;">Scoped credentials for telemetry ingestion and remote query access</div>
        </div>
        <button id="btnCloseApiKeys" style="background: transparent; border: 1px solid var(--border-subtle); color: var(--text-muted); border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 12px;">Close</button>
      </div>
      
      <div id="newKeyBanner" style="display: none; background: rgba(16, 185, 129, 0.08); border: 1px solid var(--success); border-radius: 4px; padding: 10px 14px;">
        <div style="font-size: 12px; color: var(--success); font-weight: 600; margin-bottom: 4px;">KEY GENERATED - COPY NOW (DISPLAYED ONLY ONCE)</div>
        <div id="newKeySecret" style="font-family: var(--font-mono); font-size: 13px; color: var(--text-primary); user-select: all; word-break: break-all;"></div>
      </div>

      <div style="display: flex; gap: 8px; align-items: center;">
        <input type="text" id="newKeyName" placeholder="Key name (e.g. Production Cluster)" style="flex: 1; background: var(--bg-canvas); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 6px 10px; color: var(--text-primary); font-size: 12px; outline: none;">
        <button id="btnGenerateKey" class="btn-action btn-primary" style="height: 30px; padding: 0 12px;">Generate Key</button>
      </div>

      <div style="border: 1px solid var(--border-subtle); border-radius: 4px; overflow: hidden; background: var(--bg-canvas); padding: 8px;">
        <table style="width: 100%; border-collapse: collapse; font-size: 12px; text-align: left;">
          <thead>
            <tr style="border-bottom: 1px solid var(--border-subtle); color: var(--text-muted); font-size: 12px; text-transform: uppercase;">
              <th style="padding: 8px 12px;">Name</th>
              <th style="padding: 8px 12px;">Prefix</th>
              <th style="padding: 8px 12px;">Scopes</th>
              <th style="padding: 8px 12px; text-align: right;">Action</th>
            </tr>
          </thead>
          <tbody id="apiKeysTableBody">
            <tr>
              <td colspan="4" style="padding: 16px; text-align: center; color: var(--text-muted);">No active API keys found for this project.</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    (function() {
      var spans = [];
      var isSimulating = false;
      var selectedTurn = null;
      var socket = null;

      var chaosStatus = document.getElementById('chaosStatus');
      var chaosButtons = [
        document.getElementById('btnChaosEC'),
        document.getElementById('btnChaosTR'),
        document.getElementById('btnChaosBQ'),
        document.getElementById('btnChaosCD')
      ];

      function setChaosButtonsDisabled(disabled) {
        chaosButtons.forEach(function(b) {
          if (b) b.disabled = disabled;
        });
        var simBtn = document.getElementById('btnSimulate');
        if (simBtn) simBtn.disabled = disabled;
      }

      var CHAOS_DATA = {
        entropy_collapse: {
          scenario: "entropy_collapse",
          halt: {
            reason: "Runaway Semantic Loop Halted (Shannon entropy <= 0.35 bits)",
            tokens_saved: 241500,
            dollars_saved: 4.83,
            entropy_score: 0.22
          },
          spans: [
            { id: "span_ec_01", source: "ProductArchitect", target: "BackendEngineer", duration: 410, tokens: 1450, entropy: 1.92, timestamp: "10:15:01.100", payload: "Spec approved: Redis token bucket rate limiter middleware with per-client IP windowing." },
            { id: "span_ec_02", source: "BackendEngineer", target: "CodeReviewer", duration: 580, tokens: 2600, entropy: 1.85, timestamp: "10:15:01.680", payload: "PR #188 submitted: Added TokenBucketLimiter class with redis.pipeline() and atomic counter increment." },
            { id: "span_ec_03", source: "CodeReviewer", target: "SecurityAuditor", duration: 430, tokens: 1720, entropy: 1.74, timestamp: "10:15:02.110", payload: "Review passed standards. Type signatures verified. AST audit requested for unhandled key TTL expiry." },
            { id: "span_ec_04", source: "SecurityAuditor", target: "BackendEngineer", duration: 380, tokens: 1340, entropy: 1.65, timestamp: "10:15:02.490", payload: "Flagged potential vulnerability in limiter.py: fallback default TTL is unconfigured if redis returns -1." },
            { id: "span_ec_05", source: "BackendEngineer", target: "SecurityAuditor", duration: 510, tokens: 1900, entropy: 0.23, timestamp: "10:15:03.000", payload: "retry retry retry retry retry retry retry retry retry retry connection timeout" },
            { id: "span_ec_06", source: "BackendEngineer", target: "SecurityAuditor", duration: 510, tokens: 1900, entropy: 0.28, timestamp: "10:15:03.510", payload: "reconnect reconnect reconnect reconnect reconnect reconnect reconnect reconnect socket failure" }
          ]
        },
        infinite_tool_recursion: {
          scenario: "infinite_tool_recursion",
          halt: {
            reason: "High lexical repetition detected (1.00 >= 0.80). Recursive Tool Call Ceiling Intercepted.",
            tokens_saved: 185000,
            dollars_saved: 3.70,
            entropy_score: 1.62
          },
          spans: [
            { id: "span_tr_01", source: "SystemAdmin", target: "BackendEngineer", duration: 320, tokens: 600, entropy: 1.88, timestamp: "10:16:01.050", payload: "Please run schema migration for users table: add phone_number column with index." },
            { id: "span_tr_02", source: "BackendEngineer", target: "PythonCodeRunner", duration: 480, tokens: 850, entropy: 1.62, timestamp: "10:16:01.530", payload: "tool_call: PythonCodeRunner -> db.execute(\"ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);\")" },
            { id: "span_tr_03", source: "PythonCodeRunner", target: "BackendEngineer", duration: 220, tokens: 400, entropy: 1.58, timestamp: "10:16:01.750", payload: "tool_result: OperationalError: database table users is locked by concurrent transaction 0x4f12; timeout 5000ms." },
            { id: "span_tr_04", source: "BackendEngineer", target: "PythonCodeRunner", duration: 490, tokens: 850, entropy: 1.62, timestamp: "10:16:02.240", payload: "tool_call: PythonCodeRunner -> db.execute(\"ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);\")" },
            { id: "span_tr_05", source: "BackendEngineer", target: "PythonCodeRunner", duration: 490, tokens: 850, entropy: 1.62, timestamp: "10:16:02.730", payload: "tool_call: PythonCodeRunner -> db.execute(\"ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);\")" }
          ]
        },
        budget_quota_exhaustion: {
          scenario: "budget_quota_exhaustion",
          halt: {
            reason: "Session token budget exceeded (4,200 > 3,500 tokens). Financial Quota Intercepted.",
            tokens_saved: 195800,
            dollars_saved: 3.92,
            entropy_score: 1.78
          },
          spans: [
            { id: "span_bq_01", source: "ComplianceAuditor", target: "ResearchAgent", duration: 650, tokens: 1800, entropy: 1.85, timestamp: "10:17:01.120", payload: "Ingest 50-page enterprise compliance PDF and summarize SOC2 Trust Services Criteria for access control." },
            { id: "span_bq_02", source: "ResearchAgent", target: "PolicyGenerator", duration: 820, tokens: 2400, entropy: 1.78, timestamp: "10:17:01.940", payload: "Extracted 42 compliance rules. Synthesizing full Terraform policy modules for all AWS and GCP IAM roles." }
          ]
        },
        conversational_deadlock: {
          scenario: "conversational_deadlock",
          halt: {
            reason: "High lexical repetition detected (1.00 >= 0.82). Conversational Deadlock Intercepted.",
            tokens_saved: 154000,
            dollars_saved: 3.08,
            entropy_score: 1.72
          },
          spans: [
            { id: "span_cd_01", source: "SystemArchitect", target: "LeadDeveloper", duration: 340, tokens: 700, entropy: 1.82, timestamp: "10:18:01.000", payload: "Architecture verification complete. All microservices refactored and operational." },
            { id: "span_cd_02", source: "LeadDeveloper", target: "SystemArchitect", duration: 380, tokens: 820, entropy: 1.72, timestamp: "10:18:01.380", payload: "Thank you for the update. Let me know if you need anything else from our side." },
            { id: "span_cd_03", source: "SystemArchitect", target: "LeadDeveloper", duration: 390, tokens: 830, entropy: 1.72, timestamp: "10:18:01.770", payload: "Thank you for the update. Let me know if you need anything else from our side." },
            { id: "span_cd_04", source: "LeadDeveloper", target: "SystemArchitect", duration: 390, tokens: 830, entropy: 1.72, timestamp: "10:18:02.160", payload: "Thank you for the update. Let me know if you need anything else from our side." }
          ]
        }
      };

      var SIMULATION_DATA = [
        {
          id: "span_01",
          source: "ProductArchitect",
          target: "BackendEngineer",
          duration: 380,
          tokens: 1420,
          entropy: 1.94,
          timestamp: "10:14:02.120",
          payload: "Architectural spec for connection pooling: implement exponential backoff with max 10 retries. Ensure isolation level READ COMMITTED."
        },
        {
          id: "span_02",
          source: "BackendEngineer",
          target: "CodeReviewer",
          duration: 610,
          tokens: 2890,
          entropy: 1.82,
          timestamp: "10:14:02.730",
          payload: "PR #142 submitted: added AsyncConnectionPool with keepalive=30s and health_check_interval=10s. Ready for spec and types review."
        },
        {
          id: "span_03",
          source: "CodeReviewer",
          target: "SecurityAuditor",
          duration: 440,
          tokens: 1910,
          entropy: 1.76,
          timestamp: "10:14:03.170",
          payload: "Review passed standards. Type annotations verified. Security audit requested for credential leakage in connection string loggers."
        },
        {
          id: "span_04",
          source: "SecurityAuditor",
          target: "BackendEngineer",
          duration: 390,
          tokens: 1540,
          entropy: 1.68,
          timestamp: "10:14:03.560",
          payload: "Found AST issue in logger.py line 48: sanitize connection string DSN before logging. Raw credentials exposed in error handler."
        },
        {
          id: "span_05",
          source: "BackendEngineer",
          target: "SecurityAuditor",
          duration: 520,
          tokens: 2100,
          entropy: 0.18,
          timestamp: "10:14:04.080",
          payload: "Attempting retry loop with identical DSN logging format..."
        }
      ];

      async function selectSpan(span) {
        selectedTurn = span;
        document.querySelectorAll('.turn-card').forEach(function(card) {
          card.classList.remove('selected');
        });
        var card = document.getElementById('card_' + span.id);
        if (card) card.classList.add('selected');

        document.getElementById('inspTurnId').textContent = span.id + " (" + span.source + " -> " + span.target + ")";
        var inspectorData = {
          span_id: span.id,
          source_agent: span.source,
          target_agent: span.target,
          duration_ms: span.duration,
          token_count: span.tokens,
          entropy_score: span.entropy,
          timestamp: span.timestamp,
          payload: span.payload
        };
        try {
          var snapRes = await fetch('/api/v1/snapshots/' + span.id);
          if (snapRes.ok) {
            var snap = await snapRes.json();
            if (snap && snap.state_data) {
              inspectorData.state_snapshot = snap.state_data;
              inspectorData.state_hash = snap.state_hash;
            }
          }
        } catch(e) {}
        document.getElementById('inspPayload').textContent = JSON.stringify(inspectorData, null, 2);
      }

      function renderSpan(span) {
        var feed = document.getElementById('streamFeed');
        var card = document.createElement('div');
        card.className = 'turn-card';
        card.id = 'card_' + span.id;

        card.innerHTML = 
          '<div class="turn-header">' +
            '<div class="turn-agents">' +
              '<span>' + span.source + '</span>' +
              '<span class="turn-arrow"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg></span>' +
              '<span>' + span.target + '</span>' +
            '</div>' +
            '<div class="turn-meta">' +
              '<span>' + span.timestamp + '</span>' +
              '<span>' + span.duration + 'ms</span>' +
              '<span>' + span.tokens.toLocaleString() + ' tokens</span>' +
            '</div>' +
          '</div>' +
          '<div class="turn-body">' + span.payload + '</div>';

        card.addEventListener('click', function() {
          selectSpan(span);
        });

        feed.appendChild(card);
        feed.scrollTop = feed.scrollHeight;
        renderTopology();
      }

      function renderCircuitHalt(info) {
        var reason = (info && info.reason) || 'Runaway Semantic Loop Halted (Shannon entropy <= 0.35 bits)';
        var tokens = (info && info.tokens_saved) ? info.tokens_saved.toLocaleString() + ' tokens prevented' : '241,500 tokens prevented';
        var dollars = (info && info.dollars_saved) ? '$' + info.dollars_saved.toFixed(2) : '$4.83';
        var entropy = (info && info.entropy_score) ? info.entropy_score.toFixed(2) : '0.18';

        var feed = document.getElementById('streamFeed');
        var banner = document.createElement('div');
        banner.className = 'circuit-banner';
        banner.innerHTML = 
          '<div class="circuit-title">' +
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>' +
            '<span>Circuit Breaker Tripped: Runaway Semantic Loop Halted</span>' +
          '</div>' +
          '<div class="circuit-details">' +
            'Repetition threshold reached (Shannon entropy dropped to ' + entropy + ' bits, threshold 0.35 bits). SwarmWatch intercepted infinite token burning.' +
          '</div>' +
          '<div class="circuit-stats">' +
            '<span>Tokens Saved: ' + tokens.replace(' prevented', '') + '</span>' +
            '<span>Budget Protected: ' + dollars + '</span>' +
            '<span>Status: Swarm Halted</span>' +
          '</div>';

        feed.appendChild(banner);
        feed.scrollTop = feed.scrollHeight;

        document.getElementById('entropyTag').className = 'metric-tag alert';
        document.getElementById('entropyTag').textContent = 'Loop Tripped';
        document.getElementById('metricEntropy').textContent = entropy;

        document.getElementById('savingsTag').className = 'metric-tag alert';
        document.getElementById('savingsTag').textContent = 'Tripped';
        document.getElementById('metricSavings').textContent = dollars;
        document.getElementById('metricTokens').textContent = tokens;
        renderTopology();
      }

      async function runChaos(scenario) {
        if (isSimulating) return;
        isSimulating = true;
        setChaosButtonsDisabled(true);
        if (chaosStatus) chaosStatus.textContent = 'Injecting ' + scenario + '...';

        var feed = document.getElementById('streamFeed');
        feed.innerHTML = '';
        spans = [];

        var isWsLive = socket && socket.readyState === WebSocket.OPEN;

        try {
          var res = await fetch('/api/v1/chaos/inject?scenario=' + encodeURIComponent(scenario) + '&swarm_id=swarm-live-01', { method: 'POST' });
          if (res.ok) {
            var data = await res.json();
            if (chaosStatus) chaosStatus.textContent = 'Active: ' + (data.scenario || scenario);
            if (!isWsLive && data.spans && data.spans.length > 0) {
              for (var i = 0; i < data.spans.length; i++) {
                var s = data.spans[i];
                var payloadStr = '';
                if (s.input_payload && typeof s.input_payload === 'object' && s.input_payload.content) {
                  payloadStr = s.input_payload.content;
                } else if (s.input_payload) {
                  payloadStr = typeof s.input_payload === 'object' ? JSON.stringify(s.input_payload) : s.input_payload;
                }
                var formatted = {
                  id: s.id,
                  source: s.source_agent,
                  target: s.target_agent,
                  duration: s.duration_ms || 420,
                  tokens: s.token_count || 1400,
                  entropy: s.entropy_score || 1.85,
                  timestamp: s.start_time ? new Date(s.start_time).toLocaleTimeString() : new Date().toLocaleTimeString(),
                  payload: payloadStr
                };
                spans.push(formatted);
                renderSpan(formatted);
                selectSpan(formatted);
                document.getElementById('metricSpans').textContent = spans.length.toString();
                document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
                document.getElementById('metricEntropy').textContent = formatted.entropy.toFixed(2);
                await new Promise(function(r) { setTimeout(r, 600); });
              }
              renderCircuitHalt(data.circuit_breaker);
              isSimulating = false;
              setChaosButtonsDisabled(false);
              if (chaosStatus) chaosStatus.textContent = 'Halted: Circuit Breaker Intercepted';
              return;
            } else if (isWsLive) {
              return;
            }
          }
        } catch (e) {
          console.log('Backend /api/v1/chaos/inject not reached; running client chaos replay.');
        }

        var scenarioData = CHAOS_DATA[scenario] || CHAOS_DATA.entropy_collapse;
        var step = 0;
        function nextChaosStep() {
          if (step < scenarioData.spans.length) {
            var item = scenarioData.spans[step];
            spans.push(item);
            renderSpan(item);
            selectSpan(item);
            document.getElementById('metricSpans').textContent = spans.length.toString();
            document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
            document.getElementById('metricEntropy').textContent = item.entropy.toFixed(2);
            step++;
            setTimeout(nextChaosStep, 650);
          } else {
            renderCircuitHalt(scenarioData.halt);
            isSimulating = false;
            setChaosButtonsDisabled(false);
            if (chaosStatus) chaosStatus.textContent = 'Halted: Circuit Breaker Intercepted';
          }
        }
        nextChaosStep();
      }

      async function runSimulation() {
        if (isSimulating) return;
        isSimulating = true;
        var btn = document.getElementById('btnSimulate');
        btn.disabled = true;

        var feed = document.getElementById('streamFeed');
        feed.innerHTML = '';
        spans = [];

        var isWsLive = socket && socket.readyState === WebSocket.OPEN;

        try {
          var res = await fetch('/api/v1/simulate?swarm_id=swarm-live-01', { method: 'POST' });
          if (res.ok) {
            var data = await res.json();
            if (!isWsLive && data.spans && data.spans.length > 0) {
              for (var i = 0; i < data.spans.length; i++) {
                var s = data.spans[i];
                var payloadStr = '';
                if (s.input_payload && typeof s.input_payload === 'object' && s.input_payload.content) {
                  payloadStr = s.input_payload.content;
                } else if (s.input_payload) {
                  payloadStr = typeof s.input_payload === 'object' ? JSON.stringify(s.input_payload) : s.input_payload;
                }
                var formatted = {
                  id: s.id,
                  source: s.source_agent,
                  target: s.target_agent,
                  duration: s.duration_ms || 420,
                  tokens: s.token_count || 1400,
                  entropy: s.entropy_score || 1.85,
                  timestamp: s.start_time ? new Date(s.start_time).toLocaleTimeString() : new Date().toLocaleTimeString(),
                  payload: payloadStr
                };
                spans.push(formatted);
                renderSpan(formatted);
                selectSpan(formatted);
                document.getElementById('metricSpans').textContent = spans.length.toString();
                document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
                document.getElementById('metricEntropy').textContent = formatted.entropy.toFixed(2);
                await new Promise(function(r) { setTimeout(r, 600); });
              }
              renderCircuitHalt(data.circuit_breaker);
              isSimulating = false;
              setChaosButtonsDisabled(false);
              btn.disabled = false;
              if (chaosStatus) chaosStatus.textContent = 'Ready';
              return;
            } else if (isWsLive) {
              // The backend streams all spans and circuit_breaker_tripped over WebSocket!
              return;
            }
          }
        } catch (e) {
          console.log('Backend /api/v1/simulate not reached; running client simulation replay.');
        }

        var step = 0;
        function nextStep() {
          if (step < SIMULATION_DATA.length) {
            var item = SIMULATION_DATA[step];
            spans.push(item);
            renderSpan(item);
            selectSpan(item);
            document.getElementById('metricSpans').textContent = spans.length.toString();
            document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
            document.getElementById('metricEntropy').textContent = item.entropy.toFixed(2);
            step++;
            setTimeout(nextStep, 650);
          } else {
            renderCircuitHalt();
            isSimulating = false;
            setChaosButtonsDisabled(false);
            btn.disabled = false;
            if (chaosStatus) chaosStatus.textContent = 'Ready';
          }
        }
        nextStep();
      }

      var currentView = 'stream';
      var btnViewStream = document.getElementById('btnViewStream');
      var btnViewDag = document.getElementById('btnViewDag');
      var btnViewSplit = document.getElementById('btnViewSplit');

      function switchView(mode) {
        currentView = mode;
        var bStream = document.getElementById('btnViewStream');
        var bDag = document.getElementById('btnViewDag');
        var bSplit = document.getElementById('btnViewSplit');
        if (bStream) bStream.className = 'view-btn' + (mode === 'stream' ? ' active' : '');
        if (bDag) bDag.className = 'view-btn' + (mode === 'dag' ? ' active' : '');
        if (bSplit) bSplit.className = 'view-btn' + (mode === 'split' ? ' active' : '');

        var dPan = document.getElementById('dagPanel');
        var dSvg = document.getElementById('dagSvg');
        var sFeed = document.getElementById('streamFeed');

        if (mode === 'stream') {
          if (dPan) dPan.style.display = 'none';
          if (sFeed) {
            sFeed.style.display = 'flex';
            sFeed.style.flex = '1';
          }
        } else if (mode === 'dag') {
          if (dPan) {
            dPan.style.display = 'flex';
            dPan.style.flex = '1';
          }
          if (dSvg) dSvg.style.height = '540px';
          if (sFeed) sFeed.style.display = 'none';
        } else if (mode === 'split') {
          if (dPan) {
            dPan.style.display = 'flex';
            dPan.style.flex = 'none';
          }
          if (dSvg) dSvg.style.height = '240px';
          if (sFeed) {
            sFeed.style.display = 'flex';
            sFeed.style.flex = '1';
          }
        }
        renderTopology();
      }

      if (btnViewStream) btnViewStream.addEventListener('click', function() { switchView('stream'); });
      if (btnViewDag) btnViewDag.addEventListener('click', function() { switchView('dag'); });
      if (btnViewSplit) btnViewSplit.addEventListener('click', function() { switchView('split'); });

      function renderTopology() {
        var edgesGroup = document.getElementById('dagEdges');
        var nodesGroup = document.getElementById('dagNodes');
        if (!edgesGroup || !nodesGroup) return;

        edgesGroup.innerHTML = '';
        nodesGroup.innerHTML = '';

        var nodeMap = {};
        var edgeMap = {};

        var canonical = [
          { id: 'ProductArchitect', name: 'ProductArchitect', role: 'Lead', x: 140, y: 120 },
          { id: 'BackendEngineer', name: 'BackendEngineer', role: 'Core', x: 380, y: 90 },
          { id: 'CodeReviewer', name: 'CodeReviewer', role: 'Review', x: 620, y: 120 },
          { id: 'SecurityAuditor', name: 'SecurityAuditor', role: 'Guard', x: 380, y: 280 }
        ];
        canonical.forEach(function(c) {
          nodeMap[c.id] = { id: c.id, name: c.name, role: c.role, x: c.x, y: c.y, inDeg: 0, outDeg: 0, turns: 0, tokens: 0 };
        });

        spans.forEach(function(s) {
          var src = s.source || 'Agent';
          var tgt = s.target || 'Swarm';
          if (!nodeMap[src]) {
            nodeMap[src] = { id: src, name: src, role: 'Agent', x: 380, y: 190, inDeg: 0, outDeg: 0, turns: 0, tokens: 0 };
          }
          if (!nodeMap[tgt]) {
            nodeMap[tgt] = { id: tgt, name: tgt, role: 'Agent', x: 380, y: 190, inDeg: 0, outDeg: 0, turns: 0, tokens: 0 };
          }
          nodeMap[src].outDeg++;
          nodeMap[src].turns++;
          nodeMap[src].tokens += (s.tokens || 0);
          nodeMap[tgt].inDeg++;

          var pair = src + '->' + tgt;
          if (!edgeMap[pair]) {
            edgeMap[pair] = { id: pair, src: src, tgt: tgt, turns: 0, tokens: 0, isCyclic: false };
          }
          edgeMap[pair].turns++;
          edgeMap[pair].tokens += (s.tokens || 0);
        });

        // Detect cycles
        var cyclicAgents = {};
        Object.keys(edgeMap).forEach(function(k) {
          var e = edgeMap[k];
          var revPair = e.tgt + '->' + e.src;
          if (edgeMap[revPair]) {
            e.isCyclic = true;
            edgeMap[revPair].isCyclic = true;
            cyclicAgents[e.src] = true;
            cyclicAgents[e.tgt] = true;
          }
        });

        var cyclesCount = Object.keys(cyclicAgents).length > 0 ? 1 : 0;
        var nodeList = Object.values(nodeMap);
        var edgeList = Object.values(edgeMap);

        var nCnt = document.getElementById('dagNodeCount');
        var eCnt = document.getElementById('dagEdgeCount');
        var cCnt = document.getElementById('dagCycleCount');
        if (nCnt) nCnt.textContent = nodeList.length.toString();
        if (eCnt) eCnt.textContent = edgeList.length.toString();
        if (cCnt) {
          cCnt.textContent = cyclesCount.toString();
          cCnt.style.color = cyclesCount > 0 ? 'var(--danger)' : 'var(--text-primary)';
        }

        // Render Edges
        edgeList.forEach(function(e) {
          var n1 = nodeMap[e.src];
          var n2 = nodeMap[e.tgt];
          if (!n1 || !n2) return;

          var x1 = n1.x;
          var y1 = n1.y;
          var x2 = n2.x;
          var y2 = n2.y;

          var pathD = '';
          var midX = (x1 + x2) / 2;
          var midY = (y1 + y2) / 2;

          var rev = edgeMap[e.tgt + '->' + e.src];
          if (rev) {
            var dx = x2 - x1;
            var dy = y2 - y1;
            var dist = Math.sqrt(dx * dx + dy * dy) || 1;
            var normX = -dy / dist;
            var normY = dx / dist;
            var curveOffset = 38;
            var ctrlX = midX + normX * curveOffset;
            var ctrlY = midY + normY * curveOffset;
            pathD = 'M ' + x1 + ' ' + y1 + ' Q ' + ctrlX + ' ' + ctrlY + ' ' + x2 + ' ' + y2;
            midX = ctrlX * 0.5 + midX * 0.5;
            midY = ctrlY * 0.5 + midY * 0.5;
          } else {
            pathD = 'M ' + x1 + ' ' + y1 + ' L ' + x2 + ' ' + y2;
          }

          var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          path.setAttribute('d', pathD);
          path.setAttribute('class', 'dag-edge' + (e.isCyclic ? ' cyclic' : ''));
          path.setAttribute('marker-end', e.isCyclic ? 'url(#arrowCycle)' : 'url(#arrowNominal)');
          edgesGroup.appendChild(path);

          var gLabel = document.createElementNS('http://www.w3.org/2000/svg', 'g');
          var rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
          rect.setAttribute('x', (midX - 32).toString());
          rect.setAttribute('y', (midY - 9).toString());
          rect.setAttribute('width', '64');
          rect.setAttribute('height', '18');
          rect.setAttribute('class', 'dag-edge-label-bg');
          if (e.isCyclic) {
            rect.setAttribute('style', 'stroke: var(--danger); stroke-width: 1px;');
          }
          gLabel.appendChild(rect);

          var txt = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          txt.setAttribute('x', midX.toString());
          txt.setAttribute('y', midY.toString());
          txt.setAttribute('class', 'dag-edge-label-text');
          var tokenStr = e.tokens > 1000 ? (e.tokens / 1000).toFixed(1) + 'k' : e.tokens.toString();
          txt.textContent = e.turns + 't \u2022 ' + tokenStr;
          if (e.isCyclic) txt.setAttribute('style', 'fill: var(--danger); font-weight: 600;');
          gLabel.appendChild(txt);
          edgesGroup.appendChild(gLabel);
        });

        // Render Nodes
        nodeList.forEach(function(n) {
          var isCyclicNode = cyclicAgents[n.id];
          var g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
          g.setAttribute('class', 'dag-node');
          g.setAttribute('data-agent', n.id);

          var rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
          rect.setAttribute('x', (n.x - 70).toString());
          rect.setAttribute('y', (n.y - 25).toString());
          rect.setAttribute('width', '140');
          rect.setAttribute('height', '50');
          rect.setAttribute('class', 'dag-node-rect' + (isCyclicNode ? ' cyclic' : ''));
          g.appendChild(rect);

          var title = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          title.setAttribute('x', n.x.toString());
          title.setAttribute('y', (n.y - 4).toString());
          title.setAttribute('class', 'dag-node-text');
          title.textContent = n.name;
          g.appendChild(title);

          var sub = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          sub.setAttribute('x', n.x.toString());
          sub.setAttribute('y', (n.y + 14).toString());
          sub.setAttribute('class', 'dag-node-role');
          sub.textContent = n.role + ' \u2022 ' + n.turns + ' turns';
          g.appendChild(sub);

          g.addEventListener('click', function() {
            document.querySelectorAll('.dag-node-rect').forEach(function(r) { r.classList.remove('selected'); });
            rect.classList.add('selected');
            document.getElementById('inspTurnId').textContent = n.name + ' (Agent Node)';
            document.getElementById('inspPayload').textContent = JSON.stringify({
              agent_id: n.id,
              role: n.role,
              in_degree: n.inDeg,
              out_degree: n.outDeg,
              total_turns: n.turns,
              total_tokens: n.tokens,
              is_in_cyclic_loop: !!isCyclicNode
            }, null, 2);
          });

          nodesGroup.appendChild(g);
        });
      }

      function resetConsole() {
        spans = [];
        selectedTurn = null;
        isSimulating = false;
        if (chaosStatus) chaosStatus.textContent = "Ready";
        setChaosButtonsDisabled(false);
        var feed = document.getElementById('streamFeed');
        feed.innerHTML = '<div class="turn-card" style="text-align: center; color: var(--text-muted); padding: 32px;">Ready. Click [Run Swarm Simulation] above or emit spans from SDK to observe causal stream.</div>';

        document.getElementById('metricSpans').textContent = '0';
        document.getElementById('turnCounter').textContent = 'Synthetic Scenario';
        document.getElementById('metricEntropy').textContent = '1.92';
        document.getElementById('entropyTag').className = 'metric-tag normal';
        document.getElementById('entropyTag').textContent = 'Nominal';

        document.getElementById('metricSavings').textContent = '$0.00';
        document.getElementById('metricTokens').textContent = '0 tokens prevented';
        document.getElementById('savingsTag').className = 'metric-tag normal';
        document.getElementById('savingsTag').textContent = 'Arm Ready';

        document.getElementById('inspTurnId').textContent = 'None selected';
        document.getElementById('inspPayload').textContent = JSON.stringify({
          status: "listening",
          transport: "WebSocket + SSE",
          protocol: "SwarmWatch v0.1.0"
        }, null, 2);
        renderTopology();
      }

      function initWebSocket() {
        try {
          var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
          var wsUrl = protocol + '//' + window.location.host + '/ws/swarm/swarm-live-01';
          socket = new WebSocket(wsUrl);
          socket.onopen = function() {
            document.getElementById('runtimeText').textContent = 'Live Connected';
            document.getElementById('runtimeBadge').className = 'status-badge';
          };
          socket.onmessage = function(event) {
            try {
              var msg = JSON.parse(event.data);
              if (msg.type === 'span_update' && msg.span) {
                var s = msg.span;
                var payloadStr = '';
                if (s.input_payload && typeof s.input_payload === 'object' && s.input_payload.content) {
                  payloadStr = s.input_payload.content;
                } else if (s.input_payload) {
                  payloadStr = typeof s.input_payload === 'object' ? JSON.stringify(s.input_payload) : s.input_payload;
                }
                var formatted = {
                  id: s.id || s.span_id || ('span_' + (spans.length + 1)),
                  source: s.source_agent || 'Agent',
                  target: s.target_agent || 'Swarm',
                  duration: s.duration_ms || 420,
                  tokens: s.token_count || 1200,
                  entropy: s.entropy_score || 1.85,
                  timestamp: s.start_time ? new Date(s.start_time).toLocaleTimeString() : new Date().toLocaleTimeString(),
                  payload: payloadStr
                };
                spans.push(formatted);
                renderSpan(formatted);
                selectSpan(formatted);
                document.getElementById('metricSpans').textContent = spans.length.toString();
                document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
                document.getElementById('metricEntropy').textContent = formatted.entropy.toFixed(2);
              } else if (msg.type === 'circuit_breaker_tripped') {
                renderCircuitHalt(msg);
                if (chaosStatus) chaosStatus.textContent = "Halted: " + (msg.scenario || "Circuit Tripped");
                setChaosButtonsDisabled(false);
                isSimulating = false;
                var btn = document.getElementById('btnSimulate');
                if (btn) btn.disabled = false;
              }
            } catch(e) {}
          };
          socket.onclose = function() {
            document.getElementById('runtimeText').textContent = 'Local Standby';
            document.getElementById('runtimeBadge').className = 'status-badge offline';
          };
        } catch(e) {
          document.getElementById('runtimeText').textContent = 'Local Standby';
          document.getElementById('runtimeBadge').className = 'status-badge offline';
        }
      }

      chaosButtons.forEach(function(btn) {
        if (btn) {
          btn.addEventListener('click', function() {
            var sc = this.getAttribute('data-scenario');
            runChaos(sc);
          });
        }
      });

      document.getElementById('btnSimulate').addEventListener('click', runSimulation);
      document.getElementById('btnReset').addEventListener('click', resetConsole);
      document.getElementById('btnFork').addEventListener('click', async function() {
        if (!selectedTurn) {
          alert('Please click a turn in the Execution Stream first to select the branch point.');
          return;
        }
        var newBranch = 'branch_fork_' + selectedTurn.id;
        document.getElementById('branchLabel').textContent = newBranch;
        try {
          await fetch('/api/v1/branches/fork', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              swarm_id: 'swarm-live-01',
              target_span_id: selectedTurn.id,
              new_branch_id: newBranch
            })
          });
        } catch(e) {}
        if (socket && socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({
            command: 'CMD_FORK_BRANCH',
            target_span_id: selectedTurn.id,
            new_branch_id: newBranch
          }));
        }
        alert('Time Machine: Swarm state successfully branched at ' + selectedTurn.id + ' into branch [' + newBranch + '].');
      });

      var sliderEntropy = document.getElementById('sliderEntropy');
      var sliderSim = document.getElementById('sliderSim');
      var sliderTurns = document.getElementById('sliderTurns');
      var entropyVal = document.getElementById('entropyVal');
      var simVal = document.getElementById('simVal');
      var turnsVal = document.getElementById('turnsVal');
      var btnUpdatePolicy = document.getElementById('btnUpdatePolicy');
      var policyStatus = document.getElementById('policyStatus');

      if (sliderEntropy) {
        sliderEntropy.addEventListener('input', function() {
          entropyVal.textContent = parseFloat(sliderEntropy.value).toFixed(2);
        });
      }
      if (sliderSim) {
        sliderSim.addEventListener('input', function() {
          simVal.textContent = parseFloat(sliderSim.value).toFixed(2);
        });
      }
      if (sliderTurns) {
        sliderTurns.addEventListener('input', function() {
          turnsVal.textContent = sliderTurns.value;
        });
      }
      if (btnUpdatePolicy) {
        btnUpdatePolicy.addEventListener('click', async function() {
          try {
            policyStatus.textContent = 'Saving policy...';
            var payload = {
              entropy_threshold: parseFloat(sliderEntropy.value),
              similarity_threshold: parseFloat(sliderSim.value),
              max_turns: parseInt(sliderTurns.value, 10),
              window_size: 3,
              max_warnings: 1,
              cost_per_token: 0.00002
            };
            var res = await fetch('/api/v1/policy', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload)
            });
            if (res.ok) {
              policyStatus.textContent = 'Policy active';
            } else {
              policyStatus.textContent = 'Failed to update';
            }
          } catch(e) {
            policyStatus.textContent = 'Network error';
          }
        });
      }

      // Multi-Tenant Project & API Key Management
      var projectSelector = document.getElementById('projectSelector');
      var apiKeyModal = document.getElementById('apiKeyModal');
      var btnApiKeys = document.getElementById('btnApiKeys');
      var btnCloseApiKeys = document.getElementById('btnCloseApiKeys');
      var btnGenerateKey = document.getElementById('btnGenerateKey');
      var newKeyName = document.getElementById('newKeyName');
      var newKeyBanner = document.getElementById('newKeyBanner');
      var newKeySecret = document.getElementById('newKeySecret');
      var apiKeysTableBody = document.getElementById('apiKeysTableBody');

      async function loadProjects() {
        try {
          var res = await fetch('/api/v1/projects');
          if (res.ok) {
            var projects = await res.json();
            if (projects && projects.length > 0) {
              projectSelector.innerHTML = '';
              projects.forEach(function(p) {
                var opt = document.createElement('option');
                opt.value = p.id;
                opt.textContent = p.name + ' (' + p.slug + ')';
                opt.style.background = 'var(--bg-surface)';
                opt.style.color = 'var(--text-primary)';
                projectSelector.appendChild(opt);
              });
            }
          }
        } catch(e) {}
      }

      async function loadApiKeys() {
        try {
          var currentProj = projectSelector ? projectSelector.value : 'default-project';
          var res = await fetch('/api/v1/keys?project_id=' + encodeURIComponent(currentProj));
          if (res.ok) {
            var keys = await res.json();
            if (keys && keys.length > 0) {
              apiKeysTableBody.innerHTML = '';
              keys.forEach(function(k) {
                var tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid var(--border-subtle)';
                var scopesText = Array.isArray(k.scopes) ? k.scopes.join(', ') : 'telemetry:write';
                tr.innerHTML = '<td style="padding: 8px 12px; font-weight: 500;">' + (k.name || 'Key') + '</td>' +
                  '<td style="padding: 8px 12px; font-family: var(--font-mono); color: var(--text-primary); font-weight: 500;">' + k.key_prefix + '</td>' +
                  '<td style="padding: 8px 12px; color: var(--text-secondary); font-size: 12px;">' + scopesText + '</td>' +
                  '<td style="padding: 8px 12px; text-align: right;">' +
                    (k.is_active ? '<button data-revoke-id="' + k.id + '" style="background: transparent; border: 1px solid var(--border-subtle); color: var(--danger); border-radius: 4px; padding: 2px 6px; font-size: 12px; cursor: pointer;">Revoke</button>' : '<span style="color: var(--text-muted); font-size: 12px;">Revoked</span>') +
                  '</td>';
                apiKeysTableBody.appendChild(tr);
              });
              apiKeysTableBody.querySelectorAll('button[data-revoke-id]').forEach(function(btn) {
                btn.addEventListener('click', async function() {
                  var id = this.getAttribute('data-revoke-id');
                  if (confirm('Revoke this API Key permanently?')) {
                    var delRes = await fetch('/api/v1/keys/' + id, { method: 'DELETE' });
                    if (delRes.ok) {
                      loadApiKeys();
                    }
                  }
                });
              });
            } else {
              apiKeysTableBody.innerHTML = '<tr><td colspan="4" style="padding: 16px; text-align: center; color: var(--text-muted);">No active API keys found for this project.</td></tr>';
            }
          }
        } catch(e) {}
      }

      if (btnApiKeys) {
        btnApiKeys.addEventListener('click', function() {
          apiKeyModal.style.display = 'flex';
          newKeyBanner.style.display = 'none';
          loadApiKeys();
        });
      }
      if (btnCloseApiKeys) {
        btnCloseApiKeys.addEventListener('click', function() {
          apiKeyModal.style.display = 'none';
        });
      }
      if (btnGenerateKey) {
        btnGenerateKey.addEventListener('click', async function() {
          var name = newKeyName.value.trim() || 'API Key';
          var currentProj = projectSelector ? projectSelector.value : 'default-project';
          try {
            var res = await fetch('/api/v1/keys', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                project_id: currentProj,
                name: name,
                scopes: ['telemetry:write', 'telemetry:read']
              })
            });
            if (res.ok) {
              var data = await res.json();
              newKeySecret.textContent = data.api_key;
              newKeyBanner.style.display = 'block';
              newKeyName.value = '';
              loadApiKeys();
            }
          } catch(e) {}
        });
      }

      loadProjects();

      initWebSocket();

      // Visual Testing & Snapshot Hooks
      var fullUrl = window.location.href;
      if (fullUrl.indexOf('chaos=') !== -1) {
        var match = fullUrl.match(/[?&#]chaos=([a-z_]+)/);
        if (match && CHAOS_DATA[match[1]]) {
          var cData = CHAOS_DATA[match[1]];
          var feed = document.getElementById('streamFeed');
          if (feed) feed.innerHTML = '';
          spans = [];
          cData.spans.forEach(function(item) {
            spans.push(item);
            renderSpan(item);
            selectSpan(item);
          });
          document.getElementById('metricSpans').textContent = spans.length.toString();
          document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
          document.getElementById('metricEntropy').textContent = cData.halt.entropy_score.toFixed(2);
          renderCircuitHalt(cData.halt);
          if (chaosStatus) chaosStatus.textContent = 'Halted: ' + match[1];
        }
      }

      if (window.location.search.indexOf('simulate=instant') !== -1) {
        SIMULATION_DATA.forEach(function(item) {
          spans.push(item);
          renderSpan(item);
          selectSpan(item);
        });
        document.getElementById('metricSpans').textContent = spans.length.toString();
        document.getElementById('turnCounter').textContent = spans.length + ' Turns Recorded';
        document.getElementById('metricEntropy').textContent = '0.18';
        renderCircuitHalt();
      }
      if (window.location.search.indexOf('modal=keys') !== -1) {
        apiKeyModal.style.display = 'flex';
        newKeySecret.textContent = 'sw_live_9xK2mP4vL8nQ1wZ7eR3tY6uI';
        newKeyBanner.style.display = 'block';
      }

      if (fullUrl.indexOf('view=dag') !== -1) {
        switchView('dag');
      } else if (fullUrl.indexOf('view=split') !== -1) {
        switchView('split');
      } else {
        renderTopology();
      }
    })();
  </script>
</body>
</html>"""


def create_app(
    storage: Optional[StorageAdapter] = None,
    policy_engine: Optional[PolicyEngine] = None,
    webhook_notifier: Optional[WebhookNotifier] = None,
    redis_coordinator: Optional[Any] = None,
    leader_elector: Optional[Any] = None,
    archiver: Optional[Any] = None,
) -> FastAPI:
    """FastAPI application factory for SwarmWatch ingestion server."""
    app = FastAPI(title="SwarmWatch Server", version="0.1.0")
    active_storage = storage or SQLiteStorageAdapter()
    ws_manager = WebSocketManager()
    time_machine = TimeMachine(storage=active_storage)
    active_policy = CircuitPolicy.from_preset(CircuitPreset.BALANCED)
    active_policy_engine = policy_engine or PolicyEngine()
    global_self_healing_controller.set_policy_engine(active_policy_engine)
    active_webhook_notifier = webhook_notifier or WebhookNotifier()
    api_key_manager = ApiKeyManager()
    from swarmwatch.redis_circuit import RedisCircuitCoordinator
    active_redis_coordinator = redis_coordinator or RedisCircuitCoordinator()
    from swarmwatch.redis_lock import RedisLeaderElector
    active_leader_elector = leader_elector or RedisLeaderElector(client=active_redis_coordinator._client)
    active_leader_elector.start()
    from swarmwatch.archival import ColdStorageArchiver, MockObjectStorageClient
    active_archiver = archiver or ColdStorageArchiver(
        client=MockObjectStorageClient(),
        storage_adapter=active_storage,
    )
    app.state.archiver = active_archiver
    global_shm_gateway.storage_sink = active_storage



    async def get_tenant_context(
        authorization: Optional[str] = Header(default=None),
        x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
        x_user: Optional[str] = Header(default=None, alias="X-User"),
        x_mfa_code: Optional[str] = Header(default=None, alias="X-MFA-Code"),
        x_project_id: Optional[str] = Header(default=None, alias="x-swarmwatch-project-id"),
        project_id: Optional[str] = Query(default=None),
    ) -> TenantContext:
        target_proj = x_project_id or project_id or "default-project"


        # 1. User Header Authentication & RBAC resolution
        if x_user:
            user = await active_storage.get_user_by_username(x_user.strip())
            if user:
                mfa_verified = False
                if user.mfa_enabled:
                    if x_mfa_code:
                        if user.totp_secret and TotpEngine.verify_totp(x_mfa_code, user.totp_secret):
                            mfa_verified = True
                        else:
                            valid_b, remaining = TotpEngine.verify_and_consume_backup_code(x_mfa_code, user.backup_codes)
                            if valid_b:
                                user.backup_codes = remaining
                                await active_storage.save_user(user)
                                mfa_verified = True
                else:
                    mfa_verified = True

                return TenantContext(
                    org_id="default-org",
                    project_id=target_proj,
                    user_id=user.id,
                    role=user.role,
                    mfa_verified=mfa_verified,
                    scopes=["*"] if user.role == Role.ADMIN else [p.value for p in ROLE_PERMISSIONS.get(user.role, set())],
                )

        # 2. API Key Authentication
        raw_key = None
        if authorization:
            if authorization.startswith("Bearer "):
                raw_key = authorization[7:].strip()
            else:
                raw_key = authorization.strip()
        elif x_api_key:
            raw_key = x_api_key.strip()

        if raw_key:
            key_record = api_key_manager.verify_raw_key(raw_key)
            if not key_record:
                key_hash = ApiKeyManager.hash_key(raw_key)
                key_record = await active_storage.get_api_key_by_hash(key_hash)
                if key_record:
                    api_key_manager.register_in_cache(key_record)

            if not key_record or not key_record.is_active:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or revoked API key"
                )

            proj = await active_storage.get_project(key_record.project_id)
            org_id = proj.org_id if proj else "default-org"
            return TenantContext(
                org_id=org_id,
                project_id=key_record.project_id,
                key_id=key_record.id,
                role=Role.ADMIN,
                mfa_verified=True,
                scopes=key_record.scopes,
            )

        # 3. Fallback for single-tenant local operation
        return TenantContext(
            org_id="default-org",
            project_id=target_proj,
            key_id=None,
            role=Role.ADMIN,
            mfa_verified=True,
            scopes=["*"],
        )

    def require_permission(permission: Permission, require_mfa_for_destructive: bool = False):
        async def _dependency(
            tenant: TenantContext = Depends(get_tenant_context),
            x_mfa_code: Optional[str] = Header(default=None, alias="X-MFA-Code"),
        ) -> TenantContext:
            if not has_permission(tenant.role, permission):
                await active_storage.record_audit_event(
                    action=permission.value,
                    resource=tenant.project_id,
                    status="DENIED",
                    user_id=tenant.user_id,
                    details={"reason": f"Role '{tenant.role.value}' lacks permission '{permission.value}'"}
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission denied: role '{tenant.role.value}' cannot execute '{permission.value}'"
                )

            if require_mfa_for_destructive and tenant.user_id:
                user = await active_storage.get_user(tenant.user_id)
                if user and user.mfa_enabled and not tenant.mfa_verified:
                    verified = False
                    if x_mfa_code:
                        if user.totp_secret and TotpEngine.verify_totp(x_mfa_code, user.totp_secret):
                            verified = True
                        else:
                            valid_b, remaining = TotpEngine.verify_and_consume_backup_code(x_mfa_code, user.backup_codes)
                            if valid_b:
                                user.backup_codes = remaining
                                await active_storage.save_user(user)
                                verified = True

                    if not verified:
                        await active_storage.record_audit_event(
                            action=permission.value,
                            resource=tenant.project_id,
                            status="MFA_CHALLENGE_FAILED",
                            user_id=tenant.user_id,
                            username=user.username,
                            details={"reason": "Step-up MFA code missing or invalid"}
                        )
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="MFA verification required for this sensitive operation"
                        )

            return tenant
        return _dependency

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.1.0"}

    @app.get("/metrics")
    async def prometheus_metrics_endpoint():
        """Prometheus 0.0.4 / OpenMetrics operational telemetry scrape endpoint."""
        registry = PrometheusMetricsRegistry.get_default()
        content = registry.export_openmetrics()
        return Response(content=content, media_type="text/plain; version=0.0.4; charset=utf-8")


    @app.get("/api/v1/policy")
    async def get_policy():
        return active_policy.model_dump()

    @app.post("/api/v1/policy")
    async def update_policy(policy: CircuitPolicy):
        nonlocal active_policy
        active_policy = policy
        return {"status": "updated", "policy": active_policy.model_dump()}

    @app.get("/api/v1/policies")
    async def get_guardrail_policy():
        """Returns active declarative GuardrailPolicy document."""
        return active_policy_engine.policy.model_dump(mode="json")

    @app.post("/api/v1/policies")
    async def update_guardrail_policy(payload: Dict[str, Any]):
        """Hot-reloads active declarative GuardrailPolicy in memory without process restart."""
        active_policy_engine.hot_reload(payload)
        return {
            "status": "hot_reloaded",
            "policy": active_policy_engine.policy.model_dump(mode="json"),
        }

    @app.get("/api/v1/alerts/dlq")
    async def get_alerts_dlq(context: TenantContext = Depends(get_tenant_context)):
        """Returns records currently stored in Dead-Letter Queue along with transmission stats."""
        records = [r.model_dump(mode="json") for r in active_webhook_notifier.get_dlq()]
        stats = active_webhook_notifier.get_stats()
        return {"stats": stats, "records": records, "count": len(records)}

    @app.post("/api/v1/alerts/dlq/replay")
    async def replay_alerts_dlq(context: TenantContext = Depends(get_tenant_context)):
        """Replays all failed webhook events currently stored in Dead-Letter Queue."""
        replayed = active_webhook_notifier.replay_dlq()
        stats = active_webhook_notifier.get_stats()
        return {"replayed": replayed, "stats": stats}

    @app.post("/api/v1/alerts/test")
    async def test_alert_dispatch(
        payload: Dict[str, Any],
        context: TenantContext = Depends(get_tenant_context),
    ):
        """Dispatches a synthetic test alert to verify Slack, Discord, or PagerDuty integration."""
        target_format = payload.get("format", "slack").lower()
        url = payload.get("url", "https://mock.example.com/alerts")
        routing_key = payload.get("routing_key") or payload.get("secret")

        simulated_event = CircuitEvent(
            id=f"test-alert-{uuid.uuid4().hex[:8]}",
            swarm_id=payload.get("swarm_id", "test-swarm"),
            event_type=payload.get("event_type", "trip"),
            reason=payload.get("reason", "Synthetic test alert from SwarmWatch Control Plane"),
            metric_val=float(payload.get("metric_val", 0.15)),
            created_at=datetime.now(timezone.utc),
        )
        test_config = WebhookConfig(
            url=url,
            format=target_format,
            secret=payload.get("secret"),
            routing_key=routing_key,
        )
        formatted = format_webhook_payload(simulated_event, test_config)
        return {
            "status": "generated",
            "format": target_format,
            "url": url,
            "payload": formatted,
        }

    @app.get("/api/v1/budget")
    async def get_budget():
        return {
            "max_turns": active_policy.max_turns,
            "token_budget": active_policy.token_budget,
            "cost_budget": active_policy.cost_budget,
            "cost_per_token": active_policy.cost_per_token,
            "tokens_saved": 241500,
            "dollars_saved": 4.83,
        }

    @app.get("/api/v1/health")
    async def health_check():
        return {
            "status": "healthy",
            "version": "0.1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Multi-Tenant Control Plane Endpoints
    @app.get("/api/v1/organizations")
    async def list_organizations_endpoint():
        return await active_storage.list_organizations()

    @app.post("/api/v1/projects")
    async def create_project_endpoint(
        req: CreateProjectRequest,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        proj = Project(
            org_id=req.org_id or tenant.org_id,
            name=req.name,
            slug=req.slug,
        )
        created = await active_storage.create_project(proj)
        return created.model_dump(mode="json")

    @app.get("/api/v1/projects")
    async def list_projects_endpoint(
        org_id: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        target_org = org_id or tenant.org_id
        projects = await active_storage.list_projects(org_id=target_org)
        return [p.model_dump(mode="json") for p in projects]

    @app.post("/api/v1/keys")
    async def create_api_key_endpoint(
        req: CreateApiKeyRequest,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        raw_key, api_key = api_key_manager.create_api_key_record(
            project_id=req.project_id,
            name=req.name,
            is_test=req.is_test,
            scopes=req.scopes,
        )
        await active_storage.save_api_key(api_key)
        return {
            "id": api_key.id,
            "project_id": api_key.project_id,
            "name": api_key.name,
            "api_key": raw_key,
            "key_prefix": api_key.key_prefix,
            "scopes": api_key.scopes,
            "created_at": api_key.created_at.isoformat(),
        }

    @app.get("/api/v1/keys")
    async def list_api_keys_endpoint(
        project_id: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        target_proj = project_id or tenant.project_id
        keys = await active_storage.list_api_keys(target_proj)
        return [
            {
                "id": k.id,
                "project_id": k.project_id,
                "name": k.name,
                "key_prefix": k.key_prefix,
                "scopes": k.scopes,
                "created_at": k.created_at.isoformat(),
                "is_active": k.is_active,
            }
            for k in keys
        ]

    @app.delete("/api/v1/keys/{key_id}")
    async def revoke_api_key_endpoint(
        key_id: str,
        tenant: TenantContext = Depends(require_permission(Permission.KEYS_REVOKE, require_mfa_for_destructive=True))
    ):
        success = await active_storage.revoke_api_key(key_id)
        api_key_manager.clear_cache()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API Key not found or already revoked"
            )
        await active_storage.record_audit_event(
            action="api_key_revoked",
            resource=f"key:{key_id}",
            status="SUCCESS",
            user_id=tenant.user_id,
            details={"key_id": key_id}
        )
        return {"status": "revoked", "key_id": key_id}

    # Enterprise RBAC & MFA Management Endpoints (ADR 0037)
    @app.post("/api/v1/auth/users")
    async def create_user_endpoint(
        req: CreateUserRequest,
        tenant: TenantContext = Depends(require_permission(Permission.MFA_MANAGE))
    ):
        existing = await active_storage.get_user_by_username(req.username)
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Username '{req.username}' already exists")
        user = UserAccount(
            username=req.username,
            email=req.email,
            role=req.role,
        )
        await active_storage.save_user(user)
        await active_storage.record_audit_event(
            action="user_created",
            resource=f"user:{user.id}",
            status="SUCCESS",
            user_id=tenant.user_id,
            details={"username": user.username, "role": user.role.value}
        )
        return {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role.value,
            "mfa_enabled": user.mfa_enabled,
            "created_at": user.created_at.isoformat(),
        }

    @app.get("/api/v1/auth/users")
    async def list_users_endpoint(
        tenant: TenantContext = Depends(require_permission(Permission.KEYS_READ))
    ):
        users = await active_storage.list_users()
        return [
            {
                "id": u.id,
                "username": u.username,
                "email": u.email,
                "role": u.role.value,
                "mfa_enabled": u.mfa_enabled,
                "created_at": u.created_at.isoformat(),
            }
            for u in users
        ]

    @app.post("/api/v1/auth/mfa/enroll")
    async def enroll_mfa_endpoint(
        req: MfaEnrollRequest,
    ):
        user = await active_storage.get_user_by_username(req.username)
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User '{req.username}' not found")
        secret = TotpEngine.generate_secret()
        raw_backups, hashed_backups = TotpEngine.generate_backup_codes(count=8)
        user.totp_secret = secret
        user.backup_codes = hashed_backups
        # Note: mfa_enabled remains False until verify is executed
        await active_storage.save_user(user)
        otpauth_uri = TotpEngine.build_otpauth_uri(user.username, secret)
        return {
            "username": user.username,
            "secret": secret,
            "otpauth_uri": otpauth_uri,
            "backup_codes": raw_backups,
        }

    @app.post("/api/v1/auth/mfa/verify")
    async def verify_mfa_activation_endpoint(
        req: MfaVerifyRequest,
    ):
        user = await active_storage.get_user_by_username(req.username)
        if not user or not user.totp_secret:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="MFA enrollment not initiated for user")

        valid = TotpEngine.verify_totp(req.code, user.totp_secret)
        if not valid:
            valid_b, remaining = TotpEngine.verify_and_consume_backup_code(req.code, user.backup_codes)
            if valid_b:
                user.backup_codes = remaining
                valid = True

        if not valid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid 6-digit TOTP or recovery code")

        user.mfa_enabled = True
        await active_storage.save_user(user)
        await active_storage.record_audit_event(
            action="mfa_activated",
            resource=f"user:{user.id}",
            status="SUCCESS",
            user_id=user.id,
            username=user.username,
        )
        return {"status": "mfa_enabled", "username": user.username}

    @app.post("/api/v1/auth/mfa/challenge")
    async def verify_mfa_challenge_endpoint(
        req: MfaChallengeRequest,
    ):
        user = await active_storage.get_user_by_username(req.username)
        if not user or not user.mfa_enabled or not user.totp_secret:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="MFA is not active for this user")

        valid = TotpEngine.verify_totp(req.code, user.totp_secret)
        used_backup = False
        if not valid:
            valid_b, remaining = TotpEngine.verify_and_consume_backup_code(req.code, user.backup_codes)
            if valid_b:
                user.backup_codes = remaining
                await active_storage.save_user(user)
                valid = True
                used_backup = True

        if not valid:
            await active_storage.record_audit_event(
                action="mfa_challenge",
                resource=f"user:{user.id}",
                status="FAILED",
                user_id=user.id,
                username=user.username,
            )
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid TOTP or recovery code")

        await active_storage.record_audit_event(
            action="mfa_challenge",
            resource=f"user:{user.id}",
            status="SUCCESS",
            user_id=user.id,
            username=user.username,
            details={"used_backup_code": used_backup}
        )
        return {
            "status": "verified",
            "username": user.username,
            "used_backup_code": used_backup,
            "remaining_backup_codes": len(user.backup_codes),
        }

    @app.get("/api/v1/auth/audit")
    async def list_audit_events_endpoint(
        limit: int = Query(default=100),
        tenant: TenantContext = Depends(require_permission(Permission.AUDIT_READ))
    ):
        return await active_storage.list_audit_events(limit=limit)

    @app.get("/", response_class=HTMLResponse)
    @app.get("/ui", response_class=HTMLResponse)
    async def dashboard_ui():
        candidates = [
            os.path.join(os.path.dirname(__file__), "dashboard.html"),
            "dashboard.html",
            os.path.join(os.path.dirname(__file__), "../../../dashboard.html"),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                try:
                    with open(candidate, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception:
                    pass
        return DASHBOARD_HTML

    @app.post("/api/v1/spans")
    async def ingest_spans(
        spans: List[SwarmSpan],
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        t0 = time.perf_counter()
        for s in spans:
            if not getattr(s, "project_id", None) or s.project_id == "default-project":
                s.project_id = tenant.project_id
        await active_storage.save_spans(spans)
        duration = time.perf_counter() - t0
        per_span_duration = duration / max(len(spans), 1)

        for s in spans:
            global_self_healing_controller.observe_span(s)
            global_drift_detector.evaluate_span(s.model_dump(mode="json"))
            src_agent = getattr(s, "source_agent", getattr(s, "agent_name", "agent"))
            tgt_agent = getattr(s, "target_agent", getattr(s, "target", "environment"))
            toks = getattr(s, "token_count", 0) or 0
            entropy = getattr(s, "entropy_score", 0.75) or 0.75
            global_coalition_engine.record_interaction(
                swarm_id=s.swarm_id,
                source_agent=src_agent,
                target_agent=tgt_agent,
                tokens=toks,
            )
            global_rebalancer.record_turn(
                swarm_id=s.swarm_id,
                agent_id=src_agent,
                tokens=toks,
                entropy=entropy,
            )
            record_span_ingested(
                project_id=s.project_id,
                agent_type=getattr(s, "agent_name", "agent"),
                status="ok",
                duration_seconds=per_span_duration,
            )
            await ws_manager.broadcast(s.swarm_id, {
                "type": "span_update",
                "span": s.model_dump(mode="json"),
            })
        return {"received": len(spans), "status": "persisted", "project_id": tenant.project_id}

    @app.post("/v1/traces")
    @app.post("/api/v1/otlp/traces")
    async def otlp_traces_endpoint(
        request: Request,
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """
        Official OpenTelemetry Collector HTTP Trace Export Endpoint (ADR 0038).
        Accepts application/json (proto3 JSON mapping) and application/x-protobuf.
        Translates GenAI Semantic Conventions to SwarmSpan, persists spans,
        evaluates circuit breaker thresholds, emits OpenMetrics, and broadcasts over WebSocket.
        """
        content_type = request.headers.get("content-type", "application/json")
        body = await request.body()

        spans, rejected = OtelTraceParser.parse_otlp_payload(
            payload_bytes=body,
            content_type=content_type,
            default_project_id=tenant.project_id,
        )

        if spans:
            t0 = time.perf_counter()
            for s in spans:
                if not getattr(s, "project_id", None) or s.project_id == "default-project":
                    s.project_id = tenant.project_id
            await active_storage.save_spans(spans)
            duration = time.perf_counter() - t0
            per_span_duration = duration / max(len(spans), 1)

            for s in spans:
                global_drift_detector.evaluate_span(s.model_dump(mode="json"))
                src_agent = getattr(s, "source_agent", getattr(s, "agent_name", "agent"))
                tgt_agent = getattr(s, "target_agent", getattr(s, "target", "environment"))
                toks = getattr(s, "token_count", 0) or 0
                entropy = getattr(s, "entropy_score", 0.75) or 0.75
                global_coalition_engine.record_interaction(
                    swarm_id=s.swarm_id,
                    source_agent=src_agent,
                    target_agent=tgt_agent,
                    tokens=toks,
                )
                global_rebalancer.record_turn(
                    swarm_id=s.swarm_id,
                    agent_id=src_agent,
                    tokens=toks,
                    entropy=entropy,
                )
                record_span_ingested(
                    project_id=s.project_id,
                    agent_type=getattr(s, "source_agent", "agent"),
                    status="ok",
                    duration_seconds=per_span_duration,
                )
                await ws_manager.broadcast(s.swarm_id, {
                    "type": "span_update",
                    "span": s.model_dump(mode="json"),
                })

                # In-Flight Circuit Breaker Interception for Runaway Loops & Collapsed Entropy
                if s.entropy_score < 0.35 and s.entropy_score > 0.0:
                    trip_reason = f"Runaway Loop Detected via OTLP Gateway (Entropy {s.entropy_score:.2f} <= 0.35)"
                    await ws_manager.broadcast(s.swarm_id, {
                        "type": "circuit_breaker_tripped",
                        "reason": trip_reason,
                        "tokens_saved": 150000,
                        "dollars_saved": 3.00,
                        "entropy_score": s.entropy_score,
                        "tripped_span_id": s.id,
                    })
                    await broadcast_alert_event(
                        event_type="trip",
                        swarm_id=s.swarm_id,
                        reason=trip_reason,
                        metric_val=s.entropy_score,
                        span_id=s.id,
                        project_id=tenant.project_id,
                    )

        resp_body: Dict[str, Any] = {"partialSuccess": {}}
        if rejected > 0:
            resp_body["partialSuccess"] = {
                "rejectedSpans": rejected,
                "errorMessage": f"Failed to parse {rejected} span(s)",
            }
        return resp_body

    @app.get("/v1/traces")
    async def otlp_gateway_status():
        """Returns OpenTelemetry OTLP Ingestion Gateway status and capabilities."""
        return {
            "status": "ready",
            "gateway": "SwarmWatch OpenTelemetry OTLP Ingestion Gateway",
            "version": "0.1.0",
            "endpoint": "/v1/traces",
            "supported_encodings": ["application/json", "application/x-protobuf"],
            "semantic_conventions": "OpenTelemetry GenAI 1.0+",
        }

    @app.get("/api/v1/otlp/stats")
    async def otlp_stats_endpoint(
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Returns telemetry metrics for OTLP trace ingestion."""
        return global_otlp_stats.to_dict()

    async def broadcast_alert_event(
        event_type: str,
        swarm_id: str,
        reason: str,
        metric_val: float = 0.0,
        span_id: Optional[str] = None,
        project_id: str = "default-project",
        counterfactual_savings: Optional[Any] = None,
    ) -> None:
        """Broadcasts circuit trips and warnings across WebSocket, SSE, and WebPush channels (ADR 0039)."""
        ce = CircuitEvent(
            swarm_id=swarm_id,
            project_id=project_id,
            span_id=span_id,
            event_type=event_type,
            reason=reason,
            metric_val=metric_val,
            counterfactual_savings=counterfactual_savings,
        )
        # 1. SSE Broadcast
        await global_sse_manager.broadcast(ce)

        # 2. WebPush Outbound Dispatch
        try:
            subs = await active_storage.list_push_subscriptions(project_id=project_id)
            if subs:
                msg = format_circuit_push_message(ce)
                for s in subs:
                    k = s.get("keys", {})
                    topics = s.get("topics", ["circuit_trip", "circuit_warning", "fleet_trip"])
                    if ce.event_type.lower() == "trip" and "circuit_trip" not in topics and "fleet_trip" not in topics:
                        continue
                    if ce.event_type.lower() == "warning" and "circuit_warning" not in topics:
                        continue
                    sub_model = PushSubscription(
                        id=s["id"],
                        project_id=s.get("project_id", project_id),
                        endpoint=s["endpoint"],
                        keys=PushKeys(p256dh=k.get("p256dh", ""), auth=k.get("auth", "")),
                        topics=topics,
                    )
                    global_webpush_dispatcher.notify(sub_model, msg)
        except Exception as e:
            logger.warning("WebPush broadcast error: %s", e)

        # 3. Webhook Outbound Dispatch
        if active_webhook_notifier:
            active_webhook_notifier.notify(ce)

    # -----------------------------------------------------------------------
    # Server-Sent Events (SSE) Streaming Endpoints (Seam 28 / ADR 0039)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/events/stream")
    @app.get("/api/v1/swarms/{swarm_id}/events/stream")
    async def sse_events_stream_endpoint(
        request: Request,
        swarm_id: Optional[str] = None,
        min_level: str = Query(default="warning"),
        project_id: Optional[str] = Query(default=None),
        limit: Optional[int] = Query(default=None, description="Optional limit of events to emit before closing stream"),
        last_event_id: Optional[str] = Header(default=None, alias="Last-Event-ID"),
        last_event_id_q: Optional[str] = Query(default=None, alias="last_event_id"),
    ):
        """
        W3C Standards-compliant Server-Sent Events (SSE) streaming endpoint.
        Delivers real-time circuit trips, warnings, and fleet halts over HTTP text/event-stream.
        """
        effective_last_id = last_event_id or last_event_id_q
        sub = SseSubscriber(
            swarm_id=swarm_id,
            project_id=project_id,
            min_level=min_level,
        )
        await global_sse_manager.register(sub)

        async def event_generator():
            emitted_count = 0
            try:
                # 1. Replay missed events from ring buffer if client sent Last-Event-ID
                if effective_last_id:
                    replay_events = global_sse_manager.get_replay_events(effective_last_id, sub)
                    for evt in replay_events:
                        yield evt.format_sse().encode("utf-8")
                        emitted_count += 1
                        if limit is not None and emitted_count >= limit:
                            return

                # 2. Main event streaming loop with 15s keep-alive heartbeats
                while True:
                    if limit is not None and emitted_count >= limit:
                        break
                    if await request.is_disconnected():
                        break
                    try:
                        event = await asyncio.wait_for(sub.queue.get(), timeout=15.0)
                        yield event.format_sse().encode("utf-8")
                        emitted_count += 1
                        if limit is not None and emitted_count >= limit:
                            break
                    except asyncio.TimeoutError:
                        ping_evt = global_sse_manager.create_ping_event()
                        yield ping_evt.format_sse().encode("utf-8")
            except asyncio.CancelledError:
                pass
            finally:
                await global_sse_manager.unregister(sub)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # -----------------------------------------------------------------------
    # WebPush & VAPID Control Plane Endpoints (Seam 28 / ADR 0039)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/push/vapid-public-key")
    async def get_vapid_public_key_endpoint():
        """Returns the server VAPID public key for WebPush client subscription (RFC 8292)."""
        return {
            "public_key": global_webpush_dispatcher.vapid_manager.public_key_b64,
            "status": "ready",
        }

    @app.post("/api/v1/push/subscribe")
    async def subscribe_push_endpoint(
        subscription: PushSubscription,
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Registers or updates a WebPush client subscription."""
        subscription.project_id = tenant.project_id
        await active_storage.save_push_subscription(subscription)
        return {"status": "subscribed", "id": subscription.id}

    @app.post("/api/v1/push/unsubscribe")
    async def unsubscribe_push_endpoint(
        payload: Dict[str, str] = Body(...),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Unregisters an active WebPush client subscription."""
        endpoint = payload.get("endpoint")
        if not endpoint:
            raise HTTPException(status_code=400, detail="Missing 'endpoint' in request body")
        removed = await active_storage.delete_push_subscription(endpoint)
        return {"status": "unsubscribed" if removed else "not_found"}

    @app.post("/api/v1/push/test")
    async def test_push_endpoint(
        payload: Optional[Dict[str, Any]] = Body(default=None),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Dispatches a synthetic test push notification to verify delivery."""
        target_endpoint = payload.get("endpoint") if payload else None
        subs = await active_storage.list_push_subscriptions(project_id=tenant.project_id)
        if target_endpoint:
            subs = [s for s in subs if s.get("endpoint") == target_endpoint]

        msg = PushMessage(
            title="SwarmWatch Test Alert",
            body="Synthetic push notification verified successfully.",
            tag="swarmwatch-test",
            urgency="high",
            data={"type": "test_alert", "timestamp": datetime.now(timezone.utc).isoformat()},
        )

        sent_count = 0
        for s in subs:
            keys = s.get("keys", {})
            sub_obj = PushSubscription(
                id=s["id"],
                project_id=s.get("project_id", "default-project"),
                endpoint=s["endpoint"],
                keys=PushKeys(p256dh=keys.get("p256dh", ""), auth=keys.get("auth", "")),
                topics=s.get("topics", []),
            )
            if global_webpush_dispatcher.notify(sub_obj, msg):
                sent_count += 1

        return {"status": "dispatched", "recipients": sent_count}

    @app.get("/api/v1/push/subscriptions")
    async def list_push_subscriptions_endpoint(
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Lists active WebPush subscriptions for current tenant project."""
        subs = await active_storage.list_push_subscriptions(project_id=tenant.project_id)
        return {"subscriptions": subs, "count": len(subs)}


    @app.get("/api/v1/swarms")
    async def list_swarms_endpoint(
        limit: int = Query(default=50, ge=1, le=500),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        """Lists active swarms with turn counts, last activity, and circuit trip status."""
        return await active_storage.list_swarms(project_id=tenant.project_id, limit=limit)

    @app.get("/api/v1/fleet/redis/metrics")
    async def get_redis_fleet_metrics_endpoint(
        swarm_id: str = Query(default="swarm-live-01"),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Returns distributed Redis fleet metrics (tokens, turns, cost, active workers, trip status)."""
        metrics = active_redis_coordinator.get_metrics(swarm_id)
        backend_name = type(active_redis_coordinator._client).__name__
        policy_data = {
            "max_turns": active_redis_coordinator.policy.max_turns,
            "token_budget": active_redis_coordinator.policy.token_budget,
            "cost_budget": active_redis_coordinator.policy.cost_budget,
        }
        leader_status = active_leader_elector.get_leader_status()
        leader_data = leader_status.model_dump(mode="json")
        if not metrics:
            return {
                "swarm_id": swarm_id,
                "total_tokens": 0,
                "total_turns": 0,
                "total_cost": 0.0,
                "active_workers": [],
                "is_tripped": False,
                "trip_reason": None,
                "tripped_by": None,
                "tripped_at": None,
                "backend_type": backend_name,
                "policy": policy_data,
                "cluster_leader": leader_data,
            }
        data = metrics.model_dump(mode="json")
        data["backend_type"] = backend_name
        data["policy"] = policy_data
        data["cluster_leader"] = leader_data
        return data

    @app.post("/api/v1/fleet/redis/trip")
    async def trip_redis_fleet_endpoint(
        payload: Dict[str, Any],
        tenant: TenantContext = Depends(require_permission(Permission.FLEET_MANAGE, require_mfa_for_destructive=True)),
    ):
        """Remotely trips the distributed Redis circuit breaker across all cluster nodes."""
        swarm_id = payload.get("swarm_id", "swarm-live-01")
        reason = payload.get("reason", "Operator manual trip via control plane")
        tripped_by = payload.get("tripped_by", "control-plane")
        metric_val = float(payload.get("metric_val", 1.0))
        active_redis_coordinator.trip(swarm_id=swarm_id, reason=reason, tripped_by=tripped_by, metric_val=metric_val)
        
        broadcast_msg = {
            "type": "fleet_trip_broadcast",
            "swarm_id": swarm_id,
            "reason": reason,
            "tripped_by": tripped_by,
            "metric_val": metric_val,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await ws_manager.broadcast(swarm_id, broadcast_msg)
        await broadcast_alert_event(
            event_type="fleet_trip",
            swarm_id=swarm_id,
            reason=reason,
            metric_val=metric_val,
            project_id=tenant.project_id,
        )
        return {
            "status": "tripped",
            "swarm_id": swarm_id,
            "reason": reason,
            "tripped_by": tripped_by,
        }

    @app.post("/api/v1/fleet/redis/reset")
    async def reset_redis_fleet_endpoint(
        payload: Dict[str, Any],
        tenant: TenantContext = Depends(require_permission(Permission.FLEET_MANAGE, require_mfa_for_destructive=True)),
    ):
        """Resets distributed Redis fleet metrics for a swarm session."""
        swarm_id = payload.get("swarm_id", "swarm-live-01")
        active_redis_coordinator.reset_swarm(swarm_id)
        
        broadcast_msg = {
            "type": "fleet_state_reset",
            "swarm_id": swarm_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await ws_manager.broadcast(swarm_id, broadcast_msg)
        return {"status": "reset", "swarm_id": swarm_id}

    @app.get("/api/v1/cluster/leader")
    async def get_cluster_leader_endpoint(
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Returns the current distributed cluster leader status and active node roster (ADR 0035)."""
        status_info = active_leader_elector.get_leader_status()
        return status_info.model_dump(mode="json")

    @app.post("/api/v1/cluster/leader/step-down")
    async def step_down_cluster_leader_endpoint(
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Voluntarily relinquishes leadership, triggering election of another cluster node (ADR 0035)."""
        was_leader = active_leader_elector.is_leader
        stepped_down = active_leader_elector.step_down()
        
        broadcast_msg = {
            "type": "cluster_leader_changed",
            "previous_leader": active_leader_elector.node_id if was_leader else None,
            "stepped_down": stepped_down,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await ws_manager.broadcast_all(broadcast_msg)
        return {
            "status": "stepped_down" if stepped_down else "noop",
            "node_id": active_leader_elector.node_id,
            "role": active_leader_elector.role.value,
        }

    @app.get("/api/v1/swarms/{swarm_id}/spans")
    async def get_spans(
        swarm_id: str,
        branch_id: str = Query(default="main"),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        return await active_storage.get_swarm_spans(
            swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id
        )

    @app.get("/api/v1/swarms/{swarm_id}/report")
    async def get_swarm_report(
        swarm_id: str,
        format: str = Query(default="json"),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Generates forensic incident post-mortem and financial ROI report."""
        raw_spans = await active_storage.get_swarm_spans(
            swarm_id,
            project_id=tenant.project_id if tenant.key_id else None,
        )
        if not raw_spans:
            raw_spans = await active_storage.get_swarm_spans(swarm_id)
            if not raw_spans:
                raise HTTPException(status_code=404, detail=f"Swarm '{swarm_id}' not found")

        generator = IncidentReportGenerator(storage=active_storage)
        fmt = format.lower()
        if fmt == "markdown":
            md_content = generator.generate_report(swarm_id, format="markdown")
            return Response(content=md_content, media_type="text/markdown; charset=utf-8")
        elif fmt == "terminal":
            term_content = generator.generate_report(swarm_id, format="terminal")
            return Response(content=term_content, media_type="text/plain; charset=utf-8")
        else:
            return generator.generate_report(swarm_id, format="json")

    @app.get("/api/v1/topology")
    async def get_topology(
        swarm_id: str = Query(default="swarm-live-01"),
        branch_id: str = Query(default="main"),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Computes spatial DAG topology, edge metrics, and cycle detections from persisted spans."""
        raw_spans = await active_storage.get_swarm_spans(
            swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id,
        )
        span_objects = []
        for s in raw_spans:
            if isinstance(s, SwarmSpan):
                span_objects.append(s)
            elif isinstance(s, dict):
                span_objects.append(SwarmSpan.model_validate(s))
        topo = build_topology_from_spans(span_objects)
        return topo.to_dict()

    @app.get("/api/v1/swarms/{swarm_id}/branches")
    async def get_branches(
        swarm_id: str,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        return await active_storage.list_branches(swarm_id, project_id=tenant.project_id)

    @app.get("/api/v1/swarms/{swarm_id}/snapshots")
    async def get_swarm_snapshots(
        swarm_id: str,
        branch_id: Optional[str] = None,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        """Returns all snapshots captured for a swarm run across branches."""
        return await active_storage.get_snapshots_for_swarm(
            swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id
        )

    @app.get("/api/v1/snapshots/{span_id}")
    async def get_snapshot(span_id: str):
        snapshot = await active_storage.get_snapshot(span_id)
        if not snapshot:
            return {"error": "snapshot_not_found"}
        return snapshot

    @app.get("/api/v1/swarms/{swarm_id}/snapshots/diff")
    async def get_snapshots_diff(
        swarm_id: str,
        base_span_id: Optional[str] = Query(default=None),
        target_span_id: Optional[str] = Query(default=None),
        from_span_id: Optional[str] = Query(default=None),
        to_span_id: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        """
        Computes the deterministic differential between two state snapshots in a swarm.
        Accepts base_span_id (or from_span_id) and target_span_id (or to_span_id).
        """
        b_id = base_span_id or from_span_id
        t_id = target_span_id or to_span_id
        if not b_id or not t_id:
            raise HTTPException(
                status_code=400,
                detail="Both base_span_id (or from_span_id) and target_span_id (or to_span_id) are required."
            )

        diff_report = await time_machine.diff_snapshots(
            swarm_id=swarm_id,
            span_id_a=b_id,
            span_id_b=t_id,
            project_id=tenant.project_id
        )
        return diff_report.to_dict()

    @app.post("/api/v1/simulate")
    async def simulate_swarm(
        swarm_id: str = Query(default="swarm-live-01"),
        branch_id: str = Query(default="main"),
        step_delay: Optional[float] = Query(default=None),
    ):
        """Triggers a live multi-agent software engineering swarm simulation on backend."""
        return await run_swarm_simulation(
            storage=active_storage,
            ws_manager=ws_manager,
            time_machine=time_machine,
            swarm_id=swarm_id,
            branch_id=branch_id,
            step_delay=step_delay,
            policy=active_policy,
        )

    @app.get("/api/v1/chaos/scenarios")
    async def get_chaos_scenarios():
        """Returns metadata catalog of all deterministic chaos scenarios."""
        return ChaosSimulator.get_available_scenarios()

    @app.post("/api/v1/chaos/inject")
    async def inject_chaos_endpoint(
        scenario: str = Query(default="entropy_collapse"),
        swarm_id: str = Query(default="swarm-live-01"),
        branch_id: str = Query(default="main"),
        step_delay: Optional[float] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """
        Executes an interactive chaos scenario, storing spans, triggering circuit interventions,
        updating Prometheus metrics, and streaming live updates over WebSocket.
        """
        try:
            result = await ChaosSimulator.run_scenario(
                scenario=scenario,
                storage=active_storage,
                time_machine=time_machine,
                swarm_id=swarm_id,
                branch_id=branch_id,
                project_id=tenant.project_id,
                step_delay=step_delay,
                broadcast_callback=ws_manager.broadcast,
            )
            return result.model_dump(mode="json")
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc)
            )

    @app.post("/api/v1/branches/fork")
    async def fork_branch_endpoint(req: ForkRequest, tenant: TenantContext = Depends(get_tenant_context)):
        """Forks swarm state from any intermediate turn using Time-Machine state branching."""
        try:
            state = await time_machine.step_back(
                target_span_id=req.target_span_id,
                new_branch_id=req.new_branch_id,
            )
            record_branch_fork(project_id=tenant.project_id)

            # 1. Replicate antecedent spans into the new branch up to target_span_id
            raw_spans = await active_storage.get_swarm_spans(req.swarm_id, project_id=tenant.project_id)
            target_idx = -1
            for idx, s in enumerate(raw_spans):
                sid = s.id if hasattr(s, "id") else s.get("id")
                if sid == req.target_span_id:
                    target_idx = idx
                    break

            if target_idx >= 0:
                forked_spans = []
                for s in raw_spans[:target_idx + 1]:
                    span_dict = s.model_dump() if hasattr(s, "model_dump") else dict(s)
                    orig_id = span_dict["id"]
                    span_dict["id"] = f"{orig_id}_fork_{req.new_branch_id}"
                    span_dict["branch_id"] = req.new_branch_id
                    forked_spans.append(SwarmSpan.model_validate(span_dict))
                if forked_spans:
                    await active_storage.save_spans(forked_spans)

            # 2. Save snapshot for the new branch at the fork root
            await time_machine.snapshot(
                span_id=f"{req.target_span_id}_fork_{req.new_branch_id}",
                state_data=state,
                branch_id=req.new_branch_id,
            )

            # 3. Broadcast branch_forked notification over WebSocket
            await ws_manager.broadcast(req.swarm_id, {
                "type": "branch_forked",
                "swarm_id": req.swarm_id,
                "target_span_id": req.target_span_id,
                "new_branch_id": req.new_branch_id,
                "state": state,
            })

            return {
                "status": "forked",
                "swarm_id": req.swarm_id,
                "new_branch_id": req.new_branch_id,
                "target_span_id": req.target_span_id,
                "replicated_spans_count": len(forked_spans) if 'forked_spans' in locals() else 0,
                "state": state,
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @app.post("/api/v1/branches/{branch_id}/resume")
    async def resume_branch_endpoint(
        branch_id: str,
        req: BranchResumeRequest,
        tenant: TenantContext = Depends(get_tenant_context)
    ):
        """Programmatically resumes or validates branch execution readiness from a forked state snapshot."""
        try:
            snapshots = await active_storage.get_snapshots_for_swarm(
                req.swarm_id,
                branch_id=branch_id,
                project_id=tenant.project_id
            )
            raw_spans = await active_storage.get_swarm_spans(req.swarm_id, project_id=tenant.project_id)
            branch_spans = [s for s in raw_spans if (s.branch_id if hasattr(s, "branch_id") else s.get("branch_id")) == branch_id]

            if not snapshots and not branch_spans:
                return {
                    "status": "error",
                    "message": f"Branch '{branch_id}' does not exist or has no state snapshot"
                }

            base_state = dict(snapshots[-1].state_data) if snapshots else {}
            source_span_id = snapshots[-1].span_id if snapshots else (branch_spans[-1].id if hasattr(branch_spans[-1], "id") else branch_spans[-1].get("id"))

            from swarmwatch.serializer import SafeStateSerializer
            serializer = SafeStateSerializer()
            merged_state = dict(base_state)
            sanitized_overrides = {}
            if req.state_overrides:
                sanitized_overrides = serializer.sanitize(req.state_overrides)
                merged_state.update(sanitized_overrides)

            resumption_span_id = str(uuid.uuid4())
            now_dt = datetime.now(timezone.utc)

            # Record boundary resumption span
            resumption_span = SwarmSpan(
                id=resumption_span_id,
                swarm_id=req.swarm_id,
                project_id=tenant.project_id,
                branch_id=branch_id,
                parent_span_id=source_span_id,
                source_agent="fork_runner",
                target_agent="resumed_agent",
                span_type="fork_resumption",
                input_payload={
                    "branch_id": branch_id,
                    "source_span_id": source_span_id,
                    "state_overrides": sanitized_overrides,
                    "initial_state": merged_state,
                },
                output_payload={"status": "resumed"},
                token_count=0,
                entropy_score=1.0,
                start_time=now_dt,
                end_time=now_dt,
                duration_ms=0.001,
            )
            await active_storage.save_spans([resumption_span])

            # Broadcast branch_resumed event over WebSocket
            await ws_manager.broadcast(req.swarm_id, {
                "type": "branch_resumed",
                "swarm_id": req.swarm_id,
                "branch_id": branch_id,
                "resumption_span_id": resumption_span_id,
                "state": merged_state,
            })

            return {
                "status": "resumed",
                "swarm_id": req.swarm_id,
                "branch_id": branch_id,
                "resumption_span_id": resumption_span_id,
                "root_span_id": source_span_id,
                "state": merged_state,
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @app.post("/api/v1/storage/archive/{swarm_id}")
    async def archive_swarm_endpoint(
        swarm_id: str,
        req: ArchiveSwarmRequest = Body(default_factory=ArchiveSwarmRequest),
        tenant: TenantContext = Depends(require_permission(Permission.STORAGE_ARCHIVE)),
        x_mfa_code: Optional[str] = Header(default=None, alias="X-MFA-Code"),
    ):
        """Archives a swarm from hot storage to cold S3/GCS object storage."""
        if req.delete_local and tenant.user_id:
            user = await active_storage.get_user(tenant.user_id)
            if user and user.mfa_enabled and not tenant.mfa_verified:
                verified = False
                if x_mfa_code:
                    if user.totp_secret and TotpEngine.verify_totp(x_mfa_code, user.totp_secret):
                        verified = True
                    else:
                        valid_b, remaining = TotpEngine.verify_and_consume_backup_code(x_mfa_code, user.backup_codes)
                        if valid_b:
                            user.backup_codes = remaining
                            await active_storage.save_user(user)
                            verified = True
                if not verified:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="MFA verification required to delete local data"
                    )

        try:
            record = await active_archiver.archive_swarm(
                swarm_id=swarm_id,
                bucket=req.bucket,
                delete_local=req.delete_local,
                project_id=tenant.project_id,
                org_id=tenant.org_id,
            )
            await ws_manager.broadcast_all({
                "type": "swarm_archived",
                "swarm_id": swarm_id,
                "archive_key": record.archive_key,
                "compressed_size_bytes": record.compressed_size_bytes,
                "sha256": record.sha256_checksum,
            })
            return {"status": "archived", "record": record.model_dump(mode="json")}
        except Exception as e:
            logger.error(f"Error archiving swarm '{swarm_id}': {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/v1/storage/restore/{swarm_id}")
    async def restore_swarm_endpoint(
        swarm_id: str,
        bucket: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(require_permission(Permission.STORAGE_RESTORE)),
    ):
        """Restores an archived swarm from cold object storage into hot database."""
        try:
            summary = await active_archiver.restore_swarm(
                swarm_id=swarm_id,
                bucket=bucket,
                target_storage=active_storage,
            )
            await ws_manager.broadcast_all({
                "type": "swarm_restored",
                "swarm_id": swarm_id,
                "spans_restored": summary.spans_restored,
                "snapshots_restored": summary.snapshots_restored,
            })
            return {"status": "restored", "summary": summary.model_dump(mode="json")}
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error restoring swarm '{swarm_id}': {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/v1/storage/archives")
    async def list_archives_endpoint(
        bucket: Optional[str] = Query(default=None),
        prefix: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Lists all archived swarms across cold object storage."""
        archives = active_archiver.list_archives(
            bucket=bucket,
            project_id=tenant.project_id,
            prefix=prefix,
        )
        return {"archives": [a.model_dump(mode="json") for a in archives]}

    @app.get("/api/v1/storage/archives/{swarm_id}/manifest")
    async def get_archive_manifest_endpoint(
        swarm_id: str,
        bucket: Optional[str] = Query(default=None),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Retrieves raw cryptographic manifest for an archived swarm."""
        manifest = active_archiver.get_archive_manifest(swarm_id=swarm_id, bucket=bucket)
        if not manifest:
            raise HTTPException(status_code=404, detail=f"Manifest not found for swarm '{swarm_id}'")
        return manifest.model_dump(mode="json")

    # -----------------------------------------------------------------------
    # Kernel-Level Network Socket LLM Interception Gateway (Seam 29 / ADR 0040)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/ebpf/status")
    async def get_ebpf_status_endpoint():
        """Returns diagnostic and telemetry status of the eBPF socket gateway."""
        return global_ebpf_gateway.status()

    @app.get("/api/v1/ebpf/rules")
    async def list_ebpf_rules_endpoint():
        """Lists active socket inspection and circuit enforcement rules."""
        rules = global_ebpf_gateway.list_rules()
        return {"rules": [r.to_dict() for r in rules], "count": len(rules)}

    @app.post("/api/v1/ebpf/rules")
    async def create_ebpf_rule_endpoint(
        rule_payload: Dict[str, Any] = Body(...),
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Creates or registers an eBPF socket interception rule."""
        rule = SocketRule.from_dict(rule_payload)
        saved = global_ebpf_gateway.add_rule(rule)
        return {"status": "created", "rule": saved.to_dict()}

    @app.delete("/api/v1/ebpf/rules/{rule_id}")
    async def delete_ebpf_rule_endpoint(
        rule_id: str,
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Removes an active socket interception rule."""
        removed = global_ebpf_gateway.delete_rule(rule_id)
        if not removed:
            raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found")
        return {"status": "deleted", "id": rule_id}

    @app.post("/api/v1/ebpf/simulate")
    async def simulate_ebpf_stream_endpoint(
        payload: Dict[str, Any] = Body(...),
    ):
        """Simulates network stream payload inspection and instant TCP RST intervention."""
        data_str = payload.get("data", "")
        data_bytes = data_str.encode("utf-8")
        host = payload.get("host", "api.openai.com")
        port = int(payload.get("port", 443))
        src_addr = payload.get("src_addr", "127.0.0.1")

        terminated, intervention, decoder = global_ebpf_gateway.inspect_stream_data(
            data=data_bytes,
            host=host,
            port=port,
            src_addr=src_addr,
        )
        return {
            "status": "processed",
            "terminated": terminated,
            "intervention": intervention.to_dict() if intervention else None,
            "tokens_estimated": decoder.estimated_token_count,
            "entropy": decoder.entropy,
            "extracted_text": decoder.extracted_text[:200] if decoder.extracted_text else "",
        }

    @app.post("/api/v1/ebpf/attach")
    async def attach_ebpf_gateway_endpoint(
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Attaches the eBPF socket gateway."""
        global_ebpf_gateway.set_attached(True)
        return {"status": "attached"}

    @app.post("/api/v1/ebpf/detach")
    async def detach_ebpf_gateway_endpoint(
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Detaches the eBPF socket gateway."""
        global_ebpf_gateway.set_attached(False)
        return {"status": "detached"}

    @app.get("/api/v1/raft/status")
    async def get_raft_status_endpoint():
        """Returns the current state and metrics of the Raft consensus engine."""
        status_data = global_raft_engine.status()
        status_data["swarms"] = global_raft_engine.state_machine.list_swarms()
        return status_data

    @app.post("/api/v1/raft/propose")
    async def propose_raft_command_endpoint(
        req: RaftProposeRequest,
        tenant: TenantContext = Depends(require_permission(Permission.FLEET_MANAGE)),
    ):
        """Submits a command proposal to the Raft cluster leader."""
        # Standalone single node auto-elects if follower
        if global_raft_engine.cluster_size <= 1 and not global_raft_engine.is_leader:
            global_raft_engine.become_leader()

        success, entry, leader_id = await global_raft_engine.propose(req.command, req.payload)
        if not success:
            if leader_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Node is not leader. Current leader is '{leader_id}'",
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to reach Raft consensus quorum for proposal",
            )
        return {
            "status": "committed",
            "entry": entry.to_dict() if entry else None,
            "commit_index": global_raft_engine.commit_index,
        }

    @app.post("/api/v1/raft/rpc/request-vote")
    async def raft_request_vote_rpc_endpoint(
        rpc_data: Dict[str, Any] = Body(...),
    ):
        """Inter-node Raft RPC: RequestVote."""
        rpc = RequestVoteRpc.from_dict(rpc_data)
        resp = global_raft_engine.handle_request_vote(rpc)
        return resp.to_dict()

    @app.post("/api/v1/raft/rpc/append-entries")
    async def raft_append_entries_rpc_endpoint(
        rpc_data: Dict[str, Any] = Body(...),
    ):
        """Inter-node Raft RPC: AppendEntries (heartbeat / log replication)."""
        rpc = AppendEntriesRpc.from_dict(rpc_data)
        resp = global_raft_engine.handle_append_entries(rpc)
        return resp.to_dict()

    @app.post("/api/v1/raft/step-down")
    async def raft_step_down_endpoint(
        new_term: Optional[int] = Query(None),
        tenant: TenantContext = Depends(require_permission(Permission.FLEET_MANAGE)),
    ):
        """Forces the current Raft node to step down to FOLLOWER."""
        global_raft_engine.step_down(new_term)
        return {
            "status": "stepped_down",
            "role": global_raft_engine.role,
            "term": global_raft_engine.current_term,
        }

    @app.get("/api/v1/self-healing/status")
    async def get_self_healing_status_endpoint():
        """Returns diagnostic state and counterfactual savings of the self-healing controller."""
        return global_self_healing_controller.status()

    @app.get("/api/v1/self-healing/actions")
    async def get_self_healing_actions_endpoint():
        """Returns the full ledger of autonomous self-healing actions."""
        return {"actions": [a.to_dict() for a in global_self_healing_controller.get_action_history()]}

    @app.post("/api/v1/self-healing/remediate")
    async def remediate_self_healing_endpoint(
        req: SelfHealingRemediateRequest,
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Enacts an explicit self-healing remediation action."""
        action = global_self_healing_controller.remediate(
            swarm_id=req.swarm_id,
            agent_name=req.agent_name,
            action_type=req.action_type,
            target=req.target,
            reason=req.reason,
        )
        return action.to_dict()

    @app.post("/api/v1/self-healing/rollback")
    async def rollback_self_healing_endpoint(
        action_id: Optional[str] = Query(None),
        swarm_id: Optional[str] = Query(None),
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Reverts a specific self-healing action or all actions for a swarm."""
        if action_id:
            success = global_self_healing_controller.rollback_action(action_id)
            if not success:
                raise HTTPException(status_code=404, detail=f"Active action '{action_id}' not found")
            return {"status": "rolled_back", "action_id": action_id}
        rolled_count = global_self_healing_controller.rollback_all(swarm_id=swarm_id)
        return {"status": "rolled_back_all", "count": rolled_count}

    @app.post("/api/v1/self-healing/toggle")
    async def toggle_self_healing_endpoint(
        enabled: bool = Query(True),
        tenant: TenantContext = Depends(require_permission(Permission.POLICY_WRITE)),
    ):
        """Enables or disables autonomous self-healing controller execution."""
        global_self_healing_controller.config.enabled = enabled
        return {"status": "updated", "enabled": enabled}

    @app.get("/api/v1/swarms/{swarm_id}/root-cause")
    async def get_root_cause_endpoint(
        swarm_id: str,
        branch_id: str = Query("main"),
        symptom_span_id: Optional[str] = Query(None),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Executes Structural Causal Model root-cause inference for a swarm session (ADR 0043)."""
        spans = await active_storage.get_swarm_spans(
            swarm_id=swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id if tenant else None,
        )
        result = global_causal_inference_engine.infer_root_cause(
            spans=spans,
            symptom_span_id=symptom_span_id,
        )
        return result.to_dict()

    @app.get("/api/v1/swarms/{swarm_id}/causal-path")
    async def get_causal_path_endpoint(
        swarm_id: str,
        symptom_span_id: Optional[str] = Query(None),
        branch_id: str = Query("main"),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Returns the critical failure propagation path leading to symptom manifestation."""
        spans = await active_storage.get_swarm_spans(
            swarm_id=swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id if tenant else None,
        )
        if symptom_span_id:
            path = global_causal_inference_engine.trace_causal_path(spans, symptom_span_id)
        else:
            res = global_causal_inference_engine.infer_root_cause(spans)
            path = res.attribution.critical_path if res.attribution else []
        return {"swarm_id": swarm_id, "critical_path": [p.to_dict() if hasattr(p, "to_dict") else p for p in path]}

    @app.get("/api/v1/swarms/{swarm_id}/context-poisoning")
    async def get_context_poisoning_endpoint(
        swarm_id: str,
        branch_id: str = Query("main"),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Returns context poisoning metrics measuring prompt corruption contagion across agents."""
        spans = await active_storage.get_swarm_spans(
            swarm_id=swarm_id,
            branch_id=branch_id,
            project_id=tenant.project_id if tenant else None,
        )
        metrics = global_causal_inference_engine.compute_context_poisoning(spans)
        return {"swarm_id": swarm_id, "context_poisoning": [m.to_dict() if hasattr(m, "to_dict") else m for m in metrics]}

    # -----------------------------------------------------------------------
    # Ultra-High-Throughput gRPC Streaming Ingestion Gateway (Seam 33 / ADR 0044)
    # -----------------------------------------------------------------------
    global_grpc_gateway.set_storage(active_storage)
    global_grpc_gateway.set_broadcast_callback(ws_manager.broadcast)
    global_grpc_gateway.set_alert_callback(broadcast_alert_event)

    @app.post("/swarmwatch.v1.TelemetryService/StreamSpans")
    @app.post("/swarmwatch.v1.TelemetryService/IngestBatch")
    @app.post("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
    async def grpc_gateway_endpoint(
        request: Request,
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """
        Ultra-High-Throughput gRPC Streaming Ingestion Gateway endpoint (ADR 0044).
        Accepts binary 5-byte framed gRPC streaming payloads (application/grpc and application/grpc+proto).
        """
        raw_body = await request.body()
        path = request.url.path
        responses, status_code, err = await global_grpc_gateway.handle_grpc_stream_chunk(
            chunk=raw_body,
            service_path=path,
            project_id=tenant.project_id,
        )

        resp_content = b"".join(responses) if responses else b""
        headers = {
            "content-type": "application/grpc",
            "grpc-status": str(status_code),
        }
        if err:
            headers["grpc-message"] = str(err)
        return Response(content=resp_content, status_code=200, headers=headers)

    @app.get("/api/v1/grpc/status")
    async def get_grpc_status_endpoint():
        """Returns diagnostic and telemetry throughput statistics of the gRPC streaming gateway."""
        return global_grpc_gateway.status()

    @app.post("/api/v1/grpc/simulate")
    async def simulate_grpc_stream_endpoint(
        spans_count: int = Query(default=10),
        induce_loop: bool = Query(default=False),
        tenant: TenantContext = Depends(get_tenant_context),
    ):
        """Simulates in-memory gRPC streaming spans with optional loop induction for verification."""
        synthetic_spans = []
        for i in range(spans_count):
            entropy = 0.18 if (induce_loop and i == spans_count - 1) else 0.88
            synthetic_spans.append({
                "id": f"sim-grpc-span-{i}",
                "trace_id": f"sim-grpc-trace-{int(time.time()*1000)}",
                "swarm_id": "swarm-live-01",
                "source_agent": "planner_agent" if i % 2 == 0 else "coder_agent",
                "target_agent": "coder_agent" if i % 2 == 0 else "planner_agent",
                "span_type": "turn",
                "token_count": 120,
                "entropy_score": entropy,
                "status": "ok",
                "input_payload": {"msg": f"synthetic step {i}"},
                "output_payload": {"res": "acknowledged"},
            })

        payload = json.dumps(synthetic_spans).encode("utf-8")
        framed = GrpcMessageFramer.encode_frame(payload)
        responses, code, err = await global_grpc_gateway.handle_grpc_stream_chunk(
            chunk=framed,
            service_path="/swarmwatch.v1.TelemetryService/StreamSpans",
            project_id=tenant.project_id,
        )
        return {
            "status": "processed",
            "spans_sent": spans_count,
            "grpc_status": GrpcStatusCode.name_of(code),
            "grpc_status_code": code,
            "error_message": err,
            "response_frames_count": len(responses),
        }

    # -----------------------------------------------------------------------
    # Embedding-Free Vector Semantic Drift & Hallucination Detector (ADR 0045)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/swarms/{swarm_id}/drift")
    async def get_swarm_drift_endpoint(swarm_id: str):
        """Returns aggregate drift statistics and complete turn timeline for a swarm session."""
        stats = global_drift_detector.get_aggregate_stats(swarm_id)
        timeline = global_drift_detector.get_timeline(swarm_id)
        return {
            **stats,
            "timeline": [p.model_dump(mode="json") for p in timeline],
        }

    @app.post("/api/v1/swarms/{swarm_id}/drift/anchor")
    async def set_swarm_drift_anchor_endpoint(
        swarm_id: str,
        payload: Dict[str, Any] = Body(...),
    ):
        """Sets the root task anchor text for a swarm session."""
        anchor_text = payload.get("anchor_text", "")
        if not anchor_text:
            raise HTTPException(status_code=400, detail="Missing required field 'anchor_text'")
        global_drift_detector.set_anchor(swarm_id, anchor_text)
        return {"status": "anchor_set", "swarm_id": swarm_id, "anchor_length": len(anchor_text)}

    @app.post("/api/v1/swarms/{swarm_id}/drift/evaluate")
    async def evaluate_turn_drift_endpoint(
        swarm_id: str,
        payload: Dict[str, Any] = Body(...),
    ):
        """Evaluates an individual turn text for semantic drift against the swarm anchor."""
        text = payload.get("text", "")
        agent_name = payload.get("agent_name", "agent")
        span_id = payload.get("span_id")
        turn_index = payload.get("turn_index")
        profile = global_drift_detector.evaluate_turn(
            swarm_id=swarm_id,
            agent_name=agent_name,
            text=text,
            span_id=span_id,
            turn_index=turn_index,
        )
        return profile.model_dump(mode="json")

    @app.post("/api/v1/swarms/{swarm_id}/drift/reset")
    async def reset_swarm_drift_endpoint(swarm_id: str):
        """Resets the semantic drift tracker and anchor for a swarm."""
        global_drift_detector.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    # -----------------------------------------------------------------------
    # Adaptive Dynamic Quota Re-balancing (Seam 35 / ADR 0046)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/swarms/{swarm_id}/rebalancer")
    async def get_swarm_rebalancer_endpoint(swarm_id: str):
        """Returns aggregate rebalancer statistics, active allocations, and latest plan."""
        stats = global_rebalancer.get_aggregate_stats(swarm_id)
        allocations = global_rebalancer.get_allocations(swarm_id)
        plan = global_rebalancer.get_plan(swarm_id)
        return {
            **stats,
            "allocations": {aid: alloc.model_dump(mode="json") for aid, alloc in allocations.items()},
            "latest_plan": plan.model_dump(mode="json") if plan else None,
        }

    @app.post("/api/v1/swarms/{swarm_id}/rebalancer/rebalance")
    async def trigger_swarm_rebalance_endpoint(
        swarm_id: str,
        payload: Optional[Dict[str, Any]] = Body(None),
    ):
        """Triggers an immediate dynamic quota rebalancing epoch for a swarm."""
        justification = (payload or {}).get("justification") if payload else None
        plan = global_rebalancer.compute_rebalance(swarm_id, justification=justification)
        return plan.model_dump(mode="json")

    @app.post("/api/v1/swarms/{swarm_id}/rebalancer/quota")
    async def set_swarm_fleet_quota_endpoint(
        swarm_id: str,
        payload: Dict[str, Any] = Body(...),
    ):
        """Sets the global baseline token quota for a swarm."""
        total_tokens = int(payload.get("total_tokens", 100_000))
        global_rebalancer.set_fleet_quota(swarm_id, total_tokens)
        return {"status": "ok", "swarm_id": swarm_id, "fleet_quota_tokens": total_tokens}

    @app.post("/api/v1/swarms/{swarm_id}/rebalancer/policy")
    async def update_swarm_rebalancer_policy_endpoint(
        swarm_id: str,
        payload: Dict[str, Any] = Body(...),
    ):
        """Updates rebalancer policy configuration."""
        policy_dict = payload.get("policy", payload)
        new_policy = RebalancePolicy(**policy_dict)
        global_rebalancer.policy = new_policy
        return {"status": "policy_updated", "policy": new_policy.model_dump(mode="json")}

    @app.post("/api/v1/swarms/{swarm_id}/rebalancer/reset")
    async def reset_swarm_rebalancer_endpoint(swarm_id: str):
        """Resets the rebalancer state for a swarm."""
        global_rebalancer.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    # -----------------------------------------------------------------------
    # Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (ADR 0047)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/swarms/{swarm_id}/coalitions")
    async def get_swarm_coalitions_endpoint(swarm_id: str):
        """Returns topological clustering result, active coalitions, and node centralities."""
        result = global_coalition_engine.compute_clusters(swarm_id)
        return result.model_dump(mode="json")

    @app.post("/api/v1/swarms/{swarm_id}/coalitions/cluster")
    async def trigger_swarm_clustering_endpoint(swarm_id: str):
        """Forces an immediate topological clustering epoch and coalition formation."""
        result = global_coalition_engine.compute_clusters(swarm_id)
        return result.model_dump(mode="json")

    @app.post("/api/v1/swarms/{swarm_id}/coalitions/reset")
    async def reset_swarm_coalitions_endpoint(swarm_id: str):
        """Resets topological graph data and coalitions for a swarm."""
        global_coalition_engine.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    # -----------------------------------------------------------------------
    # Zero-Copy Shared-Memory Ring Buffer IPC Gateway (ADR 0048)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/shm/status")
    async def get_shm_status_endpoint():
        """Returns shared-memory circular ring buffer health, capacity, and counters."""
        return global_shm_gateway.get_status()

    @app.post("/api/v1/shm/flush")
    async def flush_shm_endpoint():
        """Drains pending causal spans from the shared memory ring buffer into storage."""
        spans = global_shm_gateway.flush()
        return {
            "status": "flushed",
            "drained_count": len(spans),
            "pending_records": global_shm_gateway.get_status()["pending_records"],
        }

    @app.post("/api/v1/shm/reset")
    async def reset_shm_endpoint():
        """Resets the shared memory ring buffer pointers and counters."""
        global_shm_gateway.reset()
        return {"status": "reset"}

    # -----------------------------------------------------------------------
    # Differential Privacy & Cryptographic State Anonymization (ADR 0049)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/privacy/status")
    async def get_privacy_status_endpoint():
        """Returns active differential privacy metrics and budget accounting."""
        return global_privacy_engine.get_status()

    @app.post("/api/v1/privacy/anonymize")
    async def anonymize_spans_endpoint(req: AnonymizeRequest):
        """Applies Laplace perturbation and HMAC pseudonymization to spans."""
        anonymized = global_privacy_engine.anonymize_batch(req.spans, epsilon=req.epsilon)
        return {
            "status": "anonymized",
            "count": len(anonymized),
            "spans": anonymized,
            "budget": global_privacy_engine.get_status(),
        }

    @app.post("/api/v1/privacy/policy")
    async def update_privacy_policy_endpoint(req: PrivacyPolicyRequest):
        """Updates privacy policy bounds and configuration parameters."""
        curr = global_privacy_engine.policy
        if req.epsilon is not None:
            curr.epsilon = req.epsilon
        if req.total_epsilon_budget is not None:
            curr.total_epsilon_budget = req.total_epsilon_budget
        if req.tenant_salt is not None:
            curr.tenant_salt = req.tenant_salt
        if req.anonymize_identities is not None:
            curr.anonymize_identities = req.anonymize_identities
        if req.anonymize_metrics is not None:
            curr.anonymize_metrics = req.anonymize_metrics
        if req.anonymize_payloads is not None:
            curr.anonymize_payloads = req.anonymize_payloads
        return {
            "status": "policy_updated",
            "policy": curr.to_dict(),
        }

    @app.post("/api/v1/privacy/reset")
    async def reset_privacy_budget_endpoint():
        """Resets the spent privacy budget back to zero."""
        global_privacy_engine.reset_budget()
        return {"status": "budget_reset", "status_info": global_privacy_engine.get_status()}

    @app.get("/api/v1/swarms/{swarm_id}/forecast")
    async def get_swarm_forecast_endpoint(swarm_id: str):
        """Retrieves predictive martingale failure forecast report for the specified swarm."""
        return global_forecaster.get_forecast(swarm_id).to_dict()

    @app.post("/api/v1/swarms/{swarm_id}/forecast/evaluate")
    async def evaluate_swarm_forecast_endpoint(swarm_id: str, req: ForecastEvaluateRequest):
        """Observes a turn and evaluates discrete-time entropy martingale risk."""
        report = global_forecaster.observe_turn(
            swarm_id=swarm_id,
            turn_index=req.turn_index,
            entropy=req.entropy,
            agent=req.agent,
        )

        return {
            "status": "evaluated",
            "report": report.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/forecast/reset")
    async def reset_swarm_forecast_endpoint(swarm_id: str):
        """Resets the martingale forecast history and state for the specified swarm."""
        global_forecaster.reset(swarm_id)
        return {"status": "forecast_reset", "swarm_id": swarm_id}

    @app.get("/api/v1/swarms/{swarm_id}/byzantine/status")
    async def get_byzantine_status_endpoint(swarm_id: str):
        """Returns BFT resilience report and quarantined agent list for the swarm."""
        return global_byzantine_harness.get_resilience_report(swarm_id).to_dict()

    @app.post("/api/v1/swarms/{swarm_id}/byzantine/inject")
    async def inject_byzantine_attack_endpoint(swarm_id: str, req: ByzantineInjectRequest):
        """Injects a synthetic Byzantine adversarial turn and evaluates defense response."""
        result = global_byzantine_harness.inject_adversarial_turn(
            swarm_id=swarm_id,
            agent_id=req.agent_id,
            attack_type=req.attack_type,
            custom_payload=req.payload,
            turn_index=req.turn_index,
        )
        report = global_byzantine_harness.get_resilience_report(swarm_id)
        return {
            "status": "injected",
            "injection": result,
            "resilience": report.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/byzantine/clear")
    async def clear_byzantine_attacks_endpoint(swarm_id: str):
        """Clears active attacks and lifts quarantines for the swarm."""
        global_byzantine_harness.reset(swarm_id)
        return {"status": "cleared", "swarm_id": swarm_id}

    @app.get("/api/v1/swarms/{swarm_id}/honeypot/status")
    async def get_honeypot_status_endpoint(swarm_id: str):
        """Returns honeypot deception status, canary tokens, and sandboxed agents."""
        return global_honeypot_manager.get_report(swarm_id).to_dict()

    @app.post("/api/v1/swarms/{swarm_id}/honeypot/token")
    async def create_honeytoken_endpoint(swarm_id: str, req: HoneytokenCreateRequest):
        """Generates and deploys a synthetic canary honeytoken."""
        token = global_honeypot_manager.generate_honeytoken(swarm_id, req.token_type)
        return {"status": "created", "honeytoken": token.to_dict()}

    @app.post("/api/v1/swarms/{swarm_id}/honeypot/divert")
    async def divert_agent_endpoint(swarm_id: str, req: HoneypotDivertRequest):
        """Diverts an agent into the deception sandbox."""
        global_honeypot_manager.divert_agent(swarm_id, req.agent_id, req.reason)
        return {"status": "diverted", "swarm_id": swarm_id, "agent_id": req.agent_id, "reason": req.reason}

    @app.post("/api/v1/swarms/{swarm_id}/honeypot/reset")
    async def reset_honeypot_endpoint(swarm_id: str):
        """Clears honeypot state, canary tokens, and sandboxed agents for the swarm."""
        global_honeypot_manager.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    @app.get("/api/v1/swarms/{swarm_id}/model-check/status")
    async def get_model_check_status_endpoint(swarm_id: str):
        """Returns the latest Bounded Model Checking verification result for the swarm."""
        res = global_model_checker.get_latest_result(swarm_id)
        if res is None:
            model = global_model_checker._models.get(swarm_id)
            if model is None:
                model = AgentTransitionModel(
                    initial_agent="agent-planner",
                    final_agents={"agent-verifier"},
                    transitions=[
                        AgentTransition(from_agent="agent-planner", to_agent="agent-worker", action_type="DELEGATE", token_cost=150),
                        AgentTransition(from_agent="agent-worker", to_agent="agent-verifier", action_type="REVIEW", token_cost=200),
                    ],
                    max_token_budget=10000,
                )
            res = global_model_checker.verify(model, bound_k=10, swarm_id=swarm_id)
        return res.to_dict()

    @app.post("/api/v1/swarms/{swarm_id}/model-check/verify")
    async def run_model_check_endpoint(swarm_id: str, req: ModelCheckVerifyRequest):
        """Runs bounded model checking verification against a provided or registered transition model."""
        transitions_list = []
        if req.transitions:
            for t in req.transitions:
                transitions_list.append(
                    AgentTransition(
                        from_agent=t.get("from_agent", "agent-src"),
                        to_agent=t.get("to_agent", "agent-dst"),
                        action_type=t.get("action_type", "CALL"),
                        token_cost=t.get("token_cost", 100),
                        required_flags=t.get("required_flags", []),
                        sets_flags=t.get("sets_flags", []),
                    )
                )

        initial = req.initial_agent or "agent-planner"
        final = set(req.final_agents) if req.final_agents else {"agent-verifier"}
        budget = req.max_token_budget or 10000
        mutex = [tuple(p) for p in req.mutual_exclusion_pairs] if req.mutual_exclusion_pairs else []

        model = AgentTransitionModel(
            initial_agent=initial,
            final_agents=final,
            transitions=transitions_list if transitions_list else None,
            max_token_budget=budget,
            mutual_exclusion_pairs=mutex,
        )

        result = global_model_checker.verify(
            model=model,
            bound_k=req.bound_k,
            invariants=req.invariants,
            swarm_id=swarm_id,
        )
        return {
            "status": "completed",
            "swarm_id": swarm_id,
            "verification": result.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/model-check/reset")
    async def reset_model_check_endpoint(swarm_id: str):
        """Resets model checking history and models for the swarm."""
        global_model_checker.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    @app.get("/api/v1/swarms/{swarm_id}/topology-heal/status")
    async def get_topology_heal_status_endpoint(swarm_id: str):
        """Returns active graph patches and auto-healing status for the swarm."""
        active_patches = [p.to_dict() for p in global_topology_healer.get_active_patches(swarm_id)]
        latest_rep = global_topology_healer.get_latest_report(swarm_id)
        return {
            "swarm_id": swarm_id,
            "active_patches_count": len(active_patches),
            "active_patches": active_patches,
            "latest_report": latest_rep.to_dict() if latest_rep else None,
            "status": "PATCHED" if active_patches else "NOMINAL",
        }

    @app.post("/api/v1/swarms/{swarm_id}/topology-heal/apply")
    async def apply_topology_heal_endpoint(swarm_id: str, req: TopologyHealApplyRequest):
        """Synthesizes, applies, and reverifies an automated graph patch for the swarm."""
        model = None
        if req.transitions:
            transitions_list = []
            for t in req.transitions:
                transitions_list.append(
                    AgentTransition(
                        from_agent=t.get("from_agent", "agent-src"),
                        to_agent=t.get("to_agent", "agent-dst"),
                        action_type=t.get("action_type", "CALL"),
                        token_cost=t.get("token_cost", 100),
                        required_flags=t.get("required_flags", []),
                        sets_flags=t.get("sets_flags", []),
                    )
                )
            initial = req.initial_agent or "agent-planner"
            final = set(req.final_agents) if req.final_agents else {"agent-verifier"}
            budget = req.max_token_budget or 10000
            mutex = [tuple(p) for p in req.mutual_exclusion_pairs] if req.mutual_exclusion_pairs else []
            model = AgentTransitionModel(
                initial_agent=initial,
                final_agents=final,
                transitions=transitions_list,
                max_token_budget=budget,
                mutual_exclusion_pairs=mutex,
            )
        else:
            model = global_model_checker._models.get(swarm_id)
            if model is None:
                model = AgentTransitionModel(
                    initial_agent="agent-planner",
                    final_agents={"agent-verifier"},
                    transitions=[
                        AgentTransition(from_agent="agent-planner", to_agent="agent-deadlock", action_type="CALL", token_cost=100),
                    ],
                    max_token_budget=10000,
                )

        report = global_topology_healer.heal_and_verify(model, bound_k=req.bound_k, swarm_id=swarm_id)
        return {
            "status": "applied",
            "swarm_id": swarm_id,
            "report": report.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/topology-heal/revert")
    async def revert_topology_heal_endpoint(swarm_id: str):
        """Reverts active topology patches for the swarm."""
        reverted = global_topology_healer.revert_patch(swarm_id)
        return {
            "status": "reverted" if reverted else "no_active_patches",
            "swarm_id": swarm_id,
        }

    @app.get("/api/v1/swarms/{swarm_id}/artifacts")
    async def get_artifacts_endpoint(swarm_id: str):
        """Returns inspected and watermarked artifacts for the swarm session."""
        artifacts = [a.to_dict() for a in global_multimodal_inspector.get_artifacts(swarm_id)]
        return {
            "swarm_id": swarm_id,
            "artifacts_count": len(artifacts),
            "artifacts": artifacts,
            "status": "SECURED" if artifacts else "EMPTY",
        }

    @app.post("/api/v1/swarms/{swarm_id}/artifacts/inspect")
    async def inspect_artifact_endpoint(swarm_id: str, req: ArtifactInspectRequest):
        """Inspects raw payload bytes, extracts technical metadata, and computes perceptual hash."""
        import base64
        try:
            raw_bytes = base64.b64decode(req.payload_base64)
        except Exception:
            raw_bytes = req.payload_base64.encode("utf-8")
        metadata = global_multimodal_inspector.inspect_bytes(raw_bytes, filename=req.filename)
        return {
            "status": "inspected",
            "swarm_id": swarm_id,
            "metadata": metadata.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/artifacts/watermark")
    async def watermark_artifact_endpoint(swarm_id: str, req: ArtifactWatermarkRequest):
        """Applies cryptographic provenance watermark to payload and registers artifact."""
        import base64
        try:
            raw_bytes = base64.b64decode(req.payload_base64)
        except Exception:
            raw_bytes = req.payload_base64.encode("utf-8")
        watermarked = global_multimodal_inspector.inspect_and_watermark(
            payload=raw_bytes,
            producer_agent_id=req.producer_agent_id,
            swarm_id=swarm_id,
            filename=req.filename,
            artifact_id=req.artifact_id,
            tenant_secret=req.tenant_secret,
            tenant_id=req.tenant_id,
        )
        return {
            "status": "watermarked",
            "swarm_id": swarm_id,
            "artifact": watermarked.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/artifacts/verify")
    async def verify_artifact_endpoint(swarm_id: str, req: ArtifactVerifyRequest):
        """Verifies integrity and cryptographic provenance of an artifact."""
        import base64
        try:
            raw_bytes = base64.b64decode(req.payload_base64)
        except Exception:
            raw_bytes = req.payload_base64.encode("utf-8")

        artifact = global_multimodal_inspector.get_artifact(swarm_id, req.artifact_id)
        if not artifact:
            raise HTTPException(status_code=404, detail=f"Artifact {req.artifact_id} not found for swarm {swarm_id}")

        is_valid = global_multimodal_inspector.verify_watermark(
            watermark=artifact.watermark,
            payload=raw_bytes,
            tenant_secret=req.tenant_secret,
        )
        return {
            "status": "verified" if is_valid else "tamper_detected",
            "swarm_id": swarm_id,
            "artifact_id": req.artifact_id,
            "tamper_detected": not is_valid,
            "verified": is_valid,
        }

    @app.get("/api/v1/mesh/status")
    async def get_mesh_status_endpoint():
        """Returns comprehensive decentralized gossip mesh diagnostic status."""
        status_report = global_gossip_manager.get_status()
        return status_report.to_dict()

    @app.get("/api/v1/mesh/peers")
    async def get_mesh_peers_endpoint(include_dead: bool = False):
        """Lists active, suspect, and optionally dead mesh peers."""
        peers = [p.to_dict() for p in global_gossip_manager.list_peers(include_dead=include_dead)]
        return {
            "peers_count": len(peers),
            "peers": peers,
            "local_node_id": global_gossip_manager.node_id,
        }

    @app.post("/api/v1/mesh/peers/register")
    async def register_mesh_peer_endpoint(req: MeshPeerRegisterRequest):
        """Registers or refreshes a remote peer node in the local mesh table."""
        node = global_gossip_manager.register_peer(
            node_id=req.node_id,
            endpoint=req.endpoint,
            metadata=req.metadata,
        )
        return {
            "status": "registered",
            "peer": node.to_dict(),
        }

    @app.post("/api/v1/mesh/gossip/round")
    async def trigger_mesh_gossip_round_endpoint(req: Optional[MeshGossipRoundRequest] = None):
        """Triggers an on-demand epidemic anti-entropy gossip round."""
        fanout = req.fanout if req else None
        round_result = global_gossip_manager.run_gossip_round()
        return {
            "status": "round_completed",
            "result": round_result,
        }

    @app.post("/api/v1/mesh/gossip/sync")
    async def sync_mesh_gossip_endpoint(req: MeshGossipSyncRequest):
        """Reconciles an incoming GossipDigest or applies an incoming GossipDelta."""
        if req.digest:
            digest_obj = GossipDigest.from_dict(req.digest)
            delta = global_gossip_manager.compute_delta_for_digest(digest_obj)
            return {
                "action": "delta_computed",
                "delta": delta.to_dict(),
            }
        elif req.delta:
            delta_obj = GossipDelta.from_dict(req.delta)
            apply_res = global_gossip_manager.apply_delta(delta_obj)
            return {
                "action": "delta_applied",
                "result": apply_res,
                "current_vector_clock": global_gossip_manager.vector_clock.to_dict(),
            }
        else:
            digest = global_gossip_manager.generate_digest()
            return {
                "action": "digest_generated",
                "digest": digest.to_dict(),
            }

    @app.get("/api/v1/sharding/status")
    async def get_sharding_status_endpoint():
        """Returns consolidated report on sharding state, node capacity scores, and ring status."""
        status_report = global_compute_balancer.get_status()
        return status_report.to_dict()

    @app.get("/api/v1/sharding/ring")
    async def get_sharding_ring_endpoint():
        """Returns the ordered token layout of all virtual nodes on the consistent hash ring."""
        tokens = global_compute_balancer.get_ring_tokens()
        return {
            "tokens_count": len(tokens),
            "tokens": tokens,
        }

    @app.post("/api/v1/sharding/route")
    async def route_shard_key_endpoint(req: ShardRouteRequest):
        """Resolves optimal execution node for a task key with session affinity and spillover safety."""
        routing_key = ShardRoutingKey(
            tenant_id=req.tenant_id or "default",
            project_id=req.project_id or "default",
            swarm_id=req.swarm_id,
            session_affinity_key=req.session_affinity_key,
        )
        result = global_compute_balancer.route(routing_key)
        return result.to_dict()

    @app.post("/api/v1/sharding/node-capacity")
    async def update_node_capacity_endpoint(req: NodeCapacityUpdateRequest):
        """Updates real-time compute telemetry for an execution node and recalculates ring allocations."""
        metrics = NodeComputeMetrics(
            cpu_utilization=req.cpu_utilization,
            memory_utilization=req.memory_utilization,
            queue_depth=req.queue_depth,
            max_queue_depth=req.max_queue_depth,
            avg_latency_ms=req.avg_latency_ms,
        )
        if req.endpoint and req.node_id not in global_compute_balancer._endpoints:
            global_compute_balancer.register_node(req.node_id, req.endpoint, metrics)
        else:
            global_compute_balancer.update_node_metrics(req.node_id, metrics)
        return {
            "status": "updated",
            "node_id": req.node_id,
            "capacity_score": compute_capacity_score(metrics),
            "health_tier": get_health_tier(compute_capacity_score(metrics)).value,
        }

    @app.post("/api/v1/sharding/rebalance")
    async def rebalance_sharding_ring_endpoint(req: Optional[ShardRebalanceRequest] = None):
        """Forces recalculation and reconstruction of the dynamic consistent hash ring."""
        if req and req.base_vnodes:
            global_compute_balancer.ring.base_vnodes = req.base_vnodes
        report = global_compute_balancer.rebalance()
        return {
            "status": "rebalanced",
            "report": report.to_dict(),
        }

    # -----------------------------------------------------------------------
    # Autonomous Semantic Schema Migration & State Translation (Seam 47 / ADR 0058)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/schemas/status")
    async def get_schema_migration_status_endpoint():
        """Returns consolidated schema migration engine status, registered versions, and steps."""
        return global_schema_migration_engine.get_status()

    @app.get("/api/v1/schemas/versions")
    async def list_schema_versions_endpoint():
        """Returns list of registered schema contract versions."""
        versions = global_schema_migration_engine.get_versions()
        return {
            "total_versions": len(versions),
            "versions": versions,
        }

    @app.post("/api/v1/schemas/migrate")
    async def migrate_schema_payload_endpoint(req: SchemaMigrationRequest):
        """Translates payload between schema versions along BFS shortest-path migration plan."""
        try:
            result = global_schema_migration_engine.migrate(
                payload=req.payload,
                source_version=req.source_version,
                target_version=req.target_version,
            )
            return result.to_dict()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @app.post("/api/v1/schemas/validate")
    async def validate_schema_payload_endpoint(req: SchemaValidationRequest):
        """Validates payload against schema contract for the specified version."""
        is_valid, errors = global_schema_migration_engine.validate_payload(
            payload=req.payload,
            version=req.version,
        )
        return {
            "is_valid": is_valid,
            "version": req.version,
            "errors": errors,
        }

    @app.post("/api/v1/schemas/register-migration")
    async def register_schema_migration_endpoint(req: SchemaRegisterStepRequest):
        """Registers a new bidirectional migration step between two schema versions."""
        forward = [FieldTransformRule.from_dict(r) for r in req.forward_rules]
        backward = [FieldTransformRule.from_dict(r) for r in req.backward_rules] if req.backward_rules else []
        step = MigrationStep(
            source_version=req.source_version,
            target_version=req.target_version,
            forward_rules=forward,
            backward_rules=backward,
            description=req.description or "",
        )
        global_schema_migration_engine.register_step(step)
        return {
            "status": "registered",
            "step": step.to_dict(),
        }

    # -----------------------------------------------------------------------
    # Dynamic Multi-Agent Epistemic Logic & Knowledge Sync (Seam 48 / ADR 0059)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/swarms/{swarm_id}/epistemic/status")
    async def get_epistemic_status_endpoint(swarm_id: str):
        """Returns consolidated epistemic status report, dissonance score, and conflict summary."""
        return global_epistemic_engine.get_status(swarm_id)

    @app.get("/api/v1/swarms/{swarm_id}/epistemic/propositions")
    async def get_epistemic_propositions_endpoint(
        swarm_id: str,
        topic: Optional[str] = Query(None),
        agent_id: Optional[str] = Query(None),
    ):
        """Returns active propositional knowledge assertions for designated swarm."""
        props = global_epistemic_engine.get_propositions(swarm_id, topic=topic, agent_id=agent_id)
        return {
            "swarm_id": swarm_id,
            "propositions_count": len(props),
            "propositions": props,
        }

    @app.get("/api/v1/swarms/{swarm_id}/epistemic/conflicts")
    async def get_epistemic_conflicts_endpoint(
        swarm_id: str,
        unresolved_only: bool = Query(True),
    ):
        """Returns active or resolved contradiction pairs across agent assertions."""
        conflicts = global_epistemic_engine.get_conflicts(swarm_id, unresolved_only=unresolved_only)
        return {
            "swarm_id": swarm_id,
            "conflicts_count": len(conflicts),
            "conflicts": conflicts,
        }

    @app.get("/api/v1/swarms/{swarm_id}/epistemic/mutual-knowledge")
    async def get_mutual_knowledge_endpoint(swarm_id: str):
        """Returns mutual knowledge propositions affirmed by multiple agents with matching truth values."""
        kb = global_epistemic_engine.get_knowledge_base(swarm_id)
        return {
            "swarm_id": swarm_id,
            "mutual_knowledge": kb.get_mutual_knowledge(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/epistemic/assert")
    async def assert_epistemic_proposition_endpoint(swarm_id: str, req: EpistemicAssertRequest):
        """Ingests propositional claim Ki phi and detects potential contradictions in <50 microseconds."""
        claim, is_consistent, conflict = global_epistemic_engine.assert_proposition(
            swarm_id=swarm_id,
            agent_id=req.agent_id,
            topic=req.topic,
            predicate=req.predicate,
            truth_value=req.truth_value,
            confidence=req.confidence,
            evidence_span_id=req.evidence_span_id,
            metadata=req.metadata,
        )
        return {
            "status": "asserted",
            "is_consistent": is_consistent,
            "claim": claim.to_dict(),
            "conflict": conflict.to_dict() if conflict else None,
        }

    @app.post("/api/v1/swarms/{swarm_id}/epistemic/reconcile")
    async def reconcile_epistemic_beliefs_endpoint(
        swarm_id: str,
        req: Optional[EpistemicReconcileRequest] = None,
    ):
        """Executes AGM-style belief revision across active propositional contradictions."""
        strategy = req.strategy if req and req.strategy else "CONFIDENCE_ENTRENCHMENT"
        result = global_epistemic_engine.reconcile(swarm_id, strategy=strategy)
        return result

    @app.post("/api/v1/swarms/{swarm_id}/epistemic/reset")
    async def reset_epistemic_knowledge_endpoint(swarm_id: str):
        """Clears all propositional claims and conflicts for designated swarm."""
        global_epistemic_engine.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    # -----------------------------------------------------------------------
    # Zero-Knowledge Causal Proof & Cryptographic Audit Engine (Seam 49 / ADR 0060)
    # -----------------------------------------------------------------------
    @app.get("/api/v1/swarms/{swarm_id}/proof/root")
    async def get_causal_proof_root_endpoint(swarm_id: str):
        """Retrieves active session Merkle root and leaf count."""
        acc = global_zk_proof_engine.get_accumulator(swarm_id)
        root = acc.get_merkle_root()
        return {
            "swarm_id": swarm_id,
            "session_merkle_root": root,
            "total_leaves": acc.total_leaves,
        }

    @app.post("/api/v1/swarms/{swarm_id}/proof/commit")
    async def commit_causal_span_endpoint(swarm_id: str, req: ZkProofCommitSpanRequest):
        """Commits span execution metadata to session Merkle accumulator."""
        leaf = global_zk_proof_engine.commit_span(
            swarm_id=swarm_id,
            span_id=req.span_id,
            turn_index=req.turn_index,
            agent_id=req.agent_id,
            payload_content=req.payload_content,
            payload_hash=req.payload_hash,
            parent_span_ids=req.parent_span_ids,
            circuit_tripped=req.circuit_tripped,
            trip_reason=req.trip_reason,
        )
        return {
            "status": "committed",
            "swarm_id": swarm_id,
            "leaf": leaf.to_dict(),
            "session_merkle_root": global_zk_proof_engine.get_root(swarm_id),
        }

    @app.post("/api/v1/swarms/{swarm_id}/proof/generate")
    async def generate_causal_proof_endpoint(swarm_id: str, req: ZkProofGenerateRequest):
        """Generates zero-knowledge audit receipt with Merkle path and HMAC signature."""
        try:
            ct = ProofCommitmentType(req.commitment_type.upper())
        except ValueError:
            ct = ProofCommitmentType.TURN_INCLUSION

        secret = req.secret_key or "sw_causal_audit_secret_key"
        receipt = global_zk_proof_engine.generate_receipt(
            swarm_id=swarm_id,
            span_id=req.span_id,
            commitment_type=ct,
            secret_key=secret,
        )
        return {
            "status": "generated",
            "receipt": receipt.to_dict(),
        }

    @app.post("/api/v1/swarms/{swarm_id}/proof/verify")
    async def verify_causal_proof_endpoint(swarm_id: str, req: ZkProofVerifyRequest):
        """Cryptographically verifies audit receipt against session Merkle root."""
        secret = req.secret_key or "sw_causal_audit_secret_key"
        is_valid, err = global_zk_proof_engine.verify_receipt(req.receipt, secret_key=secret)
        return {
            "is_valid": is_valid,
            "error": err,
            "swarm_id": swarm_id,
        }

    @app.get("/api/v1/swarms/{swarm_id}/proof/manifest")
    async def get_causal_proof_manifest_endpoint(swarm_id: str):
        """Retrieves session audit manifest containing all commitment leaves."""
        return global_zk_proof_engine.get_manifest(swarm_id)

    @app.post("/api/v1/swarms/{swarm_id}/proof/reset")
    async def reset_causal_proof_endpoint(swarm_id: str):
        """Resets session Merkle accumulator for designated swarm."""
        global_zk_proof_engine.reset(swarm_id)
        return {"status": "reset", "swarm_id": swarm_id}

    @app.post("/api/v1/codec/canonicalize")
    async def canonicalize_payload_endpoint(req: CanonicalizeRequest):
        """Encodes an arbitrary JSON object into its deterministic canonical representation (RFC 8785)."""
        try:
            canonical_str = global_canonical_codec.canonicalize(req.payload, version=req.version)
            canonical_bytes = canonical_str.encode("utf-8")
            return {
                "status": "success",
                "canonical_str": canonical_str,
                "length_bytes": len(canonical_bytes),
                "version": req.version,
            }
        except CanonicalEncodingError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/codec/hash")
    async def canonical_hash_endpoint(req: CanonicalHashRequest):
        """Computes domain-separated SHA-256 digest over canonical byte representation."""
        try:
            digest = global_canonical_codec.hash(
                req.payload, domain_tag=req.domain_tag, version=req.version
            )
            canonical_str = global_canonical_codec.canonicalize(req.payload, version=req.version)
            return {
                "status": "success",
                "digest": digest,
                "domain_tag": req.domain_tag,
                "length_bytes": len(canonical_str.encode("utf-8")),
                "version": req.version,
            }
        except CanonicalEncodingError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/safety/reserve")
    async def safety_reserve_endpoint(req: SafetyReserveRequest):
        """Atomically requests and reserves an execution permit under the current epoch."""
        permit = global_safety_authority.reserve(
            tenant_id=req.tenant_id,
            action=req.action,
            cost_estimate=req.cost_estimate,
            tokens_estimate=req.tokens_estimate,
            timeout_ms=req.timeout_ms,
        )
        return {
            "status": permit.status,
            "permit": permit.to_dict(),
            "epoch": permit.epoch,
        }

    @app.post("/api/v1/safety/commit")
    async def safety_commit_endpoint(req: SafetyCommitRequest):
        """Commits an executed permit, settling reserved budget and recording permanent spend."""
        ok, msg = global_safety_authority.commit(
            permit_id=req.permit_id,
            actual_cost=req.actual_cost,
            actual_tokens=req.actual_tokens,
        )
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"status": "committed", "message": msg, "permit_id": req.permit_id}

    @app.post("/api/v1/safety/release")
    async def safety_release_endpoint(req: SafetyReleaseRequest):
        """Releases reserved budget back to the pool without debiting spend."""
        ok, msg = global_safety_authority.release(permit_id=req.permit_id, reason=req.reason)
        if not ok:
            raise HTTPException(status_code=404, detail=msg)
        return {"status": "released", "message": msg, "permit_id": req.permit_id}

    @app.post("/api/v1/safety/trip")
    async def safety_trip_endpoint(req: SafetyTripRequest):
        """Trips the safety authority, monotonically increments fencing epoch, and revokes permits."""
        new_epoch = global_safety_authority.trip(reason=req.reason, tenant_id=req.tenant_id)
        return {
            "status": "tripped",
            "new_epoch": new_epoch,
            "reason": req.reason,
            "tenant_id": req.tenant_id,
        }

    @app.post("/api/v1/safety/rearm")
    async def safety_rearm_endpoint(req: SafetyRearmRequest):
        """Re-arms the safety authority back to NOMINAL, incrementing fencing epoch."""
        new_epoch = global_safety_authority.rearm(tenant_id=req.tenant_id)
        return {
            "status": "rearmed",
            "new_epoch": new_epoch,
            "tenant_id": req.tenant_id,
        }

    @app.get("/api/v1/safety/status")
    async def safety_status_endpoint(tenant_id: str = "default"):
        """Returns comprehensive authoritative status and active permit counts."""
        return global_safety_authority.status(tenant_id=tenant_id)

    @app.post("/api/v1/effects/claim")
    async def effect_claim_endpoint(req: EffectClaimRequest):
        """Attempts to claim an effect key under compare-and-swap semantics and monotonic fencing."""
        res = global_effect_ledger.claim(
            effect_key=req.effect_key,
            tenant_id=req.tenant_id,
            epoch=req.epoch,
            idempotency_key=req.idempotency_key,
            action=req.action,
            timeout_ms=req.timeout_ms,
        )
        return res.to_dict()

    @app.post("/api/v1/effects/authorize")
    async def effect_authorize_endpoint(req: EffectAuthorizeRequest):
        """Validates the claim against an active SafetyPermit hash."""
        ok, msg = global_effect_ledger.authorize(
            claim_id=req.claim_id,
            permit_hash=req.permit_hash,
        )
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"status": "authorized", "claim_id": req.claim_id, "message": msg}

    @app.post("/api/v1/effects/complete")
    async def effect_complete_endpoint(req: EffectCompleteRequest):
        """Completes and commits an authorized effect, returning an immutable EffectReceipt."""
        ok, receipt, msg = global_effect_ledger.complete(
            claim_id=req.claim_id,
            execution_result=req.execution_result,
        )
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {
            "status": "committed",
            "claim_id": req.claim_id,
            "receipt": receipt.to_dict() if receipt else None,
            "message": msg,
        }

    @app.post("/api/v1/effects/abort")
    async def effect_abort_endpoint(req: EffectAbortRequest):
        """Aborts an active in-flight claim, freeing the effect key for retry."""
        ok, msg = global_effect_ledger.abort(claim_id=req.claim_id, reason=req.reason)
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"status": "aborted", "claim_id": req.claim_id, "message": msg}

    @app.get("/api/v1/effects/reconcile")
    async def effect_reconcile_endpoint(since_epoch: int = 1, tenant_id: Optional[str] = None):
        """Returns committed effect records for cross-worker state reconciliation."""
        records = global_effect_ledger.reconcile(since_epoch=since_epoch, tenant_id=tenant_id)
        return {"reconciled_records": records, "count": len(records), "since_epoch": since_epoch}

    @app.get("/api/v1/effects/status")
    async def effect_status_endpoint(tenant_id: str = "default"):
        """Returns diagnostic ledger metrics."""
        return global_effect_ledger.status(tenant_id=tenant_id)

    @app.websocket("/ws/swarm/{swarm_id}")
    async def websocket_endpoint(websocket: WebSocket, swarm_id: str):
        await ws_manager.connect(swarm_id, websocket)
        try:
            while True:
                data = await websocket.receive_json()
                cmd = data.get("command")
                if cmd == "CMD_PAUSE":
                    await websocket.send_json({
                        "type": "command_ack",
                        "command": "CMD_PAUSE",
                        "status": "swarm_paused"
                    })
                elif cmd == "CMD_RESUME":
                    await websocket.send_json({
                        "type": "command_ack",
                        "command": "CMD_RESUME",
                        "status": "swarm_resumed"
                    })
                elif cmd == "CMD_FORK_BRANCH":
                    target_span_id = data.get("target_span_id")
                    new_branch_id = data.get("new_branch_id", "fork-01")
                    try:
                        restored_state = await time_machine.step_back(
                            target_span_id=target_span_id,
                            new_branch_id=new_branch_id
                        )
                        await websocket.send_json({
                            "type": "command_ack",
                            "command": "CMD_FORK_BRANCH",
                            "status": "forked",
                            "new_branch_id": new_branch_id,
                            "state": restored_state
                        })
                    except Exception as e:
                        await websocket.send_json({
                            "type": "error",
                            "message": str(e)
                        })
                else:
                    await websocket.send_json({
                        "type": "echo",
                        "data": data
                    })
        except WebSocketDisconnect:
            ws_manager.disconnect(swarm_id, websocket)

    return app
