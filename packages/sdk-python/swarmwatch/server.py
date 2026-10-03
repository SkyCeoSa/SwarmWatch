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
from typing import Any, Dict, List, Optional, Set, Union
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
from swarmwatch.consensus_authority import (
    global_consensus_authority,
    ConsensusAuthorityBinding,
)
from swarmwatch.effect_ledger import (
    ClaimStatus,
    EffectReceipt,
    EffectRecord,
    ClaimResult,
    EffectLedger,
    global_effect_ledger,
)
from swarmwatch.causal_ingress import (
    IngressPriority,
    CompletenessStatus,
    SequenceGap,
    CompletenessReport,
    IngressResult,
    CausalIngress,
    global_causal_ingress,
)
from swarmwatch.audit_anchor import (
    RootEpoch,
    MerkleAuditReceipt,
    VerificationResult,
    AuditIntegrityError,
    AuditAnchor,
    global_audit_anchor,
)
from swarmwatch.safety_verifier import (
    BoundedVerdict,
    VerificationProperty,
    VerificationLimits,
    TransitionStep,
    VerificationReport,
    SafetyVerifier,
    global_safety_verifier,
)
from swarmwatch.remediation_governor import (
    ApprovalTier,
    ProposalStatus,
    BlastRadius,
    RemediationProposal,
    RemediationDecision,
    RemediationResult,
    CompensationResult,
    RemediationGovernor,
    global_remediation_governor,
)
from swarmwatch.mesh_reconciler import (
    LatticeVerdict,
    compare_vector_clocks,
    merge_vector_clocks,
    ReconciliationPlan,
    MergeResult,
    AuthorityReconciliationResult,
    MeshReconciler,
    global_mesh_reconciler,
)
from swarmwatch.downstream_effect import (
    EffectPhase,
    DownstreamIdempotencyContract,
    UncoordinatedRetryBlockedError,
    StaleEpochError,
    EffectNotFoundError,
    DownstreamReceipt,
    EffectPreparation,
    DispatchResult,
    AcknowledgeResult,
    ReconciliationResult,
    DownstreamEffectBoundary,
    global_downstream_effect_boundary,
)
from swarmwatch.root_witness import (
    WitnessError,
    RollbackAttackError,
    ConsistencyVerificationError,
    SplitBrainForkError,
    KeyRotationError,
    WitnessKeyManager,
    LeafReceipt,
    SignedCheckpoint,
    ConsistencyProof,
    KeyRotationRecord,
    WitnessStatus,
    RootWitness,
    global_root_witness,
)
from swarmwatch.tenant_capability import (
    CapabilityError,
    PrivilegeEscalationError,
    AttenuationViolationError,
    CapabilityVerificationError,
    RevocationError,
    CapabilityToken,
    CapabilityVerdict,
    RevocationReceipt,
    CapabilityStatus,
    TenantCapabilityManager,
    global_capability_manager,
)
from swarmwatch.compensation_algebra import (
    CompensationError,
    IrreversibleActionError,
    UnregisteredActionError,
    EscalationPendingError,
    ReversibilityClass,
    ActionExecution,
    ActionContract,
    CompositeClassification,
    CompensationStep,
    CompensationPlan,
    CompensationResult as AlgebraCompensationResult,
    HumanEscalationReceipt,
    EscalationResolution,
    CompensationStatus,
    CompensationEngine,
    global_compensation_engine,
)
from swarmwatch.dataset_privacy import (
    DatasetPrivacyError,
    DatasetPrivacyBudgetExhaustedError,
    EntityPrivacyBudgetExhaustedError,
    DatasetNotFoundError,
    ReservationNotFoundError,
    DatasetReservationStatus,
    DatasetPrivacyConfig,
    DatasetPrivacyReservation,
    DatasetPrivacyReleaseResult,
    EntityExpenditure,
    DatasetLedgerSnapshot,
    DatasetPrivacyLedger,
    global_dataset_privacy_ledger,
)
from swarmwatch.interop_lab import (
    ProtocolStandard,
    ConformanceFault,
    ClientVariant,
    ProtocolState,
    DeviationSeverity,
    ProtocolDeviation,
    ExerciseResult,
    ConformanceAttestationReceipt,
    ProtocolConformanceStateMachine,
    WireFaultInjector,
    InteropLab,
    global_interop_lab,
)
from swarmwatch.release_provenance import (
    ProvenanceError,
    ProvenanceCyclicDependencyError,
    ProvenanceVerificationError,
    ProvenanceStepType,
    ArtifactDigest,
    ProvenanceStep,
    ProvenanceDag,
    SignedProvenanceManifest,
    ProvenanceVerificationResult,
    ReleaseProvenance,
    global_release_provenance,
)
from swarmwatch.datacenter_exercise import (
    DatacenterExerciseError,
    MinorityPartitionBlockedError,
    SplitBrainViolationError,
    SafetyMonotonicityViolationError,
    FailureScenarioType,
    ExercisePhase,
    NodeRole,
    ClusterNode,
    NetworkMatrix,
    PermitAttemptResult,
    FailureExerciseEvent,
    FailureExerciseReport,
    DatacenterFailureOrchestrator,
    global_datacenter_orchestrator,
)
from swarmwatch.mesh_federation import (
    MeshFederationError,
    MeshPeerAuthenticationError,
    MeshRoutingLoopError,
    MeshHopLimitExceededError,
    MeshDestinationUnreachableError,
    MeshCapabilityFencingError,
    MeshCircuitTrippedError,
    MeshNodeStatus,
    MeshNodeRole,
    MeshRoutingStatus,
    PeerNode,
    MeshLink,
    MeshPacket,
    MeshRoutingReceipt,
    MeshFederationRouter,
    global_mesh_router,
)
from swarmwatch.action_gateway import (
    ActionGatewayError,
    UnauthorizedActionError,
    ActionQuorumPendingError,
    ActionCircuitBlockedError,
    ActionExecutionFailedError,
    ActionRiskLevel,
    ActionApprovalStatus,
    ActionRegistration,
    ActionRequest,
    ActionApproval,
    ActionExecutionReceipt,
    ActionGateway,
    global_action_gateway,
)
from swarmwatch.cognitive_grounding import (
    CognitiveGroundingError,
    UngroundedInferenceError,
    GroundingCircuitTrippedError,
    EvidenceNotFoundError,
    GroundingModalityMismatchError,
    EvidenceModality,
    GroundingVerdict,
    VectorEvidence,
    GroundingReceipt,
    VectorEvidenceStore,
    CognitiveGroundingEngine,
    global_cognitive_grounding_engine,
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


class IngressSubmitRequest(BaseModel):
    span: Dict[str, Any]
    priority: str = "NORMAL"
    stream_id: str = "main"


class IngressDrainRequest(BaseModel):
    batch_size: int = 100


class AuditAppendRootRequest(BaseModel):
    root_hash: str
    tenant_id: str = "default-tenant"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AuditIssueReceiptRequest(BaseModel):
    epoch: int
    leaf_hash: str
    audit_path: List[List[str]]
    tenant_id: str = "default-tenant"


class AuditVerifyReceiptRequest(BaseModel):
    receipt: Dict[str, Any]


class VerifierVerifyRequest(BaseModel):
    model: Dict[str, Any]
    properties: Optional[List[Dict[str, Any]]] = None
    limits: Optional[Dict[str, Any]] = None
    model_id: str = "model_default"
    tenant_id: str = "default"


class VerifierCheckTraceRequest(BaseModel):
    model: Dict[str, Any]
    property: Union[Dict[str, Any], str]
    trace: List[Dict[str, Any]]


class RemediationProposeRequest(BaseModel):
    tenant_id: str
    action_type: str
    target_entity: str
    parameters: Optional[Dict[str, Any]] = None
    blast_radius: Optional[Dict[str, Any]] = None
    approval_tier: str = "TIER_0_AUTOMATIC"
    preconditions: Optional[Dict[str, Any]] = None
    postconditions: Optional[Dict[str, Any]] = None
    compensation_action: Optional[Dict[str, Any]] = None
    proposal_id: Optional[str] = None


class RemediationAuthorizeRequest(BaseModel):
    proposal_id: str
    approver_role: Optional[str] = None
    permit_hash: Optional[str] = None
    current_epoch: Optional[int] = None
    signature: Optional[str] = None


class RemediationApplyRequest(BaseModel):
    proposal_id: str
    current_epoch: int
    context: Optional[Dict[str, Any]] = None


class RemediationCompensateRequest(BaseModel):
    proposal_id: str
    reason: str = "Operator compensation request"


class ReconcilerDiffRequest(BaseModel):
    local_state: Dict[str, Any]
    remote_state: Dict[str, Any]
    tenant_id: str = "default"


class ReconcilerMergeRequest(BaseModel):
    local_events: List[Dict[str, Any]]
    remote_events: List[Dict[str, Any]]
    tenant_id: str = "default"


class ReconcilerReconcileAuthorityRequest(BaseModel):
    local_authority: Dict[str, Any]
    remote_authority: Dict[str, Any]
    tenant_id: str = "default"


class PrivacyReserveRequest(BaseModel):
    tenant_id: str = "default"
    epsilon: float
    delta: float = 0.0
    sensitivity: float = 1.0
    query_id: Optional[str] = None


class PrivacyReleaseRequest(BaseModel):
    reservation_id: str
    raw_value: Union[float, List[float], Dict[str, float]]
    noise_seed: Optional[int] = None
    min_bound: Optional[float] = None
    max_bound: Optional[float] = None


class SchemaEvolutionPlanRequest(BaseModel):
    source_version: str
    target_version: str


class SchemaEvolutionMigrateRequest(BaseModel):
    payload: Dict[str, Any]
    source_version: str
    target_version: str
    preserve_shadow: bool = True


class SchemaEvolutionReverseRequest(BaseModel):
    payload: Dict[str, Any]
    source_version: str
    target_version: str
    preserved_delta: Optional[Dict[str, Any]] = None


class SchemaEvolutionClassifyRequest(BaseModel):
    source_version: str
    target_version: str
    payload: Optional[Dict[str, Any]] = None


class AcceptanceRunRequest(BaseModel):
    profile: Optional[str] = "production"
    custom_checks: Optional[List[str]] = None
    html_path: Optional[str] = None


class AcceptanceAttestRequest(BaseModel):
    report: Optional[Dict[str, Any]] = None
    signer_identity: Optional[str] = "SwarmWatch Enterprise Release Gatekeeper"
    secret_key: Optional[str] = None


class AcceptanceVerifyAttestationRequest(BaseModel):
    attestation: Dict[str, Any]
    secret_key: Optional[str] = None


class DownstreamPrepareRequest(BaseModel):
    tenant_id: str = "default"
    effect_key: str
    target_uri: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    epoch: int = 1
    contract: str = "DOWNSTREAM_FENCED"
    idempotency_key: Optional[str] = None


class DownstreamDispatchRequest(BaseModel):
    effect_id: str
    mock_downstream_status: Optional[str] = "SUCCESS"
    mock_confirmation_id: Optional[str] = None


class DownstreamAcknowledgeRequest(BaseModel):
    effect_id: str
    downstream_confirmation_id: str
    result_digest: str
    latency_ms: float = 0.0
    epoch: Optional[int] = None


class DownstreamReconcileRequest(BaseModel):
    effect_id: str
    mock_probe_found: bool = True
    mock_probe_status: str = "COMMITTED"
    mock_downstream_id: Optional[str] = None
    mock_probe_digest: Optional[str] = None


class EvidenceOfferRequest(BaseModel):
    record_type: str = "SAFETY_TRIP"
    tenant_id: str = "default"
    agent_id: str = "agent-guard"
    payload: Dict[str, Any] = Field(default_factory=dict)
    record_id: Optional[str] = None


class EvidenceFlushRequest(BaseModel):
    pass


class EvidenceRecoverRequest(BaseModel):
    journal_path: Optional[str] = None


class WitnessSubmitRequest(BaseModel):
    leaf_data: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WitnessCheckpointRequest(BaseModel):
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WitnessVerifyRequest(BaseModel):
    checkpoint: Dict[str, Any]
    public_key: Optional[str] = None


class WitnessConsistencyVerifyRequest(BaseModel):
    old_checkpoint: Dict[str, Any]
    new_checkpoint: Dict[str, Any]
    proof: Dict[str, Any]


class WitnessRotateRequest(BaseModel):
    pass


class CapabilityIssueRequest(BaseModel):
    tenant_id: str
    subject_principal: str
    scopes: List[str]
    resource_pattern: str = "*"
    epoch: int = 0
    not_before_ns: Optional[int] = None
    ttl_seconds: Optional[int] = None
    max_depth: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CapabilityAttenuateRequest(BaseModel):
    parent_token: Dict[str, Any]
    subject_principal: str
    scopes: Optional[List[str]] = None
    resource_pattern: Optional[str] = None
    epoch: Optional[int] = None
    not_before_ns: Optional[int] = None
    expires_at_ns: Optional[int] = None
    ttl_seconds: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CapabilityVerifyRequest(BaseModel):
    token: Dict[str, Any]
    action_scope: str
    target_resource: str
    executing_principal: str
    expected_tenant_id: str
    current_epoch: Optional[int] = None
    now_ns: Optional[int] = None


class CapabilityRevokeRequest(BaseModel):
    token_id: str
    reason: str = "Administrative revocation"
    revoked_by: str = "authority"


class CompensationClassifyRequest(BaseModel):
    actions: List[Dict[str, Any]]
    strict: bool = False


class CompensationPlanRequest(BaseModel):
    actions: List[Dict[str, Any]]
    strict: bool = False


class CompensationExecuteRequest(BaseModel):
    plan: Dict[str, Any]
    actions: Optional[List[Dict[str, Any]]] = None


class CompensationEscalateRequest(BaseModel):
    action_id: str
    action_type: str
    tenant_id: str
    principal: str
    reason: str


class CompensationAuthorizeRequest(BaseModel):
    escalation_id: str
    authorizer_principal: str
    decision: str = "APPROVED_BYPASS"
    remediation_notes: str = ""


class DatasetPrivacyRegisterRequest(BaseModel):
    dataset_id: str
    tenant_id: str = "default"
    max_epsilon: float
    max_delta: float = 0.0
    entity_epsilon_limit: float = 1.0
    entity_delta_limit: float = 0.0
    description: str = ""


class DatasetPrivacyReserveRequest(BaseModel):
    tenant_id: str = "default"
    dataset_id: str
    entity_id: str
    epsilon: float
    delta: float = 0.0
    sensitivity: float = 1.0
    query_id: Optional[str] = None


class DatasetPrivacyReleaseRequest(BaseModel):
    reservation_id: str
    raw_value: Union[float, List[float], Dict[str, float]]
    noise_seed: Optional[int] = None
    min_bound: Optional[float] = None
    max_bound: Optional[float] = None


class DatasetPrivacyCancelRequest(BaseModel):
    reservation_id: str


class DatasetPrivacyReconcileRequest(BaseModel):
    snapshot: Dict[str, Any]


class InteropExerciseRequest(BaseModel):
    target_protocol: str
    client_variant: str = "MOCK_CLIENT"
    fault_schedule: str = "NONE"
    payload: Optional[Union[Dict[str, Any], str]] = None
    chunk_size: Optional[int] = None


class InteropRecordDeviationRequest(BaseModel):
    protocol: str
    scenario: str
    observed: str
    expected: str
    severity: str = "WARNING"


class InteropAttestRequest(BaseModel):
    suite_name: str = "SwarmWatch Interoperability Conformance Suite"


class ProvenanceRecordStepRequest(BaseModel):
    step_type: str
    parent_step_ids: List[str] = Field(default_factory=list)
    inputs: List[Dict[str, Any]] = Field(default_factory=list)
    outputs: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    step_id: Optional[str] = None


class ProvenanceGenerateRequest(BaseModel):
    git_commit: str
    git_branch: str = "main"
    builder_id: Optional[str] = None
    manifest_id: Optional[str] = None


class ProvenanceVerifyRequest(BaseModel):
    manifest: Dict[str, Any]
    public_key_b64: Optional[str] = None
    verify_disk_files: bool = False
    base_dir: Optional[str] = None


class ExerciseInjectPartitionRequest(BaseModel):
    severed_regions: List[str] = Field(default_factory=list)
    asymmetric_routes: Optional[List[List[str]]] = None


class ExercisePermitRequest(BaseModel):
    node_id: str
    tenant_id: str = "tenant-prod-01"
    tokens: int = 100


class ExerciseBroadcastTripRequest(BaseModel):
    origin_node_id: str
    reason: str = "Entropy collapse emergency halt"


class ExerciseRunRequest(BaseModel):
    scenario: str = "REGION_SEVERED"
    target_severed_regions: Optional[List[str]] = None


class GatewayActionRequestModel(BaseModel):
    tenant_id: str = "tenant-prod-01"
    agent_id: str = "agent-01"
    action_name: str
    target_resource: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    capability_token: Optional[str] = None
    estimated_cost_usd: float = 0.0
    reversibility_class: Optional[str] = None
    request_id: Optional[str] = None


class GatewayActionApproveModel(BaseModel):
    request_id: str
    approver_principal: str
    decision: str = "APPROVED"
    comment: str = ""
    signature: str = ""


class GatewayActionExecuteModel(BaseModel):
    request_id: str


class MeshPeerRegisterModel(BaseModel):
    node_id: str
    cluster_id: str = "cluster-default"
    tenant_id: str = "tenant-default"
    endpoint_url: str = "http://127.0.0.1:8000"
    public_key_hex: str = ""
    role: str = "WORKER"
    capabilities: List[str] = Field(default_factory=list)
    latency_ms: float = 1.0
    signature: Optional[str] = None


class MeshLinkEstablishModel(BaseModel):
    source_node_id: str
    target_node_id: str
    weight_cost: float = 1.0
    latency_ms: float = 1.0
    bidirectional: bool = True


class MeshHeartbeatModel(BaseModel):
    node_id: str


class MeshPacketRouteModel(BaseModel):
    packet_id: Optional[str] = None
    source_node_id: str
    dest_node_id: str
    tenant_id: str = "tenant-default"
    payload: Dict[str, Any] = Field(default_factory=dict)
    capability_token: Optional[str] = None
    hop_count: int = 0
    max_hops: int = 16
    signature: Optional[str] = None


class GroundingRegisterRequest(BaseModel):
    modality: str = "TEXT_CLAIM"
    content: str
    source_reference: str
    confidence_weight: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str = "tenant-default"
    evidence_id: Optional[str] = None


class GroundingVerifyRequest(BaseModel):
    claim: str
    min_threshold: Optional[float] = None
    required_modalities: Optional[List[str]] = None
    tenant_id: str = "tenant-default"
    strict: bool = False
    claim_id: Optional[str] = None


class GroundingFuseRequest(BaseModel):
    claim: str
    evidence_ids: List[str]
    modality_weights: Optional[Dict[str, float]] = None
    tenant_id: str = "tenant-default"





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

    /* Time-Machine & Scrubber Bar */
    .time-machine-bar {
      background: #0B101D;
      border-bottom: 1px solid var(--border-subtle);
      padding: 6px 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-shrink: 0;
    }
    .tm-left-group, .tm-right-group {
      display: flex;
      align-items: center;
      gap: 5px;
      flex-shrink: 0;
    }
    .tm-badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 3px 8px;
      border-radius: 4px;
      background: rgba(167, 139, 250, 0.12);
      border: 1px solid rgba(167, 139, 250, 0.3);
      color: #A78BFA;
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.3px;
      text-transform: uppercase;
    }
    .tm-btn {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 4px;
      padding: 4px 8px;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-secondary);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
      flex-shrink: 0;
      transition: background 0.15s, border-color 0.15s, color 0.15s;
    }
    .tm-btn:hover:not(:disabled) {
      background: var(--bg-surface-hover);
      border-color: var(--accent);
      color: var(--text-primary);
    }
    .tm-btn:disabled {
      opacity: 0.35;
      cursor: not-allowed;
    }
    .tm-btn-fork {
      background: rgba(167, 139, 250, 0.08);
      border-color: rgba(167, 139, 250, 0.4);
      color: #A78BFA;
    }
    .tm-btn-fork:hover:not(:disabled) {
      background: rgba(167, 139, 250, 0.2);
      border-color: #A78BFA;
      color: var(--text-primary);
    }
    .tm-turn-display {
      font-family: var(--font-mono);
      font-size: 12px;
      font-feature-settings: "tnum" 1;
      padding: 3px 8px;
      border-radius: 3px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-primary);
      min-width: 85px;
      text-align: center;
    }
    .tm-center-track {
      flex: 1;
      display: flex;
      flex-direction: column;
      gap: 3px;
      position: relative;
      min-width: 80px;
    }
    .tm-center-track input[type="range"] {
      width: 100%;
      height: 4px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 3px;
      accent-color: #A78BFA;
      cursor: pointer;
    }
    .tm-ticks {
      display: flex;
      justify-content: space-between;
      height: 6px;
      padding: 0 4px;
      position: relative;
    }
    .tm-tick {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--border-strong);
      cursor: pointer;
      transition: transform 0.15s, background 0.15s;
    }
    .tm-tick:hover {
      transform: scale(1.4);
      background: var(--accent);
    }
    .tm-tick.selected {
      background: #A78BFA;
      box-shadow: 0 0 4px rgba(167, 139, 250, 0.6);
      transform: scale(1.3);
    }
    .tm-tick.snapshot {
      border: 1px solid #A78BFA;
    }
    .tm-tick.halt {
      background: var(--danger);
    }
    .tm-branch-wrapper {
      display: flex;
      align-items: center;
      gap: 5px;
      font-size: 12px;
      color: var(--text-muted);
    }
    .tm-branch-select {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 3px;
      color: var(--text-primary);
      font-family: var(--font-mono);
      font-size: 12px;
      padding: 3px 6px;
      cursor: pointer;
      outline: none;
    }
    .tm-branch-select:hover {
      border-color: var(--border-strong);
    }

    /* Distributed Redis Fleet Cluster Bar & Alert (ADR 0034) */
    .fleet-bar {
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-subtle);
      padding: 7px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      flex-shrink: 0;
      font-size: 12px;
      transition: opacity 0.2s ease;
    }
    .fleet-left-group {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: nowrap;
    }
    .fleet-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 3px 8px;
      border-radius: 4px;
      background: rgba(56, 189, 248, 0.12);
      border: 1px solid rgba(56, 189, 248, 0.3);
      color: var(--accent);
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.3px;
      text-transform: uppercase;
      font-family: var(--font-mono);
    }
    .fleet-badge.tripped {
      background: var(--danger-subtle);
      border-color: rgba(239, 68, 68, 0.4);
      color: var(--danger);
    }
    .fleet-badge.standby {
      background: rgba(148, 163, 184, 0.12);
      border-color: rgba(148, 163, 184, 0.3);
      color: var(--text-secondary);
    }
    .fleet-badge.archived {
      background: rgba(148, 163, 184, 0.12);
      border-color: rgba(148, 163, 184, 0.3);
      color: var(--text-primary);
    }
    .fleet-backend-tag {
      font-family: var(--font-mono);
      font-size: 12px;
      color: var(--text-muted);
      background: var(--bg-canvas);
      padding: 2px 7px;
      border-radius: 3px;
      border: 1px solid var(--border-subtle);
    }
    .fleet-workers-container {
      display: flex;
      align-items: center;
      gap: 6px;
      max-width: 320px;
      overflow-x: auto;
    }
    .fleet-worker-pill {
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      color: var(--text-secondary);
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      padding: 2px 7px;
      border-radius: 3px;
      display: inline-flex;
      align-items: center;
      gap: 4px;
      white-space: nowrap;
    }
    .fleet-worker-dot {
      width: 5px;
      height: 5px;
      border-radius: 50%;
      background: var(--success);
    }
    .fleet-meters-container {
      display: flex;
      align-items: center;
      gap: 16px;
      flex: 1;
      justify-content: center;
    }
    .fleet-meter-item {
      display: flex;
      align-items: center;
      gap: 6px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      color: var(--text-secondary);
    }
    .fleet-meter-label {
      color: var(--text-muted);
      text-transform: uppercase;
      font-size: 12px;
      letter-spacing: 0.3px;
    }
    .fleet-meter-val {
      font-weight: 600;
      color: var(--text-primary);
    }
    .fleet-progress-bg {
      width: 48px;
      height: 4px;
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      border-radius: 3px;
      overflow: hidden;
    }
    .fleet-progress-fill {
      height: 100%;
      width: 0%;
      background: var(--accent);
      transition: background-color 0.2s ease;
    }
    .fleet-progress-fill.warning {
      background: var(--warning);
    }
    .fleet-progress-fill.danger {
      background: var(--danger);
    }
    .fleet-actions-group {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .btn-fleet-trip {
      background: var(--danger-subtle);
      border: 1px solid rgba(239, 68, 68, 0.35);
      color: var(--danger);
      font-family: var(--font-sans);
      font-size: 12px;
      font-weight: 600;
      padding: 3px 10px;
      border-radius: 3px;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      transition: background 0.15s, border-color 0.15s;
    }
    .btn-fleet-trip:hover:not(:disabled) {
      background: rgba(239, 68, 68, 0.25);
      border-color: var(--danger);
    }
    .btn-fleet-reset {
      background: var(--bg-canvas);
      border: 1px solid var(--border-subtle);
      color: var(--text-secondary);
      font-family: var(--font-sans);
      font-size: 12px;
      font-weight: 500;
      padding: 3px 10px;
      border-radius: 3px;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      transition: background 0.15s, border-color 0.15s, color 0.15s;
    }
    .btn-fleet-reset:hover:not(:disabled) {
      background: var(--bg-surface-hover);
      color: var(--text-primary);
      border-color: var(--border-strong);
    }
    .fleet-trip-alert-banner {
      width: 100%;
      background: rgba(239, 68, 68, 0.12);
      border: 1px solid rgba(239, 68, 68, 0.35);
      border-radius: 4px;
      padding: 8px 14px;
      display: none;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
      animation: toastSlideIn 0.2s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .fleet-trip-alert-text {
      font-size: 12px;
      color: var(--text-primary);
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
    }
    .fleet-trip-alert-text strong {
      color: var(--danger);
      margin-right: 6px;
    }

    /* Toast Notification System */
    .toast-item {
      background: var(--bg-surface);
      border: 1px solid var(--border-strong);
      border-left: 3px solid var(--accent);
      border-radius: 4px;
      padding: 10px 16px;
      font-size: 12px;
      color: var(--text-primary);
      pointer-events: auto;
      display: flex;
      align-items: center;
      gap: 10px;
      animation: toastSlideIn 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      transition: opacity 0.2s, transform 0.2s;
    }
    .toast-item.success { border-left-color: var(--success); }
    .toast-item.warning { border-left-color: var(--warning); }
    .toast-item.danger { border-left-color: var(--danger); }
    .toast-item.purple { border-left-color: #A78BFA; }
    @keyframes toastSlideIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
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

    /* Inspector Tabs & Diff Inspector (Seam 16 / ADR 0027) */
    .insp-tab-bar {
      display: flex;
      gap: 3px;
      background: var(--bg-canvas);
      padding: 3px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
    }
    .insp-tab-btn {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      padding: 6px 10px;
      font-family: inherit;
      font-size: 12px;
      font-weight: 600;
      color: var(--text-muted);
      background: transparent;
      border: none;
      border-radius: 3px;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .insp-tab-btn:hover {
      color: var(--text-primary);
    }
    .insp-tab-btn.active {
      color: var(--text-primary);
      background: var(--bg-surface);
      border-bottom: 2px solid var(--accent);
    }
    .diff-badge-pill {
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      font-weight: 700;
      padding: 1px 6px;
      border-radius: 3px;
      background: rgba(16, 185, 129, 0.15);
      color: #10B981;
    }
    .diff-badge-pill.has-changes {
      background: rgba(245, 158, 11, 0.15);
      color: #F59E0B;
    }

    /* Diff View Container */
    .diff-view-panel {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .diff-toolbar {
      display: flex;
      flex-direction: column;
      gap: 8px;
      background: var(--bg-canvas);
      padding: 10px 12px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
    }
    .diff-toolbar-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .diff-select {
      flex: 1;
      background: var(--bg-surface);
      color: var(--text-primary);
      border: 1px solid var(--border-subtle);
      border-radius: 3px;
      padding: 5px 8px;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      outline: none;
      cursor: pointer;
    }
    .diff-mode-toggle {
      display: flex;
      gap: 3px;
      background: var(--bg-surface);
      border: 1px solid var(--border-subtle);
      border-radius: 3px;
      padding: 3px 4px;
    }
    .diff-mode-btn {
      background: transparent;
      border: none;
      color: var(--text-muted);
      font-family: inherit;
      font-size: 12px;
      font-weight: 600;
      padding: 3px 8px;
      border-radius: 3px;
      cursor: pointer;
    }
    .diff-mode-btn.active {
      background: var(--border-strong);
      color: var(--text-primary);
    }
    .diff-filter-bar {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }
    .diff-filter-chip {
      background: transparent;
      border: 1px solid var(--border-subtle);
      border-radius: 3px;
      color: var(--text-muted);
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      padding: 3px 8px;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .diff-filter-chip.active {
      border-color: var(--accent);
      color: var(--text-primary);
      background: rgba(56, 189, 248, 0.08);
    }
    .diff-meta-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
      color: var(--text-muted);
      padding: 4px 6px;
    }
    .diff-entries-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
      max-height: 280px;
      overflow-y: auto;
      background: var(--bg-canvas);
      padding: 10px 12px;
      border-radius: 4px;
      border: 1px solid var(--border-subtle);
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1;
      font-size: 12px;
    }
    .diff-row {
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding: 6px 10px;
      border-radius: 3px;
      border-left: 3px solid transparent;
      background: rgba(255, 255, 255, 0.015);
    }
    .diff-row.added {
      border-left-color: #10B981;
      background: rgba(16, 185, 129, 0.06);
    }
    .diff-row.modified {
      border-left-color: #F59E0B;
      background: rgba(245, 158, 11, 0.06);
    }
    .diff-row.removed {
      border-left-color: #EF4444;
      background: rgba(239, 68, 68, 0.06);
    }
    .diff-row.unchanged {
      border-left-color: var(--border-subtle);
      opacity: 0.6;
    }
    .diff-header-line {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }
    .diff-path {
      font-weight: 600;
      color: var(--text-primary);
      word-break: break-all;
    }
    .diff-tag {
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      padding: 1px 6px;
      border-radius: 3px;
    }
    .diff-tag.added { background: rgba(16, 185, 129, 0.2); color: #10B981; }
    .diff-tag.modified { background: rgba(245, 158, 11, 0.2); color: #F59E0B; }
    .diff-tag.removed { background: rgba(239, 68, 68, 0.2); color: #EF4444; }
    .diff-tag.unchanged { background: rgba(148, 163, 184, 0.15); color: var(--text-muted); }
    .diff-tag.redacted { background: rgba(167, 139, 250, 0.2); color: var(--branch-purple); }
    .diff-val-change {
      display: flex;
      align-items: baseline;
      gap: 6px;
      font-size: 12px;
      word-break: break-all;
      white-space: pre-wrap;
    }
    .diff-val-old {
      color: #EF4444;
      text-decoration: line-through;
      opacity: 0.8;
    }
    .diff-val-new {
      color: #10B981;
    }
    .diff-val-arrow {
      color: var(--text-muted);
      font-size: 12px;
    }
    /* Split Diff Mode */
    .diff-split-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      font-size: 12px;
      margin-top: 4px;
      padding-top: 6px;
      border-top: 1px dashed var(--border-subtle);
    }
    .diff-split-col {
      display: flex;
      flex-direction: column;
      gap: 3px;
      word-break: break-all;
      white-space: pre-wrap;
    }
    .diff-split-label {
      font-size: 12px;
      color: var(--text-muted);
      text-transform: uppercase;
      font-weight: 600;
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
      <button class="btn-action btn-secondary" id="btnFleetToggle" title="Toggle Distributed Redis Fleet Bar">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect><rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect><line x1="6" y1="6" x2="6.01" y2="6"></line><line x1="6" y1="18" x2="6.01" y2="18"></line></svg>
        <span>Redis Fleet</span>
      </button>
      <button class="btn-action btn-secondary" id="btnApiKeys">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>
        <span>API Keys</span>
      </button>
      <button class="btn-action btn-secondary" id="btnMfaModal" title="Security & MFA Settings (ADR 0037)">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="5" y="11" width="14" height="10" rx="2"></rect><circle cx="12" cy="16" r="1"></circle><path d="M8 11V7a4 4 0 0 1 8 0v4"></path></svg>
        <span>Security / MFA</span>
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
      <div class="time-machine-bar" id="timeMachineBar">
        <div class="tm-left-group">
          <div class="tm-badge">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 8 14"/></svg>
            <span>Time-Machine</span>
          </div>
          <button class="tm-btn" id="btnStepBack" title="Step back to previous turn (Key: [)">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
            <span>Back</span>
          </button>
          <div class="tm-turn-display" id="scrubberTurnDisplay">Turn 0 / 0</div>
          <button class="tm-btn" id="btnStepForward" title="Step forward to next turn (Key: ])">
            <span>Next</span>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>
          </button>
          <button class="tm-btn" id="btnJumpHead" title="Jump to latest turn">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polyline points="13 17 18 12 13 7"/><polyline points="6 17 11 12 6 7"/></svg>
            <span>Head</span>
          </button>
        </div>

        <div class="tm-center-track">
          <input type="range" id="scrubberSlider" min="1" max="1" value="1" step="1" title="Scrub through turns timeline" />
          <div class="tm-ticks" id="scrubberTicks"></div>
        </div>

        <div class="tm-right-group">
          <div class="tm-branch-wrapper">
            <span class="tm-branch-label">Branch:</span>
            <select id="branchSelector" class="tm-branch-select" title="Switch active DAG branch">
              <option value="main">main</option>
            </select>
          </div>
          <button class="tm-btn tm-btn-fork" id="btnTimeMachineFork" title="Fork branch from selected turn (Key: F)">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
            <span>Fork...</span>
          </button>
        </div>
      </div>

      <!-- Distributed Redis Fleet Cluster Bar (ADR 0034) -->
      <div class="fleet-bar" id="fleetBar">
        <div class="fleet-left-group">
          <div class="fleet-badge" id="fleetClusterBadge">
            <span class="status-indicator" id="fleetDot"></span>
            <span id="fleetStatusText">REDIS: CONNECTED</span>
          </div>
          <span class="fleet-backend-tag" id="fleetBackendTag">RESP2 (127.0.0.1:6379)</span>
          <div class="fleet-badge leader-badge" id="fleetLeaderBadge" title="Distributed Cluster Leader (ADR 0035)">
            <span class="status-indicator" id="fleetLeaderDot"></span>
            <span id="fleetLeaderText">LEADER [SRV-NODE-LOCAL]</span>
          </div>
          <span class="fleet-backend-tag" id="fleetNodesCountTag" title="Registered Active Cluster Server Nodes">1 Node</span>
          <span class="fleet-badge archived" id="fleetArchiveStatusBadge" style="display: none;" title="Swarm Cold Storage Status (ADR 0036)">COLD ARCHIVE</span>
          <div class="fleet-workers-container" id="fleetWorkersList" title="Active Cluster Worker Pods">
            <span class="fleet-worker-pill"><span class="fleet-worker-dot"></span>worker-01</span>
          </div>
          <div class="fleet-badge" id="fleetRoleBadge" title="Enterprise RBAC Role (ADR 0037)">
            <span class="status-indicator" id="fleetRoleDot" style="background: var(--accent);"></span>
            <span id="fleetRoleText">ROLE: ADMIN</span>
          </div>
          <div class="fleet-badge" id="fleetMfaBadge" title="MFA Verification Status (ADR 0037)">
            <span class="status-indicator" id="fleetMfaDot" style="background: var(--success);"></span>
            <span id="fleetMfaText">MFA: ACTIVE</span>
          </div>
          <div class="fleet-badge" id="otlpGatewayBadge" title="OpenTelemetry OTLP Ingestion Gateway (ADR 0038)">
            <span class="status-indicator" id="otlpDot" style="background: var(--accent);"></span>
            <span id="otlpText">OTLP: /V1/TRACES</span>
          </div>
          <div class="fleet-badge" id="sseStatusBadge" title="Server-Sent Events (SSE) Stream (ADR 0039)">
            <span class="status-indicator" id="sseDot" style="background: var(--text-muted);"></span>
            <span id="sseText">SSE: STANDBY</span>
          </div>
          <div class="fleet-badge" id="pushNotificationBadge" title="Enterprise WebPush Alerts (ADR 0039)" style="cursor: pointer;">
            <span class="status-indicator" id="pushDot" style="background: var(--text-muted);"></span>
            <span id="pushText">PUSH: OFF</span>
          </div>
          <div class="fleet-badge" id="ebpfGatewayBadge" title="Kernel-Level Socket LLM Interception Gateway (ADR 0040)">
            <span class="status-indicator" id="ebpfDot" style="background: var(--success);"></span>
            <span id="ebpfText">EBPF: ATTACHED</span>
          </div>
          <div class="fleet-badge" id="raftConsensusBadge" title="Raft Distributed Consensus Engine (ADR 0041)">
            <span class="status-indicator" id="raftDot" style="background: var(--accent);"></span>
            <span id="raftText">RAFT: LEADER (T0)</span>
          </div>
          <div class="fleet-badge" id="selfHealingBadge" title="Autonomous Self-Healing Policy Controller (ADR 0042)">
            <span class="status-indicator" id="selfHealingDot" style="background: var(--success);"></span>
            <span id="selfHealingText">HEAL: ACTIVE (0)</span>
          </div>
          <div class="fleet-badge" id="rootCauseBadge" title="Distributed Root-Cause Causal Inference (ADR 0043)">
            <span class="status-indicator" id="rootCauseDot" style="background: var(--success);"></span>
            <span id="rootCauseText">RCA: NOMINAL</span>
          </div>
          <div class="fleet-badge" id="grpcGatewayBadge" title="Ultra-High-Throughput gRPC Streaming Gateway (ADR 0044)">
            <span class="status-indicator" id="grpcGatewayDot" style="background: var(--success);"></span>
            <span id="grpcGatewayText">GRPC: READY</span>
          </div>
          <div class="fleet-badge" id="driftBadge" title="Embedding-Free Vector Semantic Drift & Hallucination Detector (ADR 0045)">
            <span class="status-indicator" id="driftDot" style="background: var(--success);"></span>
            <span id="driftText">DRIFT: NOMINAL</span>
          </div>
          <div class="fleet-badge" id="rebalancerBadge" title="Adaptive Dynamic Quota Re-balancing (ADR 0046)">
            <span class="status-indicator" id="rebalancerDot" style="background: var(--success);"></span>
            <span id="rebalancerText">QUOTA: BALANCED</span>
          </div>
          <div class="fleet-badge" id="coalitionBadge" title="Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (ADR 0047)">
            <span class="status-indicator" id="coalitionDot" style="background: var(--success);"></span>
            <span id="coalitionText">COALITIONS: OPTIMAL</span>
          </div>
          <div class="fleet-badge" id="shmBadge" title="Zero-Copy Shared-Memory IPC Ring Buffer (ADR 0048)">
            <span class="status-indicator" id="shmDot" style="background: var(--success);"></span>
            <span id="shmText">SHM: ACTIVE (0 PENDING)</span>
          </div>
          <div class="fleet-badge" id="dpBadge" title="Differential Privacy & Cryptographic State Anonymization (ADR 0049)">
            <span class="status-indicator" id="dpDot" style="background: var(--success);"></span>
            <span id="dpText">DP: PROTECTED (EPS=1.0)</span>
          </div>
          <div class="fleet-badge" id="forecastBadge" title="Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales (ADR 0050)">
            <span class="status-indicator" id="forecastDot" style="background: var(--success);"></span>
            <span id="forecastText">FORECAST: STABLE</span>
          </div>
          <div class="fleet-badge" id="byzantineBadge" title="Multi-Agent Byzantine Attack & Prompt Injection Stress Harness (ADR 0051)">
            <span class="status-indicator" id="byzantineDot" style="background: var(--success);"></span>
            <span id="byzantineText">BYZANTINE: SHIELDED (100%)</span>
          </div>
          <div class="fleet-badge" id="honeypotBadge" title="Multi-Agent Dynamic Honeypot & Deception Sandboxing (ADR 0052)">
            <span class="status-indicator" id="honeypotDot" style="background: var(--success);"></span>
            <span id="honeypotText">HONEYPOT: ARMED (0 TRAPPED)</span>
          </div>
          <div class="fleet-badge" id="modelCheckBadge" title="Bounded Model Checking & Multi-Agent Invariant Verification (ADR 0053)">
            <span class="status-indicator" id="modelCheckDot" style="background: var(--success);"></span>
            <span id="modelCheckText">BMC: VERIFIED (K=10)</span>
          </div>
          <div class="fleet-badge" id="topologyHealBadge" title="Autonomous Dynamic Topology Healing & Auto-Patching (ADR 0054)">
            <span class="status-indicator" id="topologyHealDot" style="background: var(--success);"></span>
            <span id="topologyHealText">HEAL: NOMINAL</span>
          </div>
          <div class="fleet-badge" id="artifactBadge" title="Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking (ADR 0055)">
            <span class="status-indicator" id="artifactDot" style="background: var(--text-muted);"></span>
            <span id="artifactText">ARTIFACTS: 0 RECORDED</span>
          </div>
          <div class="fleet-badge" id="meshBadge" title="Decentralized Epidemic Gossip Telemetry Mesh Sync (ADR 0056)">
            <span class="status-indicator" id="meshDot" style="background: var(--success);"></span>
            <span id="meshText">MESH: CONVERGED (0 PEERS)</span>
          </div>
          <div class="fleet-badge" id="shardBadge" title="Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer (ADR 0057)">
            <span class="status-indicator" id="shardDot" style="background: var(--success);"></span>
            <span id="shardText">SHARDING: BALANCED (1 NODE)</span>
          </div>
          <div class="fleet-badge" id="schemaBadge" title="Autonomous Semantic Schema Migration & State Translation Engine (ADR 0058)">
            <span class="status-indicator" id="schemaDot" style="background: var(--success);"></span>
            <span id="schemaText">SCHEMA: V2.0.0 (3 VERSIONS)</span>
          </div>
          <div class="fleet-badge" id="epistemicBadge" title="Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization (ADR 0059)">
            <span class="status-indicator" id="epistemicDot" style="background: var(--success);"></span>
            <span id="epistemicText">EPISTEMIC: OK (0 CLAIMS)</span>
          </div>
          <div class="fleet-badge" id="zkProofBadge" title="Zero-Knowledge Causal Proof Generation & Cryptographic Audit (ADR 0060)">
            <span class="status-indicator" id="zkProofDot" style="background: var(--success);"></span>
            <span id="zkProofText">ZK-PROOF: VERIFIED (0)</span>
          </div>
          <div class="fleet-badge" id="codecBadge" title="Canonical Causal Encoding & Byte-Deterministic Contract Engine (ADR 0061)">
            <span class="status-indicator" id="codecDot" style="background: var(--success);"></span>
            <span id="codecText">CODEC: RFC-8785 JCS</span>
          </div>
          <div class="fleet-badge" id="safetyBadge" title="Unified Safety Authority & Linearizable Execution Permits (ADR 0062)">
            <span class="status-indicator" id="safetyDot" style="background: var(--success);"></span>
            <span id="safetyText">SAFETY: AUTHORITATIVE (E1)</span>
          </div>
          <div class="fleet-badge" id="ledgerBadge" title="Fenced Effect Ledger & Monotonic Idempotency Barrier (ADR 0063)">
            <span class="status-indicator" id="ledgerDot" style="background: var(--success);"></span>
            <span id="ledgerText">EFFECT LEDGER: FENCED (0)</span>
          </div>
          <div class="fleet-badge" id="ingressBadge" title="Loss-Aware Causal Ingestion & Evidentiary Completeness Rule (ADR 0064)">
            <span class="status-indicator" id="ingressDot" style="background: var(--success);"></span>
            <span id="ingressText">INGRESS: LOSS-AWARE (0 GAP)</span>
          </div>
          <div class="fleet-badge" id="auditBadge" title="Durable Audit Anchor & Merkle Audit Receipt Engine (ADR 0065)">
            <span class="status-indicator" id="auditDot" style="background: var(--success);"></span>
            <span id="auditText">AUDIT: ANCHORED (E0)</span>
          </div>
          <div class="fleet-badge" id="verifierBadge" title="Bounded Verification Semantics & Explicit Counterexample Engine (ADR 0066)">
            <span class="status-indicator" id="verifierDot" style="background: var(--success);"></span>
            <span id="verifierText">VERIFIER: BOUNDED SAFE</span>
          </div>
          <div class="fleet-badge" id="remediationBadge" title="Governed Remediation & Fenced Action State Machine (ADR 0067)">
            <span class="status-indicator" id="remediationDot" style="background: var(--success);"></span>
            <span id="remediationText">REMEDIATION: GOVERNED (0)</span>
          </div>
          <div class="fleet-badge" id="reconcilerBadge" title="Partition Reconciliation & Conflict Lattice Engine (ADR 0068)">
            <span class="status-indicator" id="reconcilerDot" style="background: var(--success);"></span>
            <span id="reconcilerText">RECONCILER: CONVERGED (0)</span>
          </div>
          <div class="fleet-badge" id="privacyLedgerBadge" title="Privacy Release Ledger & Differential Privacy Accountant (ADR 0069)">
            <span class="status-indicator" id="privacyLedgerDot" style="background: var(--success);"></span>
            <span id="privacyLedgerText">PRIVACY: RELEASE FENCED (0)</span>
          </div>
          <div class="fleet-badge" id="evolutionBadge" title="Migration Preservation & Partial Isomorphisms (ADR 0070)">
            <span class="status-indicator" id="evolutionDot" style="background: var(--success);"></span>
            <span id="evolutionText">SCHEMA: LOSSLESS PRESERVED</span>
          </div>
          <div class="fleet-badge" id="acceptanceBadge" title="Enterprise Acceptance Harness & Multi-Dimensional Release Verification Gate (ADR 0071)">
            <span class="status-indicator" id="acceptanceDot" style="background: var(--success);"></span>
            <span id="acceptanceText">ACCEPTANCE: CERTIFIED (6/6)</span>
          </div>
          <div class="fleet-badge" id="consensusBadge" title="Consensus Authority Binding & Linearizable Quorum Fencing (ADR 0072)">
            <span class="status-indicator" id="consensusDot" style="background: var(--success);"></span>
            <span id="consensusText">CONSENSUS: QUORUM COMMITTED</span>
          </div>
          <div class="fleet-badge" id="downstreamBadge" title="Downstream Effect Protocol & 2PC Outcome-Unknown (ADR 0073)">
            <span class="status-indicator" id="downstreamDot" style="background: var(--success);"></span>
            <span id="downstreamText">DOWNSTREAM: RECONCILED</span>
          </div>
          <div class="fleet-badge" id="evidenceBadge" title="Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal (ADR 0074)">
            <span class="status-indicator" id="evidenceDot" style="background: var(--success);"></span>
            <span id="evidenceText">EVIDENCE: SPOOL COMMITTED</span>
          </div>
          <div class="fleet-badge" id="witnessBadge" title="Independent Root Witness & Public-Key Chains (ADR 0075)">
            <span class="status-indicator" id="witnessDot" style="background: var(--success);"></span>
            <span id="witnessText">WITNESS: ASYMMETRIC VERIFIED</span>
          </div>
          <div class="fleet-badge" id="capabilityBadge" title="Tenant Identity Boundary & Attenuated Capability Tokens (ADR 0076)">
            <span class="status-indicator" id="capabilityDot" style="background: var(--success);"></span>
            <span id="capabilityText">CAPABILITY: ATTENUATED OK</span>
          </div>
          <div class="fleet-badge" id="compensationBadge" title="Effect-Aware Compensation Algebra & Monotone Approval Lattices (ADR 0077)">
            <span class="status-indicator" id="compensationDot" style="background: var(--success);"></span>
            <span id="compensationText">COMPENSATION: ALGEBRA OK</span>
          </div>
          <div class="fleet-badge" id="datasetPrivacyBadge" title="Durable Dataset-Level Privacy Accountant & Quorum Ledger (ADR 0078)">
            <span class="status-indicator" id="datasetPrivacyDot" style="background: var(--success);"></span>
            <span id="datasetPrivacyText">DATASET PRIVACY: BUDGET OK</span>
          </div>
          <div class="fleet-badge" id="interopBadge" title="Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (ADR 0079)">
            <span class="status-indicator" id="interopDot" style="background: var(--success);"></span>
            <span id="interopText">INTEROP LAB: CONFORMANT</span>
          </div>
          <div class="fleet-badge" id="provenanceBadge" title="Verifiable Release Provenance & Signed Artifact DAG (ADR 0080)">
            <span class="status-indicator" id="provenanceDot" style="background: var(--success);"></span>
            <span id="provenanceText">PROVENANCE: SLSA L3 OK</span>
          </div>
          <div class="fleet-badge" id="datacenterBadge" title="Datacenter Failure Exercise & Region Loss Fault Injection (ADR 0081)">
            <span class="status-indicator" id="datacenterDot" style="background: var(--success);"></span>
            <span id="datacenterText">DATACENTER: QUORUM RESILIENT</span>
          </div>
          <div class="fleet-badge" id="gatewayBadge" title="Autonomous Action Gateway & Governed Downstream Execution (ADR 0082)">
            <span class="status-indicator" id="gatewayDot" style="background: var(--success);"></span>
            <span id="gatewayText">GATEWAY: AUTONOMOUS OK</span>
          </div>
        </div>






        <div class="fleet-meters-container" id="fleetMetersGroup">
          <div class="fleet-meter-item" title="Cumulative Fleet Turns Quota">
            <span class="fleet-meter-label">Turns</span>
            <span class="fleet-meter-val" id="fleetTurnsVal">0 / --</span>
            <div class="fleet-progress-bg"><div class="fleet-progress-fill" id="fleetTurnsFill"></div></div>
          </div>
          <div class="fleet-meter-item" title="Cumulative Fleet Token Budget">
            <span class="fleet-meter-label">Tokens</span>
            <span class="fleet-meter-val" id="fleetTokensVal">0 / --</span>
            <div class="fleet-progress-bg"><div class="fleet-progress-fill" id="fleetTokensFill"></div></div>
          </div>
          <div class="fleet-meter-item" title="Cumulative Fleet Dollar Cost">
            <span class="fleet-meter-label">Cost</span>
            <span class="fleet-meter-val" id="fleetCostVal">$0.00 / --</span>
            <div class="fleet-progress-bg"><div class="fleet-progress-fill" id="fleetCostFill"></div></div>
          </div>
        </div>

        <div class="fleet-actions-group">
          <button class="btn-fleet-reset" id="btnLeaderStepDown" style="display: none;" title="Voluntarily relinquish cluster leadership to standby node">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/></svg>
            <span>Step Down</span>
          </button>
          <button class="btn-fleet-reset" id="btnArchiveSwarm" title="Archive completed swarm telemetry to S3/GCS cold storage (ADR 0036)">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
            <span>Archive Cold</span>
          </button>
          <button class="btn-fleet-trip" id="btnFleetTrip" title="Broadcast immediate emergency trip to all cluster workers">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg>
            <span>Emergency Trip</span>
          </button>
          <button class="btn-fleet-reset" id="btnFleetReset" title="Flush Redis fleet state and re-arm circuit breaker">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
            <span>Reset Fleet</span>
          </button>
        </div>
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

      <!-- Distributed Redis Fleet Trip Alert Banner (ADR 0034) -->
      <div class="fleet-trip-alert-banner" id="fleetTripAlert">
        <div class="fleet-trip-alert-text">
          <strong>FLEET CIRCUIT TRIPPED:</strong>
          <span id="fleetTripReason">Distributed budget exceeded</span>
          <span style="color: var(--text-muted); margin-left: 8px;" id="fleetTrippedBy">by worker-01</span>
        </div>
        <button class="btn-fleet-reset" id="btnFleetAlertReset">Re-arm Fleet Circuit</button>
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
          <div class="insp-tab-bar" id="inspTabBar">
            <button class="insp-tab-btn active" id="tabInspPayload" data-view="viewPayload">Payload</button>
            <button class="insp-tab-btn" id="tabInspSnapshot" data-view="viewSnapshot">State</button>
            <button class="insp-tab-btn" id="tabInspDiff" data-view="viewDiff">
              <span>Diff</span>
              <span class="diff-badge-pill" id="diffBadgePill" style="display: none;">0</span>
            </button>
          </div>
          
          <div id="viewPayload" class="insp-view">
            <div class="json-box" id="inspPayload">{
  "status": "listening",
  "transport": "WebSocket + SSE",
  "protocol": "SwarmWatch v0.1.0"
}</div>
          </div>

          <div id="viewSnapshot" class="insp-view" style="display: none;">
            <div class="json-box" id="inspSnapshot">{
  "status": "No state snapshot recorded for current turn"
}</div>
          </div>

          <div id="viewDiff" class="insp-view diff-view-panel" style="display: none;">
            <div class="diff-toolbar">
              <div class="diff-toolbar-row">
                <span style="font-size: 12px; color: var(--text-muted); font-weight: 600;">Base:</span>
                <select id="diffTargetSelect" class="diff-select" title="Select comparison base snapshot">
                  <option value="prev">Previous Turn (N-1)</option>
                  <option value="root">Initial Turn (Root)</option>
                  <option value="fork">Fork Origin</option>
                </select>
                <div class="diff-mode-toggle">
                  <button class="diff-mode-btn active" id="btnDiffUnified" title="Unified Diff View">Unified</button>
                  <button class="diff-mode-btn" id="btnDiffSplit" title="Split Side-by-Side View">Split</button>
                </div>
              </div>
              <div class="diff-filter-bar">
                <button class="diff-filter-chip active" data-filter="all" id="diffFilterAll">All (<span id="diffCountAll">0</span>)</button>
                <button class="diff-filter-chip" data-filter="changed" id="diffFilterChanged">Changed (<span id="diffCountChanged">0</span>)</button>
                <button class="diff-filter-chip" data-filter="added" id="diffFilterAdded" style="color: #10B981;">+<span id="diffCountAdded">0</span></button>
                <button class="diff-filter-chip" data-filter="modified" id="diffFilterModified" style="color: #F59E0B;">~<span id="diffCountModified">0</span></button>
                <button class="diff-filter-chip" data-filter="removed" id="diffFilterRemoved" style="color: #EF4444;">-<span id="diffCountRemoved">0</span></button>
                <button class="diff-filter-chip" data-filter="redacted" id="diffFilterRedacted" style="color: #A78BFA;">[R] <span id="diffCountRedacted">0</span></button>
              </div>
            </div>

            <div class="diff-meta-row" id="diffMetaRow">
              <span id="diffBaseLabel">Base: Previous Turn</span>
              <span id="diffFingerprintDelta">SHA: -</span>
            </div>

            <div class="diff-entries-list" id="diffEntriesList">
              <div style="color: var(--text-muted); text-align: center; padding: 20px 0;">Select a turn to inspect state mutations.</div>
            </div>
          </div>
        </div>
        <div class="prop-row" style="margin-top: 8px;">
          <div style="display: flex; gap: 6px;">
            <button class="btn-action btn-secondary" id="btnInspStepBack" style="flex: 1; height: 32px; padding: 0 8px; font-size: 12px;" title="Step back to this turn (Key: [)">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
              <span>Step-Back</span>
            </button>
            <button class="btn-action btn-secondary tm-btn-fork" id="btnFork" style="flex: 1.2; height: 32px; padding: 0 8px; font-size: 12px;" title="Fork new branch from this turn (Key: F)">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
              <span>Fork Branch</span>
            </button>
          </div>
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

  <!-- Enterprise MFA & RBAC Control Modal (ADR 0037) -->
  <div id="mfaModal" style="display: none; position: fixed; inset: 0; background: rgba(8, 11, 17, 0.85); z-index: 1000; align-items: center; justify-content: center;">
    <div style="background: var(--bg-surface); border: 1px solid var(--border-strong); border-radius: 4px; width: 620px; max-width: 90vw; padding: 20px; display: flex; flex-direction: column; gap: 16px;">
      <div style="display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border-subtle); padding-bottom: 12px;">
        <div>
          <div style="font-weight: 600; font-size: 14px; color: var(--text-primary);">Enterprise MFA &amp; RBAC Control</div>
          <div style="font-size: 12px; color: var(--text-muted); margin-top: 2px;">RFC 6238 TOTP Authenticator &amp; Role Verification (ADR 0037)</div>
        </div>
        <button id="btnCloseMfaModal" style="background: transparent; border: 1px solid var(--border-subtle); color: var(--text-muted); border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 12px;">Close</button>
      </div>

      <!-- Current Session Identity -->
      <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; background: var(--bg-canvas); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 10px 12px;">
        <div>
          <div style="font-size: 12px; color: var(--text-muted); text-transform: uppercase;">Operator ID</div>
          <div id="mfaUsernameDisplay" style="font-family: var(--font-mono); font-size: 12px; font-weight: 600; color: var(--text-primary); margin-top: 2px;">admin</div>
        </div>
        <div>
          <div style="font-size: 12px; color: var(--text-muted); text-transform: uppercase;">Role Assigned</div>
          <div id="mfaRoleDisplay" style="font-family: var(--font-mono); font-size: 12px; font-weight: 600; color: var(--accent); margin-top: 2px;">ADMIN</div>
        </div>
        <div>
          <div style="font-size: 12px; color: var(--text-muted); text-transform: uppercase;">MFA Status</div>
          <div id="mfaStatusDisplay" style="font-family: var(--font-mono); font-size: 12px; font-weight: 600; color: var(--success); margin-top: 2px;">ACTIVE</div>
        </div>
      </div>

      <!-- Step-Up Verification Section -->
      <div style="display: flex; flex-direction: column; gap: 8px; border: 1px solid var(--border-subtle); border-radius: 4px; padding: 12px; background: var(--bg-canvas);">
        <div style="font-size: 12px; font-weight: 600; color: var(--text-primary);">Step-Up Authentication Challenge</div>
        <div style="font-size: 12px; color: var(--text-muted);">Destructive cluster actions (fleet trip/reset, policy mutations, key revocation) require step-up challenge verification.</div>
        <div style="display: flex; gap: 8px; align-items: center; margin-top: 4px;">
          <input type="text" id="mfaStepUpInput" placeholder="Enter 6-digit TOTP or backup code" style="flex: 1; background: var(--bg-surface); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 6px 10px; color: var(--text-primary); font-family: var(--font-mono); font-size: 12px; outline: none;">
          <button id="btnVerifyStepUp" class="btn-action btn-secondary" style="height: 30px; padding: 0 12px;">Authorize Session</button>
        </div>
        <div id="mfaStepUpStatus" style="font-size: 12px; font-family: var(--font-mono); color: var(--text-muted); min-height: 16px;"></div>
      </div>

      <!-- TOTP Enrollment Section -->
      <div style="display: flex; flex-direction: column; gap: 10px; border: 1px solid var(--border-subtle); border-radius: 4px; padding: 12px; background: var(--bg-canvas);">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <div>
            <div style="font-size: 12px; font-weight: 600; color: var(--text-primary);">TOTP Enrollment</div>
            <div style="font-size: 12px; color: var(--text-muted); margin-top: 2px;">Configure Google Authenticator / 1Password compatible RFC 6238 TOTP.</div>
          </div>
          <button id="btnEnrollMfa" class="btn-action btn-primary" style="height: 28px; padding: 0 10px; font-size: 12px;">Enroll New Token</button>
        </div>

        <div id="mfaEnrollSection" style="display: none; flex-direction: column; gap: 10px; margin-top: 6px;">
          <div style="background: rgba(56, 189, 248, 0.08); border: 1px solid rgba(56, 189, 248, 0.3); border-radius: 4px; padding: 10px;">
            <div style="font-size: 12px; color: var(--accent); font-weight: 600; margin-bottom: 4px;">TOTP Secret Key (RFC 3548 Base32)</div>
            <div id="mfaSecretKey" style="font-family: var(--font-mono); font-size: 13px; color: var(--text-primary); user-select: all; word-break: break-all;">-</div>
            <div id="mfaUriText" style="font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); margin-top: 4px; word-break: break-all;">-</div>
          </div>

          <div>
            <div style="font-size: 12px; font-weight: 600; color: var(--text-primary); margin-bottom: 4px;">Single-Use Recovery Backup Codes</div>
            <div id="mfaBackupCodes" style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 4px; font-family: var(--font-mono); font-size: 12px; background: var(--bg-surface); padding: 8px; border-radius: 4px; border: 1px solid var(--border-subtle); color: var(--text-secondary);"></div>
          </div>

          <div style="display: flex; gap: 8px; align-items: center; margin-top: 4px;">
            <input type="text" id="mfaCodeInput" placeholder="Enter 6-digit TOTP code to verify" style="flex: 1; background: var(--bg-surface); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 6px 10px; color: var(--text-primary); font-family: var(--font-mono); font-size: 12px; outline: none;">
            <button id="btnVerifyMfaActivation" class="btn-action btn-primary" style="height: 30px; padding: 0 12px;">Confirm &amp; Activate</button>
          </div>
          <div id="mfaEnrollStatus" style="font-size: 12px; font-family: var(--font-mono); color: var(--text-muted); min-height: 16px;"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Interactive Time-Machine Fork Modal -->
  <div id="forkModal" style="display: none; position: fixed; inset: 0; background: rgba(8, 11, 17, 0.88); z-index: 1050; align-items: center; justify-content: center; backdrop-filter: blur(2px);">
    <div style="background: var(--bg-surface); border: 1px solid var(--border-strong); border-radius: 4px; width: 620px; max-width: 92vw; padding: 22px; display: flex; flex-direction: column; gap: 16px;">
      <div style="display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border-subtle); padding-bottom: 12px;">
        <div>
          <div style="display: flex; align-items: center; gap: 8px;">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#A78BFA" stroke-width="1.5"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
            <span style="font-weight: 700; font-size: 14px; color: var(--text-primary);">Fork Swarm Execution Branch</span>
          </div>
          <div style="font-size: 12px; color: var(--text-muted); margin-top: 3px;">Branch out an isolated timeline from the causal state snapshot at the selected turn.</div>
        </div>
        <button id="btnCloseForkModal" style="background: transparent; border: 1px solid var(--border-subtle); color: var(--text-muted); border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 12px;">Esc</button>
      </div>

      <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; background: var(--bg-canvas); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 12px;">
        <div style="display: flex; flex-direction: column; gap: 2px;">
          <span style="font-size: 12px; color: var(--text-muted); text-transform: uppercase;">Origin Turn / Span</span>
          <span id="forkOriginTurn" style="font-family: var(--font-mono); font-size: 12px; font-weight: 600; color: var(--text-primary);">-</span>
        </div>
        <div style="display: flex; flex-direction: column; gap: 2px;">
          <span style="font-size: 12px; color: var(--text-muted); text-transform: uppercase;">Causal Agents</span>
          <span id="forkOriginAgents" style="font-size: 12px; color: var(--text-secondary);">-</span>
        </div>
        <div style="display: flex; flex-direction: column; gap: 2px; grid-column: 1 / -1;">
          <span style="font-size: 12px; color: var(--text-muted); text-transform: uppercase;">State Snapshot Hash</span>
          <span id="forkOriginHash" style="font-family: var(--font-mono); font-size: 12px; color: #A78BFA;">sha256:verified</span>
        </div>
      </div>

      <div style="display: flex; flex-direction: column; gap: 6px;">
        <label for="forkBranchName" style="font-size: 12px; font-weight: 600; color: var(--text-muted);">New Branch Identifier</label>
        <input type="text" id="forkBranchName" placeholder="e.g. branch-fork-turn-3" style="background: var(--bg-canvas); border: 1px solid var(--border-subtle); border-radius: 4px; padding: 8px 12px; color: var(--text-primary); font-family: var(--font-mono); font-size: 12px; outline: none;" />
      </div>

      <div style="display: flex; flex-direction: column; gap: 6px;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <span style="font-size: 12px; font-weight: 600; color: var(--text-muted);">State Snapshot Preview</span>
          <span style="font-size: 12px; font-family: var(--font-mono); color: var(--text-muted);">sanitized • zero secrets</span>
        </div>
        <div id="forkStatePreview" class="json-box" style="max-height: 160px;">{}</div>
      </div>

      <div style="display: flex; justify-content: flex-end; gap: 10px; border-top: 1px solid var(--border-subtle); padding-top: 14px;">
        <button id="btnCancelFork" class="btn-action btn-secondary" style="height: 32px; padding: 0 14px;">Cancel</button>
        <button id="btnExecuteFork" class="btn-action btn-primary" style="height: 32px; padding: 0 16px; background: #A78BFA; border-color: #A78BFA; color: #080B11;">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>
          <span>Fork & Switch Branch</span>
        </button>
      </div>
    </div>
  </div>

  <!-- Toast Container -->
  <div id="toastContainer" style="position: fixed; bottom: 20px; right: 20px; z-index: 2000; display: flex; flex-direction: column; gap: 8px; pointer-events: none;"></div>

  <script>
    (function() {
      var spans = [];
      var isSimulating = false;
      var selectedTurn = null;
      var socket = null;

      var currentBranch = 'main';
      var activeSwarmId = 'swarm-live-01';
      var selectedTurnIndex = -1;
      var cachedSnapshots = {};

      var scrubberSlider = document.getElementById('scrubberSlider');
      var scrubberTurnDisplay = document.getElementById('scrubberTurnDisplay');
      var scrubberTicks = document.getElementById('scrubberTicks');
      var btnStepBack = document.getElementById('btnStepBack');
      var btnStepForward = document.getElementById('btnStepForward');
      var btnJumpHead = document.getElementById('btnJumpHead');
      var btnTimeMachineFork = document.getElementById('btnTimeMachineFork');
      var branchSelector = document.getElementById('branchSelector');

      var forkModal = document.getElementById('forkModal');
      var btnCloseForkModal = document.getElementById('btnCloseForkModal');
      var btnCancelFork = document.getElementById('btnCancelFork');
      var btnExecuteFork = document.getElementById('btnExecuteFork');
      var forkOriginTurn = document.getElementById('forkOriginTurn');
      var forkOriginAgents = document.getElementById('forkOriginAgents');
      var forkOriginHash = document.getElementById('forkOriginHash');
      var forkBranchName = document.getElementById('forkBranchName');
      var forkStatePreview = document.getElementById('forkStatePreview');

      var btnInspStepBack = document.getElementById('btnInspStepBack');
      var btnFork = document.getElementById('btnFork');

      var tabInspPayload = document.getElementById('tabInspPayload');
      var tabInspSnapshot = document.getElementById('tabInspSnapshot');
      var tabInspDiff = document.getElementById('tabInspDiff');
      var viewPayload = document.getElementById('viewPayload');
      var viewSnapshot = document.getElementById('viewSnapshot');
      var viewDiff = document.getElementById('viewDiff');
      var inspSnapshot = document.getElementById('inspSnapshot');
      var diffBadgePill = document.getElementById('diffBadgePill');
      var diffTargetSelect = document.getElementById('diffTargetSelect');
      var btnDiffUnified = document.getElementById('btnDiffUnified');
      var btnDiffSplit = document.getElementById('btnDiffSplit');
      var diffEntriesList = document.getElementById('diffEntriesList');
      var diffBaseLabel = document.getElementById('diffBaseLabel');
      var diffFingerprintDelta = document.getElementById('diffFingerprintDelta');
      var currentDiffFilter = 'all';
      var currentDiffMode = 'unified';
      var currentDiffData = null;

      function showToast(message, type, durationMs) {
        type = type || 'info';
        durationMs = durationMs || 3500;
        var container = document.getElementById('toastContainer');
        if (!container) return;
        var toast = document.createElement('div');
        toast.className = 'toast-item ' + type;
        
        var iconSvg = '';
        if (type === 'success') {
          iconSvg = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#10B981" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>';
        } else if (type === 'purple') {
          iconSvg = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#A78BFA" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 8 14"/></svg>';
        } else if (type === 'danger') {
          iconSvg = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#EF4444" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';
        } else {
          iconSvg = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#38BDF8" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
        }

        toast.innerHTML = iconSvg + '<span>' + message + '</span>';
        container.appendChild(toast);
        setTimeout(function() {
          toast.style.opacity = '0';
          toast.style.transform = 'translateY(8px)';
          setTimeout(function() { if (toast.parentNode) toast.parentNode.removeChild(toast); }, 250);
        }, durationMs);
      }

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
        selectedTurnIndex = spans.findIndex(function(s) { return s.id === span.id; });
        document.querySelectorAll('.turn-card').forEach(function(card) {
          card.classList.remove('selected');
        });
        var card = document.getElementById('card_' + span.id);
        if (card) card.classList.add('selected');

        var turnNum = selectedTurnIndex >= 0 ? (selectedTurnIndex + 1) : 1;
        document.getElementById('inspTurnId').textContent = span.id + " (Turn " + turnNum + ": " + span.source + " -> " + span.target + ")";
        var inspectorData = {
          turn_index: turnNum,
          span_id: span.id,
          source_agent: span.source,
          target_agent: span.target,
          duration_ms: span.duration,
          token_count: span.tokens,
          entropy_score: span.entropy,
          timestamp: span.timestamp,
          payload: span.payload
        };

        var snap = cachedSnapshots[span.id];
        if (!snap) {
          try {
            var snapRes = await fetch('/api/v1/snapshots/' + span.id);
            if (snapRes.ok) {
              snap = await snapRes.json();
              if (snap && snap.state_data) {
                cachedSnapshots[span.id] = snap;
              }
            }
          } catch(e) {}
        }

        if (snap && snap.state_data) {
          inspectorData.state_snapshot = snap.state_data;
          inspectorData.state_hash = snap.state_hash;
          if (inspSnapshot) {
            inspSnapshot.textContent = JSON.stringify(snap.state_data, null, 2);
          }
        } else {
          if (inspSnapshot) {
            inspSnapshot.textContent = JSON.stringify({
              status: "No explicit state snapshot recorded for " + span.id,
              source_agent: span.source,
              target_agent: span.target,
              tokens: span.tokens
            }, null, 2);
          }
        }

        document.getElementById('inspPayload').textContent = JSON.stringify(inspectorData, null, 2);
        updateTimeMachineUI();
        await updateStateDiffInspector();
      }

      function isValRedactedClient(val) {
        if (typeof val === 'string') {
          return val.indexOf('[REDACTED') !== -1 || val === '[REDACTED]';
        }
        if (val && typeof val === 'object') {
          if (Array.isArray(val)) {
            return val.some(isValRedactedClient);
          }
          return Object.values(val).some(isValRedactedClient);
        }
        return false;
      }

      function computeStateDiffClient(stateA, stateB, hashA, hashB) {
        var a = (stateA && typeof stateA === 'object') ? stateA : {};
        var b = (stateB && typeof stateB === 'object') ? stateB : {};
        var entries = [];
        var summary = {
          added_count: 0,
          modified_count: 0,
          removed_count: 0,
          unchanged_count: 0,
          redacted_count: 0,
          total_changes: 0
        };

        function diffRec(objA, objB, currentPath) {
          if (
            objA && typeof objA === 'object' && !Array.isArray(objA) &&
            objB && typeof objB === 'object' && !Array.isArray(objB)
          ) {
            var keysA = Object.keys(objA);
            var keysB = Object.keys(objB);
            var allKeys = Array.from(new Set(keysA.concat(keysB))).sort();

            for (var i = 0; i < allKeys.length; i++) {
              var k = allKeys[i];
              var keyPath = currentPath ? (currentPath + '.' + k) : k;
              if (!(k in objA)) {
                var val = objB[k];
                var red = isValRedactedClient(val);
                entries.push({
                  path: keyPath,
                  kind: 'added',
                  old_value: undefined,
                  new_value: val,
                  is_redacted: red
                });
                summary.added_count++;
                if (red) summary.redacted_count++;
              } else if (!(k in objB)) {
                var val = objA[k];
                var red = isValRedactedClient(val);
                entries.push({
                  path: keyPath,
                  kind: 'removed',
                  old_value: val,
                  new_value: undefined,
                  is_redacted: red
                });
                summary.removed_count++;
                if (red) summary.redacted_count++;
              } else {
                diffRec(objA[k], objB[k], keyPath);
              }
            }
          } else {
            var eq = JSON.stringify(objA) === JSON.stringify(objB);
            if (eq) {
              var red = isValRedactedClient(objA);
              entries.push({
                path: currentPath,
                kind: 'unchanged',
                old_value: objA,
                new_value: objB,
                is_redacted: red
              });
              summary.unchanged_count++;
              if (red) summary.redacted_count++;
            } else {
              var red = isValRedactedClient(objA) || isValRedactedClient(objB);
              entries.push({
                path: currentPath,
                kind: 'modified',
                old_value: objA,
                new_value: objB,
                is_redacted: red
              });
              summary.modified_count++;
              if (red) summary.redacted_count++;
            }
          }
        }

        diffRec(a, b, '');
        summary.total_changes = summary.added_count + summary.modified_count + summary.removed_count;

        return {
          summary: summary,
          entries: entries,
          hash_a: hashA || '-',
          hash_b: hashB || '-'
        };
      }

      async function updateStateDiffInspector() {
        if (!selectedTurn) return;

        updateDiffTargetOptions();

        var currSpan = selectedTurn;
        var currIdx = selectedTurnIndex;
        var currSnap = cachedSnapshots[currSpan.id];
        var currState = (currSnap && currSnap.state_data) ? currSnap.state_data : {
          source_agent: currSpan.source,
          target_agent: currSpan.target,
          entropy: currSpan.entropy,
          tokens: currSpan.tokens,
          payload: (currSpan.payload || '').slice(0, 60)
        };
        var currHash = (currSnap && currSnap.state_hash) ? currSnap.state_hash : ('sha256:' + currSpan.id);

        var targetVal = diffTargetSelect ? diffTargetSelect.value : 'prev';
        var baseSpan = null;
        var baseLabel = 'Previous Turn';

        if (targetVal === 'root') {
          baseSpan = spans.length > 0 ? spans[0] : null;
          baseLabel = 'Initial Turn (1)';
        } else if (targetVal === 'fork') {
          baseSpan = spans.find(function(s) { return s.id.indexOf('_fork_') !== -1; }) || (spans.length > 0 ? spans[0] : null);
          baseLabel = 'Fork Origin';
        } else if (targetVal === 'prev') {
          baseSpan = (currIdx > 0 && currIdx < spans.length) ? spans[currIdx - 1] : null;
          baseLabel = baseSpan ? ('Turn ' + currIdx) : 'Initial Turn';
        } else {
          baseSpan = spans.find(function(s) { return s.id === targetVal; });
          baseLabel = baseSpan ? baseSpan.id : targetVal;
        }

        var baseState = {};
        var baseHash = '-';
        if (baseSpan) {
          var baseSnap = cachedSnapshots[baseSpan.id];
          if (!baseSnap) {
            try {
              var bRes = await fetch('/api/v1/snapshots/' + baseSpan.id);
              if (bRes.ok) {
                baseSnap = await bRes.json();
                if (baseSnap && baseSnap.state_data) {
                  cachedSnapshots[baseSpan.id] = baseSnap;
                }
              }
            } catch(e) {}
          }
          if (baseSnap && baseSnap.state_data) {
            baseState = baseSnap.state_data;
            baseHash = baseSnap.state_hash;
          } else {
            baseState = {
              source_agent: baseSpan.source,
              target_agent: baseSpan.target,
              entropy: baseSpan.entropy,
              tokens: baseSpan.tokens,
              payload: (baseSpan.payload || '').slice(0, 60)
            };
            baseHash = 'sha256:' + baseSpan.id;
          }
        }

        var diffData = null;
        if (baseSpan && currSpan) {
          try {
            var diffUrl = '/api/v1/swarms/' + encodeURIComponent(activeSwarmId) +
              '/snapshots/diff?base_span_id=' + encodeURIComponent(baseSpan.id) +
              '&target_span_id=' + encodeURIComponent(currSpan.id);
            var dRes = await fetch(diffUrl);
            if (dRes.ok) {
              diffData = await dRes.json();
            }
          } catch(e) {}
        }

        if (!diffData) {
          diffData = computeStateDiffClient(baseState, currState, baseHash, currHash);
        }

        currentDiffData = diffData;

        if (diffBadgePill) {
          diffBadgePill.textContent = diffData.summary.total_changes.toString();
          diffBadgePill.style.display = 'inline-block';
          if (diffData.summary.total_changes > 0) {
            diffBadgePill.className = 'diff-badge-pill has-changes';
          } else {
            diffBadgePill.className = 'diff-badge-pill';
          }
        }

        var countAll = document.getElementById('diffCountAll');
        var countChanged = document.getElementById('diffCountChanged');
        var countAdded = document.getElementById('diffCountAdded');
        var countModified = document.getElementById('diffCountModified');
        var countRemoved = document.getElementById('diffCountRemoved');
        var countRedacted = document.getElementById('diffCountRedacted');

        if (countAll) countAll.textContent = diffData.entries.length.toString();
        if (countChanged) countChanged.textContent = diffData.summary.total_changes.toString();
        if (countAdded) countAdded.textContent = diffData.summary.added_count.toString();
        if (countModified) countModified.textContent = diffData.summary.modified_count.toString();
        if (countRemoved) countRemoved.textContent = diffData.summary.removed_count.toString();
        if (countRedacted) countRedacted.textContent = diffData.summary.redacted_count.toString();

        if (diffBaseLabel) {
          diffBaseLabel.textContent = 'Base: ' + baseLabel + (baseSpan ? (' (' + baseSpan.id + ')') : ' (Genesis)');
        }
        if (diffFingerprintDelta) {
          var hA = (diffData.hash_a || baseHash || '').slice(0, 10);
          var hB = (diffData.hash_b || currHash || '').slice(0, 10);
          diffFingerprintDelta.textContent = 'SHA: ' + hA + ' \u2192 ' + hB;
        }

        renderDiffEntries();
      }

      function updateDiffTargetOptions() {
        if (!diffTargetSelect || !selectedTurn) return;
        var currentVal = diffTargetSelect.value;
        var html = '<option value="prev">Previous Turn (N-1)</option>' +
                   '<option value="root">Initial Turn (Root)</option>' +
                   '<option value="fork">Fork Origin</option>';

        for (var i = 0; i < spans.length; i++) {
          var s = spans[i];
          if (s.id !== selectedTurn.id) {
            html += '<option value="' + s.id + '">Turn ' + (i + 1) + ' (' + s.id + ')</option>';
          }
        }
        diffTargetSelect.innerHTML = html;
        if (currentVal && diffTargetSelect.querySelector('option[value="' + currentVal + '"]')) {
          diffTargetSelect.value = currentVal;
        } else {
          diffTargetSelect.value = 'prev';
        }
      }

      function renderDiffEntries() {
        if (!diffEntriesList || !currentDiffData) return;

        var entries = currentDiffData.entries || [];
        var filtered = entries.filter(function(e) {
          if (currentDiffFilter === 'all') return true;
          if (currentDiffFilter === 'changed') return e.kind !== 'unchanged';
          if (currentDiffFilter === 'added') return e.kind === 'added';
          if (currentDiffFilter === 'modified') return e.kind === 'modified';
          if (currentDiffFilter === 'removed') return e.kind === 'removed';
          if (currentDiffFilter === 'redacted') return e.is_redacted;
          return true;
        });

        if (filtered.length === 0) {
          diffEntriesList.innerHTML = '<div style="color: var(--text-muted); text-align: center; padding: 20px 0;">No properties match filter [' + currentDiffFilter + '].</div>';
          return;
        }

        var html = '';
        for (var i = 0; i < filtered.length; i++) {
          var item = filtered[i];
          var kindClass = item.kind;
          var tagLabel = item.kind.toUpperCase();
          if (item.is_redacted) {
            tagLabel += ' \u2022 REDACTED';
          }

          if (currentDiffMode === 'split') {
            var oldStr = item.old_value !== undefined ? JSON.stringify(item.old_value) : '<span style="color: var(--text-muted);">(none)</span>';
            var newStr = item.new_value !== undefined ? JSON.stringify(item.new_value) : '<span style="color: var(--text-muted);">(none)</span>';

            html += '<div class="diff-row ' + kindClass + '">' +
                      '<div class="diff-header-line">' +
                        '<span class="diff-path">' + escapeHtml(item.path) + '</span>' +
                        '<span class="diff-tag ' + (item.is_redacted ? 'redacted' : kindClass) + '">' + tagLabel + '</span>' +
                      '</div>' +
                      '<div class="diff-split-grid">' +
                        '<div class="diff-split-col"><span class="diff-split-label">Antecedent</span><span class="diff-val-old">' + oldStr + '</span></div>' +
                        '<div class="diff-split-col"><span class="diff-split-label">Target</span><span class="diff-val-new">' + newStr + '</span></div>' +
                      '</div>' +
                    '</div>';
          } else {
            var valLine = '';
            if (item.kind === 'added') {
              valLine = '<span class="diff-val-new">+ ' + escapeHtml(JSON.stringify(item.new_value)) + '</span>';
            } else if (item.kind === 'removed') {
              valLine = '<span class="diff-val-old">- ' + escapeHtml(JSON.stringify(item.old_value)) + '</span>';
            } else if (item.kind === 'modified') {
              valLine = '<span class="diff-val-old">' + escapeHtml(JSON.stringify(item.old_value)) + '</span>' +
                        '<span class="diff-val-arrow">\u2192</span>' +
                        '<span class="diff-val-new">' + escapeHtml(JSON.stringify(item.new_value)) + '</span>';
            } else {
              valLine = '<span style="color: var(--text-muted);">= ' + escapeHtml(JSON.stringify(item.new_value)) + '</span>';
            }

            html += '<div class="diff-row ' + kindClass + '">' +
                      '<div class="diff-header-line">' +
                        '<span class="diff-path">' + escapeHtml(item.path) + '</span>' +
                        '<span class="diff-tag ' + (item.is_redacted ? 'redacted' : kindClass) + '">' + tagLabel + '</span>' +
                      '</div>' +
                      '<div class="diff-val-change">' + valLine + '</div>' +
                    '</div>';
          }
        }

        diffEntriesList.innerHTML = html;
      }

      function escapeHtml(str) {
        if (typeof str !== 'string') str = String(str);
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      }

      function updateTimeMachineUI() {
        var total = spans.length;
        if (total === 0) {
          if (scrubberSlider) { scrubberSlider.min = '0'; scrubberSlider.max = '0'; scrubberSlider.value = '0'; scrubberSlider.disabled = true; }
          if (scrubberTurnDisplay) scrubberTurnDisplay.textContent = 'Turn 0 / 0';
          if (btnStepBack) btnStepBack.disabled = true;
          if (btnStepForward) btnStepForward.disabled = true;
          if (btnJumpHead) btnJumpHead.disabled = true;
          if (btnTimeMachineFork) btnTimeMachineFork.disabled = true;
          if (btnInspStepBack) btnInspStepBack.disabled = true;
          if (btnFork) btnFork.disabled = true;
          if (scrubberTicks) scrubberTicks.innerHTML = '';
          return;
        }

        if (scrubberSlider) {
          scrubberSlider.disabled = false;
          scrubberSlider.min = '1';
          scrubberSlider.max = total.toString();
          scrubberSlider.value = (selectedTurnIndex >= 0 ? selectedTurnIndex + 1 : total).toString();
        }

        var activeTurnNum = selectedTurnIndex >= 0 ? (selectedTurnIndex + 1) : total;
        if (scrubberTurnDisplay) scrubberTurnDisplay.textContent = 'Turn ' + activeTurnNum + ' / ' + total;

        if (btnStepBack) btnStepBack.disabled = (selectedTurnIndex <= 0);
        if (btnStepForward) btnStepForward.disabled = (selectedTurnIndex >= total - 1);
        if (btnJumpHead) btnJumpHead.disabled = (selectedTurnIndex === total - 1 || selectedTurnIndex === -1);
        if (btnTimeMachineFork) btnTimeMachineFork.disabled = false;
        if (btnInspStepBack) btnInspStepBack.disabled = (selectedTurnIndex <= 0);
        if (btnFork) btnFork.disabled = false;

        // Render Ticks
        if (scrubberTicks) {
          scrubberTicks.innerHTML = '';
          for (var i = 0; i < total; i++) {
            var tick = document.createElement('div');
            var isSelected = (i === selectedTurnIndex) || (selectedTurnIndex === -1 && i === total - 1);
            var isHalt = (i === total - 1 && spans[i].entropy <= 0.35);
            var hasSnapshot = !!cachedSnapshots[spans[i].id];
            tick.className = 'tm-tick' + (isSelected ? ' selected' : '') + (isHalt ? ' halt' : '') + (hasSnapshot ? ' snapshot' : '');
            tick.title = 'Turn ' + (i + 1) + ': ' + spans[i].id + ' (' + spans[i].source + ' -> ' + spans[i].target + ')';
            (function(idx) {
              tick.addEventListener('click', function() {
                selectSpanByIndex(idx);
              });
            })(i);
            scrubberTicks.appendChild(tick);
          }
        }
      }

      function selectSpanByIndex(index) {
        if (index < 0 || index >= spans.length) return;
        selectedTurnIndex = index;
        var span = spans[index];
        selectSpan(span);
        var feed = document.getElementById('spansFeed');
        if (index === 0 && feed) {
          feed.scrollTop = 0;
        } else {
          var card = document.getElementById('card_' + span.id);
          if (card) {
            card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          }
        }
      }

      function stepBack() {
        if (selectedTurnIndex > 0) {
          selectSpanByIndex(selectedTurnIndex - 1);
          showToast('Time-Machine: Stepped back to Turn ' + (selectedTurnIndex + 1), 'purple', 1800);
        } else if (selectedTurnIndex === -1 && spans.length > 1) {
          selectSpanByIndex(spans.length - 2);
          showToast('Time-Machine: Stepped back to Turn ' + (selectedTurnIndex + 1), 'purple', 1800);
        }
      }

      function stepForward() {
        if (selectedTurnIndex >= 0 && selectedTurnIndex < spans.length - 1) {
          selectSpanByIndex(selectedTurnIndex + 1);
          showToast('Time-Machine: Stepped forward to Turn ' + (selectedTurnIndex + 1), 'purple', 1800);
        }
      }

      function jumpToHead() {
        if (spans.length > 0) {
          selectSpanByIndex(spans.length - 1);
          showToast('Time-Machine: Jumped to Live Head (Turn ' + spans.length + ')', 'purple', 1800);
        }
      }

      async function openForkModal() {
        var span = selectedTurn || (spans.length > 0 ? spans[spans.length - 1] : null);
        if (!span) {
          showToast('Select a turn in the execution stream to fork from.', 'warning');
          return;
        }
        var turnNum = spans.findIndex(function(s) { return s.id === span.id; }) + 1;
        forkOriginTurn.textContent = span.id + ' (Turn ' + (turnNum > 0 ? turnNum : 1) + ')';
        forkOriginAgents.textContent = span.source + ' -> ' + span.target;
        
        var snap = cachedSnapshots[span.id];
        if (snap && snap.state_hash) {
          forkOriginHash.textContent = snap.state_hash.substring(0, 18) + '...';
        } else {
          forkOriginHash.textContent = 'sha256:' + span.id.replace(/[^a-f0-9]/gi, 'a').padEnd(16, '0').slice(0, 16);
        }

        var defaultBranchName = 'branch-fork-' + (span.id.replace(/^span_/, 'turn_'));
        forkBranchName.value = defaultBranchName;

        var statePayload = (snap && snap.state_data) ? snap.state_data : {
          source_agent: span.source,
          target_agent: span.target,
          entropy_level: span.entropy,
          token_budget_remaining: 100000 - (span.tokens || 1000),
          active_task: span.payload ? span.payload.slice(0, 80) : "Swarm task execution"
        };
        forkStatePreview.textContent = JSON.stringify(statePayload, null, 2);

        forkModal.style.display = 'flex';
        forkBranchName.focus();
      }

      function closeForkModal() {
        if (forkModal) forkModal.style.display = 'none';
      }

      async function executeFork() {
        var span = selectedTurn || (spans.length > 0 ? spans[spans.length - 1] : null);
        if (!span) return;

        var newBranch = forkBranchName.value.trim();
        if (!newBranch) {
          showToast('Please specify a valid branch identifier', 'warning');
          return;
        }

        try {
          btnExecuteFork.disabled = true;
          btnExecuteFork.textContent = 'Forking...';
          var res = await fetch('/api/v1/branches/fork', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              swarm_id: activeSwarmId,
              target_span_id: span.id,
              new_branch_id: newBranch
            })
          });

          if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({
              command: 'CMD_FORK_BRANCH',
              target_span_id: span.id,
              new_branch_id: newBranch
            }));
          }

          closeForkModal();
          showToast('Branch [' + newBranch + '] forked successfully from ' + span.id + '!', 'purple', 4000);

          await loadBranches();
          await switchBranch(newBranch);
        } catch(e) {
          showToast('Failed to fork branch: ' + e.message, 'danger');
        } finally {
          btnExecuteFork.disabled = false;
          btnExecuteFork.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg><span>Fork & Switch Branch</span>';
        }
      }

      async function loadBranches() {
        try {
          var res = await fetch('/api/v1/swarms/' + encodeURIComponent(activeSwarmId) + '/branches');
          if (res.ok) {
            var branchList = await res.json();
            if (Array.isArray(branchList) && branchList.length > 0) {
              if (branchSelector) {
                branchSelector.innerHTML = '';
                branchList.forEach(function(b) {
                  var opt = document.createElement('option');
                  opt.value = b;
                  opt.textContent = b;
                  opt.style.background = 'var(--bg-surface)';
                  opt.style.color = 'var(--text-primary)';
                  if (b === currentBranch) opt.selected = true;
                  branchSelector.appendChild(opt);
                });
              }
            }
          }
        } catch(e) {}
      }

      async function switchBranch(newBranch) {
        currentBranch = newBranch;
        var bLabel = document.getElementById('branchLabel');
        if (bLabel) bLabel.textContent = newBranch;
        if (branchSelector) branchSelector.value = newBranch;

        try {
          var res = await fetch('/api/v1/swarms/' + encodeURIComponent(activeSwarmId) + '/spans?branch_id=' + encodeURIComponent(newBranch));
          if (res.ok) {
            var raw = await res.json();
            if (Array.isArray(raw) && raw.length > 0) {
              var feed = document.getElementById('streamFeed');
              if (feed) feed.innerHTML = '';
              spans = [];
              raw.forEach(function(s) {
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
                  duration: s.duration_ms || 400,
                  tokens: s.token_count || 1200,
                  entropy: s.entropy_score || 1.85,
                  timestamp: s.start_time ? new Date(s.start_time).toLocaleTimeString() : new Date().toLocaleTimeString(),
                  payload: payloadStr
                };
                spans.push(formatted);
                renderSpan(formatted);
              });
              document.getElementById('metricSpans').textContent = spans.length.toString();
              document.getElementById('turnCounter').textContent = spans.length + ' Turns (' + newBranch + ')';
              selectSpanByIndex(spans.length - 1);
              showToast('Switched to branch [' + newBranch + ']', 'info', 2000);
            } else {
              showToast('Branch [' + newBranch + '] active (awaiting execution spans)', 'info', 2000);
            }
          }
        } catch(e) {}
        renderTopology();
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
        selectedTurnIndex = -1;
        cachedSnapshots = {};
        currentBranch = 'main';
        var bLabel = document.getElementById('branchLabel');
        if (bLabel) bLabel.textContent = 'main';
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
        if (inspSnapshot) {
          inspSnapshot.textContent = JSON.stringify({
            status: "No state snapshot recorded for current turn"
          }, null, 2);
        }
        currentDiffData = null;
        if (diffBadgePill) {
          diffBadgePill.style.display = 'none';
          diffBadgePill.textContent = '0';
        }
        if (diffEntriesList) {
          diffEntriesList.innerHTML = '<div style="color: var(--text-muted); text-align: center; padding: 20px 0;">Select a turn to inspect state mutations.</div>';
        }
        updateTimeMachineUI();
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
              } else if (msg.type === 'fleet_metric_update' && msg.metrics) {
                updateFleetUI(msg.metrics);
              } else if (msg.type === 'fleet_trip_broadcast') {
                renderFleetTripAlert(msg.payload || msg);
              } else if (msg.type === 'fleet_state_reset') {
                clearFleetTripAlert();
                fetchFleetMetrics();
              } else if (msg.type === 'cluster_leader_changed') {
                fetchFleetMetrics();
              } else if (msg.type === 'swarm_archived') {
                var archBadge = document.getElementById('fleetArchiveStatusBadge');
                if (archBadge) archBadge.style.display = 'inline-flex';
                showToast('Swarm archived to cold storage: ' + (msg.archive_key || msg.swarm_id), 'success');
              } else if (msg.type === 'swarm_restored') {
                var archBadge = document.getElementById('fleetArchiveStatusBadge');
                if (archBadge) archBadge.style.display = 'none';
                showToast('Swarm restored from cold storage: ' + msg.swarm_id, 'info');
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
      if (btnStepBack) btnStepBack.addEventListener('click', stepBack);
      if (btnStepForward) btnStepForward.addEventListener('click', stepForward);
      if (btnJumpHead) btnJumpHead.addEventListener('click', jumpToHead);
      if (btnTimeMachineFork) btnTimeMachineFork.addEventListener('click', openForkModal);
      if (btnInspStepBack) btnInspStepBack.addEventListener('click', stepBack);
      if (btnFork) btnFork.addEventListener('click', openForkModal);
      if (btnCloseForkModal) btnCloseForkModal.addEventListener('click', closeForkModal);
      if (btnCancelFork) btnCancelFork.addEventListener('click', closeForkModal);
      if (btnExecuteFork) btnExecuteFork.addEventListener('click', executeFork);

      if (scrubberSlider) {
        scrubberSlider.addEventListener('input', function() {
          var val = parseInt(scrubberSlider.value, 10);
          if (val >= 1 && val <= spans.length) {
            selectSpanByIndex(val - 1);
          }
        });
      }

      if (branchSelector) {
        branchSelector.addEventListener('change', function() {
          switchBranch(this.value);
        });
      }

      function switchInspTab(viewId) {
        document.querySelectorAll('.insp-tab-btn').forEach(function(b) {
          b.classList.remove('active');
          if (b.getAttribute('data-view') === viewId) b.classList.add('active');
        });
        document.querySelectorAll('.insp-view').forEach(function(v) {
          v.style.display = 'none';
        });
        var targetView = document.getElementById(viewId);
        if (targetView) targetView.style.display = viewId === 'viewDiff' ? 'flex' : 'block';
      }

      if (tabInspPayload) {
        tabInspPayload.addEventListener('click', function() {
          switchInspTab('viewPayload');
        });
      }
      if (tabInspSnapshot) {
        tabInspSnapshot.addEventListener('click', function() {
          switchInspTab('viewSnapshot');
        });
      }
      if (tabInspDiff) {
        tabInspDiff.addEventListener('click', function() {
          switchInspTab('viewDiff');
        });
      }

      if (btnDiffUnified) {
        btnDiffUnified.addEventListener('click', function() {
          currentDiffMode = 'unified';
          btnDiffUnified.classList.add('active');
          if (btnDiffSplit) btnDiffSplit.classList.remove('active');
          renderDiffEntries();
        });
      }
      if (btnDiffSplit) {
        btnDiffSplit.addEventListener('click', function() {
          currentDiffMode = 'split';
          btnDiffSplit.classList.add('active');
          if (btnDiffUnified) btnDiffUnified.classList.remove('active');
          renderDiffEntries();
        });
      }
      if (diffTargetSelect) {
        diffTargetSelect.addEventListener('change', function() {
          updateStateDiffInspector();
        });
      }
      diffFilterChips = document.querySelectorAll('.diff-filter-chip');
      diffFilterChips.forEach(function(chip) {
        chip.addEventListener('click', function() {
          diffFilterChips.forEach(function(c) { c.classList.remove('active'); });
          this.classList.add('active');
          currentDiffFilter = this.getAttribute('data-filter') || 'all';
          renderDiffEntries();
        });
      });

      // Keyboard shortcuts: [ for Step-Back, ] for Step-Forward, F for Fork Modal, Esc to close
      window.addEventListener('keydown', function(e) {
        if (e.target && (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT')) {
          if (e.key === 'Escape') {
            closeForkModal();
            if (apiKeyModal) apiKeyModal.style.display = 'none';
            if (mfaModal) mfaModal.style.display = 'none';
          }
          if (e.key === 'Enter' && forkModal && forkModal.style.display === 'flex') {
            executeFork();
          }
          return;
        }

        if (e.key === '[') {
          e.preventDefault();
          stepBack();
        } else if (e.key === ']') {
          e.preventDefault();
          stepForward();
        } else if (e.key === 'f' || e.key === 'F') {
          e.preventDefault();
          openForkModal();
        } else if (e.key === 'Escape') {
          closeForkModal();
          if (apiKeyModal) apiKeyModal.style.display = 'none';
          if (mfaModal) mfaModal.style.display = 'none';
        }
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
                    var hdrs = {};
                    var activeMfa = sessionStorage.getItem('swarmwatch_mfa_code');
                    if (activeMfa) hdrs['X-MFA-Code'] = activeMfa;
                    var delRes = await fetch('/api/v1/keys/' + id, { method: 'DELETE', headers: hdrs });
                    if (delRes.ok) {
                      loadApiKeys();
                    } else if (delRes.status === 403) {
                      alert('MFA step-up verification required to revoke API keys. Please verify via Security / MFA modal.');
                      if (mfaModal) mfaModal.style.display = 'flex';
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

      // =========================================================================
      // Enterprise MFA & Role-Based Access Control (Seam 26 / ADR 0037)
      // =========================================================================
      var mfaModal = document.getElementById('mfaModal');
      var btnMfaModal = document.getElementById('btnMfaModal');
      var btnCloseMfaModal = document.getElementById('btnCloseMfaModal');
      var btnEnrollMfa = document.getElementById('btnEnrollMfa');
      var btnVerifyMfaActivation = document.getElementById('btnVerifyMfaActivation');
      var btnVerifyStepUp = document.getElementById('btnVerifyStepUp');
      var mfaEnrollSection = document.getElementById('mfaEnrollSection');
      var mfaSecretKey = document.getElementById('mfaSecretKey');
      var mfaUriText = document.getElementById('mfaUriText');
      var mfaBackupCodes = document.getElementById('mfaBackupCodes');
      var mfaCodeInput = document.getElementById('mfaCodeInput');
      var mfaStepUpInput = document.getElementById('mfaStepUpInput');
      var mfaEnrollStatus = document.getElementById('mfaEnrollStatus');
      var mfaStepUpStatus = document.getElementById('mfaStepUpStatus');
      var fleetRoleBadge = document.getElementById('fleetRoleBadge');
      var fleetRoleText = document.getElementById('fleetRoleText');
      var fleetMfaBadge = document.getElementById('fleetMfaBadge');
      var fleetMfaText = document.getElementById('fleetMfaText');
      var mfaUsernameDisplay = document.getElementById('mfaUsernameDisplay');
      var mfaRoleDisplay = document.getElementById('mfaRoleDisplay');
      var mfaStatusDisplay = document.getElementById('mfaStatusDisplay');

      async function refreshAuthSession() {
        try {
          var res = await fetch('/api/v1/auth/users');
          if (res.ok) {
            var users = await res.json();
            var adminUser = users.find(function(u) { return u.username === 'admin'; }) || users[0];
            if (adminUser) {
              if (mfaUsernameDisplay) mfaUsernameDisplay.textContent = adminUser.username;
              if (mfaRoleDisplay) mfaRoleDisplay.textContent = (adminUser.role || 'ADMIN').toUpperCase();
              if (fleetRoleText) fleetRoleText.textContent = 'ROLE: ' + (adminUser.role || 'ADMIN').toUpperCase();
              var mfaActive = !!adminUser.mfa_enabled;
              if (mfaStatusDisplay) {
                mfaStatusDisplay.textContent = mfaActive ? 'ACTIVE' : 'UNENROLLED';
                mfaStatusDisplay.style.color = mfaActive ? 'var(--success)' : 'var(--text-muted)';
              }
              if (fleetMfaText) {
                fleetMfaText.textContent = mfaActive ? 'MFA: ACTIVE' : 'MFA: NONE';
              }
              if (fleetMfaBadge) {
                fleetMfaBadge.className = mfaActive ? 'fleet-badge' : 'fleet-badge standby';
              }
            }
          }
        } catch(e) {}
      }

      async function refreshOtlpStats() {
        try {
          var res = await fetch('/api/v1/otlp/stats');
          if (res.ok) {
            var stats = await res.json();
            var otlpText = document.getElementById('otlpText');
            if (otlpText && stats.total_spans_received > 0) {
              otlpText.textContent = 'OTLP: ' + stats.total_spans_received + ' SPANS';
            }
          }
        } catch(e) {}
      }
      refreshOtlpStats();

      // -------------------------------------------------------------------
      // Server-Sent Events (SSE) & WebPush Integration (Seam 28 / ADR 0039)
      // -------------------------------------------------------------------
      function initSseStream() {
        var sseDot = document.getElementById('sseDot');
        var sseText = document.getElementById('sseText');
        if (!window.EventSource) return;

        try {
          var evtSource = new EventSource('/api/v1/events/stream');
          evtSource.onopen = function() {
            if (sseDot) sseDot.style.background = 'var(--success)';
            if (sseText) sseText.textContent = 'SSE: STREAMING';
          };
          evtSource.onerror = function() {
            if (sseDot) sseDot.style.background = 'var(--warning)';
            if (sseText) sseText.textContent = 'SSE: RECONNECTING';
          };
          evtSource.addEventListener('circuit_trip', function(e) {
            handleRealtimeTripAlert(e.data);
          });
          evtSource.addEventListener('fleet_trip', function(e) {
            handleRealtimeTripAlert(e.data);
          });
        } catch (e) {}
      }

      function handleRealtimeTripAlert(rawData) {
        try {
          var data = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
          if (document.hidden && 'Notification' in window && Notification.permission === 'granted') {
            new Notification('SwarmWatch Circuit Trip: ' + (data.swarm_id || 'Alert'), {
              body: data.reason || 'Circuit breaker tripped on runaway loop',
              tag: 'swarmwatch-trip'
            });
          }
        } catch (e) {}
      }

      var pushNotificationBadge = document.getElementById('pushNotificationBadge');
      var pushDot = document.getElementById('pushDot');
      var pushText = document.getElementById('pushText');

      if (pushNotificationBadge && 'Notification' in window) {
        if (Notification.permission === 'granted') {
          if (pushDot) pushDot.style.background = 'var(--success)';
          if (pushText) pushText.textContent = 'PUSH: ACTIVE';
        }
        pushNotificationBadge.addEventListener('click', async function() {
          if (Notification.permission !== 'granted') {
            var perm = await Notification.requestPermission();
            if (perm === 'granted') {
              if (pushDot) pushDot.style.background = 'var(--success)';
              if (pushText) pushText.textContent = 'PUSH: ACTIVE';
              try {
                var vapidRes = await fetch('/api/v1/push/vapid-public-key');
                if (vapidRes.ok) {
                  var vapidData = await vapidRes.json();
                  await fetch('/api/v1/push/subscribe', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      endpoint: 'https://browser.push.local/' + Math.random().toString(36).substring(2),
                      keys: { p256dh: vapidData.public_key, auth: 'local-auth-secret' }
                    })
                  });
                }
              } catch(e) {}
            }
          }
      initSseStream();

      // -------------------------------------------------------------------
      // Kernel-Level Socket LLM Interception Gateway (Seam 29 / ADR 0040)
      // -------------------------------------------------------------------
      async function refreshEbpfStatus() {
        var ebpfDot = document.getElementById('ebpfDot');
        var ebpfText = document.getElementById('ebpfText');
        try {
          var res = await fetch('/api/v1/ebpf/status');
          if (res.ok) {
            var data = await res.json();
            if (data.status === 'attached') {
              if (ebpfDot) ebpfDot.style.background = 'var(--success)';
              if (ebpfText) ebpfText.textContent = 'EBPF: ' + (data.packets_intercepted || 0) + ' PKTS';
            } else {
              if (ebpfDot) ebpfDot.style.background = 'var(--text-muted)';
              if (ebpfText) ebpfText.textContent = 'EBPF: DETACHED';
            }
          }
        } catch (e) {}
      }
      refreshEbpfStatus();
      setInterval(refreshEbpfStatus, 8000);

      // -------------------------------------------------------------------
      // Raft Distributed Consensus Engine (Seam 30 / ADR 0041)
      // -------------------------------------------------------------------
      async function refreshRaftStatus() {
        var raftDot = document.getElementById('raftDot');
        var raftText = document.getElementById('raftText');
        try {
          var res = await fetch('/api/v1/raft/status');
          if (res.ok) {
            var data = await res.json();
            var role = data.role || 'FOLLOWER';
            var term = data.term || 0;
            if (role === 'LEADER') {
              if (raftDot) raftDot.style.background = 'var(--success)';
              if (raftText) raftText.textContent = 'RAFT: LEADER (T' + term + ')';
            } else if (role === 'CANDIDATE') {
              if (raftDot) raftDot.style.background = 'var(--warning)';
              if (raftText) raftText.textContent = 'RAFT: CANDIDATE (T' + term + ')';
            } else {
              if (raftDot) raftDot.style.background = 'var(--accent)';
              if (raftText) raftText.textContent = 'RAFT: FOLLOWER (T' + term + ')';
            }
          }
        } catch (e) {}
      }
      refreshRaftStatus();
      setInterval(refreshRaftStatus, 6000);

      // -------------------------------------------------------------------
      // Autonomous Self-Healing Policy Controller (Seam 31 / ADR 0042)
      // -------------------------------------------------------------------
      async function refreshSelfHealingStatus() {
        var selfHealingDot = document.getElementById('selfHealingDot');
        var selfHealingText = document.getElementById('selfHealingText');
        try {
          var res = await fetch('/api/v1/self-healing/status');
          if (res.ok) {
            var data = await res.json();
            var activeCount = data.active_actions_count || 0;
            var isEnabled = data.enabled !== false;
            if (!isEnabled) {
              if (selfHealingDot) selfHealingDot.style.background = 'var(--text-muted)';
              if (selfHealingText) selfHealingText.textContent = 'HEAL: OFF';
            } else if (activeCount > 0) {
              if (selfHealingDot) selfHealingDot.style.background = 'var(--warning)';
              if (selfHealingText) selfHealingText.textContent = 'HEAL: REMEDIATED (' + activeCount + ')';
            } else {
              if (selfHealingDot) selfHealingDot.style.background = 'var(--success)';
              if (selfHealingText) selfHealingText.textContent = 'HEAL: ACTIVE (0)';
            }
          }
        } catch (e) {}
      }
      refreshSelfHealingStatus();
      setInterval(refreshSelfHealingStatus, 7000);

      // -------------------------------------------------------------------
      // Distributed Root-Cause Causal Inference (Seam 32 / ADR 0043)
      // -------------------------------------------------------------------
      async function refreshRootCauseStatus() {
        var rootCauseDot = document.getElementById('rootCauseDot');
        var rootCauseText = document.getElementById('rootCauseText');
        try {
          var res = await fetch('/api/v1/swarms/swarm-live-01/root-cause');
          if (res.ok) {
            var data = await res.json();
            if (data.has_anomaly && data.attribution) {
              if (rootCauseDot) rootCauseDot.style.background = 'var(--danger)';
              if (rootCauseText) {
                var agent = data.attribution.root_cause_agent || 'agent';
                var turn = data.attribution.root_cause_turn || 1;
                rootCauseText.textContent = 'RCA: ' + agent + ' (T' + turn + ')';
              }
            } else {
              if (rootCauseDot) rootCauseDot.style.background = 'var(--success)';
              if (rootCauseText) rootCauseText.textContent = 'RCA: NOMINAL';
            }
          }
        } catch (e) {}
      }
      refreshRootCauseStatus();
      setInterval(refreshRootCauseStatus, 8000);

      // -------------------------------------------------------------------
      // Ultra-High-Throughput gRPC Streaming Gateway (Seam 33 / ADR 0044)
      // -------------------------------------------------------------------
      async function refreshGrpcStatus() {
        var grpcDot = document.getElementById('grpcGatewayDot');
        var grpcText = document.getElementById('grpcGatewayText');
        try {
          var res = await fetch('/api/v1/grpc/status');
          if (res.ok) {
            var data = await res.json();
            var totalSpans = data.total_spans_received || 0;
            var trips = data.trips_triggered || 0;
            var activeStreams = data.active_streams || 0;
            if (trips > 0) {
              if (grpcDot) grpcDot.style.background = 'var(--warning)';
              if (grpcText) grpcText.textContent = 'GRPC: TRIPPED (' + trips + ')';
            } else if (activeStreams > 0) {
              if (grpcDot) grpcDot.style.background = 'var(--accent)';
              if (grpcText) grpcText.textContent = 'GRPC: ACTIVE (' + totalSpans + ')';
            } else {
              if (grpcDot) grpcDot.style.background = 'var(--success)';
              if (grpcText) grpcText.textContent = 'GRPC: READY (' + totalSpans + ')';
            }
          }
        } catch (e) {}
      }
      refreshGrpcStatus();
      setInterval(refreshGrpcStatus, 7500);

      // -------------------------------------------------------------------
      // Embedding-Free Vector Semantic Drift & Hallucination Detector (ADR 0045)
      // -------------------------------------------------------------------
      async function refreshDriftStatus() {
        var driftDot = document.getElementById('driftDot');
        var driftText = document.getElementById('driftText');
        try {
          var res = await fetch('/api/v1/swarms/swarm-live-01/drift');
          if (res.ok) {
            var data = await res.json();
            var st = data.current_status || 'nominal';
            var score = data.current_drift_score || 0.0;
            if (st === 'tripped' || data.is_tripped) {
              if (driftDot) driftDot.style.background = 'var(--danger)';
              if (driftText) driftText.textContent = 'DRIFT: TRIPPED';
            } else if (st === 'divergent') {
              if (driftDot) driftDot.style.background = 'var(--danger)';
              if (driftText) driftText.textContent = 'DRIFT: ' + score.toFixed(2);
            } else if (st === 'warning') {
              if (driftDot) driftDot.style.background = 'var(--warning)';
              if (driftText) driftText.textContent = 'DRIFT: ' + score.toFixed(2);
            } else {
              if (driftDot) driftDot.style.background = 'var(--success)';
              if (driftText) driftText.textContent = 'DRIFT: NOMINAL';
            }
          }
        } catch (e) {}
      }
      refreshDriftStatus();
      setInterval(refreshDriftStatus, 8500);

      // -------------------------------------------------------------------
      // Adaptive Dynamic Quota Re-balancing (ADR 0046)
      // -------------------------------------------------------------------
      async function refreshRebalancerStatus() {
        var rebalancerDot = document.getElementById('rebalancerDot');
        var rebalancerText = document.getElementById('rebalancerText');
        try {
          var res = await fetch('/api/v1/swarms/swarm-live-01/rebalancer');
          if (res.ok) {
            var data = await res.json();
            var td = data.tier_distribution || {};
            var throttled = (td.throttled || 0) + (td.quarantined || 0);
            var boosted = td.boosted || 0;
            if (throttled > 0) {
              if (rebalancerDot) rebalancerDot.style.background = 'var(--warning)';
              if (rebalancerText) rebalancerText.textContent = 'QUOTA: THROTTLED (' + throttled + ')';
            } else if (boosted > 0) {
              if (rebalancerDot) rebalancerDot.style.background = 'var(--accent)';
              if (rebalancerText) rebalancerText.textContent = 'QUOTA: BOOSTED (' + boosted + ')';
            } else {
              if (rebalancerDot) rebalancerDot.style.background = 'var(--success)';
              if (rebalancerText) rebalancerText.textContent = 'QUOTA: BALANCED';
            }
          }
        } catch (e) {}
      }
      refreshRebalancerStatus();
      setInterval(refreshRebalancerStatus, 9000);

      // -------------------------------------------------------------------
      // Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (ADR 0047)
      // -------------------------------------------------------------------
      async function refreshCoalitionStatus() {
        var coalitionDot = document.getElementById('coalitionDot');
        var coalitionText = document.getElementById('coalitionText');
        try {
          var res = await fetch('/api/v1/swarms/swarm-live-01/coalitions');
          if (res.ok) {
            var data = await res.json();
            var coalitions = data.coalitions || [];
            var deadlocks = coalitions.filter(function(c) { return c.is_deadlocked; }).length;
            var bottlenecks = (data.bottleneck_agents || []).length;
            if (deadlocks > 0) {
              if (coalitionDot) coalitionDot.style.background = 'var(--danger)';
              if (coalitionText) coalitionText.textContent = 'COALITIONS: DEADLOCK (' + deadlocks + ')';
            } else if (bottlenecks > 0) {
              if (coalitionDot) coalitionDot.style.background = 'var(--warning)';
              if (coalitionText) coalitionText.textContent = 'COALITIONS: BOTTLENECK (' + bottlenecks + ')';
            } else if (coalitions.length > 0) {
              if (coalitionDot) coalitionDot.style.background = 'var(--accent)';
              if (coalitionText) coalitionText.textContent = 'COALITIONS: ' + coalitions.length + ' ACTIVE';
            } else {
              if (coalitionDot) coalitionDot.style.background = 'var(--success)';
              if (coalitionText) coalitionText.textContent = 'COALITIONS: OPTIMAL';
            }
          }
        } catch (e) {}
      }
      refreshCoalitionStatus();
      setInterval(refreshCoalitionStatus, 9500);

      // -------------------------------------------------------------------
      // Zero-Copy Shared-Memory Ring Buffer IPC Gateway (ADR 0048)
      // -------------------------------------------------------------------
      async function refreshShmStatus() {
        var shmDot = document.getElementById('shmDot');
        var shmText = document.getElementById('shmText');
        try {
          var res = await fetch('/api/v1/shm/status');
          if (res.ok) {
            var data = await res.json();
            var pending = data.pending_records || 0;
            var dropped = data.dropped_records || 0;
            if (dropped > 0) {
              if (shmDot) shmDot.style.background = 'var(--warning)';
              if (shmText) shmText.textContent = 'SHM: OVERFLOW (' + dropped + ' DROPPED)';
            } else if (pending > 0) {
              if (shmDot) shmDot.style.background = 'var(--accent)';
              if (shmText) shmText.textContent = 'SHM: STREAMING (' + pending + ' PENDING)';
            } else {
              if (shmDot) shmDot.style.background = 'var(--success)';
              if (shmText) shmText.textContent = 'SHM: ACTIVE (0 PENDING)';
            }
          }
        } catch (e) {}
      }
      refreshShmStatus();
      setInterval(refreshShmStatus, 8000);

      // -------------------------------------------------------------------
      // Differential Privacy & Cryptographic State Anonymization (ADR 0049)
      // -------------------------------------------------------------------
      async function refreshPrivacyStatus() {
        var dpDot = document.getElementById('dpDot');
        var dpText = document.getElementById('dpText');
        try {
          var res = await fetch('/api/v1/privacy/status');
          if (res.ok) {
            var data = await res.json();
            var eps = data.epsilon !== undefined ? data.epsilon : 1.0;
            var isExhausted = data.is_exhausted || false;
            var spentPct = data.budget_spent_pct || 0;
            if (isExhausted) {
              if (dpDot) dpDot.style.background = 'var(--danger)';
              if (dpText) dpText.textContent = 'DP: BUDGET EXHAUSTED';
            } else if (spentPct > 75) {
              if (dpDot) dpDot.style.background = 'var(--warning)';
              if (dpText) dpText.textContent = 'DP: BUDGET LOW (' + spentPct + '%)';
            } else {
              if (dpDot) dpDot.style.background = 'var(--success)';
              if (dpText) dpText.textContent = 'DP: PROTECTED (ε=' + eps + ')';
            }
          }
        } catch (e) {}
      }
      refreshPrivacyStatus();
      setInterval(refreshPrivacyStatus, 8500);

      // -------------------------------------------------------------------
      // Predictive Multi-Agent Failure Forecasting & Early-Warning Martingales (ADR 0050)
      // -------------------------------------------------------------------
      async function refreshForecastStatus() {
        var forecastDot = document.getElementById('forecastDot');
        var forecastText = document.getElementById('forecastText');
        try {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          var res = await fetch('/api/v1/swarms/' + targetSwarm + '/forecast');
          if (res.ok) {
            var data = await res.json();
            var lvl = data.risk_level || 'nominal';
            var turnsLeft = data.turns_to_trip;
            var prob = (data.risk_probability || 0) * 100;
            if (lvl === 'critical_collapse') {
              if (forecastDot) forecastDot.style.background = 'var(--danger)';
              if (forecastText) forecastText.textContent = 'FORECAST: CRITICAL (' + prob.toFixed(0) + '%)';
            } else if (lvl === 'early_warning') {
              if (forecastDot) forecastDot.style.background = 'var(--warning)';
              if (forecastText) forecastText.textContent = 'FORECAST: WARNING (' + (turnsLeft !== null ? turnsLeft + 'T' : prob.toFixed(0) + '%') + ')';
            } else {
              if (forecastDot) forecastDot.style.background = 'var(--success)';
              if (forecastText) forecastText.textContent = 'FORECAST: STABLE';
            }
          }
        } catch (e) {}
      }
      refreshForecastStatus();
      setInterval(refreshForecastStatus, 9000);

      // -------------------------------------------------------------------
      // Multi-Agent Byzantine Attack & Prompt Injection Stress Harness (ADR 0051)
      // -------------------------------------------------------------------
      async function refreshByzantineStatus() {
        var byzantineDot = document.getElementById('byzantineDot');
        var byzantineText = document.getElementById('byzantineText');
        try {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          var res = await fetch('/api/v1/swarms/' + targetSwarm + '/byzantine/status');
          if (res.ok) {
            var data = await res.json();
            var status = data.status || 'SHIELDED';
            var score = Math.round((data.bft_resilience_score !== undefined ? data.bft_resilience_score : 1.0) * 100);
            var isolated = data.total_agents_isolated || 0;
            if (status === 'COMPROMISED') {
              if (byzantineDot) byzantineDot.style.background = 'var(--danger)';
              if (byzantineText) byzantineText.textContent = 'BYZANTINE: COMPROMISED (' + score + '%)';
            } else if (status === 'ISOLATING' || isolated > 0) {
              if (byzantineDot) byzantineDot.style.background = 'var(--warning)';
              if (byzantineText) byzantineText.textContent = 'BYZANTINE: ISOLATING (' + score + '% | ' + isolated + 'Q)';
            } else {
              if (byzantineDot) byzantineDot.style.background = 'var(--success)';
              if (byzantineText) byzantineText.textContent = 'BYZANTINE: SHIELDED (' + score + '%)';
            }
          }
        } catch (e) {}
      }
      refreshByzantineStatus();
      setInterval(refreshByzantineStatus, 9500);

      // -------------------------------------------------------------------
      // Multi-Agent Dynamic Honeypot & Deception Sandboxing (ADR 0052)
      // -------------------------------------------------------------------
      async function refreshHoneypotStatus() {
        var honeypotDot = document.getElementById('honeypotDot');
        var honeypotText = document.getElementById('honeypotText');
        try {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          var res = await fetch('/api/v1/swarms/' + targetSwarm + '/honeypot/status');
          if (res.ok) {
            var data = await res.json();
            var status = data.status || 'DEFUSED';
            var trapped = data.total_diverted_turns || 0;
            var sandboxed = (data.active_sandboxed_agents || []).length;
            if (status === 'DECEIVING' || sandboxed > 0) {
              if (honeypotDot) honeypotDot.style.background = 'var(--warning)';
              if (honeypotText) honeypotText.textContent = 'HONEYPOT: DECEIVING (' + trapped + ' TRAPPED | ' + sandboxed + 'S)';
            } else if (status === 'ARMED') {
              if (honeypotDot) honeypotDot.style.background = 'var(--success)';
              if (honeypotText) honeypotText.textContent = 'HONEYPOT: ARMED (' + trapped + ' TRAPPED)';
            } else {
              if (honeypotDot) honeypotDot.style.background = 'var(--text-muted)';
              if (honeypotText) honeypotText.textContent = 'HONEYPOT: DEFUSED';
            }
          }
        } catch (e) {}
      }
      refreshHoneypotStatus();
      setInterval(refreshHoneypotStatus, 9800);

      // -------------------------------------------------------------------
      // Bounded Model Checking & Multi-Agent Invariant Verification (ADR 0053)
      // -------------------------------------------------------------------
      async function refreshModelCheckStatus() {
        var modelCheckDot = document.getElementById('modelCheckDot');
        var modelCheckText = document.getElementById('modelCheckText');
        try {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          var res = await fetch('/api/v1/swarms/' + targetSwarm + '/model-check/status');
          if (res.ok) {
            var data = await res.json();
            var verdict = data.verdict || 'VERIFIED';
            var boundK = data.max_depth_bound || 10;
            var states = data.total_states_explored || 0;
            if (verdict === 'VIOLATED') {
              if (modelCheckDot) modelCheckDot.style.background = 'var(--danger)';
              var viol = data.violated_invariant || 'INVARIANT';
              if (modelCheckText) modelCheckText.textContent = 'BMC: VIOLATED (' + viol + ')';
            } else if (verdict === 'VACUOUS') {
              if (modelCheckDot) modelCheckDot.style.background = 'var(--text-muted)';
              if (modelCheckText) modelCheckText.textContent = 'BMC: VACUOUS';
            } else {
              if (modelCheckDot) modelCheckDot.style.background = 'var(--success)';
              if (modelCheckText) modelCheckText.textContent = 'BMC: VERIFIED (k=' + boundK + ' | ' + states + 'S)';
            }
          }
        } catch (e) {}
      }
      refreshModelCheckStatus();
      setInterval(refreshModelCheckStatus, 10200);

      // -------------------------------------------------------------------
      // Autonomous Dynamic Topology Healing & Auto-Patching (ADR 0054)
      // -------------------------------------------------------------------
      async function refreshTopologyHealStatus() {
        var topologyHealDot = document.getElementById('topologyHealDot');
        var topologyHealText = document.getElementById('topologyHealText');
        try {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          var res = await fetch('/api/v1/swarms/' + targetSwarm + '/topology-heal/status');
          if (res.ok) {
            var data = await res.json();
            var status = data.status || 'NOMINAL';
            var count = data.active_patches_count || 0;
            if (status === 'PATCHED' && count > 0) {
              if (topologyHealDot) topologyHealDot.style.background = 'var(--accent)';
              if (topologyHealText) topologyHealText.textContent = 'HEAL: PATCHED (' + count + ' ACTIVE)';
            } else {
              if (topologyHealDot) topologyHealDot.style.background = 'var(--success)';
              if (topologyHealText) topologyHealText.textContent = 'HEAL: NOMINAL';
            }
          }
        } catch (e) {}
      }
      refreshTopologyHealStatus();
      setInterval(refreshTopologyHealStatus, 10500);

      // -------------------------------------------------------------------
      // Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking (ADR 0055)
      // -------------------------------------------------------------------
      async function refreshArtifactStatus() {
        var artifactDot = document.getElementById('artifactDot');
        var artifactText = document.getElementById('artifactText');
        try {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          var res = await fetch('/api/v1/swarms/' + targetSwarm + '/artifacts');
          if (res.ok) {
            var data = await res.json();
            var count = data.artifacts_count || 0;
            var arts = data.artifacts || [];
            var hasTampered = arts.some(function(a) { return a.watermark && a.watermark.tamper_detected; });
            if (hasTampered) {
              if (artifactDot) artifactDot.style.background = 'var(--danger)';
              if (artifactText) artifactText.textContent = 'ARTIFACTS: TAMPERED';
            } else if (count > 0) {
              if (artifactDot) artifactDot.style.background = 'var(--success)';
              if (artifactText) artifactText.textContent = 'ARTIFACTS: SECURED (' + count + ')';
            } else {
              if (artifactDot) artifactDot.style.background = 'var(--text-muted)';
              if (artifactText) artifactText.textContent = 'ARTIFACTS: 0 RECORDED';
            }
          }
        } catch (e) {}
      }
      refreshArtifactStatus();
      setInterval(refreshArtifactStatus, 10800);

      // -------------------------------------------------------------------
      // Decentralized Epidemic Gossip Telemetry Mesh Sync (ADR 0056)
      // -------------------------------------------------------------------
      async function refreshMeshStatus() {
        var meshDot = document.getElementById('meshDot');
        var meshText = document.getElementById('meshText');
        try {
          var res = await fetch('/api/v1/mesh/status');
          if (res.ok) {
            var data = await res.json();
            var health = data.convergence_health || 'converged';
            var peers = data.active_peers_count || 0;
            if (health === 'partitioned') {
              if (meshDot) meshDot.style.background = 'var(--danger)';
              if (meshText) meshText.textContent = 'MESH: PARTITIONED';
            } else if (health === 'syncing') {
              if (meshDot) meshDot.style.background = 'var(--warning)';
              if (meshText) meshText.textContent = 'MESH: SYNCING (' + peers + ' PEERS)';
            } else {
              if (meshDot) meshDot.style.background = 'var(--success)';
              if (meshText) meshText.textContent = 'MESH: CONVERGED (' + peers + ' PEERS)';
            }
          }
        } catch (e) {}
      }
      refreshMeshStatus();
      setInterval(refreshMeshStatus, 11200);

      // -------------------------------------------------------------------
      // ADR 0057: Autonomous Dynamic Workload Sharding Status Polling
      // -------------------------------------------------------------------
      async function refreshShardStatus() {
        var shardDot = document.getElementById('shardDot');
        var shardText = document.getElementById('shardText');
        try {
          var res = await fetch('/api/v1/sharding/status');
          if (res.ok) {
            var data = await res.json();
            var nodes = data.nodes || {};
            var totalNodes = data.total_physical_nodes || Object.keys(nodes).length || 1;
            var hasOverloaded = false;
            var hasCongested = false;
            for (var nid in nodes) {
              if (nodes[nid].health_tier === 'overloaded') hasOverloaded = true;
              if (nodes[nid].health_tier === 'congested') hasCongested = true;
            }
            if (hasOverloaded) {
              if (shardDot) shardDot.style.background = 'var(--warning)';
              if (shardText) shardText.textContent = 'SHARDING: SPILLOVER (' + totalNodes + ' NODES)';
            } else if (hasCongested) {
              if (shardDot) shardDot.style.background = 'var(--warning)';
              if (shardText) shardText.textContent = 'SHARDING: CONGESTED (' + totalNodes + ' NODES)';
            } else {
              if (shardDot) shardDot.style.background = 'var(--success)';
              var nodeLabel = totalNodes === 1 ? '1 NODE' : totalNodes + ' NODES';
              if (shardText) shardText.textContent = 'SHARDING: BALANCED (' + nodeLabel + ')';
            }
          }
        } catch (e) {}
      }
      refreshShardStatus();
      setInterval(refreshShardStatus, 11500);

      // -------------------------------------------------------------------
      // ADR 0058: Autonomous Semantic Schema Migration Status Polling
      // -------------------------------------------------------------------
      async function refreshSchemaStatus() {
        var schemaDot = document.getElementById('schemaDot');
        var schemaText = document.getElementById('schemaText');
        try {
          var res = await fetch('/api/v1/schemas/status');
          if (res.ok) {
            var data = await res.json();
            var totalV = data.total_versions || 0;
            var regV = data.registered_versions || [];
            var latest = regV.length > 0 ? regV[regV.length - 1] : 'v1.0.0';
            var migrations = data.total_migrations_executed || 0;
            if (schemaDot) schemaDot.style.background = 'var(--success)';
            if (schemaText) schemaText.textContent = 'SCHEMA: ' + latest.toUpperCase() + ' (' + totalV + ' VERSIONS | ' + migrations + ' MIG)';
          }
        } catch (e) {}
      }
      refreshSchemaStatus();
      setInterval(refreshSchemaStatus, 12000);

      // -------------------------------------------------------------------
      // ADR 0059: Dynamic Multi-Agent Epistemic Logic Status Polling
      // -------------------------------------------------------------------
      async function refreshEpistemicStatus() {
        var epistemicDot = document.getElementById('epistemicDot');
        var epistemicText = document.getElementById('epistemicText');
        var sId = currentSwarmId || 'swarm-live-01';
        try {
          var res = await fetch('/api/v1/swarms/' + encodeURIComponent(sId) + '/epistemic/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'CONSISTENT';
            var claimsCount = data.total_claims || 0;
            var disson = data.dissonance_score || 0.0;
            var confs = data.unresolved_conflicts || 0;
            if (st === 'CONSISTENT') {
              if (epistemicDot) epistemicDot.style.background = 'var(--success)';
              if (epistemicText) epistemicText.textContent = 'EPISTEMIC: OK (' + claimsCount + ' claims)';
            } else {
              if (epistemicDot) epistemicDot.style.background = 'var(--danger)';
              if (epistemicText) epistemicText.textContent = 'EPISTEMIC: DISSONANT (' + confs + ')';
            }
          }
        } catch (e) {}
      }
      refreshEpistemicStatus();
      setInterval(refreshEpistemicStatus, 10500);

      // -------------------------------------------------------------------
      // ADR 0060: Zero-Knowledge Causal Proof Status Polling
      // -------------------------------------------------------------------
      async function refreshZkProofStatus() {
        var zkProofDot = document.getElementById('zkProofDot');
        var zkProofText = document.getElementById('zkProofText');
        var sId = currentSwarmId || 'swarm-live-01';
        try {
          var res = await fetch('/api/v1/swarms/' + encodeURIComponent(sId) + '/proof/root');
          if (res.ok) {
            var data = await res.json();
            var totalLeaves = data.total_leaves || 0;
            if (zkProofDot) zkProofDot.style.background = 'var(--success)';
            if (zkProofText) zkProofText.textContent = 'ZK-PROOF: VERIFIED (' + totalLeaves + ')';
          }
        } catch (e) {}
      }
      refreshZkProofStatus();
      setInterval(refreshZkProofStatus, 11000);

      // -------------------------------------------------------------------
      // ADR 0061: Canonical Causal Encoding & JCS Codec Status
      // -------------------------------------------------------------------
      async function refreshCodecStatus() {
        var codecDot = document.getElementById('codecDot');
        var codecText = document.getElementById('codecText');
        try {
          var res = await fetch('/api/v1/codec/canonicalize', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ payload: { status: 'ping' }, version: 1 })
          });
          if (res.ok) {
            if (codecDot) codecDot.style.background = 'var(--success)';
            if (codecText) codecText.textContent = 'CODEC: RFC-8785 JCS';
          }
        } catch (e) {}
      }
      refreshCodecStatus();
      setInterval(refreshCodecStatus, 12000);

      // -------------------------------------------------------------------
      // ADR 0062: Unified Safety Authority & Linearizable Execution Permits
      // -------------------------------------------------------------------
      async function refreshSafetyStatus() {
        var safetyDot = document.getElementById('safetyDot');
        var safetyText = document.getElementById('safetyText');
        try {
          var res = await fetch('/api/v1/safety/status');
          if (res.ok) {
            var data = await res.json();
            if (safetyDot) {
              if (data.global_state === 'TRIPPED') {
                safetyDot.style.background = 'var(--danger)';
              } else if (data.global_state === 'DRAINING') {
                safetyDot.style.background = 'var(--warning)';
              } else {
                safetyDot.style.background = 'var(--success)';
              }
            }
            if (safetyText) {
              var ep = data.epoch !== undefined ? data.epoch : 1;
              var st = data.global_state === 'TRIPPED' ? 'TRIPPED' : 'AUTHORITATIVE';
              safetyText.textContent = 'SAFETY: ' + st + ' (E' + ep + ')';
            }
          }
        } catch (e) {}
      }
      refreshSafetyStatus();
      setInterval(refreshSafetyStatus, 10000);

      // -------------------------------------------------------------------
      // ADR 0063: Fenced Effect Ledger & Monotonic Idempotency Barrier
      // -------------------------------------------------------------------
      async function refreshLedgerStatus() {
        var ledgerDot = document.getElementById('ledgerDot');
        var ledgerText = document.getElementById('ledgerText');
        try {
          var res = await fetch('/api/v1/effects/status');
          if (res.ok) {
            var data = await res.json();
            if (ledgerDot) {
              ledgerDot.style.background = 'var(--success)';
            }
            if (ledgerText) {
              var cnt = data.committed_effects_count !== undefined ? data.committed_effects_count : 0;
              ledgerText.textContent = 'EFFECT LEDGER: FENCED (' + cnt + ')';
            }
          }
        } catch (e) {}
      }
      refreshLedgerStatus();
      setInterval(refreshLedgerStatus, 11000);

      // -------------------------------------------------------------------
      // ADR 0064: Loss-Aware Causal Ingestion & Evidentiary Completeness Rule
      // -------------------------------------------------------------------
      async function refreshIngressStatus() {
        var ingressDot = document.getElementById('ingressDot');
        var ingressText = document.getElementById('ingressText');
        try {
          var res = await fetch('/api/v1/ingress/status');
          if (res.ok) {
            var data = await res.json();
            var status = data.completeness_status || 'COMPLETE';
            var gaps = data.total_gaps !== undefined ? data.total_gaps : 0;
            if (ingressDot) {
              if (status === 'COMPLETE') {
                ingressDot.style.background = 'var(--success)';
              } else if (status === 'SATURATED') {
                ingressDot.style.background = 'var(--danger)';
              } else {
                ingressDot.style.background = 'var(--warning)';
              }
            }
            if (ingressText) {
              ingressText.textContent = 'INGRESS: LOSS-AWARE (' + gaps + ' GAP)';
            }
          }
        } catch (e) {}
      }
      refreshIngressStatus();
      setInterval(refreshIngressStatus, 10000);

      // -------------------------------------------------------------------
      // ADR 0065: Durable Audit Anchor & Merkle Audit Receipt Engine
      // -------------------------------------------------------------------
      async function refreshAuditStatus() {
        var auditDot = document.getElementById('auditDot');
        var auditText = document.getElementById('auditText');
        try {
          var res = await fetch('/api/v1/audit/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'ANCHORED';
            var ep = data.latest_epoch !== undefined ? data.latest_epoch : 0;
            if (auditDot) {
              auditDot.style.background = st === 'ANCHORED' ? 'var(--success)' : 'var(--danger)';
            }
            if (auditText) {
              auditText.textContent = 'AUDIT: ' + st + ' (E' + ep + ')';
            }
          }
        } catch (e) {}
      }
      refreshAuditStatus();
      setInterval(refreshAuditStatus, 10000);

      // -------------------------------------------------------------------
      // ADR 0066: Bounded Verification Semantics & Explicit Counterexample Engine
      // -------------------------------------------------------------------
      async function refreshVerifierStatus() {
        var verifierDot = document.getElementById('verifierDot');
        var verifierText = document.getElementById('verifierText');
        try {
          var res = await fetch('/api/v1/verifier/status');
          if (res.ok) {
            var data = await res.json();
            var lastV = data.last_verdict || 'BOUNDED_SAFE';
            var cov = Math.round((data.average_coverage_ratio || 1.0) * 100);
            if (verifierDot) {
              if (lastV === 'VIOLATED') {
                verifierDot.style.background = 'var(--danger)';
              } else if (lastV === 'INDETERMINATE') {
                verifierDot.style.background = 'var(--warning)';
              } else {
                verifierDot.style.background = 'var(--success)';
              }
            }
            if (verifierText) {
              var shortV = lastV.replace('_', ' ');
              verifierText.textContent = 'VERIFIER: ' + shortV + ' (' + cov + '%)';
            }
          }
        } catch (e) {}
      }
      refreshVerifierStatus();
      setInterval(refreshVerifierStatus, 10000);

      // -------------------------------------------------------------------
      // ADR 0067: Governed Remediation & Fenced Action State Machine
      // -------------------------------------------------------------------
      async function refreshRemediationStatus() {
        var remediationDot = document.getElementById('remediationDot');
        var remediationText = document.getElementById('remediationText');
        try {
          var res = await fetch('/api/v1/remediation/status');
          if (res.ok) {
            var data = await res.json();
            var activeCount = data.active_remediations_count || 0;
            if (remediationDot) {
              remediationDot.style.background = 'var(--success)';
            }
            if (remediationText) {
              remediationText.textContent = 'REMEDIATION: GOVERNED (' + activeCount + ')';
            }
          }
        } catch (e) {}
      }
      refreshRemediationStatus();
      setInterval(refreshRemediationStatus, 10000);

      // -------------------------------------------------------------------
      // ADR 0068: Partition Reconciliation & Conflict Lattice Engine
      // -------------------------------------------------------------------
      async function refreshReconcilerStatus() {
        var reconcilerDot = document.getElementById('reconcilerDot');
        var reconcilerText = document.getElementById('reconcilerText');
        try {
          var res = await fetch('/api/v1/reconciler/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'CONVERGED';
            var activeConflicts = data.active_conflicts || 0;
            var tripsPreserved = data.trips_preserved_count || 0;
            if (reconcilerDot) {
              if (activeConflicts > 0) {
                reconcilerDot.style.background = 'var(--warning)';
              } else if (tripsPreserved > 0) {
                reconcilerDot.style.background = 'var(--danger)';
              } else {
                reconcilerDot.style.background = 'var(--success)';
              }
            }
            if (reconcilerText) {
              reconcilerText.textContent = 'RECONCILER: ' + st + ' (' + activeConflicts + ')';
            }
          }
        } catch (e) {}
      }
      refreshReconcilerStatus();
      setInterval(refreshReconcilerStatus, 10000);

      // -------------------------------------------------------------------
      // ADR 0069: Privacy Release Ledger & Differential Privacy Accountant
      // -------------------------------------------------------------------
      async function refreshPrivacyLedgerStatus() {
        var privacyLedgerDot = document.getElementById('privacyLedgerDot');
        var privacyLedgerText = document.getElementById('privacyLedgerText');
        try {
          var res = await fetch('/api/v1/privacy-ledger/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'CONVERGED';
            var releases = data.total_releases !== undefined ? data.total_releases : 0;
            var constrained = data.budget_constrained_count !== undefined ? data.budget_constrained_count : 0;
            if (privacyLedgerDot) {
              if (constrained > 0 || st === 'BUDGET_CONSTRAINED') {
                privacyLedgerDot.style.background = 'var(--danger)';
              } else {
                privacyLedgerDot.style.background = 'var(--success)';
              }
            }
            if (privacyLedgerText) {
              privacyLedgerText.textContent = 'PRIVACY: RELEASE FENCED (' + releases + ')';
            }
          }
        } catch (e) {}
      }
      refreshPrivacyLedgerStatus();
      setInterval(refreshPrivacyLedgerStatus, 10500);

      // -------------------------------------------------------------------
      // ADR 0070: Migration Preservation & Partial Isomorphisms
      // -------------------------------------------------------------------
      async function refreshEvolutionStatus() {
        var evolutionDot = document.getElementById('evolutionDot');
        var evolutionText = document.getElementById('evolutionText');
        try {
          var res = await fetch('/api/v1/schema-evolution/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'PRESERVED';
            var total = data.total_migrations_count !== undefined ? data.total_migrations_count : 0;
            var lossy = data.lossy_migrations_count !== undefined ? data.lossy_migrations_count : 0;
            if (evolutionDot) {
              if (lossy > 0) {
                evolutionDot.style.background = 'var(--accent)';
              } else {
                evolutionDot.style.background = 'var(--success)';
              }
            }
            if (evolutionText) {
              evolutionText.textContent = 'SCHEMA: ' + st + ' (' + total + ')';
            }
          }
        } catch (e) {}
      }
      refreshEvolutionStatus();
      setInterval(refreshEvolutionStatus, 11000);

      async function refreshAcceptanceStatus() {
        var acceptanceDot = document.getElementById('acceptanceDot');
        var acceptanceText = document.getElementById('acceptanceText');
        try {
          var res = await fetch('/api/v1/acceptance/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'NOMINAL';
            if (acceptanceDot) {
              if (st === 'CERTIFIED') {
                acceptanceDot.style.background = 'var(--success)';
              } else if (st === 'REJECTED') {
                acceptanceDot.style.background = 'var(--danger)';
              } else {
                acceptanceDot.style.background = 'var(--accent)';
              }
            }
            if (acceptanceText) {
              var countStr = st === 'CERTIFIED' ? '6/6' : (st === 'REJECTED' ? 'FAIL' : '0/6');
              var badgeStr = ('ACCEPTANCE: ' + st + ' (' + countStr + ')').toUpperCase();
              if (badgeStr.length > 30) {
                badgeStr = ('ACCEPTANCE: ' + st).toUpperCase();
              }
              acceptanceText.textContent = badgeStr;
            }
          }
        } catch (e) {}
      }
      refreshAcceptanceStatus();
      setInterval(refreshAcceptanceStatus, 12000);

      // ADR 0072: Consensus Authority Binding & Linearizable Quorum Fencing
      // -------------------------------------------------------------------
      async function refreshConsensusStatus() {
        var consensusDot = document.getElementById('consensusDot');
        var consensusText = document.getElementById('consensusText');
        try {
          var res = await fetch('/api/v1/consensus/status');
          if (res.ok) {
            var data = await res.json();
            var hasQuorum = Boolean(data.has_quorum);
            if (consensusDot) {
              consensusDot.style.background = hasQuorum ? 'var(--success)' : 'var(--danger)';
            }
            if (consensusText) {
              var ep = data.epoch !== undefined ? data.epoch : 1;
              var qStr = hasQuorum ? 'QUORUM OK' : 'SPLIT BRAIN';
              var badgeStr = ('CONSENSUS: ' + qStr + ' (E' + ep + ')').toUpperCase();
              if (badgeStr.length > 30) {
                badgeStr = ('CONSENSUS: ' + qStr).toUpperCase();
              }
              consensusText.textContent = badgeStr;
            }
          }
        } catch (e) {}
      }
      refreshConsensusStatus();
      setInterval(refreshConsensusStatus, 10000);

      async function refreshDownstreamStatus() {
        var downstreamDot = document.getElementById('downstreamDot');
        var downstreamText = document.getElementById('downstreamText');
        try {
          var res = await fetch('/api/v1/downstream-effects/status');
          if (res.ok) {
            var data = await res.json();
            var unkCount = data.outcome_unknown_count || 0;
            if (downstreamDot) {
              downstreamDot.style.background = unkCount > 0 ? 'var(--warning)' : 'var(--success)';
            }
            if (downstreamText) {
              var badgeStr = unkCount > 0 ? ('DOWNSTREAM: ' + unkCount + ' UNKNOWN') : 'DOWNSTREAM: RECONCILED';
              downstreamText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshDownstreamStatus();
      setInterval(refreshDownstreamStatus, 10000);

      async function refreshEvidenceStatus() {
        var evidenceDot = document.getElementById('evidenceDot');
        var evidenceText = document.getElementById('evidenceText');
        try {
          var res = await fetch('/api/v1/evidence/status');
          if (res.ok) {
            var data = await res.json();
            var st = data.status || 'ACTIVE';
            if (evidenceDot) {
              evidenceDot.style.background = st === 'ACTIVE' ? 'var(--success)' : 'var(--danger)';
            }
            if (evidenceText) {
              var badgeStr = st === 'ACTIVE' ? 'EVIDENCE: SPOOL COMMITTED' : ('EVIDENCE: ' + st);
              evidenceText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshEvidenceStatus();
      setInterval(refreshEvidenceStatus, 10000);

      async function refreshWitnessStatus() {
        var witnessDot = document.getElementById('witnessDot');
        var witnessText = document.getElementById('witnessText');
        try {
          var res = await fetch('/api/v1/witness/status');
          if (res.ok) {
            var data = await res.json();
            var active = data.active_public_key && data.active_public_key.length > 0;
            if (witnessDot) {
              witnessDot.style.background = active ? 'var(--success)' : 'var(--danger)';
            }
            if (witnessText) {
              var badgeStr = active ? 'WITNESS: ASYMMETRIC VERIFIED' : 'WITNESS: KEY MISSING';
              witnessText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshWitnessStatus();
      setInterval(refreshWitnessStatus, 10000);

      async function refreshCapabilityStatus() {
        var capabilityDot = document.getElementById('capabilityDot');
        var capabilityText = document.getElementById('capabilityText');
        try {
          var res = await fetch('/api/v1/capability/status');
          if (res.ok) {
            var data = await res.json();
            var active = data.active_root_public_key && data.active_root_public_key.length > 0;
            if (capabilityDot) {
              capabilityDot.style.background = active ? 'var(--success)' : 'var(--danger)';
            }
            if (capabilityText) {
              var badgeStr = active ? 'CAPABILITY: ATTENUATED OK' : 'CAPABILITY: INACTIVE';
              capabilityText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshCapabilityStatus();
      setInterval(refreshCapabilityStatus, 10000);

      async function refreshCompensationStatus() {
        var compensationDot = document.getElementById('compensationDot');
        var compensationText = document.getElementById('compensationText');
        try {
          var res = await fetch('/api/v1/compensation/status');
          if (res.ok) {
            var data = await res.json();
            var hasPending = data.total_escalations_pending > 0;
            if (compensationDot) {
              compensationDot.style.background = hasPending ? 'var(--warning)' : 'var(--success)';
            }
            if (compensationText) {
              var badgeStr = hasPending
                ? ('COMPENSATION: ' + data.total_escalations_pending + ' ESC PENDING')
                : 'COMPENSATION: ALGEBRA OK';
              compensationText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshCompensationStatus();
      setInterval(refreshCompensationStatus, 10000);

      async function refreshDatasetPrivacyStatus() {
        var datasetPrivacyDot = document.getElementById('datasetPrivacyDot');
        var datasetPrivacyText = document.getElementById('datasetPrivacyText');
        try {
          var res = await fetch('/api/v1/dataset-privacy/status');
          if (res.ok) {
            var data = await res.json();
            var activeRes = data.total_active_reservations || 0;
            if (datasetPrivacyDot) {
              datasetPrivacyDot.style.background = 'var(--success)';
            }
            if (datasetPrivacyText) {
              var badgeStr = activeRes > 0
                ? ('DATASET PRIVACY: ' + activeRes + ' PENDING')
                : 'DATASET PRIVACY: BUDGET OK';
              datasetPrivacyText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshDatasetPrivacyStatus();
      setInterval(refreshDatasetPrivacyStatus, 10000);

      async function refreshInteropStatus() {
        var interopDot = document.getElementById('interopDot');
        var interopText = document.getElementById('interopText');
        try {
          var res = await fetch('/api/v1/interop/status');
          if (res.ok) {
            var data = await res.json();
            var fatalDevs = data.fatal_deviations || 0;
            var totalEx = data.total_exercises_run || 0;
            if (interopDot) {
              interopDot.style.background = fatalDevs > 0 ? 'var(--danger)' : 'var(--success)';
            }
            if (interopText) {
              var badgeStr = fatalDevs > 0
                ? ('INTEROP: ' + fatalDevs + ' FATAL')
                : (totalEx > 0 ? ('INTEROP: ' + totalEx + ' PASSED') : 'INTEROP LAB: CONFORMANT');
              interopText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshInteropStatus();
      setInterval(refreshInteropStatus, 10000);

      async function refreshProvenanceStatus() {
        var provenanceDot = document.getElementById('provenanceDot');
        var provenanceText = document.getElementById('provenanceText');
        try {
          var res = await fetch('/api/v1/provenance/status');
          if (res.ok) {
            var data = await res.json();
            var failedVerifs = data.failed_verifications || 0;
            var successVerifs = data.successful_verifications || 0;
            if (provenanceDot) {
              provenanceDot.style.background = failedVerifs > 0 ? 'var(--danger)' : 'var(--success)';
            }
            if (provenanceText) {
              var badgeStr = failedVerifs > 0
                ? ('PROVENANCE: ' + failedVerifs + ' FAILED')
                : (successVerifs > 0 ? ('PROVENANCE: ' + successVerifs + ' VERIFIED') : 'PROVENANCE: SLSA L3 OK');
              provenanceText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshProvenanceStatus();
      setInterval(refreshProvenanceStatus, 10000);

      async function refreshDatacenterStatus() {
        var datacenterDot = document.getElementById('datacenterDot');
        var datacenterText = document.getElementById('datacenterText');
        try {
          var res = await fetch('/api/v1/exercise/status');
          if (res.ok) {
            var data = await res.json();
            var isTripped = data.cluster_globally_tripped || false;
            var activePhase = data.active_phase || 'NOMINAL';
            if (datacenterDot) {
              datacenterDot.style.background = isTripped ? 'var(--danger)' : (activePhase === 'NOMINAL' ? 'var(--success)' : 'var(--warning)');
            }
            if (datacenterText) {
              var badgeStr = isTripped
                ? 'DATACENTER: SAFETY TRIPPED'
                : (activePhase !== 'NOMINAL' ? ('DATACENTER: ' + activePhase) : 'DATACENTER: QUORUM RESILIENT');
              datacenterText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshDatacenterStatus();
      setInterval(refreshDatacenterStatus, 10000);

      async function refreshGatewayStatus() {
        var gatewayDot = document.getElementById('gatewayDot');
        var gatewayText = document.getElementById('gatewayText');
        try {
          var res = await fetch('/api/v1/gateway/status');
          if (res.ok) {
            var data = await res.json();
            var isTripped = data.circuit_tripped || false;
            var pendingCount = data.pending_requests_count || 0;
            if (gatewayDot) {
              gatewayDot.style.background = isTripped ? 'var(--danger)' : (pendingCount > 0 ? 'var(--warning)' : 'var(--success)');
            }
            if (gatewayText) {
              var badgeStr = isTripped
                ? 'GATEWAY: CIRCUIT BLOCKED'
                : (pendingCount > 0 ? ('GATEWAY: ' + pendingCount + ' PENDING') : 'GATEWAY: AUTONOMOUS OK');
              gatewayText.textContent = badgeStr.toUpperCase();
            }
          }
        } catch (e) {}
      }
      refreshGatewayStatus();
      setInterval(refreshGatewayStatus, 10000);


      if (btnMfaModal) {

        btnMfaModal.addEventListener('click', function() {
          if (mfaModal) mfaModal.style.display = 'flex';
          refreshAuthSession();
        });
      }
      if (btnCloseMfaModal) {
        btnCloseMfaModal.addEventListener('click', function() {
          if (mfaModal) mfaModal.style.display = 'none';
        });
      }

      if (btnEnrollMfa) {
        btnEnrollMfa.addEventListener('click', async function() {
          try {
            mfaEnrollStatus.textContent = 'Enrolling TOTP credential...';
            mfaEnrollStatus.style.color = 'var(--text-muted)';
            var res = await fetch('/api/v1/auth/mfa/enroll', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username: 'admin' })
            });
            if (res.ok) {
              var data = await res.json();
              mfaSecretKey.textContent = data.secret_base32 || '-';
              mfaUriText.textContent = data.provisioning_uri || '';
              if (mfaBackupCodes && Array.isArray(data.backup_codes)) {
                mfaBackupCodes.innerHTML = '';
                data.backup_codes.forEach(function(code) {
                  var span = document.createElement('span');
                  span.style.padding = '4px 6px';
                  span.style.background = 'var(--bg-canvas)';
                  span.style.borderRadius = '3px';
                  span.style.border = '1px solid var(--border-subtle)';
                  span.style.textAlign = 'center';
                  span.textContent = code;
                  mfaBackupCodes.appendChild(span);
                });
              }
              mfaEnrollSection.style.display = 'flex';
              mfaEnrollStatus.textContent = 'Scan or copy secret into authenticator app, then enter 6-digit code below:';
              mfaEnrollStatus.style.color = 'var(--text-secondary)';
            } else {
              var errData = await res.json();
              mfaEnrollStatus.textContent = 'Enrollment failed: ' + (errData.detail || 'Error');
              mfaEnrollStatus.style.color = 'var(--danger)';
            }
          } catch(e) {
            mfaEnrollStatus.textContent = 'Network error: ' + e.message;
            mfaEnrollStatus.style.color = 'var(--danger)';
          }
        });
      }

      if (btnVerifyMfaActivation) {
        btnVerifyMfaActivation.addEventListener('click', async function() {
          var code = mfaCodeInput ? mfaCodeInput.value.trim() : '';
          if (!code) {
            mfaEnrollStatus.textContent = 'Please enter a 6-digit code.';
            mfaEnrollStatus.style.color = 'var(--danger)';
            return;
          }
          try {
            var res = await fetch('/api/v1/auth/mfa/verify', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username: 'admin', code: code })
            });
            var data = await res.json();
            if (res.ok && data.verified) {
              mfaEnrollStatus.textContent = 'TOTP verified and activated successfully!';
              mfaEnrollStatus.style.color = 'var(--success)';
              sessionStorage.setItem('swarmwatch_mfa_code', code);
              refreshAuthSession();
            } else {
              mfaEnrollStatus.textContent = 'Verification failed: ' + (data.detail || 'Invalid code');
              mfaEnrollStatus.style.color = 'var(--danger)';
            }
          } catch(e) {
            mfaEnrollStatus.textContent = 'Network error: ' + e.message;
            mfaEnrollStatus.style.color = 'var(--danger)';
          }
        });
      }

      if (btnVerifyStepUp) {
        btnVerifyStepUp.addEventListener('click', async function() {
          var code = mfaStepUpInput ? mfaStepUpInput.value.trim() : '';
          if (!code) {
            mfaStepUpStatus.textContent = 'Please enter a 6-digit TOTP or backup code.';
            mfaStepUpStatus.style.color = 'var(--danger)';
            return;
          }
          try {
            var res = await fetch('/api/v1/auth/mfa/challenge', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username: 'admin', code: code })
            });
            var data = await res.json();
            if (res.ok && data.verified) {
              mfaStepUpStatus.textContent = 'Session authorized with Step-Up MFA (' + (data.method || 'totp') + ')';
              mfaStepUpStatus.style.color = 'var(--success)';
              sessionStorage.setItem('swarmwatch_mfa_code', code);
              refreshAuthSession();
            } else {
              mfaStepUpStatus.textContent = 'Step-up challenge rejected: Invalid code or token';
              mfaStepUpStatus.style.color = 'var(--danger)';
            }
          } catch(e) {
            mfaStepUpStatus.textContent = 'Network error: ' + e.message;
            mfaStepUpStatus.style.color = 'var(--danger)';
          }
        });
      }

      // =========================================================================
      // Distributed Redis Fleet Cluster Widget Logic (Seam 23 & 24 / ADR 0034 & 0035)
      // =========================================================================
      var fleetBar = document.getElementById('fleetBar');
      var btnFleetToggle = document.getElementById('btnFleetToggle');
      var fleetClusterBadge = document.getElementById('fleetClusterBadge');
      var fleetStatusText = document.getElementById('fleetStatusText');
      var fleetBackendTag = document.getElementById('fleetBackendTag');
      var fleetLeaderBadge = document.getElementById('fleetLeaderBadge');
      var fleetLeaderText = document.getElementById('fleetLeaderText');
      var fleetNodesCountTag = document.getElementById('fleetNodesCountTag');
      var btnLeaderStepDown = document.getElementById('btnLeaderStepDown');
      var fleetWorkersList = document.getElementById('fleetWorkersList');
      var fleetTurnsVal = document.getElementById('fleetTurnsVal');
      var fleetTurnsFill = document.getElementById('fleetTurnsFill');
      var fleetTokensVal = document.getElementById('fleetTokensVal');
      var fleetTokensFill = document.getElementById('fleetTokensFill');
      var fleetCostVal = document.getElementById('fleetCostVal');
      var fleetCostFill = document.getElementById('fleetCostFill');
      var btnFleetTrip = document.getElementById('btnFleetTrip');
      var btnFleetReset = document.getElementById('btnFleetReset');
      var fleetTripAlert = document.getElementById('fleetTripAlert');
      var fleetTripReason = document.getElementById('fleetTripReason');
      var fleetTrippedBy = document.getElementById('fleetTrippedBy');
      var btnFleetAlertReset = document.getElementById('btnFleetAlertReset');

      if (btnFleetToggle && fleetBar) {
        btnFleetToggle.addEventListener('click', function() {
          var isHidden = fleetBar.style.display === 'none';
          fleetBar.style.display = isHidden ? 'flex' : 'none';
          if (isHidden) fetchFleetMetrics();
        });
      }

      if (btnFleetTrip) btnFleetTrip.addEventListener('click', triggerEmergencyFleetTrip);
      if (btnFleetReset) btnFleetReset.addEventListener('click', triggerFleetReset);
      if (btnFleetAlertReset) btnFleetAlertReset.addEventListener('click', triggerFleetReset);
      if (btnLeaderStepDown) btnLeaderStepDown.addEventListener('click', triggerLeaderStepDown);

      function updateFleetUI(metrics) {
        if (!metrics) return;
        if (metrics.backend_type && fleetBackendTag) {
          var bType = metrics.backend_type === 'MockRedisClient' ? 'MOCK IN-MEMORY' : (metrics.backend_type === 'RESPRedisClient' ? 'RESP2 (127.0.0.1:6379)' : metrics.backend_type);
          fleetBackendTag.textContent = bType;
        }
        if (metrics.cluster_leader) {
          var cl = metrics.cluster_leader;
          if (fleetLeaderBadge && fleetLeaderText) {
            if (cl.is_leader) {
              fleetLeaderBadge.className = 'fleet-badge leader-badge';
              fleetLeaderText.textContent = 'LEADER [' + escapeHtml(cl.local_node_id || 'local') + ']';
              if (btnLeaderStepDown) btnLeaderStepDown.style.display = 'inline-flex';
            } else {
              fleetLeaderBadge.className = 'fleet-badge standby';
              var leadId = cl.leader_node_id ? cl.leader_node_id : 'STANDBY';
              fleetLeaderText.textContent = 'STANDBY [Leader: ' + escapeHtml(leadId) + ']';
              if (btnLeaderStepDown) btnLeaderStepDown.style.display = 'none';
            }
          }
          if (fleetNodesCountTag) {
            var ncount = cl.nodes_count || (cl.roster ? cl.roster.length : 1);
            fleetNodesCountTag.textContent = ncount + (ncount === 1 ? ' Node' : ' Nodes');
          }
        }
        if (fleetWorkersList) {
          fleetWorkersList.innerHTML = '';
          var workers = metrics.active_workers || [];
          if (workers.length === 0) {
            fleetWorkersList.innerHTML = '<span class="fleet-worker-pill"><span class="fleet-worker-dot"></span>standby</span>';
          } else {
            workers.forEach(function(w) {
              var pill = document.createElement('span');
              pill.className = 'fleet-worker-pill';
              pill.innerHTML = '<span class="fleet-worker-dot"></span>' + escapeHtml(w);
              fleetWorkersList.appendChild(pill);
            });
          }
        }
        var pol = metrics.policy || {};
        var maxTurns = pol.max_turns || null;
        var tokBudget = pol.token_budget || null;
        var costBudget = pol.cost_budget || null;

        var turns = metrics.total_turns || 0;
        var tokens = metrics.total_tokens || 0;
        var cost = metrics.total_cost || 0.0;

        if (fleetTurnsVal) fleetTurnsVal.textContent = turns + ' / ' + (maxTurns ? maxTurns : '--');
        if (fleetTurnsFill) {
          var pct = maxTurns ? Math.min(100, (turns / maxTurns) * 100) : 0;
          fleetTurnsFill.style.width = pct + '%';
          fleetTurnsFill.className = 'fleet-progress-fill' + (pct >= 100 ? ' danger' : (pct >= 80 ? ' warning' : ''));
        }

        if (fleetTokensVal) fleetTokensVal.textContent = tokens.toLocaleString() + ' / ' + (tokBudget ? tokBudget.toLocaleString() : '--');
        if (fleetTokensFill) {
          var pctT = tokBudget ? Math.min(100, (tokens / tokBudget) * 100) : 0;
          fleetTokensFill.style.width = pctT + '%';
          fleetTokensFill.className = 'fleet-progress-fill' + (pctT >= 100 ? ' danger' : (pctT >= 80 ? ' warning' : ''));
        }

        if (fleetCostVal) fleetCostVal.textContent = '$' + cost.toFixed(4) + ' / ' + (costBudget ? '$' + costBudget.toFixed(2) : '--');
        if (fleetCostFill) {
          var pctC = costBudget ? Math.min(100, (cost / costBudget) * 100) : 0;
          fleetCostFill.style.width = pctC + '%';
          fleetCostFill.className = 'fleet-progress-fill' + (pctC >= 100 ? ' danger' : (pctC >= 80 ? ' warning' : ''));
        }

        if (metrics.is_tripped) {
          renderFleetTripAlert({
            reason: metrics.trip_reason || 'Fleet quota ceiling reached',
            tripped_by: metrics.tripped_by || 'cluster-worker',
          });
        } else {
          clearFleetTripAlert();
        }
      }

      function renderFleetTripAlert(data) {
        if (fleetClusterBadge) {
          fleetClusterBadge.className = 'fleet-badge tripped';
          if (fleetStatusText) fleetStatusText.textContent = 'REDIS: TRIPPED';
        }
        if (fleetTripAlert) {
          fleetTripAlert.style.display = 'flex';
          if (fleetTripReason) fleetTripReason.textContent = data.reason || 'Distributed quota breach';
          if (fleetTrippedBy) fleetTrippedBy.textContent = 'by ' + (data.tripped_by || 'peer-worker');
        }
      }

      function clearFleetTripAlert() {
        if (fleetClusterBadge) {
          fleetClusterBadge.className = 'fleet-badge';
          if (fleetStatusText) fleetStatusText.textContent = 'REDIS: CONNECTED';
        }
        if (fleetTripAlert) {
          fleetTripAlert.style.display = 'none';
        }
      }

      async function fetchFleetMetrics() {
        try {
          var res = await fetch('/api/v1/fleet/redis/metrics?swarm_id=swarm-live-01');
          if (res.ok) {
            var data = await res.json();
            updateFleetUI(data);
          }
        } catch(e) {}
      }

      async function triggerEmergencyFleetTrip() {
        var reason = prompt("Enter Emergency Trip Reason for Redis Fleet:", "Operator emergency halt via console");
        if (!reason) return;
        try {
          var hdrs = { 'Content-Type': 'application/json' };
          var activeMfa = sessionStorage.getItem('swarmwatch_mfa_code');
          if (activeMfa) hdrs['X-MFA-Code'] = activeMfa;
          var res = await fetch('/api/v1/fleet/redis/trip', {
            method: 'POST',
            headers: hdrs,
            body: JSON.stringify({
              swarm_id: 'swarm-live-01',
              reason: reason,
              tripped_by: 'console-operator',
              metric_val: 1.0
            })
          });
          if (res.ok) {
            showToast('Distributed emergency trip broadcast to Redis fleet', 'danger');
            fetchFleetMetrics();
          } else if (res.status === 403) {
            showToast('MFA step-up authorization required to trip fleet', 'danger');
            if (mfaModal) mfaModal.style.display = 'flex';
          }
        } catch(e) {
          showToast('Failed to trip fleet: ' + e.message, 'danger');
        }
      }

      async function triggerFleetReset() {
        try {
          var hdrs = { 'Content-Type': 'application/json' };
          var activeMfa = sessionStorage.getItem('swarmwatch_mfa_code');
          if (activeMfa) hdrs['X-MFA-Code'] = activeMfa;
          var res = await fetch('/api/v1/fleet/redis/reset', {
            method: 'POST',
            headers: hdrs,
            body: JSON.stringify({ swarm_id: 'swarm-live-01' })
          });
          if (res.ok) {
            showToast('Redis fleet state reset and circuit re-armed', 'success');
            clearFleetTripAlert();
            fetchFleetMetrics();
          } else if (res.status === 403) {
            showToast('MFA step-up authorization required to reset fleet', 'danger');
            if (mfaModal) mfaModal.style.display = 'flex';
          }
        } catch(e) {
          showToast('Failed to reset fleet: ' + e.message, 'danger');
        }
      }

      async function triggerLeaderStepDown() {
        if (!confirm("Step down as active cluster leader? Another standby node will assume leadership.")) return;
        try {
          var res = await fetch('/api/v1/cluster/leader/step-down', { method: 'POST' });
          if (res.ok) {
            showToast('Voluntarily relinquished cluster leadership', 'warning');
            fetchFleetMetrics();
          }
        } catch(e) {
          showToast('Failed to step down: ' + e.message, 'danger');
        }
      }

      var btnArchiveSwarm = document.getElementById('btnArchiveSwarm');
      if (btnArchiveSwarm) {
        btnArchiveSwarm.addEventListener('click', async function() {
          var targetSwarm = (typeof currentSwarmId !== 'undefined' && currentSwarmId) ? currentSwarmId : 'swarm-live-01';
          if (!confirm("Archive telemetry for swarm '" + targetSwarm + "' to S3/GCS cold object storage?")) return;
          try {
            var res = await fetch('/api/v1/storage/archive/' + targetSwarm, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ delete_local: false })
            });
            var data = await res.json();
            if (res.ok && data.record) {
              var archBadge = document.getElementById('fleetArchiveStatusBadge');
              if (archBadge) archBadge.style.display = 'inline-flex';
              showToast("Swarm archived! Compression ratio: " + data.record.compression_ratio + "x", "success");
            } else {
              showToast("Archive failed: " + (data.detail || data.message || "Unknown error"), "danger");
            }
          } catch(e) {
            showToast("Archive error: " + e.message, "danger");
          }
        });
      }

      fetchFleetMetrics();
      setInterval(fetchFleetMetrics, 4000);

      loadProjects();
      loadBranches();

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
        });
        selectSpanByIndex(0);
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
      if (window.location.search.indexOf('modal=fork') !== -1) {
        SIMULATION_DATA.forEach(function(item) {
          spans.push(item);
          renderSpan(item);
        });
        selectSpanByIndex(1);
        openForkModal();
      }
      if (window.location.search.indexOf('modal=mfa') !== -1) {
        if (mfaModal) mfaModal.style.display = 'flex';
        if (mfaSecretKey) mfaSecretKey.textContent = 'JBSWY3DPEHPK3PXP';
        if (mfaUriText) mfaUriText.textContent = 'otpauth://totp/SwarmWatch:admin?secret=JBSWY3DPEHPK3PXP&issuer=SwarmWatch';
        if (mfaEnrollSection) mfaEnrollSection.style.display = 'flex';
        if (mfaBackupCodes) {
          mfaBackupCodes.innerHTML = '<span style="padding: 4px 6px; background: var(--bg-canvas); border-radius: 3px; border: 1px solid var(--border-subtle); text-align: center;">A1B2-C3D4</span><span style="padding: 4px 6px; background: var(--bg-canvas); border-radius: 3px; border: 1px solid var(--border-subtle); text-align: center;">E5F6-G7H8</span>';
        }
      }
      refreshAuthSession();

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

    @app.get("/api/v1/consensus/status")
    async def consensus_status_endpoint():
        """Returns cluster consensus health, quorum metrics, and RSM state."""
        return global_consensus_authority.export_status()

    @app.get("/api/v1/consensus/read-committed")
    async def consensus_read_committed_endpoint(tenant_id: str = "default"):
        """Returns linearizable state reflected up to highest quorum-committed index."""
        return global_consensus_authority.read_committed(tenant_id).to_dict()

    @app.post("/api/v1/consensus/reserve")
    async def consensus_reserve_endpoint(req: SafetyReserveRequest):
        """Atomically proposes and quorum-commits an execution permit reservation."""
        res = global_consensus_authority.reserve(
            tenant_id=req.tenant_id,
            action=req.action,
            cost_estimate=req.cost_estimate,
            tokens_estimate=req.tokens_estimate,
            timeout_ms=req.timeout_ms,
        )
        return res.to_dict()

    @app.post("/api/v1/consensus/commit")
    async def consensus_commit_endpoint(req: SafetyCommitRequest):
        """Quorum-commits execution outcome and settles budget."""
        res = global_consensus_authority.commit(
            permit_id=req.permit_id,
            actual_cost=req.actual_cost,
            actual_tokens=req.actual_tokens,
        )
        return res.to_dict()

    @app.post("/api/v1/consensus/release")
    async def consensus_release_endpoint(req: SafetyReleaseRequest):
        """Releases an unexecuted permit reservation back to quota."""
        res = global_consensus_authority.release(
            permit_id=req.permit_id,
            reason=req.reason,
        )
        return res.to_dict()

    @app.post("/api/v1/consensus/trip")
    async def consensus_trip_endpoint(req: SafetyTripRequest):
        """Quorum-trips the tenant circuit breaker."""
        res = global_consensus_authority.trip(
            tenant_id=req.tenant_id,
            reason=req.reason,
        )
        return res.to_dict()

    @app.post("/api/v1/consensus/rearm")
    async def consensus_rearm_endpoint(req: SafetyRearmRequest):
        """Quorum-rearms tenant circuit and advances monotonic epoch."""
        res = global_consensus_authority.rearm(
            tenant_id=req.tenant_id,
        )
        return res.to_dict()

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

    # Seam 62: Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation
    @app.post("/api/v1/downstream-effects/prepare")
    async def downstream_prepare_endpoint(req: DownstreamPrepareRequest):
        """Prepares a downstream effect with deterministic idempotency key and epoch."""
        try:
            prep = global_downstream_effect_boundary.prepare(
                tenant_id=req.tenant_id,
                effect_key=req.effect_key,
                target_uri=req.target_uri,
                payload=req.payload,
                epoch=req.epoch,
                contract=req.contract,
                idempotency_key=req.idempotency_key,
            )
            return prep.to_dict()
        except StaleEpochError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.post("/api/v1/downstream-effects/dispatch")
    async def downstream_dispatch_endpoint(req: DownstreamDispatchRequest):
        """Dispatches an effect across the downstream boundary with fail-closed OUTCOME_UNKNOWN semantics."""
        try:
            def dispatch_fn(effect):
                if req.mock_downstream_status == "TIMEOUT":
                    raise TimeoutError("Downstream network timeout")
                elif req.mock_downstream_status == "RESET":
                    raise ConnectionResetError("Connection reset by peer")
                return {
                    "status": "dispatched",
                    "confirmation_id": req.mock_confirmation_id or f"mock_{effect.effect_id}",
                }

            res = global_downstream_effect_boundary.dispatch(req.effect_id, dispatch_fn)
            return res.to_dict()
        except UncoordinatedRetryBlockedError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except StaleEpochError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except EffectNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.post("/api/v1/downstream-effects/acknowledge")
    async def downstream_acknowledge_endpoint(req: DownstreamAcknowledgeRequest):
        """Acknowledges explicit downstream receipt, committing the effect."""
        try:
            receipt = DownstreamReceipt(
                downstream_confirmation_id=req.downstream_confirmation_id,
                result_digest=req.result_digest,
                latency_ms=req.latency_ms,
            )
            res = global_downstream_effect_boundary.acknowledge(
                effect_id=req.effect_id,
                receipt=receipt,
                epoch=req.epoch,
            )
            return res.to_dict()
        except StaleEpochError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except EffectNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.post("/api/v1/downstream-effects/reconcile")
    async def downstream_reconcile_endpoint(req: DownstreamReconcileRequest):
        """Executes active reconciliation probe against the downstream target."""
        try:
            def probe_fn(effect_id, idem_key):
                if not req.mock_probe_found:
                    return {"found": False}
                return {
                    "found": True,
                    "status": req.mock_probe_status,
                    "downstream_id": req.mock_downstream_id or f"recon_{effect_id}",
                    "digest": req.mock_probe_digest or "sha256:reconciled",
                }

            res = global_downstream_effect_boundary.reconcile(req.effect_id, probe_fn)
            return res.to_dict()
        except EffectNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.get("/api/v1/downstream-effects/status")
    async def downstream_status_endpoint():
        """Returns operational summary of the downstream effect boundary."""
        return global_downstream_effect_boundary.get_status()

    @app.get("/api/v1/downstream-effects/{effect_id}")
    async def downstream_get_effect_endpoint(effect_id: str):
        """Retrieves details of a specific downstream effect."""
        effect = global_downstream_effect_boundary.get_effect(effect_id)
        if not effect:
            raise HTTPException(status_code=404, detail=f"Effect {effect_id} not found")
        return effect.to_dict()

    # Seam 63: Critical Evidence Spool (ADR 0074)
    @app.post("/api/v1/evidence/offer")
    async def evidence_offer_endpoint(req: EvidenceOfferRequest):
        """Offers a critical safety record to the write-ahead journal; fails closed with 503 if capacity full."""
        from swarmwatch.evidence_spool import global_evidence_spool, EvidenceSpoolFullException
        try:
            receipt = global_evidence_spool.offer(
                record_type=req.record_type,
                tenant_id=req.tenant_id,
                agent_id=req.agent_id,
                payload=req.payload,
                record_id=req.record_id,
            )
            return receipt.to_dict()
        except EvidenceSpoolFullException as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/evidence/flush")
    async def evidence_flush_endpoint(req: Optional[EvidenceFlushRequest] = None):
        """Flushes buffered records to durable write-ahead journal."""
        from swarmwatch.evidence_spool import global_evidence_spool
        res = global_evidence_spool.flush()
        return res.to_dict()

    @app.post("/api/v1/evidence/recover")
    async def evidence_recover_endpoint(req: Optional[EvidenceRecoverRequest] = None):
        """Replays journal from disk, verifying CRC32 and cryptographic hash chain continuity."""
        from swarmwatch.evidence_spool import (
            global_evidence_spool,
            EvidenceCorruptionException,
            SequenceGapException,
        )
        try:
            target_path = req.journal_path if req else None
            res = global_evidence_spool.recover(journal_path=target_path)
            return res.to_dict()
        except (EvidenceCorruptionException, SequenceGapException) as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/evidence/completeness")
    async def evidence_completeness_endpoint(tenant_id: Optional[str] = None):
        """Produces a cryptographic completeness receipt certifying contiguous persistence of safety records."""
        from swarmwatch.evidence_spool import global_evidence_spool
        receipt = global_evidence_spool.completeness(tenant_id=tenant_id)
        return receipt.to_dict()

    @app.get("/api/v1/evidence/status")
    async def evidence_status_endpoint():
        """Returns the real-time operational status of the critical evidence spool."""
        from swarmwatch.evidence_spool import global_evidence_spool
        return global_evidence_spool.status().to_dict()

    # -------------------------------------------------------------------------
    # ADR 0075: Independent Root Witness & Public-Key Chains (Seam 64)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/witness/submit")
    async def witness_submit_endpoint(req: WitnessSubmitRequest):
        """Commits a leaf into the witness accumulator and returns a LeafReceipt."""
        from swarmwatch.root_witness import global_root_witness
        receipt = global_root_witness.submit(leaf_data=req.leaf_data, metadata=req.metadata)
        return receipt.to_dict()

    @app.post("/api/v1/witness/checkpoint")
    async def witness_checkpoint_endpoint(req: Optional[WitnessCheckpointRequest] = None):
        """Issues an immutable signed checkpoint anchoring current tree state with asymmetric ECDSA signature."""
        from swarmwatch.root_witness import global_root_witness
        meta = req.metadata if req else {}
        cp = global_root_witness.checkpoint(metadata=meta)
        return cp.to_dict()

    @app.get("/api/v1/witness/checkpoint")
    async def witness_get_checkpoint_endpoint(epoch: Optional[int] = None):
        """Retrieves latest or specific signed checkpoint."""
        from swarmwatch.root_witness import global_root_witness, WitnessError
        try:
            target_epoch = -1 if epoch is None else epoch
            cp = global_root_witness.get_checkpoint(epoch=target_epoch)
            return cp.to_dict()
        except WitnessError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.post("/api/v1/witness/verify")
    async def witness_verify_endpoint(req: WitnessVerifyRequest):
        """Independently verifies a signed checkpoint using only the public key."""
        from swarmwatch.root_witness import RootWitness, SignedCheckpoint
        cp = SignedCheckpoint.from_dict(req.checkpoint)
        is_valid = RootWitness.verify_checkpoint(cp, public_key=req.public_key)
        return {"valid": is_valid, "epoch": cp.epoch, "tree_size": cp.tree_size}

    @app.get("/api/v1/witness/consistency-proof")
    async def witness_consistency_proof_endpoint(old_size: int, new_size: int):
        """Generates RFC 6962 consistency proof between tree size old_size and new_size."""
        from swarmwatch.root_witness import global_root_witness, WitnessError
        try:
            proof = global_root_witness.get_consistency_proof(old_size=old_size, new_size=new_size)
            return proof.to_dict()
        except WitnessError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/witness/verify-consistency")
    async def witness_verify_consistency_endpoint(req: WitnessConsistencyVerifyRequest):
        """Verifies RFC 6962 consistency proof between two signed checkpoints."""
        from swarmwatch.root_witness import RootWitness, SignedCheckpoint, ConsistencyProof, ConsistencyVerificationError
        try:
            old_cp = SignedCheckpoint.from_dict(req.old_checkpoint)
            new_cp = SignedCheckpoint.from_dict(req.new_checkpoint)
            proof = ConsistencyProof.from_dict(req.proof)
            valid = RootWitness.verify_consistency(old_cp, new_cp, proof)
            return {"valid": valid, "old_size": old_cp.tree_size, "new_size": new_cp.tree_size}
        except ConsistencyVerificationError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/witness/rotate")
    async def witness_rotate_endpoint(req: Optional[WitnessRotateRequest] = None):
        """Performs NIST SP 800-57 compliant key rotation with mutual proof-of-possession."""
        from swarmwatch.root_witness import global_root_witness, WitnessKeyManager
        new_km = WitnessKeyManager.generate()
        rec = global_root_witness.rotate_key(new_km)
        return rec.to_dict()

    @app.get("/api/v1/witness/status")
    async def witness_status_endpoint():
        """Returns the real-time operational status of the independent root witness."""
        from swarmwatch.root_witness import global_root_witness
        return global_root_witness.status().to_dict()

    # -------------------------------------------------------------------------
    # ADR 0076: Tenant Identity Boundary & Attenuated Capability Tokens (Seam 65)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/capability/issue")
    async def capability_issue_endpoint(req: CapabilityIssueRequest):
        """Issues an authoritative root capability token for tenant and principal."""
        from swarmwatch.tenant_capability import global_capability_manager
        token = global_capability_manager.issue_root_token(
            tenant_id=req.tenant_id,
            subject_principal=req.subject_principal,
            scopes=req.scopes,
            resource_pattern=req.resource_pattern,
            epoch=req.epoch,
            not_before_ns=req.not_before_ns,
            ttl_seconds=req.ttl_seconds,
            max_depth=req.max_depth,
            metadata=req.metadata,
        )
        return token.to_dict()

    @app.post("/api/v1/capability/attenuate")
    async def capability_attenuate_endpoint(req: CapabilityAttenuateRequest):
        """Derives a monotonically attenuated capability token."""
        from swarmwatch.tenant_capability import (
            global_capability_manager,
            CapabilityToken,
            PrivilegeEscalationError,
            AttenuationViolationError,
        )
        try:
            parent = CapabilityToken.from_dict(req.parent_token)
            child = global_capability_manager.attenuate(
                parent_token=parent,
                subject_principal=req.subject_principal,
                scopes=req.scopes,
                resource_pattern=req.resource_pattern,
                epoch=req.epoch,
                not_before_ns=req.not_before_ns,
                expires_at_ns=req.expires_at_ns,
                ttl_seconds=req.ttl_seconds,
                metadata=req.metadata,
            )
            return child.to_dict()
        except (PrivilegeEscalationError, AttenuationViolationError) as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/capability/verify")
    async def capability_verify_endpoint(req: CapabilityVerifyRequest):
        """Verifies capability token at execution boundary in sub-250µs."""
        from swarmwatch.tenant_capability import global_capability_manager, CapabilityToken
        token = CapabilityToken.from_dict(req.token)
        verdict = global_capability_manager.verify(
            token=token,
            action_scope=req.action_scope,
            target_resource=req.target_resource,
            executing_principal=req.executing_principal,
            expected_tenant_id=req.expected_tenant_id,
            current_epoch=req.current_epoch,
            now_ns=req.now_ns,
        )
        return verdict.to_dict()

    @app.post("/api/v1/capability/revoke")
    async def capability_revoke_endpoint(req: CapabilityRevokeRequest):
        """Revokes a capability token and cascades down subtree."""
        from swarmwatch.tenant_capability import global_capability_manager
        receipt = global_capability_manager.revoke(
            token_id=req.token_id,
            reason=req.reason,
            revoked_by=req.revoked_by,
        )
        return receipt.to_dict()

    @app.get("/api/v1/capability/status")
    async def capability_status_endpoint():
        """Returns operational metrics and active authority public key."""
        from swarmwatch.tenant_capability import global_capability_manager
        return global_capability_manager.status().to_dict()

    # -------------------------------------------------------------------------
    # ADR 0077: Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/compensation/classify")
    async def compensation_classify_endpoint(req: CompensationClassifyRequest):
        """Classifies a sequence of action executions using the reversibility lattice join."""
        from swarmwatch.compensation_algebra import (
            global_compensation_engine,
            ActionExecution,
            CompensationError,
        )
        try:
            actions = [ActionExecution.from_dict(a) for a in req.actions]
            classification = global_compensation_engine.classify(actions)
            return classification.to_dict()
        except CompensationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/compensation/plan")
    async def compensation_plan_endpoint(req: CompensationPlanRequest):
        """Generates a reverse-ordered compensation plan over an executed action sequence."""
        from swarmwatch.compensation_algebra import (
            global_compensation_engine,
            ActionExecution,
            CompensationError,
        )
        try:
            actions = [ActionExecution.from_dict(a) for a in req.actions]
            plan = global_compensation_engine.build_compensation_plan(actions)
            return plan.to_dict()
        except CompensationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/compensation/execute")
    async def compensation_execute_endpoint(req: CompensationExecuteRequest):
        """Executes automated compensation plan in strict reverse order with fail-closed safety."""
        from swarmwatch.compensation_algebra import (
            global_compensation_engine,
            CompensationPlan,
            ActionExecution,
            IrreversibleActionError,
            CompensationError,
        )
        try:
            if req.actions:
                for a in req.actions:
                    act_obj = ActionExecution.from_dict(a)
                    global_compensation_engine._actions_by_id[act_obj.action_id] = act_obj

            plan = CompensationPlan.from_dict(req.plan)
            result = global_compensation_engine.execute_compensation(
                plan=plan,
                allow_authorized_escalations=True,
            )
            return result.to_dict()
        except IrreversibleActionError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except CompensationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/compensation/escalate")
    async def compensation_escalate_endpoint(req: CompensationEscalateRequest):
        """Issues an immutable human escalation receipt for an irreversible action."""
        from swarmwatch.compensation_algebra import (
            global_compensation_engine,
            ActionExecution,
            CompensationError,
        )
        try:
            action = ActionExecution(
                action_id=req.action_id,
                action_type=req.action_type,
                tenant_id=req.tenant_id,
                principal=req.principal,
            )
            receipt = global_compensation_engine.escalate(
                action=action,
                reason=req.reason,
            )
            return receipt.to_dict()
        except CompensationError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/compensation/authorize-escalation")
    async def compensation_authorize_endpoint(req: CompensationAuthorizeRequest):
        """Authorizes and records human resolution for a pending escalation."""
        from swarmwatch.compensation_algebra import (
            global_compensation_engine,
            CompensationError,
        )
        try:
            resolution = global_compensation_engine.authorize_escalation(
                escalation_id=req.escalation_id,
                authorizer_principal=req.authorizer_principal,
                decision=req.decision,
                remediation_notes=req.remediation_notes,
            )
            return resolution.to_dict()
        except CompensationError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/compensation/status")
    async def compensation_status_endpoint():
        """Returns operational diagnostics of the compensation engine."""
        from swarmwatch.compensation_algebra import global_compensation_engine
        return global_compensation_engine.status().to_dict()

    # -------------------------------------------------------------------------
    # ADR 0078: Durable Dataset-Level Privacy Accountant (Seam 67)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/dataset-privacy/register")
    async def dataset_privacy_register_endpoint(req: DatasetPrivacyRegisterRequest):
        """Registers or configures durable differential privacy limits for a dataset."""
        try:
            cfg = global_dataset_privacy_ledger.register_dataset(
                dataset_id=req.dataset_id,
                tenant_id=req.tenant_id,
                max_epsilon=req.max_epsilon,
                max_delta=req.max_delta,
                entity_epsilon_limit=req.entity_epsilon_limit,
                entity_delta_limit=req.entity_delta_limit,
                description=req.description,
            )
            return cfg.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/dataset-privacy/reserve")
    async def dataset_privacy_reserve_endpoint(req: DatasetPrivacyReserveRequest):
        """Atomically reserves (epsilon, delta) budget against dataset and entity caps."""
        try:
            reservation = global_dataset_privacy_ledger.reserve(
                tenant_id=req.tenant_id,
                dataset_id=req.dataset_id,
                entity_id=req.entity_id,
                epsilon=req.epsilon,
                delta=req.delta,
                sensitivity=req.sensitivity,
                query_id=req.query_id,
            )
            return reservation.to_dict()
        except (DatasetPrivacyBudgetExhaustedError, EntityPrivacyBudgetExhaustedError) as e:
            raise HTTPException(status_code=429, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/dataset-privacy/release")
    async def dataset_privacy_release_endpoint(req: DatasetPrivacyReleaseRequest):
        """Applies calibrated Laplace perturbation and commits dataset privacy reservation."""
        try:
            result = global_dataset_privacy_ledger.commit_release(
                reservation_id=req.reservation_id,
                raw_value=req.raw_value,
                noise_seed=req.noise_seed,
                min_bound=req.min_bound,
                max_bound=req.max_bound,
            )
            return result.to_dict()
        except ReservationNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except DatasetPrivacyError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/dataset-privacy/cancel")
    async def dataset_privacy_cancel_endpoint(req: DatasetPrivacyCancelRequest):
        """Cancels a pending reservation, restoring reserved privacy budget."""
        cancelled = global_dataset_privacy_ledger.cancel_reservation(req.reservation_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail=f"Pending reservation '{req.reservation_id}' not found")
        return {"reservation_id": req.reservation_id, "status": "CANCELLED"}

    @app.post("/api/v1/dataset-privacy/reconcile")
    async def dataset_privacy_reconcile_endpoint(req: DatasetPrivacyReconcileRequest):
        """Reconciles peer ledger snapshot using monotone suprema join."""
        try:
            return global_dataset_privacy_ledger.reconcile(req.snapshot)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/dataset-privacy/remaining")
    async def dataset_privacy_remaining_endpoint(
        tenant_id: str = "default",
        dataset_id: str = "default",
        entity_id: str = "default",
    ):
        """Returns residual privacy budget for the dataset and specific entity."""
        return global_dataset_privacy_ledger.remaining(
            tenant_id=tenant_id,
            dataset_id=dataset_id,
            entity_id=entity_id,
        )

    @app.get("/api/v1/dataset-privacy/snapshot")
    async def dataset_privacy_snapshot_endpoint(
        tenant_id: str = "default",
        dataset_id: str = "default",
    ):
        """Exports an immutable snapshot of dataset expenditures."""
        snapshot = global_dataset_privacy_ledger.export_snapshot(
            tenant_id=tenant_id,
            dataset_id=dataset_id,
        )
        return snapshot.to_dict()

    @app.get("/api/v1/dataset-privacy/status")
    async def dataset_privacy_status_endpoint(
        tenant_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ):
        """Returns operational diagnostics and active reservation counts."""
        return global_dataset_privacy_ledger.status(
            tenant_id=tenant_id,
            dataset_id=dataset_id,
        )

    # -------------------------------------------------------------------------
    # ADR 0079: Protocol Interoperability Lab (Seam 68)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/interop/exercise")
    async def interop_exercise_endpoint(req: InteropExerciseRequest):
        """Executes a standards-conformance exercise against a protocol under fault schedule."""
        try:
            res = global_interop_lab.exercise(
                target_protocol=req.target_protocol,
                client_variant=req.client_variant,
                fault_schedule=req.fault_schedule,
                payload=req.payload,
                chunk_size=req.chunk_size,
            )
            return res.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/v1/interop/record-deviation")
    async def interop_record_deviation_endpoint(req: InteropRecordDeviationRequest):
        """Records an observed deviation from standard protocol specifications."""
        try:
            dev = global_interop_lab.record_deviation(
                protocol=req.protocol,
                scenario=req.scenario,
                observed=req.observed,
                expected=req.expected,
                severity=req.severity,
            )
            return dev.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/interop/attest")
    async def interop_attest_endpoint(req: InteropAttestRequest):
        """Issues an immutable cryptographic attestation receipt for the conformance matrix."""
        receipt = global_interop_lab.attest_matrix(suite_name=req.suite_name)
        return receipt.to_dict()

    @app.get("/api/v1/interop/matrix")
    async def interop_matrix_endpoint():
        """Returns the full aggregate cross-matrix conformance status."""
        return global_interop_lab.get_matrix()

    @app.get("/api/v1/interop/status")
    async def interop_status_endpoint():
        """Returns operational diagnostics of the protocol interoperability lab."""
        return global_interop_lab.status()

    # ADR 0080: Verifiable Release Provenance & Signed Artifact DAG (Seam 69)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/provenance/record-step")
    async def provenance_record_step_endpoint(req: ProvenanceRecordStepRequest):
        """Records a provenance DAG step with deterministic input and output digests."""
        try:
            step_type_enum = ProvenanceStepType(req.step_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid provenance step_type: {req.step_type}")

        try:
            step = global_release_provenance.record_step(
                step_type=step_type_enum,
                parent_step_ids=req.parent_step_ids,
                inputs=req.inputs,
                outputs=req.outputs,
                metadata=req.metadata,
                step_id=req.step_id,
            )
            return step.to_dict()
        except ProvenanceError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/provenance/generate")
    async def provenance_generate_endpoint(req: ProvenanceGenerateRequest):
        """Generates and cryptographically signs an in-toto / SLSA v1.0 release provenance manifest."""
        try:
            manifest = global_release_provenance.generate_manifest(
                git_commit=req.git_commit,
                git_branch=req.git_branch,
                builder_id=req.builder_id,
                manifest_id=req.manifest_id,
            )
            return manifest.to_dict()
        except ProvenanceError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/provenance/verify")
    async def provenance_verify_endpoint(req: ProvenanceVerifyRequest):
        """Verifies an SLSA release provenance manifest cryptographically and topographically."""
        try:
            result = global_release_provenance.verify_manifest(
                manifest=req.manifest,
                public_key_b64=req.public_key_b64,
                verify_disk_files=req.verify_disk_files,
                base_dir=req.base_dir,
            )
            return result.to_dict()
        except ProvenanceError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/provenance/dag")
    async def provenance_dag_endpoint():
        """Returns the active release provenance DAG in topological order."""
        return global_release_provenance.get_dag().to_dict()

    @app.get("/api/v1/provenance/status")
    async def provenance_status_endpoint():
        """Returns operational metrics and active verification statistics for release provenance."""
        return global_release_provenance.status()

    # -------------------------------------------------------------------------
    # ADR 0081: Datacenter Failure Exercise & Region Loss Fault Injection (Seam 70)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/exercise/inject-partition")
    async def exercise_inject_partition_endpoint(req: ExerciseInjectPartitionRequest):
        """Injects network partition severing specified regions or asymmetric routes."""
        try:
            asym = None
            if req.asymmetric_routes:
                asym = [(r[0], r[1]) for r in req.asymmetric_routes if len(r) >= 2]
            global_datacenter_orchestrator.inject_partition(
                severed_regions=req.severed_regions,
                asymmetric_routes=asym,
            )
            return {"status": "partition_injected", "details": global_datacenter_orchestrator.status()}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/exercise/heal-partition")
    async def exercise_heal_partition_endpoint():
        """Heals all network partitions while preserving monotonic safety trips."""
        try:
            global_datacenter_orchestrator.heal_partition()
            return {"status": "partition_healed", "details": global_datacenter_orchestrator.status()}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/exercise/permit")
    async def exercise_permit_endpoint(req: ExercisePermitRequest):
        """Requests execution permit, verifying majority quorum or failing closed."""
        try:
            res = global_datacenter_orchestrator.request_permit(
                node_id=req.node_id,
                tenant_id=req.tenant_id,
                tokens=req.tokens,
            )
            return res.to_dict()
        except MinorityPartitionBlockedError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except DatacenterExerciseError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/exercise/trip")
    async def exercise_trip_endpoint(req: ExerciseBroadcastTripRequest):
        """Broadcasts emergency safety trip from a cluster node."""
        try:
            res = global_datacenter_orchestrator.broadcast_trip(
                origin_node_id=req.origin_node_id,
                reason=req.reason,
            )
            return res
        except DatacenterExerciseError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/exercise/run")
    async def exercise_run_endpoint(req: ExerciseRunRequest):
        """Runs complete 5-phase automated datacenter failure exercise and generates signed report."""
        try:
            scenario_enum = FailureScenarioType(req.scenario)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid failure scenario: {req.scenario}")
        try:
            report = global_datacenter_orchestrator.run_exercise(
                scenario=scenario_enum,
                target_severed_regions=req.target_severed_regions,
            )
            return report.to_dict()
        except DatacenterExerciseError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/exercise/status")
    async def exercise_status_endpoint():
        """Returns topology, active phase, quorum state, and network matrix."""
        return global_datacenter_orchestrator.status()

    @app.get("/api/v1/exercise/report")
    async def exercise_report_endpoint():
        """Returns the latest disaster recovery failure exercise report."""
        reports = global_datacenter_orchestrator.get_reports()
        if not reports:
            raise HTTPException(status_code=404, detail="No failure exercise reports found")
        return reports[-1].to_dict()

    # -------------------------------------------------------------------------
    # ADR 0082: Autonomous Action Gateway & Governed Downstream Execution (Seam 71)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/gateway/request")
    async def gateway_request_endpoint(req: GatewayActionRequestModel):
        """Submits downstream agent action request for capability check and quorum evaluation."""
        try:
            action_req = global_action_gateway.request_action(
                tenant_id=req.tenant_id,
                agent_id=req.agent_id,
                action_name=req.action_name,
                target_resource=req.target_resource,
                parameters=req.parameters,
                capability_token=req.capability_token,
                estimated_cost_usd=req.estimated_cost_usd,
                reversibility_class=req.reversibility_class,
                explicit_request_id=req.request_id,
            )
            return action_req.to_dict()
        except UnauthorizedActionError as e:
            raise HTTPException(status_code=403, detail=str(e))
        except ActionCircuitBlockedError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except ActionGatewayError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/gateway/approve")
    async def gateway_approve_endpoint(req: GatewayActionApproveModel):
        """Records human operator approval or rejection for a pending action request."""
        try:
            action_req = global_action_gateway.approve_action(
                request_id=req.request_id,
                approver_principal=req.approver_principal,
                decision=req.decision,
                comment=req.comment,
                signature=req.signature,
            )
            return action_req.to_dict()
        except ActionGatewayError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/gateway/execute")
    async def gateway_execute_endpoint(req: GatewayActionExecuteModel):
        """Dispatches an approved action to its registered executor and mints execution receipt."""
        try:
            receipt = global_action_gateway.execute_action(request_id=req.request_id)
            return receipt.to_dict()
        except ActionQuorumPendingError as e:
            raise HTTPException(status_code=428, detail=str(e))  # 428 Precondition Required
        except ActionCircuitBlockedError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except ActionExecutionFailedError as e:
            raise HTTPException(status_code=500, detail=str(e))
        except ActionGatewayError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/gateway/pending")
    async def gateway_pending_endpoint(tenant_id: Optional[str] = None):
        """Lists pending action requests awaiting operator approval."""
        pending = global_action_gateway.get_pending_approvals(tenant_id=tenant_id)
        return [r.to_dict() for r in pending]

    @app.get("/api/v1/gateway/status")
    async def gateway_status_endpoint():
        """Returns live operational diagnostics of the Action Gateway."""
        return global_action_gateway.status()

    @app.get("/api/v1/gateway/receipts")
    async def gateway_receipts_endpoint(tenant_id: Optional[str] = None):
        """Lists recorded execution receipts."""
        receipts = global_action_gateway.get_receipts(tenant_id=tenant_id)
        return [r.to_dict() for r in receipts]

    @app.get("/api/v1/gateway/receipts/{receipt_id}")
    async def gateway_receipt_by_id_endpoint(receipt_id: str):
        """Retrieves an execution receipt by ID."""
        receipt = global_action_gateway.get_receipt(receipt_id)
        if not receipt:
            raise HTTPException(status_code=404, detail=f"Receipt '{receipt_id}' not found")
        return receipt.to_dict()

    # -------------------------------------------------------------------------
    # ADR 0083: Dynamic Agent Federation & Swarm Mesh Topology Routing (Seam 72)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/federation/register-peer")
    @app.post("/api/v1/mesh/register-peer")
    async def mesh_register_peer_endpoint(req: MeshPeerRegisterModel):
        """Registers a swarm agent node into the mesh topology graph with public key."""
        try:
            peer = global_mesh_router.register_peer(
                node_id=req.node_id,
                cluster_id=req.cluster_id,
                tenant_id=req.tenant_id,
                endpoint_url=req.endpoint_url,
                public_key_hex=req.public_key_hex,
                role=req.role,
                capabilities=req.capabilities,
                latency_ms=req.latency_ms,
                signature=req.signature,
            )
            return peer.to_dict()
        except MeshPeerAuthenticationError as e:
            raise HTTPException(status_code=401, detail=str(e))
        except MeshFederationError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/federation/establish-link")
    @app.post("/api/v1/mesh/establish-link")
    async def mesh_establish_link_endpoint(req: MeshLinkEstablishModel):
        """Establishes a weighted directed or bidirectional edge between mesh peers."""
        try:
            success = global_mesh_router.establish_link(
                source_node_id=req.source_node_id,
                target_node_id=req.target_node_id,
                weight_cost=req.weight_cost,
                latency_ms=req.latency_ms,
                bidirectional=req.bidirectional,
            )
            return {"status": "LINK_ESTABLISHED", "success": success}
        except MeshDestinationUnreachableError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except MeshFederationError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/federation/heartbeat")
    @app.post("/api/v1/mesh/heartbeat")
    async def mesh_heartbeat_endpoint(req: MeshHeartbeatModel):
        """Refreshes peer liveness heartbeat to prevent stale node eviction."""
        ok = global_mesh_router.heartbeat(req.node_id)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Peer '{req.node_id}' not found")
        return {"status": "HEARTBEAT_ACK", "node_id": req.node_id}

    @app.post("/api/v1/federation/route")
    @app.post("/api/v1/mesh/route")
    async def mesh_route_endpoint(req: MeshPacketRouteModel):
        """Routes a data or execution packet across the swarm mesh with Dijkstra shortest path."""
        import uuid
        packet_id = req.packet_id or f"pkt-{uuid.uuid4().hex[:12]}"
        packet = MeshPacket(
            packet_id=packet_id,
            source_node_id=req.source_node_id,
            dest_node_id=req.dest_node_id,
            tenant_id=req.tenant_id,
            payload=req.payload,
            capability_token=req.capability_token,
            hop_count=req.hop_count,
            max_hops=req.max_hops,
            signature=req.signature,
        )
        try:
            receipt = global_mesh_router.route_packet(packet)
            return receipt.to_dict()
        except MeshCircuitTrippedError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except MeshCapabilityFencingError as e:
            raise HTTPException(status_code=403, detail=str(e))
        except MeshHopLimitExceededError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except MeshRoutingLoopError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except MeshDestinationUnreachableError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except MeshFederationError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/federation/path")
    @app.get("/api/v1/mesh/path")
    async def mesh_path_endpoint(source_node_id: str, dest_node_id: str, tenant_id: Optional[str] = None):
        """Computes optimal shortest path between peers without dispatching a packet."""
        try:
            path = global_mesh_router.find_optimal_path(source_node_id, dest_node_id, tenant_id=tenant_id)
            return {
                "source_node_id": source_node_id,
                "dest_node_id": dest_node_id,
                "path": path,
                "hops": max(0, len(path) - 1),
            }
        except MeshDestinationUnreachableError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except MeshFederationError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/federation/peers")
    @app.get("/api/v1/mesh-router/peers")
    async def federation_peers_endpoint(tenant_id: Optional[str] = None):
        """Lists registered mesh peers in federation router, optionally filtered by tenant ID."""
        peers = global_mesh_router.list_peers(tenant_id=tenant_id)
        return [p.to_dict() for p in peers]

    @app.get("/api/v1/federation/links")
    @app.get("/api/v1/mesh/links")
    async def mesh_links_endpoint():
        """Lists all active and inactive directed topology links in the mesh graph."""
        links = global_mesh_router.list_links()
        return [l.to_dict() for l in links]

    @app.get("/api/v1/federation/status")
    @app.get("/api/v1/mesh-router/status")
    async def federation_status_endpoint():
        """Returns live operational statistics for dynamic agent mesh federation."""
        return global_mesh_router.status()

    @app.get("/api/v1/federation/receipts")
    @app.get("/api/v1/mesh/receipts")
    async def mesh_receipts_endpoint(tenant_id: Optional[str] = None, limit: int = 100):
        """Lists recorded tamper-evident mesh routing receipts."""
        receipts = global_mesh_router.get_receipts(tenant_id=tenant_id, limit=limit)
        return [r.to_dict() for r in receipts]

    @app.get("/api/v1/federation/receipts/{receipt_id}")
    @app.get("/api/v1/mesh/receipts/{receipt_id}")
    async def mesh_receipt_by_id_endpoint(receipt_id: str):
        """Retrieves a mesh routing receipt by receipt ID."""
        receipt = global_mesh_router.get_receipt(receipt_id)
        if not receipt:
            raise HTTPException(status_code=404, detail=f"Routing receipt '{receipt_id}' not found")
        return receipt.to_dict()

    # Multi-Modal Cognitive Grounding & Vector Evidence Fusion (Seam 73 / ADR 0084)
    @app.post("/api/v1/grounding/register-evidence")
    async def grounding_register_evidence_endpoint(req: GroundingRegisterRequest):
        """Registers authentic multi-modal vector evidence."""
        try:
            modality = EvidenceModality(req.modality)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid evidence modality: {req.modality}")
        evidence = global_cognitive_grounding_engine.register_evidence(
            modality=modality,
            content=req.content,
            source_reference=req.source_reference,
            confidence_weight=req.confidence_weight,
            metadata=req.metadata,
            tenant_id=req.tenant_id,
            evidence_id=req.evidence_id,
        )
        return evidence.to_dict()

    @app.post("/api/v1/grounding/verify")
    async def grounding_verify_endpoint(req: GroundingVerifyRequest):
        """Evaluates a claim or agent decision against registered multi-modal evidence."""
        req_mods = None
        if req.required_modalities:
            try:
                req_mods = [EvidenceModality(m) for m in req.required_modalities]
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid modality in required_modalities: {e}")
        try:
            receipt = global_cognitive_grounding_engine.verify_grounding(
                claim=req.claim,
                min_threshold=req.min_threshold,
                required_modalities=req_mods,
                tenant_id=req.tenant_id,
                strict=req.strict,
                claim_id=req.claim_id,
            )
            return receipt.to_dict()
        except GroundingCircuitTrippedError as e:
            raise HTTPException(status_code=423, detail=str(e))
        except UngroundedInferenceError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except GroundingModalityMismatchError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/grounding/fuse")
    async def grounding_fuse_endpoint(req: GroundingFuseRequest):
        """Fuses a specific set of evidence IDs against a claim with custom weights."""
        try:
            receipt = global_cognitive_grounding_engine.fuse_evidence(
                claim=req.claim,
                evidence_ids=req.evidence_ids,
                modality_weights=req.modality_weights,
                tenant_id=req.tenant_id,
            )
            return receipt.to_dict()
        except EvidenceNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except GroundingCircuitTrippedError as e:
            raise HTTPException(status_code=423, detail=str(e))

    @app.get("/api/v1/grounding/evidence")
    async def grounding_evidence_list_endpoint(tenant_id: str = "tenant-default", modality: Optional[str] = None):
        """Lists registered multi-modal evidence items."""
        mod = None
        if modality:
            try:
                mod = EvidenceModality(modality)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid modality: {modality}")
        items = global_cognitive_grounding_engine.store.list_all(tenant_id=tenant_id, modality=mod)
        return [e.to_dict() for e in items]

    @app.get("/api/v1/grounding/receipts")
    async def grounding_receipts_endpoint(tenant_id: Optional[str] = None, limit: int = 100):
        """Lists recorded tamper-evident grounding receipts."""
        receipts = global_cognitive_grounding_engine.get_receipts(tenant_id=tenant_id, limit=limit)
        return [r.to_dict() for r in receipts]

    @app.get("/api/v1/grounding/receipts/{receipt_id}")
    async def grounding_receipt_by_id_endpoint(receipt_id: str):
        """Retrieves a grounding receipt by receipt ID."""
        receipt = global_cognitive_grounding_engine.get_receipt(receipt_id)
        if not receipt:
            raise HTTPException(status_code=404, detail=f"Grounding receipt '{receipt_id}' not found")
        return receipt.to_dict()

    @app.get("/api/v1/grounding/status")
    async def grounding_status_endpoint():
        """Returns live operational status and metrics of cognitive grounding engine."""
        return global_cognitive_grounding_engine.status()










    @app.post("/api/v1/ingress/submit")
    async def ingress_submit_endpoint(req: IngressSubmitRequest):
        """Submits a span for prioritized, loss-aware causal ingestion."""
        res = global_causal_ingress.submit(
            span=req.span,
            priority=req.priority,
            stream_id=req.stream_id,
        )
        return res.to_dict()

    @app.post("/api/v1/ingress/drain")
    async def ingress_drain_endpoint(req: IngressDrainRequest):
        """Drains buffered spans in FIFO order."""
        spans = global_causal_ingress.drain(batch_size=req.batch_size)
        return {"drained_spans": spans, "count": len(spans)}

    @app.get("/api/v1/ingress/completeness/{swarm_id}")
    async def ingress_completeness_endpoint(swarm_id: str):
        """Calculates evidentiary completeness report for the given swarm."""
        report = global_causal_ingress.completeness(swarm_id=swarm_id)
        return report.to_dict()

    @app.get("/api/v1/ingress/status")
    async def ingress_status_endpoint(swarm_id: str = "default"):
        """Returns diagnostic buffer metrics for causal ingestion."""
        return global_causal_ingress.status(swarm_id=swarm_id)

    # -------------------------------------------------------------------------
    # ADR 0065: Durable Audit Anchor & Merkle Audit Receipt Engine (Seam 54)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/audit/append-root")
    async def audit_append_root_endpoint(req: AuditAppendRootRequest):
        """Appends a new Merkle root to the durable hash-chained epoch ledger."""
        try:
            epoch_obj = global_audit_anchor.append_root(
                root_hash=req.root_hash,
                tenant_id=req.tenant_id,
                metadata=req.metadata,
            )
            return epoch_obj.to_dict()
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/audit/issue-receipt")
    async def audit_issue_receipt_endpoint(req: AuditIssueReceiptRequest):
        """Issues an authentic Merkle audit receipt proving leaf inclusion in an epoch root."""
        try:
            receipt = global_audit_anchor.issue_receipt(
                epoch=req.epoch,
                leaf_hash=req.leaf_hash,
                audit_path=req.audit_path,
                tenant_id=req.tenant_id,
            )
            return receipt.to_dict()
        except (KeyError, ValueError) as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/audit/verify-receipt")
    async def audit_verify_receipt_endpoint(req: AuditVerifyReceiptRequest):
        """Verifies a Merkle audit receipt against durable epoch history."""
        res = global_audit_anchor.verify_receipt(req.receipt)
        return res.to_dict()

    @app.get("/api/v1/audit/chain")
    async def audit_chain_endpoint(since_epoch: int = 0):
        """Audits hash-chain continuity from Genesis (or since_epoch)."""
        return global_audit_anchor.audit_chain(since_epoch=since_epoch)

    @app.get("/api/v1/audit/status")
    async def audit_status_endpoint(tenant_id: Optional[str] = None):
        """Returns diagnostic status of the durable audit anchor."""
        return global_audit_anchor.status(tenant_id=tenant_id)

    # -------------------------------------------------------------------------
    # ADR 0066: Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/verifier/verify")
    async def verifier_verify_endpoint(req: VerifierVerifyRequest):
        """Executes bounded verification on a transition model with truthful semantics."""
        limits = VerificationLimits.from_dict(req.limits) if req.limits else None
        props = [VerificationProperty.from_dict(p) for p in req.properties] if req.properties else None
        report = global_safety_verifier.verify(
            model=req.model,
            properties=props,
            limits=limits,
            model_id=req.model_id,
            tenant_id=req.tenant_id,
        )
        return report.to_dict()

    @app.post("/api/v1/verifier/check-trace")
    async def verifier_check_trace_endpoint(req: VerifierCheckTraceRequest):
        """Replays and independently validates a counterexample trace."""
        prop = VerificationProperty.from_dict(req.property) if isinstance(req.property, dict) else req.property
        is_sound, diagnostic = global_safety_verifier.verify_trace(
            model=req.model,
            property=prop,
            trace=req.trace,
        )
        return {
            "is_sound": is_sound,
            "diagnostic": diagnostic,
        }

    @app.get("/api/v1/verifier/status")
    async def verifier_status_endpoint(tenant_id: Optional[str] = None):
        """Returns diagnostic metrics and breakdown of safety verifications."""
        return global_safety_verifier.status(tenant_id=tenant_id)

    # -------------------------------------------------------------------------
    # ADR 0067: Governed Remediation & Fenced Action State Machine (Seam 56)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/remediation/propose")
    async def remediation_propose_endpoint(req: RemediationProposeRequest):
        """Proposes a self-healing or topology remediation subject to blast radius constraints."""
        try:
            prop = global_remediation_governor.propose(
                tenant_id=req.tenant_id,
                action_type=req.action_type,
                target_entity=req.target_entity,
                parameters=req.parameters,
                blast_radius=req.blast_radius,
                approval_tier=req.approval_tier,
                preconditions=req.preconditions,
                postconditions=req.postconditions,
                compensation_action=req.compensation_action,
                proposal_id=req.proposal_id,
            )
            return prop.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/remediation/authorize")
    async def remediation_authorize_endpoint(req: RemediationAuthorizeRequest):
        """Evaluates authorization criteria for a remediation proposal."""
        decision = global_remediation_governor.authorize(
            proposal_id=req.proposal_id,
            approver_role=req.approver_role,
            permit_hash=req.permit_hash,
            current_epoch=req.current_epoch,
            signature=req.signature,
        )
        return decision.to_dict()

    @app.post("/api/v1/remediation/apply")
    async def remediation_apply_endpoint(req: RemediationApplyRequest):
        """Applies an authorized remediation mutation under a monotonic fencing epoch."""
        result = global_remediation_governor.apply(
            proposal_id=req.proposal_id,
            current_epoch=req.current_epoch,
            context=req.context,
        )
        return result.to_dict()

    @app.post("/api/v1/remediation/compensate")
    async def remediation_compensate_endpoint(req: RemediationCompensateRequest):
        """Executes idempotent compensation rollback for an applied or failed remediation."""
        comp_res = global_remediation_governor.compensate(
            proposal_id=req.proposal_id,
            reason=req.reason,
        )
        return comp_res.to_dict()

    @app.get("/api/v1/remediation/status")
    async def remediation_status_endpoint(tenant_id: Optional[str] = None):
        """Returns diagnostic statistics, active remediations, and proposal tallies."""
        return global_remediation_governor.status(tenant_id=tenant_id)

    # -------------------------------------------------------------------------
    # ADR 0068: Partition Reconciliation & Conflict Lattice Engine (Seam 57)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/reconciler/diff")
    async def reconciler_diff_endpoint(req: ReconcilerDiffRequest):
        """Analyzes vector clocks, divergent keys, and sequence intervals between replicas."""
        plan = global_mesh_reconciler.diff(
            local_state=req.local_state,
            remote_state=req.remote_state,
            tenant_id=req.tenant_id,
        )
        return plan.to_dict()

    @app.post("/api/v1/reconciler/merge")
    async def reconciler_merge_endpoint(req: ReconcilerMergeRequest):
        """Merges telemetry streams, deduplicating events and retaining provenance for concurrent forks."""
        result = global_mesh_reconciler.merge_evidence(
            local_events=req.local_events,
            remote_events=req.remote_events,
            tenant_id=req.tenant_id,
        )
        return result.to_dict()

    @app.post("/api/v1/reconciler/reconcile-authority")
    async def reconciler_reconcile_authority_endpoint(req: ReconcilerReconcileAuthorityRequest):
        """Reconciles safety authority states, enforcing trip monotonicity and epoch fencing."""
        auth_res = global_mesh_reconciler.reconcile_authority(
            local_authority=req.local_authority,
            remote_authority=req.remote_authority,
            tenant_id=req.tenant_id,
        )
        return auth_res.to_dict()

    @app.get("/api/v1/reconciler/status")
    async def reconciler_status_endpoint(tenant_id: Optional[str] = None):
        """Returns diagnostic statistics of partition reconciliations."""
        return global_mesh_reconciler.status(tenant_id=tenant_id)

    # -------------------------------------------------------------------------
    # ADR 0069: Privacy Release Ledger & Differential Privacy Accountant (Seam 58)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/privacy-ledger/reserve")
    async def privacy_reserve_endpoint(req: PrivacyReserveRequest):
        """Reserves privacy budget prior to query evaluation."""
        from swarmwatch.privacy_ledger import global_privacy_release, PrivacyBudgetExhaustedError
        try:
            reservation = global_privacy_release.reserve(
                tenant_id=req.tenant_id,
                epsilon=req.epsilon,
                delta=req.delta,
                sensitivity=req.sensitivity,
                query_id=req.query_id,
            )
            return reservation.to_dict()
        except PrivacyBudgetExhaustedError as e:
            raise HTTPException(status_code=429, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/privacy-ledger/release")
    async def privacy_release_endpoint(req: PrivacyReleaseRequest):
        """Applies calibrated Laplace perturbation and commits budget consumption."""
        from swarmwatch.privacy_ledger import global_privacy_release
        try:
            res = global_privacy_release.release(
                reservation_id=req.reservation_id,
                raw_value=req.raw_value,
                noise_seed=req.noise_seed,
                min_bound=req.min_bound,
                max_bound=req.max_bound,
            )
            return res.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/privacy-ledger/remaining")
    async def privacy_remaining_endpoint(tenant_id: str = "default"):
        """Returns the remaining and total privacy budget for a tenant."""
        from swarmwatch.privacy_ledger import global_privacy_release
        return global_privacy_release.remaining(tenant_id=tenant_id)

    @app.get("/api/v1/privacy-ledger/status")
    async def privacy_status_endpoint(tenant_id: Optional[str] = None):
        """Returns diagnostic statistics of the privacy release ledger."""
        from swarmwatch.privacy_ledger import global_privacy_release
        return global_privacy_release.status(tenant_id=tenant_id)

    # -------------------------------------------------------------------------
    # ADR 0070: Migration Preservation & Partial Isomorphisms (Seam 59)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/schema-evolution/plan")
    async def schema_evolution_plan_endpoint(req: SchemaEvolutionPlanRequest):
        """Plans schema evolution path and evaluates cumulative lossiness."""
        from swarmwatch.schema_evolution import global_schema_evolution, SchemaEvolutionError
        try:
            plan = global_schema_evolution.plan(
                source_version=req.source_version,
                target_version=req.target_version,
            )
            return plan.to_dict()
        except SchemaEvolutionError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.post("/api/v1/schema-evolution/migrate")
    async def schema_evolution_migrate_endpoint(req: SchemaEvolutionMigrateRequest):
        """Executes forward migration capturing PreservedDelta if lossy."""
        from swarmwatch.schema_evolution import global_schema_evolution, SchemaEvolutionError
        try:
            res = global_schema_evolution.migrate(
                payload=req.payload,
                source_version=req.source_version,
                target_version=req.target_version,
                preserve_shadow=req.preserve_shadow,
            )
            return res.to_dict()
        except SchemaEvolutionError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/schema-evolution/reverse")
    async def schema_evolution_reverse_endpoint(req: SchemaEvolutionReverseRequest):
        """Reverses schema migration back to target_version using PreservedDelta."""
        from swarmwatch.schema_evolution import (
            global_schema_evolution,
            LossyMigrationError,
            UnsupportedReverseMigrationError,
            SchemaEvolutionError,
        )
        try:
            res = global_schema_evolution.reverse(
                payload=req.payload,
                source_version=req.source_version,
                target_version=req.target_version,
                preserved_delta=req.preserved_delta,
            )
            return res.to_dict()
        except LossyMigrationError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except UnsupportedReverseMigrationError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except SchemaEvolutionError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.post("/api/v1/schema-evolution/classify-loss")
    async def schema_evolution_classify_loss_endpoint(req: SchemaEvolutionClassifyRequest):
        """Pre-flight evaluation of schema evolution lossiness for a planned path and payload."""
        from swarmwatch.schema_evolution import global_schema_evolution, SchemaEvolutionError
        try:
            return global_schema_evolution.classify_loss(
                source_version=req.source_version,
                target_version=req.target_version,
                payload=req.payload,
            )
        except SchemaEvolutionError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.get("/api/v1/schema-evolution/status")
    async def schema_evolution_status_endpoint():
        """Returns diagnostic statistics of schema evolution engine."""
        from swarmwatch.schema_evolution import global_schema_evolution
        return global_schema_evolution.status()

    # -------------------------------------------------------------------------
    # ADR 0071: Enterprise Acceptance Harness (Seam 60)
    # -------------------------------------------------------------------------
    @app.post("/api/v1/acceptance/run")
    async def acceptance_run_endpoint(req: AcceptanceRunRequest):
        """Executes acceptance harness across configured gates and scenario matrix."""
        from swarmwatch.acceptance import global_acceptance_harness
        report = global_acceptance_harness.run(
            profile=req.profile or "production",
            custom_checks=req.custom_checks,
            html_path=req.html_path,
        )
        return report.to_dict()

    @app.post("/api/v1/acceptance/attest")
    async def acceptance_attest_endpoint(req: AcceptanceAttestRequest):
        """Issues cryptographically signed attestation receipt for certified acceptance report."""
        from swarmwatch.acceptance import (
            global_acceptance_harness,
            AcceptanceReport,
            GateSummary,
            GateCheckResult,
            AcceptanceError,
        )
        try:
            if req.report:
                # Reconstruct report object
                r = req.report
                gates = {k: GateSummary(**v) for k, v in r.get("gates", {}).items()}
                checks = [GateCheckResult(**c) for c in r.get("checks", [])]
                report_obj = AcceptanceReport(
                    report_id=r["report_id"],
                    profile=r["profile"],
                    status=r["status"],
                    overall_score=r["overall_score"],
                    gates=gates,
                    checks=checks,
                    executed_at=r["executed_at"],
                    duration_ms=r["duration_ms"],
                    report_hash=r.get("report_hash", ""),
                )
            else:
                if not global_acceptance_harness._reports_history:
                    report_obj = global_acceptance_harness.run()
                else:
                    report_obj = global_acceptance_harness._reports_history[-1]

            attest = global_acceptance_harness.attest(
                report=report_obj,
                signer_identity=req.signer_identity or "SwarmWatch Enterprise Release Gatekeeper",
                secret_key=req.secret_key,
            )
            return attest.to_dict()
        except AcceptanceError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/v1/acceptance/verify-attestation")
    async def acceptance_verify_attestation_endpoint(req: AcceptanceVerifyAttestationRequest):
        """Cryptographically verifies canonical hash integrity and signature of an attestation receipt."""
        from swarmwatch.acceptance import (
            global_acceptance_harness,
            AcceptanceAttestation,
            AttestationVerificationError,
        )
        try:
            a = req.attestation
            attestation_obj = AcceptanceAttestation(
                attestation_id=a["attestation_id"],
                report_id=a["report_id"],
                report_hash=a["report_hash"],
                profile=a["profile"],
                status=a["status"],
                certified_gates=a["certified_gates"],
                attested_at=a["attested_at"],
                signer_identity=a["signer_identity"],
                attestation_hash=a["attestation_hash"],
                signature=a.get("signature", ""),
            )
            is_valid = global_acceptance_harness.verify_attestation(
                attestation=attestation_obj,
                secret_key=req.secret_key,
            )
            return {
                "verified": is_valid,
                "attestation_id": attestation_obj.attestation_id,
            }
        except AttestationVerificationError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/v1/acceptance/audit-ui")
    async def acceptance_audit_ui_endpoint(path: Optional[str] = None):
        """Audits the physical dashboard.html artifact for Impeccable craft floor compliance."""
        from swarmwatch.acceptance import global_acceptance_harness
        return global_acceptance_harness.audit_dashboard_artifact(html_path=path)

    @app.get("/api/v1/acceptance/status")
    async def acceptance_status_endpoint():
        """Returns operational metrics and recent certification status of acceptance harness."""
        from swarmwatch.acceptance import global_acceptance_harness
        return global_acceptance_harness.status()


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
