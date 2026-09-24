# -*- coding: utf-8 -*-
"""
SwarmWatch: Universal Observability, Entropy Circuit Breaker & Time-Machine Debugging for Multi-Agent Systems.
"""

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent, CounterfactualSavings
from swarmwatch.storage import StorageAdapter, SQLiteStorageAdapter, ClickHouseStorageAdapter
from swarmwatch.transport import TelemetryTransport, TransportConfig
from swarmwatch.webhooks import WebhookConfig, WebhookNotifier, format_webhook_payload, DeadLetterRecord
from swarmwatch.otel import to_otel_genai_span, OtelGenAiExporter
from swarmwatch.core import (
    SwarmClient,
    SpanContext,
    init,
    get_client,
    agent,
    span,
    flush,
    shutdown,
    sync_state,
    record_circuit_event,
    snapshot,
    step_back,
    fork_branch,
    list_branches,
    list_snapshots,
    resume_from_fork,
    resume_from_fork_sync,
)
from swarmwatch.serializer import (
    SafeStateSerializer,
    TimeMachine,
    DiffEntry,
    DiffSummary,
    StateDiffReport,
    compute_state_diff,
)
from swarmwatch.runner import (
    ForkRunner,
    ForkResumptionContext,
    ForkResumptionResult,
)

from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPolicy,
    CircuitPreset,
    CircuitDecision,
    SwarmCircuitBreakException,
    compute_shannon_entropy,
    compute_lexical_similarity,
)

from swarmwatch.tenant import (
    Organization,
    Project,
    ApiKey,
    TenantContext,
    ApiKeyManager,
)
from swarmwatch.fleet import (
    FleetCircuitCoordinator,
    FleetMetrics,
    FleetEvent,
)
from swarmwatch.metrics import (
    PrometheusMetricsRegistry,
    Counter,
    Gauge,
    Histogram,
    record_span_ingested,
    record_circuit_trip,
    record_state_snapshot,
    record_branch_fork,
    set_active_swarms,
    set_active_websockets,
    set_telemetry_queue_depth,
)
from swarmwatch.adapters.llamaindex import SwarmWatchLlamaIndexHandler
from swarmwatch.adapters.autogen import SwarmWatchAutoGenListener
from swarmwatch.adapters.dspy import SwarmWatchDSPyTracker
from swarmwatch.adapters.semantic_kernel import (
    SwarmWatchSemanticKernelFilter,
    FunctionInvocationContext,
)
from swarmwatch.topology import (
    AgentNode,
    CausalEdge,
    CausalTopology,
    build_topology_from_spans,
)
from swarmwatch.security import (
    SensitiveDataMasker,
    SecuritySanitizationPolicy,
    mask_payload,
    mask_text,
)
from swarmwatch.report import (
    IncidentReportGenerator,
    IncidentPostMortem,
    FinancialRoiMetrics,
    RootCauseDiagnosis,
)
from swarmwatch.policy import (
    RuleConfig,
    RuleCondition,
    DeclarativeRule,
    GuardrailPolicy,
    PolicyEngine,
    SafePolicyParser,
    PolicyEvaluationResult,
)
from swarmwatch.synthetic_alert import (
    SyntheticAlertReceiver,
    AlertSimulationPipeline,
    AlertSimulationReport,
    RouteSimulationResult,
    ReceivedAlertEvent,
)
from swarmwatch.profiler import (
    TelemetryProfiler,
    IngestionBenchmarkConfig,
    IngestionBenchmarkReport,
    LatencyPercentiles,
    BenchmarkMemoryStats,
)
from swarmwatch.tui import TerminalMonitor, AnsiColor
from swarmwatch.redis_circuit import (
    RedisCircuitCoordinator,
    RedisFleetBackend,
    RESPRedisClient,
    MockRedisClient,
    RedisTripBroadcast,
)


__all__ = [
    "RedisCircuitCoordinator",
    "RedisFleetBackend",
    "RESPRedisClient",
    "MockRedisClient",
    "RedisTripBroadcast",
    "TerminalMonitor",

    "AnsiColor",
    "TelemetryProfiler",
    "IngestionBenchmarkConfig",
    "IngestionBenchmarkReport",
    "LatencyPercentiles",
    "BenchmarkMemoryStats",
    "SwarmSpan",
    "StateSnapshot",
    "CircuitEvent",
    "CounterfactualSavings",
    "StorageAdapter",
    "SQLiteStorageAdapter",
    "ClickHouseStorageAdapter",
    "TelemetryTransport",
    "TransportConfig",
    "WebhookConfig",
    "WebhookNotifier",
    "format_webhook_payload",
    "DeadLetterRecord",
    "to_otel_genai_span",
    "OtelGenAiExporter",
    "SwarmClient",
    "SpanContext",
    "EntropyCircuitBreaker",
    "CircuitPolicy",
    "CircuitPreset",
    "CircuitDecision",
    "SwarmCircuitBreakException",
    "compute_shannon_entropy",
    "compute_lexical_similarity",
    "SafeStateSerializer",
    "TimeMachine",
    "DiffEntry",
    "DiffSummary",
    "StateDiffReport",
    "compute_state_diff",
    "ForkRunner",
    "ForkResumptionContext",
    "ForkResumptionResult",
    "sync_state",
    "snapshot",
    "step_back",
    "fork_branch",
    "list_branches",
    "list_snapshots",
    "resume_from_fork",
    "resume_from_fork_sync",
    "init",
    "get_client",
    "agent",
    "span",
    "flush",
    "shutdown",
    "record_circuit_event",
    "Organization",
    "Project",
    "ApiKey",
    "TenantContext",
    "ApiKeyManager",
    "FleetCircuitCoordinator",
    "FleetMetrics",
    "FleetEvent",
    "PrometheusMetricsRegistry",
    "Counter",
    "Gauge",
    "Histogram",
    "record_span_ingested",
    "record_circuit_trip",
    "record_state_snapshot",
    "record_branch_fork",
    "set_active_swarms",
    "set_active_websockets",
    "set_telemetry_queue_depth",
    "SwarmWatchLlamaIndexHandler",
    "SwarmWatchAutoGenListener",
    "SwarmWatchDSPyTracker",
    "SwarmWatchSemanticKernelFilter",
    "FunctionInvocationContext",
    "AgentNode",
    "CausalEdge",
    "CausalTopology",
    "build_topology_from_spans",
    "SensitiveDataMasker",
    "SecuritySanitizationPolicy",
    "mask_payload",
    "mask_text",
    "IncidentReportGenerator",
    "IncidentPostMortem",
    "FinancialRoiMetrics",
    "RootCauseDiagnosis",
    "RuleConfig",
    "RuleCondition",
    "DeclarativeRule",
    "GuardrailPolicy",
    "PolicyEngine",
    "SafePolicyParser",
    "PolicyEvaluationResult",
    "SyntheticAlertReceiver",
    "AlertSimulationPipeline",
    "AlertSimulationReport",
    "RouteSimulationResult",
    "ReceivedAlertEvent",
    "RedisDistributedLock",
    "RedisLeaderElector",
    "ServerNodeRole",
    "ClusterNodeInfo",
    "ClusterLeaderStatus",
    "ObjectStorageClient",
    "MockObjectStorageClient",
    "S3ObjectStorageClient",
    "ColdStorageArchiver",
    "ArchiveManifest",
    "ArchiveRecord",
    "RestoredSwarmSummary",
    "Role",
    "Permission",
    "ROLE_PERMISSIONS",
    "has_permission",
    "UserAccount",
    "TotpEngine",
    "SseEvent",
    "SseSubscriber",
    "SseEventManager",
    "global_sse_manager",
    "VapidKeyManager",
    "PushKeys",
    "PushSubscription",
    "PushMessage",
    "format_circuit_push_message",
    "WebPushDispatcher",
    "global_webpush_dispatcher",
    "GrpcStatusCode",
    "GrpcCircuitBreakException",
    "GrpcMessageFramer",
    "SwarmProtobufCodec",
    "GrpcGatewayStats",
    "global_grpc_stats",
    "GrpcStreamingGateway",
    "global_grpc_gateway",
    "GrpcStreamServer",
    "DivergenceStatus",
    "SemanticDriftProfile",
    "MinHashSigner",
    "SemanticDriftDetector",
    "global_drift_detector",
    "AgentTier",
    "RebalancePolicy",
    "AgentAllocation",
    "RebalancePlan",
    "AdaptiveQuotaRebalancer",
    "global_rebalancer",
    "NodeCentrality",
    "AgentCoalition",
    "TopologicalClusterResult",
    "TopologicalCoalitionEngine",
    "global_coalition_engine",
    "ShmRingHeader",
    "ShmRingBuffer",
    "ShmRingBufferWriter",
    "ShmRingBufferReader",
    "ShmIpcGateway",
    "global_shm_gateway",
    "DifferentialPrivacyPolicy",
    "LaplaceMechanism",
    "DifferentialPrivacyEngine",
    "global_privacy_engine",
    "ForecastRiskLevel",
    "MartingaleStep",
    "SwarmForecastReport",
    "EntropyMartingaleForecaster",
    "global_forecaster",
    "ByzantineAttackType",
    "ByzantineFaultProfile",
    "ByzantineAttackEvent",
    "ByzantineResilienceReport",
    "ByzantineStressHarness",
    "global_byzantine_harness",
    "HoneypotStatus",
    "Honeytoken",
    "ShadowGraphSpan",
    "HoneypotReport",
    "DeceptionResponseGenerator",
    "HoneypotManager",
    "global_honeypot_manager",
    "VerificationInvariant",
    "VerificationVerdict",
    "AgentTransition",
    "AgentTransitionModel",
    "CounterexampleStep",
    "VerificationResult",
    "BoundedModelChecker",
    "global_model_checker",
]


from swarmwatch.redis_lock import (
    RedisDistributedLock,
    RedisLeaderElector,
    ServerNodeRole,
    ClusterNodeInfo,
    ClusterLeaderStatus,
)
from swarmwatch.archival import (
    ObjectStorageClient,
    MockObjectStorageClient,
    S3ObjectStorageClient,
    ColdStorageArchiver,
    ArchiveManifest,
    ArchiveRecord,
    RestoredSwarmSummary,
)
from swarmwatch.rbac import (
    Role,
    Permission,
    ROLE_PERMISSIONS,
    has_permission,
    UserAccount,
    TotpEngine,
)
from swarmwatch.otlp import (
    OtelTraceParser,
    ProtobufWireDecoder,
    otlp_span_to_swarm_span,
    extract_otlp_attributes,
    OtlpIngestionStats,
    global_otlp_stats,
)
from swarmwatch.sse import (
    SseEvent,
    SseSubscriber,
    SseEventManager,
    global_sse_manager,
)
from swarmwatch.webpush import (
    VapidKeyManager,
    PushKeys,
    PushSubscription,
    PushMessage,
    format_circuit_push_message,
    WebPushDispatcher,
    global_webpush_dispatcher,
)
from swarmwatch.ebpf import (
    SocketRule,
    SocketStreamDecoder,
    SocketIntervention,
    TcpRstEmitter,
    EbpfSocketGateway,
    MockEbpfEngine,
    global_ebpf_gateway,
    calculate_shannon_entropy,
)
from swarmwatch.raft import (
    RaftNodeRole,
    RaftLogEntry,
    RequestVoteRpc,
    RequestVoteResponse,
    AppendEntriesRpc,
    AppendEntriesResponse,
    SwarmRaftStateMachine,
    RaftConsensusEngine,
    MockRaftCluster,
    global_raft_engine,
)
from swarmwatch.selfhealing import (
    AnomalyType,
    SelfHealingActionType,
    SelfHealingAction,
    SelfHealingPolicyConfig,
    CounterfactualEvaluation,
    SwarmSelfHealingController,
    global_self_healing_controller,
)
from swarmwatch.causal_inference import (
    CausalPathNode,
    ContextPoisoningMetric,
    RootCauseAttribution,
    CausalInferenceResult,
    CausalInferenceEngine,
    global_causal_inference_engine,
)
from swarmwatch.grpc_gateway import (
    GrpcStatusCode,
    GrpcCircuitBreakException,
    GrpcMessageFramer,
    SwarmProtobufCodec,
    GrpcGatewayStats,
    global_grpc_stats,
    GrpcStreamingGateway,
    global_grpc_gateway,
    GrpcStreamServer,
)
from swarmwatch.drift import (
    DivergenceStatus,
    SemanticDriftProfile,
    MinHashSigner,
    SemanticDriftDetector,
    global_drift_detector,
)
from swarmwatch.rebalancer import (
    AgentTier,
    RebalancePolicy,
    AgentAllocation,
    RebalancePlan,
    AdaptiveQuotaRebalancer,
    global_rebalancer,
)
from swarmwatch.coalition import (
    NodeCentrality,
    AgentCoalition,
    TopologicalClusterResult,
    TopologicalCoalitionEngine,
    global_coalition_engine,
)
from swarmwatch.shm_ring import (
    ShmRingHeader,
    ShmRingBuffer,
    ShmRingBufferWriter,
    ShmRingBufferReader,
    ShmIpcGateway,
    global_shm_gateway,
)
from swarmwatch.diff_privacy import (
    DifferentialPrivacyPolicy,
    LaplaceMechanism,
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
    ByzantineAttackEvent,
    ByzantineResilienceReport,
    ByzantineStressHarness,
    global_byzantine_harness,
)
from swarmwatch.honeypot import (
    HoneypotStatus,
    Honeytoken,
    ShadowGraphSpan,
    HoneypotReport,
    DeceptionResponseGenerator,
    HoneypotManager,
    global_honeypot_manager,
)
from swarmwatch.model_checker import (
    VerificationInvariant,
    VerificationVerdict,
    AgentTransition,
    AgentTransitionModel,
    CounterexampleStep,
    VerificationResult,
    BoundedModelChecker,
    global_model_checker,
)












