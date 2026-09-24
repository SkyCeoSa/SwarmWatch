/**
 * SwarmWatch TypeScript SDK Main Entry Point.
 * Universal Observability, Zero-Token Shannon Entropy Circuit Breaker & Safe Time-Machine.
 */

// Core Schema & OpenTelemetry GenAI Semantic Conventions
export type {
  SwarmSpan,
  StateSnapshot,
  CounterfactualSavings,
  CircuitEvent,
} from "./schema.ts";
export { toOtelGenAiSpan } from "./schema.ts";

// Shannon Entropy & Lexical Similarity Engine
export {
  computeShannonEntropy,
  computeLexicalSimilarity,
} from "./entropy.ts";

// Entropy Circuit Breaker & Counterfactual Savings Engine
export type {
  CircuitPresetType,
  CircuitPolicyConfig,
  CircuitDecision,
} from "./circuit.ts";
export {
  CircuitPolicy,
  SwarmCircuitBreakException,
  EntropyCircuitBreaker,
} from "./circuit.ts";

// Safe State Serializer & Hash for Time-Machine
export type { SerializerConfig } from "./serializer.ts";
export { SafeStateSerializer } from "./serializer.ts";

// In-Process Telemetry Transport & Sinks
export type {
  StorageSink,
  TransportConfig,
  HttpStorageSinkOptions,
} from "./transport.ts";
export {
  InMemoryStorageSink,
  HttpStorageSink,
  TelemetryTransport,
} from "./transport.ts";

// Enterprise Security & Secret Redaction Engine
export type { SecuritySanitizationPolicy } from "./security.ts";
export {
  SensitiveDataMasker,
  getDefaultMasker,
} from "./security.ts";

// Multi-Tenant Core & Cryptographic API Keys
export type {
  Organization,
  Project,
  ApiKey,
  TenantContext,
} from "./tenant.ts";
export {
  ApiKeyManager,
  isApiKeyActive,
} from "./tenant.ts";

// Time-Machine & State Branching Coordinator (Seam 14 / ADR 0025 & Seam 16 / ADR 0027)
export type {
  ForkBranchResult,
  DiffEntry,
  DiffSummary,
  StateDiffReport,
} from "./timemachine.ts";
export { TimeMachine, computeStateDiff } from "./timemachine.ts";

// Live Fork Resumption Runner (Seam 15 / ADR 0026)
export type {
  ForkResumptionContext,
  ForkResumptionResult,
} from "./runner.ts";
export { ForkRunner } from "./runner.ts";

// Developer Surface & Client Singleton
export type {
  SpanOptions,
  ClientOptions,
} from "./client.ts";
export {
  SwarmClient,
  init,
  getClient,
  withSpan,
  agent,
  syncState,
  snapshot,
  stepBack,
  forkBranch,
  listBranches,
  listSnapshots,
  diffSnapshots,
  resumeFromFork,
  evaluateOutput,
  flush,
  shutdown,
} from "./client.ts";

// Framework Adapters
export type {
  VercelOnFinishEvent,
  VercelAdapterOptions,
} from "./adapters/vercel.ts";
export { swarmWatchVercelOnFinish } from "./adapters/vercel.ts";

export type { LangChainHandlerOptions } from "./adapters/langchain.ts";
export { SwarmWatchLangChainCallbackHandler } from "./adapters/langchain.ts";

// Incident Post-Mortem & Financial ROI Report Generator (Seam 11 / ADR 0020)
export type {
  FinancialRoiMetrics,
  RootCauseDiagnosis,
  EntropyTrajectoryPoint,
  SecurityRedactionSummary,
  IncidentPostMortem,
  ReportFormat,
  ReportStorageSource,
} from "./report.ts";
export { IncidentReportGenerator } from "./report.ts";

// Declarative Guardrail Policy Engine & Policy DSL (Seam 12 / ADR 0021 & ADR 0022)
export type {
  RuleConfig,
  RuleCondition,
  DeclarativeRule,
  GuardrailPolicy,
  PolicyEvaluationResult,
} from "./policy.ts";
export {
  SafePolicyParser,
  PolicyEngine,
  globMatch,
  mergeRuleConfigs,
} from "./policy.ts";

// Enterprise Webhook Alerting Dispatcher & PagerDuty (Seam 6 & 7 / ADR 0007 & ADR 0023)
export type {
  WebhookFormat,
  WebhookConfig,
  DeadLetterRecord,
  WebhookStats,
  WebhookHttpSender,
} from "./webhooks.ts";
export {
  WebhookNotifier,
  formatWebhookPayload,
  defaultHttpSender,
} from "./webhooks.ts";

// High-Frequency Ingestion Micro-Benchmark & Flamegraph Profile Suite (Seam 19 / ADR 0030)
export type {
  LatencyPercentiles,
  BenchmarkMemoryStats,
  IngestionBenchmarkConfig,
} from "./profiler.ts";
export {
  IngestionBenchmarkReport,
  TelemetryProfiler,
} from "./profiler.ts";

// Distributed Redis Circuit Breaker & Fleet State Synchronization (Seam 22 / ADR 0033)
export type {
  RedisTripBroadcast,
  FleetMetrics,
  FleetEvent,
  RedisClientInterface,
} from "./redis_circuit.ts";
export {
  MockRedisClient,
  RedisFleetBackend,
  RedisCircuitCoordinator,
} from "./redis_circuit.ts";

// Distributed Redis Lock & Leader Election (Seam 24 / ADR 0035)
export type {
  ServerNodeRole,
  ClusterNodeInfo,
  ClusterLeaderStatus,
} from "./redis_lock.ts";
export {
  RedisDistributedLock,
  RedisLeaderElector,
} from "./redis_lock.ts";

// S3 / Google Cloud Storage Streaming Log Archival & Cold Storage Tier (Seam 25 / ADR 0036)
export type {
  ObjectStorageClient,
  ArchiveManifest,
  ArchiveRecord,
  RestoredSwarmSummary,
} from "./archival.ts";
export {
  MockObjectStorageClient,
  ColdStorageArchiver,
} from "./archival.ts";

// Enterprise MFA / TOTP Authentication & Strict RBAC (Seam 26 / ADR 0037)
export type {
  UserAccount,
} from "./rbac.ts";
export {
  Role,
  Permission,
  ALL_PERMISSIONS,
  ROLE_PERMISSIONS,
  hasPermission,
  TotpEngine,
  base32Encode,
  base32Decode,
} from "./rbac.ts";

// OpenTelemetry OTLP Ingestion & GenAI Semantic Conventions (Seam 27 / ADR 0038)
export type {
  OtelAttributeValue,
  OtelKeyValue,
  OtelSpanEvent,
  OtelSpan,
  OtelScopeSpans,
  OtelResourceSpans,
  OtlpExportTraceRequest,
} from "./otlp.ts";
export {
  extractOtlpValue,
  extractOtlpAttributes,
  formatOtlpTracesPayload,
  parseOtlpTracesPayload,
  OtlpTraceExporter,
} from "./otlp.ts";

// Enterprise WebPush & Server-Sent Events (SSE) Real-Time Alerts (Seam 28 / ADR 0039)
export type {
  SseEvent,
  SseClientOptions,
  SseEventHandler,
} from "./sse.ts";
export {
  SseStreamParser,
  SwarmEventSource,
} from "./sse.ts";

export type {
  VapidKeys,
  PushSubscriptionKeys,
  PushSubscription,
  PushMessage,
} from "./webpush.ts";
export {
  VapidSigner,
} from "./webpush.ts";
export { listenEvents } from "./client.ts";

// Kernel-Level Network Socket LLM Interception Gateway (Seam 29 / ADR 0040)
export type {
  SocketRule,
  SocketIntervention,
} from "./ebpf.ts";
export {
  calculateShannonEntropy,
  SocketStreamDecoder,
  EbpfSocketGateway,
  MockEbpfEngine,
} from "./ebpf.ts";

// Multi-Cluster High-Availability Raft Consensus Engine (Seam 30 / ADR 0041)
export type {
  RaftLogEntry,
  RequestVoteRpc,
  RequestVoteResponse,
  AppendEntriesRpc,
  AppendEntriesResponse,
  SwarmRaftState,
  RaftRpcSender,
} from "./raft.ts";
export {
  RaftNodeRole,
  SwarmRaftStateMachine,
  RaftConsensusEngine,
  MockRaftCluster,
} from "./raft.ts";

// Autonomous Counterfactual Swarm Optimization & Self-Healing Policy Controller (Seam 31 / ADR 0042)
export type {
  AnomalyType,
  SelfHealingActionType,
  CounterfactualEvaluation,
  SelfHealingAction,
  SelfHealingPolicyConfig,
} from "./selfhealing.ts";
export {
  SwarmSelfHealingController,
} from "./selfhealing.ts";

// Distributed Multi-Agent Root-Cause Causal Inference Engine (Seam 32 / ADR 0043)
export type {
  CausalPathNode,
  ContextPoisoningMetric,
  RootCauseAttribution,
  CausalInferenceResult,
} from "./causal_inference.ts";
export {
  CausalInferenceEngine,
  globalCausalInferenceEngine,
} from "./causal_inference.ts";

// Ultra-High-Throughput gRPC Streaming Ingestion Gateway (Seam 33 / ADR 0044)
export type {
  GrpcStatusCode,
} from "./grpc.ts";
export {
  GrpcMessageFramer,
  GrpcStreamClient,
} from "./grpc.ts";

// Embedding-Free Vector Semantic Drift & Hallucination Divergence Detector (Seam 34 / ADR 0045)
export type {
  DivergenceStatus,
  SemanticDriftProfile,
  DriftAggregateStats,
} from "./drift.ts";
export {
  MinHashSigner,
  SemanticDriftDetector,
  globalDriftDetector,
} from "./drift.ts";

// Adaptive Dynamic Quota Re-balancing across Distributed Fleets (Seam 35 / ADR 0046)
export type {
  AgentTierType,
  RebalancePolicy,
  AgentAllocation,
  RebalancePlan,
} from "./rebalancer.ts";
export {
  AgentTier,
  DEFAULT_REBALANCE_POLICY,
  AdaptiveQuotaRebalancer,
} from "./rebalancer.ts";

// Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (Seam 36 / ADR 0047)
export type {
  NodeCentrality,
  AgentCoalition,
  TopologicalClusterResult,
} from "./coalition.ts";
export {
  TopologicalCoalitionEngine,
  globalCoalitionEngine,
} from "./coalition.ts";

// Zero-Copy Shared-Memory Ring Buffer IPC Gateway (Seam 37 / ADR 0048)
export type {
  ShmRingHeader,
  ShmRingStatus,
  ShmSlotStatusType,
} from "./shm_ring.ts";
export {
  SHM_MAGIC,
  SHM_HEADER_SIZE,
  SHM_SLOT_HEADER_SIZE,
  ShmSlotStatus,
  ShmRingBuffer,
  ShmRingBufferWriter,
  ShmRingBufferReader,
  ShmIpcGateway,
  globalShmGateway,
} from "./shm_ring.ts";

// Differential Privacy & Cryptographic State Anonymization (Seam 38 / ADR 0049)
export type {
  DifferentialPrivacyPolicy,
  DifferentialPrivacyStatus,
} from "./diff_privacy.ts";
export {
  LaplaceMechanism,
  DifferentialPrivacyEngine,
  globalPrivacyEngine,
} from "./diff_privacy.ts";

// Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales (Seam 39 / ADR 0050)
export type {
  ForecastRiskLevelType,
  MartingaleStep,
  SwarmForecastReport,
} from "./forecast.ts";
export {
  ForecastRiskLevel,
  EntropyMartingaleForecaster,
  globalForecaster,
} from "./forecast.ts";






