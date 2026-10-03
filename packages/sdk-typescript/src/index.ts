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

// Multi-Agent Byzantine Attack & Prompt Injection Stress Harness (Seam 40 / ADR 0051)
export type {
  ByzantineAttackTypeValue,
  ByzantineFaultProfile,
  ByzantineAttackEvent,
  ByzantineResilienceReport,
} from "./byzantine.ts";
export {
  ByzantineAttackType,
  PROMPT_INJECTION_PATTERNS,
  STATE_POISONING_PATTERNS,
  ByzantineStressHarness,
  globalByzantineHarness,
} from "./byzantine.ts";

// Multi-Agent Dynamic Honeypot & Deception Sandboxing (Seam 41 / ADR 0052)
export type {
  HoneypotStatusType,
  Honeytoken,
  ShadowGraphSpan,
  HoneypotReport,
} from "./honeypot.ts";
export {
  HoneypotStatus,
  DeceptionResponseGenerator,
  HoneypotManager,
  globalHoneypotManager,
} from "./honeypot.ts";

// Bounded Model Checking & Multi-Agent Invariant Verification (Seam 42 / ADR 0053)
export type {
  VerificationInvariantType,
  VerificationVerdictType,
  AgentTransition,
  AgentTransitionModel,
  CounterexampleStep,
  VerificationResult,
} from "./model_checker.ts";
export {
  VerificationInvariant,
  VerificationVerdict,
  BoundedModelChecker,
  globalModelChecker,
} from "./model_checker.ts";

// Autonomous Dynamic Topology Healing & Auto-Patching (Seam 43 / ADR 0054)
export type {
  PatchActionType,
  TopologyPatch,
  HealedModelReport,
} from "./topology_healer.ts";
export {
  PatchAction,
  TopologyAutoHealer,
  globalTopologyHealer,
} from "./topology_healer.ts";

// Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking (Seam 44 / ADR 0055)
export type {
  MediaModalityType,
  MediaMetadata,
  ArtifactWatermark,
  WatermarkedArtifact,
} from "./multimodal.ts";
export {
  MediaModality,
  MultiModalInspector,
  globalMultiModalInspector,
} from "./multimodal.ts";

// Decentralized Epidemic Gossip Telemetry Mesh Sync (Seam 45 / ADR 0056)
export type {
  MeshNodeStatusType,
  MeshConvergenceHealthType,
  MeshNode,
  GossipDigest,
  GossipDelta,
  MeshStatusReport,
} from "./gossip.ts";
export {
  MeshNodeStatus,
  MeshConvergenceHealth,
  VectorClock,
  GossipMeshManager,
  globalGossipManager,
} from "./gossip.ts";

// Autonomous Dynamic Workload Sharding & Compute-Aware Load Balancer (Seam 46 / ADR 0057)
export type {
  NodeHealthTierType,
  NodeComputeMetrics,
  VirtualNode,
  ShardRoutingKey,
  ShardRouteResult,
  ShardingStatusReport,
} from "./sharding.ts";
export {
  NodeHealthTier,
  computeCapacityScore,
  getHealthTier,
  hashToken32,
  formatShardRoutingKey,
  parseShardRoutingKey,
  ConsistentHashRing,
  ComputeAwareBalancer,
  globalComputeBalancer,
} from "./sharding.ts";

// Autonomous Semantic Schema Migration & State Translation Engine (Seam 47 / ADR 0058)
export type {
  FieldTransformActionType,
  MigrationPlan,
  MigrationResult,
} from "./schema_migration.ts";
export {
  FieldTransformAction,
  SchemaVersion,
  FieldTransformRule,
  MigrationStep,
  SchemaMigrationEngine,
  globalSchemaMigrationEngine,
} from "./schema_migration.ts";

// Dynamic Multi-Agent Epistemic Logic & Knowledge Base State Synchronization (Seam 48 / ADR 0059)
export type {
  EpistemicTruthValueType,
  EpistemicStatusType,
  PropositionalClaimOptions,
} from "./epistemic.ts";
export {
  EpistemicTruthValue,
  EpistemicStatus,
  PropositionalClaim,
  EpistemicConflict,
  EpistemicKnowledgeBase,
  EpistemicEngine,
  globalEpistemicEngine,
} from "./epistemic.ts";

// Zero-Knowledge Causal Proof Generation & Cryptographic Audit Engine (Seam 49 / ADR 0060)
export type {
  ProofCommitmentTypeEnum,
  SpanCommitmentLeafOptions,
  MerkleAuditProofOptions,
  CausalAuditReceiptOptions,
  CommitSpanOptions,
} from "./zk_proof.ts";
export {
  ProofCommitmentType,
  SpanCommitmentLeaf,
  MerkleAuditProof,
  CausalAuditReceipt,
  CausalProofAccumulator,
  ZkProofEngine,
  globalZkProofEngine,
  sha256Digest,
} from "./zk_proof.ts";

// Canonical Causal Encoding & Byte-Deterministic Contract Engine (Seam 50 / ADR 0061)
export type {
  CanonicalDomainTagEnum,
} from "./canonical.ts";
export {
  CanonicalDomainTag,
  CanonicalEncodingError,
  CanonicalCodec,
  globalCanonicalCodec,
} from "./canonical.ts";

// Unified Safety Authority & Linearizable Execution Permit Engine (Seam 51 / ADR 0062)
export type {
  PermitStatusEnum,
  AuthorityStateEnum,
  SafetyPermitOptions,
} from "./safety_authority.ts";
export {
  PermitStatus,
  AuthorityState,
  SafetyPermit,
  TenantQuota,
  SafetyAuthority,
  globalSafetyAuthority,
} from "./safety_authority.ts";

// Fenced Effect Ledger & Monotonic Idempotency Barrier (Seam 52 / ADR 0063)
export type {
  ClaimStatusEnum,
  EffectReceiptOptions,
  EffectRecordOptions,
} from "./effect_ledger.ts";
export {
  ClaimStatus,
  EffectReceipt,
  EffectRecord,
  ClaimResult,
  EffectLedger,
  globalEffectLedger,
} from "./effect_ledger.ts";

// Loss-Aware Causal Ingestion & Evidentiary Completeness Rule (Seam 53 / ADR 0064)
export type {
  IngressPriorityEnum,
  CompletenessStatusEnum,
  SequenceGapOptions,
  IngressResultOptions,
  CompletenessReportOptions,
} from "./causal_ingress.ts";
export {
  IngressPriority,
  CompletenessStatus,
  SequenceGap,
  IngressResult,
  CompletenessReport,
  CausalIngress,
  globalCausalIngress,
} from "./causal_ingress.ts";

// Durable Audit Anchor & Merkle Audit Receipt Engine (Seam 54 / ADR 0065)
export type {
  RootEpochOptions,
  MerkleAuditReceiptOptions,
  VerificationResultOptions,
} from "./audit_anchor.ts";
export {
  AuditIntegrityError,
  RootEpoch,
  MerkleAuditReceipt,
  VerificationResult,
  AuditAnchor,
  globalAuditAnchor,
} from "./audit_anchor.ts";

// Bounded Verification Semantics & Explicit Counterexample Engine (Seam 55 / ADR 0066)
export type {
  BoundedVerdictType,
  VerificationProperty,
  VerificationLimits,
  TransitionStep,
  VerificationReport,
  VerifierStatus,
} from "./safety_verifier.ts";
export {
  BoundedVerdict,
  SafetyVerifier,
  globalSafetyVerifier,
} from "./safety_verifier.ts";

// Governed Remediation & Fenced Action State Machine (Seam 56 / ADR 0067)
export type {
  ApprovalTierEnum,
  ProposalStatusEnum,
  BlastRadius,
  RemediationProposal,
  RemediationDecision,
  RemediationResult,
  CompensationResult,
  ProposeOptions,
  AuthorizeOptions,
  ApplyOptions,
  CompensateOptions,
} from "./remediation_governor.ts";
export {
  ApprovalTier,
  ProposalStatus,
  RemediationGovernor,
  globalRemediationGovernor,
} from "./remediation_governor.ts";

// Partition Reconciliation & Conflict Lattice Engine (Seam 57 / ADR 0068)
export type {
  LatticeVerdictEnum,
  ReconciliationPlan,
  MergeResult,
  AuthorityReconciliationResult,
} from "./mesh_reconciler.ts";
export {
  LatticeVerdict,
  compareVectorClocks,
  mergeVectorClocks,
  MeshReconciler,
  globalMeshReconciler,
} from "./mesh_reconciler.ts";

// Privacy Release Ledger & Differential Privacy Accountant (Seam 58 / ADR 0069)
export type {
  ReservationStatusEnum,
  PrivacyReservation,
  PrivacyReleaseResult,
} from "./privacy_ledger.ts";
export {
  PrivacyBudgetExhaustedError,
  ReservationStatus,
  sampleLaplace,
  clipValue,
  CompositionAccountant,
  PrivacyRelease,
  globalPrivacyRelease,
} from "./privacy_ledger.ts";

// Migration Preservation & Partial Isomorphisms (Seam 59 / ADR 0070)
export type {
  LossClassificationEnum,
  EvolutionActionEnum,
  PreservedDelta,
  EvolutionRule,
  EvolutionStep,
  EvolutionPlan,
  MigrationResult,
} from "./schema_evolution.ts";
export {
  LossyMigrationError,
  UnsupportedReverseMigrationError,
  SchemaEvolutionError,
  LossClassification,
  EvolutionAction,
  computeDeltaHash,
  computeMigrationHash,
  SchemaEvolution,
  globalSchemaEvolution,
} from "./schema_evolution.ts";

// Enterprise Acceptance Harness & Multi-Dimensional Release Gate (Seam 60 / ADR 0071)
export type {
  AcceptanceGateNameEnum,
  AcceptanceStatusEnum,
  AcceptanceProfileEnum,
  GateCheckResult,
  GateSummary,
  AcceptanceReportOptions,
  AcceptanceAttestationOptions,
} from "./acceptance.ts";
export {
  AcceptanceGateName,
  AcceptanceStatus,
  AcceptanceProfile,
  AcceptanceError,
  AttestationVerificationError,
  AcceptanceReport,
  AcceptanceAttestation,
  AcceptanceHarness,
  globalAcceptanceHarness,
} from "./acceptance.ts";

// Consensus Authority Binding & Linearizable Quorum Fencing (Seam 61 / ADR 0072)
export type {
  ConsensusPermitStatusEnum,
  ConsensusLogEntryOptions,
  ConsensusPermitResultOptions,
  CommittedTenantStateOptions,
} from "./consensus_authority.ts";
export {
  ConsensusPermitStatus,
  ConsensusLogEntry,
  ConsensusPermitResult,
  CommittedTenantState,
  ConsensusTripResult,
  ConsensusRearmResult,
  ConsensusAuthorityBinding,
  globalConsensusAuthority,
} from "./consensus_authority.ts";

// Downstream Effect Protocol & 2PC Outcome-Unknown Reconciliation (Seam 62 / ADR 0073)
export type {
  EffectPhaseType,
  DownstreamIdempotencyContractType,
  PrepareOptions,
} from "./downstream_effect.ts";
export {
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
  globalDownstreamEffectBoundary,
} from "./downstream_effect.ts";

// Critical Evidence Spool & NIST SP 800-53 Write-Ahead Journal (Seam 63 / ADR 0074)
export type {
  EvidenceRecordTypeEnum,
  JournalReceipt,
  FlushResult,
  RecoveryResult,
  CompletenessReceipt,
  EvidenceSpoolStatus,
  EvidenceRecordData,
} from "./evidence_spool.ts";
export {
  EvidenceRecordType,
  EvidenceSpoolFullException,
  EvidenceCorruptionException,
  SequenceGapException,
  computeCrc32,
  EvidenceRecord,
  EvidenceSpool,
  globalEvidenceSpool,
} from "./evidence_spool.ts";

// Independent Root Witness & Public-Key Chains (Seam 64 / ADR 0075)
export {
  WitnessError,
  RollbackAttackError,
  ConsistencyVerificationError,
  SplitBrainForkError,
  KeyRotationError,
  base64UrlEncode,
  base64UrlDecode,
  WitnessKeyManager,
  leafHash,
  nodeHash,
  mth,
  consistencyProof,
  verifyConsistencyProof,
  LeafReceipt,
  SignedCheckpoint,
  ConsistencyProof,
  KeyRotationRecord,
  WitnessStatus,
  RootWitness,
  globalRootWitness,
} from "./root_witness.ts";

// Tenant Identity Boundary & Attenuated Capability Tokens (Seam 65 / ADR 0076)
export type {
  CapabilityTokenData,
  CapabilityVerdictData,
  RevocationReceiptData,
  CapabilityStatusData,
  IssueRootTokenOptions,
  AttenuateOptions,
  VerifyExecutionOptions,
} from "./tenant_capability.ts";
export {
  CapabilityError,
  PrivilegeEscalationError,
  AttenuationViolationError,
  CapabilityVerificationError,
  RevocationError,
  resourcePatternMatches,
  isSubpattern,
  currentEpochNs,
  CapabilityToken,
  CapabilityVerdict,
  RevocationReceipt,
  CapabilityStatus,
  TenantCapabilityManager,
  globalCapabilityManager,
} from "./tenant_capability.ts";

// Effect-Aware Compensation Algebra & Monotone Approval Lattices (Seam 66 / ADR 0077)
export type {
  ActionExecutionData,
  ActionContractOptions,
  CompositeClassificationData,
  CompensationStepData,
  CompensationPlanData,
  CompensationResultData,
  HumanEscalationReceiptData,
  EscalationResolutionData,
  CompensationStatusData,
} from "./compensation_algebra.ts";
export {
  CompensationError,
  IrreversibleActionError,
  UnregisteredActionError,
  EscalationPendingError,
  ReversibilityClass,
  parseReversibilityClass,
  reversibilityClassName,
  reversibilityJoin,
  sha256Hex,
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
  globalCompensationEngine,
} from "./compensation_algebra.ts";

// Durable Dataset-Level Privacy Accountant & Quorum (ε,δ) Ledger (Seam 67 / ADR 0078)
export type {
  DatasetPrivacyConfigData,
  DatasetPrivacyReservationData,
  DatasetPrivacyReleaseResultData,
  DatasetLedgerSnapshotData,
} from "./dataset_privacy.ts";
export {
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
  globalDatasetPrivacyLedger,
} from "./dataset_privacy.ts";

// Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (Seam 68 / ADR 0079)
export type {
  ProtocolDeviationData,
  ExerciseResultData,
  ConformanceAttestationReceiptData,
} from "./interop_lab.ts";
export {
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
  globalInteropLab,
} from "./interop_lab.ts";

// Verifiable Release Provenance & Signed Artifact DAG (Seam 69 / ADR 0080)
export type {
  ArtifactDigestData,
  ProvenanceStepData,
  SignedProvenanceManifestData,
  ProvenanceVerificationResultData,
} from "./release_provenance.ts";
export {
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
  globalReleaseProvenance,
} from "./release_provenance.ts";

// Datacenter Failure Exercise & Region Loss Fault Injection (Seam 70 / ADR 0081)
export type {
  ClusterNodeData,
  PermitAttemptResultData,
  FailureExerciseEventData,
  FailureExerciseReportData,
} from "./datacenter_exercise.ts";
export {
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
  globalDatacenterOrchestrator,
} from "./datacenter_exercise.ts";

// Autonomous Action Gateway & Governed Downstream Agent Execution (Seam 71 / ADR 0082)
export type {
  ActionRegistrationData,
  ActionApprovalData,
  ActionRequestData,
  ActionExecutionReceiptData,
  ActionHandler,
  CapabilityValidator,
} from "./action_gateway.ts";
export {
  ActionGatewayError,
  UnauthorizedActionError,
  ActionQuorumPendingError,
  ActionCircuitBlockedError,
  ActionExecutionFailedError,
  ActionRiskLevel,
  ActionApprovalStatus,
  ActionRegistration,
  ActionApproval,
  ActionRequest,
  ActionExecutionReceipt,
  ActionGateway,
  globalActionGateway,
} from "./action_gateway.ts";

// Dynamic Agent Federation & Swarm Mesh Topology Routing (Seam 72 / ADR 0083)
export type {
  PeerNodeData,
  MeshLinkData,
  PeerHandshakeRequestData,
  PeerHandshakeResponseData,
  MeshPacketData,
  MeshRoutingReceiptData,
} from "./mesh_federation.ts";
export {
  MeshFederationError,
  MeshPeerAuthenticationError,
  MeshRoutingLoopError,
  MeshHopLimitExceededError,
  MeshDestinationUnreachableError,
  MeshCapabilityFencingError,
  MeshCircuitTrippedError,
  MeshNodeStatus as FederationNodeStatus,
  MeshNodeRole,
  MeshRoutingStatus,
  PeerNode,
  MeshLink,
  MeshPacket,
  MeshRoutingReceipt,
  MeshFederationRouter,
  globalMeshRouter,
  generatePeerKeypair,
  signPeerData,
  verifyPeerData,
} from "./mesh_federation.ts";
