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

// Time-Machine & State Branching Coordinator (Seam 14 / ADR 0025)
export type { ForkBranchResult } from "./timemachine.ts";
export { TimeMachine } from "./timemachine.ts";

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


