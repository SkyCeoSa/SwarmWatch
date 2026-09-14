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
} from "./transport.ts";
export {
  InMemoryStorageSink,
  HttpStorageSink,
  TelemetryTransport,
} from "./transport.ts";

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
