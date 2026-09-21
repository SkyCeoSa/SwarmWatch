/**
 * SwarmWatch TypeScript Developer Surface & Singleton Client.
 * Uses AsyncLocalStorage for seamless async context propagation of multi-agent spans.
 */

import { AsyncLocalStorage } from "node:async_hooks";
import * as crypto from "node:crypto";
import { CircuitPolicy, EntropyCircuitBreaker } from "./circuit.ts";
import type { CircuitDecision } from "./circuit.ts";
import { computeShannonEntropy } from "./entropy.ts";
import type { CircuitEvent, StateSnapshot, SwarmSpan } from "./schema.ts";
import { SafeStateSerializer } from "./serializer.ts";
import { TimeMachine } from "./timemachine.ts";
import type { ForkBranchResult } from "./timemachine.ts";
import { InMemoryStorageSink, TelemetryTransport } from "./transport.ts";
import type { StorageSink, TransportConfig } from "./transport.ts";

const currentSpanStorage = new AsyncLocalStorage<SwarmSpan>();

export interface SpanOptions {
  name: string;
  source?: string;
  target?: string;
  span_type?: string;
  branch_id?: string;
  input_payload?: Record<string, any>;
}

export interface ClientOptions {
  swarm_id?: string;
  project_id?: string;
  api_key?: string;
  sink?: StorageSink;
  transport_config?: TransportConfig;
  circuit_policy?: CircuitPolicy;
}

export class SwarmClient {
  public swarmId: string;
  public projectId: string;
  public apiKey?: string;
  public transport: TelemetryTransport;
  public circuitBreaker: EntropyCircuitBreaker;
  public serializer: SafeStateSerializer;
  public timeMachine: TimeMachine;

  constructor(options: ClientOptions = {}) {
    this.swarmId = options.swarm_id || "default-swarm";
    this.projectId = options.project_id || "default-project";
    this.apiKey = options.api_key;
    this.transport = new TelemetryTransport(options.sink, options.transport_config);
    this.transport.start();
    this.circuitBreaker = new EntropyCircuitBreaker({ policy: options.circuit_policy });
    this.serializer = new SafeStateSerializer();
    this.timeMachine = new TimeMachine(this.transport.sink, this.serializer);
  }

  public getCurrentSpan(): SwarmSpan | undefined {
    return currentSpanStorage.getStore();
  }

  public async withSpan<T>(
    options: SpanOptions,
    fn: (span: SwarmSpan) => Promise<T> | T
  ): Promise<T> {
    const parentSpan = this.getCurrentSpan();
    const spanId = crypto.randomUUID();
    const startTime = new Date().toISOString();
    const perfStart = performance.now();

    const span: SwarmSpan = {
      id: spanId,
      swarm_id: this.swarmId,
      project_id: this.projectId,
      branch_id: options.branch_id || (parentSpan ? parentSpan.branch_id : "main"),
      parent_span_id: parentSpan ? parentSpan.id : null,
      source_agent: options.source || options.name || "system",
      target_agent: options.target || "system",
      span_type: options.span_type || "span",
      input_payload: options.input_payload || {},
      output_payload: {},
      token_count: 0,
      entropy_score: 1.0,
      start_time: startTime,
      end_time: startTime,
      duration_ms: 0.0,
    };

    return currentSpanStorage.run(span, async () => {
      try {
        const result = await fn(span);
        const perfEnd = performance.now();
        span.end_time = new Date().toISOString();
        span.duration_ms = Math.max(0.001, perfEnd - perfStart);

        if (result && typeof result === "object") {
          span.output_payload = { ...span.output_payload, ...result };
        } else if (result !== undefined) {
          span.output_payload = { ...span.output_payload, result };
        }

        // If result contains string, compute entropy
        const outStr = typeof result === "string" ? result : JSON.stringify(result || "");
        span.entropy_score = computeShannonEntropy(outStr);

        this.transport.emitSpan(span);
        return result;
      } catch (err: any) {
        const perfEnd = performance.now();
        span.end_time = new Date().toISOString();
        span.duration_ms = Math.max(0.001, perfEnd - perfStart);
        span.output_payload = {
          ...span.output_payload,
          error: err?.name || "Error",
          message: err?.message || String(err),
          stack: err?.stack?.slice(0, 1000),
        };
        this.transport.emitSpan(span);
        throw err;
      }
    });
  }

  public agent<TArgs extends any[], TReturn>(
    name: string,
    fn: (...args: TArgs) => Promise<TReturn> | TReturn,
    options?: Partial<SpanOptions>
  ): (...args: TArgs) => Promise<TReturn> {
    return async (...args: TArgs): Promise<TReturn> => {
      const inputPayload: Record<string, any> = { args };
      return this.withSpan(
        {
          name: `agent:${name}`,
          source: name,
          target: options?.target || "environment",
          span_type: "agent_call",
          branch_id: options?.branch_id,
          input_payload: inputPayload,
        },
        async () => {
          return await fn(...args);
        }
      );
    };
  }

  public evaluateOutput(
    message: string,
    agentId?: string,
    tokenCount?: number | null
  ): CircuitDecision {
    const agent = agentId || (this.getCurrentSpan()?.source_agent ?? "agent");
    const decision = this.circuitBreaker.evaluate(message, agent, this.swarmId, tokenCount);

    if (decision.decision === "warning" || decision.decision === "trip") {
      const event: CircuitEvent = {
        id: crypto.randomUUID(),
        swarm_id: this.swarmId,
        span_id: this.getCurrentSpan()?.id || null,
        event_type: decision.decision,
        reason: decision.reason || "Entropy circuit breaker intervention",
        metric_val: decision.metric_val,
        counterfactual_savings: decision.counterfactual_savings,
        created_at: new Date().toISOString(),
      };
      if (this.transport.sink.recordCircuitEvent) {
        this.transport.sink.recordCircuitEvent(event);
      }
    }

    return decision;
  }

  public syncState(stateData: Record<string, any>, branchId?: string): StateSnapshot {
    const currentSpan = this.getCurrentSpan();
    const spanId = currentSpan?.id || crypto.randomUUID();
    const branch = branchId || currentSpan?.branch_id || "main";

    const snap = this.serializer.createSnapshot(spanId, stateData, branch);
    if (this.transport.sink.saveSnapshot) {
      this.transport.sink.saveSnapshot(snap);
    }
    return snap;
  }

  public async snapshot(stateData: Record<string, any>, branchId?: string): Promise<StateSnapshot> {
    const currentSpan = this.getCurrentSpan();
    const spanId = currentSpan?.id || crypto.randomUUID();
    const branch = branchId || currentSpan?.branch_id || "main";
    return await this.timeMachine.snapshot(spanId, stateData, branch);
  }

  public async stepBack(targetSpanId: string, newBranchId?: string): Promise<Record<string, any>> {
    return await this.timeMachine.stepBack(targetSpanId, newBranchId);
  }

  public async forkBranch(targetSpanId: string, newBranchId: string): Promise<ForkBranchResult> {
    return await this.timeMachine.forkBranch(this.swarmId, targetSpanId, newBranchId);
  }

  public async listBranches(): Promise<string[]> {
    return await this.timeMachine.listBranches(this.swarmId);
  }

  public async listSnapshots(branchId?: string): Promise<StateSnapshot[]> {
    return await this.timeMachine.getSnapshots(this.swarmId, branchId);
  }

  public async flush(): Promise<void> {
    await this.transport.flush();
  }

  public async shutdown(): Promise<void> {
    await this.transport.shutdown();
  }
}

// Global Singleton
let globalClient: SwarmClient | null = null;

export function init(options?: ClientOptions): SwarmClient {
  if (globalClient) {
    globalClient.shutdown().catch(() => {});
  }
  globalClient = new SwarmClient(options);
  return globalClient;
}

export function getClient(): SwarmClient {
  if (!globalClient) {
    globalClient = new SwarmClient();
  }
  return globalClient;
}

export function withSpan<T>(
  options: SpanOptions,
  fn: (span: SwarmSpan) => Promise<T> | T
): Promise<T> {
  return getClient().withSpan(options, fn);
}

export function agent<TArgs extends any[], TReturn>(
  name: string,
  fn: (...args: TArgs) => Promise<TReturn> | TReturn,
  options?: Partial<SpanOptions>
): (...args: TArgs) => Promise<TReturn> {
  return getClient().agent(name, fn, options);
}

export function syncState(stateData: Record<string, any>, branchId?: string): StateSnapshot {
  return getClient().syncState(stateData, branchId);
}

export function snapshot(stateData: Record<string, any>, branchId?: string): Promise<StateSnapshot> {
  return getClient().snapshot(stateData, branchId);
}

export function stepBack(targetSpanId: string, newBranchId?: string): Promise<Record<string, any>> {
  return getClient().stepBack(targetSpanId, newBranchId);
}

export function forkBranch(targetSpanId: string, newBranchId: string): Promise<ForkBranchResult> {
  return getClient().forkBranch(targetSpanId, newBranchId);
}

export function listBranches(): Promise<string[]> {
  return getClient().listBranches();
}

export function listSnapshots(branchId?: string): Promise<StateSnapshot[]> {
  return getClient().listSnapshots(branchId);
}

export function evaluateOutput(
  message: string,
  agentId?: string,
  tokenCount?: number | null
): CircuitDecision {
  return getClient().evaluateOutput(message, agentId, tokenCount);
}

export function flush(): Promise<void> {
  return getClient().flush();
}

export function shutdown(): Promise<void> {
  if (globalClient) {
    const client = globalClient;
    globalClient = null;
    return client.shutdown();
  }
  return Promise.resolve();
}
