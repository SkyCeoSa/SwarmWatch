/**
 * SwarmWatch Live Fork Resumption Runner (TypeScript Edition - Seam 15 / ADR 0026).
 * Enables programmatic resumption of autonomous agents from a forked state snapshot,
 * establishing scoped branch context, causal span linkage, and zero-token deterministic execution.
 */

import * as crypto from "node:crypto";
import type { SwarmClient, SpanOptions } from "./client.ts";
import { currentSpanStorage } from "./client.ts";
import type { StateSnapshot, SwarmSpan } from "./schema.ts";
import { SafeStateSerializer } from "./serializer.ts";

export interface ForkResumptionContext {
  branch_id: string;
  swarm_id: string;
  source_span_id: string;
  resumption_span_id: string;
  state: Record<string, any>;
  state_overrides: Record<string, any>;
  client?: SwarmClient;
  span<T>(options: SpanOptions, fn: (span: SwarmSpan) => Promise<T> | T): Promise<T>;
  syncState(stateData: Record<string, any>): StateSnapshot;
  [key: string]: any;
}

export interface ForkResumptionResult<T = any> {
  status: "completed" | "error";
  branch_id: string;
  swarm_id: string;
  resumption_span_id: string;
  output: T | null;
  final_state: Record<string, any>;
  duration_ms: number;
  error?: string;
}

export class ForkRunner {
  public client: SwarmClient;
  public serializer: SafeStateSerializer;

  constructor(client: SwarmClient) {
    this.client = client;
    this.serializer = new SafeStateSerializer();
  }

  public async resume<T = any>(
    branchId: string,
    handler: (ctx: ForkResumptionContext) => Promise<T> | T,
    stateOverrides?: Record<string, any>
  ): Promise<ForkResumptionResult<T>> {
    const perfStart = performance.now();
    const startTime = new Date().toISOString();

    // 1. Retrieve branch snapshots
    const snapshots = await this.client.listSnapshots(branchId);
    let baseState: Record<string, any> = {};
    let sourceSpanId = `fork_root_${branchId}`;

    if (snapshots && snapshots.length > 0) {
      const baseSnapshot = snapshots[snapshots.length - 1];
      sourceSpanId = baseSnapshot.span_id;
      baseState = { ...baseSnapshot.state_data };
    } else {
      // Check in-memory sink spans if available
      const sink = (this.client.transport as any).sink;
      const spans: SwarmSpan[] = sink?.spans || [];
      const branchSpans = spans.filter((s) => s.branch_id === branchId);
      if (branchSpans.length === 0) {
        throw new Error(
          `Branch '${branchId}' does not exist or has no recorded state snapshots in swarm '${this.client.swarmId}'`
        );
      }
      sourceSpanId = branchSpans[branchSpans.length - 1].id;
    }

    // 2. Merge and sanitize state overrides
    const mergedState: Record<string, any> = { ...baseState };
    let sanitizedOverrides: Record<string, any> = {};
    if (stateOverrides) {
      sanitizedOverrides = this.serializer.sanitize(stateOverrides);
      Object.assign(mergedState, sanitizedOverrides);
    }

    const resumptionSpanId = crypto.randomUUID();

    // 3. Construct boundary resumption span
    const resumptionSpan: SwarmSpan = {
      id: resumptionSpanId,
      swarm_id: this.client.swarmId,
      project_id: this.client.projectId,
      branch_id: branchId,
      parent_span_id: sourceSpanId,
      source_agent: "fork_runner",
      target_agent: "resumed_agent",
      span_type: "fork_resumption",
      input_payload: {
        branch_id: branchId,
        source_span_id: sourceSpanId,
        state_overrides: sanitizedOverrides,
        initial_state: mergedState,
      },
      output_payload: {},
      token_count: 0,
      entropy_score: 1.0,
      start_time: startTime,
      end_time: startTime,
      duration_ms: 0,
    };

    const ctx: ForkResumptionContext = {
      branch_id: branchId,
      swarm_id: this.client.swarmId,
      source_span_id: sourceSpanId,
      resumption_span_id: resumptionSpanId,
      state: mergedState,
      state_overrides: sanitizedOverrides,
      client: this.client,
      span: async <U>(options: SpanOptions, fn: (span: SwarmSpan) => Promise<U> | U) => {
        return await this.client.withSpan(
          { ...options, branch_id: options.branch_id || branchId },
          fn
        );
      },
      syncState: (stateData: Record<string, any>) => {
        Object.assign(mergedState, stateData);
        return this.client.syncState(mergedState, branchId);
      },
    };

    // Proxy support for property access like ctx.my_field
    const proxyCtx = new Proxy(ctx, {
      get(target, prop, receiver) {
        if (typeof prop === "string" && prop in target.state) {
          return target.state[prop];
        }
        return Reflect.get(target, prop, receiver);
      },
      set(target, prop, value, receiver) {
        if (typeof prop === "string" && prop in target.state) {
          target.state[prop] = value;
          return true;
        }
        return Reflect.set(target, prop, value, receiver);
      },
    });

    let output: T | null = null;
    let errorMsg: string | undefined;
    let status: "completed" | "error" = "completed";

    await currentSpanStorage.run(resumptionSpan, async () => {
      try {
        output = await handler(proxyCtx);
        if (output && typeof output === "object") {
          resumptionSpan.output_payload = { ...resumptionSpan.output_payload, ...output };
        } else if (output !== undefined && output !== null) {
          resumptionSpan.output_payload = { ...resumptionSpan.output_payload, result: String(output) };
        }
        resumptionSpan.output_payload.status = "completed";

        // Auto-persist post-resumption snapshot
        this.client.syncState(proxyCtx.state, branchId);
      } catch (err: any) {
        status = "error";
        errorMsg = err?.message || String(err);
        resumptionSpan.output_payload.status = "error";
        resumptionSpan.output_payload.error = err?.name || "Error";
        resumptionSpan.output_payload.message = errorMsg;
      } finally {
        const perfEnd = performance.now();
        resumptionSpan.end_time = new Date().toISOString();
        resumptionSpan.duration_ms = Math.max(0.001, perfEnd - perfStart);

        this.client.transport.emitSpan(resumptionSpan);
        await this.client.flush();
      }
    });

    const perfEnd = performance.now();
    return {
      status,
      branch_id: branchId,
      swarm_id: this.client.swarmId,
      resumption_span_id: resumptionSpanId,
      output,
      final_state: proxyCtx.state,
      duration_ms: Math.max(0.001, perfEnd - perfStart),
      error: errorMsg,
    };
  }
}
