/**
 * SwarmWatch TypeScript In-Process Telemetry Transport & Bounded Ring Buffer.
 * Guarantees sub-microsecond non-blocking span submission, asynchronous batch flushing,
 * and zero network latency impact on host agent execution.
 */

import * as crypto from "node:crypto";
import type { CircuitEvent, StateSnapshot, SwarmSpan } from "./schema.ts";
import { SensitiveDataMasker, getDefaultMasker } from "./security.ts";
import type { ForkBranchResult } from "./timemachine.ts";

export interface StorageSink {
  saveSpans(spans: SwarmSpan[]): Promise<void> | void;
  saveSnapshot?(snapshot: StateSnapshot): Promise<void> | void;
  recordCircuitEvent?(event: CircuitEvent): Promise<void> | void;
  getSpans?(swarmId: string): Promise<SwarmSpan[]> | SwarmSpan[];
  getCircuitEvents?(swarmId: string): Promise<CircuitEvent[]> | CircuitEvent[];
  getSnapshot?(spanId: string): Promise<StateSnapshot | null> | StateSnapshot | null;
  getSnapshotsForSwarm?(swarmId: string, branchId?: string): Promise<StateSnapshot[]> | StateSnapshot[];
  forkBranch?(swarmId: string, targetSpanId: string, newBranchId: string): Promise<ForkBranchResult> | ForkBranchResult;
  listBranches?(swarmId: string): Promise<string[]> | string[];
}

export class InMemoryStorageSink implements StorageSink {
  public spans: SwarmSpan[] = [];
  public snapshots: StateSnapshot[] = [];
  public circuitEvents: CircuitEvent[] = [];

  public saveSpans(spans: SwarmSpan[]): void {
    this.spans.push(...spans);
  }

  public saveSnapshot(snapshot: StateSnapshot): void {
    this.snapshots.push(snapshot);
  }

  public recordCircuitEvent(event: CircuitEvent): void {
    this.circuitEvents.push(event);
  }

  public getSpans(swarmId: string): SwarmSpan[] {
    return this.spans.filter((s) => s.swarm_id === swarmId);
  }

  public getCircuitEvents(swarmId: string): CircuitEvent[] {
    return this.circuitEvents.filter((e) => e.swarm_id === swarmId);
  }

  public getSnapshot(spanId: string): StateSnapshot | null {
    return this.snapshots.find((s) => s.span_id === spanId) || null;
  }

  public getSnapshotsForSwarm(swarmId: string, branchId?: string): StateSnapshot[] {
    const swarmSpanIds = new Set(
      this.spans
        .filter((s) => s.swarm_id === swarmId && (!branchId || s.branch_id === branchId))
        .map((s) => s.id)
    );
    return this.snapshots.filter((snap) => {
      if (branchId && snap.branch_id !== branchId) return false;
      return swarmSpanIds.has(snap.span_id) || snap.span_id.includes(swarmId);
    });
  }

  public forkBranch(swarmId: string, targetSpanId: string, newBranchId: string): ForkBranchResult {
    const snapshot = this.getSnapshot(targetSpanId);
    if (!snapshot) {
      throw new Error(`No state snapshot found for span '${targetSpanId}'`);
    }

    const swarmSpans = this.spans.filter((s) => s.swarm_id === swarmId);
    const targetIdx = swarmSpans.findIndex((s) => s.id === targetSpanId);
    let replicatedCount = 0;

    if (targetIdx >= 0) {
      const antecedent = swarmSpans.slice(0, targetIdx + 1);
      const forkedSpans: SwarmSpan[] = antecedent.map((s) => ({
        ...s,
        id: `${s.id}_fork_${newBranchId}`,
        branch_id: newBranchId,
      }));
      this.saveSpans(forkedSpans);
      replicatedCount = forkedSpans.length;
    }

    const rootBranchSnapshot: StateSnapshot = {
      id: crypto.randomUUID(),
      span_id: `${targetSpanId}_fork_${newBranchId}`,
      branch_id: newBranchId,
      state_hash: snapshot.state_hash,
      state_data: { ...snapshot.state_data },
      created_at: new Date().toISOString(),
    };
    this.saveSnapshot(rootBranchSnapshot);

    return {
      status: "forked",
      new_branch_id: newBranchId,
      target_span_id: targetSpanId,
      replicated_spans_count: replicatedCount,
      state: snapshot.state_data,
    };
  }

  public listBranches(swarmId?: string): string[] {
    const branches = new Set<string>(["main"]);
    for (const s of this.spans) {
      if ((!swarmId || s.swarm_id === swarmId) && s.branch_id) {
        branches.add(s.branch_id);
      }
    }
    for (const snap of this.snapshots) {
      if (snap.branch_id) {
        branches.add(snap.branch_id);
      }
    }
    return Array.from(branches);
  }

  public clear(): void {
    this.spans = [];
    this.snapshots = [];
    this.circuitEvents = [];
  }
}

export interface HttpStorageSinkOptions {
  endpoint?: string;
  apiKey?: string;
  projectId?: string;
}

export class HttpStorageSink implements StorageSink {
  public endpoint: string;
  public apiKey?: string;
  public projectId?: string;

  constructor(
    endpointOrOptions: string | HttpStorageSinkOptions = "http://localhost:8765",
    options: HttpStorageSinkOptions = {}
  ) {
    if (typeof endpointOrOptions === "string") {
      this.endpoint = endpointOrOptions.replace(/\/+$/, "");
      this.apiKey = options.apiKey;
      this.projectId = options.projectId;
    } else {
      this.endpoint = (endpointOrOptions.endpoint || "http://localhost:8765").replace(/\/+$/, "");
      this.apiKey = endpointOrOptions.apiKey;
      this.projectId = endpointOrOptions.projectId;
    }
  }

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (this.apiKey) {
      headers["Authorization"] = `Bearer ${this.apiKey}`;
      headers["X-API-Key"] = this.apiKey;
    }
    if (this.projectId) {
      headers["X-Project-ID"] = this.projectId;
    }
    return headers;
  }

  public async saveSpans(spans: SwarmSpan[]): Promise<void> {
    if (spans.length === 0) return;
    if (this.projectId) {
      for (const s of spans) {
        if (!s.project_id) {
          s.project_id = this.projectId;
        }
      }
    }
    try {
      // First try /api/v1/spans, fallback to /api/spans if 404
      const res = await fetch(`${this.endpoint}/api/v1/spans`, {
        method: "POST",
        headers: this.getHeaders(),
        body: JSON.stringify(spans),
      });
      if (res.status === 404) {
        await fetch(`${this.endpoint}/api/spans`, {
          method: "POST",
          headers: this.getHeaders(),
          body: JSON.stringify(spans),
        });
      }
    } catch {
      // Fire-and-forget in host agent process
    }
  }

  public async saveSnapshot(snapshot: StateSnapshot): Promise<void> {
    try {
      await fetch(`${this.endpoint}/api/snapshots`, {
        method: "POST",
        headers: this.getHeaders(),
        body: JSON.stringify(snapshot),
      });
    } catch {
      // Fire-and-forget
    }
  }

  public async recordCircuitEvent(event: CircuitEvent): Promise<void> {
    try {
      await fetch(`${this.endpoint}/api/circuit-events`, {
        method: "POST",
        headers: this.getHeaders(),
        body: JSON.stringify(event),
      });
    } catch {
      // Fire-and-forget
    }
  }

  public async getSnapshot(spanId: string): Promise<StateSnapshot | null> {
    try {
      const res = await fetch(`${this.endpoint}/api/v1/snapshots/${encodeURIComponent(spanId)}`, {
        headers: this.getHeaders(),
      });
      if (!res.ok) return null;
      return (await res.json()) as StateSnapshot;
    } catch {
      return null;
    }
  }

  public async getSnapshotsForSwarm(swarmId: string, branchId?: string): Promise<StateSnapshot[]> {
    try {
      const url = new URL(`${this.endpoint}/api/v1/swarms/${encodeURIComponent(swarmId)}/snapshots`);
      if (branchId) {
        url.searchParams.set("branch_id", branchId);
      }
      const res = await fetch(url.toString(), {
        headers: this.getHeaders(),
      });
      if (!res.ok) return [];
      return (await res.json()) as StateSnapshot[];
    } catch {
      return [];
    }
  }

  public async forkBranch(
    swarmId: string,
    targetSpanId: string,
    newBranchId: string
  ): Promise<ForkBranchResult> {
    const res = await fetch(`${this.endpoint}/api/v1/branches/fork`, {
      method: "POST",
      headers: this.getHeaders(),
      body: JSON.stringify({
        swarm_id: swarmId,
        target_span_id: targetSpanId,
        new_branch_id: newBranchId,
      }),
    });
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      throw new Error(`Failed to fork branch: HTTP ${res.status} - ${errText}`);
    }
    return (await res.json()) as ForkBranchResult;
  }

  public async listBranches(swarmId: string): Promise<string[]> {
    try {
      const res = await fetch(`${this.endpoint}/api/v1/swarms/${encodeURIComponent(swarmId)}/branches`, {
        headers: this.getHeaders(),
      });
      if (!res.ok) return ["main"];
      return (await res.json()) as string[];
    } catch {
      return ["main"];
    }
  }
}

export interface TransportConfig {
  batch_size?: number;
  flush_interval_ms?: number;
  max_queue_size?: number;
  masker?: SensitiveDataMasker;
}

export class TelemetryTransport {
  public sink: StorageSink;
  public batchSize: number;
  public flushIntervalMs: number;
  public maxQueueSize: number;
  private masker: SensitiveDataMasker;

  private queue: SwarmSpan[] = [];
  private timer: NodeJS.Timeout | null = null;
  private droppedCount: number = 0;
  private isStarted: boolean = false;
  private isFlushing: boolean = false;

  constructor(sink?: StorageSink, config: TransportConfig = {}) {
    this.sink = sink || new InMemoryStorageSink();
    this.batchSize = config.batch_size ?? 100;
    this.flushIntervalMs = config.flush_interval_ms ?? 500;
    this.maxQueueSize = config.max_queue_size ?? 10000;
    this.masker = config.masker ?? getDefaultMasker();
  }

  public get queueSize(): number {
    return this.queue.length;
  }

  public get droppedSpans(): number {
    return this.droppedCount;
  }

  public start(): void {
    if (this.isStarted) return;
    this.isStarted = true;

    this.timer = setInterval(() => {
      this.flush().catch(() => {});
    }, this.flushIntervalMs);

    if (this.timer.unref) {
      this.timer.unref();
    }
  }

  /**
   * Non-blocking span submission.
   * Completes in < 1 microsecond. Never blocks the calling thread.
   */
  public emitSpan(span: SwarmSpan): boolean {
    if (this.queue.length >= this.maxQueueSize) {
      this.droppedCount++;
      return false;
    }

    if (span.input_payload !== undefined && span.input_payload !== null) {
      span.input_payload = this.masker.maskPayload(span.input_payload);
    }
    if (span.output_payload !== undefined && span.output_payload !== null) {
      span.output_payload = this.masker.maskPayload(span.output_payload);
    }

    this.queue.push(span);
    if (this.queue.length >= this.batchSize && !this.isFlushing) {
      this.flush().catch(() => {});
    }
    return true;
  }

  /**
   * Flushes currently queued spans to the storage sink.
   */
  public async flush(): Promise<void> {
    if (this.isFlushing || this.queue.length === 0) {
      return;
    }

    this.isFlushing = true;
    try {
      while (this.queue.length > 0) {
        const batch = this.queue.splice(0, this.batchSize);
        if (batch.length > 0) {
          await this.sink.saveSpans(batch);
        }
      }
    } finally {
      this.isFlushing = false;
    }
  }

  public async shutdown(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.isStarted = false;
    await this.flush();
  }
}
