/**
 * SwarmWatch Distributed Redis Circuit Breaker & Fleet State Synchronization (TypeScript Edition).
 * Provides atomic quota ledgers, sub-millisecond Pub/Sub trip broadcasting,
 * and high-fidelity mock client for deterministic multi-worker execution (ADR 0033).
 */

import { CircuitPolicy, SwarmCircuitBreakException } from "./circuit.ts";

export interface RedisTripBroadcast {
  swarm_id: string;
  reason: string;
  tripped_by: string;
  tripped_at: string;
  metric_val?: number;
}

export interface FleetMetrics {
  swarm_id: string;
  total_tokens: number;
  total_cost: number;
  total_turns: number;
  active_workers: Set<string>;
  is_tripped: boolean;
  trip_reason?: string | null;
  tripped_by?: string | null;
  tripped_at?: string | null;
}

export interface FleetEvent {
  id: string;
  swarm_id: string;
  event_type: "metric_update" | "trip_broadcast" | "worker_joined" | "worker_left";
  worker_id: string;
  payload: Record<string, any>;
  timestamp: string;
}

export interface RedisClientInterface {
  ping(): Promise<boolean>;
  set(name: string, value: string, options?: { px?: number; ex?: number; nx?: boolean }): Promise<boolean>;
  get(name: string): Promise<string | null>;
  pexpire(name: string, ms: number): Promise<boolean>;
  evalScript(script: string, keys: string[], args: any[]): Promise<any>;
  hset(name: string, key: string, value: string): Promise<number>;
  hget(name: string, key: string): Promise<string | null>;
  hgetall(name: string): Promise<Record<string, string>>;
  hincrby(name: string, key: string, amount?: number): Promise<number>;
  hincrbyfloat(name: string, key: string, amount?: number): Promise<number>;
  sadd(name: string, ...values: string[]): Promise<number>;
  srem(name: string, ...values: string[]): Promise<number>;
  smembers(name: string): Promise<Set<string>>;
  delKey(...names: string[]): Promise<number>;
  publish(channel: string, message: string): Promise<number>;
  subscribe(channel: string, callback: (channel: string, message: string) => void): Promise<any>;
  close(): Promise<void>;
}

export class MockRedisClient implements RedisClientInterface {
  private strings = new Map<string, string>();
  private expires = new Map<string, number>();
  private hashes = new Map<string, Map<string, string>>();
  private sets = new Map<string, Set<string>>();
  private subscribers = new Map<string, Array<(channel: string, message: string) => void>>();
  private isClosed = false;

  async ping(): Promise<boolean> {
    return !this.isClosed;
  }

  async set(name: string, value: string, options?: { px?: number; ex?: number; nx?: boolean }): Promise<boolean> {
    const now = Date.now();
    const exp = this.expires.get(name);
    if (exp && exp <= now) {
      this.strings.delete(name);
      this.expires.delete(name);
    }
    if (options?.nx && this.strings.has(name)) {
      return false;
    }
    this.strings.set(name, String(value));
    if (options?.px) {
      this.expires.set(name, now + options.px);
    } else if (options?.ex) {
      this.expires.set(name, now + options.ex * 1000);
    } else {
      this.expires.delete(name);
    }
    return true;
  }

  async get(name: string): Promise<string | null> {
    const now = Date.now();
    const exp = this.expires.get(name);
    if (exp && exp <= now) {
      this.strings.delete(name);
      this.expires.delete(name);
      return null;
    }
    return this.strings.get(name) ?? null;
  }

  async pexpire(name: string, ms: number): Promise<boolean> {
    const val = await this.get(name);
    if (val === null) return false;
    this.expires.set(name, Date.now() + ms);
    return true;
  }

  async evalScript(script: string, keys: string[], args: any[]): Promise<any> {
    if (script.toLowerCase().includes("del")) {
      const key = keys[0] || "";
      const expectedToken = String(args[0] || "");
      const current = await this.get(key);
      if (current === expectedToken) {
        await this.delKey(key);
        return 1;
      }
      return 0;
    } else if (script.toLowerCase().includes("pexpire")) {
      const key = keys[0] || "";
      const expectedToken = String(args[0] || "");
      const ttlMs = parseInt(String(args[1]), 10) || 0;
      const current = await this.get(key);
      if (current === expectedToken) {
        await this.pexpire(key, ttlMs);
        return 1;
      }
      return 0;
    }
    return 0;
  }

  async hset(name: string, key: string, value: string): Promise<number> {
    let hash = this.hashes.get(name);
    if (!hash) {
      hash = new Map();
      this.hashes.set(name, hash);
    }
    const isNew = !hash.has(key);
    hash.set(key, String(value));
    return isNew ? 1 : 0;
  }

  async hget(name: string, key: string): Promise<string | null> {
    const hash = this.hashes.get(name);
    return hash?.get(key) ?? null;
  }

  async hgetall(name: string): Promise<Record<string, string>> {
    const hash = this.hashes.get(name);
    if (!hash) return {};
    const res: Record<string, string> = {};
    for (const [k, v] of hash.entries()) {
      res[k] = v;
    }
    return res;
  }

  async hincrby(name: string, key: string, amount: number = 1): Promise<number> {
    let hash = this.hashes.get(name);
    if (!hash) {
      hash = new Map();
      this.hashes.set(name, hash);
    }
    const curRaw = hash.get(key) ?? "0";
    const cur = parseInt(curRaw, 10) || 0;
    const next = cur + amount;
    hash.set(key, String(next));
    return next;
  }

  async hincrbyfloat(name: string, key: string, amount: number = 1.0): Promise<number> {
    let hash = this.hashes.get(name);
    if (!hash) {
      hash = new Map();
      this.hashes.set(name, hash);
    }
    const curRaw = hash.get(key) ?? "0.0";
    const cur = parseFloat(curRaw) || 0.0;
    const next = Number((cur + amount).toFixed(6));
    hash.set(key, String(next));
    return next;
  }

  async sadd(name: string, ...values: string[]): Promise<number> {
    let s = this.sets.get(name);
    if (!s) {
      s = new Set();
      this.sets.set(name, s);
    }
    let added = 0;
    for (const v of values) {
      if (!s.has(v)) {
        s.add(v);
        added++;
      }
    }
    return added;
  }

  async srem(name: string, ...values: string[]): Promise<number> {
    const s = this.sets.get(name);
    if (!s) return 0;
    let removed = 0;
    for (const v of values) {
      if (s.has(v)) {
        s.delete(v);
        removed++;
      }
    }
    return removed;
  }

  async smembers(name: string): Promise<Set<string>> {
    const s = this.sets.get(name);
    return new Set(s ? Array.from(s) : []);
  }

  async delKey(...names: string[]): Promise<number> {
    let count = 0;
    for (const n of names) {
      if (this.strings.delete(n)) {
        this.expires.delete(n);
        count++;
      }
      if (this.hashes.delete(n)) count++;
      if (this.sets.delete(n)) count++;
    }
    return count;
  }

  async publish(channel: string, message: string): Promise<number> {
    const cbs = this.subscribers.get(channel) || [];
    for (const cb of cbs) {
      try {
        cb(channel, message);
      } catch (e) {
        console.error("Error in MockRedis subscriber:", e);
      }
    }
    return cbs.length;
  }

  async subscribe(channel: string, callback: (channel: string, message: string) => void): Promise<any> {
    let cbs = this.subscribers.get(channel);
    if (!cbs) {
      cbs = [];
      this.subscribers.set(channel, cbs);
    }
    cbs.push(callback);
    return { channel, callback };
  }

  async close(): Promise<void> {
    this.isClosed = true;
    this.subscribers.clear();
  }
}

export class RedisFleetBackend {
  public client: RedisClientInterface;

  constructor(client?: RedisClientInterface) {
    this.client = client || new MockRedisClient();
  }

  private metricsKey(swarmId: string): string {
    return `swarmwatch:swarm:${swarmId}:metrics`;
  }

  private workersKey(swarmId: string): string {
    return `swarmwatch:swarm:${swarmId}:workers`;
  }

  private tripChannel(swarmId: string): string {
    return `swarmwatch:fleet:${swarmId}:trips`;
  }

  private globalTripChannel(): string {
    return "swarmwatch:trips";
  }

  async registerWorker(workerId: string, swarmId: string): Promise<void> {
    await this.client.sadd(this.workersKey(swarmId), workerId);
  }

  async unregisterWorker(workerId: string, swarmId: string): Promise<void> {
    await this.client.srem(this.workersKey(swarmId), workerId);
  }

  async recordTurnAtomic(
    workerId: string,
    swarmId: string,
    tokens: number,
    cost: number | null,
    turns: number,
    policy: CircuitPolicy,
  ): Promise<FleetMetrics> {
    const mKey = this.metricsKey(swarmId);
    const wKey = this.workersKey(swarmId);

    // 1. Check if already tripped
    const isTrippedRaw = await this.client.hget(mKey, "is_tripped");
    if (isTrippedRaw === "1" || isTrippedRaw === "true") {
      const allFields = await this.client.hgetall(mKey);
      const active = await this.client.smembers(wKey);
      return this.parseMetrics(swarmId, allFields, active);
    }

    // 2. Add worker to active set
    await this.client.sadd(wKey, workerId);

    // 3. Turn cost
    const turnCost = cost !== null ? cost : tokens * policy.cost_per_token;

    // 4. Atomic increments
    const totTokens = await this.client.hincrby(mKey, "total_tokens", tokens);
    const totTurns = await this.client.hincrby(mKey, "total_turns", turns);
    const totCost = await this.client.hincrbyfloat(mKey, "total_cost", turnCost);

    // 5. Evaluate thresholds
    let breached = false;
    let tripReason = "";
    let metricVal = 0.0;

    if (policy.max_turns !== null && totTurns >= policy.max_turns) {
      breached = true;
      tripReason = `Fleet max turns quota reached (${totTurns}/${policy.max_turns})`;
      metricVal = totTurns;
    } else if (policy.token_budget !== null && totTokens >= policy.token_budget) {
      breached = true;
      tripReason = `Fleet token budget exceeded (${totTokens}/${policy.token_budget})`;
      metricVal = totTokens;
    } else if (policy.cost_budget !== null && totCost >= policy.cost_budget) {
      breached = true;
      tripReason = `Fleet cost budget exceeded ($${totCost.toFixed(4)}/$${policy.cost_budget.toFixed(4)})`;
      metricVal = totCost;
    }

    if (breached) {
      await this.trip(swarmId, tripReason, workerId, metricVal);
      throw new SwarmCircuitBreakException(
        `Fleet Circuit Breaker TRIPPED: ${tripReason}`,
        metricVal,
        workerId,
        swarmId,
      );
    }

    const active = await this.client.smembers(wKey);
    const allFields = await this.client.hgetall(mKey);
    return this.parseMetrics(swarmId, allFields, active);
  }

  async trip(swarmId: string, reason: string, trippedBy: string, metricVal: number = 0.0): Promise<void> {
    const mKey = this.metricsKey(swarmId);
    const nowIso = new Date().toISOString();

    await this.client.hset(mKey, "is_tripped", "1");
    await this.client.hset(mKey, "trip_reason", reason);
    await this.client.hset(mKey, "tripped_by", trippedBy);
    await this.client.hset(mKey, "tripped_at", nowIso);

    const payload: RedisTripBroadcast = {
      swarm_id: swarmId,
      reason,
      tripped_by: trippedBy,
      tripped_at: nowIso,
      metric_val: metricVal,
    };
    const serialized = JSON.stringify(payload);
    await this.client.publish(this.tripChannel(swarmId), serialized);
    await this.client.publish(this.globalTripChannel(), serialized);
  }

  async isTripped(swarmId: string): Promise<boolean> {
    const raw = await this.client.hget(this.metricsKey(swarmId), "is_tripped");
    return raw === "1" || raw === "true";
  }

  async getMetrics(swarmId: string): Promise<FleetMetrics | null> {
    const allFields = await this.client.hgetall(this.metricsKey(swarmId));
    if (!allFields || Object.keys(allFields).length === 0) return null;
    const active = await this.client.smembers(this.workersKey(swarmId));
    return this.parseMetrics(swarmId, allFields, active);
  }

  async resetSwarm(swarmId: string): Promise<void> {
    await this.client.delKey(this.metricsKey(swarmId), this.workersKey(swarmId));
  }

  private parseMetrics(swarmId: string, fields: Record<string, string>, active: Set<string>): FleetMetrics {
    return {
      swarm_id: swarmId,
      total_tokens: parseInt(fields.total_tokens || "0", 10) || 0,
      total_turns: parseInt(fields.total_turns || "0", 10) || 0,
      total_cost: parseFloat(fields.total_cost || "0.0") || 0.0,
      active_workers: active,
      is_tripped: fields.is_tripped === "1" || fields.is_tripped === "true",
      trip_reason: fields.trip_reason || null,
      tripped_by: fields.tripped_by || null,
      tripped_at: fields.tripped_at || null,
    };
  }
}

export class RedisCircuitCoordinator {
  public policy: CircuitPolicy;
  public backend: RedisFleetBackend;
  private client: RedisClientInterface;
  private trippedCache = new Map<string, boolean>();
  private subscribers: Array<(event: FleetEvent) => void> = [];

  constructor(
    policyOrOptions?: CircuitPolicy | {
      policy?: CircuitPolicy;
      client?: RedisClientInterface;
    },
    client?: RedisClientInterface,
  ) {
    if (policyOrOptions instanceof CircuitPolicy) {
      this.policy = policyOrOptions;
      this.client = client || new MockRedisClient();
    } else if (policyOrOptions && typeof policyOrOptions === "object" && ("policy" in policyOrOptions || "client" in policyOrOptions)) {
      this.policy = policyOrOptions.policy || new CircuitPolicy();
      this.client = policyOrOptions.client || client || new MockRedisClient();
    } else {
      this.policy = new CircuitPolicy();
      this.client = client || new MockRedisClient();
    }
    this.backend = new RedisFleetBackend(this.client);

    // Subscribe to global trip broadcasts
    this.client.subscribe("swarmwatch:trips", (channel, message) => {
      this.onRemoteTrip(channel, message);
    });
  }

  private onRemoteTrip(channel: string, message: string): void {
    try {
      const data: RedisTripBroadcast = JSON.parse(message);
      if (data.swarm_id) {
        this.trippedCache.set(data.swarm_id, true);
        const event: FleetEvent = {
          id: `evt-${Date.now()}`,
          swarm_id: data.swarm_id,
          event_type: "trip_broadcast",
          worker_id: data.tripped_by || "remote-worker",
          payload: data,
          timestamp: new Date().toISOString(),
        };
        for (const cb of this.subscribers) {
          try {
            cb(event);
          } catch (e) {
            console.error("Error in subscriber callback:", e);
          }
        }
      }
    } catch (e) {
      console.error("Failed to parse trip broadcast message:", e);
    }
  }

  async registerWorker(workerId: string, swarmId: string): Promise<void> {
    await this.backend.registerWorker(workerId, swarmId);
    this.emitLocalEvent({
      id: `evt-${Date.now()}`,
      swarm_id: swarmId,
      event_type: "worker_joined",
      worker_id: workerId,
      payload: {},
      timestamp: new Date().toISOString(),
    });
  }

  async unregisterWorker(workerId: string, swarmId: string): Promise<void> {
    await this.backend.unregisterWorker(workerId, swarmId);
    this.emitLocalEvent({
      id: `evt-${Date.now()}`,
      swarm_id: swarmId,
      event_type: "worker_left",
      worker_id: workerId,
      payload: {},
      timestamp: new Date().toISOString(),
    });
  }

  async recordTurn(
    workerId: string,
    swarmId: string,
    tokens: number = 0,
    cost: number | null = null,
    turns: number = 1,
  ): Promise<FleetMetrics> {
    if (this.trippedCache.get(swarmId)) {
      const m = await this.backend.getMetrics(swarmId);
      const reason = m?.trip_reason || "Distributed circuit breaker tripped";
      const trippedBy = m?.tripped_by || "peer-worker";
      throw new SwarmCircuitBreakException(
        `Fleet Circuit Breaker TRIPPED for swarm '${swarmId}': ${reason} (tripped by ${trippedBy})`,
        m ? m.total_turns : turns,
        workerId,
        swarmId,
      );
    }

    const metrics = await this.backend.recordTurnAtomic(
      workerId,
      swarmId,
      tokens,
      cost,
      turns,
      this.policy,
    );

    if (metrics.is_tripped) {
      this.trippedCache.set(swarmId, true);
      throw new SwarmCircuitBreakException(
        `Fleet Circuit Breaker TRIPPED for swarm '${swarmId}': ${metrics.trip_reason} (tripped by ${metrics.tripped_by})`,
        metrics.total_turns,
        workerId,
        swarmId,
      );
    }

    this.emitLocalEvent({
      id: `evt-${Date.now()}`,
      swarm_id: swarmId,
      event_type: "metric_update",
      worker_id: workerId,
      payload: {
        total_tokens: metrics.total_tokens,
        total_cost: metrics.total_cost,
        total_turns: metrics.total_turns,
      },
      timestamp: new Date().toISOString(),
    });

    return metrics;
  }

  async trip(swarmId: string, reason: string, trippedBy: string, metricVal: number = 0.0): Promise<void> {
    this.trippedCache.set(swarmId, true);
    await this.backend.trip(swarmId, reason, trippedBy, metricVal);
  }

  async isTripped(swarmId: string): Promise<boolean> {
    if (this.trippedCache.get(swarmId)) return true;
    const tripped = await this.backend.isTripped(swarmId);
    if (tripped) this.trippedCache.set(swarmId, true);
    return tripped;
  }

  async getMetrics(swarmId: string): Promise<FleetMetrics | null> {
    return this.backend.getMetrics(swarmId);
  }

  async resetSwarm(swarmId: string): Promise<void> {
    this.trippedCache.delete(swarmId);
    await this.backend.resetSwarm(swarmId);
  }

  subscribe(callback: (event: FleetEvent) => void): void {
    this.subscribers.push(callback);
  }

  private emitLocalEvent(event: FleetEvent): void {
    for (const cb of this.subscribers) {
      try {
        cb(event);
      } catch (e) {
        console.error("Error in fleet subscriber callback:", e);
      }
    }
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
