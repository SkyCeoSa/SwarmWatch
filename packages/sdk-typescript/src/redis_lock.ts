/**
 * SwarmWatch Distributed Redis Lock & Lease-Based Leader Election (TypeScript Edition / ADR 0035).
 * Provides atomic mutual exclusion, lease-based leader election, automatic heartbeat renewal,
 * and zero-downtime standby failover for multi-server SwarmWatch deployments.
 */

import * as crypto from "node:crypto";
import * as os from "node:os";
import type { RedisClientInterface } from "./redis_circuit.ts";
import { MockRedisClient } from "./redis_circuit.ts";

export type ServerNodeRole = "LEADER" | "STANDBY" | "STEPPED_DOWN";

export interface ClusterNodeInfo {
  node_id: string;
  role: ServerNodeRole;
  hostname: string;
  pid: number;
  last_heartbeat: number;
  started_at: number;
}

export interface ClusterLeaderStatus {
  leader_node_id: string | null;
  is_leader: boolean;
  lease_expires_in_ms: number | null;
  local_node_id: string;
  local_role: ServerNodeRole;
  nodes_count: number;
  roster: ClusterNodeInfo[];
}

const LUA_RELEASE_LOCK = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`;

const LUA_EXTEND_LEASE = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
`;

export class RedisDistributedLock {
  private client: RedisClientInterface;
  public readonly resourceName: string;
  public readonly ttlMs: number;
  public readonly retryIntervalMs: number;
  private _token: string | null = null;

  constructor(
    client: RedisClientInterface,
    resourceName: string,
    ttlMs: number = 5000,
    retryIntervalMs: number = 50
  ) {
    this.client = client;
    this.resourceName = resourceName;
    this.ttlMs = ttlMs;
    this.retryIntervalMs = retryIntervalMs;
  }

  get token(): string | null {
    return this._token;
  }

  get isHeld(): boolean {
    return this._token !== null;
  }

  async acquire(options?: { blocking?: boolean; timeoutMs?: number }): Promise<boolean> {
    const blocking = options?.blocking ?? true;
    const timeoutMs = options?.timeoutMs;
    const startTime = Date.now();
    const token = crypto.randomUUID();

    while (true) {
      const acquired = await this.client.set(this.resourceName, token, {
        px: this.ttlMs,
        nx: true,
      });

      if (acquired) {
        this._token = token;
        return true;
      }

      if (!blocking) {
        return false;
      }

      if (timeoutMs !== undefined && Date.now() - startTime >= timeoutMs) {
        return false;
      }

      await new Promise((resolve) => setTimeout(resolve, this.retryIntervalMs));
    }
  }

  async release(): Promise<boolean> {
    if (!this._token) return false;

    try {
      const res = await this.client.evalScript(
        LUA_RELEASE_LOCK,
        [this.resourceName],
        [this._token]
      );
      const released = res === 1 || res === "1";
      return released;
    } catch {
      return false;
    } finally {
      this._token = null;
    }
  }

  async extend(additionalTtlMs?: number): Promise<boolean> {
    if (!this._token) return false;
    const ms = additionalTtlMs || this.ttlMs;
    try {
      const res = await this.client.evalScript(
        LUA_EXTEND_LEASE,
        [this.resourceName],
        [this._token, ms]
      );
      return res === 1 || res === "1";
    } catch {
      return false;
    }
  }

  async isLocked(): Promise<boolean> {
    const val = await this.client.get(this.resourceName);
    return val !== null;
  }
}

export class RedisLeaderElector {
  public readonly client: RedisClientInterface;
  public readonly nodeId: string;
  public readonly electionKey: string;
  public readonly rosterKey: string;
  public readonly leaseTtlMs: number;
  public readonly heartbeatIntervalMs: number;

  private _role: ServerNodeRole = "STANDBY";
  private _running = false;
  private _timer: NodeJS.Timeout | null = null;
  private _onElectedCallbacks: Array<() => void> = [];
  private _onSteppedDownCallbacks: Array<() => void> = [];
  private _steppedDownAt = 0;

  private _info: ClusterNodeInfo;

  constructor(options?: {
    client?: RedisClientInterface;
    nodeId?: string;
    electionKey?: string;
    rosterKey?: string;
    leaseTtlMs?: number;
    heartbeatIntervalMs?: number;
  }) {
    this.client = options?.client || new MockRedisClient();
    this.nodeId = options?.nodeId || `srv-node-${crypto.randomUUID().slice(0, 8)}`;
    this.electionKey = options?.electionKey || "swarmwatch:leader:lock";
    this.rosterKey = options?.rosterKey || "swarmwatch:cluster:nodes";
    this.leaseTtlMs = options?.leaseTtlMs || 5000;
    this.heartbeatIntervalMs = options?.heartbeatIntervalMs || 1500;

    const now = Date.now();
    this._info = {
      node_id: this.nodeId,
      role: this._role,
      hostname: os.hostname(),
      pid: process.pid,
      last_heartbeat: now,
      started_at: now,
    };
  }

  get role(): ServerNodeRole {
    return this._role;
  }

  get isLeader(): boolean {
    return this._role === "LEADER";
  }

  onElected(callback: () => void): void {
    this._onElectedCallbacks.push(callback);
  }

  onSteppedDown(callback: () => void): void {
    this._onSteppedDownCallbacks.push(callback);
  }

  async start(): Promise<void> {
    if (this._running) return;
    this._running = true;
    await this._tick();
    this._timer = setInterval(() => {
      this._tick().catch(() => {});
    }, this.heartbeatIntervalMs);
  }

  async stop(): Promise<void> {
    if (!this._running) return;
    this._running = false;
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    if (this.isLeader) {
      await this.stepDown();
    }
    try {
      await this.client.srem(this.rosterKey, this.nodeId);
      await this.client.delKey(`${this.rosterKey}:${this.nodeId}`);
    } catch {}
  }

  async stepDown(): Promise<boolean> {
    if (this._role !== "LEADER") return false;
    this._role = "STEPPED_DOWN";
    this._info.role = "STEPPED_DOWN";
    this._steppedDownAt = Date.now();

    let released = false;
    try {
      const res = await this.client.evalScript(
        LUA_RELEASE_LOCK,
        [this.electionKey],
        [this.nodeId]
      );
      released = res === 1 || res === "1";
    } catch {
      released = false;
    }

    this._notifySteppedDown();
    return released;
  }

  private async _tick(): Promise<void> {
    if (!this._running) return;

    try {
      await this._recordHeartbeat();

      if (this._role === "LEADER") {
        // Renew lease
        const res = await this.client.evalScript(
          LUA_EXTEND_LEASE,
          [this.electionKey],
          [this.nodeId, this.leaseTtlMs]
        );
        const renewed = res === 1 || res === "1";
        if (!renewed) {
          this._role = "STANDBY";
          this._info.role = "STANDBY";
          this._notifySteppedDown();
        }
      } else if (this._role === "STEPPED_DOWN") {
        // Yield to standby nodes; transition back to STANDBY once another node is elected or grace period expires
        const currentLeader = await this.client.get(this.electionKey);
        if (currentLeader && currentLeader !== this.nodeId) {
          this._role = "STANDBY";
          this._info.role = "STANDBY";
        } else if ((Date.now() - this._steppedDownAt) > this.leaseTtlMs) {
          this._role = "STANDBY";
          this._info.role = "STANDBY";
        }
      } else {
        // STANDBY: Try to acquire lock
        const acquired = await this.client.set(this.electionKey, this.nodeId, {
          px: this.leaseTtlMs,
          nx: true,
        });
        if (acquired) {
          this._role = "LEADER";
          this._info.role = "LEADER";
          this._notifyElected();
        } else {
          const currentLeader = await this.client.get(this.electionKey);
          if (currentLeader === this.nodeId) {
            this._role = "LEADER";
            this._info.role = "LEADER";
          } else if (this._role !== "STANDBY") {
            this._role = "STANDBY";
            this._info.role = "STANDBY";
          }
        }
      }
    } catch {}
  }

  private async _recordHeartbeat(): Promise<void> {
    const now = Date.now();
    this._info.last_heartbeat = now;
    const nodeKey = `${this.rosterKey}:${this.nodeId}`;
    await this.client.set(nodeKey, JSON.stringify(this._info), {
      px: this.heartbeatIntervalMs * 4,
    });
    await this.client.sadd(this.rosterKey, this.nodeId);
  }

  private _notifyElected(): void {
    for (const cb of [...this._onElectedCallbacks]) {
      try {
        cb();
      } catch {}
    }
  }

  private _notifySteppedDown(): void {
    for (const cb of [...this._onSteppedDownCallbacks]) {
      try {
        cb();
      } catch {}
    }
  }

  async getLeaderStatus(): Promise<ClusterLeaderStatus> {
    const leaderId = await this.client.get(this.electionKey);
    const allNodeIds = await this.client.smembers(this.rosterKey);

    const roster: ClusterNodeInfo[] = [];
    const deadNodes: string[] = [];
    const now = Date.now();

    for (const nid of allNodeIds) {
      const nodeKey = `${this.rosterKey}:${nid}`;
      const raw = await this.client.get(nodeKey);
      if (raw) {
        try {
          const info = JSON.parse(raw) as ClusterNodeInfo;
          if (now - info.last_heartbeat < 10000) {
            if (leaderId && info.node_id === leaderId) {
              info.role = "LEADER";
            }
            roster.push(info);
          } else {
            deadNodes.push(nid);
          }
        } catch {
          deadNodes.push(nid);
        }
      } else {
        deadNodes.push(nid);
      }
    }

    if (deadNodes.length > 0) {
      await this.client.srem(this.rosterKey, ...deadNodes);
    }

    if (!roster.some((n) => n.node_id === this.nodeId)) {
      roster.push(this._info);
    }

    roster.sort((a, b) => {
      if (a.role === "LEADER") return -1;
      if (b.role === "LEADER") return 1;
      return a.node_id.localeCompare(b.node_id);
    });

    return {
      leader_node_id: leaderId,
      is_leader: leaderId === this.nodeId,
      lease_expires_in_ms: leaderId ? this.leaseTtlMs : 0,
      local_node_id: this.nodeId,
      local_role: this._role,
      nodes_count: roster.length,
      roster,
    };
  }
}
