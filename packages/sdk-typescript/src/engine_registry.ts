/**
 * SwarmWatch Tenant-Scoped Engine Registry & Non-Interference (Seam 89 / ADR 0100).
 *
 * Implements strict multi-tenant non-interference (T1), dynamic engine isolation (T2),
 * and zero-bleed tenant lifecycle eviction (T3), eliminating un-scoped global singletons.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import { SafetyCircuitBus } from "./circuit_bus.ts";
import { DeltaCrdtKernel } from "./crdt_kernel.ts";
import { LeasedBarrierCoordinator } from "./barrier_lease.ts";
import { DurableCausalStream } from "./durable_stream.ts";
import { OptimisticAttestationEngine } from "./optimistic_attestation.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class EngineRegistryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EngineRegistryError";
  }
}

export class UnknownEngineKindError extends EngineRegistryError {
  constructor(message: string) {
    super(message);
    this.name = "UnknownEngineKindError";
  }
}

// ==============================================================================
// 2. Tenant Engine Registry Deep Module
// ==============================================================================

export class TenantEngineRegistry {
  private _registry: Map<string, Map<string, unknown>> = new Map();
  private _factories: Map<string, (tenantId: string) => unknown> = new Map();

  constructor() {
    this._factories.set("circuit_bus", () => new SafetyCircuitBus());
    this._factories.set("crdt_kernel", (tid) => new DeltaCrdtKernel(tid));
    this._factories.set("leased_barrier", () => new LeasedBarrierCoordinator());
    this._factories.set("durable_stream", (tid) => new DurableCausalStream(tid));
    this._factories.set("attestation", () => new OptimisticAttestationEngine());
  }

  registerFactory<T>(kind: string, factory: (tenantId: string) => T): void {
    this._factories.set(kind.toLowerCase(), factory);
  }

  get<T>(
    tenantId: string,
    kind: string,
    factory?: (tenantId: string) => T
  ): T {
    const tid = tenantId.trim();
    const k = kind.toLowerCase().trim();

    if (!this._registry.has(tid)) {
      this._registry.set(tid, new Map());
    }

    const tenantMap = this._registry.get(tid)!;
    if (!tenantMap.has(k)) {
      const fact = factory ?? (this._factories.get(k) as ((t: string) => T) | undefined);
      if (!fact) {
        throw new UnknownEngineKindError(
          `No factory registered for engine kind '${k}'`
        );
      }
      tenantMap.set(k, fact(tid));
    }

    return tenantMap.get(k) as T;
  }

  hasEngine(tenantId: string, kind: string): boolean {
    const tid = tenantId.trim();
    const k = kind.toLowerCase().trim();
    const tenantMap = this._registry.get(tid);
    return tenantMap ? tenantMap.has(k) : false;
  }

  evictTenant(tenantId: string): void {
    const tid = tenantId.trim();
    this._registry.delete(tid);
  }

  listTenants(): string[] {
    return Array.from(this._registry.keys()).sort();
  }

  clearForTest(): void {
    this._registry.clear();
  }
}

export const globalEngineRegistry = new TenantEngineRegistry();
