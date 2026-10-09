/**
 * SwarmWatch Unified Safety Circuit Bus & Quorum-Gated Rearm (Seam 82 / ADR 0093).
 *
 * Implements a centralized, fail-closed safety circuit event bus across all subsystems,
 * monotonic generation counters, multi-operator Ed25519 threshold quorum rearming (Seam 81),
 * scope-isolated and global guardrails, and non-blocking pub/sub trip notifications.
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import {
  type CommitteeRoster,
  type ThresholdQuorumCertificate,
  verifyQuorumCertificate,
} from "./qc.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class CircuitBusError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CircuitBusError";
  }
}

export class CircuitBusTrippedError extends CircuitBusError {
  readonly reason: string;
  readonly scope: string;
  readonly generation: number;

  constructor(reason: string, scope: string, generation: number) {
    super(`Safety circuit TRIPPED (scope=${scope}, gen=${generation}): ${reason}`);
    this.name = "CircuitBusTrippedError";
    this.reason = reason;
    this.scope = scope;
    this.generation = generation;
  }
}

export class UnauthorizedRearmError extends CircuitBusError {
  constructor(message: string) {
    super(message);
    this.name = "UnauthorizedRearmError";
  }
}

export class InvalidSnapshotHashError extends CircuitBusError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidSnapshotHashError";
  }
}

// ==============================================================================
// 2. Domain Types & Object Enums
// ==============================================================================

export const CircuitState = {
  CLOSED: "CLOSED",
  TRIPPED: "TRIPPED",
  REARMING: "REARMING",
} as const;

export type CircuitStateType = (typeof CircuitState)[keyof typeof CircuitState];

export interface CircuitTripEvent {
  readonly trip_id: string;
  readonly generation: number;
  readonly timestamp_ns: number;
  readonly reason: string;
  readonly scope: string;
  readonly metadata: Record<string, unknown>;
}

export interface CircuitBusStatus {
  readonly state: CircuitStateType;
  readonly generation: number;
  readonly tripped_scopes: readonly string[];
  readonly last_trip_event: CircuitTripEvent | null;
  readonly total_trips: number;
  readonly last_rearm_ns: number | null;
  readonly last_snapshot_hash: string | null;
}

// ==============================================================================
// 3. Unified Safety Circuit Bus Deep Module
// ==============================================================================

export class SafetyCircuitBus {
  private _state: CircuitStateType = CircuitState.CLOSED;
  private _generation: number = 0;
  private _trippedScopes: Set<string> = new Set();
  private _lastTripEvent: CircuitTripEvent | null = null;
  private _totalTrips: number = 0;
  private _lastRearmNs: number | null = null;
  private _lastSnapshotHash: string | null = null;
  private _subscribers: Array<(event: CircuitTripEvent) => void> = [];

  get state(): CircuitStateType {
    return this._state;
  }

  get generation(): number {
    return this._generation;
  }

  isTripped(scope: string = "GLOBAL"): boolean {
    if (this._state === CircuitState.TRIPPED) {
      return true;
    }
    const normalized = scope.toUpperCase();
    return this._trippedScopes.has(normalized) || this._trippedScopes.has("GLOBAL");
  }

  trip(
    reason: string,
    scope: string = "GLOBAL",
    metadata: Record<string, unknown> = {}
  ): string {
    const normalizedScope = scope.toUpperCase();
    const nowNs = Date.now() * 1_000_000;

    this._generation += 1;
    const tripMaterial = `trip:${this._generation}:${normalizedScope}:${nowNs}:${reason}`;
    const tripId = crypto.createHash("sha256").update(tripMaterial, "utf-8").digest("hex");

    const event: CircuitTripEvent = {
      trip_id: tripId,
      generation: this._generation,
      timestamp_ns: nowNs,
      reason,
      scope: normalizedScope,
      metadata: { ...metadata },
    };

    this._state = CircuitState.TRIPPED;
    this._trippedScopes.add(normalizedScope);
    this._lastTripEvent = event;
    this._totalTrips += 1;

    for (const sub of [...this._subscribers]) {
      try {
        sub(event);
      } catch {
        // Subscribers must never interrupt safety trip propagation
      }
    }

    return tripId;
  }

  rearm(
    qc: ThresholdQuorumCertificate,
    snapshotHash: string,
    expectedRoster?: CommitteeRoster
  ): number {
    if (!snapshotHash || snapshotHash.length < 32) {
      throw new InvalidSnapshotHashError("Rearm requires a valid cryptographic state snapshot hash");
    }

    if (qc.domain !== "circuit:rearm") {
      throw new UnauthorizedRearmError(`Certificate domain '${qc.domain}' is not 'circuit:rearm'`);
    }

    if (qc.signers.length < 2) {
      throw new UnauthorizedRearmError(
        `Multi-operator rule requires >= 2 operator signatures, found ${qc.signers.length}`
      );
    }

    if (qc.digest !== snapshotHash) {
      throw new UnauthorizedRearmError(
        `Certificate digest '${qc.digest}' does not match snapshot hash '${snapshotHash}'`
      );
    }

    const verdict = verifyQuorumCertificate(qc, expectedRoster);
    if (!verdict.is_valid) {
      throw new UnauthorizedRearmError(
        `Quorum certificate verification failed: ${verdict.error_reason ?? "unknown error"}`
      );
    }

    this._state = CircuitState.CLOSED;
    this._trippedScopes.clear();
    this._lastRearmNs = Date.now() * 1_000_000;
    this._lastSnapshotHash = snapshotHash;

    return this._generation;
  }

  async guard<T>(scope: string, fn: () => T | Promise<T>): Promise<T> {
    const normalized = scope.toUpperCase();
    if (this.isTripped(normalized)) {
      const reason = this._lastTripEvent ? this._lastTripEvent.reason : "Circuit tripped";
      throw new CircuitBusTrippedError(reason, normalized, this._generation);
    }
    return await fn();
  }

  subscribe(callback: (event: CircuitTripEvent) => void): void {
    if (!this._subscribers.includes(callback)) {
      this._subscribers.push(callback);
    }
  }

  unsubscribe(callback: (event: CircuitTripEvent) => void): void {
    const idx = this._subscribers.indexOf(callback);
    if (idx !== -1) {
      this._subscribers.splice(idx, 1);
    }
  }

  getStatus(): CircuitBusStatus {
    return {
      state: this._state,
      generation: this._generation,
      tripped_scopes: Array.from(this._trippedScopes).sort(),
      last_trip_event: this._lastTripEvent,
      total_trips: this._totalTrips,
      last_rearm_ns: this._lastRearmNs,
      last_snapshot_hash: this._lastSnapshotHash,
    };
  }

  resetForTest(): void {
    this._state = CircuitState.CLOSED;
    this._trippedScopes.clear();
    this._lastTripEvent = null;
  }
}

export const globalCircuitBus = new SafetyCircuitBus();
export const globalSafetyCircuitBus = globalCircuitBus;
