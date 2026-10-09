/**
 * SwarmWatch Hybrid Logical Clocks & Bounded-Skew Causal Timestamps (Seam 83 / ADR 0094).
 *
 * Implements Kulkarni et al. Hybrid Logical Clocks (HLC) for distributed multi-cluster agent systems,
 * guaranteeing causal monotonicity (H1), bounded physical drift protection (H2), and deterministic
 * tie-breaking (H3).
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class HlcError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "HlcError";
  }
}

export class BoundedSkewViolationError extends HlcError {
  readonly remote_physical_ns: number;
  readonly local_physical_ns: number;
  readonly max_drift_ns: number;

  constructor(remoteNs: number, localNs: number, maxDriftNs: number) {
    const skewMs = (remoteNs - localNs) / 1_000_000;
    const maxMs = maxDriftNs / 1_000_000;
    super(
      `Bounded skew violation: remote clock leads local clock by ${skewMs.toFixed(2)}ms (tolerated max=${maxMs.toFixed(2)}ms)`
    );
    this.name = "BoundedSkewViolationError";
    this.remote_physical_ns = remoteNs;
    this.local_physical_ns = localNs;
    this.max_drift_ns = maxDriftNs;
  }
}

// ==============================================================================
// 2. Domain Types & Interfaces
// ==============================================================================

export interface HybridLogicalTimestamp {
  readonly physical_ns: number;
  readonly logical_counter: number;
  readonly node_id: string;
}

export function compareHlc(
  a: HybridLogicalTimestamp,
  b: HybridLogicalTimestamp
): number {
  if (a.physical_ns !== b.physical_ns) {
    return a.physical_ns < b.physical_ns ? -1 : 1;
  }
  if (a.logical_counter !== b.logical_counter) {
    return a.logical_counter < b.logical_counter ? -1 : 1;
  }
  if (a.node_id !== b.node_id) {
    return a.node_id < b.node_id ? -1 : 1;
  }
  return 0;
}

export function formatHlc(ts: HybridLogicalTimestamp): string {
  return `${ts.physical_ns}:${ts.logical_counter}:${ts.node_id}`;
}

export function parseHlc(s: string): HybridLogicalTimestamp {
  const parts = s.split(":");
  if (parts.length !== 3) {
    throw new Error(`Malformed HLC timestamp string: ${s}`);
  }
  return {
    physical_ns: Number(parts[0]),
    logical_counter: Number(parts[1]),
    node_id: parts[2],
  };
}

// ==============================================================================
// 3. Hybrid Logical Clock Engine
// ==============================================================================

export class HybridLogicalClock {
  readonly node_id: string;
  readonly max_drift_ns: number;
  private readonly _physicalClock: () => number;
  private _l: number;
  private _c: number = 0;

  constructor(
    nodeId: string,
    maxDriftNs: number = 500_000_000,
    physicalClock?: () => number
  ) {
    this.node_id = nodeId;
    this.max_drift_ns = maxDriftNs;
    this._physicalClock = physicalClock ?? (() => Date.now() * 1_000_000);
    this._l = -1;
  }

  getCurrentState(): { l: number; c: number } {
    return { l: this._l, c: this._c };
  }

  now(): HybridLogicalTimestamp {
    const pt = this._physicalClock();
    const lOld = this._l;
    this._l = Math.max(lOld, pt);

    if (this._l === lOld) {
      this._c += 1;
    } else {
      this._c = 0;
    }

    return {
      physical_ns: this._l,
      logical_counter: this._c,
      node_id: this.node_id,
    };
  }

  observe(remote: HybridLogicalTimestamp): HybridLogicalTimestamp {
    const pt = this._physicalClock();

    if (remote.physical_ns > pt + this.max_drift_ns) {
      throw new BoundedSkewViolationError(
        remote.physical_ns,
        pt,
        this.max_drift_ns
      );
    }

    const lOld = this._l;
    const cOld = this._c;

    this._l = Math.max(lOld, remote.physical_ns, pt);

    if (this._l === lOld && this._l === remote.physical_ns) {
      this._c = Math.max(cOld, remote.logical_counter) + 1;
    } else if (this._l === lOld) {
      this._c = cOld + 1;
    } else if (this._l === remote.physical_ns) {
      this._c = remote.logical_counter + 1;
    } else {
      this._c = 0;
    }

    return {
      physical_ns: this._l,
      logical_counter: this._c,
      node_id: this.node_id,
    };
  }

  compare(a: HybridLogicalTimestamp, b: HybridLogicalTimestamp): number {
    return compareHlc(a, b);
  }
}

export const globalHlc = new HybridLogicalClock("cluster-default");
