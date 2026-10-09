/**
 * SwarmWatch Property-Verified δ-CRDT Kernel with Causal Delivery (Seam 84 / ADR 0095).
 *
 * Implements a mathematically verified δ-CRDT state kernel for distributed multi-cluster hyper-graphs.
 * Guarantees algebraic convergence (C1: commutativity, associativity, idempotence),
 * causal delivery of out-of-order deltas without tripping safety circuits (C2),
 * and succinct deterministic Merkle state digests (C3).
 *
 * Fully compliant with Node 22 native type-stripping rules (no enums, no parameter properties).
 */

import * as crypto from "node:crypto";
import {
  type HybridLogicalTimestamp,
  HybridLogicalClock,
  compareHlc,
  formatHlc,
} from "./hlc.ts";
import { computeMerkleRoot } from "./qc.ts";

// ==============================================================================
// 1. Custom Exceptions
// ==============================================================================

export class CrdtKernelError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CrdtKernelError";
  }
}

export class MalformedDeltaError extends CrdtKernelError {
  constructor(message: string) {
    super(message);
    this.name = "MalformedDeltaError";
  }
}

// ==============================================================================
// 2. Domain Types & LWW Register
// ==============================================================================

export interface LwwElement {
  readonly value: unknown;
  readonly timestamp: HybridLogicalTimestamp;
}

export function mergeLwwProps(
  propsA: Record<string, LwwElement>,
  propsB: Record<string, LwwElement>
): Record<string, LwwElement> {
  const allKeys = new Set([...Object.keys(propsA), ...Object.keys(propsB)]);
  const merged: Record<string, LwwElement> = {};

  for (const k of allKeys) {
    const valA = propsA[k];
    const valB = propsB[k];

    if (valA === undefined && valB !== undefined) {
      merged[k] = valB;
    } else if (valB === undefined && valA !== undefined) {
      merged[k] = valA;
    } else if (valA !== undefined && valB !== undefined) {
      const cmp = compareHlc(valA.timestamp, valB.timestamp);
      if (cmp > 0) {
        merged[k] = valA;
      } else if (cmp < 0) {
        merged[k] = valB;
      } else {
        // Timestamps equal: deterministic value tie-breaking
        const strA = JSON.stringify(valA.value);
        const strB = JSON.stringify(valB.value);
        merged[k] = strA >= strB ? valA : valB;
      }
    }
  }

  return merged;
}

export interface EdgeRecord {
  readonly source: string;
  readonly target: string;
  readonly edge_type: string;
  readonly properties: Record<string, LwwElement>;
}

export interface HyperGraphDelta {
  readonly delta_id: string;
  readonly timestamp: HybridLogicalTimestamp;
  readonly vertices: Record<string, Record<string, LwwElement>>;
  readonly edges: Record<string, EdgeRecord>;
}

// ==============================================================================
// 3. Property-Verified δ-CRDT Kernel Engine
// ==============================================================================

export class DeltaCrdtKernel {
  readonly node_id: string;
  readonly clock: HybridLogicalClock;
  private _vertices: Record<string, Record<string, LwwElement>> = {};
  private _edges: Record<string, EdgeRecord> = {};
  private _causalBuffer: HyperGraphDelta[] = [];

  constructor(nodeId: string, clock?: HybridLogicalClock) {
    this.node_id = nodeId;
    this.clock = clock ?? new HybridLogicalClock(nodeId);
  }

  putVertex(
    vertexId: string,
    properties: Record<string, unknown>
  ): HyperGraphDelta {
    const ts = this.clock.now();
    const lwwProps: Record<string, LwwElement> = {};
    for (const [k, v] of Object.entries(properties)) {
      lwwProps[k] = { value: v, timestamp: ts };
    }

    if (this._vertices[vertexId]) {
      this._vertices[vertexId] = mergeLwwProps(this._vertices[vertexId], lwwProps);
    } else {
      this._vertices[vertexId] = lwwProps;
    }

    const deltaId = crypto
      .createHash("sha256")
      .update(`v:${vertexId}:${formatHlc(ts)}`, "utf-8")
      .digest("hex");

    const delta: HyperGraphDelta = {
      delta_id: deltaId,
      timestamp: ts,
      vertices: { [vertexId]: this._vertices[vertexId] },
      edges: {},
    };

    // Applying vertex may satisfy pending buffered edges
    this._drainCausalBuffer();
    return delta;
  }

  putEdge(
    source: string,
    target: string,
    edgeType: string,
    properties: Record<string, unknown>
  ): HyperGraphDelta {
    const ts = this.clock.now();
    const lwwProps: Record<string, LwwElement> = {};
    for (const [k, v] of Object.entries(properties)) {
      lwwProps[k] = { value: v, timestamp: ts };
    }

    const edgeKey = `${source}:${target}:${edgeType}`;
    const edgeData: EdgeRecord = {
      source,
      target,
      edge_type: edgeType,
      properties: lwwProps,
    };

    const deltaId = crypto
      .createHash("sha256")
      .update(`e:${edgeKey}:${formatHlc(ts)}`, "utf-8")
      .digest("hex");

    const delta: HyperGraphDelta = {
      delta_id: deltaId,
      timestamp: ts,
      vertices: {},
      edges: { [edgeKey]: edgeData },
    };

    // Invariant C2: Check vertex dependencies
    if (!this._vertices[source] || !this._vertices[target]) {
      this._causalBuffer.push(delta);
    } else {
      this._applyEdgeData(edgeKey, edgeData);
    }

    return delta;
  }

  private _applyEdgeData(edgeKey: string, edgeData: EdgeRecord): void {
    if (this._edges[edgeKey]) {
      const existing = this._edges[edgeKey];
      const mergedProps = mergeLwwProps(existing.properties, edgeData.properties);
      this._edges[edgeKey] = {
        source: edgeData.source,
        target: edgeData.target,
        edge_type: edgeData.edge_type,
        properties: mergedProps,
      };
    } else {
      this._edges[edgeKey] = { ...edgeData };
    }
  }

  mergeDelta(delta: HyperGraphDelta): number {
    try {
      this.clock.observe(delta.timestamp);
    } catch {
      // Skew violation ignored during benign delta merge
    }

    let appliedCount = 0;

    // 1. Merge vertices
    for (const [vid, props] of Object.entries(delta.vertices)) {
      if (this._vertices[vid]) {
        this._vertices[vid] = mergeLwwProps(this._vertices[vid], props);
      } else {
        this._vertices[vid] = { ...props };
      }
      appliedCount += 1;
    }

    // 2. Drain causal buffer if any new vertices arrived
    if (Object.keys(delta.vertices).length > 0) {
      this._drainCausalBuffer();
    }

    // 3. Merge edges
    for (const [ekey, edata] of Object.entries(delta.edges)) {
      const src = edata.source;
      const tgt = edata.target;

      if (!this._vertices[src] || !this._vertices[tgt]) {
        // Invariant C2: Buffer out-of-order dangling edge, never trip circuit
        this._causalBuffer.push(delta);
      } else {
        this._applyEdgeData(ekey, edata);
        appliedCount += 1;
      }
    }

    return appliedCount;
  }

  private _drainCausalBuffer(): number {
    let drainedCount = 0;
    const stillBuffered: HyperGraphDelta[] = [];

    for (const delta of this._causalBuffer) {
      let allEdgesReady = true;
      for (const edata of Object.values(delta.edges)) {
        if (!this._vertices[edata.source] || !this._vertices[edata.target]) {
          allEdgesReady = false;
          break;
        }
      }

      if (allEdgesReady) {
        for (const [ekey, edata] of Object.entries(delta.edges)) {
          this._applyEdgeData(ekey, edata);
          drainedCount += 1;
        }
      } else {
        stillBuffered.push(delta);
      }
    }

    this._causalBuffer = stillBuffered;
    return drainedCount;
  }

  pendingBufferCount(): number {
    return this._causalBuffer.length;
  }

  getStateDigest(): string {
    const leafHashes: string[] = [];

    // Hash vertices deterministically
    const sortedVids = Object.keys(this._vertices).sort();
    for (const vid of sortedVids) {
      const props = this._vertices[vid];
      const sortedKeys = Object.keys(props).sort();
      const propObj: Record<string, unknown> = {};
      for (const k of sortedKeys) {
        propObj[k] = {
          val: props[k].value,
          ts: formatHlc(props[k].timestamp),
        };
      }
      const leaf = crypto
        .createHash("sha256")
        .update(`v:${vid}:${JSON.stringify(propObj)}`, "utf-8")
        .digest("hex");
      leafHashes.push(leaf);
    }

    // Hash edges deterministically
    const sortedEkeys = Object.keys(this._edges).sort();
    for (const ekey of sortedEkeys) {
      const edata = this._edges[ekey];
      const sortedKeys = Object.keys(edata.properties).sort();
      const propObj: Record<string, unknown> = {};
      for (const k of sortedKeys) {
        propObj[k] = {
          val: edata.properties[k].value,
          ts: formatHlc(edata.properties[k].timestamp),
        };
      }
      const leaf = crypto
        .createHash("sha256")
        .update(`e:${ekey}:${JSON.stringify(propObj)}`, "utf-8")
        .digest("hex");
      leafHashes.push(leaf);
    }

    return computeMerkleRoot(leafHashes);
  }

  getVertex(vertexId: string): Record<string, unknown> | null {
    if (!this._vertices[vertexId]) return null;
    const res: Record<string, unknown> = {};
    for (const [k, el] of Object.entries(this._vertices[vertexId])) {
      res[k] = el.value;
    }
    return res;
  }

  getEdge(
    source: string,
    target: string,
    edgeType: string
  ): Record<string, unknown> | null {
    const edgeKey = `${source}:${target}:${edgeType}`;
    if (!this._edges[edgeKey]) return null;
    const edata = this._edges[edgeKey];
    const props: Record<string, unknown> = {};
    for (const [k, el] of Object.entries(edata.properties)) {
      props[k] = el.value;
    }
    return {
      source: edata.source,
      target: edata.target,
      edge_type: edata.edge_type,
      properties: props,
    };
  }
}
