/**
 * SwarmWatch Time-Machine & State Branching Coordinator (TypeScript Edition).
 * Enables non-destructive Time-Machine Fork ('stepBack') and parallel branch exploration
 * with automatic secret sanitization and deterministic SHA-256 state verification.
 */

import type { StateSnapshot } from "./schema.ts";
import { SafeStateSerializer } from "./serializer.ts";
import type { StorageSink } from "./transport.ts";
import { InMemoryStorageSink } from "./transport.ts";

export interface ForkBranchResult {
  status: "forked" | "error";
  new_branch_id: string;
  target_span_id: string;
  replicated_spans_count: number;
  state: Record<string, any>;
}

export class TimeMachine {
  public sink: StorageSink;
  public serializer: SafeStateSerializer;

  constructor(sink?: StorageSink, serializer?: SafeStateSerializer) {
    this.sink = sink || new InMemoryStorageSink();
    this.serializer = serializer || new SafeStateSerializer();
  }

  /**
   * Captures a sanitized StateSnapshot with deterministic SHA-256 state hash.
   */
  public async snapshot(
    spanId: string,
    stateData: Record<string, any>,
    branchId: string = "main"
  ): Promise<StateSnapshot> {
    const snap = this.serializer.createSnapshot(spanId, stateData, branchId);
    if (this.sink.saveSnapshot) {
      await this.sink.saveSnapshot(snap);
    }
    return snap;
  }

  /**
   * Retrieves historical state snapshot associated with targetSpanId,
   * returning sanitized state data ready for resumption.
   */
  public async stepBack(
    targetSpanId: string,
    newBranchId?: string
  ): Promise<Record<string, any>> {
    let snapshot: StateSnapshot | null = null;
    if (this.sink.getSnapshot) {
      snapshot = await this.sink.getSnapshot(targetSpanId);
    }
    if (!snapshot) {
      throw new Error(`No state snapshot found for span '${targetSpanId}'`);
    }
    return snapshot.state_data;
  }

  /**
   * Forks an isolated execution branch from an antecedent turn, replicating
   * antecedent causal spans and persisting the root snapshot for the new branch.
   */
  public async forkBranch(
    swarmId: string,
    targetSpanId: string,
    newBranchId: string
  ): Promise<ForkBranchResult> {
    if (this.sink.forkBranch) {
      return await this.sink.forkBranch(swarmId, targetSpanId, newBranchId);
    }

    // Fallback: in-memory state rewind and snapshot
    const state = await this.stepBack(targetSpanId, newBranchId);
    await this.snapshot(`${targetSpanId}_fork_${newBranchId}`, state, newBranchId);
    return {
      status: "forked",
      new_branch_id: newBranchId,
      target_span_id: targetSpanId,
      replicated_spans_count: 0,
      state,
    };
  }

  /**
   * Returns chronological state snapshots for a swarm and optional branch.
   */
  public async getSnapshots(
    swarmId: string,
    branchId?: string
  ): Promise<StateSnapshot[]> {
    if (this.sink.getSnapshotsForSwarm) {
      return await this.sink.getSnapshotsForSwarm(swarmId, branchId);
    }
    return [];
  }

  /**
   * Lists all active DAG branches associated with a swarm.
   */
  public async listBranches(swarmId: string): Promise<string[]> {
    if (this.sink.listBranches) {
      return await this.sink.listBranches(swarmId);
    }
    return ["main"];
  }

  /**
   * Retrieves two state snapshots by span_id and computes their deterministic differential.
   */
  public async diffSnapshots(
    swarmId: string,
    spanIdA: string,
    spanIdB: string
  ): Promise<StateDiffReport> {
    const snapA = this.sink.getSnapshot ? await this.sink.getSnapshot(spanIdA) : null;
    const snapB = this.sink.getSnapshot ? await this.sink.getSnapshot(spanIdB) : null;

    const dataA = snapA ? snapA.state_data : {};
    const dataB = snapB ? snapB.state_data : {};
    const hashA = snapA ? snapA.state_hash : undefined;
    const hashB = snapB ? snapB.state_hash : undefined;

    return computeStateDiff(dataA, dataB, hashA, hashB, spanIdA, spanIdB);
  }
}

export interface DiffEntry {
  path: string;
  kind: "added" | "modified" | "removed" | "unchanged";
  old_value?: any;
  new_value?: any;
  is_redacted: boolean;
}

export interface DiffSummary {
  added_count: number;
  modified_count: number;
  removed_count: number;
  unchanged_count: number;
  redacted_count: number;
  total_changes: number;
}

export interface StateDiffReport {
  summary: DiffSummary;
  entries: DiffEntry[];
  hash_a?: string;
  hash_b?: string;
  span_id_a?: string;
  span_id_b?: string;
}

function isValueRedacted(val: any): boolean {
  if (typeof val === "string") {
    return val.includes("[REDACTED") || val === "[REDACTED]";
  }
  if (val && typeof val === "object") {
    if (Array.isArray(val)) {
      return val.some(isValueRedacted);
    }
    return Object.values(val).some(isValueRedacted);
  }
  return false;
}

export function computeStateDiff(
  stateA?: Record<string, any> | null,
  stateB?: Record<string, any> | null,
  hashA?: string,
  hashB?: string,
  spanIdA?: string,
  spanIdB?: string
): StateDiffReport {
  const a = stateA && typeof stateA === "object" ? stateA : {};
  const b = stateB && typeof stateB === "object" ? stateB : {};

  const entries: DiffEntry[] = [];
  const summary: DiffSummary = {
    added_count: 0,
    modified_count: 0,
    removed_count: 0,
    unchanged_count: 0,
    redacted_count: 0,
    total_changes: 0,
  };

  function diffRecursive(objA: any, objB: any, currentPath: string) {
    if (
      objA &&
      typeof objA === "object" &&
      !Array.isArray(objA) &&
      objB &&
      typeof objB === "object" &&
      !Array.isArray(objB)
    ) {
      const allKeys = Array.from(
        new Set([...Object.keys(objA), ...Object.keys(objB)])
      ).sort();

      for (const k of allKeys) {
        const keyPath = currentPath ? `${currentPath}.${k}` : k;
        if (!(k in objA)) {
          const val = objB[k];
          const redacted = isValueRedacted(val);
          entries.push({
            path: keyPath,
            kind: "added",
            old_value: undefined,
            new_value: val,
            is_redacted: redacted,
          });
          summary.added_count++;
          if (redacted) summary.redacted_count++;
        } else if (!(k in objB)) {
          const val = objA[k];
          const redacted = isValueRedacted(val);
          entries.push({
            path: keyPath,
            kind: "removed",
            old_value: val,
            new_value: undefined,
            is_redacted: redacted,
          });
          summary.removed_count++;
          if (redacted) summary.redacted_count++;
        } else {
          diffRecursive(objA[k], objB[k], keyPath);
        }
      }
    } else {
      const isEqual = JSON.stringify(objA) === JSON.stringify(objB);
      if (isEqual) {
        const redacted = isValueRedacted(objA);
        entries.push({
          path: currentPath,
          kind: "unchanged",
          old_value: objA,
          new_value: objB,
          is_redacted: redacted,
        });
        summary.unchanged_count++;
        if (redacted) summary.redacted_count++;
      } else {
        const redacted = isValueRedacted(objA) || isValueRedacted(objB);
        entries.push({
          path: currentPath,
          kind: "modified",
          old_value: objA,
          new_value: objB,
          is_redacted: redacted,
        });
        summary.modified_count++;
        if (redacted) summary.redacted_count++;
      }
    }
  }

  diffRecursive(a, b, "");
  summary.total_changes =
    summary.added_count + summary.modified_count + summary.removed_count;

  return {
    summary,
    entries,
    hash_a: hashA,
    hash_b: hashB,
    span_id_a: spanIdA,
    span_id_b: spanIdB,
  };
}

