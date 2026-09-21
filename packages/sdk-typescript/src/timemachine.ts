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
}
