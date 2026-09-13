# ADR 0003: Git-like DAG State Branching for Time Machine Debugging

## Status
Accepted

## Context
Debugging multi-agent systems is notoriously difficult because agent states are mutable and non-deterministic. Traditional debugging requires replaying the entire execution from turn 1, which wastes API tokens and triggers duplicate external side-effects (e.g. sending webhooks or mutating databases).

## Decision
We implement non-destructive Git-like state branching:
- Every call to `sync_state()` serializes state through `SafeSerializer` (redacting secrets like `sk-...` and compressing via GZIP).
- Snapshots are indexed by `(span_id, branch_id)`.
- The `fork_branch(source_branch, target_branch, at_span_id)` operation clones the DAG state tree at the designated span, allowing developers to branch into `fork-01` and experiment with altered prompts without modifying the canonical `main` history.

## Consequences
- **Positive**: Instant time-machine rewinds without token re-execution or side-effect duplication.
- **Positive**: Full auditability of swarm state progression over time.
- **Negative**: GZIP compression and storage incur small disk overhead (~1-2KB per snapshot).
