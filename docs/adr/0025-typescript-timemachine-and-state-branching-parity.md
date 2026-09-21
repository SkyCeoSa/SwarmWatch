# ADR 0025: TypeScript TimeMachine & State Branching Parity (Seam 14 TS Parity)

## Status
Accepted

## Context
In ADR 0003 and ADR 0024, SwarmWatch established Git-like Directed Acyclic Graph (DAG) state branching, non-destructive state snapshots (`StateSnapshot`), and interactive developer console controls (`dashboard.html`) for the Python core engine.

However, production applications built on modern JavaScript/TypeScript stacks (Next.js 15, Vercel AI SDK, LangChain.js, Node.js microservices) required identical client-side time-machine primitives:
1. Programmatic state snapshotting (`TimeMachine.snapshot`, `client.snapshot`) capturing sanitized agent memory with deterministic SHA-256 state hashes.
2. Non-destructive historical rewind (`TimeMachine.stepBack`, `client.stepBack`) returning clean state objects from antecedent turns.
3. In-process and remote branch forking (`TimeMachine.forkBranch`, `client.forkBranch`) replicating antecedent causal spans up to the target turn and writing root branch snapshots.
4. Dual-sink support: Seamless execution in local offline environments (`InMemoryStorageSink`) and enterprise networked setups (`HttpStorageSink` communicating with `/api/v1/swarms/{id}/snapshots` and `/api/v1/branches/fork`).
5. Zero third-party dependency footprint in Node.js 22+ ESM, maintaining sub-millisecond execution.

## Decision
1. **TypeScript TimeMachine Deep Module (`timemachine.ts`)**:
   - Implemented `TimeMachine` class encapsulating `SafeStateSerializer`, `SensitiveDataMasker`, and `StorageSink`.
   - Exposed methods:
     - `snapshot(spanId, stateData, branchId?)`: Produces sanitized `StateSnapshot` and persists via storage sink.
     - `stepBack(targetSpanId, newBranchId?)`: Validates and extracts antecedent state.
     - `forkBranch(swarmId, targetSpanId, newBranchId)`: Orchestrates antecedent span replication, records new branch root snapshot, and returns structured `ForkBranchResult`.
     - `getSnapshots(swarmId, branchId?)`: Queries chronological snapshots.
     - `listBranches(swarmId)`: Discovers unique branches across spans and snapshots.

2. **Storage Sink Protocol Extensions (`transport.ts`)**:
   - Extended `StorageSink` interface with `getSnapshot`, `getSnapshotsForSwarm`, `forkBranch`, and `listBranches`.
   - Implemented in-memory DAG branch replication and snapshot tracking in `InMemoryStorageSink`.
   - Implemented REST API integration in `HttpStorageSink` connecting to `/api/v1/snapshots`, `/api/v1/swarms/{id}/snapshots`, `/api/v1/swarms/{id}/branches`, and `/api/v1/branches/fork`.

3. **SwarmClient Integration (`client.ts`)**:
   - Exposed `client.timeMachine` instance on `SwarmClient`.
   - Provided high-level contextual helpers (`snapshot`, `stepBack`, `forkBranch`, `listBranches`, `listSnapshots`) automatically linking active async span context from `AsyncLocalStorage`.

## Consequences
- **Positive**:
  - Full functional and architectural parity between Python and TypeScript SDKs.
  - Node.js and Next.js developers can rewind agent state and explore counterfactual execution branches without switching runtimes.
  - Zero external dependencies; 100% standard library Node.js implementation.
  - Test suite coverage expanded to 99 tests in TypeScript and 218 total automated tests across the monorepo.
- **Negative**:
  - In-memory antecedent span replication duplicates spans in the local array to ensure branch isolation.
