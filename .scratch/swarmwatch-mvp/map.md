# Wayfinder Map: SwarmWatch MVP

**Destination**: A battle-tested, modular, agent-codable SwarmWatch MVP.

## Notes & Principles
- Plan, don't do: The design tree is completely exhausted. All decisions locked.
- Deep Modules & thin interfaces (John Ousterhout Philosophy of Software Design).
- Test-first (TDD) on the highest architectural seam.
- Zero-token local entropy circuit breaker, zero-docker SQLite WAL engine.

## Decisions So Far (All 16 Decisions Locked)
- [x] **01: SDK Hooking Strategy**: Hybrid OTel GenAI conventions + MCP interceptor. Framework-agnostic.
- [x] **02: Storage Engine**: Pluggable `StorageAdapter`. SQLite-WAL for zero-docker local DX; ClickHouse for scale.
- [x] **03: Circuit Breaker**: Local Shannon entropy + token distance sliding window. Zero token burn, <1ms.
- [x] **04: Time-Machine Replay**: State snapshotting & tool memoization. Causal branch isolation.
- [x] **05: Telemetry Transport**: In-process bounded queue + background thread batch flush (<1 microsecond non-blocking).
- [x] **06: Event Schema**: Typed Pydantic `SwarmSpan` with multi-agent causal fields + `.to_otel_span()`.
- [x] **07: Intervention Policy**: 2-step escalation: System hint injection -> Safe halt with `SwarmCircuitBreakException`.
- [x] **08: UI Control Plane**: Bi-directional WebSocket (`/ws/swarm/{session_id}`) for streaming and Time-Machine commands.
- [x] **09: SDK Developer Surface**: Clean `@swarmwatch.agent` decorator + `with swarmwatch.span()` context manager.
- [x] **10: State Serialization**: `SafeStateSerializer` with 2MB cap, GZIP compression, and regex API key masking.
- [x] **11: Execution Branching**: Git-like DAG execution tree (`branch_id`, A/B scenario comparison).
- [x] **12: Visual Graph Topology**: Hierarchical Swimlanes with Causal Edges, WebGL-ready flat data feed.
- [x] **13: Breaker Tuning**: Presets (`strict`, `balanced`, `lenient`) + typed `CustomPolicy`.
- [x] **14: State Re-injection**: Zero-friction `swarmwatch.sync_state(default=...)` hook.
- [x] **15: SQLite DDL & Indexes**: 4 normalized tables with WAL mode, compound indexes, foreign keys enabled.
- [x] **16: TDD Highest Seam**: E2E integration fixture testing full observable lifecycle.

## Frontier
*(Empty! All 16 decision branches settled. Ready for `/to-spec` and execution tickets).*
