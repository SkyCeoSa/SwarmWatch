# SwarmWatch Architecture & Deep Module Design

This document details the architectural seams, deep module boundaries, and system invariants of SwarmWatch following Matt Pocock's `codebase-design` principles.

---

## 1. Deep Module Philosophy

In accordance with John Ousterhout's *Philosophy of Software Design*, SwarmWatch prioritizes **deep modules**: narrow, highly leveraged interfaces that hide complex internal implementations. Callers interact with minimal primitives while the underlying engine handles thread scheduling, mathematical vectorization, WAL serialization, and WebSocket broadcasting.

```
┌────────────────────────────────────────────────────────┐
│ Small Interface: @swarmwatch.agent(), span(), sync()    │
├────────────────────────────────────────────────────────┤
│ Deep Implementation:                                   │
│  - Lock-free bounded ring buffer                       │
│  - Zero-token Shannon entropy calculation (<0.5ms)     │
│  - Thread-local SQLite WAL connection pooling          │
│  - GZIP compression & regex-based secret sanitization  │
│  - Real-time WebSocket broadcasting & SSE fallback     │
└────────────────────────────────────────────────────────┘
```

---

## 2. Defined Seams

A **seam** is a place where behavior can be altered or inspected without editing the calling site.

### 1. Telemetry Ingestion Seam
- **Interface**: `SwarmClient.span(...)`, `@swarmwatch.agent()`, `SwarmClient.sync_state(...)`.
- **Behind the Seam**: In-process non-blocking queue (`queue.Queue`), atomic monotonic sequence generator, sentinel-driven drain worker thread.
- **Guarantee**: Sub-microsecond return on caller threads (<1μs). Zero network I/O during agent reasoning.

### 2. Storage Adapter Seam
- **Interface**: `StorageAdapter` abstract base class (`save_spans`, `get_swarm_spans`, `save_snapshot`, `get_snapshot`, `list_branches`).
- **Implementations**:
  - `SQLiteStorageAdapter`: Zero-docker local embedded storage running `PRAGMA journal_mode=WAL` with thread-local connection caches.
  - `ClickHouseStorageAdapter`: Scalable columnar storage for team and multi-tenant cloud ingestion.

### 3. Entropy Circuit Breaker Seam
- **Interface**: `EntropyCircuitBreaker.evaluate(message, agent_id, swarm_id) -> CircuitDecision`.
- **Behind the Seam**: Normalized Shannon entropy (H_norm) and sliding-window Jaccard overlap computed without LLM API calls.
- **Policies**: Configurable thresholds via `CircuitPolicy` presets (`STRICT`, `BALANCED`, `LENIENT`).

### 4. State Time-Machine Seam
- **Interface**: `TimeMachine.snapshot(...)` and `TimeMachine.step_back(...)`.
- **Behind the Seam**: `SafeStateSerializer` cycle-breaking, recursive secret masking (`sk-...`, Bearer tokens), GZIP compression, and SHA-256 state hashing.

### 5. Ecosystem Adapter Seam
- **Interface**: `SwarmWatchCallbackHandler` (LangGraph/LangChain) and `SwarmWatchCrewListener` (CrewAI).
- **Behind the Seam**: Zero monkey-patching event translation using public callback hooks.

### 6. Real-Time Observability Seam
- **Interface**: FastAPI HTTP ingestion (`POST /api/v1/spans`, `POST /api/v1/simulate`) and WebSocket stream (`/ws/swarm/{swarm_id}`).
- **Behind the Seam**: Broadcaster managing active connection registries per swarm session, streaming span updates with controlled pacing.

### 7. Package Distribution Integrity Seam
- **Interface**: `PackageReleaseValidator.validate_wheel(wheel_path) -> ArtifactIntegrityReport`, `validate_tarball(tarball_path) -> ArtifactIntegrityReport`, and `validate_distribution(...) -> FullDistributionReport`.
- **Implementation**: `packages/sdk-python/swarmwatch/release_validator.py`.
- **Depth**: High depth / narrow interface. Behind a single call, the module transparently executes archive decompression, ZIP central directory CRC inspection, POSIX ustar stream parsing, streaming SHA-256 calculation, PEP 566 email-style metadata parsing, JSON package.json schema validation, console asset integrity verification, and regex-based secret/forbidden-file auditing.
- **Adapter**: CLI runner in `scripts/verify_release.py` adapts the deep module to interactive developer terminal tables and JSON CI/CD outputs.
- **Leverage**: High leverage — converts 8 complex physical file inspections into a single deterministic integrity report.
- **Locality**: 100% zero external dependencies; isolated from agent runtime imports to allow execution in clean packaging environments without virtualenv activation.

---


## 3. Core System Invariants

1. **Zero Agent Latency Penalty**: Agent execution loops must never block on disk or network telemetry flushes.
2. **Deterministic Process Termination**: On process exit (`atexit` or `shutdown()`), queue workers must flush all pending spans and snapshots using sentinel protocols before closing connections.
3. **Thread Safety**: SQLite connections are strictly thread-local; connections are never shared across OS threads.
4. **Permanent Secret Masking**: Payloads matching sensitive token patterns are irreversibly redacted before persistence.
