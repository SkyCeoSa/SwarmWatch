# 05. System Architecture & Seams

This document details the architectural topology, defined seams, and runtime dataflow of SwarmWatch.

---

## 1. System Topology Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Multi-Agent Application Space                        │
│                                                                        │
│   LangGraph Graph           CrewAI Crew          Custom Python Agents  │
│          │                       │                         │           │
│   SwarmWatchCallback    SwarmWatchListener        @swarmwatch.agent    │
└──────────┬───────────────────────┬─────────────────────────┬───────────┘
           │                       │                         │
           ▼                       ▼                         ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        SwarmWatch Client Core                          │
│                                                                        │
│   [Seam 1] Telemetry Ingestion (<1µs non-blocking queue.Queue)         │
│   [Seam 2] Mathematical Circuit Breaker (<0.5ms Shannon Entropy)       │
│   [Seam 3] State Time-Machine (Cycle detection, Redaction, GZIP)       │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ Background Worker Thread
┌────────────────────────────────────────────────────────────────────────┐
│                   Storage & Control Plane Tier                         │
│                                                                        │
│   [Seam 4] Storage Adapter Interface                                   │
│            ├── SQLiteStorageAdapter (WAL Mode, Thread-Local Pools)     │
│            └── ClickHouseStorageAdapter (Columnar Enterprise Scale)    │
│                                                                        │
│   [Seam 5] Real-Time FastAPI Server & WebSocket Manager                │
│            ├── REST Ingestion: POST /api/v1/spans, POST /api/v1/simulate│
│            ├── State Forks: POST /api/v1/branches/fork                 │
│            └── WebSocket Streaming: /ws/swarm/{swarm_id}               │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ WebSocket Stream
┌────────────────────────────────────────────────────────────────────────┐
│               [Seam 6] Impeccable Developer Console                    │
│   - Live Execution Stream (350ms Cadence)                              │
│   - Real-Time Shannon Entropy & Metric Cards                           │
│   - Time-Machine Turn Inspector & State Forking                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Defined Seams Description

1. **Telemetry Ingestion Seam**:
   - Primitives: `SwarmClient.span(...)`, `@swarmwatch.agent()`, `SwarmClient.sync_state(...)`.
   - Invariant: Zero synchronous disk or network I/O on caller threads.
2. **Entropy Circuit Breaker Seam**:
   - Primitives: `EntropyCircuitBreaker.evaluate(message, agent_id, swarm_id) -> CircuitDecision`.
   - Invariant: Zero LLM API calls, zero token burn.
3. **State Time-Machine Seam**:
   - Primitives: `TimeMachine.snapshot(...)`, `TimeMachine.step_back(...)`.
   - Invariant: Secret redaction and cycle-breaking before serialization.
4. **Storage Adapter Seam**:
   - Primitives: `StorageAdapter` abstract base class.
   - Invariant: Thread safety; SQLite connections never shared across threads.
5. **Ecosystem Adapter Seam**:
   - Primitives: `SwarmWatchCallbackHandler` and `SwarmWatchCrewListener`.
   - Invariant: Public official interfaces only; zero monkey-patching.
6. **Real-Time Observability Seam**:
   - Primitives: `create_app(storage)` FastAPI factory and `WebSocketManager`.
   - Invariant: Graceful disconnection handling and fallback replay.
