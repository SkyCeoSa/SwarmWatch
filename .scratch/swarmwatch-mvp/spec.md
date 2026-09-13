# SwarmWatch MVP Specification (spec.md)
Triage: `ready-for-agent`

## Problem Statement
Building and operating multi-agent AI systems (using LangGraph, CrewAI, AutoGen, or custom runtimes) is notoriously painful. Developers face three crippling problems:
1. **Infinite Loop Token Burning**: Autonomous agents get stuck in repetitive polite loops, circular debates, or identical tool failures, silently burning hundreds of dollars in API credits within minutes. Existing observability tools (LangSmith, Datadog) are passive loggers that record the financial disaster but do nothing to stop it.
2. **The "Hairball" Non-Observability**: Multi-agent interaction graphs are non-linear, stochastic, and opaque. Traditional APM tracing shows linear waterfalls that fail to represent causal agent-to-agent negotiation, state mutation, and collective drift.
3. **Impossibility of Step-Debugging**: When an agent fails at Step 7 of a 10-step swarm workflow, developers currently have to re-run the entire swarm from Step 1, wasting time and money while hoping the non-deterministic LLMs reproduce the exact same bug.

## Solution
SwarmWatch is a framework-agnostic, developer-first observability and control-plane platform featuring:
1. **Zero-Token Entropy Circuit Breaker**: An algorithmic evaluator computing Shannon token entropy and sliding-window cosine similarity locally in <1ms without LLM calls, providing a 2-step smart intervention (system hint injection, followed by safe halt).
2. **Time-Machine Debugger**: A state snapshotting and tool-call memoization engine that allows developers to pause execution, step back to Step K, mutate prompt/variables, and execute a divergent Git-like DAG branch without rerunning past steps.
3. **Interactive Swimlane Visual Graph**: A real-time, hardware-accelerated dashboard rendering actor swimlanes, causal message edges, live token burn, and spinning circuit breaker alerts over a bi-directional WebSocket.
4. **Zero-Docker Local Experience**: Pluggable storage defaulting to embedded SQLite in WAL mode (`pip install swarmwatch && swarmwatch ui`) with a seamless seam to ClickHouse for enterprise scale.

## User Stories

### Core Instrumentation & Developer Experience
1. As an AI engineer, I want to initialize SwarmWatch with a single call (`swarmwatch.init()`), so that telemetry is captured without modifying my system architecture.
2. As an AI engineer, I want to annotate agent functions using `@swarmwatch.agent(role="coder")`, so that their identity and model calls are tracked cleanly.
3. As an AI engineer, I want to wrap arbitrary code blocks using `with swarmwatch.span("analyze")`, so that custom logic and tool calls appear in the causal trace.
4. As an AI engineer, I want telemetry submission (`queue.put_nowait()`) to execute in under 1 microsecond, so that SwarmWatch never adds latency to my agent execution.
5. As an AI engineer, I want background flushing to occur automatically in a non-daemon worker thread, so that batched network requests do not block my main process.
6. As an AI engineer, I want an `atexit` flush guarantee, so that my trace data is preserved even if my script finishes quickly or crashes unexpectedly.

### Circuit Breaker & Safety
7. As a startup founder, I want SwarmWatch to detect infinite agent loops locally using Shannon entropy, so that I don't pay another LLM to monitor my LLMs.
8. As a developer, I want SwarmWatch to inject a `[SWARMWATCH CIRCUIT WARNING]` system hint when similarity exceeds 0.85, so that the agent has a chance to self-correct before being killed.
9. As a developer, I want SwarmWatch to throw `SwarmCircuitBreakException` if the loop persists, so that execution is safely halted and my API budget is protected.
10. As a developer, I want to select circuit breaker sensitivity presets (`strict`, `balanced`, `lenient`) or define a `CustomPolicy`, so that my coding agents aren't falsely flagged for repetitive syntax.

### Time-Machine & State Branching
11. As an AI engineer, I want SwarmWatch to snapshot agent memory and variables safely using `SafeStateSerializer`, so that I can inspect state at any historical step.
12. As a security officer, I want SwarmWatch to automatically redact API keys and secrets via regex, so that credentials are never stored in plain text.
13. As an AI engineer, I want to pause a live swarm and step back to Step K, so that I don't have to restart a 10-minute swarm from the beginning.
14. As an AI engineer, I want historical tool calls up to Step K to be served from memoized cache, so that external APIs aren't called twice during replay.
15. As an AI engineer, I want forked executions to be stored as a new `branch_id`, so that I can compare the original run and the forked run side by side.
16. As an AI engineer, I want to access mutated state via `swarmwatch.sync_state()`, so that my agent code seamlessly adopts the new variables without refactoring into generators.

### Visual Command Center & Storage
17. As a developer, I want to run `swarmwatch ui` and immediately view my swarm in the browser, so that I don't have to configure Docker, Redis, or cloud accounts.
18. As a developer, I want SQLite to run in WAL mode with normal synchronous pragma, so that high-throughput batch writes never lock the UI read queries.
19. As a developer, I want to view my agents arranged in horizontal actor swimlanes with causal arrows, so that I never suffer from force-directed graph spaghetti.
20. As a developer, I want real-time node updates pushed over WebSocket with <10ms latency, so that I can watch my agents collaborate live.
21. As an enterprise architect, I want a clean `StorageAdapter` interface, so that my team can point SwarmWatch to ClickHouse in production with zero code changes.

## Implementation Decisions

### 1. Module Structure (`packages/`)
- `packages/sdk-python/swarmwatch/`:
  - `core.py`: Public API (`init()`, `agent()`, `span()`, `sync_state()`).
  - `transport.py`: Bounded queue worker with batch flush and retry resilience.
  - `schema.py`: Pydantic `SwarmSpan`, `SwarmEvent`, and OTel mapping.
  - `circuit.py`: Shannon entropy, sliding window, and `CustomPolicy`.
  - `serializer.py`: `SafeStateSerializer`, GZIP compression, and secret redaction.
- `packages/server-ingestion/app/`:
  - `main.py`: FastAPI application serving REST endpoints and WebSocket `/ws/swarm/{session_id}`.
  - `storage.py`: `StorageAdapter` abstract base class and `SQLiteStorageAdapter` implementation.
  - `models.py`: DDL schemas for `swarms`, `spans`, `snapshots`, `circuit_events`.
- `packages/dashboard/`:
  - Fast WebGL/Canvas + HTML swimlane visualization engine rendering flat WebSocket event feeds.

### 2. Interfaces & Contracts
- **`StorageAdapter` Interface**:
  ```python
  class StorageAdapter(ABC):
      async def save_spans(self, spans: list[SwarmSpan]) -> None: ...
      async def save_snapshot(self, snapshot: StateSnapshot) -> None: ...
      async def record_circuit_event(self, event: CircuitEvent) -> None: ...
      async def get_swarm_spans(self, swarm_id: str, branch_id: str = "main") -> list[SwarmSpan]: ...
      async def get_snapshot(self, span_id: str) -> Optional[StateSnapshot]: ...
      async def list_branches(self, swarm_id: str) -> list[str]: ...
  ```

- **`CircuitBreaker` Interface**:
  ```python
  class CircuitBreaker:
      def evaluate(self, history: list[str], new_text: str) -> CircuitResult: ...
  ```

- **WebSocket Protocol**:
  - Server -> Client: `{"type": "SPAN_CREATED", "payload": {...}}`, `{"type": "CIRCUIT_TRIPPED", "payload": {...}}`
  - Client -> Server: `{"type": "CMD_PAUSE"}`, `{"type": "CMD_FORK_BRANCH", "payload": {"at_span_id": "...", "mutations": {...}}}`

## Testing Decisions
- **Highest Seam Strategy (Matt Pocock Principle)**: We test external observable behavior at the highest boundary.
- **Master E2E Test Fixture (`tests/test_lifecycle_e2e.py`)**:
  - Instantiates in-memory SQLite storage and launches test client.
  - Executes a simulated 3-agent research swarm.
  - Verifies:
    1. Spans are batched and persisted with valid causal parent IDs.
    2. Repeated dialogue triggers `SwarmCircuitBreakException` on turn 3.
    3. `step_back()` creates a new branch preserving main branch integrity.
    4. WebSocket receives correct graph delta stream.
- Unit tests only for mathematical edge cases (e.g. Shannon entropy with zero words, unicode sanitization).

## Out of Scope (For MVP)
- Distributed multi-tenant user authentication (RBAC / Okta SAML) - deferred to v1.1.
- Cloud Kubernetes Helm charts - deferred to v1.1.
- Native TypeScript SDK (Python first; TS SDK in Sprint 2).

## Further Notes
- Adheres 100% to John Ousterhout's *Philosophy of Software Design*: Deep modules hiding queueing, OTel conversions, and SQLite pragmas behind a 2-line developer interface.
