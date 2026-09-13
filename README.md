# 👁️ SwarmWatch

**Universal Observability, Entropy Circuit Breaker & Time-Machine Debugging for Multi-Agent AI Systems.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10%2B-brightgreen.svg)](https://python.org)
[![Tests: 100% Passed](https://img.shields.io/badge/Tests-19%2F19%20Passed-success.svg)](https://github.com/SkyCeoSa/SwarmWatch)
[![Latency: Sub-Microsecond](https://img.shields.io/badge/Telemetry%20Overhead-%3C1%C2%B5s-orange.svg)](https://github.com/SkyCeoSa/SwarmWatch)

SwarmWatch provides mission-critical telemetry, infinite loop prevention, and non-destructive Git-like DAG state branching for multi-agent frameworks (**LangGraph**, **CrewAI**, **AutoGen**, **Google Antigravity**, and **Custom Agent Loops**).

---

## ⚡ Why SwarmWatch?

As multi-agent systems grow, they fail in unpredictable ways:
1. **The Telemetry Latency Trap**: Sending network telemetry inline adds 150–400ms to every agent turn. *SwarmWatch uses an in-process bounded queue with a daemon worker thread (<1µs submission overhead).*
2. **The LLM-Judge Bankruptcy Trap**: Calling GPT-4 to judge if agents are stuck burns thousands of tokens. *SwarmWatch computes Shannon Entropy and token similarity in <1ms with zero LLM calls.*
3. **The Hallucinated Time Machine**: Naive rewind systems replay past actions, re-triggering expensive LLM tokens and API side-effects. *SwarmWatch snapshots state in compressed memory, enabling instant branching (`fork-1`, `fork-2`) without re-execution.*

---

## 📦 Key Capabilities

- **Zero-Token Entropy Circuit Breaker**: Mathematical loop detector utilizing Shannon entropy \(H(X)\) and sliding-window lexical similarity. Implements a 2-step intervention:
  1. *Step 1*: Injects a `[SWARMWATCH CIRCUIT WARNING]` hint into agent context.
  2. *Step 2*: Safely halts execution with `SwarmCircuitBreakException` before token budget explodes.
- **Time-Machine State Branching**: Snapshots agent memory with automatic secret masking (`sk-...`, JWTs, passwords) and GZIP compression. Rewind to any historical span and fork new branches without corrupting the `main` execution branch.
- **In-Process Bounded Queue**: Spans are emitted in <1 microsecond via non-blocking queues, flushed asynchronously in batches (100 spans or 500ms), and drained cleanly on process exit with `atexit`.
- **Embedded Real-time Dashboard**: Interactive 60fps HTML5 Canvas swimlane visualizer, live WebSocket control plane (`CMD_PAUSE`, `CMD_RESUME`, `CMD_FORK_BRANCH`), and instant inspector.
- **Universal Storage Adapter**: Embedded SQLite with WAL mode (`PRAGMA journal_mode=WAL`) for local zero-Docker usage, pluggable into ClickHouse for enterprise scale.

---

## 🚀 Quickstart

### 1. Installation

```bash
git clone https://github.com/SkyCeoSa/SwarmWatch.git
cd SwarmWatch
pip install -e packages/sdk-python
```

### 2. Instrument Your Agents in < 5 Lines of Code

```python
import swarmwatch

# Initialize SwarmWatch
swarmwatch.init(swarm_id="my-swarm")

@swarmwatch.agent(name="Researcher", role="analyst", target="Writer")
def conduct_research(topic: str):
    # Snapshot agent state into the Time Machine
    swarmwatch.sync_state({"findings": ["Item A", "Item B"], "confidence": 0.98})
    return {"summary": f"Research on {topic} complete"}

# Execute inside a span
with swarmwatch.span("research_phase", source="Coordinator", target="Researcher"):
    result = conduct_research("Autonomous Agents")
```

### 3. Launch Live Visualizer & Time Machine

```bash
swarmwatch ui
```

Opens the real-time visualizer at `http://127.0.0.1:8765`:
- **Swimlane Tracks**: Visualizes message flows between agents.
- **Entropy Gauge**: Live health index indicating whether agents are operating productively or repeating themselves.
- **Time-Machine Rewind**: Click any historical span to inspect state and fork a new branch (`fork-01`).

---

## 🧪 Testing

SwarmWatch is built strictly according to Test-Driven Development (TDD) best practices. Run the complete test suite:

```bash
pytest packages/sdk-python/tests/ -v
```

```text
============================= test session starts =============================
packages/sdk-python/tests/test_circuit_breaker.py::test_shannon_entropy_calculation PASSED
packages/sdk-python/tests/test_circuit_breaker.py::test_lexical_similarity PASSED
packages/sdk-python/tests/test_circuit_breaker.py::test_circuit_breaker_sub_millisecond_eval PASSED
packages/sdk-python/tests/test_circuit_breaker.py::test_two_step_intervention_warning_then_trip PASSED
packages/sdk-python/tests/test_circuit_breaker.py::test_recovery_resets_warning PASSED
packages/sdk-python/tests/test_developer_surface.py::test_init_and_sync_agent_decorator PASSED
packages/sdk-python/tests/test_developer_surface.py::test_async_agent_decorator PASSED
packages/sdk-python/tests/test_developer_surface.py::test_agent_decorator_exception_handling PASSED
packages/sdk-python/tests/test_developer_surface.py::test_nested_span_context_managers PASSED
packages/sdk-python/tests/test_safe_serializer_time_machine.py::test_safe_serializer_handles_non_serializable_and_redacts_keys PASSED
packages/sdk-python/tests/test_safe_serializer_time_machine.py::test_time_machine_step_back_and_branch_forking PASSED
packages/sdk-python/tests/test_safe_serializer_time_machine.py::test_swarmwatch_sync_state_integration PASSED
packages/sdk-python/tests/test_server_and_dashboard.py::test_server_health_and_spans_rest_api PASSED
packages/sdk-python/tests/test_server_and_dashboard.py::test_server_dashboard_ui_endpoint PASSED
packages/sdk-python/tests/test_server_and_dashboard.py::test_websocket_broadcast_and_command_handling PASSED
packages/sdk-python/tests/test_storage_lifecycle.py::test_sqlite_storage_adapter_lifecycle_and_branch_isolation PASSED
packages/sdk-python/tests/test_transport_queue.py::test_transport_non_blocking_enqueue PASSED
packages/sdk-python/tests/test_transport_queue.py::test_transport_bounded_queue_drop PASSED
packages/sdk-python/tests/test_transport_queue.py::test_transport_with_real_sqlite_storage PASSED

============================= 19 passed in 4.05s ==============================
```

---

## 🏛️ Architecture & Vertical Slices

```
SwarmWatch/
├── packages/
│   ├── sdk-python/
│   │   ├── swarmwatch/
│   │   │   ├── schema.py        # Canonical SwarmSpan, StateSnapshot & CircuitEvent models
│   │   │   ├── storage.py       # Pluggable StorageAdapter & SQLite-WAL zero-docker engine
│   │   │   ├── transport.py     # Sub-microsecond bounded queue & async batch worker
│   │   │   ├── core.py          # @swarmwatch.agent & with swarmwatch.span(...) context
│   │   │   ├── circuit.py       # Zero-Token Shannon Entropy Circuit Breaker
│   │   │   ├── serializer.py    # Safe state serializer & Time-Machine DAG branching
│   │   │   ├── server.py        # FastAPI REST ingestion, WebSocket control & 60fps UI
│   │   │   └── cli.py           # CLI entrypoint: `swarmwatch ui`
│   │   └── tests/               # Comprehensive 19-test suite (100% green)
│   └── dashboard/               # Visualizer assets and swimlane canvas components
└── examples/
    └── multi_agent_demo.py      # End-to-end 4-agent financial simulation
```

---

## 📄 License

MIT License. Designed and crafted with mathematical precision.
