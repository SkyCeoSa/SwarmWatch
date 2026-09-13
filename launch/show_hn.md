# Show HN: SwarmWatch – Zero-Token Observability & Time Machine for Multi-Agent AI

**Link**: https://github.com/SkyCeoSa/SwarmWatch

Hey HN,

Over the past year building autonomous multi-agent pipelines (financial research, code refactoring, automated auditing), we kept hitting three brutal realities that caused our agent swarms to fail in production or burn our API keys overnight:

1. **The Telemetry Latency Trap**: Most observability tools were designed for web microservices. Sending JSON spans over HTTP inline at every agent thought or tool turn adds 150–400ms. In a 5-agent swarm doing 30 conversational rounds, you're waiting 10+ seconds just for your monitoring SDK to flush network packets.
2. **The LLM-Judge Bankruptcy Trap**: When agents get stuck in conversational loops (e.g. arguing about a SQL query syntax or calling the same failing tool 12 times), the common industry recommendation is to "use an LLM evaluator / guardrail". Calling GPT-4 to monitor an agent loop is insane: it costs real money, adds 2 seconds of latency, and burns thousands of tokens while your agent is already failing.
3. **The Hallucinated Time Machine Trap**: When an agent swarm goes off the rails on step 14 of 20, debugging usually means reading through 10,000 lines of messy console logs, tweaking a prompt, and re-running the entire swarm from step 1—burning fresh tokens and re-triggering external APIs (sending emails, modifying databases).

We built **SwarmWatch** to solve these problems from first principles:

### How it works:

- **Sub-Microsecond In-Process Queue**: Spans are pushed to an in-memory thread-safe bounded buffer in < 1 microsecond. An asynchronous daemon worker thread batches flushes to local SQLite in WAL mode (`PRAGMA journal_mode=WAL`). Your agent execution runs at bare-metal speed without waiting on external HTTP roundtrips.
- **Zero-Token Shannon Entropy Circuit Breaker**: Instead of calling an LLM judge, SwarmWatch computes normalized Shannon entropy $H_{norm}(X)$ and sliding-window token Jaccard similarity across recent turns in under 0.5ms:
  $$H(X) = -\sum_{i=1}^{N} p(x_i) \log_2 p(x_i), \quad H_{norm} = \frac{H(X)}{\log_2 N}$$
  When an agent begins repeating reasoning patterns or enters a low-entropy stagnation loop:
  - *Intervention 1 (Warning)*: Injects an explicit `[SWARMWATCH CIRCUIT WARNING]` hint into the agent's prompt context to self-correct.
  - *Intervention 2 (Hard Halt)*: If the loop persists, raises a `SwarmCircuitBreakException` before your token budget is burned.
- **Git-like State Time Machine**: Agent memory is snapshot using safe serialization (automatic masking of `sk-...` API keys and bearer tokens) and GZIP compression. If a 4-agent swarm takes a bad turn at step 10, you can rewind state and fork a new branch (`fork-01`) without re-running steps 1-9.
- **Amir-Minimal 60fps Canvas Dashboard**: Run `swarmwatch ui` to launch an ultra-fast HTML5 canvas visualizer showing live agent swimlanes, glowing bezier particle flows, real-time entropy health, and instant state inspectors.

### Zero-Friction Adapters:

We built native adapters for the most popular frameworks:
- **LangGraph / LangChain**: Plug in `SwarmWatchCallbackHandler()` or wrap with `patch_graph(graph)`.
- **CrewAI**: Pass `listener = SwarmWatchCrewListener()` as `step_callback` and `task_callback`.
- **Custom Agent Loops / Plain Python**: Just wrap functions with `@swarmwatch.agent()` and `with swarmwatch.span()`.

### Code Example:
```python
import swarmwatch
from swarmwatch.adapters.langgraph import SwarmWatchCallbackHandler

swarmwatch.init(swarm_id="production-swarm")
handler = SwarmWatchCallbackHandler()

# Runs with zero telemetry latency & circuit-breaker protection
result = app.invoke({"task": "Analyze SEC filings"}, config={"callbacks": [handler]})
```

The repo is fully open-source under MIT, comes with 24/24 passing unit & integration tests, and runs with zero external Docker dependencies.

We’d love to hear your feedback, bug reports, and ideas!

**GitHub**: https://github.com/SkyCeoSa/SwarmWatch
