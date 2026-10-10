# 📖 SwarmWatch Real-World Production Cookbook & Architecture Scenarios

This cookbook demonstrates **concrete, production-ready integration patterns** for SwarmWatch across mission-critical systems. Whether safeguarding automated financial execution desks, orchestrating multi-agent software engineering swarms in LangGraph, or streaming telemetry from Vercel AI SDK on Node.js / Edge runtimes, SwarmWatch provides mathematical fail-safes with zero external token overhead.

---

## ⚡ The 3-Minute Live Verification

You can verify SwarmWatch's real-world circuit breaker in your terminal in 10 seconds:

```bash
# 1. Install SwarmWatch
pip install swarmwatch

# 2. Run the included multi-framework live execution demo
python examples/langgraph_crewai_demo.py
```

### What You See in the Terminal:
```text
=================================================================
  👁️ SwarmWatch Multi-Framework Ecosystem Demonstration
=================================================================

 1. LangGraph / LangChain Swarm Integration
-----------------------------------------------------------------
[LangGraph] Starting chain execution with SwarmWatchCallbackHandler...
[LangGraph] Tracing LLM generation step (Claude-3.5-Sonnet)...
[LangGraph] Tracing tool step (AST_Linter_Engine)...
[LangGraph] Chain completed and traced successfully into causal DAG.

 2. CrewAI Multi-Agent Swarm with Circuit-Breaker Protection
-----------------------------------------------------------------
[CrewAI] SeniorArchitect executing initial migration audit...
[CrewAI] Simulating stalled agent caught in repetitive retry loop...
  -> Agent loop attempt #1: 'I need to query the migration table again...'
  -> Agent loop attempt #2: 'I need to query the migration table again...'
  -> Agent loop attempt #3: 'I need to query the migration table again...'

 [CIRCUIT BREAKER TRIGGERED!]
   Reason: High lexical repetition detected (1.00 >= 0.70)
   Action: Terminated runaway loop immediately. Zero LLM tokens wasted!

=================================================================
>> All traces & state snapshots stored in SQLite WAL ('swarmwatch.db').
>> Launch the visual dashboard to inspect live swimlanes:
   $ swarmwatch ui --port 8765
=================================================================
```

---

## 🏢 Scenario 1: Financial Desks & Algorithmic Trading Systems

### The Problem:
In automated trading and family office operations, autonomous LLM swarms synthesize market signals, calculate position sizing, and send orders to execution engines (e.g., MetaTrader 5, FIX Protocol, Interactive Brokers). If an agent encounters an edge case (e.g., unusual market volatility, rejected order due to margin limits), it frequently enters a **hallucinatory retry loop**, repeating the same order payload or spinning until capital is exhausted.

### The SwarmWatch Solution:
Wrap the agent pipeline with `EntropyCircuitBreaker`. In-process Shannon entropy ($H_{norm}$) and lexical distance are computed in **<250µs**. If entropy collapses or repetition spikes, the circuit trips immediately, raising `SwarmCircuitBreakException` and halting execution **before malformed trades reach the broker**.

```python
import os
import swarmwatch
from swarmwatch.circuit import EntropyCircuitBreaker, CircuitPreset, SwarmCircuitBreakException

# 1. Initialize SwarmWatch client with tenant credentials
swarmwatch.init(
    swarm_id="quant-execution-desk-01",
    project_id="hedge-fund-alpha",
    api_key=os.getenv("SWARMWATCH_API_KEY", "sw_live_alpha_trade_key")
)

# 2. Configure strict mathematical circuit breaker for financial risk
circuit = EntropyCircuitBreaker(
    preset=CircuitPreset.PRODUCTION,
    max_turns=5,                   # Max 5 iterative agent turns per trade cycle
    entropy_threshold=0.35,        # Trip if response entropy collapses <= 0.35 bits
    repetition_ceiling=0.70,       # Trip if n-gram token overlap exceeds 70%
    token_budget=50_000,           # Trip if session consumes > 50,000 tokens
)

def execute_trading_cycle(market_signal: dict):
    agent_history = []
    
    with swarmwatch.span("market_synthesis", source="MarketDataAgent", target="RiskAgent"):
        print(f"[RiskAgent] Processing signal for {market_signal['symbol']}...")
        
        for turn in range(1, 10):
            # Simulated agent response (in production: call your LLM or prompt)
            agent_output = market_signal.get("simulated_stall_text", "BUY 1.5 BTC at limit 64200")
            agent_history.append(agent_output)
            
            # Record state snapshot with automatic secret and credential redaction
            swarmwatch.sync_state({
                "turn": turn,
                "symbol": market_signal["symbol"],
                "account_id": "ACC-992182",
                "risk_score": 0.82
            })
            
            # Step the circuit breaker: computes Shannon entropy in-process in <250µs
            result = circuit.step(
                text=agent_output,
                tokens=450,
                cost=0.009
            )
            
            if not result.allowed:
                # IMMEDIATE FAIL-SAFE INTERVENTION
                print(f"[SHUTDOWN] Circuit tripped at turn {turn}! Reason: {result.reason}")
                raise SwarmCircuitBreakException(f"Trading halted: {result.reason}")
                
            print(f"Turn {turn} verified nominal (Entropy: {result.entropy:.2f} bits)")
            
# Run nominal trade
execute_trading_cycle({"symbol": "BTCUSD", "simulated_stall_text": "Signal valid, spread 0.02%"})
```

---

## 🛠️ Scenario 2: Autonomous Software Engineering (LangGraph Supervisor)

### The Problem:
A multi-agent team (ProductArchitect, BackendEngineer, CodeReviewer, SecurityAuditor) refactors a microservice. The CodeReviewer rejects a PR, and the BackendEngineer attempts to fix it. Without circuit breakers, the two agents can exchange polite conversational pleasantries or endlessly re-propose identical invalid patches, burning hundreds of dollars in API credits overnight.

### The SwarmWatch Solution:
Attach `SwarmWatchCallbackHandler` directly to the LangGraph execution config. Telemetry streams asynchronously to your local dashboard, and state snapshots enable **Time-Machine DAG branching**: if an agent fails, step back to turn 3 and resume down an alternative prompt branch without re-executing previous turns.

```python
import swarmwatch
from swarmwatch.adapters.langgraph import SwarmWatchCallbackHandler
from langgraph.graph import StateGraph, END
from typing import TypedDict

class AgentState(TypedDict):
    task: str
    code: str
    review_status: str
    iterations: int

# 1. Initialize SwarmWatch
swarmwatch.init(swarm_id="langgraph-engineering-swarm")
handler = SwarmWatchCallbackHandler()

# 2. Define standard LangGraph nodes
def architect_node(state: AgentState):
    return {"code": "def connect_pool(): return asyncpg.create_pool()", "iterations": state["iterations"] + 1}

def reviewer_node(state: AgentState):
    # If stalled in identical rejection loops, SwarmWatch intercepts automatically
    return {"review_status": "APPROVED"}

# 3. Build graph
workflow = StateGraph(AgentState)
workflow.add_node("architect", architect_node)
workflow.add_node("reviewer", reviewer_node)
workflow.set_entry_point("architect")
workflow.add_edge("architect", "reviewer")
workflow.add_edge("reviewer", END)

app = workflow.compile()

# 4. Invoke with SwarmWatch monitoring enabled
final_state = app.invoke(
    {"task": "Implement connection pool", "code": "", "review_status": "PENDING", "iterations": 0},
    config={"callbacks": [handler]}
)
print("Graph execution complete:", final_state["review_status"])
```

---

## 🌐 Scenario 3: Full-Stack Web & Next.js with Vercel AI SDK (TypeScript)

### The Problem:
Serverless and Edge environments cannot block HTTP threads to report telemetry without increasing Time-to-First-Byte (TTFB) and customer-facing response latency.

### The SwarmWatch Solution:
The TypeScript SDK (`@swarmwatch/sdk`) uses an **in-process non-blocking ring buffer** with native Node.js 22+ type-stripping and `AsyncLocalStorage` for parent-child trace lineage. Spans are batched on background micro-tasks with sub-microsecond overhead.

```typescript
import { generateText } from 'ai';
import { openai } from '@ai-sdk/openai';
import { SwarmClient, swarmWatchVercelOnFinish } from '@swarmwatch/sdk';

// 1. Initialize client
const client = new SwarmClient({
  swarmId: 'nextjs-customer-support-swarm',
  projectId: 'prod-saas-01',
  apiKey: process.env.SWARMWATCH_API_KEY,
  serverUrl: process.env.SWARMWATCH_SERVER_URL || 'https://swarmwatch.onrender.com'
});

// 2. Wrap generateText or streamText with SwarmWatch hook
export async function handleUserQuery(prompt: string) {
  const result = await generateText({
    model: openai('gpt-4o'),
    prompt,
    onFinish: swarmWatchVercelOnFinish(client, {
      agentName: 'SupportRouterAgent',
      targetAgent: 'BillingSpecialistAgent',
    }),
  });

  return result.text;
}
```

---

## 🖥️ Scenario 4: Self-Hosted Production Deployment

SwarmWatch requires **zero cloud dependencies** and runs as a single self-contained container with embedded SQLite WAL storage and an austere developer console:

### Docker Deployment (1-Command):
```bash
docker run -d \
  --name swarmwatch \
  -p 8765:8765 \
  -v swarmwatch_data:/data \
  ghcr.io/skyceosa/swarmwatch:latest
```

### Accessing the System:
* **Interactive Developer Console**: `http://localhost:8765`
* **Health Endpoint**: `http://localhost:8765/api/v1/health`
* **OTLP Traces Ingestion**: `http://localhost:8765/v1/traces`
* **Telemetry Spans Ingestion**: `http://localhost:8765/api/v1/spans`

---

## 📊 Summary of Architectural Invariants

| Dimension | Standard Approaches | SwarmWatch Production Standard |
| :--- | :--- | :--- |
| **Telemetry Ingestion Overhead** | 150–400ms inline HTTP POST | **<1µs lock-free ring buffer** (background worker) |
| **Loop & Hallucination Detection** | Secondary GPT-4 call ($0.03–$0.12/step, 2–4s) | **$0.00 zero-token Shannon Entropy ($H_{norm}$)** (<250µs) |
| **State Inspection** | Destructive rerun of all past turns | **Non-destructive Git-like DAG snapshotting & branching** |
| **Data Privacy** | Raw keys and passwords sent to SaaS | **In-process regex masking (`sk-...`, Bearer, DSN)** |
| **Multi-Tenancy** | Single shared database | **Row-level project isolation with constant-time SHA-256 keys** |
