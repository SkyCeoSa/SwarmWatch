# ADR 0047: Real-Time Topological Graph Clustering & Dynamic Agent Coalitions (Seam 36)

## Status
Accepted

## Date
2026-09-24

## Context
Multi-agent swarms operating in production environments frequently exhibit complex topological interaction patterns. As the number of concurrent agents grows (5 to 50+ agents), several structural pathologies emerge:
1. **Conversational Deadlocks & Dense Silos**: Subsets of agents engage in dense bidirectional communication loops, excluding external feedback and producing repetitive chatter.
2. **Bottleneck Centrality Congestion**: Key intermediary agents (e.g. planner or tool proxy) accumulate excessive in-degree and out-degree communication edges, creating systemic latency cascades and single points of failure.
3. **Absence of Autonomous Dynamic Coalitions**: When complex sub-tasks or conversational deadlocks arise, swarms lack the topological intelligence to partition agents into isolated, specialized sub-graphs (coalitions) with designated leaders and targeted token budgets, leading to global context pollution.

An in-process, zero-external-dependency, zero-token-cost graph analytics and clustering engine is required to evaluate interaction density, detect bottleneck agents, and dynamically form autonomous agent coalitions in $<50\mu\text{s}$ per turn without remote LLM calls or dense graph database overhead.

## Decision
We implement an in-process, deterministic **Topological Graph Clustering & Dynamic Agent Coalition Engine** (`TopologicalCoalitionEngine`) across the Python and TypeScript SDKs:

1. **Directed Causal Interaction Graph ($G = (V, E, W)$)**:
   - Evaluates active agents ($V$) and directed causal spans ($E$).
   - Computes weighted interaction strength ($W(u, v)$) combining message count, token volume, and recency:
     $$W(u, v) = \sum_{s \in \text{spans}(u \to v)} \left( 1.0 + \ln(1.0 + \text{tokens}(s)) \right)$$

2. **Topological Centrality & Bottleneck Detection**:
   - **In-Degree Centrality**: $C_{\text{in}}(v) = \frac{\sum_{u} W(u, v)}{\sum_{u, w} W(u, w)}$
   - **Out-Degree Centrality**: $C_{\text{out}}(v) = \frac{\sum_{w} W(v, w)}{\sum_{u, w} W(u, w)}$
   - **Bottleneck Score**: $B(v) = \sqrt{C_{\text{in}}(v) \cdot C_{\text{out}}(v)} \cdot \text{deg}(v)$, flagging congested transit nodes when $B(v) > \theta_{\text{bottleneck}}$.

3. **Sub-Millisecond Modularity & Community Clustering**:
   - Employs an in-process label propagation and mutual affinity coalescence algorithm:
     - Symmetrized affinity matrix: $M_{uv} = \frac{W(u, v) + W(v, u)}{2}$
     - Coalesces densely interconnected agents into coherent topological communities.
   - Evaluates Newman-Girvan Graph Modularity ($Q \in [-0.5, 1.0]$):
     $$Q = \frac{1}{2m} \sum_{u, v} \left[ A_{uv} - \frac{k_u k_v}{2m} \right] \delta(c_u, c_v)$$
     where $Q \ge 0.30$ indicates significant community structure.

4. **Dynamic Agent Coalition Formation (`AgentCoalition`)**:
   - For each detected cluster with high interaction density ($\ge 0.50$), forms an autonomous `AgentCoalition`.
   - Designates a **Coalition Leader**: the agent with the highest internal degree centrality within the cluster.
   - Provides isolation boundaries, scoped sub-budgets, and targeted deadlock alleviation.
   - Detects circular conversational deadlocks within coalitions and recommends arbitration or temporary sub-graph decoupling.

5. **Dual-SDK Parity (TypeScript)**:
   - Full Node 22 type-stripping compliant implementation in `@swarmwatch/sdk/src/coalition.ts`:
     - `TopologicalCoalitionEngine`, `AgentCoalition`, `TopologicalClusterResult`, `NodeCentrality`.
   - Exported in `packages/sdk-typescript/src/index.ts`.

6. **REST Control Plane, CLI & Web Console**:
   - REST Endpoints:
     - `GET /api/v1/swarms/{swarm_id}/coalitions`: Current clusters, coalitions, node centrality metrics, and bottlenecks.
     - `POST /api/v1/swarms/{swarm_id}/coalitions/cluster`: Force immediate topological clustering re-computation.
     - `POST /api/v1/swarms/{swarm_id}/coalitions/reset`: Reset graph topology for a swarm.
   - CLI Subcommands:
     - `swarmwatch coalition status <swarm_id>`: Formatted tabular summary of clusters, coalitions, and bottlenecks.
     - `swarmwatch coalition cluster <swarm_id>`: Execute clustering epoch.
   - Web Console (`dashboard.html`):
     - Added `#coalitionBadge` inside `.fleet-left-group` in `#fleetBar`.
     - 100% compliant with Impeccable Craft Floor (0 anti-patterns `[]`).

## Consequences

### Positive
- Surfaces communication silos, conversational loops, and bottleneck agents in real-time.
- Enables autonomous partitioning of multi-agent workloads into focused coalitions.
- Runs 100% in-process with zero external LLM tokens and sub-millisecond execution ($<50\mu\text{s}$ per turn).
- Complete dual-SDK parity across Python and TypeScript.

### Negative
- Dense graphs with hundreds of nodes require quadratic affinity evaluation $O(|V|^2)$, bounded in SwarmWatch to active swarms ($|V| \le 100$).
