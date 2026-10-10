# ADR 0017: Visual Causal DAG Graph View & Cycle Topology in Developer Console

## Status
Accepted

## Context
In multi-agent systems (swarms), understanding communication topology is critical. Chronological event feeds (Stream View) are effective for turn-by-turn debugging, but fail to provide spatial clarity on agent interaction networks, bottlenecks, and structural feedback loops.
When an agent loop trips (e.g. `BackendEngineer` <-> `SecurityAuditor` or recursive tool calls), developers need to immediately identify:
1. Which agents form the cycle.
2. The direction of causality and token/turn accumulation along each communication edge.
3. The overall cluster topology without loading heavy, CPU-intensive external visualization libraries (e.g. D3.js, Cytoscape, Vis.js) that would degrade host system performance or interfere with real-time processes such as algorithmic trading systems.

## Decision
We implement a lightweight, non-blocking **Visual Causal DAG Graph View** in the SwarmWatch Developer Console (`dashboard.html`) powered by a backend deep module (`topology.py`):

1. **Deterministic Backend Deep Module (`topology.py`)**:
   - Extracts `AgentNode` and `CausalEdge` structures directly from stored `SwarmSpan` items.
   - Calculates directed communication frequencies, cumulative tokens, and mean latency between agents.
   - Employs DFS cycle-detection algorithm to deterministically tag cyclic communication paths (`is_cyclic = True`).
   - Exposes `GET /api/v1/topology` on the control plane.

2. **Ultra-Lightweight Vector SVG Console Renderer**:
   - Uses native browser SVG with pre-computed mathematical coordinate layouts (layered circular/hierarchical positioning).
   - Zero external JavaScript dependencies (<15KB code overhead, zero CDN requests).
   - Zero WebGL or GPU canvas memory footprint, guaranteeing <0.05% CPU load.
   - Features view switching (`Stream`, `DAG Graph`, `Split View`).
   - Visualizes cyclic loops with high-contrast alert strokes (`#EF4444`) and subtle animated dash offsets.

3. **High-Density Craft Standard Adherence**:
   - Pinned `#080B11` canvas palette and `#0E1422` card surfaces.
   - Hairline borders (`1px solid var(--border-subtle)`), tabular numbers (`tnum`), zero neon glows, zero emojis.
   - Mechanical scan with `impeccable detect` must pass with 0 findings (`[]`).

4. **Zero-Cloud & Zero-Token Guarantees**:
   - 100% in-process calculation without remote cloud dependencies or GitHub Actions minutes.
   - Zero external LLM token expenditure for graph synthesis.

## Consequences
- Developers can immediately switch between chronological stream cards and spatial causal network topology.
- Cyclic feedback loops and agent deadlocks are instantly recognizable visually in red.
- Host machine resources remain completely unburdened, allowing trading platforms and sensitive processes to operate with uninterrupted priority.
