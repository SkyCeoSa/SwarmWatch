# ADR 0054: Autonomous Dynamic Topology Healing & Auto-Patching

## Status
Accepted

## Context
In multi-agent architectures (LangGraph, CrewAI, AutoGen, LlamaIndex), interaction graphs can experience structural runtime failures—such as conversational deadlocks (agents with no outgoing edges that fail to reach a terminal state), unbounded circular delegation loops ($A \to B \to A$), cumulative token budget exhaustion along recursive sub-graphs, and concurrent mutual exclusion violations between conflicting agent roles.

While Seam 42 (ADR 0053: Bounded Model Checking) deterministically detects and proves these invariant violations and constructs detailed counterexample traces, passive detection alone requires human operator intervention or system aborts. 

To achieve truly autonomous self-repair without human-in-the-loop delays or remote LLM API overhead, SwarmWatch requires an in-process, deterministic **Topology Auto-Healer** (`TopologyAutoHealer`). When an invariant violation is identified, the engine synthesizes a formal graph patch (`TopologyPatch`), applies it to the `AgentTransitionModel`, and immediately runs a secondary BMC pass to mathematically prove that the patched graph satisfies all safety and liveness invariants.

## Decision
We implement Seam 43: Autonomous Dynamic Topology Healing & Auto-Patching as a deep, zero-token module with dual-SDK parity across Python and TypeScript.

### 1. Canonical Patch Actions (`PatchAction`)
- `FALLBACK_REDIRECT`: For deadlocked nodes without transitions, injects a fallback transition to an approved supervisor or recovery agent (e.g. `agent-supervisor` or `agent-fallback`).
- `CYCLE_GATE_BREAK`: For unbounded cyclic loops, introduces a progress guard flag (e.g., `CYCLE_BREAK_FLAG`) or injects an exit edge directly to an evaluation node after threshold iterations.
- `BUDGET_REWIRE`: For token-budget-exhausting paths, substitutes or rewires greedy transitions to low-cost summarizer/compacting agent nodes.
- `MUTEX_MEDIATE`: For mutual exclusion violations between incompatible agents ($A_1$ and $A_2$), inserts an intermediate mediator/buffer transition step that decouples direct execution.

### 2. Core Entities
- `TopologyPatch`:
  - `patch_id`: Unique identifier (`patch-<hex>`).
  - `swarm_id`: Target swarm identifier.
  - `action`: Canonical `PatchAction` enum value.
  - `target_agent`: Affected agent requiring repair.
  - `rewired_to`: The new destination agent introduced by the patch.
  - `synthetic_transition`: The newly injected `AgentTransition`.
  - `rationale`: Human-readable mathematical justification based on the counterexample trace.
  - `applied_at`: ISO 8601 timestamp.
  - `status`: `ACTIVE`, `REVERTED`, `PENDING`.
- `HealedModelReport`:
  - `swarm_id`: Identifier of the swarm.
  - `original_verdict`: Initial BMC verdict (`VIOLATED`).
  - `violated_invariant`: The invariant that triggered the patch.
  - `applied_patch`: The synthesized `TopologyPatch`.
  - `patched_model`: The resulting `AgentTransitionModel`.
  - `healed_verdict`: Secondary BMC verdict proving the patch resolves the violation (`VERIFIED`).
  - `execution_time_ms`: Total execution time for synthesis, patching, and reverification (<2.0ms).

### 3. Zero-Token Cost Guarantee
Patch synthesis is strictly deterministic and graph-theoretic. It operates directly upon transition matrices and counterexample step lineages without calling external LLM APIs, SAT solvers, or cloud services.

### 4. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/topology_healer.py` (`PatchAction`, `TopologyPatch`, `HealedModelReport`, `TopologyAutoHealer`, `global_topology_healer`).
- **TypeScript**: `packages/sdk-typescript/src/topology_healer.ts` (`PatchAction`, `TopologyPatch`, `HealedModelReport`, `TopologyAutoHealer`, `globalTopologyHealer`) complying with Node 22 type-stripping rules.

### 5. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/swarms/{id}/topology-heal/status`: Returns current active patches and healing statistics.
  - `POST /api/v1/swarms/{id}/topology-heal/apply`: Computes and applies an auto-patch to resolve failing model invariants, verifying with BMC.
  - `POST /api/v1/swarms/{id}/topology-heal/revert`: Reverts active patches and restores original model topology.
- **CLI Subcommand**:
  - `swarmwatch heal status|apply|revert --swarm-id <id>`
- **Web Console**:
  - `#topologyHealBadge` in `.fleet-left-group` of `#fleetBar` in `dashboard.html` with real-time polling.
  - Impeccable Craft Floor: 100% compliant with 0 anti-patterns (`[]`).

## Consequences
- Multi-agent interaction graphs can self-correct dynamically when encountering unexpected deadlocks or cycles.
- Every auto-patch is mathematically verified with BMC before being deployed or committed.
- Reversible patch history ensures complete operational transparency and auditability.
