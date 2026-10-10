# ADR 0027: State Snapshot Diff Inspector in Developer Console & Dual-SDK Diff Engine (Seam 16)

## Status
Accepted

## Context
Following the implementation of visual Time-Machine controls (ADR 0024), TypeScript state branching (ADR 0025), and programmatic fork resumption (ADR 0026), operators can travel backwards and forwards through multi-agent execution history and fork execution branches.

However, diagnosing multi-agent divergence, memory degradation, and state poisoning requires understanding **precisely what changed** between execution turns.
In complex agent architectures:
1. Agent states contain nested object graphs (conversation histories, retrieved contexts, tool outputs, planning trees, variable memory).
2. Manually comparing multi-kilobyte JSON snapshots across consecutive turns is slow, error-prone, and visually fatiguing.
3. Operators investigating rogue loops or hallucinatory drift need immediate visual highlighting of added keys, modified values, removed fields, and masked sensitive credentials.
4. Developers need programmatic access to deterministic state diffs across both Python and TypeScript to automate regression assertions and verification in CI without remote LLM costs.

## Decision
We established **Seam 16: State Snapshot Diff Inspector & Dual-SDK Diff Engine**:

1. **Deterministic Recursive State Diff Engine (Dual-SDK Parity)**:
   - Implemented `compute_state_diff()` in Python (`packages/sdk-python/swarmwatch/serializer.py`) and `computeStateDiff()` in TypeScript (`packages/sdk-typescript/src/timemachine.ts`).
   - Produces a structured `StateDiffReport` with:
     - `summary`: `added_count`, `modified_count`, `removed_count`, `unchanged_count`, `redacted_count`.
     - `entries`: Array of `DiffEntry` items detailing `path` (JSON dot-notation), `kind` (`added`, `modified`, `removed`, `unchanged`), `old_value`, `new_value`, and `is_redacted`.
     - `hash_a` / `hash_b`: SHA-256 state fingerprints for cryptographic verification.
   - Fully deterministic recursive traversal for arbitrary nested dictionaries, arrays, and primitive values.
   - Zero external LLM token expenditure (100% deterministic local computation).

2. **Control Plane REST Endpoint**:
   - `GET /api/v1/swarms/{swarm_id}/snapshots/diff`: Query endpoint accepting `base_span_id` (or `from_span_id`) and `target_span_id` (or `to_span_id`).
   - Fetches historical snapshots from storage, validates swarm/branch context, executes `compute_state_diff()`, and returns the structured diff report.

3. **Developer Console Inspector (`dashboard.html`)**:
   - Upgraded Right Inspector Panel with a 3-tab segmented control:
     - `Turn Payload`: Telemetry metrics, causal agents, duration, and raw payload text.
     - `State Snapshot`: Formatted, syntax-colored JSON of the turn's sanitized state.
     - `Snapshot Diff`: Interactive visual diff inspector displaying added, modified, and removed variables with live delta counters (`+N ~N -N`).
   - Comparison Selector:
     - Allows comparing current turn with `Previous Turn (N-1)` (default), `Initial Turn (Root)`, `Fork Origin`, or any specific turn in the stream.
   - View Modes:
     - `Unified`: Inline delta list with colored indicator badges, old-value strikethroughs, and new-value highlights.
     - `Split`: Side-by-side comparison table showing antecedent vs subsequent states.
   - Filter Controls:
     - Quick filter chips for `All`, `Changed`, `Added`, `Modified`, `Removed`, and `Redacted`.
   - Visual Semantics strictly conforming to High-Density Craft Standard:
     - Canvas `#080B11`, surface `#0E1422`, hairline borders `#1D2840`.
     - Added: Emerald green (`#10B981` / background `#064e3b22`).
     - Modified: Amber (`#F59E0B` / background `#78350f22`).
     - Removed: Crimson red (`#EF4444` / background `#7f1d1d22`).
     - Redacted: Indigo pill (`#6366F1`) with `[REDACTED]` badge.
     - Zero emojis, zero neon glows, monospace tabular numerals (`font-variant-numeric: tabular-nums`).

## Consequences
- **Positive**:
  - Instant visual triage of agent memory mutations during debugging and time-travel inspection.
  - Zero-token local computation eliminates cost and latency for diff generation.
  - Programmatic API parity in Python and TypeScript enables automated test assertions on agent state transitions.
  - Unified and split views accommodate both quick line scans and deep side-by-side structural comparison.
- **Negative**:
  - Very deep object graphs (>100 levels) require defensive truncation, handled gracefully by `SafeStateSerializer`.
