# ADR 0024: Visual Time-Machine Controls, Turn Timeline Scrubber & In-Browser Branch Forking

## Status
Accepted

## Context
In ADR 0003, SwarmWatch introduced Git-like Directed Acyclic Graph (DAG) state branching and non-destructive Time-Machine snapshots (`StateSnapshot`). While the core backend primitives existed in `TimeMachine` and `StorageAdapter`, console operators lacked interactive in-browser controls to navigate historical execution steps, inspect state snapshots, and fork divergent execution branches without modifying agent code or invoking CLI scripts.

To elevate developer experience and operational triage during autonomous agent multi-turn failures, the console (`dashboard.html`) required:
1. An integrated, responsive **Time-Machine control bar** beneath the Chaos bar, featuring bi-directional stepping (`Back` / `Next`), Jump to Head, an interactive scrubber slider, and turn counter.
2. Direct integration with backend state snapshot archives (`GET /api/v1/swarms/{swarm_id}/snapshots`) to surface exact state hashes and sanitized memory payloads per turn.
3. An interactive modal dialog (`#forkModal`) allowing operators to inspect origin turns, causal agents, state snapshot previews, configure new branch identifiers, and execute server-side branching.
4. Server-side antecedent span replication into the newly forked branch timeline, accompanied by real-time WebSocket broadcast (`branch_forked`).
5. An in-app, non-blocking toast notification system (`showToast()`) replacing jarring standard browser `alert()` popups in compliance with the **Impeccable Craft Floor** design standard (#080B11 palette, tabular numbers, zero emojis, zero neon glows).
6. Single-key keyboard navigation shortcuts (`[`, `]`, `f`, `Esc`) for lightning-fast operator triage.

## Decision
1. **Console UI Architecture (`dashboard.html`)**:
   - **Time-Machine Control Bar (`#timeMachineBar`)**: Located below the chaos bar. Contains `Back` (`#btnStepBack`), Turn counter display (`#scrubberTurnDisplay`), `Next` (`#btnStepForward`), `Head` (`#btnJumpHead`), Scrubber range input (`#scrubberSlider`), Ticks bar (`#scrubberTicks`), Branch dropdown (`#branchSelector`), and Fork action (`#btnTimeMachineFork`).
   - **Fork Modal (`#forkModal`)**: A dark-surfaced modal displaying origin turn/span, causal agents, state snapshot hash, branch identifier input, and sanitized state preview, with `Cancel` and `Fork & Switch Branch` actions.
   - **Non-blocking Toasts (`#toastContainer`)**: Toast notifications for step actions, head jumps, copy events, and fork completions.
   - **Keyboard Navigation**:
     - `[`: Step-Back to previous turn.
     - `]`: Step-Forward to next turn.
     - `f` / `F`: Open Fork Swarm Execution Branch modal for current turn.
     - `Escape`: Close open modal dialogs.

2. **Backend Server Integration (`server.py`)**:
   - **Snapshots Endpoint (`GET /api/v1/swarms/{swarm_id}/snapshots`)**: Returns chronological state snapshots filtered by `swarm_id` and optional `branch_id`.
   - **Enhanced Branch Forking (`POST /api/v1/branches/fork`)**:
     - Steps back agent memory state via `time_machine.step_back()`.
     - Replicates antecedent historical spans up to `target_span_id` into the new branch namespace (`{orig_id}_fork_{new_branch_id}`).
     - Persists root state snapshot for the new branch.
     - Broadcasts `branch_forked` event over WebSocket to all live console clients.
     - Returns `{ status: "forked", new_branch_id, target_span_id, replicated_spans_count, state }`.

3. **Storage Tier Support (`storage.py`)**:
   - Implemented `get_snapshots_for_swarm()` in `StorageAdapter` base class and `SQLiteStorageAdapter`.
   - Updated `list_branches()` to compute `UNION` of distinct branches across both `spans` and `snapshots` tables with guaranteed default `"main"` presence.

## Consequences
- **Positive**:
  - Operators can visually scrub through hundreds of multi-agent execution turns with instantaneous DOM updates and zero backend latency.
  - Failures and loops can be debugged and branched in seconds directly from the browser UI.
  - Zero modal lockup in headless automation test environments thanks to native in-app toasts.
  - Strict compliance with Impeccable Craft Floor guidelines and Matt Pocock software engineering standards.
- **Negative**:
  - Branching duplicates antecedent spans in storage for isolation, increasing SQLite storage linearly with the number of forks created.
