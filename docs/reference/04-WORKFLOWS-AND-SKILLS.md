# 04. Mandatory Workflows & Skills

This document details the exact workflows and skill protocols governing SwarmWatch development.

---

## 1. The Matt Pocock Engineering Skills Suite

### 1. `diagnosing-bugs`
- **When to trigger**: Whenever a bug, exception, test failure, or UI defect is observed.
- **Workflow**:
  1. *Step 1 (Reproduce)*: Write an isolated automated test or reproduction script (e.g. CDP script for browser UI).
  2. *Step 2 (Root Cause Analysis)*: Trace the failure to the exact line, type violation, or syntax error before editing code. Never guess or apply speculative patches.
  3. *Step 3 (Fix at Seam)*: Address the defect at the deepest possible seam.
  4. *Step 4 (Verify)*: Confirm the reproduction script passes and no regressions exist.

### 2. `domain-modeling`
- **When to trigger**: When defining, modifying, or challenging domain terminology.
- **Rules**:
  1. `CONTEXT.md` is strictly a dictionary of Ubiquitous Language. It contains **no implementation details, no code snippets, no configuration parameters**.
  2. Each term must define what it IS in 1–2 sentences, followed by an explicit `_Avoid_` list of rejected synonyms.
  3. Significant architectural decisions must be recorded in sequential ADRs under `docs/adr/`.

### 3. `codebase-design`
- **When to trigger**: When creating or modifying module interfaces.
- **Rules**:
  1. Enforce deep modules: small surface area, deep capability.
  2. Place seams where behavior varies (storage backend, telemetry transport, LLM framework integration).
  3. Protect system invariants (thread safety, zero agent latency, secret redaction).

### 4. `tdd` (Test-Driven Development)
- **When to trigger**: Before writing any new functional logic.
- **Rules**:
  1. Red: Write the failing test asserting the expected behavior.
  2. Green: Write the minimal code required to pass.
  3. Refactor: Polish design and verify that all 27 tests in the suite remain 100% green.

### 5. `writing-for-agents`
- **When to trigger**: When writing skills, runbooks, or instructions for autonomous agents.
- **Rules**:
  1. Keep context load minimal: avoid bloat in always-loaded files (`AGENTS.md`).
  2. Use crisp context pointers pointing to detailed reference documents.
  3. Define checkable, exhaustive completion criteria for each action.

---

## 2. The Impeccable Design Skills Suite

### 1. `impeccable`
- **Console Thesis**: High-density, austere, calm developer console for multi-agent observability.
- **Craft Floor Verification**: Run mechanical check `impeccable.cmd detect --json dashboard.html`. Output must be `[]`.
- **Reviewer Workflow**: Document changes in `DESIGN.md` and execute finish review checks before shipping UI changes.
