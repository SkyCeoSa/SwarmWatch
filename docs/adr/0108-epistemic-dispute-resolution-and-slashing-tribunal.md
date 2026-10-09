# ADR 0108: Real-Time Epistemic Dispute Resolution & Dynamic Slashing Escalation Tribunal

- **Status**: Accepted
- **Date**: 2026-10-05
- **Seam**: Seam 97 (Phase 8: SwarmWatch Cloud SaaS & Marketplace)
- **Author**: SwarmWatch Architectural Committee

---

## 1. Context & Problem Statement

As autonomous agent swarms trade verification proofs (Seam 93), process micro-payments (Seam 95), and participate in multi-cloud gossip overlays (Seam 96), disagreements over task correctness, execution claims, and equivocation are inevitable. A flat, single-tier dispute mechanism fails in production because:
1. **Dispute Flooding**: Trivial or low-stakes disputes consume full consensus bandwidth, risking denial-of-service.
2. **Disproportionate Penalties**: Fixed slashing rules either under-penalize malicious double-signing or over-penalize benign network flaps.
3. **Appellate Integrity**: Autonomous agents require a deterministic, multi-signature appellate tier to audit contested arbitrations before irrevocable slashing.

---

## 2. Decision & Architecture

We introduce **Seam 97: Real-Time Epistemic Dispute Resolution & Dynamic Slashing Escalation Tribunal** (`slashing_tribunal.py` in Python, `slashing_tribunal.ts` in TypeScript).

This deep module provides hierarchical dispute progression, dynamically graded collateral slashing, and super-majority appellate finality.

### 2.1 Mathematical Invariants

- **Invariant TRB1: Multi-Tiered Dispute Escalation**:
  Disputes progress monotonically across 3 formal stages:
  $$\text{TIER\_1\_LOCAL} \xrightarrow{\Delta_1} \text{TIER\_2\_COMMITTEE} \xrightarrow{\Delta_2} \text{TIER\_3\_TRIBUNAL}$$
  Escalation requires an escalating challenge bond $B_{\text{esc}} = B_0 \cdot 2^{\text{tier}-1}$ to prevent sybil dispute spamming.

- **Invariant TRB2: Severity-Graded Slashing Matrix**:
  Slashing penalties are mathematically computed based on adjudicated severity class:
  - $\text{SEVERITY\_LOW}$ (Liveliness timeout): $S_{\text{slash}} = 0.05 \times S_{\text{bonded}}$
  - $\text{SEVERITY\_MEDIUM}$ (Contradictory state claim): $S_{\text{slash}} = 0.25 \times S_{\text{bonded}}$
  - $\text{SEVERITY\_CRITICAL}$ (Cryptographic equivocation / double-sign): $S_{\text{slash}} = 1.00 \times S_{\text{bonded}}$ + permanent quarantine.

- **Invariant TRB3: Non-Repudiable Super-Majority Appellate Finality**:
  Final appellate rulings require a $\ge \lfloor 2K/3 \rfloor + 1$ super-majority signature quorum from registered tribunal arbiters. Once finalized, a `TribunalVerdictReceipt` is published with canonical Merkle root anchoring.

- **Invariant TRB4: Fail-Closed Safety Circuit Latching**:
  If concurrent active critical disputes exceed the safety threshold $\sigma_{\text{surge}}$, the tribunal triggers `TRIBUNAL_SYSTEMIC_DISPUTE_SURGE` on `SafetyCircuitBus`, freezing disputed fund transfers fail-closed.

---

## 3. Dual-SDK Parity & API Specification

Both Python and TypeScript SDKs expose identical classes, methods, and error hierarchies:

### Python (`swarmwatch.slashing_tribunal`):
- Dataclasses: `DisputeClaim`, `TribunalArbiter`, `TribunalVerdictReceipt`, `TribunalEscalationRecord`
- Gateway: `SlashingEscalationTribunal`, `global_slashing_tribunal`
- Exceptions: `TribunalError`, `DisputeNotFoundError`, `InvalidDisputeTierError`, `InsufficientArbiterQuorumError`, `TribunalArbiterQuarantinedError`, `TribunalCircuitTrippedError`

### TypeScript (`@swarmwatch/sdk/src/slashing_tribunal.ts`):
- Interfaces & Types: `DisputeClaim`, `TribunalArbiter`, `TribunalVerdictReceipt`, `TribunalEscalationRecord`
- Class: `SlashingEscalationTribunal`, `globalSlashingTribunal`
- Error classes with 100% naming parity.

---

## 4. Craft Floor & Observability Compliance

- **Latency SLO**: Dispute filing, escalation, and verdict synthesis execute in $< 250\mu\text{s}$.
- **Zero-Token Cost Guarantee**: Zero external LLM calls ($0.00 USD).
- **Control Plane Endpoints**:
  - `POST /api/v1/tribunal/disputes` (Open new dispute claim)
  - `POST /api/v1/tribunal/escalate` (Escalate dispute to next tier)
  - `POST /api/v1/tribunal/adjudicate` (Submit arbiter vote / verdict)
  - `GET /api/v1/tribunal/status` (Get tribunal metrics)
- **CLI Subcommand**: `swarmwatch tribunal status|open|escalate|adjudicate`
- **Dashboard HUD Badge**: `#tribunalBadge` with status `TRIBUNAL: QUORUM RESOLVED OK` (28 chars $\le 30$).
