# ADR 0106: Autonomous Cloud SaaS Billing, Usage Metering & Micro-Payment Channel Ledger

- **Status**: Accepted
- **Date**: 2026-10-05
- **Seam**: Seam 95 (Phase 8: SwarmWatch Cloud SaaS & Marketplace)
- **Author**: SwarmWatch Architectural Committee

---

## 1. Context & Problem Statement

Autonomous multi-agent swarms operating in SwarmWatch Cloud SaaS perform millions of high-frequency operations per hour—including anycast telemetry routing, recursive SNARK verification, cross-cluster hyper-graph synchronization, and proof marketplace settlement. Traditional billing pipelines relying on synchronous payment APIs, periodic batch reconciliation, or unbacked post-paid invoicing fail in decentralized, high-throughput autonomous agent networks due to:
1. **Latency Overhead**: External billing API roundtrips (100ms–2000ms) violate the sub-250µs execution latency SLO of the SwarmWatch control plane.
2. **Transaction Fee Friction**: Micro-transactions of sub-cent value are made economically infeasible by flat per-transaction processor fees.
3. **Counterparty Default & Overdraw Risk**: Rogue, looping, or rogue hallucinating agent swarms can rapidly exhaust credit lines before async batch billing detectors trip.
4. **Trust Minimization**: Autonomous agents require cryptographic certainty that fees deducted correspond exactly to provable service consumption without vendor lock-in or arbitrary debiting.

---

## 2. Decision & Architecture

We introduce **Seam 95: Autonomous Cloud SaaS Billing, Usage Metering & Micro-Payment Channel Ledger** (`billing_channel.py` in Python, `billing_channel.ts` in TypeScript).

This deep module provides an off-chain cryptographic state channel protocol combined with deterministic sub-microsecond usage metering, strictly conserving deposited collateral while enabling zero-fee off-chain balance updates.

### 2.1 Mathematical Invariants

- **Invariant MPC1: Channel Balance Conservation**:
  For any channel with initial locked deposit $D_0$:
  $$D_0 = B_{\text{agent}}(\nu) + B_{\text{provider}}(\nu) + E_{\text{escrow}}(\nu)$$
  Every micro-transfer preserves the invariant. Negative balances or unbacked fund generation are rejected fail-closed with `ChannelBalanceConservationError`.

- **Invariant MPC2: Monotonic Nonce & Dual Cryptographic Signatures**:
  Every off-chain balance update must advance the channel nonce strictly monotonically:
  $$\nu_{k+1} > \nu_k$$
  State transitions must be authenticated via Ed25519 signatures from both the tenant agent ($K_{\text{agent}}$) and the SwarmWatch SaaS billing authority ($K_{\text{provider}}$) over the canonical commitment digest:
  $$\text{CommitmentDigest} = \text{sha256}(\text{channel\_id} \parallel \nu \parallel B_{\text{agent}} \parallel B_{\text{provider}} \parallel \text{hlc})$$
  Submissions with non-increasing nonces $\nu \le \nu_{\text{current}}$ are rejected fail-closed with `StaleChannelNonceError`.

- **Invariant MPC3: Dispute Window & Challenge Slashing**:
  When either party initiates channel settlement (`initiate_closure`), a time-bounded challenge window $T_{\text{dispute}}$ is opened.
  If an adversary attempts to close the channel using an obsolete state $\nu_{\text{stale}} < \nu_{\text{latest}}$, the counterparty can submit a valid higher-nonce state certificate. The dispute engine verifies the fraud, settles the channel immediately in favor of the challenger, and slashes the adversary's entire remaining collateral as a penalty.

- **Invariant MPC4: Real-Time Usage Metering & Safety Circuit Latching**:
  The gateway tracks granular usage units (telemetry spans ingested, SNARK proofs verified, barrier cuts coordinated) and debits the agent's balance in sub-microsecond steps. If the agent's available channel balance drops below the minimum reserve threshold $B_{\text{reserve}}$, the billing manager trips `BILLING_CHANNEL_DEPLETED` on `SafetyCircuitBus`, immediately halting billable execution until the channel is replenished.

---

## 3. Dual-SDK Parity & API Specification

Both Python and TypeScript SDKs expose identical types, interfaces, error hierarchies, and authoritative singletons:

### Python (`swarmwatch.billing_channel`):
- Dataclasses: `ChannelStateCommitment`, `MicroPaymentChannel`, `UsageMeterEvent`, `ChannelSettlementCertificate`
- Gateway: `MicroPaymentChannelLedger`, `global_billing_ledger`
- Exceptions: `BillingChannelError`, `ChannelNotFoundError`, `ChannelBalanceConservationError`, `StaleChannelNonceError`, `InvalidChannelSignatureError`, `ChannelDisputeActiveError`, `BillingChannelExhaustedError`

### TypeScript (`@swarmwatch/sdk/src/billing_channel.ts`):
- Interfaces & Types: `ChannelStateCommitment`, `MicroPaymentChannel`, `UsageMeterEvent`, `ChannelSettlementCertificate`
- Class: `MicroPaymentChannelLedger`, `globalBillingLedger`
- Error classes with 100% naming parity.

---

## 4. Craft Floor & Observability Compliance

- **Latency SLO**: Micro-payment deduction and state transition verify in $< 250\mu\text{s}$.
- **Zero-Token Cost Guarantee**: Zero external LLM calls ($0.00 USD).
- **Control Plane Endpoints**:
  - `POST /api/v1/billing/channels` (Open new micro-payment channel)
  - `POST /api/v1/billing/channels/update` (Submit off-chain signed state update)
  - `POST /api/v1/billing/channels/meter` (Record usage meter event)
  - `POST /api/v1/billing/channels/close` (Initiate channel closure / settlement)
  - `GET /api/v1/billing/channels/status` (Get channel ledger status)
- **CLI Subcommand**: `swarmwatch billing status|open|meter|settle`
- **Dashboard HUD Badge**: `#billingBadge` with status `BILLING: CHANNEL OK` (19 chars $\le 30$).
