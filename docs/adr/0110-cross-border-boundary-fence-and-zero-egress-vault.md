# ADR 0110: Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault

- **Status**: Accepted
- **Date**: 2026-10-05
- **Seam**: Seam 99 (Phase 8: SwarmWatch Cloud SaaS & Marketplace)
- **Author**: SwarmWatch Architectural Committee

---

## 1. Context & Problem Statement

As SwarmWatch Cloud SaaS operates across global multi-cloud regions (AWS, GCP, Azure, Sovereign on-premise clusters), multi-agent swarms ingest, process, and transfer sensitive tenant telemetry, execution artifacts, and state transitions across national and regulatory borders.

Data sovereignty regulations (such as EU GDPR, US HIPAA, Swiss FADP, and national data localization mandates) strictly prohibit unauthorized exfiltration or extraterritorial data transit. If an autonomous agent accidentally routes customer telemetry across unauthorized boundaries or suffers a compromised egress channel, catastrophic regulatory and security breaches occur.

SwarmWatch requires a mathematically sound **Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault** that operates in-process with sub-250µs latency, verifying data residency policies, cryptographically enforcing zero-egress envelopes, issuing single-use export passports, and latching fail-closed safety circuits upon breach.

---

## 2. Decision & Architecture

We introduce **Seam 99: Sovereign Cross-Border Data Boundary Fence & Zero-Egress Encryption Vault** (`boundary_fence.py` in Python, `boundary_fence.ts` in TypeScript).

This deep module provides geofenced residency boundaries, envelope encryption vaults, single-use cryptographic egress export passports, and automated containment circuit latching.

### 2.1 Mathematical Invariants

- **Invariant BND1: Geographic Residency Policy Constraint (Geofence)**:
  Every data envelope is tagged with a sovereign residency zone $Z_{\text{origin}} \in \mathcal{Z}$ (e.g. `EU_DE`, `US_EAST`, `CH_ZH`, `JP_TYO`). Cross-zone transmission $Z_{\text{src}} \to Z_{\text{dst}}$ is strictly disallowed unless an active export treaty $T(Z_{\text{src}}, Z_{\text{dst}}) = \text{PERMITTED}$ is explicitly registered. Unauthorized transfers fail fail-closed with `CrossBorderResidencyViolationError`.

- **Invariant BND2: Zero-Egress Vault Envelope Encryption**:
  Sensitive data crossing boundary perimeters must be encapsulated in an authenticated zero-egress vault envelope using tenant/zone root keys:
  $$C = \text{Encrypt}_{K_Z}(P) \parallel \text{HMAC}_{K_Z}(C)$$
  Raw, unsealed plaintext attempting perimeter crossing without a valid cryptographic container is rejected with `UnsealedDataEgressError`.

- **Invariant BND3: Cryptographic Egress Export Passport**:
  Approved cross-border data transfers require an immutable, cryptographically signed `EgressExportPassport`:
  $$\text{Passport} = \text{Sign}_{K_{\text{fence}}}(\text{TenantID} \parallel Z_{\text{src}} \parallel Z_{\text{dst}} \parallel \text{PayloadDigest} \parallel \text{Expiry})$$
  Transfers with expired, forged, or mismatched passports are rejected with `InvalidEgressPassportError`.

- **Invariant BND4: Boundary Breach Containment & Safety Circuit Latching**:
  Repeated unauthorized egress attempts ($\ge \theta_{\text{breach}}$) or detected exfiltration triggers immediate quarantine of the offending tenant/zone and latches `BOUNDARY_FENCE_EGRESS_BREACH` on `SafetyCircuitBus`.

---

## 3. Dual-SDK Parity & API Specification

Both Python and TypeScript SDKs expose identical classes, methods, and error hierarchies:

### Python (`swarmwatch.boundary_fence`):
- Dataclasses: `ResidencyZone`, `EgressTreaty`, `VaultEnvelope`, `EgressExportPassport`, `BoundaryInspectionResult`
- Coordinator: `DataBoundaryFence`, `global_boundary_fence`
- Exceptions: `BoundaryFenceError`, `CrossBorderResidencyViolationError`, `UnsealedDataEgressError`, `InvalidEgressPassportError`, `FenceZoneQuarantinedError`, `FenceCircuitTrippedError`

### TypeScript (`@swarmwatch/sdk/src/boundary_fence.ts`):
- Interfaces & Types: `ResidencyZone`, `EgressTreaty`, `VaultEnvelope`, `EgressExportPassport`, `BoundaryInspectionResult`, `FenceStatus`
- Class: `DataBoundaryFence`, `globalBoundaryFence`
- Error classes with 100% naming parity.

---

## 4. Craft Floor & Observability Compliance

- **Latency SLO**: Treaty verification, payload sealing, and inspection execute in $< 250\mu\text{s}$.
- **Zero-Token Cost Guarantee**: Zero external LLM calls ($0.00 USD).
- **Control Plane Endpoints**:
  - `POST /api/v1/fence/treaties` (Register cross-border egress treaty)
  - `POST /api/v1/fence/seal` (Seal sensitive payload into vault envelope)
  - `POST /api/v1/fence/passport` (Issue cross-border egress passport)
  - `POST /api/v1/fence/inspect` (Inspect packet at egress boundary)
  - `GET /api/v1/fence/status` (Get boundary fence metrics)
- **CLI Subcommand**: `swarmwatch fence status|treaty|seal|passport|inspect`
- **Dashboard HUD Badge**: `#fenceBadge` with status `FENCE: DATA BOUNDARY OK` (23 chars $\le 30$).
