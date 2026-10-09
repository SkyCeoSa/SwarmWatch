# ADR 0109: Autonomous Agent Capability Attestation & Hardware Secure Enclave (SGX/TEE) Remote Witness

- **Status**: Accepted
- **Date**: 2026-10-05
- **Seam**: Seam 98 (Phase 8: SwarmWatch Cloud SaaS & Marketplace)
- **Author**: SwarmWatch Architectural Committee

---

## 1. Context & Problem Statement

In distributed agent networks and multi-cloud SaaS environments, agents perform high-value operations including micro-payment settlement (Seam 95), recursive SNARK proof generation (Seam 94), and dispute arbitration (Seam 97). However, operating in un-trusted host environments exposes agent processes to memory extraction, prompt injection, binary substitution, and spoofed execution claims.

Pure software authentication is insufficient when the underlying host or hypervisor is untrusted. Agents require **Hardware Secure Enclave / Trusted Execution Environment (TEE) Remote Attestation** (e.g. Intel SGX/TDX, AMD SEV-SNP, AWS Nitro Enclaves) to mathematically prove:
1. The exact software identity (code hash, measurement) running inside isolated enclave memory.
2. The freshness of execution via cryptographic challenge-response nonces.
3. Unforgeable, lease-bounded capability delegation without exposing long-term root secrets.

---

## 2. Decision & Architecture

We introduce **Seam 98: Autonomous Agent Capability Attestation & Hardware Secure Enclave (SGX/TEE) Remote Witness** (`enclave_attestation.py` in Python, `enclave_attestation.ts` in TypeScript).

This deep module provides hardware measurement verification, anti-replay challenge protocols, capability token issuance, and fail-closed safety circuit latching under attestation failures.

### 2.1 Mathematical Invariants

- **Invariant TEE1: Cryptographic Enclave Measurement Integrity (MRENCLAVE / PCR)**:
  The runtime measurement of the enclave execution payload $M_{\text{runtime}} = \text{SHA-256}(\text{Code} \parallel \text{Config} \parallel \text{State})$ must bitwise match the registered approved policy measurement $M_{\text{policy}}$. Any deviation immediately rejects quote verification with `EnclaveMeasurementMismatchError`.

- **Invariant TEE2: Nonce-Challenged Freshness & Anti-Replay**:
  Remote attestation quotes must bind an active cryptographic challenge nonce:
  $$N_{\text{quote}} = \text{SHA-256}(\text{Challenge} \parallel \text{Timestamp} \parallel \text{AgentID})$$
  Quotes submitted with mismatched, expired, or previously consumed nonces fail fail-closed with `StaleAttestationReportError`.

- **Invariant TEE3: Lease-Bounded Capability Delegation**:
  Upon successful attestation, the enclave witness issues a cryptographically signed `EnclaveCapabilityToken` granting permissions $\mathcal{C} \subseteq \mathcal{C}_{\text{allowed}}$ valid strictly for lease duration $T_{\text{lease}} \le T_{\max}$. Invocations beyond lease expiration or outside $\mathcal{C}$ fail with `UnauthorizedCapabilityError`.

- **Invariant TEE4: Enclave Compromise Quarantine & Safety Circuit Latching**:
  If signature verification fails repeatedly ($\ge \theta_{\text{fail}}$) or if a revoked enclave key attempts attestation, the enclave ID is permanently quarantined and the coordinator trips `TEE_HARDWARE_ATTESTATION_BREACH` on `SafetyCircuitBus`.

---

## 3. Dual-SDK Parity & API Specification

Both Python and TypeScript SDKs expose identical classes, methods, and error hierarchies:

### Python (`swarmwatch.enclave_attestation`):
- Dataclasses: `EnclaveMeasurement`, `AttestationQuote`, `EnclaveCapabilityToken`, `EnclaveChallenge`
- Coordinator: `EnclaveAttestationCoordinator`, `global_enclave_coordinator`
- Exceptions: `EnclaveAttestationError`, `EnclaveMeasurementMismatchError`, `StaleAttestationReportError`, `UnauthorizedCapabilityError`, `EnclaveQuarantinedError`, `EnclaveCircuitTrippedError`

### TypeScript (`@swarmwatch/sdk/src/enclave_attestation.ts`):
- Interfaces & Types: `EnclaveMeasurement`, `AttestationQuote`, `EnclaveCapabilityToken`, `EnclaveChallenge`, `EnclaveStatus`
- Class: `EnclaveAttestationCoordinator`, `globalEnclaveCoordinator`
- Error classes with 100% naming parity.

---

## 4. Craft Floor & Observability Compliance

- **Latency SLO**: Quote generation, verification, and capability checks execute in $< 250\mu\text{s}$.
- **Zero-Token Cost Guarantee**: Zero external LLM calls ($0.00 USD).
- **Control Plane Endpoints**:
  - `POST /api/v1/enclave/challenge` (Issue fresh attestation challenge)
  - `POST /api/v1/enclave/attest` (Submit and verify remote attestation quote)
  - `POST /api/v1/enclave/verify-capability` (Validate capability token)
  - `GET /api/v1/enclave/status` (Get enclave attestation metrics)
- **CLI Subcommand**: `swarmwatch enclave status|challenge|attest|verify`
- **Dashboard HUD Badge**: `#enclaveBadge` with status `ENCLAVE: HARDWARE WITNESS OK` (28 chars $\le 30$).
