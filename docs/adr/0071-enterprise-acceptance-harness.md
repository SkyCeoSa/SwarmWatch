# ADR 0071: Enterprise Acceptance Harness & Multi-Dimensional Release Verification Gate (Seam 60)

## Status
Accepted

## Context
Deploying multi-agent observability and causal control infrastructure into production and regulated environments requires mathematically rigorous, reproducible verification rather than unverified marketing claims or manual checklists:
1. **The Need for Executable Deployment Gates**: As highlighted in the independent software engineering audit, claims of "production readiness", "zero-token execution", and "offline capability" must be verified by automated, executable gates. A release candidate must not rely on passive documentation or optimistic assumptions.
2. **Multi-Dimensional Risk Surface**: Critical multi-agent infrastructure can fail across six distinct dimensions:
   - *Security*: Unauthorized access, fallback to admin in production, cross-tenant leaks, prompt exfiltration, and unescaped XSS injections.
   - *Interoperability*: Divergent trace protocols, inconsistent OTLP GenAI payloads, and mismatched serialization between runtimes.
   - *Data Loss*: Silent queue drops under high concurrency, unacknowledged socket buffer overflow, or state corruption during schema migration.
   - *Latency Floor*: Regressions in sub-microsecond in-process execution that cause telemetry to block mission-critical agent loops.
   - *Dual-SDK Parity*: Divergent mathematical algorithms or feature drift between Python and TypeScript SDKs.
   - *UI Craft & Ergonomics*: Accidental regressions in dashboard accessibility, inconsistent font styling, emojis, neon glows, or oversized status badges.
3. **Reproducible Independent Attestation**: An auditor or automated CI pipeline must be able to execute an acceptance harness against raw code and UI artifacts, producing a canonically hashed cryptographic attestation receipt (`sw:acc:v1`) that proves integrity before deployment.

## Decision
We implement **Seam 60: Enterprise Acceptance Harness (`AcceptanceHarness`)**:
1. **Acceptance Invariant**:
   - A release candidate is accepted (`ACCEPTANCE_CERTIFIED`) if and only if **all configured gates pass with zero regressions**.
   - Any single security breach, interoperability gap, unacknowledged loss, latency regression, SDK disparity, or UI craft defect triggers immediate **fail-closed rejection** (`ACCEPTANCE_REJECTED`) with specific violation details.
2. **The Six Acceptance Gates**:
   - **`SecurityGate`**:
     - Enforces production mode rejecting unauthenticated `ADMIN` bypasses.
     - Validates tenant authorization and step-up MFA challenge requirements on all state-changing endpoints.
     - Tests resilience against malformed JSON, prompt-injection sequences, and XSS script tags.
   - **`InteroperabilityGate`**:
     - Validates cross-runtime OTLP HTTP trace submission (`/v1/traces`).
     - Verifies gRPC streaming message framing and Swarm protobuf serialization.
     - Asserts RFC-8785 canonical encoding byte equality across platforms.
   - **`LossGate`**:
     - Asserts loss-aware causal ingress accounting under queue saturation.
     - Verifies monotonic sequence numbers and explicit gap detection.
     - Enforces fail-closed rejection for lossy schema migrations lacking preservation deltas.
   - **`LatencyGate`**:
     - Enforces latency performance floors: in-process evaluation, Shannon entropy calculation, canonical hashing, and ring buffer operations must execute in $<250\mu\text{s}$.
   - **`ParityGate`**:
     - Verifies 100% mathematical and behavioral parity between Python and TypeScript implementations.
     - Evaluates shared golden vectors for DP Laplace perturbation, epistemic contradiction detection, and causal inference.
   - **`UiCraftGate`**:
     - Directly parses and scans the raw `dashboard.html` artifact.
     - Asserts background palette `#080B11`.
     - Asserts zero emojis across UI templates.
     - Asserts zero neon glow box-shadow anti-patterns.
     - Asserts tabular monospace numerals (`font-feature-settings: "tnum" 1`).
     - Programmatically scans every `.fleet-badge` and asserts all badge text lengths are $\le 30$ characters and uppercase.
3. **Deep Module Interface (`AcceptanceHarness`)**:
   - `AcceptanceHarness.run(profile: str = "production", custom_checks: Optional[List[str]] = None) -> AcceptanceReport`:
     Executes configured gates across the scenario matrix and returns structured evaluation results.
   - `AcceptanceHarness.attest(report: AcceptanceReport, signer_key: Optional[str] = None) -> AcceptanceAttestation`:
     Generates an append-only, canonically hashed attestation receipt with domain tag `sw:acc:v1`.
   - `AcceptanceHarness.verify_attestation(attestation: AcceptanceAttestation, public_key: Optional[str] = None) -> bool`:
     Cryptographically verifies that an attestation receipt has not been tampered with.
   - `AcceptanceHarness.audit_dashboard_artifact(html_path: str) -> Dict[str, Any]`:
     Performs automated structural and visual AST analysis on `dashboard.html`.
   - `AcceptanceHarness.status() -> Dict[str, Any]` & `reset() -> None`:
     Returns execution status, pass rates, and attestation history.
4. **Attestation Receipts & Canonical Tag**:
   - Canonical Domain Tag: `CanonicalDomainTag.ACCEPTANCE = "sw:acc:v1"`.
   - Receipts are identified by `acc-attest-<hex>` containing SHA-256 hashes of gate outputs and timestamps.
5. **Zero-Token & Dual-SDK Guarantee**:
   - 100% deterministic local in-process evaluation with 0 external LLM API calls.
   - Full dual-SDK parity across Python (`acceptance.py`) and TypeScript (`acceptance.ts`).
   - Integrated with CLI (`swarmwatch acceptance`), REST API (`/api/v1/acceptance/*`), and Web Console (`#acceptanceBadge`).

## Consequences
- **Positive**:
  - Replaces vague claims with programmatic, reproducible proof of correctness and security.
  - Blocks deployment if any gate fails, preventing regressions from slipping into production.
  - Directly audits the physical `dashboard.html` file to enforce the craft floor continuously.
  - Provides cryptographic attestation receipts verifiable by third-party auditors.
- **Negative**:
  - Running comprehensive acceptance suites adds a minor execution step (~10-50ms) to the release pipeline.
