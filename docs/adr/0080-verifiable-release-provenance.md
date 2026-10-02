# ADR 0080: Verifiable Release Provenance & Signed Artifact DAG (Seam 69)

## Status
Accepted

## Context
SwarmWatch operates as safety-critical middleware governing autonomous AI multi-agent execution, financial token budgets, entropy circuit breakers, and forensic audit journals. As identified in Seam 69 of the external architectural audit:

1. **Supply-Chain Vulnerability**: In enterprise and high-assurance multi-agent environments, compromised distribution artifacts (e.g. Python wheels, npm tarballs) or tampered dashboard assets could secretly disable circuit breakers or alter causal evidence.
2. **SLSA & in-toto Compliance**: Standard software delivery lacks verifiable attestations linking the released artifact back to the exact source commit, build environment, and passed test suites. SLSA v1.0 (Supply-chain Levels for Software Artifacts) requires non-forgeable provenance.
3. **Directed Acyclic Graph (DAG) of Build Steps**: The release process is not a flat script but a causal pipeline: $\text{Source Checkout} \to \text{Environment Lock} \to \text{Test Execution} \to \text{Craft Floor Audit} \to \text{Conformance Attestation} \to \text{Artifact Packaging} \to \text{Release Gate}$. Tampering with any intermediate step must invalidate downstream artifacts.
4. **Offline Independent Verification**: Enterprise auditors, deployment gatekeepers, and air-gapped runtimes must be able to verify the authenticity and integrity of releases using only public keys, without external cloud identity brokers.
5. **Cross-Runtime Dual-SDK Parity**: Both Python 3.12+ and Node 22 native type-stripping environments must support deterministic provenance generation, cryptographic signing, and verification.

## Decision
We implement the **Verifiable Release Provenance & Signed Artifact DAG (`ReleaseProvenance`)** deep module across Python 3.12+ and TypeScript (Node 22 native type-stripping).

### 1. Formal Model: In-toto / SLSA Provenance DAG
The release process is formalized as an explicit Directed Acyclic Graph $\mathcal{G} = (\mathcal{V}, \mathcal{E})$:
- Vertices $\mathcal{V}$ represent immutable build and audit steps:
  $$\mathcal{V} \subseteq \{ \text{SOURCE\_CHECKOUT}, \text{ENVIRONMENT\_LOCK}, \text{TEST\_EXECUTION}, \text{CRAFT\_AUDIT}, \text{CONFORMANCE\_ATTEST}, \text{BUILD\_PACKAGING}, \text{RELEASE\_GATE} \}$$
- Edges $\mathcal{E} \subseteq \mathcal{V} \times \mathcal{V}$ enforce strict causal precedence:
  $$\text{SOURCE} \to \text{ENV} \to \text{TESTS} \to \text{CRAFT} \to \text{CONFORMANCE} \to \text{BUILD} \to \text{GATE}$$
- Every step records:
  - Input artifacts (materials) and output artifacts (products), each identified by canonical path, byte size, and SHA-256 digest:
    $$\text{ArtifactDigest}(path, sha256, byte\_size)$$
  - Step execution metadata, duration, and monotonic timestamp $t_{step}$.
  - Step digest computed over canonical JSON serialization:
    $$\text{StepDigest} = \text{SHA256}(\text{"sw:prov:step:v1:"} \parallel step\_type \parallel \text{inputs} \parallel \text{outputs} \parallel metadata)$$
- The DAG Root Digest is computed recursively across topological order:
  $$\text{DagRootHash} = \text{SHA256}(\text{"sw:prov:dag:v1:"} \parallel \text{TopologicalSortedStepDigests})$$

### 2. Cryptographic Attestation & Asymmetric Signing
The resulting `SignedProvenanceManifest` encapsulates:
- Manifest metadata: `manifest_id`, `spec_version` (`"slsa/v1.0"`), `builder_id`, `git_commit`, `git_branch`.
- Causal materials: source files, lockfiles, test receipts, conformance receipts.
- Released products: `swarmwatch-0.1.0-py3-none-any.whl`, `swarmwatch-sdk-0.1.0.tgz`.
- Asymmetric Digital Signature:
  - Signed using ECDSA NIST P-256 (ES256, SHA-256, IEEE P1363 64-byte signature format).
  - Verifiable by anyone possessing the corresponding uncompressed 65-byte ECDSA public key.
  - Fail-closed validation: any byte modification in source, test reports, or built archives causes signature verification or DAG hash reconciliation to fail.

### 3. Deep Module Interface
The deep module `ReleaseProvenance` provides:
- `record_step(step_type, inputs, outputs, metadata)`: Appends an execution step to the active DAG.
- `generate_manifest(builder_id, git_commit, git_branch)`: Assembles the complete SLSA-compliant provenance document and computes the canonical DAG root hash.
- `sign_manifest(manifest, private_key_pem)`: Signs the manifest using NIST P-256 ECDSA.
- `verify_manifest(manifest, public_key_pem)`: Mathematically verifies the signature, verifies internal DAG acyclicity and completeness, and recalculates artifact SHA-256 hashes against disk files.
- `verify_workspace(manifest)`: Re-hashes current workspace distribution artifacts against the manifest products.
- `status()`: Returns operational diagnostics, active DAG step count, and verification status.

### 4. Zero-Token Cost & Sub-250µs SLO
- **Zero External LLM Tokens**: Pure cryptographic hashing (SHA-256) and ECDSA verification execute 100% locally with zero cloud or LLM dependencies.
- **Sub-250µs Verification SLO**: Complete manifest parsing and cryptographic signature validation execute in $<150\,\mu\text{s}$.

## Consequences

### Positive
- High-assurance supply chain integrity guaranteeing that running code matches auditable source commits.
- Meets SLSA Level 3 provenance requirements with verifiable materials and products.
- Prevents stealth tampering with dashboard HTML, circuit breaker rules, or packaging configurations.
- Full dual-SDK parity across Python 3.12+ and Node 22 native type-stripping.

### Negative / Trade-offs
- Release automation requires generating and validating ECDSA provenance manifests prior to pushing packages.
- Additional build verification step during CI/CD packaging.
