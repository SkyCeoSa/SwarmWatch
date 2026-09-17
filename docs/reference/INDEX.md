# SwarmWatch Canonical Reference Handbook

Welcome to the central reference repository for **SwarmWatch**. This handbook contains the authoritative technical, algorithmic, architectural, and operational documentation for both human developers and autonomous AI agents.

Whenever beginning a new session or designing new features, consult this index to align with project standards and domain contracts.

---

## Document Catalog

1. [01. Research & Market Landscape](./01-RESEARCH.md)
   - Industry context, competitor teardown (Langfuse, Phoenix, LangSmith, Portkey), and zero-token mathematical foundations (Shannon Entropy, Jaccard similarity).
2. [02. Project Roadmap & Milestones](./02-ROADMAP.md)
   - Completed milestones (Core Engine, Circuit Breaker, Real-time Console, Columnar ClickHouse Storage, TypeScript SDK, Multi-Tenant Architecture & Distributed Fleet Sync) and SaaS horizons.
3. [03. Project Policies & Engineering Standards](./03-POLICIES.md)
   - Deep module code standards, zero-token cost guarantees, Impeccable craft floor rules, and data privacy/secret redaction policies.
4. [04. Mandatory Workflows & Skills](./04-WORKFLOWS-AND-SKILLS.md)
   - Exhaustive operational guide for Matt Pocock's skills (`diagnosing-bugs`, `domain-modeling`, `codebase-design`, `tdd`, `writing-for-agents`) and the Impeccable design suite.
5. [05. System Architecture & Seams](./05-ARCHITECTURE-AND-SEAMS.md)
   - The 6 system seams, dataflow diagrams, thread boundaries, and non-negotiable architectural invariants.
6. [06. Domain Glossary & Ubiquitous Language](./06-DOMAIN-GLOSSARY.md)
   - Strict terminology, single-sentence definitions, and banned synonyms (_Avoid_).
7. [07. Operations & Developer Runbook](./07-OPERATIONS-RUNBOOK.md)
   - Practical commands for running the server, executing the test suite, running headless CDP tests, and validating UI anti-patterns.
8. [08. Commercialization & Open-Core Model](./08-COMMERCIALIZATION-AND-OPEN-CORE.md)
   - Monetization strategy: Open-source local-first engine vs. Enterprise Cloud SaaS.
9. [Architecture Decision Records (ADRs)](../adr/)
   - [ADR 0001: In-Process Sub-Microsecond Telemetry Queue](../adr/0001-in-process-sub-microsecond-telemetry-queue.md)
   - [ADR 0002: Zero-Token Shannon Entropy Circuit Breaker](../adr/0002-zero-token-shannon-entropy-circuit-breaker.md)
   - [ADR 0003: Git-Like DAG Branching for State Time-Machine](../adr/0003-git-like-dag-branching-for-state-time-machine.md)
   - [ADR 0004: Hybrid Ecosystem Adapters Without Monkey Patching](../adr/0004-hybrid-ecosystem-adapters-without-monkey-patching.md)
   - [ADR 0005: Hybrid Storage Strategy](../adr/0005-hybrid-storage-strategy.md)
   - [ADR 0006: Counterfactual Token Savings Estimation](../adr/0006-counterfactual-token-savings-estimation.md)
   - [ADR 0007: Enterprise Webhook Notifications](../adr/0007-enterprise-webhook-notifications.md)
   - [ADR 0008: OpenTelemetry GenAI Federation](../adr/0008-opentelemetry-genai-federation.md)
   - [ADR 0009: TypeScript SDK Architecture & Monorepo Topology](../adr/0009-typescript-sdk-and-monorepo-topology.md)
   - [ADR 0010: Multi-Tenant Isolation & Cryptographic API Keys](../adr/0010-multi-tenant-isolation-and-api-keys.md)
   - [ADR 0011: Distributed Fleet Circuit Synchronization](../adr/0011-fleet-circuit-synchronization.md)
   - [ADR 0012: Zero-Cloud Self-Hosted Containerized Deployment](../adr/0012-self-hosted-containerized-deployment.md)
   - [ADR 0013: Zero-Cloud Distribution Package Validation](../adr/0013-zero-cloud-distribution-package-validation.md)
   - [ADR 0014: Enterprise Prometheus Metrics Exporter](../adr/0014-enterprise-prometheus-metrics-exporter.md)
   - [ADR 0015: LlamaIndex & Microsoft AutoGen Framework Adapters](../adr/0015-llamaindex-and-autogen-framework-adapters.md)
   - [ADR 0016: Interactive Chaos & Loop Simulator in Developer Console](../adr/0016-interactive-chaos-simulator.md)
   - [ADR 0017: Visual Causal DAG Graph View & Cycle Topology in Developer Console](../adr/0017-visual-causal-dag-graph-view.md)
   - [ADR 0018: DSPy and Microsoft Semantic Kernel Framework Adapters](../adr/0018-dspy-and-semantic-kernel-framework-adapters.md)
    - [ADR 0019: Enterprise Security & Automated Secret Redaction Engine](../adr/0019-enterprise-security-and-secret-redaction-engine.md)
    - [ADR 0020: Incident Post-Mortem & Financial ROI Report Generator](../adr/0020-incident-post-mortem-and-roi-report-engine.md)
    - [ADR 0021: Declarative Guardrail Policy Engine & Policy DSL](../adr/0021-declarative-guardrail-policy-dsl.md)

---

## Quick Reference Verification Matrix

| Component | Verification Command | Expected Standard |
| :--- | :--- | :--- |
| **Python Test Suite** | `.venv\Scripts\pytest.exe packages\sdk-python\tests\` | 100% Pass (114/114 tests) |
| **TypeScript Test Suite** | `cmd.exe /c npm.cmd test` (in `packages/sdk-typescript/`) | 100% Pass (70/70 tests) |
| **Prometheus Exporter** | `curl http://127.0.0.1:8765/metrics` | 100% OpenMetrics 0.0.4 Valid |
| **Package Distribution Integrity** | `python scripts\verify_release.py` | 100% Valid (Wheel & Tarball) |
| **Console Craft Floor** | Inspect `dashboard.html` (`#080B11`, `tabular-nums`) | 0 Anti-Patterns |
| **Local Dashboard Server** | `.venv\Scripts\swarmwatch.exe ui --port 8765` | `http://127.0.0.1:8765` Active |
| **Live Interactive Tour** | `.venv\Scripts\python.exe scripts\run_live_demo.py` | End-to-end Simulation Complete |
| **Git Working Tree** | `git status -s` | Clean, synchronized with `origin/main` |
