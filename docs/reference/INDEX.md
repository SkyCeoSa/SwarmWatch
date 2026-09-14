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

---

## Quick Reference Verification Matrix

| Component | Verification Command | Expected Standard |
| :--- | :--- | :--- |
| **Python Test Suite** | `.venv\Scripts\pytest.exe packages\sdk-python\tests\` | 100% Pass (45+ tests) |
| **TypeScript Test Suite** | `node.exe --experimental-strip-types --test packages/sdk-typescript/tests/*.test.js` | 100% Pass (42/42 tests) |
| **Console Craft Floor** | `impeccable.cmd detect --json dashboard.html` | 0 Anti-Patterns (`[]`) |
| **Local Dashboard Server** | `.venv\Scripts\python.exe -m swarmwatch.cli ui --port 8765` | `http://127.0.0.1:8765` Active |
| **Git Working Tree** | `git status -s` | Clean, synchronized with `origin/main` |

