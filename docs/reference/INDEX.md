# SwarmWatch Canonical Reference Handbook

Welcome to the central reference repository for **SwarmWatch**. This handbook contains the authoritative technical, algorithmic, architectural, and operational documentation for both human developers and autonomous AI agents.

Whenever beginning a new session or designing new features, consult this index to align with project standards and domain contracts.

---

## Document Catalog

1. [01. Research & Market Landscape](./01-RESEARCH.md)
   - Industry context, competitor teardown (Langfuse, Phoenix, LangSmith, Portkey), and zero-token mathematical foundations (Shannon Entropy, Jaccard similarity).
2. [02. Project Roadmap & Milestones](./02-ROADMAP.md)
   - Completed foundations (Python SDK, SQLite WAL, Circuit Breaker, Console) and upcoming enterprise horizons (ClickHouse, Cloud SaaS, OpenTelemetry).
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

---

## Quick Reference Verification Matrix

| Component | Verification Command | Expected Standard |
| :--- | :--- | :--- |
| **Python Test Suite** | `.venv\Scripts\pytest.exe packages\sdk-python\tests\` | 100% Pass (43/43 tests) |
| **TypeScript Test Suite** | `node.exe --experimental-strip-types --test packages/sdk-typescript/tests/*.test.js` | 100% Pass (33/33 tests) |
| **Console Craft Floor** | `impeccable.cmd detect --json dashboard.html` | 0 Anti-Patterns (`[]`) |
| **Local Dashboard Server** | `.venv\Scripts\python.exe -m swarmwatch.cli ui --port 8765` | `http://127.0.0.1:8765` Active |
| **Git Working Tree** | `git status -s` | Clean, synchronized with `origin/main` |

