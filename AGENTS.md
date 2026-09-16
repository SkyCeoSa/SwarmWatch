# SwarmWatch Engineering & Agent Guidelines

This repository strictly adheres to Matt Pocock's software engineering and autonomous agent principles:
- **Domain Modeling**: Ubiquitous language defined in [CONTEXT.md](./CONTEXT.md). All code, variables, and tests must use these exact terms.
- **Architecture Decisions**: Significant, hard-to-reverse architectural choices documented in sequential ADRs under [docs/adr/](./docs/adr/).
- **Deep Modules & Seams**: Narrow interfaces hiding deep implementations documented in [ARCHITECTURE.md](./ARCHITECTURE.md).
- **Test-Driven Development (TDD)**: 100% green test suite across all adapters, storage lifecycle, and transport queue. Run tests via:
  ```powershell
  .venv\Scripts\pytest.exe packages\sdk-python\tests\
  ```
- **Console & Design Contract**: High-density Impeccable console documented in [PRODUCT.md](./PRODUCT.md) and [DESIGN.md](./DESIGN.md). Must pass zero-finding detector:
  ```powershell
  impeccable.cmd detect --json dashboard.html
  ```
- **Package Release & Distribution Integrity**: Zero-dependency validation of Python wheel and npm tarball packages per [ADR 0013](./docs/adr/0013-zero-cloud-distribution-package-validation.md). Must exit 0 with all checks passing:
  ```powershell
  python scripts/verify_release.py --json
  ```
- **Enterprise Operational Metrics Exporter**: Prometheus 0.0.4 / OpenMetrics exposition (`GET /metrics`) per [ADR 0014](./docs/adr/0014-enterprise-prometheus-metrics-exporter.md). Must pass unit and integration test suite:
  ```powershell
  .venv\Scripts\pytest.exe packages\sdk-python\tests\test_metrics_prometheus.py
  ```
- **Ecosystem Framework Adapters**: Non-invasive integration for LangGraph, CrewAI, LlamaIndex, and AutoGen per [ADR 0004](./docs/adr/0004-hybrid-ecosystem-adapters-without-monkey-patching.md) and [ADR 0015](./docs/adr/0015-llamaindex-and-autogen-framework-adapters.md). Must pass adapter test suites:
  ```powershell
  .venv\Scripts\pytest.exe packages\sdk-python\tests\test_adapters_*.py
  ```

- **Interactive Chaos & Loop Simulator**: Deterministic failure injection panel in Developer Console per [ADR 0016](./docs/adr/0016-interactive-chaos-simulator.md). Must pass unit and endpoint test suite:
  ```powershell
  .venv\Scripts\pytest.exe packages\sdk-python\tests\test_chaos_simulator.py
  ```

## Context Pointers

- **[docs/reference/INDEX.md](./docs/reference/INDEX.md)**: **Canonical Reference Handbook** (Research, Roadmap, Policies, Workflows, Seams, Glossary, Runbook, and Monetization). **Consult this first in any new session.**
- [CONTEXT.md](./CONTEXT.md): Ubiquitous domain vocabulary and core concepts. Consult whenever naming new entities or APIs.
- [ARCHITECTURE.md](./ARCHITECTURE.md): Deep module interfaces, seams, and system invariants.
- [docs/adr/](./docs/adr/): Sequential Architecture Decision Records (ADR 0001 through 0016).
- [docs/adr/0013-zero-cloud-distribution-package-validation.md](./docs/adr/0013-zero-cloud-distribution-package-validation.md): Package distribution validation architecture and zero-cloud integrity engine.
- [docs/adr/0014-enterprise-prometheus-metrics-exporter.md](./docs/adr/0014-enterprise-prometheus-metrics-exporter.md): Enterprise Prometheus metrics exporter & OpenMetrics operational observability.
- [docs/adr/0015-llamaindex-and-autogen-framework-adapters.md](./docs/adr/0015-llamaindex-and-autogen-framework-adapters.md): Non-invasive LlamaIndex and AutoGen framework adapters.
- [docs/adr/0016-interactive-chaos-simulator.md](./docs/adr/0016-interactive-chaos-simulator.md): Interactive Chaos & Loop Simulator in Developer Console.
- [PRODUCT.md](./PRODUCT.md): Product thesis, user stories, and craft floor requirements.
- [DESIGN.md](./DESIGN.md): Design tokens, typography, layout, and component rules.




