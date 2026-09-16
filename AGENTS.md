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

## Context Pointers

- **[docs/reference/INDEX.md](./docs/reference/INDEX.md)**: **Canonical Reference Handbook** (Research, Roadmap, Policies, Workflows, Seams, Glossary, Runbook, and Monetization). **Consult this first in any new session.**
- [CONTEXT.md](./CONTEXT.md): Ubiquitous domain vocabulary and core concepts. Consult whenever naming new entities or APIs.
- [ARCHITECTURE.md](./ARCHITECTURE.md): Deep module interfaces, seams, and system invariants.
- [docs/adr/](./docs/adr/): Sequential Architecture Decision Records (ADR 0001 through 0013).
- [docs/adr/0013-zero-cloud-distribution-package-validation.md](./docs/adr/0013-zero-cloud-distribution-package-validation.md): Package distribution validation architecture and zero-cloud integrity engine.
- [PRODUCT.md](./PRODUCT.md): Product thesis, user stories, and craft floor requirements.
- [DESIGN.md](./DESIGN.md): Design tokens, typography, layout, and component rules.

