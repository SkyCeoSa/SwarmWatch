# ADR 0013: Zero-Cloud Distribution Package Validation

## Status
Accepted

## Context
Publishing multi-language observability runtimes across Python (PyPI) and TypeScript (NPM) requires strict pre-flight artifact validation. A distribution package with a missing embedded asset (such as `dashboard.html`), broken export map, unredacted internal test cache, or corrupted archive header degrades the end-user experience and risks production outages. Relying on remote CI pipelines or external publishing dry-runs (like `twine check` or `npm publish --dry-run` over the network) is brittle, slow, and unsuitable for air-gapped environments. We need an in-process, deep validation module that enforces strict mathematical and structural guarantees on compiled binary wheels and npm tarballs locally.

## Decision
We implement `PackageReleaseValidator` as a deep module (`codebase-design`) with a small interface and exhaustive internal verification:

1. **Deep Module Interface**:
   - `PackageReleaseValidator` exposes only high-level operations: `validate_wheel(path)`, `validate_npm_tarball(path)`, and `run_full_suite()`.
   - Returns structured `ArtifactIntegrityReport` and `FullDistributionReport` domain models.

2. **Wheel Package Verification (`.whl`)**:
   - Validates zip stream integrity and header checksums via `zipfile.is_zipfile` and `testzip()`.
   - Parses `METADATA` to enforce PEP 566 compliance, version alignment (`0.1.0`), license declarations, and required dependencies.
   - Enforces the presence and non-trivial byte size (> 30KB) of the embedded Impeccable console (`swarmwatch/dashboard.html`).
   - Verifies entry points in `entry_points.txt` (`swarmwatch = swarmwatch.cli:main`).
   - Prohibits forbidden artifacts (`.pyc`, `__pycache__`, `.env`, secret tokens).

3. **NPM Tarball Verification (`.tgz`)**:
   - Validates tar/gzip stream structure via `tarfile.is_tarfile` and tar stream reading.
   - Inspects `package.json` to verify package scope (`@swarmwatch/sdk`), version alignment (`0.1.0`), module format (`type: module`), and exports maps.
   - Verifies presence of compiled TypeScript ESM modules (`index.ts`, `client.ts`, `circuit.ts`, `tenant.ts`, `transport.ts`, `serializer.ts`, adapters).
   - Enforces absence of sensitive files, local environment configurations, or git artifacts.

4. **Zero-Cloud & Zero-Token Guarantee**:
   - All validation runs completely in-process using standard library modules (`zipfile`, `tarfile`, `hashlib`, `json`, `pathlib`).
   - Zero remote network calls, zero PyPI/NPM credentials required, and zero external LLM tokens consumed.

## Consequences
- **Positive**: Complete local confidence before publishing to public or private artifact registries.
- **Positive**: Single-command execution (`python scripts/verify_release.py`) for maintainers, git pre-commit hooks, and air-gapped CI.
- **Positive**: Strict domain boundary and testability via public interfaces without implementation leakage.
