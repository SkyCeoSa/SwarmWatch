# ADR 0070: Migration Preservation & Partial Isomorphisms (Seam 59)

## Status
Accepted

## Context
In evolving multi-agent frameworks and long-running distributed traces, state schemas change continuously (field renames, type conversions, field deprecations, structural nesting/flattening).
1. **The Flaw of Blanket "Reversible" Claims**: Earlier schema migration engines (such as Seam 47 / ADR 0058) asserted universal reversibility of migration graphs. However, as identified in the independent software audit, operations like `REMOVE_DEPRECATED`, downcasting types (e.g. float to integer, losing decimal precision), or replacing raw fields with computed aggregations are inherently lossy. Without preserving the deleted or truncated source fields, reverse round-trips cannot reconstruct the original state faithfully.
2. **Silent Failure and Information Fabrication**: When a lossy migration is reversed without source preservation, naive engines either drop the missing fields silently or fabricate synthetic default values. This corrupts causal forensic records, invalidates cryptographic state hashes, and creates false audit trails.
3. **Formal Requirement for Versioned Partial Isomorphisms**: A migration engine must formally distinguish between true isomorphisms (lossless round-trips) and partial isomorphisms (lossy forward steps requiring an explicit preservation delta $\Delta$ for exact inverse reconstruction).
4. **Fail-Closed Reverse Execution**: If a migration is lossy and no preservation delta was captured or provided, attempting to reverse the transformation must fail explicitly rather than pretend to succeed.

## Decision
We implement **Seam 59: Migration Preservation & Partial Isomorphisms (`SchemaEvolution`)**:
1. **Mathematical Model — Versioned Partial Isomorphisms**:
   - A migration step between schema version $V_A$ and $V_B$ is modeled as a tuple $(f, g, \text{LossClass})$:
     - Forward transformation: $f(x) \to (y, \Delta)$ where $x \in \mathcal{S}_A$, $y \in \mathcal{S}_B$, and $\Delta \in \mathcal{P}$ is the `PreservedDelta`.
     - Inverse reconstruction: $g(y, \Delta) \to x$ where $g(f(x).y, f(x).\Delta) = x$ identically.
2. **Loss Classification**:
   - `LOSSLESS`: True isomorphism ($\Delta = \emptyset$). Forward transformation preserves all information (e.g. pure renames, non-narrowing casts, non-colliding nest/flatten). Inverse $g(y)$ succeeds without requiring $\Delta$.
   - `LOSSY_PRESERVED`: Partial isomorphism ($\Delta \neq \emptyset$). Forward transformation drops deprecated fields, truncates precision, or aggregates data. Exact reversal $g(y, \Delta) = x$ succeeds if and only if $\Delta$ is supplied.
   - `IRREVERSIBLE`: Forward transformation where no inverse $g$ exists or reverse is explicitly prohibited. Calling `reverse()` fails immediately.
3. **Migration Preservation Invariant**:
   - When migrating forward with `preserve_shadow=True` (default), all removed fields, pre-cast values, and overwritten keys are stored in `PreservedDelta` with a canonical domain hash (`sw:evo:v1`).
   - If `reverse()` is invoked on a `LOSSY_PRESERVED` migration without a valid `PreservedDelta`, it MUST fail closed with `LossyMigrationError`, enumerating the exact missing fields and precision losses.
   - If any step in a migration chain is `IRREVERSIBLE`, attempting `reverse()` raises `UnsupportedReverseMigrationError`.
4. **Deep Module Interface (`SchemaEvolution`)**:
   - `SchemaEvolution.plan(source_version, target_version) -> EvolutionPlan`: Computes shortest path and evaluates total lossiness (`LOSSLESS`, `LOSSY_PRESERVED`, `IRREVERSIBLE`).
   - `SchemaEvolution.migrate(payload, source_version, target_version, preserve_shadow=True) -> MigrationResult`: Executes forward migration, captures $\Delta$, and returns transformed payload with classification.
   - `SchemaEvolution.reverse(payload, source_version, target_version, preserved_delta=None) -> MigrationResult`: Inverts transformation; verifies $\Delta$ if lossy; fails closed if delta is missing.
   - `SchemaEvolution.classify_loss(source_version, target_version, payload) -> Dict[str, Any]`: Pre-flight audit reporting whether payload will experience loss, exact affected keys, and classification.
   - `SchemaEvolution.register_step(step: EvolutionStep) -> None`: Registers discrete version transitions with explicit lossiness classifications.
   - `SchemaEvolution.status() -> Dict[str, Any]` & `reset() -> None`: Operational diagnostics and state cleanup.
5. **Zero-Token & Dual-SDK Guarantee**:
   - 100% in-process deterministic execution (<25µs per payload).
   - Zero external LLM calls or cloud service dependencies.
   - Full dual-SDK parity across Python (`schema_evolution.py`) and TypeScript (`schema_evolution.ts`).
   - Canonical hashing with tag `CanonicalDomainTag.EVOLUTION` (`"sw:evo:v1"`).

## Consequences
- **Positive**:
  - Replaces blanket "reversible" assertions with mathematically sound partial isomorphisms.
  - Guarantees byte-for-byte state recovery across lossy schema migrations when preservation delta is provided.
  - Fail-closed error handling prevents silent data truncation and fabricated defaults.
  - Complete pre-flight visibility into field-level migration impacts via `classify_loss()`.
- **Negative**:
  - Storing `PreservedDelta` introduces slight in-memory overhead when preserving large dropped dictionaries until committed or discarded.
