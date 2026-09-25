# ADR 0061: Canonical Causal Encoding & Byte-Deterministic Contract Engine

## Status
Accepted

## Context
In cross-runtime distributed multi-agent systems spanning Python 3.12+ and TypeScript/Node.js 22+:
1. **The Cross-Runtime Hash Divergence Hazard**: Python's standard `json.dumps()` and JavaScript's `JSON.stringify()` do not produce identical byte representations for equivalent semantic objects. Minor divergences in key ordering, whitespace separators (`: ` vs `:`), Unicode escape sequences (`\u00e9` vs `é`), floating-point representations (`1.0` vs `1`, `-0.0` vs `0`), boolean/null casing, and dictionary formatting cause SHA-256 state hashes, Merkle leaves, state snapshot diffs, and cryptographic audit receipts to diverge across runtimes.
2. **Hash Malleability & Cross-Context Substitution**: Without cryptographic domain separation, an adversary could substitute a serialized span commitment leaf as a state snapshot or audit receipt.
3. **Lossy Serialization Pitfalls**: Non-canonical serialization silently tolerates unrepresentable types (`NaN`, `Infinity`, cyclic objects, non-serializable callables), leading to silent data corruption or post-crash deserialize failures.
4. **Independent Verifiability**: Regulated enterprise compliance (EU AI Act, SOC2 Type II) mandates that an independent auditor running any runtime (Python, Node.js, Go, or Rust) must verify identical cryptographic Merkle roots and state snapshot diffs from canonical byte sequences.

## Decision
We implement Seam 50: Canonical Causal Encoding & Byte-Deterministic Contract Engine (`CanonicalCodec`) adhering to RFC 8785 (JSON Canonicalization Scheme - JCS) principles, with domain-separated cryptographic hashing and full dual-SDK parity across Python (`swarmwatch.canonical`) and TypeScript (`@swarmwatch/sdk/src/canonical.ts`).

### 1. Canonical Encoding Scheme $C_v(x)$ (RFC 8785 Aligned)
The canonical encoding function $C_v(x)$ converts any domain object $x$ into deterministic, versioned UTF-8 bytes under the following strict invariants:
- **Lexicographical Key Ordering**: Object keys are sorted lexicographically by Unicode code point values (UTF-16 code units matching RFC 8785).
- **Whitespace Stripping**: Zero insignificant whitespace outside string literals (colons `:` and commas `,` have no leading or trailing spaces).
- **Deterministic Number Serialization**: Integers and IEEE 754 floats are serialized deterministically without unnecessary scientific notation within standard ranges. Negative zero (`-0.0`) is strictly normalized to `0`. `NaN`, `+Infinity`, and `-Infinity` are rejected as illegal.
- **Literal Value Canonicalization**: Booleans serialize strictly as lowercase `true` or `false`. Nulls serialize strictly as lowercase `null`.
- **Unicode Preservation**: Valid UTF-8 characters are encoded directly without unnecessary `\uXXXX` escaping unless required by JSON syntax (control characters `0x00`-`0x1F`, `"`, `\`).
- **Fail-Closed Validation**: Circular references, unrepresentable types, or non-finite numbers fail closed immediately with a descriptive `CanonicalEncodingError`.

### 2. Domain-Separated Cryptographic Hashing
To prevent cross-context substitution attacks, all state and telemetry hashes are computed using prefix domain separation:
$$H_v(\text{domain\_tag}, x) = \text{SHA256}(\text{domain\_tag} \parallel \text{0x00} \parallel C_v(x))$$
Standard domain tags include:
- `sw:leaf:v1`: Span commitment leaves in the Merkle accumulator.
- `sw:snap:v1`: State snapshots in the safe time-machine.
- `sw:rcpt:v1`: Verifiable Merkle audit receipts.
- `sw:pmt:v1`: Multi-agent safety permits.

### 3. Deep Module Interface (`CanonicalCodec`)
A minimal, powerful public interface hiding encoding rules and buffer optimizations:
- `CanonicalCodec.canonicalize(obj, version=1) -> str`
- `CanonicalCodec.encode(obj, version=1) -> bytes`
- `CanonicalCodec.decode(payload) -> Any`
- `CanonicalCodec.hash(obj, domain_tag="", version=1) -> str` (hex-encoded SHA-256)
- `CanonicalCodec.validate(obj) -> (bool, Optional[str])`

### 4. Zero-Token Cost Guarantee
100% in-process deterministic string manipulation and SHA-256 calculation ($<5\mu\text{s}$ per small object, $<50\mu\text{s}$ for complex state). Zero external LLM token expenditure.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/canonical.py`
- **TypeScript**: `packages/sdk-typescript/src/canonical.ts` conforming strictly to Node 22 `--experimental-strip-types`.
- Verified across identical cross-runtime golden vector suites.

## Consequences
### Positive
- Byte-for-byte identical state hashes and Merkle roots between Python and TypeScript.
- Complete immunity to JSON key order malleability and cross-context hash substitution attacks.
- Foundation for truthful Merkle audit receipts, loss-aware ingestion, and distributed safety authority.

### Negative / Trade-offs
- Slight serialization overhead (sorting dictionary keys), mitigated by optimized in-process recursive traversal.
- Rejection of non-standard numbers (`NaN`, `Infinity`) requires callers to sanitize inputs before serialization.
