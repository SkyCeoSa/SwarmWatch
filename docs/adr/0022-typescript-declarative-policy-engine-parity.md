# ADR 0022: TypeScript Declarative Policy Engine Parity & Zero-Dependency DSL

## Status
Accepted

## Context
In ADR 0021 (Seam 12), SwarmWatch introduced a declarative YAML/JSON guardrail policy DSL, role-based inheritance, and sub-microsecond in-memory policy evaluation in Python. To maintain SwarmWatch's dual-SDK architectural guarantee (Python and TypeScript parity across all core seams), TypeScript-based multi-agent swarms (e.g., using Vercel AI SDK, LangChain.js, Mastra, or custom orchestrators) require an identical declarative guardrail engine.

JavaScript and Node.js ecosystems commonly rely on external heavy dependencies like `js-yaml` or `yaml` for YAML parsing. However, SwarmWatch enforces a strict zero-runtime-dependency invariant for `@swarmwatch/sdk` to avoid supply chain vulnerabilities, version conflicts, and overhead.

## Decision
1. **Zero-Dependency SafePolicyParser in TypeScript**:
   - Implement `SafePolicyParser` in `packages/sdk-typescript/src/policy.ts` using built-in standard library APIs (`node:fs`, `node:path`, `JSON.parse`).
   - Implement an indentation-aware YAML parser supporting nested objects, list items (`- item`), scalar type casting (booleans, integers, floats, strings), inline lists (`[a, b]`), and comment stripping.
   - Provide file loading via `fromFile(filePath)` and in-memory loading via `fromYaml(content)`, `fromJson(content)`, and `fromObject(data)`.

2. **Zero-Dependency Wildcard Matching**:
   - Implement `globMatch(str, pattern)` in pure TypeScript without external glob dependencies, supporting `*` and `?` patterns for agent roles and tool names.

3. **High-Performance In-Memory PolicyEngine**:
   - `getEffectiveConfig(agentName)`: Resolves hierarchical inheritance by layering matching agent configurations over global defaults in $<1\mu s$.
   - `evaluateTurn({ agent, text, token_count, tool_name, payload, entropy_score, similarity_score })`: Evaluates prohibited tools, entropy safety floors, similarity ceilings, token budgets, and declarative event-condition-action rules.
   - `evaluateSpan(span)`: Evaluates standard causal `SwarmSpan` records against the active guardrail document.
   - `hotReload(policy)`: Dynamically swaps the active policy document at runtime without server/process restart.

4. **Deep Circuit Breaker Integration**:
   - Extend `EntropyCircuitBreaker` in `packages/sdk-typescript/src/circuit.ts` to accept an optional `policyEngine?: PolicyEngine`.
   - Automatically pull effective role-specific thresholds (`entropy_threshold`, `similarity_threshold`, `token_budget`, `max_turns`) and immediately trip when prohibited tools are invoked.

## Consequences
- **Positive**:
  - Complete 1:1 dual-SDK parity across Python and TypeScript for declarative guardrails.
  - Zero third-party npm runtime dependencies maintained.
  - Sub-microsecond deterministic evaluation with zero external LLM token burn.
  - Seamless interoperability with Next.js, Node.js microservices, and serverless edge runtimes.
- **Negative**:
  - The YAML parser supports standard configuration subsets (mappings, lists, scalar types, comments) rather than full YAML 1.2 spec features like anchors or binary blobs, which is ideal and sufficient for guardrail configuration.
