# ADR 0009: TypeScript SDK Architecture & Monorepo Package Topology

## Status
Accepted

## Context
Multi-agent systems are rapidly expanding in TypeScript and JavaScript runtimes (Vercel AI SDK, Mastra, LangChain.js, Node.js, Bun, Edge functions). Developers in these ecosystems suffer from the identical failure modes as Python developers: semantic deadlock, cyclic loop stagnation, and silent token burn. SwarmWatch must provide a first-class, idiomatic TypeScript SDK (`@swarmwatch/sdk`).

## Decision
We create `packages/sdk-typescript/` within the repository with the following architectural invariants:
1. **Zero External Runtime Dependencies**:
   - Normalized Shannon entropy ($H_{norm}$) and sliding-window Jaccard lexical similarity ($J$) implemented purely in native TypeScript using fast regex tokenizers and ES6 Sets.
   - Zero LLM API calls, zero token burn.
2. **Deep Module Parity with Python SDK**:
   - `SwarmClient`: Core client managing non-blocking ring buffer queue and span contexts.
   - `EntropyCircuitBreaker`: Mathematical governor with strict, balanced, and lenient policies, max turn ceilings, and counterfactual savings calculation.
   - `SwarmSpan` & `StateSnapshot`: Immutable, serializable schema identical to the Python core schema.
3. **Ecosystem Adapters**:
   - `VercelAiMiddleware`: Integration for Vercel AI SDK `generateText` / `streamText` `onFinish` callbacks.
   - `LangChainJsHandler`: `BaseCallbackHandler` for LangChain.js agent graphs.
4. **Dual Build & Local Test Runner**:
   - Configured with `package.json`, `tsconfig.json`, and comprehensive unit tests using Node.js native test runner (`node --test`).

## Consequences
- **Positive**: Complete feature and architectural parity across Python and TypeScript.
- **Positive**: Zero native binary compilation issues across Linux, macOS, and Windows.
- **Positive**: Sub-microsecond execution on V8 event loops.
