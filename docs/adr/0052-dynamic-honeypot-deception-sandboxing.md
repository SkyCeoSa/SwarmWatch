# ADR 0052: Multi-Agent Dynamic Honeypot & Deception Sandboxing

- **Status**: Accepted
- **Date**: 2026-09-24
- **Context**: SwarmWatch Seam 41 (Multi-Agent Dynamic Honeypot & Deception Sandboxing)

---

## 1. Context and Problem Statement
In multi-agent collaborative LLM systems, simply terminating an adversarial agent or returning a hard error when prompt injections, state poisoning, or credential exfiltration are detected alerts the adversary immediately. The adversary can quickly adapt their prompt engineering, rotate agent identities, or retry with slightly perturbed bypasses.

To observe, profile, and defuse adversarial agents without alerting them or risking real production state, SwarmWatch requires an in-process, deterministic **Deception Sandboxing & Honeypot Engine**.

## 2. Decision & Architectural Principles
We implement `HoneypotManager`, `DeceptionSandbox`, and `HoneytokenGenerator` as a deep module:

1. **Canary Honeytokens (`HoneytokenGenerator`)**:
   - Injects cryptographically identifiable synthetic canary credentials (`sw_canary_...`, `sw_honey_...`) into simulated agent environments.
   - Any agent attempting to exfiltrate or query these tokens immediately trips the honeypot detection mechanism.

2. **Deception Sandbox (`DeceptionSandbox`)**:
   - Diverts compromised or quarantined agents to an isolated in-process shadow interaction graph.
   - Generates deterministic, plausible synthetic responses (dummy database schemas, fake tokens, simulated command execution) satisfying the agent's prompt while preventing real downstream actions.

3. **Diversion Router (`HoneypotDiversionRouter`)**:
   - Inspects in-flight spans.
   - If an agent is in the diversion roster or accesses canary tokens, subsequent spans are intercepted and served synthetic responses from the sandbox without propagating changes to the active causal DAG.

4. **Zero-Token Cost Guarantee**:
   - 100% in-process local execution (<50µs per turn).
   - Zero remote LLM inference calls for canary generation, shadow state emulation, or payload profiling.

5. **Dual-SDK Parity & Impeccable Craft Floor**:
   - Python: `packages/sdk-python/swarmwatch/honeypot.py`
   - TypeScript: `packages/sdk-typescript/src/honeypot.ts` adhering strictly to Node 22 type-stripping rules (`node --experimental-strip-types`).
   - Web Console: `#honeypotBadge` in `#fleetBar` of `dashboard.html` with 0 anti-patterns (`[]`).

## 3. Consequences
- **Positive**:
  - Traps adversarial agents in a closed deception loop without alerting them.
  - Generates comprehensive forensic telemetry of attack payloads.
  - Zero performance impact on benign agents.
- **Negative**:
  - Requires maintaining plausible synthetic response templates for common adversarial queries.
