# ADR 0051: Multi-Agent Byzantine Attack & Prompt Injection Stress Harness (Seam 40)

## Context & Problem Statement
In production multi-agent LLM systems, agents operate collaboratively by passing messages, tool invocations, and shared state snapshots. However, swarms are vulnerable to Byzantine failure modes and adversarial attacks:
1. **Adversarial Prompt Injection**: Rogue agents or malicious user inputs inject prompt escapes (`SYSTEM OVERRIDE: Ignore all previous instructions...`, jailbreaks) attempting to compromise subsequent agent personas.
2. **State Memory Poisoning**: An agent writes conflicting, corrupted, or malicious key-value states to shared memory, which cascades across downstream agents.
3. **Entropy Jamming & Denial of Service**: Attackers repeatedly emit repetitive low-entropy loops to force premature circuit breaker trips or flood swarms with white-noise tokens to exhaust financial quotas.
4. **Sybil Contagion**: A compromised agent impersonates peers, fabricating consensus votes or corrupting causal interaction graphs.

Prior to Seam 40, SwarmWatch possessed reactive circuit breakers and static data sanitizers, but lacked an automated **adversarial stress test harness** to actively inject Byzantine anomalies, measure swarm resilience, compute quantitative Byzantine Fault Tolerance (BFT) metrics, and quarantine compromised agents.

## Decision
We implement **Seam 40: Multi-Agent Byzantine Attack & Prompt Injection Stress Harness**, providing an in-process, deterministic adversarial stress testing framework and Byzantine resilience scoring engine.

### Core Mathematical & Architectural Primitives

1. **Adversarial Attack Vectors (`ByzantineAttackType`)**:
   - `PROMPT_INJECTION`: Evaluates prompt override attempts, persona hijack payloads, and instruction leakage patterns.
   - `STATE_POISONING`: Injects corrupted, high-entropy, or conflicting key-value state mutations into the active context.
   - `ENTROPY_JAMMING`: Injects artificial zero-entropy repeating phrases or high-entropy pseudo-random white noise.
   - `SYBIL_CONTAGION`: Simulates agent impersonation and conflicting peer signals in multi-agent graph topologies.

2. **Byzantine Fault Tolerance (BFT) Resilience Metric**:
   The stress harness calculates the quantitative resilience score $B_{\text{resilience}} \in [0.0, 1.0]$:
   $$B_{\text{resilience}} = \left(\frac{N_{\text{intercepted}} + N_{\text{isolated}}}{N_{\text{total\_attacks}}}\right) \times (1 - C_{\text{contagion}})$$
   where:
   - $N_{\text{intercepted}}$: Attacks detected and blocked by circuit breaker, sanitizers, or guardrails before reaching peer agents.
   - $N_{\text{isolated}}$: Attacks where the offending agent was quarantined from future interactions.
   - $C_{\text{contagion}} \in [0.0, 1.0]$: Ratio of downstream peer agents whose Shannon entropy or outputs were destabilized by the poisoned context.

3. **Autonomous Agent Quarantine (`AgentQuarantinePolicy`)**:
   When an agent exceeds the maximum allowed adversarial violation threshold, the harness triggers an immediate quarantine:
   - Removes the agent from the active interaction graph.
   - Rejects future span submissions from the quarantined agent ID.
   - Emits a quarantine alert over WebSockets and control plane metrics.

4. **Zero-Token Invariant & Microsecond Execution**:
   - Injections, signature pattern matching, and resilience evaluations execute 100% in-process with pure Python / TypeScript string and numeric operations.
   - Zero external LLM API calls, zero cloud dependencies.
   - Turn evaluation latency strictly $<40\mu\text{s}$.

### Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/byzantine.py` (`ByzantineAttackType`, `ByzantineFaultProfile`, `ByzantineResilienceReport`, `ByzantineStressHarness`, `global_byzantine_harness`).
- **TypeScript**: `packages/sdk-typescript/src/byzantine.ts` conforming strictly to Node 22 `--experimental-strip-types`.

### Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/swarms/{id}/byzantine/status`: Retrieve active resilience report and quarantined agents list.
  - `POST /api/v1/swarms/{id}/byzantine/inject`: Trigger synthetic adversarial injection.
  - `POST /api/v1/swarms/{id}/byzantine/clear`: Clear active attacks and reset quarantined agents.
- **CLI Subcommand**:
  - `swarmwatch byzantine status|inject|clear`
- **Web Console (`dashboard.html`)**:
  - `#byzantineBadge` in `#fleetBar`: displays real-time BFT shield status (e.g., `BYZANTINE: SHIELDED (100%)`).

## Consequences
### Positive
- Enables automated security regression testing and adversarial prompt injection fuzzing for multi-agent applications.
- Quantifies swarm Byzantine resilience with mathematical rigor.
- Provides immediate quarantine isolation to prevent systemic swarm infection.

### Negative / Trade-offs
- Synthetic attack injection must be carefully isolated to designated test environments to avoid disrupting live production pipelines.
