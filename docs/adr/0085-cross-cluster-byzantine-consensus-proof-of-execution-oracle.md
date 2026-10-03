# ADR 0085: Cross-Cluster Byzantine Consensus & Proof-of-Execution Oracles

## Status
Accepted

## Context
As autonomous agent swarms expand into multi-cluster, cross-region, and multi-tenant operational topologies (established in ADR 0083 and ADR 0084), swarms are increasingly delegated authority to execute high-stakes, irreversible side-effects—such as automated database migrations, financial transactions, cloud infrastructure modifications, and safety-critical policy rollouts.

In distributed multi-agent operations, individual agents or entire cluster sub-swarms are exposed to arbitrary and malicious failures:
1. **Byzantine & Corrupt Failures**: Autonomous agents may hallucinate divergent action parameters, become compromised by prompt injection, or experience silent data corruption. Traditional Crash Fault Tolerant (CFT) protocols (such as Raft, ADR 0041) assume benign crash-only failures; under Byzantine behavior (such as equivocation or conflicting double-proposals), CFT clusters can commit catastrophic, contradictory state transitions.
2. **Lack of Verifiable Execution Attestations**: External consuming applications, enterprise auditing bodies, and cross-cluster gateways lack an independent, cryptographic **Proof-of-Execution Oracle** to verify that an agent's side-effect was validated by a verifiable supermajority of independent nodes before execution, and that pre-state and post-state invariants were strictly preserved.
3. **Exorbitant Token Cost & Latency of LLM-based Voting**: Naive consensus schemes that prompt multiple LLM judges or external consensus models introduce massive token overhead ($0.05–$0.25 per consensus round) and 1,000–3,000ms latency, violating SwarmWatch's **Zero-Token Cost Guarantee** and real-time execution SLOs.
4. **Decoupling from Swarm Circuit Breakers**: If malicious or corrupted nodes outnumber fault thresholds, traditional distributed databases continue attempting reconciliation or enter split-brain states, rather than cleanly tripping safety circuit breakers to halt runaway swarms.

Seam 74 resolves these challenges by introducing a lightweight, in-process, zero-token **Cross-Cluster Byzantine Consensus & Proof-of-Execution Oracle Engine** (`ByzantineConsensusOracle`).

## Decision
We implement an in-process, deterministic **Cross-Cluster Byzantine Consensus & Proof-of-Execution Oracle Engine** (`ByzantineConsensusOracle` and `ProofOfExecutionOracle`) with complete dual-SDK parity across Python 3.12+ and Node 22 native type-stripping (`@swarmwatch/sdk`).

### 1. Byzantine Fault Tolerant (BFT) State Machine
For a network of $N$ cross-cluster validator nodes, the engine guarantees safety and liveness under up to $f$ arbitrary (Byzantine) faulty nodes where:
$$N \ge 3f + 1, \quad f = \left\lfloor \frac{N - 1}{3} \right\rfloor$$
The quorum threshold required to finalize any consensus stage is:
$$Q = 2f + 1$$

The protocol advances through four deterministic phases:
1. **`PRE_PREPARE`**: The designated primary node $\text{primary}(v) = \text{nodes}[v \pmod N]$ for view $v$ receives a proposal $m$ from an agent, assigns a monotonically increasing sequence number $n$, computes the cryptographic digest $d = \mathcal{H}(m)$, signs the pre-prepare envelope, and broadcasts it across clusters.
2. **`PREPARE`**: Non-primary nodes verify the primary's signature, view, sequence number, and pre-state hash. If valid, each node broadcasts a signed prepare vote $\langle \text{PREPARE}, v, n, d, i \rangle_{\sigma_i}$. When a node collects $2f+1$ valid prepare votes (including its own and the primary's), it forms a **Prepare Certificate**.
3. **`COMMIT`**: Upon achieving a Prepare Certificate, nodes broadcast a signed commit vote $\langle \text{COMMIT}, v, n, d, i \rangle_{\sigma_i}$. When a node collects $2f+1$ valid commit votes from distinct nodes, it forms a **Commit Certificate**.
4. **`DECIDED` / `EXECUTED`**: The proposed action is committed to the local ledger, verified against pre/post-state conditions, and certified by the Proof-of-Execution Oracle.

### 2. View Change & Liveness Recovery
If a primary node fails to propose within the configured timeout $\tau_{\text{view}}$, goes offline, or produces an invalid pre-prepare, backup nodes initiate a `VIEW_CHANGE` transition:
- Nodes broadcast $\langle \text{VIEW\_CHANGE}, v+1, n, \mathcal{P}, i \rangle_{\sigma_i}$ containing their latest prepare certificate $\mathcal{P}$.
- When the new primary receives $2f+1$ valid `VIEW_CHANGE` messages, it broadcasts $\langle \text{NEW\_VIEW}, v+1, \mathcal{V} \rangle$ to restore normal 3-phase consensus.

### 3. Equivocation Detection & Node Slashing
Equivocation occurs when a node signs two distinct digests $d_1 \ne d_2$ for the same view $v$ and sequence number $n$:
$$\exists i, v, n : \quad \sigma_i(v, n, d_1) \land \sigma_i(v, n, d_2) \quad \text{with } d_1 \ne d_2$$
- The oracle detects equivocation in real-time ($O(1)$ lookup).
- The offending node is immediately marked as `SLASHED`, evicted from active consensus quorums, and registered in an immutable `EquivocationEvent`.
- If total slashed/byzantine nodes exceed $f$, the engine immediately trips the SwarmWatch safety circuit.

### 4. Cryptographic Proof-of-Execution (PoE) Certificates
Upon achieving a Commit Certificate and executing the action, the Proof-of-Execution Oracle synthesizes an immutable, self-contained `ProofOfExecutionCertificate`:
$$\text{PoE} = \big( \text{cert\_id}, \text{proposal\_id}, v, n, d, \text{pre\_hash}, \text{post\_hash}, \Sigma_Q, \tau \big)$$
where $\Sigma_Q$ is the aggregated multi-node threshold signature set ($|\Sigma_Q| \ge 2f+1$).
- The certificate is assigned a canonical, tamper-evident domain-tagged hash:
  $$\text{Receipt Hash} = \text{"sw:oracle:poe:v1:"} \,\|\, \text{SHA256}(\text{canonical\_json}(\text{PoE}))$$
- Any external cluster or third-party verifier can validate the PoE certificate in $<50\,\mu\text{s}$ using public keys alone, without contacting the originating cluster.

### 5. Fail-Closed Safety Circuit Coupling
The Byzantine Consensus Oracle couples directly with SwarmWatch's circuit breaker:
- **Safety Lockout**: When the circuit is tripped, all consensus proposals, prepare votes, and oracle certificate minting operations immediately reject with `OracleCircuitTrippedError`.
- **Fault Overload Trip**: If the number of faulty or slashed nodes exceeds $f$ ($N_{\text{faulty}} > f$), the oracle detects that Byzantine safety is compromised and autonomously trips the global safety circuit, halting all agent execution.

## Invariants
1. **Zero-Token Cost Guarantee**: All PBFT state transitions, threshold signature validations, and oracle certificates operate 100% locally with zero external LLM API calls ($0.00 USD token cost).
2. **Sub-250µs Execution SLO**: Pre-prepare, prepare, commit voting, and certificate verification execute in $<250\,\mu\text{s}$ per consensus round for clusters of up to 16 validator nodes.
3. **BFT Quorum Safety**: An action is never decided or certified unless signed by at least $2f+1$ distinct, authorized validator nodes out of $N \ge 3f+1$.
4. **Deterministic Equivocation Slashing**: Any node attempting double-signing is immediately slashed and evicted.
5. **Fail-Closed Safety Coupling**: Latching circuit trips when faults exceed $f$, preventing split-brain or rogue execution.
6. **Dual-SDK Parity**: 100% identical state transitions, cryptographic verification, and certificate formats across Python 3.12+ and Node 22 native type-stripping.

## Consequences
- **Positive**: Provides mathematical Byzantine fault tolerance for high-stakes multi-agent tool execution across heterogeneous clusters.
- **Positive**: External consumers can verify agent actions via self-contained, tamper-evident Proof-of-Execution certificates.
- **Positive**: Zero external cloud or blockchain dependencies; runs 100% in-process and local.
- **Negative**: Requires configuring at least 4 nodes ($3f+1$ for $f=1$) to tolerate 1 Byzantine fault in distributed mode; single-node clusters can operate in development standalone mode ($f=0$).
