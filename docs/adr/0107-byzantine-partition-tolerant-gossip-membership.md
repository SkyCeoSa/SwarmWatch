# ADR 0107: Multi-Cloud Byzantine Partition Tolerant Gossip Membership Protocol

- **Status**: Accepted
- **Date**: 2026-10-05
- **Seam**: Seam 96 (Phase 8: SwarmWatch Cloud SaaS & Marketplace)
- **Author**: SwarmWatch Architectural Committee

---

## 1. Context & Problem Statement

SwarmWatch Cloud SaaS spans hybrid, multi-cloud infrastructure (AWS, Azure, GCP, and sovereign on-premise enclaves). Nodes within the cluster must continuously monitor member liveliness, propagate cluster topology changes, and detect node failures. However, open distributed systems face adversarial conditions:
1. **Asymmetric Network Partitions & Transient Flaps**: Network boundaries between cloud providers drop packets directionally, causing naive failure detectors to trigger false positive evictions.
2. **Byzantine Faults & Sybil Suspicions**: Malicious or compromised nodes can spam fake "DEAD" accusations to force legitimate nodes out of the active quorum.
3. **Split-Brain Reconnect Divergence**: Upon partition healing, diverging membership lists must converge deterministically without dropping active tenants or replaying obsolete state.

---

## 2. Decision & Architecture

We introduce **Seam 96: Multi-Cloud Byzantine Partition Tolerant Gossip Membership Protocol** (`byzantine_gossip.py` in Python, `byzantine_gossip.ts` in TypeScript).

This deep module provides Byzantine fault-tolerant decentralized cluster membership tracking using threshold-corroborated suspicion escalation, cryptographic heartbeat attestations, and deterministic Merkle anti-entropy synchronization.

### 2.1 Mathematical Invariants

- **Invariant BGM1: Cryptographically Certified Heartbeat Progress**:
  Every membership gossip beacon emitted by node $N_i$ must advance a strictly monotonic sequence number:
  $$\text{seq}_{k+1} > \text{seq}_k$$
  and bear an Ed25519 signature over the beacon digest:
  $$D_{\text{beacon}} = \text{sha256}(N_i \parallel \text{seq} \parallel \text{status} \parallel \text{hlc})$$
  Unsigned, unauthenticated, or non-advancing beacons are dropped fail-closed with `UnauthenticatedGossipBeaconError`.

- **Invariant BGM2: Quorum-Gated Suspicion Escalation & Refutation**:
  A node cannot be marked `SUSPECT` or `DEAD` by a solitary witness. Transition to `SUSPECT` requires $\ge f + 1$ independent, signed suspicion reports from distinct peer nodes within observation window $\tau_{\text{suspect}}$.
  During the grace period $\Delta_{\text{refute}}$, the accused node can broadcast a higher-sequence heartbeat to immediately refute the accusation and restore `ALIVE` status. If refutation fails before deadline, the node transitions to `DEAD`.

- **Invariant BGM3: Partition Healing & Epidemic Merkle Anti-Entropy**:
  During partition reconciliation, peers compute and compare compact Merkle roots of their membership tables. If root hashes diverge, nodes perform efficient binary tree diff exchange to reconcile member states by selecting the state with the highest monotonic sequence number and causal HLC timestamp.

- **Invariant BGM4: Equivocation Detection & Safety Circuit Latching**:
  If a node broadcasts conflicting beacons with identical sequence numbers or mutually contradictory statuses, the detector synthesizes a `ByzantineEquivocationProof`. The equivocator is permanently quarantined from the cluster overlay, and if the count of quarantined nodes approaches Byzantine tolerance limit $f \ge \lfloor (N-1)/3 \rfloor$, `BYZANTINE_GOSSIP_COLLAPSE` trips on `SafetyCircuitBus`.

---

## 3. Dual-SDK Parity & API Specification

Both Python and TypeScript SDKs expose identical classes, methods, and error hierarchies:

### Python (`swarmwatch.byzantine_gossip`):
- Dataclasses: `GossipMember`, `HeartbeatBeacon`, `SuspicionReport`, `GossipAntiEntropyDigest`, `GossipEquivocationProof`
- Gateway: `ByzantineGossipCoordinator`, `global_byzantine_gossip`
- Exceptions: `ByzantineGossipError`, `UnauthenticatedGossipBeaconError`, `InsufficientSuspicionQuorumError`, `GossipPeerQuarantinedError`, `GossipEquivocationDetectedError`, `GossipCircuitTrippedError`

### TypeScript (`@swarmwatch/sdk/src/byzantine_gossip.ts`):
- Interfaces & Types: `GossipMember`, `HeartbeatBeacon`, `SuspicionReport`, `GossipAntiEntropyDigest`, `GossipEquivocationProof`
- Class: `ByzantineGossipCoordinator`, `globalByzantineGossip`
- Error classes with 100% naming parity.

---

## 4. Craft Floor & Observability Compliance

- **Latency SLO**: Gossip beacon processing and suspicion verification execute in $< 250\mu\text{s}$.
- **Zero-Token Cost Guarantee**: Zero external LLM calls ($0.00 USD).
- **Control Plane Endpoints**:
  - `POST /api/v1/gossip/beacons` (Submit signed heartbeat beacon)
  - `POST /api/v1/gossip/suspect` (File peer suspicion report)
  - `POST /api/v1/gossip/refute` (Submit self-refutation beacon)
  - `GET /api/v1/gossip/status` (Get membership cluster status)
- **CLI Subcommand**: `swarmwatch gossip status|beacon|suspect|refute`
- **Dashboard HUD Badge**: `#gossipBadge` with status `GOSSIP: QUORUM MESH OK` (22 chars $\le 30$).
