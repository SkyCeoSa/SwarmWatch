# ADR 0033: Distributed Redis Circuit Breaker & Fleet State Synchronization

## Status
Accepted

## Context
Production multi-agent swarms frequently scale horizontally across distributed container replicas, Kubernetes worker pods, or multi-process task queues (e.g. Celery, Ray, or Ray/LangGraph clusters). 

In ADR 0011, we established the `FleetCircuitCoordinator` to synchronize quotas across workers. However, that coordinator relied on in-process threading primitives (`threading.RLock()` and local dictionary storage). When agents execute across separate machine or container boundaries:
1. **Quota Blindness**: Worker pods cannot atomically observe cumulative token or turn expenditure from peer pods without a shared, low-latency datastore. A runaway prompt loop across 10 pods can consume 10x the allocated budget before any single pod breaches its local ceiling.
2. **Delayed Intervention**: When Worker A encounters a Shannon entropy collapse or tool deadlock and trips the circuit breaker, Workers B through Z continue running and burning tokens until their own local heuristics trigger or until a human intervenes.
3. **Race Conditions**: Concurrent turns from dozens of parallel workers require atomic counter increments to prevent race conditions during budget ceiling evaluations.

## Decision
We introduce **Seam 22**: Distributed Redis Circuit Breaker & Fleet State Synchronization (`RedisCircuitCoordinator`), establishing Redis as the ultra-low-latency distributed coordination backbone.

### Architectural Invariants:
1. **Atomic Quota Ledger**:
   - Stores cumulative turn counts, token expenditure, and dollar costs in Redis Hashes (`swarmwatch:swarm:{swarm_id}:metrics`).
   - Uses atomic Redis commands (`HINCRBY`, `HINCRBYFLOAT`) or pipelined transactions to aggregate multi-worker turns with zero race conditions.
   - Evaluates session ceilings (`max_turns`, `token_budget`, `cost_budget`) atomically upon each turn execution.
2. **Sub-Millisecond Pub/Sub Trip Broadcasting**:
   - When any worker trips the circuit breaker (whether due to budget quota exhaustion, low Shannon entropy, or lexical similarity loops), it immediately writes the trip state to Redis and publishes a `RedisTripBroadcast` event to `swarmwatch:fleet:{swarm_id}:trips` and `swarmwatch:trips`.
   - All participating worker instances maintain a lightweight, non-blocking Pub/Sub listener.
   - Upon receiving the trip event, peer workers immediately set their local trip cache to true. Any ongoing or subsequent agent turns instantly raise `SwarmCircuitBreakException`, stopping fleet execution in $<1\text{ms}$.
3. **Zero Required Third-Party Dependencies (Pure Stdlib RESP Engine)**:
   - Includes a pure Python standard library socket client (`RESPRedisClient`) implementing the Redis Serialization Protocol (RESP2) over standard TCP sockets.
   - Seamlessly adopts official `redis-py` (`redis.Redis`) if already installed in the environment.
   - Includes an in-memory `MockRedisClient` for fast, 100% deterministic local test execution and offline development without needing a running Redis server.
4. **Dual-SDK Parity (TypeScript SDK)**:
   - Full feature parity implemented in `@swarmwatch/sdk` (`packages/sdk-typescript/src/redis_circuit.ts`), providing `RedisCircuitCoordinator`, `RedisFleetBackend`, and `MockRedisClient` for Node.js, Bun, and TypeScript agent swarms.
5. **Zero-Token Cost Guarantee**:
   - Quota aggregation, entropy evaluations, and trip broadcasting are purely mathematical and protocol-driven; zero external LLM API calls are made.

## Consequences
- Multi-worker and multi-container agent clusters achieve instant, synchronized halting upon circuit breaker intervention.
- Quota ceilings (`max_turns`, `token_budget`, `cost_budget`) hold strictly true across any number of parallel distributed workers.
- Eliminates the need for mandatory third-party pip dependencies while allowing seamless adoption of production Redis clusters.
