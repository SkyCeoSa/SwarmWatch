# ADR 0011: Distributed Fleet Circuit Synchronization & Quota Aggregation

## Status
Accepted

## Context
Production multi-agent swarms frequently execute across multiple worker threads, processes, and container replicas. When each worker tracks quota independently, a runaway loop or prompt injection distributed across $ workers can consume  \times$ the allocated token or cost budget before any individual worker breaches its local ceiling. SwarmWatch requires distributed fleet coordination to aggregate cumulative usage and broadcast immediate halts across all participating workers.

## Decision
We introduce the FleetCircuitCoordinator (Seam 6):

1. **Fleet Quota Aggregation**:
   - Maintains an atomic, thread-safe central ledger per swarm_id.
   - Each worker registers turn executions via ecord_turn(worker_id, swarm_id, tokens, cost, turns).
   - Cumulative fleet metrics (	otal_tokens, 	otal_cost, 	otal_turns) are calculated in real time.

2. **Distributed Trip Broadcasting**:
   - If fleet-wide ceilings (max_turns, 	oken_budget, or cost_budget) or entropy anomalies are detected by any worker, the coordinator marks the swarm session as tripped (is_tripped = True).
   - A 	rip_broadcast event is published to all subscribed workers via the inter-process/inter-thread event bus.
   - Any subsequent worker invocation for that swarm_id immediately raises SwarmCircuitBreakException, stopping all parallel agent turns.

3. **Zero-Token Cost Guarantee**:
   - Aggregation and policy checks are purely mathematical with (1)$ lookup complexity and sub-microsecond latency.

## Consequences
- **Positive**: Strict financial and operational budget ceilings hold true even in multi-process distributed swarm deployments.
- **Positive**: Coordinated halting prevents runaway token expenditure across parallel workers.
- **Positive**: Minimal latency overhead (<1 μs per turn recording).
