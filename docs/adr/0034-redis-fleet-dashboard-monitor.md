# ADR 0034: Distributed Redis Fleet & Cluster Monitor Widget in Web Dashboard

## Status
Accepted

## Context
Following the implementation of Seam 22 (ADR 0033), SwarmWatch coordinates distributed agent swarms across cluster nodes using Redis as the high-throughput, low-latency synchronization backbone. Atomic quota ledgers (`HINCRBY`/`HINCRBYFLOAT`) prevent budget overruns, while sub-millisecond Pub/Sub trip broadcasts immediately halt all workers across Kubernetes pods or multi-container deployments.

However, developers and cluster operators inspecting live swarms in the SwarmWatch Developer Console (`dashboard.html`) previously had no visual interface for:
1. Monitoring the connection state and latency of the Redis cluster synchronization layer.
2. Observing the active roster of worker pods executing concurrent turns in the swarm.
3. Tracking real-time cumulative consumption against fleet-wide quota ceilings (turns, tokens, dollar cost).
4. Viewing immediate visual broadcast alerts when a peer worker trips the circuit breaker anywhere in the cluster.
5. Remotely triggering emergency halts or flushing fleet state directly from the console UI.

## Decision
We introduce **Seam 23**: **Distributed Redis Fleet & Cluster Monitor Widget** (`RedisFleetWidget`) into the developer console (`dashboard.html`) and wire real-time event broadcasting through the server WebSocket manager (`server.py`).

### Architectural Invariants:
1. **Console HUD Integration (`#fleetBar`)**:
   - Embeds an austere, dark status bar (`#fleetBar`) styled with `--bg-surface` (`#0E1422`), `--border-subtle` (`#1D2840`), and hairline 1px borders, conforming with Matt Pocock's deep module principles and the High-Density Craft Standard.
   - Provides a toggle button (`#btnFleetToggle`) in the console header displaying live connection status (`REDIS: CONNECTED` or `MOCK IN-MEMORY`).
2. **Active Workers Roster**:
   - Renders a live pill badge list of all active worker identifiers registered in `swarmwatch:swarm:{swarm_id}:workers`.
   - Each worker badge features a subtle status pulse and turn counter.
3. **Atomic Quota Progress Meters (`FleetQuotaMeter`)**:
   - Tabular numeral display showing current consumption vs limit:
     - **Turns**: `total_turns / max_turns`
     - **Tokens**: `total_tokens / token_budget`
     - **Cost**: `$total_cost / $cost_budget`
   - Integrated horizontal micro-progress bars that transition smoothly from subtle blue (`--accent`) to amber (`--warning`) at 80% quota, and red (`--danger`) upon breach.
4. **Instant Cluster Trip Alert Banner (`FleetTripBanner`)**:
   - When a `fleet_trip_broadcast` is received over the WebSocket, `#fleetBar` dynamically renders an austere red alert banner with the tripping worker ID, exact ISO timestamp, and breach reason, preventing further UI actions.
5. **Interactive Control Plane Actions**:
   - **Emergency Fleet Trip** button (`#btnFleetTrip`): Dispatches `POST /api/v1/fleet/redis/trip`, halting all workers across the distributed cluster.
   - **Reset Fleet State** button (`#btnFleetReset`): Dispatches `POST /api/v1/fleet/redis/reset`, atomically flushing Redis keys and re-arming the circuit breaker.
6. **Real-Time WebSocket & Fallback Polling**:
   - The server subscribes to `RedisCircuitCoordinator` events and immediately broadcasts `fleet_metric_update`, `fleet_trip_broadcast`, and `fleet_state_reset` messages over `/ws/swarm/{swarm_id}`.
   - A lightweight 4-second polling timer queries `GET /api/v1/fleet/redis/metrics` as a resilient fallback.
7. **Craft Floor Compliance**:
   - Strict `#080B11` canvas palette, `#0E1422` surface, tabular numerals (`tnum`), zero emojis, zero neon glows, zero nested cards, strict 3-4px radii.

## Consequences
- Operators gain single-pane-of-glass observability into distributed swarm clusters without needing external Redis CLI tools or Grafana dashboards.
- Emergency manual interventions and resets can be triggered with one click directly from the developer console.
- Zero external LLM token overhead; 100% deterministic, in-process local execution.
