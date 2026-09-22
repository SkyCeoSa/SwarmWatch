# ADR 0029: Automated Alert Route Webhook Testing & Synthetic Simulation Pipeline

## Status
Accepted (Seam 18)

## Context
SwarmWatch provides an enterprise-grade, asynchronous non-blocking webhook dispatcher (`WebhookNotifier`, Seam 13 / ADR 0023) capable of sending structured incident alerts to Slack Block Kit, Discord Embeds, PagerDuty Events API v2, and custom JSON webhooks upon runtime circuit breaker events (`CircuitEvent`). It includes Dead-Letter Queue (DLQ) buffering to ensure sub-microsecond non-blocking returns on agent threads even during external network partitions.

However, in production deployments and enterprise CI/CD workflows, SRE and AI platform teams require deterministic validation of alerting routes *before* swarms are deployed. Specifically, operators must verify that:
1. Webhook URLs correctly receive, parse, and validate the target platform schemas (e.g. Slack blocks, Discord embeds, PagerDuty trigger envelopes).
2. Counterfactual ROI metrics (dollars saved, tokens saved, turns averted) are correctly formatted and rendered.
3. Transient network failures (e.g. HTTP 503, 500, timeouts) trigger automatic non-blocking retry loops without dropping alerts.
4. Exhausted retries cleanly accumulate into the Dead-Letter Queue (DLQ) with intact payloads.
5. Upstream recovery allows complete, zero-loss replay (`replay_dlq()`).
6. Live multi-agent circuit halts (e.g., triggered by `ChaosSimulator`) end-to-end trigger webhook delivery to all configured channels.

Testing against live third-party APIs (Slack/Discord/PagerDuty) introduces external network dependencies, flakiness, rate-limiting, and credentials management overhead, violating SwarmWatch's guarantee of 100% local determinism and zero cloud costs.

## Decision
We implement the **Automated Alert Route Webhook Testing & Synthetic Simulation Pipeline (Seam 18)**:

1. **Synthetic Mock Webhook Receiver (`SyntheticAlertReceiver`)**:
   - Zero-dependency local mock HTTP server built on Python standard library `http.server.HTTPServer` / `ThreadingHTTPServer`.
   - Binds to `127.0.0.1` on an ephemeral port (`port=0`) to eliminate port collisions.
   - Distinct synthetic webhook routes:
     - `/webhook/slack`: Validates Slack Block Kit structure, header blocks, text blocks, and counterfactual ROI markdown.
     - `/webhook/discord`: Validates Discord embed fields, color mapping (`#EF4444` for trips, `#F59E0B` for warnings), and titles.
     - `/webhook/pagerduty`: Validates PagerDuty Events API v2 trigger envelopes (`routing_key`, `event_action: trigger`, `severity`, `custom_details`).
     - `/webhook/custom`: Validates native SwarmWatch JSON schema.
   - Programmable fault injection:
     - `inject_fault(route, status_code=503, count=2)`: Simulates transient outages.
     - `inject_delay(route, delay_seconds=0.5)`: Simulates network latency.
     - `recover_route(route)`: Restores healthy 200 OK responses.
     - `get_received_events(route=None)`: Captures received requests for assertion.

2. **Synthetic Alert Simulation Pipeline (`AlertSimulationPipeline`)**:
   - Orchestrates a 4-phase automated simulation:
     - **Phase 1: Multi-Channel Delivery**: Dispatches warning and trip events to all 3 platform formats and validates payload structure, status codes, and latency.
     - **Phase 2: Fault Injection & DLQ Buffering**: Injects synthetic 503 failures, verifies sub-microsecond non-blocking caller return, retry attempts, and Dead-Letter Queue accumulation.
     - **Phase 3: Network Recovery & DLQ Replay**: Restores receiver health, triggers `replay_dlq()`, and asserts 100% recovery with zero event loss.
     - **Phase 4: Live Swarm Circuit Trip Linkage**: Spawns an agent swarm session using `ChaosSimulator` (e.g., `entropy_collapse`), trips the circuit breaker, and verifies that the resulting `CircuitEvent` propagates through the notifier to the synthetic receiver with exact counterfactual dollars and tokens saved.

3. **CLI Subcommand (`swarmwatch test-alerts`)**:
   - Terminal utility allowing instant SRE verification with an Impeccable tabular summary report (`terminal` and `json` outputs).

4. **Dual-SDK Parity**:
   - TypeScript verification suite (`packages/sdk-typescript/tests/alert_simulation.test.js`) implementing a lightweight Node.js `http.createServer` synthetic receiver to validate `@swarmwatch/sdk` `WebhookNotifier`, fault handling, and DLQ replay.

## Consequences
- **Positive**: Operations teams can test, verify, and monitor incident alerting paths locally in < 1 second with 0 external network requests.
- **Positive**: 100% deterministic local execution guarantee with zero external cloud egress.
- **Positive**: Strict validation of Slack, Discord, and PagerDuty payload schemas before production swarm runs.
