# ADR 0023: Enterprise PagerDuty Events API v2, Slack Block Kit & Dead-Letter Alerting

## Status
Accepted

## Context
When autonomous multi-agent swarms experience semantic collapse, tool recursion, or quota exhaustion, mission-critical operations require instant, high-fidelity out-of-band notifications. In ADR 0007, SwarmWatch introduced an asynchronous in-memory webhook dispatcher with Slack and Discord formatters.

However, enterprise Site Reliability Engineering (SRE) and quantitative trading teams require:
1. Native integration with **PagerDuty Events API v2** (`POST https://events.pagerduty.com/v2/enqueue`) with deterministic deduplication keys, severity mapping (`critical` vs `warning`), and rich custom details containing exact counterfactual financial and token ROI metrics.
2. Resilience against temporary network outages and target API rate limits through an in-memory **Dead-Letter Queue (DLQ)** with dead-letter inspection and programmatic replay capabilities.
3. First-class dual-SDK parity across Python and TypeScript.
4. HTTP control plane inspection endpoints (`GET /api/v1/alerts/dlq`, `POST /api/v1/alerts/dlq/replay`, `POST /api/v1/alerts/test`) for observability and synthetic testing.

## Decision
1. **PagerDuty Events API v2 Native Protocol**:
   - In `format_webhook_payload`, support `format="pagerduty"`.
   - Map event severity: `event_type == "trip"` -> `severity: "critical"`, `event_type == "warning"` -> `severity: "warning"`.
   - Set deterministic deduplication key: `swarmwatch-{swarm_id}-{event_id}` to prevent incident storms.
   - Embed comprehensive `custom_details` including normalized Shannon entropy, lexical similarity, tokens saved, dollars saved, turns averted, and budget consumption.

2. **In-Memory Dead-Letter Queue (DLQ)**:
   - When all delivery attempts (`config.max_retries`) are exhausted, record a `DeadLetterRecord` in a bounded circular buffer (max 500 records).
   - Expose `get_dlq()`, `clear_dlq()`, and `replay_dlq()` methods on `WebhookNotifier`.
   - Provide atomic transmission statistics: `delivered`, `failed`, `retried`, and `dlq_count`.

3. **Full Dual-SDK Parity in TypeScript**:
   - Implement `packages/sdk-typescript/src/webhooks.ts` with zero external dependencies, supporting Slack Block Kit, Discord embeds, PagerDuty Events API v2, async queue worker, and DLQ replay.

4. **Control Plane REST API**:
   - Expose `/api/v1/alerts/dlq`, `/api/v1/alerts/dlq/replay`, and `/api/v1/alerts/test` in `server.py`.

## Consequences
- **Positive**:
  - Full compatibility with enterprise SRE incident workflows (PagerDuty on-call escalation, Slack war rooms).
  - High resilience: temporary network failures do not result in unmonitored dropped alerts.
  - Zero latency impact ($<1\mu s$) on agent execution threads.
  - Complete parity between Python and TypeScript runtimes.
- **Negative**:
  - In-memory DLQ is bounded (500 items) to ensure zero memory leaks; un-replayed items are dropped if process terminates unless persisted via external sink.
