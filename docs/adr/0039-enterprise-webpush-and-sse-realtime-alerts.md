# ADR 0039: Enterprise WebPush & Server-Sent Events (SSE) Real-Time Alert Notification Engine

## Status
Accepted

## Context
SwarmWatch delivers multi-agent observability, entropy circuit breaking, causal DAG tracing, and time-machine debugging. To date, real-time alerting and event streaming relied on:
1. Bidirectional WebSockets (`/ws/{swarm_id}`) for the interactive developer console.
2. Outbound asynchronous webhooks (`WebhookNotifier`) dispatching to Slack, Discord, and PagerDuty.

However, in production enterprise environments, several operational challenges arise:
- **Firewall & Corporate Proxy Restrictions**: Many corporate gateways and enterprise network appliances terminate or aggressively disconnect long-lived HTTP/1.1 WebSocket Upgrade connections (`101 Switching Protocols`), while transparently supporting standard HTTP/1.1 and HTTP/2 persistent streaming (`text/event-stream`).
- **Headless CLI & Daemon Ingestion**: Automation tools, CI/CD runners, and operator command-line listeners require lightweight, read-only event streams without the overhead and framing complexity of WebSockets.
- **Background & Mobile Operator Notification**: When operators are working in other windows, browser tabs are idle, or administrators are on mobile devices, WebSocket connections in dormant tabs are throttled or terminated by browsers. Traditional polling is wasteful and introduces high latency (e.g. 5–30 seconds), risking runaway LLM loops continuing undetected.
- **Connection Recovery & Event Replay**: Network blips must not result in dropped circuit breaker notifications. Streaming must provide standard `Last-Event-ID` resumption without full reconnections or state loss.

To resolve these challenges, SwarmWatch introduces **Seam 28: Enterprise WebPush & Server-Sent Events (SSE) Real-Time Alert Notification Engine**.

---

## Decision

### 1. Server-Sent Events (SSE) Event Stream (`GET /api/v1/events/stream`)
SwarmWatch implements a standards-compliant W3C Server-Sent Events endpoint:
- **Endpoints**:
  - `GET /api/v1/events/stream`: Global or tenant-filtered event stream.
  - `GET /api/v1/swarms/{swarm_id}/events/stream`: Swarm-specific stream.
- **Content Type**: `text/event-stream; charset=utf-8`.
- **Wire Format**:
  ```http
  id: evt-4a8b1c
  event: circuit_trip
  retry: 3000
  data: {"id":"evt-4a8b1c","swarm_id":"fleet-prod-1","event_type":"trip","metric_val":0.18,"reason":"Shannon entropy collapsed < 0.35"}
  ```
- **Connection Heartbeats**: Automatic keep-alive pings (`event: ping` or `: ping`) dispatched every 15 seconds to prevent intermediary proxy timeouts.
- **Event Replay via `Last-Event-ID`**:
  - The server maintains an in-memory thread-safe ring buffer of the last 200 events.
  - When a reconnecting client sends `Last-Event-ID: <id>` (or `?last_event_id=<id>`), missed events are immediately replayed before resuming live streaming.
- **Filtering**: Query parameters `min_level` (`warning` | `trip`), `project_id`, and `swarm_id`.

### 2. Zero-Dependency Cryptographic WebPush Engine (RFC 8291 / RFC 8292)
To comply with SwarmWatch's strict zero-external-dependency policy (no external cryptography or pywebpush packages):
- **NIST P-256 (secp256r1) Integer Arithmetic**:
  - Implemented in pure Python standard library (`hashlib`, `hmac`, `struct`, `base64`).
  - Implements field arithmetic modulo $p = 2^{256} - 2^{224} + 2^{192} + 2^{96} - 1$, point addition, point doubling, and scalar multiplication.
  - Generates standard 65-byte uncompressed public keys (`0x04 || X || Y`) encoded in URL-safe Base64.
- **VAPID JWT Signing (ES256)**:
  - Mints VAPID JWT tokens conforming to RFC 8292:
    - Header: `{"typ": "JWT", "alg": "ES256"}`
    - Claims: `{"aud": "<push_service_origin>", "exp": <timestamp>, "sub": "mailto:admin@swarmwatch.local"}`
    - Signature: IEEE P1363 standard 64-byte $(r, s)$ format.
  - Includes HMAC-SHA256 signature fallback for mock and internal test endpoints.
- **WebPush Dispatcher**:
  - Outbound push notifications dispatched asynchronously via background worker thread without blocking calling agent execution.
  - Headers: `TTL`, `Urgency: high`, `Authorization: vapid t=..., k=...`, and `Crypto-Key: p256ecdsa=...`.
  - Automatic unregistration on `404 Not Found` or `410 Gone` (expired subscriptions).

### 3. Persistent Push Subscription Storage
- New SQLite table `push_subscriptions` in WAL mode:
  - Fields: `id`, `project_id`, `endpoint` (UNIQUE), `p256dh`, `auth`, `user_id`, `topics`, `created_at`.
  - CRUD operations: `save_push_subscription`, `delete_push_subscription`, `get_push_subscription`, `list_push_subscriptions`.

### 4. Dual-SDK Parity in TypeScript (`@swarmwatch/sdk`)
- **`SwarmEventSource`**:
  - Universal SSE client for Node.js and modern browsers.
  - Automatically handles line buffering, `id:` tracking, exponential backoff reconnects, and typed event listeners (`on('trip', ...)`, `on('warning', ...)`, `on('all', ...)`).
- **`VapidSigner`**:
  - Generates EC P-256 keys and signs VAPID tokens natively using `node:crypto`.
- **`SwarmClient` Integration**:
  - Exposes `client.listenEvents(handler, options)`.

### 5. CLI & Web Console Integration
- **CLI Commands**:
  - `swarmwatch events listen`: Real-time streaming to terminal with ANSI color highlighting.
  - `swarmwatch push keys`: Export or regenerate VAPID keypairs.
  - `swarmwatch push test`: Dispatch test notification to verify delivery.
  - `swarmwatch push subscriptions`: Tabular list of active subscriptions.
- **Web Console (`dashboard.html`)**:
  - `#sseStatusBadge` in `#fleetBar` indicating active SSE connectivity.
  - `#btnPushNotify` toggle in header for desktop notifications via browser Notification API.
  - Strict compliance with Impeccable Craft Floor (0 anti-patterns).

---

## Consequences

### Positive
- Zero polling overhead on servers and clients.
- Immediate (<50ms) push notifications delivered to backgrounded browsers and operators.
- Strict zero-external-dependency guarantee preserved (no `cryptography` pip package required).
- Resilient recovery from transient connection drops via `Last-Event-ID` ring-buffer replay.
- Dual-SDK parity across Python and TypeScript.

### Negative / Trade-offs
- Pure Python P-256 scalar multiplication requires ~25ms on key generation / token minting; mitigated by caching VAPID tokens (valid for up to 24 hours per push service origin).
