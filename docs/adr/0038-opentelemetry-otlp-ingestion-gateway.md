# ADR 0038: OpenTelemetry OTLP Ingestion Gateway (HTTP / Protobuf & JSON)

## Status
Accepted

## Context
SwarmWatch provides multi-agent observability, lexical & Shannon entropy circuit breaking, and time-machine debugging. Previously, incorporating SwarmWatch into multi-agent systems required applications to directly install and configure the Python (`swarmwatch`) or TypeScript (`@swarmwatch/sdk`) libraries or utilize runtime framework adapters (LangGraph, CrewAI, AutoGen, LlamaIndex, DSPy, Semantic Kernel, Vercel AI SDK).

However, in modern enterprise architectures and heterogeneous polyglot production environments:
1. Multi-agent systems are increasingly composed across diverse languages (Go, Java, Rust, C#, Python, TypeScript) and containerized microservices.
2. Enterprise platforms standardize on the **OpenTelemetry (OTel) Semantic Conventions for Generative AI systems** (`gen_ai.*` attributes, spans, and span events) and emit standard OTLP traces (`POST /v1/traces`) to vendor-neutral collectors.
3. Requiring proprietary SDK installation within every third-party service creates adoption friction and prevents transparent drop-in circuit breaking for existing deployed agents.

To solve this, SwarmWatch must act as an in-process, high-throughput **OpenTelemetry OTLP Ingestion Gateway**:
- **Standard Endpoint Conformance**: Provide native `/v1/traces` (and `/api/v1/otlp/traces`) HTTP endpoints conforming strictly to OpenTelemetry Specification 1.0+.
- **Zero-Dependency Protobuf Wire Decoder**: Implement standard wire-format decoding in pure Python standard library to process `application/x-protobuf` and `application/json` without mandatory third-party pip dependencies.
- **Bi-Directional GenAI Semantic Translation**: Translate OpenTelemetry GenAI spans (`gen_ai.system`, `gen_ai.agent.name`, `gen_ai.usage.total_tokens`, `gen_ai.operation.name`, events) directly into SwarmWatch `SwarmSpan` primitives in real time.
- **In-Flight Circuit Breaker Interception**: Feed incoming OTLP spans through the `EntropyCircuitBreaker` and `RedisCircuitCoordinator`, calculating Shannon entropy and triggering immediate circuit halts and alerts upon detecting runaway conversational loops or quota exhaustion.
- **Live Observability & Control Plane Integration**: Instantly broadcast ingested OTLP spans to the Web Console over WebSockets and export Prometheus OpenMetrics without latency penalties.
- **Dual-SDK Parity**: Maintain native OTLP trace export, parsing, and semantic convention transformation in both Python and TypeScript runtimes.

---

## Decision

### 1. Standard OTLP HTTP Ingestion Endpoint (`/v1/traces`)
SwarmWatch server implements the official OpenTelemetry HTTP Trace Export specification:
- **Path**: `POST /v1/traces` and `POST /api/v1/otlp/traces`.
- **Content Negotiation**:
  - `Content-Type: application/json` (standard proto3 JSON mapping or flattened key-value maps).
  - `Content-Type: application/x-protobuf` (binary OTLP protobuf payload decoded via stdlib `ProtobufWireDecoder`).
- **Response Format**:
  - HTTP 200 OK with body `{"partialSuccess": {}}` or `{}` upon full ingestion.
  - HTTP 200 OK with `{"partialSuccess": {"rejectedSpans": N, "errorMessage": "..."}}` if partial spans are dropped or rejected.
- **Multi-Tenant Context Propagation**:
  - Tenant and project resolution derived from standard headers: `x-swarmwatch-project-id`, `X-API-Key`, or `Authorization: Bearer sw_live_...`.
  - Fallback to `default-project` for seamless, unauthenticated local development environments.

### 2. Zero-Dependency Protobuf Wire Format Decoder (`ProtobufWireDecoder`)
To avoid heavy external dependencies (`protobuf`, `grpcio`), SwarmWatch provides a lightweight, pure Python standard library decoder for tag-length-value (TLV) wire protocol:
- **Wire Type 0 (Varint)**: Decodes arbitrary-length varints (integers, booleans, enums).
- **Wire Type 1 (64-bit)**: Decodes fixed 8-byte integers/doubles.
- **Wire Type 2 (Length-Delimited)**: Decodes nested sub-messages, UTF-8 strings, and byte sequences.
- **Wire Type 5 (32-bit)**: Decodes fixed 4-byte integers/floats.
- Transparently decodes `ExportTraceServiceRequest` protobuf binary streams into structured dict representations.

### 3. OpenTelemetry GenAI Semantic Convention Mapping Matrix
The ingestion gateway extracts standard OpenTelemetry AI attributes into SwarmWatch primitives:

| OpenTelemetry Attribute / Field | SwarmWatch Primitive Field | Transformation / Fallback Rule |
| :--- | :--- | :--- |
| `span.traceId` | `span.swarm_id` | 16-byte hex string converted to trace identifier |
| `span.spanId` | `span.id` | 8-byte hex string converted to span identifier |
| `span.parentSpanId` | `span.parent_span_id` | Mapped directly; `None` if empty string or root |
| `gen_ai.agent.name` / `gen_ai.agent.id` | `span.source_agent` | Fallback: span name, `service.name`, or `"external_agent"` |
| `gen_ai.target.agent` / `gen_ai.request.model` | `span.target_agent` | Fallback: parsed target from span name or `"agent_target"` |
| `gen_ai.operation.name` / `gen_ai.span_type` | `span.span_type` | Normalized to `"agent_call"`, `"llm_completion"`, `"tool_call"` |
| `gen_ai.usage.total_tokens` | `span.token_count` | Sum of prompt + completion tokens or character estimate |
| `gen_ai.usage.cost` | `span.cost_estimate` | Explicit cost or computed via `$0.00002 / token` baseline |
| `span.startTimeUnixNano` | `span.start_time` | Converted from epoch nanoseconds to ISO 8601 UTC |
| `span.endTimeUnixNano` | `span.end_time` | Converted from epoch nanoseconds to ISO 8601 UTC |
| `gen_ai.content.prompt` (event / attr) | `span.input_payload` | Structured input message payload |
| `gen_ai.content.completion` (event / attr)| `span.output_payload` | Structured output message payload |
| `gen_ai.swarmwatch.entropy_score` | `span.entropy_score` | Computed on completion text via `calculate_normalized_shannon_entropy` |

### 4. Real-Time Circuit Breaker Ingestion Pipeline
When spans arrive via the OTLP gateway:
1. Spans are parsed and mapped to `SwarmSpan` entities.
2. Spans are asynchronously saved to active storage (`SQLiteStorageAdapter` / `ClickHouseStorageAdapter`).
3. Each span is evaluated against the `EntropyCircuitBreaker` and `RedisCircuitCoordinator` (if Redis cluster fleet active):
   - Computes Shannon entropy on output content.
   - Detects cyclic loops between communicating agents.
   - Evaluates token and turn budget quotas.
   - If a threshold is violated, immediately trips the circuit, emits `circuit_breaker_tripped` WebSocket broadcast, and records financial ROI savings.
4. Prometheus OpenMetrics are recorded (`record_span_ingested`).
5. Live span updates are broadcast over WebSockets to all connected dashboard consoles.

### 5. Dual-SDK Parity in TypeScript
The `@swarmwatch/sdk` TypeScript package implements full parity:
- `toOtelGenAiSpan(span)`: Maps TypeScript SDK spans to standard OTel GenAI structure.
- `formatOtlpTracesPayload(spans)`: Constructs standard OTLP JSON trace payloads.
- `parseOtlpTracesPayload(payload)`: Decodes OTLP trace JSON payloads into `SwarmSpan` items.
- `OtlpTraceExporter`: In-process non-blocking trace exporter queue dispatching to any standard OTLP receiver.

---

## Consequences

### Positive
- **Zero-SDK Adoption**: Any system emitting OpenTelemetry traces (Python, Node.js, Go, Rust, Java, LangChain, CrewAI, AutoGen, OpenAI) can be protected by SwarmWatch without modifying agent code.
- **Zero-Dependency Wire Parsing**: Ingests both JSON and binary Protobuf without requiring bulky Google protobuf pip dependencies.
- **Full Control Plane Integration**: Ingested OTLP spans participate seamlessly in time-machine debugging, state snapshot diffs, DAG topology visualization, and circuit breaking.
- **Multi-Tenant Isolation**: Supports API keys and project headers transparently.

### Negative / Trade-offs
- Protobuf wire format parsing in pure Python standard library has slightly higher CPU overhead than C-compiled extensions for massive (>100MB) batches. However, for typical OTLP batch sizes (<50 spans, <256KB), parsing takes $<0.8\text{ms}$.
