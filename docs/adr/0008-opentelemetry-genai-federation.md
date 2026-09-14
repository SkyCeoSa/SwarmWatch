# ADR 0008: OpenTelemetry GenAI Semantic Conventions & OTLP Federation

## Status
Accepted

## Context
Enterprise operations teams require multi-agent observability to be federated into existing distributed tracing backends (Grafana Tempo, Datadog, Dynatrace, Honeycomb, New Relic) via the OpenTelemetry (OTel) Collector. However, traditional APM OTel SDKs introduce heavy dependencies (`grpcio`, `protobuf`, `opentelemetry-sdk`) that would bloat the lightweight SwarmWatch client and potentially breach the sub-microsecond caller latency guarantee.

## Decision
We implement a dedicated, deep OpenTelemetry Federation module (`swarmwatch.otel`):
1. **Decoupled Architecture**:
   - The core SDK remains completely independent of heavy external OTel packages.
   - We implement native OTLP/HTTP JSON formatting conforming strictly to the official **OpenTelemetry GenAI Semantic Conventions**:
     - `gen_ai.system`: `"swarmwatch"`
     - `gen_ai.operation.name`: `"{source_agent} -> {target_agent}:{span_type}"`
     - `gen_ai.usage.total_tokens`: `token_count`
     - `gen_ai.swarmwatch.entropy_score`: `entropy_score`
     - `gen_ai.swarmwatch.branch_id`: `branch_id`
2. **OTLP Exporter (`OtelGenAiExporter`)**:
   - Asynchronous batch export to any standard OTLP HTTP receiver endpoint (`/v1/traces`).
   - Uses the non-blocking transport queue pattern, ensuring zero caller thread blocking.
   - Built-in Mock / In-Memory harness for offline testing.
3. **Optional OpenTelemetry SDK Tracer Bridge**:
   - If `opentelemetry-api` is present in the environment, provides a seamless `SwarmOtelSpanProcessor` bridge.

## Consequences
- **Positive**: Native enterprise APM federation with zero lock-in.
- **Positive**: Zero bloat in the core Python SDK.
- **Positive**: Strict compliance with OpenTelemetry GenAI standards.
- **Negative**: Advanced gRPC-only OTel collectors require the OTLP HTTP endpoint (standard on port 4318) or an OTel Collector gateway.
