# ADR 0079: Protocol Interoperability Lab & OTLP/gRPC Conformance Matrix (Seam 68)

## Status
Accepted

## Context
SwarmWatch provides native OpenTelemetry OTLP/HTTP (JSON and binary protobuf) ingestion (Seam 27 / ADR 0038) and high-throughput gRPC streaming/unary trace gateways (Seam 33 / ADR 0044).
However, as identified in Seam 68 of the external architectural audit:

1. **Internal Framing vs. Standards Interoperability**: While internal serializers (`ProtobufWireDecoder`, `GrpcMessageFramer`) pass isolated unit tests, production deployments interact with diverse official OpenTelemetry SDKs (Python, Go, Java, JS), collectors, and reverse proxies (Envoy, NGINX).
2. **Streaming Frame Fragmentation & Chunking**: Real-world TCP fragmentation and HTTP/2 multiplexing split 5-byte gRPC headers and length-delimited protobuf records across arbitrary byte boundaries (e.g., 1-byte, 7-byte chunks). Ingestion engines must reassemble frames without memory leaks or premature EOF errors.
3. **Premature Cancellation & Deadline Expiration**: Clients regularly abort HTTP/gRPC streams mid-flight or hit strict deadlines. The ingestion gateway must handle context cancellation (`CANCELLED` / code 1, `DEADLINE_EXCEEDED` / code 4) gracefully without corrupting subsequent spans on multiplexed connections.
4. **Flow Control & Ingestion Backpressure**: Under high-velocity multi-agent traffic bursts, gateways must enforce window limits, buffer bounds, and flow control without dropping critical safety evidence or deadlocking client event loops.
5. **Cross-Runtime Behavioral Equivalence**: Both Python 3.12+ and Node 22 implementations must produce identical validation outcomes, status codes, and deviation records when subjected to identical fault schedules.

## Decision
We implement the **Protocol Interoperability Lab & Conformance Matrix (`InteropLab`)** across both Python 3.12+ and TypeScript (Node 22 native type-stripping).

### 1. Formal Model: Protocol Conformance State Machine
We model telemetry client-server interactions as an explicit 6-state automaton:
$$\mathcal{S} = \{ \text{IDLE}, \text{CONNECTING}, \text{STREAMING}, \text{FRAME\_ACCUMULATION}, \text{DISPATCHING}, \text{TERMINATED} \}$$

Transitions are triggered by wire events:
- $\text{IDLE} \xrightarrow{\text{HANDSHAKE}} \text{CONNECTING}$
- $\text{CONNECTING} \xrightarrow{\text{OPEN}} \text{STREAMING}$
- $\text{STREAMING} \xrightarrow{\text{CHUNK}} \text{FRAME\_ACCUMULATION}$
- $\text{FRAME\_ACCUMULATION} \xrightarrow{\text{FRAME\_COMPLETE}} \text{DISPATCHING}$
- $\text{DISPATCHING} \xrightarrow{\text{INGEST\_ACK}} \text{STREAMING}$
- Any state $\xrightarrow{\text{CANCEL / TIMEOUT / RST}} \text{TERMINATED}$

### 2. Systematic Conformance Fault Injection Schedule
`InteropLab` defines and executes standardized fault scenarios:
- **`FRAGMENTATION`**: Splits wire frames into sub-header byte slices ($1 \text{B} \le \text{slice} \le 7 \text{B}$) to test zero-copy ring-buffer reassembly.
- **`EARLY_CANCELLATION`**: Aborts request mid-transmission (e.g., after 50% of frames sent) and verifies that partial state is cleaned up and downstream circuits remain unaffected.
- **`BACKPRESSURE_FLOW_CONTROL`**: Inundates ingestion socket with bursts exceeding buffer ceiling to verify backpressure pushback (HTTP 429 / gRPC `RESOURCE_EXHAUSTED`).
- **`MALFORMED_HEADER`**: Mutates wire headers (corrupt varint, invalid magic byte, negative length) and asserts immediate fail-closed rejection without process crash.
- **`TRUNCATED_BODY`**: Emits header claiming $N$ bytes followed by abruptly closing socket after $M < N$ bytes.

### 3. Deep Module Interface
The deep module `InteropLab` provides:
- `exercise(target_protocol, client_variant, fault_schedule, payload)`: Executes a client-server interaction against the protocol state machine under the specified fault schedule, returning a structured `ExerciseResult`.
- `record_deviation(protocol, scenario, observed, expected, severity)`: Records a detected protocol deviation from official OpenTelemetry / gRPC RFC specifications (`FATAL`, `WARNING`, `INFORMATIONAL`).
- `attest_matrix(suite_name)`: Computes an aggregate cryptographic attestation certifying compliance across tested protocols, faults, and client implementations. Emits canonical SHA-256 receipt:
  $$\text{AttestationHash} = \text{SHA256}(\text{"sw:conformance:v1:"} \parallel \text{suite\_name} \parallel \text{pass\_count} \parallel \text{fail\_count} \parallel \text{timestamp})$$
- `get_matrix()`: Returns the cumulative cross-matrix evaluation grid.
- `status()`: Returns operational telemetry on completed exercises and active deviations.

### 4. Zero-Token Cost & Sub-250µs SLO
- **Zero External LLM Tokens**: All protocol state machine transitions, framing checks, fault injection, and cryptographic matrix attestations execute 100% locally with zero external API calls.
- **Sub-250µs Execution SLO**: Hot-path exercise dispatch and frame reassembly complete in $<100\,\mu\text{s}$ for payloads under 64KB.

## Consequences

### Positive
- Guarantees standards compliance and robust client interoperability across real-world network perturbations.
- Eliminates silent span loss caused by streaming fragmentation or unhandled socket aborts.
- Verifiable conformance matrix attestation receipt provides tamper-evident audit evidence for enterprise IT and compliance teams.
- Strict dual-SDK parity across Python 3.12+ and Node 22 native type-stripping.

### Negative / Trade-offs
- Added state machine tracking overhead in test and diagnostic execution paths (isolated from hot-path production ingestion).
