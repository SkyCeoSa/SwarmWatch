# ADR 0040: eBPF & Kernel-Level Network Socket LLM Interception Gateway

## Status
Accepted

## Context
In production AI environments, enterprise swarms are composed of diverse runtimes: uninstrumented third-party agent binaries, Python/Node.js scripts running closed-source agents, containerized tools executing shell tasks, and external microservices. While SwarmWatch provides rich SDKs and an OpenTelemetry OTLP Ingestion Gateway (ADR 0038), agents that omit tracing or malicious runaway loops can completely bypass user-space SDK instrumentation.

To enforce zero-trust governance and eliminate rogue token burn or confidential data leakage without modifying client code, SwarmWatch requires an un-bypassable kernel-level socket interception gateway. This layer must operate directly on TCP socket streams, inspect outbound LLM payloads, evaluate Shannon entropy and budget limits, and execute immediate physical connection disconnections (TCP RST / socket shutdown) when anomalies occur.

## Decision Drivers
1. **Un-Bypassable Observation & Enforcement**: Closed-source or uninstrumented AI agents running inside containers or host networks cannot evade socket-level interception.
2. **Real-Time Physical Intervention (TCP RST)**: When an infinite tool recursion, Shannon entropy collapse, or token quota breach occurs, terminating the TCP socket immediately severs the connection before runaway tokens are billed.
3. **Zero Third-Party Dependency Invariant**: Must operate on standard library networking primitives (`socket.socket`, `asyncio.Protocol`, pure byte stream parsing) with fallback transparent proxy and mock simulation engines for 100% deterministic testing across all operating systems (Windows, macOS, Linux).
4. **Dual-SDK Parity**: Symmetrical protocol dissection, rule evaluation, and socket management across Python and TypeScript (`@swarmwatch/sdk`).
5. **Zero-Token Cost Guarantee**: All payload analysis, token estimations, and rule matching execute in-memory with sub-microsecond latency and zero external LLM API costs.

## Architecture

### 1. Dual Backend Architecture
- **Linux eBPF Socket Filter (`BPF_PROG_TYPE_SOCKET_FILTER` / `sock_ops`)**: On Linux hosts with root/`CAP_BPF`, attaches eBPF bytecode to TCP sockets on configured ports (`80`, `443`, `11434`, `8000`).
- **Transparent Socket Gateway (`TransparentSocketGateway`)**: Pure Python/TypeScript non-blocking socket proxy that transparently relays and inspects TCP connections redirected via `iptables`/`nftables` or local socks listeners.
- **Mock eBPF Engine (`MockEbpfEngine`)**: Thread-safe in-memory stream simulator enabling deterministic test execution in $<10\text{ms}$ on any platform.

### 2. Stream Protocol Decoder (`SocketStreamDecoder`)
Parses raw TCP byte streams without third-party HTTP libraries:
- Extracts HTTP request headers (Method, Path, Host, Authorization).
- Disassembles JSON request bodies (`messages`, `prompt`, `model`, `max_tokens`).
- Decodes Server-Sent Events (SSE) streaming response chunks (`data: {"choices": [...]}`).
- Estimates token counts and prompt entropy in real time.

### 3. Circuit Breaker & Socket Intervention (`TcpRstEmitter`)
- Evaluates active interception rules (`SocketRule`).
- If an entropy collapse ($<0.35$), cyclical repetition, or quota breach is detected, dispatches a physical `SocketIntervention`:
  - Emits TCP RST packet or executes immediate socket shutdown (`SHUT_RDWR`).
  - Records Prometheus metric `swarmwatch_ebpf_rst_interventions_total`.
  - Dispatches fleet trip broadcast to WebSocket, SSE, and WebPush channels.

## Consequences
### Positive
- Closed-source and uninstrumented agent frameworks (e.g. CLI tools, Docker containers) are monitored without SDK code modifications.
- Rogue loops are halted physically at the network boundary, preventing costly API overages.
- Full parity between Python and TypeScript SDKs.
- 100% deterministic local offline testing across Windows and Linux.

### Negative
- Intercepting TLS traffic directly at the socket level requires either a transparent TLS termination proxy or local loopback inspection (e.g., local Ollama/vLLM endpoints on port 11434/8000, or trusted local CA certs).
