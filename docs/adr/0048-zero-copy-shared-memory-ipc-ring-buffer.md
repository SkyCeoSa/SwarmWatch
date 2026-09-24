# ADR 0048: Zero-Copy Shared-Memory Ring Buffer IPC Gateway (Cross-Process Telemetry Acceleration)

## Status
Accepted

## Context
In high-throughput multi-agent swarms (such as parallel multi-core simulations, financial backtesting, local sandbox processes, and containerized agent pods), telemetry volume frequently exceeds 50,000 to 200,000 spans per second. Emitting each turn or span over HTTP or TCP loopback sockets introduces significant overhead:
1. HTTP request framing and TLS/socket handshakes.
2. Kernel context switches on socket send/recv buffers.
3. Latency penalty of 200µs to 2ms per batch on calling agent threads, violating the sub-microsecond non-blocking invariant of SwarmWatch.

We require a zero-external-dependency, zero-copy, cross-process IPC telemetry ingestion channel that operates directly via shared physical memory (`mmap` / POSIX `SharedMemory` / Windows named shared memory), allowing agent processes to record spans in $<1.0\mu\text{s}$ with zero kernel socket transitions.

## Decision
We implement a zero-copy shared-memory circular ring buffer (`ShmRingBuffer`) and background ingestion gateway (`ShmIpcGateway`):

1. **Memory-Mapped Binary Layout**:
   - Fixed 64-byte Header:
     - `magic`: 4 bytes (`b"SWRB"`, SwarmWatch Ring Buffer)
     - `version`: uint16 (`1`)
     - `flags`: uint16 (reserved/options)
     - `capacity_slots`: uint32 (e.g. 1024 or 4096 fixed slots)
     - `slot_size`: uint32 (e.g. 2048 bytes per slot)
     - `write_head`: uint64 (monotonically increasing write sequence)
     - `read_tail`: uint64 (monotonically increasing read sequence)
     - `dropped_records`: uint64 (overflow counter when `write_head - read_tail >= capacity_slots`)
     - `total_written`: uint64
     - `total_read`: uint64
     - `reserved`: 16 bytes
   - Fixed Slot Layout:
     - `status`: uint8 (`0=EMPTY`, `1=WRITING`, `2=READY`, `3=READING`)
     - `payload_len`: uint32
     - `payload_data`: byte payload (compact serialized span JSON)

2. **Concurrency & Thread Safety**:
   - Write Operation: Producer advances write sequence, updates slot to `WRITING`, writes payload, sets slot to `READY`. If buffer is full, producer either overwrites oldest or increments `dropped_records` without blocking agent execution.
   - Read Operation: Consumer gateway reads ready slots from `read_tail`, marks `READING` -> `EMPTY`, advances `read_tail`, and passes spans to SwarmWatch storage.

3. **Dual SDK Parity**:
   - Python: `packages/sdk-python/swarmwatch/shm_ring.py` using `multiprocessing.shared_memory.SharedMemory` with anonymous memoryview fallback.
   - TypeScript: `packages/sdk-typescript/src/shm_ring.ts` using portable Node.js buffer and memory framing conforming strictly to Node 22 `--experimental-strip-types`.

4. **Control Plane & Web Console**:
   - REST endpoints: `GET /api/v1/shm/status`, `POST /api/v1/shm/flush`, `POST /api/v1/shm/reset`.
   - CLI subcommands: `swarmwatch shm status`, `swarmwatch shm flush`.
   - Web Console: `#shmBadge` inside `#fleetBar` in `dashboard.html` with real-time pending slot count and overflow alerts.

## Consequences
- **Positive**: Sub-microsecond (<1.0µs) span recording directly in shared memory; $>500,000$ spans/sec throughput; 0 kernel socket context switches; 0 external dependencies; 0 remote token costs.
- **Negative**: Memory mapped buffers must be sized appropriately (e.g., 2MB to 16MB) based on peak burst volume.
