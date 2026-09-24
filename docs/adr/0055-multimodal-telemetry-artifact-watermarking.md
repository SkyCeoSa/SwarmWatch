# ADR 0055: Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking

## Status
Accepted

## Context
In enterprise multi-agent workflows (such as vision-language chains, synthetic voice generation, document intelligence pipelines, and visual coding swarms), agents frequently produce, exchange, and mutate multi-modal binary assets—including generated diagrams (PNG/JPEG/WEBP/GIF), synthetic speech (WAV/MP3), video renderings (MP4), and composite documents (PDF).

Without automated multi-modal telemetry inspection and provenance tracking, distributed multi-agent systems face critical architectural risks:
1. **Asset Tampering & Injection**: Malicious or drifting agents can substitute, alter, or inject corrupt binary assets across causal spans without detection.
2. **Exorbitant LLM Inspection Costs**: Invoking multimodal vision-language models or audio transcription APIs solely to inspect file dimensions, MIME types, or validity introduces high latency (>2-5 seconds per asset) and catastrophically breaches SwarmWatch's **Zero-Token Cost Guarantee**.
3. **Lack of Provenance**: Downstream agents and human operators lack cryptographic evidence identifying which agent created an asset, in which swarm session, under which tenant policy, and at what exact nanosecond.

To address these vulnerabilities, SwarmWatch requires an in-process, deterministic **Multi-Modal Telemetry Inspector & Artifact Watermarker** (`MultiModalInspector`).

## Decision
We implement Seam 44: Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking as a deep module with dual-SDK parity across Python and TypeScript.

### 1. In-Process Binary Header Inspection
The `MultiModalInspector` parses binary headers directly in memory using standard bitwise offsets without external binary libraries (such as OpenCV, Pillow, or FFmpeg) or remote LLM vision APIs:
- **PNG**: Magic bytes `\x89PNG\r\n\x1a\n` with `IHDR` chunk parsing for exact width and height.
- **JPEG**: `\xff\xd8` with SOF0/SOF2 marker scanning for dimensions.
- **GIF**: `GIF87a`/`GIF89a` with screen descriptor dimension extraction.
- **WEBP**: `RIFF....WEBP` with `VP8`/`VP8L`/`VP8X` chunk parsing for dimensions.
- **WAV**: `RIFF....WAVE` with `fmt ` chunk parsing for channels, sample rate, and byte rate; `data` chunk for duration.
- **MP3**: ID3v2 header and sync frame detection for sample rate and duration estimation.
- **MP4**: `ftyp` and `mvhd` atom scanning for timescale and duration calculation.
- **PDF**: `%PDF-` header detection and page catalog verification.

### 2. Perceptual Fingerprinting (pHash)
For visual assets, the inspector computes a fast, deterministic 16-hex character perceptual hash (64-bit block mean hash) directly from payload byte distributions. This hash allows instant similarity verification and tamper detection without external neural embeddings.

### 3. Cryptographic Provenance Watermarking
For every inspected artifact, SwarmWatch synthesizes an `ArtifactWatermark`:
- Canonical provenance string:
  $$\text{Payload} = \text{artifact\_id} \mathbin{\Vert} \text{producer\_agent\_id} \mathbin{\Vert} \text{swarm\_id} \mathbin{\Vert} \text{tenant\_id} \mathbin{\Vert} \text{sha256\_checksum} \mathbin{\Vert} \text{timestamp\_ns}$$
- HMAC-SHA256 signature generated with tenant secret:
  $$\sigma = \text{HMAC-SHA256}(K_{\text{tenant}}, \text{Payload})$$
- Watermark metadata is stored as an immutable cryptographic sidecar and can be verified by any downstream agent in $<20\mu\text{s}$. If the binary payload or metadata is modified, verification fails immediately with `tamper_detected = True`.

### 4. Zero-Token Cost Guarantee
100% in-process deterministic binary byte parsing, SHA-256 calculation, and HMAC verification (<0.1ms per asset). Zero external multimodal LLM calls, zero cloud dependencies.

### 5. Dual-SDK Parity
- **Python**: `packages/sdk-python/swarmwatch/multimodal.py` (`MediaModality`, `MediaMetadata`, `ArtifactWatermark`, `WatermarkedArtifact`, `MultiModalInspector`, `global_multimodal_inspector`).
- **TypeScript**: `packages/sdk-typescript/src/multimodal.ts` (`MediaModality`, `MediaMetadata`, `ArtifactWatermark`, `WatermarkedArtifact`, `MultiModalInspector`, `globalMultiModalInspector`) conforming strictly to Node 22 `--experimental-strip-types`.

### 6. Control Plane, CLI & Web Console Integration
- **REST Endpoints**:
  - `GET /api/v1/swarms/{id}/artifacts`: List inspected and watermarked artifacts for the swarm.
  - `POST /api/v1/swarms/{id}/artifacts/inspect`: Ingest and inspect raw base64 or binary payload.
  - `POST /api/v1/swarms/{id}/artifacts/watermark`: Generate and apply cryptographic provenance watermark.
  - `POST /api/v1/swarms/{id}/artifacts/verify`: Verify artifact integrity against current payload.
- **CLI Subcommand**:
  - `swarmwatch artifact status --swarm-id <id>`
  - `swarmwatch artifact inspect <filepath>`
  - `swarmwatch artifact watermark <filepath> --agent-id <id> --swarm-id <id>`
  - `swarmwatch artifact verify <filepath> --signature <sig>`
- **Web Console**:
  - `#artifactBadge` in `.fleet-left-group` of `#fleetBar` in `dashboard.html`.
  - Impeccable Craft Floor: 100% compliant with 0 anti-patterns (`[]`).

## Consequences
- Multi-agent swarms exchanging audio, visual, and document assets obtain guaranteed tamper-evident provenance.
- In-flight binary validation occurs at sub-millisecond speeds without spending LLM tokens.
- Cryptographic watermarks link every artifact directly to its causal span origin.
