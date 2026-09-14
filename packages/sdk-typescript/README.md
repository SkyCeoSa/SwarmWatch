# @swarmwatch/sdk

Universal Multi-Agent Observability, Zero-Token Shannon Entropy Circuit Breaker & Safe Time-Machine for TypeScript and Node.js.

## Highlights
- **Zero-Token Cost Guarantee**: All entropy calculations, similarity metrics, and circuit breakers run 100% locally in-process with 0 external API costs.
- **Enterprise Multi-Tenancy**: Scoped `Organization`, `Project`, and cryptographic `ApiKey` management (`sw_live_...` / `sw_test_...`) with constant-time verification.
- **OpenTelemetry Native**: Direct bidirectional mapping to OpenTelemetry GenAI Semantic Conventions (`gen_ai.agent.name`, `gen_ai.usage.total_tokens`, `gen_ai.system`).
- **Framework Adapters**: First-class adapters for Vercel AI SDK and LangChain.js.
- **Sub-Microsecond Transport**: Bounded ring buffer ensures zero latency overhead on host agent reasoning loops.

## Installation

```bash
npm install @swarmwatch/sdk
```

## Quick Start

```typescript
import { SwarmClient, HttpStorageSink } from "@swarmwatch/sdk";

const client = new SwarmClient({
  swarm_id: "agent-swarm-prod-01",
  project_id: "fintech-arbitrage",
  api_key: "sw_live_your_api_key_here",
  sink: new HttpStorageSink({
    endpoint: "http://localhost:8765",
    apiKey: "sw_live_your_api_key_here",
    projectId: "fintech-arbitrage"
  })
});

// Trace agent execution with parent-child lineage
await client.withSpan({ name: "MarketAnalyzer", target: "RiskManager" }, async (span) => {
  const result = await runAnalysis();
  
  // Safe state snapshot
  await client.syncState(span.id, { portfolioValue: 1250000 });
  return result;
});
```

## License
Apache-2.0
