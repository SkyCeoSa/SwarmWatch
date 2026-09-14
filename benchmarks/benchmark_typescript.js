/**
 * SwarmWatch TypeScript Performance Benchmark.
 * Measures in-process telemetry emission latency in V8,
 * Shannon entropy throughput, and state serialization hashing performance.
 */

import { performance } from "node:perf_hooks";
import {
  computeShannonEntropy,
  computeLexicalSimilarity,
  SafeStateSerializer,
  TelemetryTransport,
  InMemoryStorageSink,
} from "../packages/sdk-typescript/src/index.ts";

function percentile(data, p) {
  if (!data || data.length === 0) return 0.0;
  const sorted = [...data].sort((a, b) => a - b);
  const k = (sorted.length - 1) * (p / 100.0);
  const f = Math.floor(k);
  const c = Math.min(f + 1, sorted.length - 1);
  const d = k - f;
  return sorted[f] + d * (sorted[c] - sorted[f]);
}

async function runBenchmark(nSpans = 5000, nEntropyOps = 10000) {
  const results = {};

  // -------------------------------------------------------------------------
  // 1. In-Process Telemetry Ring Buffer Latency Benchmark
  // -------------------------------------------------------------------------
  const sink = new InMemoryStorageSink();
  const transport = new TelemetryTransport(sink, {
    batch_size: 500,
    max_queue_size: nSpans + 2000,
  });
  transport.start();

  const baseSpan = {
    id: "span-bench-01",
    swarm_id: "bench-swarm",
    source_agent: "AgentBenchmark",
    target_agent: "Database",
    span_type: "agent_call",
    start_time: new Date().toISOString(),
    end_time: new Date().toISOString(),
  };

  // Warm-up (1000 iterations)
  for (let i = 0; i < 1000; i++) {
    transport.emitSpan(baseSpan);
  }
  await transport.flush();
  sink.clear();

  const latenciesUs = [];

  // Timed run
  for (let i = 0; i < nSpans; i++) {
    const t0 = performance.now();
    transport.emitSpan(baseSpan);
    const t1 = performance.now();
    latenciesUs.push((t1 - t0) * 1000.0); // milliseconds to microseconds
  }

  await transport.shutdown();

  const sumLatencies = latenciesUs.reduce((a, b) => a + b, 0);
  results.emission = {
    count: nSpans,
    mean_us: sumLatencies / latenciesUs.length,
    p50_us: percentile(latenciesUs, 50),
    p95_us: percentile(latenciesUs, 95),
    p99_us: percentile(latenciesUs, 99),
    max_us: Math.max(...latenciesUs),
  };

  // -------------------------------------------------------------------------
  // 2. Normalized Shannon Entropy Throughput Benchmark
  // -------------------------------------------------------------------------
  const sampleText = (
    "Agent received customer support ticket regarding API gateway rate limiting. " +
    "Analyzing request payload, headers, and authentication tokens before routing to microservice."
  );

  // Warm-up
  for (let i = 0; i < 1000; i++) {
    computeShannonEntropy(sampleText);
  }

  const tEntropyStart = performance.now();
  for (let i = 0; i < nEntropyOps; i++) {
    computeShannonEntropy(sampleText);
  }
  const tEntropyEnd = performance.now();

  const entropyDuration = Math.max(0.0001, (tEntropyEnd - tEntropyStart) / 1000.0);
  const entropyOpsSec = Math.floor(nEntropyOps / entropyDuration);
  results.entropy = {
    ops: nEntropyOps,
    duration_sec: Number(entropyDuration.toFixed(4)),
    ops_per_sec: entropyOpsSec,
  };

  // -------------------------------------------------------------------------
  // 3. Lexical Jaccard Similarity Throughput Benchmark
  // -------------------------------------------------------------------------
  const textA = "Agent received customer support ticket regarding API gateway rate limiting.";
  const textB = "Agent analyzing customer support ticket for API gateway rate limits.";

  const tJaccardStart = performance.now();
  for (let i = 0; i < nEntropyOps; i++) {
    computeLexicalSimilarity(textA, textB);
  }
  const tJaccardEnd = performance.now();

  const jaccardDuration = Math.max(0.0001, (tJaccardEnd - tJaccardStart) / 1000.0);
  const jaccardOpsSec = Math.floor(nEntropyOps / jaccardDuration);
  results.jaccard = {
    ops: nEntropyOps,
    duration_sec: Number(jaccardDuration.toFixed(4)),
    ops_per_sec: jaccardOpsSec,
  };

  // -------------------------------------------------------------------------
  // 4. Safe State Serializer & SHA-256 Hashing Benchmark
  // -------------------------------------------------------------------------
  const serializer = new SafeStateSerializer();
  const sampleState = {
    user_id: "usr_9921",
    api_key: "sk-proj-super-secret-key-12345",
    nested: { tokens: 450, authorized: true },
    tags: ["prod", "us-east-1", "copilot"],
  };

  const nSerializerOps = 10000;
  const tSerializerStart = performance.now();
  for (let i = 0; i < nSerializerOps; i++) {
    const sanitized = serializer.sanitize(sampleState);
    serializer.computeHash(sanitized);
  }
  const tSerializerEnd = performance.now();

  const serializerDuration = Math.max(0.0001, (tSerializerEnd - tSerializerStart) / 1000.0);
  const serializerOpsSec = Math.floor(nSerializerOps / serializerDuration);
  results.serializer = {
    ops: nSerializerOps,
    duration_sec: Number(serializerDuration.toFixed(4)),
    ops_per_sec: serializerOpsSec,
  };

  return results;
}

async function main() {
  const results = await runBenchmark();
  if (process.argv.includes("--json")) {
    process.stdout.write(JSON.stringify(results) + "\n");
    return;
  }

  const width = 72;
  console.log("\n" + "=".repeat(width));
  console.log("  SwarmWatch TypeScript SDK Performance Benchmark Report");
  console.log("=".repeat(width));

  const em = results.emission;
  console.log(`\n1. In-Process Telemetry Ring Buffer Latency (${em.count.toLocaleString()} spans):`);
  console.log(`   Mean Latency        : ${em.mean_us.toFixed(3).padStart(8)} us`);
  console.log(`   Median (p50) Latency: ${em.p50_us.toFixed(3).padStart(8)} us`);
  console.log(`   95th Percentile     : ${em.p95_us.toFixed(3).padStart(8)} us`);
  console.log(`   99th Percentile     : ${em.p99_us.toFixed(3).padStart(8)} us`);
  console.log(`   Max Latency         : ${em.max_us.toFixed(3).padStart(8)} us`);

  const ent = results.entropy;
  console.log(`\n2. Shannon Entropy Math Engine (${ent.ops.toLocaleString()} iterations):`);
  console.log(`   Duration            : ${ent.duration_sec.toFixed(4).padStart(8)} sec`);
  console.log(`   Throughput          : ${ent.ops_per_sec.toLocaleString().padStart(8)} ops/sec`);

  const jac = results.jaccard;
  console.log(`\n3. Jaccard Lexical Similarity Engine (${jac.ops.toLocaleString()} iterations):`);
  console.log(`   Duration            : ${jac.duration_sec.toFixed(4).padStart(8)} sec`);
  console.log(`   Throughput          : ${jac.ops_per_sec.toLocaleString().padStart(8)} ops/sec`);

  const ser = results.serializer;
  console.log(`\n4. Safe State Serializer & SHA-256 Hashing (${ser.ops.toLocaleString()} iterations):`);
  console.log(`   Duration            : ${ser.duration_sec.toFixed(4).padStart(8)} sec`);
  console.log(`   Throughput          : ${ser.ops_per_sec.toLocaleString().padStart(8)} ops/sec`);
  console.log("\n" + "=".repeat(width) + "\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
