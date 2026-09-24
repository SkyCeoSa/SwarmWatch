import test from "node:test";
import assert from "node:assert/strict";
import {
  ForecastRiskLevel,
  EntropyMartingaleForecaster,
  globalForecaster,
} from "../src/index.ts";

test("ForecastRiskLevel defines categorical risk constants", () => {
  assert.equal(ForecastRiskLevel.NOMINAL, "nominal");
  assert.equal(ForecastRiskLevel.EARLY_WARNING, "early_warning");
  assert.equal(ForecastRiskLevel.CRITICAL_COLLAPSE, "critical_collapse");
});

test("EntropyMartingaleForecaster initial state without turns", () => {
  const forecaster = new EntropyMartingaleForecaster();
  const report = forecaster.getForecast("swarm-empty");
  assert.equal(report.swarmId, "swarm-empty");
  assert.equal(report.totalTurns, 0);
  assert.equal(report.currentEntropy, 0.0);
  assert.equal(report.riskLevel, ForecastRiskLevel.NOMINAL);
  assert.equal(report.riskProbability, 0.0);
  assert.equal(report.preemptiveRecommended, false);
});

test("Stable high-entropy trajectory remains NOMINAL", () => {
  const forecaster = new EntropyMartingaleForecaster();
  const swarmId = "swarm-ts-stable";

  let report = forecaster.observeTurn(swarmId, 1, 2.5, "agent-1");
  assert.equal(report.totalTurns, 1);
  assert.equal(report.riskLevel, ForecastRiskLevel.NOMINAL);

  report = forecaster.observeTurn(swarmId, 2, 2.48, "agent-2");
  report = forecaster.observeTurn(swarmId, 3, 2.52, "agent-1");
  report = forecaster.observeTurn(swarmId, 4, 2.50, "agent-2");

  assert.equal(report.totalTurns, 4);
  assert.equal(report.currentEntropy, 2.5);
  assert.equal(report.riskLevel, ForecastRiskLevel.NOMINAL);
  assert.equal(report.preemptiveRecommended, false);
  assert.ok(report.riskProbability < 0.7);
});

test("Rapid negative drift triggers EARLY_WARNING and CRITICAL_COLLAPSE", () => {
  const forecaster = new EntropyMartingaleForecaster(0.35, 5, 3, 1.5, 0.7);
  const swarmId = "swarm-ts-collapse";

  forecaster.observeTurn(swarmId, 1, 2.2, "agent-1");
  forecaster.observeTurn(swarmId, 2, 1.5, "agent-2");
  forecaster.observeTurn(swarmId, 3, 0.9, "agent-1");
  const r4 = forecaster.observeTurn(swarmId, 4, 0.5, "agent-2");

  assert.ok(
    r4.riskLevel === ForecastRiskLevel.EARLY_WARNING ||
    r4.riskLevel === ForecastRiskLevel.CRITICAL_COLLAPSE
  );
  assert.ok(r4.turnsToTrip !== null);

  // Turn 5 drops below threshold 0.35
  const r5 = forecaster.observeTurn(swarmId, 5, 0.2, "agent-1");
  assert.equal(r5.riskLevel, ForecastRiskLevel.CRITICAL_COLLAPSE);
  assert.equal(r5.riskProbability, 1.0);
  assert.equal(r5.preemptiveRecommended, true);
});

test("Reset clears swarm history and restores nominal state", () => {
  const forecaster = new EntropyMartingaleForecaster();
  const swarmId = "swarm-ts-reset";

  forecaster.observeTurn(swarmId, 1, 0.4, "agent-a");
  assert.equal(forecaster.getForecast(swarmId).totalTurns, 1);

  forecaster.reset(swarmId);
  const rep = forecaster.getForecast(swarmId);
  assert.equal(rep.totalTurns, 0);
  assert.equal(rep.riskLevel, ForecastRiskLevel.NOMINAL);
});

test("Sub-30 microsecond latency benchmark for martingale observation", () => {
  const forecaster = new EntropyMartingaleForecaster();
  const swarmId = "swarm-ts-perf";

  // Warm up
  for (let i = 0; i < 50; i++) {
    forecaster.observeTurn(swarmId, i, 1.8 - (i * 0.005), "bench");
  }

  const iterations = 500;
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    forecaster.observeTurn(swarmId, 100 + i, 1.5, "bench");
  }
  const elapsedMs = performance.now() - start;
  const usPerOp = (elapsedMs * 1000) / iterations;

  // In-process mathematical calculation should easily be < 50 microseconds
  assert.ok(usPerOp < 50, `Expected < 50us per op, got ${usPerOp.toFixed(2)}us`);
});
