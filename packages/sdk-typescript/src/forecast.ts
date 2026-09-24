/**
 * SwarmWatch Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales (Seam 39 / ADR 0050).
 * Calculates turn-to-turn conditional entropy drift, supermartingale convergence processes,
 * and Azuma-Hoeffding concentration risk bounds to predict runaway conversational failure 2-4 turns in advance.
 *
 * Conforms strictly to Node 22 type-stripping rules: no TS enums, no parameter properties.
 */

export const ForecastRiskLevel = {
  NOMINAL: "nominal",
  EARLY_WARNING: "early_warning",
  CRITICAL_COLLAPSE: "critical_collapse",
} as const;

export type ForecastRiskLevelType = typeof ForecastRiskLevel[keyof typeof ForecastRiskLevel];

export interface MartingaleStep {
  turnIndex: number;
  entropy: number;
  delta: number;
  centeredDiff: number;
  cumulativeMartingale: number;
  agent?: string;
}

export interface SwarmForecastReport {
  swarmId: string;
  totalTurns: number;
  currentEntropy: number;
  threshold: number;
  driftRate: number;
  turnsToTrip: number | null;
  riskProbability: number;
  riskLevel: ForecastRiskLevelType;
  preemptiveRecommended: boolean;
  justification: string;
  history: MartingaleStep[];
}

export class EntropyMartingaleForecaster {
  public threshold: number;
  public windowSize: number;
  public horizon: number;
  public maxDifferenceBound: number;
  public riskThreshold: number;
  private swarmHistories: Map<string, MartingaleStep[]>;

  constructor(
    threshold = 0.35,
    windowSize = 5,
    horizon = 3,
    maxDifferenceBound = 1.5,
    riskThreshold = 0.70
  ) {
    this.threshold = threshold;
    this.windowSize = windowSize;
    this.horizon = horizon;
    this.maxDifferenceBound = maxDifferenceBound;
    this.riskThreshold = riskThreshold;
    this.swarmHistories = new Map();
  }

  /**
   * Records an execution turn's Shannon entropy, updates the martingale sequence,
   * and computes the real-time early warning forecast.
   */
  public observeTurn(
    swarmId: string,
    turnIndex: number,
    entropy: number,
    agent = ""
  ): SwarmForecastReport {
    let history = this.swarmHistories.get(swarmId);
    if (!history) {
      history = [];
      this.swarmHistories.set(swarmId, history);
    }

    const currEntropy = Math.max(0.0, Math.min(8.0, Number(entropy)));

    if (history.length === 0) {
      const step: MartingaleStep = {
        turnIndex,
        entropy: currEntropy,
        delta: 0.0,
        centeredDiff: 0.0,
        cumulativeMartingale: 0.0,
        agent,
      };
      history.push(step);
      return this.generateReport(swarmId, currEntropy, 0.0, 0.0);
    }

    const prevEntropy = history[history.length - 1].entropy;
    const delta = currEntropy - prevEntropy;

    // Sliding window drift estimation
    const recentDeltas = history
      .slice(-this.windowSize)
      .map((s) => s.delta)
      .filter((d) => d !== 0.0);
    const baselineMean =
      recentDeltas.length > 0
        ? recentDeltas.reduce((a, b) => a + b, 0) / recentDeltas.length
        : 0.0;

    const centeredDiff = delta - baselineMean;
    const prevM = history[history.length - 1].cumulativeMartingale;
    const cumulativeM = prevM + centeredDiff;

    const step: MartingaleStep = {
      turnIndex,
      entropy: currEntropy,
      delta: Math.round(delta * 10000) / 10000,
      centeredDiff: Math.round(centeredDiff * 10000) / 10000,
      cumulativeMartingale: Math.round(cumulativeM * 10000) / 10000,
      agent,
    };
    history.push(step);

    // Compute effective drift rate over recent turns
    const windowSteps = history.slice(-this.windowSize);
    let effectiveDrift = delta;
    if (windowSteps.length >= 2) {
      const totalDelta =
        windowSteps[windowSteps.length - 1].entropy - windowSteps[0].entropy;
      const turnsSpan = Math.max(1, windowSteps.length - 1);
      effectiveDrift = totalDelta / turnsSpan;
    }

    return this.generateReport(swarmId, currEntropy, delta, effectiveDrift);
  }

  private generateReport(
    swarmId: string,
    currentEntropy: number,
    _lastDelta: number,
    driftRate: number
  ): SwarmForecastReport {
    const history = this.swarmHistories.get(swarmId) || [];
    const totalTurns = history.length;
    const lambdaGap = Math.max(0.0, currentEntropy - this.threshold);

    // Baseline case: too few turns for meaningful statistical projection
    if (totalTurns < 2) {
      return {
        swarmId,
        totalTurns,
        currentEntropy: Math.round(currentEntropy * 10000) / 10000,
        threshold: this.threshold,
        driftRate: Math.round(driftRate * 10000) / 10000,
        turnsToTrip: null,
        riskProbability: 0.0,
        riskLevel: ForecastRiskLevel.NOMINAL,
        preemptiveRecommended: false,
        justification: "Insufficient history: calibrating baseline filtration.",
        history: [...history],
      };
    }

    // Entropy is already at or below threshold
    if (currentEntropy <= this.threshold) {
      return {
        swarmId,
        totalTurns,
        currentEntropy: Math.round(currentEntropy * 10000) / 10000,
        threshold: this.threshold,
        driftRate: Math.round(driftRate * 10000) / 10000,
        turnsToTrip: 0.0,
        riskProbability: 1.0,
        riskLevel: ForecastRiskLevel.CRITICAL_COLLAPSE,
        preemptiveRecommended: true,
        justification: `Current entropy ${currentEntropy.toFixed(4)} is at or below critical threshold ${this.threshold.toFixed(2)}.`,
        history: [...history],
      };
    }

    // Negative drift indicates potential supermartingale trajectory towards collapse
    if (driftRate < -0.01) {
      const absDrift = Math.abs(driftRate);
      const estimatedTurns = Math.max(0.1, lambdaGap / absDrift);

      // Azuma-Hoeffding Tail Bound for supermartingale:
      // P(H_{t+h} - H_t <= -lambda) >= 1 - exp(-2 * (abs_drift * h)^2 / (h * c^2))
      const h = Number(this.horizon);
      const c = this.maxDifferenceBound;
      const projectedDrop = absDrift * h;

      let riskProb = 0.0;
      if (projectedDrop >= lambdaGap) {
        const excess = projectedDrop - lambdaGap;
        riskProb = Math.min(
          0.9999,
          0.5 + 0.5 * (1.0 - Math.exp((-2.0 * Math.pow(excess + 0.1, 2)) / (h * c * c)))
        );
      } else {
        const shortfall = lambdaGap - projectedDrop;
        riskProb = Math.max(
          0.01,
          0.5 * Math.exp((-2.0 * Math.pow(shortfall, 2)) / (h * c * c))
        );
      }

      riskProb = Math.round(Math.min(1.0, Math.max(0.0, riskProb)) * 10000) / 10000;
      const turnsToTrip = Math.round(estimatedTurns * 100) / 100;

      let level: ForecastRiskLevelType = ForecastRiskLevel.NOMINAL;
      let preempt = false;
      let just = "";

      if (turnsToTrip <= 2.0 || riskProb >= this.riskThreshold) {
        level = ForecastRiskLevel.CRITICAL_COLLAPSE;
        preempt = true;
        just = `Supermartingale failure imminent: entropy dropping by ${absDrift.toFixed(3)} bits/turn. Trip predicted in ${turnsToTrip} turns (confidence: ${(riskProb * 100).toFixed(1)}%).`;
      } else if (turnsToTrip <= 5.0 || riskProb >= 0.3) {
        level = ForecastRiskLevel.EARLY_WARNING;
        preempt = false;
        just = `Early warning: steady negative drift of ${absDrift.toFixed(3)} bits/turn detected. Horizon to threshold: ${turnsToTrip} turns (confidence: ${(riskProb * 100).toFixed(1)}%).`;
      } else {
        level = ForecastRiskLevel.NOMINAL;
        preempt = false;
        just = `Stable trajectory: safe margin of ${turnsToTrip} turns remaining.`;
      }

      return {
        swarmId,
        totalTurns,
        currentEntropy: Math.round(currentEntropy * 10000) / 10000,
        threshold: this.threshold,
        driftRate: Math.round(driftRate * 10000) / 10000,
        turnsToTrip,
        riskProbability: riskProb,
        riskLevel: level,
        preemptiveRecommended: preempt,
        justification: just,
        history: [...history],
      };
    }

    // Positive or flat drift: nominal health
    return {
      swarmId,
      totalTurns,
      currentEntropy: Math.round(currentEntropy * 10000) / 10000,
      threshold: this.threshold,
      driftRate: Math.round(driftRate * 10000) / 10000,
      turnsToTrip: null,
      riskProbability: 0.0,
      riskLevel: ForecastRiskLevel.NOMINAL,
      preemptiveRecommended: false,
      justification: `Nominal operation: non-negative drift (${driftRate >= 0 ? "+" : ""}${driftRate.toFixed(3)} bits/turn); entropy stable.`,
      history: [...history],
    };
  }

  public getForecast(swarmId: string): SwarmForecastReport {
    const history = this.swarmHistories.get(swarmId);
    if (!history || history.length === 0) {
      return {
        swarmId,
        totalTurns: 0,
        currentEntropy: 0.0,
        threshold: this.threshold,
        driftRate: 0.0,
        turnsToTrip: null,
        riskProbability: 0.0,
        riskLevel: ForecastRiskLevel.NOMINAL,
        preemptiveRecommended: false,
        justification: "No turns observed for this swarm session.",
        history: [],
      };
    }
    const curr = history[history.length - 1].entropy;
    const lastDelta = history[history.length - 1].delta;
    const recentDeltas = history
      .slice(-this.windowSize)
      .map((s) => s.delta)
      .filter((d) => d !== 0.0);
    const drift =
      recentDeltas.length > 0
        ? recentDeltas.reduce((a, b) => a + b, 0) / recentDeltas.length
        : 0.0;
    return this.generateReport(swarmId, curr, lastDelta, drift);
  }

  public reset(swarmId?: string): void {
    if (swarmId) {
      this.swarmHistories.delete(swarmId);
    } else {
      this.swarmHistories.clear();
    }
  }
}

export const globalForecaster = new EntropyMartingaleForecaster();
