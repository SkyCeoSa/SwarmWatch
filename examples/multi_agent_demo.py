# -*- coding: utf-8 -*-
"""
SwarmWatch End-to-End Multi-Agent Observability & Time-Machine Demo.
Simulates a real financial intelligence swarm with 4 cooperating agents,
demonstrating state snapshotting, causal span DAG tracing, and live telemetry.
"""

import sys
import time
import swarmwatch

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def main():
    print("\n" + "=" * 60)
    print("[SwarmWatch] Live Multi-Agent Simulation Starting...")
    print("=" * 60 + "\n")

    # 1. Initialize SwarmWatch client
    swarmwatch.init(swarm_id="hedge-fund-swarm-01", db_path="swarmwatch.db")

    # 2. Define Agent Functions with @swarmwatch.agent
    @swarmwatch.agent(name="MacroEconomist", role="analyst", target="SectorResearcher")
    def analyze_macro_trends(focus_sector: str):
        print(f"[MacroEconomist] Analyzing macroeconomic signals for sector: {focus_sector}...")
        time.sleep(0.05)
        # Snapshot agent memory state into SwarmWatch Time Machine
        swarmwatch.sync_state({
            "fed_rate": 4.5,
            "inflation_cpi": 2.4,
            "sentiment": "bullish"
        })
        return {
            "regime": "disinflationary_growth",
            "recommended_weight": "overweight_tech"
        }

    @swarmwatch.agent(name="SectorResearcher", role="researcher", target="RiskAuditor")
    def screen_equities(macro_output: dict):
        print(f"[SectorResearcher] Screening equities based on {macro_output['recommended_weight']}...")
        time.sleep(0.05)
        return {
            "candidate_tickers": ["NVDA", "MSFT", "GOOGL"],
            "expected_alpha": 0.18
        }

    @swarmwatch.agent(name="RiskAuditor", role="risk_manager", target="ExecutionTrader")
    def stress_test_portfolio(candidates: list):
        print(f"[RiskAuditor] Stress-testing candidate assets: {candidates} against 2008 liquidity shock...")
        time.sleep(0.05)
        # Snapshot portfolio risk metrics
        swarmwatch.sync_state({
            "var_99": 0.032,
            "max_drawdown": 0.08,
            "approved": True
        })
        return {"approval": "APPROVED", "size_limit_millions": 25.0}

    # 3. Run the Multi-Agent Swarm Pipeline
    with swarmwatch.span("full_investment_decision_cycle", source="SwarmDirector", target="MacroEconomist"):
        macro_report = analyze_macro_trends("semiconductors")
        equities = screen_equities(macro_report)
        risk_decision = stress_test_portfolio(equities["candidate_tickers"])

        print("\n" + "-" * 60)
        print(f"[SwarmWatch] Swarm Cycle Complete! Final Decision: {risk_decision}")
        print("-" * 60)

    # 4. Flush telemetry and launch instructions
    swarmwatch.flush()
    swarmwatch.shutdown()

    print("\n[SwarmWatch] Telemetry persisted to SQLite WAL ('swarmwatch.db').")
    print(">> Run `swarmwatch ui` to explore live swimlane DAGs, entropy metrics, and time-machine rewinds!\n")


if __name__ == "__main__":
    main()
