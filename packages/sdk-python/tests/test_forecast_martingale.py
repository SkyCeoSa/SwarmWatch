# -*- coding: utf-8 -*-
"""
Tests for Seam 39 (ADR 0050):
Predictive Multi-Agent Failure Forecasting & Early-Warning Entropy Martingales.
"""

import threading
import pytest
from fastapi.testclient import TestClient

from swarmwatch.forecast import (
    ForecastRiskLevel,
    MartingaleStep,
    SwarmForecastReport,
    EntropyMartingaleForecaster,
    global_forecaster,
)
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.cli import main


@pytest.fixture
def fresh_forecaster():
    """Returns a newly initialized, clean EntropyMartingaleForecaster."""
    return EntropyMartingaleForecaster(
        threshold=0.35,
        window_size=5,
        horizon=3,
        max_difference_bound=1.5,
        risk_threshold=0.70,
    )


def test_forecast_risk_levels():
    assert ForecastRiskLevel.NOMINAL == "nominal"
    assert ForecastRiskLevel.EARLY_WARNING == "early_warning"
    assert ForecastRiskLevel.CRITICAL_COLLAPSE == "critical_collapse"


def test_martingale_step_and_report_serialization():
    step = MartingaleStep(
        turn_index=1,
        entropy=1.85,
        delta=-0.15,
        centered_diff=-0.10,
        cumulative_martingale=-0.05,
        agent="coder",
    )
    d = step.to_dict()
    assert d["turn_index"] == 1
    assert d["entropy"] == 1.85
    assert d["delta"] == -0.15
    assert d["centered_diff"] == -0.10
    assert d["cumulative_martingale"] == -0.05
    assert d["agent"] == "coder"

    rep = SwarmForecastReport(
        swarm_id="swarm-test-01",
        total_turns=1,
        current_entropy=1.85,
        threshold=0.35,
        drift_rate=-0.05,
        turns_to_trip=15.0,
        risk_probability=0.04,
        risk_level=ForecastRiskLevel.NOMINAL,
        preemptive_recommended=False,
        justification="Entropy drift within nominal bounds",
        history=[step.to_dict()],
    )
    rep_dict = rep.to_dict()
    assert rep_dict["swarm_id"] == "swarm-test-01"
    assert rep_dict["risk_level"] == "nominal"
    assert rep_dict["risk_probability"] == 0.04
    assert len(rep_dict["history"]) == 1


def test_stable_trajectory_stays_nominal(fresh_forecaster):
    swarm_id = "swarm-stable"
    # Provide stable high-entropy turns
    for t, h in enumerate([2.5, 2.45, 2.52, 2.48, 2.50], start=1):
        report = fresh_forecaster.observe_turn(
            swarm_id=swarm_id,
            turn_index=t,
            entropy=h,
            agent="agent-a",
        )

    assert report.risk_level == ForecastRiskLevel.NOMINAL
    assert report.risk_probability < 0.70
    assert report.preemptive_recommended is False
    assert report.total_turns == 5
    assert report.current_entropy == 2.50


def test_declining_trajectory_triggers_early_warning_and_critical(fresh_forecaster):
    swarm_id = "swarm-collapse"
    # Provide rapid decline leading to predicted collapse
    # Step 1: 2.2
    fresh_forecaster.observe_turn(swarm_id=swarm_id, turn_index=1, entropy=2.2, agent="agent-1")
    # Step 2: 1.5 (delta -0.7)
    fresh_forecaster.observe_turn(swarm_id=swarm_id, turn_index=2, entropy=1.5, agent="agent-2")
    # Step 3: 0.9 (delta -0.6)
    fresh_forecaster.observe_turn(swarm_id=swarm_id, turn_index=3, entropy=0.9, agent="agent-1")
    # Step 4: 0.5 (delta -0.4, near threshold 0.35, next turns will breach 0.35)
    r4 = fresh_forecaster.observe_turn(swarm_id=swarm_id, turn_index=4, entropy=0.5, agent="agent-2")

    assert r4.risk_level in (ForecastRiskLevel.EARLY_WARNING, ForecastRiskLevel.CRITICAL_COLLAPSE)
    assert r4.turns_to_trip is not None

    # Step 5: drops to 0.2 (below threshold 0.35)
    r5 = fresh_forecaster.observe_turn(swarm_id=swarm_id, turn_index=5, entropy=0.2, agent="agent-1")
    assert r5.risk_level == ForecastRiskLevel.CRITICAL_COLLAPSE
    assert r5.risk_probability == 1.0
    assert r5.preemptive_recommended is True


def test_forecast_reset(fresh_forecaster):
    swarm_id = "swarm-reset-me"
    fresh_forecaster.observe_turn(swarm_id=swarm_id, turn_index=1, entropy=0.4, agent="agent-x")
    assert fresh_forecaster.get_forecast(swarm_id).total_turns == 1

    fresh_forecaster.reset(swarm_id)
    report = fresh_forecaster.get_forecast(swarm_id)
    assert report.total_turns == 0
    assert report.risk_level == ForecastRiskLevel.NOMINAL


def test_thread_safety_concurrent_observations(fresh_forecaster):
    swarm_id = "swarm-concurrent"
    num_threads = 10
    turns_per_thread = 20

    def worker(tid):
        for i in range(turns_per_thread):
            turn_idx = tid * turns_per_thread + i + 1
            fresh_forecaster.observe_turn(
                swarm_id=swarm_id,
                turn_index=turn_idx,
                entropy=1.5 - (i * 0.02),
                agent=f"worker-{tid}",
            )

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    report = fresh_forecaster.get_forecast(swarm_id)
    assert report.total_turns == num_threads * turns_per_thread


def test_rest_forecast_endpoints():
    storage = SQLiteStorageAdapter(":memory:")
    app = create_app(storage=storage)
    client = TestClient(app)

    swarm_id = "swarm-rest-test"

    # 1. Reset
    resp = client.post(f"/api/v1/swarms/{swarm_id}/forecast/reset")
    assert resp.status_code == 200
    assert resp.json()["status"] == "forecast_reset"

    # 2. Get initial forecast
    resp = client.get(f"/api/v1/swarms/{swarm_id}/forecast")
    assert resp.status_code == 200
    data = resp.json()
    assert data["swarm_id"] == swarm_id
    assert data["risk_level"] == "nominal"

    # 3. Evaluate turns
    eval_resp = client.post(
        f"/api/v1/swarms/{swarm_id}/forecast/evaluate",
        json={"turn_index": 1, "entropy": 2.1, "agent": "Planner"},
    )
    assert eval_resp.status_code == 200
    assert eval_resp.json()["status"] == "evaluated"
    assert eval_resp.json()["report"]["total_turns"] == 1


def test_cli_forecast_subcommand(capsys):
    global_forecaster.reset("swarm-cli-test")

    # Status action
    with pytest.raises(SystemExit) as exc:
        main(["forecast", "status", "--swarm-id", "swarm-cli-test", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "swarm-cli-test" in captured.out

    # Predict action
    with pytest.raises(SystemExit) as exc:
        main([
            "forecast", "predict",
            "--swarm-id", "swarm-cli-test",
            "--turn", "1",
            "--entropy", "0.25",
            "--format", "json",
        ])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "risk_probability" in captured.out

    # Reset action
    with pytest.raises(SystemExit) as exc:
        main(["forecast", "reset", "--swarm-id", "swarm-cli-test", "--format", "json"])
    assert exc.value.code == 0
