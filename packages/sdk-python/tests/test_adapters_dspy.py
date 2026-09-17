# -*- coding: utf-8 -*-
"""
TDD Tests for SwarmWatch Stanford DSPy Adapter (Seam 5 & ADR 0018).
Verifies module forward tracking, teleprompter optimization iterations,
and proactive Shannon entropy circuit breaker halts on degenerate prompt loops.
"""

from unittest.mock import MagicMock
import pytest

import swarmwatch
from swarmwatch.adapters.dspy import SwarmWatchDSPyTracker
from swarmwatch.circuit import SwarmCircuitBreakException, EntropyCircuitBreaker, CircuitPreset


def test_dspy_tracker_module_forward_lifecycle(tmp_path):
    db_path = str(tmp_path / "dspy_lifecycle.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-dspy-lifecycle")

    with SwarmWatchDSPyTracker(swarm_id="swarm-dspy-lifecycle") as tracker:
        tracker.track_forward(
            module_name="GenerateOutline",
            input_kwargs={"topic": "Quantum Computing Architectures"},
            output="1. Qubits and Superposition\n2. Quantum Gates\n3. Error Correction",
        )

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-dspy-lifecycle")
    assert len(spans) == 1
    span = spans[0]
    assert span.span_type == "dspy_module"
    assert span.source_agent == "dspy:GenerateOutline"
    assert span.target_agent == "dspy_program"
    assert span.input_payload["topic"] == "Quantum Computing Architectures"
    assert "Error Correction" in span.output_payload["result"]
    assert span.token_count > 0


def test_dspy_optimization_teleprompter_tracking(tmp_path):
    db_path = str(tmp_path / "dspy_teleprompter.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-dspy-teleprompter")

    tracker = SwarmWatchDSPyTracker(swarm_id="swarm-dspy-teleprompter")

    # Step 1: Candidate iteration
    tracker.track_optimization_step(
        teleprompter="BootstrapFewShot",
        candidate_id="cand_v1",
        prompt="Analyze quantum error mitigation strategies with few-shot demonstrations.",
        metric_score=0.88,
    )

    # Step 2: Improved candidate iteration
    tracker.track_optimization_step(
        teleprompter="BootstrapFewShot",
        candidate_id="cand_v2",
        prompt="Evaluate topological fault-tolerant quantum error thresholds.",
        metric_score=0.94,
    )

    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-dspy-teleprompter")
    assert len(spans) == 2
    for s in spans:
        assert s.span_type == "teleprompter_iteration"
        assert s.source_agent == "dspy:BootstrapFewShot"
    assert spans[0].output_payload["metric_score"] == 0.88
    assert spans[1].output_payload["metric_score"] == 0.94


def test_dspy_circuit_breaker_halts_degenerate_loop(tmp_path):
    db_path = str(tmp_path / "dspy_loop.db")
    swarmwatch.init(db_path=db_path, swarm_id="swarm-dspy-loop")

    circuit = EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
    tracker = SwarmWatchDSPyTracker(
        swarm_id="swarm-dspy-loop",
        circuit_breaker=circuit,
    )

    degenerate_prompt = "Retrying identical candidate prompt variation across iterations with zero lexical diversity."

    # Turn 1: Passes
    tracker.track_optimization_step(
        teleprompter="MIPRO",
        candidate_id="cand_01",
        prompt=degenerate_prompt,
        metric_score=0.45,
    )

    # Turn 2: Repetition issues warning
    tracker.track_optimization_step(
        teleprompter="MIPRO",
        candidate_id="cand_02",
        prompt=degenerate_prompt,
        metric_score=0.45,
    )

    # Turn 3: Consecutive repetition trips circuit breaker
    with pytest.raises(SwarmCircuitBreakException):
        tracker.track_optimization_step(
            teleprompter="MIPRO",
            candidate_id="cand_03",
            prompt=degenerate_prompt,
            metric_score=0.45,
        )


def test_dspy_wrap_callable(tmp_path):
    db_path = str(tmp_path / "dspy_wrap.db")
    client = swarmwatch.init(db_path=db_path, swarm_id="swarm-dspy-wrap")

    tracker = SwarmWatchDSPyTracker(swarm_id="swarm-dspy-wrap")

    def dummy_predict(question: str) -> str:
        return f"Answer for {question}"

    wrapped = tracker.wrap(dummy_predict, module_name="PredictQA")
    result = wrapped(question="What is Shor's algorithm?")

    assert result == "Answer for What is Shor's algorithm?"
    swarmwatch.flush()

    spans = client.storage.query_spans(swarm_id="swarm-dspy-wrap")
    assert len(spans) == 1
    assert spans[0].source_agent == "dspy:PredictQA"
    assert spans[0].input_payload["question"] == "What is Shor's algorithm?"
