# -*- coding: utf-8 -*-
"""
TDD Tests for Session Budget Quota & Dynamic Counterfactual Savings Engine.
Validates ADR 0006 calculation models:
- Loop ceiling averted turns: Delta_turns = N_max - N_halt
- Counterfactual tokens: Delta_turns * T_bar
- Dynamic dollar savings: Tokens_saved * CostPerToken
- Deterministic session budget caps (token and cost ceilings).
"""
import pytest

from swarmwatch.circuit import (
    CircuitPolicy,
    CircuitPreset,
    EntropyCircuitBreaker,
    SwarmCircuitBreakException,
)
from swarmwatch.schema import CounterfactualSavings


def test_counterfactual_savings_model_defaults():
    """Verify CounterfactualSavings schema structure."""
    savings = CounterfactualSavings(
        tokens_saved=12000,
        dollars_saved=0.24,
        turns_averted=4,
        budget_consumed=0.10,
        budget_protected=0.90,
    )
    assert savings.tokens_saved == 12000
    assert savings.dollars_saved == 0.24
    assert savings.turns_averted == 4
    assert savings.budget_consumed == 0.10
    assert savings.budget_protected == 0.90


def test_circuit_breaker_computes_counterfactual_savings_on_halt():
    """
    Verify that when an agent triggers a hard halt on repetitive babble,
    the circuit breaker calculates counterfactual savings based on configured max_turns.
    """
    policy = CircuitPolicy(
        similarity_threshold=0.80,
        entropy_threshold=0.40,
        window_size=3,
        max_warnings=1,
        max_turns=20,
        cost_per_token=0.00002,
    )
    breaker = EntropyCircuitBreaker(policy=policy)

    # Turn 1: normal message, 500 tokens
    decision1 = breaker.evaluate("Hello team, let us analyze the architecture.", token_count=500)
    assert decision1.decision == "pass"

    # Turn 2: normal message, 600 tokens
    decision2 = breaker.evaluate("The architecture consists of distributed microservices.", token_count=600)
    assert decision2.decision == "pass"

    # Turn 3: warning repetition, 550 tokens
    decision3 = breaker.evaluate("Loop message: retry connecting to database now.", token_count=550)
    assert decision3.decision == "pass"

    # Turn 4: warning trigger (same message)
    decision4 = breaker.evaluate("Loop message: retry connecting to database now.", token_count=550)
    assert decision4.decision == "warning"
    assert decision4.counterfactual_savings is not None
    assert decision4.counterfactual_savings.turns_averted > 0

    # Turn 5: hard halt (consecutive warning exceeded)
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        breaker.evaluate("Loop message: retry connecting to database now.", token_count=550)

    exc = exc_info.value
    assert exc.savings is not None
    # 5 turns completed out of 20 -> 15 turns averted
    assert exc.savings.turns_averted == 15
    # Average tokens per turn ~ 550 -> tokens saved = 15 * 550 = 8250
    assert exc.savings.tokens_saved > 0
    assert exc.savings.dollars_saved > 0.0
    assert exc.savings.dollars_saved == pytest.approx(exc.savings.tokens_saved * 0.00002, rel=1e-3)


def test_circuit_breaker_enforces_token_budget_quota():
    """Verify that exceeding session token budget halts swarm immediately."""
    policy = CircuitPolicy(
        max_turns=50,
        token_budget=1000,
        cost_per_token=0.00002,
    )
    breaker = EntropyCircuitBreaker(policy=policy)

    # First turn: 600 tokens (within 1000 budget)
    d1 = breaker.evaluate("Message 1 with fresh information.", token_count=600)
    assert d1.decision == "pass"

    # Second turn: 500 tokens (cumulative 1100 > 1000 budget) -> Must trip!
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        breaker.evaluate("Message 2 with another set of information.", token_count=500)

    exc = exc_info.value
    assert "token budget" in exc.reason.lower()
    assert exc.savings is not None
    assert exc.savings.budget_consumed == pytest.approx(1100 * 0.00002, rel=1e-3)


def test_circuit_breaker_enforces_cost_budget_quota():
    """Verify that exceeding session cost budget halts swarm immediately."""
    policy = CircuitPolicy(
        max_turns=50,
        cost_budget=0.05,  # $0.05 budget
        cost_per_token=0.0001,  # $0.0001 per token -> 500 tokens max
    )
    breaker = EntropyCircuitBreaker(policy=policy)

    # Turn 1: 300 tokens ($0.03 consumed)
    d1 = breaker.evaluate("Working on initial discovery step.", token_count=300)
    assert d1.decision == "pass"

    # Turn 2: 300 tokens ($0.06 cumulative > $0.05) -> Must trip!
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        breaker.evaluate("Working on second discovery step.", token_count=300)

    exc = exc_info.value
    assert "cost budget" in exc.reason.lower()


def test_circuit_breaker_enforces_max_turns_ceiling():
    """Verify that exceeding configured max_turns halts swarm cleanly."""
    policy = CircuitPolicy(max_turns=3)
    breaker = EntropyCircuitBreaker(policy=policy)

    d1 = breaker.evaluate("Turn 1 data")
    assert d1.decision == "pass"
    d2 = breaker.evaluate("Turn 2 data with more context")
    assert d2.decision == "pass"
    d3 = breaker.evaluate("Turn 3 data with even more context")
    assert d3.decision == "pass"

    # Turn 4 > max_turns=3 -> Must trip!
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        breaker.evaluate("Turn 4 beyond maximum allowed iterations")

    exc = exc_info.value
    assert "maximum turns" in exc.reason.lower() or "max turns" in exc.reason.lower()
