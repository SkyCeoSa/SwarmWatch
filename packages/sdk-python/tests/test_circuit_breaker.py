"""
TDD Tests for Ticket 04: Zero-Token Shannon Entropy Circuit Breaker
"""
import pytest
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPolicy,
    CircuitPreset,
    SwarmCircuitBreakException,
    CircuitDecision,
    compute_shannon_entropy,
    compute_lexical_similarity,
)


def test_shannon_entropy_calculation():
    # Repetitive text should have very low normalized entropy
    repetitive_text = "error retry error retry error retry error retry error retry"
    entropy_rep = compute_shannon_entropy(repetitive_text)

    # Diverse text should have high entropy
    diverse_text = "The quick brown fox jumps over the lazy dog and discovers a hidden treasure in the woods."
    entropy_div = compute_shannon_entropy(diverse_text)

    assert 0.0 <= entropy_rep <= 1.0
    assert 0.0 <= entropy_div <= 1.0
    assert entropy_rep < entropy_div, f"Repetitive entropy {entropy_rep} should be lower than diverse {entropy_div}"


def test_lexical_similarity():
    text_a = "Please provide the database connection string."
    text_b = "Please provide the database connection string."
    text_c = "Deploy the application to Kubernetes cluster."

    sim_identical = compute_lexical_similarity(text_a, text_b)
    sim_different = compute_lexical_similarity(text_a, text_c)

    assert sim_identical == 1.0
    assert sim_different < 0.3


def test_circuit_breaker_sub_millisecond_eval():
    import time
    breaker = EntropyCircuitBreaker(preset=CircuitPreset.BALANCED)

    start = time.perf_counter()
    for i in range(100):
        breaker.evaluate(f"Agent step {i} response discussing financial data analysis with unique token {i*7}.")
    elapsed = (time.perf_counter() - start) / 100

    # Ensure evaluation executes in < 1 millisecond (< 1000 microseconds)
    assert elapsed < 0.001, f"Average execution took {elapsed*1000}ms, expected < 1ms"


def test_two_step_intervention_warning_then_trip():
    policy = CircuitPolicy(
        similarity_threshold=0.8,
        entropy_threshold=0.3,
        window_size=3,
        max_warnings=1,
    )
    breaker = EntropyCircuitBreaker(policy=policy)

    repetitive_msg = "Cannot connect to database host 127.0.0.1. Retrying..."

    # Turn 1: Initial message - PASS
    decision_1 = breaker.evaluate(repetitive_msg)
    assert decision_1.decision == "pass"

    # Turn 2: Exact repeat - Triggers Warning (Step 1)
    decision_2 = breaker.evaluate(repetitive_msg)
    assert decision_2.decision == "warning"
    assert "[SWARMWATCH CIRCUIT WARNING]" in decision_2.warning_hint

    # Turn 3: Second consecutive repeat - Triggers Trip (Step 2: Halts execution)
    with pytest.raises(SwarmCircuitBreakException) as exc_info:
        breaker.evaluate(repetitive_msg)

    assert exc_info.value.metric_val >= 0.8
    assert "consecutive warning limit exceeded" in str(exc_info.value).lower()


def test_recovery_resets_warning():
    policy = CircuitPolicy(
        similarity_threshold=0.8,
        window_size=3,
        max_warnings=1,
    )
    breaker = EntropyCircuitBreaker(policy=policy)

    # Trigger warning
    breaker.evaluate("Attempting step A")
    decision_warn = breaker.evaluate("Attempting step A")
    assert decision_warn.decision == "warning"

    # Break the loop with diverse content - should reset warning streak
    decision_recovered = breaker.evaluate("Completely different plan: switching to alternative backup endpoint in AWS.")
    assert decision_recovered.decision == "pass"
    assert breaker.consecutive_warnings == 0
