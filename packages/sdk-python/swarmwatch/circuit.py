# -*- coding: utf-8 -*-
"""
SwarmWatch Zero-Token Shannon Entropy Circuit Breaker.
Provides fast mathematical loop detection, sliding-window lexical similarity,
and a 2-step intervention engine (prompt warning hint -> SwarmCircuitBreakException).
"""

from collections import Counter, deque
from enum import Enum
import math
import re
from typing import Deque, List, Optional, Union
from pydantic import BaseModel, Field


def compute_shannon_entropy(text: str) -> float:
    """
    Computes normalized Shannon entropy H_norm in range [0.0, 1.0] for input text tokens.
    H = - sum(p * log2(p))
    Normalized by log2(N) where N is total tokens.
    Low entropy indicates repetitive patterns or loop stagnation.
    """
    tokens = re.findall(r"\b\w+\b", text.lower())
    n = len(tokens)
    if n <= 1:
        return 1.0

    counts = Counter(tokens)
    entropy = 0.0
    for count in counts.values():
        p = count / n
        entropy -= p * math.log2(p)

    max_possible_entropy = math.log2(n)
    if max_possible_entropy <= 0.0:
        return 1.0

    normalized = entropy / max_possible_entropy
    return max(0.0, min(1.0, normalized))


def compute_lexical_similarity(text_a: str, text_b: str) -> float:
    """
    Computes token-level Jaccard similarity in range [0.0, 1.0].
    Zero LLM calls, zero network latency.
    """
    tokens_a = set(re.findall(r"\b\w+\b", text_a.lower()))
    tokens_b = set(re.findall(r"\b\w+\b", text_b.lower()))

    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0

    intersection = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    return intersection / union if union > 0 else 0.0


class CircuitPreset(str, Enum):
    STRICT = "strict"
    BALANCED = "balanced"
    LENIENT = "lenient"


class CircuitPolicy(BaseModel):
    similarity_threshold: float = Field(default=0.85, description="Threshold above which messages are considered looping")
    entropy_threshold: float = Field(default=0.40, description="Entropy floor below which output is flagged as low variety")
    window_size: int = Field(default=3, description="Number of historical turns to compare against")
    max_warnings: int = Field(default=1, description="Number of warning hints before hard halting")

    @classmethod
    def from_preset(cls, preset: Union[str, CircuitPreset]) -> "CircuitPolicy":
        val = preset.value if isinstance(preset, CircuitPreset) else str(preset).lower()
        if val == "strict":
            return cls(similarity_threshold=0.70, entropy_threshold=0.55, max_warnings=1, window_size=3)
        elif val == "lenient":
            return cls(similarity_threshold=0.95, entropy_threshold=0.25, max_warnings=2, window_size=4)
        else:  # balanced
            return cls(similarity_threshold=0.85, entropy_threshold=0.40, max_warnings=1, window_size=3)


class CircuitDecision(BaseModel):
    decision: str = Field(description="'pass' | 'warning' | 'trip'")
    reason: Optional[str] = None
    metric_val: float = 0.0
    warning_hint: Optional[str] = None


class SwarmCircuitBreakException(Exception):
    """
    Raised when an agent swarm becomes trapped in an infinite loop or low-entropy deadlock,
    surpassing the warning threshold.
    """

    def __init__(
        self,
        reason: str,
        metric_val: float,
        agent_id: str = "agent",
        swarm_id: str = "swarm",
    ):
        self.reason = reason
        self.metric_val = metric_val
        self.agent_id = agent_id
        self.swarm_id = swarm_id
        super().__init__(
            f"[SWARMWATCH CIRCUIT TRIP] Swarm '{swarm_id}' agent '{agent_id}' halted: "
            f"{reason} (metric={metric_val:.4f}). Consecutive warning limit exceeded."
        )


class EntropyCircuitBreaker:
    """
    In-process, zero-token mathematical circuit breaker.
    Monitors agent outputs across a sliding window and enforces 2-step intervention.
    """

    def __init__(
        self,
        policy: Optional[CircuitPolicy] = None,
        preset: Optional[CircuitPreset] = None,
    ):
        if policy is not None:
            self.policy = policy
        elif preset is not None:
            self.policy = CircuitPolicy.from_preset(preset)
        else:
            self.policy = CircuitPolicy.from_preset(CircuitPreset.BALANCED)

        self._history: Deque[str] = deque(maxlen=self.policy.window_size)
        self.consecutive_warnings = 0

    def evaluate(
        self,
        message: str,
        agent_id: str = "agent",
        swarm_id: str = "swarm",
    ) -> CircuitDecision:
        """
        Evaluates message against historical window in < 1ms.
        Returns CircuitDecision ('pass' or 'warning') or raises SwarmCircuitBreakException.
        """
        if not message or not message.strip():
            return CircuitDecision(decision="pass")

        # 1. Compute entropy and sliding-window similarity
        entropy = compute_shannon_entropy(message)
        max_similarity = 0.0
        for prior in self._history:
            sim = compute_lexical_similarity(message, prior)
            if sim > max_similarity:
                max_similarity = sim

        # 2. Check violation conditions
        violation = False
        reason = ""
        metric_val = 0.0

        if max_similarity >= self.policy.similarity_threshold and len(self._history) > 0:
            violation = True
            reason = f"High lexical repetition detected ({max_similarity:.2f} >= {self.policy.similarity_threshold:.2f})"
            metric_val = max_similarity
        elif entropy <= self.policy.entropy_threshold and len(message.split()) >= 5:
            violation = True
            reason = f"Low Shannon entropy detected ({entropy:.2f} <= {self.policy.entropy_threshold:.2f})"
            metric_val = entropy

        if violation:
            if self.consecutive_warnings < self.policy.max_warnings:
                self.consecutive_warnings += 1
                hint = (
                    f"[SWARMWATCH CIRCUIT WARNING] Potential repetitive loop detected: {reason}. "
                    "Please diversify your approach, try an alternative tool, or summarize current findings."
                )
                self._history.append(message)
                return CircuitDecision(
                    decision="warning",
                    reason=reason,
                    metric_val=metric_val,
                    warning_hint=hint,
                )
            else:
                # Consecutive warning limit reached -> Trip circuit breaker!
                raise SwarmCircuitBreakException(
                    reason=reason,
                    metric_val=metric_val,
                    agent_id=agent_id,
                    swarm_id=swarm_id,
                )
        else:
            # Message is healthy; reset warning streak
            self.consecutive_warnings = 0
            self._history.append(message)
            return CircuitDecision(
                decision="pass",
                metric_val=max_similarity if max_similarity > 0 else entropy,
            )

    def reset(self) -> None:
        """Resets the circuit breaker state and historical window."""
        self._history.clear()
        self.consecutive_warnings = 0
