# -*- coding: utf-8 -*-
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, model_validator



class SwarmSpan(BaseModel):
    """
    Canonical Multi-Agent Execution Span representing a single interaction,
    LLM reasoning turn, or tool-call in a swarm.
    """
    id: str
    swarm_id: str
    project_id: str = "default-project"
    branch_id: str = "main"
    parent_span_id: Optional[str] = None
    source_agent: str
    target_agent: str
    span_type: str = "agent_call"  # "agent_call" | "tool_call" | "system"
    input_payload: Dict[str, Any] = Field(default_factory=dict)
    output_payload: Dict[str, Any] = Field(default_factory=dict)
    token_count: int = 0
    entropy_score: float = 1.0
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: float = 0.0

    @model_validator(mode="before")
    @classmethod
    def _resolve_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "id" not in data and "span_id" in data:
                data["id"] = data["span_id"]
            if "parent_span_id" not in data and "parent_id" in data:
                data["parent_span_id"] = data["parent_id"]
            if "target_agent" not in data:
                data["target_agent"] = data.get("target", "environment")
        return data

    @property
    def span_id(self) -> str:
        return self.id

    @property
    def parent_id(self) -> Optional[str]:
        return self.parent_span_id


    def to_otel_span(self) -> Dict[str, Any]:
        """
        Maps the multi-agent span to OpenTelemetry GenAI Semantic Conventions.
        Information hiding: complex conversions remain inside the domain model.
        """
        return {
            "name": f"{self.source_agent} -> {self.target_agent}:{self.span_type}",
            "context": {
                "trace_id": self.swarm_id,
                "span_id": self.id,
            },
            "parent_id": self.parent_span_id,
            "start_time_unix_nano": int(self.start_time.timestamp() * 1e9),
            "end_time_unix_nano": int(self.end_time.timestamp() * 1e9),
            "attributes": {
                "gen_ai.system": "swarmwatch",
                "gen_ai.swarmwatch.project_id": self.project_id,
                "gen_ai.swarmwatch.branch_id": self.branch_id,
                "gen_ai.swarmwatch.source_agent": self.source_agent,
                "gen_ai.swarmwatch.target_agent": self.target_agent,
                "gen_ai.swarmwatch.span_type": self.span_type,
                "gen_ai.swarmwatch.entropy_score": self.entropy_score,
                "gen_ai.usage.total_tokens": self.token_count,
            }
        }


class StateSnapshot(BaseModel):
    """
    Safe snapshot of agent memory, variables, and context at a specific span,
    enabling non-destructive Time-Machine branching.
    """
    id: str
    span_id: str
    project_id: str = "default-project"
    branch_id: str = "main"
    state_hash: str
    state_data: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CounterfactualSavings(BaseModel):
    """
    Counterfactual computation of prevented token burn and dollar spend
    derived from early circuit breaker intervention (ADR 0006).
    """
    tokens_saved: int = 0
    dollars_saved: float = 0.0
    turns_averted: int = 0
    budget_consumed: float = 0.0
    budget_protected: float = 0.0


class CircuitEvent(BaseModel):
    """
    Audit event emitted when the Entropy Circuit Breaker trips or issues a warning.
    """
    id: str
    swarm_id: str
    project_id: str = "default-project"
    span_id: Optional[str] = None
    event_type: str = "warning"  # "warning" | "trip"
    reason: str
    metric_val: float
    counterfactual_savings: Optional[CounterfactualSavings] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
