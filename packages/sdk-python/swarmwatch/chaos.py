# -*- coding: utf-8 -*-
"""
SwarmWatch Interactive Chaos & Loop Simulator.
Provides deterministic, zero-token synthetic multi-agent failure scenarios:
1. Lexical and Shannon entropy collapse (repetitive refactoring loops).
2. Infinite tool recursion loops (repeated execution failures).
3. Session token and financial budget quota exhaustion.
4. Conversational ping-pong deadlock (polite pleasantry loops).
"""

import asyncio
from datetime import datetime, timezone
from enum import Enum
import time
from typing import Any, Callable, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from swarmwatch.schema import SwarmSpan, CounterfactualSavings
from swarmwatch.circuit import (
    EntropyCircuitBreaker,
    CircuitPolicy,
    CircuitPreset,
    CircuitDecision,
    SwarmCircuitBreakException,
)
from swarmwatch.storage import StorageAdapter
from swarmwatch.serializer import TimeMachine
from swarmwatch.metrics import (
    record_span_ingested,
    record_circuit_trip,
)


class ChaosScenario(str, Enum):
    ENTROPY_COLLAPSE = "entropy_collapse"
    TOOL_RECURSION = "infinite_tool_recursion"
    BUDGET_QUOTA_EXHAUSTION = "budget_quota_exhaustion"
    CONVERSATIONAL_DEADLOCK = "conversational_deadlock"


class ChaosStep(BaseModel):
    id: str
    source: str
    target: str
    span_type: str = "agent_call"
    duration: float = 420.0
    tokens: int = 1200
    entropy: float = 1.85
    content: str
    state_key: str
    state_val: Any
    tool_name: Optional[str] = None


class ChaosScenarioConfig(BaseModel):
    name: str
    scenario_id: ChaosScenario
    description: str
    expected_failure_mode: str
    policy: CircuitPolicy
    steps: List[ChaosStep]


class ChaosInjectionResult(BaseModel):
    status: str
    scenario: str
    swarm_id: str
    branch_id: str
    spans_count: int
    circuit_breaker: Dict[str, Any]
    spans: List[Dict[str, Any]]


class ChaosSimulator:
    """
    Deep module orchestrating deterministic chaos injection into SwarmWatch.
    Hides scenario definitions, token calculations, and circuit evaluation behind
    a single asynchronous execution method.
    """

    @classmethod
    def get_available_scenarios(cls) -> List[Dict[str, str]]:
        """Returns catalog of available chaos scenarios for UI and API clients."""
        return [
            {
                "id": ChaosScenario.ENTROPY_COLLAPSE.value,
                "name": "Entropy Collapse",
                "description": "Repetitive code refactoring loop collapsing lexical Shannon entropy below safety floor.",
                "failure_mode": "Shannon entropy collapse (H_norm <= 0.35)",
            },
            {
                "id": ChaosScenario.TOOL_RECURSION.value,
                "name": "Infinite Tool Recursion",
                "description": "Agent repeatedly calling failing database migration tool with identical arguments.",
                "failure_mode": "High lexical similarity & tool recursion ceiling (>= 0.80)",
            },
            {
                "id": ChaosScenario.BUDGET_QUOTA_EXHAUSTION.value,
                "name": "Budget Quota Exhaustion",
                "description": "Document analysis agent exceeding max allowed token consumption & dollar budget.",
                "failure_mode": "Session token quota breached (> 3,500 tokens / > $0.07)",
            },
            {
                "id": ChaosScenario.CONVERSATIONAL_DEADLOCK.value,
                "name": "Conversational Deadlock",
                "description": "AutoGen-style polite pleasantry ping-pong loop between autonomous agents.",
                "failure_mode": "Conversational similarity loop (Jaccard >= 0.82)",
            },
        ]

    @classmethod
    def get_scenario_config(cls, scenario: Union[str, ChaosScenario]) -> ChaosScenarioConfig:
        """Constructs canonical scenario configuration and step definitions."""
        s_val = scenario.value if isinstance(scenario, ChaosScenario) else str(scenario).lower()

        if s_val == ChaosScenario.ENTROPY_COLLAPSE.value:
            return ChaosScenarioConfig(
                name="Entropy Collapse",
                scenario_id=ChaosScenario.ENTROPY_COLLAPSE,
                description="Simulated code-refactoring loop that collapses lexical diversity.",
                expected_failure_mode="Shannon entropy <= 0.35 bits",
                policy=CircuitPolicy(
                    similarity_threshold=0.85,
                    entropy_threshold=0.35,
                    window_size=3,
                    max_warnings=1,
                    max_turns=20,
                    cost_per_token=0.00002,
                ),
                steps=[
                    ChaosStep(
                        id="span_ec_01",
                        source="ProductArchitect",
                        target="BackendEngineer",
                        duration=410.0,
                        tokens=1450,
                        entropy=1.92,
                        content="Spec approved: Redis token bucket throughput throttler middleware with per-client IP windowing.",
                        state_key="spec_approved",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_ec_02",
                        source="BackendEngineer",
                        target="CodeReviewer",
                        duration=580.0,
                        tokens=2600,
                        entropy=1.85,
                        content="PR #188 submitted: Added TokenBucketLimiter class with redis.pipeline() and atomic counter increment.",
                        state_key="pr_ready",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_ec_03",
                        source="CodeReviewer",
                        target="SecurityAuditor",
                        duration=430.0,
                        tokens=1720,
                        entropy=1.74,
                        content="Review passed standards. Type signatures verified. AST audit requested for unhandled key TTL expiry.",
                        state_key="review_passed",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_ec_04",
                        source="SecurityAuditor",
                        target="BackendEngineer",
                        duration=380.0,
                        tokens=1340,
                        entropy=1.65,
                        content="Flagged potential vulnerability in limiter.py: fallback default TTL is unconfigured if redis returns -1.",
                        state_key="security_flagged",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_ec_05",
                        source="BackendEngineer",
                        target="SecurityAuditor",
                        duration=510.0,
                        tokens=1900,
                        entropy=0.23,
                        content="retry retry retry retry retry retry retry retry retry retry connection timeout",
                        state_key="retry_count",
                        state_val=1,
                    ),
                    ChaosStep(
                        id="span_ec_06",
                        source="BackendEngineer",
                        target="SecurityAuditor",
                        duration=510.0,
                        tokens=1900,
                        entropy=0.28,
                        content="reconnect reconnect reconnect reconnect reconnect reconnect reconnect reconnect socket failure",
                        state_key="retry_count",
                        state_val=2,
                    ),
                ],
            )

        elif s_val == ChaosScenario.TOOL_RECURSION.value:
            return ChaosScenarioConfig(
                name="Infinite Tool Recursion",
                scenario_id=ChaosScenario.TOOL_RECURSION,
                description="Agent repeatedly calling failing database migration tool with identical inputs.",
                expected_failure_mode="Tool repetition similarity ceiling >= 0.80",
                policy=CircuitPolicy(
                    similarity_threshold=0.80,
                    entropy_threshold=0.30,
                    window_size=3,
                    max_warnings=1,
                    max_turns=20,
                    cost_per_token=0.00002,
                ),
                steps=[
                    ChaosStep(
                        id="span_tr_01",
                        source="SystemAdmin",
                        target="BackendEngineer",
                        duration=320.0,
                        tokens=600,
                        entropy=1.88,
                        content="Please run schema migration for users table: add phone_number column with index.",
                        state_key="migration_requested",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_tr_02",
                        source="BackendEngineer",
                        target="PythonCodeRunner",
                        span_type="tool_call",
                        tool_name="PythonCodeRunner",
                        duration=480.0,
                        tokens=850,
                        entropy=1.62,
                        content='db.execute("ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);")',
                        state_key="tool_exec_attempt",
                        state_val=1,
                    ),
                    ChaosStep(
                        id="span_tr_03",
                        source="PythonCodeRunner",
                        target="BackendEngineer",
                        span_type="tool_result",
                        tool_name="PythonCodeRunner",
                        duration=220.0,
                        tokens=400,
                        entropy=1.58,
                        content="OperationalError: database table users is locked by concurrent transaction 0x4f12; timed out after 5000ms.",
                        state_key="tool_error",
                        state_val="table_locked",
                    ),
                    ChaosStep(
                        id="span_tr_04",
                        source="BackendEngineer",
                        target="PythonCodeRunner",
                        span_type="tool_call",
                        tool_name="PythonCodeRunner",
                        duration=490.0,
                        tokens=850,
                        entropy=1.62,
                        content='db.execute("ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);")',
                        state_key="tool_exec_attempt",
                        state_val=2,
                    ),
                    ChaosStep(
                        id="span_tr_05",
                        source="BackendEngineer",
                        target="PythonCodeRunner",
                        span_type="tool_call",
                        tool_name="PythonCodeRunner",
                        duration=490.0,
                        tokens=850,
                        entropy=1.62,
                        content='db.execute("ALTER TABLE users ADD COLUMN phone_number VARCHAR(20);")',
                        state_key="tool_exec_attempt",
                        state_val=3,
                    ),
                ],
            )

        elif s_val == ChaosScenario.BUDGET_QUOTA_EXHAUSTION.value:
            return ChaosScenarioConfig(
                name="Budget Quota Exhaustion",
                scenario_id=ChaosScenario.BUDGET_QUOTA_EXHAUSTION,
                description="Rapid token burn exceeding session budget limits.",
                expected_failure_mode="Token budget quota exceeded (> 3,500 tokens)",
                policy=CircuitPolicy(
                    similarity_threshold=0.90,
                    entropy_threshold=0.20,
                    window_size=3,
                    max_warnings=1,
                    token_budget=3500,
                    cost_budget=0.07,
                    max_turns=10,
                    cost_per_token=0.00002,
                ),
                steps=[
                    ChaosStep(
                        id="span_bq_01",
                        source="ComplianceAuditor",
                        target="ResearchAgent",
                        duration=650.0,
                        tokens=1800,
                        entropy=1.85,
                        content="Ingest 50-page enterprise compliance PDF and summarize SOC2 Trust Services Criteria for access control.",
                        state_key="ingest_complete",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_bq_02",
                        source="ResearchAgent",
                        target="PolicyGenerator",
                        duration=820.0,
                        tokens=2400,
                        entropy=1.78,
                        content="Extracted 42 compliance rules. Synthesizing full Terraform policy modules for all AWS and GCP IAM roles.",
                        state_key="policy_generation",
                        state_val="active",
                    ),
                ],
            )

        elif s_val == ChaosScenario.CONVERSATIONAL_DEADLOCK.value:
            return ChaosScenarioConfig(
                name="Conversational Deadlock",
                scenario_id=ChaosScenario.CONVERSATIONAL_DEADLOCK,
                description="Polite ping-pong pleasantry loop between autonomous agents.",
                expected_failure_mode="High conversational similarity (Jaccard >= 0.82)",
                policy=CircuitPolicy(
                    similarity_threshold=0.82,
                    entropy_threshold=0.30,
                    window_size=3,
                    max_warnings=1,
                    max_turns=20,
                    cost_per_token=0.00002,
                ),
                steps=[
                    ChaosStep(
                        id="span_cd_01",
                        source="SystemArchitect",
                        target="LeadDeveloper",
                        duration=340.0,
                        tokens=700,
                        entropy=1.82,
                        content="Architecture verification complete. All microservices refactored and operational.",
                        state_key="system_verified",
                        state_val=True,
                    ),
                    ChaosStep(
                        id="span_cd_02",
                        source="LeadDeveloper",
                        target="SystemArchitect",
                        duration=380.0,
                        tokens=820,
                        entropy=1.72,
                        content="Thank you for the update. Let me know if you need anything else from our side.",
                        state_key="deadlock_turn",
                        state_val=1,
                    ),
                    ChaosStep(
                        id="span_cd_03",
                        source="SystemArchitect",
                        target="LeadDeveloper",
                        duration=390.0,
                        tokens=830,
                        entropy=1.72,
                        content="Thank you for the update. Let me know if you need anything else from our side.",
                        state_key="deadlock_turn",
                        state_val=2,
                    ),
                    ChaosStep(
                        id="span_cd_04",
                        source="LeadDeveloper",
                        target="SystemArchitect",
                        duration=390.0,
                        tokens=830,
                        entropy=1.72,
                        content="Thank you for the update. Let me know if you need anything else from our side.",
                        state_key="deadlock_turn",
                        state_val=3,
                    ),
                ],
            )

        else:
            raise ValueError(f"Unknown chaos scenario '{scenario}'. Available: {[s.value for s in ChaosScenario]}")

    @classmethod
    async def run_scenario(
        cls,
        scenario: Union[str, ChaosScenario],
        storage: StorageAdapter,
        time_machine: TimeMachine,
        swarm_id: str = "swarm-live-01",
        branch_id: str = "main",
        project_id: str = "default-project",
        step_delay: Optional[float] = None,
        broadcast_callback: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
    ) -> ChaosInjectionResult:
        """
        Executes a chaos scenario end-to-end:
        1. Emits spans & snapshots into storage.
        2. Evaluates turns against EntropyCircuitBreaker.
        3. Fires WebSocket messages via broadcast_callback if provided.
        4. Halts when the circuit breaker trips and returns the complete execution report.
        """
        config = cls.get_scenario_config(scenario)
        breaker = EntropyCircuitBreaker(policy=config.policy)

        emitted_spans: List[SwarmSpan] = []
        accumulated_state: Dict[str, Any] = {
            "swarm_id": swarm_id,
            "branch_id": branch_id,
            "chaos_scenario": config.scenario_id.value,
        }

        circuit_tripped = False
        trip_reason = ""
        tripped_span_id: Optional[str] = None
        tokens_saved = 0
        dollars_saved = 0.0
        entropy_score = 1.0

        delay = 0.0 if "test" in swarm_id else (0.25 if step_delay is None else step_delay)
        now_iso = datetime.now(timezone.utc).isoformat()

        for step in config.steps:
            span_id = step.id
            accumulated_state[step.state_key] = step.state_val

            # 1. Evaluate Circuit Breaker
            try:
                decision = breaker.evaluate(
                    message=step.content,
                    agent_id=step.source,
                    swarm_id=swarm_id,
                    token_count=step.tokens,
                )
            except SwarmCircuitBreakException as exc:
                circuit_tripped = True
                trip_reason = exc.reason
                tripped_span_id = span_id
                entropy_score = exc.metric_val if "entropy" in trip_reason.lower() else step.entropy
                if exc.savings:
                    tokens_saved = exc.savings.tokens_saved or int(exc.savings.budget_protected / config.policy.cost_per_token) if exc.savings.budget_protected else 241500
                    dollars_saved = exc.savings.dollars_saved or exc.savings.budget_protected or 4.83
                else:
                    tokens_saved = 241500
                    dollars_saved = 4.83

            # 2. Construct SwarmSpan
            span = SwarmSpan(
                id=span_id,
                swarm_id=swarm_id,
                branch_id=branch_id,
                project_id=project_id,
                source_agent=step.source,
                target_agent=step.target,
                span_type=step.span_type,
                input_payload={"content": step.content, "tool": step.tool_name} if step.tool_name else {"content": step.content},
                output_payload={"status": "halted" if circuit_tripped else "dispatched"},
                token_count=step.tokens,
                entropy_score=step.entropy,
                start_time=now_iso,
                end_time=now_iso,
                duration_ms=step.duration,
            )
            emitted_spans.append(span)

            # 3. Persist to storage & snapshot
            await storage.save_spans([span])
            await time_machine.snapshot(
                span_id=span_id,
                state_data=dict(accumulated_state),
                branch_id=branch_id,
            )

            record_span_ingested(
                project_id=project_id,
                agent_type=step.source,
                status="ok",
                duration_seconds=step.duration / 1000.0,
            )

            # 4. Broadcast span update
            if broadcast_callback:
                msg = {
                    "type": "span_update",
                    "span": span.model_dump(mode="json"),
                }
                res = broadcast_callback(swarm_id, msg)
                if asyncio.iscoroutine(res):
                    await res

            if delay > 0:
                await asyncio.sleep(delay)

            # If tripped during this step, stop further execution
            if circuit_tripped:
                break

        # 5. Broadcast circuit breaker trip if triggered
        if circuit_tripped:
            record_circuit_trip(
                project_id=project_id,
                reason="chaos_trip_" + config.scenario_id.value,
                tokens_saved=tokens_saved,
            )
            if broadcast_callback:
                trip_msg = {
                    "type": "circuit_breaker_tripped",
                    "scenario": config.scenario_id.value,
                    "reason": trip_reason,
                    "tokens_saved": tokens_saved,
                    "dollars_saved": dollars_saved,
                    "entropy_score": entropy_score,
                    "tripped_span_id": tripped_span_id,
                }
                res = broadcast_callback(swarm_id, trip_msg)
                if asyncio.iscoroutine(res):
                    await res

        return ChaosInjectionResult(
            status="simulation_halted" if circuit_tripped else "simulation_completed",
            scenario=config.scenario_id.value,
            swarm_id=swarm_id,
            branch_id=branch_id,
            spans_count=len(emitted_spans),
            circuit_breaker={
                "tripped": circuit_tripped,
                "reason": trip_reason,
                "tokens_saved": tokens_saved,
                "dollars_saved": dollars_saved,
                "entropy_score": entropy_score,
                "tripped_span_id": tripped_span_id,
            },
            spans=[s.model_dump(mode="json") for s in emitted_spans],
        )
