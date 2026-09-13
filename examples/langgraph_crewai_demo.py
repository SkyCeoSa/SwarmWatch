# -*- coding: utf-8 -*-
"""
SwarmWatch Multi-Framework Integration Demo (LangGraph & CrewAI).
Demonstrates zero-code-change telemetry capture, causal DAG tracing,
state snapshotting, and zero-token Shannon entropy circuit breaking
across both LangGraph and CrewAI swarms in an autonomous software engineering context.
"""

import sys
import time
import uuid

import swarmwatch
from swarmwatch.adapters.langgraph import SwarmWatchCallbackHandler
from swarmwatch.adapters.crewai import SwarmWatchCrewListener
from swarmwatch.circuit import EntropyCircuitBreaker, CircuitPreset, SwarmCircuitBreakException

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def run_langgraph_demo():
    print("\n" + "=" * 65)
    print(" 1. LangGraph / LangChain Swarm Integration")
    print("=" * 65)

    handler = SwarmWatchCallbackHandler()
    run_id = uuid.uuid4()

    print("[LangGraph] Starting chain execution with SwarmWatchCallbackHandler...")
    handler.on_chain_start({"name": "AutonomousCodeRefactorer"}, {"query": "Diagnose database connection leak"}, run_id=run_id)

    # Simulate LLM Generation Step
    llm_run_id = uuid.uuid4()
    handler.on_llm_start({"name": "Claude-3.5-Sonnet"}, ["Screening async connection pool lifecycle in connection_manager.py..."], run_id=llm_run_id, parent_run_id=run_id)
    time.sleep(0.05)

    class MockLLMResult:
        generations = [[type("Gen", (), {"text": "Identified dangling async session context on unhandled HTTPException."})]]
    handler.on_llm_end(MockLLMResult(), run_id=llm_run_id, parent_run_id=run_id)

    # Simulate Tool Step
    tool_run_id = uuid.uuid4()
    handler.on_tool_start({"name": "AST_Linter_Engine"}, "file: connection_manager.py", run_id=tool_run_id, parent_run_id=run_id)
    time.sleep(0.05)
    handler.on_tool_end("Found missing finally: await session.close() at line 84.", run_id=tool_run_id, parent_run_id=run_id)

    handler.on_chain_end({"patch": "Wrapped session in async with lifespan context.", "status": "RESOLVED"}, run_id=run_id)
    print("[LangGraph] Chain completed and traced successfully into causal DAG.\n")


def run_crewai_demo():
    print("=" * 65)
    print(" 2. CrewAI Multi-Agent Swarm with Circuit-Breaker Protection")
    print("=" * 65)

    listener = SwarmWatchCrewListener(
        circuit_breaker=EntropyCircuitBreaker(preset=CircuitPreset.STRICT)
    )

    class MockStep:
        def __init__(self, agent, thought, tool, result):
            self.agent = agent
            self.thought = thought
            self.tool = tool
            self.result = result

    # Agent Step 1: Valid Work
    print("[CrewAI] SeniorArchitect executing initial migration audit...")
    listener.on_step(MockStep(
        agent="SeniorArchitect",
        thought="Verifying schema migration compatibility with PostgreSQL v16",
        tool="SchemaValidator",
        result="Schema compatible. Foreign key indexes verified."
    ))

    # Agent Step 2: Stalling Loop detection test
    print("[CrewAI] Simulating stalled agent caught in repetitive retry loop...")
    stalled_text = "I need to query the migration table again to verify the lock status."

    try:
        for loop_idx in range(1, 6):
            print(f"  -> Agent loop attempt #{loop_idx}: '{stalled_text[:40]}...'")
            listener.on_step(MockStep(
                agent="SeniorArchitect",
                thought=stalled_text,
                tool="DatabaseQuery",
                result="0 rows returned."
            ))
    except SwarmCircuitBreakException as exc:
        print("\n [CIRCUIT BREAKER TRIGGERED!]")
        print(f"   Reason: {exc.reason}")
        print(f"   Current Metric: {exc.metric_val:.3f}")
        print("   Action: Terminated runaway loop immediately. Zero LLM tokens wasted!")

    # Final Task completion
    listener.on_task(type("TaskOutput", (), {
        "agent": "SeniorArchitect",
        "description": "PostgreSQL 16 migration audit",
        "raw": "Task safely halted and quarantined by SwarmWatch."
    }))


def main():
    print("\n=================================================================")
    print("  👁️ SwarmWatch Multi-Framework Ecosystem Demonstration")
    print("=================================================================")

    swarmwatch.init(swarm_id="multi-framework-dev-demo", db_path="swarmwatch.db")

    run_langgraph_demo()
    run_crewai_demo()

    swarmwatch.flush()
    swarmwatch.shutdown()

    print("\n" + "=" * 65)
    print(">> All traces & state snapshots stored in SQLite WAL ('swarmwatch.db').")
    print(">> Launch the visual dashboard to inspect live swimlanes:")
    print("   $ swarmwatch ui --port 8420")
    print("=================================================================\n")


if __name__ == "__main__":
    main()
