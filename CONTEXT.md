# SwarmWatch Architecture & Domain Glossary (CONTEXT.md)

## Core Mission
SwarmWatch is a universal, framework-agnostic Observability, Interactive Visual Graph, Time-Machine Debugging, and Entropy-Based Circuit-Breaker platform for Multi-Agent AI Systems.

## Domain Glossary
- **Swarm / Multi-Agent System (MAS)**: A collection of autonomous AI agents collaborating on complex workflows.
- **Node / Actor**: A single agent instance (e.g., Planner, Researcher, Coder, Critic) with a distinct role, model, and system prompt.
- **Trace & Span**: Standard OpenTelemetry-compatible units of execution. A Span represents a single LLM generation, tool-call, or agent-to-agent message.
- **Decision Graph**: The directed acyclic or cyclic graph (DAG/DCG) formed by agent interactions, tool calls, and state transitions.
- **Entropy Circuit Breaker**: An algorithmic evaluator that computes semantic similarity / token entropy across consecutive agent utterances to instantly terminate infinite loops before token budgets are exhausted.
- **Time-Machine State**: A snapshot of agent memory, variables, and context at step N, allowing pause, human-in-the-loop intervention, state mutation, and non-destructive replay.

## High-Level Architecture
1. **Collector SDK (`packages/sdk-python`)**: Zero-config OpenTelemetry and MCP hooks for LangGraph, CrewAI, AutoGen, and raw OpenAI/Anthropic clients.
2. **Ingestion Engine (`packages/server-ingestion`)**: High-throughput FastAPI collector with async batch buffer storing spans in ClickHouse (and embedded SQLite for local zero-dependency development).
3. **Command & Visual Dashboard (`packages/dashboard`)**: Interactive WebGL / React Flow graph showing real-time token burn, message flows, and loop alerts.
