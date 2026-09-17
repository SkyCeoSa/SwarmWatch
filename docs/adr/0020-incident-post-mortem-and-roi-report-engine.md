# ADR 0020: Incident Post-Mortem & Financial ROI Report Generator

## Status
Accepted

## Context
In production deployments of multi-agent LLM systems, autonomous swarms are susceptible to catastrophic failure modes:
1. **Entropy Collapse**: Agents degrade into repetitive token generations, consuming budget without progress.
2. **Conversational Deadlocks & Ping-Pong Loops**: Agent pairs engage in circular politeness loops.
3. **Infinite Tool Recursions**: Agents invoke failing database/API tools with identical arguments indefinitely.
4. **Runaway Quota Exhaustion**: Swarms breach turn or token limits, threatening thousands of dollars in cloud LLM billing.

When SwarmWatch's `EntropyCircuitBreaker` halts an autonomous swarm or when a session completes, engineering leadership and AI platform teams require a standardized, audit-ready forensic report that answers:
- **Financial ROI**: Exactly how many dollars and tokens were saved by cutting off the swarm before reaching its ceiling?
- **Root Cause Analysis (RCA)**: Which specific agent pair triggered the circuit halt, at which turn index, and under what prompt condition?
- **Entropy Trajectory**: How did lexical diversity and information content degrade turn-by-turn?
- **Security Audit**: How many sensitive credentials or PII elements were intercepted and neutralized during the run?

Previously, this information was scattered across raw SQLite/ClickHouse rows or transient WebSocket logs. We require a unified, zero-token, in-process deep module that synthesizes forensic incident reports in Markdown, JSON, and terminal ASCII formats for instant human triage and automated incident response.

## Decision
We establish **Seam 11 (Swarm Incident Post-Mortem & Financial ROI Engine)** and implement `IncidentReportGenerator` in `packages/sdk-python/swarmwatch/report.py`:

1. **Class `IncidentReportGenerator`**:
   - Accepts a `StorageAdapter` (or SQLite/ClickHouse connection) and retrieves all `SwarmSpan`, `StateSnapshot`, and `CircuitEvent` records for a target `swarm_id`.
   - Computes:
     - **Executive Summary**: Status (`HALTED_BY_CIRCUIT_BREAKER` / `COMPLETED_SUCCESSFULLY`), total duration, span count, and turn count.
     - **Financial ROI Metrics**: Counterfactual token savings and compute dollars avoided.
     - **Root Cause Diagnosis**: Identifies the primary agent pair (`source_agent` -> `target_agent`), triggering reason (`ENTROPY_COLLAPSE`, `TOOL_RECURSION`, `BUDGET_QUOTA`, `DEADLOCK`), and prompt snippet.
     - **Entropy & Similarity Trajectory**: Step-by-step table displaying Shannon entropy ($H_{norm}$) and Jaccard similarity across conversational turns.
     - **Security Redaction Audit**: Count of masked tokens by category (API keys, AWS keys, Database DSNs, JWTs, emails).
     - **Actionable Remediation**: Recommended policy threshold adjustments and prompt guardrails.
   - Formats:
     - `markdown`: Formatted GFM report ready for GitHub issues, PRs, or Notion documentation.
     - `json`: Machine-readable schema for automated CI/CD gating and Datadog/PagerDuty payloads.
     - `terminal`: Rich ASCII summary table for fast developer CLI inspection.

2. **Developer Interface & CLI Integration**:
   - CLI Subcommand: `swarmwatch report --swarm-id <id> [--format markdown|json|terminal] [--output file.md]`
   - Server REST Endpoint: `GET /api/v1/swarms/{swarm_id}/report?format=markdown|json`

3. **Zero Token Cost & Performance Guarantee**:
   - 100% deterministic local computation with zero external LLM API calls.
   - Report generation executes in $<5ms$ per swarm session.
   - Zero impact on host trading systems or MetaTrader execution.

## Consequences
- AI teams obtain immediate, mathematically verifiable proof of SwarmWatch's financial ROI.
- Incident triage time drops from hours of manual log parsing to instant post-mortem analysis.
- Post-mortems can be committed to git or attached to incident tickets automatically.
