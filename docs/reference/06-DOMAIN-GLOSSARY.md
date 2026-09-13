# 06. Domain Glossary & Ubiquitous Language

This document reflects the authoritative ubiquitous language defined in `CONTEXT.md`. All team communications, variable names, APIs, and tests must adhere strictly to these terms.

---

## Core Entities

### Observability & Telemetry

**SwarmSpan**:
An immutable atomic record of an agent action, tool invocation, or causal message exchange within a multi-agent swarm.
_Avoid_: Log, trace event, message object

**Causal Stream**:
The chronologically ordered and parent-linked sequence of agent interactions forming a directed acyclic graph of swarm reasoning.
_Avoid_: Activity log, execution history, chat transcript

**StateSnapshot**:
A sanitized, compressed, and SHA-256 hashed capture of an agent's memory and working context at a specific turn.
_Avoid_: Memory dump, checkpoint, state backup

---

### Circuit Breaking & Safety

**Shannon Entropy**:
A normalized statistical score measuring the lexical diversity and information content of an agent's token stream.
_Avoid_: Repetition score, randomness metric, diversity index

**Lexical Similarity**:
A zero-token token-level Jaccard overlap measure computed across a sliding window of recent agent responses.
_Avoid_: Semantic similarity, embedding distance, LLM evaluation

**Circuit Breaker**:
An in-process mathematical governor that halts runaway agent execution when entropy collapses or repetitive loops are detected.
_Avoid_: Rate limiter, watchdog, abort controller

**Intervention**:
An automated action taken when a loop is identified, progressing from a context warning hint to an execution halt.
_Avoid_: Kill switch, exception handler, interrupt

**Counterfactual Savings**:
The estimated tokens and compute expenditure prevented by terminating an autonomous agent before reaching its configured iteration or budget ceiling.
_Avoid_: Predicted savings, future tokens, imaginary cost

---

### State & Time Machine

**Branch**:
An independent, isolated trajectory of swarm execution states originating from a root session or an explicit fork.
_Avoid_: Fork name, thread, session variant

**Time-Machine Fork**:
A non-destructive operation that clones historical state at a target span into a new branch for parallel exploration or bug recovery.
_Avoid_: Rewind, roll back, restore point
