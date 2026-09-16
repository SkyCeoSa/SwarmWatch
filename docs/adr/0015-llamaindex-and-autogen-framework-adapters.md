# ADR 0015: LlamaIndex & Microsoft AutoGen Framework Adapters

## Status
Accepted

## Date
2026-09-16

## Context
Following our successful adapters for LangGraph and CrewAI (ADR 0004), enterprise multi-agent applications increasingly deploy **LlamaIndex** (for workflow orchestration and multi-agent RAG) and **Microsoft AutoGen** (for conversational multi-agent systems).

Both ecosystems have distinct lifecycle hooks:
- LlamaIndex provides a structured `BaseCallbackHandler` and `CallbackManager` supporting event start/end hooks with payload dictionaries and event IDs.
- Microsoft AutoGen provides conversational message hooks (`register_hook` on `ConversableAgent` for `process_message_before_send`) and message exchange callbacks.

Direct monkey-patching of agent classes in either framework introduces brittle coupling that breaks on upstream minor updates.

## Decision
We implement non-invasive ecosystem adapters extending **Seam 5 (Ecosystem Adapter Seam)**:

1. **`SwarmWatchLlamaIndexHandler`**:
   - Inherits from `BaseCallbackHandler` (or standard object fallback if LlamaIndex is not installed).
   - Listens to `CBEventType.LLM`, `CBEventType.FUNCTION_CALL`, `CBEventType.AGENT_STEP`, and `CBEventType.SYNTHESIZE`.
   - Records each event as an immutable `SwarmSpan` preserving parent-child DAG causality.
   - Evaluates Shannon entropy on output synthesis and step thoughts to halt infinite query refinement loops.

2. **`SwarmWatchAutoGenListener`**:
   - Integrates via `agent.register_hook(hookable_method="process_message_before_send", hook=...)` or standalone conversation turn inspector.
   - Measures token-level Jaccard lexical similarity and Shannon entropy across conversational turns.
   - Automatically detects conversational ping-pong loops (e.g. repeated polite affirmations or recursive unresolvable code execution errors) and halts execution via `SwarmCircuitBreakException`.
   - Emits spans capturing sender, recipient, content payload, and token metrics.

3. **Graceful Fallback**:
   - Neither framework is a mandatory runtime requirement.
   - Both modules import safely in environments without `llama-index` or `pyautogen`.

## Consequences

### Positive
- Zero monkey-patching; 100% resilient to upstream internal refactorings.
- Unified multi-agent observability and circuit breaking across LangGraph, CrewAI, LlamaIndex, and AutoGen.
- Preserves SwarmWatch's zero-token cost guarantee.

### Negative
- Developers must pass the handler into LlamaIndex's `CallbackManager` or call `listener.register(agent)` on AutoGen agents.
