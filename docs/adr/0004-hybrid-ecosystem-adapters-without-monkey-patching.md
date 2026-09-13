# ADR 0004: Hybrid Ecosystem Adapters Without Monkey-Patching

## Status
Accepted

## Context
Agent frameworks like LangGraph and CrewAI evolve rapidly. Invasive monkey-patching or subclassing internal engine classes causes brittle breakages on minor upstream library updates.

## Decision
We adopt a hybrid, non-invasive adapter pattern:
- **LangGraph**: Implement `SwarmWatchCallbackHandler` adhering to the official `BaseCallbackHandler` interface, plus an optional non-invasive `patch_graph()` wrapper that decorates public graph nodes for deep state snapshotting.
- **CrewAI**: Implement `SwarmWatchCrewListener` designed to be passed directly to `step_callback` and `task_callback`.
- **Packaging**: Framework dependencies are segregated as optional extras (`[project.optional-dependencies]`) in `pyproject.toml` with lazy imports and graceful fallbacks.

## Consequences
- **Positive**: 100% immune to internal private API refactorings in LangGraph/CrewAI.
- **Positive**: Clean separation of concerns; users who don't use CrewAI don't install CrewAI.
- **Negative**: Graph node state patching requires an explicit call to `patch_graph(graph)` if full time-machine state snapshotting is desired.
