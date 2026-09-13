Type: grilling
Status: resolved

# Ticket 09: SDK Developer Surface & Instrumentation Ergonomics
What is the exact public API surface developers write in their code to instrument agents, tools, and swarms?

## Answer
Adopted Option C: Hybrid Decorator + Context Manager ergonomics.
- `@swarmwatch.agent(role="...", swarm="...")` for class/function instrumentation.
- `with swarmwatch.span("...", source="...", target="..."):` for explicit interaction blocks.
- `swarmwatch.wrap_mcp_server(server)` for zero-touch Model Context Protocol tool hooks.
- Deep module design: all OTel extraction, buffering, and background queueing hidden beneath a clean Pythonic API with full type hints.
