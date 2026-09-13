Type: grilling
Status: resolved

# Ticket 01: SDK Hooking Strategy & Target Runtimes
How should the SwarmWatch SDK attach to host agents without causing framework lock-in or latency overhead?

## Answer
Adopted Option C: Hybrid OpenTelemetry GenAI Semantic Conventions + native Model Context Protocol (MCP) interceptor.
Exposed via a clean, 2-line initializer: swarmwatch.init(). Framework-agnostic, zero hardcoded couplings to LangGraph/CrewAI.
