Type: grilling
Status: resolved

# Ticket 06: Canonical Swarm Event Schema & Causal Graph Representation
What is the exact data model representing multi-agent causal interactions, tool-calls, and state hashes?

## Answer
Adopted Option C: Two-level strongly-typed Pydantic schema (SwarmSpan and SwarmEvent).
- Native multi-agent fields: swarm_id, source_agent_id, 	arget_agent_id, parent_span_id, 	ool_calls, entropy_score, state_snapshot_hash.
- Information hiding: Clean .to_otel_span() export method mapping multi-agent attributes to OTel GenAI Semantic Conventions with namespaced tags (gen_ai.swarmwatch.*).
