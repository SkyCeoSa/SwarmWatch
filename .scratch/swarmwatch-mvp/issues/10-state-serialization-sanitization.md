Type: grilling
Status: resolved

# Ticket 10: State Serialization, Memory Budgets & Sensitive Data Masking
How does SwarmWatch capture and serialize complex agent state objects safely without blowing memory or leaking API keys?

## Answer
Adopted Option B: `SafeStateSerializer` with memory caps and regex secret redaction.
- Custom JSON serializer fallback to `__repr__` for non-serializable objects (sockets, threads).
- Hard memory cap of 2MB per snapshot with GZIP compression for large context payloads.
- Automated secret redactor scanning API keys (`sk-...`, `ghp_...`, bearer tokens) replacing them with `[REDACTED_SECRET]`.
