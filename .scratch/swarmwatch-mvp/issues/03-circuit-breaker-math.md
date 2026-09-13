Type: grilling
Status: resolved

# Ticket 03: Entropy Circuit Breaker Mathematical Mechanism
How do we mathematically detect infinite loops and degenerate agent babble in real time without calling another expensive LLM?

## Answer
Adopted Option C: Local mathematical calculation over a sliding window of the last 3 turns.
Combines Shannon token entropy, character/token edit distance, and cosine similarity over local embeddings or character n-gram shingles.
Sub-millisecond execution, zero token cost, zero LLM-as-a-judge latency.
