Type: grilling
Status: resolved

# Ticket 12: Graph Topology, Layout Engine & Visual Hierarchy
How do we layout the multi-agent graph to prevent spaghetti/hairball graphs when 10+ agents exchange hundreds of spans?

## Answer
Adopted Option B: Hierarchical Swimlanes with Causal Edges.
- Horizontal actor swimlanes with time flowing left-to-right.
- Directional pulsing edge arrows for inter-agent messages and tool executions.
- Loop and entropy warnings rendered as rotating red badges with live metrics.
- Flat, lightweight WebSocket payload optimized for 60fps WebGL rendering.
