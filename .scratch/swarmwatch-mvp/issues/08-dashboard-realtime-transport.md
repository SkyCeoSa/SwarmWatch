Type: grilling
Status: resolved

# Ticket 08: Real-Time UI Transport & Time-Machine Control Plane
How does the interactive React Flow visual dashboard receive live updates and send Time-Machine branch commands?

## Answer
Adopted Option C: Bi-directional event-driven WebSocket on /ws/swarm/{session_id}.
- Server push: Streams GRAPH_NODE_ADDED and SPAN_UPDATED events with <10ms latency to WebGL/React Flow frontend.
- Client control: Transmits typed commands (COMMAND_PAUSE_EXECUTION, COMMAND_STEP_BACK, COMMAND_RESUME_BRANCH) over the same channel to fork state without polling.
