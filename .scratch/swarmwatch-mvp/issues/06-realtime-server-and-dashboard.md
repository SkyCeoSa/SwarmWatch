# 06: Real-time Ingestion Server, WebSocket Control Plane & Live Swimlane Dashboard

**What to build:**
The FastAPI server serving REST ingestion, real-time bi-directional WebSocket on `/ws/swarm/{session_id}`, and the lightweight WebGL/Swimlane graph visualization.

**Blocked by:** 01, 04, 05

**Status:** ready-for-agent

## Acceptance Criteria
- [ ] FastAPI boots and serves WebSocket endpoint streaming live graph updates with <10ms latency.
- [ ] Client commands (`CMD_PAUSE`, `CMD_FORK_BRANCH`) are accepted over WebSocket.
- [ ] WebGL/Swimlane view renders actor tracks and animated message arrows at 60fps.
- [ ] CLI command `swarmwatch ui` launches server and opens dashboard in browser.
