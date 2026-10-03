# -*- coding: utf-8 -*-
"""
Test harness executing the Seam 72 Live Mesh Federation Simulation Suite.
Ensures that all 8 operational phases pass in automated CI/CD and local test environments.
"""

import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from scripts.simulate_seam72_mesh_federation import MeshSimulationAuditor


def test_seam72_full_simulation_lifecycle() -> None:
    auditor = MeshSimulationAuditor()
    auditor.run_step1_peer_provisioning()
    auditor.run_step2_mutual_handshakes()
    auditor.run_step3_mesh_topology_graph()
    auditor.run_step4_dijkstra_and_rerouting()
    auditor.run_step5_ttl_and_loop_prevention()
    auditor.run_step6_capability_fencing()
    auditor.run_step7_safety_circuit_coupling()
    auditor.run_step8_latency_slo_and_receipts()

    st = auditor.router.status()
    assert st["online_peers_count"] == 5
    assert st["active_links_count"] >= 5
    assert st["active_sessions_count"] == 1
    assert st["circuit_tripped"] is False
    assert auditor.metrics["avg_us"] < 250.0
