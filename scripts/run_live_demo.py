# -*- coding: utf-8 -*-
"""
SwarmWatch Live Interactive Demo Tour.
Demonstrates:
- Local server startup on http://127.0.0.1:8765
- Project provisioning ("AlphaQuant Arbitrage") & Cryptographic API Key generation
- Multi-agent swarm telemetry ingestion
- Shannon entropy evaluation and circuit breaker trip on runaway loop
- Live WebSocket stream & Impeccable console UI
"""

import asyncio
import os
import sys
import threading
import time
import webbrowser
import httpx
import uvicorn

# Add sdk-python to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../packages/sdk-python")))

from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter


def run_server(port=8765, db_path="swarmwatch_demo.db"):
    storage = SQLiteStorageAdapter(db_path=db_path)
    app = create_app(storage=storage)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


def main():
    port = 8765
    db_path = "swarmwatch_demo.db"
    base_url = f"http://127.0.0.1:{port}"

    print("\n" + "=" * 65)
    print("  \033[96mSWARMWATCH INDUSTRIAL OBSERVABILITY PLATFORM\033[0m")
    print("  \033[90mZero-Token Shannon Entropy Circuit Breaker & Safe Time-Machine\033[0m")
    print("=" * 65)

    # 1. Launch server thread
    print(f"\n[1/5] Starting local server on {base_url} ...")
    server_thread = threading.Thread(target=run_server, args=(port, db_path), daemon=True)
    server_thread.start()

    # Wait for server readiness
    ready = False
    for _ in range(30):
        try:
            r = httpx.get(f"{base_url}/api/v1/health", timeout=1.0)
            if r.status_code == 200:
                ready = True
                break
        except Exception:
            time.sleep(0.2)

    if not ready:
        print("\033[91mFailed to connect to local server.\033[0m")
        sys.exit(1)

    print(f"      \033[92m✓\033[0m Server healthy (version {r.json().get('version')})")

    # 2. Provision Tenant Hierarchy
    print("\n[2/5] Provisioning Enterprise Tenant & Cryptographic API Key ...")
    with httpx.Client(base_url=base_url) as client:
        # Create Project
        proj_res = client.post("/api/v1/projects", json={
            "name": "AlphaQuant Arbitrage",
            "slug": "alphaquant-arbitrage",
            "org_id": "org-deepmind-alpha"
        })
        project = proj_res.json()
        project_id = project["id"]
        print(f"      \033[92m✓\033[0m Project Created: {project['name']} (ID: {project_id[:8]}...)")

        # Create API Key
        key_res = client.post("/api/v1/keys", json={
            "project_id": project_id,
            "name": "Primary Production Agent Key",
            "is_test": False,
        })
        key_data = key_res.json()
        raw_key = key_data["api_key"]
        prefix = key_data["key_prefix"]
        print(f"      \033[92m✓\033[0m Cryptographic API Key: {prefix} (Raw: {raw_key[:16]}...)")

        # 3. Simulate Swarm Execution
        print("\n[3/5] Ingesting Multi-Agent Swarm Spans under Authenticated Context ...")
        headers = {
            "Authorization": f"Bearer {raw_key}",
            "X-Project-ID": project_id,
        }

        sim_res = client.post(
            f"/api/v1/simulate?swarm_id=swarm-alpha-01&branch_id=main",
            headers=headers
        )
        sim_data = sim_res.json()
        print(f"      \033[92m✓\033[0m Simulation Status: {sim_data.get('status')}")
        print(f"      \033[92m✓\033[0m Spans Generated: {sim_data.get('spans_count')}")

        # 4. Circuit Breaker Result
        cb = sim_data.get("circuit_breaker", {})
        print("\n[4/5] Evaluating Shannon Entropy Circuit Breaker ...")
        if cb.get("tripped"):
            print(f"      \033[93m⚡ CIRCUIT BREAKER TRIPPED\033[0m")
            print(f"         Reason:        {cb.get('reason')}")
            print(f"         Tokens Saved:  {cb.get('tokens_saved'):,}")
            print(f"         USD Protected: ${cb.get('dollars_saved'):.2f}")

    # 5. Dashboard Link
    print("\n[5/5] Dashboard Console Ready:")
    print(f"      \033[94m--> Open: {base_url}\033[0m")
    print("=" * 65)
    print("Press Ctrl+C to stop the demo server.\n")

    try:
        webbrowser.open(base_url)
    except Exception:
        pass

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping SwarmWatch demo server.")


if __name__ == "__main__":
    main()
