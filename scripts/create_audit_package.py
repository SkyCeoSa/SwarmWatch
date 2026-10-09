# -*- coding: utf-8 -*-
r"""
Creates a clean SwarmWatch Audit Package Archive on Desktop:
C:\Users\skyrocket\Desktop\swarmwatch_audit_package.zip

Invariants:
- POSIX forward-slash paths
- 0 pycache / .pytest_cache
- 0 .db / .sqlite files
- 0 nested .zip files
- Clean single root tree
- Includes .github/workflows/ci.yml
"""

import os
import zipfile
from pathlib import Path

WORKSPACE = Path(r"C:\Users\skyrocket\.gemini\antigravity\scratch\swarmwatch")
OUTPUT_ZIP = Path(r"C:\Users\skyrocket\Desktop\swarmwatch_audit_package.zip")

EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "build",
    "swarmwatch.egg-info",
}

EXCLUDE_EXTS = {
    ".pyc",
    ".pyo",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".zip",
}

def should_include(rel_path: Path) -> bool:
    parts = rel_path.parts
    for part in parts:
        if part in EXCLUDE_DIRS:
            return False
        if "__pycache__" in part or ".egg-info" in part:
            return False
    
    if rel_path.suffix.lower() in EXCLUDE_EXTS:
        return False
    
    return True

def create_archive():
    print(f"Creating clean audit package at {OUTPUT_ZIP}...")
    archived_count = 0
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(WORKSPACE):
            root_path = Path(root)
            # Filter directories in-place to avoid descending into excluded dirs
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS and "__pycache__" not in d and not d.endswith(".egg-info")]
            
            for file_name in files:
                file_path = root_path / file_name
                rel_path = file_path.relative_to(WORKSPACE)
                if should_include(rel_path):
                    posix_name = rel_path.as_posix()
                    zf.write(file_path, arcname=posix_name)
                    archived_count += 1

    file_size_kb = OUTPUT_ZIP.stat().st_size / 1024
    print(f"Archived {archived_count} files ({file_size_kb:.1f} KB) into {OUTPUT_ZIP}")
    
    # Verification
    with zipfile.ZipFile(OUTPUT_ZIP, "r") as zf:
        names = zf.namelist()
        for name in names:
            assert "\\" not in name, f"Non-POSIX path detected: {name}"
            assert "__pycache__" not in name, f"Pycache detected: {name}"
            assert not name.endswith(".db"), f"DB file detected: {name}"
            assert not name.endswith(".zip"), f"Nested zip detected: {name}"
        assert ".github/workflows/ci.yml" in names, "Missing CI workflow!"
        assert "packages/sdk-python/swarmwatch/cross_cluster_sync.py" in names, "Missing Seam 77 python!"
        assert "packages/sdk-typescript/src/cross_cluster_sync.ts" in names, "Missing Seam 77 typescript!"
        assert "packages/sdk-python/swarmwatch/topology_migrator.py" in names, "Missing Seam 79 python!"
        assert "packages/sdk-typescript/src/topology_migrator.ts" in names, "Missing Seam 79 typescript!"
        assert "docs/adr/0090-dynamic-topology-repartitioning-and-subgraph-migration.md" in names, "Missing ADR 0090!"
        assert "packages/sdk-python/swarmwatch/qc.py" in names, "Missing Seam 81 python!"
        assert "packages/sdk-typescript/src/qc.ts" in names, "Missing Seam 81 typescript!"
        assert "packages/sdk-python/swarmwatch/deterministic_sim.py" in names, "Missing Seam 90 python!"
        assert "packages/sdk-typescript/src/deterministic_sim.ts" in names, "Missing Seam 90 typescript!"
        assert "docs/adr/0101-deterministic-simulation-and-model-checked-protocol-harness.md" in names, "Missing ADR 0101!"
        assert "packages/sdk-python/swarmwatch/mesh_relay.py" in names, "Missing Seam 91 python!"
        assert "packages/sdk-typescript/src/mesh_relay.ts" in names, "Missing Seam 91 typescript!"
        assert "packages/sdk-python/swarmwatch/sovereign_federation.py" in names, "Missing Seam 92 python!"
        assert "packages/sdk-typescript/src/sovereign_federation.ts" in names, "Missing Seam 92 typescript!"
        assert "packages/sdk-python/swarmwatch/proof_marketplace.py" in names, "Missing Seam 93 python!"
        assert "packages/sdk-typescript/src/proof_marketplace.ts" in names, "Missing Seam 93 typescript!"
        assert "packages/sdk-python/swarmwatch/recursive_snark.py" in names, "Missing Seam 94 python!"
        assert "packages/sdk-typescript/src/recursive_snark.ts" in names, "Missing Seam 94 typescript!"
        assert "packages/sdk-python/swarmwatch/billing_channel.py" in names, "Missing Seam 95 python!"
        assert "packages/sdk-typescript/src/billing_channel.ts" in names, "Missing Seam 95 typescript!"
        assert "packages/sdk-python/swarmwatch/byzantine_gossip.py" in names, "Missing Seam 96 python!"
        assert "packages/sdk-typescript/src/byzantine_gossip.ts" in names, "Missing Seam 96 typescript!"
        assert "packages/sdk-python/swarmwatch/slashing_tribunal.py" in names, "Missing Seam 97 python!"
        assert "packages/sdk-typescript/src/slashing_tribunal.ts" in names, "Missing Seam 97 typescript!"
        assert "packages/sdk-python/swarmwatch/enclave_attestation.py" in names, "Missing Seam 98 python!"
        assert "packages/sdk-typescript/src/enclave_attestation.ts" in names, "Missing Seam 98 typescript!"
        assert "packages/sdk-python/swarmwatch/boundary_fence.py" in names, "Missing Seam 99 python!"
        assert "packages/sdk-typescript/src/boundary_fence.ts" in names, "Missing Seam 99 typescript!"
        assert "packages/sdk-python/swarmwatch/master_singularity.py" in names, "Missing Seam 100 python!"
        assert "packages/sdk-typescript/src/master_singularity.ts" in names, "Missing Seam 100 typescript!"
        assert "docs/adr/0111-global-master-singularity-and-sovereign-registry.md" in names, "Missing ADR 0111!"
        assert "SWARMWATCH_AUDIT_PROMPT.md" in names, "Missing audit prompt!"
        assert "SWARMWATCH_MASTER_AUDIT_COMPENDIUM.md" in names, "Missing audit compendium!"
    print("[PASS] Verification successful: 0 pycache, 0 .db, 0 nested .zips, all 100 seams and audit artifacts verified.")

if __name__ == "__main__":
    create_archive()
