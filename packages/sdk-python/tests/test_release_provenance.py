"""
Unit and integration test suite for Verifiable Release Provenance & Signed Artifact DAG (ADR 0080 / Seam 69).
"""

import os
from pathlib import Path
import tempfile
import time
import pytest

from swarmwatch.release_provenance import (
    ArtifactDigest,
    ProvenanceStep,
    ProvenanceStepType,
    ProvenanceDag,
    SignedProvenanceManifest,
    ProvenanceVerificationResult,
    ProvenanceCyclicDependencyError,
    ProvenanceError,
    ReleaseProvenance,
    global_release_provenance,
)
from swarmwatch.root_witness import WitnessKeyManager


def test_artifact_digest_from_file_and_dict() -> None:
    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        f.write(b"Hello SwarmWatch SLSA Level 3 Provenance")
        temp_path = f.name

    try:
        digest = ArtifactDigest.from_file(temp_path)
        assert digest.byte_size == len(b"Hello SwarmWatch SLSA Level 3 Provenance")
        assert len(digest.sha256) == 64
        assert digest.mime_type == "application/octet-stream"

        d = digest.to_dict()
        restored = ArtifactDigest.from_dict(d)
        assert restored.path == digest.path
        assert restored.sha256 == digest.sha256
        assert restored.byte_size == digest.byte_size
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_provenance_step_digest_deterministic() -> None:
    step1 = ProvenanceStep(
        step_id="step-source-01",
        step_type=ProvenanceStepType.SOURCE_CHECKOUT,
        metadata={"git_branch": "main", "clean": True},
    )
    assert len(step1.step_digest) == 64
    assert step1.step_digest == step1.compute_digest()

    step2 = ProvenanceStep(
        step_id="step-source-01",
        step_type=ProvenanceStepType.SOURCE_CHECKOUT,
        metadata={"clean": True, "git_branch": "main"},
    )
    assert step1.step_digest == step2.step_digest


def test_provenance_dag_topological_sort_and_hash() -> None:
    dag = ProvenanceDag()

    s1 = ProvenanceStep("s1", ProvenanceStepType.SOURCE_CHECKOUT)
    s2 = ProvenanceStep("s2", ProvenanceStepType.ENVIRONMENT_LOCK, parent_step_ids=["s1"])
    s3 = ProvenanceStep("s3", ProvenanceStepType.TEST_EXECUTION, parent_step_ids=["s2"])
    s4 = ProvenanceStep("s4", ProvenanceStepType.BUILD_PACKAGING, parent_step_ids=["s3"])

    dag.add_step(s1)
    dag.add_step(s2)
    dag.add_step(s3)
    dag.add_step(s4)

    order = dag.topological_sort()
    assert [s.step_id for s in order] == ["s1", "s2", "s3", "s4"]

    dag_hash = dag.compute_dag_hash()
    assert len(dag_hash) == 64
    assert dag_hash.startswith("sw:prov:dag:") or len(dag_hash) == 64


def test_provenance_dag_cyclic_dependency_rejection() -> None:
    dag = ProvenanceDag()
    s1 = ProvenanceStep("s1", ProvenanceStepType.SOURCE_CHECKOUT)
    s2 = ProvenanceStep("s2", ProvenanceStepType.BUILD_PACKAGING, parent_step_ids=["s1"])
    dag.add_step(s1)
    dag.add_step(s2)

    # s3 points back to s2, and introduces cycle s1 -> s2 -> s3 -> s2
    s3 = ProvenanceStep("s3", ProvenanceStepType.TEST_EXECUTION, parent_step_ids=["s2"])
    dag.add_step(s3)

    # Force cycle by modifying internal step dependencies
    s2.parent_step_ids.append("s3")
    with pytest.raises(ProvenanceCyclicDependencyError):
        dag.topological_sort()


def test_provenance_dag_missing_parent_rejection() -> None:
    dag = ProvenanceDag()
    s_orphan = ProvenanceStep("s_orphan", ProvenanceStepType.BUILD_PACKAGING, parent_step_ids=["non-existent-parent"])
    with pytest.raises(ProvenanceError, match="Missing parent step"):
        dag.add_step(s_orphan)


def test_generate_manifest_and_verify_signature() -> None:
    prov = ReleaseProvenance()

    s1 = prov.record_step(
        step_type=ProvenanceStepType.SOURCE_CHECKOUT,
        metadata={"commit": "abc1234", "branch": "main"},
    )
    s2 = prov.record_step(
        step_type=ProvenanceStepType.TEST_EXECUTION,
        parent_step_ids=[s1.step_id],
        metadata={"tests_passed": 1051, "duration_s": 387.2},
    )
    s3 = prov.record_step(
        step_type=ProvenanceStepType.BUILD_PACKAGING,
        parent_step_ids=[s2.step_id],
        outputs=[ArtifactDigest("dist/swarmwatch-0.1.0-py3-none-any.whl", "a" * 64, 489003, "application/x-wheel+zip")],
    )

    manifest = prov.generate_manifest(
        git_commit="abc1234",
        git_branch="main",
    )

    assert manifest.git_commit == "abc1234"
    assert manifest.signature != ""
    assert manifest.public_key_b64 == prov.key_manager.public_key_b64
    assert len(manifest.steps) == 3
    assert len(manifest.products) == 1

    # Verify manifest with default public key
    result = prov.verify_manifest(manifest)
    assert result.is_valid is True
    assert len(result.errors) == 0
    assert result.verified_steps_count == 3


def test_tamper_detection_in_manifest_signature() -> None:
    prov = ReleaseProvenance()
    prov.record_step(ProvenanceStepType.SOURCE_CHECKOUT)
    manifest = prov.generate_manifest(git_commit="clean-sha-01")

    # Tamper with commit SHA in manifest without re-signing
    manifest_dict = manifest.to_dict()
    manifest_dict["git_commit"] = "tampered-sha-02"

    result = prov.verify_manifest(manifest_dict)
    assert result.is_valid is False
    assert any("signature mismatch" in e.lower() or "signature" in e.lower() for e in result.errors)


def test_tamper_detection_in_step_digest() -> None:
    prov = ReleaseProvenance()
    s1 = prov.record_step(ProvenanceStepType.SOURCE_CHECKOUT, metadata={"clean": True})
    manifest = prov.generate_manifest(git_commit="sha-001")

    # Tamper with step metadata without updating step_digest or signature
    manifest_dict = manifest.to_dict()
    manifest_dict["steps"][0]["metadata"]["clean"] = False

    result = prov.verify_manifest(manifest_dict)
    assert result.is_valid is False
    assert any("Step digest corruption" in e or "signature" in e.lower() for e in result.errors)


def test_tamper_detection_in_disk_artifacts() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        product_file = tmp_path / "mock-package.whl"
        product_file.write_bytes(b"Clean Build Bytes v0.1.0")

        prov = ReleaseProvenance()
        prod_digest = ArtifactDigest.from_file(product_file, relative_to=tmp_path)
        prov.record_step(
            ProvenanceStepType.BUILD_PACKAGING,
            outputs=[prod_digest],
        )
        manifest = prov.generate_manifest(git_commit="build-sha-1")

        # 1. Clean verification against disk
        res_clean = prov.verify_manifest(manifest, verify_disk_files=True, base_dir=tmp_path)
        assert res_clean.is_valid is True
        assert res_clean.verified_products_count == 1

        # 2. Tamper with file on disk
        product_file.write_bytes(b"Tampered Injected Malicious Bytes")
        res_tampered = prov.verify_manifest(manifest, verify_disk_files=True, base_dir=tmp_path)
        assert res_tampered.is_valid is False
        assert any("checksum tampering" in e for e in res_tampered.errors)


def test_cross_key_signature_rejection() -> None:
    prov1 = ReleaseProvenance()
    prov1.record_step(ProvenanceStepType.SOURCE_CHECKOUT)
    manifest = prov1.generate_manifest(git_commit="sha-key-test")

    # Generate an independent keypair
    alien_key = WitnessKeyManager.generate()
    alien_pub = alien_key.public_key_b64

    # Verification with alien public key must fail
    res = prov1.verify_manifest(manifest, public_key_b64=alien_pub)
    assert res.is_valid is False
    assert any("signature mismatch" in e.lower() for e in res.errors)


def test_sub_250us_provenance_verification_slo() -> None:
    prov = ReleaseProvenance()
    s1 = prov.record_step(ProvenanceStepType.SOURCE_CHECKOUT)
    s2 = prov.record_step(ProvenanceStepType.TEST_EXECUTION, parent_step_ids=[s1.step_id])
    prov.record_step(ProvenanceStepType.BUILD_PACKAGING, parent_step_ids=[s2.step_id])
    manifest = prov.generate_manifest(git_commit="bench-sha-01")

    # Warmup
    for _ in range(5):
        prov.verify_manifest(manifest)

    # Benchmark: 200 iterations of full verification
    iterations = 200
    t0 = time.perf_counter()
    for _ in range(iterations):
        res = prov.verify_manifest(manifest)
        assert res.is_valid is True
    t1 = time.perf_counter()

    avg_us = ((t1 - t0) / iterations) * 1_000_000
    assert avg_us < 250.0, f"Provenance verification took {avg_us:.2f}µs, exceeding 250µs SLO"


def test_global_release_provenance_status_and_reset() -> None:
    st = global_release_provenance.status()
    assert "active_dag_step_count" in st
    assert "slsa_level" in st
    assert st["slsa_level"] == "SLSA_BUILD_LEVEL_3"

    global_release_provenance.record_step(ProvenanceStepType.SOURCE_CHECKOUT)
    assert global_release_provenance.status()["active_dag_step_count"] >= 1

    global_release_provenance.reset()
    assert global_release_provenance.status()["active_dag_step_count"] == 0


def test_release_provenance_rest_api() -> None:
    from starlette.testclient import TestClient
    from swarmwatch.server import create_app
    from swarmwatch.storage import SQLiteStorageAdapter

    app = create_app(storage=SQLiteStorageAdapter(db_path=":memory:"))
    client = TestClient(app)

    # Reset state
    global_release_provenance.reset()

    # 1. Record checkout step
    res1 = client.post(
        "/api/v1/provenance/record-step",
        json={
            "step_type": "SOURCE_CHECKOUT",
            "metadata": {"repo": "git@github.com:SkyCeoSa/SwarmWatch.git"},
            "step_id": "step-rest-checkout",
        }
    )
    assert res1.status_code == 200
    s1 = res1.json()
    assert s1["step_id"] == "step-rest-checkout"
    assert s1["step_type"] == "SOURCE_CHECKOUT"
    assert "step_digest" in s1

    # 2. Record build step with parent linkage
    res2 = client.post(
        "/api/v1/provenance/record-step",
        json={
            "step_type": "BUILD_PACKAGING",
            "parent_step_ids": ["step-rest-checkout"],
            "outputs": [
                {
                    "path": "dist/swarmwatch_sdk-0.1.0-py3-none-any.whl",
                    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    "byte_size": 1024,
                    "mime_type": "application/x-wheel+zip",
                }
            ],
            "metadata": {"clean": True},
            "step_id": "step-rest-build",
        }
    )
    assert res2.status_code == 200
    s2 = res2.json()
    assert s2["step_id"] == "step-rest-build"
    assert s2["parent_step_ids"] == ["step-rest-checkout"]

    # 3. Generate manifest
    res_gen = client.post(
        "/api/v1/provenance/generate",
        json={
            "git_commit": "rest-api-sha-12345678",
            "git_branch": "main",
            "builder_id": "https://github.com/SkyCeoSa/SwarmWatch/actions/runner",
        }
    )
    assert res_gen.status_code == 200
    manifest = res_gen.json()
    assert manifest["git_commit"] == "rest-api-sha-12345678"
    assert len(manifest["steps"]) == 2
    assert len(manifest["products"]) == 1
    assert "signature" in manifest
    assert "dag_root_hash" in manifest

    # 4. Verify manifest (valid)
    res_ver = client.post(
        "/api/v1/provenance/verify",
        json={
            "manifest": manifest,
            "verify_disk_files": False,
        }
    )
    assert res_ver.status_code == 200
    v_clean = res_ver.json()
    assert v_clean["is_valid"] is True
    assert v_clean["verified_steps_count"] == 2

    # 5. Verify manifest (tampered)
    tampered_manifest = dict(manifest)
    tampered_manifest["git_commit"] = "tampered-commit-sha"
    res_ver_bad = client.post(
        "/api/v1/provenance/verify",
        json={
            "manifest": tampered_manifest,
            "verify_disk_files": False,
        }
    )
    assert res_ver_bad.status_code == 200
    v_bad = res_ver_bad.json()
    assert v_bad["is_valid"] is False
    assert len(v_bad["errors"]) > 0

    # 6. GET DAG endpoint
    res_dag = client.get("/api/v1/provenance/dag")
    assert res_dag.status_code == 200
    dag_data = res_dag.json()
    assert dag_data["step_count"] == 2
    assert "dag_hash" in dag_data

    # 7. GET status endpoint
    res_status = client.get("/api/v1/provenance/status")
    assert res_status.status_code == 200
    st_data = res_status.json()
    assert st_data["active_dag_step_count"] == 2
    assert st_data["total_manifests_issued"] == 1
    assert st_data["slsa_level"] == "SLSA_BUILD_LEVEL_3"

