# -*- coding: utf-8 -*-
"""
Automated Test Suite for SwarmWatch S3 / GCS Cold Storage Archival & Restore (Seam 25 / ADR 0036).
Verifies MockObjectStorageClient, S3ObjectStorageClient, ColdStorageArchiver,
streaming gzip compression, deterministic SHA-256 manifest, REST endpoints, and CLI.
"""

from datetime import datetime, timezone
import json
import pytest
from starlette.testclient import TestClient

from swarmwatch.archival import (
    ObjectStorageClient,
    MockObjectStorageClient,
    S3ObjectStorageClient,
    ColdStorageArchiver,
    ArchiveManifest,
    ArchiveRecord,
    RestoredSwarmSummary,
)
from swarmwatch.schema import SwarmSpan, StateSnapshot
from swarmwatch.server import create_app
from swarmwatch.storage import SQLiteStorageAdapter


@pytest.fixture
def mock_storage(tmp_path):
    """Isolated SQLite storage adapter for tests."""
    db_file = str(tmp_path / "archival_test.db")
    return SQLiteStorageAdapter(db_path=db_file)


@pytest.fixture
def mock_obj_store():
    """In-memory mock object storage client."""
    return MockObjectStorageClient()


@pytest.fixture
def archiver(mock_obj_store, mock_storage):
    """Configured ColdStorageArchiver instance."""
    return ColdStorageArchiver(
        client=mock_obj_store,
        default_bucket="test-cold-bucket",
        storage_adapter=mock_storage,
    )


def test_mock_object_storage_primitives(mock_obj_store):
    """Verifies atomic upload, download, list, head, and delete on MockObjectStorageClient."""
    bucket = "my-bucket"
    key = "folder/data.bin"
    payload = b"test payload bytes for cold storage"

    mock_obj_store.upload_object(bucket, key, payload, content_type="application/octet-stream", metadata={"author": "test"})

    # Download
    downloaded = mock_obj_store.download_object(bucket, key)
    assert downloaded == payload

    # Head
    head = mock_obj_store.head_object(bucket, key)
    assert head is not None
    assert head["size_bytes"] == len(payload)
    assert head["content_type"] == "application/octet-stream"
    assert head["custom_metadata"]["author"] == "test"

    # List
    keys = mock_obj_store.list_objects(bucket, prefix="folder/")
    assert keys == [key]

    # Delete
    deleted = mock_obj_store.delete_object(bucket, key)
    assert deleted is True
    assert mock_obj_store.list_objects(bucket, prefix="folder/") == []

    with pytest.raises(FileNotFoundError):
        mock_obj_store.download_object(bucket, key)


def test_s3_object_storage_client_initialization():
    """Verifies S3ObjectStorageClient constructor defaults and SigV4 parameter setups."""
    client = S3ObjectStorageClient(
        endpoint_url="https://s3.us-west-2.amazonaws.com",
        access_key_id="test-key-id",
        secret_access_key="test-secret-key",
        region_name="us-west-2",
    )
    assert client.endpoint_url == "https://s3.us-west-2.amazonaws.com"
    assert client.access_key_id == "test-key-id"
    assert client.region_name == "us-west-2"


@pytest.mark.asyncio
async def test_cold_storage_archiver_roundtrip(archiver, mock_storage, mock_obj_store, tmp_path):
    """
    Verifies full roundtrip:
    1. Populate hot SQLite storage with spans and snapshots.
    2. Archive swarm to cold storage (gzip, SHA-256, partitioned keys).
    3. Verify manifest integrity.
    4. Restore swarm back into fresh storage and verify exact match.
    """
    await mock_storage.initialize()
    now_dt = datetime.now(timezone.utc)
    swarm_id = "swarm-cold-rt-01"

    # 1. Populate hot storage
    spans = [
        SwarmSpan(
            id="span_01",
            swarm_id=swarm_id,
            project_id="proj-finance",
            source_agent="AgentAlpha",
            target_agent="AgentBeta",
            token_count=1250,
            entropy_score=1.85,
            start_time=now_dt,
            end_time=now_dt,
            duration_ms=250.0,
            input_payload={"task": "financial_reconciliation"},
            output_payload={"status": "completed"},
        ),
        SwarmSpan(
            id="span_02",
            swarm_id=swarm_id,
            project_id="proj-finance",
            source_agent="AgentBeta",
            target_agent="AgentGamma",
            token_count=1800,
            entropy_score=1.72,
            start_time=now_dt,
            end_time=now_dt,
            duration_ms=310.0,
            input_payload={"task": "audit_review"},
            output_payload={"status": "approved"},
        ),
    ]
    await mock_storage.save_spans(spans)

    snap = StateSnapshot(
        id="snap_01",
        span_id="span_02",
        swarm_id=swarm_id,
        project_id="proj-finance",
        state_data={"reconciliation_id": "rec_9921", "is_balanced": True},
        state_hash="sha256-mock-state-hash",
    )
    await mock_storage.save_snapshot(snap)

    # 2. Archive swarm
    record = await archiver.archive_swarm(
        swarm_id=swarm_id,
        bucket="test-cold-bucket",
        project_id="proj-finance",
        org_id="org-fintech",
    )

    assert record.swarm_id == swarm_id
    assert record.spans_count == 2
    assert record.snapshots_count == 1
    assert "org=org-fintech/proj=proj-finance" in record.archive_key
    assert record.archive_key.endswith(f"swarm={swarm_id}.jsonl.gz")
    assert record.manifest_key.endswith(f"swarm={swarm_id}.manifest.json")
    assert len(record.sha256_checksum) == 64
    assert record.compressed_size_bytes > 0
    assert record.uncompressed_size_bytes > record.compressed_size_bytes

    # 3. Check manifest and integrity
    manifest = archiver.get_archive_manifest(swarm_id)
    assert manifest is not None
    assert manifest.swarm_id == swarm_id
    assert manifest.total_tokens == 3050
    assert manifest.sha256_checksum == record.sha256_checksum

    is_valid = archiver.verify_archive_integrity(swarm_id)
    assert is_valid is True

    # 4. List archives
    archives = archiver.list_archives(bucket="test-cold-bucket", project_id="proj-finance")
    assert len(archives) == 1
    assert archives[0].swarm_id == swarm_id

    # 5. Restore swarm into a fresh SQLite storage adapter
    fresh_db = str(tmp_path / "fresh_restore.db")
    fresh_storage = SQLiteStorageAdapter(db_path=fresh_db)
    await fresh_storage.initialize()

    restored = await archiver.restore_swarm(
        swarm_id=swarm_id,
        bucket="test-cold-bucket",
        target_storage=fresh_storage,
    )

    assert restored.swarm_id == swarm_id
    assert restored.spans_restored == 2
    assert restored.snapshots_restored == 1
    assert restored.sha256_verified is True

    # Verify restored records in fresh storage
    fresh_spans = await fresh_storage.get_swarm_spans(swarm_id)
    assert len(fresh_spans) == 2
    assert fresh_spans[0].id == "span_01"
    assert fresh_spans[1].id == "span_02"

    fresh_snapshots = await fresh_storage.get_snapshots_for_swarm(swarm_id)
    assert len(fresh_snapshots) == 1
    assert fresh_snapshots[0].span_id == "span_02"
    assert fresh_snapshots[0].state_data["reconciliation_id"] == "rec_9921"


@pytest.mark.asyncio
async def test_cold_storage_tamper_detection(archiver, mock_storage, mock_obj_store):
    """Verifies that tampering with the compressed bundle triggers SHA-256 integrity breach."""
    await mock_storage.initialize()
    now_dt = datetime.now(timezone.utc)
    swarm_id = "swarm-tamper-01"

    spans = [
        SwarmSpan(
            id="span_t1",
            swarm_id=swarm_id,
            source_agent="A",
            target_agent="B",
            token_count=100,
            entropy_score=1.0,
            start_time=now_dt,
            end_time=now_dt,
            duration_ms=50.0,
        )
    ]
    await mock_storage.save_spans(spans)

    record = await archiver.archive_swarm(swarm_id=swarm_id)

    # Tamper with archive bundle in object store
    mock_obj_store.upload_object(record.bucket, record.archive_key, b"corrupted bytes")

    # Verification should fail
    assert archiver.verify_archive_integrity(swarm_id) is False

    # Restore should raise ValueError
    with pytest.raises(ValueError, match="Cryptographic integrity breach: SHA-256 checksum mismatch"):
        await archiver.restore_swarm(swarm_id=swarm_id)


def test_server_cold_storage_rest_endpoints(tmp_path, mock_obj_store):
    """Verifies FastAPI control plane endpoints for archiving, restoring, and listing archives."""
    server_db = str(tmp_path / "server_cold_endpoints.db")
    server_storage = SQLiteStorageAdapter(db_path=server_db)
    archiver = ColdStorageArchiver(
        client=mock_obj_store,
        default_bucket="server-test-bucket",
        storage_adapter=server_storage,
    )
    app = create_app(storage=server_storage, archiver=archiver)
    client = TestClient(app)

    # Ingest a span first via client
    client.post("/api/v1/spans", json=[{
        "id": "srv_span_01",
        "swarm_id": "swarm-server-arch-01",
        "source_agent": "SrvAgentA",
        "target_agent": "SrvAgentB",
        "token_count": 890,
        "entropy_score": 1.75,
        "duration_ms": 110.0,
    }])

    # 1. Archive endpoint
    res_arch = client.post("/api/v1/storage/archive/swarm-server-arch-01", json={"delete_local": False})
    assert res_arch.status_code == 200
    arch_data = res_arch.json()
    assert arch_data["status"] == "archived"
    assert arch_data["record"]["swarm_id"] == "swarm-server-arch-01"
    assert arch_data["record"]["spans_count"] >= 1

    # 2. List archives endpoint
    res_list = client.get("/api/v1/storage/archives")
    assert res_list.status_code == 200
    list_data = res_list.json()
    assert len(list_data["archives"]) >= 1
    assert any(a["swarm_id"] == "swarm-server-arch-01" for a in list_data["archives"])

    # 3. Get manifest endpoint
    res_man = client.get("/api/v1/storage/archives/swarm-server-arch-01/manifest")
    assert res_man.status_code == 200
    man_data = res_man.json()
    assert man_data["swarm_id"] == "swarm-server-arch-01"
    assert man_data["spans_count"] >= 1

    # 4. Restore endpoint
    res_rest = client.post("/api/v1/storage/restore/swarm-server-arch-01")
    assert res_rest.status_code == 200
    rest_data = res_rest.json()
    assert rest_data["status"] == "restored"
    assert rest_data["summary"]["swarm_id"] == "swarm-server-arch-01"
    assert rest_data["summary"]["sha256_verified"] is True


def test_cli_archive_subcommands(tmp_path, monkeypatch):
    """Verifies swarmwatch archive CLI execution: export, list, manifest, restore."""
    from swarmwatch.cli import main
    db_file = str(tmp_path / "cli_arch_test.db")
    storage = SQLiteStorageAdapter(db_path=db_file)
    now_dt = datetime.now(timezone.utc)
    swarm_id = "swarm-cli-arch-01"

    # Populate storage
    import asyncio
    asyncio.run(storage.initialize())
    asyncio.run(storage.save_spans([
        SwarmSpan(
            id="span_cli_01",
            swarm_id=swarm_id,
            source_agent="CliAgentA",
            target_agent="CliAgentB",
            token_count=650,
            entropy_score=1.80,
            start_time=now_dt,
            end_time=now_dt,
            duration_ms=150.0,
        )
    ]))

    # Set mock storage dir in env
    mock_dir = str(tmp_path / "mock_store")
    monkeypatch.setenv("SWARMWATCH_STORAGE_DIR", mock_dir)

    # 1. swarmwatch archive export
    with pytest.raises(SystemExit) as exc:
        main(["archive", "export", "--swarm-id", swarm_id, "--db", db_file, "--format", "json"])
    assert exc.value.code == 0

    # 2. swarmwatch archive list
    with pytest.raises(SystemExit) as exc:
        main(["archive", "list", "--db", db_file, "--format", "json"])
    assert exc.value.code == 0

    # 3. swarmwatch archive manifest
    with pytest.raises(SystemExit) as exc:
        main(["archive", "manifest", "--swarm-id", swarm_id, "--db", db_file, "--format", "json"])
    assert exc.value.code == 0

    # 4. swarmwatch archive restore
    fresh_db = str(tmp_path / "fresh_cli.db")
    with pytest.raises(SystemExit) as exc:
        main(["archive", "restore", "--swarm-id", swarm_id, "--db", fresh_db, "--format", "json"])
    assert exc.value.code == 0

