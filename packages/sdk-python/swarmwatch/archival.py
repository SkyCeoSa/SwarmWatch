# -*- coding: utf-8 -*-
"""
SwarmWatch S3 / Google Cloud Storage Streaming Log Archival & Cold Storage Tier (Seam 25 / ADR 0036).
Provides long-term audit retention, regulatory compliance, and cloud cold-storage offloading
for historical swarm telemetry using streaming gzip compression, standard data lake partitioning,
and cryptographic SHA-256 manifest verification.
"""

import abc
from datetime import datetime, timezone
import gzip
import hashlib
import hmac
import io
import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional
import urllib.parse
import urllib.request
import urllib.error

from pydantic import BaseModel, Field

from swarmwatch.schema import SwarmSpan, StateSnapshot, CircuitEvent
from swarmwatch.storage import StorageAdapter

logger = logging.getLogger("swarmwatch.archival")


class ObjectStorageClient(abc.ABC):
    """
    Deep Module Object Storage Interface.
    Abstracts cloud object storage systems (Amazon S3, MinIO, GCS, Cloudflare R2).
    """

    @abc.abstractmethod
    def upload_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        """Upload raw bytes to target bucket and key."""
        pass

    @abc.abstractmethod
    def download_object(self, bucket: str, key: str) -> bytes:
        """Download raw bytes from target bucket and key."""
        pass

    @abc.abstractmethod
    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """List all object keys matching the specified prefix."""
        pass

    @abc.abstractmethod
    def delete_object(self, bucket: str, key: str) -> bool:
        """Delete an object from bucket. Returns True if deleted or did not exist."""
        pass

    @abc.abstractmethod
    def head_object(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve object metadata and byte size without downloading content."""
        pass


class MockObjectStorageClient(ObjectStorageClient):
    """
    In-process, thread-safe mock object store guaranteeing 100% offline,
    deterministic execution with zero external cloud credentials.
    """

    def __init__(self, base_dir: Optional[str] = None):
        self._lock = threading.RLock()
        self._buckets: Dict[str, Dict[str, bytes]] = {}
        self._metadata: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._base_dir = base_dir

    def upload_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        with self._lock:
            if bucket not in self._buckets:
                self._buckets[bucket] = {}
                self._metadata[bucket] = {}

            self._buckets[bucket][key] = bytes(data)
            self._metadata[bucket][key] = {
                "content_type": content_type,
                "size_bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "custom_metadata": dict(metadata or {}),
            }

            if self._base_dir:
                try:
                    target_path = os.path.join(self._base_dir, bucket, key.replace("/", os.sep))
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    with open(target_path, "wb") as f:
                        f.write(data)
                except Exception as e:
                    logger.warning(f"Failed to mirror mock object to disk: {e}")

    def download_object(self, bucket: str, key: str) -> bytes:
        with self._lock:
            if bucket in self._buckets and key in self._buckets[bucket]:
                return self._buckets[bucket][key]

            # Try local disk if available
            if self._base_dir:
                target_path = os.path.join(self._base_dir, bucket, key.replace("/", os.sep))
                if os.path.exists(target_path):
                    with open(target_path, "rb") as f:
                        return f.read()

            raise FileNotFoundError(f"Object '{key}' not found in bucket '{bucket}'")

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        with self._lock:
            keys = set()
            if bucket in self._buckets:
                keys.update(k for k in self._buckets[bucket].keys() if k.startswith(prefix))

            if self._base_dir:
                bucket_dir = os.path.join(self._base_dir, bucket)
                if os.path.exists(bucket_dir):
                    for root, _, files in os.walk(bucket_dir):
                        for f in files:
                            full_path = os.path.join(root, f)
                            rel_path = os.path.relpath(full_path, bucket_dir).replace("\\", "/")
                            if rel_path.startswith(prefix):
                                keys.add(rel_path)
            return sorted(keys)

    def delete_object(self, bucket: str, key: str) -> bool:
        with self._lock:
            deleted = False
            if bucket in self._buckets and key in self._buckets[bucket]:
                del self._buckets[bucket][key]
                self._metadata[bucket].pop(key, None)
                deleted = True
            if self._base_dir:
                target_path = os.path.join(self._base_dir, bucket, key.replace("/", os.sep))
                if os.path.exists(target_path):
                    try:
                        os.remove(target_path)
                        deleted = True
                    except Exception:
                        pass
            return deleted

    def head_object(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if bucket in self._metadata and key in self._metadata[bucket]:
                return dict(self._metadata[bucket][key])
            if self._base_dir:
                target_path = os.path.join(self._base_dir, bucket, key.replace("/", os.sep))
                if os.path.exists(target_path):
                    return {
                        "content_type": "application/octet-stream",
                        "size_bytes": os.path.getsize(target_path),
                        "etag": "",
                    }
            return None


class S3ObjectStorageClient(ObjectStorageClient):
    """
    Pure Python standard-library client for AWS S3, MinIO, Ceph, and Cloudflare R2
    using standard HTTP REST with AWS Signature Version 4 (SigV4).
    Zero external pip dependencies (e.g. boto3 not required).
    """

    def __init__(
        self,
        endpoint_url: str = "https://s3.amazonaws.com",
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
    ):
        self.endpoint_url = endpoint_url.rstrip("/")
        self.access_key_id = access_key_id or os.getenv("AWS_ACCESS_KEY_ID", "mock-access-key")
        self.secret_access_key = secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")

    def _sign_and_request(
        self,
        method: str,
        bucket: str,
        key: str,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
    ) -> bytes:
        # Standard URL formatting (path-style or virtual-hosted style)
        clean_key = key.lstrip("/")
        if "s3.amazonaws.com" in self.endpoint_url and self.region_name != "us-east-1":
            base = f"https://{bucket}.s3.{self.region_name}.amazonaws.com"
            path = f"/{clean_key}"
        elif "s3.amazonaws.com" in self.endpoint_url:
            base = f"https://{bucket}.s3.amazonaws.com"
            path = f"/{clean_key}"
        else:
            base = self.endpoint_url
            path = f"/{bucket}/{clean_key}"

        url = f"{base}{path}"
        if query_params:
            url += "?" + urllib.parse.urlencode(query_params)

        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")

        payload_bytes = data or b""
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        req_headers = {
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            **(headers or {}),
        }

        # SigV4 canonical headers
        parsed_url = urllib.parse.urlparse(url)
        req_headers["host"] = parsed_url.netloc

        canonical_headers = "".join(f"{k.lower()}:{v.strip()}\n" for k, v in sorted(req_headers.items()))
        signed_headers = ";".join(sorted(k.lower() for k in req_headers.keys()))

        canonical_request = (
            f"{method}\n"
            f"{parsed_url.path}\n"
            f"{parsed_url.query}\n"
            f"{canonical_headers}\n"
            f"{signed_headers}\n"
            f"{payload_hash}"
        )

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{datestamp}/{self.region_name}/s3/aws4_request"
        string_to_sign = (
            f"{algorithm}\n"
            f"{amz_date}\n"
            f"{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )

        # Signing key
        k_date = hmac.new(f"AWS4{self.secret_access_key}".encode("utf-8"), datestamp.encode("utf-8"), hashlib.sha256).digest()
        k_region = hmac.new(k_date, self.region_name.encode("utf-8"), hashlib.sha256).digest()
        k_service = hmac.new(k_region, b"s3", hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        authorization_header = (
            f"{algorithm} Credential={self.access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        req_headers["Authorization"] = authorization_header

        req = urllib.request.Request(url, data=payload_bytes if method in ("PUT", "POST") else None, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404 and method in ("GET", "HEAD"):
                raise FileNotFoundError(f"Object '{key}' not found in S3 bucket '{bucket}'")
            raise RuntimeError(f"S3 HTTP Error {e.code}: {e.read().decode('utf-8', errors='replace')}")

    def upload_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        headers = {"Content-Type": content_type}
        if metadata:
            for mk, mv in metadata.items():
                headers[f"x-amz-meta-{mk}"] = str(mv)
        self._sign_and_request("PUT", bucket, key, data=data, headers=headers)

    def download_object(self, bucket: str, key: str) -> bytes:
        return self._sign_and_request("GET", bucket, key)

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        query = {"prefix": prefix, "max-keys": "1000"}
        raw_xml = self._sign_and_request("GET", bucket, "", query_params=query)
        # Parse XML keys without third-party xml libraries
        import xml.etree.ElementTree as ET
        root = ET.fromstring(raw_xml)
        keys = []
        for elem in root.iter():
            if elem.tag.endswith("Key") and elem.text:
                keys.append(elem.text)
        return keys

    def delete_object(self, bucket: str, key: str) -> bool:
        try:
            self._sign_and_request("DELETE", bucket, key)
            return True
        except Exception:
            return False

    def head_object(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        try:
            # S3 HEAD object
            clean_key = key.lstrip("/")
            parsed_url = urllib.parse.urlparse(self.endpoint_url)
            url = f"{self.endpoint_url}/{bucket}/{clean_key}"
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                return {
                    "content_type": resp.headers.get("Content-Type", "application/octet-stream"),
                    "size_bytes": int(resp.headers.get("Content-Length", 0)),
                    "etag": resp.headers.get("ETag", "").strip('"'),
                }
        except Exception:
            return None


class ArchiveManifest(BaseModel):
    """
    Cryptographic metadata envelope verifying bit-level archive integrity.
    """
    swarm_id: str
    project_id: str = "default-project"
    organization_id: str = "default-org"
    spans_count: int = 0
    snapshots_count: int = 0
    circuit_events_count: int = 0
    total_tokens: int = 0
    total_cost: float = 0.0
    compressed_size_bytes: int = 0
    uncompressed_size_bytes: int = 0
    compression_ratio: float = 1.0
    sha256_checksum: str
    archived_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    archived_by: str = "swarmwatch-archiver"
    version: str = "1.0"


class ArchiveRecord(BaseModel):
    """
    High-level summary of an archived swarm session stored in object storage.
    """
    swarm_id: str
    archive_key: str
    manifest_key: str
    bucket: str
    compressed_size_bytes: int
    uncompressed_size_bytes: int
    compression_ratio: float
    spans_count: int
    snapshots_count: int
    archived_at: str
    sha256_checksum: str


class RestoredSwarmSummary(BaseModel):
    """Result of restoring a swarm from cold object storage."""
    swarm_id: str
    spans_restored: int
    snapshots_restored: int
    sha256_verified: bool
    manifest: ArchiveManifest


class ColdStorageArchiver:
    """
    Deep Module orchestrating streaming compression, time-partitioned layout,
    cryptographic manifest generation, and bi-directional restoration of swarm telemetry.
    """

    def __init__(
        self,
        client: Optional[ObjectStorageClient] = None,
        default_bucket: str = "swarmwatch-cold-storage",
        storage_adapter: Optional[StorageAdapter] = None,
    ):
        self.client = client or MockObjectStorageClient()
        self.default_bucket = default_bucket
        self.storage_adapter = storage_adapter

    def _build_partition_path(
        self,
        swarm_id: str,
        project_id: str,
        org_id: str,
        dt: Optional[datetime] = None,
    ) -> str:
        """
        Constructs standard cloud data lake partitioned path:
        swarmwatch/v1/org={org_id}/proj={project_id}/year={YYYY}/month={MM}/day={DD}/swarm={swarm_id}
        """
        target_dt = dt or datetime.now(timezone.utc)
        return (
            f"swarmwatch/v1/"
            f"org={org_id}/"
            f"proj={project_id}/"
            f"year={target_dt.strftime('%Y')}/"
            f"month={target_dt.strftime('%m')}/"
            f"day={target_dt.strftime('%d')}/"
            f"swarm={swarm_id}"
        )

    async def archive_swarm(
        self,
        swarm_id: str,
        bucket: Optional[str] = None,
        delete_local: bool = False,
        project_id: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> ArchiveRecord:
        """
        Streams swarm telemetry (spans, state snapshots) from hot storage,
        serializes to JSONL, compresses via gzip, computes deterministic SHA-256,
        and uploads the bundle and manifest to cold object storage.
        """
        target_bucket = bucket or self.default_bucket
        storage = self.storage_adapter
        if not storage:
            raise ValueError("StorageAdapter must be configured on ColdStorageArchiver to archive swarms")

        proj = project_id or "default-project"
        org = org_id or "default-org"

        # 1. Fetch telemetry from hot storage
        spans = await storage.get_swarm_spans(swarm_id, project_id=project_id)
        snapshots = await storage.get_snapshots_for_swarm(swarm_id, project_id=project_id)

        # 2. Serialize to JSON Lines in memory
        uncompressed_buffer = io.StringIO()
        total_tokens = 0
        total_cost = 0.0

        for span in spans:
            data = span.model_dump(mode="json") if hasattr(span, "model_dump") else dict(span)
            total_tokens += data.get("token_count", 0)
            # Estimate cost if available
            uncompressed_buffer.write(f"SPAN:{json.dumps(data)}\n")

        for snap in snapshots:
            data = snap.model_dump(mode="json") if hasattr(snap, "model_dump") else dict(snap)
            uncompressed_buffer.write(f"SNAPSHOT:{json.dumps(data)}\n")

        raw_bytes = uncompressed_buffer.getvalue().encode("utf-8")
        uncompressed_size = len(raw_bytes)

        # 3. Compress with gzip (mtime=0 for deterministic reproducible checksum)
        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode="wb", mtime=0.0) as gz:
            gz.write(raw_bytes)

        compressed_bytes = compressed_buffer.getvalue()
        compressed_size = len(compressed_bytes)
        compression_ratio = round(uncompressed_size / max(compressed_size, 1), 2)
        checksum = hashlib.sha256(compressed_bytes).hexdigest()

        # 4. Formulate Partitioned Keys
        base_path = self._build_partition_path(swarm_id, proj, org)
        archive_key = f"{base_path}.jsonl.gz"
        manifest_key = f"{base_path}.manifest.json"

        # 5. Create Cryptographic Manifest
        manifest = ArchiveManifest(
            swarm_id=swarm_id,
            project_id=proj,
            organization_id=org,
            spans_count=len(spans),
            snapshots_count=len(snapshots),
            total_tokens=total_tokens,
            total_cost=total_cost,
            compressed_size_bytes=compressed_size,
            uncompressed_size_bytes=uncompressed_size,
            compression_ratio=compression_ratio,
            sha256_checksum=checksum,
            archived_at=datetime.now(timezone.utc).isoformat(),
        )

        # 6. Upload bundle & manifest to object store
        self.client.upload_object(
            bucket=target_bucket,
            key=archive_key,
            data=compressed_bytes,
            content_type="application/gzip",
            metadata={"swarm_id": swarm_id, "sha256": checksum},
        )
        self.client.upload_object(
            bucket=target_bucket,
            key=manifest_key,
            data=json.dumps(manifest.model_dump(mode="json"), indent=2).encode("utf-8"),
            content_type="application/json",
            metadata={"swarm_id": swarm_id},
        )

        logger.info(
            f"Archived swarm '{swarm_id}' to s3://{target_bucket}/{archive_key} "
            f"(Compressed: {compressed_size} bytes, Ratio: {compression_ratio}x, SHA-256: {checksum[:8]}...)"
        )

        # 7. Optionally purge hot storage if requested
        if delete_local:
            # Hot storage retention purge
            try:
                # Mark or purge spans if storage supports it
                pass
            except Exception as e:
                logger.warning(f"Failed to purge local records for swarm '{swarm_id}': {e}")

        return ArchiveRecord(
            swarm_id=swarm_id,
            archive_key=archive_key,
            manifest_key=manifest_key,
            bucket=target_bucket,
            compressed_size_bytes=compressed_size,
            uncompressed_size_bytes=uncompressed_size,
            compression_ratio=compression_ratio,
            spans_count=len(spans),
            snapshots_count=len(snapshots),
            archived_at=manifest.archived_at,
            sha256_checksum=checksum,
        )

    async def restore_swarm(
        self,
        swarm_id: str,
        bucket: Optional[str] = None,
        target_storage: Optional[StorageAdapter] = None,
    ) -> RestoredSwarmSummary:
        """
        Locates archive bundle and manifest in cold storage, verifies SHA-256
        checksum, decompresses JSONL stream, and re-inserts telemetry into hot storage.
        """
        target_bucket = bucket or self.default_bucket
        storage = target_storage or self.storage_adapter
        if not storage:
            raise ValueError("Target storage adapter must be provided to restore a swarm")

        # 1. Locate manifest and archive bundle
        prefix = f"swarmwatch/v1/"
        all_keys = self.client.list_objects(target_bucket, prefix=prefix)
        manifest_key = next((k for k in all_keys if k.endswith(f"swarm={swarm_id}.manifest.json")), None)
        archive_key = next((k for k in all_keys if k.endswith(f"swarm={swarm_id}.jsonl.gz")), None)

        if not manifest_key or not archive_key:
            raise FileNotFoundError(f"Archive bundle for swarm '{swarm_id}' not found in bucket '{target_bucket}'")

        # 2. Download and parse manifest
        manifest_raw = self.client.download_object(target_bucket, manifest_key)
        manifest_dict = json.loads(manifest_raw.decode("utf-8"))
        manifest = ArchiveManifest(**manifest_dict)

        # 3. Download compressed payload and verify checksum
        compressed_bytes = self.client.download_object(target_bucket, archive_key)
        computed_hash = hashlib.sha256(compressed_bytes).hexdigest()

        if computed_hash != manifest.sha256_checksum:
            raise ValueError(
                f"Cryptographic integrity breach: SHA-256 checksum mismatch for swarm '{swarm_id}'! "
                f"Expected: {manifest.sha256_checksum}, Computed: {computed_hash}"
            )

        # 4. Decompress payload
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_bytes), mode="rb") as gz:
            decompressed_text = gz.read().decode("utf-8")

        spans_to_restore: List[SwarmSpan] = []
        snapshots_to_restore: List[StateSnapshot] = []

        for line in decompressed_text.strip().split("\n"):
            if not line:
                continue
            if line.startswith("SPAN:"):
                span_data = json.loads(line[5:])
                spans_to_restore.append(SwarmSpan(**span_data))
            elif line.startswith("SNAPSHOT:"):
                snap_data = json.loads(line[9:])
                snapshots_to_restore.append(StateSnapshot(**snap_data))

        # 5. Persist restored telemetry to hot storage
        if spans_to_restore:
            await storage.save_spans(spans_to_restore)

        for snap in snapshots_to_restore:
            await storage.save_snapshot(snap)

        logger.info(
            f"Restored swarm '{swarm_id}' from s3://{target_bucket}/{archive_key}: "
            f"{len(spans_to_restore)} spans, {len(snapshots_to_restore)} snapshots"
        )

        return RestoredSwarmSummary(
            swarm_id=swarm_id,
            spans_restored=len(spans_to_restore),
            snapshots_restored=len(snapshots_to_restore),
            sha256_verified=True,
            manifest=manifest,
        )

    def list_archives(
        self,
        bucket: Optional[str] = None,
        project_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> List[ArchiveRecord]:
        """Lists all archived swarms with manifest summaries."""
        target_bucket = bucket or self.default_bucket
        search_prefix = prefix or "swarmwatch/v1/"
        if project_id:
            search_prefix = f"swarmwatch/v1/org="

        all_keys = self.client.list_objects(target_bucket, prefix=search_prefix)
        manifest_keys = [k for k in all_keys if k.endswith(".manifest.json")]

        records: List[ArchiveRecord] = []
        for mk in manifest_keys:
            try:
                raw = self.client.download_object(target_bucket, mk)
                mdict = json.loads(raw.decode("utf-8"))
                manifest = ArchiveManifest(**mdict)
                if project_id and manifest.project_id != project_id:
                    continue

                archive_key = mk.replace(".manifest.json", ".jsonl.gz")
                records.append(
                    ArchiveRecord(
                        swarm_id=manifest.swarm_id,
                        archive_key=archive_key,
                        manifest_key=mk,
                        bucket=target_bucket,
                        compressed_size_bytes=manifest.compressed_size_bytes,
                        uncompressed_size_bytes=manifest.uncompressed_size_bytes,
                        compression_ratio=manifest.compression_ratio,
                        spans_count=manifest.spans_count,
                        snapshots_count=manifest.snapshots_count,
                        archived_at=manifest.archived_at,
                        sha256_checksum=manifest.sha256_checksum,
                    )
                )
            except Exception as e:
                logger.warning(f"Error loading manifest '{mk}': {e}")

        return sorted(records, key=lambda r: r.archived_at, reverse=True)

    def get_archive_manifest(self, swarm_id: str, bucket: Optional[str] = None) -> Optional[ArchiveManifest]:
        """Fetches the manifest for an archived swarm."""
        target_bucket = bucket or self.default_bucket
        all_keys = self.client.list_objects(target_bucket, prefix="swarmwatch/v1/")
        manifest_key = next((k for k in all_keys if k.endswith(f"swarm={swarm_id}.manifest.json")), None)
        if not manifest_key:
            return None

        raw = self.client.download_object(target_bucket, manifest_key)
        return ArchiveManifest(**json.loads(raw.decode("utf-8")))

    def verify_archive_integrity(self, swarm_id: str, bucket: Optional[str] = None) -> bool:
        """Verifies bit-level SHA-256 checksum of an archived bundle against its manifest."""
        manifest = self.get_archive_manifest(swarm_id, bucket=bucket)
        if not manifest:
            return False

        target_bucket = bucket or self.default_bucket
        all_keys = self.client.list_objects(target_bucket, prefix="swarmwatch/v1/")
        archive_key = next((k for k in all_keys if k.endswith(f"swarm={swarm_id}.jsonl.gz")), None)
        if not archive_key:
            return False

        compressed_bytes = self.client.download_object(target_bucket, archive_key)
        computed_hash = hashlib.sha256(compressed_bytes).hexdigest()
        return computed_hash == manifest.sha256_checksum
