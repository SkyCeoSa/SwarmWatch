import { test } from "node:test";
import assert from "node:assert";
import {
  MockObjectStorageClient,
  ColdStorageArchiver,
} from "../src/archival.ts";

test("S3 / GCS Cold Storage Archival Suite (TypeScript / ADR 0036)", async (t) => {
  const mockClient = new MockObjectStorageClient();
  const archiver = new ColdStorageArchiver({
    client: mockClient,
    defaultBucket: "test-cold-bucket",
  });

  const sampleSpans = [
    {
      id: "span-01",
      swarm_id: "swarm-cold-01",
      source_agent: "Architect",
      target_agent: "Developer",
      token_count: 520,
      entropy_score: 1.84,
      start_time: new Date().toISOString(),
      duration_ms: 120.5,
    },
    {
      id: "span-02",
      swarm_id: "swarm-cold-01",
      source_agent: "Developer",
      target_agent: "Tester",
      token_count: 840,
      entropy_score: 1.76,
      start_time: new Date().toISOString(),
      duration_ms: 240.2,
    },
  ];

  const sampleSnapshots = [
    {
      span_id: "span-01",
      swarm_id: "swarm-cold-01",
      state_data: { status: "spec_ready", version: 1 },
      state_hash: "hash-01",
    },
    {
      span_id: "span-02",
      swarm_id: "swarm-cold-01",
      state_data: { status: "code_built", version: 2 },
      state_hash: "hash-02",
    },
  ];

  await t.test("MockObjectStorageClient primitives: upload, download, list, head, delete", async () => {
    await mockClient.uploadObject("test-bucket", "test/file.txt", Buffer.from("hello world", "utf-8"), "text/plain");

    const dl = await mockClient.downloadObject("test-bucket", "test/file.txt");
    assert.strictEqual(dl.toString("utf-8"), "hello world");

    const head = await mockClient.headObject("test-bucket", "test/file.txt");
    assert.ok(head);
    assert.strictEqual(head.contentType, "text/plain");
    assert.strictEqual(head.sizeBytes, 11);

    const list = await mockClient.listObjects("test-bucket", "test/");
    assert.strictEqual(list.length, 1);
    assert.strictEqual(list[0], "test/file.txt");

    const deleted = await mockClient.deleteObject("test-bucket", "test/file.txt");
    assert.strictEqual(deleted, true);

    const listAfter = await mockClient.listObjects("test-bucket", "test/");
    assert.strictEqual(listAfter.length, 0);
  });

  let archiveRecord;

  await t.test("ColdStorageArchiver archives swarm with gzip compression and SHA-256 manifest", async () => {
    archiveRecord = await archiver.archiveSwarm("swarm-cold-01", {
      bucket: "test-cold-bucket",
      projectId: "proj-alpha",
      orgId: "org-finance",
      spans: sampleSpans,
      snapshots: sampleSnapshots,
    });

    assert.strictEqual(archiveRecord.swarm_id, "swarm-cold-01");
    assert.strictEqual(archiveRecord.spans_count, 2);
    assert.strictEqual(archiveRecord.snapshots_count, 2);
    assert.ok(archiveRecord.archive_key.includes("swarm=swarm-cold-01.jsonl.gz"));
    assert.ok(archiveRecord.manifest_key.includes("swarm=swarm-cold-01.manifest.json"));
    assert.ok(archiveRecord.sha256_checksum.length === 64);
    assert.ok(archiveRecord.compressed_size_bytes > 0);
    assert.ok(archiveRecord.uncompressed_size_bytes > archiveRecord.compressed_size_bytes);
    assert.ok(archiveRecord.compression_ratio >= 1.0);
  });

  await t.test("ColdStorageArchiver retrieves manifest and verifies bit-level integrity", async () => {
    const manifest = await archiver.getArchiveManifest("swarm-cold-01", "test-cold-bucket");
    assert.ok(manifest);
    assert.strictEqual(manifest.swarm_id, "swarm-cold-01");
    assert.strictEqual(manifest.project_id, "proj-alpha");
    assert.strictEqual(manifest.spans_count, 2);
    assert.strictEqual(manifest.total_tokens, 1360);
    assert.strictEqual(manifest.sha256_checksum, archiveRecord.sha256_checksum);

    const isValid = await archiver.verifyArchiveIntegrity("swarm-cold-01", "test-cold-bucket");
    assert.strictEqual(isValid, true);
  });

  await t.test("ColdStorageArchiver lists archived swarms across project partitions", async () => {
    const archives = await archiver.listArchives({
      bucket: "test-cold-bucket",
      projectId: "proj-alpha",
    });

    assert.ok(archives.length >= 1);
    assert.strictEqual(archives[0].swarm_id, "swarm-cold-01");
    assert.strictEqual(archives[0].spans_count, 2);
  });

  await t.test("ColdStorageArchiver restores archived swarm with round-trip verification", async () => {
    let restoredSpans = [];
    let restoredSnapshots = [];

    const summary = await archiver.restoreSwarm("swarm-cold-01", {
      bucket: "test-cold-bucket",
      onRestoreSpans: async (spans) => {
        restoredSpans = spans;
      },
      onRestoreSnapshots: async (snapshots) => {
        restoredSnapshots = snapshots;
      },
    });

    assert.strictEqual(summary.swarm_id, "swarm-cold-01");
    assert.strictEqual(summary.spans_restored, 2);
    assert.strictEqual(summary.snapshots_restored, 2);
    assert.strictEqual(summary.sha256_verified, true);
    assert.strictEqual(restoredSpans.length, 2);
    assert.strictEqual(restoredSpans[0].id, "span-01");
    assert.strictEqual(restoredSnapshots.length, 2);
    assert.strictEqual(restoredSnapshots[0].span_id, "span-01");
  });

  await t.test("ColdStorageArchiver detects cryptographic checksum tampering and halts restore", async () => {
    // Tamper with archive bundle bytes
    await mockClient.uploadObject("test-cold-bucket", archiveRecord.archive_key, Buffer.from("corrupted payload"));

    await assert.rejects(
      async () => {
        await archiver.restoreSwarm("swarm-cold-01", {
          bucket: "test-cold-bucket",
        });
      },
      /Cryptographic integrity breach: SHA-256 checksum mismatch/
    );
  });
});
