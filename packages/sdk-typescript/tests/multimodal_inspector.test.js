import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  MediaModality,
  MultiModalInspector,
  globalMultiModalInspector,
} from "../src/multimodal.ts";

describe("Seam 44: Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking", () => {
  test("inspects PNG binary header and computes deterministic perceptual hash", () => {
    const inspector = new MultiModalInspector();
    // 800x600 PNG
    const buf = Buffer.alloc(32);
    buf[0] = 0x89;
    buf[1] = 0x50;
    buf[2] = 0x4e;
    buf[3] = 0x47;
    buf.writeUInt32BE(800, 16);
    buf.writeUInt32BE(600, 20);

    const meta = inspector.inspectBytes(buf, "diagram.png");
    assert.equal(meta.modality, MediaModality.IMAGE);
    assert.equal(meta.mimeType, "image/png");
    assert.deepEqual(meta.dimensions, [800, 600]);
    assert.equal(typeof meta.perceptualHash, "string");
    assert.equal(meta.perceptualHash.length, 16);
  });

  test("inspects GIF and WAV audio headers", () => {
    const inspector = new MultiModalInspector();

    // GIF (320x240)
    const gif = Buffer.alloc(16);
    gif.write("GIF89a", 0);
    gif.writeUInt16LE(320, 6);
    gif.writeUInt16LE(240, 8);
    const metaGif = inspector.inspectBytes(gif, "anim.gif");
    assert.equal(metaGif.modality, MediaModality.IMAGE);
    assert.equal(metaGif.mimeType, "image/gif");
    assert.deepEqual(metaGif.dimensions, [320, 240]);

    // WAV (2 channels, 44100Hz, 176400 B/s, 1.0s duration)
    const wav = Buffer.alloc(64);
    wav.write("RIFF", 0);
    wav.write("WAVE", 8);
    wav.write("fmt ", 12);
    wav.writeUInt16LE(1, 20); // audio format (1 = PCM)
    wav.writeUInt16LE(2, 22); // channels = 2
    wav.writeUInt32LE(44100, 24); // sample rate
    wav.writeUInt32LE(176400, 28); // byte rate
    wav.write("data", 36);
    wav.writeUInt32LE(176400, 40); // data size
    const metaWav = inspector.inspectBytes(wav, "audio.wav");
    assert.equal(metaWav.modality, MediaModality.AUDIO);
    assert.equal(metaWav.mimeType, "audio/wav");
    assert.equal(metaWav.channels, 2);
    assert.equal(metaWav.sampleRate, 44100);
    assert.equal(metaWav.durationSeconds, 1.0);
  });

  test("creates cryptographic watermark and detects tampering", () => {
    const inspector = new MultiModalInspector();
    const payload = Buffer.from("Synthetic image artifact buffer payload for testing");
    const artifact = inspector.inspectAndWatermark(
      payload,
      "agent-vision",
      "swarm-live-01",
      "chart.png",
      "art-test-01",
      "my_secret_salt_key",
      "tenant-prod"
    );

    assert.equal(artifact.artifactId, "art-test-01");
    assert.equal(artifact.watermark.producerAgentId, "agent-vision");
    assert.equal(artifact.watermark.swarmId, "swarm-live-01");
    assert.equal(artifact.watermark.tenantId, "tenant-prod");

    // 1. Untampered payload passes verification
    const isValid = inspector.verifyWatermark(artifact.watermark, payload, "my_secret_salt_key");
    assert.equal(isValid, true);

    // 2. Tampered payload fails verification
    const tamperedPayload = Buffer.from(payload);
    tamperedPayload[0] ^= 0xff;
    const isTamperedValid = inspector.verifyWatermark(artifact.watermark, tamperedPayload, "my_secret_salt_key");
    assert.equal(isTamperedValid, false);

    // 3. Invalid secret fails verification
    const isWrongSecretValid = inspector.verifyWatermark(artifact.watermark, payload, "wrong_secret");
    assert.equal(isWrongSecretValid, false);
  });

  test("manages artifact registry and lifecycle per swarm", () => {
    const inspector = new MultiModalInspector();
    inspector.reset();

    const p1 = Buffer.from("Artifact payload 1");
    const p2 = Buffer.from("Artifact payload 2");

    const a1 = inspector.inspectAndWatermark(p1, "agent-01", "swarm-ts-01");
    const a2 = inspector.inspectAndWatermark(p2, "agent-02", "swarm-ts-01");

    const list = inspector.getArtifacts("swarm-ts-01");
    assert.equal(list.length, 2);
    assert.deepEqual(inspector.getArtifact("swarm-ts-01", a1.artifactId), a1);

    inspector.reset("swarm-ts-01");
    assert.equal(inspector.getArtifacts("swarm-ts-01").length, 0);
  });
});
