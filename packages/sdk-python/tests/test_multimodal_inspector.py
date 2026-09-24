"""Tests for Seam 44 / ADR 0055: Multi-Modal Audio/Visual Telemetry Inspection & Artifact Watermarking."""

import base64
import struct
import pytest
from fastapi.testclient import TestClient

from swarmwatch.multimodal import (
    MediaModality,
    MediaMetadata,
    ArtifactWatermark,
    WatermarkedArtifact,
    MultiModalInspector,
    global_multimodal_inspector,
)
from swarmwatch.server import create_app


def test_png_header_inspection():
    inspector = MultiModalInspector()
    # 800x600 PNG
    png_bytes = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
        + struct.pack(">II", 800, 600)
        + b"\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    meta = inspector.inspect_bytes(png_bytes, filename="chart.png")
    assert meta.modality == MediaModality.IMAGE
    assert meta.mime_type == "image/png"
    assert meta.dimensions == (800, 600)
    assert meta.perceptual_hash is not None
    assert len(meta.perceptual_hash) == 16
    assert len(meta.sha256_checksum) == 64


def test_gif_header_inspection():
    inspector = MultiModalInspector()
    # 320x240 GIF
    gif_bytes = b"GIF89a" + struct.pack("<HH", 320, 240) + b"\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x00\x00\x00\x00\x00,\x00\x00\x00\x00"
    meta = inspector.inspect_bytes(gif_bytes, filename="anim.gif")
    assert meta.modality == MediaModality.IMAGE
    assert meta.mime_type == "image/gif"
    assert meta.dimensions == (320, 240)
    assert meta.perceptual_hash is not None


def test_wav_audio_inspection():
    inspector = MultiModalInspector()
    # Stereo 44.1kHz 16-bit PCM WAV, 1.0 second duration (176400 bytes data)
    byte_rate = 176400
    data_size = 176400
    wav_bytes = (
        b"RIFF"
        + struct.pack("<I", 36 + data_size)
        + b"WAVEfmt \x10\x00\x00\x00\x01\x00"
        + struct.pack("<HIIH", 2, 44100, byte_rate, 4)
        + b"\x10\x00data"
        + struct.pack("<I", data_size)
        + b"\x00" * 256
    )
    meta = inspector.inspect_bytes(wav_bytes, filename="speech.wav")
    assert meta.modality == MediaModality.AUDIO
    assert meta.mime_type == "audio/wav"
    assert meta.channels == 2
    assert meta.sample_rate == 44100
    assert meta.duration_seconds == 1.0


def test_pdf_and_mp4_inspection():
    inspector = MultiModalInspector()
    pdf_bytes = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n"
    meta_pdf = inspector.inspect_bytes(pdf_bytes, filename="report.pdf")
    assert meta_pdf.modality == MediaModality.DOCUMENT
    assert meta_pdf.mime_type == "application/pdf"

    mp4_bytes = b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00isommp42\x00\x00\x00\x08free"
    meta_mp4 = inspector.inspect_bytes(mp4_bytes, filename="render.mp4")
    assert meta_mp4.modality == MediaModality.VIDEO
    assert meta_mp4.mime_type == "video/mp4"


def test_cryptographic_watermark_and_tamper_detection():
    inspector = MultiModalInspector()
    payload = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + struct.pack(">II", 100, 100) + b"\x08\x06\x00\x00\x00\x00"
    watermarked = inspector.inspect_and_watermark(
        payload=payload,
        producer_agent_id="agent-designer",
        swarm_id="swarm-test-01",
        tenant_secret="super_secret_salt_123",
        tenant_id="tenant-alpha",
    )

    assert watermarked.artifact_id.startswith("art-")
    assert watermarked.watermark.producer_agent_id == "agent-designer"
    assert watermarked.watermark.swarm_id == "swarm-test-01"
    assert watermarked.watermark.tenant_id == "tenant-alpha"
    assert not watermarked.watermark.tamper_detected

    # 1. Untampered verification succeeds
    assert inspector.verify_watermark(
        watermark=watermarked.watermark,
        payload=payload,
        tenant_secret="super_secret_salt_123",
    )

    # 2. Modified payload fails verification (tamper detected)
    tampered_payload = bytearray(payload)
    tampered_payload[10] ^= 0xFF
    assert not inspector.verify_watermark(
        watermark=watermarked.watermark,
        payload=bytes(tampered_payload),
        tenant_secret="super_secret_salt_123",
    )

    # 3. Wrong tenant secret fails verification
    assert not inspector.verify_watermark(
        watermark=watermarked.watermark,
        payload=payload,
        tenant_secret="wrong_secret",
    )


def test_registry_and_reset():
    inspector = MultiModalInspector()
    inspector.reset()

    p1 = b"Sample byte payload 1"
    p2 = b"Sample byte payload 2"

    a1 = inspector.inspect_and_watermark(p1, producer_agent_id="agent-1", swarm_id="swarm-reg-01")
    a2 = inspector.inspect_and_watermark(p2, producer_agent_id="agent-2", swarm_id="swarm-reg-01")

    arts = inspector.get_artifacts("swarm-reg-01")
    assert len(arts) == 2
    assert inspector.get_artifact("swarm-reg-01", a1.artifact_id) == a1

    inspector.reset("swarm-reg-01")
    assert len(inspector.get_artifacts("swarm-reg-01")) == 0


def test_server_artifact_endpoints():
    app = create_app()
    client = TestClient(app)

    payload_bytes = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
        + struct.pack(">II", 400, 300)
        + b"\x08\x06\x00\x00\x00\x1f\x15c4"
    )
    b64 = base64.b64encode(payload_bytes).decode("ascii")

    # 1. Inspect endpoint
    resp = client.post(
        "/api/v1/swarms/swarm-srv-01/artifacts/inspect",
        json={"payload_base64": b64, "filename": "diagram.png"},
    )
    assert resp.status_code == 200
    meta_data = resp.json()["metadata"]
    assert meta_data["modality"] == "IMAGE"
    assert meta_data["mime_type"] == "image/png"
    assert meta_data["dimensions"] == [400, 300]

    # 2. Watermark endpoint
    resp_wm = client.post(
        "/api/v1/swarms/swarm-srv-01/artifacts/watermark",
        json={
            "payload_base64": b64,
            "producer_agent_id": "agent-vision",
            "filename": "diagram.png",
            "artifact_id": "art-srv-test-01",
        },
    )
    assert resp_wm.status_code == 200
    art_data = resp_wm.json()["artifact"]
    assert art_data["artifact_id"] == "art-srv-test-01"

    # 3. Get artifacts endpoint
    resp_list = client.get("/api/v1/swarms/swarm-srv-01/artifacts")
    assert resp_list.status_code == 200
    assert resp_list.json()["artifacts_count"] >= 1

    # 4. Verify endpoint (valid)
    resp_ver = client.post(
        "/api/v1/swarms/swarm-srv-01/artifacts/verify",
        json={
            "artifact_id": "art-srv-test-01",
            "payload_base64": b64,
        },
    )
    assert resp_ver.status_code == 200
    assert resp_ver.json()["verified"] is True

    # 5. Verify endpoint (tampered)
    tampered_b64 = base64.b64encode(payload_bytes + b"\x00").decode("ascii")
    resp_tamper = client.post(
        "/api/v1/swarms/swarm-srv-01/artifacts/verify",
        json={
            "artifact_id": "art-srv-test-01",
            "payload_base64": tampered_b64,
        },
    )
    assert resp_tamper.status_code == 200
    assert resp_tamper.json()["verified"] is False
    assert resp_tamper.json()["tamper_detected"] is True


def test_cli_artifact_subcommands(capsys):
    from swarmwatch.cli import main

    # 1. CLI Inspect
    with pytest.raises(SystemExit) as exc:
        main(["artifact", "inspect", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert '"modality": "IMAGE"' in captured.out

    # 2. CLI Watermark
    with pytest.raises(SystemExit) as exc:
        main(["artifact", "watermark", "--swarm-id", "cli-swarm-01", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert '"status": "watermarked"' in captured.out

    # 3. CLI Status
    with pytest.raises(SystemExit) as exc:
        main(["artifact", "status", "--swarm-id", "cli-swarm-01"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "SWARMWATCH ARTIFACTS REGISTRY" in captured.out
