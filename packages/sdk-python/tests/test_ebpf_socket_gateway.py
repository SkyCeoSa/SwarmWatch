# -*- coding: utf-8 -*-
"""
Tests for SwarmWatch Kernel-Level Network Socket LLM Interception Gateway (Seam 29 / ADR 0040).
"""

import json
import os
import socket
import tempfile
import pytest
from starlette.testclient import TestClient

from swarmwatch.ebpf import (
    calculate_shannon_entropy,
    SocketRule,
    SocketIntervention,
    SocketStreamDecoder,
    TcpRstEmitter,
    EbpfSocketGateway,
    MockEbpfEngine,
    global_ebpf_gateway,
)
from swarmwatch.storage import SQLiteStorageAdapter
from swarmwatch.server import create_app
from swarmwatch.cli import main as cli_main


def test_shannon_entropy_calculation():
    """Verify Shannon entropy accurately detects repetitive text degradation."""
    natural = "SwarmWatch monitors multi-agent autonomous swarms and detects conversational entropy loops."
    h_nat = calculate_shannon_entropy(natural)
    assert h_nat > 0.45

    degenerate = "repeat " * 50
    h_deg = calculate_shannon_entropy(degenerate)
    assert h_deg < h_nat
    assert h_deg < 0.35

    all_same = "A" * 100
    h_same = calculate_shannon_entropy(all_same)
    assert h_same < 0.10


def test_socket_stream_decoder_http_json():
    """Verify SocketStreamDecoder extracts HTTP headers and OpenAI JSON messages."""
    decoder = SocketStreamDecoder()
    body = json.dumps({"model": "gpt-4o", "messages": [{"role": "user", "content": "How do black holes form?"}]})
    raw = (
        f"POST /v1/chat/completions HTTP/1.1\r\n"
        f"Host: api.openai.com\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
        f"{body}"
    ).encode("utf-8")

    decoder.feed(raw)

    assert decoder.method == "POST"
    assert decoder.path == "/v1/chat/completions"
    assert decoder.headers.get("host") == "api.openai.com"
    assert decoder.extracted_text == "How do black holes form?"
    assert decoder.estimated_token_count >= 5
    assert decoder.entropy > 0.40


def test_socket_stream_decoder_sse_stream():
    """Verify SocketStreamDecoder parses Server-Sent Events (SSE) token chunks."""
    decoder = SocketStreamDecoder()
    headers = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/event-stream\r\n"
        b"Transfer-Encoding: chunked\r\n\r\n"
    )
    decoder.feed(headers)

    chunk1 = b'data: {"choices":[{"delta":{"content":"Deep "}}]}\n\n'
    chunk2 = b'data: {"choices":[{"delta":{"content":"space."}}]}\n\n'
    chunk3 = b'data: [DONE]\n\n'

    decoder.feed(chunk1 + chunk2 + chunk3)

    assert decoder.streamed_tokens == ["Deep ", "space."]
    assert decoder.extracted_text == "Deep space."
    assert decoder.estimated_token_count >= 2


def test_mock_ebpf_engine_clean_request():
    """Verify clean LLM requests are forwarded without socket termination."""
    engine = MockEbpfEngine()
    terminated, intervention, decoder = engine.simulate_http_request(
        method="POST",
        path="/v1/chat/completions",
        host="api.openai.com",
        port=443,
        json_body={"messages": [{"role": "user", "content": "Tell me a story about exploration."}]},
    )

    assert not terminated
    assert intervention is None
    assert decoder.estimated_token_count > 0


def test_mock_ebpf_engine_entropy_collapse_rst():
    """Verify repetitive low-entropy prompt triggers physical TCP RST intervention."""
    engine = MockEbpfEngine()
    degenerate_prompt = "loop " * 40
    terminated, intervention, decoder = engine.simulate_http_request(
        method="POST",
        path="/v1/chat/completions",
        host="api.openai.com",
        port=443,
        json_body={"messages": [{"role": "user", "content": degenerate_prompt}]},
    )

    assert terminated
    assert intervention is not None
    assert intervention.action == "TCP_RST"
    assert "entropy collapse" in intervention.reason.lower()
    assert decoder.entropy < 0.35


def test_mock_ebpf_engine_token_budget_quota_breach():
    """Verify token ceiling breach triggers TCP RST intervention."""
    gateway = EbpfSocketGateway()
    gateway.add_rule(
        SocketRule(
            id="rule-token-limit",
            name="Strict 10 Token Limit",
            ports=[443],
            domains=["api.openai.com"],
            action="inspect",
            max_tokens_per_stream=10,
        )
    )
    engine = MockEbpfEngine(gateway)

    long_prompt = "The universe is full of magical things patiently waiting for our wits to grow sharper and deeper."
    terminated, intervention, decoder = engine.simulate_http_request(
        method="POST",
        path="/v1/chat/completions",
        host="api.openai.com",
        port=443,
        json_body={"messages": [{"role": "user", "content": long_prompt}]},
    )

    assert terminated
    assert intervention is not None
    assert "token stream quota exceeded" in intervention.reason.lower()


def test_tcp_rst_emitter_socket_abort():
    """Verify TcpRstEmitter aborts socket without raising unhandled exceptions."""
    s1, s2 = socket.socketpair()
    try:
        TcpRstEmitter.abort_socket(s1)
    finally:
        s2.close()


def test_fastapi_ebpf_endpoints():
    """Verify FastAPI REST control plane endpoints for eBPF gateway."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    storage = SQLiteStorageAdapter(db_path=db_path)
    try:
        app = create_app(storage=storage)
        client = TestClient(app)

        # 1. GET /api/v1/ebpf/status
        resp = client.get("/api/v1/ebpf/status")
        assert resp.status_code == 200
        st = resp.json()
        assert st["status"] == "attached"
        assert st["mode"] == "ebpf_kernel_socket_gateway"

        # 2. GET /api/v1/ebpf/rules
        resp = client.get("/api/v1/ebpf/rules")
        assert resp.status_code == 200
        data = resp.json()
        assert "rules" in data
        assert data["count"] >= 1

        # 3. POST /api/v1/ebpf/rules
        rule_payload = {
            "name": "Custom Ollama Rule",
            "ports": [11434],
            "domains": ["localhost", "127.0.0.1"],
            "action": "inspect",
            "min_shannon_entropy": 0.30,
        }
        resp = client.post("/api/v1/ebpf/rules", json=rule_payload)
        assert resp.status_code == 200
        created = resp.json()
        assert created["status"] == "created"
        rule_id = created["rule"]["id"]

        # 4. POST /api/v1/ebpf/simulate (clean)
        sim_payload = {
            "data": "POST /v1/chat/completions HTTP/1.1\r\nHost: api.openai.com\r\n\r\n{\"messages\":[{\"role\":\"user\",\"content\":\"Explain cosmos\"}]}",
            "host": "api.openai.com",
            "port": 443,
        }
        resp = client.post("/api/v1/ebpf/simulate", json=sim_payload)
        assert resp.status_code == 200
        sim_res = resp.json()
        assert not sim_res["terminated"]

        # 5. POST /api/v1/ebpf/simulate (degenerate loop)
        deg_payload = {
            "data": "POST /v1/chat/completions HTTP/1.1\r\nHost: api.openai.com\r\n\r\n{\"messages\":[{\"role\":\"user\",\"content\":\"" + ("repeat " * 40) + "\"}]}",
            "host": "api.openai.com",
            "port": 443,
        }
        resp = client.post("/api/v1/ebpf/simulate", json=deg_payload)
        assert resp.status_code == 200
        deg_res = resp.json()
        assert deg_res["terminated"]
        assert deg_res["intervention"]["action"] == "TCP_RST"

        # 6. DELETE /api/v1/ebpf/rules/{id}
        resp = client.delete(f"/api/v1/ebpf/rules/{rule_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "deleted"

    finally:
        storage.close_sync()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass


def test_cli_ebpf_subcommands(capsys):
    """Verify CLI subcommands `swarmwatch ebpf status|rules|simulate`."""
    with pytest.raises(SystemExit) as exc:
        cli_main(["ebpf", "status", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    status_obj = json.loads(captured.out)
    assert status_obj["status"] in ("attached", "detached")

    with pytest.raises(SystemExit) as exc:
        cli_main(["ebpf", "rules", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    rules_obj = json.loads(captured.out)
    assert "rules" in rules_obj

    with pytest.raises(SystemExit) as exc:
        cli_main(["ebpf", "simulate", "--format", "json"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    sim_obj = json.loads(captured.out)
    assert "terminated" in sim_obj
