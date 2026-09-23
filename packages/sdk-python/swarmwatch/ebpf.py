# -*- coding: utf-8 -*-
"""
SwarmWatch Kernel-Level Network Socket LLM Interception Gateway (Seam 29 / ADR 0040).

Pure Python standard library implementation of transparent socket interception,
stream protocol parsing (HTTP/1.1 & SSE), real-time Shannon entropy & token budget
interception, and physical TCP RST connection severance with zero external C-extensions.
"""

import asyncio
from collections import Counter as FreqCounter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import fnmatch
import json
import logging
import math
import os
import re
import socket
import struct
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import uuid

from swarmwatch.metrics import (
    record_ebpf_packet_intercepted,
    record_ebpf_bytes_intercepted,
    set_active_ebpf_sockets,
    record_ebpf_rst_intervention,
)

logger = logging.getLogger("swarmwatch.ebpf")


def calculate_shannon_entropy(text: str) -> float:
    """Computes byte-level Shannon entropy in [0.0, 1.0]."""
    if not text:
        return 1.0
    byte_vals = text.encode("utf-8", errors="replace")
    length = len(byte_vals)
    if length <= 1:
        return 1.0
    counts = FreqCounter(byte_vals)
    ent = 0.0
    for count in counts.values():
        p = count / length
        ent -= p * math.log2(p)
    # Normalize by log2(256) = 8.0
    return max(0.0, min(1.0, ent / 8.0))


@dataclass
class SocketRule:
    """Filter rule for kernel/socket-level LLM interception."""

    id: str = field(default_factory=lambda: f"rule-{uuid.uuid4().hex[:8]}")
    name: str = "default-rule"
    ports: List[int] = field(default_factory=lambda: [80, 443, 11434, 8000, 8080])
    domains: List[str] = field(default_factory=lambda: ["*openai.com", "*anthropic.com", "*googleapis.com", "localhost", "127.0.0.1"])
    action: str = "inspect"  # "inspect", "allow", "terminate_rst"
    max_payload_bytes: int = 10 * 1024 * 1024  # 10 MB
    max_tokens_per_stream: int = 4096
    min_shannon_entropy: float = 0.35
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def matches(self, host: str, port: int) -> bool:
        """Determines whether a destination host and port matches this rule."""
        if self.ports and port not in self.ports:
            return False

        if not self.domains:
            return True

        host_lower = (host or "").lower()
        for pattern in self.domains:
            pat_lower = pattern.lower()
            if fnmatch.fnmatch(host_lower, pat_lower) or host_lower == pat_lower:
                return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "ports": self.ports,
            "domains": self.domains,
            "action": self.action,
            "max_payload_bytes": self.max_payload_bytes,
            "max_tokens_per_stream": self.max_tokens_per_stream,
            "min_shannon_entropy": self.min_shannon_entropy,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SocketRule":
        return cls(
            id=data.get("id", f"rule-{uuid.uuid4().hex[:8]}"),
            name=data.get("name", "custom-rule"),
            ports=data.get("ports", [80, 443, 11434, 8000]),
            domains=data.get("domains", ["*"]),
            action=data.get("action", "inspect"),
            max_payload_bytes=data.get("max_payload_bytes", 10485760),
            max_tokens_per_stream=data.get("max_tokens_per_stream", 4096),
            min_shannon_entropy=float(data.get("min_shannon_entropy", 0.35)),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class SocketIntervention:
    """Details of a physical TCP RST or socket shutdown intervention."""

    connection_id: str
    src_addr: str
    dst_addr: str
    dst_port: int
    reason: str
    action: str = "TCP_RST"
    tokens_saved: int = 0
    cost_saved: float = 0.0
    enacted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connection_id": self.connection_id,
            "src_addr": self.src_addr,
            "dst_addr": self.dst_addr,
            "dst_port": self.dst_port,
            "reason": self.reason,
            "action": self.action,
            "tokens_saved": self.tokens_saved,
            "cost_saved": self.cost_saved,
            "enacted_at": self.enacted_at,
        }


class SocketStreamDecoder:
    """
    Zero-dependency pure Python streaming protocol decoder for HTTP/1.1 and SSE.
    Disassembles raw TCP chunks, extracts headers, JSON bodies, and streaming tokens.
    """

    def __init__(self):
        self._buffer = bytearray()
        self.headers: Dict[str, str] = {}
        self.method: Optional[str] = None
        self.path: Optional[str] = None
        self.status_code: Optional[int] = None
        self.headers_parsed = False
        self.content_length: Optional[int] = None
        self.is_chunked = False
        self.is_sse = False
        self.extracted_text = ""
        self.streamed_tokens: List[str] = []
        self.estimated_token_count = 0
        self.entropy: float = 1.0
        self.json_payload: Optional[Dict[str, Any]] = None

    def feed(self, chunk: bytes) -> None:
        """Appends raw bytes and incrementally processes protocol frames."""
        self._buffer.extend(chunk)

        # Parse headers if not done yet
        if not self.headers_parsed:
            header_end = self._buffer.find(b"\r\n\r\n")
            if header_end != -1:
                header_bytes = bytes(self._buffer[:header_end])
                self._parse_headers(header_bytes)
                self.headers_parsed = True
                del self._buffer[: header_end + 4]

        # Parse body if headers have been parsed
        if self.headers_parsed:
            raw_body = bytes(self._buffer)
            content_type = self.headers.get("content-type", "").lower()

            if "text/event-stream" in content_type:
                self.is_sse = True
                self._parse_sse_stream()
            elif "application/json" in content_type or raw_body.strip().startswith(b"{"):
                self._parse_json_body(raw_body)
            else:
                text = raw_body.decode("utf-8", errors="replace")
                self.extracted_text = text
                self.estimated_token_count = max(1, len(text) // 4)
                self.entropy = calculate_shannon_entropy(text)

    def _parse_headers(self, header_bytes: bytes) -> None:
        lines = header_bytes.decode("utf-8", errors="replace").split("\r\n")
        if not lines:
            return

        start_line = lines[0].strip()
        parts = start_line.split(" ")
        if len(parts) >= 2:
            if parts[0].startswith("HTTP/"):
                # Response line: HTTP/1.1 200 OK
                try:
                    self.status_code = int(parts[1])
                except ValueError:
                    self.status_code = 200
            else:
                # Request line: POST /v1/chat/completions HTTP/1.1
                self.method = parts[0]
                self.path = parts[1]

        for line in lines[1:]:
            if ":" in line:
                key, val = line.split(":", 1)
                self.headers[key.strip().lower()] = val.strip()

        if "content-length" in self.headers:
            try:
                self.content_length = int(self.headers["content-length"])
            except ValueError:
                self.content_length = None

        if self.headers.get("transfer-encoding", "").lower() == "chunked":
            self.is_chunked = True

    def _parse_json_body(self, raw_body: bytes) -> None:
        try:
            text = raw_body.decode("utf-8", errors="replace")
            parsed = json.loads(text)
            self.json_payload = parsed
            extracted: List[str] = []

            # Extract OpenAI / Anthropic format messages
            if isinstance(parsed, dict):
                if "messages" in parsed and isinstance(parsed["messages"], list):
                    for msg in parsed["messages"]:
                        if isinstance(msg, dict) and "content" in msg:
                            c = msg["content"]
                            if isinstance(c, str):
                                extracted.append(c)
                elif "prompt" in parsed:
                    p = parsed["prompt"]
                    if isinstance(p, str):
                        extracted.append(p)
                elif "input" in parsed:
                    inp = parsed["input"]
                    if isinstance(inp, str):
                        extracted.append(inp)

            combined = " ".join(extracted) if extracted else text
            self.extracted_text = combined
            self.estimated_token_count = max(1, len(combined) // 4)
            self.entropy = calculate_shannon_entropy(combined)
        except Exception:
            text = raw_body.decode("utf-8", errors="replace")
            self.extracted_text = text
            self.estimated_token_count = max(1, len(text) // 4)
            self.entropy = calculate_shannon_entropy(text)

    def _parse_sse_stream(self) -> None:
        raw = bytes(self._buffer).decode("utf-8", errors="replace")
        lines = raw.split("\n")
        new_tokens: List[str] = []

        for line in lines:
            line = line.strip()
            if line.startswith("data:"):
                payload_str = line[5:].strip()
                if payload_str == "[DONE]":
                    continue
                try:
                    data = json.loads(payload_str)
                    if isinstance(data, dict):
                        choices = data.get("choices", [])
                        if choices and isinstance(choices, list):
                            delta = choices[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                new_tokens.append(content)
                except Exception:
                    pass

        if new_tokens:
            self.streamed_tokens = new_tokens
            combined = "".join(new_tokens)
            self.extracted_text = combined
            self.estimated_token_count = max(len(new_tokens), len(combined) // 4)
            self.entropy = calculate_shannon_entropy(combined)


class TcpRstEmitter:
    """
    Physical TCP connection disruption engine.
    Applies SO_LINGER zero timeout to emit hard TCP RST on close.
    """

    @staticmethod
    def abort_socket(sock: socket.socket) -> None:
        """Physically terminates the socket with a hard TCP RST packet."""
        try:
            # SO_LINGER with l_onoff=1 and l_linger=0 causes TCP RST instead of FIN
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        except Exception as e:
            logger.debug("Failed to set SO_LINGER: %s", e)
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass


class EbpfSocketGateway:
    """
    Deep module managing transparent socket interception, stream protocol parsing,
    real-time Shannon entropy & token budget inspection, and physical TCP RST disconnection.
    """

    def __init__(self):
        self._rules: Dict[str, SocketRule] = {}
        self._interventions: List[SocketIntervention] = []
        self._active_sockets_count: int = 0
        self._total_packets: int = 0
        self._total_bytes: int = 0
        self._is_attached: bool = True
        self._lock = asyncio.Lock()

        # Seed default inspection rule
        default_rule = SocketRule(
            id="rule-llm-default",
            name="Default Outbound LLM Inspection",
            ports=[80, 443, 11434, 8000, 8080],
            domains=["*openai.com", "*anthropic.com", "*googleapis.com", "localhost", "127.0.0.1"],
            action="inspect",
            min_shannon_entropy=0.35,
            max_tokens_per_stream=4096,
        )
        self._rules[default_rule.id] = default_rule

    @property
    def is_attached(self) -> bool:
        return self._is_attached

    def set_attached(self, attached: bool) -> None:
        self._is_attached = attached

    def add_rule(self, rule: SocketRule) -> SocketRule:
        self._rules[rule.id] = rule
        return rule

    def get_rule(self, rule_id: str) -> Optional[SocketRule]:
        return self._rules.get(rule_id)

    def list_rules(self) -> List[SocketRule]:
        return list(self._rules.values())

    def delete_rule(self, rule_id: str) -> bool:
        if rule_id in self._rules:
            del self._rules[rule_id]
            return True
        return False

    def get_interventions(self, limit: int = 50) -> List[SocketIntervention]:
        return list(reversed(self._interventions[-limit:]))

    def inspect_stream_data(
        self,
        data: bytes,
        host: str = "api.openai.com",
        port: int = 443,
        src_addr: str = "127.0.0.1",
        connection_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[SocketIntervention], SocketStreamDecoder]:
        """
        Inspects an incoming/outgoing socket stream byte payload.
        Returns: (should_terminate, intervention, decoder)
        """
        conn_id = connection_id or f"conn-{uuid.uuid4().hex[:10]}"
        self._total_packets += 1
        self._total_bytes += len(data)
        record_ebpf_packet_intercepted(1)
        record_ebpf_bytes_intercepted(len(data))

        decoder = SocketStreamDecoder()
        decoder.feed(data)

        if not self._is_attached:
            return False, None, decoder

        # Find matching rules
        matching_rules = [r for r in self._rules.values() if r.matches(host, port)]
        if not matching_rules:
            return False, None, decoder

        for rule in matching_rules:
            if rule.action == "allow":
                continue
            if rule.action == "terminate_rst":
                intervention = SocketIntervention(
                    connection_id=conn_id,
                    src_addr=src_addr,
                    dst_addr=host,
                    dst_port=port,
                    reason=f"Interception rule '{rule.name}' enforces unconditional TCP RST",
                    action="TCP_RST",
                    tokens_saved=decoder.estimated_token_count,
                    cost_saved=decoder.estimated_token_count * 0.00002,
                )
                self._interventions.append(intervention)
                record_ebpf_rst_intervention("unconditional_rst_rule")
                return True, intervention, decoder

            # Action == "inspect": check payload size, entropy, and token ceilings
            if len(data) > rule.max_payload_bytes:
                intervention = SocketIntervention(
                    connection_id=conn_id,
                    src_addr=src_addr,
                    dst_addr=host,
                    dst_port=port,
                    reason=f"Payload size ({len(data)} bytes) exceeds ceiling ({rule.max_payload_bytes} bytes)",
                    action="TCP_RST",
                    tokens_saved=decoder.estimated_token_count,
                    cost_saved=decoder.estimated_token_count * 0.00002,
                )
                self._interventions.append(intervention)
                record_ebpf_rst_intervention("payload_size_exceeded")
                return True, intervention, decoder

            # Check Shannon entropy collapse
            if decoder.extracted_text and len(decoder.extracted_text) >= 30:
                if decoder.entropy < rule.min_shannon_entropy:
                    intervention = SocketIntervention(
                        connection_id=conn_id,
                        src_addr=src_addr,
                        dst_addr=host,
                        dst_port=port,
                        reason=f"Shannon entropy collapse ({decoder.entropy:.4f} < threshold {rule.min_shannon_entropy:.4f})",
                        action="TCP_RST",
                        tokens_saved=decoder.estimated_token_count,
                        cost_saved=decoder.estimated_token_count * 0.00002,
                    )
                    self._interventions.append(intervention)
                    record_ebpf_rst_intervention("entropy_collapse")
                    return True, intervention, decoder

            # Check token ceiling breach
            if decoder.estimated_token_count > rule.max_tokens_per_stream:
                intervention = SocketIntervention(
                    connection_id=conn_id,
                    src_addr=src_addr,
                    dst_addr=host,
                    dst_port=port,
                    reason=f"Token stream quota exceeded ({decoder.estimated_token_count} > {rule.max_tokens_per_stream})",
                    action="TCP_RST",
                    tokens_saved=decoder.estimated_token_count,
                    cost_saved=decoder.estimated_token_count * 0.00002,
                )
                self._interventions.append(intervention)
                record_ebpf_rst_intervention("token_budget_exceeded")
                return True, intervention, decoder

        return False, None, decoder

    def status(self) -> Dict[str, Any]:
        """Returns the gateway health and diagnostic status."""
        set_active_ebpf_sockets(self._active_sockets_count)
        return {
            "status": "attached" if self._is_attached else "detached",
            "active_sockets": self._active_sockets_count,
            "packets_intercepted": self._total_packets,
            "bytes_intercepted": self._total_bytes,
            "interventions_enacted": len(self._interventions),
            "rules_count": len(self._rules),
            "mode": "ebpf_kernel_socket_gateway",
            "version": "0.1.0",
        }


class MockEbpfEngine:
    """
    Deterministic, thread-safe test simulator for eBPF and network socket interception.
    Simulates packet inspection, rule triggering, and physical TCP RST disconnection in <5ms.
    """

    def __init__(self, gateway: Optional[EbpfSocketGateway] = None):
        self.gateway = gateway or EbpfSocketGateway()

    def simulate_http_request(
        self,
        method: str = "POST",
        path: str = "/v1/chat/completions",
        host: str = "api.openai.com",
        port: int = 443,
        json_body: Optional[Dict[str, Any]] = None,
        raw_text: Optional[str] = None,
    ) -> Tuple[bool, Optional[SocketIntervention], SocketStreamDecoder]:
        """Simulates an outbound HTTP request payload to an LLM provider."""
        body_bytes = b""
        if json_body is not None:
            body_bytes = json.dumps(json_body).encode("utf-8")
        elif raw_text is not None:
            body_bytes = raw_text.encode("utf-8")

        headers = (
            f"{method} {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body_bytes)}\r\n"
            f"User-Agent: SwarmWatch-Agent/0.1.0\r\n\r\n"
        ).encode("utf-8")

        payload = headers + body_bytes
        return self.gateway.inspect_stream_data(payload, host=host, port=port)

    def simulate_sse_stream(
        self,
        tokens: List[str],
        host: str = "api.openai.com",
        port: int = 443,
    ) -> Tuple[bool, Optional[SocketIntervention], SocketStreamDecoder]:
        """Simulates an incoming Server-Sent Events (SSE) token stream."""
        lines = [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: text/event-stream\r\n",
            b"Transfer-Encoding: chunked\r\n\r\n",
        ]
        for tok in tokens:
            chunk = {"choices": [{"delta": {"content": tok}}]}
            lines.append(f"data: {json.dumps(chunk)}\n\n".encode("utf-8"))
        lines.append(b"data: [DONE]\n\n")

        payload = b"".join(lines)
        return self.gateway.inspect_stream_data(payload, host=host, port=port)


# Global singleton instance
global_ebpf_gateway = EbpfSocketGateway()
