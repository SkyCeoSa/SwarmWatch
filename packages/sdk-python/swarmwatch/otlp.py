# -*- coding: utf-8 -*-
"""
SwarmWatch OpenTelemetry OTLP Ingestion Gateway & Wire Format Decoder.
Provides native OTLP trace reception (POST /v1/traces and POST /api/v1/otlp/traces)
conforming to OpenTelemetry 1.0+ specifications.

Supports:
1. Standard OTLP/HTTP JSON payloads (proto3 JSON mapping and flattened formats).
2. Pure Python standard library binary protobuf wire format decoding without
   requiring external Google protobuf pip dependencies.
3. Bidirectional translation of standard OpenTelemetry GenAI Semantic Conventions
   (gen_ai.system, gen_ai.agent.name, gen_ai.usage.total_tokens, etc.) into SwarmSpan.
"""
from datetime import datetime, timezone
import json
import logging
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from swarmwatch.schema import SwarmSpan
from swarmwatch.circuit import compute_shannon_entropy

logger = logging.getLogger("swarmwatch.otlp")


# ==============================================================================
# 1. Pure Standard Library Protobuf Wire Format Decoder (Zero-Dependency)
# ==============================================================================

class ProtobufWireDecoder:
    """
    Pure Python standard library decoder for Google Protocol Buffers wire format.
    Decodes tag-length-value (TLV) binary payloads without external dependencies.
    """

    WIRE_VARINT = 0
    WIRE_FIXED64 = 1
    WIRE_LENGTH_DELIMITED = 2
    WIRE_FIXED32 = 5

    @classmethod
    def read_varint(cls, data: bytes, offset: int = 0) -> Tuple[int, int]:
        """Reads a base-128 varint starting at offset. Returns (value, new_offset)."""
        res = 0
        shift = 0
        n = len(data)
        while offset < n:
            b = data[offset]
            offset += 1
            res |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return res, offset
            shift += 7
            if shift >= 64:
                raise ValueError("Varint exceeds 64-bit integer range")
        raise ValueError("Unexpected end of data while decoding varint")

    @classmethod
    def decode_raw_message(cls, data: bytes) -> Dict[int, List[Any]]:
        """
        Decodes arbitrary binary protobuf data into field_number -> list of values.
        Values are:
        - int for WIRE_VARINT
        - bytes for WIRE_FIXED64 and WIRE_FIXED32
        - bytes for WIRE_LENGTH_DELIMITED
        """
        fields: Dict[int, List[Any]] = {}
        offset = 0
        n = len(data)

        while offset < n:
            tag, offset = cls.read_varint(data, offset)
            field_num = tag >> 3
            wire_type = tag & 0x07

            if wire_type == cls.WIRE_VARINT:
                val, offset = cls.read_varint(data, offset)
                fields.setdefault(field_num, []).append(val)
            elif wire_type == cls.WIRE_FIXED64:
                if offset + 8 > n:
                    break
                val = data[offset:offset + 8]
                offset += 8
                fields.setdefault(field_num, []).append(val)
            elif wire_type == cls.WIRE_LENGTH_DELIMITED:
                length, offset = cls.read_varint(data, offset)
                if offset + length > n:
                    break
                val = data[offset:offset + length]
                offset += length
                fields.setdefault(field_num, []).append(val)
            elif wire_type == cls.WIRE_FIXED32:
                if offset + 4 > n:
                    break
                val = data[offset:offset + 4]
                offset += 4
                fields.setdefault(field_num, []).append(val)
            else:
                # Unsupported or deprecated wire type (e.g. start/end group)
                break

        return fields

    @classmethod
    def decode_otlp_protobuf(cls, data: bytes) -> Dict[str, Any]:
        """
        Decodes binary OTLP ExportTraceServiceRequest protobuf bytes into standard
        nested dictionary format (resourceSpans -> scopeSpans -> spans).
        """
        root = cls.decode_raw_message(data)
        # ExportTraceServiceRequest field 1 = repeated ResourceSpans resource_spans
        raw_resource_spans = root.get(1, [])
        resource_spans = []

        for rs_bytes in raw_resource_spans:
            if not isinstance(rs_bytes, bytes):
                continue
            rs_msg = cls.decode_raw_message(rs_bytes)
            
            # ResourceSpans field 1 = Resource resource
            res_attrs: List[Dict[str, Any]] = []
            if 1 in rs_msg and rs_msg[1]:
                res_bytes = rs_msg[1][0]
                if isinstance(res_bytes, bytes):
                    res_msg = cls.decode_raw_message(res_bytes)
                    # Resource field 1 = repeated KeyValue attributes
                    res_attrs = cls._decode_key_values(res_msg.get(1, []))

            # ResourceSpans field 2 = repeated ScopeSpans scope_spans
            scope_spans = []
            for ss_bytes in rs_msg.get(2, []):
                if not isinstance(ss_bytes, bytes):
                    continue
                ss_msg = cls.decode_raw_message(ss_bytes)
                
                # ScopeSpans field 1 = InstrumentationScope scope
                scope_info = {"name": "", "version": ""}
                if 1 in ss_msg and ss_msg[1]:
                    scope_bytes = ss_msg[1][0]
                    if isinstance(scope_bytes, bytes):
                        sc_msg = cls.decode_raw_message(scope_bytes)
                        # InstrumentationScope field 1 = string name, field 2 = string version
                        if 1 in sc_msg and sc_msg[1]:
                            scope_info["name"] = sc_msg[1][0].decode("utf-8", errors="replace")
                        if 2 in sc_msg and sc_msg[2]:
                            scope_info["version"] = sc_msg[2][0].decode("utf-8", errors="replace")

                # ScopeSpans field 2 = repeated Span spans
                spans = []
                for s_bytes in ss_msg.get(2, []):
                    if not isinstance(s_bytes, bytes):
                        continue
                    span_msg = cls.decode_raw_message(s_bytes)
                    
                    # Span field 1 = bytes trace_id (16 bytes)
                    trace_id = ""
                    if 1 in span_msg and span_msg[1]:
                        t_val = span_msg[1][0]
                        trace_id = t_val.hex() if isinstance(t_val, bytes) else str(t_val)
                    
                    # Span field 2 = bytes span_id (8 bytes)
                    span_id = ""
                    if 2 in span_msg and span_msg[2]:
                        s_val = span_msg[2][0]
                        span_id = s_val.hex() if isinstance(s_val, bytes) else str(s_val)

                    # Span field 4 = bytes parent_span_id (8 bytes)
                    parent_span_id = ""
                    if 4 in span_msg and span_msg[4]:
                        p_val = span_msg[4][0]
                        parent_span_id = p_val.hex() if isinstance(p_val, bytes) else str(p_val)

                    # Span field 5 = string name
                    name = ""
                    if 5 in span_msg and span_msg[5]:
                        n_val = span_msg[5][0]
                        name = n_val.decode("utf-8", errors="replace") if isinstance(n_val, bytes) else str(n_val)

                    # Span field 6 = SpanKind kind (varint)
                    kind = span_msg.get(6, [1])[0]

                    # Span field 7 = fixed64 start_time_unix_nano
                    start_nano = 0
                    if 7 in span_msg and span_msg[7]:
                        st_val = span_msg[7][0]
                        start_nano = struct.unpack("<Q", st_val)[0] if isinstance(st_val, bytes) and len(st_val) == 8 else (int(st_val) if isinstance(st_val, int) else 0)

                    # Span field 8 = fixed64 end_time_unix_nano
                    end_nano = 0
                    if 8 in span_msg and span_msg[8]:
                        et_val = span_msg[8][0]
                        end_nano = struct.unpack("<Q", et_val)[0] if isinstance(et_val, bytes) and len(et_val) == 8 else (int(et_val) if isinstance(et_val, int) else 0)

                    # Span field 9 = repeated KeyValue attributes
                    attributes = cls._decode_key_values(span_msg.get(9, []))

                    # Span field 11 = repeated Event events
                    events = []
                    for ev_bytes in span_msg.get(11, []):
                        if isinstance(ev_bytes, bytes):
                            ev_msg = cls.decode_raw_message(ev_bytes)
                            ev_name = ""
                            if 2 in ev_msg and ev_msg[2]:
                                ev_name = ev_msg[2][0].decode("utf-8", errors="replace") if isinstance(ev_msg[2][0], bytes) else str(ev_msg[2][0])
                            ev_attrs = cls._decode_key_values(ev_msg.get(3, []))
                            events.append({"name": ev_name, "attributes": ev_attrs})

                    spans.append({
                        "traceId": trace_id,
                        "spanId": span_id,
                        "parentSpanId": parent_span_id,
                        "name": name,
                        "kind": kind,
                        "startTimeUnixNano": start_nano,
                        "endTimeUnixNano": end_nano,
                        "attributes": attributes,
                        "events": events,
                    })

                scope_spans.append({
                    "scope": scope_info,
                    "spans": spans,
                })

            resource_spans.append({
                "resource": {"attributes": res_attrs},
                "scopeSpans": scope_spans,
            })

        return {"resourceSpans": resource_spans}

    @classmethod
    def _decode_key_values(cls, kv_list: List[Any]) -> List[Dict[str, Any]]:
        """Decodes repeated KeyValue protobuf messages into list of {'key': ..., 'value': ...}."""
        result = []
        for kv_bytes in kv_list:
            if not isinstance(kv_bytes, bytes):
                continue
            kv_msg = cls.decode_raw_message(kv_bytes)
            # KeyValue field 1 = string key
            key = ""
            if 1 in kv_msg and kv_msg[1]:
                k_val = kv_msg[1][0]
                key = k_val.decode("utf-8", errors="replace") if isinstance(k_val, bytes) else str(k_val)
            
            # KeyValue field 2 = AnyValue value
            val_obj: Dict[str, Any] = {}
            if 2 in kv_msg and kv_msg[2]:
                val_bytes = kv_msg[2][0]
                if isinstance(val_bytes, bytes):
                    v_msg = cls.decode_raw_message(val_bytes)
                    if 1 in v_msg and v_msg[1]:  # string_value
                        s = v_msg[1][0]
                        val_obj = {"stringValue": s.decode("utf-8", errors="replace") if isinstance(s, bytes) else str(s)}
                    elif 2 in v_msg and v_msg[2]:  # bool_value
                        val_obj = {"boolValue": bool(v_msg[2][0])}
                    elif 3 in v_msg and v_msg[3]:  # int_value
                        val_obj = {"intValue": int(v_msg[3][0])}
                    elif 4 in v_msg and v_msg[4]:  # double_value
                        d_bytes = v_msg[4][0]
                        val_obj = {"doubleValue": struct.unpack("<d", d_bytes)[0] if isinstance(d_bytes, bytes) and len(d_bytes) == 8 else float(d_bytes)}
                    elif 7 in v_msg and v_msg[7]:  # bytes_value
                        val_obj = {"bytesValue": v_msg[7][0].hex()}

            result.append({"key": key, "value": val_obj})
        return result


# ==============================================================================
# 2. OpenTelemetry Attribute & Value Normalization
# ==============================================================================

def extract_otlp_value(val_obj: Any) -> Any:
    """Recursively unpacks an OTLP proto3 AnyValue object into native Python types."""
    if not isinstance(val_obj, dict):
        return val_obj

    if "stringValue" in val_obj:
        return val_obj["stringValue"]
    if "intValue" in val_obj:
        try:
            return int(val_obj["intValue"])
        except (ValueError, TypeError):
            return val_obj["intValue"]
    if "doubleValue" in val_obj:
        try:
            return float(val_obj["doubleValue"])
        except (ValueError, TypeError):
            return val_obj["doubleValue"]
    if "boolValue" in val_obj:
        return bool(val_obj["boolValue"])
    if "bytesValue" in val_obj:
        return str(val_obj["bytesValue"])
    if "arrayValue" in val_obj:
        items = val_obj.get("arrayValue", {}).get("values", [])
        return [extract_otlp_value(it) for it in items]
    if "kvlistValue" in val_obj:
        items = val_obj.get("kvlistValue", {}).get("values", [])
        return extract_otlp_attributes(items)

    # If it's already a native dict without OTLP wrappers, return directly
    return val_obj


def extract_otlp_attributes(attrs_raw: Any) -> Dict[str, Any]:
    """
    Normalizes OTLP attributes from either:
    1. Standard proto3 JSON list: [{"key": "name", "value": {"stringValue": "bot"}}]
    2. Decoded protobuf KeyValue structures.
    3. Flat key-value dictionaries: {"name": "bot", "tokens": 100}
    """
    if isinstance(attrs_raw, dict):
        return {k: extract_otlp_value(v) for k, v in attrs_raw.items()}

    res: Dict[str, Any] = {}
    if isinstance(attrs_raw, list):
        for item in attrs_raw:
            if isinstance(item, dict):
                k = item.get("key")
                if k is not None:
                    res[str(k)] = extract_otlp_value(item.get("value"))
    return res


# ==============================================================================
# 3. OpenTelemetry GenAI Semantic Conventions -> SwarmSpan Translator
# ==============================================================================

def otlp_span_to_swarm_span(
    span_data: Dict[str, Any],
    resource_attrs: Optional[Dict[str, Any]] = None,
    scope_name: Optional[str] = None,
    default_project_id: str = "default-project",
) -> SwarmSpan:
    """
    Translates an OpenTelemetry trace span conforming to GenAI Semantic Conventions
    into a native SwarmWatch SwarmSpan primitive.
    """
    res_attrs = resource_attrs or {}
    span_attrs = extract_otlp_attributes(span_data.get("attributes", []))

    # 1. Identifiers
    trace_id = str(span_data.get("traceId") or span_data.get("trace_id") or "swarm-otlp-default")
    span_id = str(span_data.get("spanId") or span_data.get("span_id") or "span-otlp-0")
    parent_id = span_data.get("parentSpanId") or span_data.get("parent_span_id") or None
    if parent_id == "" or parent_id == "0" * len(str(parent_id)):
        parent_id = None

    raw_name = str(span_data.get("name") or "agent_execution")

    # 2. Source & Target Agents (GenAI Semantic Conventions)
    source_agent = (
        span_attrs.get("gen_ai.agent.name")
        or span_attrs.get("gen_ai.agent.id")
        or span_attrs.get("agent.name")
        or res_attrs.get("service.name")
    )
    if not source_agent:
        if " -> " in raw_name:
            source_agent = raw_name.split(" -> ")[0].strip()
        else:
            source_agent = "external_agent"

    target_agent = (
        span_attrs.get("gen_ai.target.agent")
        or span_attrs.get("gen_ai.request.model")
        or span_attrs.get("gen_ai.response.model")
        or span_attrs.get("server.address")
    )
    if not target_agent:
        if " -> " in raw_name:
            part = raw_name.split(" -> ")[1]
            target_agent = part.split(":")[0].strip() if ":" in part else part.strip()
        else:
            target_agent = "agent_target"

    # 3. Span Type Normalization
    op_name = span_attrs.get("gen_ai.operation.name") or span_attrs.get("gen_ai.span_type")
    if not op_name:
        if ":" in raw_name:
            op_name = raw_name.split(":")[-1].strip()
        else:
            op_name = "agent_call"

    # Normalize standard OTel operations
    op_lower = op_name.lower()
    if op_lower in ("chat", "completion", "generate_content"):
        span_type = "llm_completion"
    elif op_lower in ("tool", "execute_tool", "function_call"):
        span_type = "tool_call"
    elif op_lower in ("embeddings", "embedding"):
        span_type = "embedding"
    else:
        span_type = op_name

    # 4. Token Counting & Usage
    token_count = 0
    if "gen_ai.usage.total_tokens" in span_attrs:
        token_count = int(span_attrs["gen_ai.usage.total_tokens"])
    elif "gen_ai.usage.input_tokens" in span_attrs or "gen_ai.usage.output_tokens" in span_attrs:
        inp = int(span_attrs.get("gen_ai.usage.input_tokens", 0))
        out = int(span_attrs.get("gen_ai.usage.output_tokens", 0))
        token_count = inp + out
    elif "llm.usage.total_tokens" in span_attrs:
        token_count = int(span_attrs["llm.usage.total_tokens"])

    # 5. Cost Estimation
    cost_estimate = 0.0
    if "gen_ai.usage.cost" in span_attrs:
        cost_estimate = float(span_attrs["gen_ai.usage.cost"])
    else:
        cost_estimate = round(token_count * 0.00002, 6)

    # 6. Timing & Duration
    start_nano = span_data.get("startTimeUnixNano") or span_data.get("start_time_unix_nano") or 0
    end_nano = span_data.get("endTimeUnixNano") or span_data.get("end_time_unix_nano") or 0
    now_iso = datetime.now(timezone.utc).isoformat()

    if start_nano:
        start_time = datetime.fromtimestamp(start_nano / 1e9, tz=timezone.utc).isoformat()
    else:
        start_time = now_iso

    if end_nano:
        end_time = datetime.fromtimestamp(end_nano / 1e9, tz=timezone.utc).isoformat()
        duration_ms = max(0.0, (end_nano - start_nano) / 1e6)
    else:
        end_time = start_time
        duration_ms = 0.0

    # 7. Payload Extraction & Event Reconstruction
    events = span_data.get("events", [])
    prompt_text = span_attrs.get("gen_ai.prompt") or ""
    completion_text = span_attrs.get("gen_ai.completion") or ""

    for ev in events:
        ev_name = ev.get("name", "")
        ev_attrs = extract_otlp_attributes(ev.get("attributes", []))
        if "prompt" in ev_name.lower():
            prompt_text = ev_attrs.get("content") or ev_attrs.get("prompt") or prompt_text
        elif "completion" in ev_name.lower():
            completion_text = ev_attrs.get("content") or ev_attrs.get("completion") or completion_text

    input_payload = {"prompt": prompt_text, "raw_name": raw_name}
    output_payload = {"completion": completion_text, "status": "ok"}

    # Fallback token estimate from text length if token count was omitted
    if token_count == 0:
        total_chars = len(str(prompt_text)) + len(str(completion_text))
        if total_chars > 0:
            token_count = max(1, total_chars // 4)
            cost_estimate = round(token_count * 0.00002, 6)

    # 8. Shannon Entropy Calculation
    if "gen_ai.swarmwatch.entropy_score" in span_attrs:
        entropy_score = float(span_attrs["gen_ai.swarmwatch.entropy_score"])
    elif completion_text:
        entropy_score = compute_shannon_entropy(str(completion_text))
    else:
        entropy_score = 0.85

    branch_id = span_attrs.get("gen_ai.swarmwatch.branch_id") or "main"
    project_id = span_attrs.get("swarmwatch.project_id") or default_project_id

    return SwarmSpan(
        id=span_id,
        swarm_id=trace_id,
        branch_id=branch_id,
        parent_span_id=parent_id,
        source_agent=str(source_agent),
        target_agent=str(target_agent),
        span_type=str(span_type),
        input_payload=input_payload,
        output_payload=output_payload,
        token_count=token_count,
        entropy_score=entropy_score,
        start_time=start_time,
        end_time=end_time,
        duration_ms=duration_ms,
        cost_estimate=cost_estimate,
        project_id=project_id,
    )


# ==============================================================================
# 4. Ingestion Parser Dispatcher & Statistics Tracker
# ==============================================================================

class OtlpIngestionStats:
    """Thread-safe telemetry metrics for the OTLP Ingestion Gateway."""

    def __init__(self):
        self._lock = threading.Lock()
        self.total_spans_received = 0
        self.total_spans_parsed = 0
        self.total_spans_rejected = 0
        self.active_systems: Set[str] = set()
        self.last_ingested_at: Optional[str] = None

    def record_ingestion(self, received: int, parsed: int, rejected: int, systems: Optional[List[str]] = None) -> None:
        with self._lock:
            self.total_spans_received += received
            self.total_spans_parsed += parsed
            self.total_spans_rejected += rejected
            if systems:
                for sys_name in systems:
                    if sys_name:
                        self.active_systems.add(sys_name)
            self.last_ingested_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_spans_received": self.total_spans_received,
                "total_spans_parsed": self.total_spans_parsed,
                "total_spans_rejected": self.total_spans_rejected,
                "active_systems": sorted(list(self.active_systems)),
                "last_ingested_at": self.last_ingested_at,
            }


global_otlp_stats = OtlpIngestionStats()


class OtelTraceParser:
    """
    Parser and normalizer for incoming OpenTelemetry OTLP trace payloads.
    Processes both JSON and binary Protobuf streams into SwarmSpan collections.
    """

    @classmethod
    def parse_otlp_payload(
        cls,
        payload_bytes: bytes,
        content_type: str = "application/json",
        default_project_id: str = "default-project",
    ) -> Tuple[List[SwarmSpan], int]:
        """
        Parses raw HTTP request body bytes into a list of SwarmSpan primitives.
        Returns (parsed_spans, rejected_count).
        """
        if not payload_bytes:
            return [], 0

        is_proto = "protobuf" in content_type.lower() or "octet-stream" in content_type.lower()
        parsed_data: Optional[Dict[str, Any]] = None

        if is_proto:
            try:
                parsed_data = ProtobufWireDecoder.decode_otlp_protobuf(payload_bytes)
            except Exception as e:
                logger.error("Failed to decode OTLP binary protobuf payload: %s", e)
                return [], 1
        else:
            try:
                text = payload_bytes.decode("utf-8")
                parsed_data = json.loads(text)
            except Exception as e:
                logger.error("Failed to decode OTLP JSON payload: %s", e)
                return [], 1

        if not parsed_data or not isinstance(parsed_data, dict):
            return [], 1

        spans: List[SwarmSpan] = []
        rejected = 0
        systems: List[str] = []

        resource_spans = parsed_data.get("resourceSpans") or parsed_data.get("resource_spans") or []
        for rs in resource_spans:
            if not isinstance(rs, dict):
                continue
            res_obj = rs.get("resource", {})
            res_attrs = extract_otlp_attributes(res_obj.get("attributes", []))
            
            scope_spans = rs.get("scopeSpans") or rs.get("scope_spans") or []
            for ss in scope_spans:
                if not isinstance(ss, dict):
                    continue
                scope_info = ss.get("scope", {})
                scope_name = scope_info.get("name", "")

                raw_spans = ss.get("spans", [])
                for s in raw_spans:
                    if not isinstance(s, dict):
                        rejected += 1
                        continue
                    try:
                        swarm_span = otlp_span_to_swarm_span(
                            span_data=s,
                            resource_attrs=res_attrs,
                            scope_name=scope_name,
                            default_project_id=default_project_id,
                        )
                        spans.append(swarm_span)
                        
                        # Track gen_ai.system
                        s_attrs = extract_otlp_attributes(s.get("attributes", []))
                        sys_name = s_attrs.get("gen_ai.system") or res_attrs.get("service.name")
                        if sys_name:
                            systems.append(str(sys_name))
                    except Exception as err:
                        logger.warning("Failed to translate OTLP span: %s", err)
                        rejected += 1

        global_otlp_stats.record_ingestion(
            received=len(spans) + rejected,
            parsed=len(spans),
            rejected=rejected,
            systems=systems,
        )

        return spans, rejected
