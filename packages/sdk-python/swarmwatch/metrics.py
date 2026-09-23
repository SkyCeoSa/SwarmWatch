# -*- coding: utf-8 -*-
"""
SwarmWatch Enterprise Prometheus Metrics Exporter (Seam 8).

Pure Python standard library implementation of thread-safe atomic counters,
gauges, and cumulative histograms exposing Prometheus 0.0.4 / OpenMetrics 1.0.0
telemetry with sub-microsecond overhead and zero token cost.
"""

from collections import defaultdict
import threading
from typing import Any, Dict, Optional, Sequence, Tuple, Union


def _format_labels(labels: Sequence[Tuple[str, str]]) -> str:
    """Format a sequence of key-value pairs into a Prometheus label string."""
    if not labels:
        return ""
    # Sort keys for deterministic output
    sorted_labels = sorted(labels, key=lambda x: x[0])
    escaped = [f'{k}="{v}"' for k, v in sorted_labels]
    return "{" + ",".join(escaped) + "}"


def _format_val(val: Union[int, float]) -> str:
    """Format a numeric value cleanly for Prometheus exposition."""
    if isinstance(val, int):
        return str(val)
    if val.is_integer():
        return str(int(val))
    return f"{val:.6g}"


class Counter:
    """A thread-safe monotonically increasing counter."""

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()):
        self.name = name
        self.help_text = help_text
        self.label_names = tuple(sorted(label_names))
        self._values: Dict[Tuple[Tuple[str, str], ...], float] = defaultdict(float)
        self._lock = threading.Lock()

    def _normalize_labels(self, labels: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
        if not self.label_names:
            return ()
        labels = labels or {}
        return tuple(sorted((k, str(labels.get(k, ""))) for k in self.label_names))

    def inc(self, labels: Optional[Dict[str, Any]] = None, value: float = 1.0) -> None:
        if value < 0:
            raise ValueError(f"Counter increment must be non-negative (got {value})")
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            self._values[norm_labels] += value

    def get(self, labels: Optional[Dict[str, Any]] = None) -> float:
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            return self._values.get(norm_labels, 0.0)

    def export_samples(self) -> Sequence[Tuple[str, Sequence[Tuple[str, str]], float]]:
        with self._lock:
            return [(self.name, labels, val) for labels, val in self._values.items()]


class Gauge:
    """A thread-safe gauge that can increase, decrease, or be set arbitrarily."""

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()):
        self.name = name
        self.help_text = help_text
        self.label_names = tuple(sorted(label_names))
        self._values: Dict[Tuple[Tuple[str, str], ...], float] = defaultdict(float)
        self._lock = threading.Lock()

    def _normalize_labels(self, labels: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
        if not self.label_names:
            return ()
        labels = labels or {}
        return tuple(sorted((k, str(labels.get(k, ""))) for k in self.label_names))

    def set(self, value: float, labels: Optional[Dict[str, Any]] = None) -> None:
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            self._values[norm_labels] = float(value)

    def inc(self, labels: Optional[Dict[str, Any]] = None, value: float = 1.0) -> None:
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            self._values[norm_labels] += float(value)

    def dec(self, labels: Optional[Dict[str, Any]] = None, value: float = 1.0) -> None:
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            self._values[norm_labels] -= float(value)

    def get(self, labels: Optional[Dict[str, Any]] = None) -> float:
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            return self._values.get(norm_labels, 0.0)

    def export_samples(self) -> Sequence[Tuple[str, Sequence[Tuple[str, str]], float]]:
        with self._lock:
            return [(self.name, labels, val) for labels, val in self._values.items()]


class Histogram:
    """A cumulative histogram for measuring latency and duration distributions."""

    DEFAULT_BUCKETS = (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)

    def __init__(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_BUCKETS,
    ):
        self.name = name
        self.help_text = help_text
        self.label_names = tuple(sorted(label_names))
        # Ensure buckets are sorted and unique
        clean_buckets = sorted(set(buckets))
        if float("inf") not in clean_buckets:
            clean_buckets.append(float("inf"))
        self.buckets = tuple(clean_buckets)

        # Map labels to (bucket_counts, sum, count)
        self._data: Dict[
            Tuple[Tuple[str, str], ...],
            Tuple[Dict[float, int], float, int],
        ] = {}
        self._lock = threading.Lock()

    def _normalize_labels(self, labels: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
        if not self.label_names:
            return ()
        labels = labels or {}
        return tuple(sorted((k, str(labels.get(k, ""))) for k in self.label_names))

    def observe(self, value: float, labels: Optional[Dict[str, Any]] = None) -> None:
        norm_labels = self._normalize_labels(labels)
        val = float(value)
        with self._lock:
            if norm_labels not in self._data:
                b_counts = {b: 0 for b in self.buckets}
                self._data[norm_labels] = (b_counts, 0.0, 0)

            b_counts, total_sum, total_count = self._data[norm_labels]
            for b in self.buckets:
                if val <= b:
                    b_counts[b] += 1
            self._data[norm_labels] = (b_counts, total_sum + val, total_count + 1)

    def get_snapshot(self, labels: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        norm_labels = self._normalize_labels(labels)
        with self._lock:
            if norm_labels not in self._data:
                return {
                    "buckets": {b: 0 for b in self.buckets},
                    "sum": 0.0,
                    "count": 0,
                }
            b_counts, total_sum, total_count = self._data[norm_labels]
            return {
                "buckets": dict(b_counts),
                "sum": total_sum,
                "count": total_count,
            }

    def export_series(self) -> Sequence[Tuple[Tuple[Tuple[str, str], ...], Dict[float, int], float, int]]:
        with self._lock:

            return [
                (labels, dict(b_counts), total_sum, total_count)
                for labels, (b_counts, total_sum, total_count) in self._data.items()
            ]


class PrometheusMetricsRegistry:
    """Registry maintaining collection of metrics and generating OpenMetrics output."""

    _DEFAULT_INSTANCE: Optional["PrometheusMetricsRegistry"] = None
    _CLASS_LOCK = threading.Lock()

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.Lock()

    @classmethod
    def get_default(cls) -> "PrometheusMetricsRegistry":
        with cls._CLASS_LOCK:
            if cls._DEFAULT_INSTANCE is None:
                reg = cls()
                reg._init_default_metrics()
                cls._DEFAULT_INSTANCE = reg
            return cls._DEFAULT_INSTANCE

    def _init_default_metrics(self) -> None:
        """Initialize core enterprise telemetry catalog."""
        self.counter(
            name="swarmwatch_spans_ingested_total",
            help_text="Total number of agent spans ingested by SwarmWatch",
            label_names=("project_id", "agent_type", "status"),
        )
        self.counter(
            name="swarmwatch_circuit_trips_total",
            help_text="Total number of circuit breaker trips halted by SwarmWatch",
            label_names=("project_id", "reason"),
        )
        self.counter(
            name="swarmwatch_tokens_saved_total",
            help_text="Estimated counterfactual tokens saved by circuit breaker",
            label_names=("project_id",),
        )
        self.counter(
            name="swarmwatch_dollars_saved_total",
            help_text="Estimated counterfactual dollar budget saved by circuit breaker",
            label_names=("project_id",),
        )
        self.counter(
            name="swarmwatch_state_snapshots_total",
            help_text="Total state snapshots recorded in time-machine DAG",
            label_names=("project_id",),
        )
        self.counter(
            name="swarmwatch_time_machine_forks_total",
            help_text="Total non-destructive branches created via time-machine forks",
            label_names=("project_id",),
        )
        self.gauge(
            name="swarmwatch_active_swarms_gauge",
            help_text="Current number of active multi-agent swarms",
        )
        self.gauge(
            name="swarmwatch_connected_websockets_gauge",
            help_text="Current number of connected WebSocket clients listening to console",
        )
        self.gauge(
            name="swarmwatch_telemetry_queue_depth_gauge",
            help_text="Current number of spans buffered in non-blocking telemetry queue",
        )
        self.histogram(
            name="swarmwatch_span_latency_seconds",
            help_text="Span processing latency distribution in seconds",
            label_names=("project_id", "status"),
            buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
        )
        self.histogram(
            name="swarmwatch_entropy_calculation_seconds",
            help_text="Shannon entropy calculation latency in seconds",
            label_names=("project_id",),
            buckets=(0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.005),
        )

    def counter(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
    ) -> Counter:
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, help_text, label_names)
            return self._counters[name]

    def gauge(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
    ) -> Gauge:
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, help_text, label_names)
            return self._gauges[name]

    def histogram(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = Histogram.DEFAULT_BUCKETS,
    ) -> Histogram:
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, help_text, label_names, buckets)
            return self._histograms[name]

    def export_openmetrics(self) -> str:
        """Export all registered metrics into Prometheus 0.0.4 text format."""
        lines = []

        # Export Counters
        with self._lock:
            all_counters = list(self._counters.values())
            all_gauges = list(self._gauges.values())
            all_histograms = list(self._histograms.values())

        for c in all_counters:
            lines.append(f"# HELP {c.name} {c.help_text}")
            lines.append(f"# TYPE {c.name} counter")
            samples = c.export_samples()
            if not samples:
                # If metric declared without samples yet, output empty default if no labels
                if not c.label_names:
                    lines.append(f"{c.name} 0")
            else:
                for name, labels, val in samples:
                    lbl_str = _format_labels(labels)
                    lines.append(f"{name}{lbl_str} {_format_val(val)}")

        for g in all_gauges:
            lines.append(f"# HELP {g.name} {g.help_text}")
            lines.append(f"# TYPE {g.name} gauge")
            samples = g.export_samples()
            if not samples:
                if not g.label_names:
                    lines.append(f"{g.name} 0")
            else:
                for name, labels, val in samples:
                    lbl_str = _format_labels(labels)
                    lines.append(f"{name}{lbl_str} {_format_val(val)}")

        for h in all_histograms:
            lines.append(f"# HELP {h.name} {h.help_text}")
            lines.append(f"# TYPE {h.name} histogram")
            series = h.export_series()
            if not series:
                # Default empty histogram
                pass
            else:
                for labels, b_counts, total_sum, total_count in series:
                    base_labels = dict(labels)
                    for b, count in sorted(b_counts.items(), key=lambda x: x[0]):
                        le_str = "+Inf" if b == float("inf") else _format_val(b)
                        bucket_labels = dict(base_labels)
                        bucket_labels["le"] = le_str
                        lbl_str = _format_labels(list(bucket_labels.items()))
                        lines.append(f"{h.name}_bucket{lbl_str} {count}")

                    sum_lbl_str = _format_labels(list(base_labels.items()))
                    lines.append(f"{h.name}_sum{sum_lbl_str} {_format_val(total_sum)}")
                    lines.append(f"{h.name}_count{sum_lbl_str} {total_count}")

        # Prometheus text format requires a trailing newline
        return "\n".join(lines) + "\n"



# ---------------------------------------------------------------------------
# High-Level Facade Helpers (Seam 8 Primitives)
# ---------------------------------------------------------------------------

def record_span_ingested(
    project_id: str = "default-project",
    agent_type: str = "agent",
    status: str = "ok",
    duration_seconds: float = 0.0,
) -> None:
    """Record a span ingestion event, updating counters and latency histograms."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.counter(
        "swarmwatch_spans_ingested_total",
        "Total number of agent spans ingested by SwarmWatch",
        ("project_id", "agent_type", "status"),
    ).inc(labels={"project_id": project_id, "agent_type": agent_type, "status": status})

    if duration_seconds > 0:
        reg.histogram(
            "swarmwatch_span_latency_seconds",
            "Span processing latency distribution in seconds",
            ("project_id", "status"),
        ).observe(duration_seconds, labels={"project_id": project_id, "status": status})


def record_circuit_trip(
    project_id: str = "default-project",
    reason: str = "entropy_collapse",
    tokens_saved: int = 0,
    dollars_saved: float = 0.0,
) -> None:
    """Record a circuit breaker trip with associated counterfactual savings."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.counter(
        "swarmwatch_circuit_trips_total",
        "Total number of circuit breaker trips halted by SwarmWatch",
        ("project_id", "reason"),
    ).inc(labels={"project_id": project_id, "reason": reason})

    if tokens_saved > 0:
        reg.counter(
            "swarmwatch_tokens_saved_total",
            "Estimated counterfactual tokens saved by circuit breaker",
            ("project_id",),
        ).inc(labels={"project_id": project_id}, value=tokens_saved)

    if dollars_saved > 0:
        reg.counter(
            "swarmwatch_dollars_saved_total",
            "Estimated counterfactual dollar budget saved by circuit breaker",
            ("project_id",),
        ).inc(labels={"project_id": project_id}, value=dollars_saved)


def record_state_snapshot(project_id: str = "default-project") -> None:
    """Record a captured state snapshot."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.counter(
        "swarmwatch_state_snapshots_total",
        "Total state snapshots recorded in time-machine DAG",
        ("project_id",),
    ).inc(labels={"project_id": project_id})


def record_branch_fork(project_id: str = "default-project") -> None:
    """Record a non-destructive state time-machine branch fork."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.counter(
        "swarmwatch_time_machine_forks_total",
        "Total non-destructive branches created via time-machine forks",
        ("project_id",),
    ).inc(labels={"project_id": project_id})


def set_active_swarms(count: int) -> None:
    """Set the number of currently active swarms."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.gauge("swarmwatch_active_swarms_gauge", "Current number of active multi-agent swarms").set(count)


def set_active_websockets(count: int) -> None:
    """Set the number of active WebSocket connections."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.gauge("swarmwatch_connected_websockets_gauge", "Current number of connected WebSocket clients").set(count)


def set_telemetry_queue_depth(depth: int) -> None:
    """Set the current telemetry queue depth."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.gauge("swarmwatch_telemetry_queue_depth_gauge", "Current number of spans buffered in queue").set(depth)


def record_sse_event_dispatched(event_type: str = "circuit_trip") -> None:
    """Record an SSE event dispatched to client streams."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.counter(
        "swarmwatch_sse_events_sent_total",
        "Total count of Server-Sent Events dispatched to subscribers",
        ("event_type",),
    ).inc(labels={"event_type": event_type})


def set_active_sse_clients(count: int) -> None:
    """Set the gauge of currently active SSE subscriber connections."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.gauge("swarmwatch_active_sse_clients", "Current number of active Server-Sent Events subscriber connections").set(count)


def record_webpush_dispatched(status: str = "success") -> None:
    """Record a WebPush notification dispatch attempt."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.counter(
        "swarmwatch_push_notifications_sent_total",
        "Total count of WebPush alert notifications dispatched",
        ("status",),
    ).inc(labels={"status": status})


def set_active_push_subscriptions(count: int) -> None:
    """Set the gauge of registered WebPush subscriptions."""
    reg = PrometheusMetricsRegistry.get_default()
    reg.gauge("swarmwatch_push_subscriptions_active", "Current count of active registered WebPush subscriptions").set(count)

