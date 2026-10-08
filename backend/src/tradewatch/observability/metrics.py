"""Prometheus metrics: every series this system exports, defined in one place.

One module rather than a metric next to each call site, so the full list of what can be
graphed is readable at a glance, names cannot drift between services, and a label set is
decided once. The dashboard in `observability/grafana/` and the benchmarks read these
exact names.

**Each process serves its own `/metrics` on its own port** (`DEFAULT_PORTS`, overridable
with `TW_METRICS_PORT`), bound to `TW_METRICS_HOST` — loopback by default. The API does not
add `/metrics` to its public router: `ARCHITECTURE.md` §8.1 says the endpoint is not public,
and a separate listener is how that stays true without an auth rule to forget. Prometheus
tells the processes apart by its scrape job, so no metric carries a `service` label.

**What is deliberately not here:**

- *Consumer-group lag.* A consumer cannot report its own lag at the moment it matters most —
  when it is dead. Lag is measured from outside, by `kafka-exporter` in the Compose stack
  (`kafka_consumergroup_lag`), which reads committed offsets from the broker itself.
- *Per-symbol or per-user labels.* Cardinality is bounded by design: labels are event
  types, consumer groups, route templates, cache kinds — sets that grow with code, not data.
"""

from __future__ import annotations

from typing import Final

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from tradewatch.config.settings import AppSettings

__all__ = [
    "ALERTS_DISPATCHED",
    "ANOMALIES_DETECTED",
    "BATCH_DURATION",
    "CACHE_LOOKUPS",
    "DB_POOL_CONNECTIONS",
    "DEFAULT_PORTS",
    "EVENTS_CONSUMED",
    "EVENTS_MALFORMED",
    "EVENTS_PUBLISHED",
    "EVENT_DELIVERY",
    "HTTP_REQUESTS",
    "HTTP_REQUEST_DURATION",
    "LOG_EVENTS",
    "ORDERS",
    "PROVIDER_LAG",
    "SIGNALS",
    "WEBSOCKET_CONNECTIONS",
    "start_metrics_server",
]

# structlog directly, not `observability.logging.get_logger`: logging imports this module for
# `LOG_EVENTS`, and the reverse import would be a cycle.
log = structlog.get_logger(__name__)

#: One port per process, so every service can run on one machine at once. 9308 is
#: kafka-exporter's own default and is left free for it.
DEFAULT_PORTS: Final[dict[str, int]] = {
    "api": 9310,
    "producer": 9311,
    "consumer": 9312,
    "aggregator": 9313,
    "broadcaster": 9314,
    "backtest-runner": 9315,
    "paper-trading": 9316,
    "anomaly-detector": 9317,
    "alert-dispatcher": 9318,
}

#: Seconds. Spans a sub-millisecond in-memory step to a multi-second stall, so a p99 in the
#: tail lands in a bucket rather than in +Inf.
_LATENCY_BUCKETS: Final = (
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
)  # fmt: skip

#: Delivery delay can be a backlog hours deep after an outage — a consumer resuming from its
#: committed offset — and a top bucket of 30 s would report all of it as "30 s".
_DELIVERY_BUCKETS: Final = (*_LATENCY_BUCKETS, 60.0, 300.0, 900.0, 3600.0, 4 * 3600.0, 24 * 3600.0)

# --- Event flow ----------------------------------------------------------------------------

EVENTS_PUBLISHED = Counter(
    "tradewatch_events_published_total",
    "Events the broker acknowledged, by event type. rate() of this is ingest events/sec.",
    ["event_type"],
)
EVENTS_CONSUMED = Counter(
    "tradewatch_events_consumed_total",
    "Events decoded by a consumer group, by event type.",
    ["group", "event_type"],
)
EVENTS_MALFORMED = Counter(
    "tradewatch_events_malformed_total",
    "Records a consumer group could not decode (quarantined or dropped).",
    ["group"],
)
PROVIDER_LAG = Histogram(
    "tradewatch_provider_lag_seconds",
    "occurred_at -> ingested_at at the producer: how far behind the venue ingestion runs.",
    buckets=_LATENCY_BUCKETS,
)
EVENT_DELIVERY = Histogram(
    "tradewatch_event_delivery_seconds",
    "ingested_at -> decoded by this consumer group: producer, broker and poll delay together.",
    ["group"],
    buckets=_DELIVERY_BUCKETS,
)
BATCH_DURATION = Histogram(
    "tradewatch_stage_duration_seconds",
    "Time a consumer group's handler spent on one polled batch — its processing stage.",
    ["group"],
    buckets=_LATENCY_BUCKETS,
)

# --- HTTP, cache, WebSocket, database --------------------------------------------------------

HTTP_REQUESTS = Counter(
    "tradewatch_http_requests_total",
    "HTTP requests by route template (never the raw path) and status code.",
    ["method", "route", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "tradewatch_http_request_duration_seconds",
    "HTTP request duration by route template, including middleware.",
    ["method", "route"],
    buckets=_LATENCY_BUCKETS,
)
CACHE_LOOKUPS = Counter(
    "tradewatch_cache_lookups_total",
    "Read-through cache lookups by cache kind and result (hit or miss).",
    ["cache", "result"],
)
WEBSOCKET_CONNECTIONS = Gauge(
    "tradewatch_websocket_connections",
    "Open WebSocket connections by channel.",
    ["channel"],
)
DB_POOL_CONNECTIONS = Gauge(
    "tradewatch_db_pool_connections",
    "SQLAlchemy pool connections by state: checked_out, idle, overflow, size.",
    ["state"],
)

# --- Domain outcomes -------------------------------------------------------------------------

SIGNALS = Counter(
    "tradewatch_strategy_signals_total",
    "Signals a live paper-trading deployment's strategy produced.",
)
ORDERS = Counter(
    "tradewatch_orders_total",
    "Paper-trading orders by outcome (filled or rejected).",
    ["outcome"],
)
ANOMALIES_DETECTED = Counter(
    "tradewatch_anomalies_detected_total",
    "Bars the detector scored as statistically unusual, by severity.",
    ["severity"],
)
ALERTS_DISPATCHED = Counter(
    "tradewatch_alerts_dispatched_total",
    "Alerts raised and stored for a user, by triggering event type.",
    ["event_type"],
)

# --- Errors ----------------------------------------------------------------------------------

LOG_EVENTS = Counter(
    "tradewatch_log_events_total",
    "Log lines at warning level and above. rate() of the error series is the error rate.",
    ["level"],
)


def start_metrics_server(process: str, settings: AppSettings) -> int | None:
    """Serve this process's `/metrics`. Returns the port, or None when switched off.

    A port that is already taken is logged and survived rather than fatal: losing a
    process's metrics is worse than losing nothing, but taking a worker down over its
    exporter would turn an observability problem into an outage.
    """
    if not settings.metrics_enabled:
        return None
    port = settings.metrics_port or DEFAULT_PORTS[process]
    try:
        start_http_server(port, addr=settings.metrics_host)
    except OSError as error:
        log.error("metrics.server_failed", process=process, port=port, error=str(error))
        return None
    log.info("metrics.serving", process=process, host=settings.metrics_host, port=port)
    return port
