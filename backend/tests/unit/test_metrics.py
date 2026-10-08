"""Metrics are emitted *and readable by a scraper* — not merely incremented without error.

Each test drives the real code path (the Kafka adapters with their fake clients, the
logging pipeline) and then reads the result back the way Prometheus does: an HTTP GET of
`/metrics` from the exporter `start_metrics_server` starts, parsed with the reference
exposition-format parser. A metric that is incremented but registered wrongly, renamed,
or given a label the dashboard does not query fails here, not on a blank Grafana panel.

Counters are process-global, so every assertion is on the *change* across the action.
"""

from __future__ import annotations

import socket
import urllib.request
from collections.abc import Callable

import pytest
import structlog
from prometheus_client.parser import text_string_to_metric_families

from tests.kafka_support import FakeConsumerClient, FakeProducerClient, FakeRecord, kafka_settings
from tests.kafka_support import trade as make_trade
from tradewatch.config.settings import AppSettings
from tradewatch.infrastructure.events.envelope import encode
from tradewatch.infrastructure.events.kafka import KafkaEventConsumer, KafkaEventPublisher
from tradewatch.observability.logging import configure_logging, get_logger
from tradewatch.observability.metrics import start_metrics_server

pytestmark = pytest.mark.unit

Samples = dict[tuple[str, tuple[tuple[str, str], ...]], float]
Scrape = Callable[[], Samples]


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


@pytest.fixture(scope="module")
def scrape() -> Scrape:
    """Start one real exporter for the module and return a function that scrapes it."""
    port = start_metrics_server(
        "consumer", AppSettings(metrics_host="127.0.0.1", metrics_port=_free_port())
    )
    assert port is not None

    def _scrape() -> Samples:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as reply:
            body = reply.read().decode()
        return {
            (sample.name, tuple(sorted(sample.labels.items()))): sample.value
            for family in text_string_to_metric_families(body)
            for sample in family.samples
        }

    return _scrape


def _value(samples: Samples, name: str, **labels: str) -> float:
    return float(samples.get((name, tuple(sorted(labels.items()))), 0.0))


def _delta(before: Samples, after: Samples, name: str, **labels: str) -> float:
    return _value(after, name, **labels) - _value(before, name, **labels)


async def test_consumed_events_delivery_latency_and_batch_duration_are_scraped(
    scrape: Scrape,
) -> None:
    group = "metrics-test-consume"
    records = [
        FakeRecord(value=encode(make_trade()), offset=0, key=b"AAPL"),
        FakeRecord(value=encode(make_trade(trade_id="t-2")), offset=1, key=b"AAPL"),
    ]
    consumer = KafkaEventConsumer(
        kafka_settings(), client=FakeConsumerClient([records]), group_id=group
    )
    await consumer.start()
    before = scrape()

    batches = consumer.batches()
    assert len(await anext(batches)) == 2
    await anext(batches)  # resuming the generator closes the first batch's stage timing
    after = scrape()

    consumed = "tradewatch_events_consumed_total"
    assert _delta(before, after, consumed, group=group, event_type="market.trade") == 2
    assert _delta(before, after, "tradewatch_event_delivery_seconds_count", group=group) == 2
    assert _delta(before, after, "tradewatch_stage_duration_seconds_count", group=group) == 1


async def test_a_malformed_record_is_counted_for_its_group(scrape: Scrape) -> None:
    group = "metrics-test-malformed"
    consumer = KafkaEventConsumer(
        kafka_settings(dlq_enabled=False),
        client=FakeConsumerClient([[FakeRecord(value=b"not json", offset=0, key=b"AAPL")]]),
        group_id=group,
    )
    await consumer.start()
    before = scrape()

    await anext(consumer.batches())

    assert _delta(before, scrape(), "tradewatch_events_malformed_total", group=group) == 1


async def test_published_events_are_counted_by_type(scrape: Scrape) -> None:
    publisher = KafkaEventPublisher(kafka_settings(), client=FakeProducerClient())
    await publisher.start()
    before = scrape()

    await publisher.publish(make_trade())
    await publisher.publish_many([make_trade(trade_id="t-3"), make_trade(trade_id="t-4")])

    name = "tradewatch_events_published_total"
    assert _delta(before, scrape(), name, event_type="market.trade") == 3


async def test_the_consumer_binds_the_batch_correlation_id_into_log_context() -> None:
    event = make_trade()
    consumer = KafkaEventConsumer(
        kafka_settings(),
        client=FakeConsumerClient([[FakeRecord(value=encode(event), offset=0, key=b"AAPL")]]),
    )
    await consumer.start()
    structlog.contextvars.clear_contextvars()

    await anext(consumer.batches())

    bound = structlog.contextvars.get_contextvars()
    assert bound["correlation_id"] == str(event.correlation_id)
    structlog.contextvars.clear_contextvars()


def test_warning_and_error_log_lines_feed_the_error_rate(scrape: Scrape) -> None:
    configure_logging(level="INFO")
    before = scrape()

    get_logger("metrics-test").error("metrics_test.failed")
    get_logger("metrics-test").warning("metrics_test.degraded")
    get_logger("metrics-test").info("metrics_test.fine")

    after = scrape()
    name = "tradewatch_log_events_total"
    assert _delta(before, after, name, level="error") == 1
    assert _delta(before, after, name, level="warning") == 1
    assert _value(after, name, level="info") == 0  # info is not an error signal


def test_the_exporter_is_off_when_metrics_are_disabled() -> None:
    assert start_metrics_server("consumer", AppSettings(metrics_enabled=False)) is None


def test_a_taken_port_is_survived_not_fatal() -> None:
    """Losing a worker over its exporter would turn an observability problem into an outage."""
    with socket.socket() as squatter:
        squatter.bind(("127.0.0.1", 0))
        squatter.listen()
        port = squatter.getsockname()[1]

        assert (
            start_metrics_server(
                "consumer", AppSettings(metrics_host="127.0.0.1", metrics_port=port)
            )
            is None
        )


async def test_a_consumer_periodically_logs_progress_under_the_batch_correlation_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The line that makes every consumer group's log name the id it is handling.

    The module's logger is replaced rather than captured: structlog caches a logger on first
    use, so `capture_logs` cannot intercept one an earlier test already configured.
    """
    from tradewatch.infrastructure.events.kafka import consumer as consumer_module

    lines: list[dict[str, object]] = []

    class _Recorder:
        def info(self, event: str, **fields: object) -> None:
            # Read the context at call time, exactly as `merge_contextvars` would.
            lines.append({"event": event, **structlog.contextvars.get_contextvars(), **fields})

        warning = error = info

    monkeypatch.setattr(consumer_module, "log", _Recorder())
    monkeypatch.setattr(consumer_module, "_PROGRESS_EVERY_SECONDS", 0.0)
    event = make_trade()
    consumer = KafkaEventConsumer(
        kafka_settings(),
        client=FakeConsumerClient([[FakeRecord(value=encode(event), offset=0, key=b"AAPL")]]),
    )
    await consumer.start()

    await anext(consumer.batches())

    progress = [line for line in lines if line["event"] == "kafka.consumer_progress"]
    assert progress and progress[0]["correlation_id"] == str(event.correlation_id)
    structlog.contextvars.clear_contextvars()
