"""The Kafka consumer adapter: polling, decoding, quarantining, committing.

Two behaviours here matter more than the rest.

**Auto-commit is off.** A test asserts it, because the setting is one word and turning it on
silently converts at-least-once delivery into at-most-once — losing exactly the data that was
in flight when a consumer died.

**A poll where every record is malformed still advances.** The batch handed up is empty, so
the application will not commit it, and without the adapter committing those offsets itself
the same poison records would be redelivered on every poll forever. It is the kind of loop
that shows up as "the consumer is running and the row count never changes".
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from aiokafka.errors import CommitFailedError, KafkaConnectionError

from tests.kafka_support import (
    FakeConsumerClient,
    FakeProducerClient,
    FakeRecord,
    bar,
    kafka_settings,
    quote,
    trade,
)
from tradewatch.domain.events import MarketEvent
from tradewatch.infrastructure.events import EventBusError
from tradewatch.infrastructure.events.envelope import encode
from tradewatch.infrastructure.events.kafka import (
    BrokerUnavailableError,
    ConsumerEvictedError,
    KafkaDeadLetterSink,
    KafkaEventConsumer,
)

pytestmark = pytest.mark.unit


def record(event: object = None, *, offset: int = 0, value: bytes | None = None) -> FakeRecord:
    payload = value if value is not None else encode(event)  # type: ignore[arg-type]
    return FakeRecord(value=payload, offset=offset, key=b"AAPL")


async def first_batches(consumer: KafkaEventConsumer, count: int) -> list[list[MarketEvent]]:
    """Take ``count`` batches from a consumer, then stop."""
    taken: list[list[MarketEvent]] = []
    async for batch in consumer.batches():
        taken.append(list(batch))
        if len(taken) >= count:
            break
    return taken


def build(
    polls: list[list[FakeRecord]] | None = None,
    *,
    dlq_client: FakeProducerClient | None = None,
    client: FakeConsumerClient | None = None,
    **overrides: Any,
) -> tuple[KafkaEventConsumer, FakeConsumerClient, FakeProducerClient]:
    settings = kafka_settings(**overrides)
    consumer_client = client or FakeConsumerClient(polls or [])
    producer_client = dlq_client or FakeProducerClient()
    sink = KafkaDeadLetterSink(settings, client=producer_client)
    return (
        KafkaEventConsumer(settings, client=consumer_client, dead_letters=sink),
        consumer_client,
        producer_client,
    )


class TestLifecycle:
    async def test_start_subscribes(self) -> None:
        consumer, client, _ = build()

        await consumer.start()

        assert client.started is True
        assert consumer.started is True

    async def test_start_failure_is_reported_as_broker_unavailable(self) -> None:
        client = FakeConsumerClient([], start_error=KafkaConnectionError("no broker"))
        consumer, _, _ = build(client=client)

        with pytest.raises(BrokerUnavailableError) as caught:
            await consumer.start()

        assert "kafka.invalid:9092" in str(caught.value)

    async def test_polling_before_start_is_refused(self) -> None:
        consumer, _, _ = build()

        with pytest.raises(BrokerUnavailableError, match="not started"):
            await first_batches(consumer, 1)

    async def test_stop_does_not_commit(self) -> None:
        """Anything uncommitted was unfinished. Committing to look tidy loses it."""
        consumer, client, _ = build([[record(trade())]])
        await consumer.start()
        await first_batches(consumer, 1)

        await consumer.stop()

        assert client.commits == []
        assert client.stopped is True

    async def test_the_group_id_comes_from_settings(self) -> None:
        consumer, _, _ = build(kafka_consumer_group="processor-persist")

        assert consumer.group_id == "processor-persist"


class TestDecoding:
    async def test_a_valid_record_becomes_a_domain_event(self) -> None:
        original = trade()
        consumer, _, _ = build([[record(original)]])
        await consumer.start()

        batches = await first_batches(consumer, 1)

        assert batches[0] == [original]

    async def test_a_batch_of_mixed_types_decodes(self) -> None:
        events = [trade(), quote(), bar()]
        consumer, _, _ = build([[record(event, offset=i) for i, event in enumerate(events)]])
        await consumer.start()

        batches = await first_batches(consumer, 1)

        assert batches[0] == events

    async def test_an_empty_poll_yields_an_empty_batch(self) -> None:
        """Yielded, not swallowed, so the caller gets a turn to notice a shutdown."""
        consumer, _, _ = build([])
        await consumer.start()

        batches = await first_batches(consumer, 1)

        assert batches[0] == []
        assert consumer.stats.records == 0

    async def test_decoded_records_are_counted(self) -> None:
        consumer, _, _ = build([[record(trade(), offset=0), record(quote(), offset=1)]])
        await consumer.start()

        await first_batches(consumer, 1)

        assert consumer.stats.records == 2
        assert consumer.stats.decoded == 2
        assert consumer.stats.malformed == 0


class TestMalformedMessages:
    async def test_a_malformed_record_is_quarantined_and_skipped(self) -> None:
        good = trade()
        consumer, _, dlq = build([[record(value=b"not json", offset=0), record(good, offset=1)]])
        await consumer.start()

        batches = await first_batches(consumer, 1)

        assert batches[0] == [good]
        assert consumer.stats.malformed == 1
        assert consumer.stats.dead_lettered == 1
        assert dlq.topics() == ["tw.dlq.v1"]

    async def test_the_original_bytes_are_preserved_verbatim(self) -> None:
        """A repaired or truncated copy destroys the only evidence worth having."""
        consumer, _, dlq = build([[record(value=b"\x00\x01 broken", offset=7)]])
        await consumer.start()

        await first_batches(consumer, 1)

        assert dlq.values() == [b"\x00\x01 broken"]

    async def test_the_dlq_record_says_where_the_message_came_from(self) -> None:
        consumer, _, dlq = build([[record(value=b"nope", offset=42)]])
        await consumer.start()

        await first_batches(consumer, 1)

        headers = dlq.headers_of()
        assert headers["tw-dlq-source-topic"] == b"tw.market.trades.v1"
        assert headers["tw-dlq-source-offset"] == b"42"
        assert headers["tw-dlq-reason"] == b"not_json"
        assert headers["tw-dlq-consumer-group"] == b"processor-persist"
        assert "tw-dlq-failed-at" in headers

    async def test_the_original_key_is_kept_so_partitioning_is_preserved(self) -> None:
        consumer, _, dlq = build([[record(value=b"nope")]])
        await consumer.start()

        await first_batches(consumer, 1)

        assert dlq.keys() == [b"AAPL"]

    async def test_each_decode_failure_reason_reaches_the_dlq(self) -> None:
        payload = json.loads(encode(trade()))
        payload["schema_version"] = 99
        consumer, _, dlq = build([[record(value=json.dumps(payload).encode())]])
        await consumer.start()

        await first_batches(consumer, 1)

        assert dlq.headers_of()["tw-dlq-reason"] == b"unsupported_schema_version"

    async def test_with_the_dlq_disabled_the_message_is_counted_and_dropped(self) -> None:
        consumer, _, dlq = build([[record(value=b"nope")]], dlq_enabled=False)
        await consumer.start()

        await first_batches(consumer, 1)

        assert consumer.stats.dropped == 1
        assert consumer.stats.dead_lettered == 0
        assert dlq.sent == []

    async def test_a_failed_dlq_write_is_not_swallowed(self) -> None:
        """Acknowledging a message that was neither stored nor quarantined loses it."""
        dlq_client = FakeProducerClient(send_errors=[KafkaConnectionError("down")])
        consumer, _, _ = build([[record(value=b"nope")]], dlq_client=dlq_client)
        await consumer.start()

        with pytest.raises(BrokerUnavailableError):
            await first_batches(consumer, 1)


class TestPoisonPoll:
    async def test_a_poll_of_only_malformed_records_commits_itself(self) -> None:
        """Otherwise those offsets are redelivered on every poll, forever."""
        consumer, client, _ = build([[record(value=b"a"), record(value=b"b", offset=1)]])
        await consumer.start()

        batches = await first_batches(consumer, 1)

        assert batches[0] == []
        assert client.commits == [2], "the poison records' offsets were committed"

    async def test_a_partially_malformed_poll_does_not_self_commit(self) -> None:
        """There are good events in it, so the application commits after storing them."""
        consumer, client, _ = build([[record(value=b"a"), record(trade(), offset=1)]])
        await consumer.start()

        await first_batches(consumer, 1)

        assert client.commits == []

    async def test_an_empty_poll_does_not_commit(self) -> None:
        consumer, client, _ = build([])
        await consumer.start()

        await first_batches(consumer, 1)

        assert client.commits == []


class TestCommitting:
    async def test_commit_delegates_to_the_client(self) -> None:
        consumer, client, _ = build([[record(trade())]])
        await consumer.start()
        await first_batches(consumer, 1)

        await consumer.commit()

        assert client.commits == [1]
        assert consumer.stats.commits == 1

    async def test_an_eviction_is_not_reported_as_a_broker_problem(self) -> None:
        """`CommitFailedError` means this process stalled, not that Kafka is down.

        Observed for real: the service set was reaped under memory pressure, both consumers
        were evicted four seconds apart and both correctly refused to commit — and both
        reported `BrokerUnavailableError`, which sends whoever is on call to inspect a
        broker that was healthy the whole time.
        """
        client = FakeConsumerClient([], commit_error=CommitFailedError())
        consumer, _, _ = build(client=client)
        await consumer.start()

        with pytest.raises(ConsumerEvictedError) as caught:
            await consumer.commit()

        assert not isinstance(caught.value, BrokerUnavailableError)
        assert caught.value.retryable, "restarting resolves it; the events come back"
        assert consumer.stats.commits == 0

    async def test_any_other_commit_failure_is_still_a_broker_problem(self) -> None:
        """The new class is scoped to eviction. Everything else keeps its old meaning."""
        client = FakeConsumerClient([], commit_error=KafkaConnectionError("broker gone"))
        consumer, _, _ = build(client=client)
        await consumer.start()

        with pytest.raises(BrokerUnavailableError):
            await consumer.commit()

    async def test_a_failed_commit_is_reported_not_ignored(self) -> None:
        """A silently failed commit means silent reprocessing, which is at least honest —
        and a silently *successful*-looking one that did nothing is much worse."""
        client = FakeConsumerClient([], commit_error=CommitFailedError())
        consumer, _, _ = build(client=client)
        await consumer.start()

        # `EventBusError` rather than a specific subclass: this test is about the general
        # property — a commit that failed must raise and must not be counted — and the two
        # tests above pin which subclass each cause produces.
        with pytest.raises(EventBusError):
            await consumer.commit()

        assert consumer.stats.commits == 0


class TestConfiguration:
    async def test_the_batch_size_is_the_poll_limit(self) -> None:
        captured: dict[str, int] = {}

        class _Recording(FakeConsumerClient):
            async def getmany(self, *, timeout_ms: int, max_records: int):
                captured["max_records"] = max_records
                captured["timeout_ms"] = timeout_ms
                return await super().getmany(timeout_ms=timeout_ms, max_records=max_records)

        consumer, _, _ = build(
            client=_Recording([]), consumer_batch_size=37, kafka_consumer_poll_timeout_ms=250
        )
        await consumer.start()

        await first_batches(consumer, 1)

        assert captured == {"max_records": 37, "timeout_ms": 250}

    async def test_auto_commit_is_off_in_the_real_client(self) -> None:
        """The one setting that would quietly convert at-least-once into at-most-once.

        Async because ``AIOKafkaConsumer`` insists on being constructed inside a running
        loop. It is only constructed here, never started, so no broker is involved.
        """
        consumer = KafkaEventConsumer(kafka_settings(kafka_bootstrap="localhost:9092"))
        client = consumer._build_client()
        try:
            assert client._enable_auto_commit is False  # type: ignore[attr-defined]
        finally:
            # Never started, but aiokafka's finaliser complains about an unclosed consumer
            # and `filterwarnings = error` turns that into a failure somewhere unrelated.
            await client.stop()

    async def test_the_real_client_subscribes_to_the_market_topics_only(self) -> None:
        consumer = KafkaEventConsumer(kafka_settings(kafka_bootstrap="localhost:9092"))
        client = consumer._build_client()
        try:
            assert set(client._client._topics) == {  # type: ignore[attr-defined]
                "tw.market.trades.v1",
                "tw.market.quotes.v1",
                "tw.market.bars.1m.v1",
            }
        finally:
            await client.stop()
