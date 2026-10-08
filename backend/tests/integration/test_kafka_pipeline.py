"""The Phase 2 path, end to end, against a real broker and a real database.

```
Provider → Kafka → Consumer → MarketDataProcessor → PostgreSQL
```

These tests exist to settle the questions a fake cannot. A fake consumer can be *made* to
redeliver a message; only a real broker decides whether it actually does. A fake store can
be *told* a row already exists; only PostgreSQL enforces `UNIQUE (event_id)`. So what is
asserted here is deliberately narrow and expensive to fake:

* the exact OHLCV and price values that come out of the database are the ones that went into
  the producer — bar by bar, not a row count, because a row count passes on data that was
  already there;
* reprocessing the same messages writes nothing new;
* a restarted consumer resumes from its committed offset and does not re-read what it
  finished;
* a consumer that was offline catches up;
* a database failure leaves the offset **uncommitted**, so the events come back;
* a malformed message lands on the dead-letter topic with its original bytes, and the
  partition keeps moving.

**Isolation.** Every test class gets its own topic prefix and its own consumer group, so
runs cannot see each other's messages and a rerun is not affected by the last one. The
topics are deleted afterwards.

Skips cleanly when there is no broker, exactly as the PostgreSQL fixtures do: a developer
without Docker running should get one clear sentence, not a wall of connection errors.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import os
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from tradewatch.application.processing import (
    MarketDataConsumerService,
    MarketDataProcessor,
    StorageError,
)
from tradewatch.config.settings import KafkaSettings
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, MarketEvent, TradeEvent
from tradewatch.infrastructure.database.market_store import SqlMarketDataStore
from tradewatch.infrastructure.database.models import Bar, Instrument, Trade
from tradewatch.infrastructure.events.kafka import (
    BrokerUnavailableError,
    KafkaDeadLetterSink,
    KafkaEventConsumer,
    KafkaEventPublisher,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.infrastructure.providers.normalization import EventFactory

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# Read before the root `clean_env` fixture strips every TW_ variable.
BOOTSTRAP = os.environ.get("TW_KAFKA_BOOTSTRAP") or "localhost:9092"

SKIP_REASON = (
    "No Kafka broker. Start one with "
    "`docker compose -f docker/docker-compose.yml up -d kafka`, or set TW_KAFKA_BOOTSTRAP."
)

#: Symbols used only here, so committed rows cannot collide with another test's.
SYMBOLS = ("KFKA", "KFKB")

MOMENT = dt.datetime(2026, 4, 1, 15, 0, tzinfo=dt.UTC)


# --- fixtures --------------------------------------------------------------


async def _broker_reachable() -> bool:
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=3000)
    try:
        await admin.start()
    except Exception:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            await admin.close()


async def _wait_until_topics_visible(
    bootstrap: str, wanted: Sequence[str], timeout_s: float = 30.0
) -> None:
    """Poll until `wanted` all appear in `list_topics()`, the same "wait for a real
    condition, not a guessed delay" rule `TestBrokerOutage._wait_for_broker` and
    `scripts/wait_for_kafka_metadata.py` follow for this exact broker behaviour."""
    if not wanted:
        return
    remaining = set(wanted)
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, client_id="settle-probe")
        await admin.start()
        try:
            remaining -= set(await admin.list_topics())
        finally:
            await admin.close()
        if not remaining:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"topic metadata never caught up: still missing {remaining}")
        await asyncio.sleep(0.5)


@pytest_asyncio.fixture(scope="session")
async def kafka() -> AsyncIterator[str]:
    """The bootstrap address, or a skip with an explanation."""
    if not await _broker_reachable():
        pytest.skip(SKIP_REASON)
    yield BOOTSTRAP


@pytest_asyncio.fixture
async def prefix(kafka: str) -> AsyncIterator[str]:
    """A topic namespace unique to one test, deleted afterwards.

    Without this, a rerun would consume the previous run's messages and a passing test
    would mean nothing.
    """
    name = f"twt{uuid.uuid4().hex[:10]}"
    yield name

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka)
    with contextlib.suppress(Exception):
        await admin.start()
        await admin.delete_topics(list(Topics(name).all))
        # The groups too. Deleting only the topics leaves a dead group per test behind,
        # which accumulates in `kafka-consumer-groups --list` until the broker expires it
        # days later — harmless, and enough noise to hide a real group.
        groups = [
            name_
            for name_, _ in await admin.list_consumer_groups()
            if name_.startswith(("test-", "agg-", "persist-", "replay-", "reader-"))
        ]
        if groups:
            await admin.delete_consumer_groups(groups)
    with contextlib.suppress(Exception):
        await admin.close()


@pytest.fixture
def kafka_config(prefix: str, kafka: str) -> Callable[..., KafkaSettings]:
    """Kafka settings pointed at the real broker and this test's topics."""

    def build(**overrides: Any) -> KafkaSettings:
        defaults: dict[str, Any] = {
            "kafka_bootstrap": kafka,
            "kafka_topic_prefix": prefix,
            "kafka_topic_partitions": 3,
            "kafka_topic_replication": 1,
            "kafka_consumer_group": f"test-{uuid.uuid4().hex[:8]}",
            # Short polls so a bounded test does not spend its life waiting.
            "kafka_consumer_poll_timeout_ms": 250,
            "consumer_batch_size": 50,
            "consumer_retry_backoff_s": 0.05,
        }
        return KafkaSettings(**(defaults | overrides))

    return build


@pytest_asyncio.fixture
async def committing_store(engine: AsyncEngine) -> AsyncIterator[SqlMarketDataStore]:
    """The real store, writing real committed rows.

    The `session` fixture used by the rest of the integration suite wraps each test in a
    transaction it rolls back, which cannot work here: the consumer opens its own sessions,
    so it would never see — and a test would never see — what it wrote. These tests commit
    and clean up after themselves instead.
    """
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield SqlMarketDataStore(factory)

    async with engine.begin() as connection:
        await connection.execute(
            text(
                "DELETE FROM trades WHERE instrument_id IN "
                "(SELECT id FROM instruments WHERE symbol = ANY(:symbols))"
            ),
            {"symbols": list(SYMBOLS)},
        )
        await connection.execute(
            text(
                "DELETE FROM quotes WHERE instrument_id IN "
                "(SELECT id FROM instruments WHERE symbol = ANY(:symbols))"
            ),
            {"symbols": list(SYMBOLS)},
        )
        await connection.execute(
            text(
                "DELETE FROM bars WHERE instrument_id IN "
                "(SELECT id FROM instruments WHERE symbol = ANY(:symbols))"
            ),
            {"symbols": list(SYMBOLS)},
        )
        await connection.execute(
            text("DELETE FROM instruments WHERE symbol = ANY(:symbols)"),
            {"symbols": list(SYMBOLS)},
        )


# --- helpers ---------------------------------------------------------------


def factory() -> EventFactory:
    return EventFactory(source="itest", producer="itest@1.0", clock=lambda: MOMENT)


def trades(count: int, *, symbol: str = "KFKA") -> list[TradeEvent]:
    """Distinct trades with known, checkable prices."""
    return [
        factory().trade(
            symbol=symbol,
            price=Decimal(f"{100 + index}.{index:02d}"),
            size=Decimal(f"{index + 1}"),
            occurred_at=MOMENT + dt.timedelta(seconds=index),
            provider_trade_id=f"{symbol}-{index}",
        )
        for index in range(count)
    ]


def bars(count: int, *, symbol: str = "KFKA") -> list[BarEvent]:
    return [
        factory().bar(
            symbol=symbol,
            interval=BarInterval.ONE_MINUTE,
            bar_start=MOMENT + dt.timedelta(minutes=index),
            open=Decimal(f"{10 + index}.10"),
            high=Decimal(f"{10 + index}.90"),
            low=Decimal(f"{10 + index}.00"),
            close=Decimal(f"{10 + index}.50"),
            volume=Decimal(f"{1000 + index}"),
            trade_count=index + 1,
        )
        for index in range(count)
    ]


async def publish(config: KafkaSettings, events: Sequence[MarketEvent]) -> None:
    """Publish events and wait for the broker to acknowledge every one."""
    publisher = KafkaEventPublisher(config)
    await publisher.start()
    try:
        accepted = await publisher.publish_many(events)
        assert accepted == len(events), "the broker did not acknowledge every record"
        await publisher.flush()
    finally:
        await publisher.stop()


async def consume(
    config: KafkaSettings,
    store: Any,
    *,
    expect: int,
    timeout_s: float = 30.0,
    dlq: bool = True,
) -> MarketDataConsumerService:
    """Run a consumer until ``expect`` events have been handled, then stop it cleanly.

    Bounded by a timeout so a broken pipeline fails as a test failure rather than a hung
    suite.
    """
    dlq_producer: AIOKafkaProducer | None = None
    sink: KafkaDeadLetterSink | None = None
    if dlq:
        # The same call the worker makes, so a broker outage is classified here exactly as
        # it is in production rather than surfacing as a raw aiokafka error.
        sink, dlq_producer = await open_dead_letter_sink(config)

    consumer = KafkaEventConsumer(config, dead_letters=sink)
    processor = MarketDataProcessor(
        store,
        batch_size=config.consumer_batch_size,
        max_storage_attempts=1,
        retry_delay=0.05,
    )
    service = MarketDataConsumerService(consumer, processor, dead_letters=sink)
    stop = asyncio.Event()

    await consumer.start()
    task = asyncio.create_task(service.run(stop=stop))
    try:
        async with asyncio.timeout(timeout_s):
            while not task.done():
                handled = service.stats.events + consumer.stats.malformed
                if handled >= expect:
                    break
                await asyncio.sleep(0.05)
        stop.set()
        async with asyncio.timeout(timeout_s):
            await task
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        raise
    finally:
        await consumer.stop()
        if dlq_producer is not None:
            with contextlib.suppress(Exception):
                await dlq_producer.stop()

    return service


async def _snapshot(engine: AsyncEngine) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    """Everything about the stored data that reprocessing must not change.

    Values, not counts: a count is unchanged by a row being silently rewritten with
    different numbers, which is exactly the bug worth catching.
    """
    async with engine.connect() as connection:
        trade_rows = (
            await connection.execute(
                text(
                    "SELECT t.event_id, t.price, t.size, t.recorded_at FROM trades t "
                    "JOIN instruments i ON i.id = t.instrument_id "
                    "WHERE i.symbol = ANY(:symbols) ORDER BY t.event_id"
                ),
                {"symbols": list(SYMBOLS)},
            )
        ).all()
        bar_rows = (
            await connection.execute(
                text(
                    "SELECT b.bar_start, b.open, b.high, b.low, b.close, b.volume, b.revision "
                    "FROM bars b JOIN instruments i ON i.id = b.instrument_id "
                    "WHERE i.symbol = ANY(:symbols) ORDER BY b.bar_start"
                ),
                {"symbols": list(SYMBOLS)},
            )
        ).all()
    return (tuple(trade_rows), tuple(bar_rows))


async def read_topic(
    bootstrap: str, topic: str, *, expect: int, timeout_s: float = 20.0
) -> list[Any]:
    """Read the first ``expect`` records off a topic with a throwaway group."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=f"reader-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    collected: list[Any] = []
    try:
        async with asyncio.timeout(timeout_s):
            while len(collected) < expect:
                fetched = await consumer.getmany(timeout_ms=250, max_records=expect)
                for messages in fetched.values():
                    collected.extend(messages)
    finally:
        await consumer.stop()
    return collected


# --- the tests -------------------------------------------------------------


class TestEndToEnd:
    async def test_the_exact_values_published_are_the_values_stored(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """The critical test. Not a row count — the actual OHLCV, bar by bar.

        A row count passes on data that happened to already be there, which is how an
        end-to-end test convinces everybody of something that is not true.
        """
        config = kafka_config()
        await ensure_topics(config)
        published = bars(12)

        await publish(config, published)
        await consume(config, committing_store, expect=len(published))

        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(Bar.bar_start, Bar.open, Bar.high, Bar.low, Bar.close, Bar.volume)
                    .join(Instrument, Bar.instrument_id == Instrument.id)
                    .where(Instrument.symbol == "KFKA")
                    .order_by(Bar.bar_start)
                )
            ).all()

        assert len(rows) == len(published)
        for stored, original in zip(rows, published, strict=True):
            assert stored.bar_start == original.bar_start
            assert stored.open == original.open
            assert stored.high == original.high
            assert stored.low == original.low
            assert stored.close == original.close
            assert stored.volume == original.volume

    async def test_trade_prices_survive_the_whole_path_exactly(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """ADR-0009 across a process boundary: no float, no rounding, no drift."""
        config = kafka_config()
        await ensure_topics(config)
        published = trades(8)

        await publish(config, published)
        await consume(config, committing_store, expect=len(published))

        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(Trade.event_id, Trade.price, Trade.size)
                    .join(Instrument, Trade.instrument_id == Instrument.id)
                    .where(Instrument.symbol == "KFKA")
                )
            ).all()

        stored = {row.event_id: (row.price, row.size) for row in rows}
        assert len(stored) == len(published)
        for event in published:
            assert stored[event.event_id] == (event.price, event.size)

    async def test_both_symbols_and_both_event_types_arrive(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        published = [*trades(4, symbol="KFKA"), *bars(4, symbol="KFKB")]

        await publish(config, published)
        await consume(config, committing_store, expect=len(published))

        async with engine.connect() as connection:
            trade_count = await connection.scalar(
                text(
                    "SELECT count(*) FROM trades t JOIN instruments i ON i.id = t.instrument_id "
                    "WHERE i.symbol = 'KFKA'"
                )
            )
            bar_count = await connection.scalar(
                text(
                    "SELECT count(*) FROM bars b JOIN instruments i ON i.id = b.instrument_id "
                    "WHERE i.symbol = 'KFKB'"
                )
            )

        assert trade_count == 4
        assert bar_count == 4

    async def test_an_unknown_symbol_is_registered_on_first_sight(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """A symbol appearing mid-stream is normal. Dropping its data would be a silent gap."""
        config = kafka_config()
        await ensure_topics(config)

        await publish(config, trades(2, symbol="KFKB"))
        await consume(config, committing_store, expect=2)

        async with engine.connect() as connection:
            registered = await connection.scalar(
                text("SELECT count(*) FROM instruments WHERE symbol = 'KFKB'")
            )

        assert registered == 1


class TestIdempotency:
    async def test_publishing_the_same_event_twice_stores_one_row(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """Deterministic ids (ADR-0012) plus `UNIQUE (event_id)` absorb the duplicate."""
        config = kafka_config()
        await ensure_topics(config)
        published = trades(5)

        await publish(config, published)
        await publish(config, published)
        await consume(config, committing_store, expect=len(published) * 2)

        async with engine.connect() as connection:
            stored = await connection.scalar(
                text(
                    "SELECT count(*) FROM trades t JOIN instruments i ON i.id = t.instrument_id "
                    "WHERE i.symbol = 'KFKA'"
                )
            )

        assert stored == 5

    async def test_reprocessing_the_whole_topic_changes_nothing(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """The redelivery scenario, with a real broker.

        A second consumer group reads the same messages from the beginning — which is
        exactly what a consumer that crashed before committing would see on restart. The
        database must end up identical.
        """
        config = kafka_config()
        await ensure_topics(config)
        published = [*trades(6), *bars(6)]

        await publish(config, published)
        await consume(config, committing_store, expect=len(published))

        before = await _snapshot(engine)

        # A different group, same topics, from the earliest offset: every message again.
        second = kafka_config(kafka_consumer_group=f"replay-{uuid.uuid4().hex[:8]}")
        service = await consume(second, committing_store, expect=len(published))

        after = await _snapshot(engine)

        assert after == before, "reprocessing changed the stored data"
        # Trades deduplicate to nothing (`ON CONFLICT DO NOTHING`); bars are re-upserted,
        # which rewrites identical values and moves `updated_at`. Both leave the same
        # final state, and only the trades are reported as duplicates — so this number is
        # 6, not 12, and a test that expected 12 would be testing a wish.
        assert service.processor.stats.duplicates == 6


class TestOffsetsAndRestart:
    async def test_a_restarted_consumer_does_not_re_read_what_it_finished(
        self, kafka_config: Callable[..., KafkaSettings], committing_store: SqlMarketDataStore
    ) -> None:
        """The property Phase 1 could not have: a restart is uneventful."""
        config = kafka_config()
        await ensure_topics(config)

        await publish(config, trades(5))
        first = await consume(config, committing_store, expect=5)
        assert first.stats.events == 5

        # Same group, brand-new consumer objects: a restart.
        await publish(config, trades(5, symbol="KFKB"))
        second = await consume(config, committing_store, expect=5)

        assert second.stats.events == 5, "only the new messages, not all ten"

    async def test_a_consumer_that_was_offline_catches_up(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """An outage becomes lag, not data loss. This is the argument for Kafka (ADR-0001)."""
        config = kafka_config()
        await ensure_topics(config)

        # Establish the group, then go away while 20 events pile up.
        await publish(config, trades(1))
        await consume(config, committing_store, expect=1)

        await publish(config, bars(20))
        service = await consume(config, committing_store, expect=20)

        assert service.stats.events == 20

        async with engine.connect() as connection:
            stored = await connection.scalar(text("SELECT count(*) FROM bars"))
        assert stored == 20

    async def test_a_new_group_starts_from_the_beginning(
        self, kafka_config: Callable[..., KafkaSettings], committing_store: SqlMarketDataStore
    ) -> None:
        """`auto_offset_reset=earliest`: a consumer added later processes the history."""
        config = kafka_config()
        await ensure_topics(config)
        await publish(config, trades(7))

        service = await consume(config, committing_store, expect=7)

        assert service.stats.events == 7


class TestDatabaseFailure:
    async def test_a_storage_failure_leaves_the_offset_uncommitted(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """The failure that must never lose data.

        The store refuses everything, so nothing is written and nothing may be
        acknowledged. Then a healthy consumer in the same group gets the same messages
        back and stores them — which is the proof the offsets never moved.
        """
        config = kafka_config()
        await ensure_topics(config)
        published = trades(5)
        await publish(config, published)

        class BrokenStore:
            async def resolve_instrument(self, symbol: str) -> uuid.UUID:
                raise StorageError("database is down")

            async def save_trades(self, rows: Sequence[object]) -> int:
                raise StorageError("database is down")

            async def save_quotes(self, rows: Sequence[object]) -> int:
                raise StorageError("database is down")

            async def save_bars(self, rows: Sequence[object]) -> int:
                raise StorageError("database is down")

            async def health_check(self) -> str | None:
                return "down"

        with pytest.raises(StorageError):
            await consume(config, BrokenStore(), expect=5, timeout_s=20)

        # Same consumer group. If the offsets had advanced, this would see nothing.
        recovered = await consume(config, committing_store, expect=5)

        assert recovered.stats.events == 5, "the events came back after the outage"

        async with engine.connect() as connection:
            stored = await connection.scalar(
                text(
                    "SELECT count(*) FROM trades t JOIN instruments i ON i.id = t.instrument_id "
                    "WHERE i.symbol = 'KFKA'"
                )
            )
        assert stored == 5


class TestMalformedMessages:
    async def test_a_malformed_message_reaches_the_dead_letter_topic(
        self, kafka_config: Callable[..., KafkaSettings], committing_store: SqlMarketDataStore
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        topics = Topics(config.kafka_topic_prefix)

        raw = AIOKafkaProducer(bootstrap_servers=config.kafka_bootstrap)
        await raw.start()
        try:
            await raw.send_and_wait(topics.trades, value=b"{not valid json", key=b"KFKA")
        finally:
            await raw.stop()

        await consume(config, committing_store, expect=1)

        quarantined = await read_topic(config.kafka_bootstrap, topics.dead_letter, expect=1)

        assert len(quarantined) == 1
        assert quarantined[0].value == b"{not valid json", "the original bytes, verbatim"
        headers = dict(quarantined[0].headers)
        assert headers["tw-dlq-reason"] == b"not_json"
        assert headers["tw-dlq-source-topic"] == topics.trades.encode()
        assert headers["tw-dlq-consumer-group"] == config.kafka_consumer_group.encode()

    async def test_one_bad_message_does_not_stop_the_partition(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
    ) -> None:
        """Failure F3, with a real broker: the good messages behind it still land."""
        config = kafka_config()
        await ensure_topics(config)
        topics = Topics(config.kafka_topic_prefix)

        raw = AIOKafkaProducer(bootstrap_servers=config.kafka_bootstrap)
        await raw.start()
        try:
            await raw.send_and_wait(topics.trades, value=b"garbage", key=b"KFKA")
        finally:
            await raw.stop()

        good = trades(4)
        await publish(config, good)

        await consume(config, committing_store, expect=5)

        async with engine.connect() as connection:
            stored = await connection.scalar(
                text(
                    "SELECT count(*) FROM trades t JOIN instruments i ON i.id = t.instrument_id "
                    "WHERE i.symbol = 'KFKA'"
                )
            )
        assert stored == 4

    async def test_a_poison_only_poll_still_advances(
        self, kafka_config: Callable[..., KafkaSettings], committing_store: SqlMarketDataStore
    ) -> None:
        """If these offsets were not committed, they would be redelivered forever."""
        config = kafka_config()
        await ensure_topics(config)
        topics = Topics(config.kafka_topic_prefix)

        raw = AIOKafkaProducer(bootstrap_servers=config.kafka_bootstrap)
        await raw.start()
        try:
            for index in range(3):
                await raw.send_and_wait(topics.trades, value=f"bad-{index}".encode(), key=b"KFKA")
        finally:
            await raw.stop()

        await consume(config, committing_store, expect=3)

        # Same group again: the poison records were acknowledged, so there is nothing left.
        second = await consume(config, committing_store, expect=0, timeout_s=10)

        assert second.stats.events == 0


class TestOrdering:
    async def test_events_for_one_symbol_arrive_in_order(
        self, kafka_config: Callable[..., KafkaSettings], committing_store: SqlMarketDataStore
    ) -> None:
        """Keying by symbol puts them on one partition, and one partition is ordered.

        This is the guarantee the Phase 3 bar aggregator will depend on. No claim is made
        about ordering *across* symbols, and none is tested, because none is true.
        """
        config = kafka_config()
        await ensure_topics(config)
        published = trades(30)

        await publish(config, published)
        topics = Topics(config.kafka_topic_prefix)
        received = await read_topic(config.kafka_bootstrap, topics.trades, expect=30)

        partitions = {record.partition for record in received}
        assert len(partitions) == 1, "one symbol, one partition"

        from tradewatch.infrastructure.events.envelope import decode

        order = [decode(record.value).occurred_at for record in received]
        assert order == sorted(order)
        assert order == [event.occurred_at for event in published]


class TestBrokerUnavailable:
    async def test_a_producer_cannot_start_without_a_broker(
        self, kafka_config: Callable[..., KafkaSettings]
    ) -> None:
        config = kafka_config(kafka_bootstrap="127.0.0.1:1")
        publisher = KafkaEventPublisher(config)

        with pytest.raises(BrokerUnavailableError):
            await publisher.start()

    async def test_a_consumer_cannot_start_without_a_broker(
        self, kafka_config: Callable[..., KafkaSettings]
    ) -> None:
        config = kafka_config(kafka_bootstrap="127.0.0.1:1")
        consumer = KafkaEventConsumer(config)

        with pytest.raises(BrokerUnavailableError):
            await consumer.start()

    async def test_topic_creation_reports_an_unreachable_broker(
        self, kafka_config: Callable[..., KafkaSettings]
    ) -> None:
        config = kafka_config(kafka_bootstrap="127.0.0.1:1")

        with pytest.raises(BrokerUnavailableError):
            await ensure_topics(config)

    async def test_the_pipeline_works_once_the_broker_is_back(
        self, kafka_config: Callable[..., KafkaSettings], committing_store: SqlMarketDataStore
    ) -> None:
        """Recovery after an outage, without restarting the process.

        The unreachable address stands in for the outage; the real one for its end. What is
        being checked is that a failed publisher leaves nothing broken behind it.
        """
        down = kafka_config(kafka_bootstrap="127.0.0.1:1")
        with pytest.raises(BrokerUnavailableError):
            await KafkaEventPublisher(down).start()

        config = kafka_config()
        await ensure_topics(config)
        await publish(config, trades(3))
        service = await consume(config, committing_store, expect=3)

        assert service.stats.events == 3


class TestTopicCreation:
    async def test_the_topics_are_created_with_the_configured_partitions(
        self, kafka_config: Callable[..., KafkaSettings], kafka: str
    ) -> None:
        config = kafka_config(kafka_topic_partitions=3)

        created = await ensure_topics(config)

        assert set(created) == set(Topics(config.kafka_topic_prefix).all)

        # A just-created topic can be listed before its partitions are: `describe_topics`
        # briefly answers with an empty partition list on a busy local broker (observed
        # during Phase 10's full-suite run). Same rule as `_wait_until_topics_visible`:
        # wait for the real condition, bounded, never a guessed sleep.
        trades = Topics(config.kafka_topic_prefix).trades
        partitions: list[object] = []
        deadline = asyncio.get_running_loop().time() + 30
        while not partitions and asyncio.get_running_loop().time() < deadline:
            admin = AIOKafkaAdminClient(bootstrap_servers=kafka)
            await admin.start()
            try:
                partitions = (await admin.describe_topics([trades]))[0]["partitions"]
            finally:
                await admin.close()
            if not partitions:
                await asyncio.sleep(0.5)

        assert len(partitions) == 3

    async def test_calling_it_again_creates_nothing(
        self, kafka_config: Callable[..., KafkaSettings], kafka: str
    ) -> None:
        config = kafka_config()

        created = await ensure_topics(config)

        # `ensure_topics` decides what is missing from `list_topics()` (admin.py's own
        # docstring: idempotent, "creates what is missing and leaves what exists alone").
        # On a freshly-started broker, `list_topics()` can lag a just-created topic by a
        # second or two — `scripts/wait_for_kafka_metadata.py` measured this directly — and
        # this test's `prefix` fixture mints a brand-new topic set per test, so the CI
        # warmup step (which only warms its own, differently-prefixed probe topics) never
        # touches *this* lag. Production never calls `ensure_topics` twice within
        # milliseconds, so the fix belongs here, at the one caller that actually does,
        # rather than teaching the function itself to retry around a path it never
        # exercises this way.
        await _wait_until_topics_visible(kafka, created)

        assert await ensure_topics(config) == []


class TestBrokerOutage:
    """The broker actually goes away, and comes back.

    `DEVELOPMENT_PLAN.md` makes this a Phase 2 exit criterion, and it is the one claim that
    cannot be made with an unreachable address: pointing a client at a closed port tests that
    connecting fails, not that a *running system* survives its broker restarting and drains
    the lag afterwards.

    It stops and starts the Compose container, so it needs the Docker CLI and skips without
    it. The broker is always restarted, including when the test fails — leaving a suite's
    remaining tests without a broker would turn one failure into twenty.
    """

    CONTAINER = "tradewatch-kafka"

    @staticmethod
    async def _docker(*args: str) -> tuple[int, str]:
        process = await asyncio.create_subprocess_exec(
            "docker",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await process.communicate()
        return process.returncode or 0, stdout.decode(errors="replace").strip()

    @pytest_asyncio.fixture
    async def controllable_broker(self, kafka: str) -> AsyncIterator[None]:
        code, output = await self._docker(
            "inspect", "--format", "{{.State.Running}}", self.CONTAINER
        )
        if code != 0 or output.strip() != "true":
            pytest.skip(
                f"Needs the Compose broker running as `{self.CONTAINER}` and a usable "
                "Docker CLI, so the container can be stopped and started."
            )
        try:
            yield
        finally:
            await self._docker("start", self.CONTAINER)
            await self._wait_for_broker()

    @classmethod
    async def _wait_for_broker(cls, timeout_s: float = 90.0) -> None:
        async with asyncio.timeout(timeout_s):
            # Polling, because the thing being waited on is a container in another process
            # and there is no event in this one to wait on. Bounded by the timeout above.
            while not await _broker_reachable():  # noqa: ASYNC110
                await asyncio.sleep(1.0)

    async def test_an_outage_costs_lag_and_not_data(
        self,
        kafka_config: Callable[..., KafkaSettings],
        committing_store: SqlMarketDataStore,
        engine: AsyncEngine,
        controllable_broker: None,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        published = trades(10)
        await publish(config, published)

        # The broker goes away with everything still unconsumed.
        code, output = await self._docker("stop", self.CONTAINER)
        assert code == 0, f"could not stop the broker: {output}"

        # A consumer cannot start, and — the part that matters — commits nothing.
        with pytest.raises(BrokerUnavailableError):
            await consume(config, committing_store, expect=10, timeout_s=30)

        async with engine.connect() as connection:
            during = await connection.scalar(text("SELECT count(*) FROM trades"))
        assert during == 0

        code, output = await self._docker("start", self.CONTAINER)
        assert code == 0, f"could not start the broker: {output}"
        await self._wait_for_broker()

        # Same group, same topics. The lag drains and nothing was lost.
        recovered = await consume(config, committing_store, expect=10, timeout_s=90)

        assert recovered.stats.events == 10

        async with engine.connect() as connection:
            stored = (
                await connection.execute(
                    select(Trade.event_id)
                    .join(Instrument, Trade.instrument_id == Instrument.id)
                    .where(Instrument.symbol == "KFKA")
                )
            ).scalars()
            ids = set(stored)

        assert ids == {event.event_id for event in published}, "every event survived the outage"
