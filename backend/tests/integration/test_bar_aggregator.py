"""The bar aggregator, against a real broker and a real database.

```
trades on tw.market.trades.v1 → bar-aggregator group → BarAggregator → PostgreSQL
```

These are the questions a fake cannot settle:

* **Does a second consumer group actually work?** A fake consumer can be told to deliver the
  same events to two consumers; only a real broker decides whether two groups genuinely get
  independent copies with independent offsets. That is the claim ADR-0001 was accepted on
  and the reason this phase exists.
* **Does a restart mid-bar produce the right bar?** Only if the partial bar really was
  written, really was read back, and the offsets really were where the consumer left them.
* **Do the OHLC constraints hold?** The `CHECK` constraints are PostgreSQL's, and the only
  way to know a bar satisfies them is to give it to PostgreSQL.

The arithmetic is hand-calculated, as in the unit suite. A bar that is merely *present* is
not evidence of anything.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from aiokafka.admin import AIOKafkaAdminClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from tradewatch.application.aggregation import BarAggregationService, BarAggregator
from tradewatch.config.settings import KafkaSettings
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import TradeEvent
from tradewatch.infrastructure.database.bar_store import SqlBarStore
from tradewatch.infrastructure.database.models import Bar, Instrument
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    KafkaEventPublisher,
    Topics,
    ensure_topics,
)
from tradewatch.infrastructure.providers.normalization import EventFactory

pytestmark = [pytest.mark.integration, pytest.mark.slow]

BOOTSTRAP = "localhost:9092"
SYMBOLS = ("AGGA", "AGGB")
ONE_MINUTE = BarInterval.ONE_MINUTE

#: A Monday inside the US session, so the quality checks would call this data legitimate.
MINUTE = dt.datetime(2026, 4, 6, 14, 30, tzinfo=dt.UTC)

SKIP_REASON = (
    "No Kafka broker. Start one with `docker compose -f docker/docker-compose.yml up -d kafka`."
)


# --- fixtures --------------------------------------------------------------


async def _broker_reachable(bootstrap: str) -> bool:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
    try:
        await admin.start()
    except Exception:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            await admin.close()


@pytest_asyncio.fixture(scope="session")
async def kafka() -> AsyncIterator[str]:
    if not await _broker_reachable(BOOTSTRAP):
        pytest.skip(SKIP_REASON)
    yield BOOTSTRAP


@pytest_asyncio.fixture
async def prefix(kafka: str) -> AsyncIterator[str]:
    """Topics unique to one test, deleted afterwards."""
    name = f"agg{uuid.uuid4().hex[:10]}"
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
    def build(**overrides: object) -> KafkaSettings:
        defaults: dict[str, object] = {
            "kafka_bootstrap": kafka,
            "kafka_topic_prefix": prefix,
            "kafka_topic_partitions": 3,
            "kafka_topic_replication": 1,
            "kafka_consumer_group": f"persist-{uuid.uuid4().hex[:8]}",
            "kafka_aggregator_group": f"agg-{uuid.uuid4().hex[:8]}",
            "kafka_consumer_poll_timeout_ms": 250,
            "consumer_batch_size": 200,
        }
        return KafkaSettings(**(defaults | overrides))  # type: ignore[arg-type]

    return build


@pytest_asyncio.fixture
async def bar_store(engine: AsyncEngine) -> AsyncIterator[SqlBarStore]:
    """The real store, writing real committed rows, cleaned up afterwards."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield SqlBarStore(factory)

    async with engine.begin() as connection:
        await connection.execute(
            text(
                "DELETE FROM bars WHERE instrument_id IN "
                "(SELECT id FROM instruments WHERE symbol = ANY(:symbols))"
            ),
            {"symbols": list(SYMBOLS)},
        )
        await connection.execute(
            text(
                "DELETE FROM trades WHERE instrument_id IN "
                "(SELECT id FROM instruments WHERE symbol = ANY(:symbols))"
            ),
            {"symbols": list(SYMBOLS)},
        )
        await connection.execute(
            text("DELETE FROM instruments WHERE symbol = ANY(:symbols)"),
            {"symbols": list(SYMBOLS)},
        )


# --- helpers ---------------------------------------------------------------


def trade(
    *,
    seconds: float,
    price: str,
    size: str = "10",
    symbol: str = "AGGA",
    trade_id: str | None = None,
) -> TradeEvent:
    occurred = MINUTE + dt.timedelta(seconds=seconds)
    factory = EventFactory(source="itest", producer="itest@1.0", clock=lambda: occurred)
    return factory.trade(
        symbol=symbol,
        price=Decimal(price),
        size=Decimal(size),
        occurred_at=occurred,
        provider_trade_id=trade_id or f"{symbol}-{seconds}-{price}",
    )


async def publish(config: KafkaSettings, trades: Sequence[TradeEvent]) -> None:
    publisher = KafkaEventPublisher(config)
    await publisher.start()
    try:
        accepted = await publisher.publish_many(trades)
        assert accepted == len(trades)
        await publisher.flush()
    finally:
        await publisher.stop()


async def aggregate(
    config: KafkaSettings,
    store: SqlBarStore,
    *,
    expect: int,
    lateness: dt.timedelta = dt.timedelta(seconds=5),
    timeout_s: float = 45.0,
    restore: bool = True,
    finalise: bool = False,
) -> BarAggregationService:
    """Run an aggregator until ``expect`` trades have been folded in, then stop it."""
    aggregator = BarAggregator(store, lateness=lateness)
    if restore:
        await aggregator.restore(list(SYMBOLS))

    topics = Topics(config.kafka_topic_prefix)
    consumer = KafkaEventConsumer(
        config,
        topics=topics,
        group_id=config.kafka_aggregator_group,
        subscribe_to=(topics.trades,),
    )
    service = BarAggregationService(
        consumer, aggregator, consumer_group=config.kafka_aggregator_group
    )
    stop = asyncio.Event()

    await consumer.start()
    task = asyncio.create_task(service.run(stop=stop))
    try:
        async with asyncio.timeout(timeout_s):
            # Polling the service's own counter: there is no event to wait on, only the
            # consumer task making progress. Bounded by the timeout above.
            while not task.done() and service.stats.trades < expect:  # noqa: ASYNC110
                await asyncio.sleep(0.05)
        stop.set()
        async with asyncio.timeout(timeout_s):
            await task
        if finalise:
            await service.close()
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        raise
    finally:
        await consumer.stop()

    return service


async def stored_bars(engine: AsyncEngine, symbol: str, interval: BarInterval) -> list[Any]:
    """The stored bars for one symbol, oldest first.

    Columns are selected explicitly rather than with ``select(Bar)``. Over a Core
    connection the latter expands to every column and ``.scalars()`` then silently hands
    back the *first* one — which is the instrument id, and reads as a list of bars right up
    until something asks one of them for its `bar_start`.
    """
    async with engine.connect() as connection:
        result = await connection.execute(
            select(
                Bar.bar_start,
                Bar.open,
                Bar.high,
                Bar.low,
                Bar.close,
                Bar.volume,
                Bar.vwap,
                Bar.trade_count,
                Bar.is_final,
                Bar.revision,
            )
            .join(Instrument, Bar.instrument_id == Instrument.id)
            .where(Instrument.symbol == symbol, Bar.interval == interval)
            .order_by(Bar.bar_start)
        )
        return list(result.all())


# --- the tests -------------------------------------------------------------


class TestEndToEnd:
    """Three trades in one minute, worked out by hand:

    ==========  ======  ====  ==========
    at          price   size  notional
    ==========  ======  ====  ==========
    14:30:10    100.00    10     1000.00
    14:30:30    102.00     5      510.00
    14:30:50     99.00     5      495.00
    ==========  ======  ====  ==========

    open 100, high 102, low 99, close 99, volume 20, vwap 2005/20 = 100.25
    """

    async def test_trades_become_the_expected_bar(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        trades = [
            trade(seconds=10, price="100", size="10"),
            trade(seconds=30, price="102", size="5"),
            trade(seconds=50, price="99", size="5"),
            trade(seconds=65, price="101", size="1"),  # closes the 14:30 bar
        ]

        await publish(config, trades)
        await aggregate(config, bar_store, expect=len(trades))

        bars = await stored_bars(engine, "AGGA", ONE_MINUTE)
        first = next(row for row in bars if row.bar_start == MINUTE)

        assert first.open == Decimal("100")
        assert first.high == Decimal("102")
        assert first.low == Decimal("99")
        assert first.close == Decimal("99")
        assert first.volume == Decimal("20")
        assert first.trade_count == 3
        assert first.vwap == Decimal("100.25")
        assert first.is_final is True

    async def test_the_database_accepts_the_bar_the_aggregator_writes(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        """The OHLC CHECK constraints are PostgreSQL's. This is the only way to know."""
        config = kafka_config()
        await ensure_topics(config)
        trades = [trade(seconds=index * 7, price=f"{100 + index}") for index in range(8)]

        await publish(config, trades)
        await aggregate(config, bar_store, expect=len(trades), finalise=True)

        bars = await stored_bars(engine, "AGGA", ONE_MINUTE)
        assert bars
        for row in bars:
            assert row.high >= row.low
            assert row.low <= row.open <= row.high
            assert row.low <= row.close <= row.high

    async def test_symbols_get_their_own_bars(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        trades = [
            trade(seconds=10, price="100", symbol="AGGA"),
            trade(seconds=20, price="200", symbol="AGGB"),
            trade(seconds=70, price="101", symbol="AGGA"),
            trade(seconds=75, price="201", symbol="AGGB"),
        ]

        await publish(config, trades)
        await aggregate(config, bar_store, expect=len(trades))

        first_a = (await stored_bars(engine, "AGGA", ONE_MINUTE))[0]
        first_b = (await stored_bars(engine, "AGGB", ONE_MINUTE))[0]
        assert first_a.close == Decimal("100")
        assert first_b.close == Decimal("200")


class TestSecondConsumerGroup:
    """The claim ADR-0001 was accepted on, and the reason Phase 3 exists.

    Until the aggregator, Kafka had exactly one consumer — the condition the ADR's own
    "revisit when" clause names as the point at which the broker stops being justified.
    """

    async def test_both_groups_receive_every_trade(
        self, kafka_config: Callable[..., KafkaSettings], bar_store: SqlBarStore
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        topics = Topics(config.kafka_topic_prefix)
        trades = [trade(seconds=index * 10, price=f"{100 + index}") for index in range(6)]
        await publish(config, trades)

        # The aggregator's group.
        aggregated = await aggregate(config, bar_store, expect=len(trades))

        # A different group over the same topic, reading from the beginning.
        persist = KafkaEventConsumer(
            config,
            topics=topics,
            group_id=config.kafka_consumer_group,
            subscribe_to=(topics.trades,),
        )
        await persist.start()
        seen: list[TradeEvent] = []
        try:
            async with asyncio.timeout(30):
                while len(seen) < len(trades):
                    async for batch in persist.batches():
                        seen.extend(item for item in batch if isinstance(item, TradeEvent))
                        if len(seen) >= len(trades):
                            break
        finally:
            await persist.stop()

        assert aggregated.stats.trades == len(trades)
        assert len(seen) == len(trades), "the second group saw every trade independently"

    async def test_one_groups_offsets_do_not_move_the_others(
        self, kafka_config: Callable[..., KafkaSettings], bar_store: SqlBarStore, kafka: str
    ) -> None:
        """Independent offsets are the whole point. Sharing a group would halve each."""
        config = kafka_config()
        await ensure_topics(config)
        trades = [
            trade(seconds=index * 10, price="100", trade_id=f"t{index}") for index in range(5)
        ]
        await publish(config, trades)

        await aggregate(config, bar_store, expect=len(trades))

        admin = AIOKafkaAdminClient(bootstrap_servers=kafka)
        await admin.start()
        try:
            groups = {name for name, _ in await admin.list_consumer_groups()}
        finally:
            await admin.close()

        assert config.kafka_aggregator_group in groups
        assert config.kafka_consumer_group not in groups, "the persistence group never ran here"


class TestRestartMidBar:
    """Failure F13, and an explicit Phase 3 exit criterion."""

    async def test_a_restart_mid_bar_produces_the_correct_closed_bar(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)

        first_half = [
            trade(seconds=10, price="100", size="10"),
            trade(seconds=20, price="104", size="5"),
        ]
        await publish(config, first_half)

        # An aggregator runs, folds them in, writes the partial bar, and is killed.
        await aggregate(config, bar_store, expect=len(first_half))

        partial = (await stored_bars(engine, "AGGA", ONE_MINUTE))[0]
        assert partial.is_final is False, "the bar was left in progress"
        assert partial.trade_count == 2

        # A new process, same group, same database.
        second_half = [
            trade(seconds=40, price="98", size="5"),
            trade(seconds=70, price="101", size="1"),  # closes the minute
        ]
        await publish(config, second_half)
        await aggregate(config, bar_store, expect=len(second_half))

        completed = next(
            row for row in await stored_bars(engine, "AGGA", ONE_MINUTE) if row.bar_start == MINUTE
        )

        assert completed.is_final is True
        assert completed.open == Decimal("100"), "the pre-restart open survived"
        assert completed.high == Decimal("104"), "and the pre-restart high"
        assert completed.low == Decimal("98")
        assert completed.close == Decimal("98")
        assert completed.volume == Decimal("20"), "every trade is in it, none twice"
        assert completed.trade_count == 3


class TestLateTrades:
    """Failure F11: the bounded lateness window, and what falls outside it."""

    async def test_a_late_trade_inside_the_window_revises_the_bar(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        await publish(
            config,
            [
                trade(seconds=10, price="100", size="10"),
                trade(seconds=65, price="101", size="1"),  # closes 14:30
                trade(seconds=50, price="108", size="10", trade_id="late-but-inside"),
            ],
        )

        await aggregate(config, bar_store, expect=3, lateness=dt.timedelta(seconds=60))

        bar = next(
            row for row in await stored_bars(engine, "AGGA", ONE_MINUTE) if row.bar_start == MINUTE
        )

        assert bar.revision == 1, "a correction, absorbed by the revision-guarded upsert"
        assert bar.high == Decimal("108")
        assert bar.volume == Decimal("20")

    async def test_a_trade_outside_the_window_is_counted_and_dropped(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        await publish(
            config,
            [
                trade(seconds=10, price="100", size="10"),
                trade(seconds=200, price="101", size="1"),  # watermark well past the window
                trade(seconds=50, price="999", size="50", trade_id="far-too-late"),
            ],
        )

        service = await aggregate(config, bar_store, expect=3, lateness=dt.timedelta(seconds=5))

        bar = next(
            row for row in await stored_bars(engine, "AGGA", ONE_MINUTE) if row.bar_start == MINUTE
        )

        assert bar.volume == Decimal("10"), "the sealed bar was not touched"
        assert bar.high == Decimal("100")
        assert service.aggregator.stats.late_dropped == 1, "counted, not silently accepted"
        assert bar.revision == 0


class TestRollups:
    """`ARCHITECTURE.md` §3.3: coarser bars come from stored 1m bars, never from trades again."""

    async def test_five_minute_bars_are_derived_from_the_minutes(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)

        # One trade a minute for six minutes, rising: 100, 101, ... 105.
        trades = [
            trade(seconds=index * 60 + 10, price=f"{100 + index}", size="10") for index in range(7)
        ]
        await publish(config, trades)
        await aggregate(config, bar_store, expect=len(trades))

        instrument_id = await bar_store.resolve_instrument("AGGA")
        written = await bar_store.roll_up(
            instrument_id,
            ONE_MINUTE,
            BarInterval.FIVE_MINUTES,
            MINUTE,
            MINUTE + dt.timedelta(minutes=10),
        )

        assert written >= 1
        five = await stored_bars(engine, "AGGA", BarInterval.FIVE_MINUTES)
        first = next(row for row in five if row.bar_start == MINUTE)

        assert first.open == Decimal("100"), "the open of the earliest minute, not the lowest"
        assert first.close == Decimal("104"), "the close of the last minute in the bucket"
        assert first.high == Decimal("104")
        assert first.low == Decimal("100")
        assert first.volume == Decimal("50"), "five minutes of ten"

    async def test_rolling_up_twice_changes_nothing(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        config = kafka_config()
        await ensure_topics(config)
        trades = [
            trade(seconds=index * 60 + 10, price=f"{100 + index}", size="10") for index in range(7)
        ]
        await publish(config, trades)
        await aggregate(config, bar_store, expect=len(trades))

        instrument_id = await bar_store.resolve_instrument("AGGA")
        window = (MINUTE, MINUTE + dt.timedelta(minutes=10))
        await bar_store.roll_up(instrument_id, ONE_MINUTE, BarInterval.FIVE_MINUTES, *window)
        before = [
            (row.bar_start, row.open, row.high, row.low, row.close, row.volume)
            for row in await stored_bars(engine, "AGGA", BarInterval.FIVE_MINUTES)
        ]

        await bar_store.roll_up(instrument_id, ONE_MINUTE, BarInterval.FIVE_MINUTES, *window)
        after = [
            (row.bar_start, row.open, row.high, row.low, row.close, row.volume)
            for row in await stored_bars(engine, "AGGA", BarInterval.FIVE_MINUTES)
        ]

        assert after == before

    async def test_rollups_leave_vwap_null(
        self,
        kafka_config: Callable[..., KafkaSettings],
        bar_store: SqlBarStore,
        engine: AsyncEngine,
    ) -> None:
        """Averaging five VWAPs is not a VWAP. A null is honest; a number would not be."""
        config = kafka_config()
        await ensure_topics(config)
        trades = [
            trade(seconds=index * 60 + 10, price=f"{100 + index}", size="10") for index in range(7)
        ]
        await publish(config, trades)
        await aggregate(config, bar_store, expect=len(trades))

        instrument_id = await bar_store.resolve_instrument("AGGA")
        await bar_store.roll_up(
            instrument_id,
            ONE_MINUTE,
            BarInterval.FIVE_MINUTES,
            MINUTE,
            MINUTE + dt.timedelta(minutes=10),
        )

        five = await stored_bars(engine, "AGGA", BarInterval.FIVE_MINUTES)
        assert five
        assert all(row.vwap is None for row in five)

        minutes = await stored_bars(engine, "AGGA", ONE_MINUTE)
        assert any(row.vwap is not None for row in minutes), "the 1m bars do have one"
