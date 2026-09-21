"""The market data consumer service: when an offset is safe to acknowledge.

This is the class that decides whether a crash costs a redelivery or costs the data, so the
tests are mostly about *ordering* and *refusing to commit*, not about happy paths.

No broker and no database. The consumer is a scripted fake implementing the
``EventBatchConsumer`` port, and the store is an in-memory fake implementing
``MarketDataStore`` — both of which exist precisely so this logic can be tested at speed.
The same properties are then proved against a real Kafka and a real PostgreSQL in
`tests/integration/test_kafka_pipeline.py`; these tests are what make the failures
reachable, because a real database will not fail on request.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import uuid
from collections.abc import AsyncIterator, Sequence

import pytest

from tests.kafka_support import RecordingDeadLetters, bar, quote, trade
from tradewatch.application.processing import (
    MarketDataConsumerService,
    MarketDataProcessor,
    StorageError,
    UnprocessableEventError,
)
from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.events import MarketEvent
from tradewatch.domain.ids import new_id

pytestmark = pytest.mark.unit


class FakeStore:
    """An idempotent in-memory store, with a shared activity log.

    Writes append to ``log``, and so do commits, so a test can assert the *order* of the
    two — which is the whole correctness question here and is invisible to a test that only
    checks final state.
    """

    def __init__(self, log: list[str]) -> None:
        self.log = log
        self.trades: dict[uuid.UUID, NewTrade] = {}
        self.quotes: dict[uuid.UUID, NewQuote] = {}
        self.bars: dict[tuple[uuid.UUID, str, dt.datetime], NewBar] = {}
        self.instruments: dict[str, uuid.UUID] = {}
        #: Raised on the next write, then cleared.
        self.fail_next: Exception | None = None
        #: Raised on every write until cleared.
        self.fail_always: Exception | None = None
        #: Event ids the store will never accept, however often they are offered.
        self.poison: set[uuid.UUID] = set()

    def _maybe_fail(self, event_ids: Sequence[uuid.UUID]) -> None:
        if self.fail_always is not None:
            raise self.fail_always
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error
        poisoned = [item for item in event_ids if item in self.poison]
        if poisoned:
            raise UnprocessableEventError(
                f"{len(poisoned)} row(s) violated a database constraint",
                detail="fake constraint violation",
            )

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        return self.instruments.setdefault(symbol, new_id())

    async def save_trades(self, trades: Sequence[NewTrade]) -> int:
        self._maybe_fail([row.event_id for row in trades])
        written = 0
        for row in trades:
            if row.event_id not in self.trades:
                self.trades[row.event_id] = row
                written += 1
        self.log.append(f"write:trades:{written}")
        return written

    async def save_quotes(self, quotes: Sequence[NewQuote]) -> int:
        self._maybe_fail([row.event_id for row in quotes])
        written = 0
        for row in quotes:
            if row.event_id not in self.quotes:
                self.quotes[row.event_id] = row
                written += 1
        self.log.append(f"write:quotes:{written}")
        return written

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        self._maybe_fail([])
        written = 0
        for row in bars:
            key = (row.instrument_id, row.interval.value, row.bar_start)
            if key not in self.bars:
                written += 1
            self.bars[key] = row
        self.log.append(f"write:bars:{written}")
        return written

    async def health_check(self) -> str | None:
        return None

    @property
    def rows(self) -> int:
        return len(self.trades) + len(self.quotes) + len(self.bars)


class ScriptedConsumer:
    """An ``EventBatchConsumer`` that yields a fixed script, then goes quiet."""

    def __init__(
        self,
        script: Sequence[Sequence[MarketEvent]],
        log: list[str],
        *,
        commit_error: Exception | None = None,
    ) -> None:
        self.script = list(script)
        self.log = log
        self.commit_error = commit_error
        self.delivered = 0
        self.commits: list[int] = []

    async def batches(self) -> AsyncIterator[Sequence[MarketEvent]]:
        for batch in self.script:
            # A real poll is a suspension point, and code that only works because the fake
            # never yields is code that does not work.
            await asyncio.sleep(0)
            self.delivered += len(batch)
            yield batch
        while True:
            # A quiet topic: empty polls forever, exactly as the real adapter behaves.
            await asyncio.sleep(0)
            yield []

    async def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.log.append("commit")
        self.commits.append(self.delivered)


def build(
    script: Sequence[Sequence[MarketEvent]],
    *,
    dead_letters: RecordingDeadLetters | None = None,
    max_storage_attempts: int = 3,
    commit_error: Exception | None = None,
) -> tuple[MarketDataConsumerService, ScriptedConsumer, FakeStore, list[str]]:
    log: list[str] = []
    store = FakeStore(log)
    consumer = ScriptedConsumer(script, log, commit_error=commit_error)
    processor = MarketDataProcessor(
        store,
        batch_size=100,
        max_storage_attempts=max_storage_attempts,
        retry_delay=0,
    )
    service = MarketDataConsumerService(consumer, processor, dead_letters=dead_letters)
    return service, consumer, store, log


class TestHappyPath:
    async def test_a_batch_is_stored_and_then_committed(self) -> None:
        service, consumer, store, _ = build([[trade(), quote()]])

        await service.run(max_batches=1)

        assert store.rows == 2
        assert consumer.commits == [2]

    async def test_the_commit_happens_after_the_write_never_before(self) -> None:
        """`ARCHITECTURE.md` §7.4. Reverse these two and a crash between them is a gap."""
        service, _, _, log = build([[trade()]])

        await service.run(max_batches=1)

        assert log == ["write:trades:1", "commit"]

    async def test_every_event_type_reaches_the_store(self) -> None:
        service, _, store, _ = build([[trade(), quote(), bar()]])

        await service.run(max_batches=1)

        assert len(store.trades) == 1
        assert len(store.quotes) == 1
        assert len(store.bars) == 1

    async def test_several_batches_are_committed_one_by_one(self) -> None:
        script = [[trade(trade_id=f"t-{index}")] for index in range(3)]
        service, consumer, _, _ = build(script)

        await service.run(max_batches=3)

        assert consumer.commits == [1, 2, 3]
        assert service.stats.batches == 3

    async def test_an_empty_batch_is_not_committed(self) -> None:
        """Committing on an empty poll would re-commit the previous batch for no reason."""
        service, consumer, _, _ = build([[], [trade()]])

        await service.run(max_batches=1)

        assert consumer.commits == [1]
        assert service.stats.empty_polls == 1

    async def test_nothing_is_left_buffered_after_a_batch(self) -> None:
        service, _, _, _ = build([[trade(), quote(), bar()]])

        await service.run(max_batches=1)

        assert service.processor.pending == 0


class TestIdempotency:
    async def test_the_same_event_twice_writes_one_row(self) -> None:
        """At-least-once delivery, absorbed. ADR-0012 is what makes this work."""
        duplicate = trade()
        service, _, store, _ = build([[duplicate], [duplicate]])

        await service.run(max_batches=2)

        assert len(store.trades) == 1
        assert service.processor.stats.duplicates == 1

    async def test_a_redelivered_batch_after_a_crash_changes_nothing(self) -> None:
        """The exact scenario: processed, crashed before committing, delivered again."""
        events = [trade(trade_id=f"t-{index}") for index in range(5)]
        first, _, store, _ = build([events])
        await first.run(max_batches=1)
        assert store.rows == 5

        # A new process, the same store, the same events off the topic again.
        log: list[str] = []
        consumer = ScriptedConsumer([events], log)
        store.log = log
        processor = MarketDataProcessor(store, batch_size=100, retry_delay=0)
        second = MarketDataConsumerService(consumer, processor)

        await second.run(max_batches=1)

        assert store.rows == 5, "redelivery must not duplicate"
        assert consumer.commits == [5]

    async def test_a_duplicate_is_still_acknowledged(self) -> None:
        """Not committing a duplicate would replay it forever."""
        duplicate = trade()
        service, consumer, _, _ = build([[duplicate], [duplicate]])

        await service.run(max_batches=2)

        assert consumer.commits == [1, 2]


class TestStorageFailures:
    async def test_a_transient_failure_is_retried_and_then_committed(self) -> None:
        """PostgreSQL blinked. The batch is kept, retried, and lands."""
        service, consumer, store, _ = build([[trade()]])
        store.fail_next = StorageError("connection reset")

        await service.run(max_batches=1)

        assert store.rows == 1
        assert consumer.commits == [1]
        assert service.processor.stats.storage_retries == 1

    async def test_a_persistent_failure_is_not_committed(self) -> None:
        """The single most important assertion in this file.

        If this commits, the events are acknowledged without being stored and the restart
        never sees them again — silent data loss, the failure mode the whole design exists
        to prevent.
        """
        service, consumer, store, _ = build([[trade()]], max_storage_attempts=2)
        store.fail_always = StorageError("database is down")

        with pytest.raises(StorageError):
            await service.run(max_batches=1)

        assert consumer.commits == [], "nothing may be acknowledged"
        assert store.rows == 0

    async def test_the_retry_budget_is_bounded(self) -> None:
        """An unbounded retry is a consumer that never reports the outage."""
        service, _, store, _ = build([[trade()]], max_storage_attempts=3)
        store.fail_always = StorageError("down")

        with pytest.raises(StorageError):
            await service.run(max_batches=1)

        assert service.processor.stats.storage_retries == 2

    async def test_the_failed_batch_is_still_visible_afterwards(self) -> None:
        """The buffer is kept, so an operator can see exactly what did not land."""
        service, _, store, _ = build([[trade(), quote()]], max_storage_attempts=1)
        store.fail_always = StorageError("down")

        with pytest.raises(StorageError):
            await service.run(max_batches=1)

        assert service.processor.pending == 2

    async def test_a_failed_commit_propagates(self) -> None:
        """A commit that silently failed means silent reprocessing. Better to be loud."""
        service, _, _, _ = build([[trade()]], commit_error=RuntimeError("commit refused"))

        with pytest.raises(RuntimeError, match="commit refused"):
            await service.run(max_batches=1)


class TestPoisonMessages:
    async def test_a_permanently_rejected_batch_does_not_stop_the_consumer(self) -> None:
        """Failure F3. One bad row must not stop a partition forever."""
        poison = trade(trade_id="poison")
        service, consumer, store, _ = build([[poison]], dead_letters=RecordingDeadLetters())
        store.poison.add(poison.event_id)

        await service.run(max_batches=1)

        assert consumer.commits == [1], "the partition moved on"
        assert service.stats.poison_batches == 1

    async def test_the_poison_event_is_isolated_and_the_rest_of_the_batch_survives(
        self,
    ) -> None:
        """Dropping 99 good trades to be rid of 1 bad one is data loss with a nice name."""
        good = [trade(trade_id=f"t-{index}") for index in range(4)]
        poison = trade(trade_id="poison")
        dlq = RecordingDeadLetters()
        service, _, store, _ = build([[*good[:2], poison, *good[2:]]], dead_letters=dlq)
        store.poison.add(poison.event_id)

        await service.run(max_batches=1)

        assert len(store.trades) == 4, "every good event was written"
        assert dlq.events == [poison]
        assert service.stats.quarantined == 1

    async def test_the_dead_letter_record_says_why(self) -> None:
        poison = trade(trade_id="poison")
        dlq = RecordingDeadLetters()
        service, _, store, _ = build([[poison]], dead_letters=dlq)
        store.poison.add(poison.event_id)

        await service.run(max_batches=1)

        assert dlq.reasons == ["unprocessable"]
        assert "constraint" in dlq.errors[0]

    async def test_with_no_dlq_the_event_is_counted_and_the_consumer_continues(self) -> None:
        poison = trade(trade_id="poison")
        service, consumer, store, _ = build([[poison]], dead_letters=None)
        store.poison.add(poison.event_id)

        await service.run(max_batches=1)

        assert service.stats.quarantined == 1
        assert consumer.commits == [1]

    async def test_a_storage_failure_during_isolation_is_not_mistaken_for_poison(self) -> None:
        """A database that dies mid-isolation is retryable, and must not be quarantined."""
        poison = trade(trade_id="poison")
        dlq = RecordingDeadLetters()
        service, consumer, store, _ = build(
            [[poison, trade(trade_id="ok")]], dead_letters=dlq, max_storage_attempts=1
        )
        store.poison.add(poison.event_id)

        async def fail_after_first_isolation() -> None:
            return None

        # The batch write fails permanently, then the database goes down during the
        # per-event replay.
        original = store.save_trades

        calls = {"n": 0}

        async def flaky(rows: Sequence[NewTrade]) -> int:
            calls["n"] += 1
            if calls["n"] == 2:
                raise StorageError("database went away")
            return await original(rows)

        store.save_trades = flaky  # type: ignore[assignment, method-assign]

        with pytest.raises(StorageError):
            await service.run(max_batches=1)

        assert dlq.events == [], "a retryable failure is never quarantined"
        assert consumer.commits == []
        await fail_after_first_isolation()


class TestShutdown:
    async def test_a_stop_event_ends_the_loop(self) -> None:
        stop = asyncio.Event()
        stop.set()
        service, consumer, _, _ = build([[trade()]])

        await service.run(stop=stop)

        assert consumer.commits == []
        assert service.stats.batches == 0

    async def test_stopping_after_a_batch_leaves_it_committed(self) -> None:
        stop = asyncio.Event()
        service, consumer, _, _ = build([[trade()], [quote()]])

        async def stop_soon() -> None:
            await asyncio.sleep(0)
            stop.set()

        await asyncio.gather(service.run(stop=stop), stop_soon())

        assert consumer.commits, "whatever was processed was acknowledged"

    async def test_cancellation_mid_write_does_not_commit(self) -> None:
        """Shut down while a write is in flight. Nothing was stored, so nothing is acked.

        Arranged precisely rather than by racing: the store blocks on an event that is
        never set, so the cancellation lands between "buffered" and "written" every time.
        """
        service, consumer, store, _ = build([[trade()]])
        blocked = asyncio.Event()
        original = store.save_trades

        async def block(rows: Sequence[NewTrade]) -> int:
            await blocked.wait()
            return await original(rows)

        store.save_trades = block  # type: ignore[assignment, method-assign]

        task = asyncio.create_task(service.run())
        # Let the loop reach the blocked write.
        for _ in range(5):
            await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert consumer.commits == [], "an interrupted write is never acknowledged"
        assert store.rows == 0

    async def test_max_batches_counts_only_non_empty_batches(self) -> None:
        """Otherwise a bounded run in a quiet market finishes having done nothing."""
        service, consumer, _, _ = build([[], [], [trade()]])

        await service.run(max_batches=1)

        assert consumer.commits == [1]
        assert service.stats.empty_polls == 2
