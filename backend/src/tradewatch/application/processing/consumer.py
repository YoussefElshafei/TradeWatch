"""The market data consumer: batches in, rows out, offsets last.

`ARCHITECTURE.md` §7.4 in executable form. This is the piece that decides *when* a message
is acknowledged, which is the only thing standing between at-least-once delivery and silent
data loss.

It lives in `application` and names no broker. It talks to an
:class:`~tradewatch.application.ports.event_bus.EventBatchConsumer` and to the existing
:class:`~tradewatch.application.processing.service.MarketDataProcessor`, which already owns
batching, idempotent writes and storage retries. Nothing here re-implements any of that —
the Kafka path deliberately goes *through* the Phase 1 processor rather than around it, so
there is exactly one piece of code that knows how market data is written.

**The order, which is the whole design:**

1. Buffer every event in the batch through the processor.
2. Flush — one transaction, idempotent, retried internally on a storage failure.
3. Only then commit.

Reversing 2 and 3 would make a crash in between a permanent gap. Keeping them this way
makes it a redelivery, which the unique constraint on ``event_id`` absorbs (ADR-0012).

**What happens when things go wrong:**

* A *storage failure* (retryable) is retried by the processor with backoff. If it runs out
  of attempts, the offset is **not** committed and the error propagates: the worker exits,
  the supervisor restarts it, and the broker redelivers from the last commit. Nothing is
  lost and nothing is acknowledged on the strength of a write that did not happen.
* A *permanently unstorable batch* — the store refused it and always will — is not allowed
  to stop the partition. The batch is replayed one event at a time to find which event is
  actually poison; the survivors are written, the poison one is quarantined, and the offset
  advances. Isolating rather than discarding the batch matters: a constraint violation is
  reported at batch granularity, and dropping ninety-nine good trades to be rid of one bad
  one is data loss dressed up as error handling.
* A *malformed message* never reaches this class. It cannot be turned into a domain event,
  so the adapter that holds the bytes quarantines it and moves on.
* *Cancellation* drops the buffer without committing. Those events were never stored, so
  letting them be redelivered is the correct answer and committing would be a lie.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from tradewatch.application.ports.event_bus import DeadLetterSink, EventBatchConsumer
from tradewatch.application.processing.errors import StorageError, UnprocessableEventError
from tradewatch.application.processing.service import MarketDataProcessor
from tradewatch.domain.events import MarketEvent, event_summary
from tradewatch.observability.logging import get_logger

__all__ = ["ConsumerStats", "MarketDataConsumerService"]

log = get_logger(__name__)


@dataclass(slots=True)
class ConsumerStats:
    """Counters for one consumer run.

    These describe the *acknowledgement* path — how many batches were taken, how many were
    committed, how many events had to be quarantined. What happened to the rows themselves
    is :class:`~tradewatch.application.processing.service.ProcessorStats`, and the two are
    kept apart on purpose: "committed the offset" and "wrote the row" are different claims,
    and a bug that makes them disagree is the one worth catching.
    """

    batches: int = 0
    events: int = 0
    empty_polls: int = 0
    commits: int = 0
    poison_batches: int = 0
    quarantined: int = 0
    started_at: dt.datetime | None = None
    last_batch_at: dt.datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "batches": self.batches,
            "events": self.events,
            "empty_polls": self.empty_polls,
            "commits": self.commits,
            "poison_batches": self.poison_batches,
            "quarantined": self.quarantined,
        }


class MarketDataConsumerService:
    """Consume batches of market events, persist them, then acknowledge."""

    def __init__(
        self,
        consumer: EventBatchConsumer,
        processor: MarketDataProcessor,
        *,
        dead_letters: DeadLetterSink | None = None,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._dead_letters = dead_letters
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        self._stats = ConsumerStats()

    @property
    def stats(self) -> ConsumerStats:
        return self._stats

    @property
    def processor(self) -> MarketDataProcessor:
        return self._processor

    async def run(
        self,
        *,
        max_batches: int | None = None,
        stop: asyncio.Event | None = None,
    ) -> ConsumerStats:
        """Consume until cancelled, until ``stop`` is set, or until ``max_batches``.

        ``max_batches`` counts **non-empty** batches handled by this call. Counting empty
        polls would make a bounded run in a quiet market finish having processed nothing,
        which is a confusing thing for a test to assert against.
        """
        self._stats.started_at = self._clock()
        log.info("consumer.started")

        try:
            async for batch in self._consumer.batches():
                if stop is not None and stop.is_set():
                    log.info("consumer.stop_requested", **self._stats.as_dict())
                    break

                if not batch:
                    # A poll that timed out with nothing to read. There is nothing to
                    # commit — committing here would re-commit the previous batch for no
                    # reason — so the only thing to do is notice and go round again.
                    self._stats.empty_polls += 1
                    continue

                self._stats.last_batch_at = self._clock()
                await self._process_batch(batch)

                # The side effect has happened and is durable. Only now.
                await self._consumer.commit()
                self._stats.commits += 1
                self._stats.batches += 1
                self._stats.events += len(batch)
                log.debug(
                    "consumer.batch_committed",
                    size=len(batch),
                    total_batches=self._stats.batches,
                )

                if max_batches is not None and self._stats.batches >= max_batches:
                    break
        except asyncio.CancelledError:
            # Shutdown. Whatever is buffered was never written, so it must not be
            # acknowledged — dropping it means the broker redelivers it, which is exactly
            # what at-least-once is for.
            dropped = self._processor.pending
            if dropped:
                log.info("consumer.cancelled_with_pending", pending=dropped)
            log.info("consumer.cancelled", **self._stats.as_dict())
            raise
        except StorageError:
            # Not committed, on purpose. The events stay on the topic and the restart picks
            # them up from the last successful commit.
            log.error(
                "consumer.storage_failed_uncommitted",
                pending=self._processor.pending,
                **self._stats.as_dict(),
            )
            raise
        finally:
            log.info(
                "consumer.stopped",
                pending=self._processor.pending,
                **self._stats.as_dict(),
                **self._processor.stats.as_dict(),
            )

        return self._stats

    # --- one batch ---------------------------------------------------------

    async def _process_batch(self, batch: Sequence[MarketEvent]) -> None:
        """Buffer and write one batch, isolating a poison event if the store refuses it.

        Raises:
            StorageError: If the write failed in a way worth retrying and the processor ran
                out of attempts. The caller must not commit.
        """
        try:
            for event in batch:
                await self._processor.handle(event)
            await self._processor.flush()
        except UnprocessableEventError as error:
            self._stats.poison_batches += 1
            log.warning(
                "consumer.batch_rejected",
                size=len(batch),
                reason=error.message,
                detail=error.detail,
            )
            await self._isolate(batch)

    async def _isolate(self, batch: Sequence[MarketEvent]) -> None:
        """Replay a rejected batch one event at a time to find what is actually poison.

        The rewrite is safe: every write in this system is idempotent, so events that
        already landed during the failed batch attempt land again as no-ops (ADR-0012).
        That is what makes "try each one separately" a correct recovery rather than a
        second chance to duplicate the data.

        Raises:
            StorageError: If the database is the problem after all, which is retryable and
                must not be mistaken for a poison message.
        """
        self._processor.discard_pending()

        for event in batch:
            try:
                await self._processor.handle(event)
                await self._processor.flush()
            except UnprocessableEventError as error:
                self._processor.discard_pending()
                await self._quarantine(event, error)

    async def _quarantine(self, event: MarketEvent, error: UnprocessableEventError) -> None:
        """Send one permanently unstorable event to the dead-letter sink.

        With no sink configured the event is counted and logged and the consumer keeps its
        place. That is a deliberate configuration (``TW_DLQ_ENABLED=false``), and it is
        logged at error rather than warning because the event is then unrecoverable.
        """
        self._stats.quarantined += 1
        summary = event_summary(event)

        if self._dead_letters is None:
            log.error(
                "consumer.event_dropped_no_dlq",
                reason=error.message,
                total_quarantined=self._stats.quarantined,
                **summary,
            )
            return

        await self._dead_letters.send(
            event,
            reason="unprocessable",
            error=error.message,
            attempts=1,
        )
        log.warning(
            "consumer.event_quarantined",
            reason=error.message,
            detail=error.detail,
            total_quarantined=self._stats.quarantined,
            **summary,
        )
