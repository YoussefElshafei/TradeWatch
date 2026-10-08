"""The Bar Aggregator: trades in, OHLCV bars out.

`ARCHITECTURE.md` §3.3. It consumes the trade stream, maintains a partial bar per symbol per
minute, and closes each one when event time says the minute is over.

**This is the second consumer group.** Everything else in this file matters less than that.
Phase 2 introduced Kafka on the argument (ADR-0001) that several independent consumers need
the same events with independent failure domains and independent offsets — and then shipped
exactly one consumer, which is the condition ADR-0001's own "revisit when" clause names as
the point at which Kafka stops being justified. This service reads `tw.market.trades.v1`
alongside `processor-persist`, in its own group, and neither can affect the other. That is
the claim being made good.

**Event time, not wall-clock time** (failure F12). A bar closes because a trade arrived
whose venue timestamp is past the boundary, not because a clock in this process ticked.

There are **two** watermarks, and the split is not fussiness — it was put in after watching
the aggregator drop live data. A *per-symbol* watermark decides when that symbol's bars are
sealed; the *global* one, the highest timestamp seen across all symbols, decides only when a
bar is closed and written.

Closing on the global watermark is what stops a symbol that trades once an hour from holding
a bar open until it trades again. But **sealing** on it is destructive. Kafka orders a
partition, not a topic, so when a consumer catches up on a backlog it reads partitions at
different rates: one symbol's trades can be minutes ahead of another's. A single global
watermark then seals the lagging symbol's bars and every one of its trades is discarded as
late — which is exactly what happened on the first replay here, and the only sign of it was
a `late_dropped` counter climbing while the bars looked fine.

So a symbol's data can only ever be sealed by that symbol's own progress. Closing early is
harmless, because a later trade reopens the bar as a correction; sealing early is not,
because the trade is gone.

**Corrections, and the bounded window** (failure F11). A bar closes promptly at its
boundary and is written immediately, because a downstream consumer waiting on a bar should
not also wait out the lateness allowance. A trade that arrives afterwards for that bar is
folded in and the bar is rewritten with ``revision + 1`` — a real correction, absorbed by
the revision-guarded upsert. Once the watermark passes ``bar_end + lateness``, the bar is
sealed: a trade for it is counted and dropped, because accepting a correction with no bound
means a bar is never final and nothing downstream can ever act on one.

**Restart recovery** (failure F13). The open bar is written on every flush with
``is_final=False``, and offsets are committed only after that write. On startup the
non-final bars are read back and accumulation resumes. A restart mid-minute therefore
produces the same closed bar it would have produced without one — which is the property the
integration test asserts by actually killing the service.

**VWAP is real here.** The aggregator sees the trades, so it computes
``sum(price * size) / sum(size)`` exactly. The rollups to 5m and above cannot: averaging
five VWAPs is not a VWAP, so they leave it null rather than writing a number that looks
right and is not.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from tradewatch import __version__
from tradewatch.application.ports.bar_store import BarStore
from tradewatch.domain.entities import Bar, NewBar
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, EventType, TradeEvent, deterministic_event_id
from tradewatch.domain.ids import new_id
from tradewatch.observability.logging import get_logger

__all__ = ["AggregatorStats", "BarAggregator", "OpenBar", "floor_to_interval"]

log = get_logger(__name__)

#: The `producer` field of every bar this process publishes.
_PRODUCER = f"bar-aggregator@{__version__}"

#: The interval built from trades. Everything coarser is rolled up from it (§3.3).
BASE_INTERVAL = BarInterval.ONE_MINUTE

#: How many lateness windows a closed bar is kept in memory after its minute ends. Purely a
#: cache: an evicted bar is reloaded from the store if a correction arrives for it, so this
#: trades memory against queries and never against correctness.
_EVICTION_MARGIN = 4


def floor_to_interval(moment: dt.datetime, interval: BarInterval) -> dt.datetime:
    """The start of the bar ``moment`` falls in.

    Anchored at the Unix epoch rather than at whatever the first trade happened to be, so
    bucket boundaries line up with wall-clock minutes — and, crucially, with the boundaries
    the SQL rollup uses. Two anchors would produce two different 5-minute bars for the same
    data, and only one of them would be wrong.
    """
    seconds = int(interval.duration.total_seconds())
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    elapsed = int((moment - epoch).total_seconds())
    return epoch + dt.timedelta(seconds=elapsed - elapsed % seconds)


@dataclass(slots=True)
class AggregatorStats:
    """Counters for one aggregator run.

    Each sits where a trade could otherwise vanish. `late_dropped` is the one to watch: it
    is the only number here that represents data the system decided not to keep.
    """

    trades: int = 0
    bars_opened: int = 0
    bars_closed: int = 0
    corrections: int = 0
    #: Trades for a bar already sealed by the lateness window. Dropped, deliberately.
    late_dropped: int = 0
    #: Trades whose price or size the bar invariants would not accept.
    rejected: int = 0
    reopened_from_store: int = 0
    restored_open_bars: int = 0
    flushes: int = 0
    rows_written: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "trades": self.trades,
            "bars_opened": self.bars_opened,
            "bars_closed": self.bars_closed,
            "corrections": self.corrections,
            "late_dropped": self.late_dropped,
            "rejected": self.rejected,
            "reopened_from_store": self.reopened_from_store,
            "restored_open_bars": self.restored_open_bars,
            "flushes": self.flushes,
            "rows_written": self.rows_written,
        }


@dataclass(slots=True)
class OpenBar:
    """A bar being accumulated.

    ``notional`` is carried rather than derived so VWAP stays exact: recomputing it from a
    rounded VWAP and a volume loses precision every time a bar is reopened for a
    correction, and money is never allowed to drift (ADR-0009).
    """

    instrument_id: Any
    symbol: str
    interval: BarInterval
    bar_start: dt.datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    notional: Decimal
    trade_count: int
    #: When the trades that set `open` and `close` happened. Carried because a correction
    #: can arrive out of order, and "the last trade applied" is not the same thing as "the
    #: last trade by venue time" once late data is in play.
    first_at: dt.datetime
    last_at: dt.datetime
    revision: int = 0
    closed: bool = False
    #: Set when a correction lands, so a flush knows this bar needs rewriting.
    dirty: bool = True
    #: Carried from the most recent trade so the published bar is a derived event in the
    #: §7.1 sense: same source, same correlation id as the prints it was built from. A bar
    #: restored from the database has neither until a trade lands in it.
    source: str = "aggregator"
    correlation_id: uuid.UUID | None = None

    @property
    def bar_end(self) -> dt.datetime:
        return self.bar_start + self.interval.duration

    @property
    def vwap(self) -> Decimal | None:
        """Exact volume-weighted price, or None when nothing has traded."""
        if self.volume <= 0:
            return None
        return self.notional / self.volume

    @classmethod
    def from_trade(cls, trade: TradeEvent, instrument_id: Any, interval: BarInterval) -> OpenBar:
        start = floor_to_interval(trade.occurred_at, interval)
        return cls(
            instrument_id=instrument_id,
            symbol=trade.symbol,
            interval=interval,
            bar_start=start,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.size,
            notional=trade.price * trade.size,
            trade_count=1,
            first_at=trade.occurred_at,
            last_at=trade.occurred_at,
            source=trade.source,
            correlation_id=trade.correlation_id,
        )

    @classmethod
    def from_stored(cls, bar: Bar, symbol: str) -> OpenBar:
        """Rebuild from a persisted bar, for a restart or a correction.

        A stored bar records the prices but not when the trades that set them happened, so
        ``last_at`` has to be inferred — and the right inference depends on *why* the bar is
        being loaded:

        * **Still open** (a restart, failure F13). Accumulation is resuming, and Kafka
          delivers a symbol's trades in order within its partition, so every trade still to
          come is later than every trade already folded in. ``last_at`` is the bar's start,
          which lets the next trade set the close exactly as it would have without the
          restart.
        * **Already closed** (a correction). The late trade cannot be placed relative to the
          trades that are no longer in memory, so ``last_at`` is pinned to the bar's end and
          the correction adjusts the aggregate figures — high, low, volume, VWAP, trade
          count — while leaving the opening and closing prices alone.

        The second is a real limitation, handled by refusing to guess. A correction that
        rewrote a close with a price from the middle of the minute would be worse than a
        stale one, because everything downstream is priced off the close.
        """
        volume = bar.volume
        notional = (bar.vwap * volume) if (bar.vwap is not None and volume > 0) else Decimal(0)
        return cls(
            first_at=bar.bar_start,
            last_at=(bar.bar_start + bar.interval.duration) if bar.is_final else bar.bar_start,
            instrument_id=bar.instrument_id,
            symbol=symbol,
            interval=bar.interval,
            bar_start=bar.bar_start,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=volume,
            notional=notional,
            trade_count=bar.trade_count,
            revision=bar.revision,
            closed=bar.is_final,
            dirty=False,
        )

    def apply(self, trade: TradeEvent) -> None:
        """Fold one trade in.

        ``open`` and ``close`` are decided by **venue time**, not by arrival order. In the
        ordinary case they are the same thing — Kafka delivers a symbol's trades in order
        within its partition, which is the whole reason the key is the symbol — but a
        correction arrives precisely when they differ. Assigning ``close`` unconditionally
        would let a trade from the middle of the minute become the closing price simply
        because it turned up last, and every figure derived from a close would inherit that.
        """
        if trade.occurred_at < self.first_at:
            self.open = trade.price
            self.first_at = trade.occurred_at
        if trade.occurred_at >= self.last_at:
            self.close = trade.price
            self.last_at = trade.occurred_at

        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.volume += trade.size
        self.notional += trade.price * trade.size
        self.trade_count += 1
        self.dirty = True
        self.source = trade.source
        self.correlation_id = trade.correlation_id

    def to_event(self, *, producer: str, now: dt.datetime) -> BarEvent:
        """The `market.bar.closed` (or `.corrected`) event for this bar, as published.

        Same natural key as a provider-published bar (ADR-0012): a redelivered or re-flushed
        bar derives the same event id, so every consumer's idempotency covers it.
        """
        key = BarEvent.natural_key(
            source=self.source,
            symbol=self.symbol,
            interval=self.interval,
            bar_start=self.bar_start,
            revision=self.revision,
        )
        return BarEvent(
            event_id=deterministic_event_id(*key),
            event_type=EventType.BAR_CORRECTED if self.revision else EventType.BAR_CLOSED,
            symbol=self.symbol,
            interval=self.interval,
            bar_start=self.bar_start,
            occurred_at=self.bar_end,
            ingested_at=now,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            vwap=self.vwap,
            trade_count=self.trade_count,
            revision=self.revision,
            source=self.source,
            producer=producer,
            correlation_id=self.correlation_id or new_id(),
        )

    def to_new_bar(self) -> NewBar:
        """The row to write. Validates the OHLC invariants before the database has to."""
        return NewBar(
            instrument_id=self.instrument_id,
            interval=self.interval,
            bar_start=self.bar_start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            vwap=self.vwap,
            trade_count=self.trade_count,
            is_final=self.closed,
            revision=self.revision,
        )


class BarAggregator:
    """Builds 1-minute bars from a trade stream."""

    def __init__(
        self,
        store: BarStore,
        *,
        interval: BarInterval = BASE_INTERVAL,
        lateness: dt.timedelta = dt.timedelta(seconds=5),
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        if not interval.is_base:
            raise ValueError(f"the aggregator builds {BASE_INTERVAL} bars, not {interval}")
        if lateness < dt.timedelta(0):
            raise ValueError("lateness must not be negative")

        self._store = store
        self._interval = interval
        self._lateness = lateness
        self._closed_events: list[BarEvent] = []
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        #: (symbol, bar_start) -> bar. Holds open bars and closed-but-still-revisable ones;
        #: a bar leaves when the watermark passes bar_end + lateness.
        self._bars: dict[tuple[str, dt.datetime], OpenBar] = {}
        self._instruments: dict[str, Any] = {}
        #: Highest venue timestamp seen per symbol. Monotonic, and the only thing allowed
        #: to seal that symbol's bars.
        self._watermarks: dict[str, dt.datetime] = {}
        #: Highest seen across every symbol. Closes bars, so a quiet symbol still gets one;
        #: never seals them. See the module docstring.
        self._watermark: dt.datetime | None = None
        self._stats = AggregatorStats()

    @property
    def stats(self) -> AggregatorStats:
        return self._stats

    @property
    def store(self) -> BarStore:
        """The store these bars are written to.

        Exposed because the consumer's position is written *into the same store, in the same
        transaction* as the bars — see :meth:`flush`. Handing the service a second reference
        to the store would let the two drift apart, and they are only correct together.
        """
        return self._store

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    @property
    def tracked(self) -> int:
        """Bars held in memory — open plus still-revisable."""
        return len(self._bars)

    @property
    def pending(self) -> int:
        """Bars with unwritten changes."""
        return sum(1 for bar in self._bars.values() if bar.dirty)

    # --- lifecycle ---------------------------------------------------------

    async def restore(self, symbols: Sequence[str] | None = None) -> int:
        """Reload bars left open by a previous run (failure F13). Returns how many.

        Called once at startup, before any trade is handled. The watermark is seeded from
        the newest restored bar so the trades that arrive next are not all judged late
        against a watermark of "never".
        """
        stored = await self._store.load_open_bars(self._interval, symbols=symbols)
        for symbol, bar in stored:
            restored = OpenBar.from_stored(bar, symbol)
            self._bars[(symbol, bar.bar_start)] = restored
            self._instruments[symbol] = bar.instrument_id
            self._advance_watermark(restored.bar_start, symbol)

        self._stats.restored_open_bars = len(stored)
        if stored:
            log.info(
                "aggregator.restored",
                bars=len(stored),
                watermark=self._watermark.isoformat() if self._watermark else None,
            )
        return len(stored)

    # --- consuming ---------------------------------------------------------

    async def handle(self, trade: TradeEvent) -> None:
        """Fold one trade into its bar, closing whatever the new watermark seals."""
        self._stats.trades += 1
        self._advance_watermark(trade.occurred_at, trade.symbol)

        bar_start = floor_to_interval(trade.occurred_at, self._interval)
        key = (trade.symbol, bar_start)
        bar = self._bars.get(key)

        if bar is None:
            bar = await self._open_or_reopen(trade, bar_start)
            if bar is None:
                return
            self._bars[key] = bar
        else:
            if self._is_sealed(bar):
                self._drop_late(trade, bar_start)
                return
            if bar.closed:
                # A correction: the bar was written, and this trade belongs in it.
                bar.closed = False
                bar.revision += 1
                self._stats.corrections += 1
                log.info(
                    "aggregator.bar_corrected",
                    symbol=trade.symbol,
                    bar_start=bar_start.isoformat(),
                    revision=bar.revision,
                )
            bar.apply(trade)

        self._close_due_bars()

    async def handle_many(self, trades: Iterable[TradeEvent]) -> None:
        for trade in trades:
            await self.handle(trade)

    async def _open_or_reopen(self, trade: TradeEvent, bar_start: dt.datetime) -> OpenBar | None:
        """Start a new bar, or bring back one this process has never seen.

        The reopen path is what stops a restart from destroying history. A trade for a bar
        that was closed and evicted — or closed by a previous process — must not create a
        fresh bar containing that trade alone: the revision-guarded upsert would accept it,
        because a new bar's revision is 0 and so is the stored one's, and a correct bar
        would be replaced by a one-trade bar. Nothing downstream would ever know.
        """
        if self._sealed_by_watermark(trade.symbol, bar_start):
            self._drop_late(trade, bar_start)
            return None

        instrument_id = await self._instrument_id(trade.symbol)
        existing = await self._store.load_bar(instrument_id, self._interval, bar_start)

        if existing is None:
            self._stats.bars_opened += 1
            return OpenBar.from_trade(trade, instrument_id, self._interval)

        bar = OpenBar.from_stored(existing, trade.symbol)

        if not existing.is_final:
            # An unfinished bar this process had not loaded — the ordinary resume path.
            # Not a correction: nothing downstream has seen this bar as final, so there is
            # nothing to correct and the revision must stay where it is. Bumping it here
            # would announce a correction that never happened, and every consumer that
            # watches `revision` to detect rewritten history would act on it.
            bar.apply(trade)
            self._stats.bars_opened += 1
            return bar

        bar.closed = False
        bar.revision += 1
        bar.apply(trade)
        self._stats.reopened_from_store += 1
        self._stats.corrections += 1
        log.info(
            "aggregator.bar_reopened",
            symbol=trade.symbol,
            bar_start=bar_start.isoformat(),
            revision=bar.revision,
            detail="a trade arrived for a closed bar this process had not seen",
        )
        return bar

    async def _instrument_id(self, symbol: str) -> Any:
        cached = self._instruments.get(symbol)
        if cached is not None:
            return cached
        instrument_id = await self._store.resolve_instrument(symbol)
        self._instruments[symbol] = instrument_id
        return instrument_id

    # --- time --------------------------------------------------------------

    def _advance_watermark(self, moment: dt.datetime, symbol: str | None = None) -> None:
        if self._watermark is None or moment > self._watermark:
            self._watermark = moment
        if symbol is not None:
            seen = self._watermarks.get(symbol)
            if seen is None or moment > seen:
                self._watermarks[symbol] = moment

    def _sealed_by_watermark(self, symbol: str, bar_start: dt.datetime) -> bool:
        """Whether this symbol's own progress has closed the door on this bar.

        Deliberately *not* the global watermark. A symbol whose partition is being read
        more slowly than another's must not have its trades thrown away because a different
        symbol has run ahead.
        """
        seen = self._watermarks.get(symbol)
        if seen is None:
            return False
        return seen >= bar_start + self._interval.duration + self._lateness

    def _is_sealed(self, bar: OpenBar) -> bool:
        return self._sealed_by_watermark(bar.symbol, bar.bar_start)

    def _is_evictable(self, bar: OpenBar) -> bool:
        """Whether a written, closed bar can be forgotten.

        Eviction is bounded by the *global* watermark, unlike sealing, because forgetting a
        bar loses nothing: a later trade for it is reloaded from the store and applied as a
        correction. The margin keeps a bar in memory well past its window so the common case
        needs no query.
        """
        if self._watermark is None:
            return False
        margin = self._interval.duration + self._lateness * _EVICTION_MARGIN
        return self._watermark >= bar.bar_end + margin

    def _drop_late(self, trade: TradeEvent, bar_start: dt.datetime) -> None:
        """Count and log a trade that arrived after its bar was sealed (failure F11)."""
        self._stats.late_dropped += 1
        seen = self._watermarks.get(trade.symbol)
        behind = (seen - trade.occurred_at) if seen else dt.timedelta(0)
        log.warning(
            "aggregator.late_trade_dropped",
            symbol=trade.symbol,
            event_id=str(trade.event_id),
            bar_start=bar_start.isoformat(),
            occurred_at=trade.occurred_at.isoformat(),
            behind_seconds=round(behind.total_seconds(), 3),
            lateness_window_seconds=self._lateness.total_seconds(),
            total_late_dropped=self._stats.late_dropped,
        )

    def _close_due_bars(self) -> None:
        """Close every bar whose minute the watermark has passed."""
        if self._watermark is None:
            return
        for bar in self._bars.values():
            if not bar.closed and self._watermark >= bar.bar_end:
                bar.closed = True
                bar.dirty = True
                self._stats.bars_closed += 1
                log.debug(
                    "aggregator.bar_closed",
                    symbol=bar.symbol,
                    bar_start=bar.bar_start.isoformat(),
                    trades=bar.trade_count,
                )

    def _evict_sealed(self) -> None:
        """Forget bars past their lateness window, so memory is bounded by the window.

        Only after they have been written: a dirty bar is kept regardless, because dropping
        it would lose the correction it is holding.
        """
        for key, bar in list(self._bars.items()):
            if bar.closed and not bar.dirty and self._is_evictable(bar):
                del self._bars[key]

    # --- writing -----------------------------------------------------------

    async def flush(
        self,
        *,
        consumer_group: str | None = None,
        positions: Mapping[str, int] | None = None,
    ) -> int:
        """Write every bar with unsaved changes. Returns rows written.

        Open bars are written too, with ``is_final=False``. That is what makes a restart
        uneventful, and it is why the caller may commit its offsets immediately afterwards:
        everything consumed so far is now represented in the database, finished or not.

        **When ``positions`` are given, they are written in the same transaction as the
        bars** (ADR-0016). That atomicity is not an optimisation, it is the correctness
        argument: bars and progress written separately can disagree after a crash between
        them, and the two ways they disagree are "a bar counted an event the progress says
        was never applied", which double counts the next time that event is delivered, and
        "the progress claims an event no bar ever saw", which is a hole. One transaction
        admits neither.
        """
        dirty = [bar for bar in self._bars.values() if bar.dirty]
        if not dirty and not positions:
            return 0

        rows = [bar.to_new_bar() for bar in dirty]
        if consumer_group is not None and positions:
            written = await self._store.save_bars_with_progress(
                rows, consumer_group=consumer_group, positions=positions
            )
        else:
            written = await self._store.save_bars(rows)

        # Closed bars become events only once they are durable, so nothing downstream
        # ever acts on a bar the database does not have. Collected here, published by the
        # caller (`take_closed_events`): this class owns bars, not a broker connection.
        now = self._clock()
        self._closed_events.extend(
            bar.to_event(producer=_PRODUCER, now=now) for bar in dirty if bar.closed
        )
        for bar in dirty:
            bar.dirty = False

        self._stats.flushes += 1
        self._stats.rows_written += written
        self._evict_sealed()

        log.debug(
            "aggregator.flushed",
            bars=len(rows),
            written=written,
            final=sum(1 for bar in dirty if bar.closed),
            tracked=len(self._bars),
        )
        return written

    def take_closed_events(self) -> list[BarEvent]:
        """Bars that closed (or were corrected after closing) in flushes since the last call."""
        events, self._closed_events = self._closed_events, []
        return events

    def discard_pending(self) -> int:
        """Throw away bars with unsaved changes. Returns how many were dropped.

        For one situation only: the store refused them permanently, so keeping them means
        every later flush fails identically and the partition stops forever on a bar that
        will never be writable (failure F3). Dropping them leaves a hole in that symbol's
        series, which is why the caller logs it at error rather than warning — a gap in a
        bar series is the kind of thing a backtest silently reads straight through.
        """
        dropped = 0
        for key, bar in list(self._bars.items()):
            if bar.dirty:
                dropped += 1
                del self._bars[key]
        if dropped:
            log.warning("aggregator.bars_discarded", bars=dropped)
        return dropped

    async def close_all(
        self,
        *,
        consumer_group: str | None = None,
        positions: Mapping[str, int] | None = None,
    ) -> int:
        """Close and write every open bar, whatever the watermark says.

        Shutdown only. A bar closed this way covers a minute that may not have finished, so
        it is a partial bar marked final — which is the honest thing for a service that is
        stopping, and is why the flush path normally never does it.

        ``positions`` matter here more than anywhere else. These bars are written on a path
        that does **not** commit its Kafka offsets, so every trade in them is guaranteed to
        be delivered again. Without the position recorded beside them, those trades would be
        folded into the already-written bar a second time on restart — the shutdown-shaped
        instance of the doubling bug in ADR-0016, and the reason this argument is spelled out
        rather than left to the caller.
        """
        closing = [bar for bar in self._bars.values() if not bar.closed]
        for bar in closing:
            bar.closed = True
            bar.dirty = True
            self._stats.bars_closed += 1
        if closing:
            log.info("aggregator.closing_open_bars", bars=len(closing))
        return await self.flush(consumer_group=consumer_group, positions=positions)
