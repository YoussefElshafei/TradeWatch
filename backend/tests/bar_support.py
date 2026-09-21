"""Fakes shared by the bar aggregator, backfill and quality tests.

The store is idempotent in the same way PostgreSQL is — a revision-guarded upsert on
``(instrument, interval, bar_start)`` — because every one of these tests is ultimately about
whether re-running something changes the data. A fake that simply overwrote would let a
non-idempotent job pass.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from decimal import Decimal

from tradewatch.application.ports.market_data import (
    InstrumentSnapshot,
    PriceSnapshot,
    ProviderCapabilities,
)
from tradewatch.domain.entities import Bar, NewBar
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, QuoteEvent, TradeEvent
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.providers.errors import ProviderUnavailableError
from tradewatch.infrastructure.providers.normalization import EventFactory

__all__ = [
    "MINUTE",
    "ONE_MINUTE",
    "FakeBarStore",
    "FakeHistoricalProvider",
    "trade",
]

MINUTE = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)
ONE_MINUTE = BarInterval.ONE_MINUTE


def trade(
    *,
    seconds: float,
    price: str,
    size: str = "10",
    symbol: str = "AAPL",
    trade_id: str | None = None,
) -> TradeEvent:
    """A trade ``seconds`` after 14:30:00."""
    occurred = MINUTE + dt.timedelta(seconds=seconds)
    factory = EventFactory(source="test", producer="test@1.0", clock=lambda: occurred)
    return factory.trade(
        symbol=symbol,
        price=Decimal(price),
        size=Decimal(size),
        occurred_at=occurred,
        provider_trade_id=trade_id or f"{symbol}-{seconds}-{price}",
    )


class FakeBarStore:
    """An in-memory `BarStore`, idempotent like the real one."""

    def __init__(self) -> None:
        self.bars: dict[tuple[uuid.UUID, str, dt.datetime], NewBar] = {}
        self.instruments: dict[str, uuid.UUID] = {}
        self.symbols: dict[uuid.UUID, str] = {}
        self.writes: list[list[NewBar]] = []
        self.rollups: list[tuple[BarInterval, BarInterval]] = []
        self.gaps: list[tuple[dt.datetime, dt.datetime]] = []
        #: consumer group -> stream -> position, mirroring `consumer_progress`.
        self.progress: dict[str, dict[str, int]] = {}
        #: Raised by the next `save_bars`, then cleared.
        self.fail_next: Exception | None = None

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        wanted = symbol.upper()
        if wanted not in self.instruments:
            identifier = new_id()
            self.instruments[wanted] = identifier
            self.symbols[identifier] = wanted
        return self.instruments[wanted]

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error
        self.writes.append(list(bars))
        written = 0
        for row in bars:
            key = (row.instrument_id, row.interval.value, row.bar_start)
            existing = self.bars.get(key)
            if existing is None or existing.revision <= row.revision:
                self.bars[key] = row
                written += 1
        return written

    async def save_bars_with_progress(
        self,
        bars: Sequence[NewBar],
        *,
        consumer_group: str,
        positions: Mapping[str, int],
    ) -> int:
        """Atomic like the real one: either both land or neither does.

        The fake enforces the atomicity rather than assuming it — if `save_bars` raises, the
        progress must not move, because a progress ahead of the bars is exactly how a trade
        gets skipped and silently lost.
        """
        written = await self.save_bars(bars)
        group = self.progress.setdefault(consumer_group, {})
        for stream, offset in positions.items():
            group[stream] = max(group.get(stream, -1), offset)
        return written

    async def load_progress(self, consumer_group: str) -> Mapping[str, int]:
        return dict(self.progress.get(consumer_group, {}))

    async def load_bar(
        self, instrument_id: uuid.UUID, interval: BarInterval, bar_start: dt.datetime
    ) -> Bar | None:
        row = self.bars.get((instrument_id, interval.value, bar_start))
        if row is None:
            return None
        return Bar(**row.model_dump(), recorded_at=MINUTE)

    async def load_open_bars(
        self, interval: BarInterval, *, symbols: Sequence[str] | None = None
    ) -> Sequence[tuple[str, Bar]]:
        found: list[tuple[str, Bar]] = []
        for key, row in sorted(self.bars.items(), key=lambda item: item[0][2]):
            instrument_id, stored_interval, _ = key
            if stored_interval != interval.value or row.is_final:
                continue
            symbol = self.symbols[instrument_id]
            if symbols and symbol not in {item.upper() for item in symbols}:
                continue
            found.append((symbol, Bar(**row.model_dump(), recorded_at=MINUTE)))
        return found

    async def latest_bar_start(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        *,
        within: tuple[dt.datetime, dt.datetime] | None = None,
    ) -> dt.datetime | None:
        starts = [
            key[2]
            for key in self.bars
            if key[0] == instrument_id
            and key[1] == interval.value
            and (within is None or within[0] <= key[2] < within[1])
        ]
        return max(starts) if starts else None

    async def earliest_bar_start(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        *,
        within: tuple[dt.datetime, dt.datetime] | None = None,
    ) -> dt.datetime | None:
        starts = [
            key[2]
            for key in self.bars
            if key[0] == instrument_id
            and key[1] == interval.value
            and (within is None or within[0] <= key[2] < within[1])
        ]
        return min(starts) if starts else None

    async def count_bars(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        return sum(
            1
            for key in self.bars
            if key[0] == instrument_id and key[1] == interval.value and start <= key[2] < end
        )

    async def find_gaps(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[tuple[dt.datetime, dt.datetime]]:
        return list(self.gaps)

    async def roll_up(
        self,
        instrument_id: uuid.UUID,
        source: BarInterval,
        target: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        self.rollups.append((source, target))
        return 0

    async def health_check(self) -> str | None:
        return None

    # --- assertions helpers ---

    def bar_for(self, symbol: str, minute: int = 0) -> NewBar | None:
        instrument_id = self.instruments.get(symbol.upper())
        if instrument_id is None:
            return None
        start = MINUTE + dt.timedelta(minutes=minute)
        return self.bars.get((instrument_id, ONE_MINUTE.value, start))

    def stored_for(self, symbol: str, interval: BarInterval = ONE_MINUTE) -> list[NewBar]:
        instrument_id = self.instruments.get(symbol.upper())
        return sorted(
            (
                row
                for key, row in self.bars.items()
                if key[0] == instrument_id and key[1] == interval.value
            ),
            key=lambda row: row.bar_start,
        )


class FakeHistoricalProvider:
    """A provider that serves a deterministic minute series, and counts what was asked for.

    Every bar is a flat one whose price encodes its offset, so a test can tell which minute
    a stored row came from by looking at it.
    """

    name = "fake"

    def __init__(
        self,
        *,
        origin: dt.datetime = MINUTE,
        minutes: int = 10_000,
        capabilities: ProviderCapabilities | None = None,
        fail_after: int | None = None,
    ) -> None:
        self._origin = origin
        self._minutes = minutes
        self._capabilities = capabilities or ProviderCapabilities(
            historical_bars=True,
            instrument_list=True,
            intervals=frozenset({BarInterval.ONE_MINUTE, BarInterval.FIVE_MINUTES}),
        )
        #: Raise once this many windows have been served. Drives the "a job that dies
        #: partway through is safely restartable" test.
        self._fail_after = fail_after
        self.calls: list[tuple[str, dt.datetime, dt.datetime]] = []

    @property
    def capabilities(self) -> ProviderCapabilities:
        return self._capabilities

    async def list_instruments(self) -> Sequence[InstrumentSnapshot]:
        return []

    async def fetch_bars(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[BarEvent]:
        self.calls.append((symbol, start, end))
        if self._fail_after is not None and len(self.calls) > self._fail_after:
            raise ProviderUnavailableError("provider fell over", provider=self.name)

        factory = EventFactory(source=self.name, producer="test@1.0", clock=lambda: end)
        bars: list[BarEvent] = []
        moment = max(start, self._origin)
        limit = self._origin + interval.duration * self._minutes

        while moment < min(end, limit):
            offset = int((moment - self._origin) / interval.duration)
            price = Decimal(100 + offset)
            bars.append(
                factory.bar(
                    symbol=symbol,
                    interval=interval,
                    bar_start=moment,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=Decimal("1000"),
                    trade_count=1,
                )
            )
            moment += interval.duration
        return bars

    async def fetch_latest_price(self, symbol: str) -> PriceSnapshot | None:
        return None

    def stream_trades(self, symbols: Sequence[str]) -> AsyncIterator[TradeEvent]:
        """Not implemented: this fake exists for the historical path.

        Present with the port's real signature so the class still satisfies
        `MarketDataProvider` — a fake that only implements the methods a test happens to
        call drifts from the interface it is standing in for.
        """
        raise NotImplementedError

    def stream_quotes(self, symbols: Sequence[str]) -> AsyncIterator[QuoteEvent]:
        raise NotImplementedError

    async def aclose(self) -> None:
        return None
