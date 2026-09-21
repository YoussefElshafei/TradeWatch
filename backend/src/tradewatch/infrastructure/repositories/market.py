"""Repositories for reference and market data.

These carry the write path that Kafka will feed in Phase 2, so the shape that matters most
here is **idempotent bulk write**. Every method that ingests data can be called twice with
the same input and leave the database in the same state, because at-least-once delivery
guarantees it eventually will be (`ARCHITECTURE.md` §7.4, failures F6 and F7).

Market data is not owner-scoped: a trade print belongs to the market, not to a user. These
extend :class:`Repository`, not :class:`OwnedRepository`.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy import Numeric, func, select, text
from sqlalchemy.dialects.postgresql import ARRAY, aggregate_order_by
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.enums import BarInterval, InstrumentStatus
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.base import MONEY_PRECISION, MONEY_SCALE
from tradewatch.infrastructure.database.models import Bar, Instrument, Quote, Trade
from tradewatch.infrastructure.repositories.base import Repository, rowcount


class InstrumentRepository(Repository[Instrument]):
    """Reference data. Small, read constantly, and cached in Redis from Phase 4."""

    model = Instrument

    async def get_by_symbol(self, symbol: str) -> Instrument | None:
        """Look up by ticker. Symbols are stored uppercase, enforced by a CHECK."""
        result = await self.session.execute(
            select(Instrument).where(Instrument.symbol == symbol.upper())
        )
        return result.scalar_one_or_none()

    async def get_many_by_symbol(self, symbols: Sequence[str]) -> dict[str, Instrument]:
        """Resolve several tickers in one round trip.

        The ingestion path needs an instrument id for every incoming print. Doing that one
        SELECT at a time is the classic N+1 that turns a fast consumer into a slow one.
        """
        if not symbols:
            return {}
        wanted = [symbol.upper() for symbol in symbols]
        result = await self.session.execute(select(Instrument).where(Instrument.symbol.in_(wanted)))
        return {instrument.symbol: instrument for instrument in result.scalars()}

    async def list_active(self, *, limit: int = 500) -> Sequence[Instrument]:
        result = await self.session.execute(
            select(Instrument)
            .where(Instrument.status == "active")
            .order_by(Instrument.symbol)
            .limit(limit)
        )
        return result.scalars().all()

    async def upsert(self, instrument: Instrument) -> Instrument:
        """Insert, or update the mutable reference fields if the symbol already exists.

        Providers restate their instrument list on every reconnect. This makes that a no-op
        rather than a unique-violation, and keeps ``id`` stable so nothing referencing the
        instrument is orphaned.

        The defaults below are applied here rather than left to the column definitions.
        A core ``INSERT ... VALUES`` names every column explicitly, so an attribute that is
        still unset on a transient object is sent as an explicit NULL and *overrides* the
        server default — which fails a NOT NULL column with a confusing error.
        """
        statement = (
            pg_insert(Instrument)
            .values(
                # The ORM's `default=new_id` only runs on an ORM flush, not on a core
                # INSERT like this one, so a transient instrument would send id=NULL.
                id=instrument.id or new_id(),
                symbol=instrument.symbol,
                name=instrument.name,
                asset_class=instrument.asset_class,
                exchange=instrument.exchange,
                currency=instrument.currency,
                tick_size=instrument.tick_size,
                lot_size=instrument.lot_size if instrument.lot_size is not None else Decimal(1),
                status=instrument.status or InstrumentStatus.ACTIVE,
                provider_metadata=instrument.provider_metadata or {},
            )
            .on_conflict_do_update(
                index_elements=[Instrument.symbol],
                set_={
                    "name": instrument.name,
                    "asset_class": instrument.asset_class,
                    "exchange": instrument.exchange,
                    "currency": instrument.currency,
                    "tick_size": instrument.tick_size,
                    "lot_size": instrument.lot_size
                    if instrument.lot_size is not None
                    else Decimal(1),
                    "status": instrument.status or InstrumentStatus.ACTIVE,
                    "provider_metadata": instrument.provider_metadata or {},
                    "updated_at": func.now(),
                },
            )
            .returning(Instrument)
        )
        # Without `populate_existing`, an instrument already in the identity map is
        # returned with its *old* attribute values: the ORM matches the returned row to the
        # existing object and leaves it alone. The update would be in the database and
        # invisible to the caller, which is a genuinely confusing bug to chase.
        result = await self.session.execute(
            statement, execution_options={"populate_existing": True}
        )
        await self.session.flush()
        return result.scalar_one()


class TradeRepository(Repository[Trade]):
    """The trade tape."""

    model = Trade

    async def bulk_insert(self, trades: Sequence[NewTrade]) -> int:
        """Insert a batch, ignoring prints already stored.

        Returns the number of rows actually written, which is what the consumer reports as
        its duplicate rate. ``ON CONFLICT (event_id) DO NOTHING`` is the whole idempotency
        story: a consumer that dies after writing but before committing its offset replays
        the batch, and this makes the replay free.
        """
        if not trades:
            return 0
        rows = [
            {
                "event_id": trade.event_id,
                "instrument_id": trade.instrument_id,
                "price": trade.price,
                "size": trade.size,
                "occurred_at": trade.occurred_at,
                "ingested_at": trade.ingested_at,
                "provider": trade.provider,
                "provider_trade_id": trade.provider_trade_id,
            }
            for trade in trades
        ]
        statement = (
            pg_insert(Trade).values(rows).on_conflict_do_nothing(index_elements=[Trade.event_id])
        )
        result = await self.session.execute(statement)
        await self.session.flush()
        return rowcount(result)

    async def list_range(
        self,
        instrument_id: uuid.UUID,
        start: dt.datetime,
        end: dt.datetime,
        *,
        limit: int = 1000,
    ) -> Sequence[Trade]:
        """Trades for one instrument in a half-open window, newest first.

        Served by ``ix_trades_instrument_id_occurred_at``. The window is half-open
        ``[start, end)`` so consecutive windows tile without double-counting the boundary
        print — an off-by-one here silently inflates volume.
        """
        result = await self.session.execute(
            select(Trade)
            .where(
                Trade.instrument_id == instrument_id,
                Trade.occurred_at >= start,
                Trade.occurred_at < end,
            )
            .order_by(Trade.occurred_at.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def latest(self, instrument_id: uuid.UUID) -> Trade | None:
        """The most recent print. Cached in Redis from Phase 4 (ADR-0003).

        Tie-broken by `id`, which is an ascending identity and therefore insertion order.
        Several prints genuinely can share a timestamp, and ordering by `occurred_at`
        alone would let "the latest price" differ between two identical queries.
        """
        result = await self.session.execute(
            select(Trade)
            .where(Trade.instrument_id == instrument_id)
            .order_by(Trade.occurred_at.desc(), Trade.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


class QuoteRepository(Repository[Quote]):
    """Top-of-book snapshots."""

    model = Quote

    async def bulk_insert(self, quotes: Sequence[NewQuote]) -> int:
        if not quotes:
            return 0
        rows = [
            {
                "event_id": quote.event_id,
                "instrument_id": quote.instrument_id,
                "bid_price": quote.bid_price,
                "bid_size": quote.bid_size,
                "ask_price": quote.ask_price,
                "ask_size": quote.ask_size,
                "occurred_at": quote.occurred_at,
                "ingested_at": quote.ingested_at,
                "provider": quote.provider,
            }
            for quote in quotes
        ]
        statement = (
            pg_insert(Quote).values(rows).on_conflict_do_nothing(index_elements=[Quote.event_id])
        )
        result = await self.session.execute(statement)
        await self.session.flush()
        return rowcount(result)

    async def latest(self, instrument_id: uuid.UUID) -> Quote | None:
        """The most recent quote, tie-broken by insertion order like `TradeRepository`."""
        result = await self.session.execute(
            select(Quote)
            .where(Quote.instrument_id == instrument_id)
            .order_by(Quote.occurred_at.desc(), Quote.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


class BarRepository(Repository[Bar]):
    """OHLCV bars — the read surface for every chart and every backtest."""

    model = Bar

    async def upsert_many(self, bars: Sequence[NewBar]) -> int:
        """Write bars, replacing any that already exist for the same key.

        Upsert rather than insert-or-ignore, because a bar legitimately changes: a late
        trade produces a corrected bar with a higher ``revision``. The natural primary key
        (instrument, interval, start) is the conflict target, so a correction overwrites the
        bar it corrects instead of creating a second one for the same minute.
        """
        if not bars:
            return 0
        rows = [
            {
                "instrument_id": bar.instrument_id,
                "interval": bar.interval,
                "bar_start": bar.bar_start,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "vwap": bar.vwap,
                "trade_count": bar.trade_count,
                "is_final": bar.is_final,
                "revision": bar.revision,
            }
            for bar in bars
        ]
        statement = pg_insert(Bar).values(rows)
        statement = statement.on_conflict_do_update(
            index_elements=[Bar.instrument_id, Bar.interval, Bar.bar_start],
            set_={
                "open": statement.excluded.open,
                "high": statement.excluded.high,
                "low": statement.excluded.low,
                "close": statement.excluded.close,
                "volume": statement.excluded.volume,
                "vwap": statement.excluded.vwap,
                "trade_count": statement.excluded.trade_count,
                "is_final": statement.excluded.is_final,
                "revision": statement.excluded.revision,
                "updated_at": func.now(),
            },
            # Never let an older revision overwrite a newer one. Kafka guarantees order
            # within a partition, but a backfill job and the live aggregator can write the
            # same bar from different directions.
            where=Bar.revision <= statement.excluded.revision,
        )
        result = await self.session.execute(statement)
        await self.session.flush()
        return rowcount(result)

    async def list_range(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
        *,
        limit: int = 5000,
    ) -> Sequence[Bar]:
        """Bars in a half-open window, oldest first.

        Oldest first because that is the order a chart draws and a backtest replays, and
        because it is the order the primary key already stores them in — so there is no
        sort.
        """
        result = await self.session.execute(
            select(Bar)
            .where(
                Bar.instrument_id == instrument_id,
                Bar.interval == interval,
                Bar.bar_start >= start,
                Bar.bar_start < end,
            )
            .order_by(Bar.bar_start)
            .limit(limit)
        )
        return result.scalars().all()

    async def latest(self, instrument_id: uuid.UUID, interval: BarInterval) -> Bar | None:
        result = await self.session.execute(
            select(Bar)
            .where(Bar.instrument_id == instrument_id, Bar.interval == interval)
            .order_by(Bar.bar_start.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def find_gaps(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> list[tuple[dt.datetime, dt.datetime]]:
        """Return the holes in a bar series, as (gap_start, gap_end) pairs.

        Used by the Phase 3 backfill to work out what to fetch after a provider outage
        (failure F1). Implemented with a window function so it is one query rather than a
        scan in Python: comparing each bar's start to the previous one's end is exactly
        what ``lag()`` is for.

        A gap is not necessarily missing data — the market closes — so callers apply
        session hours before concluding anything is wrong.
        """
        previous_end = func.lag(Bar.bar_start).over(order_by=Bar.bar_start)
        subquery = (
            select(Bar.bar_start, previous_end.label("previous_start"))
            .where(
                Bar.instrument_id == instrument_id,
                Bar.interval == interval,
                Bar.bar_start >= start,
                Bar.bar_start < end,
            )
            .subquery()
        )
        result = await self.session.execute(
            select(subquery.c.previous_start, subquery.c.bar_start).where(
                subquery.c.previous_start.is_not(None),
                subquery.c.bar_start - subquery.c.previous_start > interval.duration,
            )
        )
        return [(previous + interval.duration, current) for previous, current in result.all()]

    async def aggregate(
        self,
        instrument_id: uuid.UUID,
        source: BarInterval,
        target: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[NewBar]:
        """Roll bars up to a coarser interval, in SQL.

        `ARCHITECTURE.md` §3.3: higher intervals are derived from 1m bars rather than
        re-aggregated from trades. Doing it in SQL keeps a year of minutes out of Python
        memory, and it is the one query in this repository that is genuinely interesting:

        * ``date_bin`` buckets each bar into a window of the target interval, anchored at
          the epoch so buckets line up with wall-clock boundaries rather than with whatever
          the earliest row happened to be;
        * high and low are plain ``max``/``min``;
        * open and close are **not**. The open of a 5-minute bar is the open of its
          *earliest* minute, not the smallest opening price in it. ``array_agg`` with an
          explicit ordering picks the right row; ``min(open)`` would look reasonable and be
          wrong, which is the sort of bug that survives into a backtest result.

        Volume-weighted price is left None: averaging five VWAPs is not a VWAP, and
        computing it properly needs the underlying trades.
        """
        if target.duration <= source.duration:
            raise ValueError(f"cannot aggregate {source} up to {target}")

        seconds = int(target.duration.total_seconds())
        bucket = func.date_bin(
            text(f"interval '{seconds} seconds'"),
            Bar.bar_start,
            text("timestamptz '1970-01-01 00:00:00+00'"),
        ).label("bucket")
        money = ARRAY(Numeric(MONEY_PRECISION, MONEY_SCALE))

        statement = (
            select(
                bucket,
                func.array_agg(aggregate_order_by(Bar.open, Bar.bar_start.asc()), type_=money)[
                    1
                ].label("open"),
                func.max(Bar.high).label("high"),
                func.min(Bar.low).label("low"),
                func.array_agg(aggregate_order_by(Bar.close, Bar.bar_start.desc()), type_=money)[
                    1
                ].label("close"),
                func.sum(Bar.volume).label("volume"),
                func.sum(Bar.trade_count).label("trade_count"),
            )
            .where(
                Bar.instrument_id == instrument_id,
                Bar.interval == source,
                Bar.bar_start >= start,
                Bar.bar_start < end,
            )
            .group_by(bucket)
            .order_by(bucket)
        )
        result = await self.session.execute(statement)
        return [
            NewBar(
                instrument_id=instrument_id,
                interval=target,
                bar_start=row.bucket,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                trade_count=int(row.trade_count),
                vwap=None,
                is_final=True,
                revision=0,
            )
            for row in result.all()
        ]


__all__ = [
    "BarRepository",
    "InstrumentRepository",
    "QuoteRepository",
    "TradeRepository",
]
