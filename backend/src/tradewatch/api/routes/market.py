"""Market data endpoints.

The read half of the vertical slice: whatever the processor stored, these serve. They are
strictly read-only — there is no client-facing route that writes a tick, a bar or an
instrument, because that write path exists only inside the trust boundary
(`ARCHITECTURE.md` §3.9).

Every failure mode has a deliberate status code, because a browser can act on some of them
and not others:

* **404** — the symbol is not one we know. A client should stop asking.
* **422** — the *request* is wrong: a reversed date range, an out-of-range limit.
* **200 with an empty list** — the request was fine and there is no data. A market that has
  not opened yet is not an error, and returning 404 for it would make a normal Sunday
  indistinguishable from a typo.
* **503** — the database is unreachable. A client should retry later.

That third case is the one worth being careful about. Conflating "no data" with "no such
thing" is the most common mistake in a market data API, and it makes every client build a
workaround.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query, status

from tradewatch.api.dependencies import SessionDep, SettingsDep
from tradewatch.api.schemas.market import (
    BarPoint,
    BarSeriesResponse,
    InstrumentListResponse,
    InstrumentResponse,
    LatestPriceResponse,
)
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.repositories import (
    BarRepository,
    InstrumentRepository,
    QuoteRepository,
    TradeRepository,
)
from tradewatch.observability.logging import get_logger

router = APIRouter(prefix="/api/v1", tags=["market data"])

log = get_logger(__name__)

#: Absolute ceiling on bars per request, whatever the caller asks for. A chart cannot draw
#: more than a few thousand points usefully, and an unbounded limit is how one client stalls
#: the database for everyone else.
MAX_BARS = 5_000

SymbolPath = Annotated[
    str,
    Path(
        min_length=1,
        max_length=32,
        description="Ticker symbol. Case-insensitive.",
        examples=["AAPL"],
    ),
]


def _normalize(symbol: str) -> str:
    """Symbols are stored upper-case; callers should not have to know that."""
    return symbol.strip().upper()


async def _require_instrument(session: SessionDep, symbol: str) -> object:
    """Look up an instrument, or 404.

    Distinguishing "unknown symbol" from "no data for a known symbol" is the whole reason
    this is a separate lookup rather than an empty result.
    """
    instrument = await InstrumentRepository(session).get_by_symbol(symbol)
    if instrument is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown symbol {symbol!r}.",
        )
    return instrument


@router.get(
    "/instruments",
    response_model=InstrumentListResponse,
    summary="List tradable instruments",
)
async def list_instruments(
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=1000, description="Maximum instruments.")] = 200,
) -> InstrumentListResponse:
    """Every active instrument. This is what populates the symbol selector."""
    instruments = await InstrumentRepository(session).list_active(limit=limit)
    return InstrumentListResponse(
        count=len(instruments),
        instruments=[InstrumentResponse.model_validate(row) for row in instruments],
    )


@router.get(
    "/instruments/{symbol}",
    response_model=InstrumentResponse,
    summary="Look up one instrument",
    responses={404: {"description": "Unknown symbol"}},
)
async def get_instrument(session: SessionDep, symbol: SymbolPath) -> InstrumentResponse:
    instrument = await _require_instrument(session, _normalize(symbol))
    return InstrumentResponse.model_validate(instrument)


@router.get(
    "/market/{symbol}/bars",
    response_model=BarSeriesResponse,
    summary="Historical OHLCV bars",
    responses={
        404: {"description": "Unknown symbol"},
        422: {"description": "Invalid time range or parameters"},
    },
)
async def get_bars(
    session: SessionDep,
    settings: SettingsDep,
    symbol: SymbolPath,
    interval: Annotated[BarInterval, Query(description="Bar interval.")] = BarInterval.ONE_MINUTE,
    start: Annotated[
        dt.datetime | None,
        Query(description="Inclusive start, ISO 8601. Defaults to one window before `end`."),
    ] = None,
    end: Annotated[
        dt.datetime | None,
        Query(description="Exclusive end, ISO 8601. Defaults to now."),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_BARS, description="Maximum bars.")] = 500,
) -> BarSeriesResponse:
    """Bars for one symbol over a half-open window `[start, end)`.

    Half-open so consecutive requests tile without the caller receiving the boundary bar
    twice — an off-by-one here shows up as a doubled candle on a chart.

    With neither bound given, the window is the last `limit` intervals ending now, which is
    what a chart opening on a symbol actually wants.
    """
    wanted = _normalize(symbol)
    if interval not in set(settings.market.bar_intervals) and settings.market.bar_intervals:
        # Configured intervals are what the aggregator actually produces; serving another
        # would always return nothing and look like missing data.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Interval {interval.value!r} is not configured. "
                f"Available: {', '.join(settings.market.bar_intervals)}."
            ),
        )

    resolved_end = end or dt.datetime.now(dt.UTC)
    resolved_start = start or (resolved_end - interval.duration * limit)

    if resolved_start.tzinfo is None or resolved_end.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="`start` and `end` must include a timezone offset, for example 'Z'.",
        )
    if resolved_end <= resolved_start:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"`end` ({resolved_end.isoformat()}) must be after "
                f"`start` ({resolved_start.isoformat()})."
            ),
        )

    instrument = await _require_instrument(session, wanted)
    bars = await BarRepository(session).list_range(
        instrument.id,  # type: ignore[attr-defined]
        interval,
        resolved_start,
        resolved_end,
        limit=limit,
    )

    return BarSeriesResponse(
        symbol=wanted,
        interval=interval,
        start=resolved_start,
        end=resolved_end,
        count=len(bars),
        bars=[BarPoint.model_validate(bar) for bar in bars],
    )


@router.get(
    "/market/{symbol}/latest",
    response_model=LatestPriceResponse,
    summary="Latest stored price",
    responses={404: {"description": "Unknown symbol"}},
)
async def get_latest(session: SessionDep, symbol: SymbolPath) -> LatestPriceResponse:
    """The most recent trade and quote for a symbol.

    Both may be absent for a known instrument that has not traded yet, which is a real
    answer rather than an error — the response says so with nulls and no timestamps.

    This reads from stored data, not from the provider. The API is downstream of the
    processor and never calls a vendor itself; that would put a third party's latency and
    quota on the browser's critical path.
    """
    wanted = _normalize(symbol)
    instrument = await _require_instrument(session, wanted)
    instrument_id = instrument.id  # type: ignore[attr-defined]

    trade = await TradeRepository(session).latest(instrument_id)
    quote = await QuoteRepository(session).latest(instrument_id)

    return LatestPriceResponse(
        symbol=wanted,
        price=trade.price if trade else None,
        size=trade.size if trade else None,
        traded_at=trade.occurred_at if trade else None,
        bid_price=quote.bid_price if quote else None,
        ask_price=quote.ask_price if quote else None,
        quoted_at=quote.occurred_at if quote else None,
        source=(trade.provider if trade else None) or (quote.provider if quote else None),
    )
