"""The normalization layer.

Adapters know where a vendor puts a price. This module knows what a price *is*. Splitting it
that way means each adapter is a short mapping from field names, and the rules that must be
identical across every provider — timestamp handling, decimal conversion, symbol casing,
event identity — have exactly one implementation.

Three rules are worth stating outright:

**Every timestamp becomes an aware UTC datetime.** Providers send RFC 3339 strings with a
`Z`, with an offset, occasionally without a zone at all; and epoch numbers in seconds,
milliseconds, microseconds or nanoseconds, sometimes as strings. :func:`parse_timestamp`
takes all of them and rejects what it cannot place, rather than guessing and producing a
timestamp that is wrong by a factor of a thousand.

**Every price becomes a Decimal, via a string.** ``Decimal(0.1)`` is
``0.1000000000000000055511151231257827021181583404541015625``; ``Decimal("0.1")`` is
``0.1``. JSON gives floats, so the conversion route matters (ADR-0009).

**Every event id is derived from the data.** :class:`EventFactory` builds ids with
:func:`~tradewatch.domain.events.deterministic_event_id`, so normalizing the same trade
twice produces the same id twice, and the database's unique constraint absorbs the duplicate
(ADR-0012).
"""

from __future__ import annotations

import datetime as dt
import math
import uuid
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any, Final

from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import (
    BarEvent,
    EventType,
    QuoteEvent,
    TradeEvent,
    deterministic_event_id,
)
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.providers.errors import ProviderDataError

__all__ = [
    "EventFactory",
    "normalize_symbol",
    "parse_decimal",
    "parse_timestamp",
    "require",
]

#: Bounds used to work out which unit an epoch number is in. 1e11 seconds is the year 5138
#: and 1e11 milliseconds is 1973, so any value above the threshold is the finer unit. The
#: ladder is unambiguous for every timestamp between 1973 and the year 5138, which covers
#: every timestamp a market data provider will ever send.
_EPOCH_SECONDS_MAX: Final = 1e11
_EPOCH_MILLIS_MAX: Final = 1e14
_EPOCH_MICROS_MAX: Final = 1e17

#: Anything outside this range is a unit the ladder above got wrong, or a corrupt value.
#: Market data from 1990 or from 2200 is not market data.
_PLAUSIBLE_MIN: Final = dt.datetime(1990, 1, 1, tzinfo=dt.UTC)
_PLAUSIBLE_MAX: Final = dt.datetime(2100, 1, 1, tzinfo=dt.UTC)


def parse_timestamp(value: object, *, field: str = "timestamp", provider: str = "") -> dt.datetime:
    """Turn whatever a provider sent into an aware UTC datetime.

    Accepts RFC 3339 strings (with `Z`, with an offset, or with none — in which case UTC is
    assumed, because every provider that omits the zone documents its timestamps as UTC),
    and epoch numbers in seconds, milliseconds, microseconds or nanoseconds, as ``int``,
    ``float`` or numeric string.

    Raises:
        ProviderDataError: If the value cannot be parsed, or lands outside 1990-2100 —
            which means the unit was misread and the result would be silently wrong.
    """
    parsed = _to_datetime(value, field=field, provider=provider)

    if parsed.tzinfo is None:
        # Documented as UTC by every provider that omits the offset. Assuming local time
        # would make the pipeline's behaviour depend on the server's timezone.
        parsed = parsed.replace(tzinfo=dt.UTC)

    parsed = parsed.astimezone(dt.UTC)

    if not (_PLAUSIBLE_MIN <= parsed <= _PLAUSIBLE_MAX):
        raise ProviderDataError(
            f"{field} {parsed.isoformat()} is outside the plausible range; "
            "the epoch unit was probably misread",
            provider=provider,
            detail={field: repr(value)},
        )
    return parsed


def _to_datetime(value: object, *, field: str, provider: str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ProviderDataError(f"{field} is empty", provider=provider)
        # A numeric string is an epoch value, not a date.
        try:
            return _from_epoch(float(text), field=field, provider=provider)
        except ValueError:
            pass
        try:
            # `fromisoformat` handles `Z` from Python 3.11 onward.
            return dt.datetime.fromisoformat(text)
        except ValueError as error:
            raise ProviderDataError(
                f"{field} {text!r} is not a recognised timestamp",
                provider=provider,
                raw=text,
            ) from error

    if isinstance(value, bool):
        # bool is an int subclass, and `True` would otherwise become 1970-01-01.
        raise ProviderDataError(f"{field} is a boolean, not a timestamp", provider=provider)

    if isinstance(value, int | float):
        return _from_epoch(float(value), field=field, provider=provider)

    raise ProviderDataError(
        f"{field} has unsupported type {type(value).__name__}", provider=provider
    )


def _from_epoch(value: float, *, field: str, provider: str) -> dt.datetime:
    """Interpret an epoch number, working out its unit from its magnitude."""
    if math.isnan(value) or math.isinf(value):
        raise ProviderDataError(f"{field} is not a finite number", provider=provider)

    magnitude = abs(value)
    if magnitude < _EPOCH_SECONDS_MAX:
        seconds = value
    elif magnitude < _EPOCH_MILLIS_MAX:
        seconds = value / 1_000
    elif magnitude < _EPOCH_MICROS_MAX:
        seconds = value / 1_000_000
    else:
        seconds = value / 1_000_000_000

    try:
        return dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ProviderDataError(
            f"{field} {value!r} is not a valid epoch timestamp", provider=provider
        ) from error


def parse_decimal(
    value: object,
    *,
    field: str = "value",
    provider: str = "",
    allow_zero: bool = False,
    allow_negative: bool = False,
) -> Decimal:
    """Turn a JSON number or string into an exact Decimal.

    Floats go through ``str`` first: ``Decimal(0.1)`` carries the binary representation's
    full error, ``Decimal(str(0.1))`` does not (ADR-0009).

    Raises:
        ProviderDataError: If the value is not a finite number, or breaks the sign rules.
    """
    if isinstance(value, bool):
        raise ProviderDataError(f"{field} is a boolean, not a number", provider=provider)

    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ProviderDataError(f"{field} is not a finite number", provider=provider)
        number = Decimal(str(value))
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ProviderDataError(f"{field} is empty", provider=provider)
        try:
            number = Decimal(text)
        except InvalidOperation as error:
            raise ProviderDataError(
                f"{field} {text!r} is not a number", provider=provider, raw=text
            ) from error
    else:
        raise ProviderDataError(
            f"{field} has unsupported type {type(value).__name__}", provider=provider
        )

    if not number.is_finite():
        raise ProviderDataError(f"{field} is not a finite number", provider=provider)
    if not allow_negative and number < 0:
        raise ProviderDataError(f"{field} {number} is negative", provider=provider)
    if not allow_zero and number == 0:
        raise ProviderDataError(f"{field} is zero", provider=provider)
    return number


def normalize_symbol(value: object, *, provider: str = "") -> str:
    """Upper-case and strip a ticker.

    Symbols are stored upper-case and a CHECK constraint enforces it, so normalising at the
    boundary means a lookup never has to guess which case a provider used.
    """
    if not isinstance(value, str):
        raise ProviderDataError(
            f"symbol has unsupported type {type(value).__name__}", provider=provider
        )
    symbol = value.strip().upper()
    if not symbol:
        raise ProviderDataError("symbol is empty", provider=provider)
    if len(symbol) > 32:
        raise ProviderDataError(f"symbol {symbol[:32]!r} is too long", provider=provider)
    return symbol


def require(payload: Mapping[str, Any], key: str, *, provider: str = "") -> Any:
    """Read a required field, failing with a message that names it.

    The alternative — ``payload["p"]`` — produces a `KeyError: 'p'` from somewhere in the
    middle of a stream, which says nothing about which message or which provider.
    """
    if key not in payload:
        raise ProviderDataError(
            f"missing required field {key!r}",
            provider=provider,
            detail={"available": sorted(payload)[:20]},
        )
    value = payload[key]
    if value is None:
        raise ProviderDataError(f"required field {key!r} is null", provider=provider)
    return value


class EventFactory:
    """Builds normalized events with consistent identity, timestamps and provenance.

    Every adapter uses this rather than constructing events directly, so that the fields
    nobody thinks about — `source`, `producer`, `ingested_at`, and above all `event_id` —
    are right by construction instead of by each adapter remembering.

    The clock is injected. Tests freeze it; without that, every event carries
    `datetime.now()` and no assertion about `ingested_at` can be exact.
    """

    def __init__(
        self,
        *,
        source: str,
        producer: str,
        clock: Callable[[], dt.datetime] | None = None,
        correlation_id: uuid.UUID | None = None,
    ) -> None:
        self.source = source
        self.producer = producer
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        #: Shared by every event from one ingestion run, so a whole session's events can be
        #: traced together. Derived events inherit it (`ARCHITECTURE.md` §7.1).
        self._correlation_id = correlation_id or new_id()

    @property
    def correlation_id(self) -> uuid.UUID:
        return self._correlation_id

    def now(self) -> dt.datetime:
        return self._clock()

    def trade(
        self,
        *,
        symbol: str,
        price: Decimal,
        size: Decimal,
        occurred_at: dt.datetime,
        provider_trade_id: str | None = None,
        conditions: Sequence[str] = (),
    ) -> TradeEvent:
        key = TradeEvent.natural_key(
            source=self.source,
            symbol=symbol,
            occurred_at=occurred_at,
            price=price,
            size=size,
            provider_trade_id=provider_trade_id,
        )
        return TradeEvent(
            event_id=deterministic_event_id(*key),
            symbol=symbol,
            price=price,
            size=size,
            occurred_at=occurred_at,
            ingested_at=self.now(),
            source=self.source,
            producer=self.producer,
            correlation_id=self._correlation_id,
            provider_trade_id=provider_trade_id,
            conditions=tuple(conditions),
        )

    def quote(
        self,
        *,
        symbol: str,
        bid_price: Decimal,
        bid_size: Decimal,
        ask_price: Decimal,
        ask_size: Decimal,
        occurred_at: dt.datetime,
    ) -> QuoteEvent:
        key = QuoteEvent.natural_key(
            source=self.source,
            symbol=symbol,
            occurred_at=occurred_at,
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
        )
        return QuoteEvent(
            event_id=deterministic_event_id(*key),
            symbol=symbol,
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
            occurred_at=occurred_at,
            ingested_at=self.now(),
            source=self.source,
            producer=self.producer,
            correlation_id=self._correlation_id,
        )

    def bar(
        self,
        *,
        symbol: str,
        interval: BarInterval,
        bar_start: dt.datetime,
        open: Decimal,  # noqa: A002 — OHLC is the domain's vocabulary, not a shadowed builtin
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        vwap: Decimal | None = None,
        trade_count: int = 0,
        revision: int = 0,
    ) -> BarEvent:
        key = BarEvent.natural_key(
            source=self.source,
            symbol=symbol,
            interval=interval,
            bar_start=bar_start,
            revision=revision,
        )
        return BarEvent(
            event_id=deterministic_event_id(*key),
            event_type=EventType.BAR_CORRECTED if revision else EventType.BAR_CLOSED,
            symbol=symbol,
            interval=interval,
            bar_start=bar_start,
            # The bar became a fact when it closed, so that is when it occurred.
            occurred_at=bar_start + interval.duration,
            ingested_at=self.now(),
            open=open,
            high=high,
            low=low,
            close=close,
            volume=volume,
            vwap=vwap,
            trade_count=trade_count,
            revision=revision,
            source=self.source,
            producer=self.producer,
            correlation_id=self._correlation_id,
        )
