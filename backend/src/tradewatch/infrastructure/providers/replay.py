"""A provider that replays recorded events from a file.

The synthetic provider invents data; this one plays back data that actually happened. Both
matter, and they answer different questions.

A generator can be asked for a volume spike at message 300 and can produce ten years of
history on demand — which a fixture cannot. A recording carries the shape real data has:
the timestamp irregularity, the odd lot sizes, the condition codes, the quiet minutes. A
normalizer that only ever sees generated input is a normalizer that has only ever been
tested against its author's assumptions.

Recordings are JSON Lines — one already-normalized event per line — so a file can be
inspected, edited, trimmed and diffed with ordinary tools. `capture_stream` writes them, so
a real provider session can be turned into a permanent regression fixture.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import AsyncIterator, Iterable, Sequence
from pathlib import Path
from typing import Any

from tradewatch.application.ports.market_data import (
    InstrumentSnapshot,
    PriceSnapshot,
    ProviderCapabilities,
)
from tradewatch.domain.enums import AssetClass, BarInterval
from tradewatch.domain.events import BarEvent, EventType, MarketEvent, QuoteEvent, TradeEvent
from tradewatch.infrastructure.providers.errors import (
    ProviderConnectionError,
    ProviderDataError,
    ProviderRequestError,
)
from tradewatch.observability.logging import get_logger

__all__ = ["ReplayProvider", "capture_stream", "load_events", "write_events"]

log = get_logger(__name__)

PROVIDER_NAME = "replay"

_CAPABILITIES = ProviderCapabilities(
    streaming_trades=True,
    streaming_quotes=True,
    historical_bars=True,
    latest_price=True,
    instrument_list=True,
    intervals=frozenset(BarInterval),
)

_EVENT_TYPES: dict[str, type[MarketEvent]] = {
    EventType.TRADE.value: TradeEvent,
    EventType.QUOTE.value: QuoteEvent,
    EventType.BAR_CLOSED.value: BarEvent,
    EventType.BAR_CORRECTED.value: BarEvent,
}


def _parse_line(line: str, *, line_number: int, path: Path | None = None) -> MarketEvent:
    """Turn one JSONL line into an event, naming the line when it will not parse."""
    where = f"{path}:{line_number}" if path else f"line {line_number}"
    try:
        payload: Any = json.loads(line)
    except json.JSONDecodeError as error:
        raise ProviderDataError(
            f"{where} is not valid JSON", provider=PROVIDER_NAME, raw=line
        ) from error

    if not isinstance(payload, dict):
        raise ProviderDataError(f"{where} is not a JSON object", provider=PROVIDER_NAME, raw=line)

    event_type = payload.get("event_type")
    model = _EVENT_TYPES.get(str(event_type))
    if model is None:
        raise ProviderDataError(
            f"{where} has unknown event_type {event_type!r}",
            provider=PROVIDER_NAME,
            raw=line,
        )

    try:
        return model.model_validate(payload)
    except Exception as error:  # pydantic ValidationError, and anything it wraps
        raise ProviderDataError(
            f"{where} does not match {model.__name__}: {error}",
            provider=PROVIDER_NAME,
            raw=line,
        ) from error


def load_events(path: Path | str) -> list[MarketEvent]:
    """Read a JSONL recording.

    Blank lines are skipped; a malformed line raises rather than being skipped, because a
    fixture that silently loses events would make a test pass for the wrong reason.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise ProviderRequestError(f"no recording at {file_path}", provider=PROVIDER_NAME)

    events: list[MarketEvent] = []
    with file_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            events.append(_parse_line(text, line_number=line_number, path=file_path))
    return events


def write_events(path: Path | str, events: Iterable[MarketEvent]) -> int:
    """Write events as JSONL. Returns how many were written."""
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with file_path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(event.model_dump_json())
            handle.write("\n")
            count += 1
    return count


async def capture_stream(
    stream: AsyncIterator[MarketEvent],
    path: Path | str,
    *,
    limit: int = 1000,
) -> int:
    """Record a live stream to a file, so a real session becomes a permanent fixture.

    This is how a recording of the real provider gets made once, by a human with
    credentials, and then replayed forever in CI by everyone who has none.
    """
    captured: list[MarketEvent] = []
    async for event in stream:
        captured.append(event)
        if len(captured) >= limit:
            break
    written = write_events(path, captured)
    log.info("replay.captured", path=str(path), events=written)
    return written


class ReplayProvider:
    """Replays a recording as if it were a live provider.

    Implements :class:`~tradewatch.application.ports.market_data.MarketDataProvider`.
    """

    def __init__(
        self,
        events: Sequence[MarketEvent],
        *,
        loop: bool = False,
        interval_seconds: float = 0.0,
        drop_after: int | None = None,
    ) -> None:
        """
        Args:
            events: What to replay, in order.
            loop: Start again at the end instead of stopping. Useful for a long-running
                demo; off by default, because a test wants the stream to end.
            interval_seconds: Delay between events. 0 replays as fast as possible.
            drop_after: Raise a connection error after this many events, to exercise
                reconnect handling against recorded data.
        """
        self._events = list(events)
        self._loop = loop
        self._interval_seconds = interval_seconds
        self._drop_after = drop_after
        self._closed = False

    @classmethod
    def from_file(cls, path: Path | str, **kwargs: Any) -> ReplayProvider:
        return cls(load_events(path), **kwargs)

    @property
    def name(self) -> str:
        return PROVIDER_NAME

    @property
    def capabilities(self) -> ProviderCapabilities:
        return _CAPABILITIES

    @property
    def events(self) -> Sequence[MarketEvent]:
        return tuple(self._events)

    async def list_instruments(self) -> Sequence[InstrumentSnapshot]:
        """Every symbol that appears in the recording."""
        symbols = sorted({event.symbol for event in self._events})
        return [
            InstrumentSnapshot(
                symbol=symbol,
                name=None,
                asset_class=AssetClass.EQUITY,
                exchange="XTST",
                provider_metadata={"replay": True},
            )
            for symbol in symbols
        ]

    async def fetch_bars(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[BarEvent]:
        """Bars from the recording that fall in ``[start, end)``."""
        self._require_open()
        wanted = symbol.strip().upper()
        return [
            event
            for event in self._events
            if isinstance(event, BarEvent)
            and event.symbol == wanted
            and event.interval is interval
            and start <= event.bar_start < end
        ]

    async def fetch_latest_price(self, symbol: str) -> PriceSnapshot | None:
        """The last trade in the recording for ``symbol``, or None if there is none."""
        self._require_open()
        wanted = symbol.strip().upper()
        trades = [
            event
            for event in self._events
            if isinstance(event, TradeEvent) and event.symbol == wanted
        ]
        if not trades:
            return None
        last = max(trades, key=lambda event: event.occurred_at)
        return PriceSnapshot(
            symbol=wanted,
            price=last.price,
            size=last.size,
            as_of=last.occurred_at,
            source=PROVIDER_NAME,
        )

    async def stream_trades(self, symbols: Sequence[str]) -> AsyncIterator[TradeEvent]:
        async for event in self._replay(symbols, TradeEvent):
            yield event

    async def stream_quotes(self, symbols: Sequence[str]) -> AsyncIterator[QuoteEvent]:
        async for event in self._replay(symbols, QuoteEvent):
            yield event

    async def aclose(self) -> None:
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise ProviderConnectionError("provider is closed", provider=PROVIDER_NAME)

    async def _replay[E: MarketEvent](
        self, symbols: Sequence[str], kind: type[E]
    ) -> AsyncIterator[E]:
        self._require_open()
        wanted = {symbol.strip().upper() for symbol in symbols}
        matching = [
            event
            for event in self._events
            if isinstance(event, kind) and (not wanted or event.symbol in wanted)
        ]

        emitted = 0
        while True:
            for event in matching:
                if self._drop_after is not None and emitted >= self._drop_after:
                    raise ProviderConnectionError(
                        f"replay dropped after {emitted} events", provider=PROVIDER_NAME
                    )
                emitted += 1
                yield event
                if self._interval_seconds:
                    await asyncio.sleep(self._interval_seconds)
            if not self._loop:
                return
            if not matching:
                # Looping over nothing would spin the event loop forever.
                return
