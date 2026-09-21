"""The ingestion worker: ``uv run tradewatch-ingest``.

Wires the pieces together and runs them. Everything interesting lives elsewhere — the
provider behind its port, the normalization in the adapter, the retry policy in
`resilience`, the counters in `IngestionService` — so this module is deliberately thin. If
it grows logic, that logic is in the wrong place.

What it does own is the **supervision shape**: a trade stream and a quote stream running
concurrently, a staleness watchdog beside them, and a shutdown that actually stops.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
from collections.abc import AsyncIterator, Sequence

from tradewatch.application.ingestion import EventDeduplicator, IngestionService
from tradewatch.application.ports.event_bus import EventPublisher
from tradewatch.application.ports.market_data import MarketDataProvider
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.events import MarketEvent
from tradewatch.infrastructure.events.memory import InMemoryEventBus
from tradewatch.infrastructure.providers.factory import build_provider, retry_policy_from_settings
from tradewatch.infrastructure.providers.resilience import StalenessMonitor, reconnecting
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

#: How often the watchdog checks for symbols that have gone quiet.
_STALENESS_CHECK_SECONDS = 15.0


def build_service(
    settings: Settings,
    publisher: EventPublisher,
    *,
    provider: MarketDataProvider | None = None,
) -> tuple[IngestionService, MarketDataProvider]:
    """Assemble the service. Injectable throughout, so tests never touch configuration."""
    resolved = provider or build_provider(settings.provider, settings.market)
    deduplicator = EventDeduplicator(
        max_entries=settings.provider.dedupe_max_entries,
        ttl_seconds=settings.provider.dedupe_window_seconds,
    )
    return IngestionService(resolved, publisher, deduplicator=deduplicator), resolved


async def run_ingestion(
    settings: Settings,
    publisher: EventPublisher,
    *,
    provider: MarketDataProvider | None = None,
    service: IngestionService | None = None,
    max_events: int | None = None,
    stop: asyncio.Event | None = None,
) -> IngestionService:
    """Run until cancelled, or until ``max_events`` have been published on each stream.

    Trades and quotes run as separate tasks. If either fails permanently the other is
    cancelled too: a half-running ingester that publishes quotes with no trades looks
    healthy and is not.

    ``service`` lets a caller supply an already-built :class:`IngestionService` instead of
    having one made here. The Phase 2 producer does that because it backfills history before
    it starts streaming, and a second service would mean a second deduplicator and a second
    set of counters — so the backfilled bars would not be visible in the stats of the run
    that published them, and a bar fetched twice would not be recognised as a duplicate.
    """
    if service is None:
        service, resolved = build_service(settings, publisher, provider=provider)
    elif provider is None:
        raise ValueError("passing `service` also requires its `provider`")
    else:
        resolved = provider
    symbols = list(settings.market.symbols)
    policy = retry_policy_from_settings(settings.provider)
    capabilities = resolved.capabilities

    staleness = StalenessMonitor(
        threshold_seconds=settings.provider.provider_staleness_threshold_s,
        monotonic=asyncio.get_running_loop().time,
    )

    def watch(event: MarketEvent) -> None:
        staleness.record(event.symbol)

    def wrap(kind: str) -> AsyncIterator[MarketEvent]:
        """A stream that reopens itself after a drop (failure F1)."""

        def open_stream() -> AsyncIterator[MarketEvent]:
            if kind == "trades":
                return resolved.stream_trades(symbols)
            return resolved.stream_quotes(symbols)

        def on_reconnect(attempt: int) -> None:
            service.record_reconnect(attempt)
            # The gap belongs to the outage, not to the symbols. Without this the first
            # check after reconnecting would report every symbol as stale.
            staleness.reset()

        return reconnecting(
            open_stream,
            policy=policy,
            description=f"{resolved.name}:{kind}",
            on_reconnect=on_reconnect,
        )

    tasks: list[asyncio.Task[object]] = []
    if capabilities.streaming_trades:
        tasks.append(
            asyncio.create_task(
                service.run_stream(
                    symbols,
                    kind="trades",
                    stream=wrap("trades"),
                    max_events=max_events,
                    on_event=watch,
                ),
                name="ingest-trades",
            )
        )
    if capabilities.streaming_quotes:
        tasks.append(
            asyncio.create_task(
                service.run_stream(
                    symbols,
                    kind="quotes",
                    stream=wrap("quotes"),
                    max_events=max_events,
                    on_event=watch,
                ),
                name="ingest-quotes",
            )
        )

    if not tasks:
        raise RuntimeError(f"{resolved.name} cannot stream anything")

    watchdog = asyncio.create_task(
        _watch_staleness(service, staleness, symbols), name="ingest-staleness"
    )
    stopper = asyncio.create_task(stop.wait(), name="ingest-stop") if stop else None

    waiting: set[asyncio.Task[object]] = {*tasks, watchdog}
    if stopper is not None:
        waiting.add(stopper)

    try:
        done, pending = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        # Surface a real failure rather than exiting quietly with a zero count.
        for task in done:
            if task is not stopper and not task.cancelled():
                task.result()
    finally:
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog
        await resolved.aclose()
        await publisher.flush()

    log.info("ingestion.complete", provider=resolved.name, **service.stats.as_dict())
    return service


async def _watch_staleness(
    service: IngestionService,
    monitor: StalenessMonitor,
    symbols: Sequence[str],
) -> None:
    """Report symbols that have gone quiet.

    Passing the full watchlist matters: a symbol that was never subscribed correctly sends
    nothing at all, and checking only symbols that have been seen would never notice.
    """
    while True:
        await asyncio.sleep(_STALENESS_CHECK_SECONDS)
        service.record_stale(monitor.stale_symbols(symbols))


def main() -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)
    log.info(
        "ingestion.starting",
        provider=settings.provider.provider.value,
        symbols=list(settings.market.symbols),
    )

    # Phase 2 replaces this with the Kafka publisher. Nothing else in this module changes,
    # which is the point of the port (ADR-0001).
    bus = InMemoryEventBus()

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signal_name in ("SIGINT", "SIGTERM"):
            handler = getattr(signal, signal_name, None)
            if handler is None:
                continue
            with contextlib.suppress(NotImplementedError):
                # Windows' proactor loop does not implement signal handlers; KeyboardInterrupt
                # still works there.
                loop.add_signal_handler(handler, stop.set)
        await run_ingestion(settings, bus, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("ingestion.interrupted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
