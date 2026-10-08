"""Building a provider from configuration.

The one place that maps `TW_PROVIDER` to a concrete adapter, and therefore the one place
that imports every adapter. Everything else takes a
:class:`~tradewatch.application.ports.market_data.MarketDataProvider` as an argument and
does not know or care which one it got — that is ADR-0004 in practice rather than in prose.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from decimal import Decimal

from tradewatch.application.ports.market_data import MarketDataProvider
from tradewatch.config.settings import MarketSettings, ProviderName, ProviderSettings
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.infrastructure.providers.replay import ReplayProvider
from tradewatch.infrastructure.providers.resilience import RetryPolicy
from tradewatch.infrastructure.providers.synthetic import SyntheticProvider
from tradewatch.observability.logging import bind_correlation, get_logger

__all__ = ["build_provider", "retry_policy_from_settings"]

log = get_logger(__name__)


def retry_policy_from_settings(settings: ProviderSettings) -> RetryPolicy:
    """The retry policy every adapter and every stream shares."""
    return RetryPolicy(
        max_attempts=settings.provider_max_reconnect_attempts,
        base_delay=0.5,
        max_delay=settings.provider_reconnect_max_backoff_s,
        jitter=True,
    )


def build_provider(
    provider_settings: ProviderSettings,
    market_settings: MarketSettings,
    *,
    producer: str = "ingestion-service",
    clock: object | None = None,
    symbols: Sequence[str] | None = None,
) -> MarketDataProvider:
    """Construct the configured provider.

    Raises:
        ValueError: If the configured provider has no adapter here. Unreachable through
            configuration — `ProviderName` is a closed enum and settings validate it — but
            an explicit failure beats falling through to None if a member is ever added
            without its branch.
    """
    watchlist = list(symbols or market_settings.symbols)
    factory = EventFactory(
        source=provider_settings.provider.value,
        producer=producer,
        clock=clock,  # type: ignore[arg-type]
        correlation_id=new_id(),
    )
    # Every log line this process writes from here on names the run's correlation id — the
    # same id every event it publishes carries, and every derived event inherits (§7.1).
    bind_correlation([factory.correlation_id])

    match provider_settings.provider:
        case ProviderName.SYNTHETIC:
            log.info(
                "provider.selected",
                provider="synthetic",
                seed=provider_settings.synthetic_seed,
                symbols=len(watchlist),
            )
            return SyntheticProvider(
                watchlist,
                seed=provider_settings.synthetic_seed,
                start_price=Decimal("100"),
                factory=factory,
            )

        case ProviderName.REPLAY:
            if provider_settings.replay_path is None:
                raise ValueError("TW_PROVIDER=replay requires TW_REPLAY_PATH")
            log.info(
                "provider.selected", provider="replay", path=str(provider_settings.replay_path)
            )
            return ReplayProvider.from_file(provider_settings.replay_path)

        case ProviderName.ALPACA:
            # Imported here rather than at module scope so that the default configuration
            # does not pay for httpx and websockets import time, and so a broken optional
            # dependency cannot stop the synthetic provider from starting.
            from tradewatch.infrastructure.providers.alpaca import AlpacaProvider, AlpacaSettings

            if not (provider_settings.provider_api_key and provider_settings.provider_api_secret):
                raise ValueError(
                    "TW_PROVIDER=alpaca requires TW_PROVIDER_API_KEY and TW_PROVIDER_API_SECRET"
                )
            log.info("provider.selected", provider="alpaca", symbols=len(watchlist))
            alpaca_settings = AlpacaSettings(
                api_key=provider_settings.provider_api_key.get_secret_value(),
                api_secret=provider_settings.provider_api_secret.get_secret_value(),
                timeout_seconds=provider_settings.provider_timeout_s,
                rate_limit_per_minute=provider_settings.provider_rate_limit_per_min,
                **(
                    {"data_base_url": provider_settings.provider_base_url}
                    if provider_settings.provider_base_url
                    else {}
                ),
            )
            return AlpacaProvider(
                alpaca_settings,
                factory=factory,
                retry_policy=retry_policy_from_settings(provider_settings),
            )

    raise ValueError(f"no adapter for provider {provider_settings.provider!r}")


def utcnow() -> dt.datetime:
    """The default clock. A function rather than a lambda so it can be patched by name."""
    return dt.datetime.now(dt.UTC)
