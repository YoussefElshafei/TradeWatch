"""Market data provider adapters.

Only the ingestion service imports from here (ADR-0004). Everything else in TradeWatch
depends on :mod:`tradewatch.application.ports.market_data` and does not know which vendor,
or whether there is a vendor at all.
"""

from tradewatch.infrastructure.providers.errors import (
    ProviderAuthError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderError,
    ProviderRateLimitError,
    ProviderRequestError,
    ProviderTimeoutError,
    ProviderUnavailableError,
)
from tradewatch.infrastructure.providers.normalization import (
    EventFactory,
    normalize_symbol,
    parse_decimal,
    parse_timestamp,
)
from tradewatch.infrastructure.providers.replay import ReplayProvider
from tradewatch.infrastructure.providers.resilience import (
    RetryPolicy,
    StalenessMonitor,
    TokenBucket,
    call_with_retry,
    reconnecting,
)
from tradewatch.infrastructure.providers.synthetic import SyntheticProvider, SyntheticScenario

__all__ = [
    "EventFactory",
    "ProviderAuthError",
    "ProviderConnectionError",
    "ProviderDataError",
    "ProviderError",
    "ProviderRateLimitError",
    "ProviderRequestError",
    "ProviderTimeoutError",
    "ProviderUnavailableError",
    "ReplayProvider",
    "RetryPolicy",
    "StalenessMonitor",
    "SyntheticProvider",
    "SyntheticScenario",
    "TokenBucket",
    "call_with_retry",
    "normalize_symbol",
    "parse_decimal",
    "parse_timestamp",
    "reconnecting",
]
