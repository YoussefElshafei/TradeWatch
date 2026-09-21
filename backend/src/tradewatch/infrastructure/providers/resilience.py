"""Retry, backoff, rate limiting, reconnection, and staleness detection.

One implementation each, shared by every adapter. The alternative — each adapter growing its
own reconnect loop — produces four subtly different backoff curves, three of which have a
bug nobody finds until the provider has an outage.

Covers `ARCHITECTURE.md` failures F1 (connection drops), F2 (rate limits) and part of F3
(malformed data does not kill the stream).

**Full jitter, not fixed backoff.** ``sleep = random(0, min(cap, base * 2**attempt))``. Fixed
backoff makes every client retry in lockstep, so a provider recovering from an outage is
immediately hit by a synchronised thundering herd from everyone who was waiting. Randomising
the whole interval spreads them out; it is the variant AWS's architecture blog measured as
best, and the reason is that it minimises both server contention and client wait.

**Time is injected.** `sleep` and `monotonic` are parameters, so tests drive backoff and
rate limiting deterministically and instantly instead of actually waiting.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from tradewatch.infrastructure.providers.errors import (
    ProviderConnectionError,
    ProviderError,
    ProviderRateLimitError,
    ProviderTimeoutError,
)
from tradewatch.observability.logging import get_logger

__all__ = [
    "RetryPolicy",
    "StalenessMonitor",
    "TokenBucket",
    "call_with_retry",
    "reconnecting",
]

log = get_logger(__name__)

Sleeper = Callable[[float], Awaitable[None]]
Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How hard to try before giving up.

    Attributes:
        max_attempts: Total attempts including the first. 0 means retry forever, which is
            what a long-lived market data stream wants — giving up on the feed at 04:00
            because the provider blipped is not an improvement over reconnecting at 04:01.
        base_delay: First backoff interval, in seconds.
        max_delay: Ceiling. Without one, exponential growth reaches hours.
        jitter: Randomise the delay across ``[0, computed]``. Off only in tests that assert
            on exact durations.
        respect_retry_after: Honour a provider's own `Retry-After`. Backing off less than
            asked is how a temporary rate limit becomes a ban.
    """

    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 30.0
    jitter: bool = True
    respect_retry_after: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 0:
            raise ValueError("max_attempts must not be negative")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must not be below base_delay")

    @property
    def unlimited(self) -> bool:
        return self.max_attempts == 0

    def delay_for(self, attempt: int, *, retry_after: float | None = None) -> float:
        """Seconds to wait before attempt ``attempt`` (1-based).

        A provider's own instruction wins over our curve: it knows when its limit resets
        and we are guessing.
        """
        if retry_after is not None and self.respect_retry_after:
            return min(retry_after, self.max_delay)
        exponential = self.base_delay * (2 ** max(0, attempt - 1))
        capped = min(exponential, self.max_delay)
        return random.uniform(0, capped) if self.jitter else capped  # noqa: S311

    def should_retry(self, error: BaseException, attempt: int) -> bool:
        """Whether to try again after ``error`` on attempt ``attempt``."""
        if not isinstance(error, ProviderError) or not error.retryable:
            return False
        return self.unlimited or attempt < self.max_attempts


async def call_with_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    description: str,
    sleep: Sleeper | None = None,
) -> T:
    """Run ``operation``, retrying retryable provider failures with backoff.

    Non-retryable errors propagate immediately and unchanged: a bad API key does not become
    good on the fourth attempt, and burning the rate-limit budget to prove it helps nobody.

    Raises:
        ProviderError: The last error, once attempts are exhausted.
    """
    sleeper = sleep or asyncio.sleep
    attempt = 0

    while True:
        attempt += 1
        try:
            return await operation()
        except ProviderError as error:
            if not policy.should_retry(error, attempt):
                if error.retryable:
                    log.error(
                        "provider.retries_exhausted",
                        operation=description,
                        attempts=attempt,
                        error=str(error),
                        error_type=type(error).__name__,
                    )
                else:
                    # Not a failure of the retry loop — the caller needs to see it now.
                    log.error(
                        "provider.permanent_failure",
                        operation=description,
                        error=str(error),
                        error_type=type(error).__name__,
                    )
                raise

            retry_after = getattr(error, "retry_after", None)
            delay = policy.delay_for(attempt, retry_after=retry_after)
            log.warning(
                "provider.retrying",
                operation=description,
                attempt=attempt,
                delay_seconds=round(delay, 3),
                error=str(error),
                error_type=type(error).__name__,
            )
            await sleeper(delay)


async def reconnecting[T](
    open_stream: Callable[[], AsyncIterator[T]],
    *,
    policy: RetryPolicy,
    description: str,
    sleep: Sleeper | None = None,
    on_reconnect: Callable[[int], None] | None = None,
) -> AsyncIterator[T]:
    """Keep a stream alive across disconnections (failure F1).

    Calls ``open_stream`` to get an iterator, yields from it, and reopens with backoff when
    it fails or ends. The attempt counter resets after any successful item, so an hour of
    healthy streaming followed by a drop starts backing off from the beginning rather than
    from wherever the last outage left it — otherwise a service up for a week would wait the
    maximum delay on its first blip.

    Cancellation passes straight through: `asyncio.CancelledError` is not a provider error
    and must never be retried, or shutdown hangs.
    """
    sleeper = sleep or asyncio.sleep
    attempt = 0

    while True:
        try:
            stream = open_stream()
            async for item in stream:
                if attempt:
                    log.info(
                        "provider.stream_recovered",
                        stream=description,
                        after_attempts=attempt,
                    )
                    attempt = 0
                yield item
            # A stream that ends without an error still needs reopening: for a WebSocket
            # feed, "the iterator finished" means the socket closed.
            raise ProviderConnectionError(f"{description} ended unexpectedly", provider=description)
        except asyncio.CancelledError:
            raise
        except ProviderError as error:
            attempt += 1
            if not policy.should_retry(error, attempt):
                log.error(
                    "provider.stream_failed",
                    stream=description,
                    attempts=attempt,
                    error=str(error),
                    error_type=type(error).__name__,
                )
                raise

            delay = policy.delay_for(attempt, retry_after=getattr(error, "retry_after", None))
            log.warning(
                "provider.stream_reconnecting",
                stream=description,
                attempt=attempt,
                delay_seconds=round(delay, 3),
                error=str(error),
                error_type=type(error).__name__,
            )
            if on_reconnect is not None:
                on_reconnect(attempt)
            await sleeper(delay)


@dataclass(slots=True)
class TokenBucket:
    """Client-side rate limiting (failure F2).

    Refuses to exceed the provider's documented ceiling rather than discovering it by
    getting a 429 — which costs a round trip, a backoff, and some goodwill.

    Tokens refill continuously, so a burst is allowed up to the bucket's capacity and the
    sustained rate is still the limit. That matches how providers actually measure.
    """

    rate_per_minute: int
    capacity: int | None = None
    #: `time.monotonic` rather than the loop's clock: this must be constructible outside a
    #: running event loop, which `asyncio.get_event_loop()` is not.
    monotonic: Clock = time.monotonic
    _tokens: float = field(init=False, default=0.0)
    _updated_at: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        if self.rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be positive")
        if self.capacity is None:
            self.capacity = self.rate_per_minute
        self._tokens = float(self.capacity)
        self._updated_at = self.monotonic()

    @property
    def available(self) -> float:
        """Tokens available now.

        Refills before answering. Without that it reports the count as of the last
        acquire, which is stale by exactly the interval a caller is usually asking about.
        """
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        now = self.monotonic()
        elapsed = max(0.0, now - self._updated_at)
        self._updated_at = now
        assert self.capacity is not None
        self._tokens = min(float(self.capacity), self._tokens + elapsed * self.rate_per_minute / 60)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Take tokens if available. Returns False rather than waiting."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def delay_until_available(self, tokens: float = 1.0) -> float:
        """Seconds until ``tokens`` would be available. 0 when they already are."""
        self._refill()
        if self._tokens >= tokens:
            return 0.0
        return (tokens - self._tokens) * 60 / self.rate_per_minute

    async def acquire(self, tokens: float = 1.0, *, sleep: Sleeper | None = None) -> None:
        """Wait until ``tokens`` are available, then take them."""
        sleeper = sleep or asyncio.sleep
        while True:
            delay = self.delay_until_available(tokens)
            if delay <= 0:
                self._tokens -= tokens
                return
            await sleeper(delay)


@dataclass(slots=True)
class StalenessMonitor:
    """Notices when a feed goes quiet (failure F1).

    A dropped connection usually announces itself. The dangerous failure is the one that
    does not: the socket stays open, the provider stops sending, and without this the
    service looks perfectly healthy while the data silently stops.

    "Quiet" is not the same as "broken" — markets close — so this reports the fact and
    leaves the judgement to a caller that knows the session calendar.
    """

    threshold_seconds: float
    monotonic: Clock = time.monotonic
    _last_seen: dict[str, float] = field(default_factory=dict)
    _started_at: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        if self.threshold_seconds <= 0:
            raise ValueError("threshold_seconds must be positive")
        self._started_at = self.monotonic()

    def record(self, symbol: str) -> None:
        """Note that a message arrived for ``symbol``."""
        self._last_seen[symbol] = self.monotonic()

    def seconds_since(self, symbol: str) -> float:
        """Time since the last message, measured from startup if there has never been one."""
        return self.monotonic() - self._last_seen.get(symbol, self._started_at)

    def stale_symbols(self, symbols: Sequence[str] | None = None) -> list[str]:
        """Symbols quiet for longer than the threshold.

        Pass the expected universe to include symbols that have *never* sent anything —
        silence from a symbol that was never subscribed correctly is exactly the failure
        worth catching, and it is invisible if only seen symbols are checked.
        """
        universe = list(symbols) if symbols is not None else list(self._last_seen)
        return sorted(
            symbol for symbol in universe if self.seconds_since(symbol) > self.threshold_seconds
        )

    def reset(self) -> None:
        """Forget everything. Called after a reconnect, so the gap is not double-counted."""
        self._last_seen.clear()
        self._started_at = self.monotonic()


def rate_limit_error(provider: str, retry_after: float | None = None) -> ProviderRateLimitError:
    """Build a rate-limit error with the provider's own retry hint attached."""
    return ProviderRateLimitError("rate limit exceeded", provider=provider, retry_after=retry_after)


def timeout_error(provider: str, operation: str, seconds: float) -> ProviderTimeoutError:
    return ProviderTimeoutError(
        f"{operation} exceeded {seconds:g}s", provider=provider, detail={"timeout": seconds}
    )
