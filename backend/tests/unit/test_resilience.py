"""Retry, backoff, rate limiting, reconnection and staleness.

Nothing here sleeps. Time is injected everywhere, so a test for "waits 30 seconds then
reconnects" finishes in microseconds and asserts on the exact delay rather than on a
wall-clock approximation.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable

import pytest

from tradewatch.infrastructure.providers.errors import (
    ProviderAuthError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderRateLimitError,
    ProviderTimeoutError,
)
from tradewatch.infrastructure.providers.resilience import (
    RetryPolicy,
    StalenessMonitor,
    TokenBucket,
    call_with_retry,
    reconnecting,
)

pytestmark = pytest.mark.unit


class FakeClock:
    """A clock the test moves by hand, and a `sleep` that moves it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


class TestRetryPolicy:
    def test_backoff_grows_exponentially(self) -> None:
        policy = RetryPolicy(base_delay=1.0, max_delay=100.0, jitter=False)

        assert [policy.delay_for(n) for n in (1, 2, 3, 4)] == [1.0, 2.0, 4.0, 8.0]

    def test_backoff_is_capped(self) -> None:
        """Without a cap, exponential growth reaches hours."""
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0, jitter=False)

        assert policy.delay_for(10) == 5.0

    def test_jitter_keeps_the_delay_within_the_cap(self) -> None:
        """Full jitter spreads a recovering provider's thundering herd."""
        policy = RetryPolicy(base_delay=1.0, max_delay=8.0, jitter=True)

        delays = [policy.delay_for(4) for _ in range(200)]

        assert all(0 <= delay <= 8.0 for delay in delays)
        assert len(set(delays)) > 1, "jitter produced identical delays"

    def test_a_provider_retry_after_wins(self) -> None:
        """It knows when its limit resets; our curve is a guess."""
        policy = RetryPolicy(base_delay=1.0, max_delay=100.0, jitter=False)

        assert policy.delay_for(1, retry_after=42.0) == 42.0

    def test_retry_after_is_still_capped(self) -> None:
        policy = RetryPolicy(base_delay=1.0, max_delay=30.0, jitter=False)

        assert policy.delay_for(1, retry_after=3600.0) == 30.0

    def test_retryable_errors_are_retried(self) -> None:
        policy = RetryPolicy(max_attempts=3)

        assert policy.should_retry(ProviderConnectionError("dropped"), attempt=1)
        assert policy.should_retry(ProviderTimeoutError("slow"), attempt=2)

    @pytest.mark.parametrize(
        "error",
        [ProviderAuthError("bad key"), ProviderDataError("garbage"), ValueError("unrelated")],
    )
    def test_permanent_failures_are_not_retried(self, error: Exception) -> None:
        """A bad key does not become good on the fourth attempt."""
        assert not RetryPolicy(max_attempts=5).should_retry(error, attempt=1)

    def test_attempts_run_out(self) -> None:
        policy = RetryPolicy(max_attempts=3)

        assert not policy.should_retry(ProviderConnectionError("dropped"), attempt=3)

    def test_zero_attempts_means_forever(self) -> None:
        """What a market data stream wants: reconnect at 04:01 rather than give up at 04:00."""
        policy = RetryPolicy(max_attempts=0)

        assert policy.should_retry(ProviderConnectionError("dropped"), attempt=10_000)

    @pytest.mark.parametrize(
        "kwargs",
        [{"max_attempts": -1}, {"base_delay": 0}, {"base_delay": 10, "max_delay": 1}],
    )
    def test_rejects_an_incoherent_policy(self, kwargs: dict[str, float]) -> None:
        with pytest.raises(ValueError, match="must"):
            RetryPolicy(**kwargs)  # type: ignore[arg-type]


class TestCallWithRetry:
    async def test_returns_on_first_success(self, clock: FakeClock) -> None:
        async def operation() -> str:
            return "ok"

        result = await call_with_retry(
            operation, policy=RetryPolicy(), description="test", sleep=clock.sleep
        )

        assert result == "ok"
        assert clock.slept == []

    async def test_retries_until_it_succeeds(self, clock: FakeClock) -> None:
        attempts = {"n": 0}

        async def flaky() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ProviderConnectionError("dropped")
            return "recovered"

        result = await call_with_retry(
            flaky,
            policy=RetryPolicy(max_attempts=5, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
        )

        assert result == "recovered"
        assert attempts["n"] == 3
        assert clock.slept == [1.0, 2.0]

    async def test_gives_up_and_raises_the_last_error(self, clock: FakeClock) -> None:
        async def always_fails() -> str:
            raise ProviderTimeoutError("too slow")

        with pytest.raises(ProviderTimeoutError, match="too slow"):
            await call_with_retry(
                always_fails,
                policy=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=False),
                description="test",
                sleep=clock.sleep,
            )

        assert len(clock.slept) == 2, "should sleep between attempts, not after the last"

    async def test_a_permanent_failure_is_not_retried(self, clock: FakeClock) -> None:
        attempts = {"n": 0}

        async def unauthorised() -> str:
            attempts["n"] += 1
            raise ProviderAuthError("bad key")

        with pytest.raises(ProviderAuthError):
            await call_with_retry(
                unauthorised,
                policy=RetryPolicy(max_attempts=5),
                description="test",
                sleep=clock.sleep,
            )

        assert attempts["n"] == 1
        assert clock.slept == []

    async def test_honours_a_rate_limit_retry_after(self, clock: FakeClock) -> None:
        attempts = {"n": 0}

        async def limited() -> str:
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ProviderRateLimitError("slow down", retry_after=7.0)
            return "ok"

        await call_with_retry(
            limited,
            policy=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
        )

        assert clock.slept == [7.0]

    async def test_a_non_provider_error_propagates_immediately(self, clock: FakeClock) -> None:
        """A bug in our own code must not be retried into a storm."""

        async def broken() -> str:
            raise ZeroDivisionError("bug")

        with pytest.raises(ZeroDivisionError):
            await call_with_retry(
                broken, policy=RetryPolicy(), description="test", sleep=clock.sleep
            )


class TestReconnecting:
    @staticmethod
    def _script(
        *batches: list[str | Exception],
    ) -> Callable[[], AsyncIterator[str]]:
        """Build a stream factory that plays one batch per connection attempt."""
        remaining = list(batches)

        def open_stream() -> AsyncIterator[str]:
            batch = remaining.pop(0) if remaining else []

            async def generate() -> AsyncIterator[str]:
                for item in batch:
                    if isinstance(item, Exception):
                        raise item
                    yield item

            return generate()

        return open_stream

    async def test_yields_through_a_healthy_stream(self, clock: FakeClock) -> None:
        received = []
        stream = reconnecting(
            self._script(["a", "b", "c"], []),
            policy=RetryPolicy(max_attempts=2, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
        )

        with pytest.raises(ProviderConnectionError):
            async for item in stream:
                received.append(item)

        assert received == ["a", "b", "c"]

    async def test_reconnects_after_a_drop_and_continues(self, clock: FakeClock) -> None:
        """Failure F1: the stream survives a disconnection."""
        received = []
        stream = reconnecting(
            self._script(
                ["a", "b", ProviderConnectionError("socket closed")],
                ["c", "d"],
            ),
            policy=RetryPolicy(max_attempts=5, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
        )

        with pytest.raises(ProviderConnectionError):
            async for item in stream:
                received.append(item)

        assert received == ["a", "b", "c", "d"]
        assert clock.slept[0] == 1.0

    async def test_the_attempt_counter_resets_after_a_success(self, clock: FakeClock) -> None:
        """Otherwise a service up for a week waits the maximum delay on its first blip."""
        received = []
        stream = reconnecting(
            self._script(
                [ProviderConnectionError("drop 1")],
                [ProviderConnectionError("drop 2")],
                ["a", ProviderConnectionError("drop 3")],
                ["b"],
            ),
            policy=RetryPolicy(max_attempts=10, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
        )

        with pytest.raises(ProviderConnectionError):
            async for item in stream:
                received.append(item)

        assert received == ["a", "b"]
        # 1s, 2s for the first two drops; then back to 1s because "a" arrived in between.
        assert clock.slept[:3] == [1.0, 2.0, 1.0]

    async def test_reports_each_reconnection(self, clock: FakeClock) -> None:
        """The callback fires on every reopen, carrying the current attempt number.

        The sequence below is `[1, 1, 2]`, and the middle 1 is the interesting part: the
        first drop is attempt 1, then "a" arrives and resets the counter, so the *next*
        reopen is attempt 1 again rather than 2. That reset is what keeps a long-lived
        stream from inheriting an old outage's backoff.
        """
        attempts: list[int] = []
        stream = reconnecting(
            self._script(
                [ProviderConnectionError("drop")],
                ["a"],
            ),
            policy=RetryPolicy(max_attempts=3, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
            on_reconnect=attempts.append,
        )

        with pytest.raises(ProviderConnectionError):
            async for _ in stream:
                pass

        assert attempts == [1, 1, 2]

    async def test_a_permanent_failure_stops_reconnecting(self, clock: FakeClock) -> None:
        stream = reconnecting(
            self._script([ProviderAuthError("bad key")]),
            policy=RetryPolicy(max_attempts=5, base_delay=1.0, jitter=False),
            description="test",
            sleep=clock.sleep,
        )

        with pytest.raises(ProviderAuthError):
            async for _ in stream:
                pass

        assert clock.slept == []

    async def test_cancellation_is_not_retried(self) -> None:
        """Retrying a cancellation would hang shutdown."""

        def open_stream() -> AsyncIterator[str]:
            async def generate() -> AsyncIterator[str]:
                # Yields nothing and cancels — the shape of a task being torn down.
                for _ in range(0):
                    yield ""
                raise asyncio.CancelledError

            return generate()

        stream = reconnecting(open_stream, policy=RetryPolicy(max_attempts=0), description="test")

        with pytest.raises(asyncio.CancelledError):
            async for _ in stream:
                pass


class TestTokenBucket:
    def test_starts_full(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, monotonic=clock.monotonic)

        assert bucket.available == 60

    def test_consumes_tokens(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, monotonic=clock.monotonic)

        assert bucket.try_acquire(10) is True
        assert bucket.available == pytest.approx(50)

    def test_refuses_when_empty(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, capacity=5, monotonic=clock.monotonic)
        bucket.try_acquire(5)

        assert bucket.try_acquire(1) is False

    def test_refills_over_time(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, capacity=10, monotonic=clock.monotonic)
        bucket.try_acquire(10)

        clock.advance(5.0)  # 60/min is 1/s

        assert bucket.available == pytest.approx(5.0)

    def test_does_not_refill_past_capacity(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, capacity=10, monotonic=clock.monotonic)

        clock.advance(3600)

        assert bucket.available == 10

    def test_reports_the_wait(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, capacity=1, monotonic=clock.monotonic)
        bucket.try_acquire(1)

        assert bucket.delay_until_available(1) == pytest.approx(1.0)

    async def test_acquire_waits_then_takes(self, clock: FakeClock) -> None:
        bucket = TokenBucket(rate_per_minute=60, capacity=1, monotonic=clock.monotonic)
        await bucket.acquire(1, sleep=clock.sleep)

        await bucket.acquire(1, sleep=clock.sleep)

        assert clock.slept == [pytest.approx(1.0)]

    def test_rejects_a_zero_rate(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            TokenBucket(rate_per_minute=0)


class TestStalenessMonitor:
    def test_a_fresh_symbol_is_not_stale(self, clock: FakeClock) -> None:
        monitor = StalenessMonitor(threshold_seconds=30, monotonic=clock.monotonic)
        monitor.record("AAPL")

        assert monitor.stale_symbols() == []

    def test_a_quiet_symbol_becomes_stale(self, clock: FakeClock) -> None:
        monitor = StalenessMonitor(threshold_seconds=30, monotonic=clock.monotonic)
        monitor.record("AAPL")

        clock.advance(31)

        assert monitor.stale_symbols() == ["AAPL"]

    def test_a_symbol_that_never_sent_anything_is_stale(self, clock: FakeClock) -> None:
        """The failure that is invisible if only seen symbols are checked.

        A symbol that was never subscribed correctly sends nothing at all.
        """
        monitor = StalenessMonitor(threshold_seconds=30, monotonic=clock.monotonic)
        monitor.record("AAPL")
        clock.advance(31)

        assert monitor.stale_symbols(["AAPL", "MSFT"]) == ["AAPL", "MSFT"]

    def test_measures_per_symbol(self, clock: FakeClock) -> None:
        monitor = StalenessMonitor(threshold_seconds=30, monotonic=clock.monotonic)
        monitor.record("AAPL")
        clock.advance(20)
        monitor.record("MSFT")
        clock.advance(15)

        assert monitor.stale_symbols() == ["AAPL"]

    def test_reset_clears_the_gap_after_a_reconnect(self, clock: FakeClock) -> None:
        """Otherwise the first check after reconnecting reports everything as stale."""
        monitor = StalenessMonitor(threshold_seconds=30, monotonic=clock.monotonic)
        monitor.record("AAPL")
        clock.advance(100)

        monitor.reset()

        assert monitor.stale_symbols(["AAPL"]) == []
