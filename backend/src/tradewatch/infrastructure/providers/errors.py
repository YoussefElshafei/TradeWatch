"""Provider failures, classified by what the caller should do about them.

Every adapter raises one of these and nothing else. A vendor SDK's own exception type
crossing this boundary would defeat the abstraction as thoroughly as its response format
would — the retry logic would have to know which vendor it was retrying.

The classification that matters is **retryable or not**, because that is the only question
the caller actually has:

* :class:`ProviderConnectionError`, :class:`ProviderTimeoutError`,
  :class:`ProviderRateLimitError`, :class:`ProviderUnavailableError` — the request might
  work later. Back off and try again.
* :class:`ProviderAuthError`, :class:`ProviderRequestError` — retrying will fail the same
  way forever. A bad API key does not become good on the fourth attempt, and hammering the
  endpoint to discover that wastes a rate-limit budget that legitimate requests need.
* :class:`ProviderDataError` — the connection is fine, this one message is not. Count it,
  record it, skip it, keep going (failure F3). Retrying a malformed message just produces
  the same malformed message.

None of these are ever caught and ignored. `ProviderDataError` is the closest thing to a
"skip it" error and it still increments a counter, logs, and — from Phase 2 — goes to the
dead-letter topic.
"""

from __future__ import annotations

__all__ = [
    "ProviderAuthError",
    "ProviderConnectionError",
    "ProviderDataError",
    "ProviderError",
    "ProviderRateLimitError",
    "ProviderRequestError",
    "ProviderTimeoutError",
    "ProviderUnavailableError",
]


class ProviderError(Exception):
    """Base for every market data provider failure."""

    #: Whether trying again could plausibly succeed.
    retryable: bool = False

    def __init__(self, message: str, *, provider: str = "", detail: object = None) -> None:
        super().__init__(message)
        self.message = message
        self.provider = provider
        #: Something small and safe to log. Never a whole response body: that is how a
        #: credential in an echoed request header ends up in a log file.
        self.detail = detail

    def __str__(self) -> str:
        prefix = f"[{self.provider}] " if self.provider else ""
        return f"{prefix}{self.message}"


class ProviderConnectionError(ProviderError):
    """The connection failed or dropped — DNS, refused, reset, closed mid-stream.

    The expected failure for a long-lived WebSocket (failure F1). Reconnect with backoff.
    """

    retryable = True


class ProviderTimeoutError(ProviderError):
    """A request or a read took longer than its deadline.

    Distinct from a connection error because it usually means the provider is degraded
    rather than gone, which is worth seeing separately in the metrics.
    """

    retryable = True


class ProviderRateLimitError(ProviderError):
    """The provider refused because we asked too often (failure F2).

    ``retry_after`` carries the provider's own instruction when it gives one. Honouring it
    matters: backing off less than asked is how a temporary limit becomes a ban.
    """

    retryable = True

    def __init__(
        self,
        message: str,
        *,
        provider: str = "",
        detail: object = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message, provider=provider, detail=detail)
        self.retry_after = retry_after


class ProviderUnavailableError(ProviderError):
    """The provider is up but broken — 5xx, maintenance, a degraded feed."""

    retryable = True


class ProviderAuthError(ProviderError):
    """Credentials were rejected: missing, wrong, expired, or lacking entitlement.

    Deliberately not retryable. A bad key does not become good on the fourth attempt, and
    retrying burns a rate-limit budget to learn nothing.
    """

    retryable = False


class ProviderRequestError(ProviderError):
    """We asked for something the provider will never give — unknown symbol, bad range.

    Not retryable, and not the provider's fault. The caller has a bug or a stale symbol
    list.
    """

    retryable = False


class ProviderDataError(ProviderError):
    """A response arrived and could not be understood (failure F3).

    Malformed JSON, a missing required field, a price that is not a number, a timestamp in
    a format the provider does not document. The connection is healthy; this one message is
    not.

    Never retried — the same bytes would parse the same way — and never silently dropped
    either. The ingestion service counts it, logs it with enough context to find the
    message again, and continues, because one bad tick must not stop the feed.
    """

    retryable = False

    def __init__(
        self,
        message: str,
        *,
        provider: str = "",
        detail: object = None,
        raw: str | None = None,
    ) -> None:
        super().__init__(message, provider=provider, detail=detail)
        #: A truncated copy of what could not be parsed. Truncated because a malformed
        #: frame can be enormous, and the first 500 characters are what identifies it.
        self.raw = raw[:500] if raw else None
