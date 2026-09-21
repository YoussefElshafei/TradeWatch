"""Processing failures.

Two categories, because the processor does two genuinely different things about them.

:class:`StorageError` means the *database* is the problem — unreachable, a transaction that
would not commit, a deadlock. The batch is intact and the right response is to keep it and
try again. It is retryable, and if retries run out it propagates so the supervisor sees a
failed processor rather than one quietly making no progress.

:class:`UnprocessableEventError` means the *event* is the problem — a type nothing knows how
to store, a value the schema refuses. Retrying replays the same rejection forever, so the
event is counted, logged with enough context to find it again, and set aside. From Phase 2
it also goes to the dead-letter topic.

The distinction matters because confusing them produces the two worst outcomes available: a
poison message that stops the pipeline forever, or a database outage that silently discards
an hour of market data.
"""

from __future__ import annotations

__all__ = ["ProcessingError", "StorageError", "UnprocessableEventError"]


class ProcessingError(Exception):
    """Base for every market data processing failure."""

    retryable: bool = False

    def __init__(self, message: str, *, detail: object = None) -> None:
        super().__init__(message)
        self.message = message
        #: Small and safe to log. Never a whole event payload at error level.
        self.detail = detail


class StorageError(ProcessingError):
    """The store could not complete an operation.

    Retryable. The batch that failed is kept, not dropped: `ARCHITECTURE.md` F8 says a
    consumer pauses and retries while the data stays safe upstream, and dropping it here
    would make that promise false.
    """

    retryable = True


class UnprocessableEventError(ProcessingError):
    """One event cannot be stored, and never will be.

    Not retryable — the same event produces the same rejection. Counted and skipped, never
    silently swallowed, because one bad tick must not stop the pipeline (F3).
    """

    retryable = False

    def __init__(
        self,
        message: str,
        *,
        event_id: object = None,
        event_type: str | None = None,
        symbol: str | None = None,
        detail: object = None,
    ) -> None:
        super().__init__(message, detail=detail)
        self.event_id = event_id
        self.event_type = event_type
        self.symbol = symbol
