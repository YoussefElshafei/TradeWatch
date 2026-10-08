"""Structured logging.

Two renderers, one pipeline: human-readable colour in a terminal, JSON everywhere a
machine reads the output. The choice is `TW_LOG_FORMAT`, so it is the environment's
decision rather than a guess about where the process is running.

Every log line carries whatever request context is bound at the time — the request id in
particular, which `ARCHITECTURE.md` §8.3 says is propagated into the Kafka
``correlation_id`` of any event a request causes. Binding it into a context variable
rather than passing it down through call signatures is what keeps that promise cheap
enough that it actually gets kept.

Standard-library logging is routed through the same pipeline, so uvicorn's and (later)
SQLAlchemy's output does not arrive in a different shape from ours.
"""

from __future__ import annotations

import logging
import logging.config
import sys
import uuid
from collections.abc import Iterable
from typing import Any

import structlog
from structlog.types import EventDict, Processor

from tradewatch.config.settings import LogFormat
from tradewatch.observability.metrics import LOG_EVENTS

#: Loggers that are noisy by default and say little we do not already log ourselves.
_NOISY_LOGGERS: dict[str, str] = {
    "uvicorn.access": "WARNING",  # we log requests ourselves, with the request id
    "watchfiles.main": "WARNING",  # the dev reloader narrates every file it looks at
}


def new_request_id() -> str:
    """A fresh request id. Also used as the initial ``correlation_id`` for an event."""
    return str(uuid.uuid4())


def bind_request_context(**values: Any) -> None:
    """Attach values to every log line emitted by this task until it is cleared."""
    structlog.contextvars.bind_contextvars(**values)


def bind_correlation(correlation_ids: Iterable[uuid.UUID]) -> None:
    """Bind the correlation id(s) of the events being handled, or unbind when there are none.

    One id is the normal case — every event from one ingestion run shares it (§7.1). More
    than one is bound comma-separated, capped, so a batch spanning a producer restart still
    names both runs without an unbounded log field.
    """
    distinct = sorted({str(value) for value in correlation_ids})
    if not distinct:
        structlog.contextvars.unbind_contextvars("correlation_id")
        return
    structlog.contextvars.bind_contextvars(correlation_id=",".join(distinct[:_MAX_BOUND_IDS]))


def clear_request_context() -> None:
    """Drop the current task's bound context. Call this when a request finishes."""
    structlog.contextvars.clear_contextvars()


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound logger. ``name`` defaults to the calling module."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger


def _drop_color_message(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Remove uvicorn's duplicate pre-coloured copy of the message."""
    event_dict.pop("color_message", None)
    return event_dict


#: More correlation ids than this in one batch means something unusual; the first few say so.
_MAX_BOUND_IDS = 3

_COUNTED_LEVELS = frozenset({"warning", "error", "critical"})


def _count_problems(_: Any, __: str, event_dict: EventDict) -> EventDict:
    """Feed `tradewatch_log_events_total`, the error-rate metric.

    Counting log lines rather than adding a counter beside every `log.error` call: every
    failure this codebase handles is already logged at error with a reason, so this covers
    all of them — library errors routed through the standard library included — and a new
    error path cannot forget to be counted.
    """
    level = event_dict.get("level")
    if level in _COUNTED_LEVELS:
        LOG_EVENTS.labels(level=level).inc()
    return event_dict


def _shared_processors() -> list[Processor]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        _count_problems,
        structlog.stdlib.ExtraAdder(),
        _drop_color_message,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]


def configure_logging(
    *,
    level: str = "INFO",
    log_format: LogFormat = LogFormat.CONSOLE,
) -> None:
    """Configure structlog and the standard library to share one pipeline.

    Safe to call more than once; the last call wins. Entry points call it before anything
    else, so that configuration errors are themselves logged in the configured format.
    """
    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if log_format is LogFormat.JSON
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=[
            *_shared_processors(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_shared_processors(),
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())

    for logger_name, logger_level in _NOISY_LOGGERS.items():
        logging.getLogger(logger_name).setLevel(logger_level)
