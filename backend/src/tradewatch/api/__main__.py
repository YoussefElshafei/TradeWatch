"""Run the API: ``uv run tradewatch-api`` or ``python -m tradewatch.api``.

Development uses the reloader; production is expected to run uvicorn or gunicorn against
``tradewatch.api.app:bootstrap`` with its own worker configuration.
"""

from __future__ import annotations

import sys

import uvicorn

from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Environment, get_settings
from tradewatch.observability.logging import configure_logging, get_logger


def main() -> int:
    """Start the development server. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        # Logging is not configured yet, and this message is the whole point of the
        # failure, so it goes straight to stderr.
        print(error, file=sys.stderr)  # noqa: T201
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)
    get_logger(__name__).info(
        "api.serving",
        host=settings.api.api_host,
        port=settings.api.api_port,
        environment=settings.app.env.value,
    )
    uvicorn.run(
        "tradewatch.api.app:bootstrap",
        factory=True,
        host=settings.api.api_host,
        port=settings.api.api_port,
        reload=(
            settings.api.api_reload
            if settings.api.api_reload is not None
            else settings.app.env is Environment.LOCAL
        ),
        log_config=None,  # structlog owns logging
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
