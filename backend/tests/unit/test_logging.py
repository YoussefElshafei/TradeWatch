"""Logging configuration.

The thing worth testing here is not that structlog works — it does — but that our two
promises hold: JSON output really is machine-readable, and bound request context really
does reach every line.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator

import pytest

from tradewatch.config.settings import LogFormat
from tradewatch.observability.logging import (
    bind_request_context,
    clear_request_context,
    configure_logging,
    get_logger,
    new_request_id,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def reset_logging() -> Iterator[None]:
    """Leave the logging system as we found it — pytest captures depend on it."""
    yield
    clear_request_context()
    logging.getLogger().handlers.clear()


def _emit(capsys: pytest.CaptureFixture[str], **context: str) -> str:
    log = get_logger("test")
    if context:
        bind_request_context(**context)
    log.info("thing.happened", symbol="AAPL")
    return capsys.readouterr().err


class TestJsonFormat:
    def test_emits_parseable_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging(level="INFO", log_format=LogFormat.JSON)

        record = json.loads(_emit(capsys))

        assert record["event"] == "thing.happened"
        assert record["symbol"] == "AAPL"
        assert record["level"] == "info"
        assert record["timestamp"].endswith("Z")

    def test_includes_bound_request_context(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging(level="INFO", log_format=LogFormat.JSON)
        request_id = new_request_id()

        record = json.loads(_emit(capsys, request_id=request_id))

        assert record["request_id"] == request_id

    def test_context_does_not_leak_after_clearing(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging(level="INFO", log_format=LogFormat.JSON)
        bind_request_context(request_id="first")
        clear_request_context()

        record = json.loads(_emit(capsys))

        assert "request_id" not in record


class TestConsoleFormat:
    def test_is_human_readable(self, capsys: pytest.CaptureFixture[str]) -> None:
        configure_logging(level="INFO", log_format=LogFormat.CONSOLE)

        output = _emit(capsys)

        assert "thing.happened" in output
        assert "AAPL" in output


class TestLevels:
    def test_below_the_configured_level_is_dropped(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure_logging(level="WARNING", log_format=LogFormat.JSON)

        assert _emit(capsys) == ""

    def test_standard_library_loggers_share_the_pipeline(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Third-party output must not arrive in a different shape from ours."""
        configure_logging(level="INFO", log_format=LogFormat.JSON)

        logging.getLogger("some.library").warning("legacy style %s", "message")

        record = json.loads(capsys.readouterr().err)
        assert record["event"] == "legacy style message"
        assert record["level"] == "warning"

    def test_reconfiguration_does_not_duplicate_handlers(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        configure_logging(level="INFO", log_format=LogFormat.JSON)
        configure_logging(level="INFO", log_format=LogFormat.JSON)

        assert len(_emit(capsys).strip().splitlines()) == 1
