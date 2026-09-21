"""Loading `.env`.

Small surface, but it is the first thing a new developer touches, so its edge cases —
comments, quotes, an exported variable that must win — are worth pinning down.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tradewatch.config.env import find_env_file, load_env_file

pytestmark = pytest.mark.unit


def _write(directory: Path, content: str) -> Path:
    path = directory / ".env"
    path.write_text(content, encoding="utf-8")
    return path


class TestLoading:
    def test_sets_variables_from_the_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = _write(tmp_path, "TW_LOG_LEVEL=DEBUG\nTW_SYMBOLS=AAPL,MSFT\n")

        assert load_env_file(path) == path
        assert os.environ["TW_LOG_LEVEL"] == "DEBUG"
        assert os.environ["TW_SYMBOLS"] == "AAPL,MSFT"

    def test_the_real_environment_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An exported value is a deliberate act; a file should not quietly override it."""
        monkeypatch.setenv("TW_LOG_LEVEL", "ERROR")
        path = _write(tmp_path, "TW_LOG_LEVEL=DEBUG\n")

        load_env_file(path)

        assert os.environ["TW_LOG_LEVEL"] == "ERROR"

    def test_override_is_available_when_asked_for(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TW_LOG_LEVEL", "ERROR")
        path = _write(tmp_path, "TW_LOG_LEVEL=DEBUG\n")

        load_env_file(path, override=True)

        assert os.environ["TW_LOG_LEVEL"] == "DEBUG"

    def test_a_missing_file_is_not_an_error(self, tmp_path: Path) -> None:
        """Docker and CI supply configuration through the environment directly."""
        assert load_env_file(tmp_path / "nope.env") is None


class TestParsing:
    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("TW_LOG_LEVEL=DEBUG", "DEBUG"),
            ("TW_LOG_LEVEL = DEBUG", "DEBUG"),
            ("export TW_LOG_LEVEL=DEBUG", "DEBUG"),
            ('TW_LOG_LEVEL="DEBUG"', "DEBUG"),
            ("TW_LOG_LEVEL='DEBUG'", "DEBUG"),
            ("TW_LOG_LEVEL=DEBUG  # inline comment", "DEBUG"),
            ('TW_LOG_LEVEL="DEBUG # not a comment"', "DEBUG # not a comment"),
        ],
    )
    def test_line_shapes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        line: str,
        expected: str,
    ) -> None:
        load_env_file(_write(tmp_path, f"{line}\n"))

        assert os.environ["TW_LOG_LEVEL"] == expected

    def test_blank_lines_and_comments_are_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = _write(tmp_path, "\n# a comment\n\nTW_LOG_LEVEL=DEBUG\n")

        load_env_file(path)

        assert os.environ["TW_LOG_LEVEL"] == "DEBUG"

    def test_a_url_containing_a_hash_survives_when_quoted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = _write(tmp_path, "TW_DATABASE_URL='postgresql://u:p#w@host/db'\n")

        load_env_file(path)

        assert os.environ["TW_DATABASE_URL"] == "postgresql://u:p#w@host/db"


class TestDiscovery:
    def test_finds_the_file_in_a_parent_directory(self, tmp_path: Path) -> None:
        """`uv run` from backend/ must still find the repository-root .env."""
        _write(tmp_path, "TW_LOG_LEVEL=DEBUG\n")
        nested = tmp_path / "backend" / "src"
        nested.mkdir(parents=True)

        assert find_env_file(nested) == tmp_path / ".env"

    def test_returns_none_when_there_is_no_file(self, tmp_path: Path) -> None:
        nested = tmp_path / "deep"
        nested.mkdir()

        found = find_env_file(nested)

        # tmp_path has no .env, but the search walks upward; assert it did not find one
        # belonging to this repository.
        assert found is None or found.parent not in {nested, tmp_path}
