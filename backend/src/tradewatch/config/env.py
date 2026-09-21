"""Loading the ``.env`` file.

This is deliberately not done at import time. A module that reads the filesystem when it is
imported makes tests depend on the developer's machine, and makes the order of imports
matter. Entry points call :func:`load_env_file` explicitly, once, before reading settings.

The file is searched for upward from the current directory, so ``uv run tradewatch-api``
finds the repository-root ``.env`` whether it is run from the repository root or from
``backend/``.
"""

from __future__ import annotations

import os
from pathlib import Path

DOTENV_NAME = ".env"
#: How far up to look before giving up. The repository is never deeper than this.
_MAX_PARENTS = 5


def find_env_file(start: Path | None = None) -> Path | None:
    """Return the nearest ``.env`` at or above ``start``, or None if there is none."""
    current = (start or Path.cwd()).resolve()
    for directory in (current, *list(current.parents)[:_MAX_PARENTS]):
        candidate = directory / DOTENV_NAME
        if candidate.is_file():
            return candidate
    return None


def load_env_file(path: Path | None = None, *, override: bool = False) -> Path | None:
    """Load ``.env`` into the process environment.

    Real environment variables win by default: an exported value is a deliberate act, and a
    file should not quietly override it.

    Args:
        path: File to load. Defaults to the nearest ``.env`` found upward from the current
            directory.
        override: Let file values replace variables already set in the environment.

    Returns:
        The file that was loaded, or None if there was none to load — which is not an
        error. Docker and CI supply configuration through the environment directly.
    """
    env_path = path or find_env_file()
    if env_path is None or not env_path.is_file():
        return None

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        name = name.strip()
        if name.startswith("export "):
            name = name.removeprefix("export ").strip()
        if not name:
            continue
        if not override and name in os.environ:
            continue
        os.environ[name] = _unquote(_strip_inline_comment(value.strip()))

    return env_path


def _strip_inline_comment(value: str) -> str:
    """Drop a trailing ``# comment``, unless the value is quoted."""
    if value[:1] in {'"', "'"}:
        return value
    head, separator, _ = value.partition(" #")
    return head.rstrip() if separator else value


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value
