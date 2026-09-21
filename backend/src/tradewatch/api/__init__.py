"""FastAPI gateway — the only component the browser talks to."""

from tradewatch.api.app import bootstrap, create_app

__all__ = ["bootstrap", "create_app"]
