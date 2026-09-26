"""`/ws/market`: live trades, quotes and closed bars, fanned out from Redis Pub/Sub.

```
tradewatch-broadcaster -> Redis Pub/Sub (tw:ws:market) -> ConnectionManager -> browsers
```

One `ConnectionManager` per API process, started at application startup (`api/app.py`). It
holds exactly one Redis subscription (`RedisCache.subscribe`) and fans a message out
in-process to every connected client currently interested in that message's symbol — not one
Redis subscription per client, which would not scale with the number of open browser tabs.

**Authentication (Phase 5, ADR-0010).** The token travels as a `?token=` query parameter,
not an `Authorization` header — a browser's `WebSocket` constructor cannot set custom headers,
so a query parameter is the only place a bearer token can go on the request that opens the
connection. It is verified during the handshake, *before* `.accept()`, and the connection is
also **closed the instant the token's own `exp` arrives**, not only checked at connect — a
long-lived socket outliving the credential that opened it is the detail ADR-0010 calls out as
"the detail most implementations get wrong."

**Slow-consumer handling (failure F19).** Every connection gets a small bounded queue. The
fan-out loop below always uses ``put_nowait`` and never ``await``s a slow client's socket —
one browser tab on a bad connection must never stall delivery to every other tab, or stall the
single Redis subscription every tab shares. When a connection's queue is full, its entire
backlog is dropped and replaced with one `market.resync` marker: the client is told to re-fetch
via REST (which is fresh — see cache invalidation in `application/broadcasting/service.py`)
rather than trying to catch up message by message.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from starlette import status
from starlette.websockets import WebSocketState

from tradewatch.application.auth import (
    ExpiredAccessTokenError,
    InvalidAccessTokenError,
    decode_access_token,
    revoked_key,
)
from tradewatch.application.broadcasting import BROADCAST_CHANNEL
from tradewatch.config.settings import Settings
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.observability.logging import get_logger

__all__ = ["ConnectionManager", "router"]

router = APIRouter(tags=["market data"])

log = get_logger(__name__)

#: Messages buffered per connection before it is considered "slow" (F19). Generous enough to
#: absorb a burst across a handful of subscribed symbols, small enough that a genuinely stuck
#: client is noticed in well under a second at live market data rates.
QUEUE_MAXSIZE = 100

#: Sent to a connection whose backlog was just dropped, so the client knows to reload its
#: REST view rather than assume it has seen everything.
RESYNC_MESSAGE: dict[str, Any] = {"event_type": "market.resync"}


def _new_queue() -> asyncio.Queue[dict[str, Any]]:
    return asyncio.Queue(maxsize=QUEUE_MAXSIZE)


@dataclass(slots=True)
class _Connection:
    queue: asyncio.Queue[dict[str, Any]] = field(default_factory=_new_queue)
    symbols: set[str] = field(default_factory=set)


class ConnectionManager:
    """Owns every open `/ws/market` connection for one API process."""

    def __init__(self, cache: RedisCache, *, channel: str = BROADCAST_CHANNEL) -> None:
        self._cache = cache
        self._channel = channel
        self._connections: dict[uuid.UUID, _Connection] = {}
        self._relay_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Begin relaying the shared Redis subscription. Call once, at application startup."""
        if self._relay_task is not None:
            return
        self._relay_task = asyncio.create_task(self._relay(), name="ws-market-relay")

    async def stop(self) -> None:
        if self._relay_task is None:
            return
        self._relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._relay_task
        self._relay_task = None

    def register(self) -> tuple[uuid.UUID, asyncio.Queue[dict[str, Any]]]:
        connection_id = uuid.uuid4()
        connection = _Connection()
        self._connections[connection_id] = connection
        return connection_id, connection.queue

    def unregister(self, connection_id: uuid.UUID) -> None:
        self._connections.pop(connection_id, None)

    def subscribe(self, connection_id: uuid.UUID, symbols: set[str]) -> None:
        connection = self._connections.get(connection_id)
        if connection is not None:
            connection.symbols |= symbols

    def unsubscribe(self, connection_id: uuid.UUID, symbols: set[str]) -> None:
        connection = self._connections.get(connection_id)
        if connection is not None:
            connection.symbols -= symbols

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    async def _relay(self) -> None:
        async for message in self._cache.subscribe(self._channel):
            self._dispatch(message)

    def _dispatch(self, message: dict[str, Any]) -> None:
        """Fan a decoded message out to every connection currently subscribed to its symbol.

        Separated from :meth:`_relay` so the filtering rule — "only a connection watching
        this symbol receives it" — is testable on its own, without a real Redis subscription
        to drive it.
        """
        symbol = message.get("symbol")
        if symbol is None:
            return
        for connection in self._connections.values():
            if symbol not in connection.symbols:
                continue
            self._deliver(connection, message)

    @staticmethod
    def _deliver(connection: _Connection, message: dict[str, Any]) -> None:
        """Never blocks. A full queue means a slow client — drop it, don't stall the relay."""
        try:
            connection.queue.put_nowait(message)
        except asyncio.QueueFull:
            while not connection.queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    connection.queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                connection.queue.put_nowait(RESYNC_MESSAGE)
            log.warning("websocket.slow_client_resynced")


def get_manager(websocket: WebSocket) -> ConnectionManager:
    manager = getattr(websocket.app.state, "ws_manager", None)
    if manager is None:
        raise RuntimeError("the WebSocket connection manager was not attached at startup")
    return manager  # type: ignore[no-any-return]


def _get_settings(websocket: WebSocket) -> Settings:
    """`api/dependencies.get_settings_dep`'s WebSocket-flavoured twin.

    Not `Depends(get_settings_dep)`: that dependency is typed to take an HTTP `Request`,
    and a WebSocket connection is a different ASGI scope — `app.state` is the same object
    either way, so reading it directly here is not a shortcut, it is the only path that
    exists for a `websocket` route.
    """
    settings = getattr(websocket.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("settings were not attached to the application")
    return settings  # type: ignore[no-any-return]


def _get_cache(websocket: WebSocket) -> RedisCache:
    cache = getattr(websocket.app.state, "cache", None)
    if cache is None:
        raise RuntimeError("the cache was not attached during application startup")
    return cache  # type: ignore[no-any-return]


async def _authenticate(websocket: WebSocket, token: str | None) -> dt.datetime | None:
    """Verify the token before `.accept()`, per ADR-0010. Returns its expiry, or `None`
    after having already closed the connection — the caller's cue to stop.
    """
    if token is None:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required."
        )
        return None

    settings = _get_settings(websocket)
    try:
        claims = decode_access_token(token, secret=settings.security.jwt_secret.get_secret_value())
    except (ExpiredAccessTokenError, InvalidAccessTokenError):
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Invalid or expired token."
        )
        return None

    cache = _get_cache(websocket)
    if await cache.get_json(revoked_key(claims.jti)) is not None:
        await websocket.close(
            code=status.WS_1008_POLICY_VIOLATION, reason="Token has been revoked."
        )
        return None

    return claims.expires_at


async def _close_when_expired(websocket: WebSocket, expires_at: dt.datetime) -> None:
    """A socket must not outlive its credential (ADR-0010) — enforced continuously, not
    only at connect, which is the detail the ADR calls out as easy to get wrong.
    """
    remaining = (expires_at - dt.datetime.now(dt.UTC)).total_seconds()
    if remaining > 0:
        await asyncio.sleep(remaining)
    if websocket.client_state == WebSocketState.CONNECTED:
        log.info("websocket.token_expired")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token has expired.")


@router.websocket("/ws/market")
async def market_stream(
    websocket: WebSocket,
    token: str | None = Query(
        default=None,
        description="Access token. A browser WebSocket cannot set an Authorization header, "
        "so it travels here instead.",
    ),
    symbols: str | None = Query(default=None, description="Comma-separated initial symbols."),
) -> None:
    """Subscribe to live trades, quotes and closed bars for one or more symbols.

    Protocol, matching `ARCHITECTURE.md` §8.2: after connecting, send
    ``{"action":"subscribe","symbols":["AAPL"]}`` or ``{"action":"unsubscribe",...}`` to
    change the watchlist at any time. The `symbols` query parameter is an optional shortcut
    for the common case of "subscribe to these from the start."
    """
    expires_at = await _authenticate(websocket, token)
    if expires_at is None:
        return

    manager = get_manager(websocket)
    await websocket.accept()
    connection_id, queue = manager.register()
    if symbols:
        initial = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        manager.subscribe(connection_id, initial)

    sender = asyncio.create_task(_send_loop(websocket, queue))
    watchdog = asyncio.create_task(_close_when_expired(websocket, expires_at))
    try:
        while True:
            raw = await websocket.receive_text()
            _handle_control_message(manager, connection_id, raw)
    except WebSocketDisconnect:
        pass
    finally:
        sender.cancel()
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sender
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog
        manager.unregister(connection_id)


def _handle_control_message(manager: ConnectionManager, connection_id: uuid.UUID, raw: str) -> None:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        log.debug("websocket.invalid_control_message")
        return
    if not isinstance(parsed, dict):
        return

    action = parsed.get("action")
    requested = parsed.get("symbols") or []
    if not isinstance(requested, list):
        return
    wanted = {str(item).strip().upper() for item in requested if str(item).strip()}
    if not wanted:
        return

    if action == "subscribe":
        manager.subscribe(connection_id, wanted)
    elif action == "unsubscribe":
        manager.unsubscribe(connection_id, wanted)


async def _send_loop(websocket: WebSocket, queue: asyncio.Queue[dict[str, Any]]) -> None:
    """Drain this connection's queue to its socket, independent of every other connection.

    A slow *socket write* here blocks only this task, never the shared relay in
    :meth:`ConnectionManager._relay` — that separation is what makes the queue's bound (F19)
    actually protect the other clients rather than just moving the stall one step later.
    """
    while True:
        message = await queue.get()
        await websocket.send_json(message)
