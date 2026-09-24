"""`/ws/market`: live trades, quotes and closed bars, fanned out from Redis Pub/Sub.

```
tradewatch-broadcaster -> Redis Pub/Sub (tw:ws:market) -> ConnectionManager -> browsers
```

One `ConnectionManager` per API process, started at application startup (`api/app.py`). It
holds exactly one Redis subscription (`RedisCache.subscribe`) and fans a message out
in-process to every connected client currently interested in that message's symbol — not one
Redis subscription per client, which would not scale with the number of open browser tabs.

**No authentication yet.** `ARCHITECTURE.md` §8.2 specifies this channel as "Authenticated" —
that is the *finished-system* design (see `docs/PROJECT_STATE.md` §0 on reading that document
as a target, not a description). Auth is Phase 5; market data is unauthenticated everywhere
else in this codebase today (every `GET /api/v1/market/*` route is open), so an unauthenticated
`/ws/market` is consistent with the rest of the API as it exists right now, not a gap specific
to this endpoint. Revisit this the moment Phase 5 lands.

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
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from tradewatch.application.broadcasting import BROADCAST_CHANNEL
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


@router.websocket("/ws/market")
async def market_stream(
    websocket: WebSocket,
    symbols: str | None = Query(default=None, description="Comma-separated initial symbols."),
) -> None:
    """Subscribe to live trades, quotes and closed bars for one or more symbols.

    Protocol, matching `ARCHITECTURE.md` §8.2: after connecting, send
    ``{"action":"subscribe","symbols":["AAPL"]}`` or ``{"action":"unsubscribe",...}`` to
    change the watchlist at any time. The `symbols` query parameter is an optional shortcut
    for the common case of "subscribe to these from the start."
    """
    manager = get_manager(websocket)
    await websocket.accept()
    connection_id, queue = manager.register()
    if symbols:
        initial = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        manager.subscribe(connection_id, initial)

    sender = asyncio.create_task(_send_loop(websocket, queue))
    try:
        while True:
            raw = await websocket.receive_text()
            _handle_control_message(manager, connection_id, raw)
    except WebSocketDisconnect:
        pass
    finally:
        sender.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sender
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
