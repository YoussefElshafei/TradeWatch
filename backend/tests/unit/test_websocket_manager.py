"""``ConnectionManager``: fan-out by symbol, and the slow-consumer path (failure F19).

No real WebSocket and no real Redis — `_relay()` (the loop that actually reads Redis
Pub/Sub) is exercised end-to-end via `tests/integration/test_redis_cache.py`'s publish/
subscribe test; what belongs here, fast and deterministic, is the fan-out logic itself:
register, subscribe/unsubscribe, symbol filtering (`_dispatch`), and what happens when one
connection's queue is full.
"""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from tradewatch.api.routes.websocket import QUEUE_MAXSIZE, RESYNC_MESSAGE, ConnectionManager
from tradewatch.infrastructure.cache import RedisCache

pytestmark = pytest.mark.unit


def build_manager() -> ConnectionManager:
    """`_relay()` (the only place `cache` is read) is never invoked by these tests."""
    return ConnectionManager(cast("RedisCache", None))


class TestRegistrationAndFanOut:
    async def test_a_message_reaches_only_a_connection_subscribed_to_its_symbol(self) -> None:
        manager = build_manager()
        aapl_id, aapl_queue = manager.register()
        manager.subscribe(aapl_id, {"AAPL"})
        msft_id, msft_queue = manager.register()
        manager.subscribe(msft_id, {"MSFT"})

        manager._dispatch({"symbol": "AAPL", "price": "1"})

        assert aapl_queue.qsize() == 1
        assert msft_queue.qsize() == 0

    async def test_unsubscribing_stops_further_delivery(self) -> None:
        manager = build_manager()
        connection_id, queue = manager.register()
        manager.subscribe(connection_id, {"AAPL"})
        manager.unsubscribe(connection_id, {"AAPL"})

        manager._dispatch({"symbol": "AAPL"})

        assert queue.qsize() == 0

    async def test_a_connection_can_watch_more_than_one_symbol(self) -> None:
        manager = build_manager()
        connection_id, queue = manager.register()
        manager.subscribe(connection_id, {"AAPL", "MSFT"})

        manager._dispatch({"symbol": "AAPL"})
        manager._dispatch({"symbol": "MSFT"})

        assert queue.qsize() == 2

    async def test_a_message_with_no_symbol_is_ignored(self) -> None:
        manager = build_manager()
        connection_id, queue = manager.register()
        manager.subscribe(connection_id, {"AAPL"})

        manager._dispatch({"event_type": "something.else"})

        assert queue.qsize() == 0

    async def test_unregister_removes_the_connection(self) -> None:
        manager = build_manager()
        connection_id, _ = manager.register()
        manager.subscribe(connection_id, {"AAPL"})
        assert manager.connection_count == 1

        manager.unregister(connection_id)

        assert manager.connection_count == 0
        # Not an error: unregistering twice, or dispatching after unregister, must not raise
        # — a connection can close while a message it will never see is in flight.
        manager.unregister(connection_id)
        manager._dispatch({"symbol": "AAPL"})


class TestSlowConsumer:
    """Failure F19: a stuck client must not stall delivery to anyone else."""

    async def test_a_full_queue_is_dropped_and_replaced_with_one_resync_marker(self) -> None:
        manager = build_manager()
        connection_id, queue = manager.register()
        manager.subscribe(connection_id, {"AAPL"})

        for i in range(QUEUE_MAXSIZE + 1):
            manager._dispatch({"symbol": "AAPL", "seq": i})

        assert queue.qsize() == 1
        assert queue.get_nowait() == RESYNC_MESSAGE

    async def test_delivery_to_other_connections_is_unaffected_by_one_slow_one(self) -> None:
        manager = build_manager()
        slow_id, slow_queue = manager.register()
        manager.subscribe(slow_id, {"AAPL"})
        fast_id, fast_queue = manager.register()
        manager.subscribe(fast_id, {"MSFT"})

        # AAPL gets enough messages to overflow its subscriber; MSFT gets a handful its
        # subscriber can hold comfortably — proving the second connection's delivery was
        # never even slowed by the first one's overflow, not just that it survived it.
        overflow_by = 5
        for i in range(QUEUE_MAXSIZE + overflow_by):
            manager._dispatch({"symbol": "AAPL", "seq": i})
        for i in range(10):
            manager._dispatch({"symbol": "MSFT", "seq": i})

        # The slow queue filled, overflowed once (dropped to just the resync marker), then
        # kept absorbing the messages sent *after* that reset — it is a fresh queue again,
        # not a connection frozen in a resynced state.
        assert slow_queue.qsize() == overflow_by
        assert slow_queue.get_nowait() == RESYNC_MESSAGE
        # The fast one never overflowed: it holds exactly what was sent to it.
        assert fast_queue.qsize() == 10

    async def test_dispatch_never_blocks_the_caller(self) -> None:
        """The whole point of `put_nowait`: this must return immediately, always."""
        manager = build_manager()
        connection_id, _ = manager.register()
        manager.subscribe(connection_id, {"AAPL"})
        for i in range(QUEUE_MAXSIZE * 3):
            manager._dispatch({"symbol": "AAPL", "seq": i})

        async def call_it() -> None:
            manager._dispatch({"symbol": "AAPL", "seq": -1})

        await asyncio.wait_for(call_it(), timeout=0.1)
