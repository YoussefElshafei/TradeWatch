"""`/ws/market` through the real ASGI app: connect, subscribe/unsubscribe, disconnect.

The fan-out logic itself (which connection gets which message, the slow-consumer drop) is
covered in isolation in `tests/unit/test_websocket_manager.py`, where it can be driven
directly and deterministically. What only a real connection through the app proves is the
wiring: that `/ws/market` actually accepts a connection, honours the `symbols` query
parameter, parses the subscribe/unsubscribe protocol without crashing the connection on bad
input, and cleans up its registration on disconnect.

This never needs a reachable Redis: connecting, accepting, parsing control messages and
disconnecting are all local to the API process (`ConnectionManager.register` /
`.subscribe` / `.unregister`) — only the relay loop that reads live updates *from* Redis
needs a broker, and that loop runs independently of the request/response cycle a test drives
here.
"""

from __future__ import annotations

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tradewatch.api.app import create_app
from tradewatch.config.settings import Settings

pytestmark = pytest.mark.api


@pytest.fixture
def ws_app(settings: Settings) -> FastAPI:
    return create_app(settings)


class TestConnectAndDisconnect:
    def test_connecting_registers_a_connection(self, ws_app: FastAPI) -> None:
        with TestClient(ws_app) as client, client.websocket_connect("/ws/market?symbols=AAPL"):
            assert ws_app.state.ws_manager.connection_count == 1

    def test_disconnecting_unregisters_it(self, ws_app: FastAPI) -> None:
        with TestClient(ws_app) as client:
            with client.websocket_connect("/ws/market?symbols=AAPL"):
                pass
            assert ws_app.state.ws_manager.connection_count == 0

    def test_the_symbols_query_parameter_seeds_the_initial_subscription(
        self, ws_app: FastAPI
    ) -> None:
        with TestClient(ws_app) as client, client.websocket_connect("/ws/market?symbols=aapl,msft"):
            manager = ws_app.state.ws_manager
            (connection,) = manager._connections.values()
            # Lower-case input is upper-cased, matching every other symbol path in this API.
            assert connection.symbols == {"AAPL", "MSFT"}

    def test_two_independent_connections_are_both_tracked(self, ws_app: FastAPI) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect("/ws/market?symbols=AAPL"),
            client.websocket_connect("/ws/market?symbols=MSFT"),
        ):
            assert ws_app.state.ws_manager.connection_count == 2


class TestSubscribeProtocol:
    def test_a_subscribe_message_adds_a_symbol(self, ws_app: FastAPI) -> None:
        with TestClient(ws_app) as client, client.websocket_connect("/ws/market") as ws:
            ws.send_json({"action": "subscribe", "symbols": ["AAPL"]})
            ws.send_json({"action": "subscribe", "symbols": ["MSFT"]})
            # No response protocol exists for control messages; give the server's receive
            # loop a turn before asserting on its side effect.
            _wait_for_symbols(ws_app, {"AAPL", "MSFT"})

    def test_an_unsubscribe_message_removes_a_symbol(self, ws_app: FastAPI) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect("/ws/market?symbols=AAPL,MSFT") as ws,
        ):
            ws.send_json({"action": "unsubscribe", "symbols": ["MSFT"]})
            _wait_for_symbols(ws_app, {"AAPL"})

    def test_malformed_json_does_not_close_the_connection(self, ws_app: FastAPI) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect("/ws/market?symbols=AAPL") as ws,
        ):
            ws.send_text("not json at all {{{")
            # If the malformed frame had killed the connection, this would fail with a
            # closed-socket error instead of just being ignored the same way.
            ws.send_json({"action": "subscribe", "symbols": ["MSFT"]})
            _wait_for_symbols(ws_app, {"AAPL", "MSFT"})

    def test_an_unknown_action_is_ignored_without_closing_the_connection(
        self, ws_app: FastAPI
    ) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect("/ws/market?symbols=AAPL") as ws,
        ):
            ws.send_json({"action": "do_a_barrel_roll", "symbols": ["MSFT"]})
            ws.send_json({"action": "subscribe", "symbols": ["MSFT"]})
            _wait_for_symbols(ws_app, {"AAPL", "MSFT"})


def _wait_for_symbols(app: FastAPI, expected: set[str]) -> None:
    """Poll briefly for the server's receive loop to have processed a just-sent frame.

    `TestClient`'s WebSocket is synchronous from the test's side, driven by a background
    event loop; sending a frame returns before the server necessarily finished handling it.
    """
    deadline = time.monotonic() + 2.0
    (connection,) = app.state.ws_manager._connections.values()
    while connection.symbols != expected and time.monotonic() < deadline:
        time.sleep(0.02)
    assert connection.symbols == expected
