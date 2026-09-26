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

import datetime as dt
import time
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from tradewatch.api.app import create_app
from tradewatch.application.auth import create_access_token
from tradewatch.config.settings import Settings
from tradewatch.domain.enums import UserRole

pytestmark = pytest.mark.api


@pytest.fixture
def ws_app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
def token(settings: Settings) -> str:
    """A valid access token (Phase 5): `/ws/market` now refuses a connection without one."""
    return create_access_token(
        user_id=uuid.uuid4(),
        role=UserRole.TRADER,
        secret=settings.security.jwt_secret.get_secret_value(),
        ttl_minutes=15,
    )


def _ws_url(token: str, query: str = "") -> str:
    return f"/ws/market?token={token}&{query}" if query else f"/ws/market?token={token}"


class TestConnectAndDisconnect:
    def test_connecting_registers_a_connection(self, ws_app: FastAPI, token: str) -> None:
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token, "symbols=AAPL")):
            assert ws_app.state.ws_manager.connection_count == 1

    def test_disconnecting_unregisters_it(self, ws_app: FastAPI, token: str) -> None:
        with TestClient(ws_app) as client:
            with client.websocket_connect(_ws_url(token, "symbols=AAPL")):
                pass
            assert ws_app.state.ws_manager.connection_count == 0

    def test_the_symbols_query_parameter_seeds_the_initial_subscription(
        self, ws_app: FastAPI, token: str
    ) -> None:
        url = _ws_url(token, "symbols=aapl,msft")
        with TestClient(ws_app) as client, client.websocket_connect(url):
            manager = ws_app.state.ws_manager
            (connection,) = manager._connections.values()
            # Lower-case input is upper-cased, matching every other symbol path in this API.
            assert connection.symbols == {"AAPL", "MSFT"}

    def test_two_independent_connections_are_both_tracked(
        self, ws_app: FastAPI, token: str
    ) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect(_ws_url(token, "symbols=AAPL")),
            client.websocket_connect(_ws_url(token, "symbols=MSFT")),
        ):
            assert ws_app.state.ws_manager.connection_count == 2

    def test_connecting_without_a_token_is_refused(self, ws_app: FastAPI) -> None:
        # Starlette's test client raises when the server closes during the handshake.
        with (
            TestClient(ws_app) as client,
            pytest.raises(Exception),  # noqa: B017
            client.websocket_connect("/ws/market?symbols=AAPL"),
        ):
            pass
        assert ws_app.state.ws_manager.connection_count == 0

    def test_connecting_with_a_garbage_token_is_refused(self, ws_app: FastAPI) -> None:
        with (
            TestClient(ws_app) as client,
            pytest.raises(Exception),  # noqa: B017
            client.websocket_connect("/ws/market?token=not-a-real-token"),
        ):
            pass
        assert ws_app.state.ws_manager.connection_count == 0

    def test_a_connection_closes_itself_the_instant_its_token_expires(
        self, ws_app: FastAPI, settings: Settings
    ) -> None:
        """ADR-0010: "the connection is closed when the token expires" — checked
        continuously, not only at connect. A token that is already a second from `exp`
        proves the watchdog fires without a real 15-minute wait.

        Asserted from the *client's* side — the server closing its end of an already-open
        socket races the same connection's own receive loop noticing and tearing down
        `ConnectionManager` state, so the only signal every browser actually gets to see is
        exactly the one checked here: the socket stops being usable.
        """
        near_expiry = create_access_token(
            user_id=uuid.uuid4(),
            role=UserRole.TRADER,
            secret=settings.security.jwt_secret.get_secret_value(),
            ttl_minutes=15,
            now=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=15) + dt.timedelta(seconds=1),
        )
        with (
            TestClient(ws_app) as client,
            client.websocket_connect(_ws_url(near_expiry)) as ws,
            pytest.raises(WebSocketDisconnect),
        ):
            ws.receive_text()


class TestSubscribeProtocol:
    def test_a_subscribe_message_adds_a_symbol(self, ws_app: FastAPI, token: str) -> None:
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token)) as ws:
            ws.send_json({"action": "subscribe", "symbols": ["AAPL"]})
            ws.send_json({"action": "subscribe", "symbols": ["MSFT"]})
            # No response protocol exists for control messages; give the server's receive
            # loop a turn before asserting on its side effect.
            _wait_for_symbols(ws_app, {"AAPL", "MSFT"})

    def test_an_unsubscribe_message_removes_a_symbol(self, ws_app: FastAPI, token: str) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect(_ws_url(token, "symbols=AAPL,MSFT")) as ws,
        ):
            ws.send_json({"action": "unsubscribe", "symbols": ["MSFT"]})
            _wait_for_symbols(ws_app, {"AAPL"})

    def test_malformed_json_does_not_close_the_connection(
        self, ws_app: FastAPI, token: str
    ) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect(_ws_url(token, "symbols=AAPL")) as ws,
        ):
            ws.send_text("not json at all {{{")
            # If the malformed frame had killed the connection, this would fail with a
            # closed-socket error instead of just being ignored the same way.
            ws.send_json({"action": "subscribe", "symbols": ["MSFT"]})
            _wait_for_symbols(ws_app, {"AAPL", "MSFT"})

    def test_an_unknown_action_is_ignored_without_closing_the_connection(
        self, ws_app: FastAPI, token: str
    ) -> None:
        with (
            TestClient(ws_app) as client,
            client.websocket_connect(_ws_url(token, "symbols=AAPL")) as ws,
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
