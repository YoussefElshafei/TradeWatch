"""`/ws/alerts` through the real ASGI app (Phase 9): connect, auto-subscribe, delivery,
cross-user isolation, and disconnect.

Unlike `/ws/market`, there is no `symbols` query parameter and no subscribe/unsubscribe
protocol to test — the whole point of this route is that the client cannot choose its key
(see `api/routes/websocket.py`'s `alert_stream` docstring). What is worth proving instead is
that the key really is derived from the token, that it is genuinely private per user, and
that a message reaches the browser end to end through `ConnectionManager` once dispatched —
the thing `tests/unit/test_websocket_manager.py` cannot prove because it never opens a real
socket.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tradewatch.api.app import create_app
from tradewatch.application.auth import create_access_token
from tradewatch.config.settings import Settings
from tradewatch.domain.enums import UserRole

pytestmark = pytest.mark.api


@pytest.fixture
def ws_app(settings: Settings) -> FastAPI:
    return create_app(settings)


def _token(settings: Settings, user_id: uuid.UUID) -> str:
    return create_access_token(
        user_id=user_id,
        role=UserRole.TRADER,
        secret=settings.security.jwt_secret.get_secret_value(),
        ttl_minutes=15,
    )


def _ws_url(token: str) -> str:
    return f"/ws/alerts?token={token}"


class TestConnectAndDisconnect:
    def test_connecting_registers_a_connection_on_the_alerts_manager(
        self, ws_app: FastAPI, settings: Settings
    ) -> None:
        token = _token(settings, uuid.uuid4())
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token)):
            assert ws_app.state.ws_alerts_manager.connection_count == 1
            # The market and alerts registries are independent (§9 of PROJECT_STATE.md's
            # "one Redis subscription per route" claim, made checkable).
            assert ws_app.state.ws_manager.connection_count == 0

    def test_disconnecting_unregisters_it(self, ws_app: FastAPI, settings: Settings) -> None:
        token = _token(settings, uuid.uuid4())
        with TestClient(ws_app) as client:
            with client.websocket_connect(_ws_url(token)):
                pass
            assert ws_app.state.ws_alerts_manager.connection_count == 0

    def test_connecting_without_a_token_is_refused(self, ws_app: FastAPI) -> None:
        with (
            TestClient(ws_app) as client,
            pytest.raises(Exception),  # noqa: B017 — Starlette raises on a handshake refusal
            client.websocket_connect("/ws/alerts"),
        ):
            pass
        assert ws_app.state.ws_alerts_manager.connection_count == 0


class TestAutoSubscription:
    def test_a_connection_is_subscribed_to_its_own_users_id_and_nothing_else(
        self, ws_app: FastAPI, settings: Settings
    ) -> None:
        user_id = uuid.uuid4()
        token = _token(settings, user_id)
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token)):
            manager = ws_app.state.ws_alerts_manager
            (connection,) = manager._connections.values()
            assert connection.keys == {str(user_id)}

    def test_two_users_are_subscribed_to_two_different_keys(
        self, ws_app: FastAPI, settings: Settings
    ) -> None:
        first_user, second_user = uuid.uuid4(), uuid.uuid4()
        with (
            TestClient(ws_app) as client,
            client.websocket_connect(_ws_url(_token(settings, first_user))),
            client.websocket_connect(_ws_url(_token(settings, second_user))),
        ):
            manager = ws_app.state.ws_alerts_manager
            keys = {frozenset(connection.keys) for connection in manager._connections.values()}
            assert keys == {frozenset({str(first_user)}), frozenset({str(second_user)})}


class TestEndToEndDelivery:
    """`ConnectionManager._dispatch` is what `_relay` calls for every message a real Redis
    subscription hands it — calling it directly here is the same path a live alert takes,
    without needing a reachable Redis to drive it (see `test_websocket_market.py`'s module
    docstring for why that split is safe to test across).
    """

    def test_an_alert_for_this_user_is_delivered(self, ws_app: FastAPI, settings: Settings) -> None:
        user_id = uuid.uuid4()
        token = _token(settings, user_id)
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token)) as ws:
            message = {
                "event_type": "alert.raised",
                "user_id": str(user_id),
                "title": "Unusual activity in AAPL",
            }
            ws_app.state.ws_alerts_manager._dispatch(message)

            assert ws.receive_json() == message

    def test_an_alert_for_a_different_user_is_not_delivered(
        self, ws_app: FastAPI, settings: Settings
    ) -> None:
        this_user, other_user = uuid.uuid4(), uuid.uuid4()
        token = _token(settings, this_user)
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token)):
            ws_app.state.ws_alerts_manager._dispatch(
                {"event_type": "alert.raised", "user_id": str(other_user)}
            )

            manager = ws_app.state.ws_alerts_manager
            (connection,) = manager._connections.values()
            assert connection.queue.qsize() == 0

    def test_a_market_message_never_reaches_the_alerts_channel(
        self, ws_app: FastAPI, settings: Settings
    ) -> None:
        """A message with a `symbol` but no `user_id` is exactly what `/ws/market` publishes
        — it must mean nothing to a manager filtering by `user_id`."""
        user_id = uuid.uuid4()
        token = _token(settings, user_id)
        with TestClient(ws_app) as client, client.websocket_connect(_ws_url(token)):
            ws_app.state.ws_alerts_manager._dispatch(
                {"event_type": "market.trade", "symbol": "AAPL"}
            )

            manager = ws_app.state.ws_alerts_manager
            (connection,) = manager._connections.values()
            assert connection.queue.qsize() == 0
