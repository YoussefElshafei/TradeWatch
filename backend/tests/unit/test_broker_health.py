"""The `/ready` Kafka check: a real TCP listener answers, a closed port is named."""

from __future__ import annotations

import asyncio
import socket

import pytest

from tradewatch.infrastructure.events.broker_health import BrokerHealth

pytestmark = pytest.mark.unit


async def test_a_listening_broker_address_is_ok() -> None:
    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        assert await BrokerHealth(f"127.0.0.1:{port}").check() is None


async def test_a_closed_port_reports_why() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]  # bound, never listening: connections are refused

    assert await BrokerHealth(f"127.0.0.1:{port}").check() is not None


async def test_any_reachable_bootstrap_address_is_enough() -> None:
    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        assert await BrokerHealth(f"127.0.0.1:1,127.0.0.1:{port}").check() is None


async def test_a_name_resolving_to_an_unreachable_address_first_is_still_fast() -> None:
    """`localhost` -> `::1` (nothing listening) then `127.0.0.1`: must not cost the deadline."""
    server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        started = asyncio.get_running_loop().time()
        assert await BrokerHealth(f"localhost:{port}").check() is None
        assert asyncio.get_running_loop().time() - started < 1.5
