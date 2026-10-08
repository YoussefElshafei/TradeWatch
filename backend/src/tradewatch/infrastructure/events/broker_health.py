"""Is the broker reachable? The `kafka` entry in `/ready`.

A TCP connection to each bootstrap address, not a Kafka metadata request. The API may not
import a broker client at all — the import-linter contract "Kafka is an adapter" forbids it,
transitively, because that seam is what lets the in-memory bus stand in for the broker — and
the API holds no Kafka connection of its own: every HTTP read is served from PostgreSQL.

ponytail: a broker that accepts connections but cannot serve requests reads as `ok` here.
Request-level health is what `kafka-exporter` and the consumer-lag panel show; move this to
a metadata request if `/ready` ever gates traffic on Kafka.
"""

from __future__ import annotations

import asyncio
import contextlib

__all__ = ["BrokerHealth"]

#: Short: a local or in-VPC broker answers a TCP handshake in milliseconds.
_CONNECT_TIMEOUT_SECONDS = 2.0


class BrokerHealth:
    """Readiness check: can any bootstrap address be reached over TCP?"""

    def __init__(self, bootstrap: str) -> None:
        self._addresses = [_split(address) for address in bootstrap.split(",") if address]

    async def check(self) -> str | None:
        """None when one bootstrap address accepts a connection, else a short reason."""
        reasons: list[str] = []
        for host, port in self._addresses:
            try:
                _reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=_CONNECT_TIMEOUT_SECONDS
                )
            except TimeoutError:
                reasons.append("timeout")
                continue
            except OSError as error:
                reasons.append(type(error).__name__)
                continue
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            return None
        return reasons[0] if len(set(reasons)) == 1 else "unreachable"


def _split(address: str) -> tuple[str, int]:
    host, _, port = address.strip().rpartition(":")
    return host, int(port)
