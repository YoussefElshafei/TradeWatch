"""Identifier generation.

Every identifier in TradeWatch is a **UUIDv7**: 48 bits of Unix milliseconds followed by
random bits, so values sort by creation time.

That matters twice. Random UUIDv4 primary keys scatter inserts across a B-tree, which
dirties pages all over the index and inflates it; time-ordered values append to the right
edge instead. And `ARCHITECTURE.md` §7.1 needs `event_id` to be sortable so an event log
can be read in roughly the order it happened without consulting a timestamp column.

Python 3.13 has no `uuid.uuid7`, so this is the RFC 9562 layout implemented directly. When
the project moves to 3.14 this module becomes a one-line delegation.
"""

from __future__ import annotations

import os
import time
from uuid import UUID

__all__ = ["new_id", "timestamp_of"]

_UNIX_TS_MS_BITS = 48


def new_id() -> UUID:
    """Return a fresh UUIDv7.

    Monotonic only to the millisecond: two calls within the same millisecond sort
    arbitrarily relative to each other. Nothing in TradeWatch orders by id alone where
    that matters — `occurred_at` is the ordering key for market data.
    """
    return _uuid7(time.time_ns() // 1_000_000)


def timestamp_of(value: UUID) -> int:
    """Extract the embedded Unix-millisecond timestamp from a UUIDv7.

    Raises:
        ValueError: If the UUID is not version 7, in which case the leading bits are not
            a timestamp and reading them would produce a plausible-looking lie.
    """
    if value.version != 7:
        raise ValueError(f"expected a UUIDv7, got version {value.version}")
    return value.int >> (128 - _UNIX_TS_MS_BITS)


def _uuid7(unix_ms: int) -> UUID:
    # layout: 48-bit timestamp | 4-bit version | 12-bit random | 2-bit variant | 62-bit random
    random_bits = int.from_bytes(os.urandom(10), "big")  # 80 bits, we use 74
    value = (unix_ms & ((1 << _UNIX_TS_MS_BITS) - 1)) << 80
    value |= 0x7 << 76  # version 7
    value |= ((random_bits >> 62) & 0xFFF) << 64  # rand_a
    value |= 0b10 << 62  # RFC 9562 variant
    value |= random_bits & ((1 << 62) - 1)  # rand_b
    return UUID(int=value)
