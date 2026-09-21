"""UUIDv7 generation.

Small module, but everything in the database is keyed on it, and the two properties that
matter — it is a valid v7, and values sort by creation time — are easy to break silently
with a shift in the wrong place.
"""

from __future__ import annotations

import time
from uuid import UUID, uuid4

import pytest

from tradewatch.domain.ids import new_id, timestamp_of

pytestmark = pytest.mark.unit


class TestNewId:
    def test_is_a_version_7_uuid(self) -> None:
        value = new_id()

        assert value.version == 7
        assert value.variant == "specified in RFC 4122"

    def test_ids_are_unique(self) -> None:
        ids = {new_id() for _ in range(10_000)}

        assert len(ids) == 10_000

    def test_ids_created_later_sort_later(self) -> None:
        """The whole reason for v7 over v4: inserts land at the right edge of the index."""
        first = new_id()
        time.sleep(0.005)
        second = new_id()

        assert first < second
        assert str(first) < str(second)

    def test_a_batch_is_roughly_ordered(self) -> None:
        """Within a millisecond the ordering is arbitrary; across milliseconds it is not."""
        batch = []
        for _ in range(5):
            batch.append(new_id())
            time.sleep(0.002)

        assert batch == sorted(batch)


class TestTimestampOf:
    def test_recovers_the_creation_time(self) -> None:
        before = time.time_ns() // 1_000_000
        value = new_id()
        after = time.time_ns() // 1_000_000

        assert before <= timestamp_of(value) <= after

    def test_refuses_a_uuid_that_is_not_version_7(self) -> None:
        """Reading the leading bits of a v4 would produce a plausible-looking lie."""
        with pytest.raises(ValueError, match="version 4"):
            timestamp_of(uuid4())

    def test_refuses_a_nil_uuid(self) -> None:
        with pytest.raises(ValueError):
            timestamp_of(UUID(int=0))
