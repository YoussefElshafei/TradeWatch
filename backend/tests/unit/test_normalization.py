"""The normalization layer.

The rules here apply to every provider, so a bug in one of them is a bug in all of them.
Timestamps get the most attention because a misread epoch unit is wrong by a factor of a
thousand and still looks like a plausible date.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import pytest

from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import EventType, deterministic_event_id
from tradewatch.infrastructure.providers.errors import ProviderDataError
from tradewatch.infrastructure.providers.normalization import (
    EventFactory,
    normalize_symbol,
    parse_decimal,
    parse_timestamp,
    require,
)

pytestmark = pytest.mark.unit

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


@pytest.fixture
def factory() -> EventFactory:
    """A factory on a frozen clock, so `ingested_at` is assertable."""
    return EventFactory(source="testprov", producer="test@1.0", clock=lambda: MOMENT)


class TestParseTimestamp:
    @pytest.mark.parametrize(
        "value",
        [
            "2026-03-02T14:30:00Z",
            "2026-03-02T14:30:00+00:00",
            "2026-03-02T09:30:00-05:00",
            "2026-03-02T14:30:00",  # no zone: documented as UTC by every such provider
            dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC),
        ],
    )
    def test_accepts_every_shape_of_rfc3339(self, value: object) -> None:
        assert parse_timestamp(value) == MOMENT

    @pytest.mark.parametrize(
        "value",
        [
            1_772_461_800,  # seconds
            1_772_461_800_000,  # milliseconds
            1_772_461_800_000_000,  # microseconds
            1_772_461_800_000_000_000,  # nanoseconds
            "1772461800",  # a numeric string is an epoch, not a date
            1_772_461_800.0,
        ],
    )
    def test_infers_the_epoch_unit_from_magnitude(self, value: object) -> None:
        """Providers send all four units and rarely say which."""
        assert parse_timestamp(value) == MOMENT

    def test_preserves_sub_second_precision(self) -> None:
        parsed = parse_timestamp("2026-03-02T14:30:00.123456Z")

        assert parsed.microsecond == 123_456

    def test_converts_an_offset_to_utc(self) -> None:
        parsed = parse_timestamp("2026-03-02T09:30:00-05:00")

        assert parsed.tzinfo is dt.UTC
        assert parsed.hour == 14

    def test_a_naive_timestamp_becomes_utc_not_local(self) -> None:
        """Assuming local time would make the pipeline depend on the server's timezone."""
        parsed = parse_timestamp("2026-03-02T14:30:00")

        assert parsed.tzinfo is dt.UTC
        assert parsed.hour == 14

    @pytest.mark.parametrize(
        "value",
        ["", "   ", "not-a-date", "2026-13-45T99:99:99Z", None, [], {}, True],
    )
    def test_rejects_what_it_cannot_parse(self, value: object) -> None:
        with pytest.raises(ProviderDataError):
            parse_timestamp(value)

    def test_rejects_an_implausible_date(self) -> None:
        """A value the unit ladder misread would otherwise become a silently wrong date."""
        with pytest.raises(ProviderDataError, match="plausible range"):
            parse_timestamp(1_000)  # 1970

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_rejects_non_finite_numbers(self, value: float) -> None:
        with pytest.raises(ProviderDataError, match="finite"):
            parse_timestamp(value)

    def test_the_error_names_the_field(self) -> None:
        """ "could not parse timestamp" is useless in the middle of a stream."""
        with pytest.raises(ProviderDataError, match="bar_start"):
            parse_timestamp("nope", field="bar_start")


class TestParseDecimal:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("187.42", Decimal("187.42")),
            (187, Decimal("187")),
            (Decimal("187.42"), Decimal("187.42")),
            (" 187.42 ", Decimal("187.42")),
            ("1e2", Decimal("100")),
        ],
    )
    def test_accepts_numbers_in_their_usual_shapes(self, value: object, expected: Decimal) -> None:
        assert parse_decimal(value) == expected

    def test_a_float_goes_through_string(self) -> None:
        """ADR-0009. `Decimal(0.1)` carries the binary error; `Decimal("0.1")` does not."""
        assert parse_decimal(0.1) == Decimal("0.1")
        # Decimal(float) is exactly what ADR-0009 forbids, and constructing one here is how
        # the test shows the difference it makes.
        assert parse_decimal(0.1) != Decimal(0.1)  # noqa: RUF032

    def test_a_sum_of_floats_stays_exact(self) -> None:
        total = parse_decimal(0.1) + parse_decimal(0.2)

        assert total == Decimal("0.3")

    @pytest.mark.parametrize("value", ["", "abc", None, [], True, float("nan")])
    def test_rejects_what_is_not_a_number(self, value: object) -> None:
        with pytest.raises(ProviderDataError):
            parse_decimal(value)

    def test_rejects_zero_by_default(self) -> None:
        """A price of zero is a bad tick, not a free share."""
        with pytest.raises(ProviderDataError, match="zero"):
            parse_decimal("0")

    def test_allows_zero_when_asked(self) -> None:
        """A quote side with no size is information, not an error."""
        assert parse_decimal("0", allow_zero=True) == 0

    def test_rejects_a_negative_price(self) -> None:
        with pytest.raises(ProviderDataError, match="negative"):
            parse_decimal("-1")

    def test_allows_a_negative_when_asked(self) -> None:
        assert parse_decimal("-1", allow_negative=True) == Decimal("-1")


class TestNormalizeSymbol:
    @pytest.mark.parametrize(("value", "expected"), [("aapl", "AAPL"), (" msft ", "MSFT")])
    def test_upper_cases_and_strips(self, value: str, expected: str) -> None:
        assert normalize_symbol(value) == expected

    @pytest.mark.parametrize("value", ["", "   ", 123, None, "X" * 33])
    def test_rejects_what_is_not_a_usable_symbol(self, value: object) -> None:
        with pytest.raises(ProviderDataError):
            normalize_symbol(value)


class TestRequire:
    def test_returns_a_present_value(self) -> None:
        assert require({"p": 1}, "p") == 1

    def test_names_the_missing_field(self) -> None:
        with pytest.raises(ProviderDataError, match="'p'"):
            require({"q": 1}, "p")

    def test_lists_what_was_available(self) -> None:
        """So a field rename in the provider's API is obvious from one log line."""
        with pytest.raises(ProviderDataError) as raised:
            require({"price": 1, "size": 2}, "p")

        assert raised.value.detail == {"available": ["price", "size"]}

    def test_a_null_value_counts_as_missing(self) -> None:
        with pytest.raises(ProviderDataError, match="null"):
            require({"p": None}, "p")


class TestDeterministicEventId:
    def test_the_same_key_always_gives_the_same_id(self) -> None:
        """The property everything about deduplication rests on (ADR-0012)."""
        first = deterministic_event_id("alpaca", "trade", "AAPL", "12345")
        second = deterministic_event_id("alpaca", "trade", "AAPL", "12345")

        assert first == second

    def test_different_keys_give_different_ids(self) -> None:
        assert deterministic_event_id("a", "trade", "AAPL", "1") != deterministic_event_id(
            "a", "trade", "AAPL", "2"
        )

    def test_a_none_component_is_not_skipped(self) -> None:
        """Otherwise ("AAPL", None, "10") and ("AAPL", "10") would collide."""
        assert deterministic_event_id("AAPL", None, "10") != deterministic_event_id("AAPL", "10")

    def test_component_boundaries_cannot_be_forged(self) -> None:
        """Concatenation must not let one field impersonate two."""
        assert deterministic_event_id("ab", "c") != deterministic_event_id("a", "bc")


class TestEventFactory:
    def test_builds_a_trade_with_full_provenance(self, factory: EventFactory) -> None:
        event = factory.trade(
            symbol="AAPL",
            price=Decimal("187.42"),
            size=Decimal("100"),
            occurred_at=MOMENT,
            provider_trade_id="t-1",
        )

        assert event.event_type is EventType.TRADE
        assert event.source == "testprov"
        assert event.producer == "test@1.0"
        assert event.ingested_at == MOMENT
        assert event.correlation_id == factory.correlation_id
        assert event.partition_key == "AAPL"

    def test_the_same_trade_normalizes_to_the_same_id(self, factory: EventFactory) -> None:
        """A reconnect replay must deduplicate, not duplicate."""

        def build() -> object:
            return factory.trade(
                symbol="AAPL",
                price=Decimal("187.42"),
                size=Decimal("100"),
                occurred_at=MOMENT,
                provider_trade_id="t-1",
            ).event_id

        assert build() == build()

    def test_two_factories_agree_on_the_id(self) -> None:
        """Ids must be stable across processes and restarts, not just within one run."""
        arguments: dict[str, Any] = {
            "symbol": "AAPL",
            "price": Decimal("187.42"),
            "size": Decimal("100"),
            "occurred_at": MOMENT,
            "provider_trade_id": "t-1",
        }
        first = EventFactory(source="s", producer="a", clock=lambda: MOMENT).trade(**arguments)
        second = EventFactory(source="s", producer="b", clock=lambda: MOMENT).trade(**arguments)

        assert first.event_id == second.event_id

    def test_the_source_is_part_of_the_id(self) -> None:
        """The same trade from two providers is two observations, not one."""
        arguments: dict[str, Any] = {
            "symbol": "AAPL",
            "price": Decimal("187.42"),
            "size": Decimal("100"),
            "occurred_at": MOMENT,
            "provider_trade_id": "t-1",
        }
        first = EventFactory(source="alpaca", producer="p", clock=lambda: MOMENT).trade(**arguments)
        second = EventFactory(source="other", producer="p", clock=lambda: MOMENT).trade(**arguments)

        assert first.event_id != second.event_id

    def test_without_a_trade_id_the_price_and_size_identify_the_print(
        self, factory: EventFactory
    ) -> None:
        base: dict[str, Any] = {"symbol": "AAPL", "size": Decimal("100"), "occurred_at": MOMENT}
        one = factory.trade(price=Decimal("10"), **base)
        two = factory.trade(price=Decimal("11"), **base)

        assert one.event_id != two.event_id

    def test_a_quote_id_includes_the_sizes(self, factory: EventFactory) -> None:
        """Same prices, different sizes is a new quote — not a duplicate."""
        base: dict[str, Any] = {
            "symbol": "AAPL",
            "bid_price": Decimal("10.00"),
            "ask_price": Decimal("10.02"),
            "occurred_at": MOMENT,
        }
        one = factory.quote(bid_size=Decimal("100"), ask_size=Decimal("100"), **base)
        two = factory.quote(bid_size=Decimal("200"), ask_size=Decimal("100"), **base)

        assert one.event_id != two.event_id

    def test_a_bar_occurs_when_it_closes(self, factory: EventFactory) -> None:
        event = factory.bar(
            symbol="AAPL",
            interval=BarInterval.ONE_MINUTE,
            bar_start=MOMENT,
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10.5"),
            volume=Decimal("1000"),
        )

        assert event.bar_start == MOMENT
        assert event.occurred_at == MOMENT + dt.timedelta(minutes=1)
        assert event.event_type is EventType.BAR_CLOSED

    def test_a_revision_makes_it_a_correction(self, factory: EventFactory) -> None:
        event = factory.bar(
            symbol="AAPL",
            interval=BarInterval.ONE_MINUTE,
            bar_start=MOMENT,
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10.5"),
            volume=Decimal("1000"),
            revision=1,
        )

        assert event.event_type is EventType.BAR_CORRECTED

    def test_a_correction_is_not_a_duplicate_of_the_bar_it_corrects(
        self, factory: EventFactory
    ) -> None:
        """Deduplicating the correction away would mean it never arrived."""
        shared: dict[str, Any] = {
            "symbol": "AAPL",
            "interval": BarInterval.ONE_MINUTE,
            "bar_start": MOMENT,
            "open": Decimal("10"),
            "high": Decimal("11"),
            "low": Decimal("9"),
            "close": Decimal("10.5"),
            "volume": Decimal("1000"),
        }
        original = factory.bar(**shared)
        corrected = factory.bar(**shared, revision=1)

        assert original.event_id != corrected.event_id

    def test_lag_is_measurable(self) -> None:
        late = EventFactory(
            source="s", producer="p", clock=lambda: MOMENT + dt.timedelta(milliseconds=250)
        )
        event = late.trade(
            symbol="AAPL", price=Decimal("10"), size=Decimal("1"), occurred_at=MOMENT
        )

        assert event.lag_seconds == pytest.approx(0.25)

    def test_a_provider_clock_ahead_of_ours_gives_negative_lag(self, factory: EventFactory) -> None:
        """Tolerated, and visible as a metric rather than as a dropped tick."""
        event = factory.trade(
            symbol="AAPL",
            price=Decimal("10"),
            size=Decimal("1"),
            occurred_at=MOMENT + dt.timedelta(seconds=2),
        )

        assert event.lag_seconds == pytest.approx(-2.0)
