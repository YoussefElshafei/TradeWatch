"""Domain entities.

No database, no containers, no I/O — these run in milliseconds, which is the point of
keeping the domain free of SQLAlchemy in the first place. The rules here are the ones a bad
aggregator or a careless caller breaks, checked before a row is ever built.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from tradewatch.domain.entities import (
    Bar,
    Instrument,
    NewBar,
    NewQuote,
    NewTrade,
    Portfolio,
    Position,
    Strategy,
)
from tradewatch.domain.enums import AssetClass, BarInterval, InstrumentStatus
from tradewatch.domain.ids import new_id

pytestmark = pytest.mark.unit

NOW = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


def bar_values(**overrides: Any) -> dict[str, Any]:
    return {
        "instrument_id": new_id(),
        "interval": BarInterval.ONE_MINUTE,
        "bar_start": NOW,
        "open": Decimal("10.00"),
        "high": Decimal("10.50"),
        "low": Decimal("9.80"),
        "close": Decimal("10.20"),
        "volume": Decimal("1000"),
        "trade_count": 10,
    } | overrides


class TestTimestamps:
    """UTC or nothing (`ARCHITECTURE.md` §4.4)."""

    def test_rejects_a_naive_timestamp(self) -> None:
        """A naive datetime works all year and then produces two 01:30 bars in October."""
        with pytest.raises(ValidationError, match="timezone-aware"):
            # The one place a naive datetime is intentional: it is the thing under test.
            NewBar(**bar_values(bar_start=dt.datetime(2026, 3, 2, 14, 30)))  # noqa: DTZ001

    def test_normalises_an_offset_timestamp_to_utc(self) -> None:
        eastern = dt.timezone(dt.timedelta(hours=-5))
        bar = NewBar(**bar_values(bar_start=dt.datetime(2026, 3, 2, 9, 30, tzinfo=eastern)))

        assert bar.bar_start.tzinfo is dt.UTC
        assert bar.bar_start.hour == 14


class TestBarInvariants:
    def test_accepts_a_coherent_bar(self) -> None:
        bar = NewBar(**bar_values())

        assert bar.range == Decimal("0.70")
        assert bar.end == NOW + dt.timedelta(minutes=1)

    def test_rejects_a_high_below_the_low(self) -> None:
        with pytest.raises(ValidationError, match="below low"):
            NewBar(**bar_values(high=Decimal("9.00"), low=Decimal("9.80")))

    @pytest.mark.parametrize("price", [Decimal("11.00"), Decimal("9.00")])
    def test_rejects_an_open_outside_the_range(self, price: Decimal) -> None:
        with pytest.raises(ValidationError, match=r"open .* outside"):
            NewBar(**bar_values(open=price))

    @pytest.mark.parametrize("price", [Decimal("11.00"), Decimal("9.00")])
    def test_rejects_a_close_outside_the_range(self, price: Decimal) -> None:
        with pytest.raises(ValidationError, match=r"close .* outside"):
            NewBar(**bar_values(close=price))

    def test_accepts_a_flat_bar(self) -> None:
        """Thin names trade at one price for a whole minute."""
        flat = NewBar(
            **bar_values(
                open=Decimal("10"), high=Decimal("10"), low=Decimal("10"), close=Decimal("10")
            )
        )

        assert flat.range == 0

    def test_rejects_a_negative_volume(self) -> None:
        with pytest.raises(ValidationError):
            NewBar(**bar_values(volume=Decimal("-1")))

    def test_accepts_zero_volume(self) -> None:
        """A minute with no trades still produces a bar."""
        assert NewBar(**bar_values(volume=Decimal("0"), trade_count=0)).volume == 0

    def test_rejects_a_non_positive_price(self) -> None:
        with pytest.raises(ValidationError):
            NewBar(**bar_values(low=Decimal("0"), open=Decimal("0")))


class TestBarAlignment:
    """A bar must start on a boundary of its own interval."""

    def test_rejects_a_minute_bar_starting_mid_minute(self) -> None:
        """Drift here propagates into every rollup built on the bar."""
        with pytest.raises(ValidationError, match="not aligned"):
            NewBar(**bar_values(bar_start=NOW.replace(second=17)))

    def test_rejects_sub_second_precision(self) -> None:
        with pytest.raises(ValidationError, match="not aligned"):
            NewBar(**bar_values(bar_start=NOW.replace(microsecond=500)))

    def test_rejects_a_five_minute_bar_on_a_one_minute_boundary(self) -> None:
        with pytest.raises(ValidationError, match="not aligned"):
            NewBar(
                **bar_values(
                    interval=BarInterval.FIVE_MINUTES,
                    bar_start=NOW + dt.timedelta(minutes=1),
                )
            )

    def test_accepts_an_aligned_five_minute_bar(self) -> None:
        bar = NewBar(**bar_values(interval=BarInterval.FIVE_MINUTES, bar_start=NOW))

        assert bar.end == NOW + dt.timedelta(minutes=5)


class TestImmutability:
    def test_an_entity_cannot_be_mutated(self) -> None:
        """Reproducibility (ADR-0007) needs entities whose history cannot be rewritten."""
        bar = NewBar(**bar_values())

        with pytest.raises(ValidationError):
            bar.close = Decimal("99")

    def test_unknown_fields_are_rejected(self) -> None:
        """A typo in a field name must fail, not be silently ignored."""
        with pytest.raises(ValidationError):
            NewBar(**bar_values(closing_price=Decimal("10")))


class TestTrade:
    def test_round_trips(self) -> None:
        trade = NewTrade(
            event_id=new_id(),
            instrument_id=new_id(),
            price=Decimal("10.50"),
            size=Decimal("100"),
            occurred_at=NOW,
            ingested_at=NOW,
            provider="synthetic",
        )

        assert trade.price == Decimal("10.50")

    @pytest.mark.parametrize("field", ["price", "size"])
    def test_rejects_non_positive_values(self, field: str) -> None:
        values: dict[str, Any] = {
            "event_id": new_id(),
            "instrument_id": new_id(),
            "price": Decimal("10"),
            "size": Decimal("1"),
            "occurred_at": NOW,
            "ingested_at": NOW,
            "provider": "synthetic",
            field: Decimal("0"),
        }
        with pytest.raises(ValidationError):
            NewTrade(**values)

    def test_a_provider_clock_running_ahead_is_tolerated(self) -> None:
        """Dropping real market data because someone else's NTP is off is the worse failure."""
        trade = NewTrade(
            event_id=new_id(),
            instrument_id=new_id(),
            price=Decimal("10"),
            size=Decimal("1"),
            occurred_at=NOW + dt.timedelta(seconds=5),
            ingested_at=NOW,
            provider="synthetic",
        )

        assert trade.occurred_at > trade.ingested_at


class TestQuote:
    def _quote(self, **overrides: Any) -> NewQuote:
        values: dict[str, Any] = {
            "event_id": new_id(),
            "instrument_id": new_id(),
            "bid_price": Decimal("9.99"),
            "bid_size": Decimal("100"),
            "ask_price": Decimal("10.01"),
            "ask_size": Decimal("200"),
            "occurred_at": NOW,
            "ingested_at": NOW,
            "provider": "synthetic",
        }
        return NewQuote(**(values | overrides))

    def test_computes_spread_and_mid(self) -> None:
        quote = self._quote()

        assert quote.spread == Decimal("0.02")
        assert quote.mid_price == Decimal("10.00")

    def test_accepts_a_crossed_market(self) -> None:
        """Deliberately permitted; see the class docstring in `domain.entities`."""
        crossed = self._quote(bid_price=Decimal("10.05"), ask_price=Decimal("10.01"))

        assert crossed.spread < 0


class TestInstrument:
    def test_normalises_symbol_exchange_and_currency_to_upper_case(self) -> None:
        instrument = Instrument(
            id=new_id(),
            symbol="aapl",
            asset_class=AssetClass.EQUITY,
            exchange="xnas",
            currency="usd",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            first_seen_at=NOW,
        )

        assert (instrument.symbol, instrument.exchange, instrument.currency) == (
            "AAPL",
            "XNAS",
            "USD",
        )

    def test_defaults_to_active(self) -> None:
        instrument = Instrument(
            id=new_id(),
            symbol="AAPL",
            asset_class=AssetClass.EQUITY,
            exchange="XNAS",
            currency="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            first_seen_at=NOW,
        )

        assert instrument.status is InstrumentStatus.ACTIVE

    def test_rejects_a_non_positive_tick_size(self) -> None:
        with pytest.raises(ValidationError):
            Instrument(
                id=new_id(),
                symbol="AAPL",
                asset_class=AssetClass.EQUITY,
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0"),
                lot_size=Decimal("1"),
                first_seen_at=NOW,
            )


class TestPosition:
    def _position(self, quantity: Decimal, entry: Decimal = Decimal("10")) -> Position:
        return Position(
            portfolio_id=new_id(),
            instrument_id=new_id(),
            quantity=quantity,
            avg_entry_price=entry,
            realized_pnl=Decimal("0"),
            updated_at=NOW,
        )

    def test_a_long_position_gains_when_the_price_rises(self) -> None:
        position = self._position(Decimal("100"))

        assert position.unrealized_pnl(Decimal("12")) == Decimal("200")
        assert position.market_value(Decimal("12")) == Decimal("1200")
        assert position.is_open
        assert not position.is_short

    def test_a_short_position_gains_when_the_price_falls(self) -> None:
        """Short selling is deferred (D16), but the arithmetic has to be right when it lands."""
        position = self._position(Decimal("-100"))

        assert position.unrealized_pnl(Decimal("8")) == Decimal("200")
        assert position.market_value(Decimal("8")) == Decimal("-800")
        assert position.is_short

    def test_a_closed_position_is_not_open(self) -> None:
        position = self._position(Decimal("0"))

        assert not position.is_open
        assert position.unrealized_pnl(Decimal("99")) == 0

    def test_pnl_is_exact(self) -> None:
        """ADR-0009. The same sum in floats drifts; here it does not."""
        position = self._position(Decimal("3"), entry=Decimal("0.1"))

        assert position.unrealized_pnl(Decimal("0.3")) == Decimal("0.6")


class TestStrategyAndPortfolio:
    def test_strategy_symbols_are_normalised(self) -> None:
        strategy = Strategy(
            id=new_id(),
            user_id=new_id(),
            name="Crossover",
            strategy_type="sma_crossover",
            parameters={"fast": 10, "slow": 30},
            symbols=[" aapl ", "msft"],
            created_at=NOW,
            updated_at=NOW,
        )

        assert strategy.symbols == ["AAPL", "MSFT"]

    def test_a_strategy_needs_at_least_one_symbol(self) -> None:
        with pytest.raises(ValidationError):
            Strategy(
                id=new_id(),
                user_id=new_id(),
                name="Crossover",
                strategy_type="sma_crossover",
                symbols=[],
                created_at=NOW,
                updated_at=NOW,
            )

    def test_a_portfolio_needs_positive_starting_cash(self) -> None:
        with pytest.raises(ValidationError):
            Portfolio(
                id=new_id(),
                user_id=new_id(),
                name="Broke",
                base_currency="USD",
                initial_cash=Decimal("0"),
                cash_balance=Decimal("0"),
                created_at=NOW,
            )

    def test_cash_balance_may_be_negative(self) -> None:
        """An over-drawn simulated account is a state worth surfacing, not hiding."""
        portfolio = Portfolio(
            id=new_id(),
            user_id=new_id(),
            name="Overdrawn",
            base_currency="usd",
            initial_cash=Decimal("1000"),
            cash_balance=Decimal("-50"),
            created_at=NOW,
        )

        assert portfolio.base_currency == "USD"
        assert portfolio.cash_balance < 0


class TestPersistedVariants:
    def test_a_stored_bar_carries_its_record_time(self) -> None:
        bar = Bar(**bar_values(), recorded_at=NOW)

        assert bar.recorded_at == NOW

    def test_interval_durations(self) -> None:
        assert BarInterval.ONE_MINUTE.duration == dt.timedelta(minutes=1)
        assert BarInterval.ONE_DAY.duration == dt.timedelta(days=1)
        assert BarInterval.ONE_MINUTE.is_base
        assert not BarInterval.ONE_HOUR.is_base
