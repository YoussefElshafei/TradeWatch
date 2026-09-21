"""Topic naming and routing.

Small, and worth testing precisely, because a topic name is a contract with every other
process and a routing mistake is silent: a trade published to the quotes topic is consumed,
stored and never noticed.
"""

from __future__ import annotations

import pytest

from tests.kafka_support import bar, quote, trade
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.events.kafka.topics import (
    SCHEMA_MAJOR,
    Topics,
    UnroutableEventError,
)

pytestmark = pytest.mark.unit


class TestNaming:
    def test_the_names_are_the_ones_the_architecture_specifies(self) -> None:
        """`ARCHITECTURE.md` §7.2, literally."""
        topics = Topics("tw")

        assert topics.trades == "tw.market.trades.v1"
        assert topics.quotes == "tw.market.quotes.v1"
        assert topics.bars_1m == "tw.market.bars.1m.v1"
        assert topics.dead_letter == "tw.dlq.v1"

    def test_the_prefix_is_configurable_so_environments_can_share_a_broker(self) -> None:
        topics = Topics("staging")

        assert topics.trades == "staging.market.trades.v1"
        assert topics.dead_letter == "staging.dlq.v1"

    def test_every_topic_carries_the_major_version(self) -> None:
        for name in Topics("tw").all:
            assert name.endswith(f".v{SCHEMA_MAJOR}")

    def test_this_phase_creates_exactly_four_topics(self) -> None:
        """The other five in §7.2 have no producer and no consumer yet."""
        assert len(Topics().all) == 4
        assert set(Topics().all) == {
            "tw.market.trades.v1",
            "tw.market.quotes.v1",
            "tw.market.bars.1m.v1",
            "tw.dlq.v1",
        }

    def test_the_consumer_subscribes_to_the_market_topics_only(self) -> None:
        """Subscribing to its own DLQ is how a consumer builds an infinite loop."""
        topics = Topics()

        assert topics.dead_letter not in topics.market
        assert len(topics.market) == 3


class TestRouting:
    def test_a_trade_goes_to_the_trades_topic(self) -> None:
        assert Topics().for_event(trade()) == "tw.market.trades.v1"

    def test_a_quote_goes_to_the_quotes_topic(self) -> None:
        assert Topics().for_event(quote()) == "tw.market.quotes.v1"

    def test_a_one_minute_bar_goes_to_the_bars_topic(self) -> None:
        assert Topics().for_event(bar()) == "tw.market.bars.1m.v1"

    def test_another_interval_is_refused_rather_than_invented(self) -> None:
        """No aggregator exists yet, so nothing produces these. Guessing a topic name
        would create a topic nobody creates and nobody consumes."""
        with pytest.raises(UnroutableEventError) as caught:
            Topics().for_event(bar(interval=BarInterval.ONE_HOUR))

        assert "1h" in str(caught.value)

    def test_routing_respects_the_prefix(self) -> None:
        assert Topics("staging").for_event(trade()) == "staging.market.trades.v1"


class TestPartitionKey:
    def test_the_key_is_the_symbol(self) -> None:
        """Keying is a correctness decision (§7.2): it is what orders a symbol's events."""
        assert trade(symbol="MSFT").partition_key == "MSFT"
        assert quote(symbol="MSFT").partition_key == "MSFT"
        assert bar(symbol="MSFT").partition_key == "MSFT"

    def test_every_event_for_one_symbol_shares_a_key_across_types(self) -> None:
        """So a trade, a quote and a bar for one symbol land on one partition, in order."""
        keys = {event.partition_key for event in (trade(), quote(), bar())}

        assert keys == {"AAPL"}
