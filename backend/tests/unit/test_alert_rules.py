"""Pure rule matching — no dispatcher, no database, no Kafka.

`ARCHITECTURE.md` §3.8: a rule filters by event type, symbol, and score threshold. Each gate
is tested in isolation, plus the two things Phase 9's exit criteria call out at the rule
level: dedupe keys agree for the same underlying condition and never collide across distinct
ones.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from tests.kafka_support import anomaly, fill, trade
from tradewatch.application.alerts import AlertRuleConfig, match
from tradewatch.domain.enums import AlertSeverity

pytestmark = pytest.mark.unit


def _rule(
    *,
    event_types: frozenset[str] = frozenset({"anomaly.detected"}),
    symbols: frozenset[str] | None = None,
    min_anomaly_score: Decimal | None = None,
    is_enabled: bool = True,
    cooldown_seconds: int = 300,
) -> AlertRuleConfig:
    return AlertRuleConfig(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        event_types=event_types,
        symbols=symbols,
        min_anomaly_score=min_anomaly_score,
        is_enabled=is_enabled,
        cooldown_seconds=cooldown_seconds,
    )


class TestEventTypeGate:
    def test_a_rule_matches_the_event_type_it_lists(self) -> None:
        candidate = match(_rule(event_types=frozenset({"anomaly.detected"})), anomaly())
        assert candidate is not None

    def test_a_rule_does_not_match_an_event_type_it_does_not_list(self) -> None:
        rule = _rule(event_types=frozenset({"order.filled"}))
        assert match(rule, anomaly()) is None

    def test_a_rule_can_listen_to_both_tracks(self) -> None:
        rule = _rule(event_types=frozenset({"anomaly.detected", "order.filled"}))
        assert match(rule, anomaly()) is not None
        assert match(rule, fill()) is not None

    def test_an_event_type_this_phase_does_not_route_through_alerts_never_matches(self) -> None:
        """Even a rule (mis)configured to list it — `match` only ever evaluates the two
        event types the Alert Dispatcher actually consumes."""
        rule = _rule(event_types=frozenset({"market.trade"}))
        assert match(rule, trade()) is None


class TestSymbolGate:
    def test_symbols_none_matches_every_symbol(self) -> None:
        rule = _rule(symbols=None)
        assert match(rule, anomaly(symbol="MSFT")) is not None

    def test_a_symbol_filter_rejects_a_non_matching_symbol(self) -> None:
        rule = _rule(symbols=frozenset({"MSFT"}))
        assert match(rule, anomaly(symbol="AAPL")) is None

    def test_a_symbol_filter_accepts_a_matching_symbol(self) -> None:
        rule = _rule(symbols=frozenset({"AAPL", "MSFT"}))
        assert match(rule, anomaly(symbol="AAPL")) is not None


class TestScoreThreshold:
    def test_below_the_floor_does_not_match(self) -> None:
        rule = _rule(min_anomaly_score=Decimal("5.0"))
        assert match(rule, anomaly(score="4.2")) is None

    def test_at_or_above_the_floor_matches(self) -> None:
        rule = _rule(min_anomaly_score=Decimal("4.0"))
        assert match(rule, anomaly(score="4.2")) is not None

    def test_no_floor_matches_any_score(self) -> None:
        rule = _rule(min_anomaly_score=None)
        assert match(rule, anomaly(score="0.1")) is not None

    def test_the_floor_is_meaningless_for_a_fill_and_never_rejects_one(self) -> None:
        rule = _rule(event_types=frozenset({"order.filled"}), min_anomaly_score=Decimal("999"))
        assert match(rule, fill()) is not None


class TestDisabledRule:
    def test_a_disabled_rule_never_matches(self) -> None:
        rule = _rule(is_enabled=False)
        assert match(rule, anomaly()) is None


class TestCandidateShape:
    def test_an_anomaly_candidate_carries_the_triggering_severity(self) -> None:
        event = anomaly()
        candidate = match(_rule(), event)
        assert candidate is not None
        assert candidate.severity == event.severity
        assert candidate.triggering_event_type == "anomaly.detected"
        assert candidate.symbol == event.symbol
        assert candidate.alert_rule_id
        assert candidate.user_id

    def test_a_fill_candidate_is_informational(self) -> None:
        rule = _rule(event_types=frozenset({"order.filled"}))
        candidate = match(rule, fill())
        assert candidate is not None
        assert candidate.severity == AlertSeverity.INFO
        assert candidate.triggering_event_type == "order.filled"


class TestDedupeKey:
    def test_the_same_underlying_event_produces_the_same_dedupe_key(self) -> None:
        """A dispatcher restart or a consumer replay redelivers the identical event — this is
        what makes `UNIQUE (alert_rule_id, dedupe_key)` absorb it."""
        event = anomaly()
        rule = _rule()

        first = match(rule, event)
        second = match(rule, event)

        assert first is not None
        assert second is not None
        assert first.dedupe_key == second.dedupe_key

    def test_two_distinct_detections_produce_different_dedupe_keys(self) -> None:
        """Two different windows scoring similarly are two different occurrences — cooldown,
        not dedup, is what suppresses a rule that fires on every one of them."""
        rule = _rule()

        first = match(rule, anomaly(model_version="1"))
        second = match(rule, anomaly(model_version="2"))

        assert first is not None
        assert second is not None
        assert first.dedupe_key != second.dedupe_key

    def test_the_dedupe_key_is_scoped_by_event_type_too(self) -> None:
        rule = _rule(event_types=frozenset({"anomaly.detected", "order.filled"}))
        anomaly_candidate = match(rule, anomaly())
        fill_candidate = match(rule, fill())

        assert anomaly_candidate is not None
        assert fill_candidate is not None
        assert anomaly_candidate.dedupe_key != fill_candidate.dedupe_key
