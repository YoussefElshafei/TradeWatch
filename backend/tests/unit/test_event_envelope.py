"""The wire format: what goes on a topic, and what comes back off it.

This is the contract between two processes (ADR-0005), so the tests are about the things
that break a contract: a field that changed shape, a number that lost precision, a version
nobody handles, a message that is not what it claims to be.

The single most important test here is the money one. A price that round-trips through a
JSON float loses cents, silently, and every number computed from it afterwards is wrong in a
way no exception reports (ADR-0009).
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal
from typing import Any

import pytest

from tests.kafka_support import MOMENT, bar, factory, quote, trade
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, EventType, QuoteEvent, TradeEvent
from tradewatch.infrastructure.events.envelope import (
    SUPPORTED_SCHEMA_VERSIONS,
    EventDecodeError,
    EventEnvelope,
    decode,
    encode,
    to_envelope,
)

pytestmark = pytest.mark.unit


def body(event_bytes: bytes) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(event_bytes)
    return parsed


class TestEnvelopeShape:
    def test_transport_metadata_is_separate_from_the_domain_payload(self) -> None:
        """`ARCHITECTURE.md` §7.1: the envelope never changes, the payload always does."""
        envelope = body(encode(trade()))

        assert set(envelope) == {
            "event_id",
            "event_type",
            "schema_version",
            "symbol",
            "occurred_at",
            "ingested_at",
            "source",
            "producer",
            "correlation_id",
            "payload",
        }
        # The domain's own fields live under payload and nowhere else.
        assert set(envelope["payload"]) == {"price", "size", "provider_trade_id", "conditions"}

    def test_every_event_type_carries_the_same_envelope(self) -> None:
        keys = [set(body(encode(event))) - {"payload"} for event in (trade(), quote(), bar())]

        assert keys[0] == keys[1] == keys[2]

    def test_event_type_is_the_wire_discriminator(self) -> None:
        assert body(encode(trade()))["event_type"] == "market.trade"
        assert body(encode(quote()))["event_type"] == "market.quote"
        assert body(encode(bar()))["event_type"] == "market.bar.closed"

    def test_schema_version_is_present_and_explicit(self) -> None:
        assert body(encode(trade()))["schema_version"] == 1

    def test_to_envelope_and_encode_agree(self) -> None:
        event = trade()

        assert to_envelope(event) == body(encode(event))


class TestDeterminism:
    def test_the_same_event_encodes_to_identical_bytes(self) -> None:
        """Not decoration: it makes the encoder assertable by equality.

        It also means a dead-letter record can carry the event re-encoded and still be the
        original message byte for byte.
        """
        event = trade()

        assert encode(event) == encode(event)

    def test_keys_are_sorted_so_ordering_cannot_drift(self) -> None:
        raw = encode(trade()).decode("utf-8")
        keys = [part.split('"')[1] for part in raw.split(":{")[0].split(",")[:1]]

        parsed = json.loads(raw)
        assert list(parsed) == sorted(parsed)
        assert keys  # the first key parsed cleanly

    def test_no_incidental_whitespace(self) -> None:
        assert b", " not in encode(trade())
        assert b'": ' not in encode(trade())


class TestMoneyIsNeverAFloat:
    def test_prices_and_sizes_serialize_as_strings(self) -> None:
        """ADR-0009. A float round-trip through JSON is how a cent goes missing."""
        payload = body(encode(trade(price="123.45")))["payload"]

        assert payload["price"] == "123.45"
        assert isinstance(payload["price"], str)
        assert isinstance(payload["size"], str)

    def test_a_price_survives_a_round_trip_exactly(self) -> None:
        event = factory().trade(
            symbol="AAPL",
            price=Decimal("0.07"),
            size=Decimal("3.3333333"),
            occurred_at=MOMENT,
            provider_trade_id="t-1",
        )

        restored = decode(encode(event))

        assert isinstance(restored, TradeEvent)
        assert restored.price == Decimal("0.07")
        assert restored.size == Decimal("3.3333333")

    def test_every_ohlcv_field_is_a_string_on_the_wire(self) -> None:
        payload = body(encode(bar()))["payload"]

        for name in ("open", "high", "low", "close", "volume"):
            assert isinstance(payload[name], str), name


class TestRoundTrip:
    def test_a_trade_survives_intact(self) -> None:
        original = trade()

        restored = decode(encode(original))

        assert restored == original

    def test_a_quote_survives_intact(self) -> None:
        original = quote()

        restored = decode(encode(original))

        assert isinstance(restored, QuoteEvent)
        assert restored == original

    def test_a_bar_survives_intact(self) -> None:
        original = bar()

        restored = decode(encode(original))

        assert isinstance(restored, BarEvent)
        assert restored == original

    def test_a_corrected_bar_keeps_its_type_and_revision(self) -> None:
        original = factory().bar(
            symbol="AAPL",
            interval=BarInterval.ONE_MINUTE,
            bar_start=MOMENT,
            open=Decimal("10"),
            high=Decimal("11"),
            low=Decimal("9"),
            close=Decimal("10.5"),
            volume=Decimal("1000"),
            # The factory derives the event type from the revision: a bar with a revision
            # above zero *is* a correction, so the two cannot disagree.
            revision=2,
        )

        restored = decode(encode(original))

        assert isinstance(restored, BarEvent)
        assert restored.event_type is EventType.BAR_CORRECTED
        assert restored.revision == 2

    def test_timestamps_stay_utc_and_exact(self) -> None:
        restored = decode(encode(trade()))

        assert restored.occurred_at == MOMENT
        assert restored.occurred_at.tzinfo is not None
        assert restored.occurred_at.utcoffset() == dt.timedelta(0)

    def test_event_id_is_preserved_because_deduplication_depends_on_it(self) -> None:
        """ADR-0012: if the id changed in transit, every idempotency guarantee would fail."""
        original = trade()

        assert decode(encode(original)).event_id == original.event_id

    def test_correlation_id_survives_so_a_trace_is_not_broken(self) -> None:
        original = trade()

        assert decode(encode(original)).correlation_id == original.correlation_id


class TestMalformedMessages:
    """Every one of these is permanent, and every one goes to the DLQ rather than crashing."""

    def test_not_json(self) -> None:
        with pytest.raises(EventDecodeError) as caught:
            decode(b"this is not json")

        assert caught.value.reason == "not_json"

    def test_empty_body(self) -> None:
        with pytest.raises(EventDecodeError) as caught:
            decode(b"")

        assert caught.value.reason == "not_json"

    def test_invalid_utf8(self) -> None:
        with pytest.raises(EventDecodeError) as caught:
            decode(b"\xff\xfe\x00garbage")

        assert caught.value.reason == "not_json"

    def test_json_that_is_not_an_object(self) -> None:
        with pytest.raises(EventDecodeError) as caught:
            decode(b"[1, 2, 3]")

        assert caught.value.reason == "not_an_object"

    def test_missing_required_envelope_field(self) -> None:
        payload = body(encode(trade()))
        del payload["event_id"]

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_envelope"
        assert "event_id" in str(caught.value)

    def test_missing_payload_field(self) -> None:
        payload = body(encode(trade()))
        del payload["payload"]["price"]

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_payload"

    def test_invalid_payload_value(self) -> None:
        """A negative price is refused at the boundary, not three services downstream."""
        payload = body(encode(trade()))
        payload["payload"]["price"] = "-1"

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_payload"

    def test_invalid_timestamp(self) -> None:
        payload = body(encode(trade()))
        payload["occurred_at"] = "the third of never"

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_envelope"

    def test_naive_timestamp_is_refused(self) -> None:
        """A naive datetime is the bug that produces two 01:30 bars one October morning."""
        payload = body(encode(trade()))
        payload["occurred_at"] = "2026-03-02T14:30:00"

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_payload"

    def test_empty_symbol(self) -> None:
        payload = body(encode(trade()))
        payload["symbol"] = ""

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_envelope"

    def test_absurdly_long_symbol(self) -> None:
        payload = body(encode(trade()))
        payload["symbol"] = "A" * 64

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_envelope"

    def test_incoherent_bar_is_refused(self) -> None:
        """High below low is not a bar, and the database would refuse it anyway."""
        payload = body(encode(bar()))
        payload["payload"]["high"] = "1"

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_payload"

    def test_unknown_event_type(self) -> None:
        payload = body(encode(trade()))
        payload["event_type"] = "market.something.new"

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "unsupported_event_type"
        # The envelope still parsed, so the id is available for the DLQ record.
        assert caught.value.event_id is not None


class TestSchemaVersioning:
    def test_version_one_is_what_this_build_speaks(self) -> None:
        assert frozenset({1}) == SUPPORTED_SCHEMA_VERSIONS

    def test_a_future_version_is_rejected_rather_than_guessed_at(self) -> None:
        """ADR-0005: a breaking change gets a new topic, and an old consumer must not guess."""
        payload = body(encode(trade()))
        payload["schema_version"] = 2

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "unsupported_schema_version"

    def test_version_is_checked_before_the_payload_is(self) -> None:
        """Otherwise an incompatible message reports a confusing validation error instead."""
        payload = body(encode(trade()))
        payload["schema_version"] = 9
        payload["payload"]["price"] = "-1"

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "unsupported_schema_version"

    def test_zero_is_not_a_version(self) -> None:
        payload = body(encode(trade()))
        payload["schema_version"] = 0

        with pytest.raises(EventDecodeError) as caught:
            decode(json.dumps(payload).encode())

        assert caught.value.reason == "invalid_envelope"


class TestForwardCompatibility:
    """ADR-0005's compatible-change rule, which is a promise this has to keep."""

    def test_an_unknown_payload_field_is_ignored(self) -> None:
        payload = body(encode(trade()))
        payload["payload"]["venue_sequence_number"] = 99

        restored = decode(json.dumps(payload).encode())

        assert isinstance(restored, TradeEvent)
        assert restored.price == Decimal("100")

    def test_an_unknown_envelope_field_is_ignored(self) -> None:
        payload = body(encode(trade()))
        payload["region"] = "eu-west-1"

        assert decode(json.dumps(payload).encode()).symbol == "AAPL"

    def test_ignoring_is_deliberate_not_accidental(self) -> None:
        """The domain models forbid extras, so this only works because decode filters."""
        assert TradeEvent.model_config["extra"] == "forbid"


class TestEnvelopeModel:
    def test_it_parses_a_message_whose_payload_it_cannot_interpret(self) -> None:
        """The reason the wire format nests the payload: read the envelope regardless."""
        envelope = EventEnvelope.model_validate(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "market.from.the.future",
                "schema_version": 7,
                "symbol": "AAPL",
                "occurred_at": MOMENT.isoformat(),
                "ingested_at": MOMENT.isoformat(),
                "source": "test",
                "producer": "test@1.0",
                "correlation_id": str(uuid.uuid4()),
                "payload": {"anything": "at all"},
            }
        )

        assert envelope.symbol == "AAPL"
        assert envelope.schema_version == 7
