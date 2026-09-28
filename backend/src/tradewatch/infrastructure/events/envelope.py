"""The wire format: a versioned envelope wrapping a domain payload.

`ARCHITECTURE.md` §7.1 specifies one envelope for every message on every topic. ADR-0005
specifies how it is serialized: JSON, with the Pydantic models as the single schema
definition and an explicit ``schema_version``.

**Why this is a separate shape from the domain event.** A
:class:`~tradewatch.domain.events.MarketEvent` is flat — ``symbol`` and ``price`` sit beside
``event_id`` and ``producer``. On the wire they are separated: transport metadata at the top
level, the domain's own fields nested under ``payload``. That split is what lets a consumer
read the envelope of a message it cannot interpret — a future ``schema_version``, an
``event_type`` it has never heard of — and route it somewhere useful instead of failing to
parse the whole message. A flat wire format would make "this is a trade from ingestion, at
this time, and I do not understand the body" impossible to express.

**Determinism.** :func:`encode` sorts keys and uses fixed separators, so the same event
always produces byte-identical output. That is not decoration: it makes the encoder testable
by equality, and it makes a message digest a meaningful thing to compute.

**Money is a string, never a JSON number** (ADR-0009). Pydantic's JSON mode already does
this for ``Decimal``; the round-trip test in the unit suite is what keeps it true.

**Forward compatibility.** ADR-0005 says a compatible change adds an optional field and
consumers ignore what they do not recognise. The domain models are ``extra="forbid"``, so
ignoring has to be done deliberately: :func:`decode` filters the payload to the fields the
target model actually declares. An unknown field is dropped with a debug log, not an error.
A *breaking* change does not travel this path at all — it gets a new topic (ADR-0005).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from tradewatch.domain.events import (
    AnomalyEvent,
    BarEvent,
    EventType,
    MarketEvent,
    QuoteEvent,
    TradeEvent,
)
from tradewatch.observability.logging import get_logger

__all__ = [
    "SUPPORTED_SCHEMA_VERSIONS",
    "EventDecodeError",
    "EventEncodeError",
    "EventEnvelope",
    "decode",
    "encode",
    "to_envelope",
]

log = get_logger(__name__)

#: Schema versions this build can interpret. A message carrying anything else is a
#: permanent failure for *this* consumer — it is not malformed, it is from a future the
#: code does not know about, and retrying will not teach it. It goes to the DLQ, where an
#: operator can see that a rollout is half-done.
SUPPORTED_SCHEMA_VERSIONS: Final[frozenset[int]] = frozenset({1})

#: Top-level envelope keys. Everything else on a domain event becomes payload.
_ENVELOPE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "event_id",
        "event_type",
        "schema_version",
        "symbol",
        "occurred_at",
        "ingested_at",
        "source",
        "producer",
        "correlation_id",
    }
)

#: Which domain model reconstitutes each event type. `market.bar.closed` and
#: `market.bar.corrected` share one model, which distinguishes them by `event_type`.
_MODELS: Final[dict[str, type[MarketEvent]]] = {
    EventType.TRADE.value: TradeEvent,
    EventType.QUOTE.value: QuoteEvent,
    EventType.BAR_CLOSED.value: BarEvent,
    EventType.BAR_CORRECTED.value: BarEvent,
    EventType.ANOMALY_DETECTED.value: AnomalyEvent,
}


class EventEncodeError(RuntimeError):
    """An event could not be serialized.

    Permanent by construction: the same event will fail the same way. The producer raises
    it rather than sending a message it cannot represent, because a silently truncated
    event on a topic is worse than a loud failure at the point it was created.
    """


class EventDecodeError(RuntimeError):
    """A message could not be turned into a domain event.

    Always permanent, never retryable. Bad bytes stay bad, an unknown ``event_type`` stays
    unknown, and a payload that fails validation fails identically every time. Retrying any
    of them is how one poison message stops a partition forever (failure F3), which is
    exactly what the DLQ exists to prevent.

    ``reason`` is a short, stable classifier suitable for a metric label and a DLQ header;
    the message carries the detail.
    """

    def __init__(self, message: str, *, reason: str, event_id: uuid.UUID | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason
        self.event_id = event_id


class EventEnvelope(BaseModel):
    """The transport shape, validated before anything looks at the payload.

    ``extra="ignore"`` is deliberate and is the forward-compatibility rule from ADR-0005:
    an envelope field added by a newer producer must not stop an older consumer.

    ``event_type`` is a plain ``str`` rather than the :class:`EventType` enum so that an
    unrecognised type produces a clear "unsupported event type" with the envelope intact,
    instead of a validation error that makes an unknown type look like corruption. The two
    need different handling and a DLQ record should say which one happened.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    event_id: uuid.UUID
    event_type: str = Field(min_length=1)
    schema_version: int = Field(ge=1)
    symbol: str = Field(min_length=1, max_length=32)
    occurred_at: datetime
    ingested_at: datetime
    source: str = Field(min_length=1, max_length=32)
    producer: str = Field(min_length=1, max_length=64)
    correlation_id: uuid.UUID
    payload: dict[str, Any] = Field(default_factory=dict)


def to_envelope(event: MarketEvent) -> dict[str, Any]:
    """Split a domain event into transport metadata and payload.

    ``mode="json"`` is what turns ``Decimal`` into a string, ``datetime`` into ISO-8601 and
    ``UUID`` into text — the JSON-safe representations ADR-0009 and §7.1 require.
    """
    data = event.model_dump(mode="json")
    envelope: dict[str, Any] = {name: data[name] for name in _ENVELOPE_FIELDS if name in data}
    envelope["payload"] = {
        name: value for name, value in data.items() if name not in _ENVELOPE_FIELDS
    }
    return envelope


def encode(event: MarketEvent) -> bytes:
    """Serialize an event to deterministic UTF-8 JSON.

    ``sort_keys`` and fixed separators make the output byte-stable for a given event, so
    encoding is assertable by equality rather than by parsing the result back.

    ``allow_nan=False`` refuses to emit ``NaN`` or ``Infinity``. Python's ``json`` module
    writes those happily and they are not JSON; a consumer in another language would reject
    the message, and a consumer in Python would quietly accept a price that is not a number.

    Raises:
        EventEncodeError: If the event cannot be represented as JSON.
    """
    try:
        envelope = to_envelope(event)
        return json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise EventEncodeError(
            f"cannot serialize {type(event).__name__} {event.event_id}: {error}"
        ) from error


def decode(raw: bytes) -> MarketEvent:
    """Turn a message body back into a validated domain event.

    Four things are checked, in the order that produces the most useful failure: that the
    bytes are JSON, that the envelope is well-formed, that the schema version is one this
    build understands, and only then that the payload satisfies the domain model. Checking
    the payload first would report "price must be positive" for a message that is actually
    from an incompatible future version.

    Raises:
        EventDecodeError: For every failure. Always permanent — see the class docstring.
    """
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EventDecodeError(f"message is not valid JSON: {error}", reason="not_json") from error

    if not isinstance(parsed, dict):
        raise EventDecodeError(
            f"message is a JSON {type(parsed).__name__}, not an object",
            reason="not_an_object",
        )

    try:
        envelope = EventEnvelope.model_validate(parsed)
    except ValidationError as error:
        raise EventDecodeError(
            f"envelope is invalid: {_first_problem(error)}",
            reason="invalid_envelope",
        ) from error

    if envelope.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise EventDecodeError(
            f"schema_version {envelope.schema_version} is not supported by this build "
            f"(supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)})",
            reason="unsupported_schema_version",
            event_id=envelope.event_id,
        )

    model = _MODELS.get(envelope.event_type)
    if model is None:
        raise EventDecodeError(
            f"unsupported event type {envelope.event_type!r}",
            reason="unsupported_event_type",
            event_id=envelope.event_id,
        )

    fields = _fields_for(model, envelope)
    try:
        return model.model_validate(fields)
    except ValidationError as error:
        raise EventDecodeError(
            f"{envelope.event_type} payload is invalid: {_first_problem(error)}",
            reason="invalid_payload",
            event_id=envelope.event_id,
        ) from error


def _fields_for(model: type[MarketEvent], envelope: EventEnvelope) -> dict[str, Any]:
    """Flatten an envelope back into the domain model's constructor arguments.

    The payload is filtered to fields the model declares. That is ADR-0005's "consumers
    ignore unknown fields", made explicit because the domain models forbid extras — without
    this, a producer adding an optional field would break every older consumer, which is
    precisely the compatibility promise the ADR makes.
    """
    known = set(model.model_fields)
    fields: dict[str, Any] = {
        name: getattr(envelope, name) for name in _ENVELOPE_FIELDS if name in known
    }

    ignored = [name for name in envelope.payload if name not in known]
    if ignored:
        log.debug(
            "envelope.unknown_fields_ignored",
            event_type=envelope.event_type,
            event_id=str(envelope.event_id),
            fields=sorted(ignored),
        )

    fields |= {name: value for name, value in envelope.payload.items() if name in known}
    return fields


def _first_problem(error: ValidationError) -> str:
    """One readable line from a pydantic error, short enough for a log and a DLQ header."""
    problems = error.errors()
    if not problems:
        return "validation failed"
    first = problems[0]
    location = ".".join(str(part) for part in first.get("loc", ())) or "<model>"
    return f"{location}: {first.get('msg', 'invalid')}"
