# ADR-0014: Consumer acknowledgement boundary and dead-letter policy

- **Status:** Accepted
- **Date:** 2026-09-21
- **Phase:** 2

## Context

ADR-0001 chose Kafka and stated the shape of the answer: at-least-once delivery, idempotent
consumers, offsets committed after the side effect. ADR-0005 chose the wire format. Building
Phase 2 turned that outline into a set of questions neither ADR answers, and each of them has
a wrong answer that looks reasonable and loses data quietly.

1. **Where does the acknowledgement boundary live in the code?** The Phase 1 `EventConsumer`
   port yields one event at a time and offers `commit()` meaning "everything so far". For an
   in-process queue, where commit is a no-op, that is fine. For offsets, "everything so far"
   is the entire correctness question and leaving it implicit is how a consumer ends up
   acknowledging a batch it only half wrote.

2. **What happens to an event that decodes cleanly and that the database will never
   accept?** A constraint violation is permanent. Retrying it stops a partition forever
   (failure F3). Dropping it loses data. Neither is acceptable, and the application is the
   only layer that can tell the difference — but quarantining is a transport concern, and the
   application must not import a broker client.

3. **What goes to the dead-letter topic, and what happens next?** `ARCHITECTURE.md` §7.4 says
   a poison message goes to `tw.dlq.v1` after bounded retries. It does not say which failures
   count as poison, and the tempting answer — "anything that failed too often" — is the one
   that turns a database outage into a manual replay of everything that arrived during it.

4. **How many retry budgets are there?** The processor already retries storage failures. A
   consumer that also retries produces a total attempt count nobody can derive from
   configuration.

## Decision

### The acknowledgement boundary is a port, and its unit is a batch

Two protocols were added to `tradewatch.application.ports.event_bus`. Neither existing
protocol changed, and no existing implementation needed modification.

- **`EventBatchConsumer`** — `batches()` yields lists of events; `commit()` acknowledges every
  batch yielded so far. A batch is the unit the processor already flushes in and the unit
  Kafka already commits in, so making it the unit of the port removes a guess rather than
  adding a concept. An empty batch is a legitimate result (a poll that timed out) and is
  yielded rather than swallowed, so a consumer gets a regular turn to notice a shutdown.

- **`DeadLetterSink`** — `send(event, reason, error, attempts)`. The application discovers
  that an event is permanently unstorable; this is how it says so without knowing what a
  topic is.

`MarketDataConsumerService` sits in `application` and enforces the order: buffer the batch,
flush it, **then** commit. A new `import-linter` contract forbids `tradewatch.domain`,
`tradewatch.application` and `tradewatch.api` from importing `aiokafka` at all, so the seam
is checked rather than asserted.

Messages that never become domain events — bytes that are not JSON, an unknown `event_type` —
are handled entirely inside the adapter, because the application has nothing useful to say
about bytes.

### A permanently rejected batch is isolated, not discarded

The store reports a constraint violation at **batch** granularity: one bad row fails the
transaction and the error does not say which row. On that failure the consumer replays the
batch one event at a time, writes the survivors, quarantines the single event that is
actually poison, and commits. Rewriting events that already landed is safe because every
write here is idempotent (ADR-0012).

Discarding the whole batch would have been three lines shorter and would throw away ninety-
nine good trades to be rid of one bad one — data loss with a tidy name.

This required one addition to `MarketDataProcessor`: `discard_pending()`, which abandons the
buffer and counts the abandoned rows as unprocessable. Without it the poison rows stay
buffered and every subsequent flush fails identically, which is a partition stopped forever.

### Only permanent failures are dead-lettered

`tw.dlq.v1` receives a message when, and only when, processing it can never succeed:
undecodable bytes, an envelope that fails validation, an unsupported `schema_version`, an
unknown `event_type`, an invalid payload, or an event the database refuses permanently.

A **retryable** failure never goes there. A database that is down leaves the offset
uncommitted and the events wait on their original topic.

A record carries the original bytes verbatim and the original key, plus namespaced headers
naming the reason, the error, the source topic/partition/offset, the consumer group, the
attempt count and the time.

**There is no redrive, deliberately.** Every reason a message is in the DLQ is a reason that
repeating it unchanged would fail again, so it is a message a human has to look at. A replay
tool is worth building once there is evidence about what actually lands there.

A failure to *write* to the DLQ propagates: the offset is not committed and the message is
redelivered. Acknowledging a message that was neither stored nor quarantined would be the
only path in this system that loses data silently.

### One retry owner

`TW_CONSUMER_MAX_RETRIES` becomes the processor's storage-attempt budget. The consumer
service does not retry; on a storage failure it declines to commit and propagates, the worker
exits non-zero, and the supervisor restarts it into a redelivery.

### A poll where every record is malformed commits itself

If a poll yields records but no decodable events, the adapter commits those offsets after
their dead-letter writes are acknowledged. The application would not commit an empty batch —
correctly, since there is nothing to store — and without this the same poison records would
be redelivered on every poll forever. It presents as "the consumer is running and the row
count never changes", which is an unpleasant thing to diagnose.

## Alternatives considered

**Keep `EventConsumer` and commit after each auto-flush.** No new protocol. Rejected: the
processor flushes on its own when its buffer fills, so the consumer would have to infer that
a flush had happened by watching a counter, and the acknowledgement boundary would be a
side effect of a buffer size. The thing most worth being explicit about would have been the
thing least visible in the code.

**Put the DLQ decision in the adapter for every failure.** The adapter already handles
undecodable bytes, so it could catch `UnprocessableEventError` too. Rejected: it would have
to interpret an application-level exception to decide, which inverts the dependency — the
transport would be deciding what "permanently unstorable" means.

**Discard a rejected batch wholesale.** Simpler, and genuinely defensible when batches are
small. Rejected: a batch is up to `TW_CONSUMER_BATCH_SIZE` events, the isolation pass is rare
by construction, and idempotent writes make the replay free of consequences.

**Dead-letter after N retries regardless of classification.** The conventional pattern, and
the reason so many systems quarantine an hour of perfectly good events during a database
outage. Rejected: retry count is evidence about *duration*, not about whether a message is
poison.

**Kafka transactions for exactly-once.** Rejected: it would narrow the duplicate window
without closing it — a producer restart is a new session — and would not remove the need for
idempotent writes, which already make duplicates harmless. Paying transaction coordination
costs for a guarantee that changes nothing downstream is not a trade worth making.

**Auto-commit with a short interval.** One setting instead of a service. Rejected outright:
it acknowledges messages that have been *fetched*, not *stored*, converting at-least-once
into at-most-once and losing exactly the data in flight when a consumer dies.

## Consequences

**Positive.** The acknowledgement boundary is one method call in one class, and the tests
that assert its ordering read as prose. The application stays free of broker types, so the
consumer logic is unit-tested against an in-memory bus in milliseconds while the same
properties are proved against a real broker in the integration suite. One bad event costs one
event.

**Negative.** Two more protocols to understand, and a batch-shaped port is slightly more
awkward for a consumer that genuinely wants one event at a time. The isolation pass makes a
rejected batch O(n) round trips instead of one — acceptable because it is rare, and visible
in `poison_batches` if it stops being rare. No redrive means a DLQ message needs manual work.

**Neutral.** `ProcessorStats`, `ConsumerStats` and `KafkaConsumerStats` are deliberately
separate. "Committed the offset", "wrote the row" and "read the message" are different claims,
and a bug that makes them disagree is the one worth catching.

## Revisit when

- The DLQ accumulates enough messages of one shape that a redrive tool would be applied more
  than once — at which point build it for that shape, not in general.
- `poison_batches` stops being rare, which would mean the isolation pass is on a hot path and
  the store should report failures per row instead of per batch.
- A second consumer group appears (Phase 3's aggregator) and wants different retry or
  quarantine behaviour, which would make these settings per-group rather than global.
