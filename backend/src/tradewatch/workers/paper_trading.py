"""The Paper Trading Engine: `uv run tradewatch-paper-trading`.

```
Kafka (tw.market.bars.1m.v1) → PaperTradingService → PostgreSQL (orders, fills, positions)
```

The fourth independent reader of the bar topic, in its own group `paper-trading` — killing
this worker affects no other consumer, and vice versa (ADR-0001).

`PaperTradingService`, not `application/trading/`, is where the database lives — `workers`
sits above `application` in the layered-architecture contract (`pyproject.toml`'s
import-linter config) exactly the way `workers/backtest_runner.py` already does: the pure,
portable decision-making (`application.trading.step`, `.engine.process_signal`,
`application.execution.apply_signal`/`simulate_fill`) has no I/O and could run in a backtest
or a unit test with no database at all; the repository calls, the transaction boundaries, and
the Kafka wiring that turn those decisions into rows belong to the driver that actually needs
a database, which is this file.

**One process, not two.** `ARCHITECTURE.md` §7.2 lists `tw.signals.v1` as a topic between a
Strategy Engine and a Paper Trading Engine. Nothing yet needs that second hop — there is no
independent consumer of raw signals — so this worker does both jobs in one pass over each
bar, per `infrastructure/events/kafka/topics.py`'s own rule against creating a topic with no
real consumer. Splitting evaluation from execution across `tw.signals.v1` is a later,
mechanical change to make *if* a second consumer of signals ever exists; it is not a
correctness requirement today.

**State restoration.** A deployment's `strategy_state` lives only in memory, rebuilt on
`warm_up()` by replaying every stored bar from `deployment.started_at` to now through
`evaluate()` — the same function a backtest calls, which is what `ARCHITECTURE.md` means by
"the `evaluate(state, bar)` contract... exists specifically so this is possible." A
deployment's cash/position, by contrast, is *not* cached in memory: it is read fresh from
`portfolios`/`positions` before every decision, because a portfolio's cash is shared across
every symbol it trades, and an in-memory ledger for one symbol would not see what another
symbol in the same portfolio just spent.

**Known gap.** A `pending_signal` deferred under `FillModel.NEXT_BAR_OPEN` lives only in
memory too. A crash between a signal firing and its deferred fill loses that one pending
fill on restart — reported here rather than hidden, and left for the Phase 11 failure-
injection work, the same way `ARCHITECTURE.md` §12 tracks F11's residual gap.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import signal
import sys
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.execution import ExecutionConfig, SymbolLedger
from tradewatch.application.ports.event_bus import EventBatchConsumer
from tradewatch.application.strategy import CompiledStrategy, Signal, compile_strategy
from tradewatch.application.trading import (
    OrderExecuted,
    OrderRejected,
    PositionSizingConfig,
    RiskLimitsConfig,
    SymbolRuntimeState,
    step,
)
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.entities import Bar
from tradewatch.domain.enums import BarInterval, DeploymentStatus, OrderStatus, OrderType
from tradewatch.domain.events import BarEvent, deterministic_event_id
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Bar as BarRow
from tradewatch.infrastructure.database.models import Fill as FillRow
from tradewatch.infrastructure.database.models import Order as OrderRow
from tradewatch.infrastructure.database.models import Portfolio, Strategy, StrategyDeployment
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.infrastructure.repositories import (
    BarRepository,
    DeploymentRepository,
    EquityPointRepository,
    InstrumentRepository,
    OrderRepository,
    PositionRepository,
)
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

__all__ = ["PaperTradingService", "SessionSource", "main", "run_paper_trading"]

#: The order-rate ceiling counts orders in this trailing window (failure F15).
_RATE_WINDOW = dt.timedelta(minutes=1)
#: How far back state warm-up ever looks, regardless of how old a deployment is — a cheap
#: guard against an absurd `started_at` turning a restart into an unbounded backfill.
_MAX_WARM_UP_LOOKBACK = dt.timedelta(days=365)


@dataclass(slots=True)
class _SymbolMemory:
    """The only state this service keeps between bars — see the module docstring.

    `last_bar_at` guards against evaluating the same bar twice: a brand-new consumer group
    starts from `TW_KAFKA_AUTO_OFFSET_RESET` (earliest, by default), which would otherwise
    redeliver every bar `warm_up`'s database replay already folded into `strategy_state` —
    and unlike a redelivered *order* (caught by `client_order_id`'s uniqueness), a bar fed
    through `evaluate` twice silently double-counts it in a rolling indicator window. This is
    the state-level idempotency guard client_order_id is the order-level one for.
    """

    strategy_state: Any
    pending_signal: Signal | None = None
    last_bar_at: dt.datetime | None = None


@dataclass(slots=True)
class _DeploymentContext:
    deployment: StrategyDeployment
    compiled: CompiledStrategy[Any, Any]
    symbols: tuple[str, ...]
    position_sizing: PositionSizingConfig
    risk_limits: RiskLimitsConfig
    execution_config: ExecutionConfig
    memory: dict[str, _SymbolMemory] = field(default_factory=dict)


class SessionSource(Protocol):
    """What this service actually needs: a way to open a transactional session.

    A `Protocol` rather than the concrete `Database`, exactly like
    `workers/backtest_runner.py`'s `SessionSource` — so a test can satisfy it with a fake
    sharing the test's own rolled-back transaction.
    """

    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...


class PaperTradingService:
    """Consume bars, evaluate running deployments, execute and persist. See module docstring."""

    def __init__(self, consumer: EventBatchConsumer, database: SessionSource) -> None:
        self._consumer = consumer
        self._database = database
        self._contexts: dict[uuid.UUID, _DeploymentContext] = {}

    async def warm_up(self) -> None:
        """Load every running deployment and rebuild its strategy state from stored bars."""
        async with self._database.session() as session:
            deployments = await DeploymentRepository(session).list_running()
            for deployment in deployments:
                await self._load_context(session, deployment)

    async def _load_context(self, session: AsyncSession, deployment: StrategyDeployment) -> None:
        """Build and register one deployment's context, warming its strategy state up from
        stored bars. Shared by `warm_up` (every running deployment, at startup) and
        `_reconcile` (one deployment, newly RUNNING since the last reconciliation).
        """
        strategy = await session.get(Strategy, deployment.strategy_id)
        if strategy is None:  # pragma: no cover — FK guarantees this cannot happen
            return
        compiled = compile_strategy(strategy.strategy_type, strategy.parameters)
        context = _DeploymentContext(
            deployment=deployment,
            compiled=compiled,
            symbols=tuple(strategy.symbols),
            position_sizing=PositionSizingConfig.model_validate(deployment.position_sizing),
            risk_limits=RiskLimitsConfig.model_validate(deployment.risk_limits),
            execution_config=ExecutionConfig.model_validate(deployment.execution_config),
        )
        now = dt.datetime.now(dt.UTC)
        start = max(deployment.started_at or now, now - _MAX_WARM_UP_LOOKBACK)
        instrument_repository = InstrumentRepository(session)
        for symbol in context.symbols:
            instrument = await instrument_repository.get_by_symbol(symbol)
            state = compiled.initial_state()
            last_bar_at: dt.datetime | None = None
            if instrument is not None:
                bars = await BarRepository(session).list_range(
                    instrument.id, BarInterval.ONE_MINUTE, start, now
                )
                for row in bars:
                    state, _ = compiled.evaluate(state, _to_domain_bar(row))
                    last_bar_at = row.bar_start
            context.memory[symbol] = _SymbolMemory(strategy_state=state, last_bar_at=last_bar_at)
        self._contexts[deployment.id] = context
        log.info(
            "paper_trading.warmed_up",
            deployment_id=str(deployment.id),
            symbols=list(context.symbols),
        )

    async def _reconcile(self) -> None:
        """Notice deployments the lifecycle API paused, resumed, or stopped since the last
        bar. Without this, a paused deployment would keep trading forever: `_contexts` is
        otherwise only ever populated once, at `warm_up`, and this process has no other way
        to learn that a row it already loaded changed status in a different process (the API
        server) via a different session.
        """
        async with self._database.session() as session:
            running = await DeploymentRepository(session).list_running()
            running_ids = {deployment.id for deployment in running}

            for deployment_id in [
                tracked_id for tracked_id in self._contexts if tracked_id not in running_ids
            ]:
                log.info(
                    "paper_trading.deployment_no_longer_running",
                    deployment_id=str(deployment_id),
                )
                del self._contexts[deployment_id]

            for deployment in running:
                if deployment.id not in self._contexts:
                    await self._load_context(session, deployment)

    async def handle_bar(self, event: BarEvent) -> None:
        """Process one bar for every running deployment watching its symbol."""
        await self._reconcile()
        contexts = [
            context
            for context in self._contexts.values()
            if context.deployment.status is DeploymentStatus.RUNNING
            and event.symbol in context.symbols
        ]
        for context in contexts:
            try:
                await self._handle_bar_for_deployment(context, event)
            except Exception as error:  # F14: isolate this deployment, never take others down
                log.warning(
                    "paper_trading.deployment_errored",
                    deployment_id=str(context.deployment.id),
                    symbol=event.symbol,
                    error=str(error),
                    error_type=type(error).__name__,
                )
                await self._mark_errored(context.deployment.id, str(error))
                context.deployment.status = DeploymentStatus.ERRORED

    async def _mark_errored(self, deployment_id: uuid.UUID, message: str) -> None:
        async with self._database.session() as session:
            deployment = await session.get(StrategyDeployment, deployment_id)
            if deployment is not None:
                deployment.status = DeploymentStatus.ERRORED
                deployment.error_message = message
                deployment.stopped_at = dt.datetime.now(dt.UTC)

    async def _handle_bar_for_deployment(
        self, context: _DeploymentContext, event: BarEvent
    ) -> None:
        deployment = context.deployment
        memory = context.memory[event.symbol]

        if memory.last_bar_at is not None and event.bar_start <= memory.last_bar_at:
            # Already folded into strategy_state, either by warm_up's database replay or a
            # previous call — see _SymbolMemory.last_bar_at's docstring. Evaluating it again
            # would double-count it in a rolling indicator window.
            return

        async with self._database.session() as session:
            instrument = await InstrumentRepository(session).get_by_symbol(event.symbol)
            if instrument is None:  # pragma: no cover — bars only exist for known instruments
                return
            bar = _to_domain_bar_from_event(event, instrument.id)

            portfolio = await session.get(Portfolio, deployment.portfolio_id)
            if portfolio is None:  # pragma: no cover — FK guarantees this cannot happen
                return
            position_repository = PositionRepository(session)
            position = await position_repository.get_position(portfolio.id, instrument.id)
            ledger = SymbolLedger(
                cash=portfolio.cash_balance,
                quantity=position.quantity if position else Decimal(0),
                avg_entry_price=position.avg_entry_price if position else Decimal(0),
                realized_pnl=position.realized_pnl if position else Decimal(0),
            )
            other_cost_basis = await self._other_positions_cost_basis(
                session, portfolio.id, exclude_instrument_id=instrument.id
            )
            order_repository = OrderRepository(session)
            recent_order_count = await order_repository.count_recent_for_deployment(
                deployment.id, dt.datetime.now(dt.UTC) - _RATE_WINDOW
            )

            runtime = SymbolRuntimeState(
                strategy_state=memory.strategy_state,
                pending_signal=memory.pending_signal,
                ledger=ledger,
            )
            new_runtime, outcome = step(
                runtime,
                bar,
                compiled=context.compiled,
                other_positions_cost_basis=other_cost_basis,
                recent_order_count=recent_order_count,
                position_sizing=context.position_sizing,
                risk_limits=context.risk_limits,
                execution_config=context.execution_config,
            )
            memory.strategy_state = new_runtime.strategy_state
            memory.pending_signal = new_runtime.pending_signal
            memory.last_bar_at = bar.bar_start

            if outcome is None:
                return

            # The same deterministic-id pattern as every other idempotency guard in the
            # pipeline since Phase 1 (ADR-0012): the natural key is "this deployment, this
            # instrument, this bar", so a redelivered bar always derives the identical id and
            # `orders.client_order_id`'s uniqueness constraint absorbs the duplicate.
            client_order_id = str(
                deterministic_event_id("paper_order", deployment.id, instrument.id, bar.bar_start)
            )
            if await order_repository.get_by_client_order_id(client_order_id) is not None:
                # Redelivery of a bar this deployment already turned into an order — the
                # same idempotency guard `client_order_id`'s own docstring describes.
                return

            await self._persist_outcome(
                session,
                deployment=deployment,
                portfolio=portfolio,
                instrument_id=instrument.id,
                client_order_id=client_order_id,
                outcome=outcome,
                bar=bar,
            )

    async def _other_positions_cost_basis(
        self, session: AsyncSession, portfolio_id: uuid.UUID, *, exclude_instrument_id: uuid.UUID
    ) -> Decimal:
        positions = await PositionRepository(session).list_open(portfolio_id)
        return sum(
            (
                position.quantity * position.avg_entry_price
                for position in positions
                if position.instrument_id != exclude_instrument_id
            ),
            Decimal(0),
        )

    async def _persist_outcome(
        self,
        session: AsyncSession,
        *,
        deployment: StrategyDeployment,
        portfolio: Portfolio,
        instrument_id: uuid.UUID,
        client_order_id: str,
        outcome: OrderExecuted | OrderRejected,
        bar: Bar,
    ) -> None:
        if isinstance(outcome, OrderRejected):
            order = OrderRow(
                id=new_id(),
                portfolio_id=portfolio.id,
                instrument_id=instrument_id,
                deployment_id=deployment.id,
                client_order_id=client_order_id,
                side=outcome.side,
                order_type=OrderType.MARKET,
                status=OrderStatus.REJECTED,
                # `orders.quantity` must be positive; a rejection before sizing ever ran
                # (the order-rate ceiling) has no real size to record. 1 is a nominal
                # placeholder, not a claim about what would have been ordered —
                # `reason_code` and `Order.reason_code`'s row are the actual record of what
                # happened, not this number.
                quantity=Decimal(1),
                reason_code=outcome.reason_code,
            )
            session.add(order)
            await session.flush()
            log.info(
                "paper_trading.order_rejected",
                deployment_id=str(deployment.id),
                reason_code=outcome.reason_code,
            )
            return

        fill = outcome.fill
        order = OrderRow(
            id=new_id(),
            portfolio_id=portfolio.id,
            instrument_id=instrument_id,
            deployment_id=deployment.id,
            client_order_id=client_order_id,
            side=outcome.side,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED,
            quantity=fill.quantity,
            filled_quantity=fill.quantity,
        )
        session.add(order)
        await session.flush()
        session.add(
            FillRow(
                id=new_id(),
                order_id=order.id,
                price=fill.price,
                quantity=fill.quantity,
                commission=fill.commission,
                slippage_bps=fill.slippage_bps,
                filled_at=fill.filled_at,
            )
        )
        portfolio.cash_balance = outcome.ledger.cash
        await PositionRepository(session).upsert(
            portfolio.id,
            instrument_id,
            quantity=outcome.ledger.quantity,
            avg_entry_price=outcome.ledger.avg_entry_price,
            realized_pnl=outcome.ledger.realized_pnl,
        )
        positions_value = outcome.ledger.quantity * fill.price
        await EquityPointRepository(session).upsert(
            portfolio.id,
            bar.bar_start,
            cash=portfolio.cash_balance,
            positions_value=positions_value,
        )
        log.info(
            "paper_trading.order_filled",
            deployment_id=str(deployment.id),
            side=outcome.side.value,
            quantity=str(fill.quantity),
            price=str(fill.price),
        )

    async def run(
        self, *, max_batches: int | None = None, stop: asyncio.Event | None = None
    ) -> None:
        """Consume bars until cancelled, until `stop` is set, or until `max_batches`."""
        batches = 0
        async for batch in self._consumer.batches():
            if stop is not None and stop.is_set():
                break
            for event in batch:
                if isinstance(event, BarEvent):
                    await self.handle_bar(event)
            await self._consumer.commit()
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break


def _to_domain_bar(row: BarRow) -> Bar:
    return Bar(
        instrument_id=row.instrument_id,
        interval=row.interval,
        bar_start=row.bar_start,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
        vwap=row.vwap,
        trade_count=row.trade_count,
        is_final=row.is_final,
        revision=row.revision,
        recorded_at=row.recorded_at,
    )


def _to_domain_bar_from_event(event: BarEvent, instrument_id: uuid.UUID) -> Bar:
    return Bar(
        instrument_id=instrument_id,
        interval=event.interval,
        bar_start=event.bar_start,
        open=event.open,
        high=event.high,
        low=event.low,
        close=event.close,
        volume=event.volume,
        vwap=event.vwap,
        trade_count=event.trade_count,
        is_final=True,
        revision=event.revision,
        recorded_at=event.ingested_at,
    )


async def run_paper_trading(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
) -> PaperTradingService:
    """Consume bars and run the Paper Trading Engine until cancelled, `stop`, or `max_batches`."""
    topics = Topics(settings.kafka.kafka_topic_prefix)
    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    dlq_producer = None
    dead_letters = None
    if settings.kafka.dlq_enabled:
        dead_letters, dlq_producer = await open_dead_letter_sink(
            settings.kafka,
            topics=topics,
            consumer_group=settings.kafka.kafka_paper_trading_group,
            client_id="tradewatch-paper-trading-dlq",
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
        group_id=settings.kafka.kafka_paper_trading_group,
        # Bars only. This service has no business with trades or quotes.
        subscribe_to=(topics.bars_1m,),
    )
    service = PaperTradingService(consumer, database)

    await consumer.start()
    await service.warm_up()
    log.info(
        "paper_trading.starting",
        group_id=consumer.group_id,
        topics=list(consumer.subscribed),
    )

    try:
        await service.run(max_batches=max_batches, stop=stop)
    finally:
        await consumer.stop()
        if dlq_producer is not None:
            with contextlib.suppress(Exception):
                await dlq_producer.stop()
        if owns_database:
            await database.disconnect()

    log.info("paper_trading.stopped")
    return service


def main() -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signal_name in ("SIGINT", "SIGTERM"):
            handler = getattr(signal, signal_name, None)
            if handler is None:
                continue
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(handler, stop.set)
        await run_paper_trading(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("paper_trading.interrupted")
    except Exception as error:
        log.error("paper_trading.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
