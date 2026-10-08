/**
 * A backtest's status, equity curve, trade list, and metrics (Phase 6).
 *
 * **Polling, not a socket.** `ARCHITECTURE.md` §4.3: a backtest is a batch job behind
 * Kafka, and the client polls `GET /api/v1/backtests/{id}` rather than subscribing to
 * anything — there is no `/ws/backtests` in this phase. The poll reuses `useAsync`'s own
 * `reload`, on a plain `setInterval`, stopped once the run reaches a terminal status.
 *
 * `queued` and `running` are not error states — same four-states discipline as
 * `MarketView.tsx`: a run still in progress has no trades or equity curve *yet*, which is
 * expected, not broken.
 */

import { useCallback, useEffect } from 'react';
import { useParams } from 'react-router-dom';

import {
  type BacktestEquityList,
  type BacktestMetrics,
  type BacktestRun,
  type BacktestTradeList,
  TERMINAL_STATUSES,
  fetchBacktest,
  fetchBacktestEquity,
  fetchBacktestTrades,
} from '../api/backtests';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { formatMoney, formatPercent, formatTimestamp } from '../lib/format';
import { Message } from './MarketView';
import {
  CartesianGrid,
  Line,
  LineChart,
  Tooltip,
  type TooltipContentProps,
  XAxis,
  YAxis,
} from 'recharts';

const POLL_INTERVAL_MS = 2000;

interface EquityChartPoint {
  label: string;
  equity: number;
  equityText: string;
}

function EquityTooltip({ active, payload }: TooltipContentProps) {
  const entries = active ? payload : undefined;
  const entry = entries?.at(0);
  const point = entry === undefined ? undefined : (entry.payload as EquityChartPoint);
  if (point === undefined) return null;
  return (
    <div className="chart-tooltip" role="note">
      <strong>{point.label}</strong>
      <p>{formatMoney(point.equityText)}</p>
    </div>
  );
}

function EquityChart({ backtestId }: { backtestId: string }) {
  const { accessToken } = useAuth();
  const { state } = useAsync<BacktestEquityList>(
    useCallback(
      (signal) => fetchBacktestEquity(backtestId, accessToken ?? '', signal),
      [backtestId, accessToken],
    ),
    [backtestId, accessToken],
  );

  if (state.status === 'loading')
    return <Message tone="info" title="Loading equity curve…" />;
  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not load equity curve"
        detail={state.error.message}
      />
    );
  }
  if (state.data.equity_curve.length === 0) {
    return <Message tone="info" title="No equity points recorded" />;
  }

  const points: EquityChartPoint[] = state.data.equity_curve.map((point) => ({
    label: formatTimestamp(point.as_of, 'date'),
    equity: Number(point.total_equity),
    equityText: point.total_equity,
  }));
  const values = points.map((point) => point.equity);
  const low = Math.min(...values);
  const high = Math.max(...values);
  const padding = Math.max((high - low) * 0.08, high * 0.001);

  return (
    <figure className="chart" aria-label="Equity curve">
      <div className="chart-scroll">
        <LineChart
          data={points}
          width={760}
          height={280}
          margin={{ top: 8, right: 16, bottom: 8, left: 8 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
          <XAxis dataKey="label" minTickGap={48} tick={{ fontSize: 12 }} />
          <YAxis
            domain={[low - padding, high + padding]}
            tickFormatter={(value: number) => value.toFixed(0)}
            width={72}
            tick={{ fontSize: 12 }}
          />
          <Tooltip content={EquityTooltip} />
          <Line
            type="monotone"
            dataKey="equity"
            stroke="var(--color-accent)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
            name="Equity"
          />
        </LineChart>
      </div>
      <figcaption>{points.length} equity points</figcaption>
    </figure>
  );
}

function TradeList({ backtestId }: { backtestId: string }) {
  const { accessToken } = useAuth();
  const { state } = useAsync<BacktestTradeList>(
    useCallback(
      (signal) => fetchBacktestTrades(backtestId, accessToken ?? '', signal),
      [backtestId, accessToken],
    ),
    [backtestId, accessToken],
  );

  if (state.status === 'loading') return <Message tone="info" title="Loading trades…" />;
  if (state.status === 'error') {
    return (
      <Message tone="error" title="Could not load trades" detail={state.error.message} />
    );
  }
  if (state.data.trades.length === 0) {
    return <Message tone="info" title="No trades were made" />;
  }

  return (
    <table aria-label="Trades">
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Side</th>
          <th>Quantity</th>
          <th>Entry</th>
          <th>Exit</th>
          <th>P&amp;L</th>
          <th>Return</th>
        </tr>
      </thead>
      <tbody>
        {state.data.trades.map((trade) => {
          const pnl = trade.pnl === null ? null : Number(trade.pnl);
          return (
            <tr key={trade.id}>
              <td>{trade.symbol}</td>
              <td>{trade.side}</td>
              <td>{formatMoney(trade.quantity)}</td>
              <td>
                {formatMoney(trade.entry_price)} @ {formatTimestamp(trade.entry_at)}
              </td>
              <td>
                {trade.exit_price === null || trade.exit_at === null
                  ? 'still open'
                  : `${formatMoney(trade.exit_price)} @ ${formatTimestamp(trade.exit_at)}`}
              </td>
              <td
                className={
                  pnl === null
                    ? undefined
                    : pnl >= 0
                      ? 'figure-positive'
                      : 'figure-negative'
                }
              >
                {pnl === null ? '—' : formatMoney(trade.pnl ?? '0')}
              </td>
              <td>{trade.return_pct === null ? '—' : formatPercent(trade.return_pct)}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function optionalPct(value: string | null): string {
  return value === null ? '—' : formatPercent(value);
}

function MetricsPanel({ metrics }: { metrics: BacktestMetrics }) {
  return (
    <dl className="quote" aria-label="Backtest metrics">
      <div>
        <dt>Total return</dt>
        <dd>{optionalPct(metrics.total_return_pct)}</dd>
      </div>
      <div>
        <dt>CAGR</dt>
        <dd>{optionalPct(metrics.cagr_pct)}</dd>
      </div>
      <div>
        <dt>Max drawdown</dt>
        <dd>{optionalPct(metrics.max_drawdown_pct)}</dd>
      </div>
      <div>
        <dt>Sharpe ratio</dt>
        <dd>{metrics.sharpe_ratio === null ? '—' : formatMoney(metrics.sharpe_ratio)}</dd>
      </div>
      <div>
        <dt>Volatility</dt>
        <dd>{optionalPct(metrics.volatility_pct)}</dd>
      </div>
      <div>
        <dt>Win rate</dt>
        <dd>{optionalPct(metrics.win_rate_pct)}</dd>
      </div>
      <div>
        <dt>Trades</dt>
        <dd>{metrics.trade_count}</dd>
      </div>
      <div>
        <dt>Exposure</dt>
        <dd>{optionalPct(metrics.exposure_pct)}</dd>
      </div>
    </dl>
  );
}

/** Route-level wrapper for `/backtests/:id`, mounted inside `main.app` by `App.tsx`. */
export function BacktestResultsPage() {
  const { id } = useParams<{ id: string }>();
  if (!id) return null;
  return (
    <main className="page">
      <BacktestResults backtestId={id} />
    </main>
  );
}

export function BacktestResults({ backtestId }: { backtestId: string }) {
  const { accessToken } = useAuth();
  const { state, reload } = useAsync<BacktestRun>(
    useCallback(
      (signal) => fetchBacktest(backtestId, accessToken ?? '', signal),
      [backtestId, accessToken],
    ),
    [backtestId, accessToken],
  );

  const status = state.status === 'success' ? state.data.status : undefined;
  const isPolling = status !== undefined && !TERMINAL_STATUSES.includes(status);

  useEffect(() => {
    if (!isPolling) return;
    const timer = setInterval(reload, POLL_INTERVAL_MS);
    return () => {
      clearInterval(timer);
    };
    // `reload` changes identity every render (`useAsync`); only `isPolling` should restart
    // the interval, or every poll tick would immediately be replaced by a new one.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isPolling]);

  if (state.status === 'loading') {
    return <Message tone="info" title="Loading backtest…" />;
  }
  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not load backtest"
        detail={state.error.message}
        action={{ label: 'Try again', onClick: reload }}
      />
    );
  }

  const run = state.data;

  return (
    <section aria-labelledby="backtest-heading">
      <h2 id="backtest-heading">
        Backtest {run.symbols.join(', ')} · {run.start_date} → {run.end_date}
      </h2>
      <p role="status">Status: {run.status}</p>

      {run.status === 'failed' && (
        <Message
          tone="error"
          title="Backtest failed"
          detail={run.error_message ?? 'No further detail was recorded.'}
        />
      )}

      {(run.status === 'queued' || run.status === 'running') && (
        <Message
          tone="info"
          title={run.status === 'queued' ? 'Waiting to start…' : 'Running…'}
          detail="This page updates automatically."
        />
      )}

      {run.status === 'completed' && run.metrics && (
        <>
          <MetricsPanel metrics={run.metrics} />
          <EquityChart backtestId={run.id} />
          <TradeList backtestId={run.id} />
        </>
      )}
    </section>
  );
}
