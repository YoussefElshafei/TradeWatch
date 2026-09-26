/**
 * Submit a backtest (Phase 6, `ARCHITECTURE.md` §4.3).
 *
 * Dates use native `<input type="date">` rather than a picker library — the platform already
 * does this correctly. Execution-config overrides sit behind a native `<details>` element:
 * most runs take the server's defaults (`TW_EXEC_*`), so the advanced section starts closed.
 */

import { useCallback, useState, type SubmitEvent } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';

import {
  ApiError,
  type CreateBacktestInput,
  type FillModel,
  submitBacktest,
} from '../api/backtests';
import { type StrategyList, fetchStrategies } from '../api/strategies';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import { Message } from './MarketView';

const FILL_MODELS: { value: FillModel; label: string }[] = [
  { value: 'next_bar_open', label: 'Next bar open (default)' },
  { value: 'current_close', label: 'Current bar close' },
  { value: 'vwap', label: 'VWAP' },
];

export function BacktestForm() {
  const { accessToken } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const preselectedStrategyId = searchParams.get('strategyId') ?? '';

  const { state: strategiesState } = useAsync<StrategyList>(
    useCallback(
      (signal) => fetchStrategies(accessToken ?? '', false, signal),
      [accessToken],
    ),
    [accessToken],
  );

  const [strategyId, setStrategyId] = useState(preselectedStrategyId);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [initialCash, setInitialCash] = useState('');
  const [symbolsOverride, setSymbolsOverride] = useState('');
  const [slippageBps, setSlippageBps] = useState('');
  const [commissionPerShare, setCommissionPerShare] = useState('');
  const [commissionPct, setCommissionPct] = useState('');
  const [fillModel, setFillModel] = useState<FillModel | ''>('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: SubmitEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);

    if (!accessToken) {
      setError('You must be signed in.');
      return;
    }
    if (!strategyId) {
      setError('Choose a strategy.');
      return;
    }
    if (endDate < startDate) {
      setError('End date must be on or after the start date.');
      return;
    }

    const symbols = symbolsOverride
      .split(',')
      .map((symbol) => symbol.trim().toUpperCase())
      .filter((symbol) => symbol !== '');

    const hasExecutionOverride =
      slippageBps !== '' ||
      commissionPerShare !== '' ||
      commissionPct !== '' ||
      fillModel !== '';

    const input: CreateBacktestInput = {
      strategy_id: strategyId,
      start_date: startDate,
      end_date: endDate,
      ...(symbols.length > 0 && { symbols }),
      ...(initialCash !== '' && { initial_cash: initialCash }),
      ...(hasExecutionOverride && {
        execution_config: {
          ...(slippageBps !== '' && { slippage_bps: slippageBps }),
          ...(commissionPerShare !== '' && { commission_per_share: commissionPerShare }),
          ...(commissionPct !== '' && { commission_pct: commissionPct }),
          ...(fillModel !== '' && { fill_model: fillModel }),
        },
      }),
    };

    setSubmitting(true);
    try {
      const run = await submitBacktest(input, accessToken);
      await navigate(`/backtests/${run.id}`);
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause.message
          : 'Something went wrong. Please try again.',
      );
      setSubmitting(false);
    }
  }

  if (strategiesState.status === 'loading') {
    return <Message tone="info" title="Loading strategies…" />;
  }
  if (strategiesState.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not load strategies"
        detail={strategiesState.error.message}
      />
    );
  }
  if (strategiesState.data.strategies.length === 0) {
    return (
      <Message
        tone="info"
        title="No strategies yet"
        detail="Create a strategy before running a backtest."
      />
    );
  }

  return (
    <main className="app">
      <header>
        <h1>New backtest</h1>
      </header>

      <form className="auth-form" onSubmit={(event) => void handleSubmit(event)}>
        <label htmlFor="backtest-strategy">Strategy</label>
        <select
          id="backtest-strategy"
          required
          value={strategyId}
          onChange={(event) => {
            setStrategyId(event.target.value);
          }}
        >
          <option value="" disabled>
            Choose a strategy
          </option>
          {strategiesState.data.strategies.map((strategy) => (
            <option key={strategy.id} value={strategy.id}>
              {strategy.name} ({strategy.strategy_type})
            </option>
          ))}
        </select>

        <label htmlFor="backtest-start">Start date</label>
        <input
          id="backtest-start"
          type="date"
          required
          value={startDate}
          onChange={(event) => {
            setStartDate(event.target.value);
          }}
        />

        <label htmlFor="backtest-end">End date</label>
        <input
          id="backtest-end"
          type="date"
          required
          value={endDate}
          onChange={(event) => {
            setEndDate(event.target.value);
          }}
        />

        <label htmlFor="backtest-cash">Initial cash (optional)</label>
        <input
          id="backtest-cash"
          type="number"
          min="0"
          step="0.01"
          placeholder="Server default"
          value={initialCash}
          onChange={(event) => {
            setInitialCash(event.target.value);
          }}
        />

        <label htmlFor="backtest-symbols">Symbols override (optional)</label>
        <input
          id="backtest-symbols"
          type="text"
          placeholder="Defaults to the strategy's own symbols"
          value={symbolsOverride}
          onChange={(event) => {
            setSymbolsOverride(event.target.value);
          }}
        />

        <details>
          <summary>Advanced: execution assumptions</summary>

          <label htmlFor="backtest-slippage">Slippage (bps)</label>
          <input
            id="backtest-slippage"
            type="number"
            min="0"
            placeholder="Server default"
            value={slippageBps}
            onChange={(event) => {
              setSlippageBps(event.target.value);
            }}
          />

          <label htmlFor="backtest-commission-per-share">Commission per share</label>
          <input
            id="backtest-commission-per-share"
            type="number"
            min="0"
            step="0.0001"
            placeholder="Server default"
            value={commissionPerShare}
            onChange={(event) => {
              setCommissionPerShare(event.target.value);
            }}
          />

          <label htmlFor="backtest-commission-pct">Commission (%)</label>
          <input
            id="backtest-commission-pct"
            type="number"
            min="0"
            step="0.0001"
            placeholder="Server default"
            value={commissionPct}
            onChange={(event) => {
              setCommissionPct(event.target.value);
            }}
          />

          <label htmlFor="backtest-fill-model">Fill model</label>
          <select
            id="backtest-fill-model"
            value={fillModel}
            onChange={(event) => {
              setFillModel(event.target.value as FillModel | '');
            }}
          >
            <option value="">Server default</option>
            {FILL_MODELS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </details>

        {error && (
          <div className="panel panel-error" role="alert">
            <p className="panel-title">{error}</p>
          </div>
        )}

        <button type="submit" disabled={submitting}>
          {submitting ? 'Submitting…' : 'Run backtest'}
        </button>
      </form>
    </main>
  );
}
