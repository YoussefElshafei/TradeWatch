/**
 * `BacktestResults`: loading/error, the non-terminal states, polling until a terminal
 * status, and the completed view (metrics, equity chart, trades).
 */

import { act, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { BacktestResultsPage } from './BacktestResults';

const RUN_BASE = {
  id: 'run-1',
  user_id: 'user-1',
  strategy_id: 'strategy-1',
  symbols: ['AAPL'],
  start_date: '2026-01-01',
  end_date: '2026-02-01',
  initial_cash: '100000.00000000',
  execution_config: {
    slippage_bps: '2',
    commission_per_share: '0',
    commission_pct: '0',
    fill_model: 'next_bar_open',
    allow_partial_fills: false,
  },
  started_at: null,
  completed_at: null,
  created_at: '2026-01-01T00:00:00Z',
};

const METRICS = {
  total_return_pct: '12.50000000',
  cagr_pct: '10.00000000',
  max_drawdown_pct: '-5.00000000',
  sharpe_ratio: '1.20000000',
  volatility_pct: '15.00000000',
  win_rate_pct: '60.00000000',
  trade_count: 2,
  exposure_pct: '80.00000000',
};

const TRADES = {
  count: 1,
  trades: [
    {
      id: 'trade-1',
      symbol: 'AAPL',
      side: 'buy',
      quantity: '10.00000000',
      entry_price: '100.00000000',
      entry_at: '2026-01-05T00:00:00Z',
      exit_price: '110.00000000',
      exit_at: '2026-01-10T00:00:00Z',
      pnl: '100.00000000',
      return_pct: '10.00000000',
    },
  ],
};

const EQUITY = {
  count: 2,
  points: [
    { as_of: '2026-01-01T00:00:00Z', total_equity: '100000.00000000', drawdown_pct: null },
    { as_of: '2026-01-02T00:00:00Z', total_equity: '101000.00000000', drawdown_pct: null },
  ],
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/backtests/run-1']}>
      <AuthProvider>
        <Routes>
          <Route path="/backtests/:id" element={<BacktestResultsPage />} />
        </Routes>
      </AuthProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.useRealTimers();
});

describe('BacktestResults', () => {
  it('shows a loading state before anything arrives', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise<Response>(() => undefined)), // never resolves
    );
    renderPage();
    expect(screen.getByText(/loading backtest/i)).toBeInTheDocument();
  });

  it('reports an API error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        if (requestUrl(input).includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        return Promise.resolve(new Response('{}', { status: 500 }));
      }),
    );
    renderPage();

    expect(await screen.findByRole('alert')).toBeInTheDocument();
  });

  it('shows a waiting message for a queued run, not an error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = requestUrl(input);
        if (url.includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        return Promise.resolve(
          new Response(
            JSON.stringify({
              ...RUN_BASE,
              status: 'queued',
              error_message: null,
              metrics: null,
            }),
          ),
        );
      }),
    );
    renderPage();

    expect(await screen.findByText(/waiting to start/i)).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('shows the error message for a failed run', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = requestUrl(input);
        if (url.includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        return Promise.resolve(
          new Response(
            JSON.stringify({
              ...RUN_BASE,
              status: 'failed',
              error_message: 'unknown strategy_type',
              metrics: null,
            }),
          ),
        );
      }),
    );
    renderPage();

    expect(await screen.findByText('unknown strategy_type')).toBeInTheDocument();
  });

  it('renders metrics, equity chart, and trades once completed', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = requestUrl(input);
        if (url.includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        if (url.includes('/trades'))
          return Promise.resolve(new Response(JSON.stringify(TRADES)));
        if (url.includes('/equity'))
          return Promise.resolve(new Response(JSON.stringify(EQUITY)));
        return Promise.resolve(
          new Response(
            JSON.stringify({
              ...RUN_BASE,
              status: 'completed',
              error_message: null,
              metrics: METRICS,
            }),
          ),
        );
      }),
    );
    renderPage();

    expect(await screen.findByLabelText('Backtest metrics')).toHaveTextContent('12.5%');
    expect(await screen.findByLabelText('Equity curve')).toBeInTheDocument();
    expect(await screen.findByText('100')).toBeInTheDocument(); // pnl, trimmed by formatMoney
  });

  it('polls again while the run is still going, and stops once it finishes', async () => {
    vi.useFakeTimers();
    let statusCalls = 0;
    const start = Date.now();
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = requestUrl(input);
        if (url.includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        if (url.includes('/trades'))
          return Promise.resolve(new Response(JSON.stringify(TRADES)));
        if (url.includes('/equity'))
          return Promise.resolve(new Response(JSON.stringify(EQUITY)));
        statusCalls += 1;
        // Status flips to "completed" once virtual time has actually reached the poll
        // interval — not after a fixed call count, because `useAsync` legitimately refetches
        // once more as soon as `AuthProvider`'s own `/auth/refresh` resolves and supplies the
        // real access token (the initial render fires with none). That settling fetch always
        // happens at t≈0, well before the interval below ever gets to advance the clock.
        const status = Date.now() - start < 2000 ? 'running' : 'completed';
        return Promise.resolve(
          new Response(
            JSON.stringify({
              ...RUN_BASE,
              status,
              error_message: null,
              metrics: status === 'completed' ? METRICS : null,
            }),
          ),
        );
      }),
    );

    renderPage();

    // Let the initial fetch (and the auth refresh it depends on) settle.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    // Exact text: "Status: running" also contains /running/i and would make a looser
    // matcher ambiguous between the two panels this view renders.
    expect(screen.getByText('Running…')).toBeInTheDocument();
    const callsBeforePolling = statusCalls;
    expect(callsBeforePolling).toBeGreaterThanOrEqual(1);

    // Advance past the poll interval: a further request lands, and this time completes.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(statusCalls).toBeGreaterThan(callsBeforePolling);
    // Synchronous: `MetricsPanel` renders straight from `run.metrics`, no extra fetch to
    // await — `findBy`'s own real-timer-based polling would hang under faked timers here.
    expect(screen.getByLabelText('Backtest metrics')).toBeInTheDocument();

    // No further polling once the run has reached a terminal status.
    const callsAfterCompletion = statusCalls;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(statusCalls).toBe(callsAfterCompletion);
  });
});
