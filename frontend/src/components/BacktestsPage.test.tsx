/**
 * `BacktestsPage`: the four-states pattern, same as `StrategiesPage.test.tsx`.
 *
 * The fixture's top-level key is `backtests`, matching `BacktestListResponse` in
 * `api/schemas/backtests.py` — not `runs`. No UI called `fetchBacktests` before this page
 * existed, so a frontend type declaring the wrong field name had never been exercised
 * against the real API; this pins the contract so that regresses loudly instead of quietly.
 */

import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { BacktestsPage } from './BacktestsPage';

const BACKTESTS = {
  count: 1,
  backtests: [
    {
      id: 'backtest-1',
      user_id: 'user-1',
      strategy_id: 'strategy-1',
      symbols: ['AAPL'],
      start_date: '2026-01-01',
      end_date: '2026-02-01',
      initial_cash: '10000.00',
      execution_config: {},
      status: 'completed',
      error_message: null,
      metrics: {
        total_return_pct: '12.50',
        cagr_pct: '18.00',
        max_drawdown_pct: '-4.20',
        sharpe_ratio: '1.80',
        volatility_pct: '9.00',
        win_rate_pct: '60.00',
        trade_count: 10,
        exposure_pct: '70.00',
      },
      started_at: '2026-01-01T00:00:00Z',
      completed_at: '2026-01-01T00:05:00Z',
      created_at: '2026-01-01T00:00:00Z',
    },
  ],
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/backtests']}>
      <AuthProvider>
        <BacktestsPage />
      </AuthProvider>
    </MemoryRouter>,
  );
}

function stubFetch(backtests: unknown = BACKTESTS, status = 200) {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = requestUrl(input);
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/backtests')) {
        return Promise.resolve(new Response(JSON.stringify(backtests), { status }));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
}

describe('BacktestsPage', () => {
  it('shows a loading state before anything arrives', () => {
    stubFetch();
    renderPage();
    expect(screen.getByText(/loading backtests/i)).toBeInTheDocument();
  });

  it('shows an empty state when there are none', async () => {
    stubFetch({ count: 0, backtests: [] });
    renderPage();
    expect(await screen.findByText(/no backtests yet/i)).toBeInTheDocument();
  });

  it('lists a completed run with its symbols and return', async () => {
    stubFetch();
    renderPage();

    expect(await screen.findByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('completed')).toBeInTheDocument();
    // `formatPercent` trims trailing zeros the same way `formatMoney` does (ADR-0009).
    expect(screen.getByText('12.5%')).toBeInTheDocument();
  });

  it('has a link to start a new backtest', async () => {
    stubFetch({ count: 0, backtests: [] });
    renderPage();

    const link = await screen.findByRole('link', { name: /new backtest/i });
    expect(link).toHaveAttribute('href', '/backtests/new');
  });
});
