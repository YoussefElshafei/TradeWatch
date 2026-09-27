/**
 * `PortfolioDetailPage`: positions, deployment status/actions, and deploying a new strategy.
 */

import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { PortfolioDetailPage } from './PortfolioDetailPage';

const PORTFOLIO = {
  id: 'portfolio-1',
  user_id: 'user-1',
  name: 'Main',
  base_currency: 'USD',
  initial_cash: '10000.00000000',
  cash_balance: '9500.50000000',
  created_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
  total_equity: '10200.00000000',
  positions: [
    {
      instrument_id: 'inst-1',
      symbol: 'AAPL',
      quantity: '10',
      avg_entry_price: '150.00000000',
      realized_pnl: '25.00000000',
      updated_at: '2026-01-01T00:00:00Z',
    },
  ],
};

const STRATEGIES = {
  count: 1,
  strategies: [
    {
      id: 'strategy-1',
      user_id: 'user-1',
      name: 'My SMA cross',
      strategy_type: 'sma_crossover',
      parameters: {},
      symbols: ['AAPL'],
      is_archived: false,
      created_at: '2026-01-01T00:00:00Z',
      updated_at: '2026-01-01T00:00:00Z',
    },
  ],
};

function deployment(status: string) {
  return {
    id: 'deployment-1',
    strategy_id: 'strategy-1',
    portfolio_id: 'portfolio-1',
    status,
    position_sizing: { target_equity_pct: '10' },
    risk_limits: {
      max_position_pct: '20',
      max_gross_exposure_pct: '100',
      max_orders_per_minute: 10,
    },
    execution_config: {},
    started_at: null,
    stopped_at: null,
    error_message: null,
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
  };
}

function renderPage(deployments: unknown[]) {
  const calls: { url: string; method: string }[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = requestUrl(input);
      const method = init?.method ?? 'GET';
      calls.push({ url, method });
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/deployments') && method === 'POST') {
        return Promise.resolve(
          new Response(JSON.stringify(deployment('created')), { status: 201 }),
        );
      }
      if (/\/deployments\/[^/?]+\/(start|pause|resume|stop)$/.test(url)) {
        return Promise.resolve(
          new Response(JSON.stringify(deployment('running')), { status: 200 }),
        );
      }
      if (url.includes('/deployments')) {
        return Promise.resolve(
          new Response(JSON.stringify({ count: deployments.length, deployments }), {
            status: 200,
          }),
        );
      }
      if (url.includes('/strategies')) {
        return Promise.resolve(new Response(JSON.stringify(STRATEGIES)));
      }
      if (url.includes('/portfolios/portfolio-1')) {
        return Promise.resolve(new Response(JSON.stringify(PORTFOLIO)));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );

  render(
    <MemoryRouter initialEntries={['/portfolios/portfolio-1']}>
      <AuthProvider>
        <Routes>
          <Route path="/portfolios/:id" element={<PortfolioDetailPage />} />
        </Routes>
      </AuthProvider>
    </MemoryRouter>,
  );

  return calls;
}

describe('PortfolioDetailPage', () => {
  it('shows cash, total equity, and positions', async () => {
    renderPage([]);

    expect(await screen.findByText('Main')).toBeInTheDocument();
    expect(screen.getByText(/9500.5 USD/)).toBeInTheDocument();
    expect(screen.getByText(/10200 USD/)).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('25')).toBeInTheDocument();
  });

  it('shows a Start button for a created deployment and starts it', async () => {
    const calls = renderPage([deployment('created')]);
    const user = userEvent.setup();

    const startButton = await screen.findByRole('button', { name: /^start$/i });
    await user.click(startButton);

    await waitFor(() => {
      expect(
        calls.some((call) => call.url.includes('/start') && call.method === 'POST'),
      ).toBe(true);
    });
  });

  it('shows Pause and Stop for a running deployment', async () => {
    renderPage([deployment('running')]);

    expect(await screen.findByRole('button', { name: /^pause$/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^start$/i })).not.toBeInTheDocument();
  });

  it('shows Resume and Stop for a paused deployment', async () => {
    renderPage([deployment('paused')]);

    expect(await screen.findByRole('button', { name: /^resume$/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument();
  });

  it('shows no action buttons for a stopped deployment', async () => {
    renderPage([deployment('stopped')]);

    expect(await screen.findByText('stopped')).toBeInTheDocument();
    const deploymentsTable = screen.getByRole('table', { name: /deployments/i });
    expect(within(deploymentsTable).queryByRole('button')).not.toBeInTheDocument();
  });

  it('deploys a new strategy from the dropdown', async () => {
    const calls = renderPage([]);
    const user = userEvent.setup();

    await screen.findByText('Main');
    await user.selectOptions(await screen.findByLabelText(/strategy/i), 'strategy-1');
    await user.click(screen.getByRole('button', { name: /deploy strategy/i }));

    await waitFor(() => {
      expect(
        calls.some((call) => call.url.endsWith('/deployments') && call.method === 'POST'),
      ).toBe(true);
    });
  });
});
