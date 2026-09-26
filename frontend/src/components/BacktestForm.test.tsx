/** `BacktestForm`: strategy loading states, preselection from the URL, and submit. */

import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { BacktestForm } from './BacktestForm';

const STRATEGIES = {
  count: 1,
  strategies: [
    {
      id: 'strategy-1',
      user_id: 'user-1',
      name: 'My SMA cross',
      strategy_type: 'sma_crossover',
      parameters: { fast_period: 10, slow_period: 30 },
      symbols: ['AAPL'],
      is_archived: false,
      created_at: '2026-01-01T00:00:00Z',
      updated_at: '2026-01-01T00:00:00Z',
    },
  ],
};

function renderForm(initialEntry: string, onSubmit?: (body: unknown) => Response) {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = requestUrl(input);
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/backtests') && init?.method === 'POST') {
        return Promise.resolve(
          (onSubmit ?? (() => new Response('{}')))(JSON.parse(init.body as string)),
        );
      }
      if (url.includes('/strategies')) {
        return Promise.resolve(new Response(JSON.stringify(STRATEGIES)));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );

  return render(
    <MemoryRouter initialEntries={[initialEntry]}>
      <AuthProvider>
        <Routes>
          <Route path="/backtests/new" element={<BacktestForm />} />
          <Route path="/backtests/:id" element={<p>backtest results</p>} />
        </Routes>
      </AuthProvider>
    </MemoryRouter>,
  );
}

describe('BacktestForm', () => {
  it('shows a loading state before the strategy list arrives', () => {
    renderForm('/backtests/new');
    expect(screen.getByText(/loading strategies/i)).toBeInTheDocument();
  });

  it('preselects the strategy named in the URL', async () => {
    renderForm('/backtests/new?strategyId=strategy-1');

    const select = await screen.findByLabelText(/^strategy$/i);
    expect(select).toHaveValue('strategy-1');
  });

  it('shows an empty state with no strategies to backtest', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = requestUrl(input);
        if (url.includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        return Promise.resolve(new Response(JSON.stringify({ count: 0, strategies: [] })));
      }),
    );
    render(
      <MemoryRouter initialEntries={['/backtests/new']}>
        <AuthProvider>
          <BacktestForm />
        </AuthProvider>
      </MemoryRouter>,
    );

    expect(await screen.findByText(/no strategies yet/i)).toBeInTheDocument();
  });

  it('rejects an end date before the start date, without a request', async () => {
    const submit = vi.fn(() => new Response('{}'));
    renderForm('/backtests/new?strategyId=strategy-1', submit);
    const user = userEvent.setup();
    await screen.findByLabelText(/^strategy$/i);

    await user.type(screen.getByLabelText(/start date/i), '2026-02-01');
    await user.type(screen.getByLabelText(/end date/i), '2026-01-01');
    await user.click(screen.getByRole('button', { name: /run backtest/i }));

    expect(await screen.findByRole('alert')).toHaveTextContent(
      /end date must be on or after/i,
    );
    expect(submit).not.toHaveBeenCalled();
  });

  it('submits and moves to the results page', async () => {
    renderForm(
      '/backtests/new?strategyId=strategy-1',
      () =>
        new Response(
          JSON.stringify({
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
            status: 'queued',
            error_message: null,
            metrics: null,
            started_at: null,
            completed_at: null,
            created_at: '2026-01-01T00:00:00Z',
          }),
          { status: 201 },
        ),
    );
    const user = userEvent.setup();
    await screen.findByLabelText(/^strategy$/i);

    await user.type(screen.getByLabelText(/start date/i), '2026-01-01');
    await user.type(screen.getByLabelText(/end date/i), '2026-02-01');
    await user.click(screen.getByRole('button', { name: /run backtest/i }));

    expect(await screen.findByText('backtest results')).toBeInTheDocument();
  });
});
