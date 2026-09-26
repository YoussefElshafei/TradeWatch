/** `StrategyEditor`: type-switching fields, client-side validation, and submit. */

import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { StrategyEditor } from './StrategyEditor';

function renderEditor() {
  return render(
    <MemoryRouter initialEntries={['/strategies/new']}>
      <AuthProvider>
        <Routes>
          <Route path="/strategies/new" element={<StrategyEditor />} />
          <Route path="/backtests/new" element={<p>backtest form</p>} />
        </Routes>
      </AuthProvider>
    </MemoryRouter>,
  );
}

function stubFetch(onCreate: (body: unknown) => Response) {
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = requestUrl(input);
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/strategies') && init?.method === 'POST') {
        return Promise.resolve(onCreate(JSON.parse(init.body as string)));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
}

describe('StrategyEditor', () => {
  it('defaults to the SMA crossover fields', () => {
    stubFetch(() => new Response('{}'));
    renderEditor();

    expect(screen.getByLabelText(/fast period/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/slow period/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/rsi period/i)).not.toBeInTheDocument();
  });

  it('switches fields when a different strategy type is chosen', async () => {
    stubFetch(() => new Response('{}'));
    const user = userEvent.setup();
    renderEditor();

    await user.selectOptions(screen.getByLabelText(/strategy type/i), 'rsi_reversion');

    expect(screen.getByLabelText(/rsi period/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/fast period/i)).not.toBeInTheDocument();
  });

  it('rejects a fast period that is not less than the slow period, without a request', async () => {
    const create = vi.fn(() => new Response('{}'));
    stubFetch(create);
    const user = userEvent.setup();
    renderEditor();

    await user.type(screen.getByLabelText(/^name$/i), 'Bad strategy');
    await user.type(screen.getByLabelText(/^symbols/i), 'AAPL');
    await user.clear(screen.getByLabelText(/fast period/i));
    await user.type(screen.getByLabelText(/fast period/i), '50');
    await user.clear(screen.getByLabelText(/slow period/i));
    await user.type(screen.getByLabelText(/slow period/i), '10');
    await user.click(screen.getByRole('button', { name: /create strategy/i }));

    expect(await screen.findByRole('alert')).toHaveTextContent(/fast period must be less/i);
    expect(create).not.toHaveBeenCalled();
  });

  it('creates a strategy and moves straight into the backtest form', async () => {
    stubFetch(
      () =>
        new Response(
          JSON.stringify({
            id: 'strategy-9',
            user_id: 'user-1',
            name: 'Good strategy',
            strategy_type: 'sma_crossover',
            parameters: { fast_period: 10, slow_period: 30 },
            symbols: ['AAPL'],
            is_archived: false,
            created_at: '2026-01-01T00:00:00Z',
            updated_at: '2026-01-01T00:00:00Z',
          }),
          { status: 201 },
        ),
    );
    const user = userEvent.setup();
    renderEditor();

    await user.type(screen.getByLabelText(/^name$/i), 'Good strategy');
    await user.type(screen.getByLabelText(/^symbols/i), 'aapl, msft');
    await user.click(screen.getByRole('button', { name: /create strategy/i }));

    expect(await screen.findByText('backtest form')).toBeInTheDocument();
  });

  it('surfaces a 422 field error from the server', async () => {
    stubFetch(
      () =>
        new Response(
          JSON.stringify({
            error: {
              message: 'The request did not match the expected schema.',
              details: [
                {
                  location: 'parameters.fast_period',
                  message: 'too small',
                  type: 'value_error',
                },
              ],
            },
          }),
          { status: 422 },
        ),
    );
    const user = userEvent.setup();
    renderEditor();

    await user.type(screen.getByLabelText(/^name$/i), 'Good strategy');
    await user.type(screen.getByLabelText(/^symbols/i), 'AAPL');
    await user.click(screen.getByRole('button', { name: /create strategy/i }));

    expect(await screen.findByRole('alert')).toHaveTextContent(/too small/i);
  });
});
