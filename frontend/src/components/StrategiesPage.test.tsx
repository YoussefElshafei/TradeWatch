/** `StrategiesPage`: the four-states pattern, plus archiving a strategy. */

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { StrategiesPage } from './StrategiesPage';

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

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/strategies']}>
      <AuthProvider>
        <StrategiesPage />
      </AuthProvider>
    </MemoryRouter>,
  );
}

function stubFetch(routes: { strategies?: unknown; status?: number }) {
  const { strategies = STRATEGIES, status = 200 } = routes;
  const calls: { url: string; method: string }[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = requestUrl(input);
      calls.push({ url, method: init?.method ?? 'GET' });
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/strategies') && init?.method === 'DELETE') {
        return Promise.resolve(new Response(null, { status: 204 }));
      }
      if (url.includes('/strategies')) {
        return Promise.resolve(new Response(JSON.stringify(strategies), { status }));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
  return calls;
}

describe('StrategiesPage', () => {
  it('shows a loading state before anything arrives', () => {
    stubFetch({});
    renderPage();
    expect(screen.getByText(/loading strategies/i)).toBeInTheDocument();
  });

  it('reports an API error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        if (requestUrl(input).includes('/auth/refresh')) {
          return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
        }
        return Promise.resolve(
          new Response(JSON.stringify({ error: { message: 'boom' } }), { status: 500 }),
        );
      }),
    );

    renderPage();

    expect(await screen.findByText('boom')).toBeInTheDocument();
  });

  it('shows an empty state when there are no strategies', async () => {
    stubFetch({ strategies: { count: 0, strategies: [] } });
    renderPage();
    expect(await screen.findByText(/no strategies yet/i)).toBeInTheDocument();
  });

  it('lists strategies and archives one on request', async () => {
    const calls = stubFetch({});
    const user = userEvent.setup();
    renderPage();

    expect(await screen.findByText('My SMA cross')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /archive/i }));

    await waitFor(() => {
      expect(calls.some((call) => call.method === 'DELETE')).toBe(true);
    });
  });

  it('links to a new backtest for each strategy', async () => {
    stubFetch({});
    renderPage();

    const link = await screen.findByRole('link', { name: /backtest/i });
    expect(link).toHaveAttribute('href', expect.stringContaining('strategyId=strategy-1'));
  });

  it('has a link to create a new strategy', async () => {
    stubFetch({ strategies: { count: 0, strategies: [] } });
    renderPage();

    const link = await screen.findByRole('link', { name: /new strategy/i });
    expect(link).toHaveAttribute('href', '/strategies/new');
  });
});
