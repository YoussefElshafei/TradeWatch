/** `PortfoliosPage`: the four-states pattern, plus creating a portfolio. */

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { AuthProvider } from '../hooks/useAuth';
import { TOKEN_RESPONSE } from '../test-support/authFixtures';
import { requestUrl } from '../test-support/requestUrl';

import { PortfoliosPage } from './PortfoliosPage';

const PORTFOLIOS = {
  count: 1,
  portfolios: [
    {
      id: 'portfolio-1',
      user_id: 'user-1',
      name: 'Main',
      base_currency: 'USD',
      initial_cash: '10000.00000000',
      cash_balance: '9500.50000000',
      created_at: '2026-01-01T00:00:00Z',
      updated_at: '2026-01-01T00:00:00Z',
    },
  ],
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/portfolios']}>
      <AuthProvider>
        <PortfoliosPage />
      </AuthProvider>
    </MemoryRouter>,
  );
}

function stubFetch(routes: {
  portfolios?: unknown;
  status?: number;
  onCreate?: () => Response;
}) {
  const { portfolios = PORTFOLIOS, status = 200, onCreate } = routes;
  const calls: { url: string; method: string; body?: unknown }[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = requestUrl(input);
      calls.push({
        url,
        method: init?.method ?? 'GET',
        body: init?.body ? JSON.parse(init.body as string) : undefined,
      });
      if (url.includes('/auth/refresh')) {
        return Promise.resolve(new Response(JSON.stringify(TOKEN_RESPONSE)));
      }
      if (url.includes('/portfolios') && init?.method === 'POST') {
        return Promise.resolve((onCreate ?? (() => new Response('{}', { status: 201 })))());
      }
      if (url.includes('/portfolios')) {
        return Promise.resolve(new Response(JSON.stringify(portfolios), { status }));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
  return calls;
}

describe('PortfoliosPage', () => {
  it('shows a loading state before anything arrives', () => {
    stubFetch({});
    renderPage();
    expect(screen.getByText(/loading portfolios/i)).toBeInTheDocument();
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

  it('shows an empty state when there are no portfolios', async () => {
    stubFetch({ portfolios: { count: 0, portfolios: [] } });
    renderPage();
    expect(await screen.findByText(/no portfolios yet/i)).toBeInTheDocument();
  });

  it('lists portfolios with a link to each detail page', async () => {
    stubFetch({});
    renderPage();

    expect(await screen.findByText('Main')).toBeInTheDocument();
    expect(screen.getByText(/9500.5/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /view/i })).toHaveAttribute(
      'href',
      '/portfolios/portfolio-1',
    );
  });

  it('creates a portfolio on submit', async () => {
    const calls = stubFetch({ portfolios: { count: 0, portfolios: [] } });
    const user = userEvent.setup();
    renderPage();

    await screen.findByText(/no portfolios yet/i);

    await user.type(screen.getByLabelText(/name/i), 'New fund');
    await user.type(screen.getByLabelText(/initial cash/i), '5000');
    await user.click(screen.getByRole('button', { name: /create portfolio/i }));

    await waitFor(() => {
      const created = calls.find(
        (call) => call.url.includes('/portfolios') && call.method === 'POST',
      );
      expect(created?.body).toEqual({
        name: 'New fund',
        base_currency: 'USD',
        initial_cash: '5000',
      });
    });
  });
});
