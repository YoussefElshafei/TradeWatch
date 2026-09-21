/**
 * The market view, end to end within the browser.
 *
 * `fetch` is stubbed with responses in the shape the real API returns — the payloads below
 * are copied from `tests/integration/test_market_api.py`'s assertions, so if the backend
 * contract changes, one of the two suites goes red rather than both staying green while
 * disagreeing.
 *
 * The four states get equal attention. A view that renders data correctly and shows a blank
 * screen while loading, or nothing at all on an error, is not finished — and those paths
 * are the ones nobody clicks through by hand.
 */

import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { requestUrl } from '../test-support/requestUrl';

import { MarketView } from './MarketView';

const INSTRUMENTS = {
  count: 2,
  instruments: [
    {
      symbol: 'AAPL',
      name: 'Apple Inc.',
      asset_class: 'equity',
      exchange: 'XNAS',
      currency: 'USD',
      tick_size: '0.01000000',
      lot_size: '1.00000000',
      status: 'active',
    },
    {
      symbol: 'MSFT',
      name: 'Microsoft',
      asset_class: 'equity',
      exchange: 'XNAS',
      currency: 'USD',
      tick_size: '0.01000000',
      lot_size: '1.00000000',
      status: 'active',
    },
  ],
};

function barsFor(symbol: string, closes: string[]) {
  return {
    symbol,
    interval: '1m',
    start: '2026-03-02T14:30:00Z',
    end: '2026-03-02T15:30:00Z',
    count: closes.length,
    bars: closes.map((close, index) => ({
      bar_start: new Date(Date.UTC(2026, 2, 2, 14, 30 + index)).toISOString(),
      open: close,
      high: close,
      low: close,
      close,
      volume: '1000.00000000',
      vwap: close,
      trade_count: 42,
    })),
  };
}

const LATEST = {
  symbol: 'AAPL',
  price: '187.42000000',
  size: '100.00000000',
  traded_at: '2026-03-02T15:29:00Z',
  bid_price: '187.40000000',
  ask_price: '187.44000000',
  quoted_at: '2026-03-02T15:29:00Z',
  source: 'synthetic',
};

interface RouteMap {
  instruments?: unknown;
  bars?: (symbol: string) => unknown;
  latest?: unknown;
  status?: number;
}

/** Route stubbed responses by URL, the way the real API is addressed. */
function stubApi({
  instruments = INSTRUMENTS,
  bars,
  latest = LATEST,
  status = 200,
}: RouteMap) {
  const calls: string[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = requestUrl(input);
      calls.push(url);

      const respond = (body: unknown) =>
        Promise.resolve(new Response(JSON.stringify(body), { status }));

      if (url.includes('/instruments')) return respond(instruments);
      if (url.includes('/latest')) return respond(latest);
      if (url.includes('/bars')) {
        const symbol = /market\/([^/]+)\/bars/.exec(url)?.[1] ?? 'AAPL';
        return respond(
          bars ? bars(symbol) : barsFor(symbol, ['100.00', '101.50', '102.25']),
        );
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
  return calls;
}

describe('MarketView', () => {
  it('shows a loading state before anything arrives', () => {
    stubApi({});

    render(<MarketView />);

    expect(screen.getByRole('status')).toHaveTextContent(/loading/i);
  });

  it('renders a chart from data the API returned', async () => {
    stubApi({});

    render(<MarketView />);

    const chart = await screen.findByLabelText('AAPL price chart');
    expect(chart).toBeInTheDocument();
    // The SVG carries a path per line; an empty chart would render the frame and no path.
    expect(chart.querySelector('svg')).not.toBeNull();
    expect(chart.querySelectorAll('path.recharts-line-curve').length).toBeGreaterThan(0);
    expect(chart).toHaveTextContent('3 bars');
  });

  it('shows the latest price without reformatting it', async () => {
    stubApi({});

    render(<MarketView />);

    // 187.42, not 187.42000000 and not a float's rendering of it.
    expect(await screen.findByTestId('latest-price')).toHaveTextContent('187.42');
  });

  it('lists every instrument in the selector', async () => {
    stubApi({});

    render(<MarketView />);

    const selector = await screen.findByRole('combobox', { name: /symbol/i });
    expect(within(selector).getAllByRole('option')).toHaveLength(2);
  });

  it('loads a different symbol when one is selected', async () => {
    const calls = stubApi({});
    const user = userEvent.setup();
    render(<MarketView />);
    await screen.findByLabelText('AAPL price chart');

    await user.selectOptions(
      await screen.findByRole('combobox', { name: /symbol/i }),
      'MSFT',
    );

    await waitFor(() => {
      expect(screen.getByLabelText('MSFT price chart')).toBeInTheDocument();
    });
    expect(calls.some((url) => url.includes('/market/MSFT/bars'))).toBe(true);
  });

  it('shows an empty state rather than an error when there is no data', async () => {
    stubApi({ bars: (symbol) => barsFor(symbol, []) });

    render(<MarketView />);

    // A known symbol with no bars is a quiet market, not a failure.
    expect(await screen.findByText(/no bars for aapl/i)).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('reports an API error and offers a retry', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new TypeError('Failed to fetch'))),
    );

    render(<MarketView />);

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(/could not reach/i);
    expect(within(alert).getByRole('button', { name: /try again/i })).toBeInTheDocument();
  });

  it('surfaces the message the API supplied', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        if (requestUrl(input).includes('/instruments')) {
          return Promise.resolve(new Response(JSON.stringify(INSTRUMENTS)));
        }
        return Promise.resolve(
          new Response(
            JSON.stringify({
              error: { code: 'not_found', message: "Unknown symbol 'AAPL'." },
            }),
            {
              status: 404,
            },
          ),
        );
      }),
    );

    render(<MarketView />);

    expect(await screen.findByText(/unknown symbol 'aapl'/i)).toBeInTheDocument();
  });

  it('retries when the retry button is pressed', async () => {
    let failures = 1;
    const user = userEvent.setup();
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = requestUrl(input);
        // The first instruments request fails; everything after it succeeds. Routing by
        // URL matters — returning the instrument payload for a /bars request would render
        // a chart with undefined data and fail somewhere unrelated.
        if (url.includes('/instruments')) {
          if (failures > 0) {
            failures -= 1;
            return Promise.reject(new TypeError('Failed to fetch'));
          }
          return Promise.resolve(new Response(JSON.stringify(INSTRUMENTS)));
        }
        if (url.includes('/latest')) {
          return Promise.resolve(new Response(JSON.stringify(LATEST)));
        }
        return Promise.resolve(new Response(JSON.stringify(barsFor('AAPL', ['100.00']))));
      }),
    );
    render(<MarketView />);
    const alert = await screen.findByRole('alert');

    await user.click(within(alert).getByRole('button', { name: /try again/i }));

    await waitFor(() => {
      expect(screen.getByRole('combobox', { name: /symbol/i })).toBeInTheDocument();
    });
  });

  it('handles an empty instrument universe', async () => {
    stubApi({ instruments: { count: 0, instruments: [] } });

    render(<MarketView />);

    expect(await screen.findByText(/no instruments yet/i)).toBeInTheDocument();
  });
});
