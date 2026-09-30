/** `AnomalyFeed`: the feed table, symbol-filtered score history, and the explanation panel. */

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';

import { requestUrl } from '../test-support/requestUrl';

import { AnomalyFeed } from './AnomalyFeed';

const INSTRUMENTS = {
  count: 1,
  instruments: [
    {
      symbol: 'AAPL',
      name: 'Apple Inc.',
      asset_class: 'equity',
      exchange: 'NASDAQ',
      currency: 'USD',
      tick_size: '0.01',
      lot_size: '1',
      status: 'active',
    },
  ],
};

const DETECTIONS = {
  count: 1,
  detections: [
    {
      id: 'detection-1',
      symbol: 'AAPL',
      detected_at: '2026-01-01T00:00:00Z',
      window_start: '2026-01-01T00:00:00Z',
      window_end: '2026-01-01T00:01:00Z',
      anomaly_score: '6.2',
      threshold: '5.0',
      severity: 'warning',
      model_name: 'baseline-zscore',
      model_version: 'w=60',
      features: { return: '0.05', volume_z: '4.1' },
    },
  ],
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/anomalies']}>
      <AnomalyFeed />
    </MemoryRouter>,
  );
}

function stubFetch(overrides: { detections?: unknown } = {}) {
  const { detections = DETECTIONS } = overrides;
  const calls: string[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
      const url = requestUrl(input);
      calls.push(url);
      if (url.includes('/instruments')) {
        return Promise.resolve(new Response(JSON.stringify(INSTRUMENTS)));
      }
      if (url.includes('/anomalies')) {
        return Promise.resolve(new Response(JSON.stringify(detections)));
      }
      return Promise.resolve(new Response('{}', { status: 404 }));
    }),
  );
  return calls;
}

describe('AnomalyFeed', () => {
  it('shows a loading state before anything arrives', () => {
    stubFetch();
    renderPage();
    expect(screen.getByText(/loading detections/i)).toBeInTheDocument();
  });

  it('shows an empty state when there are no detections', async () => {
    stubFetch({ detections: { count: 0, detections: [] } });
    renderPage();
    expect(await screen.findByText(/no detections/i)).toBeInTheDocument();
  });

  it('lists a detection with its score, threshold, and model', async () => {
    stubFetch();
    renderPage();

    expect(await screen.findByRole('cell', { name: 'AAPL' })).toBeInTheDocument();
    expect(screen.getByText('6.2')).toBeInTheDocument();
    expect(screen.getByText('5.0')).toBeInTheDocument();
    expect(screen.getByText(/baseline-zscore/)).toBeInTheDocument();
  });

  it('shows the explanation panel with features on request', async () => {
    stubFetch();
    const user = userEvent.setup();
    renderPage();

    await user.click(await screen.findByRole('button', { name: /explain/i }));

    expect(await screen.findByText(/why was this flagged/i)).toBeInTheDocument();
    expect(screen.getByText('return')).toBeInTheDocument();
    expect(screen.getByText('0.05')).toBeInTheDocument();
    expect(screen.getByText(/not a claim of market manipulation/i)).toBeInTheDocument();
  });

  it('refetches filtered by symbol when one is selected', async () => {
    const calls = stubFetch();
    const user = userEvent.setup();
    renderPage();

    await screen.findByRole('cell', { name: 'AAPL' });
    await user.selectOptions(screen.getByLabelText(/symbol/i), 'AAPL');

    await waitFor(() => {
      expect(calls.some((url) => url.includes('symbol=AAPL'))).toBe(true);
    });
    expect(screen.getByText(/AAPL score history/i)).toBeInTheDocument();
  });
});
