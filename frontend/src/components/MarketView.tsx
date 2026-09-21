/**
 * The market data view.
 *
 * The visible end of the vertical slice: a symbol selector, a price chart, and the latest
 * stored price. Everything it shows travelled the whole pipeline — synthetic provider →
 * normalization → event bus → processor → PostgreSQL → API → here.
 *
 * It is deliberately not the TradeWatch dashboard. No layout system, no theming beyond what
 * the stylesheet already had, no watchlists. The point is to prove the data path, and every
 * extra surface here is a surface to rewrite when the real dashboard is designed.
 *
 * What it does take seriously is the four states a real view has to handle, because getting
 * them wrong is what makes a data UI feel broken:
 *
 * * **loading** — say so, and keep the layout stable so the page does not jump;
 * * **error** — say what failed and offer a retry, rather than rendering nothing;
 * * **empty** — a known symbol with no bars yet is a normal Sunday, not a failure, and it
 *   must not look like one;
 * * **success** — the chart.
 */

import { useCallback, useState } from 'react';

import {
  type BarSeries,
  type Instrument,
  type InstrumentList,
  type LatestPrice,
  fetchBars,
  fetchInstruments,
  fetchLatestPrice,
} from '../api/market';
import { useAsync } from '../hooks/useAsync';
import { formatMoney } from '../lib/format';
import { PriceChart } from './PriceChart';

const BAR_LIMIT = 240;

function Message({
  tone,
  title,
  detail,
  action,
}: {
  tone: 'info' | 'error';
  title: string;
  detail?: string;
  action?: { label: string; onClick: () => void };
}) {
  return (
    <div className={`panel panel-${tone}`} role={tone === 'error' ? 'alert' : 'status'}>
      <p className="panel-title">{title}</p>
      {detail && <p className="panel-detail">{detail}</p>}
      {action && (
        <button type="button" onClick={action.onClick}>
          {action.label}
        </button>
      )}
    </div>
  );
}

function LatestPricePanel({ symbol }: { symbol: string }) {
  const { state } = useAsync<LatestPrice>(
    useCallback((signal) => fetchLatestPrice(symbol, signal), [symbol]),
    [symbol],
  );

  if (state.status !== 'success') return null;
  const latest = state.data;

  return (
    <dl className="quote" aria-label={`${symbol} latest price`}>
      <div>
        <dt>Last</dt>
        <dd data-testid="latest-price">
          {latest.price ? formatMoney(latest.price) : 'no trades yet'}
        </dd>
      </div>
      <div>
        <dt>Bid / Ask</dt>
        <dd>
          {latest.bid_price && latest.ask_price
            ? `${formatMoney(latest.bid_price)} / ${formatMoney(latest.ask_price)}`
            : 'no quotes yet'}
        </dd>
      </div>
      <div>
        <dt>Source</dt>
        <dd>{latest.source ?? 'unknown'}</dd>
      </div>
    </dl>
  );
}

function SymbolSelector({
  instruments,
  selected,
  onSelect,
}: {
  instruments: Instrument[];
  selected: string;
  onSelect: (symbol: string) => void;
}) {
  return (
    <label className="symbol-selector">
      <span>Symbol</span>
      <select
        value={selected}
        onChange={(event) => {
          onSelect(event.target.value);
        }}
      >
        {instruments.map((instrument) => (
          <option key={instrument.symbol} value={instrument.symbol}>
            {instrument.symbol}
            {instrument.name ? ` — ${instrument.name}` : ''}
          </option>
        ))}
      </select>
    </label>
  );
}

function BarsPanel({ symbol }: { symbol: string }) {
  const { state, reload } = useAsync<BarSeries>(
    useCallback((signal) => fetchBars({ symbol, limit: BAR_LIMIT }, signal), [symbol]),
    [symbol],
  );

  if (state.status === 'loading') {
    return <Message tone="info" title={`Loading ${symbol}…`} />;
  }

  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not load market data"
        detail={state.error.message}
        action={{ label: 'Try again', onClick: reload }}
      />
    );
  }

  if (state.data.count === 0) {
    return (
      <Message
        tone="info"
        title={`No bars for ${symbol} in this window`}
        detail="The symbol is known but has no stored data yet. Run the ingestion pipeline to populate it."
        action={{ label: 'Refresh', onClick: reload }}
      />
    );
  }

  return <PriceChart bars={state.data.bars} symbol={symbol} />;
}

export function MarketView() {
  const [selected, setSelected] = useState<string>('');
  const { state, reload } = useAsync<InstrumentList>(
    useCallback((signal) => fetchInstruments(signal), []),
    [],
  );

  if (state.status === 'loading') {
    return <Message tone="info" title="Loading instruments…" />;
  }

  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not reach the TradeWatch API"
        detail={state.error.message}
        action={{ label: 'Try again', onClick: reload }}
      />
    );
  }

  const instruments = state.data.instruments;
  const [firstInstrument] = instruments;
  if (!firstInstrument) {
    return (
      <Message
        tone="info"
        title="No instruments yet"
        detail="The database has no instruments. Start the ingestion pipeline to register some."
        action={{ label: 'Refresh', onClick: reload }}
      />
    );
  }

  // Falling back to the first instrument rather than storing it in state keeps the source
  // of truth in one place: a symbol that disappears from the list cannot leave the view
  // pointing at nothing.
  const symbol = selected || firstInstrument.symbol;

  return (
    <section className="market" aria-labelledby="market-heading">
      <div className="market-header">
        <h2 id="market-heading">{symbol}</h2>
        <SymbolSelector
          instruments={instruments}
          selected={symbol}
          onSelect={setSelected}
        />
      </div>
      <LatestPricePanel symbol={symbol} />
      <BarsPanel symbol={symbol} />
    </section>
  );
}
