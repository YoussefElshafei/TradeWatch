/**
 * A price chart for one symbol's bars.
 *
 * **On money and numbers.** Prices arrive as strings so JavaScript's JSON parser cannot
 * round them (ADR-0009). A chart library needs numbers to compute pixel positions, so the
 * conversion happens here, at the point of drawing, and the converted values are never fed
 * back anywhere. The original string is what the tooltip shows, so what a user reads is the
 * exact value the database holds rather than a float's rendering of it.
 *
 * **On fixed dimensions.** Recharts wants numeric width and height. `ResponsiveContainer`
 * would supply them from a `ResizeObserver`, which jsdom does not implement — the chart
 * would render as a 0×0 SVG and the test asserting that data reached the screen would pass
 * while showing nothing. Explicit dimensions keep the component genuinely testable; the
 * wrapper scrolls horizontally on a narrow screen. Making this responsive is worth doing
 * when the dashboard is built for real, alongside a `ResizeObserver` polyfill in the test
 * setup.
 */

import {
  CartesianGrid,
  Line,
  LineChart,
  Tooltip,
  type TooltipContentProps,
  XAxis,
  YAxis,
} from 'recharts';

import type { Bar } from '../api/market';
import { formatMoney } from '../lib/format';

export interface PriceChartProps {
  bars: Bar[];
  symbol: string;
  width?: number;
  height?: number;
}

interface ChartPoint {
  label: string;
  /** Numeric close, used only to position the line. */
  close: number;
  /** The exact stored values, for display. Never derived from `close`. */
  closeText: string;
  openText: string;
  highText: string;
  lowText: string;
  volumeText: string;
}

function toPoint(bar: Bar): ChartPoint {
  const time = new Date(bar.bar_start).getTime();
  return {
    label: new Date(time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    close: Number(bar.close),
    closeText: bar.close,
    openText: bar.open,
    highText: bar.high,
    lowText: bar.low,
    volumeText: bar.volume,
  };
}

/**
 * Custom tooltip showing the full OHLCV for a bar.
 *
 * `payload` is typed loosely by Recharts — it has to be, since it carries whatever the
 * chart's data rows are — so the one narrowing cast happens here, where the shape is known
 * because `toPoint` produced it.
 */
function ChartTooltip({ active, payload }: TooltipContentProps) {
  // `.at(0)` rather than `[0]`: its return type is explicitly `T | undefined`, which tsc
  // and the lint rule agree on. Plain index access leaves them disagreeing about whether
  // the guard is needed, because only one of them honours `noUncheckedIndexedAccess`.
  const entries = active ? payload : undefined;
  const entry = entries?.at(0);
  const point = entry === undefined ? undefined : (entry.payload as ChartPoint);
  if (point === undefined) return null;

  const rows: [string, string][] = [
    ['Open', point.openText],
    ['High', point.highText],
    ['Low', point.lowText],
    ['Close', point.closeText],
    ['Volume', point.volumeText],
  ];

  return (
    <div className="chart-tooltip" role="note">
      <strong>{point.label}</strong>
      <dl>
        {rows.map(([name, value]) => (
          <div key={name} style={{ display: 'contents' }}>
            <dt>{name}</dt>
            <dd>{formatMoney(value)}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

export function PriceChart({ bars, symbol, width = 760, height = 320 }: PriceChartProps) {
  const points = bars.map(toPoint);

  // A price axis anchored at zero wastes most of its range and flattens every move worth
  // looking at, so the domain is padded around the data instead.
  const closes = points.map((point) => point.close);
  const low = Math.min(...closes);
  const high = Math.max(...closes);
  const padding = Math.max((high - low) * 0.08, high * 0.001);

  return (
    <figure className="chart" aria-label={`${symbol} price chart`}>
      <div className="chart-scroll">
        <LineChart
          data={points}
          width={width}
          height={height}
          margin={{ top: 8, right: 16, bottom: 8, left: 8 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
          <XAxis dataKey="label" minTickGap={48} tick={{ fontSize: 12 }} />
          <YAxis
            domain={[low - padding, high + padding]}
            tickFormatter={(value: number) => value.toFixed(2)}
            width={72}
            tick={{ fontSize: 12 }}
          />
          <Tooltip content={ChartTooltip} />
          <Line
            type="monotone"
            dataKey="close"
            stroke="var(--accent)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
            name="Close"
          />
        </LineChart>
      </div>
      <figcaption>
        {points.length} bars · close price · times shown in your local timezone
      </figcaption>
    </figure>
  );
}
