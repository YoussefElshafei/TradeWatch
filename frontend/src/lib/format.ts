/**
 * Display formatting for values that arrive as strings.
 *
 * Separate from the components that use it so a fast-refresh boundary is not broken by a
 * module exporting both a component and a helper — and because formatting money is a rule
 * about the domain, not about any one view.
 */

/**
 * Trim the trailing zeros off a `NUMERIC(18,8)` string, so `187.42000000` reads as `187.42`.
 *
 * String in, string out. Routing a price through `Number` to format it would undo the
 * reason the backend sends it as a string at all (ADR-0009): `0.1 + 0.2` is not `0.3` in
 * binary floating point, and a display layer has no business being the place that
 * introduces the error.
 */
export function formatMoney(value: string): string {
  if (!value.includes('.')) return value;
  const trimmed = value.replace(/0+$/, '').replace(/\.$/, '');
  return trimmed === '' || trimmed === '-' ? '0' : trimmed;
}

/**
 * A percentage that already arrived as a string (ADR-0009 — never routed through `Number`
 * to format it). `backend/api/schemas` sends the number itself, not the `%` sign, so this
 * only trims trailing zeros the same way `formatMoney` does and appends the sign once, in
 * one place, instead of every call site writing its own template literal.
 */
export function formatPercent(value: string): string {
  return `${formatMoney(value)}%`;
}

export type TimestampStyle = 'date' | 'time' | 'datetime';

/**
 * One timestamp formatter for every screen, so "how a date looks" is a single decision
 * instead of each component picking its own `toLocaleString`/`toLocaleTimeString` call —
 * which is how the trade tape and the alert feed used to end up with different date styles
 * for the same kind of value.
 *
 * Always renders in the viewer's local timezone (`toLocaleString`'s default), which is the
 * right choice for a screen a person is looking at live, not an audit log.
 */
export function formatTimestamp(iso: string, style: TimestampStyle = 'datetime'): string {
  const date = new Date(iso);
  if (style === 'date') return date.toLocaleDateString();
  if (style === 'time') return date.toLocaleTimeString();
  return date.toLocaleString();
}
