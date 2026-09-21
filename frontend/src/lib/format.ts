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
