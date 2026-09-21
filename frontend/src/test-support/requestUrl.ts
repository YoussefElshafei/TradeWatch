/**
 * Extract the URL from whatever `fetch` was called with.
 *
 * `String(input)` looks equivalent and is not: `RequestInfo` includes `Request`, whose
 * default stringification is `[object Object]`. A stub that routes on that string matches
 * nothing and returns the wrong fixture, which surfaces as a confusing failure somewhere
 * else entirely — so the lint rule that flags it is doing real work.
 */
export function requestUrl(input: RequestInfo | URL): string {
  if (typeof input === 'string') return input;
  if (input instanceof URL) return input.href;
  return input.url;
}
