// The browser half of the end-to-end latency benchmark. Run by e2e_latency.py, not directly.
//
// Opens a real Chrome page (the installed browser, driven by playwright-core from the
// frontend's dev dependencies), connects to /ws/market from inside the page, and for every
// message records two page-side timestamps: when the message event fired, and the start of
// the next animation frame — the earliest moment anything derived from it can be painted.
// Prints one JSON document on stdout; the Python wrapper does all the arithmetic.
//
//   node e2e_latency_browser.mjs <ws-url> <seconds>

import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(path.join(here, '..', 'frontend', 'package.json'));
const { chromium } = require('playwright-core');

const [wsUrl, seconds] = process.argv.slice(2);

const browser = await chromium.launch({ channel: 'chrome', headless: true });
try {
  const page = await browser.newPage();
  const samples = await page.evaluate(
    ({ url, ms }) =>
      new Promise((resolve, reject) => {
        const out = [];
        const socket = new WebSocket(url);
        socket.onerror = () => reject(new Error('websocket error'));
        socket.onmessage = (message) => {
          const receivedMs = performance.timeOrigin + performance.now();
          let event;
          try {
            event = JSON.parse(message.data);
          } catch {
            return;
          }
          if (!event.occurred_at || !event.ingested_at) return; // control frames
          const sample = {
            type: event.event_type,
            occurred_at: event.occurred_at,
            ingested_at: event.ingested_at,
            received_ms: receivedMs,
            frame_ms: null,
          };
          out.push(sample);
          requestAnimationFrame((frameTime) => {
            sample.frame_ms = performance.timeOrigin + frameTime;
          });
        };
        setTimeout(() => {
          socket.close();
          resolve(out);
        }, ms);
      }),
    { url: wsUrl, ms: Number(seconds) * 1000 },
  );
  process.stdout.write(JSON.stringify({ samples, userAgent: await browser.version() }));
} finally {
  await browser.close();
}
