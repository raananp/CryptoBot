// services/scanner-spot-arb/src/index.js
import express from 'express';
import client from 'prom-client';
import Redis from 'ioredis';

// ---------- config ----------
const PORT = Number(process.env.PORT || 9301);
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 750);
const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 50);
const DISCOVER_EVERY_SEC = Number(process.env.DISCOVER_EVERY_SEC || 30);
const QUOTE = (process.env.QUOTE || 'USDT').toUpperCase();

const BINANCE_TAKER_BPS = Number(process.env.BINANCE_TAKER_BPS || 10);
const BYBIT_TAKER_BPS   = Number(process.env.BYBIT_TAKER_BPS   || 10);

const MIN_NET_BPS     = Number(process.env.MIN_NET_BPS     || 0);
const MIN_GROSS_BPS   = Number(process.env.MIN_GROSS_BPS   || 0);
const MIN_ABS_SPREAD  = Number(process.env.MIN_ABS_SPREAD  || 0);
const MIN_NOTIONAL    = Number(process.env.MIN_NOTIONAL    || 0);
const MAX_BOOK_AGE_MS = Number(process.env.MAX_BOOK_AGE_MS || 6000);

// rate limit emitted opps
const EMIT_RATE_PER_SEC = Number(process.env.EMIT_RATE_PER_SEC || 120);
const EMIT_BURST        = Number(process.env.EMIT_BURST        || 240);

// ---------- observability ----------
const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const scansTotal = new client.Counter({
  name: 'spot_scanner_scans_total',
  help: 'scan ticks run',
});
const universeGauge = new client.Gauge({
  name: 'spot_scanner_universe_size',
  help: 'number of symbols in current scan universe',
});
const emittedTotal = new client.Counter({
  name: 'spot_scanner_opportunities_emitted_total',
  help: 'opportunities emitted to arb.opportunities',
});
const dropsTotal = new client.Counter({
  name: 'spot_scanner_drops_total',
  help: 'dropped symbol pairs',
  labelNames: ['reason'],
});
const clockSkewGauge = new client.Gauge({
  name: 'exchange_clock_skew_ms',
  help: 'Adapter clock minus Redis TIME in milliseconds (positive = adapter ahead of Redis)',
  labelNames: ['exchange'],
});

register.registerMetric(scansTotal);
register.registerMetric(universeGauge);
register.registerMetric(emittedTotal);
register.registerMetric(dropsTotal);
register.registerMetric(clockSkewGauge);

app.get('/healthz', (_req, res) =>
  res.json({
    status: 'ok',
    quote: QUOTE,
    maxSymbols: MAX_SYMBOLS,
    scanIntervalMs: SCAN_INTERVAL_MS,
    thresholds: { MIN_GROSS_BPS, MIN_NET_BPS, MIN_ABS_SPREAD, MIN_NOTIONAL, MAX_BOOK_AGE_MS },
  })
);

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// Simple time debug endpoint (also sets the skew gauge)
app.get('/timez', async (_req, res) => {
  try {
    const [secStr, usecStr] = await redis.time();
    const redisMs = Number(secStr) * 1000 + Math.floor(Number(usecStr) / 1000);
    const nodeMs  = Date.now();
    const skewMs  = nodeMs - redisMs;
    clockSkewGauge.set({ exchange: 'scanner' }, skewMs);
    res.json({ ok: true, exchange: 'scanner', redis: { sec: Number(secStr), usec: Number(usecStr), ms: redisMs }, node: { ms: nodeMs }, skewMs });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ---------- redis ----------
const redis = new Redis(REDIS_URL);
const obKey = (ex, sym) => `orderbook:${ex}:${sym}`;
const metaSymbolsKey = (ex) => `meta:${ex}:symbols`;

// Use Redis TIME for "now" so it matches adapter timestamps
async function nowMs() {
  try {
    const [secStr, usecStr] = await redis.time();
    const redisMs = Number(secStr) * 1000 + Math.floor(Number(usecStr) / 1000);
    // record scanner vs redis skew (positive => scanner ahead)
    clockSkewGauge.set({ exchange: 'scanner' }, Date.now() - redisMs);
    return redisMs;
  } catch {
    return Date.now();
  }
}

const bps = (x) => x * 10000;
const safeNum = (x) => {
  const n = Number(x);
  return Number.isFinite(n) ? n : undefined;
};

// token bucket
let tokens = EMIT_BURST;
let lastRefill = Date.now();
function takeToken(n = 1) {
  const t = Date.now();
  const delta = (t - lastRefill) / 1000;
  lastRefill = t;
  tokens = Math.min(EMIT_BURST, tokens + delta * EMIT_RATE_PER_SEC);
  if (tokens >= n) { tokens -= n; return true; }
  return false;
}

function parseBook(raw) {
  if (!raw) return null;
  try {
    const obj = JSON.parse(raw);
    const bid = safeNum(obj.bid);
    const ask = safeNum(obj.ask);
    if (bid == null || ask == null) return null;
    const ts = Number(obj.ts);
    return (Number.isFinite(ts) ? { bid, ask, ts } : null);
  } catch {
    return null;
  }
}

function calcEdge(pxBuy, feeBuyBps, pxSell, feeSellBps) {
  const mid = (pxBuy + pxSell) / 2;
  if (!(mid > 0)) return { mid: 0, grossBps: 0, feesBps: 0, netBps: 0, abs: 0 };
  const grossBps = bps((pxSell - pxBuy) / mid);
  const feesBps = (feeBuyBps || 0) + (feeSellBps || 0);
  const netBps = grossBps - feesBps;
  const abs = pxSell - pxBuy;
  return { mid, grossBps, feesBps, netBps, abs };
}

async function emitOpp(legs, edgeBps, feesBps, paper = true) {
  if (!takeToken()) return;
  const payload = { edgeBps, legs, paper };
  await redis.xadd('arb.opportunities', '*', 'data', JSON.stringify({ ts: await nowMs(), payload }));
  emittedTotal.inc();
}

// ---------- discovery ----------
let discovered = [];
let lastDiscover = 0;

async function getMetaSymbols(ex) {
  try {
    const raw = await redis.get(metaSymbolsKey(ex));
    if (!raw) return null;
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter(Boolean).map(String) : null;
  } catch { return null; }
}

function intersect(a, b) {
  const B = new Set(b);
  return a.filter((x) => B.has(x));
}
function uniq(arr) { return [...new Set(arr)]; }

async function discoverUniverse() {
  const t = Math.floor((await nowMs()) / 1000);
  if (t - lastDiscover < DISCOVER_EVERY_SEC && discovered.length) {
    return discovered.slice(0, MAX_SYMBOLS);
  }
  lastDiscover = t;

  let binance = await getMetaSymbols('binance');
  let bybit   = await getMetaSymbols('bybit');

  if (!binance || !bybit || !binance.length || !bybit.length) {
    discovered = [];
    return discovered;
  }

  const suf = QUOTE.toUpperCase();
  binance = binance.filter((s) => s.endsWith(suf));
  bybit   = bybit.filter((s) => s.endsWith(suf));

  discovered = intersect(uniq(binance), uniq(bybit)).slice(0, MAX_SYMBOLS);
  return discovered;
}

// ---------- main scan ----------
async function scanOnce() {
  const symbols = await discoverUniverse();
  universeGauge.set(symbols.length);
  scansTotal.inc();

  if (!symbols.length) {
    dropsTotal.inc({ reason: 'no_universe' });
    return;
  }

  const keys = [];
  for (const s of symbols) {
    keys.push(obKey('binance', s), obKey('bybit', s));
  }
  const raws = await redis.mget(keys);
  const tNow = await nowMs();

  for (let i = 0; i < symbols.length; i++) {
    const sym = symbols[i];
    const bnb = parseBook(raws[i * 2 + 0]);
    const byb = parseBook(raws[i * 2 + 1]);

    if (!bnb || !byb) { dropsTotal.inc({ reason: 'missing_book' }); continue; }

    // Age in ms using Redis TIME; clamp negatives to 0
    const ageBnb = Math.max(0, tNow - bnb.ts);
    const ageByb = Math.max(0, tNow - byb.ts);
    if (ageBnb > MAX_BOOK_AGE_MS || ageByb > MAX_BOOK_AGE_MS) {
      dropsTotal.inc({ reason: 'stale_book' }); continue;
    }

    // Path A: BUY Binance ask -> SELL Bybit bid
    if (bnb.ask && byb.bid) {
      const { mid, grossBps, feesBps, netBps, abs } =
        calcEdge(bnb.ask, BINANCE_TAKER_BPS, byb.bid, BYBIT_TAKER_BPS);

      const notionalOK = mid * 1 >= MIN_NOTIONAL;
      if (
        Number.isFinite(netBps) &&
        grossBps >= MIN_GROSS_BPS &&
        netBps >= MIN_NET_BPS &&
        abs >= MIN_ABS_SPREAD &&
        notionalOK
      ) {
        await emitOpp(
          [
            { exchange: 'binance', instrumentId: sym, side: 'BUY',  estPx: bnb.ask, feeBps: BINANCE_TAKER_BPS },
            { exchange: 'bybit',   instrumentId: sym, side: 'SELL', estPx: byb.bid,  feeBps: BYBIT_TAKER_BPS   },
          ],
          grossBps, feesBps, true
        );
      }
    }

    // Path B: BUY Bybit ask -> SELL Binance bid
    if (byb.ask && bnb.bid) {
      const { mid, grossBps, feesBps, netBps, abs } =
        calcEdge(byb.ask, BYBIT_TAKER_BPS, bnb.bid, BINANCE_TAKER_BPS);

      const notionalOK = mid * 1 >= MIN_NOTIONAL;
      if (
        Number.isFinite(netBps) &&
        grossBps >= MIN_GROSS_BPS &&
        netBps >= MIN_NET_BPS &&
        abs >= MIN_ABS_SPREAD &&
        notionalOK
      ) {
        await emitOpp(
          [
            { exchange: 'bybit',   instrumentId: sym, side: 'BUY',  estPx: byb.ask, feeBps: BYBIT_TAKER_BPS   },
            { exchange: 'binance', instrumentId: sym, side: 'SELL', estPx: bnb.bid, feeBps: BINANCE_TAKER_BPS },
          ],
          grossBps, feesBps, true
        );
      }
    }
  }
}

// ---------- bootstrap ----------
(async function bootstrap() {
  console.log(
    `[scanner-spot-arb] starting; quote=${QUOTE}, cap=${MAX_SYMBOLS}, scan=${SCAN_INTERVAL_MS}ms; ` +
    `thresholds: gross>=${MIN_GROSS_BPS}bps, net>=${MIN_NET_BPS}bps, abs>=${MIN_ABS_SPREAD}, notional>=${MIN_NOTIONAL}, age<=${MAX_BOOK_AGE_MS}ms`
  );

  app.listen(PORT, () => {
    console.log(`[scanner-spot-arb] metrics on ${PORT}`);
  });

  // kick the skew gauge once at start
  try { await app.get('/timez'); } catch {}

  // main loop
  setInterval(() => { scanOnce().catch(()=>{}); }, SCAN_INTERVAL_MS);
})();