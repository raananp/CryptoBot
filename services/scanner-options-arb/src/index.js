// services/scanner-options/src/crossvenue-options.js
// Cross-venue OPTIONS scanner: Binance Options ↔ Bybit Options
// Emits normalized opportunities to Redis stream 'arb.opportunities'.
// Expects each exchange adapter to write per-instrument top-of-book to:
//    orderbook:<exPrefix>:<nativeInstrumentId>  JSON { bid, ask, ts }
// And optionally discovery lists to:
//    meta:<exPrefix>:symbols                    JSON [ "<nativeSymbol>", ... ]
//
// IMPORTANT: We normalize native instrument ids to a canonical form:
//    CANON: BASE-YYYY-MM-DD-STRIKE-C|P (e.g., BTC-2024-09-27-19000-C)
// We intersect by this canonical id, but we still read Redis using each
// exchange's *native* instrument id.
//
// You can control the exchange key prefixes with env:
//    OPT_EX1_PREFIX (default 'binance_opt')
//    OPT_EX2_PREFIX (default 'bybit_opt')
// And UI labels for emitted legs:
//    OPT_EX1_LABEL (default 'binance')
//    OPT_EX2_LABEL (default 'bybit')

import Redis from 'ioredis';

// ---------- env / config ----------
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 750);

const OPT_EX1_PREFIX = process.env.OPT_EX1_PREFIX || 'binance_opt';
const OPT_EX2_PREFIX = process.env.OPT_EX2_PREFIX || 'bybit_opt';
const OPT_EX1_LABEL  = process.env.OPT_EX1_LABEL  || 'binance';
const OPT_EX2_LABEL  = process.env.OPT_EX2_LABEL  || 'bybit';

// Universe cap & cadence
const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 400);         // cap instruments in intersection
const DISCOVER_EVERY_SEC = Number(process.env.DISCOVER_EVERY_SEC || 30);

// Filters / thresholds
const EX1_TAKER_BPS = Number(process.env.EX1_TAKER_BPS || 10);      // 0.10%
const EX2_TAKER_BPS = Number(process.env.EX2_TAKER_BPS || 10);      // 0.10%

const MIN_NET_BPS     = Number(process.env.MIN_NET_BPS     || 0);
const MIN_GROSS_BPS   = Number(process.env.MIN_GROSS_BPS   || 0);
const MIN_ABS_SPREAD  = Number(process.env.MIN_ABS_SPREAD  || 0);
const MIN_NOTIONAL    = Number(process.env.MIN_NOTIONAL    || 0);   // assumes 1 unit notional ~ mid (simplified)
const MAX_BOOK_AGE_MS = Number(process.env.MAX_BOOK_AGE_MS || 2000);

// Emission rate limit
const EMIT_RATE_PER_SEC = Number(process.env.EMIT_RATE_PER_SEC || 60);
const EMIT_BURST        = Number(process.env.EMIT_BURST        || 120);

// ---------- redis ----------
const redis = new Redis(REDIS_URL);

// Keys
const obKey = (exPrefix, nativeId) => `orderbook:${exPrefix}:${nativeId}`; // JSON { bid, ask, ts }
const metaSymbolsKey = (exPrefix) => `meta:${exPrefix}:symbols`;            // JSON [nativeId,...]

// ---------- utils ----------
const now = () => Date.now();
const bps = (x) => x * 10000;
const safeNum = (x) => {
  const n = Number(x);
  return Number.isFinite(n) ? n : undefined;
};

// Token-bucket
let tokens = EMIT_BURST;
let lastRefill = now();
function takeToken(n = 1) {
  const t = now();
  const delta = (t - lastRefill) / 1000;
  lastRefill = t;
  tokens = Math.min(EMIT_BURST, tokens + delta * EMIT_RATE_PER_SEC);
  if (tokens >= n) { tokens -= n; return true; }
  return false;
}

// ---------- canonical instrument parsing ----------
// Produces CANON: BASE-YYYY-MM-DD-STRIKE-C|P
// Supports multiple native patterns commonly seen on Binance/Bybit, e.g.:
//  - BTC-240927-19000-C
//  - BTC-27SEP24-19000-C
//  - BTC-2024-09-27-19000-C
//  - (fallback) BTC-2024-09-27-19000-P etc.
const MONTHS3 = {
  JAN: 1, FEB: 2, MAR: 3, APR: 4, MAY: 5, JUN: 6,
  JUL: 7, AUG: 8, SEP: 9, OCT: 10, NOV: 11, DEC: 12,
};

function pad2(x){ return String(x).padStart(2,'0'); }
function toCanonDate(y, m, d){
  return `${y}-${pad2(m)}-${pad2(d)}`;
}

// Try patterns, return { base, y, m, d, strike, cp } or null
function parseNativeOptionId(native) {
  if (!native || typeof native !== 'string') return null;
  const s = native.trim().toUpperCase();

  // 1) BTC-240927-19000-C (YYMMDD)
  {
    const m = s.match(/^([A-Z]+)-(\d{6})-([0-9.]+)-(C|P)$/);
    if (m) {
      const base = m[1];
      const y2 = Number(m[2].slice(0,2));
      const m2 = Number(m[2].slice(2,4));
      const d2 = Number(m[2].slice(4,6));
      const year = 2000 + y2; // assume 20xx
      const strike = Number(m[3]);
      const cp = m[4];
      if (strike > 0 && m2>=1 && m2<=12 && d2>=1 && d2<=31) {
        return { base, y: year, m: m2, d: d2, strike, cp };
      }
    }
  }

  // 2) BTC-27SEP24-19000-C (DDMMMYY)
  {
    const m = s.match(/^([A-Z]+)-(\d{2})([A-Z]{3})(\d{2})-([0-9.]+)-(C|P)$/);
    if (m) {
      const base = m[1];
      const d2 = Number(m[2]);
      const mon = MONTHS3[m[3]];
      const y2 = Number(m[4]);
      const year = 2000 + y2;
      const strike = Number(m[5]);
      const cp = m[6];
      if (strike > 0 && mon && d2>=1 && d2<=31) {
        return { base, y: year, m: mon, d: d2, strike, cp };
      }
    }
  }

  // 3) BTC-2024-09-27-19000-C (YYYY-MM-DD)
  {
    const m = s.match(/^([A-Z]+)-(\d{4})-(\d{2})-(\d{2})-([0-9.]+)-(C|P)$/);
    if (m) {
      const base = m[1];
      const y = Number(m[2]);
      const mo = Number(m[3]);
      const d = Number(m[4]);
      const strike = Number(m[5]);
      const cp = m[6];
      if (y>=2000 && strike>0 && mo>=1 && mo<=12 && d>=1 && d<=31) {
        return { base, y, m: mo, d, strike, cp };
      }
    }
  }

  // Not recognized
  return null;
}

function toCanonicalId(parsed) {
  if (!parsed) return null;
  const date = toCanonDate(parsed.y, parsed.m, parsed.d);
  // If strike has decimals, keep as-is (e.g., 0.05) but avoid trailing zeros
  const strikeStr = String(parsed.strike).replace(/\.0+$/,'');
  return `${parsed.base}-${date}-${strikeStr}-${parsed.cp}`;
}

// ---------- discovery ----------
async function getMetaSymbols(exPrefix) {
  try {
    const raw = await redis.get(metaSymbolsKey(exPrefix));
    if (!raw) return null;
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return null;
    return arr.map(s => String(s)).filter(Boolean);
  } catch {
    return null;
  }
}

async function scanOrderbookSymbols(exPrefix) {
  // Walk keys: orderbook:<exPrefix>:*
  const prefix = `orderbook:${exPrefix}:`;
  const res = [];
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', `${prefix}*`, 'COUNT', 1000);
    cursor = next;
    for (const k of keys) {
      res.push(k.slice(prefix.length));
    }
  } while (cursor !== '0');
  return res;
}

// Build a map: canonicalId -> nativeId
async function buildCanonMap(exPrefix) {
  const nativeList = (await getMetaSymbols(exPrefix)) || (await scanOrderbookSymbols(exPrefix));
  const map = new Map(); // canon -> native
  for (const native of nativeList) {
    const p = parseNativeOptionId(native);
    const canon = toCanonicalId(p);
    if (canon) {
      // Keep the first seen mapping
      if (!map.has(canon)) map.set(canon, native);
    }
  }
  return map;
}

function intersectCanonMaps(m1, m2) {
  const out = [];
  for (const canon of m1.keys()) {
    if (m2.has(canon)) {
      out.push({
        canon,
        native1: m1.get(canon),
        native2: m2.get(canon),
      });
    }
  }
  return out;
}

// ---------- pricing / edge ----------
function parseBook(raw) {
  if (!raw) return null;
  try {
    const obj = JSON.parse(raw);
    const bid = safeNum(obj.bid);
    const ask = safeNum(obj.ask);
    if (bid == null || ask == null) return null;
    const ts = Number(obj.ts || now());
    return { bid, ask, ts };
  } catch { return null; }
}

function calcEdge(pxBuy, feeBuyBps, pxSell, feeSellBps) {
  const mid = (pxBuy + pxSell) / 2;
  if (!(mid > 0)) return { mid: 0, grossBps: 0, feesBps: 0, netBps: 0, abs: 0 };
  const grossBps = bps((pxSell - pxBuy) / mid);
  const feesBps  = (feeBuyBps || 0) + (feeSellBps || 0);
  const netBps   = grossBps - feesBps;
  const abs      = pxSell - pxBuy;
  return { mid, grossBps, feesBps, netBps, abs };
}

async function emitOpp(legs, edgeBps, feesBps, paper = true) {
  if (!takeToken()) return;
  const payload = { edgeBps, legs, paper };
  await redis.xadd('arb.opportunities', '*', 'data', JSON.stringify({ ts: now(), payload }));
}

// ---------- main scan ----------
let lastDiscover = 0;
let intersection = []; // [{ canon, native1, native2 }, ...]

async function discoverIntersection() {
  const t = Math.floor(now() / 1000);
  if (intersection.length && t - lastDiscover < DISCOVER_EVERY_SEC) {
    return intersection;
  }
  lastDiscover = t;

  const m1 = await buildCanonMap(OPT_EX1_PREFIX);
  const m2 = await buildCanonMap(OPT_EX2_PREFIX);
  let pairList = intersectCanonMaps(m1, m2);

  // Cap & (optionally) randomize for variety
  pairList = pairList.slice(0, MAX_SYMBOLS);

  intersection = pairList;
  return intersection;
}

async function scanOnce() {
  const pairs = await discoverIntersection();
  if (!pairs.length) return;

  // Batch MGET: for each pair, read both orderbooks by native ids
  const keys = [];
  for (const p of pairs) {
    keys.push(obKey(OPT_EX1_PREFIX, p.native1), obKey(OPT_EX2_PREFIX, p.native2));
  }
  const raws = await redis.mget(keys);
  const tNow = now();

  for (let i = 0; i < pairs.length; i++) {
    const { canon, native1, native2 } = pairs[i];
    const b1 = parseBook(raws[i * 2 + 0]);
    const b2 = parseBook(raws[i * 2 + 1]);
    if (!b1 || !b2) continue;
    if ((tNow - b1.ts) > MAX_BOOK_AGE_MS || (tNow - b2.ts) > MAX_BOOK_AGE_MS) continue;

    // Path A: BUY EX1 ask -> SELL EX2 bid
    if (b1.ask && b2.bid) {
      const { mid, grossBps, feesBps, netBps, abs } =
        calcEdge(b1.ask, EX1_TAKER_BPS, b2.bid, EX2_TAKER_BPS);
      const notionalOK = mid * 1 >= MIN_NOTIONAL;
      if (
        Number.isFinite(netBps) &&
        grossBps >= MIN_GROSS_BPS &&
        netBps   >= MIN_NET_BPS &&
        abs      >= MIN_ABS_SPREAD &&
        notionalOK
      ) {
        await emitOpp(
          [
            { exchange: OPT_EX1_LABEL, instrumentId: canon, side: 'BUY',  estPx: b1.ask, feeBps: EX1_TAKER_BPS },
            { exchange: OPT_EX2_LABEL, instrumentId: canon, side: 'SELL', estPx: b2.bid, feeBps: EX2_TAKER_BPS },
          ],
          grossBps, feesBps, true
        );
      }
    }

    // Path B: BUY EX2 ask -> SELL EX1 bid
    if (b2.ask && b1.bid) {
      const { mid, grossBps, feesBps, netBps, abs } =
        calcEdge(b2.ask, EX2_TAKER_BPS, b1.bid, EX1_TAKER_BPS);
      const notionalOK = mid * 1 >= MIN_NOTIONAL;
      if (
        Number.isFinite(netBps) &&
        grossBps >= MIN_GROSS_BPS &&
        netBps   >= MIN_NET_BPS &&
        abs      >= MIN_ABS_SPREAD &&
        notionalOK
      ) {
        await emitOpp(
          [
            { exchange: OPT_EX2_LABEL, instrumentId: canon, side: 'BUY',  estPx: b2.ask, feeBps: EX2_TAKER_BPS },
            { exchange: OPT_EX1_LABEL, instrumentId: canon, side: 'SELL', estPx: b1.bid, feeBps: EX1_TAKER_BPS },
          ],
          grossBps, feesBps, true
        );
      }
    }
  }
}

// ---------- bootstrap ----------
async function bootstrap() {
  console.log(
    `[scanner-options] ex1=${OPT_EX1_PREFIX}→${OPT_EX1_LABEL} ex2=${OPT_EX2_PREFIX}→${OPT_EX2_LABEL} ` +
    `cap=${MAX_SYMBOLS} refresh=${DISCOVER_EVERY_SEC}s thresholds={gross>=${MIN_GROSS_BPS}bps, net>=${MIN_NET_BPS}bps, abs>=${MIN_ABS_SPREAD}, notional>=${MIN_NOTIONAL}}`
  );

  await redis.set('service:scanner-options:up', String(now()));

  // prime discovery once
  try { await discoverIntersection(); } catch {}

  setInterval(() => { scanOnce().catch(() => {}); }, SCAN_INTERVAL_MS);
}

bootstrap().catch((err) => {
  console.error('scanner-options fatal', err);
  process.exit(1);
});