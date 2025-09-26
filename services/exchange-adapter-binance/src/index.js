// services/exchange-adapter-binance/src/index.js
import express from 'express';
import client from 'prom-client';
import WebSocket from 'ws';
import Redis from 'ioredis';
import crypto from 'node:crypto';

const EXCHANGE = 'binance';
const PORT = Number(process.env.PORT || 9101);
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 400);
const SYMBOL_FILTER = process.env.SYMBOL_FILTER || 'USDT';
const PING_MS = Number(process.env.WS_PING_MS || 18000);

const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// ---------- metrics ----------
const httpReqCounter = new client.Counter({
  name: 'exchange_http_requests_total',
  help: 'HTTP requests to exchange APIs',
  labelNames: ['exchange', 'endpoint', 'method', 'status'],
});
const httpReqDuration = new client.Histogram({
  name: 'exchange_http_request_seconds',
  help: 'HTTP request duration to exchange APIs',
  labelNames: ['exchange', 'endpoint', 'method', 'status'],
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
});
const wsMsgCounter = new client.Counter({
  name: 'exchange_ws_messages_total',
  help: 'WebSocket messages',
  labelNames: ['exchange', 'direction', 'channel', 'type'],
});
const wsReconnects = new client.Counter({
  name: 'exchange_ws_reconnects_total',
  help: 'WebSocket reconnects',
  labelNames: ['exchange'],
});
const instrumentsGauge = new client.Gauge({
  name: 'exchange_instruments_total',
  help: 'Number of instruments tracked/subscribed',
  labelNames: ['exchange'],
});
const obUpdates = new client.Counter({
  name: 'exchange_orderbook_updates_total',
  help: 'Normalized orderbook (best bid/ask) updates forwarded',
  labelNames: ['exchange'],
});
const parseDrops = new client.Counter({
  name: 'exchange_parse_drops_total',
  help: 'Ticker messages dropped due to parse/shape issues',
  labelNames: ['exchange', 'reason'],
});
const kvWrites = new client.Counter({
  name: 'exchange_kv_writes_total',
  help: 'Successful KV writes of best bid/ask',
  labelNames: ['exchange'],
});
const kvWriteErrors = new client.Counter({
  name: 'exchange_kv_write_errors_total',
  help: 'KV write errors',
  labelNames: ['exchange'],
});

register.registerMetric(httpReqCounter);
register.registerMetric(httpReqDuration);
register.registerMetric(wsMsgCounter);
register.registerMetric(wsReconnects);
register.registerMetric(instrumentsGauge);
register.registerMetric(obUpdates);
register.registerMetric(parseDrops);
register.registerMetric(kvWrites);
register.registerMetric(kvWriteErrors);

// ---------- redis ----------
const redis = new Redis(REDIS_URL);

// ---------- http endpoints ----------
app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.get('/healthz', (_req, res) =>
  res.json({
    status: 'ok',
    exchange: EXCHANGE,
    mode: 'combined-stream @bookTicker',
    maxSymbols: MAX_SYMBOLS,
  })
);

app.get('/redisz', async (_req, res) => {
  try {
    const pong = await redis.ping();
    const xlen = await redis.xlen('md.orderbook.binance');
    const lastWriteKey = 'adapters:binance:last_write';
    const lastWriteTs = await redis.get(lastWriteKey);
    const info = await redis.info();
    const keys = await redis.dbsize();
    res.json({
      ok: true,
      pong,
      lastWriteKey,
      lastWriteTs,
      xlen,
      server: info.split('\n').slice(0, 25).join('\n'),
      keyspace: `dbsize=${keys}`,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/kvz', async (_req, res) => {
  try {
    const samples = ['BTCUSDT','ETHUSDT','BNBUSDT','DOGEUSDT','ADAUSDT'];
    const keys = samples.map(s => `orderbook:${EXCHANGE}:${s}`);
    const vals = await redis.mget(keys);
    const out = {};
    samples.forEach((s, i) => { out[s] = vals[i] ? JSON.parse(vals[i]) : null; });
    const lastKvSym     = await redis.get('adapters:binance:last_kv_symbol');
    const lastKvJson    = await redis.get('adapters:binance:last_kv_json');
    const lastKvExist   = await redis.get('adapters:binance:last_kv_exists');
    const lastTsSource  = await redis.get('adapters:binance:last_ts_source');
    const lastWsSymbol  = await redis.get('adapters:binance:last_ws_symbol');
    const lastWsTs      = await redis.get('adapters:binance:last_ws_ts');
    res.json({
      ok: true,
      out,
      breadcrumbs: {
        lastKvSym, lastKvJson, lastKvExist,
        lastTsSource, lastWsSymbol, lastWsTs
      }
    });
  } catch (e) {
    res.status(500).json({ ok:false, error: String(e) });
  }
});

// ---------- helpers ----------
async function meteredFetch(url, opts = {}) {
  const u = new URL(url);
  const endpoint = u.pathname;
  const method = (opts.method || 'GET').toUpperCase();
  const end = httpReqDuration.startTimer({ exchange: EXCHANGE, endpoint, method });
  let status = 'ERR';
  try {
    const res = await fetch(url, opts);
    status = String(res.status);
    httpReqCounter.inc({ exchange: EXCHANGE, endpoint, method, status }, 1);
    end({ status });
    return res;
  } catch (e) {
    httpReqCounter.inc({ exchange: EXCHANGE, endpoint, method, status }, 1);
    end({ status });
    throw e;
  }
}

function reconnecting(connectFn) {
  const connect = () => {
    const ws = connectFn();
    ws.on('open', () => wsReconnects.inc({ exchange: EXCHANGE }, 1));
    ws.on('close', () => setTimeout(connect, 1200));
    ws.on('error', () => { try { ws.close(); } catch {} });
    return ws;
  };
  return connect();
}

function instrumentWsReceive(ws, label) {
  ws.on('message', () => {
    wsMsgCounter.inc({ exchange: EXCHANGE, direction: 'in', channel: label, type: 'message' }, 1);
  });
}

// Return current ms using Redis TIME (authoritative clock) with retry
async function redisNowMs(r) {
  for (let i = 0; i < 3; i++) {
    try {
      const [secStr, usecStr] = await r.time(); // ["<sec>","<usec>"]
      return Number(secStr) * 1000 + Math.floor(Number(usecStr) / 1000);
    } catch {
      await new Promise(res => setTimeout(res, 10));
    }
  }
  return Date.now();
}

// ---------- symbols ----------
let allowedSymbols = [];

async function refreshSymbols() {
  const res = await meteredFetch('https://api.binance.com/api/v3/exchangeInfo');
  const json = await res.json();
  const list = (json?.symbols || [])
    .filter(s => s.status === 'TRADING' && typeof s.symbol === 'string' && s.symbol.includes(SYMBOL_FILTER))
    .map(s => s.symbol.toLowerCase());
  allowedSymbols = Array.from(new Set(list)).slice(0, MAX_SYMBOLS);
  instrumentsGauge.set({ exchange: EXCHANGE }, allowedSymbols.length);
  console.log(`[binance] tracking ${allowedSymbols.length} symbols (filter=${SYMBOL_FILTER})`);
  try {
    await redis.set(
      'meta:binance:symbols',
      JSON.stringify(allowedSymbols.map(s => s.toUpperCase())),
      'EX', 600
    );
  } catch (e) {
    console.warn('[binance] failed to publish meta:symbols:', e?.message);
  }
}

// ---------- ws combined stream ----------
function startWs() {
  if (!allowedSymbols.length) {
    console.warn('[binance] no symbols to subscribe');
    return;
  }

  const streams = allowedSymbols.map(sym => `${sym}@bookTicker`).join('/');
  const WS_URL = `wss://stream.binance.com:9443/stream?streams=${streams}`;
  const ws = reconnecting(() => new WebSocket(WS_URL));

  instrumentWsReceive(ws, '@bookTicker');

  let pingTmr = null;
  let noDataTmr = null;
  let gotAnyData = false;

  const resetNoDataTimer = () => {
    clearTimeout(noDataTmr);
    noDataTmr = setTimeout(() => {
      if (!gotAnyData) {
        console.warn('[binance] no data received within 20s; reconnecting');
        try { ws.close(); } catch {}
      }
      gotAnyData = false;
    }, 20000);
  };

  ws.on('open', () => {
    console.log('[binance] ws open ->', WS_URL);
    clearInterval(pingTmr);
    pingTmr = setInterval(() => { try { ws.ping(); } catch {} }, PING_MS);
    resetNoDataTimer();
  });

  ws.on('close', () => {
    clearInterval(pingTmr);
    clearTimeout(noDataTmr);
    pingTmr = null;
    noDataTmr = null;
  });

  ws.on('message', async (buf) => {
    try {
      gotAnyData = true;
      resetNoDataTimer();

      const msg = JSON.parse(buf.toString());
      const data = msg?.data || msg;  // combined stream has {stream,data}

      const s = String(data?.s || '').toUpperCase();
      const bid = Number(data?.b);
      const ask = Number(data?.a);
      if (!s || !Number.isFinite(bid) || !Number.isFinite(ask)) {
        parseDrops.inc({ exchange: EXCHANGE, reason: 'bad_bid_ask' }, 1);
        return;
      }

      // authoritative timestamp from Redis
      let tsSource = 'redis.time';
      let ts = await redisNowMs(redis);
      if (!Number.isFinite(ts)) { // ultra-defensive
        ts = Date.now();
        tsSource = 'date.now';
      }
      const REDIS_TTL_SEC = Number(process.env.REDIS_TTL_SEC || 300);

      // breadcrumbs
      try {
        await redis.set('adapters:binance:last_ws_symbol', s, 'EX', 600);
        await redis.set('adapters:binance:last_ws_ts', String(ts), 'EX', 600);
        await redis.set('adapters:binance:last_ts_source', tsSource, 'EX', 600);
      } catch {}

      const key = `orderbook:${EXCHANGE}:${s}`;
      const payload = JSON.stringify({ bid, ask, ts });

      try {
        await redis.set(key, payload, 'EX', REDIS_TTL_SEC);
        const exists = await redis.exists(key);
        await redis.set('adapters:binance:last_kv_symbol', s, 'EX', 600);
        await redis.set('adapters:binance:last_kv_json', payload, 'EX', 600);
        await redis.set('adapters:binance:last_kv_exists', String(exists), 'EX', 600);
        kvWrites.inc({ exchange: EXCHANGE });
      } catch (e) {
        kvWriteErrors.inc({ exchange: EXCHANGE });
        console.warn('[binance] KV write error:', e?.message);
      }

      const snapshot = {
        id: crypto.randomUUID(),
        ts,
        type: 'md_snapshot',
        payload: { exchange: EXCHANGE, instrumentId: s, bid, ask },
      };

      try {
        await redis.xadd('md.orderbook.binance', '*', 'data', JSON.stringify(snapshot));
      } catch (e) {
        console.warn('[binance] redis xadd error:', e?.message);
      }

      obUpdates.inc({ exchange: EXCHANGE }, 1);
      await redis.set('adapters:binance:last_write', String(ts), 'EX', 300);
    } catch (e) {
      parseDrops.inc({ exchange: EXCHANGE, reason: 'json_parse_error' }, 1);
      console.warn('[binance] message parse error:', e?.message);
    }
  });
}

// ---------- boot ----------
(async function bootstrap() {
  await refreshSymbols();
  setInterval(refreshSymbols, 10 * 60 * 1000);
  setInterval(async () => {
    try {
      if (allowedSymbols.length) {
        await redis.set(
          'meta:binance:symbols',
          JSON.stringify(allowedSymbols.map(s => s.toUpperCase())),
          'EX', 600
        );
      }
    } catch {}
  }, 30000);
  startWs();
  app.listen(PORT, () => console.log(`[binance-adapter] on ${PORT}`));
})();