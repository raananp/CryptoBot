// services/exchange-adapter-bybit/src/index.js
import express from 'express';
import client from 'prom-client';
import WebSocket from 'ws';
import Redis from 'ioredis';
import crypto from 'node:crypto';

const EXCHANGE = 'bybit';
const PORT = Number(process.env.PORT || 9102);
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 400);
const SYMBOL_FILTER = process.env.SYMBOL_FILTER || 'USDT';
const SUB_BATCH_SIZE = Number(process.env.SUB_BATCH_SIZE || 10);
const REDIS_TTL_SEC = Number(process.env.REDIS_TTL_SEC || 30);
const HEARTBEAT_KEY = 'adapters:bybit:last_write';

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
const wsSubAcks = new client.Counter({
  name: 'exchange_ws_subscribe_acks_total',
  help: 'Subscribe acknowledgements',
  labelNames: ['exchange', 'status'],
});
const wsSubAcksReason = new client.Counter({
  name: 'exchange_ws_subscribe_acks_reason_total',
  help: 'Subscribe ack outcomes by reason',
  labelNames: ['exchange', 'reason'],
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

// NEW: clock skew gauge (Node clock minus Redis TIME)
const clockSkewGauge = new client.Gauge({
  name: 'exchange_clock_skew_ms',
  help: 'Adapter clock minus Redis TIME in milliseconds (positive = adapter ahead of Redis)',
  labelNames: ['exchange'],
});

register.registerMetric(httpReqCounter);
register.registerMetric(httpReqDuration);
register.registerMetric(wsMsgCounter);
register.registerMetric(wsReconnects);
register.registerMetric(instrumentsGauge);
register.registerMetric(obUpdates);
register.registerMetric(wsSubAcks);
register.registerMetric(wsSubAcksReason);
register.registerMetric(parseDrops);
register.registerMetric(kvWrites);
register.registerMetric(kvWriteErrors);
register.registerMetric(clockSkewGauge);

// ---------- Redis ----------
const redis = new Redis(REDIS_URL);
console.log(`[bybit] Using REDIS_URL=${REDIS_URL}`);

// ---------- HTTP ----------
app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.get('/healthz', (_req, res) =>
  res.json({ status: 'ok', exchange: EXCHANGE, mode: 'orderbook.1', maxSymbols: MAX_SYMBOLS })
);

app.get('/redisz', async (_req, res) => {
  try {
    const pong = await redis.ping();
    const info = await redis.info('server');
    const dbinfo = await redis.info('keyspace');
    const last = await redis.get(HEARTBEAT_KEY);
    const xlen = await redis.xlen('md.orderbook.bybit');
    res.json({ ok: true, pong, lastWriteKey: HEARTBEAT_KEY, lastWriteTs: last, xlen, server: info, keyspace: dbinfo });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message });
  }
});

// NEW: /timez — compare Node clock vs Redis TIME and publish skew gauge
app.get('/timez', async (_req, res) => {
  try {
    const [secStr, usecStr] = await redis.time(); // ["<sec>","<usec>"]
    const redisMs = Number(secStr) * 1000 + Math.floor(Number(usecStr) / 1000);
    const nodeMs = Date.now();
    const skewMs = nodeMs - redisMs;
    clockSkewGauge.set({ exchange: EXCHANGE }, skewMs);
    res.json({
      ok: true,
      exchange: EXCHANGE,
      redis: { sec: Number(secStr), usec: Number(usecStr), ms: redisMs },
      node: { ms: nodeMs },
      skewMs,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// ---------- Helpers ----------
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

function instrumentWsSend(ws, channelLabel) {
  const orig = ws.send.bind(ws);
  ws.send = (data, ...args) => {
    try {
      let type = 'send';
      try {
        const obj = typeof data === 'string' ? JSON.parse(data) : data;
        type = obj?.op || obj?.req_id || 'send';
      } catch {}
      wsMsgCounter.inc({ exchange: EXCHANGE, direction: 'out', channel: channelLabel, type }, 1);
    } catch {}
    return orig(data, ...args);
  };
}
function instrumentWsReceive(ws, channelExtractor = (msg) => 'unknown', typeExtractor = (msg) => 'update') {
  ws.on('message', (buf) => {
    try {
      const msg = JSON.parse(buf.toString());
      const channel = channelExtractor(msg);
      const type = typeExtractor(msg);
      wsMsgCounter.inc({ exchange: EXCHANGE, direction: 'in', channel, type }, 1);
    } catch {
      wsMsgCounter.inc({ exchange: EXCHANGE, direction: 'in', channel: 'unknown', type: 'parse_error' }, 1);
    }
  });
}
function reconnecting(connectFn) {
  const connect = () => {
    const ws = connectFn();
    ws.on('open', () => wsReconnects.inc({ exchange: EXCHANGE }, 1));
    ws.on('close', () => setTimeout(connect, 1000));
    ws.on('error', () => ws.close());
    return ws;
  };
  return connect();
}

// ---------- Main ----------
async function fetchSymbols() {
  const url = 'https://api.bybit.com/v5/market/instruments-info?category=spot';
  const res = await meteredFetch(url);
  const json = await res.json();
  const rows = json?.result?.list || [];
  const list = rows
    .filter((r) => r.status === 'Trading' && typeof r.symbol === 'string' && r.symbol.includes(SYMBOL_FILTER))
    .map((r) => r.symbol.toUpperCase());
  const picked = Array.from(new Set(list)).slice(0, MAX_SYMBOLS);
  instrumentsGauge.set({ exchange: EXCHANGE }, picked.length);
  try {
    await redis.set('meta:bybit:symbols', JSON.stringify(picked), 'EX', 600);
  } catch (e) {
    console.warn('[bybit] failed to publish meta:symbols:', e?.message);
  }
  return picked;
}

async function subscribeInBatches(ws, symbols) {
  for (let i = 0; i < symbols.length; i += SUB_BATCH_SIZE) {
    const chunk = symbols.slice(i, i + SUB_BATCH_SIZE);
    const topics = chunk.map((s) => `orderbook.1.${s}`);
    const sub = { op: 'subscribe', args: topics };
    try { ws.send(JSON.stringify(sub)); } catch {}
    await new Promise((r) => setTimeout(r, 150));
  }
}

function startWs(symbols) {
  const ws = reconnecting(() => new WebSocket('wss://stream.bybit.com/v5/public/spot'));

  instrumentWsSend(ws, 'public-spot');
  instrumentWsReceive(
    ws,
    (msg) => msg?.topic || (msg?.op ? 'rpc' : 'unknown'),
    (msg) => msg?.type || msg?.op || 'event'
  );

  let pingTmr = null;

  ws.on('open', async () => {
    clearInterval(pingTmr);
    await subscribeInBatches(ws, symbols);
    pingTmr = setInterval(() => { try { ws.send(JSON.stringify({ op: 'ping' })); } catch {} }, 20000);
  });

  ws.on('close', () => { clearInterval(pingTmr); pingTmr = null; });

  ws.on('message', async (buf) => {
    try {
      const m = JSON.parse(buf.toString());

      if (m?.op === 'subscribe') {
        const ok = m?.success === true;
        const reason = String(m?.ret_msg || (ok ? 'ok' : 'unknown_error'));
        wsSubAcks.inc({ exchange: EXCHANGE, status: ok ? 'ok' : 'error' }, 1);
        wsSubAcksReason.inc({ exchange: EXCHANGE, reason }, 1);
        if (!ok) console.warn(`[bybit] subscribe error: ${reason}`);
        return;
      }
      if (m?.op === 'pong') return;

      const topic = m?.topic || '';
      if (!topic.startsWith('orderbook.1.')) return;

      const d = Array.isArray(m?.data) ? m.data[0] : m?.data;
      if (!d) { parseDrops.inc({ exchange: EXCHANGE, reason: 'no_data' }, 1); return; }

      const sym = d?.s?.toUpperCase();
      const bid = Number(d?.b?.[0]?.[0]);
      const ask = Number(d?.a?.[0]?.[0]);

      if (!sym || !Number.isFinite(bid) || !Number.isFinite(ask)) {
        parseDrops.inc({ exchange: EXCHANGE, reason: 'no_bid_or_ask' }, 1);
        return;
      }

      // Timestamp from Redis TIME (fallback to system clock), record source
      let ts;
      try {
        const [secStr, usecStr] = await redis.time();
        ts = Number(secStr) * 1000 + Math.floor(Number(usecStr) / 1000); // ms
        await redis.set('adapters:bybit:last_ts_source', 'redis.time', 'EX', 600);
      } catch {
        ts = Date.now();
        try { await redis.set('adapters:bybit:last_ts_source', 'system_clock', 'EX', 600); } catch {}
      }

      // --- KV write (+breadcrumbs/metrics) ---
      const key = `orderbook:${EXCHANGE}:${sym}`;
      const json = JSON.stringify({ bid, ask, ts });
      try {
        await redis.set(key, json, 'EX', REDIS_TTL_SEC);

        const exists = await redis.exists(key);
        await redis.set('adapters:bybit:last_kv_symbol', sym, 'EX', 600);
        await redis.set('adapters:bybit:last_kv_json', json, 'EX', 600);
        await redis.set('adapters:bybit:last_kv_exists', String(exists), 'EX', 600);

        kvWrites.inc({ exchange: EXCHANGE });
      } catch (e) {
        kvWriteErrors.inc({ exchange: EXCHANGE });
        console.warn('[bybit] Redis KV write error:', e?.message);
      }

      // --- snapshot stream ---
      try {
        const snapshot = {
          id: crypto.randomUUID(),
          ts,
          type: 'md_snapshot',
          payload: { exchange: EXCHANGE, instrumentId: sym, bid, ask },
        };
        await redis.xadd('md.orderbook.bybit', '*', 'data', JSON.stringify(snapshot));
        obUpdates.inc({ exchange: EXCHANGE }, 1);
      } catch (e) {
        console.warn('[bybit] Redis stream write error:', e?.message);
      }

      // heartbeat
      try { await redis.set(HEARTBEAT_KEY, String(ts), 'EX', 120); } catch {}
    } catch {
      parseDrops.inc({ exchange: EXCHANGE, reason: 'message_parse_error' }, 1);
    }
  });
}

// ---------- Bootstrap ----------
(async function bootstrap() {
  const symbols = await fetchSymbols();
  console.log(`[bybit] tracking ${symbols.length} symbols (filter=${SYMBOL_FILTER}) • subscribing to orderbook.1.*`);

  setInterval(async () => {
    try { await redis.set(HEARTBEAT_KEY, String(Date.now()), 'EX', 120); } catch {}
  }, 10000);

  // refresh symbols (and publish meta) periodically
  setInterval(async () => {
    try {
      const s = await fetchSymbols();
      console.log(`[bybit] refreshed symbols -> ${s.length}`);
      if (Array.isArray(s) && s.length) {
        await redis.set('meta:bybit:symbols', JSON.stringify(s.map(x => x.toUpperCase())), 'EX', 600);
      }
    } catch (e) {
      console.warn('[bybit] failed to refresh meta:symbols:', e?.message);
    }
  }, 10 * 60 * 1000);

  startWs(symbols);
  app.listen(PORT, () => console.log(`[bybit-adapter] on ${PORT}`));
})();