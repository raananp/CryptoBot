// services/exchange-adapters/deribit/src/index.js
import WebSocket from 'ws';
import Redis from 'ioredis';
import express from 'express';
import client from 'prom-client';
import crypto from 'node:crypto';

// ---------- constants / express ----------
const EXCHANGE = 'deribit';
const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

/**
 * PROMETHEUS METRICS (labeled)
 * - exchange_ws_messages_total{exchange,direction,channel,type}
 * - exchange_ws_reconnects_total{exchange}
 * - exchange_instruments_total{exchange}
 * - exchange_orderbook_updates_total{exchange}
 * - exchange_subscriptions_total{exchange}
 */
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
  help: 'Orderbook best bid/ask updates processed',
  labelNames: ['exchange'],
});
const subsSent = new client.Counter({
  name: 'exchange_subscriptions_total',
  help: 'Subscribe RPC batches sent',
  labelNames: ['exchange'],
});
register.registerMetric(wsMsgCounter);
register.registerMetric(wsReconnects);
register.registerMetric(instrumentsGauge);
register.registerMetric(obUpdates);
register.registerMetric(subsSent);

// ---------- config/env ----------
const PORT = Number(process.env.PORT || 9105);
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

// Deribit public WS
const WS_URL = process.env.DERIBIT_WS_URL || 'wss://www.deribit.com/ws/api/v2';

// Comma-separated currencies: e.g. "BTC,ETH"
const CURRENCIES = (process.env.CURRENCIES || 'BTC,ETH')
  .split(',')
  .map((s) => s.trim().toUpperCase())
  .filter(Boolean);

// "option" | "future" (Deribit is mainly options/futures)
const KIND = (process.env.KIND || 'option').toLowerCase();

// include expired instruments?
const EXPIRED = String(process.env.EXPIRED || 'false').toLowerCase() === 'true';

// Cap and batching
const MAX_COINS   = Number(process.env.MAX_COINS || 400);   // global cap across currencies
const BATCH_SIZE  = Number(process.env.SUB_BATCH_SIZE || 100);
const BATCH_DELAY = Number(process.env.SUB_BATCH_DELAY_MS || 150);

// Channel template; Deribit “book.{instrument}.none.10.100ms” etc.
const CHANNEL_TEMPLATE = process.env.CHANNEL_TEMPLATE || 'book.{instrument}.none.10.100ms';

// Redis
const STREAM_NAME = process.env.REDIS_STREAM || 'md.orderbook.deribit';
const PER_KEY_TTL_SEC = Number(process.env.PER_KEY_TTL_SEC || 30);
const perInstrumentKey = (inst) => `orderbook:${EXCHANGE}:${inst}`;
const redis = new Redis(REDIS_URL);

// ---------- HTTP ----------
app.get('/healthz', (_req, res) =>
  res.json({
    status: 'ok',
    exchange: EXCHANGE,
    ws: WS_URL,
    currencies: CURRENCIES,
    kind: KIND,
    expired: EXPIRED,
    maxCoins: MAX_COINS,
    batch: { size: BATCH_SIZE, delayMs: BATCH_DELAY },
  })
);
app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// ---------- WS instrumentation helpers ----------
function instrumentWsSend(ws, channelLabel = EXCHANGE) {
  const orig = ws.send.bind(ws);
  ws.send = (data, ...args) => {
    try {
      let type = 'send';
      try {
        const obj = typeof data === 'string' ? JSON.parse(data) : data;
        type = obj?.method || 'send';
      } catch {}
      wsMsgCounter.inc({ exchange: EXCHANGE, direction: 'out', channel: channelLabel, type }, 1);
    } catch {}
    return orig(data, ...args);
  };
}
function instrumentWsReceive(ws, channelExtractor = (msg) => 'unknown', typeExtractor = (msg) => 'update') {
  ws.on('message', (buf) => {
    let channel = 'unknown';
    let type = 'update';
    try {
      const msg = JSON.parse(buf.toString());
      channel = channelExtractor(msg);
      type = typeExtractor(msg);
    } catch {}
    wsMsgCounter.inc({ exchange: EXCHANGE, direction: 'in', channel, type }, 1);
  });
}
function reconnecting(connectFn) {
  const connect = () => {
    const ws = connectFn();
    ws.on('open', () => wsReconnects.inc({ exchange: EXCHANGE }, 1));
    ws.on('close', () => setTimeout(connect, 1500));
    ws.on('error', () => ws.close());
    return ws;
  };
  return connect();
}

// ---------- heartbeat ----------
let heartbeatTimer = null;
function startHeartbeat(ws) {
  clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(() => {
    try {
      ws?.send(JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method: 'public/ping', params: {} }));
    } catch {}
  }, 15_000);
}
function stopHeartbeat() {
  clearInterval(heartbeatTimer);
  heartbeatTimer = null;
}

// ---------- subscription state ----------
let ws;                               // active socket
const subscribed = new Set();         // instrument_name set (capped globally)
let pendingCurrencies = 0;            // track outstanding get_instruments RPCs

// subscribe in batches to avoid rate limits
async function subscribeInBatches(instNames) {
  for (let i = 0; i < instNames.length; i += BATCH_SIZE) {
    const chunk = instNames.slice(i, i + BATCH_SIZE);
    const channels = chunk.map((n) => CHANNEL_TEMPLATE.replace('{instrument}', n));
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: `sub_${i}`, method: 'public/subscribe', params: { channels } }));
    subsSent.inc({ exchange: EXCHANGE }, 1);
    await new Promise((r) => setTimeout(r, BATCH_DELAY));
  }
}

// ---------- connect & handle messages ----------
function connect() {
  ws = reconnecting(() => new WebSocket(WS_URL));
  instrumentWsSend(ws, EXCHANGE);
  instrumentWsReceive(
    ws,
    (msg) => msg?.params?.channel || (msg?.result ? 'rpc' : 'unknown'),
    (msg) => msg?.method || 'event'
  );

  ws.on('open', () => {
    // reset state
    subscribed.clear();
    pendingCurrencies = CURRENCIES.length;
    instrumentsGauge.set({ exchange: EXCHANGE }, 0);
    startHeartbeat(ws);

    // Request instrument lists per currency
    for (const currency of CURRENCIES) {
      const params = { currency, kind: KIND, expired: EXPIRED };
      ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: `get_instruments_${currency}`,
        method: 'public/get_instruments',
        params
      }));
    }
  });

  ws.on('message', async (buf) => {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch { return; }

    // 1) Handle instruments response(s)
    if (typeof msg.id === 'string' && msg.id.startsWith('get_instruments_')) {
      const list = Array.isArray(msg.result) ? msg.result : [];
      for (const x of list) {
        if (subscribed.size >= MAX_COINS) break;
        const name = x?.instrument_name;
        if (name && !subscribed.has(name)) subscribed.add(name);
      }
      instrumentsGauge.set({ exchange: EXCHANGE }, subscribed.size);

      pendingCurrencies = Math.max(0, pendingCurrencies - 1);
      // When all get_instruments calls returned OR reached cap — subscribe
      if (pendingCurrencies === 0 || subscribed.size >= MAX_COINS) {
        const targets = Array.from(subscribed);
        subscribeInBatches(targets).catch(() => {});
      }
      return;
    }

    // 2) Handle book subscription updates
    if (msg.method === 'subscription' && msg.params?.channel?.startsWith('book.')) {
      const d = msg.params?.data;
      const name = d?.instrument_name;
      const bestBid = Number(d?.best_bid_price);
      const bestAsk = Number(d?.best_ask_price);
      if (!name || !Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) return;

      const snapshot = {
        id: crypto.randomUUID(),
        ts: Date.now(),
        type: 'md_snapshot',
        payload: { exchange: EXCHANGE, instrumentId: name, bid: bestBid, ask: bestAsk },
      };

      try {
        // Stream (for XREAD consumers)
        await redis.xadd(STREAM_NAME, '*', 'data', JSON.stringify(snapshot));

        // KV with TTL (for scanners doing cheap GETs)
        await redis.set(
          perInstrumentKey(name),
          JSON.stringify({ bid: bestBid, ask: bestAsk, ts: snapshot.ts }),
          'EX',
          PER_KEY_TTL_SEC
        );
        obUpdates.inc({ exchange: EXCHANGE }, 1);
      } catch {
        // ignore write hiccups
      }
    }
  });

  ws.on('close', () => {
    stopHeartbeat();
  });

  ws.on('error', () => {
    try { ws.close(); } catch {}
  });
}

connect();

app.listen(PORT, () => console.log(`[${EXCHANGE}-adapter] on ${PORT}`));