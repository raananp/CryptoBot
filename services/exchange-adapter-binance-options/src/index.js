// index.js (ESM)

import WebSocket from "ws";
import Redis from "ioredis";
import express from "express";
import client from "prom-client";
import { fetch as undiciFetch } from "undici"; // Node 18+ has global fetch, undici for consistency
import crypto from "node:crypto";

// -------------------------------
// Config
// -------------------------------
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const PREFIX = process.env.PREFIX || "binance_opt";
const BOOK_PREFIX = `orderbook:${PREFIX}`;
const META_KEY = `meta:${PREFIX}:symbols`;

const WS_URL =  "wss://vstream.binance.com/ws";
const WS_CONN_COUNT = Number(process.env.WS_CONN_COUNT || 2); // 2-3 recommended
const MAX_TOPICS_PER_SHARD = Number(process.env.MAX_TOPICS_PER_SHARD || 600); // keep under exchange limit

const SUB_BATCH = Number(process.env.SUB_BATCH || 80);
const SUB_RATE_MS = Number(process.env.SUB_RATE_MS || 250); // throttle between batches

const RECONNECT_BASE_MS = Number(process.env.RECONNECT_BASE_MS || 3000);
const RECONNECT_MAX_MS = Number(process.env.RECONNECT_MAX_MS || 30_000);

const CONNECT_BASE_DELAY_MS = Number(process.env.CONNECT_BASE_DELAY_MS || 1500); // spacing between handshakes
const CONNECT_COOLOFF_MS_BASE = Number(process.env.CONNECT_COOLOFF_MS || 60_000); // after a 429
const CONNECT_COOLOFF_MAX_MS = Number(process.env.CONNECT_MAX_COOLOFF_MS || 10 * 60_000);

const DISCOVER_INTERVAL = Number(process.env.DISCOVER_INTERVAL || 15 * 60_000); // 15m
const DISCOVER_TIMEOUT_MS = Number(process.env.DISCOVER_TIMEOUT_MS || 8000);

const SYMBOLS_MIN = Number(process.env.SYMBOLS_MIN || 50); // minimum before subscribing (avoid 0)

// -------------------------------
/* Redis */
const redis = new Redis(REDIS_URL);

// -------------------------------
// Prometheus
// -------------------------------
const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const wsGauge = new client.Gauge({
  name: "adapter_ws_connected",
  help: "1 if websocket up",
  labelNames: ["adapter", "idx"],
});
const symGauge = new client.Gauge({
  name: "adapter_symbols_discovered",
  help: "Number of discovered symbols",
  labelNames: ["adapter"],
});
const subGauge = new client.Gauge({
  name: "adapter_topics_subscribed",
  help: "Number of topics actually subscribed on a shard",
  labelNames: ["adapter", "idx"],
});
const http429Counter = new client.Counter({
  name: "adapter_http_429_total",
  help: "Count of 429/cooldowns triggered",
  labelNames: ["adapter", "phase"], // connect | discover
});
const msgCounter = new client.Counter({
  name: "adapter_messages_total",
  help: "Messages handled (orderbook updates)",
  labelNames: ["adapter", "idx"],
});
const lastUpdateGauge = new client.Gauge({
  name: "adapter_last_update_ts",
  help: "Last message timestamp processed (ms since epoch)",
  labelNames: ["adapter", "idx"],
});

register.registerMetric(wsGauge);
register.registerMetric(symGauge);
register.registerMetric(subGauge);
register.registerMetric(http429Counter);
register.registerMetric(msgCounter);
register.registerMetric(lastUpdateGauge);

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", register.contentType);
  res.end(await register.metrics());
});
const METRICS_PORT = Number(process.env.METRICS_PORT || 9100);
app.listen(METRICS_PORT, () =>
  console.log(`[binance-options] metrics on :${METRICS_PORT}`)
);

// -------------------------------
// Helpers
// -------------------------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
async function safeSet(key, val) {
  try {
    await redis.set(key, val);
  } catch (e) {
    console.error("[binance-options] redis set failed", key, e.message);
  }
}
function symbolToTopic(sym) {
  // Depth stream per symbol @100ms
  // https://binance-docs.github.io/apidocs/voptions/en/#depth-streams
  return `${sym.toLowerCase()}@depth@100ms`;
}

// -------------------------------
// Global Connect Gate
// -------------------------------
class ConnectGate {
  constructor() {
    this.queue = Promise.resolve();
    this.cooloffUntil = 0;
    this.baseDelay = CONNECT_BASE_DELAY_MS;
    this.cooloffMs = CONNECT_COOLOFF_MS_BASE;
    this.cooloffMax = CONNECT_COOLOFF_MAX_MS;
  }
  async wait() {
    const now = Date.now();
    const pause = Math.max(0, this.cooloffUntil - now);
    const prior = this.queue;
    let release;
    this.queue = new Promise((res) => (release = res));
    await prior;
    if (pause) await sleep(pause);
    await sleep(this.baseDelay);
    release();
  }
  bumpCooloff(phase = "connect") {
    const now = Date.now();
    const until = Math.min(now + this.cooloffMs, now + this.cooloffMax);
    if (until > this.cooloffUntil) this.cooloffUntil = until;
    this.cooloffMs = Math.min(this.cooloffMs * 2, this.cooloffMax);
    http429Counter.inc({ adapter: "binance-options", phase });
    console.warn(
      `[binance-options] global connect cooloff until ${new Date(
        this.cooloffUntil
      ).toISOString()} (phase=${phase})`
    );
  }
  onSuccessfulConnect() {
    this.cooloffMs = CONNECT_COOLOFF_MS_BASE; // reset to base after a success
  }
}
const gate = new ConnectGate();

// -------------------------------
// Shard (one WS connection)
// -------------------------------
class SocketShard {
  constructor(idx) {
    this.idx = idx;
    this.ws = null;
    this.connected = false;

    this.reconnectBase = RECONNECT_BASE_MS;
    this.reconnectMax = RECONNECT_MAX_MS;
    this.reconnectDelay = this.reconnectBase;

    this.desiredTopics = new Set();
    this.subscribed = new Set();

    this.sendQueue = [];
    this.sendTimer = null;

    this.heartbeatTimer = null;
    this.pingInterval = Number(process.env.PING_INTERVAL_MS || 15000);
    this.pingTimeout = Number(process.env.PING_TIMEOUT_MS || 8000);
    this.lastPong = 0;
  }

  async connect() {
    // Serialize handshake & respect cooloff
    await gate.wait();

    // small jitter to avoid stampede
    const jitter = crypto.randomInt(150, 650);
    await sleep(jitter);

    const ws = new WebSocket(WS_URL);
    this.ws = ws;

    ws.on("open", () => {
      this.connected = true;
      this.reconnectDelay = this.reconnectBase;
      gate.onSuccessfulConnect();
      wsGauge.set({ adapter: "binance-options", idx: String(this.idx) }, 1);
      this.startHeartbeat();
      // subscribe to desired topics (bounded)
      this.applyDiff(this.desiredTopics);
      console.log(`[binance-options] shard=${this.idx} connected`);
    });

    ws.on("unexpected-response", (_req, res) => {
      const code = res?.statusCode;
      console.error(`[binance-options] unexpected-response shard=${this.idx} ${code}`);
      if (code === 429) {
        gate.bumpCooloff("connect");
        try { ws.close(); } catch {}
      }
    });

    ws.on("message", async (raw) => {
      try {
        const m = JSON.parse(raw);
        // Binance options depth stream payload shape:
        // { e: "depthUpdate", E: 169..., s: "BTC-...", b: [[price,qty],...], a:[[price,qty],...] }
        // But some streams may push arrays/other control frames—guard it.
        if (m?.b && m?.a) {
          const sym = m.s || this.topicToSymbol(m.stream) || "UNKNOWN";
          const bids = m.b;
          const asks = m.a;
          const payload = { ts: Date.now(), bids, asks };
          await safeSet(`${BOOK_PREFIX}:${sym}`, JSON.stringify(payload));
          msgCounter.inc({ adapter: "binance-options", idx: String(this.idx) });
          lastUpdateGauge.set({ adapter: "binance-options", idx: String(this.idx) }, Date.now());
        }
        if (m?.result === null && m?.id) {
          // subscribe ack; nothing to do
        }
      } catch (e) {
        console.error(`[binance-options] shard=${this.idx} parse message`, e.message);
      }
    });

    ws.on("close", () => {
      this.connected = false;
      wsGauge.set({ adapter: "binance-options", idx: String(this.idx) }, 0);
      this.stopHeartbeat();
      this.subscribed.clear();
      subGauge.set({ adapter: "binance-options", idx: String(this.idx) }, 0);

      const delay = Math.floor(Math.random() * this.reconnectDelay); // full jitter
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.reconnectMax);
      console.warn(`[binance-options] shard=${this.idx} reconnect in ${delay}ms`);
      setTimeout(() => this.connect(), delay);
    });

    ws.on("error", (err) => {
      console.error(`[binance-options] ws error shard=${this.idx}`, err.message);
      try { ws.close(); } catch {}
    });
  }

  topicToSymbol(topic) {
    // topic looks like 'eth-251226-3000-c@depth@100ms'
    if (!topic) return null;
    const base = topic.split("@")[0];
    // Binance symbol case is upper in Redis keys
    return base?.toUpperCase() || null;
  }

  startHeartbeat() {
    this.lastPong = Date.now();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      try {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
        this.ws.ping?.();
        const now = Date.now();
        if (now - this.lastPong > this.pingInterval + this.pingTimeout) {
          console.warn(`[binance-options] shard=${this.idx} heartbeat timeout; closing`);
          this.ws.close();
        }
      } catch (e) {
        console.error(`[binance-options] shard=${this.idx} heartbeat error`, e.message);
      }
    }, this.pingInterval);
    // track pongs
    this.ws?.on?.("pong", () => {
      this.lastPong = Date.now();
    });
  }
  stopHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  // Queue outgoing JSON messages and send in throttled batches
  queueSend(obj) {
    this.sendQueue.push(JSON.stringify(obj));
    if (!this.sendTimer) {
      this.sendTimer = setInterval(() => this.flushSends(), SUB_RATE_MS);
    }
  }
  flushSends() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const batch = this.sendQueue.splice(0, SUB_BATCH);
    for (const s of batch) {
      try { this.ws.send(s); } catch {}
    }
    if (this.sendQueue.length === 0) {
      clearInterval(this.sendTimer);
      this.sendTimer = null;
    }
  }

  // Apply desired topics: subscribe missing, unsubscribe extras, obey caps
  applyDiff(desired) {
    if (!this.connected || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    // enforce cap
    const desiredArr = Array.from(desired).slice(0, MAX_TOPICS_PER_SHARD);

    const want = new Set(desiredArr);
    const toAdd = desiredArr.filter((t) => !this.subscribed.has(t));
    const toDel = Array.from(this.subscribed).filter((t) => !want.has(t));

    // send subscribe in chunks
    for (let i = 0; i < toAdd.length; i += SUB_BATCH) {
      const args = toAdd.slice(i, i + SUB_BATCH);
      this.queueSend({ method: "SUBSCRIBE", params: args, id: Date.now() });
    }
    // send unsubscribe in chunks
    for (let i = 0; i < toDel.length; i += SUB_BATCH) {
      const args = toDel.slice(i, i + SUB_BATCH);
      this.queueSend({ method: "UNSUBSCRIBE", params: args, id: Date.now() });
    }
    // update local set
    this.subscribed = new Set(desiredArr);
    subGauge.set({ adapter: "binance-options", idx: String(this.idx) }, this.subscribed.size);
  }
}

// -------------------------------
// Pool: distributes topics to shards
// -------------------------------
class ShardPool {
  constructor(n) {
    this.shards = Array.from({ length: n }, (_, i) => new SocketShard(i));
    this.allSymbols = []; // upper-case list
  }
  connectAll() {
    this.shards.forEach((s) => s.connect());
  }
  setSymbols(symbols) {
    // Round-robin assignment to shards as topics
    this.allSymbols = symbols;
    const topics = symbols.map(symbolToTopic);
    // reset desired sets
    this.shards.forEach((s) => (s.desiredTopics = new Set()));
    let i = 0;
    for (const t of topics) {
      this.shards[i % this.shards.length].desiredTopics.add(t);
      i++;
    }
    // push diff to connected shards
    this.shards.forEach((s) => s.applyDiff(s.desiredTopics));
  }
}

const pool = new ShardPool(WS_CONN_COUNT);

// -------------------------------
// Discovery
// -------------------------------
let lastGoodSymbols = [];

async function timedFetch(url, opts={}) {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), DISCOVER_TIMEOUT_MS);
  try {
    const res = await undiciFetch(url, { ...opts, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(to);
  }
}

// Try both vapi & eapi variants. Cache last good list in Redis.
async function fetchBinanceOptionSymbols() {
  // Binance endpoints (subject to change/geo):
  // vapi: https://vapi.binance.com/vapi/v1/optionInfo?underlying=BTC
  // eapi: https://eapi.binance.com/eapi/v1/optionInfo?underlying=BTCUSDT
  const urls = [
    "https://vapi.binance.com/vapi/v1/optionInfo?underlying=BTC",
    "https://vapi.binance.com/vapi/v1/optionInfo?underlying=ETH",
    "https://eapi.binance.com/eapi/v1/optionInfo?underlying=BTCUSDT",
    "https://eapi.binance.com/eapi/v1/optionInfo?underlying=ETHUSDT",
  ];

  const out = new Set();

  for (const url of urls) {
    try {
      const res = await timedFetch(url);
      if (res.status === 429 || res.status === 418) {
        http429Counter.inc({ adapter: "binance-options", phase: "discover" });
        gate.bumpCooloff("discover");
        continue;
      }
      if (!res.ok) {
        // e.g., 404 HTML error page - skip
        continue;
      }
      const text = await res.text();
      if (!text) continue;

      // Some Binance edges return "" on overload -> guard JSON parse
      let json;
      try {
        json = JSON.parse(text);
      } catch {
        continue;
      }
      // vapi returns { data: [ { contractId, symbol, ... } ] }
      // eapi returns { data: { optionSymbols: [ "BTC-..." ] } } or similar
      const list =
        Array.isArray(json?.data)
          ? json.data.map((o) => o.symbol).filter(Boolean)
          : Array.isArray(json?.data?.optionSymbols)
          ? json.data.optionSymbols
          : [];

      for (const s of list) out.add(String(s).toUpperCase());
    } catch (e) {
      // likely timeout / abort / network
      // ignore and continue
    }
  }

  const result = Array.from(out);
  if (result.length) {
    lastGoodSymbols = result;
    await safeSet(META_KEY, JSON.stringify(result));
    return result;
  }

  // fallback: try Redis cache
  try {
    const cached = await redis.get(META_KEY);
    if (cached) {
      const arr = JSON.parse(cached);
      if (Array.isArray(arr) && arr.length) {
        console.warn("[binance-options] using cached symbols");
        return arr;
      }
    }
  } catch {}
  return [];
}

// -------------------------------
// Main
// -------------------------------
async function discoverAndDistribute() {
  console.log("[binance-options] discovering...");
  const syms = await fetchBinanceOptionSymbols();
  console.log(`[binance-options] discovered ${syms.length} symbols`);
  symGauge.set({ adapter: "binance-options" }, syms.length);

  if (syms.length >= SYMBOLS_MIN) {
    pool.setSymbols(syms);
  } else {
    console.warn("[binance-options] too few symbols; not applying");
  }
}

async function main() {
  pool.connectAll();
  // a small delay to let first shard open before flooding with subs
  setTimeout(discoverAndDistribute, 2000);
  setInterval(discoverAndDistribute, DISCOVER_INTERVAL);
}

main().catch((e) => {
  console.error("[binance-options] fatal", e);
  process.exit(1);
});