// index.js
import WebSocket from "ws";
import Redis from "ioredis";
import express from "express";
import client from "prom-client";
import { fetchBybitOptionSymbols, canonicalizeBybit } from "./bybitOptions.js";

/**
 * ENV
 *   REDIS_URL=redis://redis:6379
 *   PREFIX=bybit_opt
 *   WS_URL=wss://stream.bybit.com/v5/public/option
 *   BOOK_DEPTH=50
 *   DISCOVER_INTERVAL=3600000        (ms)
 *   METRICS_PORT=9101
 *   BYBIT_REST_URL=https://api.bybit.com/v5/market/instruments-info
 *   BYBIT_BASE_COINS=BTC,ETH
 */

const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

const PREFIX = process.env.PREFIX || "bybit_opt";
const BOOK_PREFIX = `orderbook:${PREFIX}`;
const META_KEY = `meta:${PREFIX}:symbols`;

const WS_URL = process.env.WS_URL || "wss://stream.bybit.com/v5/public/option";
const DEPTH = String(process.env.BOOK_DEPTH || "50");
const DISCOVER_INTERVAL = Number(process.env.DISCOVER_INTERVAL || 3600000);

// ---------- Prometheus ----------
const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const wsGauge = new client.Gauge({
  name: "adapter_ws_connected",
  help: "1 if websocket up",
  labelNames: ["adapter"],
});
const symGauge = new client.Gauge({
  name: "adapter_symbols_discovered",
  help: "Number of discovered symbols",
  labelNames: ["adapter"],
});
const subGauge = new client.Gauge({
  name: "adapter_topics_subscribed",
  help: "Number of active orderbook topics subscribed",
  labelNames: ["adapter"],
});
const booksCounter = new client.Counter({
  name: "adapter_books_updates_total",
  help: "Count of orderbook updates processed",
  labelNames: ["adapter"],
});

register.registerMetric(wsGauge);
register.registerMetric(symGauge);
register.registerMetric(subGauge);
register.registerMetric(booksCounter);

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", register.contentType);
  res.end(await register.metrics());
});

const METRICS_PORT = Number(process.env.METRICS_PORT || 9101);
app.listen(METRICS_PORT, () => {
  console.log(`[bybit-options] metrics on :${METRICS_PORT}`);
});

// ---------- WS topic helpers ----------
function topicFor(symbol) {
  return `orderbook.${DEPTH}.${symbol}`;
}

// ---------- State ----------
let ws;
let symbols = [];
const subscribed = new Set(); // tracks topics currently subscribed
let reconnectDelayMs = 2000;  // backoff up to 30s

// ---------- WS connect / lifecycle ----------
function connectWs() {
  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    console.log("[bybit-options] ws connected");
    wsGauge.set({ adapter: "bybit-options" }, 1);
    reconnectDelayMs = 2000;
    // resubscribe to currently known topics
    if (symbols.length) subscribeDiff(symbols);
    // heartbeat ping
    startHeartbeat();
  });

  ws.on("close", () => {
    console.warn("[bybit-options] ws closed, retrying...");
    wsGauge.set({ adapter: "bybit-options" }, 0);
    stopHeartbeat();
    // Clear subscribed state (server has dropped it)
    subscribed.clear();
    subGauge.set({ adapter: "bybit-options" }, 0);
    setTimeout(connectWs, Math.min(reconnectDelayMs, 30000));
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30000);
  });

  ws.on("error", (err) => {
    console.error("[bybit-options] ws error", err.message);
    try { ws.close(); } catch {}
  });

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // Standard book messages:
      if (msg?.topic?.startsWith("orderbook.") && msg?.data) {
        const native = msg.data.s || msg.topic.split(".").pop();
        const bids = (msg.data.b || []).map(([p, q]) => [Number(p), Number(q)]);
        const asks = (msg.data.a || []).map(([p, q]) => [Number(p), Number(q)]);
        const payload = {
          ts: msg.ts || Date.now(),
          symbol: native,
          canonical: canonicalizeBybit(native),
          bids,
          asks,
        };
        await redis.set(`${BOOK_PREFIX}:${native}`, JSON.stringify(payload));
        booksCounter.inc({ adapter: "bybit-options" });
        return;
      }

      // PONG (ignore), subscribe acks, etc.
      // Bybit often returns { "op":"ping" } / "pong" or result envelopes.
    } catch (e) {
      console.error("[bybit-options] parse error", e.message);
    }
  });
}

// ---------- Heartbeat ----------
let pingTimer = null;
function startHeartbeat() {
  stopHeartbeat();
  pingTimer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: "ping" }));
    }
  }, 15000);
}
function stopHeartbeat() {
  if (pingTimer) {
    clearInterval(pingTimer);
    pingTimer = null;
  }
}

// ---------- Subscribe / Unsubscribe with diff ----------
function subscribeDiff(newSymbols) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  const desiredTopics = new Set(newSymbols.map(topicFor));

  // topics to add
  const toAdd = [];
  for (const t of desiredTopics) if (!subscribed.has(t)) toAdd.push(t);

  // topics to remove
  const toRemove = [];
  for (const t of subscribed) if (!desiredTopics.has(t)) toRemove.push(t);

  const CHUNK = 100;

  // Unsubscribe removed topics first
  for (let i = 0; i < toRemove.length; i += CHUNK) {
    const args = toRemove.slice(i, i + CHUNK);
    ws.send(JSON.stringify({ op: "unsubscribe", args }));
    args.forEach(a => subscribed.delete(a));
  }

  // Subscribe new topics
  for (let i = 0; i < toAdd.length; i += CHUNK) {
    const args = toAdd.slice(i, i + CHUNK);
    ws.send(JSON.stringify({ op: "subscribe", args }));
    args.forEach(a => subscribed.add(a));
  }

  subGauge.set({ adapter: "bybit-options" }, subscribed.size);
  if (toAdd.length || toRemove.length) {
    console.log(
      `[bybit-options] subscribed=${toAdd.length} unsubscribed=${toRemove.length} total=${subscribed.size}`
    );
  }
}

// ---------- Discovery ----------
async function discover() {
  console.log("[bybit-options] discovering...");
  try {
    const syms = await fetchBybitOptionSymbols();
    symbols = syms;
    await redis.set(META_KEY, JSON.stringify(syms));
    symGauge.set({ adapter: "bybit-options" }, syms.length);
    // Diff-resubscribe
    subscribeDiff(symbols);
    console.log(`[bybit-options] discovered ${syms.length} symbols`);
  } catch (err) {
    console.error("[bybit-options] discovery failed", err.message);
    symGauge.set({ adapter: "bybit-options" }, 0);
  }
}

// ---------- Main ----------
async function main() {
  connectWs();
  // first discovery after a short delay
  setTimeout(discover, 1500);
  setInterval(discover, DISCOVER_INTERVAL);
}

main().catch((e) => {
  console.error("[bybit-options] fatal", e);
  process.exit(1);
});