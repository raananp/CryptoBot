import express from 'express';
import client from 'prom-client';
import Redis from 'ioredis';
import mongoose from 'mongoose';
import { v4 as uuidv4 } from 'uuid';

const app = express();
app.use(express.json());

// ---------- metrics ----------
const register = new client.Registry();
client.collectDefaultMetrics({ register });
const execCounter  = new client.Counter({ name:'executor_opportunities_total', help:'Opportunities received' });
const legCounter   = new client.Counter({ name:'executor_legs_sent_total', help:'Order legs sent' });
const tradeCounter = new client.Counter({ name:'executor_trades_emitted_total', help:'Trades emitted' });
register.registerMetric(execCounter);
register.registerMetric(legCounter);
register.registerMetric(tradeCounter);

// ---------- Redis clients ----------
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

// KV / fast ops
const redisKV = new Redis(REDIS_URL, {
  enableAutoPipelining: false,
  maxRetriesPerRequest: 1,
  connectTimeout: 5000,
});

// Dedicated stream readers
const redisStreamOpp   = new Redis(REDIS_URL, { maxRetriesPerRequest: 1, connectTimeout: 5000 });
const redisStreamFills = new Redis(REDIS_URL, { maxRetriesPerRequest: 1, connectTimeout: 5000 });

let kvReady = false, oppReady = false, fillsReady = false;
function bindReadyLog(r, name, setter) {
  r.on('ready', () => { setter(true);  console.log(`[executor] Redis ${name} ready`); });
  r.on('end',   () => { setter(false); console.log(`[executor] Redis ${name} ended`); });
  r.on('error', (e) => console.warn(`[executor] Redis ${name} error:`, e?.message));
}
bindReadyLog(redisKV, 'KV',    (v)=>{ kvReady    = v; });
bindReadyLog(redisStreamOpp, 'stream-opp', (v)=>{ oppReady   = v; });
bindReadyLog(redisStreamFills,'stream-fills',(v)=>{ fillsReady = v; });

// ---------- Mongo ----------
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongodb:27017/arb';
let Trade = null;
(async () => {
  try {
    await mongoose.connect(MONGO_URL);
    Trade =
      mongoose.models.Trade ||
      mongoose.model(
        'Trade',
        new mongoose.Schema(
          {
            ts: Number,
            mode: String,
            legs: Array,
            realizedPnl: Number,
            taken: Boolean,          // ✅ new
            approved: Boolean,       // ✅ new
            source: String,          // ✅ new
          },
          { collection: 'trades' }
        )
      );
    console.log('[router-executor] Mongo connected.');
  } catch (e) {
    console.warn('[router-executor] Mongo connect failed; running stream-only.', e?.message);
  }
})();

// ---------- feature toggles ----------
function parseBool(v, def = false) {
  if (v === true) return true;
  if (v === false) return false;
  if (typeof v !== 'string') return def;
  const s = v.trim().toLowerCase();
  return ['true', '1', 'yes', 'on'].includes(s);
}
const ENV_AUTO = parseBool(process.env.AUTO_TRADE, false);
const ENV_MODE = (process.env.MODE || 'paper').toLowerCase() === 'live' ? 'live' : 'paper';
let AUTO_TRADE = ENV_AUTO;
let MODE = ENV_MODE;

// --- trading guards ---
const MIN_REALIZED_PNL = Number(process.env.MIN_REALIZED_PNL || '0'); // default: allow zero
function tradingEnabled() { return !!AUTO_TRADE; }

async function refreshToggles() {
  try {
    const [at, md] = await Promise.all([
      redisKV.get('toggles:autoTrade'),
      redisKV.get('toggles:mode'),
    ]);

    if (at != null) {
      const next = parseBool(at, AUTO_TRADE);
      if (next !== AUTO_TRADE) {
        const wasOn = AUTO_TRADE;
        AUTO_TRADE = next;
        console.log(`[executor] AUTO_TRADE changed -> ${AUTO_TRADE} (raw="${at}")`);
        if (wasOn && !AUTO_TRADE) {
          inflight.clear(); // clear unfinished work when toggled OFF
        }
      }
    }
    if (md === 'live' || md === 'paper') {
      if (md !== MODE) {
        MODE = md;
        console.log(`[executor] MODE changed -> ${MODE}`);
      }
    }
  } catch (e) {
    console.warn('[executor] refreshToggles failed:', e?.message);
  }
}
(async () => {
  await refreshToggles();
  setInterval(refreshToggles, 1000);
})();

// ---------- streams & groups ----------
const GROUP = 'executor';
const CONSUMER = `executor-${process.pid}`;

async function ensureGroup(stream) {
  try {
    await redisKV.xgroup('CREATE', stream, GROUP, '0', 'MKSTREAM');
  } catch (e) {
    if (!String(e?.message || '').includes('BUSYGROUP')) {
      console.warn(`[executor] xgroup create warn for ${stream}:`, e?.message);
    }
  }
}
await ensureGroup('arb.opportunities');
await ensureGroup('arb.approved');
await ensureGroup('orders.fills');

// ---------- state ----------
const inflight = new Map(); // corrId -> { opp, legs, fills:[], startedTs }

// ---------- helpers ----------
function protectiveFirst(legs) {
  const idx = (legs || []).findIndex((l) => (l?.side || '').toUpperCase() === 'SELL');
  if (idx > -1) return [legs[idx], ...legs.filter((_, i) => i !== idx)];
  return legs || [];
}

async function sendOrder(corrId, legIndex, leg, tif = 'IOC') {
  if (!tradingEnabled()) return null; // guard
  const orderId = uuidv4();
  const msg = {
    id: orderId,
    ts: Date.now(),
    type: 'order.new',
    payload: { corrId, legIndex, tif, ...leg },
  };
  await redisKV.xadd('orders.new', '*', 'data', JSON.stringify(msg));
  legCounter.inc();
  return orderId;
}

function computePnlFromFills(opp, fills) {
  try {
    const legs = opp?.payload?.legs || [];
    if (!Array.isArray(fills) || fills.length === 0) return 0;

    let gross = 0;
    let qty = 0;
    for (const f of fills) {
      const side = String(f?.payload?.side || '').toUpperCase();
      const px = Number(f?.payload?.px);
      const sz = Number(f?.payload?.filledSize || 0);
      if (!Number.isFinite(px) || !Number.isFinite(sz)) continue;
      const sgn = side === 'SELL' ? +1 : side === 'BUY' ? -1 : 0;
      gross += sgn * px * sz;
      qty += sz;
    }

    let mid = null;
    const buy = legs.find((l) => String(l?.side).toUpperCase() === 'BUY');
    const sell = legs.find((l) => String(l?.side).toUpperCase() === 'SELL');
    if (buy?.estPx != null && sell?.estPx != null) {
      mid = (Number(buy.estPx) + Number(sell.estPx)) / 2;
    }

    const feesAbs = Number(opp?.payload?.costs?.fees || 0);
    const slipAbs = Number(opp?.payload?.costs?.slippage || 0);
    const borAbs  = Number(opp?.payload?.costs?.borrow || 0);
    const feesLikeAbs = feesAbs + slipAbs + borAbs;

    let totalFees = 0;
    if (mid && qty) totalFees = feesLikeAbs * (qty * mid);

    const realized = gross - totalFees;
    return Number.isFinite(realized) ? realized : 0;
  } catch { return 0; }
}

async function emitTrade(opp, fills) {
  if (!tradingEnabled()) return; // guard
  const realizedPnl = computePnlFromFills(opp, fills);

  if (!(Number.isFinite(realizedPnl) && realizedPnl > MIN_REALIZED_PNL)) {
    // console.log('[executor] trade skipped (non-positive PnL)', { realizedPnl });
    return;
  }

  const trade = {
    ts: Date.now(),
    mode: opp?.payload?.paper ? 'paper' : 'live',
    legs: opp?.payload?.legs || [],
    realizedPnl,
    taken: true,             // ✅ mark as executed
    approved,                // ✅ carry risk decision
    source: 'executor',      // ✅ provenance
  };

  await redisKV.xadd('arb.trades', '*', 'data', JSON.stringify(trade));
  tradeCounter.inc();
  if (Trade) {
    try { await Trade.create(trade); } catch (e) {
      console.warn('[executor] Failed to persist trade:', e.message);
    }
  }
}

// ---------- consumers ----------
async function consumeOpportunities() {
  if (!tradingEnabled()) return; // HARD stop when OFF
  if (!oppReady) return;

  const source = 'arb.opportunities';
  const res = await redisStreamOpp.xreadgroup(
    'GROUP', GROUP, CONSUMER,
    'BLOCK', 1000,
    'COUNT', 20,
    'STREAMS', source, '>'
  );
  if (!res) return;

  for (const [, messages] of res) {
    for (const [id, fields] of messages) {
      try {
        const opp = JSON.parse(fields[1]);
        execCounter.inc();

        const corrId = opp.corrId || opp.id || uuidv4();
        const legs = protectiveFirst(opp?.payload?.legs || []);
        inflight.set(corrId, { opp, legs, fills: [], startedTs: Date.now() });

        if (legs[0]) await sendOrder(corrId, 0, legs[0], 'IOC');
        await redisStreamOpp.xack(source, GROUP, id);
      } catch (e) {
        console.warn('[executor] consumeOpportunities warn:', e?.message);
        await redisStreamOpp.xack(source, GROUP, id);
      }
    }
  }
}

async function consumeFills() {
  if (!tradingEnabled()) return; // HARD stop when OFF
  if (!fillsReady) return;

  const res = await redisStreamFills.xreadgroup(
    'GROUP', GROUP, CONSUMER,
    'BLOCK', 1000,
    'COUNT', 50,
    'STREAMS', 'orders.fills', '>'
  );
  if (!res) return;

  for (const [, messages] of res) {
    for (const [id, fields] of messages) {
      try {
        const fill = JSON.parse(fields[1]);
        const { corrId, legIndex, filledSize = 0 } = fill?.payload || {};
        const st = inflight.get(corrId);
        if (!st) { await redisStreamFills.xack('orders.fills', GROUP, id); continue; }

        st.fills[legIndex] = fill;
        if (legIndex === 0) {
          if (filledSize > 0) {
            if (st.legs[1]) await sendOrder(corrId, 1, st.legs[1], 'IOC');
            else { await emitTrade(st.opp, st.fills); inflight.delete(corrId); }
          } else {
            inflight.delete(corrId);
          }
        } else {
          await emitTrade(st.opp, st.fills);
          inflight.delete(corrId);
        }
        await redisStreamFills.xack('orders.fills', GROUP, id);
      } catch (e) {
        console.warn('[executor] consumeFills warn:', e?.message);
        await redisStreamFills.xack('orders.fills', GROUP, id);
      }
    }
  }
}

// ---------- tiny helper ----------
function withTimeout(promise, ms = 800) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms)),
  ]);
}

// ---------- server ----------
const PORT = process.env.PORT || 9303;

app.get('/healthz', async (_req, res) => {
  const body = {
    status: 'ok',
    inMemory: { autoTrade: AUTO_TRADE, mode: MODE },
  };

  try {
    const t0 = Date.now();
    const [at, md] = await withTimeout(
      Promise.all([redisKV.get('toggles:autoTrade'), redisKV.get('toggles:mode')]),
      400
    );
    body.redis = {
      roundtripMs: Date.now() - t0,
      autoTrade: at === 'true',
      mode: (md === 'live' || md === 'paper') ? md : MODE,
    };
  } catch (e) {
    body.redis = { error: e.message || 'timeout' };
  }

  res.json(body);
});

// metrics
app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// direct toggles (optional)
app.get('/toggles', (_req, res) => {
  res.json({ autoTrade: AUTO_TRADE, mode: MODE });
});
app.post('/toggles', async (req, res) => {
  const { autoTrade, mode } = req.body || {};
  try {
    if (autoTrade !== undefined) {
      await redisKV.set('toggles:autoTrade', autoTrade ? 'true' : 'false');
      const prev = AUTO_TRADE;
      AUTO_TRADE = !!autoTrade;
      if (AUTO_TRADE !== prev) console.log(`[executor] /toggles set autoTrade -> ${AUTO_TRADE}`);
    }
    if (mode && (mode === 'live' || mode === 'paper')) {
      await redisKV.set('toggles:mode', mode);
      if (MODE !== mode) console.log(`[executor] /toggles set mode -> ${mode}`);
      MODE = mode;
    }
    res.json({ ok: true, autoTrade: AUTO_TRADE, mode: MODE });
  } catch (e) {
    console.error('[executor] Failed to update toggles:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

setInterval(consumeOpportunities, 200);
setInterval(consumeFills, 200);

app.listen(PORT, '0.0.0.0', () => console.log('[router-executor] listening on', PORT));