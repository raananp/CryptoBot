// services/api-gateway/src/index.js
import express from 'express';
import cors from 'cors';
import jwt from 'jsonwebtoken';
import client from 'prom-client';
import Redis from 'ioredis';
import mongoose from 'mongoose';
import { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 8080;
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongodb:27017/arb';
const JWT_SECRET = process.env.JWT_SECRET || 'change-me';
const MODE = process.env.MODE || 'paper';

// Parse AUTO_TRADE env to boolean
function parseBool(v, def = false) {
  if (typeof v !== 'string') return def;
  const s = v.trim().toLowerCase();
  return ['true', '1', 'yes', 'on'].includes(s);
}
let AUTO_TRADE = parseBool(process.env.AUTO_TRADE, false);

const app = express();
app.use(cors());
app.use(express.json());

// --- Prometheus metrics ---
const register = new client.Registry();
client.collectDefaultMetrics({ register });
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// Redis (pub/sub + main)
const redis = new Redis(REDIS_URL);
const sub = new Redis(REDIS_URL);

// Mongo models (declared once, connected in bootstrap)
const Trade =
  mongoose.models.Trade ||
  mongoose.model(
    'Trade',
    new mongoose.Schema(
      {
        ts: Number,
        mode: String,
        legs: Array,
        realizedPnl: Number,
        taken: Boolean,        // ✅ new
        approved: Boolean,     // ✅ new
        source: String,        // ✅ new
      },
      { collection: 'trades' },
    ),
  );

const Position =
  mongoose.models.Position ||
  mongoose.model(
    'Position',
    new mongoose.Schema(
      { userId: String, instrumentId: String, qty: Number, avgPx: Number, mode: String },
      { collection: 'positions' },
    ),
  );

// --- Health/auth/basic routes ---
app.get('/healthz', async (_req, res) => {
  try {
    const at = await redis.get('toggles:autoTrade');
    const md = await redis.get('toggles:mode');
    res.json({
      status: 'ok',
      mode: (md === 'live' || md === 'paper') ? md : MODE,
      autoTrade: at === 'true'
    });
  } catch {
    // fall back to cached envs if Redis hiccups
    res.json({ status: 'ok', mode: MODE, autoTrade: !!AUTO_TRADE });
  }
});
app.post('/auth/login', (req, res) => {
  const { email } = req.body || {};
  const token = jwt.sign({ sub: email || 'guest', role: 'admin' }, JWT_SECRET, { expiresIn: '2h' });
  res.json({ token });
});

// replace the /trades handler with this
app.get('/trades', async (req, res) => {
  const mode = req.query.mode || 'paper';
  const onlyTaken    = req.query.onlyTaken    !== '0';                 // default ON
  const onlyApproved = req.query.onlyApproved !== '0';                 // default ON
  const minPnl       = Number(req.query.minPnl ?? '0');                // default 0
  const onlyExecutor = req.query.onlyExecutor === '1';                 // optional

  const query = { mode };
  if (onlyTaken)    query.taken = true;
  if (onlyApproved) query.approved = true;
  if (onlyExecutor) query.source = 'executor';
  // realizedPnl filter
  query.realizedPnl = { $gt: minPnl };

  const items = await Trade.find(query).sort({ ts: -1 }).limit(100).lean();
  res.json(items);
});

app.get('/positions', async (req, res) => {
  const mode = req.query.mode || 'paper';
  const items = await Position.find({ userId: 'demo', mode }).sort({ instrumentId: 1 }).limit(1000).lean();
  res.json(items);
});

// --- Feature toggles ---
app.get('/toggles', async (req, res) => {
  const at = await redis.get('toggles:autoTrade');
  const mode = (await redis.get('toggles:mode')) || 'paper';
  res.json({ autoTrade: at === 'true', mode });
});

app.post('/toggles', async (req, res) => {
  const { autoTrade, mode } = req.body || {};
  try {
    // normalize truthy/falsy for autoTrade
    const toBool = (v, def = null) => {
      if (v === true || v === false) return v;
      if (typeof v === 'string') {
        const s = v.trim().toLowerCase();
        if (['true', '1', 'yes', 'on'].includes(s)) return true;
        if (['false', '0', 'no', 'off'].includes(s)) return false;
      }
      return def;
    };

    const nextAT = toBool(autoTrade);
    if (nextAT !== null) {
      AUTO_TRADE = nextAT;
      await redis.set('toggles:autoTrade', nextAT ? 'true' : 'false');
    }

    if (mode === 'paper' || mode === 'live') {
      await redis.set('toggles:mode', mode);
    }

    const at = await redis.get('toggles:autoTrade');
    const md = await redis.get('toggles:mode');
    res.json({ ok: true, autoTrade: at === 'true', mode: md || 'paper' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Opportunities feed ---
app.get('/opportunities', async (req, res) => {
  const items = await redis.lrange('arb.opportunities.recent', -100, -1);
  res.json(items.map((x) => {
    try { return JSON.parse(x); } catch { return null; }
  }).filter(Boolean));
});

// --- Server + WebSocket, started in bootstrap() ---
let server;
let wss;

async function tailStream(stream) {
  let lastId = '$';
  for (;;) {
    try {
      const res = await sub.xread('BLOCK', 2000, 'STREAMS', stream, lastId);
      if (!res) continue;
      for (const [, messages] of res) {
        for (const [id, fields] of messages) {
          lastId = id;
          const payloadStr = fields?.[1];
          let data;
          try { data = JSON.parse(payloadStr); } catch { data = payloadStr; }
          if (stream === 'arb.opportunities') {
            await redis.rpush('arb.opportunities.recent', JSON.stringify(data));
            await redis.ltrim('arb.opportunities.recent', -200, -1);
          } else if (stream === 'arb.trades') {
            // ✅ forward only executed, approved, profitable trades from executor
            const ok =
              data &&
              data.taken === true &&
              data.approved === true &&
              typeof data.realizedPnl === 'number' &&
              data.realizedPnl > 0 &&
              data.source === 'executor';
            if (!ok) continue;
          }
          const payload = JSON.stringify({ stream, data });
          wss?.clients.forEach((c) => { try { c.send(payload); } catch {} });
        }
      }
    } catch (e) {
      // backoff a bit on errors
      await new Promise((r) => setTimeout(r, 500));
    }
  }
}

async function bootstrap() {
  // Connect Mongo
  await mongoose.connect(MONGO_URL);

  // Hydrate/persist toggles
  // If Redis already has a toggle, prefer it; otherwise store env default
  const existing = await redis.get('toggles:autoTrade');
  if (existing == null) {
    await redis.set('toggles:autoTrade', String(AUTO_TRADE));
  } else {
    AUTO_TRADE = existing === 'true';
  }
  if (!(await redis.get('toggles:mode'))) {
    await redis.set('toggles:mode', MODE);
  }

  // Start HTTP
  server = app.listen(PORT, () => console.log('api-gateway listening on', PORT));

  // WS
  wss = new WebSocketServer({ server, path: '/ws' });
  wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'hello', ts: Date.now(), autoTrade: AUTO_TRADE }));
  });

  // Start stream tails
  tailStream('arb.opportunities').catch(() => {});
  tailStream('arb.trades').catch(() => {});
}

bootstrap().catch((err) => {
  console.error('Fatal bootstrap error:', err);
  process.exit(1);
});