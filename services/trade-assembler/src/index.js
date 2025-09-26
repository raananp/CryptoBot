// services/trade-assembler/src/index.js
import express from 'express';
import client from 'prom-client';
import Redis from 'ioredis';
import mongoose from 'mongoose';

const app = express();

// --------- metrics ----------
const register = new client.Registry();
client.collectDefaultMetrics({ register });
const tradesBuilt = new client.Counter({ name: 'trades_built_total', help: 'Trades assembled from fills' });
register.registerMetric(tradesBuilt);

// --------- redis ----------
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const redis = new Redis(REDIS_URL);
const GROUP = 'asm';
const CONSUMER = process.env.HOSTNAME || 'asm-1';

async function ensureGroup(stream) {
  try { await redis.xgroup('CREATE', stream, GROUP, '0', 'MKSTREAM'); } catch (e) {}
}
await ensureGroup('orders.fills');

// --------- mongo ----------
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongodb:27017/arb';
await mongoose.connect(MONGO_URL);

const Trade =
  mongoose.models.Trade ||
  mongoose.model(
    'Trade',
    new mongoose.Schema(
      { ts: Number, mode: String, legs: Array, realizedPnl: Number },
      { collection: 'trades' },
    ),
  );

// --------- helper: simple join buffer by corrId ----------
const pending = new Map(); // corrId -> { legs: [fillLegs], ts, mode }

function upsertFill(fill) {
  const corr = fill?.payload?.corrId;
  if (!corr) return null;

  const rec = pending.get(corr) || { legs: [], ts: Date.now(), mode: 'paper' };
  rec.legs.push(fill);
  rec.ts = fill?.ts || rec.ts;

  // carry mode if provided in fill (optional)
  if (fill?.payload?.mode) rec.mode = fill.payload.mode;

  pending.set(corr, rec);
  if (rec.legs.length >= 2) {
    pending.delete(corr);
    return rec;
  }
  return null;
}

function computeTradeFromFills(rec) {
  // Expect 2 legs: one BUY, one SELL
  const legs = rec.legs.map((f) => ({
    exchange: f?.payload?.exchange,
    instrumentId: f?.payload?.instrumentId,
    side: String(f?.payload?.side || '').toUpperCase(),
    px: Number(f?.payload?.px),
    size: Number(f?.payload?.filledSize || 0),
  }));

  if (legs.length < 2) return null;

  const buy = legs.find(l => l.side === 'BUY');
  const sell = legs.find(l => l.side === 'SELL');
  if (!buy || !sell) return null;

  const size = Math.min(buy.size, sell.size);
  const realized = (sell.px - buy.px) * size; // in quote currency (per your fills)

  return {
    ts: rec.ts,
    mode: rec.mode || 'paper',
    legs,
    realizedPnl: Number.isFinite(realized) ? realized : 0,
  };
}

// --------- consumer loop ----------
async function run() {
  for (;;) {
    try {
      const res = await redis.xreadgroup('GROUP', GROUP, CONSUMER, 'BLOCK', 2000, 'COUNT', 50, 'STREAMS', 'orders.fills', '>');
      if (!res) continue;
      for (const [, messages] of res) {
        for (const [id, fields] of messages) {
          try {
            const fill = JSON.parse(fields?.[1] || '{}');
            const joined = upsertFill(fill);
            if (joined) {
              const trade = computeTradeFromFills(joined);
              if (trade) {
                // 1) persist to Mongo
                await Trade.create(trade);
                // 2) publish to stream the UI tails (and WS relays): arb.trades
                await redis.xadd('arb.trades', '*', 'data', JSON.stringify({ id, ts: trade.ts, type: 'trade', payload: trade }));
                tradesBuilt.inc();
              }
            }
          } catch (_) {}
          await redis.xack('orders.fills', GROUP, id);
        }
      }
    } catch (e) {
      await new Promise(r => setTimeout(r, 300));
    }
  }
}

app.get('/healthz', (req,res)=>res.json({status:'ok', pending: pending.size}));
app.get('/metrics', async (req,res)=>{
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

const PORT = process.env.PORT || 9310;
app.listen(PORT, ()=> console.log('trade-assembler on', PORT));
run().catch(e=>{ console.error(e); process.exit(1); });