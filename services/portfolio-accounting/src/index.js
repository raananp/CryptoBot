
import express from 'express';
import client from 'prom-client';
import Redis from 'ioredis';
import mongoose from 'mongoose';

const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const tradeCounter = new client.Counter({ name: 'accounting_trades_ingested_total', help: 'Trades ingested' });
register.registerMetric(tradeCounter);

const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');
const group = 'acct'; const consumer = 'acct-1';
async function ensureGroup(stream){
  try { await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM'); } catch(e){}
}
await ensureGroup('arb.trades');

// Mongo
const mongoUrl = process.env.MONGO_URL || 'mongodb://mongodb:27017/arb';
await mongoose.connect(mongoUrl);
const Trade = mongoose.model('Trade', new mongoose.Schema({
  ts: Number, mode: String, legs: Array, realizedPnl: Number
}, { collection:'trades' }));
const Position = mongoose.model('Position', new mongoose.Schema({
  userId: String, instrumentId: String, qty: Number, avgPx: Number, mode: String
}, { collection:'positions' }));

function updatePosition(pos, qtyDelta, px){
  const newQty = (pos.qty||0) + qtyDelta;
  if (newQty === 0) return { qty:0, avgPx:0 };
  const notional = (pos.avgPx||0) * (pos.qty||0);
  const addNotional = px * qtyDelta;
  const avgPx = (notional + addNotional) / newQty;
  return { qty:newQty, avgPx };
}

async function consumeTrades(){
  const res = await redis.xreadgroup('GROUP', group, consumer, 'BLOCK', 1000, 'COUNT', 20, 'STREAMS', 'arb.trades', '>');
  if (!res) return;
  for (const [stream, messages] of res){
    for (const [id, fields] of messages){
      const t = JSON.parse(fields[1]);
      const trade = new Trade(t);
      await trade.save();
      tradeCounter.inc();
      // naive position update: BUY +size, SELL -size at px
      for (const leg of (t.legs||[])){
        const side = (leg.side||'').toUpperCase();
        const qty = side === 'BUY' ? (leg.size||0) : -(leg.size||0);
        const instrumentId = leg.instrumentId || 'UNKNOWN';
        const mode = t.mode || 'paper';
        let pos = await Position.findOne({ userId:'demo', instrumentId, mode });
        if (!pos) pos = new Position({ userId:'demo', instrumentId, qty:0, avgPx:0, mode });
        const upd = updatePosition(pos, qty, leg.estPx||0);
        pos.qty = upd.qty; pos.avgPx = upd.avgPx;
        await pos.save();
      }
      await redis.xack(stream, group, id);
    }
  }
}
setInterval(consumeTrades, 200);

const PORT = process.env.PORT || 9401;
app.get('/healthz', (req,res)=>res.json({status:'ok'}));
app.get('/metrics', async (req,res)=>{
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});
app.listen(PORT, ()=>console.log('portfolio-accounting on', PORT));
