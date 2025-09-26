
import express from 'express';
import client from 'prom-client';
import { createRedis, xadd } from './redis.js';

const app = express();
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const oppCounter = new client.Counter({ name: 'adapter_mock_opportunities_total', help: 'Total opps published' });
register.registerMetric(oppCounter);

const PORT = process.env.PORT || 9101;
const redis = createRedis();

app.get('/healthz', (req,res)=>res.json({status:'ok'}));
app.get('/metrics', async (req,res)=>{
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

const coins = ['BTC','ETH','SOL','BNB','ADA','XRP','DOGE','OP','ARB','ATOM'];

function rnd(n){ return Math.random()*n; }
function pick(arr){ return arr[Math.floor(Math.random()*arr.length)]; }

// Periodically publish a synthetic opportunity for demo
setInterval(async ()=>{
  const base = pick(coins);
  const strike = (Math.floor(rnd(50))*50) + 500; // simplified
  const opp = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    type: 'opportunity',
    payload: {
      legs: [
        { exchange:'deribit', instrumentId:`${base}-27SEP-${strike}-C`, side:'BUY', estPx: +(0.045 + rnd(0.01)).toFixed(4), size: 1 + Math.floor(rnd(3)) },
        { exchange:'okx',     instrumentId:`${base}-27SEP-${strike}-C`, side:'SELL', estPx: +(0.046 + rnd(0.01)).toFixed(4), size: 1 + Math.floor(rnd(3)) }
      ],
      edgeBps: Math.floor(10 + rnd(90)),
      costs: { fees: 0.0002, slippage: 0.0005, borrow: 0 },
      paper: true
    },
    labels: { service:'adapter-mock', mode:'paper' }
  };
  await xadd(redis, 'arb.opportunities', opp);
  oppCounter.inc();
}, 1200);

app.listen(PORT, ()=> console.log('exchange-adapter-mock on', PORT));
