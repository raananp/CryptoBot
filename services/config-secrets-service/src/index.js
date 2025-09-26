
import express from 'express';
import client from 'prom-client';

const app=express();
app.use(express.json());
const register=new client.Registry();
client.collectDefaultMetrics({register});

// In-memory stub storage; replace with KMS-backed at prod
const store=new Map();

app.post('/keys', (req,res)=>{
  const { userId, exchange, apiKey, apiSecret } = req.body||{};
  if(!userId||!exchange||!apiKey||!apiSecret) return res.status(400).json({error:'missing fields'});
  const key=`${userId}:${exchange}`;
  store.set(key, { apiKey, apiSecret, ts:Date.now() });
  res.json({ ok:true });
});
app.get('/keys', (req,res)=>{
  const { userId, exchange } = req.query || {};
  const key=`${userId}:${exchange}`;
  const val=store.get(key);
  if(!val) return res.status(404).json({error:'not found'});
  res.json(val);
});
app.get('/healthz',(req,res)=>res.json({status:'ok'}));
app.get('/metrics', async (req,res)=>{
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.listen(9402, ()=>console.log('config-secrets-service on 9402'));
