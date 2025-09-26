
import express from 'express';
import client from 'prom-client';

const PORT=process.env.PORT||9405;
const register=new client.Registry();
client.collectDefaultMetrics({register});
const upGauge=new client.Gauge({name:'service_up', help:'service up=1 down=0', labelNames:['service']});
register.registerMetric(upGauge);

const SERVICES=[
  {name:'api-gateway', url:'http://api-gateway:8080/healthz'},
  {name:'exchange-adapter-mock', url:'http://exchange-adapter-mock:9101/healthz'},
  {name:'scanner-options-arb', url:'http://scanner-options-arb:9301/healthz'},
  {name:'risk-slippage-engine', url:'http://risk-slippage-engine:9302/healthz'},
  {name:'order-simulator', url:'http://order-simulator:9304/healthz'}
];

async function poll(){
  for (const s of SERVICES){
    try{
      const r=await fetch(s.url,{timeout:1500});
      upGauge.set({service:s.name}, r.ok?1:0);
    }catch(e){ upGauge.set({service:s.name},0); }
  }
}
setInterval(poll, 2000); poll();

const app=express();
app.get('/healthz',(req,res)=>res.json({status:'ok'}));
app.get('/metrics', async (req,res)=>{
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});
app.listen(PORT, ()=>console.log('health-aggregator on', PORT));
