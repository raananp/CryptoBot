// services/risk-slippage-engine/src/index.js
import express from 'express';
import client from 'prom-client';
import Redis from 'ioredis';

const app = express();

// ---------- metrics ----------
const register = new client.Registry();
client.collectDefaultMetrics({ register });
const approvedCounter = new client.Counter({
  name: 'approved_total',
  help: 'Approved opportunities',
});
const rejectedCounter = new client.Counter({
  name: 'rejected_total',
  help: 'Rejected opportunities',
});
register.registerMetric(approvedCounter);
register.registerMetric(rejectedCounter);

// ---------- redis ----------
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const redis = new Redis(REDIS_URL);

// Streams & consumer group
const GROUP = 'risk';
const CONSUMER = process.env.HOSTNAME || 'risk-1';
const INPUT_STREAM = process.env.INPUT_STREAM || 'scanner.to.risk';
const OUTPUT_STREAM = process.env.OUTPUT_STREAM || 'arb.approved';

async function ensureGroup(stream, group) {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (e) {
    // ignore BUSYGROUP errors (already exists)
    if (!String(e?.message || '').includes('BUSYGROUP')) {
      console.warn('[risk] xgroup create warn:', e?.message);
    }
  }
}
await ensureGroup(INPUT_STREAM, GROUP);

// ---------- helpers / policy ----------
const toUpper = (s) => String(s || '').toUpperCase();
const safeNum = (v, d = 0) => (v == null || isNaN(v) ? d : Number(v));

// Policy thresholds (override via env)
const EDGE_MIN_BPS     = Number(process.env.RISK_EDGE_MIN_BPS || 20);     // min gross edge
const NET_MIN_BPS      = Number(process.env.RISK_NET_MIN_BPS  || 0);      // optional net-after-costs floor
const MAX_TOTAL_SIZE   = Number(process.env.RISK_MAX_TOTAL_SIZE || 10);   // cap on total requested size
const REQUIRE_BOTH_SIDES = String(process.env.RISK_REQUIRE_BOTH_SIDES || 'true').toLowerCase() === 'true';
const ALLOW_PAPER_ONLY   = String(process.env.RISK_ALLOW_PAPER_ONLY || 'false').toLowerCase() === 'true';

// Return {ok:boolean, reason:string, netBps?:number, totalFeesLikeBps?:number}
function evaluateRisk(opp) {
  const p = opp?.payload || {};
  const mode = p?.paper ? 'paper' : 'live';

  if (!ALLOW_PAPER_ONLY && mode === 'paper') {
    return { ok: false, reason: 'paper_mode_not_allowed' };
  }

  const legs = Array.isArray(p.legs) ? p.legs.filter(Boolean) : [];
  if (REQUIRE_BOTH_SIDES) {
    const hasBuy = legs.some(l => toUpper(l?.side) === 'BUY');
    const hasSell = legs.some(l => toUpper(l?.side) === 'SELL');
    if (!hasBuy || !hasSell) return { ok: false, reason: 'missing_side' };
  }

  // size checks (if sizes are provided)
  const totalSize = legs.reduce((a, b) => a + safeNum(b.size, 0), 0);
  if (totalSize > 0 && totalSize > MAX_TOTAL_SIZE) {
    return { ok: false, reason: 'size_exceeds_cap' };
  }

  // edge / net computation
  const buy = legs.find(l => toUpper(l?.side) === 'BUY');
  const sell = legs.find(l => toUpper(l?.side) === 'SELL');
  const buyPx = safeNum(buy?.estPx, NaN);
  const sellPx = safeNum(sell?.estPx, NaN);

  let grossBps = Number.isFinite(p.edgeBps) ? Number(p.edgeBps) : NaN;
  if (!Number.isFinite(grossBps) && Number.isFinite(buyPx) && Number.isFinite(sellPx)) {
    const mid = (buyPx + sellPx) / 2;
    if (mid > 0) grossBps = ((sellPx - buyPx) / mid) * 10000;
  }

  if (!Number.isFinite(grossBps) || grossBps < EDGE_MIN_BPS) {
    return { ok: false, reason: 'edge_below_threshold' };
  }

  // fees/slippage/borrow (bps-like)
  const legsFeeBps = legs.map(l => safeNum(l?.feeBps, 0)).reduce((a, b) => a + b, 0);
  const feesBpsAbs = safeNum(p?.costs?.fees, 0) * 10000;
  const feesBps = legsFeeBps > 0 ? legsFeeBps : feesBpsAbs;
  const slippageBps = safeNum(p?.costs?.slippage, 0) * 10000;
  const borrowBps   = safeNum(p?.costs?.borrow, 0) * 10000;
  const totalFeesLikeBps = feesBps + slippageBps + borrowBps;

  const netBps = Number((grossBps - totalFeesLikeBps).toFixed(2));
  if (netBps < NET_MIN_BPS) {
    return { ok: false, reason: 'net_below_threshold', netBps, totalFeesLikeBps };
  }

  return { ok: true, reason: 'approved', netBps, totalFeesLikeBps };
}

function buildApproved(opp, evalRes) {
  // tag the record so downstream & UI can require approval
  return {
    ...opp,
    approved: true,
    risk: {
      reason: evalRes?.reason || 'approved',
      netBps: evalRes?.netBps,
      totalFeesLikeBps: evalRes?.totalFeesLikeBps,
      policy: {
        EDGE_MIN_BPS,
        NET_MIN_BPS,
        MAX_TOTAL_SIZE,
        REQUIRE_BOTH_SIDES,
      },
    },
  };
}

// ---------- main loop ----------
async function consume() {
  const res = await redis.xreadgroup(
    'GROUP', GROUP, CONSUMER,
    'BLOCK', 1000,
    'COUNT', 50,
    'STREAMS', INPUT_STREAM, '>'
  );
  if (!res) return;

  for (const [, messages] of res) {
    for (const [id, fields] of messages) {
      try {
        const raw = fields?.[1];
        const opp = JSON.parse(raw);

        const ev = evaluateRisk(opp);
        if (ev.ok) {
          const approved = buildApproved(opp, ev);
          await redis.xadd(OUTPUT_STREAM, '*', 'data', JSON.stringify(approved));
          approvedCounter.inc();
        } else {
          rejectedCounter.inc();
        }
      } catch (e) {
        console.warn('[risk] bad message or approval error:', e?.message);
      } finally {
        // Always ack to avoid poison messages
        try { await redis.xack(INPUT_STREAM, GROUP, id); } catch {}
      }
    }
  }
}

// poller
setInterval(() => {
  consume().catch((e) => console.warn('[risk] consume loop warn:', e?.message));
}, 250);

// ---------- http ----------
const PORT = process.env.PORT || 9302;
app.get('/healthz', (_req, res) => res.json({
  status: 'ok',
  group: GROUP,
  consumer: CONSUMER,
  input: INPUT_STREAM,
  output: OUTPUT_STREAM,
  edgeMinBps: EDGE_MIN_BPS,
  netMinBps: NET_MIN_BPS,
  maxTotalSize: MAX_TOTAL_SIZE,
  requireBothSides: REQUIRE_BOTH_SIDES,
}));
app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});
app.listen(PORT, () => console.log('[risk-slippage-engine] on', PORT));