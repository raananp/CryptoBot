// services/order-simulator/src/index.js
import express from 'express';
import client from 'prom-client';
import Redis from 'ioredis';
import mongoose from 'mongoose';

const app = express();

// ---------- metrics ----------
const register = new client.Registry();
client.collectDefaultMetrics({ register });
const fillsCounter = new client.Counter({
  name: 'fills_total',
  help: 'Order fills generated (paper)',
});
register.registerMetric(fillsCounter);

// ---------- redis ----------
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const redis = new Redis(REDIS_URL);

// Consumer group for orders.new
const GROUP = 'sim';
const CONSUMER = process.env.HOSTNAME || 'sim-1';

async function ensureGroup(stream) {
  try {
    await redis.xgroup('CREATE', stream, GROUP, '0', 'MKSTREAM');
  } catch (e) {
    // group may already exist -> ignore
  }
}

// Create group for orders.new (where executor publishes orders)
await ensureGroup('orders.new');

// ---------- mongo (optional; not required for fills) ----------
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongodb:27017/arb';
try {
  await mongoose.connect(MONGO_URL);
  console.log('[order-simulator] Mongo connected.');
} catch (e) {
  console.warn('[order-simulator] Mongo connect failed:', e?.message);
}

// ---------- helpers ----------
function readJsonFromFields(fields) {
  // fields is an array like: ['data', '{...json...}', 'maybeOtherField', '...']
  // We prefer the value next to the 'data' key, fallback to `[1]`.
  if (!Array.isArray(fields)) return null;
  const idx = fields.findIndex((x) => x === 'data');
  const raw = idx >= 0 && fields[idx + 1] ? fields[idx + 1] : fields[1];
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function emitFillFromOrder(ord) {
  // Deterministic, full fill to drive the pipeline
  const size = Number(ord?.payload?.size ?? 1) || 1;
  const fill = {
    id: ord?.id,
    ts: Date.now(),
    type: 'order.fill',
    payload: {
      corrId: ord?.payload?.corrId,
      legIndex: ord?.payload?.legIndex,
      exchange: ord?.payload?.exchange,
      instrumentId: ord?.payload?.instrumentId,
      side: ord?.payload?.side,
      px: ord?.payload?.estPx,
      requestedSize: size,
      filledSize: size, // <— always full to trigger leg 2 reliably
      // optional: mode can be carried if you want trade-assembler to see it
      mode: ord?.payload?.mode,
    },
  };

  await redis.xadd('orders.fills', '*', 'data', JSON.stringify(fill));
  fillsCounter.inc();
  console.log(
    `[order-simulator] fill corrId=${fill.payload.corrId} leg=${fill.payload.legIndex} side=${fill.payload.side} px=${fill.payload.px} size=${fill.payload.filledSize}`
  );
}

// ---------- main loop ----------
async function run() {
  console.log('[order-simulator] consuming orders.new as', GROUP, CONSUMER);
  for (;;) {
    try {
      const res = await redis.xreadgroup(
        'GROUP',
        GROUP,
        CONSUMER,
        'BLOCK',
        2000,
        'COUNT',
        50,
        'STREAMS',
        'orders.new',
        '>'
      );

      if (!res) continue;

      for (const [, messages] of res) {
        for (const [id, fields] of messages) {
          try {
            const ord = readJsonFromFields(fields);
            if (!ord) {
              console.warn('[order-simulator] parse failed; acking', id);
              await redis.xack('orders.new', GROUP, id);
              continue;
            }

            // Emit a full fill
            await emitFillFromOrder(ord);
          } catch (e) {
            console.warn('[order-simulator] error processing order:', e?.message);
          } finally {
            // Ack regardless to avoid stuck/pending
            await redis.xack('orders.new', GROUP, id);
          }
        }
      }
    } catch (e) {
      // brief backoff
      await new Promise((r) => setTimeout(r, 300));
    }
  }
}

// ---------- http ----------
app.get('/healthz', async (req, res) => {
  try {
    // show pending for visibility
    const groups = await redis.xinfo('GROUPS', 'orders.new');
    const g = Array.isArray(groups)
      ? groups.find((row, i) => Array.isArray(row) && row[1] === GROUP)
      : null;
    const pending =
      Array.isArray(g) && typeof g[5] === 'number' ? g[5] : undefined;

    res.json({ status: 'ok', pending });
  } catch {
    res.json({ status: 'ok' });
  }
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

const PORT = Number(process.env.PORT || 9304);
app.listen(PORT, () => console.log('[order-simulator] on', PORT));
run().catch((e) => {
  console.error(e);
  process.exit(1);
});