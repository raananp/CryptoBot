// frontend-web/src/components/OpportunitiesTable.jsx
import * as React from 'react';
import { Box, Chip, Grid, Typography, Divider } from '@mui/material';

// ---- constants (tweak to match your UI)
const ROW_HEIGHT = 56;     // px per row
const HEADER_HEIGHT = 44;  // px for the header labels
const MAX_RENDER_PER_SECTION = 100; // guardrail so DOM doesn't explode

// ---------- helpers ----------
const safeNum = (v, d = 0) => (v == null || isNaN(v) ? d : Number(v));
const fmtTime = (ts) => new Date(ts || Date.now()).toLocaleTimeString();
const fmtPx = (v) =>
  v == null || isNaN(v)
    ? '—'
    : Math.abs(Number(v)) >= 1000
    ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 })
    : Number(v).toFixed(6);
const fmtBps = (v) => (v == null || isNaN(v) ? '—' : Number(v).toFixed(2));
const fmtPnL = (v) =>
  v == null || isNaN(v)
    ? '—'
    : (Math.abs(v) >= 1 ? v.toFixed(4) : v.toFixed(6));

// ---------- derive ----------
function parseCoinFromInstrument(instrumentId = '') {
  if (!instrumentId) return '—';
  const coin = instrumentId.split('-')[0];
  return coin || instrumentId;
}

function derive(o) {
  const p = o?.payload || {};
  const approved = o?.approved === true;

  const legs = Array.isArray(p.legs) ? p.legs.filter(Boolean) : [];
  let buy = null, sell = null;
  for (const l of legs) {
    const s = String(l?.side || '').toUpperCase();
    if (s === 'BUY' && !buy) buy = l;
    if (s === 'SELL' && !sell) sell = l;
  }
  if (!buy && legs[0]) buy = legs[0];
  if (!sell && legs[1]) sell = legs[1];

  const ts = o?.ts ?? Date.now();
  const instrumentId = buy?.instrumentId || sell?.instrumentId || p.instrumentId || '—';
  const coin = parseCoinFromInstrument(instrumentId);
  const mode = p?.paper ? 'paper' : 'live';
  const buyEx = buy?.exchange || '—';
  const sellEx = sell?.exchange || '—';
  const buyPx = safeNum(buy?.estPx, null);
  const sellPx = safeNum(sell?.estPx, null);
  const time = fmtTime(ts);

  // fees/slip/borrow in bps
  const legsFeeBps = legs.map((l) => safeNum(l?.feeBps, 0)).reduce((a, b) => a + b, 0);
  const feesBpsAbs = safeNum(p?.costs?.fees, 0) * 10000;
  const feesBps = legsFeeBps > 0 ? legsFeeBps : feesBpsAbs;
  const slippageBps = safeNum(p?.costs?.slippage, 0) * 10000;
  const borrowBps = safeNum(p?.costs?.borrow, 0) * 10000;
  const totalFeesLikeBps = feesBps + slippageBps + borrowBps;

  // gross edge
  let grossBps = null;
  if (p?.edgeBps != null && !isNaN(p.edgeBps)) {
    grossBps = Number(p.edgeBps);
  } else if (buyPx != null && sellPx != null) {
    const mid = (buyPx + sellPx) / 2;
    if (mid > 0) grossBps = ((sellPx - buyPx) / mid) * 10000;
  }

  const netBps =
    grossBps == null ? null : Number((grossBps - totalFeesLikeBps).toFixed(2));

  // per-unit profit calc
  let perUnitNet = null;
  if (buyPx != null && sellPx != null) {
    const mid = (buyPx + sellPx) / 2;
    const perUnitGross = sellPx - buyPx;
    const perUnitFees = (totalFeesLikeBps / 10000) * mid;
    perUnitNet = perUnitGross - perUnitFees;
  }

  const signature = `${instrumentId}|${buyEx}|${sellEx}|${mode}`; // stable key
  const id = String(o?.id) || `${signature}|${ts}`;               // event id (may change)

  return {
    id,
    signature,
    time,
    mode,
    coin,
    instrumentId,
    buyEx,
    buyPx,
    sellEx,
    sellPx,
    feesBps,
    slippageBps,
    borrowBps,
    grossBps,
    netBps,
    perUnitNet,
    isProfit: perUnitNet != null && perUnitNet > 0,
    approved,
    ts,
  };
}

// ---------- row (no animation, fixed height) ----------
function Row({ r }) {
  const pnlPos = r.perUnitNet != null && r.perUnitNet > 0;
  return (
    <Box
      sx={{
        display: 'grid',
        gridTemplateColumns: {
          xs: '1.2fr 1.4fr 1fr 1fr 0.9fr 0.7fr 0.9fr',
          md: '1.2fr 1.6fr 0.9fr 0.9fr 0.9fr 0.7fr 0.9fr',
        },
        alignItems: 'center',
        gap: 1,
        px: 1,
        py: 0.75,
        minHeight: ROW_HEIGHT,
        borderBottom: '1px solid',
        borderColor: 'divider',
        '&:last-child': { borderBottom: 'none' },
        bgcolor: 'background.paper',
        '&:hover': { bgcolor: 'action.hover' },
      }}
    >
      <Box sx={{ minWidth: 0 }}>
        <Typography noWrap variant="body2" sx={{ fontWeight: 600 }}>
          {r.instrumentId}
        </Typography>
        <Typography noWrap variant="caption" sx={{ opacity: 0.7 }}>
          {r.coin}
        </Typography>
      </Box>
      <Box sx={{ fontFamily: 'monospace' }}>
        {r.buyEx}:{fmtPx(r.buyPx)} → {r.sellEx}:{fmtPx(r.sellPx)}
      </Box>
      <Box sx={{ fontFamily: 'monospace' }}>edge={fmtBps(r.grossBps)}</Box>
      <Box sx={{ fontFamily: 'monospace' }}>net={fmtBps(r.netBps)}</Box>
      <Box
        sx={{
          fontFamily: 'monospace',
          fontWeight: 700,
          color: pnlPos ? 'success.main' : 'error.main',
        }}
      >
        {fmtPnL(r.perUnitNet)}
      </Box>
      <Box>
        <Chip size="small" label={r.mode} />
      </Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, justifySelf: 'end' }}>
        {r.approved ? (
          <Chip size="small" color="success" variant="outlined" label="approved" />
        ) : (
          <Chip size="small" color="warning" variant="outlined" label="unreviewed" />
        )}
        <Typography variant="caption" sx={{ opacity: 0.7 }}>
          {r.time}
        </Typography>
      </Box>
    </Box>
  );
}

// ---------- header ----------
function HeaderRow() {
  return (
    <Box sx={{ position: 'sticky', top: 0, zIndex: 1, bgcolor: 'background.default' }}>
      <Box
        sx={{
          display: 'grid',
          gridTemplateColumns: {
            xs: '1.2fr 1.4fr 1fr 1fr 0.9fr 0.7fr 0.9fr',
            md: '1.2fr 1.6fr 0.9fr 0.9fr 0.9fr 0.7fr 0.9fr',
          },
          gap: 1,
          px: 1,
          py: 0.75,
          minHeight: HEADER_HEIGHT,
        }}
      >
        {['Instrument', 'Route', 'Edge (bps)', 'Net (bps)', 'PnL / unit', 'Mode', 'Status / Time'].map(
          (h) => (
            <Typography
              key={h}
              variant="caption"
              sx={{ textTransform: 'uppercase', letterSpacing: 0.5, opacity: 0.7 }}
            >
              {h}
            </Typography>
          )
        )}
      </Box>
      <Divider />
    </Box>
  );
}

// ---------- section (fixed height, scrollable) ----------
function Section({ title, items }) {
  const sectionHeight = HEADER_HEIGHT + ROW_HEIGHT * 4; // show 4 rows; scroll for the rest

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        {title}
      </Typography>

      <Box
        sx={{
          height: sectionHeight,
          overflowY: 'auto',
          pr: 1,
          borderRadius: 1,
          border: '1px solid',
          borderColor: 'divider',
          bgcolor: 'background.paper',
        }}
      >
        <HeaderRow />
        {/* render with stable keys; no animations */}
        {items.slice(0, MAX_RENDER_PER_SECTION).map((r) => (
          <Row key={r.signature} r={r} />
        ))}
      </Box>
    </Box>
  );
}

// ---------- main ----------
export default function OpportunitiesTable({ rows = [] }) {
  const incoming = Array.isArray(rows) ? rows.filter(Boolean) : [];
  const [latestBySig, setLatestBySig] = React.useState(new Map());

  // Deduplicate by signature and keep only newest per signature
  React.useEffect(() => {
    if (!incoming.length) return;
    setLatestBySig((prev) => {
      const next = new Map(prev);
      for (const raw of incoming) {
        const d = derive(raw);
        const old = next.get(d.signature);
        if (!old || (d.ts ?? 0) > (old.ts ?? 0)) next.set(d.signature, d);
      }
      return next;
    });
  }, [incoming]);

  const all = React.useMemo(
    () => [...latestBySig.values()].sort((a, b) => (b.ts ?? 0) - (a.ts ?? 0)),
    [latestBySig]
  );

  // groups (we render all but only 4 are visible due to fixed height + scroll)
  const profApproved   = all.filter((c) => c.isProfit &&  c.approved);
  const profUnreviewed = all.filter((c) => c.isProfit && !c.approved);
  const lossUnreviewed = all.filter((c) => !c.isProfit && !c.approved);

  return (
    <Grid container direction="column" spacing={2} sx={{ width: '100%', m: 0 }}>
      <Grid item xs={12}>
        <Section title="Profitable • Approved" items={profApproved} />
      </Grid>
      <Grid item xs={12}>
        <Section title="Profitable • Unreviewed" items={profUnreviewed} />
      </Grid>
      <Grid item xs={12}>
        <Section title="Not Profitable • Unreviewed" items={lossUnreviewed} />
      </Grid>
    </Grid>
  );
}