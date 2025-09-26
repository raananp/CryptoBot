// frontend-web/src/components/TradesTable.jsx
import * as React from 'react';
import {
  Box,
  Card,
  CardContent,
  Chip,
  Grid,
  Typography,
} from '@mui/material';
import { TransitionGroup } from 'react-transition-group';
import Collapse from '@mui/material/Collapse';

const fmtTime = (ts) => new Date(ts || Date.now()).toLocaleTimeString();
const fmtNum = (v, d = 4) =>
  v == null || isNaN(v) ? '—' : Number(v).toFixed(d);

function TradeCard({ t }) {
  const pnl = Number(t?.realizedPnl ?? 0);
  const isProfit = pnl > 0;

  return (
    <Collapse in timeout={500}>
      <Card
        sx={{
          height: '100%',
          borderLeft: `6px solid ${isProfit ? '#1b5e20' : pnl < 0 ? '#b71c1c' : '#546e7a'}`,
          mb: 1,
        }}
      >
        <CardContent sx={{ p: 1.5 }}>
          {/* Header */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
            <Chip size="small" label={t.mode || '—'} />
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
              {t.id || 'trade'}
            </Typography>
            <Box sx={{ flex: 1 }} />
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
              {fmtTime(t.ts)}
            </Typography>
          </Box>

          {/* Legs */}
          {Array.isArray(t.legs) && t.legs.length > 0 ? (
            <>
              <Typography variant="caption" sx={{ opacity: 0.7 }}>
                Legs
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.25 }}>
                {t.legs.map((l, i) => (
                  <Chip
                    key={i}
                    size="small"
                    variant="outlined"
                    label={`${l?.exchange || '—'}:${l?.instrumentId || '—'}:${String(l?.side || '').toUpperCase()}`}
                  />
                ))}
              </Box>
            </>
          ) : (
            <Typography variant="body2" sx={{ opacity: 0.7 }}>
              No legs
            </Typography>
          )}

          {/* PnL */}
          <Box sx={{ mt: 1 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
              Realized PnL
            </Typography>
            <Typography
              variant="body2"
              sx={{
                fontFamily: 'monospace',
                fontWeight: 700,
                color: isProfit ? '#1b5e20' : pnl < 0 ? '#b71c1c' : 'inherit',
              }}
            >
              {fmtNum(pnl, 6)}
            </Typography>
          </Box>
        </CardContent>
      </Card>
    </Collapse>
  );
}

export default function TradesTable({ rows = [] }) {
  const list = Array.isArray(rows) ? rows : [];
  // newest → oldest; cap to 20
  const sorted = [...list]
    .sort((a, b) => (b?.ts ?? 0) - (a?.ts ?? 0))
    .slice(0, 20);

  if (!sorted.length) {
    return (
      <Typography variant="body2" sx={{ opacity: 0.7 }}>
        No trades yet.
      </Typography>
    );
  }

  return (
    <Grid container spacing={2}>
      <Grid item xs={12}>
        <TransitionGroup>
          {sorted.map((t, i) => (
            <TradeCard key={t.id || i} t={t} />
          ))}
        </TransitionGroup>
      </Grid>
    </Grid>
  );
}