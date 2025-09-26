// frontend-web/src/components/PositionsTable.jsx
import * as React from 'react';
import {
  Card,
  CardContent,
  Chip,
  Grid,
  Typography,
  Box,
} from '@mui/material';
import { TransitionGroup } from 'react-transition-group';
import Collapse from '@mui/material/Collapse';

const fmtNum = (v, d = 6) =>
  v == null || isNaN(v) ? '—' : Number(v).toFixed(d);

function PositionCard({ p }) {
  return (
    <Collapse in timeout={500}>
      <Card sx={{ height: '100%' }}>
        <CardContent sx={{ p: 1.5 }}>
          {/* Header */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
            <Chip size="small" label={p.mode || '—'} />
            <Typography variant="body2" sx={{ fontWeight: 700 }}>
              {p.instrumentId || '—'}
            </Typography>
          </Box>

          {/* Body */}
          <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
              Quantity
            </Typography>
            <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
              {fmtNum(p.qty, 6)}
            </Typography>

            <Typography variant="caption" sx={{ opacity: 0.7 }}>
              Avg Price
            </Typography>
            <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
              {fmtNum(p.avgPx, 6)}
            </Typography>
          </Box>
        </CardContent>
      </Card>
    </Collapse>
  );
}

export default function PositionsTable({ rows = [] }) {
  const list = Array.isArray(rows) ? rows : [];
  // newest-ish: sort by instrument alphabetically then mode, cap to 20
  const sorted = [...list]
    .sort((a, b) => String(a.instrumentId).localeCompare(String(b.instrumentId)))
    .slice(0, 20);

  if (!sorted.length) {
    return (
      <Typography variant="body2" sx={{ opacity: 0.7 }}>
        No positions.
      </Typography>
    );
  }

  return (
    <Grid container spacing={2}>
      <TransitionGroup component={null}>
        {sorted.map((p, i) => (
          <Grid item xs={12} sm={6} md={4} lg={3} key={p._id || p.id || i}>
            <PositionCard p={p} />
          </Grid>
        ))}
      </TransitionGroup>
    </Grid>
  );
}