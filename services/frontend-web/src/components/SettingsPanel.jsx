// frontend-web/src/components/SettingsPanel.jsx
import * as React from 'react';
import { useState, useEffect, useRef } from 'react';
import {
  Card, CardContent, Typography, Switch, FormControlLabel, Stack, Divider,
  Checkbox, FormGroup, TextField, Button, Chip
} from '@mui/material';

export default function SettingsPanel({ apiBase = '' }) {
  const [autoTrade, setAutoTrade] = useState(false);
  const [mode, setMode] = useState('paper');
  const [saving, setSaving] = useState(false);

  // authoritative snapshot from backend
  const [backendAutoTrade, setBackendAutoTrade] = useState(false);
  const [backendMode, setBackendMode] = useState('paper');

  const [strategies, setStrategies] = useState({ crossVenue: true, parity: true, calendar: false });
  const [simSize, setSimSize] = useState(2);
  const [simDepth, setSimDepth] = useState('100@0.045, 50@0.046, 25@0.047');

  const saveTimer = useRef(null);

  const loadToggles = async () => {
    const r = await fetch(apiBase + '/toggles');
    const d = await r.json();
    setBackendAutoTrade(!!d.autoTrade);
    setBackendMode(d.mode || 'paper');
    // sync UI to backend truth
    setAutoTrade(!!d.autoTrade);
    setMode(d.mode || 'paper');
  };

  useEffect(() => {
    let mounted = true;
    loadToggles().catch(() => {});
    // keep UI honest if something else flips the toggle
    const id = setInterval(() => { if (mounted) loadToggles().catch(() => {}); }, 3000);
    return () => { mounted = false; clearInterval(id); };
  }, []);

  const postToggles = async (next) => {
    setSaving(true);
    try {
      const r = await fetch(apiBase + '/toggles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(next),
      });
      const d = await r.json();
      // reflect what actually got saved
      setBackendAutoTrade(!!d.autoTrade);
      setBackendMode(d.mode || 'paper');
      setAutoTrade(!!d.autoTrade);
      setMode(d.mode || 'paper');
    } catch (e) {
      console.error('Failed to save toggles', e);
    } finally {
      setSaving(false);
    }
  };

  // Debounced autosave whenever switches change
  useEffect(() => {
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      postToggles({ autoTrade, mode });
    }, 250);
    return () => { if (saveTimer.current) clearTimeout(saveTimer.current); };
  }, [autoTrade, mode]); // autosave on changes

  // strategies (unchanged behavior)
  useEffect(() => {
    fetch(apiBase + '/strategies')
      .then(r => r.json())
      .then(d => {
        setStrategies({
          crossVenue: !!d.crossVenue,
          parity: !!d.parity,
          calendar: !!d.calendar
        });
      })
      .catch(() => {});
  }, [apiBase]);

  const saveStrategies = async () => {
    try {
      await fetch(apiBase + '/strategies', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(strategies),
      });
    } catch (e) {
      console.error('Failed to save strategies', e);
    }
  };

  const computeSlippage = () => {
    const levels = simDepth.split(',').map(s => s.trim()).filter(Boolean).map(x => {
      const [q, p] = x.split('@').map(v => v.trim());
      return { qty: parseFloat(q), px: parseFloat(p) };
    }).filter(l => !isNaN(l.qty) && !isNaN(l.px));
    let remaining = parseFloat(simSize);
    let cost = 0, filled = 0;
    for (const lvl of levels) {
      const take = Math.min(remaining, lvl.qty);
      cost += take * lvl.px;
      filled += take;
      remaining -= take;
      if (remaining <= 0) break;
    }
    const vwap = filled ? cost / filled : 0;
    return { filled, vwap, remaining };
  };

  const result = computeSlippage();
  const unsaved = backendAutoTrade !== autoTrade || backendMode !== mode;

  return (
    <Stack spacing={2}>
      <Card>
        <CardContent>
          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
            <Typography variant="h6" sx={{ flex: 1 }}>Trading Toggles</Typography>
            <Chip
              size="small"
              label={`Backend: ${backendMode} / ${backendAutoTrade ? 'auto' : 'manual'}`}
              color={backendAutoTrade ? 'success' : 'default'}
              variant="outlined"
            />
            {unsaved && <Chip size="small" color="warning" label={saving ? 'Saving…' : 'Syncing…'} />}
          </Stack>

          <FormGroup row>
            <FormControlLabel
              control={<Switch checked={autoTrade} onChange={e => setAutoTrade(e.target.checked)} />}
              label={`Auto-Trade: ${autoTrade ? 'ON' : 'OFF'}`}
            />
            <FormControlLabel
              control={<Switch checked={mode === 'live'} onChange={e => setMode(e.target.checked ? 'live' : 'paper')} />}
              label={`Mode: ${mode}`}
            />
          </FormGroup>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>Strategies</Typography>
          <FormGroup row>
            <FormControlLabel control={<Checkbox checked={strategies.crossVenue} onChange={e => setStrategies(s => ({ ...s, crossVenue: e.target.checked }))} />} label="Cross-Venue Same Contract" />
            <FormControlLabel control={<Checkbox checked={strategies.parity} onChange={e => setStrategies(s => ({ ...s, parity: e.target.checked }))} />} label="Put/Call Parity" />
            <FormControlLabel control={<Checkbox checked={strategies.calendar} onChange={e => setStrategies(s => ({ ...s, calendar: e.target.checked }))} />} label="Calendar Spread" />
          </FormGroup>
          <Button variant="contained" onClick={saveStrategies} sx={{ mt: 1 }}>Save Strategies</Button>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>Slippage Simulator (Book Sweep)</Typography>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField label="Order Size" type="number" value={simSize} onChange={e => setSimSize(e.target.value)} />
            <TextField fullWidth label="Depth (qty@price, ...)" value={simDepth} onChange={e => setSimDepth(e.target.value)} />
          </Stack>
          <Divider sx={{ my: 2 }} />
          <Typography variant="body1">
            Filled: <b>{result.filled}</b>, VWAP: <b>{result.vwap.toFixed(6)}</b>, Remaining: <b>{result.remaining}</b>
          </Typography>
        </CardContent>
      </Card>
    </Stack>
  );
}