// frontend-web/src/App.jsx
import * as React from 'react';
import { useEffect, useMemo, useState } from 'react';
import { ThemeProvider } from '@mui/material/styles';
import theme from './theme';
import {
  AppBar,
  Toolbar,
  Typography,
  Container,
  Tabs,
  Tab,
  Card,
  CardContent,
  Stack,
  IconButton,
  Tooltip,
  Badge,           // <- NEW
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import HealthAndSafetyIcon from '@mui/icons-material/HealthAndSafety';
import InsightsIcon from '@mui/icons-material/Insights';
import ReceiptLongIcon from '@mui/icons-material/ReceiptLong';
import SettingsIcon from '@mui/icons-material/Settings';
import AccountBalanceIcon from '@mui/icons-material/AccountBalance';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';

import useWs from './hooks/useWs';
import OpportunitiesTable from './components/OpportunitiesTable';
import TradesTable from './components/TradesTable';
import SettingsPanel from './components/SettingsPanel';
import PositionsTable from './components/PositionsTable';
import PnLPanel from './components/PnLPanel';

// --- Dynamic API/WS hosts ---
const API = `${window.location.protocol}//${window.location.hostname}:8080`;
const WS =
  `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.hostname}:8080/ws`;

function useFetch(url, deps = []) {
  const [data, setData] = useState(null);
  useEffect(() => {
    let alive = true;
    fetch(url)
      .then((r) => r.json())
      .then((d) => { if (alive) setData(d); })
      .catch(() => setData(null));
    return () => { alive = false; };
  }, deps);
  return data;
}

export default function App() {
  const [tab, setTab] = useState(0);

  // WebSocket events
  const events = useWs(WS);
  const oppEvents = events.filter((e) => e.stream === 'arb.opportunities').map((e) => e.data);
  const tradeEventsAll = events.filter((e) => e.stream === 'arb.trades').map((e) => e.data);

  // Only count trades that were actually taken/executed (your executor sets taken: true)
  const tradeEvents = useMemo(
    () => tradeEventsAll.filter((t) => t && t.taken === true),
    [tradeEventsAll]
  );

  // Initial data fetches
  const oppInit = useFetch(API + '/opportunities', []);
  const tradesInit = useFetch(API + '/trades?mode=paper', []); // server already filters to taken: true
  const health = useFetch(API + '/healthz', [tab]);
  const positionsInit = useFetch(API + '/positions?mode=paper', []);

  // Combine initial + live opportunities
  const oppRows = useMemo(() => {
    const base = Array.isArray(oppInit) ? oppInit : [];
    return [...oppEvents, ...base].slice(0, 200);
  }, [oppInit, oppEvents]);

  // PnL init from initial trades
  const pnlInit = useMemo(() => {
    const base = Array.isArray(tradesInit) ? tradesInit : [];
    const sum = base.reduce((a, t) => a + (t.realizedPnl || 0), 0);
    return { realized: sum };
  }, [tradesInit]);

  // Trades rows (live + initial)
  const tradeRows = useMemo(() => {
    const base = Array.isArray(tradesInit) ? tradesInit : [];
    return [...tradeEvents, ...base].slice(0, 200);
  }, [tradesInit, tradeEvents]);

  // ---- BADGES: counts ----
  // Trades count = initial taken trades + number of live taken trade events
  const initialTradesCount = useMemo(
    () => (Array.isArray(tradesInit) ? tradesInit.filter((t) => t?.taken === true).length : 0),
    [tradesInit]
  );
  const tradesCount = initialTradesCount + tradeEvents.length;

  // Positions count (non-zero qty)
  const [positionsCount, setPositionsCount] = useState(
    Array.isArray(positionsInit)
      ? positionsInit.filter((p) => Number(p?.qty || 0) !== 0).length
      : 0
  );

  // Recompute on initial positions load
  useEffect(() => {
    if (Array.isArray(positionsInit)) {
      setPositionsCount(positionsInit.filter((p) => Number(p?.qty || 0) !== 0).length);
    }
  }, [positionsInit]);

  // Refresh positions every time a new trade arrives (keeps badge in sync)
  useEffect(() => {
    let aborted = false;
    async function refreshPositions() {
      try {
        const r = await fetch(API + '/positions?mode=paper');
        const d = await r.json();
        if (!aborted && Array.isArray(d)) {
          setPositionsCount(d.filter((p) => Number(p?.qty || 0) !== 0).length);
        }
      } catch {/* ignore */}
    }
    if (tradeEvents.length > 0) refreshPositions();
    return () => { aborted = true; };
  }, [tradeEvents.length]);

  // --- Debug (optional) ---
  useEffect(() => {
    console.log('[App] API =', API, 'WS =', WS);
  }, []);

  return (
    <ThemeProvider theme={theme}>
      <AppBar position="sticky" elevation={1}>
        <Toolbar>
          <Typography variant="h6" sx={{ flex: 1 }}>
            Arbitrage Bot
          </Typography>
          <Tooltip title="Refresh">
            <IconButton color="inherit" onClick={() => window.location.reload()}>
              <RefreshIcon />
            </IconButton>
          </Tooltip>
        </Toolbar>

        <Tabs
          value={tab}
          onChange={(_, v) => setTab(v)}
          indicatorColor="secondary"
          textColor="inherit"
          centered
        >
          <Tab icon={<InsightsIcon />} label="OPPORTUNITIES" />

          {/* TRADES with live badge */}
          <Tab
            icon={
              <Badge
                badgeContent={tradesCount}
                color="secondary"
                showZero={false}
                max={999}
                overlap="circular"
              >
                <ReceiptLongIcon />
              </Badge>
            }
            label="TRADES"
          />

          {/* POSITIONS with live badge */}
          <Tab
            icon={
              <Badge
                badgeContent={positionsCount}
                color="secondary"
                showZero={false}
                max={999}
                overlap="circular"
              >
                <AccountBalanceIcon />
              </Badge>
            }
            label="POSITIONS"
          />

          <Tab icon={<TrendingUpIcon />} label="PNL" />
          <Tab icon={<HealthAndSafetyIcon />} label="HEALTH" />
          <Tab icon={<SettingsIcon />} label="SETTINGS" />
        </Tabs>
      </AppBar>

      <Container maxWidth={false} disableGutters sx={{ py: 3, px: 0 }}>
        {tab === 0 && (
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Live Opportunities (readable feed)
              </Typography>
              <OpportunitiesTable rows={oppRows} />
            </CardContent>
          </Card>
        )}

        {tab === 1 && (
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Trades (Paper)
              </Typography>
              <TradesTable rows={tradeRows} />
            </CardContent>
          </Card>
        )}

        {tab === 2 && (
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Positions
              </Typography>
              <PositionsTable rows={Array.isArray(positionsInit) ? positionsInit : []} />
            </CardContent>
          </Card>
        )}

        {tab === 3 && (
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                PnL
              </Typography>
              <PnLPanel initial={pnlInit} events={tradeEvents} />
            </CardContent>
          </Card>
        )}

        {tab === 4 && (
          <Stack spacing={2}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Service Health
                </Typography>
                <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>
                  {JSON.stringify(health, null, 2)}
                </pre>
                <Typography variant="body2" sx={{ opacity: 0.7, mt: 1 }}>
                  See Grafana for full metrics.
                </Typography>
              </CardContent>
            </Card>
          </Stack>
        )}

        {tab === 5 && (
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Settings
              </Typography>
              <SettingsPanel apiBase={API} />
            </CardContent>
          </Card>
        )}
      </Container>
    </ThemeProvider>
  );
}