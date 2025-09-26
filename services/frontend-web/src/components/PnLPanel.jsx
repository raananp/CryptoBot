
import * as React from 'react';
import { useMemo } from 'react';
import { Typography } from '@mui/material';

export default function PnLPanel({ initial, events }){
  const realizedLive = useMemo(()=>{
    const trades = events.filter(e=>e.stream==='arb.trades').map(e=>e.data);
    const sum = trades.reduce((a,t)=> a + (t.realizedPnl||0), 0);
    return sum;
  }, [events]);

  const total = (initial?.realized||0) + realizedLive;
  return <>
    <Typography variant="body1">Realized PnL (session): <b>{total.toFixed(4)}</b></Typography>
    <Typography variant="body2" sx={{opacity:0.7}}>Calculated from trade stream; extend with daily/weekly charts.</Typography>
  </>
}
