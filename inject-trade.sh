#!/bin/bash
# Inject a fake trade into Redis stream arb.trades
# Usage: ./inject-trade.sh <instrument> <realizedPnl>

INSTRUMENT=${1:-BTCUSDT}
PNL=${2:-12.34}

docker exec -i deploy-redis-1 redis-cli \
  XADD arb.trades '*' data "$(printf '{"ts":%s,"mode":"paper","legs":[{"exchange":"binance","instrumentId":"%s","side":"BUY"},{"exchange":"bybit","instrumentId":"%s","side":"SELL"}],"realizedPnl":%s}' $(date +%s000) "$INSTRUMENT" "$INSTRUMENT" "$PNL")"