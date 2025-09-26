#!/bin/bash
# e2e-approved.sh — drive full pipeline through arb.approved
# Requires: router-executor, order-simulator, trade-assembler up

set -euo pipefail

echo "[1] Ensure AUTO_TRADE=false"
docker exec -it deploy-redis-1 redis-cli SET toggles:autoTrade false >/dev/null
docker exec -it deploy-redis-1 redis-cli GET toggles:autoTrade

echo "[2] Health checks"
curl -s http://localhost:9303/healthz | jq . || true   # executor
curl -s http://localhost:9304/healthz | jq . || true   # simulator
curl -s http://localhost:9305/healthz | jq . || true   # assembler

echo "[3] Inject APPROVED opportunity"
RID=$(docker exec -i deploy-redis-1 redis-cli XADD arb.approved '*' data \
"$(printf '{"id":"opp-%s","ts":%s,"payload":{"paper":true,"edgeBps":250,"legs":[{"exchange":"binance","instrumentId":"BTCUSDT","side":"BUY","estPx":100,"size":1},{"exchange":"bybit","instrumentId":"BTCUSDT","side":"SELL","estPx":101,"size":1}]}}' "$RANDOM" "$(date +%s000)")")
echo "arb.approved id: $RID"

echo "[4] Give it a moment to flow…"
sleep 2

echo "[5] Show executor metrics"
curl -s http://localhost:9303/metrics | egrep 'executor_.*_total' || true

echo "[6] Show orders & fills"
docker exec -it deploy-redis-1 redis-cli XRANGE orders.new - + | tail -n 6
docker exec -it deploy-redis-1 redis-cli XRANGE orders.fills - + | tail -n 6

echo "[7] Show assembled trades (what UI reads)"
docker exec -it deploy-redis-1 redis-cli XRANGE arb.trades - + | tail -n 6

echo "[8] Check Mongo (trades tab API also reads this)"
docker exec -it deploy-mongodb-1 mongosh arb --quiet --eval 'db.trades.find().sort({ts:-1}).limit(3)'
