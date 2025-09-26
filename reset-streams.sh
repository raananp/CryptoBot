#!/usr/bin/env bash
set -euo pipefail

# Container name for Redis in your compose
REDIS_CONTAINER=${REDIS_CONTAINER:-deploy-redis-1}

# If REPLAY_ALL=1, groups will be created at '0' (replay everything).
# Otherwise we create at '$' (only new entries after creation).
START_ID='$'
if [[ "${REPLAY_ALL:-0}" == "1" ]]; then
  START_ID='0'
fi

echo "[reset] Using Redis container: ${REDIS_CONTAINER}"
echo "[reset] Group start ID: ${START_ID}  (${START_ID=='$' && echo 'new only' || echo 'replay all'})"

# Stream -> Group mappings across services
# - executor reads: arb.opportunities OR arb.approved (depending on toggle), and orders.fills
# - risk engine reads: scanner.to.risk
# - simulator reads: orders.new
# - trade-assembler reads: orders.fills
declare -a MAP=(
  "scanner.to.risk:risk"
  "arb.opportunities:executor"
  "arb.approved:executor"
  "orders.new:sim"
  "orders.fills:executor"
  "orders.fills:asm"
)

xgroup_destroy() {
  local stream="$1" group="$2"
  docker exec -i "$REDIS_CONTAINER" redis-cli XGROUP DESTROY "$stream" "$group" >/dev/null 2>&1 || true
}

xgroup_create() {
  local stream="$1" group="$2"
  # MKSTREAM creates the stream if it doesn't exist
  docker exec -i "$REDIS_CONTAINER" redis-cli XGROUP CREATE "$stream" "$group" "$START_ID" MKSTREAM >/dev/null
}

echo "[reset] Destroying existing groups (ignore errors if they don't exist)…"
for pair in "${MAP[@]}"; do
  IFS=':' read -r stream group <<< "$pair"
  xgroup_destroy "$stream" "$group"
done

echo "[reset] Creating groups…"
for pair in "${MAP[@]}"; do
  IFS=':' read -r stream group <<< "$pair"
  xgroup_create "$stream" "$group"
  echo "  - ${stream} :: ${group}"
done

echo "[reset] Current group status:"
for pair in "${MAP[@]}"; do
  IFS=':' read -r stream group <<< "$pair"
  echo "---- $stream"
  docker exec -i "$REDIS_CONTAINER" redis-cli XINFO GROUPS "$stream" || true
done

cat <<'DONE'

[reset] Done.

Next steps:
1) Publish a fresh test opportunity (example for AUTO_TRADE=true):
   docker exec -i deploy-redis-1 redis-cli XADD arb.opportunities '*' data \
   "$(printf '{"id":"force-%s","ts":%s,"approved":true,"payload":{"paper":true,"edgeBps":250,"legs":[{"exchange":"binance","instrumentId":"BTCUSDT","side":"BUY","estPx":100,"size":1},{"exchange":"bybit","instrumentId":"BTCUSDT","side":"SELL","estPx":101,"size":1}]}}' "$RANDOM" "$(date +%s000)")"

2) Or if AUTO_TRADE=false, publish to arb.approved:
   docker exec -i deploy-redis-1 redis-cli XADD arb.approved '*' data \
   "$(printf '{"id":"approved-%s","ts":%s,"approved":true,"payload":{"paper":true,"edgeBps":250,"legs":[{"exchange":"binance","instrumentId":"BTCUSDT","side":"BUY","estPx":100,"size":1},{"exchange":"bybit","instrumentId":"BTCUSDT","side":"SELL","estPx":101,"size":1}]}}' "$RANDOM" "$(date +%s000)")"

3) Watch executor metrics tick up:
   curl -s http://localhost:9303/metrics | egrep 'executor_.*_total'

4) Orders/fills/trades should now flow:
   docker exec -it deploy-redis-1 redis-cli XRANGE orders.new - +
   docker exec -it deploy-redis-1 redis-cli XRANGE orders.fills - +
   docker exec -it deploy-redis-1 redis-cli XRANGE arb.trades - + | tail -n 5

If you ever want to replay all old messages to a fresh group, run:
  REPLAY_ALL=1 ./reset-streams.sh

DONE