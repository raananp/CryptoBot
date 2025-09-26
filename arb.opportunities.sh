# force-trade-via-fills.sh
# Usage: bash force-trade-via-fills.sh BTCUSDT 100 101 1 paper
INSTR=${1:-BTCUSDT}
BUY_PX=${2:-100}
SELL_PX=${3:-101}
SIZE=${4:-1}
MODE=${5:-paper}    # only used if your assembler reads mode from fills; safe to leave

CID="corr-$RANDOM"

echo "[1] Publish BUY fill (legIndex 0)"
docker exec -i deploy-redis-1 redis-cli XADD orders.fills '*' data \
"$(printf '{"id":"%s","ts":%s,"type":"order.fill","payload":{"corrId":"%s","legIndex":0,"exchange":"binance","instrumentId":"%s","side":"BUY","px":%s,"requestedSize":%s,"filledSize":%s,"mode":"%s"}}' \
 "fill-$RANDOM" "$(date +%s000)" "$CID" "$INSTR" "$BUY_PX" "$SIZE" "$SIZE" "$MODE")"

echo "[2] Publish SELL fill (legIndex 1)"
docker exec -i deploy-redis-1 redis-cli XADD orders.fills '*' data \
"$(printf '{"id":"%s","ts":%s,"type":"order.fill","payload":{"corrId":"%s","legIndex":1,"exchange":"bybit","instrumentId":"%s","side":"SELL","px":%s,"requestedSize":%s,"filledSize":%s,"mode":"%s"}}' \
 "fill-$RANDOM" "$(date +%s000)" "$CID" "$INSTR" "$SELL_PX" "$SIZE" "$SIZE" "$MODE")"

echo "[3] Check assembled trade stream"
docker exec -it deploy-redis-1 redis-cli XRANGE arb.trades - + | tail -n 10

echo "[4] Check Mongo (optional)"
docker exec -it deploy-mongodb-1 mongosh arb --quiet --eval 'db.trades.find().sort({ts:-1}).limit(3)'