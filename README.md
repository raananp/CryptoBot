
# Arbitrage Options Bot (Starter Monorepo)

This is a runnable starter pack for the arbitrage bot architecture. It spins up multiple containers and demonstrates
the end-to-end pipeline with mock data:

- exchange-adapter-mock → scanner → risk-engine → order-simulator → api-gateway → frontend-web
- Redis Streams for messaging, MongoDB for trades storage
- Prometheus + Grafana for metrics (basic)

## Quick start

```bash
cp .env.example .env   # edit if needed
docker compose -f deploy/docker-compose.yml up -d --build
# Frontend at http://localhost:3000  (served by Nginx)
# API at http://localhost:8080
# Grafana at http://localhost:3001  (admin/admin)
# Prometheus at http://localhost:9090
```


## Added (pro) services
- **api-gateway-nest** (NestJS) on http://localhost:8081
- **config-secrets-service** on http://localhost:9402
- **health-aggregator** on http://localhost:9405/metrics (exposes `service_up`)
- **Loki + Promtail + Tempo** (example configs)


## New in this build
- **Settings tab (MUI)** with Auto-Trade toggle, Paper/Live mode switch, and per-strategy checkboxes.
- **Slippage Simulator** panel (book-sweep calculator).
- **Deribit adapter (paper)** connects to Deribit public WS and publishes best bid/ask snapshots to Redis.
- **Scanner** now synthesizes cross-venue opportunities using Deribit mid vs a simulated OKX premium.
- **API Gateway** exposes `/toggles` and `/strategies` (GET/POST) to persist UI choices in Redis.


## Router-Executor Added
- New service **router-executor** coordinates multi-leg IOC flow:
  1) Sends protective leg first (SELL if available).
  2) Waits for `orders.fills` from `order-simulator`.
  3) Proceeds with next leg or aborts on zero fill.
  4) Emits `arb.trades` on success.

- **order-simulator** now consumes `orders.new` and emits `orders.fills` (no longer produces trades directly).
- **scanner** respects strategy toggles (`strategy:parity`, `strategy:calendar`) and synthesizes extra opps for demo.



## Binance & Bybit Added + Positions/PnL
- **exchange-adapter-binance** (bookTicker BTCUSDT/ETHUSDT → `md.orderbook.binance`)
- **exchange-adapter-bybit** (tickers BTCUSDT/ETHUSDT → `md.orderbook.bybit`)
- **portfolio-accounting** consumes `arb.trades`, writes to Mongo, and maintains `positions`.
- **API**: `/positions` endpoint.
- **Frontend**: new **Positions** and **PnL** tabs (MUI).
# CryptoBot
