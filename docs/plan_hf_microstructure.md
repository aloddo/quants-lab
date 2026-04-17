# HF Microstructure Data & Strategy Plan
**Status:** RESEARCH COMPLETE (Apr 16, 2026)
**Goal:** Expand from 1h signal strategies to sub-minute microstructure strategies

## Data Source Research Summary

### P0 — Integrate This Week (free, high value)

| Source | Data | Auth | History | Strategy Use |
|--------|------|------|---------|-------------|
| **Bybit WS Trade Stream** | Real-time trades (price, size, side, trade_id) | None | Real-time only, must collect | CVD, whale detection, flow divergence |
| **Bybit WS Orderbook L50** | 50-level depth, 20ms push | None | Real-time only, must collect | Bid-ask imbalance, absorption |
| **Binance WS Trade Stream** | aggTrades (price, qty, side, timestamp) | None | Real-time only | Cross-exchange flow divergence |
| **Binance Historical aggTrades** | CSV downloads from data.binance.vision | None | 2020+ daily/monthly CSVs, all USDT-M pairs | Backtesting tick strategies |

### P1 — Next 2 Weeks

| Source | Data | Auth | History | Strategy Use |
|--------|------|------|---------|-------------|
| **OKX** | Funding rate, OI, trades (REST + WS) | None for public | ~6yr paginated | 4th exchange for funding panel |
| **Deribit P/C ratio + Max Pain** | Compute from existing options surface data | N/A | Already in MongoDB (513K+ docs) | Options positioning, pin risk |
| **Hyperliquid Predicted Funding** | Multi-venue funding predictions | None | Real-time | Funding rate arbitrage timing |
| **DefiLlama** | DEX volume by protocol, stablecoin mint/burn/flows | None, 500 req/min | 2020+ full history | Macro flow regime, stablecoin signal |

### P2 — When P0/P1 Show Edge

| Source | Data | Auth | History | Strategy Use |
|--------|------|------|---------|-------------|
| **Santiment** | Social volume, dev activity, network growth | Free tier (GraphQL) | Multi-year | Sentiment overlay |
| **CryptoQuant** | Exchange inflow/outflow/netflow | Free API tier | Full BTC/ETH | Whale flow, exchange reserve |
| **GeckoTerminal** | DEX pair prices, volume, 1800+ DEXs | None, 30 req/min | Real-time | DEX-CEX arb signals |
| **Binance stats** | Top-trader LS ratio, taker buy/sell volume | No auth | 30 days only | Smart money indicator |
| **Dune** | On-chain SQL (stablecoin flows, whale moves) | Free key, 2500 credits/mo | Full chain | USDT/USDC mint/burn |

### Rejected (paid or redundant)
LunarCrush ($24/mo), Glassnode API (no free tier), Nansen (100 credits one-time), Tardis.dev (paid), CoinGlass (paid $50-100/mo), CoinAPI ($79/mo), AllTick (paid), Whale Alert (paid for live)

## New Strategy Ideas (X11-X15)

| Strategy | Signal | Data Source | Frequency | Thesis |
|----------|--------|------------|-----------|--------|
| **X11** Orderbook Pressure | Persistent bid/ask depth imbalance (ratio >1.5 for 30s+) | Bybit WS L50 | Sub-minute | Resting depth predicts short-term direction |
| **X12** Cross-Exchange Flow | Net buy volume divergence >2sigma between Binance and Bybit | Both WS trade streams | 5-15min | Lagging exchange catches up |
| **X13** Whale Sweep | 3+ large trades ($50k+) within 5s same side | Bybit WS trades | Seconds | Institutional sweep = momentum |
| **X14** CVD Momentum | 5min cumulative volume delta slope | Bybit WS trades | 5-15min | Sustained buying/selling pressure predicts 15-60min price |
| **X15** Absorption | Large resting orders hit repeatedly, price doesn't move | Bybit WS L50 + trades | Sub-minute | Reversal when absorption breaks |

## HB Architecture for HF

### Current Limitation
- HB clock ticks at 1s (configurable)
- Controllers CAN react sub-minute
- BacktestingEngine only supports candle-level (1m minimum)

### Recommended Architecture: External Signal Service + HB Execution

```
                    ┌──────────────────────────────┐
                    │  Microstructure Signal Server │
                    │  (Python asyncio process)     │
                    │                              │
                    │  WS Consumer:                │
                    │  - Bybit trades + orderbook  │
                    │  - Binance aggTrades         │
                    │                              │
                    │  Signal Computation:          │
                    │  - CVD, imbalance, sweeps    │
                    │  - Publishes to MQTT/Redis   │
                    │                              │
                    └──────────────┬───────────────┘
                                   │ signal
                                   ▼
                    ┌──────────────────────────────┐
                    │  HB Bot Container            │
                    │  Controller reads signal     │
                    │  from Redis/MQTT             │
                    │  Executes via position       │
                    │  executor (TP/SL/TL)         │
                    └──────────────────────────────┘
```

### Backtesting HF Strategies
- Download Binance historical aggTrades CSVs (free, 2020+)
- Build custom tick-level backtester (HB's engine can't do this)
- Or: aggregate tick signals into 1m candle columns, use HB backtester at 1m
- The aggregation approach is pragmatic and fits existing infra

## Implementation Phases

### Phase 1: Data Infrastructure (Week 1)
- Build TradeStreamCollectorTask (Bybit + Binance WS → MongoDB)
- Build OrderbookSnapshotTask (Bybit L50 → MongoDB, 1s samples)
- Download Binance historical aggTrades for top 20 pairs (backfill script)
- Collections: `trade_stream`, `orderbook_snapshots`, `binance_agg_trades`

### Phase 2: First Microstructure Strategy (Week 2)
- X14 CVD Momentum (simplest signal, well-studied)
- Aggregate CVD into 1m candle column for HB backtester
- Backtest at 1m resolution
- If PASS: deploy via Tier 1 (HB container, fast execution)

### Phase 3: Multi-Exchange Signals (Week 3)
- X12 Cross-Exchange Flow Divergence
- Requires both Bybit + Binance trade streams running
- More complex but potentially stronger alpha (cross-exchange is harder to arb away)

## Dependencies
- Infra hardening P2 (Execution Service, WS multiplexer) needed before deploying HF at scale
- Redis or MQTT for real-time signal distribution to bot containers
- Binance aggTrades backfill for backtesting
