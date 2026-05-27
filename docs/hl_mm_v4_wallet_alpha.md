# HL V4 — Wallet Alpha: Data-First Signal Extraction

## Executive Summary

Build a profitable directional trading signal from Hyperliquid's unique wallet-level trade transparency. Unlike V3 (which rushed to live with untested parameters), V4 follows a strict research pipeline: collect → profile → validate → deploy.

**What failed in V3 (live May 4):** We detected "metaorders" (3+ clip TWAPs) and rode them. Result: 84% directional accuracy but only ~4bps average MFE. Fees (2.88bps maker RT) consumed the edge. Maker fills were adversely selected (100% of live fills lost money). Parameters were vibe-traded.

**What's unique:** HL exposes buyer/seller wallet addresses on every trade. The public `userFills` API returns full trade history for ANY wallet. No CEX provides this. This is a structural information edge.

**V4 approach:** Profile wallets FIRST (who is consistently right?), THEN trade only when high-score wallets are active, with execution on the cheapest viable venue.

---

## Phase 1: Data Collection + Wallet Profiling (Week 1)

### 1.1 Data Sources

| Source | Method | What We Get |
|--------|--------|-------------|
| HL WS trade stream (50 coins) | Already collecting (hl_wallet_collector.py) | Every trade with buyer/seller wallet, ~50K trades/hour |
| HL REST userFills (per wallet) | Query top wallets by activity | Full 2000-trade history per wallet, with closedPnl, crossed (maker/taker), twapId |
| HL REST clearinghouseState | Query any wallet | Current positions, margin, leverage |
| HL 1h candles (MongoDB) | Already collected | Mid prices for markout computation |

### 1.2 Wallet Profiling Pipeline

```
Step 1: Identify active wallets from collector data (>20 trades in first 24h)
Step 2: Query userFills for each active wallet (2000 trade history)
Step 3: Compute per-wallet features:
  - Net PnL (from closedPnl field)
  - Aggressor ratio (crossed=true vs crossed=false)
  - Coin concentration (specialist vs generalist)
  - Size distribution (avg, median, max trade size)
  - Direction persistence (% same direction in 5min windows)
  - twapId usage (are they using HL's native TWAP?)
  - Trade frequency (trades/hour)
  - Time-of-day pattern
Step 4: Compute markouts at 30s, 2m, 10m, 30m from price data
Step 5: Rank wallets by markout with Bayesian shrinkage
Step 6: Classify into archetypes (informed, noise, MM, arb, liquidation)
```

### 1.3 Minimum Data Requirements Before Phase 2

- 7+ days of collector data (>8M trades)
- 500+ wallets profiled with >=100 trades each
- Markouts computed at 4 horizons
- Top/bottom decile wallets identified with t-stat > 2.0

---

## Phase 2: Signal Construction + Offline Backtest (Week 2)

### 2.1 Signal: Smart Wallet Imbalance (SWI)

```
SWI[coin, t] = sum(score[w] * sign[w,t] * notional[w,t])
              for all wallets w active in [t-L, t]
```

Where:
- `score[w]` = shrunk markout t-stat (from Phase 1)
- `sign[w,t]` = +1 if wallet bought, -1 if sold
- `notional[w,t]` = trade size in USD
- `L` = lookback window (parameter to optimize: 30s, 1m, 5m, 10m)

### 2.2 Entry/Exit Rules (to be validated, not assumed)

**Entry candidates (test all, deploy proven):**
- SWI threshold crossing (z-score > N)
- High-score wallet metaorder detection (5+ clips from wallet with markout > +5bps)
- Smart-vs-dumb divergence (smart wallets buying while dumb wallets selling)
- Cross-coin propagation (smart wallet buys coin A → trade coin B)

**Exit candidates (test all):**
- Fixed TP/SL/trailing (sweep parameters)
- Signal-based (SWI reversal)
- Time-based (optimal holding period from markout decay analysis)
- Adverse wallet detection (toxic wallet appears against us)

### 2.3 Backtest Requirements

- Event-driven (not candle-based)
- Walk-forward: train on 14 days, test on 7 days, roll forward
- Wallet scores computed ONLY from data before each test period
- Latency-adjusted: add 0.5s, 1s, 2s to entry time
- Fee-adjusted: HL maker 1.44bps, taker 4.5bps; Bybit maker 1bps, taker 5.5bps
- Slippage model: 1 tick on liquid coins, 2-3 ticks on illiquid
- Placebo tests: shuffle wallet IDs → signal should collapse

### 2.4 Deployment Bar (hard gates, no exceptions)

- OOS Sharpe > 1.5 after all costs
- Positive in >= 4 of 5 test periods
- Not dominated by 1 coin or 1 wallet (max 30% concentration)
- Survives 2x slippage assumption
- Survives 2s added latency
- Placebo test shows signal collapse (p < 0.01)

---

## Phase 3: Execution Design (Week 2-3, parallel with backtest)

### 3.1 Venue Decision Matrix

| Signal Horizon | Expected Move | Execution Venue | Entry Type | Fee Budget |
|---------------|---------------|-----------------|------------|------------|
| < 30s | < 8bps | NO TRADE | - | - |
| 30s - 2m | 8-15bps | HL maker if available | Passive limit | 2.88bps RT |
| 2m - 10m | 15-30bps | Bybit maker | Passive limit | 2.0bps RT |
| > 10m | > 30bps | Bybit taker | Market IOC | 6.5bps RT |

### 3.2 Fill Model (learned from V3 live test)

**Key finding:** Maker fills on directional signals are adversely selected. The fills we get are the ones where price crossed through our level (bad signal). Good signals move price AWAY from our order.

**Solutions to validate:**
1. Conditional taker: post maker first. If NOT filled in 5s → signal confirmed → enter taker
2. Cross-venue: HL signal → Bybit execution (different orderbook, no adverse selection)
3. Signal strength gating: only trade signals with expected move > 2x fee (>15bps for HL maker)

### 3.3 Position Management

- Max 1-2 concurrent positions
- Position size: $15-50 per trade (depending on signal strength)
- Hard SL: -5bps (from backtest optimization, not vibe)
- Trailing stop: parameters from Phase 2 backtest (activation, delta, time limit)
- Max hold: from markout decay analysis (where does the edge fully decay?)

---

## Phase 4: Paper Trading + Live (Week 3+)

### 4.1 Paper Phase (minimum 100 signals)

- Run signal generator live with no execution
- Record every signal: wallet, coin, direction, SWI value, expected move
- Compute real-time markout for each signal
- Compare to backtest predictions
- Gate: paper results must match backtest within 30% before going live

### 4.2 Live Phase

- Start with 1 coin, $15/trade
- Track fill rate, slippage, actual vs predicted markout
- Scale only after 50+ live trades with positive PnL
- Kill switch: -$2 daily loss → pause 24h

---

## Key Differences: V4 vs V3

| Aspect | V3 (failed) | V4 |
|--------|-------------|-----|
| Wallet scoring | Runtime classification, no history | Pre-computed from full trade history via API |
| Signal validation | None (vibe-traded parameters) | Walk-forward backtest + placebo tests |
| Minimum edge | 4bps (below fees) | 15bps minimum (well above fees) |
| Entry | Maker on HL (adversely selected) | Venue-optimized (Bybit for longer signals) |
| Parameters | Tuned by watching paper PnL | Optimized from offline data, fixed before live |
| Data requirement | None (deployed day 1) | 7+ days collection before Phase 2 |
| Deployment gate | "It looks good" | OOS Sharpe > 1.5, placebo collapse, multi-period positive |

---

## Infrastructure Already Built (Reusable)

From today's V3 work:
- Wide-universe monitoring (50 coins, trade-only WS subscriptions)
- Batch metaorder detection (single-pass deque scan)
- Paper PnL tracker with MFE/MAE
- Trailing stop in state machine
- Flow normalization (daily volume, clip size ratio)
- MongoDB wallet trade collector (running in tmux)

---

## Open Questions for Codex Review

1. Is SWI the right signal formulation, or should we use something else (e.g., pure copy-trade of top wallets, or conditional probability)?
2. Given ~$500 capital and 1.44bps maker fee, what's the minimum realistic Sharpe we can expect from wallet alpha?
3. Should we prioritize single-wallet metaorder piggybacking (higher expected move, fewer signals) or aggregate SWI (more signals, smaller moves)?
4. How do we handle wallet fragmentation (same entity using multiple addresses)?
5. Is 7 days enough data for stable wallet profiling, or do we need 30+?
