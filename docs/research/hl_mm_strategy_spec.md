# H3: Hyperliquid Imbalance-Directed Market Maker

## Strategy Specification & Implementation Architecture

**Version:** 2.0 (2026-05-02)
**Status:** PRE-DEPLOYMENT — Round 2 Codex fixes applied, Round 3 (strategy review) in progress
**Engine:** Custom Python (like H2 V3), NOT HB-native
**Venue:** Hyperliquid (perps, on-chain, Arbitrum L1)
**Capital:** $50 initial (proof of edge), scalable to $5K+

---

## 1. Thesis

Provide two-sided liquidity on illiquid Hyperliquid perpetual futures using the Avellaneda-Stoikov optimal market making framework, enhanced with L2 order book imbalance as a directional overlay.

**Why this works:**
- 0% maker fee at higher tiers / 1.44bps at current tier (MetaMask discount)
- Long-tail pairs have 5-8bps spreads with $5-25M daily volume
- No DMM program = no privileged access for institutional MMs
- Equal latency (on-chain, ~200ms) = no speed disadvantage for small players
- L2 imbalance signal (validated: 62% WR, 1.5bps edge, p<0.0001) provides directional intelligence beyond pure AS

**Why it might NOT work:**
- Adverse selection: informed traders pick off stale quotes
- Inventory risk: accumulate large directional exposure during trends
- Competition: other bots may already serve this niche
- Latency: 200ms means we can't cancel fast enough on news
- Small capital: $50 limits position sizing severely

---

## 2. Signal

### Primary: Avellaneda-Stoikov Reservation Price

The reservation price shifts our quote center based on inventory:

```
reservation = mid - q * gamma * sigma^2 * tau + q * funding * tau
```

This is NOT a directional signal — it's an inventory management mechanism that ensures we don't accumulate unbounded risk.

### Secondary: L2 Order Book Imbalance

```
imbalance = sum(bid_size_top10) / (sum(bid_size_top10) + sum(ask_size_top10))
imbalance_z = (imbalance - rolling_mean) / rolling_std
```

When `|imbalance_z| > 2.0`:
- Bid-heavy (z > 2): expect price UP → tighten bid, widen ask
- Ask-heavy (z < -2): expect price DOWN → tighten ask, widen bid

**Validated evidence:** 323K BTC snapshots, 7 days, 1s resolution:
- z > 2.0: +1.54bps at 60s, 62% WR, t=23.3, n=7,477
- Symmetric (both directions work equally)
- Persists: autocorrelation 0.86 at 1s, 0.42 at 10s

### Tertiary: Funding Rate Bias

When hourly funding is positive (longs pay shorts):
- Bias inventory SHORT (earn funding while providing liquidity)
- Structural edge: funding subsidizes inventory carry cost

---

## 3. Execution Model

### Quote Placement (every 5-10 seconds)

```
1. Fetch L2 book → compute mid, spread, imbalance
2. Estimate volatility (30s EMA of mid returns)
3. Estimate kappa (trade arrival rate from recent flow)
4. Compute reservation price (AS formula)
5. Compute optimal spread (AS formula)
6. Apply imbalance overlay (skew quotes toward predicted direction)
7. Clamp spread to [min_spread, max_spread]
8. Size quotes based on inventory (reduce on heavy side)
9. Cancel stale orders + place new bid/ask
```

### Order Types

- All orders: LIMIT GTC (Good Till Cancel)
- Maker-only execution (never cross the spread)
- Cancel + replace every 5-10s (or on significant mid move > 2bps)

### Fill Management

When a fill occurs:
1. Record fill (updates inventory)
2. Wait `fill_cooldown` (3s) before re-quoting
3. Check if fill was likely toxic (price continued against us > 5bps in 10s)
4. If toxic: widen spread for next quotes, reduce size

### Position Lifecycle

```
FLAT → accumulate via maker fills → hold (earn funding if favorable) → unwind via maker fills on opposite side
```

No taker exits under normal conditions. Emergency only: market close if drawdown exceeds limits.

---

## 4. Risk Management

### Inventory Limits (per pair)

| Level | Threshold | Action |
|-------|-----------|--------|
| Normal | < $7.50 (15% of capital) | Standard quoting |
| Soft | $7.50-$10 (15-20%) | Aggressive skew to reduce |
| Hard | $10-$15 (20-30%) | One-sided quoting only (reduce side) |
| Emergency | > $15 (30%) | Market close, pause quoting 5 min |

### Daily P&L Limits

| Metric | Threshold | Action |
|--------|-----------|--------|
| Daily loss | > $2.50 (5% of capital) | Stop trading for the day |
| Drawdown from peak | > $5.00 (10%) | Stop + alert |
| Consecutive same-side fills | > 5 | Likely toxic flow, pause 60s |

### Volatility Circuit Breaker

If realized vol spikes > 2x its 5-minute average:
- Widen spread to `max_spread` immediately
- Reduce order size by 50%
- Resume normal after vol returns below 1.5x

### Correlation Kill Switch

If BTC drops > 3% in 1 hour:
- Cancel all quotes
- Flatten inventory at market
- Wait 30 minutes before resuming

---

## 5. Target Pairs

### Primary: APE (proof of concept)

| Metric | Value | Source |
|--------|-------|--------|
| Spread | 7.6 bps | Live L2 snapshot (May 2) |
| 24h Volume | $9.7M | HL metaAndAssetCtxs |
| Max Leverage | ~20x | HL universe |
| Top-5 Bid Depth | $4,396 | Live L2 |
| Top-5 Ask Depth | $2,320 | Live L2 |
| Funding | +0.0013%/hr | Current |

**Why APE:**
- Widest spread among $5M+ volume pairs (7.6bps)
- Known token (not a random micro-cap that might delist)
- Enough volume for 10-20 fills/day at our size
- Good spread-to-depth ratio (our $50-100 orders won't move the book)

### Secondary (scale after APE proves profitable)

| Pair | Spread | Volume | Notes |
|------|--------|--------|-------|
| PENDLE | 5.2bps | $24.6M | Highest volume target |
| VVV | 4.5bps | $5.8M | Wide spread, low competition |
| PUMP | 5.5bps | $3.5M | Meme, volatile |
| XMR | 4.9bps | $2.5M | Privacy coin, unique flow |

---

## 6. Revenue Model

### Per Trade Economics (APE, base tier)

| Component | Value |
|-----------|-------|
| Half-spread earned | 3.8 bps ($0.019 on $50 fill) |
| Maker fee paid | 1.44 bps ($0.0072 on $50 fill) |
| **Net per fill** | **2.36 bps ($0.012)** |
| Expected adverse selection loss | ~1.0 bps ($0.005) |
| **Net after adverse selection** | **~1.4 bps ($0.007)** |

### Daily Projection ($50 capital, 10x leverage)

| Scenario | Fills/day | Net/fill | Daily | Monthly |
|----------|-----------|----------|-------|---------|
| Conservative | 10 | $0.007 | $0.07 | $2.10 |
| Base | 20 | $0.007 | $0.14 | $4.20 |
| Optimistic | 40 | $0.010 | $0.40 | $12.00 |

### Scaled Projection ($5,000 capital)

| Scenario | Fills/day | Net/fill | Daily | Monthly |
|----------|-----------|----------|-------|---------|
| Conservative | 20 | $0.70 | $14 | $420 |
| Base | 40 | $0.70 | $28 | $840 |
| Optimistic | 60 | $1.00 | $60 | $1,800 |

---

## 7. Implementation Architecture

```
scripts/hl_mm_live.py                    # Entry point (like scripts/arb_h2_live_v3.py)
    │
    └── app/services/hl_mm/
            ├── orchestrator.py          # Main loop (1s tick)
            ├── avellaneda_quoter.py     # AS model: reservation price + optimal spread
            ├── signal_engine.py         # L2 imbalance computation
            ├── quote_engine.py          # Order placement/cancel via HL SDK
            ├── inventory_manager.py     # Position tracking + hedge triggers
            ├── risk_manager.py          # Kill switches, drawdown limits
            ├── fill_tracker.py          # Fill detection, toxicity scoring
            └── telemetry.py            # Telegram reporting, MongoDB logging
```

### Dependencies

| Component | Library | Version |
|-----------|---------|---------|
| HL API | hyperliquid-python-sdk | 0.23.0 |
| Signing | eth-account | installed |
| Data | pymongo | installed |
| Notifications | telegram (MCP) | existing |

### Data Flow

```
HL L2 WebSocket → signal_engine (imbalance z-score)
                → avellaneda_quoter (reservation price + spread)
                → quote_engine (place/cancel limit orders)
                → fill_tracker (detect fills, update inventory)
                → inventory_manager (check limits)
                → risk_manager (kill switches)
                → telemetry (log to MongoDB + Telegram alerts)
```

### State Persistence

- **MongoDB collection:** `hl_mm_fills` — every fill with timestamp, price, size, side, fee
- **MongoDB collection:** `hl_mm_snapshots` — periodic (every 60s) state snapshot
- **Crash recovery:** On restart, query HL `user_state` for current positions + open orders

---

## 8. Deployment Plan

### Phase 0: Verification (current)
- [x] HL SDK installed + authenticated
- [x] Account funded ($50 in perps)
- [x] Order place + cancel verified
- [ ] Adversarial review (Codex)
- [ ] Fix any issues from review

### Phase 1: Paper Test (post-maintenance, May 2)
- Single pair (APE)
- $5 max position (10% of capital)
- Log all quotes + fills to MongoDB
- Run for 24h, analyze: fill rate, PnL, adverse selection

### Phase 2: Live Small (after 24h positive)
- Scale to $10 max position
- Add second pair (PENDLE)
- Telegram fill alerts
- Run for 1 week

### Phase 3: Scale (after 1 week positive)
- Increase capital to $500
- Add 3-5 pairs
- Optimize gamma, min_spread per pair
- Target: $10-20/day

---

## 9. Monitoring & Alerts

| Event | Alert Level | Channel |
|-------|-------------|---------|
| Fill executed | Info | MongoDB only |
| Inventory > soft limit | Warning | Telegram |
| Inventory > hard limit | Critical | Telegram + action |
| Daily loss > 3% | Critical | Telegram + stop |
| API error (rate limit, timeout) | Warning | Log only |
| HL maintenance detected | Info | Telegram + pause |
| BTC > 3% drop in 1h | Critical | Cancel all + alert |

---

## 10. Success Criteria

### Proof of Edge (Phase 1, 24h)

| Metric | Gate |
|--------|------|
| Fills | > 5 (enough to measure) |
| Gross PnL | > $0 (positive before fees) |
| Net PnL | > -$0.50 (don't blow up) |
| Max drawdown | < $2.50 (5% of capital) |
| Adverse selection rate | < 60% of fills (less than random) |

### Sustained Edge (Phase 2, 1 week)

| Metric | Gate |
|--------|------|
| Net PnL | > $0 (any positive amount) |
| Sharpe (daily) | > 0.5 |
| Profit factor | > 1.2 |
| Max drawdown | < 10% of capital |
| Uptime | > 90% |

### Scale Decision (Phase 3 trigger)

- 7 consecutive positive days
- Profit factor > 1.5
- No single day loss > 3%
