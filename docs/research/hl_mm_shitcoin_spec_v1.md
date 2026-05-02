# HL Shitcoin Market Making Strategy Spec — V1

**Date:** 2026-05-02
**Source:** Codex Round 2 (session 019de89c-b57f-72c1-963f-022461a23e8e)
**Status:** SPEC — not yet built or validated

---

## 1. Pair Ranking and Launch Order

**Launch now:** ORDI, then BIO after >=100 clean live fills on ORDI
**Shadow only:** DASH, AXS, PNUT
**Wait for lower fee / wider spread:** APE, PENDLE
**No launch:** MEGA (spread collapsed to 1.4bps, not stable)

### Measured Spreads (May 2, 2026)

| Coin | Native Spread | Edge Room/Side | Daily Volume | Depth (top 20) |
|------|--------------|----------------|-------------|----------------|
| ORDI | 13.5bps median | 5.3bps | $1.2M | $50K bid/ask |
| BIO | 11.1bps median | 4.1bps | $6.6M | $40K bid/ask |
| DASH | 9.2bps (scan) | 3.1bps | $1.7M | $4K bid/ask |
| AXS | 8.0bps (scan) | 2.5bps | $1.2M | $4K bid/ask |
| PNUT | 8.9bps (scan) | 3.0bps | $398K | $9K bid/ask |
| APE | 5.5bps median | 1.3bps | $11.7M | $65K bid/ask |
| PENDLE | 4.6bps (scan) | 0.8bps | $22.3M | $3K bid/ask |
| MEGA | 1.4bps median | -0.1bps | $17.2M | $40K bid/ask |

### Add/Remove Criteria

**Add a pair** if over trailing 6h ALL hold:
- Median spread >= 2*(maker_fee + tox_buffer + 1.0bps)
- Median top-20 same-side depth >= 250x child_order_usd
- Trade count >= 150/hour
- Anchor freshness >= 98%
- Simulated median 5s post-fill markout > -1.0bps

**Remove a pair** if ANY 2 of these occur in 6h:
- Spread below threshold
- Toxic-fill share > 55%
- Median 5s markout < -1.5bps
- Hedge rate > 1 per 20 fills
- OMS/data errors > 1%

---

## 2. Fair Value Model

Three-tier anchor system:

### Tier 1: Direct Bybit pair available
```
anchored_mid = bybit_mid * exp(EMA_300s(log(HL_mid / bybit_mid)))
fv = 0.70 * anchored_mid + 0.30 * microprice + clip(ofi_term, +/-0.75bps)
```

### Tier 2: Sparse/new Bybit pair
```
fv = 0.55 * anchored_mid + 0.45 * microprice
```

### Tier 3: No direct Bybit pair
```
synthetic_anchor = last_hl_mid * exp(beta_btc*dBTC + beta_eth*dETH + beta_sol*dSOL)
```
Betas from rolling 6h OLS on 1s returns. Use only if R^2 >= 0.25.
```
fv = 0.35 * synthetic_anchor + 0.65 * microprice
```
If no stable synthetic anchor: quote one-sided only, halve size, disable routine hedge, research-only.

### Staleness Rules
- Bybit stale > 500ms: halve anchor weight
- Bybit stale > 2s: anchor weight = 0

### Microprice
```
microprice = best_ask * I + best_bid * (1 - I)
where I = bid_qty_top1 / (bid_qty_top1 + ask_qty_top1)
```

---

## 3. Quoting Logic — Full State Machine

### Edge Calculation
```
edge_room_side = native_half_spread - maker_fee (1.44bps)
```

### Per-Pair Toxicity Buffers (starting values, bps)
| Pair | Tox Buffer |
|------|-----------|
| ORDI | 0.8 |
| BIO | 0.9 |
| DASH | 1.2 |
| AXS | 1.1 |
| PNUT | 1.4 |
| APE | 1.0 |
| PENDLE | 1.0 |
| MEGA | 1.2 |

### Inside-Spread Improvement
```
improve = min(0.35 * edge_room_side, edge_room_side - tox_buffer - 1.0bps)
```
Rounded to ticks. If improve <= 0, no inside quote on that side.

**Minimum target realized edge per side after fees and toxicity: 1.0bps**

### Contrarian Rule
- Book bid-heavy: improve the ASK, widen or cancel the BID
- Book ask-heavy: improve the BID, widen or cancel the ASK
- NEVER tighten into the aggressive side

### Child Order Size
```
size = min(12, 0.20*free_equity, 0.003*depth20_side, 0.75*Q_soft) * vol_scale * anchor_scale
```
Starting sizes: ORDI/BIO $10, DASH/AXS/PNUT $8, APE $10, PENDLE $8.

### Requote Frequency
Only requote if >= 1.2s since last action AND one of:
- Fair value moved >= 1 tick or 0.8bps
- Spread bucket changed
- Inventory changed
- Quote age > 4s

Hard refresh every 5s. One cancel/replace batch in flight per pair.

### State Machine

```
IDLE → QUOTING_BOTH:
  Data healthy, spread threshold passes, |q| < 0.5*Q_soft, both sides EV positive.

IDLE → QUOTING_ONE_SIDE:
  Only one side EV positive, or contrarian imbalance is strong and one side is toxic.

QUOTING_BOTH → QUOTING_ONE_SIDE:
  One side EV drops <= 0, |q| >= 0.5*Q_soft, or one side hits toxicity filter.

QUOTING_* → INVENTORY_EXIT:
  |q| >= Q_soft or inventory age > 30s.

INVENTORY_EXIT → HEDGE:
  |q| >= Q_hard, age > 60s with adverse move > 4bps, or HL data stale with inventory.

ANY → PAUSE:
  Circuit breaker, stale data, OMS mismatch, or regime shock.

PAUSE → IDLE:
  Cooldown expired, two fresh HL books seen, anchor healthy again.
```

---

## 4. Inventory Management

### AS Reservation Price
```
q_norm = q_usd / Q_soft
reservation = fv - q_norm * gamma * sigma_1s_bps^2 * tau
tau = 8s
```

### Gamma Calibration (target shift at q_norm=1)
| Pair | Target Shift | Gamma Formula |
|------|-------------|---------------|
| ORDI/BIO | 1.5bps | gamma = 1.5 / (sigma_1s^2 * 8) |
| DASH/AXS/PNUT | 2.0bps | gamma = 2.0 / (sigma_1s^2 * 8) |
| APE/PENDLE | 1.0bps | gamma = 1.0 / (sigma_1s^2 * 8) |

Multiply gamma by 1.5x if rv_30s > 1.75x baseline, 2x if > 2.5x.

### Position Limits (USD)

| Pair | Q_soft | Q_hard | Q_emergency |
|------|--------|--------|------------|
| ORDI/BIO | 10 | 12 | 15 |
| DASH/AXS/PNUT | 8 | 10 | 12 |
| APE | 8 | 10 | 12 |
| PENDLE | 6 | 8 | 10 |

### Inventory Age Limits
- Soft: 30s
- Hard: 60s
- Emergency: 180s

### Exit Decision Tree
- Age < 30s AND adverse move < 4bps: passive exit (improve exit side)
- Age 30-60s OR adverse move 4-8bps: exit-only quoting, improve by 1 extra tick, suppress re-entry
- Age > 60s: Bybit hedge if direct pair exists
- Age > 180s OR loss > 12bps AND no hedge: flatten on HL taker, pause 10min

### Bybit Hedge Rules
Trigger if ANY of:
- |q| >= Q_hard
- Age > 60s
- HL stale > 1.5s
- Post-fill move against you > 1 native spread

Sizing: 80-100% of delta. Direct same-symbol first, else beta hedge via BTC/ETH/SOL.
Slippage budget: direct pair <= 6.5bps all-in, proxy hedge <= 8.5bps.
Use IOC limit at mid +/- max(1 tick, 1bp).

---

## 5. Adverse Selection Defense

### Toxic Flow Flags
- Same-side top-5 depth drop > 40% in 1s
- Spread widening > 1.5x the 5-minute median
- 3s trade imbalance > 70/30
- Anchor jump > 6bps in 1s
- Touch depletion without depth replenish inside 2s

### Contrarian Regime
|z_imb| >= 1.5 AND OFI confirms: use asymmetry (widen toxic side, tighten fade side), not full pause.

### Pump/Dump Regime
|ret_5s| > max(10bps, 2*median_spread) plus same-side flow dominance:
- PAUSE 60s
- HEDGE if carrying inventory

### Circuit Breakers
- 1 toxic fill: widen next quotes +1 tick
- 2 toxic fills in 10m: PAUSE 5m
- 3 toxic fills in 30m: disable pair for rest of UTC day

### Funding Handling
HL funding is hourly. From T-180s to T+120s around settlement:
- No new same-direction entries if funding is against inventory or |funding| > 2bps/hour
- Exit-only mode
- Avoid carrying Bybit hedges through 00:00/08:00/16:00 UTC funding

---

## 6. Risk Management

### Portfolio Rules (current capital: $54 HL, $480 Bybit)
- Max 2 live pairs simultaneously
- Max gross inventory: $20
- Max beta-adjusted net exposure: $14
- Max gross resting order notional: $40

### Stops
- Daily strategy stop: -$3 combined realized + unrealized (HL + Bybit hedge)
- Hard stop: -$5 and full shutdown for UTC day

### Correlation Stop
- BTC or ETH moves > 1.5% in 5m: halve all sizes
- BTC or ETH moves > 2.5% in 5m: pause all alt pairs 15m

### Gap Risk
- HL book freshness > 1.5s: cancel all quotes
- HL freshness > 3s with inventory: hedge immediately
- No order ack/cancel in 3s: assume order may be live, stop sending, full open-order sync

### Stale Data
- HL stale > 1.5s: no quoting
- Bybit stale > 500ms: reduced anchor weight, half size
- Bybit stale > 2s: no new hedges except emergency

---

## 7. Execution Architecture

### Language
Python (asyncio) for v1. Venue latency is the bottleneck, not language runtime. Revisit Rust/Go only after strategy is positive and running > 5 pairs.

### Event Loop
```
HL book WS → trade WS → Bybit WS → feature cache → pair state machine → OMS → fill/markout logger → risk manager
```

### OMS Rules
- One order batch in flight per pair
- Cancel on disconnect/reconnect
- Full open-order reconciliation every 30s
- Mandatory state sync on startup

### Logging (EVERY quote decision, not just fills)
- pair, state, best bid/ask, spread, depth, microprice, anchor, fair value
- imbalance, OFI, order ids, ack/cancel latency
- fill price/fee, 1/5/15/60s markouts

---

## 8. Expected Economics

### Per-Pair Expected Net RT Edge (current 1.44bps fee)
| Pair | Net RT Edge | Expected RT/day | Daily PnL |
|------|-----------|----------------|-----------|
| ORDI | 5.2bps | 12-20 | $0.06-$0.10 |
| BIO | 3.6bps | 15-22 | $0.05-$0.08 |
| DASH | 2.5bps | 5-8 | $0.01-$0.02 |
| AXS | 1.5bps | 5-8 | ~$0.01 |
| PNUT | 1.6bps | 4-6 | <$0.01 |
| APE | 0.6bps | 12-20 | $0.01-$0.02 |

### Portfolio (ORDI + BIO live)
- Expected daily net: $0.11-$0.18
- Daily std: $0.15-$0.25
- Daily Sharpe: ~0.6-1.0
- 30-day max drawdown: -$1.5 to -$2.5 (with kill switches)

### Break-Even RT/day (to absorb one $0.05 toxic event)
ORDI: 10, BIO: 14, DASH: 25, AXS: 40, PNUT: 38, APE: 80

### Fee Tier Reality Check
- Current tier: 1.44bps maker. Strategy volume ~$800/day.
- VIP1 ($5M 14d): ~6,250 days at current volume. Not realistic via self-grinding.
- VIP3 ($100M 14d): impossible via self-grinding.
- **Real flywheel: prove edge with real fills → approach token projects for designated MM role → 0% fee accounts + rebates.**

---

## 9. Validation Plan (before implementation review in Round 3)

### Data Collection
- Minimum 72h, preferred 5 continuous days
- 1s HL L2 + trades for all 8 pairs
- Tick-by-tick Bybit data for every direct-anchor pair

### Offline Tests
1. Conservative replay: 1.1s effective latency, queue-ahead fill model
2. Post-only rejection model
3. 1/3/5/15/60s markouts per pair
4. Fill toxicity by state bucket
5. Hedge efficiency simulation
6. Walk-forward pair ranking by day

### Live Micro-Tests (before real capital)
1. 50 place/cancel cycles on ORDI and BIO
2. 20 one-sided quote cycles
3. 10 tiny real fills per side
4. 10 passive inventory exits
5. 5 controlled direct Bybit hedge drills

### Kill Criteria (after >= 150 live fills on ORDI+BIO)
Kill the idea entirely if ANY of:
- Median realized edge per side < +0.5bps on ORDI
- Portfolio net PnL <= 0
- Hedge needed more than 1/20 fills
- OMS mismatch more than once
- Stale-data pauses exceed 5% of supervised runtime

---

## Academic References

- Avellaneda-Stoikov (2008): inventory pricing skeleton
- GLFT (Gueant-Lehalle-Fernandez-Tapia 2013): empirical spread/intensity
- Stoikov microprice (2017): fair value residual
- Cont-Kukanov-Stoikov OFI (2014): flow toxicity
- Albers et al. (2025): fill-vs-markout tradeoff, contrarian quoting
- Bieganowski-Slepaczuk (2026): cross-asset feature stability
