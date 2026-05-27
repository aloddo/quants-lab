# HL Market Making V4 — Architecture Design

**Date:** 2026-05-08
**Status:** DESIGN V2 (post-Codex adversarial review, 10 findings addressed)

## Mission

Build a professional market maker on Hyperliquid perpetuals that:
- Earns net positive completed-cycle EV (not raw fills)
- Handles adverse selection (what killed V2)
- Proves EV with telemetry before deploying real capital
- Scales from $50 test → $500 gate → $20K deployment

## Fee Structure (verified from HL API, May 8 2026)

| | Per side | Round-trip |
|---|---------|-----------|
| Taker | 4.32bp (with 4% referral discount) | 8.64bp |
| Maker | 1.44bp (with 4% referral discount) | 2.88bp |

Maker fee is POSITIVE (we pay). No rebate at our volume tier.

**Break-even math (Codex finding #2):**
A completed round-trip (enter maker + exit maker) costs 2.88bp in fees.
But real break-even includes:

```
break-even spread = 2.88bp (maker RT fee)
                  + markout loss (3-8bp typical on alts)
                  + inventory carry cost (funding, ~0.5bp/hr avg)
                  + occasional taker exit drag (5.76bp when one leg is taker)
                  
Realistic break-even: 6-11bp full captured spread
```

A pair with <6bp natural spread is NOT viable. A pair with 10-15bp gives 2-5bp margin.

## Target Pairs

**CRITICAL: One snapshot is not pair selection (Codex finding #7).**
Phase 0 collects multi-day spread/depth/markout data before committing to pairs.

Preliminary candidates (need validation over 7+ days):

| Coin | Snapshot Spread | 24h Volume | Notes |
|------|----------------|------------|-------|
| TON | 7.7bp | $211M | Lopsided book ($37K bid / $4K ask) — needs monitoring |
| JTO | 17.7bp | $30M | Very wide, may have low fill rates |
| DYDX | 12.9bp | $14M | Wide spread, moderate volume |
| NIL | 13.5bp | $11M | Wide, but low volume |
| ENA | 3.3bp | $16M | Too tight — likely below break-even |
| VVV | 3.0bp | $19M | Too tight |

**Removed:** PUMP — contradicts "avoid meme coins" rule (Codex finding #7).
**Avoid:** BTC/ETH/SOL (<1bp, HFT dominated), all meme/micro-cap coins.
**Need to validate:** spread stability, markout distributions, queue dynamics over 7+ days.

## Architecture

```
HL MM V4
├── QuoteEngine          — Skewed reservation price + optimal spread
├── AdverseSelectionGuard — Filters that PROTECT us (correct side!)
├── InventoryManager     — Position tracking, price-based exits
├── RegimeDetector       — Ranging/trending/volatile classification
├── OrderManager         — ALO orders, fast requoting
├── RiskManager          — Circuit breakers, correlation-aware limits
├── MarkoutTracker       — Per-fill post-trade analysis (THE key metric)
└── PairScreener         — Multi-day validation, not snapshots
```

### 1. QuoteEngine

**Codex finding #4: A-S has unit problems.** The textbook formula needs proper scaling.

```python
# Inventory in NOTIONAL terms (not normalized [-1,+1])
q_notional = position_size * mid_price  # e.g., $50 of TON

# Reservation price skew in BPS (not raw price)
skew_bps = q_notional / max_inventory_notional * max_skew_bps
# max_skew_bps = 3-5bp (tuned per pair)

r = mid * (1 - skew_bps / 10000)

# Spread determined by regime, not A-S formula
# Base half-spread = max(natural_spread / 2, min_profitable_spread)
# min_profitable_spread = (break_even_spread + target_margin) / 2
half_spread = max(natural_half_spread, 4.0)  # at least 4bp per side

bid = r - half_spread_in_price
ask = r + half_spread_in_price
```

**Key change:** Use empirical spread (from collected data) as base, not theoretical A-S. 
A-S kappa estimation requires fill-intensity curves we don't have yet.
The skew formula is simpler but dimensionally correct: inventory as fraction of max 
times a tunable skew in bps.

### 2. AdverseSelectionGuard

**Codex finding #5: Filter signs were BACKWARDS. Fixed.**

The principle: when directional pressure is detected, PROTECT the side that will 
get adversely selected. If price is moving UP, our ASK is toxic (we'd sell into a rally).
Widen/pause the ASK, not the bid.

**Filter A: Book Imbalance**
```python
imbalance = (bid_depth_5lvl - ask_depth_5lvl) / (bid_depth_5lvl + ask_depth_5lvl)
# > +0.3: buying pressure, price likely UP → WIDEN ASK (toxic side), tighten bid
# < -0.3: selling pressure, price likely DOWN → WIDEN BID (toxic side), tighten ask
# |imbalance| > 0.5: PAUSE the toxic side entirely
```

**Filter B: Trade Toxicity (simplified VPIN)**
```python
# 5-second rolling window (Codex finding #6: 30s is too slow)
buy_vol = sum(taker buys in last 5s)
sell_vol = sum(taker sells in last 5s)
toxicity = (buy_vol - sell_vol) / (buy_vol + sell_vol + 1e-9)
# toxicity > +0.6: aggressive buying → PAUSE ASK for 10s
# toxicity < -0.6: aggressive selling → PAUSE BID for 10s
```

**Filter C: Micro-Price Divergence**
```python
microprice = (best_ask * bid_sz_top + best_bid * ask_sz_top) / (bid_sz_top + ask_sz_top)
divergence_bps = (microprice - mid) / mid * 10000
# divergence > +2bp: price drifting up → WIDEN ASK, tighten bid
# divergence < -2bp: price drifting down → WIDEN BID, tighten ask
```

**Filter D: EMA Momentum**
```python
ema5 = EMA(close_1m, 5)
ema20 = EMA(close_1m, 20)
momentum_bps = (ema5 - ema20) / ema20 * 10000
# momentum > +5bp: trending up → PAUSE ASK (Codex finding #5: was "pause bid" — WRONG)
# momentum < -5bp: trending down → PAUSE BID
```

**Filter E: Pre-Funding Settlement**
```python
minutes_to_settlement = 60 - current_minute
if minutes_to_settlement < 10:
    widen_both_sides(2x)
```

**Codex finding #6: Missing filters.** Additional filters needed:
- **F: Cross-venue price** — monitor Binance/Bybit mid vs HL mid. If HL lags by >3bp, pause all.
- **G: Volatility spike** — if 1m realized vol > 2x 60m avg, widen to 3x or pause.
- **H: Stale data guard** — if WS feed is >2s stale, cancel all orders immediately.

### 3. InventoryManager

**Codex finding #8: Time-based force-close fails in flash crashes.**

Exits are now PRICE/RISK triggered, not time-based:

- Max inventory: 25% of deployed capital per pair
- At 50% max: widen "loaded side" spread by 1.5x
- At 100% max: cancel all quotes on that side
- **Price-based exit:** if unrealized PnL on inventory exceeds -0.3% of position → market exit
- **Volatility-based exit:** if 1m vol spikes >3x average while holding inventory → market exit
- **Margin-based exit:** if account margin usage exceeds 60% → flatten largest position
- Time limit is a BACKSTOP only: 2h (not 4h), and only if price-based hasn't triggered

**Codex finding #8: Correlation risk.** All alt pairs are correlated to BTC.
- Track BTC beta per pair
- Max TOTAL directional inventory (adjusted for beta) = 30% of capital
- If BTC drops 2% in 5min, flatten ALL alt inventory immediately

### 4. RegimeDetector

| State | Condition | Action |
|-------|-----------|--------|
| RANGING | \|imbalance\| < 0.2 AND \|toxicity\| < 0.3 AND vol_ratio < 0.9 | Quote at base spread |
| NEUTRAL | default | Quote at 1.5x spread |
| TRENDING | \|imbalance\| > 0.5 OR \|toxicity\| > 0.6 OR vol_ratio > 1.5 | Pause or 3x spread |
| VOLATILE | 1m vol > 2x 60m avg OR gap > 0.5% | Cancel all, wait 60s |

### 5. OrderManager

**Codex finding #9: 30-60s requote is too slow.**

- ALL orders use ALO (post-only)
- Requote every **5-10 seconds** (not 30-60s)
- HL rate limit: 1200 requests/min. With 2 pairs × 2 sides × 6 requotes/min = 24 req/min. Well within limit.
- Cancel-before-replace (never have >1 order per side per pair)
- Track every fill: price, size, side, spread_at_fill, queue_position, markout_1s/5s/30s/5m

### 6. RiskManager

- Daily loss limit: -1% of deployed capital → pause all quoting for 15 min
- Hourly loss limit: -0.3% → pause for 5 min
- Gap detection: if mid-price moves >0.5% in <10 seconds → cancel all, wait 60s
- Max concurrent pairs: 4 (start small, not 8)
- Max total inventory across all pairs: 30% of total capital (beta-adjusted)
- BTC crash guard: BTC -2% in 5min → flatten everything

### 7. MarkoutTracker (NEW — Codex finding #1)

**This is the most important component.** Revenue = completed cycles, not fills.

For every fill, track:
```python
{
    'fill_id': str,
    'pair': str,
    'side': 'buy' | 'sell',
    'fill_price': float,
    'fill_size': float,
    'queue_position': int,       # where we were in queue
    'spread_at_fill': float,     # spread when we got filled
    'markout_1s': float,         # price move 1s after fill (bps)
    'markout_5s': float,         # 5s
    'markout_30s': float,        # 30s
    'markout_5m': float,         # 5m
    'exit_path': 'maker' | 'taker' | 'timeout',
    'exit_price': float,
    'cycle_pnl_bps': float,      # full round-trip P&L after fees
    'fee_paid': float,
    'regime_at_fill': str,
    'filters_active': list,
}
```

**The go/no-go metric:** Average markout at 5s and 30s across all fills.
- If markout_5s < -2bp consistently → we are being adversely selected → adjust filters
- If markout_5s > 0bp → fills are favorable → increase size
- If cycle_pnl_bps is positive on average → strategy works → scale up

## Revenue Model (CORRECTED — Codex finding #1)

**Honest math:** Revenue comes from completed round-trips, not raw fills.

```
Completed RT profit = captured_spread - 2.88bp (maker RT fee) - markout_loss
```

Assuming 8bp average captured spread and 3bp average markout loss:
- Net per RT: 8bp - 2.88bp - 3bp = 2.12bp

| Capital | Pairs | Net/RT | Completed RTs/day | Monthly Net |
|---------|-------|--------|-------------------|-------------|
| $500 | 2 | 2.12bp | 5 | $16 |
| $2,000 | 3 | 2.12bp | 12 | $38 |
| $5,000 | 4 | 2.12bp | 25 | $80 |
| $10,000 (3x lev) | 4 | 2.12bp | 60 | $191 |
| $20,000 (3x lev) | 4 | 2.12bp | 120 | $382 |

**$500 MRR requires ~$25K deployed at 3x leverage with consistent execution.**
NOT guaranteed — depends on proving positive markout first.

## Implementation Plan (REVISED — Codex finding #10)

1. **Phase 0 (7 days, not 1):** Collect L2 + trade data for 6 candidate pairs. 
   Measure: spread distribution (not one snapshot), fill probability by queue position, 
   markout distributions, volatility patterns, funding behavior, weekend effects.
   **No code until Phase 0 data validates pair selection.**

2. **Phase 1 (3 days):** Shadow quoting engine — computes where we WOULD quote but 
   doesn't place orders. Logs theoretical fills, markout, and cycle P&L. 
   Validates: are theoretical completed cycles positive EV?

3. **Phase 2 (2 days):** If shadow P&L is positive → build real OrderManager with ALO.
   Deploy with $50 on ONE pair. Tiny orders ($5-10 per side). 
   Track real fills vs shadow fills. Real markout vs theoretical.

4. **Phase 3:** If real P&L matches shadow → scale to $200, then $500.
   If not → diagnose gap (slippage, queue priority, latency).

5. **Phase 4:** Multi-pair deployment, leverage, target scale.

## Codex Adversarial Review (May 8, 2026)

10 findings, all addressed:

| # | Finding | Severity | Fix |
|---|---------|----------|-----|
| 1 | Revenue counts fills not completed cycles | FATAL | Added MarkoutTracker, corrected revenue model |
| 2 | Break-even math missing markout/carry | CRITICAL | Added full break-even formula |
| 3 | Fill rate ≠ profitable fill rate | CRITICAL | Shadow quoting phase proves EV before live |
| 4 | A-S has unit problems, kappa unknown | HIGH | Replaced with empirical spread + simple bps skew |
| 5 | Adverse selection filter signs BACKWARDS | FATAL | All filter directions corrected |
| 6 | Missing cross-venue, vol spike, stale data filters | HIGH | Added filters F, G, H |
| 7 | Pair selection is one snapshot | HIGH | Phase 0 extended to 7 days multi-metric |
| 8 | 4h force-close useless in flash crash | HIGH | Price/vol/margin triggers, BTC crash guard |
| 9 | 30-60s requote too slow | MEDIUM | Changed to 5-10s |
| 10 | Phase 0 too short, no markout evidence | HIGH | 7-day Phase 0, shadow quoting Phase 1 |

## What Killed V2 (Post-Mortem + V4 Fix)

1. **Symmetric quoting in trends** → Fixed: directional filters PAUSE toxic side
2. **No adverse selection detection** → Fixed: 8-filter guard with correct signs
3. **No inventory limits** → Fixed: price/vol/margin triggers, beta-adjusted limits
4. **Meme coin pair selection** → Fixed: 7-day validated screening, no meme coins
5. **No regime detection** → Fixed: 4-state detector including VOLATILE
6. **No markout tracking** → Fixed: MarkoutTracker is the core metric
