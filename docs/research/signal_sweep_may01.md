# Signal Sweep — May 1, 2026

**Objective:** Find new alpha sources from existing data (2.9M options, 2.46M HL-Bybit spreads, 966K HL funding, 348K liquidations, 14.6K whale consensus, 309K whale positions)

## Results Summary

| # | Signal | Data Source | Direction | Result | Best Metric | Actionable? |
|---|--------|------------|-----------|--------|-------------|-------------|
| 1 | P/C OI ratio z-score | Deribit options (2.9M docs) | SHORT | **PROMISING** | 92% WR 8h, t=-5.25 (z<-1.5) | Blocked: 18 days only |
| 2 | HL-Bybit price lead | HL-Bybit spreads (2.46M) | LONG | **PROMISING** | PENGU +24.8bps 63% WR, MON 82% WR | Blocked: needs sub-min execution |
| 3 | Whale consensus bias | HL whale (14.6K) | FOLLOW/FADE | DEAD | Mixed direction, small samples | No |
| 4 | Cross-exchange funding | HL vs Bybit funding | FADE | DEAD | 0-3 divergence events in 90h | No |
| 5 | Max pain gravity | Deribit options | Mean-revert | DEAD | 49.3% = random | No |
| 6 | Liquidation reversal | Coinalyze liqs (348K) | LONG after cascade | DEAD | WR 36-48%, negative returns | No |

## Detailed Findings

### 1. X15 Options Contrarian (PROMISING)
- **BTC P/C OI ratio SHORT**: when retail piles into calls (low P/C, z < -1.5), go SHORT
- 8h: -0.74%, 92% WR, t=-5.25 (n=26)
- 24h: -0.66%, 69% WR, t=-2.81
- LONG side (z > 1.5) is weak/mixed
- ETH: signal INVERTED, not viable
- **Blocker: only 18 days of data, need 60+ for validation**
- See: docs/research/x15_options_contrarian.md

### 2. HL-Bybit Cross-Exchange Price Lead (PROMISING)
- HL price LEADS Bybit on mid-cap pairs by 10-25bps
- Top signals at >20bps threshold:
  - PENGU: +24.8bps/1min, 63% WR, 220 signals/day
  - MON: +21.4bps/1min, 82% WR, 12/day
  - DOGE: +17.3bps/1min, 67% WR, 38/day
  - SAND: +17.0bps/1min, 73% WR, 28/day
- Majors (BTC/ETH): spread too tight, barely covers fees
- **Blocker: sub-minute execution required. HB-native controllers can't do this. Would need custom engine like H2 V3.**
- Could feed into X16 "HL Price Lead Scalper" if custom engine justified

### 3. Whale Consensus (DEAD)
- HYPE is the only coin where FOLLOW works (+0.76% spread at 8h)
- ETH, SOL, TAO: FADE direction (opposite of thesis)
- 10 days of data, n=5-23 per signal — too noisy
- Verdict: needs 30+ days minimum, but even then likely weak

### 4. Cross-Exchange Funding Divergence (DEAD)
- HL vs Bybit funding correlation only 0.3-0.5
- Divergence events (z > 2): 0-3 per pair in 90 hours
- Spread is tiny (0.1-0.2 bps)
- Funding settles every 8h — hourly noise dominates

### 5. Max Pain Gravity (DEAD)
- Price moves toward max pain 49.3% of the time = random
- Current BTC max pain at $52K vs price $78K — dominated by long-dated OTM options
- Not viable even with near-term expiry filtering (too few data points)

### 6. Liquidation Cascade Reversal (DEAD)
- After extreme long liquidations, price continues DOWN (WR 36-48% for bounce)
- SOL worst: -0.77% at 24h after liq cascade
- Liquidations are a CONTINUATION signal, not reversal
- Confirms X10 kill decision

## What's Still Running
- **OKX funding rate exploration** (background agent)
- **DefiLlama stablecoin flow EDA** (background agent)

## Additional Signals Tested (May 1 PM session)

| # | Signal | Data Source | Direction | Result | Details |
|---|--------|------------|-----------|--------|---------|
| 7 | IV term structure slope | Deribit options (2.9M) | LONG/SHORT | **VERY PROMISING** | 94% WR LONG at 24h (z>1.5), 76% WR SHORT (z<-1.5). 19d only. |
| 8 | BTC funding velocity | Bybit funding (611 obs, 203d) | FADE | DEAD | Not significant at any threshold |
| 9 | Binance taker buy ratio (spot) | Binance candles (208d) | FADE | DEAD | WR ~50% everywhere |
| 10 | Combined OI + funding | Bybit OI+funding | FADE | DEAD | p>0.15, noisy at combos |
| 11 | Cross-sectional LS ratio | Bybit LS (15 pairs, 96d) | MOMENTUM | MOMENTUM (not alpha) | Crowd is right in aggregate |
| 12 | Santiment social volume | Santiment API (336d) | SHORT | WEAK-MODERATE | 7d: -2.2%, 65% WR, p=0.034. Daily only. |
| 13 | Santiment exchange flow | Santiment API (336d) | BOTH | DEAD | No signal in net flow |
| 14 | Santiment active addresses | Santiment API (365d) | SHORT | WEAK | 7d: -2.2%, 61% WR, p=0.022. Correlated with #12. |

## Strategy Pipeline Status

| ID | Signal | Status | Validation Date | Expected Edge |
|----|--------|--------|-----------------|---------------|
| X14 | LS ratio crowd fade | LIVE (paper) | Ongoing | 1.09-1.32 PF |
| X15 | Options P/C OI ratio | Data accumulating | ~June 10 | 92% WR (BTC SHORT) |
| X16 | IV term structure | Data accumulating | ~June 10 | 94% WR (BTC LONG in fear) |

## Key Insights

1. **Individual pair LS = mean-reversion, Market-wide LS = momentum.** X14 exploits pair-level overreaction. The crowd is right in aggregate but wrong on specific pairs.
2. **Santiment social volume + active addresses are correlated** — both spike during mania. Useful as a 7-day SHORT filter (not standalone).
3. **Exchange flow is NOT predictive.** The popular "inflow = bearish" narrative is false in this 336-day sample.
4. **X16 is the strongest raw signal ever found.** Economic rationale is sound (panic overshoot → mean-reversion). Needs data depth.
5. **BTC funding-based signals at 8h resolution are too slow.** X14 works because LS ratio is hourly and per-pair.

## Next Signal Sources to Explore (LEADING signals only)

**Hard filter**: signal must LEAD price moves (peak BEFORE reversal), not COINCIDE with them.
Reactive/coincident signals (DVOL spike, liquidation cascade) are statistically significant but untradeable.

1. **GeckoTerminal DEX volume divergence** — DEX buying/selling imbalance BEFORE CEX price moves (free API)
2. **Whale position changes** — HL whale data: do whales move BEFORE retail? (we have 14.6K docs)
3. **Cross-pair momentum spillover** — when BTC/ETH move, which alts follow with a LAG? (leading indicator for laggards)
4. **Funding rate LEVEL persistence** — not velocity (dead), but sustained high funding as a slow-building crowd signal (like X14 but at 8h resolution)
5. **Order book imbalance** — bid/ask depth ratios from HL L2 data (we have tick data). Imbalance LEADS price by minutes-hours.
6. **Alt-pair options skew** — ETH/SOL IV term structure when more data accumulates (June+)
