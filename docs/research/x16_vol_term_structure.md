# X16 Vol Term Structure — Research Notes

**Created:** 2026-05-01
**Status:** KILLED — Failed backtest gate (PF=0.42, 40% WR)
**Signal:** LONG when DVOL spikes (fear) — statistically real but NOT tradeable
**Asset:** BTC-USDT only (Deribit DVOL index)
**Controller:** `app/controllers/directional_trading/x16_vol_fear_fade.py`
**Kill reason:** EDA-to-backtest gap. Entry during drawdowns, unreliable 48h recovery.

## Thesis

When the IV term structure inverts (short-term IV > long-term IV = backwardation), options traders expect imminent volatility. This happens during selloffs and panics that tend to overshoot. Going LONG during backwardation captures the mean-reversion bounce.

When the term structure is in deep contango (short-term IV << long-term IV), the market is complacent. Going SHORT captures the next correction.

## Data Source

- **Collection:** `deribit_options_surface` (2.9M docs, BTC + ETH)
- **Resolution:** 15-minute snapshots
- **Methodology:** ATM calls (strike within 5% of underlying), grouped by expiry
  - Short-term IV: expiries 1-7 DTE
  - Long-term IV: expiries 30-90 DTE
  - Slope = short_iv - long_iv (positive = backwardation/fear)
  - Z-scored over rolling window

## EDA Results (19 days, 436 hourly observations)

### Term Structure Stats
- Mean slope: -1.72% (normal contango)
- Std: 3.72%
- Short-term ATM IV: 39.3% avg
- Long-term ATM IV: 41.0% avg

### Signal Performance

**LONG on backwardation (z > threshold):**
| Threshold | 4h Return | 4h WR | 8h Return | 8h WR | 24h Return | 24h WR | N |
|-----------|-----------|-------|-----------|-------|------------|--------|---|
| z > 1.0 | +0.174% | 58% | +0.445% | 65% | +1.545% | 79% | 72 |
| z > 1.5 | +0.082% | 53% | +0.253% | 59% | **+1.973%** | **94%** | 32 |

**SHORT on contango (z < -threshold):**
| Threshold | 4h Return | 4h WR | 8h Return | 8h WR | 24h Return | 24h WR | N |
|-----------|-----------|-------|-----------|-------|------------|--------|---|
| z < -1.0 | +0.049% | 46% | -0.032% | 49% | -0.482% | 59% | 78 |
| z < -1.5 | +0.145% | 57% | +0.035% | 54% | **-1.170%** | **76%** | 17 |

### Signal Frequency
- z > 1.0: ~72/19d = 3.8/day (LONG signals)
- z > 1.5: ~32/19d = 1.7/day
- z < -1.0: ~78/19d = 4.1/day (SHORT signals)
- z < -1.5: ~32/19d = 1.7/day

## Key Concerns

1. **Only 19 days of data** — not enough for any governance gate
2. **94% WR is likely inflated** by a single market regime (April correction -> bounce)
3. **BTC-only** — no diversification across pairs
4. **24h holding period** — longer than X14's typical duration
5. **Bidirectional** — unlike X14 (SHORT-only), this trades both sides
6. **Trend dependency:** The 19-day window saw BTC go sideways/slightly up. In a strong downtrend, LONG on backwardation would get crushed. Need trend filter.

## Economic Rationale

**Why this should work:**
- IV backwardation = panic/fear. Panics overshoot. Mean-reversion is reliable at extremes.
- IV contango = complacency. Complacent markets underprice tail risk. Corrections follow.
- This is well-documented in equity options literature (VIX term structure trading has been profitable since 2004).
- Crypto-specific: retail-dominated options market likely overreacts more than institutional equity markets.

**Why it might NOT work:**
- 19 days could capture a single regime (bull bounce after correction)
- BTC options OI is concentrated in a few market makers — IV reflects their hedging needs, not retail sentiment
- Crypto vol is already high — term structure dynamics may differ from equities
- ETH options data (if testable) would add conviction, but ETH has different dynamics

## Relationship to Other Strategies

| Strategy | Signal Source | Direction | Correlation Expected |
|----------|-------------|-----------|---------------------|
| X14 | LS ratio (Bybit+Binance) | SHORT-only | LOW — different data, mechanism |
| X15 | P/C OI ratio (Deribit) | SHORT-only | MEDIUM — same data source, different metric |
| X16 | IV term structure (Deribit) | BIDIRECTIONAL | LOW-MEDIUM vs X14, HIGH vs X15 |

If X16 validates alongside X14, we have 2 uncorrelated strategies = Gate 2 progress.
X16 LONG + X14 SHORT could be portfolio-level hedging (both directions covered).

## DVOL Variant — Validated Signal (212 days)

The DVOL index (Deribit implied vol index, hourly) provides the SAME signal as term structure
but with **212 days of data** — enough for full governance validation.

### DVOL EDA Results (5000+ hourly observations, 212 days)

**DVOL spike (24h change z-scored):**
| Threshold | 24h Return | 24h WR | 48h Return | 48h WR | N |
|-----------|-----------|--------|-----------|--------|---|
| z > 1.5 | +0.582% | 56% | +1.120% | 64% | 473 |
| z > 2.0 | +0.748% | 55% | +1.499% | 69% | 263 |
| z > 2.5 | +1.433% | 64% | +1.836% | 73% | 124 |

All p < 0.001 (highly significant).

**Independence test (NOT just "buy the dip"):**
- DVOL spike + price dropped >1%: fwd 48h = +1.54%, 68% WR (n=227)
- DVOL spike + price NOT dropped: fwd 48h = +1.24%, **78% WR** (n=36)
- Price drop WITHOUT DVOL spike: fwd 24h = **-0.58%** (continues DOWN!)

This proves DVOL adds alpha BEYOND price momentum.

**Regime stability:**
- First 106 days: +1.94%, 75% WR
- Last 106 days: +1.16%, 65% WR
- Edge declined but remains STRONG in both halves

### Comparison: DVOL vs Term Structure

| Metric | DVOL (hourly index) | Term Structure (ATM slope) |
|--------|--------------------|----|
| Data depth | 212 days (365d in MongoDB) | 19 days |
| Resolution | Hourly | ~Hourly (15min options) |
| Signal | 24h delta z > 2 | Slope z > 1.5 |
| Best WR | 73% at 48h (z>2.5) | 94% at 24h (z>1.5) |
| Sample | 124-473 events | 32-72 events |
| Validation ready | YES | NO (June 10) |

**Decision:** Use DVOL for immediate backtesting and deployment. Add term structure as confirmation filter when data accumulates.

## Next Steps (IN PROGRESS)

1. ~~Wait for data~~ → DVOL has 212 days ✓
2. Controller built: `x16_vol_fear_fade.py` ✓
3. Registered in strategy_registry ✓
4. **BACKTEST RUNNING** (bulk_backtest, 200 days, BTC-USDT)
5. Walk-forward validation (after bulk passes)
6. Robustness: param sensitivity, Monte Carlo, slippage
7. Deploy alongside X14 for diversified alpha

## Rejected Sub-Signals (tested same session)

| Signal | Result | Why Dead |
|--------|--------|----------|
| BTC funding velocity (24h change) | Not significant (p>0.15) | 8h resolution too slow |
| Combined OI surge + funding | p>0.15, n too small | Noisy at combo thresholds |
| Binance taker buy ratio (spot) | WR ~50% everywhere | No signal in spot flow |
| Cross-sectional LS ratio (mkt avg) | MOMENTUM (not reversal) | Not a new alpha source |
