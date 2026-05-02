# X15 Options Contrarian — Research Notes

**Created:** 2026-05-01
**Status:** Phase 0 EDA — PROMISING, needs more data
**Signal:** Fade retail options sentiment via Deribit P/C OI ratio z-score
**Asset:** BTC-USDT only (ETH signal inverted — does NOT work)

## Thesis

When retail traders pile into calls (low P/C ratio), they're systematically wrong at extremes.
Fade bullish options sentiment by going SHORT when P/C OI ratio z-score drops below -1.5.

This is the OPTIONS MARKET version of X14's LS ratio crowd fading — same psychological mechanism,
independent data source. If validated, it's uncorrelated with X14 (different input data).

## EDA Results (18 days, 389 hourly snapshots)

### BTC P/C OI Ratio

**SHORT signal (z < -1.5, low P/C = bullish sentiment):**
| Horizon | Avg Return | WR (short) | t-stat | n |
|---------|-----------|-----------|--------|---|
| 4h | -0.30% | 73% | -2.70 | 26 |
| 8h | -0.74% | 92% | -5.25 | 26 |
| 24h | -0.66% | 69% | -2.81 | 26 |

**LONG signal (z > 1.5, high P/C = bearish sentiment):**
| Horizon | Avg Return | WR (long) | t-stat | n |
|---------|-----------|----------|--------|---|
| 4h | -0.00% | 50% | -0.01 | 78 |
| 8h | +0.12% | 42% | 0.78 | 78 |
| 24h | +0.58% | 51% | 2.33 | 78 |

**Verdict:** SHORT side is strong (92% WR at 8h!). LONG side is weak/mixed.
Same asymmetry as X14 — retail is wrong when bullish, not when bearish.

### ETH P/C OI Ratio
Signal goes WRONG direction. Low P/C → positive returns (not negative).
ETH options market has different dynamics. **ETH is NOT viable.**

### Max Pain
Price moves toward max pain 49.3% of the time = random. Not a signal.

### Quintile Analysis (raw P/C OI, not z-scored)
Clean monotonic relationship at 24h:
- Q0 (lowest P/C): -0.15% (t=-2.05)
- Q4 (highest P/C): +0.86% (t=8.70)
- Spread: +1.0% over 24h

### Signal Frequency
- z > 2.0: 2.5/day (BTC)
- z > 1.5: 4.8/day (BTC)
- z < -1.5 (SHORT triggers): ~1.6/day
- z < -2.0 (conservative): ~0.8/day

## Key Concerns

1. **Only 18 days of data (389 hours)** — NOT enough for validation
2. **26 events at z < -1.5** — very small sample for the best signal
3. **BTC-only** — can't diversify across pairs
4. **Upward trending market** — the entire 18-day window was bullish. SHORT signals working well in a bull market is suspicious (could be mean-reversion artifact)
5. **Need 60+ days minimum** for walk-forward validation

## Blockers

- Data accumulation: need 60+ days → earliest validation: ~June 10
- BTC-only limits position sizing and diversification
- Deribit options data is 15-min resolution, need to verify collector reliability

## Next Steps (when data sufficient)

1. Wait for 60+ days of data (~June 10)
2. Re-run EDA with 2x the data
3. If still significant: build X15 controller (SHORT-only, BTC-USDT)
4. Backtest with HB BacktestingEngine (need to merge options data into candles)
5. Walk-forward validation
6. Deploy alongside X14 for uncorrelated alpha

## Relationship to X14

X14 fades LS ratio crowding. X15 fades options sentiment crowding.
Both exploit the same retail behavioral bias through different instruments.
Correlation expected to be LOW because:
- Different data source (Deribit options vs Bybit/Binance LS ratio)
- Different asset (BTC-only vs multi-pair)
- Different frequency (P/C OI changes slowly vs LS ratio is more volatile)

If both validate, they form a multi-signal "retail sentiment fade" portfolio — Gate 2 material.
