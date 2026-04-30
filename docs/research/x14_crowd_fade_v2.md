# X14 Crowd Fade V2 — Research Notes

## Signal Discovery (2026-04-30)

### Hypothesis
When retail traders are extremely crowded on the long side (LS ratio z-score > 2 over 7-day rolling window), fade them by going SHORT. The crowd is wrong at extremes.

### Validation on TWO Independent Data Sources

**Binance Top Trader L/S Ratio (fetched live, 500h history):**
| Metric | Value |
|--------|-------|
| Events (crowded long z>2) | 498 |
| Avg 24h fwd return | -1.268% |
| Win rate (price drops) | 60.0% |
| t-statistic | -7.52 |
| p-value | <0.0001 |

**Bybit LS Ratio (95 days, 259 pairs available, 30 tested):**
| Metric | Value |
|--------|-------|
| Events (crowded long z>2) | 2,152 |
| Avg 24h fwd return | -0.612% |
| Win rate (price drops) | 58.4% |
| t-statistic | -5.87 |
| p-value | <0.0001 |
| Excess vs baseline | -0.542% |

### Best Pairs (sorted by WR for shorts)
| Pair | Events | 24h Fwd | WR Short |
|------|--------|---------|----------|
| UNI-USDT | 74 | -2.39% | 85.1% |
| WLD-USDT | 84 | -3.88% | 81.0% |
| ATOM-USDT | 81 | -1.37% | 79.0% |
| SEI-USDT | 27 | -1.67% | 77.8% |
| DOT-USDT | 60 | -2.63% | 76.7% |
| ETC-USDT | 49 | -0.44% | 75.5% |
| ALGO-USDT | 81 | -1.99% | 74.1% |
| SOL-USDT | 60 | -1.98% | 73.3% |
| FIL-USDT | 48 | -1.06% | 70.8% |
| OP-USDT | 123 | -2.56% | 69.1% |

### Pairs That DON'T Work (skip these)
| Pair | WR Short | Notes |
|------|----------|-------|
| APT-USDT | 19.4% | Inverted — crowded longs are RIGHT |
| AAVE-USDT | 37.3% | Same |
| HBAR-USDT | 40.1% | Same |
| ETH-USDT | 43.4% | Too efficient |

### Why This is Different From E4 (Crowd Fade)
E4 used raw LS ratio level + multiple factors. It produced PF~1.0 and was shelved.
X14 improvements:
1. **Z-score over 7d rolling** (not raw level) — captures RELATIVE crowding
2. **SHORT-only** — crowded short → long DOES NOT work (tested, p=NS)
3. **Pair selection matters** — only deploy on pairs with >65% WR
4. **Single clean signal** — no stacking with other factors

### Data Available
- Bybit LS ratio: 95 days, 259 pairs, every 15 min (in our pipeline)
- buy_ratio field in derivatives merge pipeline
- Already used by E4 — infrastructure exists

### Key Difference from E4
E4 tried to use LS ratio as one of many filters. X14 uses it as THE signal:
- Entry: buy_ratio z-score > 2.0 (extremely crowded long) → SHORT
- NO long side (crowded short doesn't predict)
- Pair-specific: only trade pairs with historical WR > 65%

### Next Steps
1. Build HB-native controller (x14_crowd_fade_v2.py)
2. Signal: buy_ratio 7d rolling z-score > threshold
3. Entry: SHORT only
4. Exit: ATR-based TP/SL, 24-48h time limit
5. Backtest at 1m resolution
6. Walk-forward validation

### Risks
- 95-day history is short (need longer to validate regime robustness)
- LS ratio has 15-min granularity — signal may be lagging
- Some pairs show INVERTED signal (APT, AAVE, HBAR) — must exclude
- Baseline is -0.07% (slight bearish drift) — need to retest in bull market
- Need to start collecting Binance top trader ratio for longer history

### Other Signals Tested This Session (Apr 30)
| Signal | Result | p-value |
|--------|--------|---------|
| Capitulation reversal (OI+price drop) | Weak in backtest despite strong EDA | Mixed |
| CVD bull divergence | +0.15%, p<0.0001 but can't get taker data from Bybit | No data |
| Taker buy/sell ratio (Binance) | No signal (p=0.33) | Fail |
| Funding rate velocity/acceleration | Tiny effect (-0.06%) | Marginal |
| Google Trends momentum | Too few events | Inconclusive |
| Fear & Greed Index | No predictive power | Fail |
| Options skew (Deribit) | Data too short to test | Blocked |
| Cross-exchange funding divergence | Marginal (p=0.10) | Weak |
| MTF momentum alignment | Positive both directions (market drift, not signal) | Fail |
| Binance global L/S account ratio | Works for crowded long side only, weaker than top trader | Partial |
