# X13 Capitulation Reversal — Research Notes

## Signal Discovery (2026-04-30)

### Hypothesis
When both OI and price drop sharply simultaneously (capitulation event), forced liquidations exhaust selling pressure and price rebounds. Go long after capitulation, exit on mean reversion.

### EDA Results (15 pairs, hourly data, ~2 years)

**Capitulation regime defined as:**
- OI 24h change < -1 std (OI dropping significantly)
- Price 24h change < -0.3 std (price dropping, not just flat)

**Forward 24h returns after capitulation:**
| Metric | Value |
|--------|-------|
| Events | 4,799 |
| Avg 24h fwd return | +0.512% |
| Win rate (>0 in 24h) | 52.1% |
| t-statistic | 7.14 |
| p-value | <0.0001 |
| Baseline avg 24h return | -0.071% |
| Baseline win rate | 47.9% |

**Comparison with other OI-price regimes:**
| Regime | Events | 24h Fwd | p-value | Signal? |
|--------|--------|---------|---------|---------|
| OI down + Price down (capitulation) | 4,799 | +0.512% | <0.0001 | YES |
| OI up + Price flat (accumulation) | 2,160 | +0.018% | 0.886 | NO |
| OI down + Price flat (distribution) | 2,252 | -0.019% | 0.848 | NO |
| OI up + Price up (trend) | 5,964 | +0.061% | 0.394 | NO |

### Why This Makes Sense
1. Forced liquidations create supply that exceeds organic demand
2. Once liquidation cascade exhausts (OI stabilizes), price reverts to fair value
3. This is the crypto equivalent of "selling climax" — a well-known pattern in equity markets
4. The signal is NOT mean-reversion on price alone (which doesn't work) — it requires OI confirmation

### Data Available
- Bybit OI: 730 days, 259 pairs, updated every 15 min
- Already in derivatives merge pipeline (BulkBacktestTask._merge_derivatives_into_candles)
- OI is already in FeatureBase as `oi_value` via BybitDerivativesTask

### Next Steps
1. Design HB-native controller (X13_capitulation_reversal.py)
2. Signal: OI 24h pct change < threshold AND price 24h pct change < threshold
3. Entry: go long when both conditions met
4. Exit: TP at mean reversion target, SL at further drawdown, time limit 24-48h
5. Backtest at 1m resolution across top 20 pairs
6. Key parameters to optimize: OI threshold, price threshold, TP/SL levels, holding period

### Risks
- Signal may be regime-dependent (works in mean-reverting markets, fails in trending)
- 52% win rate means need favorable R:R (TP > SL) to be profitable
- OI data has only 15-min granularity — signal timing may lag
- Need to check if the signal persists when excluding BTC (BTC capitulation may dominate)

### Other Signals Tested (Apr 30 EDA)
- **DVOL regime trading**: Small samples, inconclusive. DVOL changes >5% predict +1% in 24h but n<60.
- **Liquidation cascade reversal**: Cascades predict CONTINUATION, not reversal. Hypothesis was wrong.
- **Cross-exchange funding divergence (BB vs HL)**: Short signal marginal (p=0.10), -0.41% avg. Only 371 events. Insufficient for standalone strategy but could be a filter.
