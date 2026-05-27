# HL MM V4: Imbalance-Directional Market Making

## Status: PLAN — Awaiting Codex adversarial review before any code

## 1. Signal Evidence

### What we found
L2 order book imbalance on Hyperliquid predicts short-term price direction.

**Signal**: `imbalance_topn = (bid_sz - ask_sz) / (bid_sz + ask_sz)` across top 20 levels.
Positive = more bids = bullish. Negative = more asks = bearish.

### EDA Results (7.5 days, Apr 28 - May 5 2026)

| Coin | 1min IC | 5min IC | 15min IC | 30min IC | N (10s bars) |
|------|---------|---------|----------|----------|--------------|
| BTC  | 0.1348  | 0.0656  | 0.0357   | 0.0400   | 45,009       |
| ETH  | 0.1063  | 0.0632  | 0.0459   | 0.0583   | 44,643       |
| SOL  | 0.1017  | 0.0534  | 0.0509   | 0.0688   | 42,097       |
| HYPE | 0.0225  | 0.0005  | -0.0076  | -0.0134  | 44,145       |

All IC p-values < 0.0001 for BTC/ETH/SOL at 1-5min.

### Out-of-Sample Validation (train first 5d, test last 2.5d)

| Coin | Train IC (1min) | Test IC (1min) | Test mean (bps) | Test t-stat |
|------|-----------------|----------------|-----------------|-------------|
| BTC  | 0.1221          | 0.1706         | +0.77           | 9.94        |
| ETH  | 0.1005          | 0.1221         | +0.72           | 9.58        |
| SOL  | 0.0896          | 0.1324         | +0.59           | 9.83        |

Test IC is HIGHER than train IC on all 3 coins. Signal strengthened OOS.

### Threshold test (top/bottom 20% imbalance, OOS)
- BTC 1min: n=5885 events, mean=+0.77bps, t=9.94, WR=54%
- BTC 5min: n=5867 events, mean=+1.12bps, t=6.74, WR=55%
- BTC tight spread + strong imbalance -> 15min: n=240, mean=+3.58bps, t=7.32, WR=67%

### Potential concerns to triple-check
1. **Lookahead bias**: The 10s bar uses `last` imbalance and `last` mid_px from same interval. Forward return is computed from bar close to future bar close. Is the imbalance measured BEFORE the price move it's predicting?
2. **Autocorrelation**: Consecutive 10s bars are not independent. The 18K events from threshold test include overlapping forward return windows.
3. **Regime dependency**: Only 7.5 days of data during a specific market regime (late April to early May 2026). BTC was trending up ~$77K to ~$97K in this period.
4. **HYPE doesn't work**: Signal dies at >1min on HYPE. Possible explanation: HYPE L2 is thinner/more manipulated. Or signal is only for deep-book assets.
5. **Edge vs costs**: +0.77bps at 1min. HL maker entry (1.7bps) + taker exit (3.5bps) = 5.2bps. The raw edge doesn't cover costs. Need maker fill selectivity or longer hold.
6. **The imbalance might reflect, not predict**: Large resting bids might CAUSE price to stay up (support), not predict future direction. When support breaks, loss would be large.

## 2. Strategy Design (V4)

### Concept
Use L2 imbalance as directional bias for HL market making. When the book strongly predicts direction, quote ONLY the aligned side (maker entry). Exit with taker when profit target hit or signal reverses.

### Quoting Logic
```
IF imbalance_z > ENTRY_THRESHOLD (e.g., 1.0):
    # Bullish: buy with limit order
    Place bid at best_bid (maker)
    No ask
    IF filled: hold, exit when imbalance reverses OR TP hit
    
ELIF imbalance_z < -ENTRY_THRESHOLD:
    # Bearish: sell with limit order
    Place ask at best_ask (maker)
    No bid
    IF filled: hold, exit when signal reverses OR TP hit

ELSE:
    # Neutral: standard two-sided MM (or no quotes)
```

### Exit Logic
- **Signal reversal**: imbalance_z crosses zero -> exit at market
- **Time limit**: 5 minutes max hold
- **TP**: 2-3 bps (maker if possible, taker if urgent)
- **SL**: 5 bps emergency taker exit
- **Trailing**: activate at 2bps, trail 1bps

### Infrastructure Changes (V2 -> V4)
Only `state_machine.py` quoting logic needs to change:
- Current V2: quotes both sides, suppresses unsafe side on strong_imbalance
- V4: quotes ONLY the directional side when |imbalance_z| > threshold
- `signal_engine.py`: already computes imbalance_z (no changes)
- `orchestrator.py`: may need minor signal routing changes
- `fill_tracker.py`: works as-is
- `risk_manager.py`: works as-is

### Capital & Position Sizing
- HL equity: $48.46 USDC
- Position size: $10-20 per trade (conservative)
- Max positions: 2 concurrent
- Max daily loss: $5 (10% of equity)

### Pairs
Start with BTC + ETH (strongest signal, deepest books).
SOL as third after validation.
NO HYPE (signal dead at >1min).

## 3. Risk Analysis

### What can go wrong
1. **Signal is regime-specific**: 7.5 days during a bull trend. In ranging/bear markets, book imbalance may not predict direction.
2. **Fill rate too low**: When book is bid-heavy, our bid at best_bid competes with large resting orders. May rarely get filled.
3. **Adverse fills**: We get filled precisely when the signal is WRONG (price breaks through support/resistance).
4. **Latency disadvantage**: Our code runs on a Mac Mini via REST/WS. HFT firms are co-located. We see the book AFTER they've already acted.
5. **Inventory risk**: If we accumulate a position and the signal flips, we exit at taker cost (3.5bps) plus adverse price movement.

### Mitigations
- Hard daily loss limit ($5)
- Emergency flatten on any position > 5 minutes old
- Start with minimum size ($10/trade)
- Dry-run for 24h before real capital
- Continuous monitoring via Telegram

## 4. Success Criteria

### Dry Run (24h)
- [ ] >20 fills
- [ ] >50% of fills profitable (before fees)
- [ ] No position stuck > 5 minutes
- [ ] Fill rate > 10% of quotes placed

### Paper (1 week)
- [ ] Net positive after fees
- [ ] Sharpe > 1.0 on daily PnL
- [ ] No single loss > $2

### Live Gate
- [ ] Paper passes all above
- [ ] Codex adversarial review passed
- [ ] Alberto approval

## 5. Codex Adversarial Review (2026-05-05)

**Verdict: NOT DEPLOYABLE. 11 critical findings.**

### Critical Issues
1. **Edge below costs**: +0.77bps vs 5.2bps fees = negative EV
2. **T-stats inflated**: Overlapping 10s windows (5min overlaps 30x). t=9.94 is autocorrelation artifact
3. **OOS misreported**: Code does 3.49d/1.72d, not 5d/2.5d as claimed
4. **Cherry-picking**: "Tight spread + imbalance" result was in-sample, presented as OOS
5. **Actionability leak**: Bar-average imbalance isn't known until bar ends. Need 1-bar shift
6. **Signal mismatch**: EDA tests raw mean, plan proposes z-scored. No equivalence shown
7. **Regime unresolved**: One bullish episode. Long/short legs not separated
8. **No fill model**: Passive fill probability, queue position, adverse selection unmodeled
9. **Multiple testing**: Sweeps coins x horizons x variants, promotes winners uncorrected
10. **Data quality**: No gap analysis, no timestamp origin check
11. **Liquidation detection unsupported**: Same-window threshold mining

### Required Fixes Before Any Code
1. Non-overlapping samples or HAC/block-bootstrap inference
2. Point-in-time executable signal (shift signal by 1 bar minimum)
3. Long and short legs separately across bull/flat/bear regimes
4. Passive fill simulation with queue/adverse-selection model

## 6. Fixed EDA Results (2026-05-05)

Fixes applied: non-overlapping samples, 1-bar signal lag, block bootstrap, long/short separated, train quantiles only.

### BTC 1min (test period: 3.4 days, non-overlapping)
| Leg | n | Mean (bps) | WR | Bootstrap p |
|-----|---|-----------|-----|------------|
| LONG | 616 | +0.72 | 50% | **0.028** |
| SHORT | 366 | +0.23 | 50% | 0.130 |
| BOTH | 982 | +0.53 | 50% | **0.015** |
| NEUTRAL | 1498 | +0.03 | - | - |

### ETH 1min
| Leg | n | Mean (bps) | Bootstrap p |
|-----|---|-----------|------------|
| BOTH | 962 | +0.29 | 0.046 |
| Edge above neutral | - | -0.02 | No value-add |

### SOL 1min: p=0.071, not significant. 5min: ALL DEAD.

### Verdict
Signal kernel is real for BTC LONG at 1min (p=0.028). But edge is 0.7bps vs 5.2bps costs. Not tradeable without pure maker execution or fee reduction. Original t=9.94 was autocorrelation artifact (honest t=2.05).

### Remaining gap: #4 (fill model) not yet done. Even if edge were larger, passive fill economics are unmodeled.
