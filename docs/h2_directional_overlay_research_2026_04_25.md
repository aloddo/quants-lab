# H2 Directional TP/SL Overlay Research (Apr 25, 2026)

## Hypothesis
Since H2 arb entries are BUY_BB_SELL_BN (long Bybit perp, sell Binance spot), the BB perp side carries 10x leveraged directional exposure. If BB pumps significantly, closing BB only (skip BN buyback) captures the directional gain at ~5-14x the arb capture. Conversely, a directional SL could protect against approaching liquidation.

## Data
- 28 closed V2 positions (all BUY_BB_SELL_BN)
- BB move range: -2.94% to +7.20%, std=2.6%
- Hold times: 3-1014 min (median 40 min)

## Key Findings

### 1. BB and BN move together (r=0.98)
BB and BN price correlation during positions is 0.98. A -5% BB move almost always means a -5% BN move too. The SPREAD (BB-BN delta) is what the arb trades, not the directional move.

### 2. The BB-BN delta IS the arb alpha (r=0.998)
BB outperforms BN in 93% of positions (25/27) by +0.51% on average. This delta is perfectly correlated with arb PnL (r=0.998). The directional edge is NOT independent of the arb edge — they are the same thing measured differently.

### 3. Directional SL is catastrophic
| SL Level | Fires | Simulated PnL | Actual PnL | Difference |
|----------|-------|---------------|------------|------------|
| -2% | 5/27 | -$0.71 | +$0.56 | **-$1.28** |
| -3% | 0/27 | +$0.56 | +$0.56 | $0.00 |
| -5% | 0/27 | +$0.56 | +$0.56 | $0.00 |

All 5 positions with BB < -2% had positive or near-zero arb PnL. The arb spread reverted even as price dropped. Directional SL exits a hedged position based on a single-leg metric — structurally wrong.

### 4. Directional TP looks good but is misleading
| TP Level | Fires | Simulated PnL | Actual PnL | Difference |
|----------|-------|---------------|------------|------------|
| 2% | 6/27 | +$3.00 | +$0.56 | **+$2.44** |
| 3% | 6/27 | +$3.00 | +$0.56 | **+$2.44** |
| 5% | 2/27 | +$1.59 | +$0.56 | +$1.03 |
| 8% | 0/27 | +$0.56 | +$0.56 | $0.00 |

The simulation inflates TP value because it counts BB directional PnL ($0.30-0.71/trade at 10x leverage) without accounting for:
- Inventory depletion (BN sold, not bought back)
- Extra autoseed fee for next entry
- The arb was already profitable in 5/6 TP-eligible trades

Only 1/27 positions (KERNEL +3.06%) had a failing arb (-$0.057) where directional TP would have been rescue ($+0.30). 3.7% occurrence rate — too rare for a system.

### 5. The one interesting edge case
When the arb FAILS (spread widens further) but BB pumps directionally, directional TP rescues the position. This happened once in 28 trades. With 100+ trades we can determine if this is a pattern or noise.

## Recommendation
1. **Directional SL: REMOVE permanently.** Contradicts the hedged arb thesis. Arb spread SL (2x entry spread) handles downside correctly.
2. **Directional TP: Keep at 8% (inert).** Zero fires in 28 trades. Leave as dormant option until N > 100.
3. **Future research (N > 100):** Track intra-position BB peak/trough, analyze arb failure + directional opportunity overlap, model inventory opportunity cost.

## Status
- Directional SL: disabled in code (DIRECTIONAL_SL_PCT = 0.0)
- Directional TP: set to 8% (above P95, effectively inert)
- One KAT position was incorrectly closed by directional SL at -5.1%, costing $0.52. PnL corrected in MongoDB.
