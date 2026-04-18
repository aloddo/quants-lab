# Cross-Exchange Arb — Stress Test Results
**Date:** Apr 18, 2026
**Data:** 14 pairs, 208 days, 5m resolution (Binance spot vs Bybit perp)
**Cached:** `app/data/cache/arb_backtest/*.parquet` (14 files, ~15MB total)

## Two Models Compared

### Model 1: Spread-Based (old — what we were using)
P&L = (entry_spread - exit_spread) * position_size - fees

### Model 2: Leg-Level + Funding + Slippage (realistic)
- P&L computed per-leg: actual price change on each exchange
- Funding: Bybit perp funding charged every 8h (worst case: always pay)
- Slippage: 5bps adverse on entry, 2bps adverse on exit (asymmetric)
- Fees: 24bp RT (Binance 10bp/side + Bybit 2bp maker/side)

## Results (entry >= 60bps, exit <= 5bps, 24h max hold, $200/trade)

### Spread-Based
| Variant | Trades | PnL | WR | MaxDD | Sharpe | Stops |
|---------|--------|-----|----|----|--------|-------|
| SL 2.5x | 1828 | $952 | 94% | -$93 | 4.8 | 112 |
| No SL (24h) | 1762 | $1686 | 100% | -$5 | 34.8 | 0 |
| 2.5x+2h grace | 1796 | $1462 | 97% | -$26 | 17.8 | 50 |
| 5x+30m grace | 1780 | $1469 | 98% | -$28 | 14.5 | 25 |

### Leg-Level + Funding + Slippage (REALITY)
| Variant | Trades | PnL | WR | MaxDD | Sharpe | Stops | Funding |
|---------|--------|-----|----|----|--------|-------|---------|
| SL 2.5x | 1828 | $256 | 86% | -$126 | 1.5 | 112 | $63 |
| No SL (24h) | 1762 | $647 | 90% | -$87 | 7.7 | 0 | $102 |
| 2.5x+2h grace | 1796 | $408 | 88% | -$104 | 3.7 | 50 | $65 |
| 5x+30m grace | 1780 | $403 | 89% | -$132 | 3.2 | 25 | $83 |

### Key Takeaway
Spread-based model overstates P&L by 60-75%. Real edge exists but is much thinner.
No SL: $647 over 208d on $200 positions = ~$3.1/day. Still positive, Sharpe 7.7.

## Per-Pair Analysis (Leg-Level, 2.5x+2h grace)

### Winners (keep)
| Pair | Trades | PnL | WR | Funding | Days |
|------|--------|-----|----|----|------|
| NOM | 218 | $108 | 92% | $14 | 199 |
| THETA | 222 | $96 | 94% | $0 | 208 |
| BAND | 225 | $84 | 93% | $0 | 208 |
| XVS | 231 | $73 | 90% | $0 | 208 |
| METIS | 145 | $57 | 94% | $0 | 208 |
| ONT | 109 | $48 | 95% | $1 | 208 |
| KSM | 41 | $26 | 95% | $0 | 208 |
| ALICE | 41 | $21 | 88% | $0 | 105 |
| AXL | 61 | $15 | 90% | $0 | 208 |
| ONG | 139 | $7 | 90% | $14 | 208 |

### Losers (cut)
| Pair | Trades | PnL | WR | Funding | Issue |
|------|--------|-----|----|----|-------|
| FIO | 198 | -$48 | 72% | $0 | High frequency but loses on legs |
| DEXE | 28 | -$44 | 32% | $0 | Extreme spread spikes, legs diverge |
| ENJ | 114 | -$25 | 80% | $30 | Funding costs eat the spread |
| COMP | 24 | -$10 | 71% | $6 | Low volume, funding |

## Code Changes Made (not committed)
1. `arb_executor.py`: Exit spread recording uses bid/ask (was mid prices)
2. `arb_executor.py`: Fees updated to 24bp RT (was 31bp)
3. `arb_engine.py`: Entry threshold 60bps (was 35bps)
4. `arb_executor.py`: close_position accepts bn_ticker/bb_ticker for consistent spread
5. `arb_engine.py`: Passes tickers through to close_position

## Open Questions for Next Session
1. **Per-pair thresholds**: 60bps hardcoded for all pairs doesn't make sense. NOM avg spread is 23bps but BAND is 16bps. Entry threshold should be calibrated per pair based on spread distribution and fee breakeven.
2. **Inventory management**: After selling BAND on Binance, we run out. No rebalancing mechanism. Need manual transfers or direction-aware position limits.
3. **Cross-exchange capital flow**: Bybit P&L accumulates on Bybit. Binance inventory depletes. Over time, imbalance grows.
4. **Position netting**: If we're long BAND on Bybit perp and flat on Binance (sold all BAND), we have a naked directional long. The hedge is gone.
5. **Major pairs**: Only tested shitcoins with >60bps spreads. Need to analyze BTC/ETH/SOL at lower thresholds — deeper liquidity, less slippage, but tighter spreads.
6. **Trade accounting**: Need flawless tracking of: entry/exit prices per leg, fills, fees, funding, net position per exchange, inventory levels.
7. **Rebalancing cost**: What does it cost to transfer between Binance and Bybit? How often? Does this eat into the edge?

## Fee Tiers (verified Apr 18)
- Binance spot VIP0: maker=10bp, taker=10bp (same at this tier)
- Bybit perp: maker=2bp, taker=5.5bp
- No BNB discount available (0 BNB balance, API 404 on enable)
- Achievable RT: 24bp (BN 10+10 + BB 2+2)
- With BNB discount: 19bp possible
