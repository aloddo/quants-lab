# H2 Spike Fade — Execution Analysis & Codex Review Brief

**Date:** 2026-04-20
**Period:** Apr 19 14:00 UTC — Apr 20 06:30 UTC (~16.5h)
**Status:** Bot STOPPED after discovering critical bugs

## 1. Architecture

Cross-venue spot-perp arb: Binance USDC spot + Bybit USDT perpetual.
- Entry: sell spot on Binance, buy perp on Bybit (when spread > P90)
- Exit: buy back spot on Binance, sell perp on Bybit (when spread < P25)
- Position size: $10/side
- Pairs: NOM, ENJ, MOVE, KERNEL (all USDC on Binance)

Leverage asymmetry: Bybit perp is leveraged (10-20x cross margin), Binance spot is 1x physical tokens. Inventory price risk is unhedged.

## 2. Data Sources for Analysis

### Exchange APIs
```bash
# Bybit closed PnL (all round-trips)
curl -H "X-BAPI-API-KEY: $KEY" "https://api.bybit.com/v5/position/closed-pnl?category=linear&limit=200"

# Binance trade history (per USDC pair)
curl -H "X-MBX-APIKEY: $KEY" "https://api.binance.com/api/v3/myTrades?symbol=MOVEUSDC&limit=1000"

# Binance order book depth
curl "https://api.binance.com/api/v3/depth?symbol=MOVEUSDC&limit=5"

# Binance exchange info (LOT_SIZE filters)
curl "https://api.binance.com/api/v3/exchangeInfo?symbol=MOVEUSDC"
```

### MongoDB Collections
```javascript
// Dual collector spread snapshots (28h+, 2.4M Bybit + 2.1M Binance)
db.arb_bn_usdc_bb_perp_snapshots.find({symbol_bn: "MOVEUSDC"}).count()
db.arb_bb_spot_perp_snapshots.find({}).count()

// Live position records (many dropped during debugging -- use exchange APIs as source of truth)
db.arb_h2_live_positions.find({})

// Paper trader (USDT pairs, 109 trades)
db.arb_h2_positions.find({status: "CLOSED"})

// Inventory ledger
db.arb_h2_inventory.find({})
```

### Log Files
```bash
# Full execution log (all signals, fills, errors)
/tmp/arb-h2-live.log

# Key patterns to grep:
grep "ENTRY SUCCESS" /tmp/arb-h2-live.log          # successful entries
grep "LEG FAILURE" /tmp/arb-h2-live.log             # failed legs
grep "EXIT success\|EXIT partial" /tmp/arb-h2-live.log  # exit outcomes
grep "POSITION VERIFICATION" /tmp/arb-h2-live.log   # position verify triggers
grep "LOT_SIZE" /tmp/arb-h2-live.log                # Binance filter errors
grep "EMERGENCY UNWIND" /tmp/arb-h2-live.log        # unwind events
```

## 3. Execution Quality

### Entry Performance (19 successful entries)
- Latency: mean 2,623ms, median 3,471ms (dominated by 3s GTC fill timeout)
- Slippage: 0/19 had positive slippage (all fills at or better than signal)
- Edge at entry: mean +52bp vs P25 target, +21bp after 31bp fees
- 18/19 entries had positive edge after fees

### Leg Failure Rate
- 191 leg failures / 210 attempts = **91% failure rate**
- Root cause: GTC orders on Binance USDC books time out at 3s

### Exit Performance
- 12 successful exits / 193 total = **94% partial exit rate**
- Root cause: same GTC timeout + position verification bug (see section 5)

## 4. Spread Analysis (from 28h dual collector data)

| Pair | Snapshots | P90 | P25 | Capture | Net (after 31bp fees) | Entry freq |
|------|-----------|-----|-----|---------|----------------------|------------|
| ENJ | 10,722 | 131bp | 42bp | 89bp | **+58bp** | 10.0% |
| KERNEL | 10,545 | 138bp | 66bp | 72bp | **+41bp** | 10.0% |
| NOM | 9,243 | 89bp | 21bp | 68bp | **+37bp** | 10.1% |
| MOVE | 5,702 | 73bp | 15bp | 58bp | **+27bp** | 10.3% |

All 4 pairs show positive expected edge. The strategy alpha IS present in the data.

### Book Depth (USDC vs USDT, top 5 levels)
| Pair | USDC Bid $ | USDC Ask $ | USDT Bid $ | USDT Ask $ | Ratio |
|------|-----------|-----------|-----------|-----------|-------|
| ENJ | $427 | $760 | $557 | $1,277 | ~50% |
| KERNEL | $3,058 | $1,311 | $5,031 | $4,189 | ~50% |
| NOM | $10,628 | $5,337 | $55,663 | $50,384 | ~15% |
| MOVE | $14,622 | $2,964 | $114,612 | $32,103 | ~15% |

Our $10 position size is 1-2% of book depth. Slippage from book thinness should be negligible.

## 5. Bugs Found During Live Trading

### Critical: Position Verification for Binance checks wallet balance, not order fill
**Impact:** Caused the 95,168 MOVE short position ($1,813 notional)
**Root cause:** `check_position("binance", "MOVEUSDT")` returns `get_balance("MOVE")` = total wallet balance (1,764). When a GTC sell order was cancelled unfilled, the verification saw 1,764 in the wallet and concluded "the order filled for 1,764." This cascaded:
1. Entry recorded bn_qty=1,764 (wrong, should be 0 or 525)
2. Exit tried to buy back 1,764.658285 (non-integer for stepSize=0.1)
3. Binance rejected with LOT_SIZE
4. Partial exit loop: each retry sold 525 more on Bybit, Binance always rejected
5. 180 iterations built 95,168 short position

### Critical: Partial exit retry re-sells already-closed leg
**Impact:** Each retry of a partial exit submitted the SAME sell on the already-closed Bybit side
**Fix applied:** Track `_bb_exit_closed` / `_bn_exit_closed` flags, skip closed legs on retry

### Critical: Signal engine allowed negative spread entries
**Impact:** Entry at -51bp spread, immediately stop-lossed
**Fix applied:** Require `spread_bps > 0` before entry

### High: 91% leg failure rate on entries
**Possible causes:**
- GTC 3s timeout too short for USDC books?
- Or GTC orders ARE filling but detection fails (Binance WS unavailable, REST check misses)?
- Need to correlate: how many GTC orders actually filled on Binance vs how many the coordinator detected

## 6. Reconciled PnL (from exchange APIs)

### Matched Round-Trips (26 trades, excluding MOVE bug)
- Combined Binance net: -$0.43
- Combined Bybit net (normal): -$0.85
- **Total net PnL: -$1.29**

### Trades that executed cleanly (hold > 10min, proper reversion):
- ENJ 44min hold: +$0.36 net
- ENJ 47min hold: +$0.45 net
- KERNEL 96min hold: +$0.23 net
- MOVE 64min hold: +$0.29 net

The losses are dominated by zero-capture unwind trades (bug artifacts that just pay fees).

### MOVE Bug Impact
- Bybit: +$12.44 (accidental short was profitable due to price drop)
- Net including bug: +$11.15

## 7. Inventory Impairment

| Asset | Qty | Avg Cost | Current Price | Cost $ | Market $ | uPnL | % |
|-------|-----|----------|---------------|--------|----------|------|---|
| ENJ | 387.98 | 0.0614 | 0.0605 | $23.84 | $23.48 | -$0.36 | -1.5% |
| MOVE | 1,764.66 | 0.0195 | 0.0190 | $34.49 | $33.53 | -$0.96 | -2.8% |
| KERNEL | 149.67 | 0.0715 | 0.0707 | $10.70 | $10.58 | -$0.11 | -1.1% |
| NOM | 0.13 | 0.0028 | 0.0028 | $0.00 | $0.00 | $0.00 | +1.4% |
| **TOTAL** | | | | **$69.02** | **$67.59** | **-$1.43** | **-2.1%** |

Holding period: ~10-14 hours. The -2.1% impairment exceeds the strategy's gross capture.

## 8. Paper Trader Comparison (USDT pairs, $200/side)

| Pair | Trades | WR | PnL $ | Avg Capture |
|------|--------|----|-------|-------------|
| HIGH | 66 | 79% | +$42.05 | 56bp |
| NOM | 27 | 89% | +$17.18 | 56bp |
| ALICE | 10 | 90% | +$4.44 | 46bp |
| AXL | 3 | 100% | +$5.20 | 111bp |
| PORTAL | 3 | 67% | +$1.13 | 43bp |
| **TOTAL** | **109** | **83%** | **+$69.99** | **56bp** |

## 9. Questions for Codex Review

1. **Is the 91% leg failure rate a fill detection problem or a fill rate problem?** The GTC orders may be filling on Binance but the coordinator can't detect it (WS unavailable, REST check timing). How to verify: compare Binance `myTrades` count vs coordinator-detected fills.

2. **Is position verification fundamentally flawed for spot markets?** The fix (disable for Binance) means we lose a safety net. Alternative: snapshot balance BEFORE order, check delta AFTER. But balance changes from other trades would confuse this.

3. **Should the partial exit retry have a max-retry limit?** Currently retries indefinitely. Should it give up after N attempts and alert for manual intervention?

4. **Is the 3s GTC timeout optimal?** Too short = orders cancelled before filling. Too long = capital locked up, can't react to new signals. What does the Binance trade frequency data suggest?

5. **Is the inventory impairment (-2.1% in 14h) acceptable?** The strategy captures 20-50bp per trade but inventory drops 210bp from price movement. Is this structurally unprofitable?

6. **Are there any remaining race conditions or edge cases in the execution engine?** Review all files in `app/services/arb/` with the bugs-found context above.

## 10. Files to Review

```
app/services/arb/execution_engine.py   # LegCoordinator, entry/exit flows
app/services/arb/signal_engine.py      # Entry/exit signal logic
app/services/arb/order_feed.py         # WS fill handling
app/services/arb/price_feed.py         # Price feeds, freshness
app/services/arb/order_api.py          # REST order submission
app/services/arb/risk_manager.py       # Circuit breaker, risk limits
app/services/arb/inventory_ledger.py   # Inventory tracking, cost basis
app/services/arb/crash_recovery.py     # Startup reconciliation
app/services/arb/instrument_rules.py   # Exchange filters, rounding
app/services/arb/liquidity_veto.py     # Book depth checks
scripts/arb_h2_live.py                 # Main trader, all wiring
tests/arb/test_execution_engine.py     # Test coverage
```
