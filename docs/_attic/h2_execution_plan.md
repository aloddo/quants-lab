# H2 Spike Fade — Live Execution Plan

## Strategy Summary

Trade cross-venue spread spikes between Bybit USDT perpetual and Binance spot.
When the spread exceeds P90 (adaptive, 1h rolling), enter both legs simultaneously.
Exit when spread reverts below P25. Stop loss at 2x entry spread.

## Instruments

| Leg | Exchange | Instrument | Order Type | Side (BUY_BB_SELL_BN) | Side (BUY_BN_SELL_BB) |
|-----|----------|-----------|------------|----------------------|----------------------|
| A | Bybit | USDT Perpetual | Limit (post-only) | LONG | SHORT |
| B | Binance | Spot | Limit (post-only) | SELL (need inventory) | BUY (need USDT) |

### Why this mix
- Bybit perps: Alberto's primary exchange, demo tested, API familiar
- Binance spot: futures blocked in NL. Spot is accessible. No leverage needed.
- Future upgrade: when Binance futures unlocked (Spain trip), both legs become perps = no inventory problem

## Capital Requirements

### Per $200 slot
| Component | Amount | Notes |
|-----------|--------|-------|
| Bybit margin | $200 | USDT, cross margin, 1x leverage |
| Binance inventory | $200 equivalent in token | Must pre-buy and hold |
| Binance USDT reserve | $50 | For BUY_BN_SELL_BB direction |
| **Total per slot** | **$450** | vs $200 in paper |

### Inventory problem (critical)
The paper trader is direction-agnostic — it trades whichever direction has the bigger spread.
In practice, all 97 trades so far were BUY_BB_SELL_BN (buy cheap on Bybit perp, sell expensive on Binance spot).

**To SELL on Binance spot, you must already hold the token.**

Options:
1. **Pre-buy inventory**: Buy $200 of each active token on Binance before starting. Ties up $200 x N pairs.
2. **USDT-only mode**: Only trade BUY_BN_SELL_BB direction (buy spot on Binance, short perp on Bybit). But paper data shows this direction rarely triggers — almost all signals are BUY_BB_SELL_BN.
3. **Delta-neutral pre-position**: Buy spot on Binance + short perp on Bybit as the base state. Then the "trade" is closing both legs (capturing the spread). This is the cleanest but requires 2x capital upfront.

**Recommendation: Option 1 for the $20 test.** Pre-buy $20 of HIGH and ALICE on Binance. Run BUY_BB_SELL_BN only. Accept that you're holding spot exposure on Binance between trades (this IS directional risk, though small at $20).

## Execution Flow

### Signal Detection (unchanged from paper)
```
Every 5s:
  1. Poll Bybit perp ticker (bid1/ask1)
  2. Poll Binance spot ticker (bid/ask)
  3. Compute spread: (bb_bid - bn_ask) / bn_ask * 10000  (for BUY_BB_SELL_BN)
  4. Compare to adaptive P90 threshold
  5. If spread > P90 and pair viable → ENTRY SIGNAL
```

### Entry Execution (NEW — the hard part)

```
On ENTRY SIGNAL for BUY_BB_SELL_BN:
  1. SIMULTANEOUSLY (asyncio.gather):
     a. Bybit: Place LIMIT BUY order on perp at ask1 price (aggressive limit = fills like market but avoids market order fee)
     b. Binance: Place LIMIT SELL order on spot at bid price (need inventory)
  
  2. Wait max 3 seconds for both fills
  
  3. Outcome matrix:
     | Bybit | Binance | Action |
     |-------|---------|--------|
     | FILL  | FILL    | ✅ Position open, track both legs |
     | FILL  | MISS    | ⚠️ Cancel Binance. Close Bybit immediately (market). Log leg failure. |
     | MISS  | FILL    | ⚠️ Cancel Bybit. Buy back on Binance immediately (market). Log leg failure. |
     | MISS  | MISS    | ✅ No position, no risk. Retry next poll. |
  
  4. Record:
     - Both fill prices (for actual spread captured)
     - Slippage vs ticker at signal time
     - Leg fill latency (ms)
     - Any partial fills (handle as full fill if >80%, else cancel remainder)
```

### Exit Execution

```
On EXIT SIGNAL (spread < P25 or SL or max hold):
  1. SIMULTANEOUSLY:
     a. Bybit: Close perp position (LIMIT SELL at bid1 for LONG)
     b. Binance: Buy back spot (LIMIT BUY at ask) — restores inventory
  
  2. Same fill monitoring as entry (3s timeout, unwind failures)
  
  3. Record actual exit prices, compare to paper exit
```

### Stop Loss Execution

```
On STOP LOSS (spread > 2x entry):
  Same as exit but URGENT:
  - Use market orders (accept slippage, prevent further loss)
  - Log slippage vs limit-would-have-been price
  - Post-SL cooldown: 0 seconds (data shows re-entries are profitable)
```

## Risk Controls

### Leg Risk (most critical)
- **Max leg exposure time**: 5 seconds. If one leg fills and other doesn't within 5s, emergency close the filled leg at market.
- **Circuit breaker**: 3 consecutive leg failures → pause trading for 15 minutes. Likely API issue or exchange maintenance.
- **Never leave a naked leg open.** This is the #1 rule. A one-legged "arb" is just a directional bet.

### Position Limits
- **Max concurrent**: 3 (reduced from 7 in paper — execution overhead scales)
- **Max per pair**: 1 open position
- **Max loss per trade**: stop loss at 2x entry spread (unchanged)
- **Max daily loss**: $20 for the test phase → hard shutdown

### API Rate Limits
| Exchange | Limit | Our Usage | Headroom |
|----------|-------|-----------|----------|
| Bybit | 120 req/5s (GET), 50 req/5s (POST) | 1 GET/5s + orders | 99%+ |
| Binance | 6000 req/min (GET), 100 orders/10s | 1 GET/5s + orders | 99%+ |

### Inventory Management
- Track Binance spot balance per pair after each trade
- If inventory < min_order_size → mark pair as UNAVAILABLE until manually refilled
- Alert via Telegram when inventory runs low

## Fee Model (realistic)

### $20 test (lowest tier)
| Exchange | Role | Fee | Per leg |
|----------|------|-----|---------|
| Bybit perp | Taker | 0.055% | 5.5bp |
| Bybit perp | Maker | 0.02% | 2.0bp |
| Binance spot | Taker | 0.10% | 10.0bp |
| Binance spot | Maker | 0.10% | 10.0bp |

**Worst case (taker-taker): 15.5bp RT**
**Best case (maker-maker): 12.0bp RT**
**Paper assumption: 24bp RT** — paper is MORE conservative than reality on fees!

Wait — that's surprising. Let me verify.

Actually paper uses 24bp because it was modeled as 12bp/leg taker on both venues. But Binance spot base tier is 0.10% = 10bp, and Bybit perp taker is 5.5bp. So 15.5bp RT worst case. But with BNB discount on Binance (25% off) = 7.5bp → 13bp RT.

**The fee model in paper is conservative. This is good news — real fees are lower.**

## Monitoring & Reporting

### Per-trade metrics (MongoDB: `arb_h2_live_trades`)
```
{
  position_id, symbol, direction,
  signal_time, signal_spread,       # when we detected the signal
  bybit_order_id, bybit_fill_price, bybit_fill_time, bybit_slippage_bps,
  binance_order_id, binance_fill_price, binance_fill_time, binance_slippage_bps,
  actual_entry_spread,              # real spread from fills (vs paper's ticker spread)
  leg_failure: bool,                # did one leg fail?
  exit_bybit_price, exit_binance_price,
  actual_exit_spread,
  gross_pnl_usd, fees_usd, net_pnl_usd,
  close_reason,
}
```

### Telegram alerts
- OPEN: both legs confirmed, actual spread vs signal spread
- CLOSE: both legs confirmed, PnL, slippage report
- LEG FAILURE: immediate alert with details
- DAILY SUMMARY: actual vs paper comparison

### Paper vs Live comparison
Run paper trader in parallel (it's read-only, just watches spreads). Compare:
- Entry timing: did live enter at same time as paper?
- Spread captured: actual fills vs ticker prices
- Slippage: per leg, per pair
- Win rate: paper vs live
- Missed trades: signals paper took but live couldn't (inventory, leg failure, etc.)

## Implementation Phases

### Phase 1: $20 micro-test (2-3 days)
- 2 pairs only: HIGH + ALICE (highest volume + cleanest win rate)
- $20 per side ($40 total per pair, $80 total)
- Pre-buy $20 HIGH + $20 ALICE on Binance spot
- BUY_BB_SELL_BN direction only
- Paper trader runs in parallel for comparison
- Goal: validate execution mechanics, measure real slippage

### Phase 2: Scale to $100 (1 week)
- If Phase 1 slippage < 15bp and edge positive after fees
- Add NOM, AXL, PORTAL (all 5 active pairs)
- $100 per side
- Both directions enabled
- Goal: validate across pairs, measure inventory burn rate

### Phase 3: Scale to $500-1000 (2+ weeks)
- If Phase 2 edge within 30% of paper
- Full position sizing
- Automated inventory rebalancing
- Goal: steady-state operation

## Open Questions

1. **Binance account status**: Can Alberto trade spot in NL? (Spot should be fine, only derivatives are restricted)
2. **Binance API keys**: Does Alberto have Binance API keys, or need to create them?
3. **Token transfer**: If inventory depletes, can we transfer USDT Binance → buy spot? Or need manual top-up?
4. **Tax implications**: Cross-venue spot trading may have different tax treatment than derivatives in NL
5. **Bybit demo → mainnet**: Do we need new API keys for Bybit mainnet? Different base URL.

## Architecture

```
┌─────────────────────────────────────────────┐
│  H2 Live Trader (single Python process)     │
│                                             │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  │
│  │ Spread  │→ │ Signal   │→ │ Execution │  │
│  │ Monitor │  │ Engine   │  │ Engine    │  │
│  │ (5s)    │  │ (P90/P25)│  │ (2-leg)   │  │
│  └────┬────┘  └──────────┘  └─────┬─────┘  │
│       │                           │         │
│  ┌────▼────────────────────┐ ┌────▼──────┐  │
│  │ Bybit REST + Binance    │ │ Inventory │  │
│  │ REST (ticker polling)   │ │ Tracker   │  │
│  └─────────────────────────┘ └───────────┘  │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │ MongoDB  │  │ Telegram │  │ Paper     │  │
│  │ (trades) │  │ (alerts) │  │ Shadow    │  │
│  └──────────┘  └──────────┘  └───────────┘  │
└─────────────────────────────────────────────┘
        │                    │
   ┌────▼────┐          ┌───▼────┐
   │ Bybit   │          │Binance │
   │ Mainnet │          │  Spot  │
   │ (perp)  │          │        │
   └─────────┘          └────────┘
```

## Key Differences from Paper

| Aspect | Paper | Live |
|--------|-------|------|
| Entry | Instant at ticker mid | 2 limit orders, possible partial/miss |
| Exit | Instant at ticker mid | 2 limit orders, possible partial/miss |
| Fees | 24bp RT (flat) | ~13-15bp RT (actual, lower!) |
| Slippage | 0 | Unknown, measuring in Phase 1 |
| Inventory | Unlimited | Must pre-buy, can deplete |
| Leg risk | None | One leg can fail → naked exposure |
| Latency | 0 | ~100-500ms per leg |
| Capital | $200/slot | $400-450/slot (both exchanges) |
| Directions | Both | BUY_BB_SELL_BN initially (inventory constraint) |
