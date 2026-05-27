# HL-Bybit Perp/Perp Spread Arb — Implementation Plan

## Overview

Delta-neutral spread arb: SHORT on expensive venue, LONG on cheap venue. When spread reverts, close both positions. Adapts the battle-tested H2 spike-fade engine (Binance spot + Bybit perp) to Hyperliquid perp + Bybit perp.

**Backtest (7.8 days, Apr 25 - May 3) — CORRECTED:**
- APE-USDT: 194 trades, 77% WR, +6.4bp/trade net, median hold 10s
- CHIP-USDT: 179 trades, 60% WR, +3.2bp/trade net
- AXS-USDT: 107 trades, 71% WR, +4.9bp/trade net
- Fee RT: HL taker 4.32bp + Bybit taker 5.5bp = 9.82bp/side × 2 = 19.64bp total
- **NOTE: Original backtest had a fee bug (9.82bp instead of 19.64bp). Fixed.**
- **VALIDATED approach: P90/P25 with 25bp floor (entry = max(P90, 25bp), exit = P25)**
- **Result: 355 trades, 27 pairs, 90% WR, +3,894bp total in 7.8 days**
- **Revenue at $1K/trade: ~$1,498/mo. At $5K: ~$7,490/mo.**

**Key differences from H2:**
1. Both legs are perps — no spot inventory, no inventory risk
2. Delta neutral by construction — net exposure = 0
3. Lower fees (19.64bp vs 31bp)
4. Faster mean reversion (median hold 10s vs minutes/hours in H2)

---

## 1. Module Disposition

### REUSE AS-IS (import from `app.services.arb`)

| Module | Reason |
|--------|--------|
| `signal_engine.py` | P90/P25 thresholds are venue-agnostic. Change: default `fee_rt_bps` 31→19.64, seed from `arb_hl_bybit_perp_snapshots` |
| `position_store.py` | MongoDB state machine is venue-agnostic. Rename `bn_symbol` → `hl_symbol` in doc schema |
| `fill_detector.py` | `TrackedLeg` and fill detection are venue-agnostic |
| `risk_manager.py` | Three-tier risk (ALERT/PAUSE/KILL) works unchanged. Remove inventory counters. Add HL hourly funding to blackout |
| `instrument_rules.py` | `PairRules` and rounding are venue-agnostic. Add HL instrument info fetching |

### REWRITE (significant adaptation)

| Module | What Changes |
|--------|-------------|
| `price_feed.py` | Replace BinancePriceFeed with HLPriceFeed. HL WS L2 book subscription. SpreadSnapshot fields: `bn_*` → `hl_*` |
| `order_gateway.py` | Replace BinanceOrderAPI with HLOrderAPI. HL uses ETH key signing via `hyperliquid` SDK. Reuse `WSOrderClient` from `hl_mm` (~100ms RTT) |
| `order_api.py` | New `HLOrderAPI` class. HL order submission via `Exchange` SDK or WS. Position queries via `Info.user_state()` |
| `entry_flow.py` | Venue labels `bn`→`hl`. Both legs are perps, symmetric qty calc. IOC on both sides |
| `exit_flow.py` | Venue labels change. Both exits are reduce-only perp closes |
| `tier_engine.py` | Data source: `arb_hl_bybit_perp_snapshots`. Thresholds recalibrated for 19.64bp fees |
| `crash_recovery.py` | Remove InventoryLedger. Dual-venue position query. Simpler: just check both venues have matching positions |
| `universe_manager.py` | HL WS supports dynamic subscribe/unsubscribe per coin (simpler than Binance) |

### REMOVE (not needed for perp/perp)

| Module | Reason |
|--------|--------|
| `inventory_ledger.py` | No spot inventory in perp/perp |
| `inventory_guard.py` | No spot impairment risk |
| `inventory_risk_guard.py` | Delta-neutral by construction |

---

## 2. Architecture

```
scripts/arb_hl_bybit_perp_v2.py          (main script)
    |
    +-- app/services/arb_hlbb/             (NEW package, parallel to arb/)
         |
         +-- price_feed.py                 (HL WS L2 + Bybit WS, SpreadSnapshot)
         +-- signal_engine.py              (import from arb/, new defaults)
         +-- order_gateway.py              (HLOrderAPI + BybitOrderAPI)
         +-- order_api.py                  (HL via hyperliquid SDK + WSOrderClient)
         +-- order_feed.py                 (HL private WS + Bybit private WS)
         +-- entry_flow.py                 (dual-perp leg-by-leg entry)
         +-- exit_flow.py                  (dual-perp exit)
         +-- fill_detector.py              (import from arb/)
         +-- position_store.py             (import from arb/, new collection)
         +-- risk_manager.py               (import from arb/, adjusted thresholds)
         +-- tier_engine.py                (adapted for HL-Bybit snapshots)
         +-- universe_manager.py           (HL WS subscribe/unsubscribe)
         +-- instrument_rules.py           (HL szDecimals + Bybit rules)
         +-- crash_recovery.py             (simplified, no inventory)
```

**Decision: NEW package `app/services/arb_hlbb/`** — H2 continues running in parallel. No regression risk to battle-tested H2 code. Shared modules imported, not copied.

---

## 3. HL API Integration

**Authentication:**
- ETH private key: `eth_account.Account.from_key(HL_PRIVATE_KEY)`
- `Exchange(wallet, MAINNET_API_URL, account_address=HL_QUERY_ADDRESS)` for orders
- `Info(MAINNET_API_URL)` for queries
- Agent wallet (`0xdf67...`) signs, parent wallet (`0x11ca...`) holds funds

**Order Placement (WS preferred for latency):**
- WS via `WSOrderClient` from `hl_mm` — ~100ms RTT (proven)
- REST via `exchange.order()` — ~500ms RTT (fallback)
- IOC: `tif="Ioc"` with aggressive price

**Position Query:**
```python
info.user_state(HL_QUERY_ADDRESS)["assetPositions"]  # all open positions
```

**Balance:**
```python
info.user_state(HL_QUERY_ADDRESS)["marginSummary"]["accountValue"]  # perps equity
info.spot_user_state(HL_QUERY_ADDRESS)  # USDC balance
# Total = perps equity + spot USDC (HL unified wallet)
```

**WS Price Feed:**
```python
{"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}}
# Response: {"channel": "l2Book", "data": {"coin": "BTC", "levels": [[bids], [asks]], "time": ms}}
```

**Instrument Rules:**
```python
meta = info.meta()  # returns universe with szDecimals per coin
# szDecimals=2 means 0.01 step. Min notional ~$10.
```

---

## 4. Execution Flow

### Entry

```
Signal fires (spread > P90 for N consecutive ticks)
  |
  +-- Pre-checks: risk manager, concurrent count, margin on both venues
  |
  +-- Direction:
  |     HL_PREMIUM: SHORT HL (sell at hl_bid), LONG BB (buy at bb_ask)
  |     BB_PREMIUM: SHORT BB (sell at bb_bid), LONG HL (buy at hl_ask)
  |
  +-- Qty: $POSITION_USD / mid_price (same on both sides)
  |
  +-- Persist PENDING → ENTERING to MongoDB
  |
  +-- Submit BOTH legs concurrently (asyncio.gather):
  |     Leg 1: HL IOC via WS (~100ms)
  |     Leg 2: Bybit IOC via REST (~200ms)
  |
  +-- Fill detection:
  |     Both filled → OPEN (register for exit monitoring)
  |     One filled, other not → UNWIND (market-close filled leg)
  |     Neither filled → FAILED
```

### Exit

```
Exit signal (spread < P25, stop-loss, max-hold)
  |
  +-- Persist EXITING
  |
  +-- Submit BOTH exits concurrently (reduce_only on both)
  |
  +-- Both filled → CLOSED (compute PnL)
  |    One filled → retry unfilled, market escalate if needed
```

**Naked-leg window:** With WS for HL (~100ms) and REST for Bybit (~200ms), total entry latency < 300ms. Both legs submit concurrently via `asyncio.gather`.

---

## 5. Risk Management

### Unchanged from H2
- Daily PnL loss limit (proportional to sizing)
- Per-trade loss limit
- Consecutive leg failure circuit breaker
- Slippage monitoring
- Funding blackout (Bybit: 0/8/16 UTC)

### New/Changed

| Risk | Mitigation |
|------|------------|
| **No inventory risk** | Both legs are perps, delta-neutral. Closing = reversing. |
| **Funding exposure** | Median hold 10s → negligible. Max hold 1h caps worst case. Monitor net funding. |
| **Margin on both venues** | Pre-flight check: `available_margin >= position_USD / leverage + buffer` on BOTH venues |
| **Liquidation** | Set leverage conservatively (3x both venues). Delta-neutral means risk is spread widening, not directional. |
| **HL liquidity** | Cap position at 0.1% of HL 24h volume per pair |
| **HL exchange risk** | Keep positions small ($100-500). Monitor HL insurance fund. |

### Adjusted Thresholds

| Parameter | H2 | HLBB |
|-----------|-----|------|
| `FEE_RT_BPS` | 31.0 | 19.64 |
| `MIN_EXCESS_BPS` | 31.0 | 22.0 |
| Tier A threshold | excess > 35bp | excess > 25bp |
| Tier B threshold | excess > 31bp | excess > 22bp |

---

## 6. Crash Recovery

Simpler than H2 (no inventory to reconcile):

1. Query all non-terminal positions from MongoDB
2. For each:
   - **PENDING**: Mark FAILED
   - **ENTERING**: Query both venues for positions. Both exist → OPEN. One exists → close it, mark FAILED. None → FAILED.
   - **OPEN**: Verify both venues have expected position. Resume exit monitoring.
   - **EXITING**: Complete the exit
   - **UNWINDING**: Complete and mark FAILED
3. **Orphan detection**: Query ALL positions on HL + Bybit. Any not in MongoDB → close with market order + Telegram alert.

---

## 7. MongoDB Schema

### `arb_hlbb_positions`

```json
{
    "position_id": "hlbb_<uuid>",
    "symbol": "APEUSDT",
    "hl_coin": "APE",
    "pair": "APE-USDT",
    "state": "OPEN",
    "direction": "SHORT_HL_LONG_BB",
    "signal_spread_bps": 45.2,
    "threshold_p90": 42.0,
    "threshold_p25": 12.0,
    "entry": {
        "hl": { "order_id": "...", "side": "Sell", "filled_qty": 100.0, "avg_fill_price": 1.233, "fee": 0.0053, "state": "FILLED" },
        "bb": { "order_id": "...", "side": "Buy", "filled_qty": 100.0, "avg_fill_price": 1.229, "fee": 0.0068, "state": "FILLED" },
        "actual_spread_bps": 44.8,
        "slippage_bps": 0.4,
        "latency_ms": 180.0
    },
    "exit": {
        "reason": "EXIT_REVERT",
        "hl": { ... },
        "bb": { ... }
    },
    "pnl": { "gross_bps": 36.6, "fees_bps": 19.64, "net_bps": 16.96, "net_usd": 0.017 },
    "hold_seconds": 10.2,
    "created_at": "...",
    "entry_time": "...",
    "exit_time": "..."
}
```

### `arb_hlbb_trades` (simplified log)

```json
{
    "pair": "APE-USDT",
    "direction": "SHORT_HL_LONG_BB",
    "entry_spread_bps": 45.2,
    "exit_spread_bps": 8.2,
    "pnl_net_bps": 16.96,
    "hold_s": 10.2,
    "exit_type": "EXIT_REVERT",
    "timestamp": "..."
}
```

---

## 8. Testing Plan

| Phase | Duration | What |
|-------|----------|------|
| **Unit tests** | 2-3 days | Signal engine, price feed (mocked), position store, instrument rules, risk manager |
| **Integration tests** | 1-2 days | Real HL/Bybit APIs (no orders). WS connections, L2 data, position queries |
| **Dry run** | 7+ days | Signal detection only. Validate signal frequency vs backtest. Compare with V1 prototype |
| **Shadow mode** | 2-3 days | Simulated fills with realistic slippage. Full position lifecycle |
| **Small-size live** | 5+ days | $10-25/side. Manual verification of first 10 trades |
| **Scale** | Ongoing | Increase sizing after 20+ trades, leg failure < 10%, avg slippage < 5bp |

---

## 9. Known Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| HL liquidity on small caps | Large slippage | Cap at 0.1% of 24h volume |
| One-leg fill, other miss | Naked directional exposure | IOC + 3s timeout + immediate market unwind |
| Spread regime change | Edge disappears | TierEngine auto-demotes. MIN_EXCESS gate |
| HL exchange risk (clawback) | Platform-level loss | Small positions, monitor insurance fund |
| Concurrent H2 + HLBB on same Bybit | Position conflicts | Separate Bybit sub-account OR disjoint symbol sets |
| HL API rate limits | Missed entries | WS for prices (no polling). Conservative order frequency |

---

## 10. Implementation Sequence

### Week 1: Foundation
1. Create `app/services/arb_hlbb/` package
2. Adapt `price_feed.py` — HL WS L2 book + Bybit WS
3. Create `order_api.py` with HLOrderAPI (reuse WSOrderClient from hl_mm)
4. Adapt `order_gateway.py`, `instrument_rules.py`
5. Unit tests for price feed and order API

### Week 2: Execution Flows
6. Adapt `entry_flow.py` (symmetric perp/perp legs)
7. Adapt `exit_flow.py` (reduce_only on both)
8. Adapt `position_store.py` (new collection)
9. Adapt `crash_recovery.py` (no inventory)
10. Write main script `arb_hl_bybit_perp_v2.py`
11. Dry-run testing begins

### Week 3: Tier Engine & Risk
12. Adapt `tier_engine.py` (HL-Bybit snapshot collection, recalibrated thresholds)
13. Adapt `universe_manager.py` (HL WS dynamic subscriptions)
14. Adjust `risk_manager.py` thresholds
15. Shadow mode (48h minimum)

### Week 4: Go Live
16. Small-size live ($10/side)
17. Monitor and tune 3-5 days
18. Scale if criteria met
19. LaunchDaemon for process supervision
