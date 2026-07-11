# H2 Codex Adversarial Review — 2026-04-20

**Model:** OpenAI Codex (codex exec, reasoning=high)
**Tokens:** 402,849
**Gate:** FAIL (14 findings)
**Context:** Read docs/h2_execution_analysis_2026_04_20.md + all 11 source files

## Findings

### 1. Exit can falsely mark a leg "closed" on partial market fill
**File:** execution_engine.py:796, :814
Any `filled_qty > 0` from REST check marks the market order `FILLED`, then marks the original leg `FILLED` too. Can set `_bb_exit_closed` / `_bn_exit_closed` on a leg that is still partially open, unregister the position, and leave residual exposure.

### 2. Partial-exit retry state is not crash-safe
**File:** arb_h2_live.py:1111, :1081; crash_recovery.py:127
`_bb_exit_closed`/`_bn_exit_closed` flags exist only in memory. MongoDB stores only `PARTIAL_EXIT` without which leg is closed. Recovery only resumes `status: "OPEN"`. Restart after partial exit loses the retry mask entirely.

### 3. 91% leg failure rate is BOTH fill detection AND fill rate
**File:** order_api.py:210; execution_engine.py:404; arb_h2_live.py:335
Binance private WS unavailable (EU 410) means coordinator waits 3s then falls back to `check_order()`. `check_order()` only scans last 20 `myTrades` and never queries order status or open orders. Open GTCs, partial fills, and older fills are all easy to misclassify.

### 4. Trading continues during disconnect recovery
**File:** arb_h2_live.py:444, :637
`on_disconnect()` only starts background recovery. Main loop still allows new entries whenever Bybit WS is connected. Gap recovery and live execution mutate the same coordinator state concurrently.

### 5. Exit submit-failure handling is asymmetric
**File:** execution_engine.py:694, :705, :756
If one close leg submits and fills while the other submit fails, the code can return `partial` and just "retry next cycle" with real one-sided exposure.

### 6. Exit/stop-loss uses wrong spread direction
**File:** price_feed.py:355; signal_engine.py:236, :259
`PriceFeed.get_spread()` returns the best of two arbitrage directions. For an open `BUY_BB_SELL_BN` position, the correct close spread is the reverse executable spread, not whichever direction is currently largest.

### 7. BUY_BN_SELL_BB direction trades without balance checks
**File:** price_feed.py:356; arb_h2_live.py:875
`PriceFeed` can emit `BUY_BN_SELL_BB`. `_handle_entry()` executes it without quote-balance checks or an inventory model built for that side. Strategy drift with different capital and risk semantics.

### 8. Inventory drift from fee handling
**File:** inventory_ledger.py:208; arb_h2_live.py:1064
`bought()` always subtracts `fee_qty` from received base. Exit handling passes Binance fees without checking whether fee was in base, quote, or BNB. Every Binance buy charged in USDC or BNB pushes ledger inventory lower than exchange.

### 9. Emergency unwind never updates inventory
**File:** arb_h2_live.py:957; execution_engine.py:543
On leg failure the script only releases the pre-lock. `_emergency_unwind()` does not touch inventory at all. Any transient Binance fill during failed entry leaves `expected_qty` wrong until periodic reconcile.

### 10. MOVE chain only partially broken
Returning `0` for Binance `check_position()` removes the wallet-balance bug. But `check_order()` is unreliable, partial-exit state is not persisted, and exit fallback can still falsely mark a leg closed. The cascade has changed shape, not disappeared.

### 11. Position verification skips partial fills
**File:** execution_engine.py:472
A leg with a detected partial skips verification entirely. If REST/WS under-detects a fill and the exchange actually filled more, `_emergency_unwind()` submits only the detected quantity and leaves residual exposure.

### 12. Order-state updates not matched by client ID
**File:** execution_engine.py:206, :240
`on_fill()` can match by client order id. `on_order_update()` only matches by exchange `order_id` and does not buffer unmatched updates. Early rejects/cancels during submit race can still be lost.

### 13. Orphan buybacks run before feeds/rules loaded
**File:** arb_h2_live.py:521, :555, :581
Buyback code runs before feeds and instrument rules are loaded. Usually has no price/rule context. Books inventory immediately after submit without verifying fill.

### 14. Rounding helper breaks for non-power-of-10 step sizes
**File:** instrument_rules.py:27, :34
`round_qty()` and `round_price()` derive precision from `log10(step)`, which breaks for steps like `0.5`.

## Direct Answers to Review Questions

| Question | Answer |
|----------|--------|
| 91% leg failure: detection or fill rate? | BOTH. Detection is broken (check_order scans 20 trades only). 3s GTC cancel also kills fillable orders. |
| _bb/_bn_exit_closed flags: can they get stuck? | Yes, and they're lost on restart. Mongo doesn't persist which leg closed. |
| expected_qty drift? | Yes. Fee handling, unwind non-tracking, and buy detection all cause drift. |
| check_position=0 for Binance: is check_order reliable? | No. It's better than wallet-balance bug but misses fills outside last 20 trades. |
| MOVE chain fully broken? | No. Wallet-balance link broken; rest of chain changed shape but not eliminated. |
| Async races remaining? | Yes. Disconnect recovery + live execution concurrent. Order-state matching incomplete. |
| Signal engine: stop loss direction? | Wrong. Uses best-direction spread, should use reverse-direction for open position. |
