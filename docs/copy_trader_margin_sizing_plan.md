# Copy Trader V9: Margin-Based Position Sizing

## Problem

The V8 copy trader uses fixed position sizing ($20/trade, max 8 positions, one entry per coin). This causes three failures:

1. **No add-ons**: When a target wallet averages down (e.g., 0x2be81c3c buying 50K TON in 9 clusters), we enter once and ignore all subsequent buys. We miss conviction scaling.
2. **Fixed sizing ignores equity**: $20/trade is hardcoded regardless of account equity ($38 now, could be $500 later). No margin utilization awareness.
3. **Position count != risk**: 8 positions of $20 = $160 exposure is fine at $38 equity. But 8 positions of $100 = $800 would be 21x leverage. The limit should be margin-based, not count-based.

## Current Architecture (what changes)

```
Constants (module-level):
  MAX_POSITIONS = 8          # hard cap on concurrent positions
  COOLDOWN_S = 30            # per-coin cooldown between entries

__init__():
  self.order_size = order_size_usd   # from --size CLI arg (e.g., $20)

_enter_position():
  Line 367: if len(self.positions) >= MAX_POSITIONS: return  # count-based gate
  Line 372: if any(p['coin'] == coin for p in self.positions): return  # one-entry-per-coin gate
  Line 386: sz = self._round_size(coin, self.order_size / mid)  # fixed USD sizing

MID-TWAP entry (line 781):
  if opening and len(self.positions) < MAX_POSITIONS and not any(p['coin'] == coin ...):
```

## Proposed Design

### New Risk Parameters (replace MAX_POSITIONS + fixed order_size)

```python
# Risk limits (all checks use MARGIN = notional / coin_max_leverage)
MAX_MARGIN_UTIL = 0.80      # max 80% of equity used as margin
MAX_COIN_CONCENTRATION = 0.30  # max 30% of equity margin on any single coin
BASE_POSITION_USD = 20      # base notional per entry (from --size arg)
MAX_ADDON_MULTIPLIER = 3    # max 3x base notional on a single coin via add-ons
# Leverage is PER-COIN from HL metadata, CAPPED at 10x
# e.g. BTC=10x (capped from 40x), ETH=10x (capped from 25x), PURR=3x (unchanged)
MAX_LEVERAGE_CAP = 10
# Stored in self.max_leverage: dict[str, int] — min(hl_max, MAX_LEVERAGE_CAP)
```

### New Method: `_check_margin_budget(coin, additional_notional_usd) -> bool`

All budget checks use MARGIN (notional / coin_max_leverage), not raw notional. Leverage is per-coin from HL metadata.

Queries equity (cached, invalidated after each fill) and calculates:
- `total_margin_used`: sum of all open position margin (each pos notional / its coin's max_leverage) + `_pending_margin`
- `coin_margin_used`: sum of margin on this specific coin (across ALL wallets, per F2)
- `additional_margin`: `additional_notional_usd / self.max_leverage[coin]`
- Returns True if:
  - `(total_margin_used + additional_margin) / equity <= MAX_MARGIN_UTIL`
  - `(coin_margin_used + additional_margin) / equity <= MAX_COIN_CONCENTRATION`

Example at $38 equity, $20/trade (leverage capped at 10x):
- BTC entry: $20 / 10x = $2.00 margin (capped from 40x)
- PURR entry: $20 / 3x = $6.67 margin (unchanged, below cap)
- Max total margin = $38 * 0.80 = $30.40
- Mixed portfolio: 2 BTC ($4.00) + 2 PURR ($13.34) + 2 ZEC ($4.00) = $21.34 margin = 56% util

### Changes to `_enter_position()`

1. **Remove** `len(self.positions) >= MAX_POSITIONS` gate
2. **Replace** `any(p['coin'] == coin)` with add-on logic:
   - If no position on this coin: enter with `BASE_POSITION_USD`
   - If existing position AND target is adding (same direction): add-on with `BASE_POSITION_USD`, up to `MAX_ADDON_MULTIPLIER * BASE_POSITION_USD` total on this coin
   - If existing position AND target is reversing: skip (handled by exit logic)
3. **Add** `_check_margin_budget()` gate before order placement
4. **Size** each entry as `BASE_POSITION_USD` (same as today per entry, but allows multiple entries on same coin)

### Changes to MID-TWAP entry (line 781)

Replace:
```python
if opening and len(self.positions) < MAX_POSITIONS and not any(p['coin'] == coin for p in self.positions):
```
With:
```python
if opening and self._check_margin_budget(coin, self.order_size):
```

The add-on logic goes inside `_enter_position()` which already checks direction via `_is_opening_trade()`.

### Equity Cache

```python
self._equity_cache = 0.0
self._equity_cache_ts = 0

def _get_equity(self) -> float:
    """Get account equity, cached for 60s to avoid API spam."""
    now = time.time()
    if now - self._equity_cache_ts > 60:
        try:
            r1 = requests.post(HL_API + "/info", 
                json={"type": "clearinghouseState", "user": self.parent_address}, timeout=5)
            acct = float(r1.json().get("marginSummary", {}).get("accountValue", 0))
            r2 = requests.post(HL_API + "/info",
                json={"type": "spotClearinghouseState", "user": self.parent_address}, timeout=5)
            spot = sum(float(b.get("total", 0)) for b in r2.json().get("balances", []) 
                      if b.get("coin") == "USDC")
            self._equity_cache = acct + spot
            self._equity_cache_ts = now
        except Exception as e:
            logger.warning(f"Equity fetch failed: {e}")
            # Use last cached value
    return self._equity_cache
```

### Position Tracking Enhancement

Current `self.positions` is a list of dicts. For add-ons, the same coin can appear multiple times (each entry is a separate fill). The exit logic already iterates all positions matching a coin, so this works.

Alternative: merge add-ons into a single position with averaged entry price. This is simpler for PnL tracking but makes partial exit harder.

**Decision: MERGE into single position per (wallet, coin).** Each add-on updates the average entry price and total size. Simpler for exit logic (one close order for full size), simpler for PnL tracking, and avoids the orphaned-partial-exit bug found in review.

### STATS Line Enhancement

Current: `open=N[coins]`
New: `open=N[coins] margin=X% coin_max=Y%`

### Report Enhancement

Add margin utilization to the 15-min report:
- `Margin: X/Y% used (Z% max coin: COIN)`

## Files Changed

Only `scripts/hl_copy_trader.py`:
1. Constants: Replace `MAX_POSITIONS` with 4 new constants
2. `__init__()`: Add equity cache fields
3. New method: `_get_equity()`
4. New method: `_check_margin_budget(coin, additional_usd)`
5. `_enter_position()`: Replace count gate with margin gate, add add-on logic
6. MID-TWAP entry block: Replace count+coin gate with margin gate
7. STATS line: Add margin utilization
8. Report: Add margin section

## Eng Review Findings (adversarial, 6 issues)

### F1. Margin budget race condition — CRITICAL
**Problem:** Two wallets buying different coins in the same WS batch both pass the budget check BEFORE either appends to `self.positions`. Both orders go through, blowing past MAX_MARGIN_UTIL.
**Fix:** Add `self._pending_margin` counter. Increment BEFORE the `await` order call, decrement in finally block. Include in budget calculation. Safe because asyncio is single-threaded (no true concurrency, but `await` yields control).

### F2. Add-on coin concentration across wallets — HIGH
**Problem:** Two different wallets buying BTC simultaneously both pass concentration check if it only looks at the requesting wallet. Combined exposure exceeds MAX_COIN_CONCENTRATION.
**Fix:** `_check_margin_budget()` sums coin_exposure across ALL wallets holding that coin, not just the requesting wallet. Concentration is per-coin regardless of source.

### F3. Exit with merged positions — HIGH (resolved by merge decision)
**Problem (original plan):** With separate entries per coin, exiting one entry sends reduce_only for partial size, orphaning the rest. Exit TWAP buffer is cleared on first exit.
**Fix:** By merging add-ons into a single position per (wallet, coin), exit always closes the full merged size in one order. No orphan risk.

### F4. Stale equity cache masks drawdowns — MEDIUM
**Problem:** 60s cache means entries approved on pre-loss equity value.
**Fix:** Invalidate cache (`self._equity_cache_ts = 0`) after every fill. Forces fresh fetch before next budget check.

### F5. _is_opening_trade stale after exit — MEDIUM
**Problem:** After our position exits, `_target_positions[wallet][coin]` may not be reset to 0, causing next open to look like an add-on instead of new entry.
**Fix:** On every successful exit, explicitly set `_target_positions[wallet][coin] = 0`. Trade-stream accumulation rebuilds from next open.

### F6. Zero-equity startup blocks forever — MEDIUM
**Problem:** `_equity_cache = 0.0` at startup. If first API call fails, every budget check returns False forever (division by zero or 0 equity).
**Fix:** Initialize `_equity_cache = None`. In `_check_margin_budget`, if None, call `_get_equity()` and retry once. If still None, return False with clear error log.

## Edge Cases & Safety (updated with review fixes)

1. **Inflight margin tracking (F1):** `_pending_margin` prevents concurrent over-commitment
2. **Cross-wallet concentration (F2):** Per-coin limit enforced across all source wallets
3. **Merged positions (F3):** Single position per (wallet, coin) with averaged entry price
4. **Equity invalidation (F4):** Cache busted after every fill
5. **Target snapshot reset (F5):** Zeroed on our exit, rebuilt from trade stream
6. **Startup safety (F6):** None-initialized equity, blocks all trading until first successful fetch
7. **Rapid add-ons:** COOLDOWN_S (30s) still applies per (wallet, coin). Prevents runaway scaling.
8. **Leverage safety:** MAX_MARGIN_UTIL=0.80 caps total exposure at 0.8x equity (at 1x lev). With 3x leverage, effective exposure is 2.4x equity.

## Eng Review Round 2 Findings (7 more issues)

### R2-F1. Partial IOC fill on exit leaves untracked residual — CRITICAL
**Problem:** IOC exit may partially fill. Code treats it as full exit (records PnL on full size, removes position). Residual sits on-chain untracked until reconcile (5 min).
**Fix:** After IOC fill, compare `totalSz` to `pos['size']`. If partial: update pos size to remainder, keep tracked, record PnL on filled portion only. Return False to retry.

### R2-F2. Reconcile phantom cleanup matches by coin, not (wallet, coin) — HIGH
**Problem:** If two wallets hold the same coin, phantom cleanup on wallet A's BTC also deletes wallet B's legitimate BTC position.
**Fix:** Phantom detection and removal must match on `(wallet, coin)` tuple, not just coin name.

### R2-F3. _target_positions drifts from reality over time — HIGH
**Problem:** Target positions accumulated from trade-stream deltas. Over hours, float drift + missed WS messages cause divergence. _is_opening_trade returns wrong answers.
**Fix:** For non-agent-key wallets, reconcile _target_positions against clearinghouseState every 5 min. For agent-key wallets, document the risk.

### R2-F4. orderUpdates WS may use agent address, not parent — MEDIUM
**Problem:** Maker exit fill detection subscribes to parent address, but orders placed with agent address. If HL keys on agent, maker fills never detected, every exit falls through to IOC (doubling fees).
**Fix:** Verify which address HL uses for orderUpdates. Subscribe with the correct one. Test empirically.

### R2-F5. _round_size can floor to zero on exit — MEDIUM
**Problem:** After partial exit or add-on math, fractional remainder floors to 0 via _round_size. Close order sends sz=0 (does nothing).
**Fix:** Add `if sz <= 0: return False` guard in _exit_position before order call.

### R2-F6. Kill switch ignores unrealized PnL — MEDIUM
**Problem:** Kill switch checks `total_pnl` (realized only). A -$5 unrealized position doesn't trigger shutdown. Margin-based sizing allows larger positions = larger unrealized losses.
**Fix:** Include total unrealized PnL in kill switch check. Extract upnl computation to shared method.

### R2-F7. WS reconnect doesn't clear _exit_twap_buffer — LOW
**Problem:** Stale exit signals from pre-disconnect can trigger exits with inflated reverse_notional on reconnect.
**Fix:** Clear `_exit_twap_buffer` on WS reconnect alongside `_twap_buffer`.

## Testing Plan (updated)

### Round 1 fixes
1. **F1 race**: Two concurrent `_enter_position` tasks, verify `_pending_margin` prevents both from filling
2. **F2 concentration**: Two wallets buying BTC, verify combined coin exposure capped at 30%
3. **F3 merged exit**: Enter BTC $20, add-on BTC $20, verify exit closes full $40 in one order
4. **F4 cache bust**: Enter position, verify `_equity_cache_ts` reset to 0 after fill
5. **F5 target reset**: Enter+exit, verify `_target_positions[wallet][coin]` is 0
6. **F6 startup**: Mock failed API, verify no trades until equity successfully fetched
7. **Margin budget**: Mock equity $40, per-coin leverage (BTC 10x cap, PURR 3x), verify correct margin calc
8. **Add-on flow**: Enter coin, target adds, verify merged position with averaged entry_px
9. **Regression**: Full entry/exit cycle with add-on, verify PnL calculation correct

### Round 2 fixes
10. **R2-F1 partial exit**: Mock IOC partial fill, verify position stays tracked with reduced size
11. **R2-F2 phantom**: Two wallets on BTC, one orphaned, verify only the orphaned wallet's position removed
12. **R2-F3 target drift**: Simulate 100+ trades, verify _target_positions stays within 1% of clearinghouseState
13. **R2-F5 zero size**: Mock _round_size returning 0 on exit, verify exit returns False (no order sent)
14. **R2-F6 kill switch**: Set max_daily_loss=-$2, open position with -$3 unrealized, verify kill switch fires
15. **R2-F7 reconnect**: Verify _exit_twap_buffer cleared on WS reconnect
