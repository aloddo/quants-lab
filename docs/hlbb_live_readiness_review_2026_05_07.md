# HLBB Live Readiness Review - 2026-05-07

This is the handoff for the Hyperliquid-Bybit perp/perp arb engine review before live trading.

## Current Runtime State

- Dry-run HLBB engine is running in tmux session `arb-hlbb`.
- Collector is running and writing 30-pair raw snapshots.
- Dry-run logs show healthy HL and Bybit WS feeds and active signal processing.
- Tick aggregation was fixed and smoke-tested, but it is not on the live execution critical path. It is mainly needed for research, backtests, regime stats, dashboards, and future threshold/feature work. Live execution seeds from `arb_hl_bybit_perp_snapshots` and then trades from live WS quotes.

## Fixes Already Applied

Files changed:

- `app/services/arb_hlbb/config.py`
- `app/services/arb_hlbb/order_api.py`
- `app/services/arb_hlbb/orchestrator.py`
- `app/tasks/data_collection/tick_aggregation_task.py`
- `tests/arb_hlbb/test_safety_fixes.py`

Implemented safety fixes:

- Partial fills no longer treated as full success.
- Added per-pair entering guard to prevent duplicate entries.
- WS thread now submits signals into asyncio via `call_soon_threadsafe`.
- Crash recovery checks live exchange positions in live mode.
- Naked-leg unwind uses actual filled size and checks unwind result.
- Max-hold watchdog can fire independent of fresh feed availability.
- Live entry requotes immediately before execution and aborts on stale/collapsed edge.
- Bybit fill detection polls multiple times instead of one 100ms check.
- Bybit entry/exit price rounding is directional.
- Live risk state can use exchange equity instead of modeled PnL.
- `--paper` now simulates instead of initializing real APIs.
- Tick aggregation UTC datetime handling was fixed.

Verification already run:

```bash
/Users/hermes/miniforge3/envs/quants-lab/bin/python -m py_compile app/services/arb_hlbb/config.py app/services/arb_hlbb/order_api.py app/services/arb_hlbb/orchestrator.py app/tasks/data_collection/tick_aggregation_task.py tests/arb_hlbb/test_safety_fixes.py
/Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/arb_hlbb/test_safety_fixes.py -q
```

Result: `5 passed`.

Tick aggregation smoke result:

```text
processed=9683 ticks, stats_written=900, latest 1m stats around 2026-05-07 08:36 UTC
```

## Critical Remaining Findings

1. Exit logic can use the wrong spread direction.
   `app/services/arb_hlbb/signal_engine.py:223` and `app/services/arb_hlbb/orchestrator.py:887` use `best_spread_bps`. After entry, `best_spread_bps` can flip to the opposite arbitrage direction. That can delay exits, trigger false exits, or misstate PnL. Fix by tracking direction-specific spread for the opened position.

2. Emergency unwind prices may be rejected by venue precision rules.
   `app/services/arb_hlbb/orchestrator.py:1072` uses raw HL `mid * 1.02/0.98`; `app/services/arb_hlbb/orchestrator.py:1107` formats Bybit unwind price as `f"{aggressive:.6f}"`. Emergency paths must use venue-valid price precision/tick rounding.

3. Normal HL order prices are not precision-normalized.
   Entry/exit pass raw HL bid/ask into IOC orders. HL has price precision/significant-figure rules. A reject can leave the Bybit leg filled and create exposure.

4. Orders are IOC at top-of-book, not aggressively marketable.
   Entry sells at bid and buys at ask. Under fast spreads, this will miss frequently. Add configurable aggression, likely 2-5 bps for canary size, and measure actual slippage.

5. Bybit fill detection can still miss fills.
   `app/services/arb_hlbb/order_api.py:359` only checks `/v5/order/realtime`. IOC fills can move to execution history quickly. Add `/v5/execution/list` fallback or private execution WS before trusting `NOT_FILLED`.

6. Bybit account mode is not validated.
   Orders do not set `positionIdx`. If the account is in hedge mode, orders or reduce-only closes may fail or hit the wrong slot. Preflight must assert one-way mode or explicitly support hedge mode.

7. Leverage config is unused and there is no margin preflight.
   `app/services/arb_hlbb/config.py:39` defines leverage, but no setup/check enforces it. A margin reject on one venue after the other venue fills is a naked-leg path.

8. No client order IDs / idempotency.
   Bybit orders omit `orderLinkId`; HL orders omit `cloid`. Crash/retry reconciliation depends on positions only, not submitted order identity.

9. No depth/VWAP check.
   Feed uses top-of-book only. For $100 canary this may be acceptable, but scale-up needs executable depth and VWAP at target size.

10. Re-entry cooldown is missing.
    Dry-run showed repeated entries seconds apart on DYDX. Add per-pair cooldown after exit and especially after stop-loss or leg failure.

## Where Execution Is Most Likely To Fail

- HL rejects price precision/significant-figure formatting.
- Bybit rejects price precision in emergency unwind.
- Bybit rejects or misroutes orders if account is hedge mode and `positionIdx` is missing.
- One venue fills while the other misses because IOC prices are only top-of-book.
- Bybit reports `NOT_FILLED` from realtime even though an IOC fill exists in execution history.
- Emergency close fails due invalid price formatting or insufficient marketable aggression.

## Where Money Loss Is At Risk

- Wrong-direction exit logic keeps exposure open after the entry-direction spread has reverted or widened.
- False fill state causes unnecessary unwind or residual exposure.
- Emergency unwind rejection leaves a naked leg live.
- Repeated immediate re-entry churns fees and can repeatedly hit the same bad microstructure regime.
- Running on a shared account makes exchange-equity risk limits noisy and can hide or falsely attribute PnL from other bots.
- No margin/leverage preflight means one-leg rejects can happen during entry.

## First-Hours Runtime Monitoring Checklist

Monitor these continuously during any first live canary:

- HL and Bybit actual positions by pair, side, and size vs Mongo `arb_hlbb_positions`.
- Any `RECONCILE_REQUIRED`, `NAKED LEG`, `PARTIAL`, `NOT_FILLED`, `REJECTED`, or unwind failure log.
- Bybit `retCode` and HL order error payloads.
- Fill latency and fill ratio by venue.
- Direction-specific spread at entry and exit, not only `best_spread_bps`.
- Feed age for every open position.
- Live exchange equity and account-level positions, especially if account is shared with H2 or other live bots.
- Re-entry frequency per pair.
- Telegram alerts must arrive before live canary.

## Go / No-Go Recommendation

Do not go live beyond tiny canary size until findings 1-7 are fixed or explicitly accepted as known risk.

Minimum before canary:

- Fix direction-specific exit/PnL.
- Add valid HL and Bybit price formatting for normal orders and emergency unwind.
- Validate Bybit account mode and either set `positionIdx` or assert one-way mode.
- Add Bybit execution-history fallback for IOC fill detection.
- Add margin/leverage/account preflight.
- Add per-pair cooldown.

Canary should start at the smallest useful live size, with manual exchange dashboards open and immediate ability to kill tmux and manually flatten both venues.
