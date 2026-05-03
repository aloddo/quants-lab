# Adversarial Review — May 3, 2026

## Pre-Codex: My Own Review

### Bug #5 (Entry price branching) — VERIFIED FIXED
inventory_manager.py lines 624-641 handles all 5 cases:
- Flat position → reset to 0
- Position FLIPPED → fill price  
- Position INCREASED → weighted average
- Opened from flat → fill price
- Position REDUCED → unchanged

### Bug #11 (Fill hash dedup weak) — VERIFIED FIXED
orchestrator.py lines 1821-1831 includes coin, side, sz, px alongside oid/time/hash in MD5 hash.

### All 13 Overnight Bugs (Fixed)
1. Orphan 429 loop → exponential backoff 30s→600s
2. INVENTORY_EXIT with 0 inventory → abs(inventory_usd) >= 1.0 guard
3. Daily PnL not resetting → _maybe_reset_daily_pnl() on UTC date transition
4. Failed hedge = stuck position → 120s timeout, force EMERGENCY_FLATTEN
5. Rate limit competition → stopped collector during MM
6. OVERBUY root cause → record_fill now updates pos.size
7. Equity wrong → spot + perps combined
8. Ask crossing touch → maker enforcement
9. Thread race → threading.Lock
10. Peak equity wrong → track combined_equity
11. Snapshot missing PnL → realized_pnl, fees added
12. No cancel after fill → pending_cancel_coins
13. cancel_all blocking → asyncio.to_thread

## Codex Findings (10 bugs, 0 false positives — all fixed)

| # | Severity | Bug | File(s) | Fix |
|---|----------|-----|---------|-----|
| 1 | CRITICAL | Fill path not thread-safe — WS + REST concurrent _handle_fill can double-count fills and lose cancel signals | orchestrator.py | Added `_fill_lock` (threading.Lock) protecting _known_fill_hashes, _pending_cancel_coins, _pair_fill_counts, _fill_timestamps |
| 2 | CRITICAL | PAUSE preempts HEDGE/EMERGENCY_FLATTEN — stale data causes PAUSE, freezing age timer, positions unhedged indefinitely | state_machine.py | PAUSE now skips if current state is HEDGE or EMERGENCY_FLATTEN — those states fall through |
| 3 | CRITICAL | Deactivation orphans positions — cancel-race with late fill drops position on the floor | orchestrator.py | Added `_cooling_coins` dict — deactivated coins monitored for 30s, late fills re-activate for emergency close |
| 4 | CRITICAL | cancel_all() creates invisible ghost orders — clears local state even when cancels 429'd | quote_engine.py | Only clear state for coins with confirmed cancels; failed coins remain tracked for orphan cleanup |
| 5 | HIGH | Fill-sync health falsely green — "WS subscribed, 0 fills" treated as healthy forever | orchestrator.py | Added 30s grace period after startup; after that, no WS fills + REST dead = unhealthy |
| 6 | HIGH | Demoted pair keeps adding inventory — _pending_idle_close never consulted by orchestrator | orchestrator.py | Demoted coins get entry-side EV suppressed; cleared when inventory reaches zero |
| 7 | HIGH | Hedge state desyncs from Bybit — open IOC not verified, close IOC zeroed without verification | orchestrator.py | Hedge open: retry fill query 3x with 1s delay. Hedge close: verify fill qty, partial fills keep tracking, retry |
| 8 | HIGH | Portfolio risk on stale snapshots — _cached_snapshot has no age bound | orchestrator.py | Added 120s max age; stale snapshots rejected, risk checks use zero values (conservative) |
| 9 | MEDIUM | Inventory lock doesn't cover reads — compute_reservation_price, get_inventory_age_s, pause_inventory_age, get_free_equity all unlocked | inventory_manager.py | All read methods now acquire _lock; get_position returns shallow copy to prevent concurrent mutation |
| 10 | MEDIUM | Toxicity CB keys on current spread not fill-time spread — gameable/noisy | fill_tracker.py | Added spread_at_fill_bps field on Fill; markout uses fill-time spread for threshold |

### Tokens: 966,300 | Reasoning: high

## Codex Round 2 — Profitability Focus (7 findings, all fixed)

Focus: execution speed, rate limit management, spread capture, "will it make money?"

| # | Category | Bug | File(s) | Fix |
|---|----------|-----|---------|-----|
| 1 | BLOCKER | Deadlock: get_free_equity() re-acquires Lock held by sync_positions_safe() | inventory_manager | Changed threading.Lock → threading.RLock (reentrant) |
| 2 | SPEED | 1.2s requote min + 1.0s tick = effective 2s cadence | quote_engine | Dropped requote_min_interval to 0.5s |
| 3 | SPEED | Post-fill cancel waits 1 full tick (~1s stale exposure) | orchestrator | Fire cancel immediately via asyncio.run_coroutine_threadsafe from WS thread |
| 4 | SPEED | Telegram blocks WS thread for up to 5s per fill | notifier | Fire-and-forget via daemon thread |
| 5 | RATE | detect_fills() calls open_orders per-coin per-tick (5 coins = 5 REST/s) | orchestrator + quote_engine | Query open_orders ONCE per tick, pass snapshot to detect_fills_from_snapshot() |
| 6 | SPREAD | Inside improvement computed then forbidden (bid<=hl_bid, ask>=hl_ask) = join-only | quote_engine | Allow 1-tick improvement inside the touch for queue priority |
| 7 | SPREAD | Hedge costs ~12bps, wipes 5+ round-trips on narrow spreads | state_machine | Only hedge when native spread > 10bps; below that, HL taker-close is cheaper |

### Math
```
Theoretical max edge per round-trip = spread - 2*1.44bps = spread - 2.88bps
  5bps  → 2.12bps max
  10bps → 7.12bps max
  20bps → 17.12bps max
```

### Codex verdict: "No, not in this form" — but with these 7 fixes, the structural blockers are addressed.

### Tokens: 1,095,577 | Reasoning: high
