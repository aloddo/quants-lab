# HL MM V4 Plan — Reuse V2/V3 Infrastructure

**Date:** 2026-05-08
**Status:** PLAN (discussed with Alberto, not building yet)

## Goal

One process, many pairs, proper pair screening, reusing proven V2/V3 code.

## V2/V3 Module Audit (10,262 lines total)

| Module | Lines | Purpose | Reuse? |
|--------|-------|---------|--------|
| `orchestrator.py` | 3,434 | Main event loop, WS, per-pair actors | **REWRITE** — too tied to V3 metaorder rider logic. Keep structure, replace quoting. |
| `quote_engine.py` | 1,168 | Inside-improvement + contrarian quoting | **ADAPT** — replace contrarian logic with A-S skew + adverse selection pause |
| `inventory_manager.py` | 742 | Reservation price + age-based exits | **REUSE AS-IS** — already has A-S style inventory skew |
| `signal_engine.py` | 640 | L2 imbalance, VPIN, depth analysis, toxicity flags | **REUSE AS-IS** — already detects adverse selection correctly |
| `pair_screener.py` | 565 | Scan all pairs, score, rotate | **ADAPT** — expand to all >$500K vol pairs, add spread stability scoring |
| `fill_tracker.py` | 463 | Markout analysis + toxicity scoring per fill | **REUSE AS-IS** — this is exactly what we need |
| `ws_order_client.py` | 388 | WS order placement + fill detection | **REUSE AS-IS** — already uses correct parent address + nested OID format |
| `risk_manager.py` | 376 | Circuit breakers, portfolio risk | **REUSE AS-IS** |
| `fair_value.py` | 365 | Cross-venue anchor (HL vs Bybit) | **ADAPT** — simplify, remove Bybit dependency for now |
| `state_machine.py` | 680 | Per-pair lifecycle (IDLE→QUOTING→INVENTORY→EXIT) | **REUSE AS-IS** |
| `config.py` | 222 | All tunable params | **ADAPT** — update defaults for V4 |
| `notifier.py` | 232 | Telegram alerts | **REUSE AS-IS** |
| `mm_tracker.py` | 282 | Competitor MM monitoring | **DROP** — nice-to-have, not needed for V4 |
| `wallet_scorer.py` | 599 | Whale wallet identification | **DROP** — V3 metaorder specific |
| `avellaneda_quoter.py` | 75 | A-S formula | **REUSE** — integrate into quote engine |

**Reuse: ~5,500 lines (54%). Adapt: ~2,500 lines (24%). Drop/rewrite: ~2,200 lines (22%).**

## What Killed V2

From Alberto + post-mortem:
1. **Adverse selection in trending markets** — quoting both sides when one side is toxic
2. **Metaorder rider logic** (V3) — wallet detection was the wrong approach
3. **Wrong pair selection** — manually chosen, not screened

V2/V3 already HAD fixes for #1 (signal_engine.py lines 976-980: "coin trending UP → suppress asks"). The issue was these filters weren't aggressive enough, or the pairs chosen were inherently bad for MM.

## V4 Architecture (reusing V2/V3)

```
hl_mm_v4_live.py (new, ~200 lines)
│
├── config.py (ADAPT — V4 defaults)
├── pair_screener.py (ADAPT — expand monitoring to 75 pairs)
├── orchestrator.py (REWRITE — simplified, no metaorder/wallet logic)
│   ├── ONE WS connection for all active pairs
│   ├── l2Book subscriptions for screener monitoring (75 pairs)
│   ├── l2Book + orderUpdates for active quoting pairs (top 4-8)
│   └── Per-pair event loop using state_machine.py
│
├── state_machine.py (REUSE)
├── signal_engine.py (REUSE — adverse selection detection)
├── quote_engine.py (ADAPT — simpler quoting, no contrarian)
├── inventory_manager.py (REUSE — A-S reservation price)
├── fill_tracker.py (REUSE — markout tracking)
├── risk_manager.py (REUSE — circuit breakers)
├── ws_order_client.py (REUSE — proven WS parser)
├── notifier.py (REUSE — Telegram alerts)
└── fair_value.py (ADAPT — remove Bybit, pure HL mid)
```

## Key Changes from V2/V3

1. **No Bybit dependency** — V2 used Bybit as price anchor. V4 uses HL mid only.
2. **No wallet scorer / metaorder rider** — V3 tried to detect whale orders. V4 is pure MM.
3. **Expanded screener** — V2 monitored 8 pairs. V4 monitors 75, scores continuously.
4. **Simplified quote engine** — V2 had "contrarian" and "inside-improvement" modes. V4 quotes at native half-spread with inventory skew.
5. **One process** — V4 canaries were separate processes. V4 proper is one orchestrator managing N pairs.

## Pair Screening Expansion

V2 screener already:
- Fetches meta + asset contexts (volume, funding)
- Queries MongoDB L2 snapshots for rolling spread/depth stats
- Scores on edge_room, volume, depth, anchor type
- Manages ACTIVE/IDLE pair lifecycle

V4 adaptation needed:
- Pre-filter: volume > $500K AND spread > 3bp (from REST snapshot)
- This gives ~60-75 candidate pairs
- Subscribe to l2Book WS for all candidates (cheap — delta updates)
- Store snapshots to MongoDB for the screener to score
- Screener runs every 15 min, picks top 4-8 pairs as ACTIVE
- ACTIVE pairs get quote placement, others just get monitored

## Implementation Steps

1. **Strip V3 metaorder logic from orchestrator** — remove wallet_scorer, mm_tracker, contrarian quoting. Keep: WS management, per-pair state machine, OMS, fill tracking, risk management.

2. **Expand pair_screener** — change pre-filter from hardcoded 8 to all >$500K vol. Add spread_stability metric (std/mean of spread over 1h). Keep existing scoring.

3. **Simplify quote_engine** — replace inside-improvement with: `bid = mid - half_spread - skew`, `ask = mid + half_spread + skew`. Half_spread = max(native/2 * 0.8, break_even). Skew from inventory_manager.

4. **Simplify fair_value** — remove Bybit anchor, use HL mid directly.

5. **Wire up and test** — launch on PNUT first (proven fills), then let screener rotate pairs.

## What NOT to Build

- No new WS parser (reuse ws_order_client.py)
- No new fill tracking (reuse fill_tracker.py) 
- No new risk management (reuse risk_manager.py)
- No new state machine (reuse state_machine.py)
- No new adverse selection detection (reuse signal_engine.py)

## Open Questions for Alberto

1. Should the screener monitor ALL 75 pairs via WS, or start with 20-30 and expand?
2. Capital per pair: $11 (current) or increase to $25-50 for more meaningful fills?
3. Max concurrent active pairs: 4? 6? 8?
4. Should we keep the Bybit price anchor from V2 (better fair value) or pure HL?
