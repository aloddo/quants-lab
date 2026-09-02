"""Live-fix batch 1 (2026-08-06): L-F5 fast-stop kill sync (FIXED, flag-gated) + L-F4 defect pins.

L-F5: fixed behind `fast_stop_kill_sync` (default OFF = byte-identical legacy). codex-approved.
L-F4: OPEN DEFECT, deliberately NOT hot-fixed. A first fix that zeroed `_target_positions` on
usable snapshots was codex-REJECTED: ineffective (V17 verb classification reads the SEPARATE
`_v16_leader_pos` map at :7390/:5493 and suppresses the opposite-side open as REDUCE_NOT_HELD
before `_is_opening_trade` is ever consulted) and unsafe (no request-generation guard: a WS fill
landing between REST observation and application would be overwritten by the OLDER snapshot, and
zeroing can flip `_adopt_orphan` attribution into `_force_exit` — a close path). The real fix is
generation-guarded, updates BOTH maps, and lands with the target-vs-actual reconciliation build.
The pins below document the tracker-layer half of the defect so it cannot be silently forgotten;
the classifier-layer half is documented in docs/research/backtest_live_parity_map_20260806.md
(STAGE-1 FIXTURE FINDINGS, L-F4) and must gain a production-path (_on_hl_trade) pin in the
reconciliation build's test set.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_live_fix_batch1.py -q
"""
import asyncio
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "strategies" / "live"))

import hl_copy_trader_v17 as mod  # noqa: E402

W = "0xleader"


# ── L-F5 (FIXED, flag-gated): fast global stop syncs the kill switch ─────────────────────────────

def _stop_engine(flag: bool):
    eng = object.__new__(mod.V17CopyTrader)
    eng._flatten_requested = False
    eng._baseline_equity = 1000.0
    eng._session_realized_base = 0.0
    eng._exch_pnl = {"account_net": -200.0}      # realized -200 on 1000 = -20% < -15%
    eng._compute_unrealized_pnl = lambda: 0.0
    eng.global_stop_pct = 0.15
    eng._kill_reasons = {}
    eng._kill_switch_active = False
    eng.global_config = {"fast_stop_kill_sync": flag}
    return eng


def test_lf5_defect_flag_off_kill_switch_stays_false(monkeypatch):
    """DEFECT PIN (legacy behavior, byte-identical with the flag off): the fast latch sets
    _kill_reasons + _flatten_requested but the entry-blocking _kill_switch_active stays False
    until the 60s stats-loop sync (<=60s open-then-flatten churn window)."""
    monkeypatch.setattr(mod, "_tg", lambda *a, **k: None)
    eng = _stop_engine(flag=False)
    eng._evaluate_global_stop_fast()
    assert eng._flatten_requested is True
    assert eng._kill_reasons.get("global_stop") is True
    assert eng._kill_switch_active is False, "legacy path must stay byte-identical (defect pinned)"


def test_lf5_fix_flag_on_kill_switch_syncs_immediately(monkeypatch):
    monkeypatch.setattr(mod, "_tg", lambda *a, **k: None)
    eng = _stop_engine(flag=True)
    eng._evaluate_global_stop_fast()
    assert eng._flatten_requested is True
    assert eng._kill_switch_active is True, "flag ON must close the <=60s entry window at the latch"


def test_lf5_flag_on_no_stop_no_latch(monkeypatch):
    """Flag ON must not latch anything when the stop is NOT breached."""
    monkeypatch.setattr(mod, "_tg", lambda *a, **k: None)
    eng = _stop_engine(flag=True)
    eng._exch_pnl = {"account_net": -50.0}       # -5% > -15%: no stop
    eng._evaluate_global_stop_fast()
    assert eng._flatten_requested is False
    assert eng._kill_switch_active is False


# ── L-F4 (OPEN DEFECT): pins so the failure mode cannot be silently forgotten ────────────────────

def _prefetch_engine(tracker: dict):
    eng = object.__new__(mod.V17CopyTrader)
    eng._leader_snapshot = {}
    eng._leader_flat_outage = {}
    eng._leader_flat_outage_alerted = {}
    eng._target_positions = {W: dict(tracker)}
    eng.global_config = {"leader_flat_poll_s": 0.0}
    return eng


def _run_prefetch(eng, dx, body):
    """Drive the REAL _prefetch_leader_snapshots with the REST read stubbed to `body`."""
    eng._snapshot_leader_positions = lambda w, d: body
    asyncio.run(eng._prefetch_leader_snapshots([(W, dx)]))


def test_lf4_pin_snapshot_never_zeroes_absent_coins():
    """DEFECT PIN, tracker layer: a usable snapshot omitting a flat coin leaves the tracker stale
    (flat coins are simply not listed in clearinghouseState). If this test ever FAILS, someone
    changed snapshot semantics — make sure the change is the generation-guarded BOTH-maps fix from
    the reconciliation build, not a naive zeroing (codex-REJECTED 2026-08-06)."""
    eng = _prefetch_engine(tracker={"BTC": 2.0})
    _run_prefetch(eng, "", {"ETH": 1.0})                     # usable body; BTC absent = flat
    assert eng._target_positions[W]["BTC"] == 2.0, "current (defective) semantics: absentee stays stale"
    assert eng._target_positions[W]["ETH"] == 1.0


def test_lf4_pin_stale_tracker_refuses_opposite_open():
    """DEFECT PIN, _is_opening_trade layer: the stale same-sign tracker refuses the leader's next
    OPPOSITE-side open (missed entry). NOTE: in production the suppression fires even EARLIER, in
    V17 verb classification via the separate _v16_leader_pos map (REDUCE_NOT_HELD) — that layer's
    pin belongs to the reconciliation build's production-path test set."""
    eng = _prefetch_engine(tracker={"BTC": 2.0})
    _run_prefetch(eng, "", {"ETH": 1.0})
    eng._post_exit_cooldown = {}
    eng.mid_prices = {"BTC": 100.0}
    assert eng._is_opening_trade(W, "BTC", is_buy=False) is False, \
        "stale same-sign tracker refuses the opposite-side open (the missed-entry defect)"
