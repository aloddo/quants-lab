#!/usr/bin/env python3
"""V15 M02 unit tests — NON-LOOK-AHEAD invariants + state machine correctness.

Synthetic fixtures only; no network, no Mongo, no parquet. We monkeypatch
m01.get_mark so equity reconstruction is deterministic, then drive
trace_wallet / compute_event_equity directly.

Run:
    /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m02.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research" / "v15"))

import v15_m01_equity_reconstruct as m01  # noqa: E402
import v15_m02_journey_trace as m02  # noqa: E402


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #

FIXED_MARK = 100.0


@pytest.fixture(autouse=True)
def _patch_marks(monkeypatch):
    """Deterministic mark: $100 for BTC, None for an unmarkable exotic coin."""
    def fake_mark(coin, ts_ms, causal=False):
        if coin.startswith("EXOTIC"):
            return None
        return FIXED_MARK
    monkeypatch.setattr(m01, "get_mark", fake_mark)
    monkeypatch.setattr(m02.m01, "get_mark", fake_mark)


def _fill(ts, coin, side, size, price, start_pos, tid, closed=0.0, dir_="", liq=False):
    return {
        "coin": coin, "side": side, "size": float(size), "price": float(price),
        "time": int(ts), "tid": int(tid), "dir": dir_,
        "closedPnl": float(closed), "startPosition": float(start_pos),
        "fee": 0.0, "builderFee": 0.0, "deployerFee": 0.0,
        "signed_sz": float(size) if side == "B" else -float(size),
        "is_liquidation": liq,
    }


class _Anchor:
    """Minimal AnchorState stand-in: no pre-anchor positions, far-future fetch."""
    positions: dict = {}
    entry_px: dict = {}
    fetched_ms = 10**15
    dexes_seen = {"main"}
    has_flx_anchor = False
    has_unmarkable_dex_anchor = False
    acct_value_by_dex = {"main": 1000.0}
    aggregate_acct_value = 1000.0


def _run(fills, funding=None, anchors=None):
    funding = funding or []
    # default anchor strictly BEFORE the first fill so events are sizeable.
    anchors = anchors if anchors is not None else [(fills[0]["time"] - 1000, 1000.0)]
    anchor = _Anchor()
    ordered = m01.build_event_stream(fills, funding, [])
    events = m01.compute_event_equity(
        ordered, fills, anchor, "0xtest", anchors, extradex_no_anchor=False
    )
    actions, journeys = m02.trace_wallet("0xtest", events, fills, funding)
    return events, actions, journeys


# --------------------------------------------------------------------------- #
# NON-LOOK-AHEAD INVARIANTS
# --------------------------------------------------------------------------- #


def test_event_order_strictly_monotone():
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),
        _fill(3000, "BTC", "A", 1, 110, 1, tid=2, closed=10.0),
    ]
    events, actions, _ = _run(fills)
    orders = [a["event_order"] for a in actions]
    assert orders == sorted(orders)
    assert len(set(orders)) == len(orders)  # strict


def test_anchor_ts_strictly_before_ts():
    fills = [_fill(5000, "BTC", "B", 1, 100, 0, tid=1)]
    # anchor shares the action ms -> must NOT be chosen (strict <).
    _, actions, _ = _run(fills, anchors=[(2000, 1000.0), (5000, 1100.0)])
    a = actions[0]
    assert a["anchor_ts"] == 2000
    assert a["anchor_ts"] < a["ts"]


def test_equity_ts_and_mark_ts_le_ts():
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),
        _fill(4000, "BTC", "B", 1, 100, 1, tid=2),
    ]
    _, actions, _ = _run(fills)
    for a in actions:
        if a["equity_ts"] is not None:
            assert a["equity_ts"] <= a["ts"]
        if a["mark_ts"] is not None:
            assert a["mark_ts"] <= a["ts"]


def test_no_anchor_implies_null_target():
    # first fill is BEFORE any anchor -> NO_ANCHOR, target null.
    fills = [
        _fill(1000, "BTC", "B", 1, 100, 0, tid=1),   # before anchor at 2000
        _fill(3000, "BTC", "B", 1, 100, 1, tid=2),   # after anchor -> sizeable
    ]
    _, actions, _ = _run(fills, anchors=[(2000, 1000.0)])
    a0 = next(a for a in actions if a["ts"] == 1000)
    assert a0["equity_basis_mode"] == "NO_ANCHOR"
    assert a0["target_exposure_pct"] is None
    a1 = next(a for a in actions if a["ts"] == 3000)
    assert a1["equity_basis_mode"] in ("INTRAWEEK", "PARTIAL_MTM")
    assert a1["target_exposure_pct"] is not None


# --------------------------------------------------------------------------- #
# STATE MACHINE CORRECTNESS
# --------------------------------------------------------------------------- #


def test_entry_addon_trim_exit_sequence():
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),                 # ENTRY long 1
        _fill(2100, "BTC", "B", 1, 100, 1, tid=2),                 # ADDON -> 2
        _fill(2200, "BTC", "A", 1, 110, 2, tid=3, closed=10.0),    # TRIM -> 1
        _fill(2300, "BTC", "A", 1, 120, 1, tid=4, closed=20.0),    # EXIT -> 0
    ]
    _, actions, journeys = _run(fills)
    assert [a["action_type"] for a in actions] == ["ENTRY", "ADDON", "TRIM", "EXIT"]
    assert len(journeys) == 1
    j = journeys[0]
    assert j["side"] == "long"
    assert j["n_entry_fills"] == 1 and j["n_addon_fills"] == 1
    assert j["n_trim_fills"] == 1 and j["n_exit_fills"] == 1
    assert abs(j["realized_pnl"] - 30.0) < 1e-6
    # position_after sanity
    assert abs(actions[1]["position_after"] - 2.0) < 1e-9
    assert abs(actions[3]["position_after"]) < 1e-9


def test_reverse_two_journey_ids():
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),                  # ENTRY long 1
        _fill(2100, "BTC", "A", 3, 110, 1, tid=2, closed=10.0),     # REVERSE -> short 2
        _fill(2200, "BTC", "B", 2, 105, -2, tid=3, closed=10.0),    # EXIT -> 0
    ]
    _, actions, journeys = _run(fills)
    rev = next(a for a in actions if a["action_type"] == "REVERSE")
    # one row, BOTH ids, and they differ (close prior, open new).
    assert rev["closing_journey_id"] is not None
    assert rev["opening_journey_id"] is not None
    assert rev["closing_journey_id"] != rev["opening_journey_id"]
    assert rev["journey_id"] == rev["opening_journey_id"]
    # position flips sign
    assert rev["position_after"] < 0
    # two journeys: the closed long + the closed short
    assert len(journeys) == 2
    sides = sorted(j["side"] for j in journeys)
    assert sides == ["long", "short"]


def test_causal_carry_in_via_startposition():
    # first in-window fill reports startPosition = +5 (carried in pre-window).
    fills = [
        _fill(3000, "BTC", "A", 2, 110, 5, tid=1, closed=20.0),   # TRIM of carried 5 -> 3
        _fill(3100, "BTC", "A", 3, 110, 3, tid=2, closed=30.0),   # EXIT -> 0
    ]
    _, actions, journeys = _run(fills)
    # carry-in opens an in-progress journey; first action is a TRIM (not ENTRY).
    assert actions[0]["action_type"] == "TRIM"
    assert actions[0]["carry_in_status"] == "SEEDED"
    assert len(journeys) == 1
    j = journeys[0]
    assert j["n_carry_in_seeds"] == 1
    assert j["side"] == "long"
    assert j["carry_in_status"] == "SEEDED"
    # position_after of first action = 5 - 2 = 3
    assert abs(actions[0]["position_after"] - 3.0) < 1e-9


def test_liquidation_flags_involuntary_exit():
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),
        _fill(2100, "BTC", "A", 1, 80, 1, tid=2, closed=-20.0,
              dir_="Liquidated Cross Long", liq=True),
    ]
    _, actions, journeys = _run(fills)
    ex = actions[-1]
    assert ex["action_type"] == "EXIT"
    assert ex["is_liquidation"] is True
    assert journeys[0]["liq_closed"] is True


def test_target_exposure_pct_sane():
    # equity ~1000, position 5 @ 100 = 500 notional -> ~0.5 (50%).
    fills = [_fill(3000, "BTC", "B", 5, 100, 0, tid=1)]
    _, actions, _ = _run(fills, anchors=[(2000, 1000.0)])
    a = actions[0]
    # equity_post after buying 5@100: cash = 1000 - 0(no seed) - 500 = 500; posval=500 -> eq 1000.
    assert a["target_exposure_pct"] is not None
    assert 0.0 < a["target_exposure_pct"] < 1.0


def test_partial_mtm_on_unmarkable():
    # An EXOTIC coin (no mark) held alongside cash -> PARTIAL_MTM / frozen comp.
    fills = [
        _fill(3000, "EXOTIC:FOO", "B", 1, 50, 0, tid=1),   # unmarkable position
        _fill(3100, "BTC", "B", 1, 100, 0, tid=2),
    ]
    events, actions, _ = _run(fills, anchors=[(2000, 1000.0)])
    btc = next(a for a in actions if a["coin"] == "BTC")
    assert btc["equity_basis_mode"] in ("PARTIAL_MTM", "ANCHOR_FALLBACK")
    assert btc["equity_degraded"] is True


# --------------------------------------------------------------------------- #
# CAUSALITY BUG 1 — no future-fill coin in earlier equity_post (look-ahead)
# --------------------------------------------------------------------------- #


def test_future_coin_does_not_affect_earlier_equity(monkeypatch):
    """A coin first TRADED at t2 must NOT contribute a marked position at an
    earlier event t1, even if its first fill reports a nonzero startPosition
    (pre-anchor holding only LEARNED from the future fill = look-ahead).

    Per-coin marks so the leak is observable in position_value:
      BTC = 100, ALT = 50. ALT's first fill is at t2=3000 with startPosition=10.
    Before the fix, seed_positions case 2 seeded ALT=10 at the anchor, so ALT was
    in the book at t1=2000 (position_value would include 10*50=500). After the fix
    ALT is absent at t1 and materialises only at its own fill (order <= k).
    """
    def per_coin_mark(coin, ts_ms, causal=False):
        return 50.0 if coin == "ALT" else 100.0
    monkeypatch.setattr(m01, "get_mark", per_coin_mark)
    monkeypatch.setattr(m02.m01, "get_mark", per_coin_mark)

    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),       # t1: BTC entry, ALT untraded
        _fill(3000, "ALT", "A", 2, 50, 10, tid=2),       # t2: ALT first fill, startPos=10
    ]
    anchor = _Anchor()
    ordered = m01.build_event_stream(fills, [], [])
    events = m01.compute_event_equity(
        ordered, fills, anchor, "0xtest", [(1000, 1000.0)], extradex_no_anchor=False
    )
    e_t1 = next(e for e in events if e.ts == 2000 and e.type == "fill")
    # CAUSAL: at t1 only BTC (1 @ 100) is held; ALT (would be 10 @ 50 = 500) must
    # NOT leak in. position_value is exactly the BTC leg.
    assert abs(e_t1.position_value - 100.0) < 1e-6
    assert e_t1.markable_all is True
    # ALT only appears from t2 onward.
    e_t2 = next(e for e in events if e.ts == 3000 and e.type == "fill")
    # at t2 ALT pos = 10 (startPos) - 2 = 8 @ 50 = 400, plus BTC 100 -> 500.
    assert abs(e_t2.position_value - 500.0) < 1e-6


# --------------------------------------------------------------------------- #
# CAUSALITY BUG 2 — same-ms fills (no tid) get distinct correct actions
# --------------------------------------------------------------------------- #


def test_same_ms_fills_distinct_correct_actions():
    """Two same-ms fills on the same coin with NO tid (S3 partition drops tid)
    must produce TWO distinct, correctly-ordered actions with correct
    position_after — not collide on (ts, tid=0) and overwrite each other.

    Both fills at ts=3000, tid=0. Fill A: buy 1 @100 (ENTRY, pos->1).
    Fill B: buy 2 @100 (ADDON, pos->3). Their stable per-fill sequence (load
    order) keeps them ordered and distinct on the event stream.
    """
    fills = [
        _fill(3000, "BTC", "B", 1, 100, 0, tid=0),   # ENTRY -> 1
        _fill(3000, "BTC", "B", 2, 100, 1, tid=0),   # ADDON -> 3 (startPos=1 = post-A)
    ]
    _, actions, _ = _run(fills, anchors=[(2000, 1000.0)])
    btc = [a for a in actions if a["coin"] == "BTC"]
    assert len(btc) == 2  # two distinct rows, no overwrite
    assert [a["action_type"] for a in btc] == ["ENTRY", "ADDON"]
    assert abs(btc[0]["position_after"] - 1.0) < 1e-9
    assert abs(btc[1]["position_after"] - 3.0) < 1e-9
    # strictly distinct event_order despite identical (ts, tid)
    assert btc[0]["event_order"] != btc[1]["event_order"]
    # signed sizes preserved per fill (not both = the overwritten last payload)
    assert abs(btc[0]["signed_size"] - 1.0) < 1e-9
    assert abs(btc[1]["signed_size"] - 2.0) < 1e-9


# --------------------------------------------------------------------------- #
# M01 spot/collateral invariant
# --------------------------------------------------------------------------- #


def test_m01_spot_collateral_invariant():
    """Whole-account equity includes collateral (spot USDC) via the cash snap.

    With NO open positions, equity_post == anchor_equity (pure collateral/cash).
    A pure deposit-like cash addition must raise equity_post by that amount,
    proving collateral is an explicit equity component (not dropped)."""
    # one fill that immediately closes -> book flat, equity == cash == collateral.
    fills = [
        _fill(3000, "BTC", "B", 1, 100, 0, tid=1),
        _fill(3100, "BTC", "A", 1, 100, 1, tid=2, closed=0.0),
    ]
    events, _, _ = _run(fills, anchors=[(2000, 1000.0)])
    last = events[-1]
    # flat book, no PnL, no fees -> equity == anchor collateral (1000).
    assert abs(last.equity_post - 1000.0) < 1e-6
    assert abs(last.position_value) < 1e-6  # nothing held
    assert abs(last.cash - 1000.0) < 1e-6   # all equity is collateral/cash


def test_snapshot_seed_independent_of_future_fills(monkeypatch):
    """codex code-review r2 BUG: a near-fetch anchor snapshot coin must be seeded
    based on the snapshot alone, NOT suppressed by whether the wallet trades it
    LATER. Equity at an early event must be identical whether or not a future fill
    on the snapshot coin exists (else the seed reads fills with order > k = look-ahead).

    Uses TIME-VARYING marks (ETH 100 at the anchor, 120 after) so the seeded ETH
    contributes net PnL — otherwise a flat mark makes the seed net out of equity and
    the test cannot observe the bug."""

    def mark(coin, ts_ms, causal=False):
        if coin == "ETH":
            return 100.0 if ts_ms <= 1000 else 120.0
        return 100.0  # BTC flat
    monkeypatch.setattr(m01, "get_mark", mark)
    monkeypatch.setattr(m02.m01, "get_mark", mark)

    class _NearFetchAnchor(_Anchor):
        positions = {"ETH": 5.0}   # 5 ETH held pre-window, in the fetch snapshot
        fetched_ms = 1500          # near the anchor (1000) -> near_fetch True

    def equity_at_btc_event(fills):
        anchor = _NearFetchAnchor()
        ordered = m01.build_event_stream(fills, [], [])
        ev = m01.compute_event_equity(
            ordered, fills, anchor, "0xt", [(1000, 1000.0)], extradex_no_anchor=False
        )
        e = [x for x in ev if x.ts == 2000][0]   # the BTC ENTRY event
        return e.equity_post

    base = [_fill(2000, "BTC", "B", 1, 100, 0, tid=1)]                       # never trades ETH
    with_future = base + [_fill(3000, "ETH", "A", 1, 120, 5, tid=2)]         # trades ETH AFTER the event
    eq_base = equity_at_btc_event(base)
    eq_future = equity_at_btc_event(with_future)
    # cash = 1000 - 5*100(ETH@anchor) = 500; BTC buy -100 -> 400; at t=2000:
    # equity = 400 + ETH(5*120=600) + BTC(1*100) = 1100. ETH's +100 PnL is visible.
    assert abs(eq_base - 1100.0) < 1e-6, eq_base
    # THE LOOK-AHEAD CHECK: identical whether or not ETH is traded later.
    assert abs(eq_base - eq_future) < 1e-6, (eq_base, eq_future)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
