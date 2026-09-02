#!/usr/bin/env python3
"""V15 M02 unit tests — NON-LOOK-AHEAD invariants + state machine correctness.

Synthetic fixtures only; no network, no Mongo, no parquet. We monkeypatch
the optional enrichment module's get_mark so equity reconstruction is deterministic, then drive
trace_wallet / compute_event_equity directly.

Run:
    /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m02.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
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


def test_core_m02_does_not_reconstruct_equity(monkeypatch):
    """The default lifecycle lane must run while M01 is absent or distrusted."""
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),
        _fill(3000, "BTC", "A", 1, 110, 1, tid=2, closed=10.0),
    ]
    for i, fill in enumerate(fills):
        fill["fill_seq"] = i
        fill["causal_order_ok"] = True
    # The core lane is deliberately backed by the standalone hot-store I/O module, not M01.
    monkeypatch.setattr(m02.fio, "load_wallet_fills", lambda *_: fills)
    monkeypatch.setattr(m02.fio, "load_wallet_funding", lambda *_: [])

    def forbidden(*_args, **_kwargs):
        raise AssertionError("core M02 consulted M01 equity reconstruction")

    monkeypatch.setattr(m01, "reconstruct_wallet_event_equity", forbidden)
    result = m02.process_wallet(("0xtest", 0, 10_000, False))

    assert "error" not in result
    assert len(result["actions"]) == 2
    assert len(result["journeys"]) == 1
    assert result["n_anchors"] is None
    assert result["inter_drift"] is None
    assert all(a["equity_basis_mode"] == "NOT_REQUESTED" for a in result["actions"])
    assert all(a["source_equity_post"] is None for a in result["actions"])
    assert all(a["target_exposure_pct"] is None for a in result["actions"])


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


def test_fill_fee_components_are_not_double_counted():
    f = _fill(2000, "BTC", "B", 1, 100, 0, tid=1)
    f.update(fee=0.40, builderFee=0.10, deployerFee=0.05)

    # Hyperliquid reports fee as the total; the two component fields are
    # disclosures within that total.
    assert m01.fill_cash_delta(f) == pytest.approx(-100.40)
    assert m02._fill_fee_usd_actual(f) == pytest.approx(0.40)


def test_liquidation_ledger_summary_is_recognized_as_non_cash():
    e = {
        "delta": {
            "type": "liquidation",
            "accountValue": "25.0",
            "liquidatedNtlPos": "1000.0",
            "liquidatedPositions": [{"coin": "BTC", "szi": "0.01"}],
        }
    }
    d = m01.ledger_cash_delta(e, "0xtest")
    assert d.cash == 0.0
    assert d.ext_flow == 0.0
    assert d.unknown is False


def test_hip4_outcome_fills_are_not_perp_actions_or_cash_flows():
    f = _fill(2000, "#42", "B", 10, 0.4, 0, tid=1, dir_="Buy")
    assert m01.coin_is_allowed_perp(f["coin"]) is False
    assert m01.fill_cash_delta(f) == 0.0
    assert m01.build_event_stream([f], [], []) == []


def test_named_spot_pair_is_not_a_perp():
    f = _fill(2000, "PURR/USDC", "B", 10, 0.1, 0, tid=2, dir_="Buy")
    assert m01.coin_is_allowed_perp(f["coin"]) is False
    assert m01.build_event_stream([f], [], []) == []


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


def test_daily_walk_gross_notional_does_not_net_opposing_coins(monkeypatch):
    monkeypatch.setattr(m01, "get_mark", lambda coin, ts_ms, causal=False: 100.0)
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1),
        _fill(2100, "ETH", "A", 1, 100, 0, tid=2),
    ]
    stream = [(f["time"], "fill", f) for f in fills]
    wr = m01.compute_eq_at(stream, fills, _Anchor(), "0xtest", 3000, 1000, 1000.0)
    assert wr.position_value == pytest.approx(0.0)
    assert wr.gross_position_notional == pytest.approx(200.0)


def test_inter_anchor_drift_validates_causal_not_future_snapshot_seed(monkeypatch):
    """The quarantine audit must use the same causal seed as emitted equity.

    A future snapshot says BTC=10, while the observed burst causally opens only
    3 BTC in this segment.  The old audit-only ex-post path back-solved a
    pre-anchor position of 7 from that future snapshot and measured a different
    equity path from the one actually emitted.
    """
    monkeypatch.setattr(
        m01,
        "get_mark",
        lambda coin, ts_ms, causal=False: 100.0 if ts_ms <= 1000 else 200.0,
    )
    fills = m01.order_wallet_fills_causally([
        _fill(1500, "BTC", "B", 1, 100, 0, tid=20),
        _fill(1500, "BTC", "B", 2, 100, 1, tid=10),
    ])
    stream = [(f["time"], "fill", f) for f in fills]
    anchor = _Anchor()
    anchor.positions = {"BTC": 10.0}
    # Causal path: buy 3 at 100, mark at 200 -> +$300 equity.
    out = m01.inter_anchor_drift(
        stream, fills, anchor, "0xtest", [(1000, 1000.0), (2000, 1300.0)]
    )
    assert out["n_checks"] == 1
    assert out["max_drift_pct"] == pytest.approx(0.0)

    # Explicit diagnostic ex-post mode remains different and cannot be used to
    # accept the causal artifact.
    leaked = m01.inter_anchor_drift(
        stream, fills, anchor, "0xtest", [(1000, 1000.0), (2000, 1300.0)],
        causal_seed=False,
    )
    assert leaked["max_drift_pct"] > 0.5


def test_daily_causal_walk_folds_carry_when_first_revealed(monkeypatch):
    """Daily M1 must match the per-event bridge's causal carry-in behavior."""
    monkeypatch.setattr(
        m01,
        "get_mark",
        lambda coin, ts_ms, causal=False: 100.0 if ts_ms <= 1000 else 110.0,
    )
    # First observed fill is a trim of a long 10 position held at the anchor.
    fill = _fill(2000, "BTC", "A", 2, 100, 10, tid=1)
    wr = m01.compute_eq_at(
        [(2000, "fill", fill)], [fill], _Anchor(), "0xtest",
        t_ms=3000, anchor_ms=1000, anchor_eq=1000.0, causal_seed=True,
    )
    assert wr.positions["BTC"] == pytest.approx(8.0)
    # Anchor: cash=0, position=10*$100. Trim receives $200; remaining
    # position is worth 8*$110 => equity $1,080.
    assert wr.cash == pytest.approx(200.0)
    assert wr.position_value == pytest.approx(880.0)
    assert wr.equity == pytest.approx(1080.0)


def test_segment_reconcile_does_not_double_count_closed_pnl(monkeypatch):
    monkeypatch.setattr(m01, "get_mark", lambda coin, ts_ms, causal=False: 100.0)
    fills = [
        _fill(1200, "BTC", "B", 1, 100, 0, tid=1, closed=0.0),
        _fill(1800, "BTC", "A", 1, 110, 1, tid=2, closed=10.0),
    ]
    stream = [(f["time"], "fill", f) for f in fills]
    out = m01.segment_reconcile(
        stream, fills, _Anchor(), "0xtest",
        [(1000, 1000.0), (2000, 1010.0)], fills,
        causal_seed=True,
    )
    assert out["n_segments"] == 1
    assert out["max_err_usd"] == pytest.approx(0.0)


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


def test_same_ms_tid_is_identity_not_causal_order():
    # Real S3 pattern: numerical tid order differs from position evolution.
    # Correct chain: -573 -> -1428 -> -3011 -> -3584.
    fills = [
        _fill(3000, "BTC", "A", 855, 100, -573, tid=10, dir_="Open Short"),
        _fill(3000, "BTC", "A", 573, 100, -3011, tid=20, dir_="Open Short"),
        _fill(3000, "BTC", "A", 1583, 100, -1428, tid=30, dir_="Open Short"),
    ]
    ordered = m01.order_wallet_fills_causally(fills)
    assert [f["startPosition"] for f in ordered] == [-573.0, -1428.0, -3011.0]
    stream = m01.build_event_stream(fills, [], [])
    assert [e["ev"]["startPosition"] for e in stream] == [-573.0, -1428.0, -3011.0]
    assert all(e["ev"]["causal_order_ok"] for e in stream)


def test_position_gap_resyncs_and_invalidates_interrupted_journey():
    fills = [
        _fill(2000, "BTC", "B", 1, 100, 0, tid=1, dir_="Open Long"),
        # Missing fills moved the real position from 1 to 5 before this trim.
        _fill(3000, "BTC", "A", 1, 100, 5, tid=2, dir_="Close Long"),
        _fill(4000, "BTC", "A", 4, 100, 4, tid=3, dir_="Close Long"),
    ]
    _, actions, journeys = _run(fills, anchors=[(1000, 1000.0)])
    assert actions[1]["state_resynced"] is True
    assert actions[1]["position_after"] == pytest.approx(4.0)
    assert actions[1]["action_type"] == "TRIM"
    assert any(not j["lifecycle_valid"] and j["state_discontinuity"] for j in journeys)
    # The gap is learned after action 0 was emitted, so validity must propagate
    # backward to the entire interrupted journey.
    assert actions[0]["lifecycle_valid"] is False
    # The current transition can seed a new historical journey from its
    # authoritative startPosition, but a raw live stream cannot self-repair.
    assert actions[1]["lifecycle_valid"] is True
    assert actions[1]["stream_replay_valid"] is False
    assert actions[2]["lifecycle_valid"] is True
    assert actions[2]["stream_replay_valid"] is False
    assert all("stream_replay_valid" in j for j in journeys)
    assert not any(j["stream_replay_valid"] for j in journeys)


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


def test_spot_fill_cannot_create_perp_equity_or_ranking_action():
    """M1/M2/M7 are a perp-copy chain. A spot buy must be absent from both
    reconstructed perp equity and wallet-ranking actions.

    Regression: the whole-account experiment added the spot asset position but
    charged zero cash, turning a 10 @ $5 buy into a fake +$50 equity gain.
    """
    fill = _fill(3000, "@107", "B", 10, 5, 0, tid=1, dir_="Buy")
    fill["is_spot"] = True
    events, actions, journeys = _run([fill], anchors=[(2000, 1000.0)])

    assert events == []
    assert actions == []
    assert journeys == []


def test_wallet_fill_loader_excludes_spot_fast_path(tmp_path, monkeypatch):
    wallet = "0x" + "b" * 40
    rows = [
        _fill(3000, "@107", "B", 10, 5, 0, tid=1, dir_="Buy"),
        _fill(4000, "BTC", "B", 0.01, 100_000, 0, tid=2, dir_="Open Long"),
    ]
    pd.DataFrame(rows).to_parquet(tmp_path / f"{wallet}.parquet", index=False)
    monkeypatch.setattr(m01, "S3_BY_WALLET_DIR", tmp_path)

    loaded = m01.load_wallet_fills(wallet, 0, 10_000)

    assert [fill["coin"] for fill in loaded] == ["BTC"]


def test_perp_ledger_transfer_scope_is_flow_neutralized():
    wallet = "0x" + "a" * 40
    into_perp = {"delta": {"type": "accountClassTransfer", "usdc": "250", "toPerp": True}}
    out_of_perp = {"delta": {"type": "accountClassTransfer", "usdc": "250", "toPerp": False}}
    spot_only = {"delta": {"type": "spotTransfer", "usdc": "250"}}

    assert m01.ledger_cash_delta(into_perp, wallet).cash == 250.0
    assert m01.ledger_cash_delta(into_perp, wallet).ext_flow == 250.0
    assert m01.ledger_cash_delta(out_of_perp, wallet).cash == -250.0
    assert m01.ledger_cash_delta(out_of_perp, wallet).ext_flow == -250.0
    assert m01.ledger_cash_delta(spot_only, wallet).cash == 0.0


def test_non_usdc_hip3_collateral_send_uses_usdc_value():
    wallet = "0xtest"
    into_flx = {"delta": {
        "type": "send", "user": wallet, "destination": wallet,
        "sourceDex": "spot", "destinationDex": "flx", "token": "USDH",
        "amount": "100", "usdcValue": "99.5", "fee": "0",
    }}
    out_of_cash = {"delta": {
        "type": "send", "user": wallet, "destination": wallet,
        "sourceDex": "cash", "destinationDex": "spot", "token": "USDT0",
        "amount": "50", "usdcValue": "50.1", "fee": "0",
    }}
    spot_only = {"delta": {
        "type": "send", "user": wallet, "destination": "0xother",
        "sourceDex": "spot", "destinationDex": "spot", "token": "HYPE",
        "amount": "10", "usdcValue": "400", "fee": "0",
    }}
    assert m01.ledger_cash_delta(into_flx, wallet).cash == pytest.approx(99.5)
    assert m01.ledger_cash_delta(into_flx, wallet).ext_flow == pytest.approx(99.5)
    assert m01.ledger_cash_delta(out_of_cash, wallet).cash == pytest.approx(-50.1)
    assert m01.ledger_cash_delta(spot_only, wallet).cash == 0.0


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
