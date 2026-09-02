"""GOLDEN regression test for v15_m02_journey_trace trust-audit fixes (2026-07-10).

Locks the fail-CLOSED / accounting fixes from the codex m02 audit:
- P1 open-at-window-end: an open journey finalizes through END_MS (real duration + funding), not peak_ts (0).
- P1 non-finite startPosition/size/price: fails CLOSED (lifecycle_valid=False, carry_in UNKNOWN, not a
  fabricated clean flat SEEDED).
- P1 NaN/inf mark (or equity): does NOT survive into target_exposure_pct -> unsizeable (None), mark None.
"""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m02_journey_trace as m02  # noqa: E402
import v15_m01_equity_reconstruct as m01  # noqa: E402
from v15_m02_journey_trace import LifecycleFillEvent  # noqa: E402
from test_m02 import _fill  # noqa: E402


def _core_events(fills):
    return [LifecycleFillEvent(ts=int(f["time"]), event_order=i, fill=f) for i, f in enumerate(fills)]


def test_p1_open_journey_finalizes_at_window_end():
    # one open BTC long at ts=1000; funding +5 at t=2000; window end 3000. Journey stays open (exit_ts None)
    # but duration + funding must run to 3000, not collapse to peak_ts (0 duration / 0 funding).
    fills = [_fill(1000, "BTC", "B", 1, 100, 0, tid=1)]
    funding = [{"time": 2000, "delta": {"type": "funding", "coin": "BTC", "usdc": 5.0}}]
    _a, journeys = m02.trace_wallet("0x", _core_events(fills), fills, funding,
                                    equity_enriched=False, end_ms=3000)
    assert len(journeys) == 1
    j = journeys[0]
    assert j["open_at_window_end"] is True and j["exit_ts"] is None
    assert j["duration_h"] > 0.0                       # (3000-1000)/3600e3, NOT 0
    assert abs(j["funding_net"] - 5.0) < 1e-9          # funding through window end included


def test_p1_open_journey_without_end_ms_is_backward_compatible():
    # end_ms omitted -> old peak_ts behavior (byte-identical for existing callers).
    fills = [_fill(1000, "BTC", "B", 1, 100, 0, tid=1)]
    _a, journeys = m02.trace_wallet("0x", _core_events(fills), fills, [], equity_enriched=False)
    assert journeys[0]["duration_h"] == 0.0            # peak_ts == open_ts -> 0 (unchanged)


def test_p1_nonfinite_startposition_fails_closed():
    f = _fill(1000, "BTC", "A", 1, 100, 0, tid=1, closed=10.0)
    f["startPosition"] = float("nan")                  # unknown pre-position
    actions, _j = m02.trace_wallet("0x", _core_events([f]), [f], [], equity_enriched=False)
    assert len(actions) == 1
    assert actions[0]["lifecycle_valid"] is False
    assert actions[0]["stream_replay_valid"] is False
    assert actions[0]["carry_in_status"] == "UNKNOWN"


def test_p1_nonfinite_price_dropped():
    # codex r3: a non-finite price poisons notional/cost_basis/fee -> DROP the fill (no action), coin invalid.
    bad = _fill(1000, "BTC", "B", 1, 100, 0, tid=1); bad["price"] = float("inf")
    good = _fill(2000, "BTC", "B", 1, 100, 0, tid=2)
    actions, _j = m02.trace_wallet("0x", _core_events([bad, good]), [bad, good], [], equity_enriched=False)
    assert all(np.isfinite(a["price"]) for a in actions)
    assert len(actions) == 1 and actions[0]["ts"] == 2000
    assert actions[0]["lifecycle_valid"] is False and actions[0]["stream_replay_valid"] is False


def test_p1_inf_fee_does_not_flow_into_accounting():
    # codex r3: an inf reported fee must fall back to the computed fee, never inf into fees_paid.
    o = _fill(1000, "BTC", "B", 1, 100, 0, tid=1)
    c = _fill(2000, "BTC", "A", 1, 110, 1, tid=2, closed=10.0)
    c["fee"] = float("inf")
    _a, journeys = m02.trace_wallet("0x", _core_events([o, c]), [o, c], [], equity_enriched=False)
    assert len(journeys) == 1
    assert np.isfinite(journeys[0]["fees"]) and np.isfinite(journeys[0]["net_realized_pnl"])


def test_p1_inf_closedpnl_invalidates_and_stays_finite():
    # codex r3: an inf reported closedPnl -> derived (finite) + lifecycle invalidated.
    o = _fill(1000, "BTC", "B", 1, 100, 0, tid=1)
    c = _fill(2000, "BTC", "A", 1, 110, 1, tid=2)
    c["closedPnl"] = float("inf")
    _a, journeys = m02.trace_wallet("0x", _core_events([o, c]), [o, c], [], equity_enriched=False)
    assert len(journeys) == 1
    assert np.isfinite(journeys[0]["realized_pnl"]) and np.isfinite(journeys[0]["net_realized_pnl"])
    assert journeys[0]["lifecycle_valid"] is False


def test_p1_nonfinite_signed_size_dropped_no_crash():
    # codex r2: a NaN size poisoned new_pos and crashed the REVERSE branch (KeyError). Now it is dropped
    # (no action emitted) and does not raise; a following good fill on the same coin is lifecycle-invalid.
    bad = _fill(1000, "BTC", "B", 1, 100, 0, tid=1)
    bad["signed_sz"] = float("nan")
    good = _fill(2000, "BTC", "B", 1, 100, 0, tid=2)
    actions, journeys = m02.trace_wallet("0x", _core_events([bad, good]), [bad, good], [],
                                         equity_enriched=False)
    # the NaN-size fill produced no action row; only the good fill did
    assert all(np.isfinite(a["signed_size"]) for a in actions)
    assert len(actions) == 1 and actions[0]["ts"] == 2000
    # the coin lifecycle is poisoned -> the good fill's journey/action is fail-closed
    assert actions[0]["lifecycle_valid"] is False and actions[0]["stream_replay_valid"] is False


def test_p1_inf_funding_does_not_flow():
    # codex r4: an inf funding usdc must not make funding_net / net_realized_pnl non-finite.
    o = _fill(1000, "BTC", "B", 1, 100, 0, tid=1)
    c = _fill(5000, "BTC", "A", 1, 110, 1, tid=2, closed=10.0)
    funding = [{"time": 3000, "delta": {"type": "funding", "coin": "BTC", "usdc": float("inf")}}]
    _a, journeys = m02.trace_wallet("0x", _core_events([o, c]), [o, c], funding, equity_enriched=False)
    assert len(journeys) == 1
    assert np.isfinite(journeys[0]["funding_net"]) and np.isfinite(journeys[0]["net_realized_pnl"])


def test_p1_string_inf_closedpnl_invalidates():
    # codex r4: a string "inf" closedPnl (direct caller) is coerced + caught -> derived + lifecycle invalid.
    o = _fill(1000, "BTC", "B", 1, 100, 0, tid=1)
    c = _fill(2000, "BTC", "A", 1, 110, 1, tid=2)
    c["closedPnl"] = "inf"
    _a, journeys = m02.trace_wallet("0x", _core_events([o, c]), [o, c], [], equity_enriched=False)
    assert len(journeys) == 1
    assert np.isfinite(journeys[0]["realized_pnl"]) and journeys[0]["lifecycle_valid"] is False


class _EE:
    """Minimal EventEquity stand-in: anchored, finite equity, fully markable."""
    type = "fill"
    event_order = 0
    ts = 120_000
    has_past_anchor = True
    equity_post = 1000.0
    frozen_component_value = 0.0
    frozen_component_age_ms = 0
    age_since_anchor_ms = 60_000
    anchor_equity = 1000.0
    anchor_ts = 60_000
    mark_ts = 60_000
    n_unmarkable = 0
    markable_all = True
    no_extradex_without_anchor = True

    def __init__(self, fill):
        self.fill = fill


def test_p1_nonfinite_mark_is_unsizeable(monkeypatch):
    f = _fill(120_000, "BTC", "B", 1, 100, 0, tid=1)
    for bad in (float("nan"), float("inf")):
        monkeypatch.setattr(m01, "get_mark", lambda *a, **k: bad)
        actions, _j = m02.trace_wallet("0x", [_EE(f)], [f], [], equity_enriched=True)
        assert actions[0]["target_exposure_pct"] is None   # NaN/inf mark never becomes a target
        assert actions[0]["mark"] is None


class _EE_bad_equity(_EE):
    equity_post = float("inf")          # M01 emitted a non-finite equity
    anchor_equity = float("inf")


def test_p1_nonfinite_source_equity_nulled(monkeypatch):
    # codex r5: a non-finite source_equity_post from M01 must be nulled in the action row (never a trusted
    # non-finite equity), and the action is unsizeable.
    f = _fill(120_000, "BTC", "B", 1, 100, 0, tid=1)
    monkeypatch.setattr(m01, "get_mark", lambda *a, **k: 100.0)
    actions, _j = m02.trace_wallet("0x", [_EE_bad_equity(f)], [f], [], equity_enriched=True)
    assert actions[0]["source_equity_post"] is None
    assert actions[0]["target_exposure_pct"] is None


def test_p1_finite_mark_still_sizes(monkeypatch):
    f = _fill(120_000, "BTC", "B", 1, 100, 0, tid=1)
    monkeypatch.setattr(m01, "get_mark", lambda *a, **k: 100.0)
    actions, _j = m02.trace_wallet("0x", [_EE(f)], [f], [], equity_enriched=True)
    # position_after=1, mark=100, equity=1000 -> target 0.1
    assert actions[0]["target_exposure_pct"] is not None
    assert abs(actions[0]["target_exposure_pct"] - 0.1) < 1e-9
