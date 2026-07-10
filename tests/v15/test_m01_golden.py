"""GOLDEN regression test for v15_m01_equity_reconstruct trust-audit fixes (2026-07-10).

m01 is the causal-equity FOUNDATION consumed by the (trusted) m02. Codex confirmed it is look-ahead-safe;
these lock the fail-CLOSED fixes so an untrustworthy denominator can NEVER be emitted as a clean anchored
equity (M02 must see has_past_anchor=False -> NO_ANCHOR -> unsizeable):
- P1a: a seeded/carry-in position UNMARKABLE at the anchor time (cash snap incomplete) -> degraded.
- P1b: an UNKNOWN ledger type at/before an event (its real cash move applied as 0) -> events at/after tainted.
- P2:  a non-finite (inf/NaN) mark or cash delta -> degraded (never a finite-looking anchored equity).
"""
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m01_equity_reconstruct as m01  # noqa: E402
from test_m02 import _fill  # noqa: E402   reuse the fill builder


class _Anchor:
    entry_px: dict = {}
    fetched_ms = 2000            # near the 1000 anchor (< 1 day) -> near_fetch seeding
    dexes_seen = {"main"}
    has_flx_anchor = False
    has_unmarkable_dex_anchor = False
    acct_value_by_dex = {"main": 1000.0}
    aggregate_acct_value = 1000.0

    def __init__(self, positions=None):
        self.positions = positions or {}


def _events(fills, anchor, ledger=None, funding=None, window_anchors=None):
    ordered = m01.build_event_stream(fills, funding or [], ledger or [])
    return m01.compute_event_equity(ordered, fills, anchor, "0xtest",
                                    window_anchors or [(1000, 1000.0)], extradex_no_anchor=False)


def test_p1a_seeded_unmarkable_at_anchor_fails_closed(monkeypatch):
    # anchor seeds an exotic ALT that has NO mark at the anchor -> cash snap incomplete -> denominator
    # unreliable -> the BTC event must be degraded (has_past_anchor=False), NOT a clean anchored equity.
    monkeypatch.setattr(m01, "get_mark", lambda c, ts, causal=False: (100.0 if c == "BTC" else None))
    fills = [_fill(2000, "BTC", "B", 1, 100, 0, tid=1)]
    ev = _events(fills, _Anchor(positions={"ALT": 1.0}))
    btc = [e for e in ev if e.type == "fill"][0]
    assert btc.has_past_anchor is False and btc.markable_all is False
    assert not (btc.equity_post == btc.equity_post)   # NaN


def test_p1a_clean_anchor_still_anchored(monkeypatch):
    # control: everything markable at the anchor -> normal anchored equity.
    monkeypatch.setattr(m01, "get_mark", lambda c, ts, causal=False: 100.0)
    fills = [_fill(2000, "BTC", "B", 1, 100, 0, tid=1)]
    ev = _events(fills, _Anchor())
    btc = [e for e in ev if e.type == "fill"][0]
    assert btc.has_past_anchor is True and btc.markable_all is True
    assert np.isfinite(btc.equity_post)


def test_p1b_unknown_ledger_taints_subsequent_events(monkeypatch):
    monkeypatch.setattr(m01, "get_mark", lambda c, ts, causal=False: 100.0)
    fills = [_fill(1500, "BTC", "B", 1, 100, 0, tid=1),         # BEFORE the unknown ledger -> clean
             _fill(3000, "BTC", "B", 1, 100, 0, tid=2)]         # AFTER -> tainted
    ledger = [{"time": 2500, "hash": "h", "delta": {"type": "totallyNewLedgerType", "usdc": 5000}}]
    ev = _events(fills, _Anchor(), ledger=ledger)
    fills_ev = [e for e in ev if e.type == "fill"]
    assert fills_ev[0].has_past_anchor is True                 # pre-unknown-ledger fill stays clean
    assert fills_ev[1].has_past_anchor is False                # post-unknown-ledger fill fails closed


def test_p2_inf_mark_fails_closed(monkeypatch):
    monkeypatch.setattr(m01, "get_mark", lambda c, ts, causal=False: float("inf"))
    fills = [_fill(2000, "BTC", "B", 1, 100, 0, tid=1)]
    ev = _events(fills, _Anchor())
    btc = [e for e in ev if e.type == "fill"][0]
    assert btc.has_past_anchor is False and not (btc.equity_post == btc.equity_post)


def test_p2_nan_funding_fails_closed(monkeypatch):
    monkeypatch.setattr(m01, "get_mark", lambda c, ts, causal=False: 100.0)
    fills = [_fill(2000, "BTC", "B", 1, 100, 0, tid=1)]
    funding = [{"time": 2500, "coin": "BTC", "delta": {"type": "funding", "coin": "BTC", "usdc": float("nan")}}]
    ev = _events(fills, _Anchor(), funding=funding)
    # the funding event (and anything after) has non-finite cash -> degraded
    fund_ev = [e for e in ev if e.type == "funding"]
    assert fund_ev and fund_ev[0].has_past_anchor is False
