"""Exit parity: LEADER_FLAT reproduces the exit rule the V15 backtest actually measured.

Card: card/quant-engineer/engine-parity-fixes Step 1 (2026-07-31).

WHAT IS BEING PROVED. The +10.24 bps OOS cohort was scored by two things, and both end a position on
LEADER-FLAT-OR-FLIPPED, never on a partial reduce:

  * research/v15/forward_oos_hot.py roundtrips_boundary() -- emits a round trip only when
    `closed_or_reversed`; an opposite-side partial accumulates into ex/exs and the trip KEEPS RUNNING.
  * research/v15/v15_m07_engine.py:_action_target_pct() under sizing_mode="fixed_position" (the mode in
    the cohort_recent12 manifest) -- target is sign(position_after) * fixed_target_exposure, and is zero
    ONLY when |position_after| == 0.

The live engine's FIRST_CLOSE rule instead fires on
    leader_reverse_notional / OUR_accumulated_notional >= exit_min_trim_pct
which mixes scales, so at a large leader/follower size mismatch it exits on the leader's FIRST trim.

ARCHITECTURE THESE TESTS PIN (codex r2): the decision function does NO I/O. _prefetch_leader_snapshots
refreshes ONE clearinghouseState per (wallet, dex) off the event loop, and _leader_flat_or_flipped is a
pure read of that snapshot. A snapshot is ONE observation no matter how many times the sweep reads it --
that is what makes "2 confirmations" mean two REST responses rather than one response counted twice.

The logic is exercised on an uninitialised instance: __init__ opens sockets, loads configs and hits the
exchange, none of which these decisions depend on.
"""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "strategies" / "live"))

W, C = "0xleader", "BTC"
MARK = 100.0          # $100/unit -> leader 20 units = $2,000; our 0.25 units = $25 (the 80x mismatch)


def _engine(confirms=1, grace_s=0.0, flat_usd=10.0):
    """An instance carrying ONLY the state the exit decision reads."""
    import hl_copy_trader_v17 as mod
    eng = object.__new__(mod.V17CopyTrader)   # the live class; helper is inherited from CopyTrader
    eng._leader_snapshot = {}
    eng._leader_flat_decision = {}
    eng._leader_flat_confirms = {}
    eng._leader_flat_outage = {}
    eng._leader_flat_outage_alerted = {}
    eng._target_positions = {}
    eng.global_config = {"leader_flat_notional_usd": flat_usd, "leader_flat_poll_s": 10.0,
                         "leader_flat_confirms": confirms, "leader_flat_min_age_s": grace_s}
    eng.mid_prices = {}
    eng._ts = 1000.0
    return eng


def _observe(eng, szi, wallet=W, dex=""):
    """Publish a NEW snapshot = one fresh REST observation. szi None => unusable response (UNKNOWN)."""
    eng._ts += 1.0
    eng._leader_snapshot[(wallet, dex)] = (eng._ts, None if szi is None else {C: szi})


def _decide(eng, side="BUY", mark=MARK, entry_ts=0.0):
    return eng._leader_flat_or_flipped(W, C, side, mark, entry_ts=entry_ts)


# ── 1. The measured rule: hold through partials, exit only at flat / flip ──────────────────────────

def test_holds_while_leader_still_holds_after_a_large_partial():
    """Leader trims $300 of a $2,000 position (17 units left). m07 target is still sign*fixed -> HOLD."""
    eng = _engine(); _observe(eng, 17.0)
    assert _decide(eng) is False


def test_exits_when_leader_is_flat():
    eng = _engine(); _observe(eng, 0.0)
    assert _decide(eng) is True


def test_exits_when_the_coin_is_absent_from_a_usable_snapshot():
    """A usable body that simply does not list the coin IS a flat account for that coin."""
    eng = _engine()
    eng._ts += 1
    eng._leader_snapshot[(W, "")] = (eng._ts, {"ETH": 3.0})
    assert _decide(eng) is True


def test_exits_when_leader_flips_side():
    """forward_oos_hot ends the trip on close OR reverse -- a flip must end ours too."""
    eng = _engine(); _observe(eng, -5.0)
    assert _decide(eng, side="BUY") is True
    eng2 = _engine(); _observe(eng2, +5.0)
    assert _decide(eng2, side="SELL") is True


def test_dust_residue_counts_as_flat():
    """0.05 units * $100 = $5 < $10 min order: not a position we could have mirrored."""
    eng = _engine(); _observe(eng, 0.05)
    assert _decide(eng) is True


def test_just_above_the_dust_floor_is_still_a_position():
    eng = _engine(); _observe(eng, 0.15)          # $15 > $10
    assert _decide(eng) is False


# ── 2. The divergence this card exists to fix ──────────────────────────────────────────────────────

def test_first_close_would_have_exited_where_m07_holds():
    """The worked example from the card, at an 80x notional mismatch.

    Leader: $2,000 position, takes a $300 partial (15% of THEIR book -- they are still 85% in).
    Us: $25 accumulated.
      FIRST_CLOSE  -> trim_pct = min(300/25, 1.0) = 1.0 >= 0.85 -> FULL EXIT.
      m07          -> position_after = 17 units != 0 -> target unchanged -> HOLD.
      LEADER_FLAT  -> HOLD.
    The two live rules disagree on the SAME event; LEADER_FLAT is the one that matches the backtest.
    """
    leader_reverse_notional, our_accumulated = 300.0, 25.0
    first_close_trim_pct = min(leader_reverse_notional / our_accumulated, 1.0)
    assert first_close_trim_pct >= 0.85                       # FIRST_CLOSE exits

    eng = _engine(); _observe(eng, 17.0)                      # $2000 - $300 = $1700 left
    assert _decide(eng) is False                              # LEADER_FLAT holds


def test_the_two_rules_agree_once_the_leader_is_actually_out():
    """Parity is not "never exit" -- when the leader IS flat both rules close."""
    assert min(2000.0 / 25.0, 1.0) >= 0.85
    eng = _engine(); _observe(eng, 0.0)
    assert _decide(eng) is True


# ── 3. Unknown must never mean "exit" ──────────────────────────────────────────────────────────────

def test_an_unusable_snapshot_is_unknown_and_holds():
    """codex P1: the first version fell back to the WS tracker, so a stale ZERO or a stale OPPOSITE
    value would return True and close a live position. Only exchange truth may authorize a close."""
    eng = _engine()
    eng._target_positions = {W: {C: 0.0}}         # tracker says flat...
    _observe(eng, None)                           # ...but the exchange read is unusable
    assert _decide(eng) is None


def test_a_stale_opposite_tracker_value_cannot_trigger_a_flip_exit():
    eng = _engine()
    eng._target_positions = {W: {C: -8.0}}
    _observe(eng, None)
    assert _decide(eng) is None


def test_no_snapshot_at_all_is_unknown():
    eng = _engine()
    assert _decide(eng) is None


def test_a_dust_judgement_still_requires_a_mark():
    """0.05 units is only 'flat' relative to a price. With no usable mark this must stay UNKNOWN."""
    eng = _engine(); _observe(eng, 0.05)
    assert _decide(eng, mark=0.0) is None


def test_exact_zero_does_not_need_a_mark():
    """An unambiguous flat reading must not be discarded because the mark is missing -- that would
    hold past the leader's exit. Only the dust floor needs a price (codex r2 P2)."""
    eng = _engine(); _observe(eng, 0.0)
    assert _decide(eng, mark=0.0) is True


def test_sign_reversal_does_not_need_a_mark():
    eng = _engine(); _observe(eng, -4.0)
    assert _decide(eng, mark=None) is True


def test_a_live_position_with_an_unusable_mark_is_unknown_not_flat():
    eng = _engine(); _observe(eng, 17.0)
    assert _decide(eng, mark=0.0) is None


# ── 4. Grace + confirmations: the two protections copied from the engine's own leader sweep ────────

def test_a_fresh_leg_is_not_closed_before_the_grace_window():
    """Our fill can complete BEFORE the leader's position shows up in REST. Without the grace, a
    brand-new leg reads as 'leader never had it' and self-closes instantly (codex P1)."""
    import time as _t
    eng = _engine(grace_s=90.0); _observe(eng, 0.0)
    assert _decide(eng, entry_ts=_t.time()) is False


def test_after_the_grace_window_a_flat_leader_does_close_the_leg():
    import time as _t
    eng = _engine(grace_s=90.0); _observe(eng, 0.0)
    assert _decide(eng, entry_ts=_t.time() - 600) is True


def test_one_flat_snapshot_is_not_enough_when_two_confirms_are_required():
    eng = _engine(confirms=2)
    _observe(eng, 0.0); assert _decide(eng) is False
    _observe(eng, 0.0); assert _decide(eng) is True


def test_confirmations_require_FRESH_snapshots_not_repeated_reads():
    """codex r2 P1, and he proved it by probing: the first version incremented the streak on every
    ~1Hz sweep while ONE cached REST response was re-served, so "2 confirms" was one observation read
    twice. Re-reading the SAME snapshot must never manufacture a confirmation."""
    eng = _engine(confirms=2)
    _observe(eng, 0.0)
    decisions = [_decide(eng) for _ in range(6)]
    assert decisions == [False] * 6, f"one snapshot manufactured a confirmation: {decisions}"


def test_a_still_open_reading_resets_the_confirmation_streak():
    eng = _engine(confirms=2)
    _observe(eng, 0.0);  assert _decide(eng) is False    # strike 1
    _observe(eng, 17.0); assert _decide(eng) is False    # leader still in -> streak broken
    _observe(eng, 0.0);  assert _decide(eng) is False    # strike 1 again, not 2
    _observe(eng, 0.0);  assert _decide(eng) is True


def test_an_unknown_reading_breaks_the_confirmation_streak():
    """An outage in the middle of a streak must not count as a confirmation."""
    eng = _engine(confirms=2)
    _observe(eng, 0.0);  assert _decide(eng) is False
    _observe(eng, None); assert _decide(eng) is None
    _observe(eng, 0.0);  assert _decide(eng) is False    # back to strike 1


def test_a_strike_cannot_be_inherited_by_the_next_leg_on_the_same_coin():
    """Streaks are keyed by leg (wallet, coin, entry_ts). SL / trailing / max-hold / WS retirement all
    retire a leg without running the LEADER_FLAT cleanup, so a surviving strike would let the NEXT leg
    close on its first observation (codex r2 P1)."""
    eng = _engine(confirms=2)
    old_leg, new_leg = 1000.0, 2000.0
    _observe(eng, 0.0); assert _decide(eng, entry_ts=old_leg) is False    # strike 1 on the OLD leg
    # old leg retires by some other path; a NEW leg opens on the same coin
    _observe(eng, 0.0); assert _decide(eng, entry_ts=new_leg) is False    # must be strike 1, not 2
    _observe(eng, 0.0); assert _decide(eng, entry_ts=new_leg) is True


# ── 5. The snapshot fetch: "flat" must mean flat, never "the response was unusable" ────────────────
# codex P1: the old per-coin read did `data.get("assetPositions", [])` and returned 0.0 on miss, so a
# 429 body / 5xx / transient {} all reported FLAT -- which under LEADER_FLAT authorizes a real close.

class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def _snapshot_with(monkeypatch, status, payload):
    import hl_copy_trader_v17 as mod
    eng = object.__new__(mod.V17CopyTrader)
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: _Resp(status, payload))
    return mod.CopyTrader._snapshot_leader_positions(eng, W, "")


def test_rate_limited_snapshot_is_unknown_not_flat(monkeypatch):
    assert _snapshot_with(monkeypatch, 429, {"error": "rate limited"}) is None


def test_server_error_snapshot_is_unknown_not_flat(monkeypatch):
    assert _snapshot_with(monkeypatch, 503, {}) is None


def test_empty_object_snapshot_is_unknown_not_flat(monkeypatch):
    assert _snapshot_with(monkeypatch, 200, {}) is None


def test_non_dict_snapshot_body_is_unknown_not_flat(monkeypatch):
    assert _snapshot_with(monkeypatch, 200, []) is None


def test_present_but_empty_asset_positions_is_a_genuine_flat(monkeypatch):
    """The one case that legitimately means flat: the key IS there and the account holds nothing."""
    assert _snapshot_with(monkeypatch, 200, {"assetPositions": []}) == {}


def test_snapshot_returns_every_coin_in_one_call(monkeypatch):
    payload = {"assetPositions": [{"position": {"coin": "BTC", "szi": "-2.5"}},
                                  {"position": {"coin": "ETH", "szi": "3.0"}}]}
    assert _snapshot_with(monkeypatch, 200, payload) == {"BTC": -2.5, "ETH": 3.0}


# ── 6. The same hardening on the per-coin reader still used by _refresh_target_position ────────────

def _query_with(monkeypatch, status, payload):
    import hl_copy_trader_v17 as mod
    eng = object.__new__(mod.V17CopyTrader)
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: _Resp(status, payload))
    return mod.CopyTrader._query_target_position(eng, W, C)


def test_query_rate_limited_is_unknown_not_flat(monkeypatch):
    assert _query_with(monkeypatch, 429, {"error": "rate limited"}) is None


def test_query_empty_object_is_unknown_not_flat(monkeypatch):
    assert _query_with(monkeypatch, 200, {}) is None


def test_query_present_but_empty_is_a_genuine_flat(monkeypatch):
    assert _query_with(monkeypatch, 200, {"assetPositions": []}) == 0.0


def test_query_returns_signed_size(monkeypatch):
    payload = {"assetPositions": [{"position": {"coin": C, "szi": "-2.5"}}]}
    assert _query_with(monkeypatch, 200, payload) == -2.5


# ── 7. Prefetch: one request per (wallet, dex), off-loop, failures do not poison the tracker ───────

def test_prefetch_issues_one_request_per_wallet_dex_and_reuses_within_the_poll_window():
    """codex r2 P1: the old path made one BLOCKING 5s request per (wallet, COIN), inline with WS
    ingestion. clearinghouseState already returns the whole book, so per-coin polling was redundant."""
    import asyncio
    eng = _engine()
    calls = []

    def _snap(addr, dex):
        calls.append((addr, dex))
        return {"BTC": 1.0, "ETH": 2.0}

    eng._snapshot_leader_positions = _snap
    keys = {(W, ""), ("0xother", "")}
    asyncio.run(eng._prefetch_leader_snapshots(keys))
    assert sorted(calls) == sorted(keys)
    asyncio.run(eng._prefetch_leader_snapshots(keys))     # still inside poll_s -> no new requests
    assert sorted(calls) == sorted(keys)
    assert eng._target_positions[W]["BTC"] == 1.0         # ground truth flows into the shared tracker


def test_prefetch_records_unknown_without_touching_the_tracker():
    import asyncio
    eng = _engine()
    eng._target_positions = {W: {C: 5.0}}
    eng._snapshot_leader_positions = lambda a, d: None
    asyncio.run(eng._prefetch_leader_snapshots({(W, "")}))
    assert eng._leader_snapshot[(W, "")][1] is None
    assert eng._target_positions[W][C] == 5.0             # untouched, not zeroed
    assert _decide(eng) is None                           # and the decision holds


def test_a_raising_snapshot_call_is_unknown_not_flat():
    """One wallet's exception must not become 'flat', and must not take down the other wallets."""
    import asyncio
    eng = _engine()

    def _snap(addr, dex):
        if addr == W:
            raise RuntimeError("boom")
        return {"BTC": 4.0}

    eng._snapshot_leader_positions = _snap
    asyncio.run(eng._prefetch_leader_snapshots({(W, ""), ("0xok", "")}))
    assert eng._leader_snapshot[(W, "")][1] is None
    assert eng._leader_snapshot[("0xok", "")][1] == {"BTC": 4.0}
    assert _decide(eng) is None


# ── 8. Defects codex REPRODUCED in round 3 ────────────────────────────────────────────────────────

def test_two_coexisting_legs_on_one_coin_do_not_share_a_decision():
    """codex r3 P2, reproduced by him: the "already counted" memo was keyed by (wallet, coin) while the
    streak was keyed by leg. An older leg reaching its 2nd confirmation wrote a True into the coin-level
    memo, and a YOUNGER leg with only one confirmation then read that True and closed early."""
    eng = _engine(confirms=2)
    old_leg, new_leg = 1000.0, 2000.0
    _observe(eng, 0.0)
    assert _decide(eng, entry_ts=old_leg) is False      # old leg: strike 1
    assert _decide(eng, entry_ts=new_leg) is False      # new leg: strike 1 off the SAME snapshot
    _observe(eng, 0.0)
    assert _decide(eng, entry_ts=old_leg) is True       # old leg: strike 2 -> exits
    assert _decide(eng, entry_ts=new_leg) is True       # new leg: strike 2 -> its own decision
    # and a THIRD leg that has seen only one observation must still hold
    assert _decide(eng, entry_ts=3000.0) is False


def test_a_row_missing_szi_invalidates_the_whole_snapshot(monkeypatch):
    """codex r3 P2: `float(pos.get("szi", 0.0))` turned a malformed row into an assertion of flatness,
    which authorizes a close. A partially-understood book says nothing about the coins absent from it."""
    payload = {"assetPositions": [{"position": {"coin": "BTC"}}]}
    assert _snapshot_with(monkeypatch, 200, payload) is None


def test_a_non_finite_szi_invalidates_the_whole_snapshot(monkeypatch):
    """A NaN size would otherwise fail every comparison and could read as a sign flip."""
    for bad in ("NaN", "Infinity", "-Infinity"):
        payload = {"assetPositions": [{"position": {"coin": "BTC", "szi": bad}}]}
        assert _snapshot_with(monkeypatch, 200, payload) is None, bad


def test_an_unparseable_szi_invalidates_the_whole_snapshot(monkeypatch):
    payload = {"assetPositions": [{"position": {"coin": "BTC", "szi": "abc"}}]}
    assert _snapshot_with(monkeypatch, 200, payload) is None


def test_leg_scoped_state_is_pruned_against_live_legs():
    """codex r3 P2: these maps are keyed by leg and grow once per leg forever in a process meant to run
    for weeks. The per-exit cleanup cannot catch legs retired by SL / trailing / max-hold / WS."""
    eng = _engine(confirms=2)
    eng.positions = []
    eng._leader_flat_confirms = {(W, C, 111.0): 1, (W, C, 222.0): 1}
    eng._leader_flat_decision = {(W, C, 111.0): (5.0, False)}
    live = {(p.get("wallet", ""), p["coin"],
             round(float(p.get('fill_time') or p.get('entry_time') or 0.0), 3))
            for p in eng.positions if p.get("filled")}
    for m in (eng._leader_flat_confirms, eng._leader_flat_decision):
        for k in [k for k in m if k not in live]:
            m.pop(k, None)
    assert eng._leader_flat_confirms == {} and eng._leader_flat_decision == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
