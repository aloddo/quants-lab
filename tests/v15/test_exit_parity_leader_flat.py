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
These tests pin the divergence and pin LEADER_FLAT to the measured rule.

The decision logic is exercised directly on an uninitialised instance: __init__ opens sockets, loads
configs and hits the exchange, none of which this decision depends on.
"""
import sys
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "strategies" / "live"))


def _engine(leader_szi, api_ok=True, flat_usd=10.0, poll_s=10.0, confirms=1, grace_s=0.0):
    """An instance with ONLY the attributes the exit decision reads.

    confirms defaults to 1 here so the single-call tests read the RAW decision. The shipped config
    requires >= 2 (validator-enforced); the two-strike behaviour has its own tests below.
    """
    import hl_copy_trader_v17 as mod
    eng = object.__new__(mod.V17CopyTrader)   # the live class; the helper is inherited from CopyTrader
    eng._leader_flat_cache = {}
    eng._leader_flat_confirms = {}
    eng._leader_flat_outage = {}
    eng._leader_flat_outage_alerted = {}
    eng._target_positions = {}
    eng.global_config = {"leader_flat_notional_usd": flat_usd, "leader_flat_poll_s": poll_s,
                         "leader_flat_confirms": confirms, "leader_flat_min_age_s": grace_s}
    eng.mid_prices = {}
    calls = {"n": 0}

    def _q(addr, coin):
        calls["n"] += 1
        return leader_szi if api_ok else None

    eng._query_target_position = _q
    eng._calls = calls
    return eng


W, C = "0xleader", "BTC"
MARK = 100.0          # $100/unit -> leader 20 units = $2,000; our 0.25 units = $25 (the 80x mismatch)


# ── 1. The measured rule: hold through partials, exit only at flat / flip ──────────────────────────

def test_holds_while_leader_still_holds_after_a_large_partial():
    """Leader trims $300 of a $2,000 position (17 units left). m07 target is still sign*fixed -> HOLD."""
    eng = _engine(leader_szi=17.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False


def test_exits_when_leader_is_flat():
    eng = _engine(leader_szi=0.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True


def test_exits_when_leader_flips_side():
    """forward_oos_hot ends the trip on close OR reverse -- a flip must end ours too."""
    eng = _engine(leader_szi=-5.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True
    eng = _engine(leader_szi=+5.0)
    assert eng._leader_flat_or_flipped(W, C, "SELL", MARK) is True


def test_dust_residue_counts_as_flat():
    """0.05 units * $100 = $5 < $10 min order: not a position we could have mirrored."""
    eng = _engine(leader_szi=0.05)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True


def test_just_above_the_dust_floor_is_still_a_position():
    eng = _engine(leader_szi=0.15)          # $15 > $10
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False


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

    eng = _engine(leader_szi=17.0)                            # $2000 - $300 = $1700 left
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False   # LEADER_FLAT holds


def test_the_two_rules_agree_once_the_leader_is_actually_out():
    """Parity is not "never exit" -- when the leader IS flat both rules close."""
    leader_reverse_notional, our_accumulated = 2000.0, 25.0
    assert min(leader_reverse_notional / our_accumulated, 1.0) >= 0.85
    eng = _engine(leader_szi=0.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True


# ── 3. Failure modes: this drives live capital, so unknown must never mean "exit" ──────────────────

def test_api_failure_never_exits_even_when_the_tracker_says_flat():
    """codex P1 2026-07-31. The first version fell back to the WS tracker, so a stale ZERO would
    return True and close a live position. Only the exchange may authorize a close."""
    eng = _engine(leader_szi=None, api_ok=False)
    eng._target_positions = {W: {C: 0.0}}
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is None


def test_api_failure_never_exits_even_when_the_tracker_says_opposite_side():
    """Same hole, flip flavour: a stale OPPOSITE tracker value read as 'flipped' and exited."""
    eng = _engine(leader_szi=None, api_ok=False)
    eng._target_positions = {W: {C: -8.0}}
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is None


def test_api_failure_with_no_tracker_value_returns_unknown():
    """None -> the caller HOLDS. sl_bps / global_stop_pct stay the risk floor."""
    eng = _engine(leader_szi=None, api_ok=False)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is None


# ── 3b. The two protections copied from the engine's own leader sweep ──────────────────────────────

def test_a_fresh_leg_is_not_closed_before_the_grace_window():
    """Our fill can complete BEFORE the leader's position shows up in REST. Without the grace, a
    brand-new leg reads as 'leader never had it' and self-closes instantly (codex P1)."""
    import time as _t
    eng = _engine(leader_szi=0.0, grace_s=90.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK, entry_ts=_t.time()) is False
    assert eng._calls["n"] == 0                      # not even queried inside the grace window


def test_after_the_grace_window_a_flat_leader_does_close_the_leg():
    import time as _t
    eng = _engine(leader_szi=0.0, grace_s=90.0)
    old = _t.time() - 600
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK, entry_ts=old) is True


def test_one_flat_snapshot_is_not_enough_when_two_confirms_are_required():
    """A single lagging REST snapshot must not close a position."""
    eng = _engine(leader_szi=0.0, confirms=2, poll_s=0.0)   # poll_s 0 -> every call re-queries
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True


def test_confirmations_require_FRESH_snapshots_not_repeated_cache_reads():
    """codex r2 P1, and he proved it by probing: the first version incremented the streak on every
    ~1Hz sweep while the cache re-served ONE REST response, so "2 confirms" was one observation read
    twice. With a live poll window, repeated calls must NOT manufacture a confirmation."""
    eng = _engine(leader_szi=0.0, confirms=2, poll_s=10.0)
    decisions = [eng._leader_flat_or_flipped(W, C, "BUY", MARK) for _ in range(6)]
    assert eng._calls["n"] == 1, "cache should have served the repeats"
    assert decisions == [False] * 6, f"cached repeats manufactured a confirmation: {decisions}"


def test_a_cached_hit_returns_the_prior_decision_without_re_querying():
    eng = _engine(leader_szi=0.0, confirms=1, poll_s=10.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True
    assert eng._calls["n"] == 1


def test_an_api_outage_is_negative_cached_so_it_cannot_retry_storm():
    """Each failure used to fire another blocking 5s request every sweep, freezing WS ingestion
    exactly when the API was already rate-limiting us (codex r2 P1)."""
    eng = _engine(leader_szi=None, api_ok=False, poll_s=10.0)
    for _ in range(8):
        assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is None
    assert eng._calls["n"] == 1


def test_a_strike_cannot_be_inherited_by_the_next_leg_on_the_same_coin():
    """Streaks are keyed by leg (wallet, coin, entry_ts). SL / trailing / max-hold / WS retirement all
    retire a leg without running the LEADER_FLAT cleanup, so a surviving strike would let the NEXT leg
    close on its first observation (codex r2 P1)."""
    eng = _engine(leader_szi=0.0, confirms=2, poll_s=0.0)
    old_leg, new_leg = 1000.0, 2000.0
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK, entry_ts=old_leg) is False   # strike 1
    # old leg retires by some other path; a NEW leg opens on the same coin
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK, entry_ts=new_leg) is False   # must be strike 1
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK, entry_ts=new_leg) is True


def test_exact_zero_does_not_need_a_mark():
    """An unambiguous flat reading must not be discarded because the mark is missing -- that would
    hold past the leader's exit. Only the dust floor needs a price (codex r2 P2)."""
    eng = _engine(leader_szi=0.0, confirms=1)
    assert eng._leader_flat_or_flipped(W, C, "BUY", 0.0) is True


def test_sign_reversal_does_not_need_a_mark():
    eng = _engine(leader_szi=-4.0, confirms=1)
    assert eng._leader_flat_or_flipped(W, C, "BUY", None) is True


def test_a_dust_judgement_still_requires_a_mark():
    """0.05 units is only 'flat' relative to a price. With no usable mark this must stay UNKNOWN."""
    eng = _engine(leader_szi=0.05, confirms=1)
    assert eng._leader_flat_or_flipped(W, C, "BUY", 0.0) is None


def test_a_still_open_reading_resets_the_confirmation_streak():
    eng = _engine(leader_szi=0.0, confirms=2, poll_s=0.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False    # strike 1
    eng._query_target_position = lambda a, c: 17.0                    # leader is back / never left
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False    # streak broken
    eng._query_target_position = lambda a, c: 0.0
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False    # counts as strike 1 again
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is True


def test_an_unknown_reading_breaks_the_confirmation_streak():
    """An outage in the middle of a streak must not be treated as a confirmation."""
    eng = _engine(leader_szi=0.0, confirms=2, poll_s=0.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False    # strike 1
    eng._query_target_position = lambda a, c: None
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is None
    eng._query_target_position = lambda a, c: 0.0
    assert eng._leader_flat_or_flipped(W, C, "BUY", MARK) is False    # back to strike 1, not 2


def test_unusable_mark_returns_unknown_rather_than_declaring_flat():
    """A $0 mark makes abs(szi)*mark == 0, which would read as 'flat' and close a live position."""
    eng = _engine(leader_szi=17.0)
    assert eng._leader_flat_or_flipped(W, C, "BUY", 0.0) is None
    assert eng._leader_flat_or_flipped(W, C, "BUY", None) is None


def test_api_result_is_cached_within_the_poll_window():
    eng = _engine(leader_szi=17.0, poll_s=10.0)
    for _ in range(5):
        eng._leader_flat_or_flipped(W, C, "BUY", MARK)
    assert eng._calls["n"] == 1


def test_successful_poll_refreshes_the_shared_tracker():
    """The WS tracker drifted before (the 55% orphan episode); ground truth writes back."""
    eng = _engine(leader_szi=17.0)
    eng._target_positions = {W: {C: 999.0}}
    eng._leader_flat_or_flipped(W, C, "BUY", MARK)
    assert eng._target_positions[W][C] == 17.0


# ── 4. _query_target_position: "flat" must mean flat, never "the response was unusable" ───────────
# codex P1 2026-07-31: the old body did `data.get("assetPositions", [])` and returned 0.0 on miss, so
# a 429 body / 5xx / transient {} all reported FLAT -- which under LEADER_FLAT authorizes a real close.

class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def _query_with(monkeypatch, status, payload):
    import hl_copy_trader_v17 as mod
    eng = object.__new__(mod.V17CopyTrader)
    monkeypatch.setattr(mod.requests, "post", lambda *a, **k: _Resp(status, payload))
    return mod.CopyTrader._query_target_position(eng, W, C)


def test_rate_limited_response_is_unknown_not_flat(monkeypatch):
    assert _query_with(monkeypatch, 429, {"error": "rate limited"}) is None


def test_server_error_is_unknown_not_flat(monkeypatch):
    assert _query_with(monkeypatch, 503, {}) is None


def test_empty_object_is_unknown_not_flat(monkeypatch):
    """A 200 with no assetPositions key is an unusable answer, not evidence of a flat account."""
    assert _query_with(monkeypatch, 200, {}) is None


def test_non_dict_body_is_unknown_not_flat(monkeypatch):
    assert _query_with(monkeypatch, 200, []) is None


def test_present_but_empty_asset_positions_is_a_genuine_flat(monkeypatch):
    """The one case that legitimately means flat: the key IS there and the account holds nothing."""
    assert _query_with(monkeypatch, 200, {"assetPositions": []}) == 0.0


def test_other_coins_present_but_not_ours_is_flat_for_our_coin(monkeypatch):
    payload = {"assetPositions": [{"position": {"coin": "ETH", "szi": "3.0"}}]}
    assert _query_with(monkeypatch, 200, payload) == 0.0


def test_our_coin_present_returns_its_signed_size(monkeypatch):
    payload = {"assetPositions": [{"position": {"coin": C, "szi": "-2.5"}}]}
    assert _query_with(monkeypatch, 200, payload) == -2.5


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
