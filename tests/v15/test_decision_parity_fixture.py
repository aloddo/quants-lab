"""STAGE 1 of the backtest/live equivalence certification: the DECISION-PARITY FIXTURE.

Spec: docs/research/backtest_live_parity_map_20260806.md section C + PLAN v2 (certification
criterion stage 1; coin-admission case = divergence 8; refusal-to-chase case).

ONE deterministic leader-event stream (STREAM below, defined once as data) is fed to BOTH sides:
  Side A: research/v15/v15_m07_engine.step_subaccount in PARITY CONFIG
          (copy_latency_ms=4000, sizing_mode=fixed_notional/$100, reversal_mode=flatten_only,
           exit_latency_ms=30000, exit_entry_grace_ms=90000, leader_dust_floor_usd=10).
  Side B: the live V17 decision core, driven on an uninitialized instance
          (tests/v15/_decision_parity_harness.LiveHarness; pattern of test_exit_parity_leader_flat).

Each scenario asserts order-by-order equality of the normalized decision tuples
(coin, side, ~notional, reduce_only, verb). Per-scenario comments state WHAT is asserted and what
is deliberately NOT (timing, sizing granularity, IOC fill probability, knet/margin gates).

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_decision_parity_fixture.py -q
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _decision_parity_harness as H                      # noqa: E402
from _decision_parity_harness import LeaderFill, W, T0, MS_MIN  # noqa: E402


def _t(minutes: int) -> int:
    return T0 + minutes * MS_MIN


# ================================================================================================ #
# THE leader-event stream — defined ONCE as data. Scenarios consume named segments.
# All prices $100/unit unless the segment's tape says otherwise; leader sizes chosen so a $100
# fixed-notional copy is representable (1 unit) and dust/flip floors are exercised exactly.
# ================================================================================================ #
STREAM = {
    # entry -> addon -> trim -> exit (scenarios 1-4, 9, stops-off leg of 10)
    "A": [
        LeaderFill(_t(1), "BTC", "BUY", 2.0, 100.0, 0.0, "a1"),    # flat -> open long 2.0
        LeaderFill(_t(3), "BTC", "BUY", 1.0, 100.0, 2.0, "a2"),    # addon -> 3.0
        LeaderFill(_t(5), "BTC", "SELL", 0.5, 100.0, 3.0, "a3"),   # trim -> 2.5 (partial close)
        LeaderFill(_t(10), "BTC", "SELL", 2.5, 100.0, 2.5, "a4"),  # exit -> flat
    ],
    # reversal: single-fill flip, far-side add, far-side exit, re-open from flat (scenario 5)
    "B": [
        LeaderFill(_t(1), "ETH", "BUY", 2.0, 100.0, 0.0, "b1"),    # flat -> long 2.0
        LeaderFill(_t(5), "ETH", "SELL", 4.0, 100.0, 2.0, "b2"),   # FLIP through zero -> short 2.0
        LeaderFill(_t(7), "ETH", "SELL", 1.0, 100.0, -2.0, "b3"),  # far-side ADD -> short 3.0
        LeaderFill(_t(9), "ETH", "BUY", 3.0, 100.0, -3.0, "b4"),   # far-side EXIT -> flat
        LeaderFill(_t(13), "ETH", "BUY", 1.5, 100.0, 0.0, "b5"),   # re-open from flat (copyable)
    ],
    # dust: trim to a residual worth $5 < the $10 leader-dust floor (scenario 6)
    "C": [
        LeaderFill(_t(1), "SOL", "BUY", 2.0, 100.0, 0.0, "c1"),
        LeaderFill(_t(10), "SOL", "SELL", 1.95, 100.0, 2.0, "c2"),  # residual 0.05 = $5 (dust)
    ],
    # dust CONTROL: residual $11, just ABOVE the $10 floor -> both HOLD (scenario 6b, codex P2-2)
    "C2": [
        LeaderFill(_t(1), "NEAR", "BUY", 2.0, 100.0, 0.0, "c3"),
        LeaderFill(_t(10), "NEAR", "SELL", 1.89, 100.0, 2.0, "c4"),  # residual 0.11 = $11 (held)
    ],
    # exit GRACE: leader flat 20s after our entry (scenario 4b, codex P1-4)
    "G": [
        LeaderFill(_t(1), "DOT", "BUY", 2.0, 100.0, 0.0, "g1"),
        LeaderFill(_t(1) + 20_000, "DOT", "SELL", 2.0, 100.0, 2.0, "g2"),
    ],
    # coin OUTSIDE the admitted universe (scenario 7, plan v2 divergence 8)
    "D": [
        LeaderFill(_t(1), "FARCOIN", "BUY", 1.0, 100.0, 0.0, "d1"),
    ],
    # deep drawdown for the SL leg of scenario 10 (tape crashes 100 -> 70 after minute 3)
    "E": [
        LeaderFill(_t(1), "AVAX", "BUY", 1.0, 100.0, 0.0, "e1"),
        LeaderFill(_t(5), "AVAX", "BUY", 1.0, 70.0, 1.0, "e2"),    # leader ADDs into the hole
        LeaderFill(_t(7), "AVAX", "BUY", 1.0, 70.0, 2.0, "e3"),    # ADDs again post-SL: stay out
    ],
    # global-stop leg of scenario 10 (LINK crashes 100 -> 50; OP entry arrives post-halt)
    "F": [
        LeaderFill(_t(1), "LINK", "BUY", 1.0, 100.0, 0.0, "f1"),
        LeaderFill(_t(5), "LINK", "BUY", 1.0, 50.0, 1.0, "f2"),    # cursor where the stop fires
        LeaderFill(_t(7), "OP", "BUY", 1.0, 100.0, 0.0, "f3"),     # post-halt entry: must not copy
    ],
}

FLAT100 = [100.0] * 40          # 40 minutes of flat tape isolates decisions from price movement
END = _t(30)


def _m07(segment, prices, **params):
    return H.run_m07(STREAM[segment], prices, END,
                     params=H.parity_params(**params) if params else None)


# ================================================================================================ #
# Scenario 1 — ENTRY: leader flat -> open. Both sides open ONE ~$100 leg.
# Asserted: exactly one decision each; coin/side/verb identical; notional within 10% (m07 prices
# the fill through its slippage model, live records the flat $100 order size).
# NOT asserted: timing (m07 fills at leader_ts+4s; live fills whenever the IOC lands), IOC fill
# probability (harness simulates guaranteed fill), knet/margin gates (inside stubbed execution).
# ================================================================================================ #
def test_s01_entry_flat_to_open(monkeypatch):
    m07 = H.m07_decisions(H.run_m07(STREAM["A"][:1], {"BTC": FLAT100}, END))

    live = H.LiveHarness(monkeypatch, whitelist={"BTC"})
    live.set_book("BTC", 100.0)
    live.deliver(STREAM["A"][0])

    assert live.decisions == [("BTC", "BUY", 100.0, False, "open")]
    H.assert_decisions_match(m07, live.decisions)
    assert len(live.held("BTC")) == 1


# ================================================================================================ #
# Scenario 2 — ADDON: leader adds -> both do nothing (live: add_tracked_not_copied with
# copy_adds_enabled=False; m07 fixed_notional: zero drift-rebalance orders, divergence #1 closed).
# Asserted: decision list stays [open] on both sides; the live tracker DID see the add.
# NOT asserted: m07's tracker internals (the zero-drift guarantee has its own golden in
# test_m07_parity_features.py).
# ================================================================================================ #
def test_s02_addon_not_copied(monkeypatch):
    res = H.run_m07(STREAM["A"][:2], {"BTC": FLAT100}, END)
    m07 = H.m07_decisions(res)

    live = H.LiveHarness(monkeypatch, whitelist={"BTC"})
    live.set_book("BTC", 100.0)
    for f in STREAM["A"][:2]:
        live.deliver(f)

    assert len(m07) == 1 and m07[0][4] == "open"          # the ADDON produced NO m07 fill
    H.assert_decisions_match(m07, live.decisions)
    assert live.eng._v16_add_fills == 1                    # tracked, never copied
    assert live.eng._v16_leader_pos[(W, "BTC")] == 3.0     # tracker followed the leader
    assert len(live.held("BTC")) == 1


# ================================================================================================ #
# Scenario 3 — TRIM: leader partial-close -> both HOLD the full leg (the rule the backtest
# measured: exit only on flat-or-flipped, never on a partial reduce).
# Asserted: no decision beyond the entry on either side; live holds through a LEADER_FLAT sweep
# that observes the leader still in ($250 > $10 floor); m07 ends the window still holding.
# NOT asserted: drift-rebalance absence at the m07 level beyond the fill list (goldened elsewhere).
# ================================================================================================ #
def test_s03_trim_hold_full(monkeypatch):
    res = H.run_m07(STREAM["A"][:3], {"BTC": FLAT100}, END)
    m07 = H.m07_decisions(res)
    assert len(m07) == 1 and m07[0][4] == "open"
    assert "BTC" in res["ending_account_state"]["positions"]

    live = H.LiveHarness(monkeypatch, whitelist={"BTC"})
    live.set_book("BTC", 100.0)
    for f in STREAM["A"][:3]:
        live.deliver(f)
    live.observe({"BTC": 2.5})       # leader still holds $250
    live.sweep()

    H.assert_decisions_match(m07, live.decisions)
    assert len(live.held("BTC")) == 1


# ================================================================================================ #
# Scenario 4 — EXIT: leader flat -> both close fully. This is ALSO the stops-OFF run required by
# scenario 10: sl_bps/global_stop are off on both sides and must not fire.
# Asserted: sequence [open, close] on both sides; reduce_only on the close; live needs TWO fresh
# flat snapshots (deployed confirms=2); no sl/global events on the m07 side. m07-side latency model
# CERTIFIED by fill timestamps (codex P1-4): entry at leader_ts+copy_latency(4s), exit at
# leader_ts+exit_latency(30s). The live full-close is the ENGINE's choice, not the stub's
# (codex P1-1): trim_size is None and the closed size equals the held size.
# NOT asserted: CROSS-SIDE latency equality (m07 models 30s; live is poll x confirms + grace — the
# fixture compares sequence, parity map row 4/divergence 5 carries the timing delta).
# ================================================================================================ #
def test_s04_exit_on_leader_flat_stops_off(monkeypatch):
    res = _m07("A", {"BTC": FLAT100})
    m07 = H.m07_decisions(res)
    m07_ts = H.m07_decisions(res, with_ts=True)
    assert [d[4] for d in m07] == ["open", "close"]
    assert m07_ts[0][5] == _t(1) + 4_000, "entry must execute at leader_ts + copy_latency_ms"
    assert m07_ts[1][5] == _t(10) + 30_000, "exit must execute at leader_ts + exit_latency_ms"
    assert not any(e.get("event_type") in ("sl_exit", "global_stop_exit") for e in res["events"])
    assert res["ending_account_state"]["positions"] == {}

    live = H.LiveHarness(monkeypatch, whitelist={"BTC"}, confirms=2, sl_bps=None)
    live.set_book("BTC", 100.0)
    for f in STREAM["A"]:
        live.deliver(f)
    live.observe({"BTC": 0.0})
    live.sweep()                                   # strike 1 of 2: must still hold
    assert len(live.held("BTC")) == 1
    live.observe({"BTC": 0.0})
    live.sweep()                                   # strike 2: close authorized

    H.assert_decisions_match(m07, live.decisions)
    assert live.decisions[-1][3] is True           # reduce_only close
    call = live.exit_calls[-1]                     # the ENGINE requested a FULL close (P1-1)
    assert call["trim_size"] is None and call["close_sz"] == call["held_sz"] == pytest.approx(1.0)
    assert live.held("BTC") == []


# ================================================================================================ #
# Scenario 4b — EXIT GRACE (codex P1-4): leader goes flat 20 SECONDS after our entry.
# m07: exit_entry_grace_ms=90000 anchors at OUR opening fill -> the exit executes at
# leg_open_ts + 90s, NOT at leader_ts + 30s. Asserted on the fill timestamp (the mutant
# exit_latency=4000/grace=0 fails here).
# Live: the 90s min-age grace in _leader_flat_or_flipped holds a young leg even on a usable flat
# snapshot; after the grace it STILL needs two fresh confirms. Asserted step by step on the fake
# clock. Both sides end [open, close]; cross-side timing not compared (as always).
# ================================================================================================ #
def test_s04b_exit_grace_blocks_young_leg(monkeypatch):
    res = _m07("G", {"DOT": FLAT100})
    m07 = H.m07_decisions(res)
    m07_ts = H.m07_decisions(res, with_ts=True)
    assert [d[4] for d in m07] == ["open", "close"]
    entry_fill_ts = m07_ts[0][5]
    assert entry_fill_ts == _t(1) + 4_000
    assert m07_ts[1][5] == entry_fill_ts + 90_000, (
        "a leader-flat inside the grace must execute at leg_open + grace, not leader_ts + 30s")

    live = H.LiveHarness(monkeypatch, whitelist={"DOT"}, confirms=2)
    live.set_book("DOT", 100.0)
    live.deliver(STREAM["G"][0])                   # our fill at t+1m
    live.deliver(STREAM["G"][1])                   # leader flat 20s later (REDUCE_WE_HOLD, no entry)
    live.observe({"DOT": 0.0})                     # usable FLAT snapshot ~30s after entry
    live.sweep()
    assert len(live.held("DOT")) == 1, "inside the 90s grace a flat snapshot must NOT close"
    live.clock.advance(90.0)                       # past the grace window
    live.observe({"DOT": 0.0})
    live.sweep()                                   # strike 1 of 2
    assert len(live.held("DOT")) == 1, "past the grace, ONE fresh snapshot is not enough"
    live.observe({"DOT": 0.0})
    live.sweep()                                   # strike 2: close authorized
    assert live.held("DOT") == []

    H.assert_decisions_match(m07, live.decisions)
    call = live.exit_calls[-1]
    assert call["trim_size"] is None and call["close_sz"] == call["held_sz"]


# ================================================================================================ #
# Scenario 5 — REVERSAL (single-fill flip): both FLATTEN ONLY (no far side), both stay out on the
# leader's far-side add, both re-enter on leader flat -> open. Parity map divergence #3 closed by
# m07 reversal_mode="flatten_only" matching live copy_reverse_enabled=False.
# Asserted: sequence [open, close, open] on both sides; the far-side ADD and far-side EXIT emit
# NOTHING; the live flatten runs through the REAL _pending_reverse machinery
# (_on_hl_trade REVERSE branch -> _check_exits -> _execute_pending_reverse -> flatten, reap).
# NOT asserted: timing. The live re-open is placed 8 minutes after the flatten because live's 300s
# post-exit cooldown (L1331-1335) refuses re-entry sooner — a real divergence vs m07 (which has no
# such cooldown), documented in the parity map's entry-gate row; the fixture spaces the stream
# beyond it so the SEQUENCE can be compared.
# ================================================================================================ #
def test_s05_reversal_flatten_only_then_reenter(monkeypatch):
    res = _m07("B", {"ETH": FLAT100})
    m07 = H.m07_decisions(res)
    assert [d[4] for d in m07] == ["open", "close", "open"]
    assert m07[1][3] is True                                  # the flip flattens, reduce-only
    assert "ETH" in res["ending_account_state"]["positions"]  # re-entered leg still open at END

    live = H.LiveHarness(monkeypatch, whitelist={"ETH"})
    live.set_book("ETH", 100.0)
    live.deliver(STREAM["B"][0])                       # open
    live.deliver(STREAM["B"][1])                       # flip -> durable _pending_reverse intent
    assert live.held("ETH")[0].get("_pending_reverse") is not None
    live.sweep()                                       # _execute_pending_reverse: flatten + reap
    assert live.held("ETH") == []
    assert live.eng._reverse_opens == []               # copy_reverse_enabled=False: NO far side
    call = live.exit_calls[-1]                         # the flatten was a FULL close (P1-1)
    assert call["trim_size"] is None and call["close_sz"] == call["held_sz"]
    live.deliver(STREAM["B"][2])                       # far-side ADD: tracked, not copied
    live.deliver(STREAM["B"][3])                       # far-side EXIT: REDUCE_NOT_HELD, tracked
    assert [d[4] for d in live.decisions] == ["open", "close"]
    live.deliver(STREAM["B"][4])                       # leader flat -> open: copyable re-entry

    H.assert_decisions_match(m07, live.decisions)
    assert len(live.held("ETH")) == 1
    assert live.eng._v16_reverse_fills == 1


# ================================================================================================ #
# Scenario 6 — DUST: leader trims to a residual worth $5 < the $10 floor -> both treat as flat and
# close fully (m07 leader_dust_floor_usd=10; live leader_flat_notional_usd=10 in
# _leader_flat_or_flipped — the parity map's "admitted deviation" now matched by config).
# Asserted: sequence [open, close] on both sides; live's dust judgement uses the snapshot residual
# (0.05 units) priced at the mark, exactly the live rule.
# NOT asserted: exit timing (as scenario 4).
# ================================================================================================ #
def test_s06_dust_residual_reads_as_flat(monkeypatch):
    res = _m07("C", {"SOL": FLAT100})
    m07 = H.m07_decisions(res)
    assert [d[4] for d in m07] == ["open", "close"]
    assert res["ending_account_state"]["positions"] == {}

    live = H.LiveHarness(monkeypatch, whitelist={"SOL"}, confirms=2)
    live.set_book("SOL", 100.0)
    for f in STREAM["C"]:
        live.deliver(f)
    live.observe({"SOL": 0.05})      # $5 residual: below the floor -> reads flat
    live.sweep()
    live.observe({"SOL": 0.05})
    live.sweep()

    H.assert_decisions_match(m07, live.decisions)
    assert live.held("SOL") == []
    call = live.exit_calls[-1]                         # dust-exit closes OUR leg in FULL (P1-1)
    assert call["trim_size"] is None and call["close_sz"] == call["held_sz"]


# ================================================================================================ #
# Scenario 6b — DUST CONTROL (codex P2-2): leader trims to a residual worth $11, just ABOVE the
# $10 floor -> BOTH sides HOLD. Pins the threshold itself: a fixture that treated any residual as
# dust (or none) fails one of 6/6b.
# ================================================================================================ #
def test_s06b_just_above_dust_floor_holds(monkeypatch):
    res = _m07("C2", {"NEAR": FLAT100})
    m07 = H.m07_decisions(res)
    assert [d[4] for d in m07] == ["open"], "an $11 residual must NOT read as leader-flat"
    assert "NEAR" in res["ending_account_state"]["positions"]

    live = H.LiveHarness(monkeypatch, whitelist={"NEAR"}, confirms=2)
    live.set_book("NEAR", 100.0)
    for f in STREAM["C2"]:
        live.deliver(f)
    live.observe({"NEAR": 0.11})     # $11 residual: above the floor -> still a position
    live.sweep()
    live.observe({"NEAR": 0.11})
    live.sweep()                     # even two confirms of an $11 residual must not close

    H.assert_decisions_match(m07, live.decisions)
    assert len(live.held("NEAR")) == 1
    assert live.exit_calls == []


# ================================================================================================ #
# Scenario 7 — COIN ADMISSION (plan v2 divergence 8): leader opens a coin OUTSIDE the admitted
# universe. Live half: the V16 whitelist guard drops the signal before any processing (asserted,
# passing). m07 half: m07 has NO coin-admission filter yet — the parity-required assertion (no
# order) FAILS today, so it is a strict XFAIL pinning the open divergence, not a hack.
# ================================================================================================ #
def test_s07_coin_admission_live_refuses(monkeypatch):
    live = H.LiveHarness(monkeypatch, whitelist={"BTC"})   # FARCOIN not admitted
    live.set_book("FARCOIN", 100.0)                        # book exists; the guard must still drop
    live.deliver(STREAM["D"][0])
    assert live.decisions == []
    assert live.eng._v16_blocked_signals == 1              # dropped AS a target signal, not missed
    assert live.eng.positions == []


@pytest.mark.xfail(
    strict=True,
    reason="plan v2 divergence 8: parity-configured M07 has no coin-admission filter yet — it "
           "replays every coin with candle coverage. The parity requirement (leader opens a "
           "non-admitted coin -> NO order on both sides) fails on the m07 side until the filter "
           "lands (docs/research/backtest_live_parity_map_20260806.md, Divergence 8).")
def test_s07_coin_admission_m07_lacks_coin_filter():
    res = _m07("D", {"FARCOIN": FLAT100})
    assert H.m07_decisions(res) == [], (
        "m07 copied a coin outside the admitted universe (no coin-admission filter)")


# ================================================================================================ #
# Scenario 8 — REFUSAL TO CHASE: the mock book's mid sits 50bps from the leader's fill price, so
# the live chase gate (<=15bps, L3617-3620) refuses; m07 has NO entry gates and trades. This is an
# EXPECTED, RECORDED divergence (parity map divergence #2): the test asserts BOTH halves of the
# delta — live refuses AND m07 would have traded. The delta is the point; the parity report
# carries it as the biased-filter finding (fast-moving entries preferentially rejected).
# Asserted: live emits no decision AND left no entry side effects; m07 emits exactly the open.
# ================================================================================================ #
def test_s08_refusal_to_chase_is_a_recorded_divergence(monkeypatch):
    m07 = H.m07_decisions(H.run_m07(STREAM["A"][:1], {"BTC": FLAT100}, END))
    assert [d[4] for d in m07] == ["open"]                 # m07 side WOULD have traded

    live = H.LiveHarness(monkeypatch, whitelist={"BTC"})
    live.set_book("BTC", 100.0 * 1.005)                    # mid 50bps away from px -> chase gate
    live.deliver(STREAM["A"][0])

    assert live.decisions == []                            # live refuses to chase
    assert live.eng.positions == []
    assert live.eng._twap_entered == set()                 # refused BEFORE the entry commit point
    assert (W, "BTC") not in live.eng.last_entry           # no cooldown stamped on a refusal
    # The signal was still TRACKED (guard-rejected opens must not corrupt later classification).
    assert live.eng._v16_leader_pos[(W, "BTC")] == 2.0


# ================================================================================================ #
# Scenario 9 — DUPLICATE FILL: the same tid redelivered. Live dedups (`_seen_tids`: the redelivery
# classifies as ADD against the updated tracker and is consumed by the ADD-branch tid check BEFORE
# mutating state); the m02 action stream is deduped upstream so m07 sees the event once.
# Asserted: exactly ONE decision on both sides; the live tracker was not double-counted.
# NOT asserted: the 300s _seen_tids pruning window (section-C item 3's prune-then-redeliver case
# needs a long-horizon clock scenario; out of stage-1 scope, noted in the report).
# ================================================================================================ #
def test_s09_duplicate_tid_single_decision(monkeypatch):
    m07 = H.m07_decisions(H.run_m07(STREAM["A"][:1], {"BTC": FLAT100}, END))  # deduped upstream
    assert len(m07) == 1

    live = H.LiveHarness(monkeypatch, whitelist={"BTC"})
    live.set_book("BTC", 100.0)
    live.deliver(STREAM["A"][0])
    live.deliver(STREAM["A"][0])                           # same tid redelivered

    H.assert_decisions_match(m07, live.decisions)
    assert len(live.decisions) == 1
    assert live.eng._v16_leader_pos[(W, "BTC")] == 2.0     # tracker not double-counted
    assert len(live.held("BTC")) == 1


# ================================================================================================ #
# Scenario 10a — HARD SL, ON both sides: long from $100, tape collapses to $70 (-3000bps <= -2500).
# m07: sl_bps=-2500 (+flatten_only, required combo) closes at the next action cursor and latches
# rev_latch; live: EXIT LAYER 1 in _check_exits closes on the sweep. Both stay out on the leader's
# subsequent same-side ADD (m07 rev_latch; live add_tracked_not_copied on an unheld leg).
# Asserted: sequence [open, close]; the live close fired while the leader was STILL IN (proof it is
# the SL layer, not LEADER_FLAT — the snapshot shows the leader holding); no re-entry after.
# NOT asserted: SL trigger granularity (m07 checks at action cursors + fold end; live checks every
# ~1s sweep — parity map/EngineParams document the approximation), close-price equality beyond the
# 10% notional band (both close near $70).
# ================================================================================================ #
def test_s10a_hard_sl_fires_both_sides_and_latches(monkeypatch):
    tape = [100.0] * 3 + [70.0] * 37
    res = _m07("E", {"AVAX": tape}, sl_bps=-2500.0)
    m07 = H.m07_decisions(res)
    assert any(e.get("event_type") == "sl_exit" for e in res["events"])
    assert [d[4] for d in m07] == ["open", "close"]
    assert res["ending_account_state"]["positions"] == {}   # latched: the later ADDs never re-enter

    live = H.LiveHarness(monkeypatch, whitelist={"AVAX"}, sl_bps=-2500.0)
    live.set_book("AVAX", 100.0)
    live.deliver(STREAM["E"][0])                            # open @100
    live.set_book("AVAX", 70.0)                             # tape collapses
    live.deliver(STREAM["E"][1])                            # leader ADD (tracked, not copied)
    live.observe({"AVAX": 2.0})                             # leader STILL IN: not a leader-flat exit
    live.sweep()                                            # SL layer closes
    assert [d[4] for d in live.decisions] == ["open", "close"]
    live.deliver(STREAM["E"][2])                            # post-SL leader ADD: must stay out

    H.assert_decisions_match(m07, live.decisions)
    assert live.held("AVAX") == []
    assert len(live.decisions) == 2                         # no re-entry on the post-SL add
    call = live.exit_calls[-1]                              # the SL close was FULL (P1-1)
    assert call["trim_size"] is None and call["close_sz"] == call["held_sz"]


# ================================================================================================ #
# Scenario 10b — GLOBAL STOP: -15% vs start equity flattens everything and halts. Start equity
# $300, one $100 leg, tape -50% -> -$50 = -16.7% breach on both sides.
# Asserted: m07 fires global_stop_exit, flattens, and REFUSES the later OP entry (halt latch).
# Live: the fast stop (_evaluate_global_stop_fast) latches _flatten_requested + _kill_reasons; the
# REAL _emergency_flatten (L2447-2479, codex P1-2) walks the (fake) exchange truth and the recorder
# sits at its market_close boundary -> same [open, close] sequence. Then (codex P1-3) the halt is
# GENUINELY exercised: the fixture performs the stats-loop sync assignment (L4646,
# _kill_switch_active <- bool(_kill_reasons) — finding F5: the fast latch does NOT do this itself,
# entries stay unblocked for up to ~60s until the stats loop runs) and delivers the OP entry
# against the REAL base _enter_position, whose kill gate (L2096) must refuse it.
# NOT asserted / documented seams (live half):
#   * the stop's INPUT is exchange-truth PnL (`_exch_pnl` + `_compute_unrealized_pnl` ->
#     `_refresh_exchange_state` REST) — injected as scenario data by arm_global_stop;
#   * `_reconcile_positions` is mirrored minimally (drop tracker rows absent from the exchange);
#   * the OP delivery calls the BASE _enter_position (where the kill gate lives), not the V17/V16
#     wrappers (knet/whitelist/caps + exchange I/O) — if the latch were broken the real method
#     would proceed into exchange calls and fail loudly, not pass silently.
# ================================================================================================ #
def test_s10b_global_stop_flattens_and_halts(monkeypatch):
    res = H.run_m07(STREAM["F"], {"LINK": [100.0] * 3 + [50.0] * 37, "OP": FLAT100}, END,
                    params=H.parity_params(global_stop_pct=0.15), start_equity=300.0)
    m07 = H.m07_decisions(res)
    assert any(e.get("event_type") == "global_stop_exit" for e in res["events"])
    assert [d[:2] + (d[4],) for d in m07] == [("LINK", "BUY", "open"), ("LINK", "SELL", "close")]
    assert not any(d[0] == "OP" for d in m07)               # halted: post-stop entry not copied
    assert res["ending_account_state"]["positions"] == {}

    live = H.LiveHarness(monkeypatch, whitelist={"LINK", "OP"})
    live.set_book("LINK", 100.0)
    live.deliver(STREAM["F"][0])                            # open @100
    live.set_book("LINK", 50.0)                             # -50%: -$50 on $300 baseline
    live.arm_global_stop(monkeypatch, baseline_equity=300.0, unrealized_pnl=-50.0)
    live.clock.advance(10.0)
    live.sweep()                            # fast stop latches; REAL _emergency_flatten runs

    assert live.eng._flatten_requested is True
    assert live.eng._kill_reasons.get("global_stop") is True
    H.assert_decisions_match(m07, live.decisions)
    assert live.eng.positions == []
    assert live.exchange_book == {}                         # every exchange leg was market_closed

    # F5 sync-lag seam: the fast latch does NOT set _kill_switch_active — that assignment happens
    # only in the 60s stats loop (L4646). Assert the gap exists, then perform the sync explicitly.
    assert getattr(live.eng, "_kill_switch_active", False) is False
    live.eng._kill_switch_active = bool(live.eng._kill_reasons)     # the L4646 assignment

    # Post-halt entry: delivered against the REAL base entry guard (kill gate at L2096).
    live.set_book("OP", 100.0)

    async def _gated_enter(coin, is_buy, **kw):
        return await mod_base_enter(live.eng, coin, is_buy, **kw)

    mod_base_enter = H.mod.CopyTrader._enter_position
    live.eng._enter_position = _gated_enter
    live.deliver(STREAM["F"][2])                            # leader opens OP post-halt
    assert not any(d[0] == "OP" for d in live.decisions), "halted engine must refuse the entry"
    assert live.eng.positions == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
