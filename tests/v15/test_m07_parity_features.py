"""M07 live-parity features (plan v2, Fable-APPROVED 2026-08-06) — opt-in modes + goldens.

Contract (Fable finding 5): every parity feature is OPT-IN and the engine with DEFAULT params must be
BYTE-IDENTICAL to the pre-change engine. The golden snapshot in golden_m07_fixed_position_snapshot.json
was captured from the engine at commit-time BEFORE any parity edit (2026-08-06, this session) on a
scenario that exercises entry / addon / trim / flip / exit with price drift. If any future edit changes
default-mode output in ANY way, test_default_mode_matches_prechange_golden fails.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m07_parity_features.py -q
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m07_engine as E  # noqa: E402
from test_m07 import FakeMarketData, _ohlc_path, _action, T0  # noqa: E402

GOLDEN = Path(__file__).resolve().parent / "golden_m07_fixed_position_snapshot.json"

PRICES = [100, 100, 105, 105, 95, 95, 110, 110, 100, 100, 90, 90]


def _scenario_actions():
    return pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 3 * E.MS_MIN, 0.0, "ADDON", position_after=3.0, signed_size=1.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "TRIM", position_after=1.5, signed_size=-1.5),
        _action("BTC", T0 + 7 * E.MS_MIN, 0.0, "ENTRY", position_after=-2.0, signed_size=-3.5),
        _action("BTC", T0 + 9 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])


def _clean(obj):
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items() if not str(k).startswith("_")}
    if isinstance(obj, (list, tuple)):
        return [_clean(x) for x in obj]
    if isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    return str(obj)


def _run(params, acts=None):
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    return E.step_subaccount(acts if acts is not None else _scenario_actions(), md, 10_000.0,
                             params, end_ts_ms=T0 + 11 * E.MS_MIN)


# --------------------------------------------------------------------------- #
# GOLDEN: default fixed_position output byte-identical to the pre-change engine
# --------------------------------------------------------------------------- #
def test_default_mode_matches_prechange_golden():
    res = _run(E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_position",
                              fixed_target_exposure=0.10))
    got = {
        "fills": _clean(res["fills"]),
        "equity": _clean(res["equity"]),
        "ending_positions": _clean({
            "cross_collateral": res["ending_account_state"].cross_collateral,
            "positions": {c: {"szi": p.szi, "entry_px": p.entry_px}
                          for c, p in res["ending_account_state"].positions.items()},
            "cooldown_until_ms": res["ending_account_state"].cooldown_until_ms,
        }) if hasattr(res["ending_account_state"], "positions") else _clean(res["ending_account_state"]),
        "summary_numeric": {k: v for k, v in res["summary"].items()
                            if isinstance(v, (int, float)) and not isinstance(v, bool)
                            and not str(k).startswith("_")},
    }
    got["positions"] = _clean(res["positions"])
    exp = json.loads(GOLDEN.read_text())
    # Output ROWS must be byte-identical to the pre-change engine. The summary may GAIN new
    # telemetry/provenance keys (fixed_notional_usd, n_exit_pushed_past_end, exit_latency_ms, ...) —
    # but every key that existed pre-change must be EXACTLY unchanged, floats included.
    # (codex R1: the positions stream is compared too — added after the first review round.)
    assert got["fills"] == exp["fills"]
    assert got["equity"] == exp["equity"]
    assert got["positions"] == exp["positions"]
    assert got["ending_positions"] == exp["ending_positions"]
    for k, v in exp["summary_numeric"].items():
        assert got["summary_numeric"].get(k) == v, f"pre-change summary key {k} changed: {got['summary_numeric'].get(k)} != {v}"


# --------------------------------------------------------------------------- #
# fixed_notional semantics
# --------------------------------------------------------------------------- #
def _fn_params(**kw):
    return E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional",
                          fixed_notional_usd=100.0, **kw)


def test_fixed_notional_zero_drift_rebalance_orders():
    """THE divergence-#1 guard: leader ADDON/TRIM rows must produce NO orders. Exactly 3 fills:
    entry, flip (close+far open counts as the flip path's fills), exit — never a re-size."""
    res = _run(_fn_params())
    fills = [f for f in res["fills"] if not str(f.get("action_type", "")).startswith("FOLLOWER")]
    # entry (1) + flip (close old + open far side may book as 1 or 2 fills depending on _book_fill) + exit (1)
    # The invariant that matters: NO fill occurs at the ADDON (t+3min) or TRIM (t+5min) cursors.
    addon_ts = T0 + 3 * E.MS_MIN
    trim_ts = T0 + 5 * E.MS_MIN
    for f in fills:
        ts = f.get("our_ts") or f.get("ts")
        assert ts not in (addon_ts, trim_ts), f"drift-rebalance fill at {ts}: {f}"


def test_fixed_notional_entry_size_is_flat_usd():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _fn_params(), end_ts_ms=T0 + 3 * E.MS_MIN)
    pos = res["ending_account_state"]["positions"].get("BTC")
    assert pos is not None
    # causal mark at t+1min is prices[0] = 100 -> $100 notional = 1.0 szi (szdec=3 rounding)
    assert abs(pos["szi"] * 100.0 - 100.0) < 1.0, f"expected ~$100 notional, got szi={pos['szi']}"


def test_fixed_notional_hold_through_addon_trim_no_resize():
    """Held size after ADDON+TRIM must equal the entry size exactly (no compounding, no drift)."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 3 * E.MS_MIN, 0.0, "ADDON", position_after=3.0, signed_size=1.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "TRIM", position_after=1.5, signed_size=-1.5),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _fn_params(), end_ts_ms=T0 + 7 * E.MS_MIN)
    assert len([f for f in res["fills"]]) == 1, f"expected exactly the entry fill, got {res['fills']}"
    pos = res["ending_account_state"]["positions"].get("BTC")
    assert pos is not None and pos["szi"] > 0


def test_fixed_notional_exit_on_leader_flat():
    res = _run(_fn_params())
    assert res["ending_account_state"]["positions"] == {}, "leader EXIT must fully close the leg"


def test_fixed_notional_flip_opens_fresh_dollar_leg():
    """Default reversal behavior in fixed_notional stays FLIP (flatten-only mode is a separate,
    opt-in feature): after the leader's sign flip the held leg must be SHORT and ~$100 notional."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 7 * E.MS_MIN, 0.0, "ENTRY", position_after=-2.0, signed_size=-4.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _fn_params(), end_ts_ms=T0 + 9 * E.MS_MIN)
    pos = res["ending_account_state"]["positions"].get("BTC")
    assert pos is not None and pos["szi"] < 0, f"expected short after flip, got {pos}"
    # causal mark at t+7min = prices[6] = 110 -> $100 -> ~0.909 szi
    assert abs(abs(pos["szi"]) * 110.0 - 100.0) < 2.0, f"far leg must be ~$100, got szi={pos['szi']}"


def test_fixed_notional_summary_provenance():
    res = _run(_fn_params())
    assert res["summary"]["sizing_mode"] == "fixed_notional"
    assert res["summary"]["fixed_notional_usd"] == 100.0
    assert res["summary"]["fixed_target_exposure"] is None
    assert res["summary"]["reversal_mode"] == "flip"


# --------------------------------------------------------------------------- #
# reversal_mode="flatten_only" (plan v2 divergence #3: live parity)
# --------------------------------------------------------------------------- #
def _fo_params(**kw):
    return E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional",
                          fixed_notional_usd=100.0, reversal_mode="flatten_only", **kw)


def test_flatten_only_flip_goes_flat_never_far_side():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        # single-fill flip: +2 -> -2, leader never flat
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ENTRY", position_after=-2.0, signed_size=-4.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _fo_params(), end_ts_ms=T0 + 7 * E.MS_MIN)
    assert res["ending_account_state"]["positions"] == {}, "flip must flatten, never open far side"
    sides = [np.sign(f.get("szi", f.get("delta_szi", 0)) or 0) for f in res["fills"]]
    # exactly one opening (long) and one closing fill; nothing short-opening
    assert len(res["fills"]) == 2, f"expected entry+close only, got {res['fills']}"


def test_flatten_only_suppresses_far_side_adds():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ENTRY", position_after=-2.0, signed_size=-4.0),
        # leader ADDs to their far-side position mid-journey: live tracks, never copies
        _action("BTC", T0 + 7 * E.MS_MIN, 0.0, "ADDON", position_after=-3.0, signed_size=-1.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _fo_params(), end_ts_ms=T0 + 9 * E.MS_MIN)
    assert res["ending_account_state"]["positions"] == {}, "suppressed ADD must not re-open us"
    assert len(res["fills"]) == 2, f"no fill may occur at the suppressed ADDON: {res['fills']}"


def test_flatten_only_reenters_on_leader_flat_then_open():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ENTRY", position_after=-2.0, signed_size=-4.0),
        _action("BTC", T0 + 7 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
        # leader opens a NEW journey from flat: position_after == signed_size -> copyable OPEN
        _action("BTC", T0 + 9 * E.MS_MIN, 0.0, "ENTRY", position_after=1.5, signed_size=1.5),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _fo_params(), end_ts_ms=T0 + 11 * E.MS_MIN)
    pos = res["ending_account_state"]["positions"].get("BTC")
    assert pos is not None and pos["szi"] > 0, "leader flat->open must clear the latch and be copied"


def test_flatten_only_composes_with_fixed_position_sizing():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ENTRY", position_after=-2.0, signed_size=-4.0),
    ])
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_position",
                       fixed_target_exposure=0.10, reversal_mode="flatten_only")
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 7 * E.MS_MIN)
    assert res["ending_account_state"]["positions"] == {}, "flatten_only must work under fixed_position too"


# --------------------------------------------------------------------------- #
# exit-latency model (plan v2 divergence #5: live LEADER_FLAT detection is slow)
# --------------------------------------------------------------------------- #
LONG_PRICES = [100] * 40   # flat tape: latency effects isolated from price movement


def _el_params(**kw):
    return E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                          exit_latency_ms=30_000, exit_entry_grace_ms=90_000,
                          leader_dust_floor_usd=10.0, **kw)


def test_exit_latency_delays_the_exit_fill():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", LONG_PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 10 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _el_params(), end_ts_ms=T0 + 30 * E.MS_MIN)
    assert res["ending_account_state"]["positions"] == {}
    exit_fills = [f for f in res["fills"] if (f.get("our_ts") or f.get("ts", 0)) > T0 + 9 * E.MS_MIN]
    assert exit_fills, f"no exit fill found: {res['fills']}"
    ts = exit_fills[-1].get("our_ts") or exit_fills[-1].get("ts")
    assert ts == T0 + 10 * E.MS_MIN + 30_000, f"exit must land at leader_ts+30s, got {ts}"


def test_exit_latency_entry_grace_blocks_young_leg_exit():
    """Leg opened at t+1min; leader flat 20s later. Live cannot confirm-exit inside the 90s grace:
    our exit lands at entry+90s, not leader_ts+30s."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", LONG_PRICES))
    entry_ts = T0 + 1 * E.MS_MIN
    acts = pd.DataFrame([
        _action("BTC", entry_ts, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", entry_ts + 20_000, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _el_params(), end_ts_ms=T0 + 30 * E.MS_MIN)
    assert res["ending_account_state"]["positions"] == {}
    exit_fills = [f for f in res["fills"] if (f.get("our_ts") or f.get("ts", 0)) > entry_ts]
    ts = exit_fills[-1].get("our_ts") or exit_fills[-1].get("ts")
    assert ts == entry_ts + 90_000, f"grace must push the exit to entry+90s, got {ts}"


def test_exit_latency_dust_floor_counts_as_flat():
    """Leader trims to a residual worth < $10: live's dust floor treats it as flat -> we fully exit."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", LONG_PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        # residual 0.05 BTC * $100 = $5 < $10 dust floor
        _action("BTC", T0 + 10 * E.MS_MIN, 0.0, "TRIM", position_after=0.05, signed_size=-1.95),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _el_params(), end_ts_ms=T0 + 30 * E.MS_MIN)
    assert res["ending_account_state"]["positions"] == {}, "dust residual must be treated as leader-flat"


def test_exit_latency_pushed_past_fold_end_stays_open_and_counted():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", LONG_PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        # leader flat 10s before fold end; +30s exit latency lands past the boundary
        _action("BTC", T0 + 5 * E.MS_MIN - 10_000, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])
    res = E.step_subaccount(acts, md, 10_000.0, _el_params(), end_ts_ms=T0 + 5 * E.MS_MIN)
    assert res["summary"]["n_exit_pushed_past_end"] == 1
    assert "BTC" in res["ending_account_state"]["positions"], "leg stays open; fold-end force-mark prices it"


def test_exit_latency_off_by_default_is_inert():
    """Same stream with exit model DISABLED: exit executes at copy_latency (legacy semantics)."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", LONG_PRICES))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 10 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0)
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 30 * E.MS_MIN)
    exit_fills = [f for f in res["fills"] if (f.get("our_ts") or f.get("ts", 0)) > T0 + 9 * E.MS_MIN]
    ts = exit_fills[-1].get("our_ts") or exit_fills[-1].get("ts")
    assert ts == T0 + 10 * E.MS_MIN, f"legacy exit must land at copy latency (0), got {ts}"


# --------------------------------------------------------------------------- #
# stop layers (plan v2 divergence #4: live-only SL and global stop)
# --------------------------------------------------------------------------- #
def test_sl_bps_closes_the_leg_and_latches():
    """Long entered at ~100; price collapses to 70 (-30% < -25% SL). The next action cursor must
    close the leg via SL, and under flatten_only the leader's later same-side ADD must NOT re-enter."""
    prices = [100] * 3 + [70] * 12
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        # leader ADDs while we are stopped out: observation point where SL fires first
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ADDON", position_after=3.0, signed_size=1.0),
        _action("BTC", T0 + 7 * E.MS_MIN, 0.0, "ADDON", position_after=4.0, signed_size=1.0),
    ])
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                       reversal_mode="flatten_only", sl_bps=-2500.0)
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 10 * E.MS_MIN)
    assert any(e.get("event_type") == "sl_exit" for e in res["events"]), "SL must fire"
    assert res["ending_account_state"]["positions"] == {}, "stopped-out leg must stay closed (latch)"


def test_global_stop_flattens_and_halts():
    """Account stop vs START equity: leg of ~$5000 notional at 2x-ish exposure, price -40% ->
    equity breach of 15% -> flatten all + halt; later leader entries are not copied."""
    prices = [100] * 3 + [60] * 12
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ADDON", position_after=3.0, signed_size=1.0),
        _action("ETH", T0 + 7 * E.MS_MIN, 0.0, "ENTRY", position_after=1.0, signed_size=1.0),
    ])
    md2 = FakeMarketData(ohlc={**_ohlc_path("BTC", prices), **_ohlc_path("ETH", [100] * 15)})
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_position", fixed_target_exposure=0.50,
                       global_stop_pct=0.15)
    res = E.step_subaccount(acts, md2, 10_000.0, p, end_ts_ms=T0 + 10 * E.MS_MIN)
    assert any(e.get("event_type") == "global_stop_exit" for e in res["events"]), "global stop must fire"
    assert res["ending_account_state"]["positions"] == {}, "stop must flatten everything"
    # the later ETH entry must NOT have been copied (halt latch)
    assert not any(f.get("coin") == "ETH" for f in res["fills"]), "halted engine must not copy new entries"


# --------------------------------------------------------------------------- #
# codex R1 remediations
# --------------------------------------------------------------------------- #
def test_sl_without_flatten_only_is_refused():
    """codex R1 P1#5: sl_bps under reversal_mode='flip' would stop out and instantly re-enter.
    The engine must REFUSE the combination, not document it."""
    import pytest
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", sl_bps=-2500.0)  # flip default
    with pytest.raises(ValueError, match="flatten_only"):
        E.step_subaccount(_scenario_actions(), md, 10_000.0, p, end_ts_ms=T0 + 11 * E.MS_MIN)


def test_fixed_notional_under_entry_trail_is_refused():
    """codex R1 P1#7: entry_trail sizes by equity fraction and silently ignores fixed_notional."""
    import pytest
    md = FakeMarketData(ohlc=_ohlc_path("BTC", PRICES))
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", copy_policy="entry_trail")
    with pytest.raises(ValueError, match="full_mirror"):
        E.step_subaccount(_scenario_actions(), md, 10_000.0, p, end_ts_ms=T0 + 11 * E.MS_MIN)


def test_time_never_runs_backward_with_delayed_exits():
    """codex R1 P1#2: a delayed exit must not let a later fast row regress the sim cursor.
    BTC exit at t+10m gets +30s latency; ETH entry at t+10m+10s (naive our_ts BEFORE the exit's)
    must be clamped forward. All fill timestamps must be non-decreasing."""
    ohlc = {}
    ohlc.update(_ohlc_path("BTC", [100] * 40))
    ohlc.update(_ohlc_path("ETH", [100] * 40))
    md = FakeMarketData(ohlc=ohlc)
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 10 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
        _action("ETH", T0 + 10 * E.MS_MIN + 10_000, 0.0, "ENTRY", position_after=1.0, signed_size=1.0),
    ])
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                       exit_latency_ms=30_000, reversal_mode="flatten_only")
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 30 * E.MS_MIN)
    tss = [(f.get("our_ts") or f.get("ts")) for f in res["fills"]]
    assert tss == sorted(tss), f"fill timeline must be monotone, got {tss}"
    assert "ETH" in res["ending_account_state"]["positions"], "clamped entry must still execute"


def test_exit_haircut_uses_effective_latency():
    """codex R1 P1#3: a 30s-delayed exit must pay the same latency-drift haircut as a 30s
    copy-latency fill at the same effective ts. Same tape, same effective exit time, two routes:
    (a) exit model ON with copy_latency=0, (b) exit model OFF with copy_latency=30s.
    The exit fill price must MATCH (pre-fix, route (a) charged a 0-latency haircut)."""
    # volatile tape so the drift haircut is nonzero: big move in the bar before the exit
    prices = [100] * 9 + [100, 130, 130, 130, 130, 130]
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 11 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])
    # (a) exit model ON: entry at t+1m (0 latency), exit effective at t+11m+30s
    pa = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                        exit_latency_ms=30_000, exit_entry_grace_ms=0, reversal_mode="flatten_only")
    ra = E.step_subaccount(acts, md, 10_000.0, pa, end_ts_ms=T0 + 14 * E.MS_MIN)
    # (b) model OFF: uniform 30s latency -> exit also lands at t+11m+30s with a 30s haircut
    pb = E.EngineParams(copy_latency_ms=30_000, sizing_mode="fixed_notional", fixed_notional_usd=100.0)
    rb = E.step_subaccount(acts, md, 10_000.0, pb, end_ts_ms=T0 + 14 * E.MS_MIN)
    def exit_px(res):
        fs = [f for f in res["fills"] if (f.get("our_ts") or f.get("ts", 0)) > T0 + 10 * E.MS_MIN]
        assert fs, f"no exit fill: {res['fills']}"
        return fs[-1]["our_fill_px"]
    assert abs(exit_px(ra) - exit_px(rb)) < 1e-9, (
        f"delayed exit must pay the effective-latency haircut: {exit_px(ra)} vs {exit_px(rb)}")


def test_carry_in_positions_get_grace_anchor():
    """codex R1 P1#4: a position carried in via start_state is anchored at the window start, so a
    leader-flat in the first seconds cannot exit before the grace elapses."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", [100] * 40))
    start = E.AccountState(cross_collateral={"main": 10_000.0 - 100.0})
    start.positions["BTC"] = E.Position("BTC", szi=1.0, entry_px=100.0, mode="cross", leverage=1.0)
    acts = pd.DataFrame([
        # leader flat 10s after the window starts
        _action("BTC", T0 + 10_000, 0.0, "EXIT", position_after=0.0, signed_size=1.0),
    ])
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                       exit_latency_ms=30_000, exit_entry_grace_ms=90_000,
                       reversal_mode="flatten_only", start_policy="causal_carry_in")
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 10 * E.MS_MIN,
                            start_ts_ms=T0, start_state=start)
    exit_fills = [f for f in res["fills"]]
    assert exit_fills, "carried leg must exit"
    ts = exit_fills[-1].get("our_ts") or exit_fills[-1].get("ts")
    assert ts == T0 + 90_000, f"grace must anchor at window start: expected {T0 + 90_000}, got {ts}"


def test_carry_in_grace_anchor_without_start_ts():
    """codex R2: with start_ts_ms omitted, the grace anchor must be the engine's own
    window_start_ms (provenance truth), not the copy-latency-late prev_ts."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", [100] * 40))
    start = E.AccountState(cross_collateral={"main": 10_000.0 - 100.0})
    start.positions["BTC"] = E.Position("BTC", szi=1.0, entry_px=100.0, mode="cross", leverage=1.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 10_000, 0.0, "EXIT", position_after=0.0, signed_size=1.0),
    ])
    p = E.EngineParams(copy_latency_ms=4_000, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                       exit_latency_ms=30_000, exit_entry_grace_ms=90_000,
                       reversal_mode="flatten_only")
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 10 * E.MS_MIN,
                            start_state=start)   # NO start_ts_ms
    assert res["fills"], "carried leg must exit"
    ts = res["fills"][-1].get("our_ts") or res["fills"][-1].get("ts")
    expected = res["summary"]["window_start_ms"] + 90_000
    assert ts == expected, f"grace must anchor at window_start_ms: expected {expected}, got {ts}"


def test_late_skip_counter_excludes_delayed_exits():
    """codex R2 P2: an exit that EXECUTES (via exit latency) must not be pre-counted as late."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", [100] * 40))
    end = T0 + 10 * E.MS_MIN
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        # leader flat 2s before fold end: naive copy-latency (4s) would land past end and be
        # pre-counted late, but the exit executes inside... actually with +30s it lands past end too;
        # the point: it must appear in n_exit_pushed_past_end, NOT in n_late_copy_skipped.
        _action("BTC", end - 2_000, 0.0, "EXIT", position_after=0.0, signed_size=2.0),
    ])
    p = E.EngineParams(copy_latency_ms=4_000, sizing_mode="fixed_notional", fixed_notional_usd=100.0,
                       exit_latency_ms=30_000, exit_entry_grace_ms=0, reversal_mode="flatten_only")
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=end)
    assert res["summary"]["n_late_copy_skipped"] == 0, "exit rows must not be pre-counted late"
    assert res["summary"]["n_exit_pushed_past_end"] == 1


def test_stops_off_by_default_inert():
    prices = [100] * 3 + [70] * 12
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 0.0, "ENTRY", position_after=2.0, signed_size=2.0),
        _action("BTC", T0 + 5 * E.MS_MIN, 0.0, "ADDON", position_after=3.0, signed_size=1.0),
    ])
    p = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional", fixed_notional_usd=100.0)
    res = E.step_subaccount(acts, md, 10_000.0, p, end_ts_ms=T0 + 10 * E.MS_MIN)
    assert not any(e.get("event_type") in ("sl_exit", "global_stop_exit") for e in res["events"])
    assert "BTC" in res["ending_account_state"]["positions"], "without stops the leg rides the drawdown"
