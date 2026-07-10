"""V15 M8 survival-tiering tests. Empirical: stress survival on synthetic sources + tier logic.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m08.py -q
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))   # reuse test_m07 fakes
import v15_m08_survival as M8  # noqa: E402
import v15_m07_engine as E  # noqa: E402
import test_m07 as T7  # noqa: E402

DAY = 86_400_000
T0 = T7.T0


def _ohlc_flat(coin, px=100.0, n=None):
    n = n or 31 * 1440
    return T7._flat_ohlc(coin, T0, n, px)


def _ohlc_crash(coin, px=100.0, crash_min=1500, crash_px=1.0, n=None):
    n = n or 31 * 1440
    o = T7._flat_ohlc(coin, T0, n, px)
    mins, op, hi, lo, cl = o[coin]
    for arr in (op, hi, lo, cl):
        arr[crash_min:] = crash_px
    return {coin: (mins, op, hi, lo, cl)}


M = M8.M8Manifest()


def test_survival_full_weight_on_benign_source():
    # low-leverage source on flat price, RECENTLY active -> survives the stress slice comfortably -> full_weight.
    # (action at t1-2d so it passes the 14d staleness gate; staleness is exercised separately in test_m08_golden.)
    md = T7.FakeMarketData(ohlc=_ohlc_flat("BTC"))
    acts = pd.DataFrame([T7._action("BTC", T0 + 18 * DAY, 0.3)])
    out = M8.survival_tier(acts, md, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] == "full_weight"
    assert out["survival_outcome"] == "survived"
    assert out["copyability_fail_at_stress"] is False


def test_survival_kill_when_ruins_at_min_size():
    # very high leverage + a deep crash -> wiped even at the minimum slice -> KILL (mechanical).
    md = T7.FakeMarketData(ohlc=_ohlc_crash("BTC", crash_min=1500, crash_px=1.0), maxlev=10.0)
    acts = pd.DataFrame([T7._action("BTC", T0 + 10 * E.MS_MIN, 8.0),     # 8x
                         T7._action("BTC", T0 + 25 * DAY, 8.0)])
    out = M8.survival_tier(acts, md, T0, T0 + 30 * DAY, 5000.0, M)
    assert out["tier"] == "kill"
    assert out["survival_outcome"] == "ruin_at_min_size"
    assert out["max_survivable_slice"] == 0.0


def _stub_runs(monkeypatch, outcomes):
    """Patch _run_stress to return queued outcomes (by call order): big, [min], [small]."""
    calls = {"i": 0}
    def fake(actions, md, eq, t0, t1, m):
        o = outcomes[min(calls["i"], len(outcomes) - 1)]; calls["i"] += 1
        return {"wiped": o[0], "ruin": o[0], "backstop": False, "account_ruin": o[0],
                "final_equity": 0.0 if o[0] else 1.0, "roe": 0.0, "max_dd": 0.5,
                "time_to_ruin_ms": None, "n_fills": 10, "indeterminate_frac": o[1]}
    monkeypatch.setattr(M8, "_run_stress", fake)


def test_copyability_survives_smaller_stays_full_weight(monkeypatch):
    # codex r1#1+#2: wiped at big, SURVIVES at min -> copyability sizing signal, tier = full_weight
    # (NOT uncertain/downgraded). big wiped, min survives, small survives.
    _stub_runs(monkeypatch, [(True, 0.0), (False, 0.0), (False, 0.0)])
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] == "full_weight"
    assert out["survival_outcome"] == "copyability_fail_at_stress"
    assert out["copyability_fail_at_stress"] is True
    assert out["max_survivable_slice"] > 0


def test_smaller_probe_never_below_min(monkeypatch):
    # codex r2#1: big=100, frac 0.25 -> smaller probe=25 < min 50 -> SKIP the probe; min already
    # survived so max_survivable_slice = min_slice_capital (>= min), never the sub-min 25.
    _stub_runs(monkeypatch, [(True, 0.0), (False, 0.0)])   # big wiped, min survives (only 2 calls)
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 100.0, M)  # 100*0.25=25 < min 50
    assert out["tier"] == "full_weight"
    assert out["copyability_fail_at_stress"] is True
    assert out["max_survivable_slice"] >= M.min_slice_capital


def test_kill_only_when_min_also_wipes(monkeypatch):
    # codex r1#2: KILL only when the MIN slice also wipes.
    _stub_runs(monkeypatch, [(True, 0.0), (True, 0.0)])   # big wiped, min wiped
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] == "kill" and out["max_survivable_slice"] == 0.0


def test_indeterminate_heavy_caps_uncertain(monkeypatch):
    # survives at big but indeterminate-heavy -> UNCERTAIN.
    _stub_runs(monkeypatch, [(False, 0.9)])   # not wiped, 90% indeterminate
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] == "uncertain" and out["survival_outcome"] == "survived"


def test_entity_pre_m8_max_is_raw_formula():
    m = M8.M8Manifest()
    v = M8.entity_pre_m8_max(0.02, 1.0, m)
    assert v == pytest.approx(0.02 * 1.0 * 1.0 * m.m9_static_per_entity_cap * m.nominal_capital)


def test_entity_pre_m8_max_uses_configured_nominal_capital():
    m = M8.M8Manifest(nominal_capital=518.0)
    v = M8.entity_pre_m8_max(0.02, 1.0, m)
    assert v == pytest.approx(0.02 * 1.0 * 1.0 * m.m9_static_per_entity_cap * 518.0)


def test_attribution_flags_missing_m07_rows():
    # codex r1#5: a tiered entity with no M7 row is flagged, excluded from metrics (not roe=0).
    tiers = pd.DataFrame([
        {"entity_id": 1, "fold_id": 1, "m4_tier": "CLEAN", "tier": "full_weight"},
        {"entity_id": 2, "fold_id": 1, "m4_tier": "CLEAN", "tier": "full_weight"},
    ])
    summ = pd.DataFrame([{"entity_id": 1, "fold_id": 1, "roe_engine": 0.4, "ruin": False}])  # entity 2 missing
    att = M8.tier_attribution(tiers, summ)
    fw = att[(att.tier_axis == "m8_tier") & (att.tier == "full_weight")].iloc[0]
    assert fw["n_entities"] == 2 and fw["n_missing_m07"] == 1
    assert fw["mean_roe"] == pytest.approx(0.4)   # only the present entity, NOT (0.4+0)/2


def test_worst_tier_inferential_downgrades_never_kills():
    # survival says full_weight; a suspicious inferential scorer downgrades to suspicious, NOT kill.
    M8.TIER_ORDER  # exists
    t = M8._worst_tier("full_weight", {"hedge_smell": "suspicious", "funder_provenance": "no_flag",
                                       "carry_timing": "no_flag", "behavioral_linkage": "no_flag"})
    assert t == "suspicious"
    # inferential can never produce kill even if survival is full_weight
    t2 = M8._worst_tier("full_weight", {"hedge_smell": "suspicious", "funder_provenance": "uncertain",
                                        "carry_timing": "no_flag", "behavioral_linkage": "no_flag"})
    assert t2 == "suspicious"
    # a KILL survival tier is preserved (worst wins)
    assert M8._worst_tier("kill", {"hedge_smell": "no_flag", "funder_provenance": "no_flag",
                                   "carry_timing": "no_flag", "behavioral_linkage": "no_flag"}) == "kill"


def test_inferential_scorers_phase2_stub():
    out = M8.inferential_scorers(1, 1)
    assert all(out[k] == "no_flag" for k in ["hedge_smell", "funder_provenance", "carry_timing", "behavioral_linkage"])
    assert out["scorers_phase"] == "phase2_stub"
    assert M8._worst_tier("full_weight", out) == "uncertain"


def test_tier_attribution_by_m4_and_m8():
    tiers = pd.DataFrame([
        {"entity_id": 1, "fold_id": 1, "m4_tier": "CLEAN", "tier": "full_weight"},
        {"entity_id": 2, "fold_id": 1, "m4_tier": "CLEAN", "tier": "uncertain"},
        {"entity_id": 3, "fold_id": 1, "m4_tier": "UNCERTAIN", "tier": "suspicious"},
    ])
    summ = pd.DataFrame([
        {"entity_id": 1, "fold_id": 1, "roe_engine": 0.5, "ruin": False},
        {"entity_id": 2, "fold_id": 1, "roe_engine": -0.2, "ruin": False},
        {"entity_id": 3, "fold_id": 1, "roe_engine": -0.9, "ruin": True},
    ])
    att = M8.tier_attribution(tiers, summ)
    assert set(att["tier_axis"]) == {"m4_tier", "m8_tier"}
    fw = att[(att.tier_axis == "m8_tier") & (att.tier == "full_weight")].iloc[0]
    assert fw["n_entities"] == 1 and fw["mean_roe"] == pytest.approx(0.5)
    susp = att[(att.tier_axis == "m8_tier") & (att.tier == "suspicious")].iloc[0]
    assert susp["ruin_rate"] == 1.0     # the suspicious cohort here did ruin -> label justified
