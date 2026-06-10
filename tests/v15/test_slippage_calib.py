"""V15 slippage calibrator tests (v11-fills-v1). Pure functions; no DB/engine.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_slippage_calib.py -q
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_slippage_calib as C  # noqa: E402


def test_robust_base_nonneg_floor():
    # a median-negative slip (lagging ref) floors to 0 (taker cost can't be negative)
    assert C._robust_base_bps(np.array([-5.0, -4.0, -3.0, -2.0, -1.0])) == 0.0
    # positive median passes through (winsorized)
    b = C._robust_base_bps(np.array([1.0, 2.0, 3.0, 4.0, 5.0]))
    assert b == pytest.approx(3.0, abs=0.5)


def test_robust_base_winsorizes_outliers():
    # a huge outlier must not move the median much (robust)
    base = np.array([1.0, 1.1, 0.9, 1.2, 0.8, 1.0, 1.0, 1.0, 1.0, 5000.0])
    b = C._robust_base_bps(base)
    assert b < 2.0


def test_shrinkage_toward_prior():
    prior = 8.0
    # large n -> close to empirical
    near_emp = C._shrink(1.0, n=1000, prior_bps=prior, k=20.0)
    assert near_emp < 2.0
    # tiny n -> close to prior
    near_prior = C._shrink(1.0, n=2, prior_bps=prior, k=20.0)
    assert near_prior > 6.0
    # nan empirical -> prior
    assert C._shrink(float("nan"), n=5, prior_bps=prior, k=20.0) == prior


def test_realized_slip_sign_and_causal_ref():
    # buy above ref -> positive (adverse); sell below ref -> positive (adverse)
    df = pd.DataFrame({
        "coin": ["BTC", "BTC"], "px": [101.0, 99.0], "sz": [1.0, 1.0],
        "side": ["B", "A"], "time": [1000, 2000],
        "sgn": [1.0, -1.0], "notional": [101.0, 99.0],
    })
    out = C.realized_slip_bps(df, lambda c, t: 100.0)
    assert len(out) == 2
    assert (out["slip_bps"] > 0).all()  # both adverse
    assert out["slip_bps"].iloc[0] == pytest.approx(100.0)   # +1% = 100bps
    assert out["slip_bps"].iloc[1] == pytest.approx(100.0)


def test_realized_slip_drops_missing_ref():
    df = pd.DataFrame({
        "coin": ["X", "Y"], "px": [10.0, 10.0], "sz": [1.0, 1.0],
        "side": ["B", "B"], "time": [1, 2], "sgn": [1.0, 1.0], "notional": [10.0, 10.0],
    })
    # Y has no ref (None) -> dropped
    out = C.realized_slip_bps(df, lambda c, t: 9.9 if c == "X" else None)
    assert list(out["coin"]) == ["X"]


def _slipped(coin_slips: dict, t_base=0):
    rows = []
    for coin, (slips, times) in coin_slips.items():
        for s, t in zip(slips, times):
            rows.append({"coin": coin, "slip_bps": s, "time": t})
    return pd.DataFrame(rows)


def test_coin_class_by_adv():
    assert C.coin_class("BTC", 2_000e6) == "major"
    assert C.coin_class("NEAR", 33e6) == "midcap"
    assert C.coin_class("BIO", 3e6) == "microcap"
    assert C.coin_class("xyz:MU", 1e6) == "hip3"        # xyz: always hip3 regardless of ADV
    assert C.coin_class("WAT", None) == "unknown"


def test_calibrate_own_empirical_and_class_comp():
    # AAA (major, 30 fills ~2bps) -> own_empirical; BBB (major, 3 fills) -> inherits MAJOR class comp.
    rng = np.random.default_rng(0)
    aaa = (list(rng.normal(2.0, 0.3, 30)), list(range(30)))
    bbb = ([5.0, 5.0, 5.0], [0, 1, 2])
    slipped = _slipped({"AAA": aaa, "BBB": bbb})
    adv = {"AAA": 500e6, "BBB": 300e6}                   # both majors
    calib = C.calibrate(slipped, {1: 10_000}, n_min=20, adv_map=adv)
    full = calib["full_window"]
    assert full["AAA"]["covered"] is True and full["AAA"]["base_source"] == "own_empirical"
    assert full["AAA"]["base_half_spread_bps"] < C.PRIOR_BASE_BPS
    # BBB not own-covered -> inherits the major class comp (= AAA's base, the only covered major)
    assert full["BBB"]["covered"] is True
    assert full["BBB"]["base_source"] == "class_comp:major"
    assert full["BBB"]["base_half_spread_bps"] == pytest.approx(full["AAA"]["base_half_spread_bps"])
    assert full["AAA"]["impact_k_bps"] == C.PRIOR_IMPACT_K_BPS  # slope always prior


def test_calibrate_asof_purity():
    # fold 1 test_start=50 -> only fills with time<=50 used. 25 early fills (covered) + 25 late (excluded).
    early = (list(np.full(25, 2.0)), list(range(0, 25)))
    late = (list(np.full(25, 9.9)), list(range(100, 125)))
    slipped = _slipped({"AAA": (early[0] + late[0], early[1] + late[1])})
    calib = C.calibrate(slipped, {1: 50, 2: 200}, n_min=20)
    f1 = calib["per_fold_asof"]["1"]["AAA"]
    f2 = calib["per_fold_asof"]["2"]["AAA"]
    # fold 1: only the 25 early (=2bps) fills, covered
    assert f1["n_fills"] == 25
    assert f1["covered"] is True
    # fold 2: all 50 fills as-of
    assert f2["n_fills"] == 50


def test_all_nan_slips_get_no_own_empirical():
    # codex r1#1: an all-NaN coin must NOT get an own empirical base (no bogus 0). v2: it still gets a
    # base via class/global/prior (base_source != own_empirical, emp_median None).
    assert np.isnan(C._robust_base_bps(np.array([np.nan, np.nan, np.nan])))
    slipped = pd.DataFrame({"coin": ["BAD"] * 25, "slip_bps": [np.nan] * 25, "time": list(range(25))})
    calib = C.calibrate(slipped, {1: 1000}, n_min=20)
    bad = calib["full_window"]["BAD"]
    assert bad["base_source"] != "own_empirical"
    assert bad["emp_median_bps"] is None
    assert bad["covered"] is True            # v2: always calibrated (global/prior)


def test_missing_ref_coin_gets_global_or_prior_not_vanish():
    # codex r1#2: a coin in the raw universe with no usable fills must still appear, base via global/prior.
    slipped = _slipped({"AAA": (list(np.full(25, 2.0)), list(range(25)))})
    calib = C.calibrate(slipped, {1: 1000}, n_min=20, coin_universe=["AAA", "GHOST"], adv_map={"AAA": 500e6})
    assert "GHOST" in calib["full_window"]
    g = calib["full_window"]["GHOST"]
    assert g["base_source"] in ("global_median", "prior") or g["base_source"].startswith("class_comp")
    assert g["covered"] is True


def test_manifest_fields_frozen():
    slipped = _slipped({"AAA": (list(np.full(25, 2.0)), list(range(25)))})
    calib = C.calibrate(slipped, {1: 1000})
    for key in ["version", "reference", "estimator", "shrinkage", "n_min_covered",
                "prior_base_bps", "per_fold_asof", "full_window"]:
        assert key in calib
    assert calib["version"] == "v11-fills-v2"
    assert "causal" in calib["reference"]
