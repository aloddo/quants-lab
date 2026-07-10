"""GOLDEN regression test for v15_m09_sim trust-audit fixes (2026-07-10).

Locks the fail-CLOSED allocation-engine behaviours from the codex m09 audit:
- P0: a non-finite portfolio equity can NOT bypass the global-DD de-risk or the G4 intra-fold kill.
- P1: min_notional_feasible fails closed on non-finite inputs; the check is wired into allocation.
- P1: anti_corr drops the lower-ranked entity on an explicit non-finite correlation (+ deterministic ties).
- P1: cap_aware_waterfill / apply_gross_budget / effective_caps fail closed on non-finite weights/caps/lev/eq.
- P2: cap-aware water-fill never reports cash_funded above the residual cap (floored, not rounded up).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m09_sim as M9  # noqa: E402

M = M9.M9Manifest()


# ---- P0: DD / G4 kill can't be bypassed by non-finite equity ----
def test_g4_kill_fails_closed_on_nan_equity():
    df = pd.DataFrame({"ts": [1, 2], "portfolio_equity": [100.0, np.nan]})
    out = M9.g4_intrafold_kill(df, 100.0, M)
    assert out["killed"] is True and out["g4_pass"] is False and out["kill_ts"] == 2
    assert np.isfinite(out["fold_end_equity"])


def test_g4_kill_fails_closed_on_inf_equity():
    df = pd.DataFrame({"ts": [1, 2], "portfolio_equity": [100.0, np.inf]})
    out = M9.g4_intrafold_kill(df, 100.0, M)
    assert out["killed"] is True and out["g4_pass"] is False   # inf must NOT make g4_pass True


def test_global_dd_fails_closed_on_nonfinite_equity():
    df = pd.DataFrame({"ts": [1, 2], "portfolio_equity": [100.0, np.nan]})
    assert M9.detect_global_dd_derisk(df, M) == 2   # non-finite -> immediate de-risk breach


def test_g4_normal_finite_path_unchanged():
    df = pd.DataFrame({"ts": [1, 2, 3], "portfolio_equity": [100.0, 40.0, 90.0]})
    out = M9.g4_intrafold_kill(df, 100.0, M)   # 40 <= 50% of 100 -> kill at ts 2, frozen at 40
    assert out["killed"] is True and out["kill_ts"] == 2 and out["fold_end_equity"] == 40.0


# ---- P1: min_notional_feasible fail-closed on non-finite ----
def test_min_notional_nonfinite_inputs_fail_closed():
    assert M9.min_notional_feasible(0.1, 1.0, np.nan, M)[0] is False       # nan slice
    assert M9.min_notional_feasible(0.1, np.inf, 100.0, M)[0] is False     # inf accessible
    assert M9.min_notional_feasible(np.inf, 1.0, 1.0, M)[0] is False       # inf frac
    assert M9.min_notional_feasible(0.1, 1.5, 100.0, M)[0] is False        # accessible out of range
    assert M9.min_notional_feasible(0.05, 0.9, 500.0, M)[0] is True        # good case still passes


# ---- P1: anti-corr NaN corr + deterministic ties ----
def test_anti_corr_nonfinite_corr_drops_lower_ranked():
    cand = pd.DataFrame({"entity_id": [1, 2], "select_priority": [2.0, 1.0]})
    out = M9.anti_corr_select(cand, {(2, 1): np.nan}, M)
    sel = set(out[out["anti_corr_selected"]]["entity_id"])
    assert sel == {1}   # higher-ranked kept, lower dropped on corrupt corr (not both selected)


def test_anti_corr_tie_deterministic():
    a = M9.anti_corr_select(pd.DataFrame({"entity_id": [1, 2], "select_priority": [1.0, 1.0]}), {(1, 2): 0.9}, M)
    b = M9.anti_corr_select(pd.DataFrame({"entity_id": [2, 1], "select_priority": [1.0, 1.0]}), {(1, 2): 0.9}, M)
    keep_a = set(a[a["anti_corr_selected"]]["entity_id"])
    keep_b = set(b[b["anti_corr_selected"]]["entity_id"])
    assert keep_a == keep_b == {1}   # entity_id asc tie-break -> both orders keep entity 1


# ---- P1: allocation arithmetic fail-closed ----
def test_waterfill_nonfinite_weight_dropped():
    out = M9.cap_aware_waterfill({1: np.inf, 2: 1.0}, {}, {1: 100.0, 2: 100.0}, M, 100.0, 100.0)
    assert 1 not in out["per_entity"]                       # inf desired weight dropped
    assert np.isfinite(out["cash_funded_total"])


def test_waterfill_nonfinite_cash_raises():
    with pytest.raises(ValueError, match="non-finite"):
        M9.cap_aware_waterfill({1: 1.0}, {}, {1: 100.0}, M, np.nan, 100.0)


def test_waterfill_cap_not_exceeded_by_rounding():
    out = M9.cap_aware_waterfill({1: 1.0}, {}, {1: 0.0000006}, M, 1.0, 1.0)
    assert out["per_entity"][1]["cash_funded"] <= 0.0000006   # floored, not rounded up past the cap


def test_gross_budget_nan_leverage_zeroes_entity():
    scaled, returned, gross = M9.apply_gross_budget({1: 100.0}, {}, {1: np.nan}, 1500.0)
    assert scaled[1] == 0.0 and np.isfinite(returned) and np.isfinite(gross)


def test_gross_budget_mixed_nan_leverage_finite_gross():
    # codex m09 r2: one NaN-lev entity (zeroed) + a valid entity that triggers scaling must NOT return NaN gross.
    scaled, returned, gross = M9.apply_gross_budget({1: 100.0, 2: 1000.0}, {}, {1: np.nan, 2: 10.0}, 100.0)
    assert scaled[1] == 0.0
    assert np.isfinite(returned) and np.isfinite(gross)
    assert gross <= 100.0 + 1e-6                            # budget binds, finite


def test_effective_caps_nonfinite_equity_raises():
    with pytest.raises(ValueError, match="non-finite"):
        M9.effective_caps(pd.DataFrame([{"entity_id": 1, "max_survivable_slice": np.inf}]), M, np.inf)


# ---- P0 (chained): a NaN in the engine equity path is killed + never leaks into finite outputs ----
from test_m09 import _FakeEng  # noqa: E402


class _NaNPathEng(_FakeEng):
    def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                        start_state=None, entity_id=None, fold_id=None):
        mid = (start_ts_ms + end_ts_ms) // 2
        return {  # equity path goes NaN mid-fold -> corrupt state
            "equity": [{"ts": start_ts_ms, "subaccount_equity": start_equity},
                       {"ts": mid, "subaccount_equity": float("nan")},
                       {"ts": end_ts_ms, "subaccount_equity": start_equity}],
            "ending_account_state": {"cross_collateral": {"USDC": start_equity},
                                     "cooldown_until_ms": 0, "positions": {}},
            "summary": {"final_equity": float("nan")},
        }


def test_chained_nan_equity_path_killed_and_finite(tmp_path):
    folds = pd.DataFrame([{"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01",
                           "pretest_start": "2024-12-01", "pretest_end_excl": "2025-01-01",
                           "test_start": "2025-01-01", "test_end_excl": "2025-01-02"}])
    m06b = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 1.0}])
    m08 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok",
                         "max_survivable_slice": np.inf}])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"}])

    def _acts(wallet, t0, t1):
        return None if wallet is None else pd.DataFrame({"target_exposure_pct": [1.0, 1.0]})

    out = M9.run_m09_chained(m06b, m08, m04, folds, _NaNPathEng(), md=None, acts_loader=_acts,
                             m=M9.M9Manifest(per_entity_cap=1.0), b0=500.0, out_dir=str(tmp_path))
    # a corrupt (NaN) equity path must fire an intervention AND leave EVERY reported scalar output finite
    for k in ("final_equity", "max_chained_dd", "chained_roe", "chained_calmar", "top_entity_pnl_share"):
        assert np.isfinite(out[k]), f"{k} non-finite: {out[k]}"
    assert out["any_intervention"] is True


def test_chained_losing_strategy_top_share_finite(tmp_path):
    # codex m09 r4: a losing strategy must yield a FINITE, in-[0,1] top_entity_pnl_share (max-concentration
    # sentinel 1.0), NOT inf -- else m10's finite/[0,1] validation rejects it as malformed instead of failing G7.
    folds = pd.DataFrame([{"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01",
                           "pretest_start": "2024-12-01", "pretest_end_excl": "2025-01-01",
                           "test_start": "2025-01-01", "test_end_excl": "2025-01-02"}])
    m06b = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 1.0}])
    m08 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok",
                         "max_survivable_slice": np.inf}])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"}])

    def _acts(wallet, t0, t1):
        return None if wallet is None else pd.DataFrame({"target_exposure_pct": [1.0, 1.0]})

    eng = _FakeEng(ret_by_eid={1: -0.5})   # -50% -> losing strategy
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts,
                             m=M9.M9Manifest(per_entity_cap=1.0), b0=500.0, out_dir=str(tmp_path))
    ts = out["top_entity_pnl_share"]
    assert np.isfinite(ts) and 0.0 <= ts <= 1.0
