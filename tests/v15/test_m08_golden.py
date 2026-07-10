"""GOLDEN regression test for v15_m08_survival trust-audit fixes (2026-07-10).

Locks the fail-closed behaviours so a stale / evidence-less / look-ahead-sourced wallet cannot come out
"survived" and slip into the live cohort:
- P0#2/#4: a stress run with ZERO fills (empty/filtered stream or NaN sizing skipped by M7) -> KILL data-gap,
- P0#3: run_m08 without fold-pure --m04-dir (the global M4 map is look-ahead) fails closed unless explicitly
  opted into the diagnostic path.
(The run-loop guards m4_conf==0 -> kill and non-finite quality_weight -> kill are additionally codex-reviewed.)
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m08_survival as M8

M = M8.M8Manifest()
T0 = 1_700_000_000_000
DAY = 86_400_000


def _stub(monkeypatch, outcomes):
    seq = iter(outcomes)

    def fake(actions, md, cap, t0, t1, m):
        wiped, n_fills = next(seq)
        return {"wiped": wiped, "ruin": wiped, "backstop": False, "account_ruin": False,
                "final_equity": (0.0 if wiped else cap), "roe": 0.0, "max_dd": 0.0,
                "time_to_ruin_ms": None, "n_fills": n_fills, "indeterminate_frac": 0.0}
    monkeypatch.setattr(M8, "_run_stress", fake)


def test_p0_zero_fills_fail_closed_to_kill(monkeypatch):
    # first (big-slice) run has n_fills=0 -> no replayable survival evidence -> KILL, not "survived".
    _stub(monkeypatch, [(False, 0)])
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] == "kill"
    assert out["survival_outcome"] == "no_fills_data_gap"
    assert out["max_survivable_slice"] == 0.0


def test_nonzero_fills_still_survives(monkeypatch):
    # sanity: with real fills and not wiped, the module still returns survived (fix is narrow).
    _stub(monkeypatch, [(False, 12)])
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] in ("full_weight", "uncertain") and out["survival_outcome"] == "survived"


def test_min_probe_zero_fills_fail_closed(monkeypatch):
    # big probe wipes (with fills), min probe "survives" but with ZERO fills -> evidence-less -> KILL, not a
    # 0.25 copyability_fail leak. (codex completion gap)
    _stub(monkeypatch, [(True, 8), (False, 0)])  # big: wiped w/ fills; min: not wiped but no fills
    out = M8.survival_tier(pd.DataFrame(), None, T0, T0 + 20 * DAY, 1000.0, M)
    assert out["tier"] == "kill" and out["survival_outcome"] == "no_fills_at_min_data_gap"


def test_p0_1_stale_wallet_killed(monkeypatch):
    # last in-window action 20 days before the decision (t1) -> stale (>14d) -> KILL before any stress.
    # _run_stress must NOT be called for a stale wallet.
    called = {"n": 0}
    monkeypatch.setattr(M8, "_run_stress", lambda *a, **k: called.__setitem__("n", called["n"] + 1) or {})
    t1 = T0 + 60 * DAY
    acts = pd.DataFrame({"ts": [t1 - 20 * DAY]})  # dark for 20 days before decision
    out = M8.survival_tier(acts, None, T0, t1, 1000.0, M)
    assert out["tier"] == "kill" and out["survival_outcome"] == "stale_inactive"
    assert called["n"] == 0  # never stressed


def test_p0_1_recent_wallet_proceeds(monkeypatch):
    # last action 5 days before the decision -> fresh (<14d) -> proceeds to stress (here: survives).
    _stub(monkeypatch, [(False, 10)])
    t1 = T0 + 60 * DAY
    acts = pd.DataFrame({"ts": [t1 - 5 * DAY]})
    out = M8.survival_tier(acts, None, T0, t1, 1000.0, M)
    assert out["survival_outcome"] == "survived"


def test_p0_3_global_m04_lookahead_fails_closed(tmp_path):
    # run_m08 must refuse the look-ahead global M4 path unless allow_global_m04=True. It reaches the guard
    # after loading the pool + folds, so provide minimal parquets; the guard should raise before M4 load.
    d = tmp_path
    pool = pd.DataFrame({"entity_id": [1], "fold_id": [1], "in_pool": [True], "investable": [True]})
    pool.to_parquet(d / "m06b_pool.parquet")
    folds = pd.DataFrame({"fold_id": [1], "train_start": [pd.Timestamp("2026-01-01")],
                          "test_start": [pd.Timestamp("2026-02-12")]})
    folds.to_parquet(d / "m03_folds.parquet")
    with pytest.raises(ValueError, match="look-ahead"):
        M8.run_m08(d, d, M, m04_dir=None, allow_global_m04=False)
