"""Familywise corrections orchestration (codex code-gate #2/#3): runtime failures stay
in the max-stat family (-inf observed / Holm p = 1, family never shrinks), all-NaN LCB
outcomes TRIGGER the Holm fallback (never evade it), and the inherited v25 14d-block
robustness criterion fails any config whose 14d conclusion disagrees with 7d."""
import json

import numpy as np
import pytest

from v26_estimand import holm_fallback, joint_maxstat
from v26_run_grid import corrections_and_verdict

FOLDS = [{"fold": 1, "test_days": 40}]


def good_agg(cid="cfg_good", mean=0.005, n_days=40):
    rng = np.random.default_rng(7)
    return {"config_id": cid, "runtime_failure": False,
            "excess_series": rng.normal(mean, 0.001, n_days),
            "criteria": {"min_test_days_60": True}}


def failed_agg(cid="cfg_fail"):
    return {"config_id": cid, "runtime_failure": True, "error": "boom"}


def nan_agg(cid="cfg_nan", n_days=40):
    return {"config_id": cid, "runtime_failure": False,
            "excess_series": np.full(n_days, np.nan), "criteria": {}}


class TestFamilyNeverShrinks:
    def test_runtime_failure_stays_in_maxstat_family(self, tmp_path):
        aggs = [good_agg(), failed_agg()]
        v = corrections_and_verdict(aggs, FOLDS, family_size=2, out_dir=tmp_path,
                                    n_resamples=200, holm_resamples=400)
        # failures alone do NOT trigger the fallback -- they enter the joint max at
        # -inf (worst case) and can never pass
        assert v["method"] == "joint_maxstat"
        assert v["n_runtime_failures"] == 1
        bad = aggs[1]
        assert bad["adjusted_lcb"] == float("-inf")
        assert bad["mean_excess"] == float("-inf")
        assert bad["holm_p"] == 1.0
        assert not bad["PASS"] and not bad["estimand_pass"]
        ok = aggs[0]
        assert np.isfinite(ok["adjusted_lcb"]) and ok["estimand_pass"]

    def test_joint_maxstat_failures_param_inert_but_present(self):
        rng = np.random.default_rng(0)
        M = rng.normal(0.001, 0.01, size=(3, 42))
        a = joint_maxstat(M, [21, 21], n_resamples=300)
        b = joint_maxstat(M, [21, 21], n_resamples=300, n_failures=5)
        # -inf failure rows never support the max, but the family is reported whole
        assert np.allclose(a["lcb"], b["lcb"])
        assert b["family_size"] == 3 + 5 and a["family_size"] == 3

    def test_only_failures_fails_closed(self, tmp_path):
        aggs = [failed_agg("f1"), failed_agg("f2")]
        v = corrections_and_verdict(aggs, FOLDS, family_size=2, out_dir=tmp_path,
                                    n_resamples=100, holm_resamples=200)
        assert v["n_passers"] == 0 and v["winner"] is None
        assert all(not a["PASS"] for a in aggs)

    def test_holm_fallback_appends_failure_rows(self):
        M = np.full((1, 20), 0.005)
        r = holm_fallback(M, [20], family_size=3, n_resamples=100, n_failures=2)
        assert len(r["p_raw"]) == 3
        assert r["p_raw"][1] == 1.0 and r["p_raw"][2] == 1.0
        assert r["lcb"][1] == -np.inf and r["mean"][1] == -np.inf


class TestAllNaNTriggersHolm:
    def test_joint_maxstat_raises_on_all_nan_row(self):
        M = np.full((2, 20), 0.002)
        M[1, :] = np.nan                    # all-excluded series: non-finite LCB
        with pytest.raises(RuntimeError, match="non-finite"):
            joint_maxstat(M, [20], n_resamples=100)

    def test_corrections_falls_back_and_persists_trigger(self, tmp_path):
        aggs = [good_agg(), nan_agg()]
        v = corrections_and_verdict(aggs, FOLDS, family_size=2, out_dir=tmp_path,
                                    n_resamples=100, holm_resamples=200)
        assert v["method"] == "holm_fallback"       # triggered, not evaded
        with open(tmp_path / "manifest_grid.json") as fh:
            man = json.load(fh)
        assert "holm_trigger" in man
        # the all-NaN config can never look significant: p = 1, lcb = -inf
        bad = aggs[1]
        assert not bad["estimand_pass"] and bad["adjusted_lcb"] == -np.inf

    def test_holm_all_nan_row_p_is_one(self):
        M = np.vstack([np.full(20, 0.005), np.full(20, np.nan)])
        r = holm_fallback(M, [20], family_size=2, n_resamples=100)
        assert r["p_raw"][1] == 1.0 and r["lcb"][1] == -np.inf


class TestBlockRobustness14d:
    def test_14d_recomputed_and_agreeing_config_passes(self, tmp_path):
        aggs = [good_agg()]
        v = corrections_and_verdict(aggs, FOLDS, family_size=1, out_dir=tmp_path,
                                    n_resamples=300, holm_resamples=600)
        a = aggs[0]
        assert "c_maxstat_14d" in v
        assert np.isfinite(a["adjusted_lcb_14d"])
        assert a["criteria"]["block_robustness_agree"]
        assert a["PASS"]

    def test_14d_disagreement_fails_the_config(self, tmp_path, monkeypatch):
        # inherited v25 frozen criterion: if the 14d-block corrected conclusion
        # disagrees with 7d, the config FAILS
        import v26_estimand

        def stub(M, seg_lens, n_resamples=None, block_days=7, n_failures=0, **kw):
            n = M.shape[0]
            lcb = np.full(n, 0.01 if block_days == 7 else -0.01)
            return {"method": "joint_maxstat", "c_maxstat": 0.0,
                    "mean": np.full(n, 0.02), "lcb": lcb,
                    "n_resamples": n_resamples, "family_size": n + n_failures}

        monkeypatch.setattr(v26_estimand, "joint_maxstat", stub)
        aggs = [good_agg()]
        v = corrections_and_verdict(aggs, FOLDS, family_size=1, out_dir=tmp_path,
                                    n_resamples=100, holm_resamples=200)
        a = aggs[0]
        assert v["method"] == "joint_maxstat"
        assert a["estimand_pass"]                   # 7d alone passes ...
        assert not a["criteria"]["block_robustness_agree"]
        assert not a["PASS"]                        # ... but the config FAILS
        assert v["n_passers"] == 0
