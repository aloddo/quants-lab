"""V15 M10 gate-evaluation + significance (percentile ladder, bivariate G6, G7 ablation, verdict).

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m10.py -q
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m10_gates as M10  # noqa: E402

# small required-N so synthetic nulls (~100 samples) satisfy the count gate (codex r1#2 is tested
# separately via test_g6_fails_closed_on_insufficient_null).
M = M10.M10Manifest(n_null_dev=50, n_null_holdout=50)


def test_percentile_ladder_and_pvalue():
    null = list(np.arange(0.0, 1.0, 0.01))    # 100 values 0..0.99
    lad = M10.percentile_ladder(0.97, null, M)
    assert lad["p95"] == pytest.approx(np.percentile(null, 95), abs=1e-6)
    assert lad["exceeds_p95"] is True
    assert lad["p_value"] == pytest.approx(4 / 101, abs=1e-6)   # null>=0.97 -> 3 -> 4/101
    assert M10.percentile_ladder(0.5, null, M)["exceeds_p95"] is False


def test_percentile_ladder_uses_raw_not_rounded_for_logic():
    # codex r1#1 / r2: the gate logic uses the RAW percentile; display copies are rounded. Prove the
    # separation non-vacuously: _raw_pct holds full precision, display p95 = round(raw,6), and
    # exceeds_p95 is computed against RAW (a strategy just ABOVE raw exceeds; just BELOW does not).
    null = [0.0, 0.123456789012]
    raw95 = float(np.percentile(null, 95))
    lad = M10.percentile_ladder(raw95, null, M)
    assert lad["_raw_pct"]["p95"] == raw95                  # raw kept full precision
    assert lad["p95"] == round(raw95, 6)                    # display rounded
    assert M10.percentile_ladder(raw95 + 1e-9, null, M)["exceeds_p95"] is True
    assert M10.percentile_ladder(raw95 - 1e-9, null, M)["exceeds_p95"] is False
    # a value strictly between round6 and raw must follow RAW, not the rounded display.
    if round(raw95, 6) < raw95:
        mid = (round(raw95, 6) + raw95) / 2
        assert M10.percentile_ladder(mid, null, M)["exceeds_p95"] is False   # <= raw -> not exceeding


def test_clean_null_drops_none_nan_pdNA():
    # codex r1#4: None / nan / pd.NA dropped, not counted, not poisoning percentiles.
    nd = M10._clean_null([0.1, None, 0.2, np.nan, pd.NA, 0.3, float("inf")])
    assert sorted(nd.tolist()) == pytest.approx([0.1, 0.2, 0.3])
    assert M10._clean_null([None, np.nan, pd.NA]).size == 0


def test_g6_fails_closed_on_insufficient_null():
    # codex r1#2: too few valid null samples -> G6 fails closed (a p99 gate can't pass on a tiny null).
    m = M10.M10Manifest(n_null_dev=1000)
    g = M10.gate_g6_bivariate(0.99, 2.0, [0.1, 0.2, 0.3], [0.1, 0.2, 0.3], m, required_n=1000)
    assert g["pass"] is False and g["insufficient_null_reason"].startswith("insufficient_null")


def test_g6_bivariate_requires_both_roe_and_calmar():
    null_roe = list(np.arange(0.0, 1.0, 0.01))
    null_cal = list(np.arange(0.0, 2.0, 0.02))
    g = M10.gate_g6_bivariate(0.99, 0.5, null_roe, null_cal, M)   # ROE wins, Calmar doesn't
    assert g["roe_pass"] is True and g["calmar_pass"] is False and g["pass"] is False
    g2 = M10.gate_g6_bivariate(0.99, 1.99, null_roe, null_cal, M)
    assert g2["pass"] is True


def _nulls():
    return list(np.arange(0.0, 0.6, 0.006)), list(np.arange(0.0, 1.5, 0.015))   # 100 each


def test_g7_evaluates_ablation_g1_g3_g6():
    nr, nc = _nulls()
    # under cap -> pass regardless of ablation
    assert M10.gate_g7(0.3, None, nr, nc, M, pct=95.0, required_n=50)["pass"] is True
    # over cap; ablation clears G1+G3+G6 -> pass
    good_abl = {"chained_roe": 0.7, "max_chained_dd": 0.3, "chained_calmar": 2.0}
    assert M10.gate_g7(0.6, good_abl, nr, nc, M, pct=95.0, required_n=50)["pass"] is True
    # over cap; ablation fails G1 (low ROE) -> fail
    bad_abl = {"chained_roe": 0.2, "max_chained_dd": 0.3, "chained_calmar": 2.0}
    assert M10.gate_g7(0.6, bad_abl, nr, nc, M, pct=95.0, required_n=50)["pass"] is False


def _strat(**kw):
    base = dict(chained_roe=0.7, n_positive_folds=7, n_folds=8, max_chained_dd=0.3, g4_no_kill=True,
                g4_all_folds_above_floor=True, g5_pool_ok=True, chained_calmar=2.3,
                top_entity_pnl_share=0.2)
    base.update(kw)
    return base


def test_evaluate_gates_all_pass_dev_greenlight_live_small():
    nr, nc = _nulls()
    res = M10.evaluate_gates(_strat(), nr, nc, M)
    assert res["all_pass"] is True
    assert res["verdict_kind"] == "development_verdict"
    assert res["greenlight"] == "live_small"     # dev pass -> live-small ONLY (one-regime caveat)


def test_evaluate_gates_g1_fail_blocks():
    nr, nc = _nulls()
    res = M10.evaluate_gates(_strat(chained_roe=0.40), nr, nc, M)
    assert res["gates"]["G1"]["pass"] is False and res["all_pass"] is False and res["greenlight"] == "no"


def test_holdout_uses_p99_and_confirmatory():
    nr, nc = _nulls()
    res = M10.evaluate_gates(_strat(), nr, nc, M, holdout=True)
    assert res["verdict_kind"] == "confirmatory"
    assert res["gates"]["G6"]["at_pct"] == 99.0
    if res["all_pass"]:
        assert res["greenlight"] == "lp_confirmatory"


def test_g7_over_cap_blocks_without_ablation():
    nr, nc = _nulls()
    res = M10.evaluate_gates(_strat(top_entity_pnl_share=0.6), nr, nc, M, ablation=None)
    assert res["gates"]["G7"]["pass"] is False and res["all_pass"] is False


def test_holdout_requires_5000_null_dev_requires_1000():
    # codex r2: prove the dev(1000) vs holdout(5000) required-N switch with a real count gate.
    m = M10.M10Manifest(n_null_dev=50, n_null_holdout=5000)   # dev satisfiable by 100-sample null, holdout NOT
    nr, nc = _nulls()                                          # 100 samples each
    dev = M10.evaluate_gates(_strat(), nr, nc, m, holdout=False)
    hold = M10.evaluate_gates(_strat(), nr, nc, m, holdout=True)
    assert dev["gates"]["G6"]["pass"] is True                  # 100 >= 50 dev requirement
    assert hold["gates"]["G6"]["pass"] is False                # 100 < 5000 holdout requirement -> fails closed
    assert hold["gates"]["G6"]["insufficient_null_reason"].startswith("insufficient_null")
    assert dev["required_n"] == 50 and hold["required_n"] == 5000


def test_g3_dd_and_g2_folds_gate():
    nr, nc = _nulls()
    assert M10.evaluate_gates(_strat(max_chained_dd=0.6), nr, nc, M)["gates"]["G3"]["pass"] is False
    assert M10.evaluate_gates(_strat(n_positive_folds=5), nr, nc, M)["gates"]["G2"]["pass"] is False


def test_manifest_frozen_gates():
    m = M10.M10Manifest()
    assert m.g1_chained_roe == pytest.approx(0.5302) and m.g3_max_chained_dd == 0.50
    assert m.g2_min_positive_folds == 6 and m.n_null_dev == 1000 and m.n_null_holdout == 5000
    assert tuple(m.ladder) == (75.0, 90.0, 95.0, 99.0)
