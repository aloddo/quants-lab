"""GOLDEN regression test for v15_m10_gates (trust audit 2026-07-10).

Locks the terminal-gate fixes so a corrupt/partial M9 result can never greenlight the wrong cohort:
- P0#1: NaN/non-bool kill/floor/pool status FAILS CLOSED (bool(np.nan) is True was the footgun),
- P0#2: non-finite / out-of-domain metrics FAIL CLOSED (inf ROE, negative DD, etc.),
- P0#3: G2 denominator verified (a partial <8-fold run cannot pass "6 of 8"),
- P1#4: G6 uses the EXACT requested percentile (not the truncated ladder key),
- P1#5: a non-finite strategy value yields p_value None (no fake significance),
- happy path still greenlights live_small; gate directions (>=, <=, strict >) intact.
"""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m10_gates as M10

M = M10.M10Manifest()
NR = [0.1] * 1000
NC = [0.1] * 1000


def _good(**kw):
    s = dict(chained_roe=0.7, n_positive_folds=7, n_folds=8, max_chained_dd=0.3, g4_no_kill=True,
             g4_all_folds_above_floor=True, g5_pool_ok=True, chained_calmar=2.3, top_entity_pnl_share=0.2)
    s.update(kw)
    return s


def test_happy_path_greenlights_live_small():
    r = M10.evaluate_gates(_good(), NR, NC, M)
    assert r["all_pass"] and r["greenlight"] == "live_small" and r["verdict_kind"] == "development_verdict"


@pytest.mark.parametrize("field", ["g4_no_kill", "g4_all_folds_above_floor", "g5_pool_ok"])
@pytest.mark.parametrize("bad", [np.nan, "yes", 1, None])
def test_p0_1_nonbool_status_fails_closed(field, bad):
    r = M10.evaluate_gates(_good(**{field: bad}), NR, NC, M)
    assert r["greenlight"] == "no" and r["verdict_kind"] == "invalid_input"


@pytest.mark.parametrize("field,bad", [
    ("chained_roe", np.inf), ("chained_calmar", np.inf), ("chained_roe", np.nan),
    ("max_chained_dd", -0.1), ("max_chained_dd", 1.5), ("top_entity_pnl_share", -2.0),
    ("top_entity_pnl_share", 1.2), ("n_positive_folds", 9),
])
def test_p0_2_bad_domain_fails_closed(field, bad):
    r = M10.evaluate_gates(_good(**{field: bad}), NR, NC, M)
    assert r["greenlight"] == "no" and r["verdict_kind"] == "invalid_input"


@pytest.mark.parametrize("nf", [6, 7, 9, 0])
def test_p0_3_g2_denominator_verified(nf):
    # a run that did not evaluate exactly 8 folds cannot pass, even if n_positive_folds >= 6
    r = M10.evaluate_gates(_good(n_positive_folds=6, n_folds=nf), NR, NC, M)
    assert r["verdict_kind"] == "invalid_input"


def test_p1_4_g6_exact_percentile_not_truncated():
    # null with a sharp gap between p95 and p95.5; a strategy above p95 but below p95.5 must FAIL a 95.5 gate.
    null = list(np.linspace(0.0, 1.0, 1000))
    p95 = float(np.percentile(null, 95.0))
    p955 = float(np.percentile(null, 95.5))
    mid = (p95 + p955) / 2.0  # strictly between p95 and p95.5
    g = M10.gate_g6_bivariate(mid, mid, null, null, M, pct=95.5, required_n=1000)
    assert g["pass"] is False  # would WRONGLY pass if it truncated 95.5 -> p95


def test_p1_5_nonfinite_strategy_no_fake_pvalue():
    lad = M10.percentile_ladder(np.nan, NR, M)
    assert lad["p_value"] is None and lad["strategy_finite"] is False and lad["exceeds_p95"] is None


def test_np_bool_status_accepted():
    # a numpy bool True status must PASS G4/G5 (validation accepts np.bool_; gate normalizes with bool()).
    r = M10.evaluate_gates(_good(g4_no_kill=np.bool_(True), g5_pool_ok=np.bool_(True)), NR, NC, M)
    assert r["gates"]["G4"]["pass"] is True and r["gates"]["G5"]["pass"] is True
    # numpy bool False must FAIL closed
    r2 = M10.evaluate_gates(_good(g5_pool_ok=np.bool_(False)), NR, NC, M)
    assert r2["gates"]["G5"]["pass"] is False and r2["greenlight"] == "no"


def test_gate_directions_intact():
    # G1 boundary: roe exactly == threshold passes (>=); below fails
    assert M10.evaluate_gates(_good(chained_roe=M.g1_chained_roe), NR, NC, M)["gates"]["G1"]["pass"] is True
    assert M10.evaluate_gates(_good(chained_roe=M.g1_chained_roe - 1e-6), NR, NC, M)["gates"]["G1"]["pass"] is False
    # G3 boundary: dd exactly == threshold passes (<=)
    assert M10.evaluate_gates(_good(max_chained_dd=M.g3_max_chained_dd), NR, NC, M)["gates"]["G3"]["pass"] is True
    # G6 strict >: strategy exactly at the null p95 must NOT pass
    g = M10.gate_g6_bivariate(float(np.percentile(NR, 95.0)), float(np.percentile(NC, 95.0)), NR, NC, M,
                              pct=95.0, required_n=1000)
    assert g["pass"] is False
