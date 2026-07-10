"""GOLDEN regression test for v15_m06b_ranking trust-audit fixes (2026-07-10).

Locks two provenance fail-closed behaviours:
- P0#2: a run on NON-fold-pure (global, look-ahead) M4 is never stamped investable.
- P0#3: a duplicate (entity_id, fold_id) row in any fold-keyed input fails closed (merge validate) instead of
  silently handing one entity two pool slots.
Reuses the fixture helper from test_m06b.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m06b_ranking as M
from test_m06b import _make_inputs, _equity_all_active, _folds, _patch_exposure  # noqa: E402,F401


def test_p0_2_global_m04_not_investable(_patch_exposure):
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    # SAME inputs that are investable with fold-pure M4, but here fold_pure=False (global look-ahead M4).
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05,
                       realized=True, fold_pure=False)
    out, manifest = M.build_ranking(inp, m)
    assert manifest["investable"] is False
    assert any("m04_not_fold_pure" in r for r in manifest["non_investable_reasons"])


def test_p0_3_duplicate_entity_fold_fails_closed(_patch_exposure):
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05,
                       realized=True, fold_pure=True)
    # inject a duplicate (entity_id, fold_id) into the M7 summary
    inp["m07_summary"] = pd.concat([inp["m07_summary"], inp["m07_summary"].iloc[[0]]], ignore_index=True)
    with pytest.raises((ValueError, Exception)):
        M.build_ranking(inp, m)
