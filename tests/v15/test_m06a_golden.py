"""GOLDEN regression test for v15_m06a_shortlist trust-audit fixes (2026-07-10).

Locks two fail-CLOSED behaviours the codex m06a audit surfaced:
- P1: a missing / non-bool `eligible` value must NOT slip past the I9 precheck (`== True` is False for NaN)
  and then become rankable via `bool(np.nan) is True`. It fails closed up front.
- P2: an eligible row that breaches the stated M5 range contract (n_journeys < 3, or max_dd > 0.80) must
  hard-fail here instead of being silently clamped (dd=1.5 -> dd_term=0.20 would hide the breach).
Reuses the fixture helpers from test_m06a.
"""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m06a_shortlist as m6a  # noqa: E402
from test_m06a import _folds, _entities, _pool, _elig_row, _actions, _manifest  # noqa: E402


def test_p1_nan_eligible_fails_closed():
    eids = [1]
    row = _elig_row(1, 1, roe=0.5, nj=10, dd=0.2, eligible=True)
    row["eligible"] = np.nan   # corrupt: missing eligibility must not slip through
    import pandas as pd
    elig = pd.DataFrame([row])
    with pytest.raises(AssertionError, match="non-bool"):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


def test_p2_eligible_low_journeys_hardfails():
    eids = [1]
    import pandas as pd
    elig = pd.DataFrame([_elig_row(1, 1, roe=0.5, nj=1, dd=0.2, eligible=True)])  # nj=1 < M5 floor 3
    with pytest.raises(AssertionError, match="n_journeys"):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


def test_p2_eligible_excess_dd_hardfails():
    eids = [1]
    import pandas as pd
    elig = pd.DataFrame([_elig_row(1, 1, roe=0.5, nj=10, dd=1.5, eligible=True)])  # dd 1.5 > M5 cap 0.80
    with pytest.raises(AssertionError, match="max_dd"):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


def test_activity_only_lane_defers_dd_by_design():
    # DOCUMENTS the intentional exemption (codex m06a r2): activity_only is the equity-INDEPENDENT lane where
    # M5 ran equity_required=False, so max_dd_pretest is legitimately NaN/absent and DD is deferred to M7/M6b.
    # A high (or NaN) max_dd must NOT hard-fail here -- the score has no DD term. n_journeys>=3 still applies.
    eids = [1]
    import pandas as pd
    man = _manifest(); man["score_basis"] = "activity_only"
    elig = pd.DataFrame([_elig_row(1, 1, roe=float("nan"), nj=10, dd=float("nan"), eligible=True)])
    sl, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), man)
    assert bool((sl["in_shortlist"] == True).any())  # noqa: E712
    # but a too-thin activity_only row still fails the shared n_journeys rail
    elig_thin = pd.DataFrame([_elig_row(1, 1, roe=float("nan"), nj=1, dd=float("nan"), eligible=True)])
    with pytest.raises(AssertionError, match="n_journeys"):
        m6a.run(elig_thin, _pool(eids), _folds(), _entities(eids), _actions(eids), man)


def test_good_row_still_shortlists():
    # a clean eligible row is unaffected by the new fail-closed rails.
    eids = [1]
    import pandas as pd
    elig = pd.DataFrame([_elig_row(1, 1, roe=0.5, nj=10, dd=0.2, eligible=True)])
    sl, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())
    assert bool((sl["in_shortlist"] == True).any())  # noqa: E712
