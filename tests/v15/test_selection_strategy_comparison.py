import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import selection_strategy_comparison as comp  # noqa: E402


def test_behaviour_first_rejects_long_tail_and_deep_mae():
    p = pd.DataFrame({
        "fold_id": [1, 1], "pp_n": [30, 30], "pp_p90_hold_h": [24.0, 300.0],
        "pp_mae_p90": [0.05, 0.30], "pp_uw_add": [0.1, 0.8], "pp_frac_quick": [0.9, 0.1],
        "pp_mean_r": [0.01, 0.05], "pre_roe": [0.1, 0.2], "pre_calmar": [1.0, 1.0],
        "m6b_score": [1.0, 2.0],
    })
    out = comp.add_scores(p)
    assert bool(out.loc[0, "behaviour_eligible"]) is True
    assert bool(out.loc[1, "behaviour_eligible"]) is False
    assert pd.isna(out.loc[1, "score_behaviour"])


def test_percentile_direction():
    s = pd.Series([1.0, 2.0, 3.0])
    assert comp.pct(s).iloc[-1] > comp.pct(s).iloc[0]
    assert comp.pct(s, False).iloc[-1] < comp.pct(s, False).iloc[0]
