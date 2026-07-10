"""Alignment sweep: run competing M4->M6b SELECTION RULES forward through the 8 folds via
v15_forward_select.forward_backtest (no look-ahead, M9 OOS-graded) and compare forward ROE/DD/
Calmar. Answers the alignment questions with OOS data instead of opinion:
  Q2 in-sample-ROE vs trailing-OOS selection ; Q1 leverage cap ; Q3 floor tightness (via require_eligible).
Uses the BUILT infra (forward_select + M9). Runs on whatever m07_{test,pretest}_final exist.
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_forward_select as F  # noqa: E402

DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"


def _z(s):
    s = s.astype(float)
    sd = s.std(ddof=0)
    return (s - s.mean()) / sd if sd and sd == sd else s * 0.0


# entity_id -> max leverage (from the 18k drift/profit scan, via m04 primary/member wallets)
def build_entity_lev():
    sc = pd.read_parquet(DATA / "m01_drift_profit_scan.parquet")[["wallet", "max_leverage"]]
    wlev = dict(zip(sc["wallet"].str.lower(), sc["max_leverage"]))
    ent = pd.read_parquet(DATA / "m04_entities.parquet")
    elev = {}
    for r in ent.itertuples():
        ws = [r.primary_wallet]
        mw = getattr(r, "member_wallets", None)
        if isinstance(mw, (list, np.ndarray)):
            ws += list(mw)
        levs = [wlev.get(str(w).lower()) for w in ws if w]
        levs = [x for x in levs if x is not None and x == x]
        if levs:
            elev[int(r.entity_id)] = float(max(levs))
    return elev


ELEV = build_entity_lev()


def lev_cap(score, cap):
    """Return a copy of score with NaN where entity leverage > cap (excluded by forward_backtest)."""
    return score.copy()  # placeholder; applied inside score_fn via features.entity_id


def make_rules():
    def insample(f):  # current M6b-ish: reward in-sample (pretest) ROE
        return f.pre_roe
    def trailing(f):  # trailing realized OOS mean (folds < k)
        return f.trail_mean
    def consistency(f):  # consistency-heavy: positive-OOS frac + trailing mean - trailing DD
        return _z(f.trail_pos_frac.fillna(0)) + _z(f.trail_mean.fillna(0)) - 0.5 * _z(f.trail_dd.fillna(0))
    def _capped(base, cap):
        def fn(f):
            s = base(f).astype(float).copy()
            lev = f.entity_id.map(lambda e: ELEV.get(int(e), np.nan))
            s[(lev > cap)] = np.nan  # exclude > cap; NaN-lev kept (unknown -> don't drop)
            return s
        return fn
    return {
        "Q2_insample_roe": insample,
        "Q2_trailing_oos": trailing,
        "Q2_consistency": consistency,
        "Q1_consistency_lev<=10x": _capped(consistency, 10.0),
        "Q1_consistency_lev<=5x": _capped(consistency, 5.0),
        "Q1_insample_lev<=10x": _capped(insample, 10.0),
    }


if __name__ == "__main__":
    K = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    rules = make_rules()
    print(f"forward-select sweep: k_select={K}, {len(rules)} rules, {len(ELEV)} entities with leverage", flush=True)
    out = []
    for name, fn in rules.items():
        try:
            r = F.forward_backtest(fn, k_select=K, b0=500.0, require_eligible=True)
            out.append((name, r))
            print(f"  {name:28s} chained_roe={r.get('chained_roe', float('nan')):>8.3f} "
                  f"maxDD={r.get('max_chained_dd', float('nan')):>6.3f} "
                  f"calmar={r.get('chained_calmar', float('nan')):>7.3f} "
                  f"posFolds={r.get('n_positive_folds','?')} "
                  f"topShare={r.get('top_entity_pnl_share', float('nan')):>5.2f} "
                  f"sel={sum(r.get('selections',{}).values())}", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"  {name:28s} ERROR {type(e).__name__}: {e}", flush=True)
    print("SWEEP_DONE", flush=True)
