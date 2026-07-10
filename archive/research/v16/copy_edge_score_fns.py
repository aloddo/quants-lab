"""Pre-registered SELECTION rules (score_fn) for forward_select.forward_backtest.

LOCKED 2026-06-27 BEFORE seeing rebuilt-chain results (anti-overfit / anti-dredge, per Alberto "don't
overfit" + codex design gate). Each takes the fold-k trailing_features DataFrame (columns: trail_n,
trail_pos_frac, trail_mean [= mean prior-fold realized engine-OOS net ROE], trail_dd [mean prior-fold OOS
maxDD], pre_roe, pre_dd, trail_elig, m5_eligible_k) and returns a pd.Series score (higher = pick first).

Usage: forward_select.forward_backtest(SCORE_FNS["copy_edge"], k_select=75, ...). The guarded eval
(nested 1-6 tune / 7 validate / 8 final-once + SPA bootstrap vs random+v17 + activity-baseline + trail_n
bucket) wraps these per the codex gate; see copy-rebuild/2026-06-27-canonical-forward-select-plan.
"""
import numpy as np
import pandas as pd

EPS = 1e-6


def copy_edge(f: pd.DataFrame) -> pd.Series:
    """S1 PRIMARY: rank by trailing realized engine-OOS copy edge."""
    return f["trail_mean"]


def copy_edge_dd(f: pd.DataFrame) -> pd.Series:
    """S2: DD-aware (Calmar-like) -- harvest edge, punish trailing drawdown."""
    return f["trail_mean"] / (f["trail_dd"].abs() + EPS)


def consistency(f: pd.DataFrame) -> pd.Series:
    """S3: reward reliably-positive copy edge (fraction of prior folds positive x mean)."""
    return f["trail_pos_frac"].fillna(0.0) * f["trail_mean"]


def pretest(f: pd.DataFrame) -> pd.Series:
    """S4 weak-prior baseline: pretest (in-sample as-of k) engine ROE."""
    return f["pre_roe"]


def random_seeded(f: pd.DataFrame) -> pd.Series:
    """S5 NULL baseline: deterministic per-fold shuffle (seed varies by fold to avoid a fixed order)."""
    fold = int(f["fold_id"].iloc[0]) if "fold_id" in f.columns and len(f) else 0
    rng = np.random.default_rng(10_000 + fold)
    return pd.Series(rng.permutation(len(f)).astype(float), index=f.index)


def v17_like(f: pd.DataFrame) -> pd.Series:
    """S6: the rule we are replacing -- edge WITHOUT DD/consistency awareness (proxy = trail_mean rank,
    no DD term). If S1/S2/S3 do not beat this, the rebuild added nothing over V17-style selection."""
    return f["trail_mean"].rank(method="first")


# ACTIVITY-PERSISTENCE baseline (codex guard #4): if copy_edge does not beat this, the 'edge' is just
# recent activity, not skill. Rank by trailing sample size (how active/recent the wallet is).
def activity_baseline(f: pd.DataFrame) -> pd.Series:
    return f["trail_n"].astype(float)


SCORE_FNS = {
    "copy_edge": copy_edge,          # S1 primary
    "copy_edge_dd": copy_edge_dd,    # S2
    "consistency": consistency,      # S3
    "pretest": pretest,              # S4 baseline
    "random": random_seeded,         # S5 null
    "v17_like": v17_like,            # S6 incumbent
    "activity": activity_baseline,   # codex de-bias baseline
}
