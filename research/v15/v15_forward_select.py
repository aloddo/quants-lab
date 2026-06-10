"""V15 forward-tradeable selection sweep. For ANY selection rule, run it FORWARD through the 8 folds
using ONLY trailing data at each fold (prior folds' realized OOS + pretest-as-of-k), pick the top-K,
and grade through the M9 chained OOS sim. This is the tool to 'squeeze the data' and find the best
copyable wallets WITHOUT look-ahead. (Alberto 2026-06-01: tighten + backtest with the new infra.)

trailing features at fold k (all known at test_start[k]):
- trail_n / trail_pos_frac / trail_mean / trail_dd : the wallet's REALIZED OOS results in folds < k
  (folds 1..k-1 already happened before fold k's test window -> legitimately known at decision time).
- pre_roe_k / pre_dd_k : the wallet's PRETEST (in-sample, as-of test_start[k]) M7 result for fold k.
- m5_eligible_k / trail_elig : M5 eligibility for fold k + count of prior folds eligible.
A selection rule = score_fn(features_df) -> Series score; pick top-K per fold (NaN score excluded).
"""
from __future__ import annotations
import sys
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m07_engine as E      # noqa: E402
import v15_m09_sim as M9        # noqa: E402

DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"


def load_panels():
    """Per-(entity,fold) OOS (net of cost) + pretest panels + eligibility, indexed for fast lookup."""
    oos = pd.read_parquet(DATA / "m07_test_final" / "m07_summary.parquet")[
        ["entity_id", "fold_id", "roe_engine", "max_dd", "n_backstop_transfer"]].rename(columns={"roe_engine": "oos"})
    pre = pd.read_parquet(DATA / "m07_pretest_final" / "m07_summary.parquet")[
        ["entity_id", "fold_id", "roe_engine", "max_dd"]].rename(columns={"roe_engine": "pre_roe", "max_dd": "pre_dd"})
    m5 = pd.read_parquet(DATA / "m05_eligibility.parquet")[["entity_id", "fold_id", "eligible"]]
    return oos, pre, m5


def trailing_features(oos: pd.DataFrame, pre: pd.DataFrame, m5: pd.DataFrame, k: int) -> pd.DataFrame:
    """Build the fold-k feature frame from ONLY data known at test_start[k] (no look-ahead)."""
    prior = oos[oos.fold_id < k]
    tg = prior.groupby("entity_id").agg(
        trail_n=("oos", "size"), trail_pos_frac=("oos", lambda x: (x > 0).mean()),
        trail_mean=("oos", "mean"), trail_dd=("max_dd", "mean")).reset_index()
    elig_prior = m5[(m5.fold_id < k) & (m5.eligible)].groupby("entity_id").size().rename("trail_elig").reset_index()
    pk = pre[pre.fold_id == k][["entity_id", "pre_roe", "pre_dd"]]
    ek = m5[m5.fold_id == k][["entity_id", "eligible"]].rename(columns={"eligible": "m5_eligible_k"})
    f = pk.merge(tg, on="entity_id", how="left").merge(elig_prior, on="entity_id", how="left").merge(ek, on="entity_id", how="left")
    f["trail_n"] = f["trail_n"].fillna(0).astype(int)
    f["trail_elig"] = f["trail_elig"].fillna(0).astype(int)
    f["fold_id"] = k
    return f


def forward_backtest(score_fn, k_select: int, b0: float = 500.0, min_trail: int = 1,
                     require_eligible: bool = True, uniform_weight: bool = True) -> dict:
    """Run a selection rule forward through folds 2..8 (fold 1 has no trailing -> seeded by score on
    pretest only) and grade via the M9 chained sim. score_fn(features)->Series. Top-k_select per fold."""
    oos, pre, m5 = load_panels()
    ent = pd.read_parquet(DATA / "m04_entities.parquet")
    folds = pd.read_parquet(DATA / "m03_folds.parquet")
    import json
    cal = json.loads((DATA / "slippage_calib_v11.json").read_text())
    perfold = {int(x): v for x, v in cal["per_fold_asof"].items()}
    md = E.MarketData(allow_mongo=True); md.set_slip_calib(perfold.get(8), cal["version"])
    import pyarrow.dataset as ds
    acts = ds.dataset(str(DATA / "m02_actions.parquet"))

    @lru_cache(maxsize=4096)
    def _w(w):
        return acts.to_table(filter=ds.field("wallet") == w).to_pandas()

    def loader(w, t0, t1):
        a = _w(w)
        return a[(a.ts >= t0) & (a.ts < t1)]

    pool_rows = []
    sel_log = {}
    for k in range(1, 9):
        f = trailing_features(oos, pre, m5, k)
        if require_eligible:
            f = f[f.m5_eligible_k.fillna(False)]
        f = f[f.trail_n >= (min_trail if k > 1 else 0)]
        if f.empty:
            continue
        f = f.copy(); f["score"] = score_fn(f)
        f = f[f["score"].notna()].sort_values("score", ascending=False)
        picks = f.head(k_select)
        sel_log[k] = list(picks.entity_id)
        for r in picks.itertuples():
            w = 1.0 / len(picks) if uniform_weight else float(r.score)
            pool_rows.append({"entity_id": int(r.entity_id), "fold_id": k, "in_pool": True,
                              "quality_weight": w, "entity_alloc_weight": 1.0})
    if not pool_rows:
        return {"chained_roe": float("nan"), "error": "no selections"}
    pool = pd.DataFrame(pool_rows)
    # normalize weights per fold
    pool["quality_weight"] = pool.groupby("fold_id")["quality_weight"].transform(lambda x: x / x.sum())
    tiers = pd.DataFrame([{"entity_id": e, "fold_id": fd, "tier": "full_weight", "survival_multiplier": 1.0,
                           "max_survivable_slice": np.inf} for fd, e in zip(pool.fold_id, pool.entity_id)])
    res = M9.run_m09_chained(pool, tiers, ent, folds, E, md, loader, M9.M9Manifest(), b0)
    res["selections"] = {k: len(v) for k, v in sel_log.items()}
    return res
