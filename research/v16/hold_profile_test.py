#!/usr/bin/env python3
"""SPRINT: does TRAIN-side hold-time profile predict TEST edge (beyond train_taker edge)?

Evidence motivating this (sprint_analysis, both folds): test edge concentrates in 6h-3d holds
(wmed +38..+61); <1h flat/negative; >3d negative. Hold time is unknowable at OUR entry, but if
wallets have PERSISTENT hold profiles, selection can target 6h-3d-style leaders ex ante.

Method: for each fold cohort wallet, compute TRAIN-window RT stats from fills_v2 daily parquets
(median hold, share of RTs in 6h-72h band, share <1h, share >3d, n). Join with per-wallet TEST
mean overlay bps (from sprint_trades). Report: (a) hold-profile persistence train->test
(spearman), (b) test-edge regression on train_taker + train hold features, (c) what a
"top-30 AND train_share_6h_72h >= X" filter does to test edge + flow, per fold.

Run: python research/v16/hold_profile_test.py   (~3-5 min)
"""
from __future__ import annotations
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_HERE))

from fidelity_replay import roundtrips
from select_cohort import LIQUID
from forward_test import load_fills_daily
from _streaming_io import install_memory_guard

FOLDS = [
    ("fold1", "2025-12-01", "2026-03-15", "2026-05-17"),
    ("fold2", "2025-12-15", "2026-04-15", "2026-05-23"),
]


def hold_stats(rts, lo, hi):
    holds = [(x - e) / 3_600_000.0 for c, d_, e, x, *_ in rts if lo <= e < hi and c in LIQUID]
    if len(holds) < 5:
        return None
    h = np.array(holds)
    return {"n": len(h), "med_hold_h": float(np.median(h)),
            "sh_lt1h": float((h < 1).mean()), "sh_6_72": float(((h >= 6) & (h <= 72)).mean()),
            "sh_gt72": float((h > 72).mean())}


def main():
    install_memory_guard(soft_gb=12.0, label="hold_profile")
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    tr = pd.read_parquet(_REPO / "app" / "data" / "v16" / "sprint_trades.parquet")
    test_w = (tr.groupby(["fold", "wallet"])
                .agg(test_ov=("ov_bps", "mean"), test_n=("ov_bps", "count"),
                     test_med_hold=("hold_h", "median"), rank=("rank", "first"),
                     train_taker=("train_taker", "first")).reset_index())
    wallets = set(tr.wallet.unique())
    print(f"{len(wallets)} union cohort wallets")
    wf = load_fills_daily(wallets, ms("2025-12-01"), ms("2026-05-23"))

    rows = []
    for fold, f_start, f_split, f_end in FOLDS:
        lo, hi = ms(f_start), ms(f_split)
        fw = set(test_w[test_w.fold == fold].wallet)
        for w in fw:
            fl = [f for f in wf.get(w, []) if lo <= f[0] < hi]
            if not fl:
                continue
            st = hold_stats(roundtrips(fl), lo, hi)
            if st:
                rows.append({"fold": fold, "wallet": w, **st})
    prof = pd.DataFrame(rows)
    df = test_w.merge(prof, on=["fold", "wallet"], how="inner")
    print(f"joined {len(df)} wallet-folds")

    for fold, g in df.groupby("fold"):
        print(f"\n=== {fold} ({len(g)} wallets) ===")
        print(f"  hold persistence: spearman(train med_hold, test med_hold) = "
              f"{g[['med_hold_h','test_med_hold']].corr(method='spearman').iloc[0,1]:+.3f}")
        for feat in ("med_hold_h", "sh_lt1h", "sh_6_72", "sh_gt72"):
            print(f"  spearman(train {feat:10s}, test edge) = "
                  f"{g[[feat,'test_ov']].corr(method='spearman').iloc[0,1]:+.3f}")
        print(f"  spearman(train_taker, test edge)      = "
              f"{g[['train_taker','test_ov']].corr(method='spearman').iloc[0,1]:+.3f}")
        # filters: weighted by test_n to reflect flow
        def show(label, m):
            sub = g[m]
            if len(sub) < 5:
                print(f"    {label:34s}: <5 wallets")
                return
            fl_w = sub.test_n.sum()
            pooled = (sub.test_ov * sub.test_n).sum() / fl_w
            print(f"    {label:34s}: {len(sub):3d} wallets | wmed {sub.test_ov.median():+7.2f} | "
                  f"pooled {pooled:+7.2f} | flow {fl_w:5d} trades")
        show("ALL (cohort baseline)", g.test_ov.notna())
        show("rank<=30", g["rank"] <= 30)
        for x in (0.25, 0.4, 0.5):
            show(f"sh_6_72 >= {x}", g.sh_6_72 >= x)
            show(f"rank<=30 AND sh_6_72 >= {x}", (g["rank"] <= 30) & (g.sh_6_72 >= x))
        show("sh_lt1h <= 0.3", g.sh_lt1h <= 0.3)
        show("rank<=50 AND sh_lt1h<=0.3 AND sh_6_72>=0.4",
             (g["rank"] <= 50) & (g.sh_lt1h <= 0.3) & (g.sh_6_72 >= 0.4))

    try:
        import statsmodels.formula.api as smf
        m = smf.ols("test_ov ~ train_taker + sh_6_72 + sh_lt1h + C(fold)", data=df).fit(cov_type="HC1")
        print("\nREGRESSION test_ov ~ train_taker + sh_6_72 + sh_lt1h + fold:")
        print(pd.DataFrame({"coef": m.params, "se": m.bse, "p": m.pvalues}).round(3).to_string())
    except ImportError:
        pass
    df.to_parquet(_REPO / "app" / "data" / "v16" / "hold_profile.parquet")


if __name__ == "__main__":
    main()
