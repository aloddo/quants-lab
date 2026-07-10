#!/usr/bin/env python
"""
re_feature_scan.py -- Reverse-engineering foundation (Alberto's named frontier: "validate -> extract the
alpha -> own signal"). Univariate scan of NEW feature dimensions NOT covered by the class/side/regime
structural work: LEADER-SKILL tier, HOLD-DURATION, HOUR-OF-DAY. Which separate profitable journeys from
losers? These become inputs to the own-signal model once validation clears.

Large historical skill-cohort journey sample (NOT thin live data). Memory-safe pyarrow scan.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/re_feature_scan.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

RT_BPS = 11.0
JOURNEYS = "app/data/v15/m02_journeys.parquet"


def main():
    cfg = json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"]
    sk = list(cfg.keys())
    skill_sharpe = {w: (m.get("skill_sharpe") or 0) for w, m in cfg.items()}
    skill_win = {w: (m.get("skill_win") or 0) for w, m in cfg.items()}

    dset = ds.dataset(JOURNEYS, format="parquet")
    cols = ["wallet", "coin", "side", "entry_ts", "duration_h", "max_position_notional", "net_realized_pnl"]
    j = dset.to_table(columns=cols, filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[j.max_position_notional > 10].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["edge_bps"] = (j["ret"] - RT_BPS / 1e4) * 1e4
    j["skill_sharpe"] = j["wallet"].map(skill_sharpe)
    j["skill_win"] = j["wallet"].map(skill_win)
    j["hour"] = (pd.to_datetime(j["entry_ts"].astype("int64"), unit="ms", utc=True)).dt.hour
    print(f"skill-cohort journeys: {len(j)}\n")

    def buckets(label, series, edges, fmt):
        print(f"=== {label} ===")
        cats = pd.cut(series, bins=edges, include_lowest=True)
        g = j.groupby(cats, observed=True).agg(n=("edge_bps", "size"), edge=("edge_bps", "mean"),
                                               win=("edge_bps", lambda x: (x > 0).mean() * 100))
        for b, r in g.iterrows():
            print(f"  {fmt(b):<18} n={int(r.n):>6} edge={r.edge:+7.0f}bps win={r.win:>3.0f}%")
        # monotonic signal strength = spread across buckets
        print(f"  spread top-bottom: {g.edge.max()-g.edge.min():+.0f}bps\n")

    # LEADER SKILL tier (sharpe) -- does copying higher-skill leaders pay more?
    sh_edges = j.skill_sharpe.quantile([0, .25, .5, .75, 1.0]).values
    sh_edges = np.unique(sh_edges)
    buckets("LEADER skill_sharpe tier", j.skill_sharpe, sh_edges, lambda b: f"{b.left:.1f}..{b.right:.1f}")

    # HOLD DURATION -- short scalps vs long holds
    buckets("HOLD duration (h)", j.duration_h, [0, 1, 4, 12, 48, 1e9],
            lambda b: f"{b.left:.0f}-{b.right:.0f}h" if b.right < 1e8 else f">{b.left:.0f}h")

    # HOUR OF DAY (UTC) -- session effect
    buckets("ENTRY hour (UTC)", j.hour, [-0.1, 3, 7, 11, 15, 19, 23.1],
            lambda b: f"{b.left:.0f}-{b.right:.0f}h")

    # correlations to edge
    print("=== correlation to edge_bps ===")
    for c in ["skill_sharpe", "skill_win", "duration_h"]:
        print(f"  {c:<14} corr={j[c].corr(j.edge_bps):+.3f}")
    print("\nREAD: a dimension with a large monotone spread is a real RE feature; flat = no signal. Feeds the")
    print("own-signal model (combine with class/alt-best, side/both-positive, regime-robust from prior pages).")


if __name__ == "__main__":
    main()
