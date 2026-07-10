#!/usr/bin/env python
"""
entry_style_per_wallet.py -- Alberto's challenge (msg 9681): "how come 100 wallets all have the same style?"
Right -- the pooled aggregate masks heterogeneity. This decomposes entry style PER WALLET and shows the
DISTRIBUTION: are leaders all one style, or distinct archetypes (momentum vs mean-reversion vs mixed)?

Per wallet: n journeys, mean signed trailing-6h & 24h return at entry, long-share, win-rate, median hold.
Then the cross-wallet dispersion (std, quartiles, how many momentum vs mean-reversion) + simple archetype buckets.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/entry_style_per_wallet.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pymongo import MongoClient

JOURNEYS = "app/data/v15/m02_journeys.parquet"


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc":1,"close":1,"_id":0}).sort("timestamp_utc",1))
        if len(rows) > 30:
            df = pd.DataFrame(rows); out[c] = (df["timestamp_utc"].to_numpy(), df["close"].to_numpy())
    return out


def tr(ts, close, t, hours, sgn):
    i = np.searchsorted(ts, t)
    if i <= 0 or i >= len(ts): return None
    j = np.searchsorted(ts, t - hours*3600*1000)
    if j < 0 or j >= len(ts) or close[j] <= 0: return None
    return (close[i]-close[j])/close[j]*sgn*1e4


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j = ds.dataset(JOURNEYS, format="parquet").to_table(
        columns=["wallet","coin","side","entry_ts","duration_h","max_position_notional","net_realized_pnl"],
        filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[(j.max_position_notional>10)&(~j.coin.str.startswith("xyz:"))].dropna(subset=["entry_ts"]).copy()
    j["sgn"]=j.side.str.lower().map(lambda s:1.0 if "long" in str(s) else -1.0)
    j["win"]=j.net_realized_pnl>0; j["t"]=j.entry_ts.astype("int64")
    vc=j.coin.value_counts(); cand=load_candles([c for c in vc.index if vc[c]>=30])
    j=j[j.coin.isin(cand)].copy()
    j["tr6"]=[tr(*cand[r.coin],r.t,6,r.sgn) for r in j.itertuples()]
    j["tr24"]=[tr(*cand[r.coin],r.t,24,r.sgn) for r in j.itertuples()]
    j=j.dropna(subset=["tr6","tr24"])

    g=j.groupby("wallet").agg(n=("tr6","size"),tr6=("tr6","mean"),tr24=("tr24","mean"),
                              longsh=("sgn",lambda x:(x>0).mean()*100),win=("win",lambda x:x.mean()*100),
                              dur=("duration_h","median"))
    g=g[g.n>=30]   # wallets with enough trades to characterize
    print(f"wallets characterized (>=30 trades): {len(g)} | total journeys {int(g.n.sum())}\n")

    print("=== CROSS-WALLET DISPERSION of entry style (signed trailing-6h bps) ===")
    print(f"  mean {g.tr6.mean():+.0f} | std {g.tr6.std():.0f} | min {g.tr6.min():+.0f} | "
          f"q25 {g.tr6.quantile(.25):+.0f} | median {g.tr6.median():+.0f} | q75 {g.tr6.quantile(.75):+.0f} | max {g.tr6.max():+.0f}")
    mom=(g.tr6>50).sum(); mr=(g.tr6<-50).sum(); mix=len(g)-mom-mr
    print(f"  ARCHETYPES by tr6: momentum(>+50) {mom} | mean-reversion(<-50) {mr} | mixed/neutral {mix}")
    print(f"  long-bias dispersion: median {g.longsh.median():.0f}% long | range {g.longsh.min():.0f}-{g.longsh.max():.0f}%")
    print(f"  hold dispersion: median {g.dur.median():.1f}h | range {g.dur.min():.1f}-{g.dur.max():.1f}h")
    print(f"  -> {'HETEROGENEOUS (distinct styles)' if g.tr6.std()>80 or (mom>0 and mr>0) else 'HOMOGENEOUS (one shared style)'}\n")

    print("=== most MOMENTUM wallets (top tr6) ===")
    for w,r in g.sort_values("tr6",ascending=False).head(5).iterrows():
        print(f"  {w[:12]} n={int(r.n):>4} tr6={r.tr6:+6.0f} tr24={r.tr24:+6.0f} long={r.longsh:>3.0f}% win={r.win:>3.0f}% hold={r.dur:.1f}h")
    print("=== most MEAN-REVERSION wallets (bottom tr6) ===")
    for w,r in g.sort_values("tr6").head(5).iterrows():
        print(f"  {w[:12]} n={int(r.n):>4} tr6={r.tr6:+6.0f} tr24={r.tr24:+6.0f} long={r.longsh:>3.0f}% win={r.win:>3.0f}% hold={r.dur:.1f}h")
    print("\nRE READ: if archetypes split, the own-signal should be PER-ARCHETYPE (not one blended rule). If they")
    print("cluster, the aggregate trend-following read holds. Either way this answers 'do all 100 share a style'.")


if __name__ == "__main__":
    main()
