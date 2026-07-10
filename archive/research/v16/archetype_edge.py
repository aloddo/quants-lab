#!/usr/bin/env python
"""
archetype_edge.py -- RE next step (Alberto: keep researching the leaders' strategies). The leaders are
heterogeneous (26 momentum / 52 mean-rev / 22 mixed -- 2026-06-18-leader-archetypes). Question: WHICH archetype
is the actual alpha? Cluster the 100 leaders by entry-style + hold + long-bias, then measure each archetype's
per-journey edge, win-rate, Sharpe, and $ contribution. The best archetype = the one to replicate as our own
signal (and to weight the copy cohort toward).

Large historical skill-cohort journey sample. Read-only research. Run:
~/miniforge3/envs/quants-lab/bin/python research/v16/archetype_edge.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pymongo import MongoClient

RT = 11.0
JOURNEYS = "app/data/v15/m02_journeys.parquet"


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) > 30:
            df = pd.DataFrame(rows); out[c] = (df.timestamp_utc.to_numpy(), df.close.to_numpy())
    return out


def tr(ts, close, t, hours, sgn):
    i = np.searchsorted(ts, t)
    if i <= 0 or i >= len(ts): return None
    j = np.searchsorted(ts, t - hours * 3600 * 1000)
    if j < 0 or j >= len(ts) or close[j] <= 0: return None
    return (close[i] - close[j]) / close[j] * sgn * 1e4


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j = ds.dataset(JOURNEYS, format="parquet").to_table(
        columns=["wallet", "coin", "side", "entry_ts", "duration_h", "max_position_notional", "net_realized_pnl"],
        filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[(j.max_position_notional > 10) & (~j.coin.str.startswith("xyz:"))].dropna(subset=["entry_ts"]).copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["edge_bps"] = (j.ret - RT / 1e4) * 1e4
    j["t"] = j.entry_ts.astype("int64"); j["win"] = j.net_realized_pnl > 0
    vc = j.coin.value_counts(); cand = load_candles([c for c in vc.index if vc[c] >= 30])
    j = j[j.coin.isin(cand)].copy()
    j["tr6"] = [tr(*cand[r.coin], r.t, 6, r.sgn) for r in j.itertuples()]
    j = j.dropna(subset=["tr6"])

    # per-wallet entry-style -> archetype label
    g = j.groupby("wallet").agg(n=("tr6", "size"), tr6=("tr6", "mean"))
    g = g[g.n >= 30]
    def lab(x): return "MOMENTUM" if x > 50 else ("MEAN-REV" if x < -50 else "MIXED")
    g["arch"] = g.tr6.map(lab)
    j = j.merge(g[["arch"]], left_on="wallet", right_index=True, how="inner")
    print(f"journeys {len(j)} across {len(g)} wallets | archetypes: "
          f"{dict(g.arch.value_counts())}\n")

    print(f"=== EDGE BY ARCHETYPE (which leader-style is the alpha?) ===")
    print('per-trade Sharpe (mean/std); annualizing by trade-count is meaningless w/ overlapping positions')
    print(f"{'archetype':<12}{'wallets':>8}{'journeys':>9}{'edge_bps':>9}{'median':>8}{'win%':>6}{'pt_Shrp':>8}{'sum_ret':>9}")
    rows = []
    for a in ["MOMENTUM", "MEAN-REV", "MIXED"]:
        d = j[j.arch == a]
        if not len(d): continue
        nb = d.edge_bps.to_numpy()
        sharpe = nb.mean() / nb.std() if nb.std() > 0 else 0  # PER-TRADE Sharpe (fixed: was mean/std*sqrt(N)=t-stat)
        nw = (g.arch == a).sum()
        rows.append((a, nw, len(d), nb.mean(), np.median(nb), (nb > 0).mean() * 100, sharpe, d.ret.sum()))
        print(f"{a:<12}{nw:>8}{len(d):>9}{nb.mean():>9.0f}{np.median(nb):>8.0f}{(nb>0).mean()*100:>6.0f}{sharpe:>8.1f}{d.ret.sum():>9.1f}")

    # long vs short within each archetype (the RE signal detail)
    print(f"\n=== ARCHETYPE x SIDE (edge_bps / n) ===")
    for a in ["MOMENTUM", "MEAN-REV", "MIXED"]:
        d = j[j.arch == a]
        L = d[d.sgn > 0]; S = d[d.sgn < 0]
        if len(d):
            print(f"  {a:<10} LONG {L.edge_bps.mean():+5.0f}({len(L)})  SHORT {S.edge_bps.mean():+5.0f}({len(S)})")

    best = max(rows, key=lambda r: r[6])  # by sharpe
    print(f"\nBEST archetype by Sharpe: {best[0]} (edge {best[3]:+.0f}bps, win {best[5]:.0f}%, Sharpe {best[6]:.1f}, {best[1]} wallets).")
    print(f"RE READ: replicate / weight the cohort toward the highest-Sharpe archetype -- that's the cleanest alpha")
    print(f"to turn into our own signal. (Note: leaders' realized edge; copy execution applies the same costs.)")


if __name__ == "__main__":
    main()
