#!/usr/bin/env python
"""
coin_class_structural.py -- is the xyz (builder-dex equities/commodities) drag STRUCTURAL?

Live (thin) shows xyz coins dragging ~-119bps incl. the SNDK/CL/SPCX liquidations. Critically, xyz coins
are NOT in the L2-calibrated copyable set (0 of them), yet the live engine trades them. This tests, on the
LARGE historical skill-cohort journey sample (NO calib filter so xyz is included; 50 xyz coins present),
whether xyz is structurally negative vs majors/liquid-alts.

READ: if xyz edge is structurally << majors/alts (esp. after fees + the liq tail), that is a decision-grade
case to DROP xyz from the live copy universe (config change, codex + Alberto gate). If xyz is comparable,
the live drag is small-sample / execution, not the coin class.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/coin_class_structural.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

RT_BPS = 11.0
MAJORS = {"ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"}
JOURNEYS = "app/data/v15/m02_journeys.parquet"


def klass(c):
    c = str(c)
    if c.startswith("xyz:"):
        return "xyz"
    return "major" if c in MAJORS else "alt"


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    dset = ds.dataset(JOURNEYS, format="parquet")
    cols = ["wallet", "coin", "side", "max_position_notional", "net_realized_pnl", "liq_closed", "duration_h"]
    j = dset.to_table(columns=cols, filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[j.max_position_notional > 10].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["edge_bps"] = (j["ret"] - RT_BPS / 1e4) * 1e4
    j["klass"] = j["coin"].map(klass)
    j["side_n"] = j["side"].str.lower().map(lambda s: "LONG" if "long" in str(s) else ("SHORT" if "short" in str(s) else "?"))
    print(f"skill-cohort journeys (NO calib filter): {len(j)}")

    def stat(d):
        if not len(d):
            return "n=0"
        liqrate = d.liq_closed.mean() * 100 if "liq_closed" in d else 0
        return (f"n={len(d):>6} edge={d.edge_bps.mean():+7.0f}bps med={d.edge_bps.median():+6.0f} "
                f"win={(d.edge_bps>0).mean()*100:>3.0f}% liq={liqrate:>4.1f}% sum_ret={d.ret.sum():+8.1f} dur={d.duration_h.median():>4.1f}h")

    print(f"\n=== BY COIN CLASS ===")
    for k in ["major", "alt", "xyz"]:
        print(f"{k:<7}{stat(j[j.klass==k])}")
    print(f"{'ALL':<7}{stat(j)}")

    print(f"\n=== COIN CLASS x SIDE ===")
    for k in ["major", "alt", "xyz"]:
        for s in ["LONG", "SHORT"]:
            d = j[(j.klass == k) & (j.side_n == s)]
            if len(d):
                print(f"{k+'/'+s:<14}{stat(d)}")

    # within xyz, worst coins
    print(f"\n=== xyz coins (worst by sum_ret) ===")
    xy = j[j.klass == "xyz"]
    g = xy.groupby("coin").agg(n=("ret", "size"), edge=("edge_bps", "mean"),
                               win=("edge_bps", lambda x: (x > 0).mean() * 100),
                               sum_ret=("ret", "sum"), liq=("liq_closed", "mean")).sort_values("sum_ret")
    for c, r in g.head(8).iterrows():
        print(f"  {c:<16} n={int(r.n):>4} edge={r.edge:+7.0f}bps win={r.win:>3.0f}% liq={r.liq*100:>4.1f}% sum_ret={r.sum_ret:+7.1f}")

    # verdict
    me = j[j.klass == "major"].edge_bps.mean()
    ae = j[j.klass == "alt"].edge_bps.mean()
    xe = j[j.klass == "xyz"].edge_bps.mean()
    xliq = j[j.klass == "xyz"].liq_closed.mean() * 100
    print(f"\nmajor {me:+.0f} | alt {ae:+.0f} | xyz {xe:+.0f}bps (xyz liq-rate {xliq:.1f}%)")
    if xe < 0 or xe < min(me, ae) * 0.4:
        print("VERDICT: xyz STRUCTURALLY WEAK -- decision-grade case to DROP xyz from copy universe (codex + Alberto gate).")
    elif xliq > 2 * max(j[j.klass=='major'].liq_closed.mean(), j[j.klass=='alt'].liq_closed.mean()) * 100:
        print("VERDICT: xyz edge OK but liq-tail elevated -- consider tighter xyz risk (iso leverage already 5x), not a full drop.")
    else:
        print("VERDICT: xyz comparable to alts -- the live xyz drag is small-sample/execution, not the coin class.")


if __name__ == "__main__":
    main()
