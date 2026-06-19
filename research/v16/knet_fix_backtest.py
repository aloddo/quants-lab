#!/usr/bin/env python
"""
knet_fix_backtest.py -- VALIDATE the knet fix (Alberto 9745): the knet gate blocks a SHORT when the cohort is
net-LONG that coin (herd-unconfirmed = knet<0). Those are exactly the contrarian / de-risking shorts. Question:
do they have POSITIVE edge historically? If yes, knet is blocking our best edge (mean-rev shorts) and the fix
(let de-risking shorts through) is validated.

Method: for each SHORT journey, reconstruct the cohort's NET position on that coin at entry (running sum of all
cohort leaders' open positions, via per-coin enter/exit events). knet-BLOCKED = cohort net-LONG at entry
(contrarian short). knet-ALLOWED = cohort net-short/neutral. Compare each subset's edge.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/knet_fix_backtest.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

RT = 11.0


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j = ds.dataset("app/data/v15/m02_journeys.parquet", format="parquet").to_table(
        columns=["wallet", "coin", "side", "entry_ts", "exit_ts", "max_position_notional", "net_realized_pnl"],
        filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[j.max_position_notional > 10].dropna(subset=["entry_ts", "exit_ts"]).copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["edge_bps"] = (j.ret - RT / 1e4) * 1e4
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].reset_index(drop=True)

    # per-coin event timeline -> running net cohort position (count-based, signed by side)
    j["cohort_net_at_entry"] = np.nan
    for coin, g in j.groupby("coin"):
        ev = []  # (t, delta_net)
        for r in g.itertuples():
            ev.append((r.t_en, r.sgn)); ev.append((r.t_ex, -r.sgn))
        ev.sort()
        ev_t = np.array([e[0] for e in ev]); ev_c = np.cumsum([e[1] for e in ev])
        # net just BEFORE each short's entry = cumulative delta of events strictly before t_en
        idx = g.index
        for i in idx:
            t = j.at[i, "t_en"]
            k = np.searchsorted(ev_t, t, side="left")  # events strictly before t
            j.at[i, "cohort_net_at_entry"] = ev_c[k - 1] if k > 0 else 0.0

    shorts = j[j.sgn < 0].copy()
    # knet gate blocks a short when cohort is NET-LONG the coin (contrarian / herd-unconfirmed short)
    shorts["knet_blocked"] = shorts.cohort_net_at_entry > 0
    print(f"shorts: {len(shorts)} | knet-BLOCKED (cohort net-long at entry) {int(shorts.knet_blocked.sum())} "
          f"| knet-ALLOWED {int((~shorts.knet_blocked).sum())}\n")

    def stat(d, lbl):
        nb = d.edge_bps.to_numpy()
        print(f"{lbl:<34} n={len(d):>5} edge={nb.mean():+6.0f}bps median={np.median(nb):+5.0f} "
              f"win={(nb>0).mean()*100:>3.0f}% pt_Sharpe={nb.mean()/nb.std() if nb.std()>0 else 0:.2f} sum_ret={d.ret.sum():+.1f}")

    print("=== SHORT edge: what knet BLOCKS vs ALLOWS ===")
    stat(shorts, "ALL shorts")
    stat(shorts[shorts.knet_blocked], "knet-BLOCKED (contrarian)")
    stat(shorts[~shorts.knet_blocked], "knet-ALLOWED (herd-confirmed)")

    blk = shorts[shorts.knet_blocked]
    print(f"\nVERDICT: the knet-BLOCKED contrarian shorts have edge {blk.edge_bps.mean():+.0f}bps "
          f"({(blk.edge_bps>0).mean()*100:.0f}% win).")
    if blk.edge_bps.mean() > 30 and (blk.edge_bps > 0).mean() > 0.55:
        print("-> POSITIVE edge -> knet is BLOCKING PROFITABLE SHORTS. The fix (allow de-risking shorts through")
        print("   knet) is VALIDATED on history (consistent with the 48h counterfactual +$168). Propose + codex.")
    else:
        print("-> NOT clearly positive -> knet is correctly filtering weak shorts. LEAVE knet alone.")
    print("(Caveat: count-based cohort net, not notional-weighted; leaders' realized edge. Robust on sign.)")


if __name__ == "__main__":
    main()
