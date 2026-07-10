#!/usr/bin/env python
"""
exit_trail_v2_backtest.py -- recover the exit-capture leak (research/2026-06-16-exit-capture-trail-cuts-longrun-
winners): our 600/300 trail captures 0.91 of leader winner-edge overall but only 0.58 on >48h trend-runs (exit
+270bps while leader rides +469bps). Candidate fix: a PEAK-SCALED giveback -- widen the trail giveback ONLY
once a position has run far (peak excursion is observable in real-time, so this is causal / live-implementable),
so big trend-winners ride longer, while the 0-48h bulk (capture ~1.0) is untouched.

CRITICAL: measure the NET effect across ALL journeys (winners AND reversals), not just winner-capture -- a wider
giveback also gives back MORE on positions that peak then reverse. Reports total captured bps/journey (net of
RT), winner-capture by hold bucket, and the reversal cost (activated-then-ended-negative journeys).

Candle-walk identical to exit_capture_sim.py (v1 hourly). Validate finer + codex before any live change.
Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/exit_trail_v2_backtest.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pymongo import MongoClient

ACT_BPS = 600.0; STOP_BPS = -1500.0; RT = 11.0
JOURNEYS = "app/data/v15/m02_journeys.parquet"

# trail schedules: peak-excursion (bps) -> giveback (bps). Baseline = flat 300.
SCHEDULES = {
    "BASE 300 (live)":      [(0, 300)],
    "peak>=1000 ->500":     [(0, 300), (1000, 500)],
    "peak>=1000->500/2000->800": [(0, 300), (1000, 500), (2000, 800)],
    "peak>=800->450/1500->650":  [(0, 300), (800, 450), (1500, 650)],
    # ride WITH the leader (disable trail) once the position has run big -- giveback 1e9 never triggers
    "peak>=1500 ride-leader":    [(0, 300), (1500, 1e9)],
    "peak>=2500 ride-leader":    [(0, 300), (2500, 1e9)],
}


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "high": 1, "low": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) < 10:
            continue
        df = pd.DataFrame(rows)
        out[c] = (df.timestamp_utc.to_numpy(), df.close.to_numpy(), df.high.to_numpy(), df.low.to_numpy())
    return out


def giveback_for(peak, sched):
    g = sched[0][1]
    for thr, gb in sched:
        if peak >= thr:
            g = gb
    return g


def sim_one(ts, close, high, low, t_en, t_ex, sgn, sched):
    i0 = np.searchsorted(ts, t_en); i1 = np.searchsorted(ts, t_ex)
    if i0 >= len(ts) or i1 <= i0:
        return None
    entry_px = close[i0]
    if entry_px <= 0:
        return None
    leader_exit_px = close[min(i1, len(ts) - 1)]
    leader_gross = (leader_exit_px - entry_px) / entry_px * sgn * 1e4
    peak = 0.0; activated = False; our_exit_px = leader_exit_px; activated_ever = False
    for k in range(i0, min(i1 + 1, len(ts))):
        fav = ((high[k] - entry_px) / entry_px * sgn if sgn > 0 else (entry_px - low[k]) / entry_px) * 1e4
        adv = ((low[k] - entry_px) / entry_px * sgn if sgn > 0 else (entry_px - high[k]) / entry_px) * 1e4
        if adv <= STOP_BPS:
            our_exit_px = entry_px * (1 + sgn * STOP_BPS / 1e4); break
        peak = max(peak, fav)
        if peak >= ACT_BPS:
            activated = True; activated_ever = True
        if activated and (peak - fav) >= giveback_for(peak, sched):
            our_exit_px = entry_px * (1 + sgn * (peak - giveback_for(peak, sched)) / 1e4); break
    our_gross = (our_exit_px - entry_px) / entry_px * sgn * 1e4
    return leader_gross, our_gross, activated_ever


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "duration_h", "max_position_notional"]
    j = ds.dataset(JOURNEYS, format="parquet").to_table(columns=cols, filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[(j.max_position_notional > 10) & (~j.coin.str.startswith("xyz:"))].dropna(subset=["entry_ts", "exit_ts"]).copy()
    j = j[np.isfinite(j.entry_ts) & np.isfinite(j.exit_ts)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("int64"); j["t_ex"] = j.exit_ts.astype("int64")
    j = j[j.t_ex > j.t_en]
    vc = j.coin.value_counts(); cand = load_candles([c for c in vc.index if vc[c] >= 20])
    j = j[j.coin.isin(cand)].reset_index(drop=True)
    print(f"journeys: {len(j)} on {len(cand)} candle-covered coins\n")

    # precompute candle slices once; run each schedule
    base_rows = None
    print(f"{'schedule':<30}{'our_net_bps/j':>14}{'vs_base':>9}{'>48h_cap':>10}{'rev_cost_bps':>13}")
    for name, sched in SCHEDULES.items():
        rows = []
        for r in j.itertuples():
            ts, cl, hi, lo = cand[r.coin]
            out = sim_one(ts, cl, hi, lo, r.t_en, r.t_ex, r.sgn, sched)
            if out:
                rows.append((r.duration_h, out[0], out[1], out[2]))
        d = pd.DataFrame(rows, columns=["dur_h", "leader_bps", "our_bps", "activated"])
        net = d.our_bps.mean() - RT
        win = d[d.leader_bps > 0]
        long_win = win[win.dur_h >= 48]
        cap48 = long_win.our_bps.sum() / long_win.leader_bps.sum() if len(long_win) and long_win.leader_bps.sum() else 0
        # reversal cost: journeys that activated (+600 peak) but ended negative -> what we gave back
        rev = d[(d.activated) & (d.our_bps < 0)]
        rev_cost = rev.our_bps.mean() if len(rev) else 0
        if base_rows is None:
            base_net = net; base_rows = d
        vs = net - base_net
        print(f"{name:<30}{net:>14.1f}{vs:>+9.1f}{cap48:>10.2f}{rev_cost:>13.0f}")

    print("\nREAD: pick the schedule with the HIGHEST our_net_bps/journey (net of RT) AND >48h capture lifted")
    print("toward 1.0, WITHOUT the reversal cost eating the gain. If BASE is best -> leak not worth the giveback")
    print("risk, leave the trail. v1/hourly (intra-hour trail fills slightly optimistic) -- finer-grain + codex before ship.")


if __name__ == "__main__":
    main()
