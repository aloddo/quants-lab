#!/usr/bin/env python
"""
exit_capture_sim.py -- does our trailing-TP (600/300bps + -1500 stop) cut WINNERS short vs the leaders'
own exits? Tests the live hypothesis from the RE feature scan (duration -> edge). Reverse-engineering frontier.

Method (candle-based, v1, hourly): for each skill-cohort journey on candle-covered coins, anchor entry price
= HL 1h candle close at entry_ts. Walk candles entry_ts->exit_ts:
  - leader_gross = (exit_px - entry_px)/entry_px * sidesign   (leader holds to their exit_ts)
  - our trail: track peak favorable excursion; ACTIVATE trail at +600bps; EXIT on 300bps retrace from peak;
    STOP at -1500bps; else exit at leader exit. our_gross = (our_exit_px - entry_px)/entry_px * sidesign.
capture_ratio = our_gross / leader_gross on WINNERS (leader_gross>0). <1 => we leave edge on the table.

LIMITATIONS (v1): hourly granularity (intra-hour path approximated by candle high/low -> trail fills are
slightly optimistic); entry/exit anchored to candle close at the fill hour (not the leader's exact fill px).
Relative capture across hold-buckets is the robust signal, not the absolute bps.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/exit_capture_sim.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pymongo import MongoClient

ACT_BPS = 600.0    # trail activation
TRAIL_BPS = 300.0  # giveback from peak
STOP_BPS = -1500.0
JOURNEYS = "app/data/v15/m02_journeys.parquet"


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "high": 1, "low": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) < 10:
            continue
        df = pd.DataFrame(rows)
        out[c] = (df["timestamp_utc"].to_numpy(), df["close"].to_numpy(), df["high"].to_numpy(), df["low"].to_numpy())
    return out


def sim_one(ts, close, high, low, t_en, t_ex, sgn):
    # slice [t_en, t_ex]
    i0 = np.searchsorted(ts, t_en)
    i1 = np.searchsorted(ts, t_ex)
    if i0 >= len(ts) or i1 <= i0:
        return None
    entry_px = close[i0]
    if entry_px <= 0:
        return None
    leader_exit_px = close[min(i1, len(ts) - 1)]
    leader_gross = (leader_exit_px - entry_px) / entry_px * sgn * 1e4  # bps
    # walk hours, track peak favorable, apply trail/stop
    peak = 0.0  # best favorable excursion bps
    activated = False
    our_exit_px = leader_exit_px  # default: exit with leader
    for k in range(i0, min(i1 + 1, len(ts))):
        fav = ((high[k] - entry_px) / entry_px * sgn if sgn > 0 else (entry_px - low[k]) / entry_px) * 1e4
        adv = ((low[k] - entry_px) / entry_px * sgn if sgn > 0 else (entry_px - high[k]) / entry_px) * 1e4
        # stop check (adverse)
        if adv <= STOP_BPS:
            our_exit_px = entry_px * (1 + sgn * STOP_BPS / 1e4)
            break
        peak = max(peak, fav)
        if peak >= ACT_BPS:
            activated = True
        if activated and (peak - fav) >= TRAIL_BPS:
            trail_px = entry_px * (1 + sgn * (peak - TRAIL_BPS) / 1e4)
            our_exit_px = trail_px
            break
    our_gross = (our_exit_px - entry_px) / entry_px * sgn * 1e4
    return leader_gross, our_gross


def main():
    cfg = json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"]
    sk = list(cfg.keys())
    dset = ds.dataset(JOURNEYS, format="parquet")
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "duration_h", "max_position_notional", "net_realized_pnl"]
    j = dset.to_table(columns=cols, filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[(j.max_position_notional > 10) & (~j.coin.str.startswith("xyz:"))].copy()
    j = j.dropna(subset=["entry_ts", "exit_ts"])
    j = j[np.isfinite(j.entry_ts) & np.isfinite(j.exit_ts)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("int64"); j["t_ex"] = j.exit_ts.astype("int64")
    j = j[j.t_ex > j.t_en]
    coins = j.coin.value_counts()
    coins = [c for c in coins.index if coins[c] >= 20]
    cand = load_candles(coins)
    j = j[j.coin.isin(cand.keys())].copy()
    print(f"journeys simulated: {len(j)} on {len(cand)} candle-covered coins\n")

    rows = []
    for _, r in j.iterrows():
        ts, cl, hi, lo = cand[r.coin]
        out = sim_one(ts, cl, hi, lo, r.t_en, r.t_ex, r.sgn)
        if out:
            rows.append((r.coin, r.duration_h, out[0], out[1]))
    d = pd.DataFrame(rows, columns=["coin", "dur_h", "leader_bps", "our_bps"])
    win = d[d.leader_bps > 0]
    print(f"=== EXIT CAPTURE (winners: leader_gross>0, n={len(win)}) ===")
    print(f"leader gross mean: {win.leader_bps.mean():+.0f}bps | our gross mean: {win.our_bps.mean():+.0f}bps")
    cr = win.our_bps.sum() / win.leader_bps.sum()
    print(f"CAPTURE RATIO (sum our / sum leader): {cr:.2f}  ({'we LEAVE edge -- trail cuts winners short' if cr < 0.9 else 'trail captures most of it'})")
    print(f"\n=== capture by hold-duration bucket (does the trail cut LONG winners most?) ===")
    for lo, hi_ in [(0, 1), (1, 4), (4, 12), (12, 48), (48, 1e9)]:
        b = win[(win.dur_h >= lo) & (win.dur_h < hi_)]
        if len(b):
            print(f"  {lo:>2.0f}-{hi_ if hi_<1e8 else 999:>3.0f}h n={len(b):>5} leader={b.leader_bps.mean():+6.0f} our={b.our_bps.mean():+6.0f} capture={b.our_bps.sum()/b.leader_bps.sum():.2f}")
    print("\nREAD: capture<1 on long-hold winners => our 600/300 trail exits before the leader => loosening the")
    print("trail (or widening activation) on majors/liquid-alts could recover edge. v1/hourly -- validate finer + codex before any change.")


if __name__ == "__main__":
    main()
