#!/usr/bin/env python
"""
skill_slot_sweep.py -- DATA verification for the gross-leverage gate (Alberto: verify the edge supports the
larger size BEFORE building). Q: does allowing more CONCURRENT positions (raising gross ~2.3x -> ~4x)
capture proportionally more skill edge, or does it dilute / just add correlated drawdown?

Occupancy replay on the SKILL COHORT (the live wallets), copyable/calibrated coins, sweeping max concurrent
positions. Each position $150, 1 per coin, held entry->exit. copy-edge = net_realized_pnl/max_notional minus
a flat RT cost (relative comparison across slot caps is what matters; same trade stream every cap).

Reads: $/mo, mean edge/trade (dilution check), max concurrent, max drawdown (correlated-DD check) per cap.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/skill_slot_sweep.py
"""
import json
import numpy as np
import pandas as pd

BASE = 150.0
EQUITY = 518.0
RT_BPS = 11.0
CAPS = [14, 18, 22, 26, 30]   # 14 ~ current (2.3x gross); 28 ~ 4x gross at $150/pos


def main():
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())
    cols = ["wallet", "coin", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional", "liq_closed"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.wallet.isin(sk)) & (j.coin.isin(calib)) & (j.max_position_notional > 10)].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t_en"] = j["entry_ts"].astype("float64") / 1000.0
    j["t_ex"] = j["exit_ts"].astype("float64") / 1000.0
    j = j[j.t_ex > j.t_en].sort_values("t_en").reset_index(drop=True)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400.0
    print(f"skill-cohort copyable journeys: {len(j)} over {ndays:.0f}d")

    def replay(max_slots):
        open_pos, held, realized, fills, maxc = [], set(), [], 0, 0
        for _, r in j.iterrows():
            now = r.t_en
            if open_pos:
                keep = []
                for ex, c, p in open_pos:
                    if ex <= now:
                        held.discard(c); realized.append((ex, p))
                    else:
                        keep.append((ex, c, p))
                open_pos = keep
            if r.coin in held or len(open_pos) >= max_slots:
                continue
            pnl = (r.ret - RT_BPS / 1e4) * BASE
            open_pos.append((r.t_ex, r.coin, pnl)); held.add(r.coin)
            fills += 1; maxc = max(maxc, len(open_pos))
        for ex, c, p in open_pos:
            realized.append((ex, p))
        rl = pd.DataFrame(realized, columns=["t", "pnl"]).sort_values("t")
        eq = EQUITY + rl.pnl.cumsum().to_numpy()
        peak = np.maximum.accumulate(eq) if len(eq) else np.array([EQUITY])
        maxdd = float((peak - eq).max()) if len(eq) else 0.0
        tot = rl.pnl.sum()
        return dict(slots=max_slots, fills=fills, trips_day=fills / ndays,
                    edge_bps=(tot / (fills * BASE) * 1e4) if fills else 0,
                    usd_day=tot / ndays, usd_mo=tot / ndays * 30.4,
                    maxc=maxc, maxdd=maxdd, maxdd_pct=maxdd / EQUITY * 100,
                    gross=maxc * BASE / EQUITY)

    print(f"\n{'slots':>6}{'maxconc':>8}{'gross~':>8}{'trips/d':>9}{'edge_bps':>9}{'$/day':>8}{'$/mo':>8}{'maxDD%':>8}")
    base = None
    for c in CAPS:
        m = replay(c)
        if base is None:
            base = m
        print(f"{m['slots']:>6}{m['maxc']:>8}{m['gross']:>7.1f}x{m['trips_day']:>9.1f}{m['edge_bps']:>9.1f}"
              f"{m['usd_day']:>8.1f}{m['usd_mo']:>8.0f}{m['maxdd_pct']:>8.0f}")
    print("\nREAD: does $/mo rise with slots (edge supports the size) AND edge_bps hold (no dilution)?")
    print("If edge_bps stays flat as slots rise -> the extra positions carry the SAME edge -> the gross gate")
    print("is justified. If edge_bps decays -> dilution, the bigger size buys correlated risk not edge.")


if __name__ == "__main__":
    main()
