#!/usr/bin/env python
"""
antifreeze_netx_backtest.py -- VALIDATED backtest of the anti-freeze fix (Alberto 9722: "bring me a validated
backtest first or it's bullshit"). Question: anchoring the netx cap to a HIGH-WATER-MARK equity (proposed) vs
to CURRENT equity (live, pro-cyclical) -- does it capture MORE edge in drawdowns WITHOUT worse drawdown/risk?

Rigorous: equity is marked-to-market each decision (open positions priced at the coin's candle), so the
pro-cyclical tightening + the freeze are actually modeled (the live freeze comes from open-MTM cutting equity).
Replay skill-cohort journeys, $150/copy, netx cap 2.5x, two anchor modes + unconstrained. Reports edge $/mo,
fill-rate, PEAK net exposure (risk), and MTM max-drawdown.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/antifreeze_netx_backtest.py
"""
import json
import numpy as np
import pandas as pd
from pymongo import MongoClient

BASE = 150.0; EQ0 = 505.0; RT = 11.0; NETX = 2.5; GROSS = 3.5


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) > 20:
            df = pd.DataFrame(rows); out[c] = (df.timestamp_utc.to_numpy(), df.close.to_numpy())
    return out


def mark(cand, coin, t):
    if coin not in cand:
        return None
    ts, cl = cand[coin]; i = np.searchsorted(ts, t)
    return cl[min(i, len(cl) - 1)] if i < len(ts) or len(ts) else None


def main():
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.wallet.isin(sk)) & (j.coin.isin(calib)) & (~j.coin.str.startswith("xyz:")) & (j.max_position_notional > 10)].copy()
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].dropna(subset=["t_en", "t_ex"]).sort_values("t_en").reset_index(drop=True)
    cand = load_candles([c for c in j.coin.value_counts().index if j.coin.value_counts()[c] >= 30])
    j = j[j.coin.isin(cand)].copy().reset_index(drop=True)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    print(f"journeys: {len(j)} over {ndays:.0f}d ({(j.sgn>0).mean()*100:.0f}% long), candle-marked equity\n")

    def replay(mode):   # mode: 'none' | 'current' | 'hwm'
        realized = 0.0; hwm = EQ0; open_pos = []  # (t_ex, coin, sgn, entry_px, pnl)
        taken = 0; blocked = 0; peak_netx = 0.0; eq_curve = []; freeze = cur_freeze = 0
        for r in j.itertuples():
            now = r.t_en
            # close due
            still = []
            for ex, c, sg, epx, p in open_pos:
                if ex <= now:
                    realized += p
                else:
                    still.append((ex, c, sg, epx, p))
            open_pos = still
            # mark-to-market equity
            mtm = 0.0; net_ntl = 0.0; gross_ntl = 0.0
            for ex, c, sg, epx, p in open_pos:
                mpx = mark(cand, c, now)
                if mpx and epx:
                    mtm += sg * (mpx - epx) / epx * BASE
                net_ntl += sg * BASE; gross_ntl += BASE
            eq = EQ0 + realized + mtm
            hwm = max(hwm, eq); eq_curve.append(eq)
            cap_eq = eq if mode == "current" else (hwm if mode == "hwm" else None)
            # entry decision
            new_signed = r.sgn * BASE; epx = mark(cand, r.coin, now)
            if mode != "none":
                if abs(net_ntl + new_signed) > NETX * cap_eq or gross_ntl + BASE > GROSS * max(cap_eq, 1):
                    blocked += 1; cur_freeze += 1; freeze = max(freeze, cur_freeze); continue
            cur_freeze = 0
            pnl = (r.ret - RT / 1e4) * BASE
            open_pos.append((r.t_ex, r.coin, r.sgn, epx if epx else 1, pnl)); taken += 1
            peak_netx = max(peak_netx, abs(net_ntl + new_signed) / max(eq, 1))
        for ex, c, sg, epx, p in open_pos:
            realized += p
        ec = np.array(eq_curve); peak = np.maximum.accumulate(ec); maxdd = ((peak - ec) / peak).max() * 100 if len(ec) else 0
        return dict(realized=realized, usd_mo=realized / ndays * 30.4, taken=taken, blocked=blocked,
                    peak_netx=peak_netx, maxdd=maxdd, freeze=freeze)

    print(f"{'mode':<28}{'copies':>7}{'$/mo':>8}{'peak_netx':>10}{'maxDD%':>8}{'longest_freeze':>15}")
    res = {}
    for m, lbl in [("none", "UNCONSTRAINED"), ("current", "netx@CURRENT eq (LIVE)"), ("hwm", "netx@HWM eq (PROPOSED)")]:
        d = replay(m); res[m] = d
        print(f"{lbl:<28}{d['taken']:>7}{d['usd_mo']:>8.0f}{d['peak_netx']:>9.2f}x{d['maxdd']:>8.1f}{d['blocked']:>10} blk")
    print()
    cur, hwm = res["current"], res["hwm"]
    d_usd = (hwm["usd_mo"] / cur["usd_mo"] - 1) * 100 if cur["usd_mo"] else 0
    print(f"PROPOSED (HWM) vs LIVE (current): $/mo {d_usd:+.0f}% | extra copies {hwm['taken']-cur['taken']} | "
          f"peak netx {cur['peak_netx']:.2f}x -> {hwm['peak_netx']:.2f}x | maxDD {cur['maxdd']:.1f}% -> {hwm['maxdd']:.1f}%")
    better = hwm["usd_mo"] > cur["usd_mo"] * 1.03 and hwm["maxdd"] <= cur["maxdd"] * 1.15
    print(f"VERDICT: {'SHIP candidate -- more edge captured without materially worse drawdown' if better else 'NOT clearly better -- do NOT ship (Alberto: validated or bullshit)'}")
    print("(Limitation: journey-level adds aren't modeled; netx anchor + freeze ARE, via candle-marked equity.)")


if __name__ == "__main__":
    main()
