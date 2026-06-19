#!/usr/bin/env python
"""
cap_level_sweep_v2.py -- live-faithful rebuild after codex NO-SHIP on v1 (Alberto 9763 throttle).
Codex P0s addressed:
  P0.1/P0.2 (existing pos at flat $150 vs live actual tilted notional): model EVERY fill at a CONSISTENT
    notional N and SWEEP N in {150, 225, 300} (base / mid-tilt / max-tilt = order_size*tilt_cap). If the
    gross-loosening benefit survives across all three, it is robust to the unknown per-journey tilt. Gross,
    net, and PnL all use the SAME N (no $150-vs-$300 inconsistency).
  P1.7/P0.3 (hard validator gate <= backstop-0.5; thin buffer): cap the gate sweep at 4.5 and COUNT
    backstop-trip events (times gross would exceed 5.0x -> a trim) at each gate.
Remaining LIMIT (documented, next gate before ship): xyz iso-5x + margin-util(0.70) interaction is NOT modeled
  here (non-xyz universe); flagged for the final pre-ship validation.

netx held at 2.5 (codex agreed it is the directional-risk control; saved $407 in the drawdown).
Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/cap_level_sweep_v2.py
"""
import json
import numpy as np
import pandas as pd
from pymongo import MongoClient

EQ0 = 505.0; RT = 11.0; NETX = 2.5; BACKSTOP = 5.0
N_GRID = [150.0, 225.0, 300.0]          # base / mid-tilt / max-tilt
GROSS_GRID = [3.5, 4.0, 4.5]            # 4.5 = hard ceiling (gate <= backstop 5.0 - 0.5)


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
    return cl[min(i, len(cl) - 1)] if len(ts) else None


def main():
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.wallet.isin(sk)) & (~j.coin.str.startswith("xyz:")) & (j.max_position_notional > 10)].copy()
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].dropna(subset=["t_en", "t_ex"]).sort_values("t_en").reset_index(drop=True)
    cand = load_candles([c for c in j.coin.value_counts().index if j.coin.value_counts()[c] >= 30])
    j = j[j.coin.isin(cand)].reset_index(drop=True)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    print(f"journeys {len(j)} over {ndays:.0f}d ({(j.sgn>0).mean()*100:.0f}% long). netx={NETX}. CONSISTENT-notional, sweep N.\n")

    def replay(N, gross):
        # every fill uses notional N (existing AND new) -> codex P0.1/P0.2 consistency. Backstop: if gross
        # exceeds BACKSTOP*eq, trim worst-uPnL positions back toward gross*eq (count the event).
        realized = 0.0; open_pos = []; taken = 0; eq_curve = []; peak_netx = 0.0; backstop_trips = 0
        for r in j.itertuples():
            now = r.t_en
            still = []
            for ex, c, sg, epx, p in open_pos:
                if ex <= now: realized += p
                else: still.append((ex, c, sg, epx, p))
            open_pos = still
            # MTM + aggregates on consistent N
            mtm = 0.0; net_ntl = 0.0; gross_ntl = 0.0
            marks = []
            for ex, c, sg, epx, p in open_pos:
                mpx = mark(cand, c, now); upl = sg * (mpx - epx) / epx * N if (mpx and epx) else 0.0
                mtm += upl; net_ntl += sg * N; gross_ntl += N; marks.append(upl)
            eq = EQ0 + realized + mtm; eq_curve.append(eq)
            # backstop: gross over BACKSTOP*eq -> trim worst-uPnL to <= gross*eq (realize their pnl)
            if gross_ntl > BACKSTOP * max(eq, 1) and open_pos:
                backstop_trips += 1
                order = np.argsort(marks)  # worst uPnL first
                keep = list(open_pos); target = gross * max(eq, 1)
                g = gross_ntl
                for idx in order:
                    if g <= target: break
                    ex, c, sg, epx, p = open_pos[idx]
                    realized += p; g -= N
                    keep[idx] = None
                open_pos = [pp for pp in keep if pp is not None]
                net_ntl = sum(sg * N for _, _, sg, _, _ in open_pos); gross_ntl = g
            # entry gate (consistent N for the new order too)
            new_signed = r.sgn * N
            if abs(net_ntl + new_signed) > NETX * eq and abs(net_ntl + new_signed) > abs(net_ntl):
                continue
            if gross_ntl + N > gross * max(eq, 1):
                continue
            epx = mark(cand, r.coin, now)
            pnl = (r.ret - RT / 1e4) * N
            open_pos.append((r.t_ex, r.coin, r.sgn, epx if epx else 1, pnl)); taken += 1
            peak_netx = max(peak_netx, abs(net_ntl + new_signed) / max(eq, 1))
        for ex, c, sg, epx, p in open_pos: realized += p
        ec = np.array(eq_curve); peak = np.maximum.accumulate(ec)
        maxdd = ((peak - ec) / peak).max() * 100 if len(ec) else 0
        return dict(taken=taken, usd_mo=realized / ndays * 30.4, peak_netx=peak_netx, maxdd=maxdd, trips=backstop_trips)

    robust = True
    print(f"{'N':>5}{'gross':>7}{'copies':>8}{'$/mo':>8}{'vs3.5':>7}{'peak_netx':>11}{'maxDD%':>8}{'bkstp':>7}")
    for N in N_GRID:
        base = replay(N, 3.5)
        for gr in GROSS_GRID:
            d = replay(N, gr)
            dv = (d["usd_mo"] / base["usd_mo"] - 1) * 100 if base["usd_mo"] else 0
            tag = " <-base" if gr == 3.5 else ""
            print(f"{N:>5.0f}{gr:>7.1f}{d['taken']:>8}{d['usd_mo']:>8.0f}{dv:>+6.0f}%{d['peak_netx']:>10.2f}x{d['maxdd']:>8.1f}{d['trips']:>7}{tag}")
        # robustness: does gross 4.0 beat 3.5 by >3% at this N?
        d40 = replay(N, 4.0)
        if not (d40["usd_mo"] > base["usd_mo"] * 1.03):
            robust = False
        print()

    print(f"ROBUSTNESS: gross 4.0 > 3.5 by >3% $/mo at ALL notional levels (150/225/300)? -> {'YES' if robust else 'NO'}")
    print("READ: if YES across all N -> the gross-throttle finding survives the tilt-modeling gap (codex P0.1/P0.2);")
    print("pick gross 4.0 (buffer to 5.0 backstop, low trip count). If NO -> magnitude was a flat-$150 artifact.")
    print("REMAINING GATE before ship: xyz iso-5x + margin-util(0.70) interaction (not modeled here). Then re-codex.")


if __name__ == "__main__":
    main()
