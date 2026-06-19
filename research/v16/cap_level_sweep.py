#!/usr/bin/env python
"""
cap_level_sweep.py -- Alberto 9763 ("we didn't trade for 2 days for these restrictions"): the netx(2.5x)+
gross(3.5x) caps blocked ~2/3 of entry signals over 48h (187 netx + 170 gross rejects of 284 signals). Does
LOOSENING the caps capture materially more edge WITHOUT materially worse drawdown? Sweep netx x gross and
measure. Candle-marked evolving equity so the pro-cyclical tightening + the freeze-in-drawdown are MODELED
(the very dynamic Alberto is feeling). $150/copy, skill cohort, RT 11bps.

Reports per (netx,gross): copies (fill-rate), edge $/mo, peak net exposure (risk), MTM maxDD%.
Current LIVE = netx 2.5 / gross 3.5. Validate before any change; codex before ship (rule 13).
Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/cap_level_sweep.py
"""
import json
import numpy as np
import pandas as pd
from pymongo import MongoClient

BASE = 150.0; EQ0 = 505.0; RT = 11.0; TILT_CAP = 2.0
RESV = BASE * TILT_CAP
NETX_GRID = [2.5, 3.0, 3.5, 4.0]
GROSS_GRID = [3.5, 4.0, 4.5, 5.0]


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
    print(f"journeys {len(j)} over {ndays:.0f}d ({(j.sgn>0).mean()*100:.0f}% long), candle-marked equity. RESV=${RESV:.0f}\n")

    def replay(netx, gross):
        realized = 0.0; open_pos = []; taken = 0; eq_curve = []; peak_netx = 0.0
        for r in j.itertuples():
            now = r.t_en
            still = []
            for ex, c, sg, epx, p in open_pos:
                if ex <= now: realized += p
                else: still.append((ex, c, sg, epx, p))
            open_pos = still
            mtm = 0.0; net_ntl = 0.0; gross_ntl = 0.0
            for ex, c, sg, epx, p in open_pos:
                mpx = mark(cand, c, now)
                if mpx and epx: mtm += sg * (mpx - epx) / epx * BASE
                net_ntl += sg * BASE; gross_ntl += BASE
            eq = EQ0 + realized + mtm; eq_curve.append(eq)
            new_signed = r.sgn * RESV
            if abs(net_ntl + new_signed) > netx * eq and abs(net_ntl + new_signed) > abs(net_ntl):
                continue
            if gross_ntl + RESV > gross * max(eq, 1):
                continue
            epx = mark(cand, r.coin, now)
            pnl = (r.ret - RT / 1e4) * BASE
            open_pos.append((r.t_ex, r.coin, r.sgn, epx if epx else 1, pnl)); taken += 1
            peak_netx = max(peak_netx, abs(net_ntl + new_signed) / max(eq, 1))
        for ex, c, sg, epx, p in open_pos: realized += p
        ec = np.array(eq_curve); peak = np.maximum.accumulate(ec)
        maxdd = ((peak - ec) / peak).max() * 100 if len(ec) else 0
        return dict(taken=taken, usd_mo=realized / ndays * 30.4, peak_netx=peak_netx, maxdd=maxdd)

    base = replay(2.5, 3.5)
    print(f"LIVE (netx 2.5 / gross 3.5): copies {base['taken']}, ${base['usd_mo']:.0f}/mo, peak_netx {base['peak_netx']:.2f}x, maxDD {base['maxdd']:.1f}%\n")
    print(f"{'netx':>5}{'gross':>6}{'copies':>8}{'$/mo':>8}{'vs_base':>9}{'peak_netx':>11}{'maxDD%':>8}")
    rows = []
    for nx in NETX_GRID:
        for gr in GROSS_GRID:
            d = replay(nx, gr)
            dv = (d["usd_mo"] / base["usd_mo"] - 1) * 100 if base["usd_mo"] else 0
            star = " <-LIVE" if (nx == 2.5 and gr == 3.5) else ""
            rows.append((nx, gr, d))
            print(f"{nx:>5.1f}{gr:>6.1f}{d['taken']:>8}{d['usd_mo']:>8.0f}{dv:>+8.0f}%{d['peak_netx']:>10.2f}x{d['maxdd']:>8.1f}{star}")

    # best by $/mo with maxDD guardrail (<=15% worse than live)
    ok = [(nx, gr, d) for nx, gr, d in rows if d["maxdd"] <= base["maxdd"] * 1.15]
    best = max(ok, key=lambda x: x[2]["usd_mo"]) if ok else None
    print()
    if best and best[2]["usd_mo"] > base["usd_mo"] * 1.05 and not (best[0] == 2.5 and best[1] == 3.5):
        print(f"CANDIDATE: netx {best[0]} / gross {best[1]} -> ${best[2]['usd_mo']:.0f}/mo "
              f"(+{(best[2]['usd_mo']/base['usd_mo']-1)*100:.0f}% vs live), maxDD {best[2]['maxdd']:.1f}% "
              f"(vs {base['maxdd']:.1f}%), peak netx {best[2]['peak_netx']:.2f}x. Bring to Alberto + codex.")
    else:
        print(f"NO loosening beats live by >5% within the maxDD guardrail -> the caps are NOT leaving edge on the")
        print(f"table in aggregate; the 2-day throttle is a LOCAL drawdown-state effect (held net-long book eats")
        print(f"the budget), self-correcting as equity recovers. Keep current caps (data, not sentiment).")
    print("(Limit: journey-level adds not modeled; netx/gross + pro-cyclical equity + freeze ARE. v1 candle-hourly.)")


if __name__ == "__main__":
    main()
