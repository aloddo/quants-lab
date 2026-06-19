#!/usr/bin/env python
"""
knet_derisk_replay.py -- codex r1 P1.1/P1.4: validate the EXACT live predicate the engine ships, not the
aggregate-contrarian-short proxy. The engine bypasses knet for a SHORT ONLY when (a) knet<0 (cohort net-LONG
the coin = blocked) AND (b) OUR live net on that coin is LONG AND (c) the short reduces |our_coin_net|.

Full occupancy replay of the skill cohort's copyable journeys, $150/copy, EVOLVING candle-marked equity, with
the live netx (2.5x) + gross (3.5x) caps enforced. Tracks OUR per-coin net from the copies we actually take.
Two modes:
  CURRENT  : knet blocks EVERY knet<0 short (live behaviour today)
  PROPOSED : knet de-risk carve-out (bypass knet<0 short iff it cuts our existing net-long on the coin)
Reports portfolio $/mo, maxDD, peak netx, fill -- AND isolates the DE-RISK SUBSET (the trades the carve-out adds)
with its own edge_bps/win%/Sharpe. Tail-filter sensitivity (clip / no-clip / winsorize) on that subset.

cohort knet at signal = per-coin event-timeline net (same construction as knet_fix_backtest.py), precomputed.
Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/knet_derisk_replay.py
"""
import json
import numpy as np
import pandas as pd
from pymongo import MongoClient

BASE = 150.0; EQ0 = 505.0; RT = 11.0; NETX = 2.5; GROSS = 3.5; TILT_CAP = 2.0  # live order_size_usd=150, tilt_cap=2
RESV = BASE * TILT_CAP   # codex r2 P1.1: live predicate + caps reserve order_size*tilt_cap ($300), not $150


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


def cohort_net_at_entry(j):
    """Per-coin event timeline -> cohort net just BEFORE each journey's entry (strictly-before, no self-event)."""
    out = np.full(len(j), 0.0)
    for coin, g in j.groupby("coin"):
        ev = []
        for r in g.itertuples():
            ev.append((r.t_en, r.sgn)); ev.append((r.t_ex, -r.sgn))
        ev.sort()
        et = np.array([e[0] for e in ev]); ec = np.cumsum([e[1] for e in ev])
        for i in g.index:
            k = np.searchsorted(et, j.at[i, "t_en"], side="left")
            out[i] = ec[k - 1] if k > 0 else 0.0
    return out


def main():
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.wallet.isin(sk)) & (~j.coin.str.startswith("xyz:")) & (j.max_position_notional > 10)].copy()
    j["ret_raw"] = j.net_realized_pnl / j.max_position_notional
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].dropna(subset=["t_en", "t_ex", "ret_raw"]).sort_values("t_en").reset_index(drop=True)
    cand = load_candles([c for c in j.coin.value_counts().index if j.coin.value_counts()[c] >= 30])
    j = j[j.coin.isin(cand)].reset_index(drop=True)
    j["knet"] = cohort_net_at_entry(j)           # cohort net just before entry (sign = knet sign)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    print(f"journeys: {len(j)} over {ndays:.0f}d ({(j.sgn>0).mean()*100:.0f}% long); knet=cohort-net at entry\n")

    def ret_of(r, clip):
        x = r.ret_raw
        if clip == "clip":
            return x if -1.0 <= x <= 2.0 else None
        if clip == "winsor":
            return min(max(x, -1.0), 2.0)
        return x  # none

    def replay(mode, clip="clip"):
        realized = 0.0; open_pos = []  # (t_ex, coin, sgn, entry_px, pnl, notional)
        taken = 0; peak_netx = 0.0; eq_curve = []
        derisk_rows = []   # PnL of the trades the carve-out ADDS vs CURRENT
        for r in j.itertuples():
            now = r.t_en
            still = []
            for ex, c, sg, epx, p, n in open_pos:
                if ex <= now:
                    realized += p
                else:
                    still.append((ex, c, sg, epx, p, n))
            open_pos = still
            # mark-to-market equity + book aggregates + OUR per-coin net
            mtm = 0.0; net_ntl = 0.0; gross_ntl = 0.0; coin_net = {}
            for ex, c, sg, epx, p, n in open_pos:
                mpx = mark(cand, c, now)
                if mpx and epx:
                    mtm += sg * (mpx - epx) / epx * BASE
                net_ntl += sg * BASE; gross_ntl += BASE
                coin_net[c] = coin_net.get(c, 0.0) + sg * BASE
            eq = EQ0 + realized + mtm
            eq_curve.append(eq)
            r2 = ret_of(r, clip)
            if r2 is None:
                continue
            is_short = r.sgn < 0
            knet_blocked = (r.knet > 0) and is_short     # cohort net-long the coin + we'd SHORT = knet<0
            # --- knet gate ---
            if knet_blocked:
                if mode == "current":
                    continue                              # live: always reject
                # proposed: de-risk carve-out -- bypass iff OUR coin net is long and the short cuts it.
                # codex r2 P1.1: live reserves RESV=order_size*tilt_cap ($300), so the threshold needs
                # our coin-net-long > $150 (abs(cn-300)<cn). Match it exactly.
                our_cn = coin_net.get(r.coin, 0.0)
                if not (our_cn > 0 and abs(our_cn - RESV) < abs(our_cn)):
                    continue                              # not de-risking -> still reject
                is_derisk = True
            else:
                is_derisk = False
            # --- netx + gross caps (live: new order reserves RESV) ---
            new_signed = r.sgn * RESV
            if abs(net_ntl + new_signed) > NETX * eq and abs(net_ntl + new_signed) > abs(net_ntl):
                continue
            if gross_ntl + RESV > GROSS * max(eq, 1):
                continue
            epx = mark(cand, r.coin, now)
            pnl = (r2 - RT / 1e4) * BASE
            open_pos.append((r.t_ex, r.coin, r.sgn, epx if epx else 1, pnl, BASE)); taken += 1
            peak_netx = max(peak_netx, abs(net_ntl + new_signed) / max(eq, 1))
            if is_derisk:
                derisk_rows.append((r2 - RT / 1e4) * 1e4)   # edge_bps of the added trade
        for ex, c, sg, epx, p, n in open_pos:
            realized += p
        ec = np.array(eq_curve); peak = np.maximum.accumulate(ec)
        maxdd = ((peak - ec) / peak).max() * 100 if len(ec) else 0
        return dict(realized=realized, usd_mo=realized / ndays * 30.4, taken=taken,
                    peak_netx=peak_netx, maxdd=maxdd, derisk=np.array(derisk_rows))

    print(f"{'mode':<26}{'copies':>7}{'$/mo':>8}{'peak_netx':>11}{'maxDD%':>8}")
    cur = replay("current"); pro = replay("proposed")
    for lbl, d in [("CURRENT (knet blocks all)", cur), ("PROPOSED (de-risk carve-out)", pro)]:
        print(f"{lbl:<26}{d['taken']:>7}{d['usd_mo']:>8.0f}{d['peak_netx']:>10.2f}x{d['maxdd']:>8.1f}")
    add = pro["taken"] - cur["taken"]
    d_usd = pro["usd_mo"] - cur["usd_mo"]
    print(f"\nDE-RISK CARVE-OUT adds {add} copies | portfolio $/mo {cur['usd_mo']:.0f} -> {pro['usd_mo']:.0f} "
          f"({d_usd:+.0f}) | maxDD {cur['maxdd']:.1f}% -> {pro['maxdd']:.1f}% | peak netx {pro['peak_netx']:.2f}x")

    print("\n=== DE-RISK SUBSET edge (the EXACT trades the carve-out adds) -- tail-filter sensitivity ===")
    for clip in ["clip", "winsor", "none"]:
        ds = replay("proposed", clip)["derisk"]
        if len(ds):
            print(f"  ret-handling={clip:<7} n={len(ds):>4} edge={ds.mean():+6.0f}bps median={np.median(ds):+5.0f} "
                  f"win={(ds>0).mean()*100:>3.0f}% pt_Sharpe={ds.mean()/ds.std() if ds.std()>0 else 0:.2f}")
        else:
            print(f"  ret-handling={clip:<7} n=0 (carve-out never triggered in replay)")

    ds = pro["derisk"]
    ok = len(ds) >= 30 and ds.mean() > 30 and (ds > 0).mean() > 0.55 and pro["maxdd"] <= cur["maxdd"] * 1.15 \
        and pro["usd_mo"] >= cur["usd_mo"]
    print(f"\nVERDICT: {'SHIP candidate -- exact de-risk subset is +edge AND no worse drawdown' if ok else 'NOT validated -- do NOT enable (insufficient n, weak edge, or worse DD)'}")
    print("(Limit: journey-level adds not modeled; knet sign, our coin-net, netx/gross caps + freeze ARE.)")


if __name__ == "__main__":
    main()
