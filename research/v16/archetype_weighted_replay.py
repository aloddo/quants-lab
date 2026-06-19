#!/usr/bin/env python
"""
archetype_weighted_replay.py -- next edge toward Gate 2 (RE: mean-rev is the leaders' best archetype, +133bps;
state: "skill selection is the real edge"). Question: on TOP of the skill-selected cohort, does TILTING copy
size toward the mean-rev archetype (and away from momentum) add realized edge, or is skill-selection already
capturing it? Replay the skill cohort's copyable journeys with $150 base, evolving equity, live netx(2.5)+
gross(3.5) caps, under three sizing schemes:
  EQUAL      : every leader $150
  DROP-MOMO  : momentum leaders excluded, mean-rev+mixed $150
  ARCH-TILT  : mean-rev 1.5x, mixed 1.0x, momentum 0.5x (size tilt, same caps)
Reports edge_bps, $/mo, maxDD, peak netx per scheme. Archetype label = per-wallet mean trailing-6h entry
return (same construction as archetype_edge.py): >+50 MOMENTUM, <-50 MEAN-REV, else MIXED.

Read-only research. Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/archetype_weighted_replay.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pymongo import MongoClient

BASE = 150.0; EQ0 = 505.0; RT = 11.0; NETX = 2.5; GROSS = 3.5
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


def mark(cand, coin, t):
    if coin not in cand: return None
    ts, cl = cand[coin]; i = np.searchsorted(ts, t)
    return cl[min(i, len(cl) - 1)] if len(ts) else None


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j = ds.dataset(JOURNEYS, format="parquet").to_table(
        columns=["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"],
        filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[(j.max_position_notional > 10) & (~j.coin.str.startswith("xyz:"))].dropna(subset=["entry_ts", "exit_ts"]).copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].copy()
    vc = j.coin.value_counts(); cand = load_candles([c for c in vc.index if vc[c] >= 30])
    j = j[j.coin.isin(cand)].sort_values("t_en").reset_index(drop=True)

    # per-wallet archetype via mean trailing-6h entry return
    j["tr6"] = [tr(*cand[r.coin], r.t_en, 6, r.sgn) for r in j.itertuples()]
    g = j.dropna(subset=["tr6"]).groupby("wallet").agg(n=("tr6", "size"), tr6=("tr6", "mean"))
    g = g[g.n >= 30]
    def lab(x): return "MOMENTUM" if x > 50 else ("MEAN-REV" if x < -50 else "MIXED")
    arch = g.tr6.map(lab).to_dict()
    j["arch"] = j.wallet.map(arch)
    j = j.dropna(subset=["arch"]).reset_index(drop=True)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    print(f"journeys {len(j)} over {ndays:.0f}d | archetypes {dict(pd.Series(arch).value_counts())}\n")

    TILT = {"MEAN-REV": 1.5, "MIXED": 1.0, "MOMENTUM": 0.5}

    def replay(scheme):
        realized = 0.0; open_pos = []; taken = 0; eq_curve = []; peak_netx = 0.0
        for r in j.itertuples():
            now = r.t_en
            still = []
            for ex, c, sg, epx, p, n in open_pos:
                if ex <= now: realized += p
                else: still.append((ex, c, sg, epx, p, n))
            open_pos = still
            mtm = 0.0; net_ntl = 0.0; gross_ntl = 0.0
            for ex, c, sg, epx, p, n in open_pos:
                mpx = mark(cand, c, now)
                if mpx and epx: mtm += sg * (mpx - epx) / epx * n
                net_ntl += sg * n; gross_ntl += n
            eq = EQ0 + realized + mtm; eq_curve.append(eq)
            if scheme == "drop_momo" and r.arch == "MOMENTUM":
                continue
            size = BASE * (TILT[r.arch] if scheme == "tilt" else 1.0)
            new_signed = r.sgn * size
            if abs(net_ntl + new_signed) > NETX * eq and abs(net_ntl + new_signed) > abs(net_ntl):
                continue
            if gross_ntl + size > GROSS * max(eq, 1):
                continue
            epx = mark(cand, r.coin, now)
            pnl = (r.ret - RT / 1e4) * size
            open_pos.append((r.t_ex, r.coin, r.sgn, epx if epx else 1, pnl, size)); taken += 1
            peak_netx = max(peak_netx, abs(net_ntl + new_signed) / max(eq, 1))
        for ex, c, sg, epx, p, n in open_pos: realized += p
        ec = np.array(eq_curve); peak = np.maximum.accumulate(ec)
        maxdd = ((peak - ec) / peak).max() * 100 if len(ec) else 0
        notional = taken * BASE  # equal-weight proxy for edge_bps comparability
        return dict(taken=taken, usd_mo=realized / ndays * 30.4, maxdd=maxdd, peak_netx=peak_netx,
                    edge_bps=realized / notional * 1e4 if notional else 0)

    print(f"{'scheme':<14}{'copies':>8}{'$/mo':>9}{'edge_bps':>10}{'maxDD%':>9}{'peak_netx':>11}")
    res = {}
    for s, lbl in [("equal", "EQUAL"), ("drop_momo", "DROP-MOMO"), ("tilt", "ARCH-TILT")]:
        d = replay(s); res[s] = d
        print(f"{lbl:<14}{d['taken']:>8}{d['usd_mo']:>9.0f}{d['edge_bps']:>10.1f}{d['maxdd']:>9.1f}{d['peak_netx']:>10.2f}x")

    eq = res["equal"]
    print(f"\nvs EQUAL baseline:")
    for s, lbl in [("drop_momo", "DROP-MOMO"), ("tilt", "ARCH-TILT")]:
        d = res[s]
        du = (d["usd_mo"] / eq["usd_mo"] - 1) * 100 if eq["usd_mo"] else 0
        print(f"  {lbl:<10} $/mo {du:+.0f}% | edge {d['edge_bps']-eq['edge_bps']:+.1f}bps | maxDD {eq['maxdd']:.1f}->{d['maxdd']:.1f}%")
    best = max(res.items(), key=lambda kv: kv[1]["usd_mo"])
    win = best[0] != "equal" and res[best[0]]["usd_mo"] > eq["usd_mo"] * 1.05 and res[best[0]]["maxdd"] <= eq["maxdd"] * 1.15
    print(f"\nREAD: {'archetype tilt/drop ADDS edge on top of skill-selection -> candidate (validate + codex)' if win else 'archetype tilt does NOT beat equal-weight materially -> skill-selection already captures it; leave cohort equal-weight'}")
    print("(Leaders realized edge; copy pays same RT. Archetype label = mean trailing-6h entry return >=30 trips.)")


if __name__ == "__main__":
    main()
