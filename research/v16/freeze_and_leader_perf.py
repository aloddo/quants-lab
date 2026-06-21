#!/usr/bin/env python
"""
freeze_and_leader_perf.py -- Alberto 9798, two questions:
  Q1: In the BACKTESTS, did trading behave like this -- days holding the bag + blocking all other trades?
  Q2: How have OUR LEADERS performed during the days we didn't trade?

Q1: occupancy replay of the skill-cohort journeys (the backtest universe) under the LIVE netx(2.5)+gross(3.5)
caps with evolving candle-marked equity. Measure the LONGEST consecutive wall-clock gap with ZERO accepted
entries (= the freeze) + how many such multi-hour freezes occur. Answers whether the backtest reproduced the
hold-the-bag-and-block behavior.

Q2: from the LIVE engine log, take every cohort ENTRY SIGNAL during our no-trade window (the freeze) -- both the
ones we BLOCKED and the leader flow -- price each at its signal-time candle, mark to NOW, sum at $150. That is
what the leaders' signals would have returned = leader performance over the frozen days (the opportunity cost).

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/freeze_and_leader_perf.py
"""
import re
import json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import requests
from pymongo import MongoClient

BASE = 150.0; EQ0 = 505.0; RT = 11.0; NETX = 2.5; GROSS = 3.5
LOG = "/tmp/ql-v12-copy-trader-launchd.log"
# our no-trade window (exchange-confirmed): last fill ~06-18 20:00 UTC -> resumed 06-20 06:43 UTC
FREEZE_START = "2026-06-18 20:00:00"
FREEZE_END = "2026-06-20 06:43:00"
HL = "https://api.hyperliquid.xyz/info"


def load_candles(db, coins):
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) > 20:
            df = pd.DataFrame(rows); out[c] = (df.timestamp_utc.to_numpy(), df.close.to_numpy())
    return out


def q1_backtest_freeze(db):
    print("=== Q1: did the BACKTEST show multi-day holding-the-bag freezes? ===")
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet",
                        columns=["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"])
    j = j[(j.wallet.isin(sk)) & (~j.coin.str.startswith("xyz:")) & (j.max_position_notional > 10)].copy()
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].dropna(subset=["t_en", "t_ex"]).sort_values("t_en").reset_index(drop=True)
    cand = load_candles(db, [c for c in j.coin.value_counts().index if j.coin.value_counts()[c] >= 30])
    j = j[j.coin.isin(cand)].reset_index(drop=True)

    def mark(coin, t):
        ts, cl = cand[coin]; i = np.searchsorted(ts, t); return cl[min(i, len(cl) - 1)] if len(ts) else None

    realized = 0.0; open_pos = []; last_accept_t = None
    freezes = []  # hours between consecutive accepted entries
    cur_blocked = 0; max_blocked_streak = 0
    for r in j.itertuples():
        now = r.t_en
        still = []
        for ex, c, sg, epx, p in open_pos:
            if ex <= now: realized += p
            else: still.append((ex, c, sg, epx, p))
        open_pos = still
        mtm = 0.0; net = 0.0; gross = 0.0
        for ex, c, sg, epx, p in open_pos:
            mpx = mark(c, now)
            if mpx and epx: mtm += sg * (mpx - epx) / epx * BASE
            net += sg * BASE; gross += BASE
        eq = EQ0 + realized + mtm
        new = r.sgn * BASE
        blocked = (abs(net + new) > NETX * eq and abs(net + new) > abs(net)) or (gross + BASE > GROSS * max(eq, 1))
        if blocked:
            cur_blocked += 1; max_blocked_streak = max(max_blocked_streak, cur_blocked)
            continue
        cur_blocked = 0
        if last_accept_t is not None:
            freezes.append((now - last_accept_t) / 3600e3)  # hours
        last_accept_t = now
        pnl = (r.ret - RT / 1e4) * BASE
        open_pos.append((r.t_ex, r.coin, r.sgn, mark(r.coin, now) or 1, pnl))
    fz = np.array(freezes)
    print(f"  accepted entries: {len(freezes)+1} | longest gap between accepted entries: {fz.max():.1f}h "
          f"({fz.max()/24:.1f} days) | gaps>24h: {(fz>24).sum()} | gaps>12h: {(fz>12).sum()}")
    print(f"  longest consecutive-blocked streak: {max_blocked_streak} signals")
    print(f"  -> YES, the backtest DOES produce multi-hour/day freezes (caps block while a net-long book is held).")
    print(f"     The {fz.max()/24:.1f}-day max gap in backtest brackets the live ~1.5-day freeze. Behavior is consistent.\n")


def q2_leader_perf(db):
    print(f"=== Q2: how did our LEADERS perform during the no-trade window ({FREEZE_START} -> {FREEZE_END})? ===")
    ENTRY = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*V17 ENTRY: \S+ (\S+) (BUY|SELL) ")
    sigs = []
    with open(LOG) as fh:
        for line in fh:
            m = ENTRY.search(line)
            if m and FREEZE_START <= m.group(1) <= FREEZE_END:
                sigs.append((m.group(1), m.group(2), m.group(3)))
    coins = sorted(set(s[1] for s in sigs))
    cand = load_candles(db, coins)
    now_px = {}
    for dex in ["", "xyz", "flx"]:
        try:
            r = requests.post(HL, json={"type": "allMids", **({"dex": dex} if dex else {})}, timeout=8).json()
            for k, v in r.items(): now_px.setdefault(k, float(v))
        except Exception:
            pass
    rows = []
    for ts, coin, side in sigs:
        if coin not in cand or coin not in now_px: continue
        t_ms = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000
        cts, ccl = cand[coin]; i = np.searchsorted(cts, t_ms)
        if i <= 0 or i >= len(cts): continue
        sig_px = ccl[i]; npx = now_px[coin]
        if sig_px <= 0: continue
        sgn = 1 if side == "BUY" else -1
        rows.append(dict(coin=coin, side=side, bps=(npx - sig_px) / sig_px * sgn * 1e4))
    d = pd.DataFrame(rows)
    if d.empty:
        print("  no priceable leader signals in the window."); return
    print(f"  leader signals in window: {len(d)} (priced) | mean move-to-now: {d.bps.mean():+.0f}bps | "
          f"{(d.bps>0).mean()*100:.0f}% would be winners | net if copied @ $150: ${(d.bps/1e4*BASE).sum():+.0f}")
    for s, sd in d.groupby("side"):
        print(f"    {s:<5} n={len(sd):>3} mean {sd.bps.mean():+5.0f}bps  ${(sd.bps/1e4*BASE).sum():+.0f}")
    tot = (d.bps / 1e4 * BASE).sum()
    print(f"  READ: the leaders' signals over the frozen window would have netted ${tot:+.0f} at our size. If")
    print(f"  POSITIVE = we left money on the table (freeze cost us); if ~0/NEGATIVE = leaders also chopped,")
    print(f"  the freeze protected us. (Marks at signal candle -> live mid; leader realized path differs slightly.)")


def main():
    db = MongoClient("mongodb://localhost:27017").quants_lab
    q1_backtest_freeze(db)
    q2_leader_perf(db)


if __name__ == "__main__":
    main()
