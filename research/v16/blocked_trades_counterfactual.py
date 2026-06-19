#!/usr/bin/env python
"""
blocked_trades_counterfactual.py -- Alberto 9742: "if over the last 48h we hadn't blocked ANY trade, where
would we be now?" Pull every entry the live gates REJECTED (netx + knet + margin), price each at the moment it
was signaled (candle mark), mark to NOW, and sum the hypothetical PnL at OUR copy size ($150). Splits by gate
and by side -> shows whether the gates SAVED us (blocked trades would be losing) or TRAPPED us (blocked trades
would be winning, esp. the knet-blocked shorts).

Read-only. Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/blocked_trades_counterfactual.py
"""
import re
from datetime import datetime
import requests
import numpy as np
import pandas as pd
from pymongo import MongoClient

LOG = "/tmp/ql-v12-copy-trader-launchd.log"
SINCE = "2026-06-17 08:00:00"   # ~48h
SIZE = 150.0
HL = "https://api.hyperliquid.xyz/info"

ENTRY = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*V17 ENTRY: \S+ (\S+) (BUY|SELL) ")
NETX = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*NETX CAP: rejected \[\w+\] (\S+) ")
KNET = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*KNET GATE: rejected (\S+) (BUY|SELL) ")
MARG = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*Margin BLOCKED (\S+):")


def candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) > 5:
            df = pd.DataFrame(rows); out[c] = (df.timestamp_utc.to_numpy(), df.close.to_numpy())
    return out


def now_prices(coins):
    out = {}
    for dex in ["", "xyz", "flx"]:
        try:
            r = requests.post(HL, json={"type": "allMids", **({"dex": dex} if dex else {})}, timeout=8).json()
            for k, v in r.items():
                out.setdefault(k, float(v))
        except Exception:
            pass
    return out


def main():
    blocked = []   # (ts_str, coin, side, gate)
    last_entry = {}   # coin -> (ts, side)
    with open(LOG) as fh:
        for line in fh:
            m = ENTRY.search(line)
            if m and m.group(1) >= SINCE:
                last_entry[m.group(2)] = (m.group(1), m.group(3)); continue
            m = KNET.search(line)
            if m and m.group(1) >= SINCE:
                blocked.append((m.group(1), m.group(2), m.group(3), "knet")); continue
            m = NETX.search(line)
            if m and m.group(1) >= SINCE:
                coin = m.group(2); side = last_entry.get(coin, (None, "BUY"))[1]
                blocked.append((m.group(1), coin, side, "netx")); continue
            m = MARG.search(line)
            if m and m.group(1) >= SINCE:
                coin = m.group(2); side = last_entry.get(coin, (None, "BUY"))[1]
                blocked.append((m.group(1), coin, side, "margin"))

    coins = sorted(set(b[1] for b in blocked))
    cand = candles(coins); now = now_prices(coins)
    print(f"blocked entries since {SINCE}: {len(blocked)} | coins {len(coins)} | priced {len(cand)}\n")

    rows = []
    for ts, coin, side, gate in blocked:
        if coin not in cand or coin not in now:
            continue
        t_ms = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
        cts, ccl = cand[coin]; i = np.searchsorted(cts, t_ms)
        if i <= 0 or i >= len(cts):
            continue
        sig_px = ccl[i]; now_px = now[coin]
        if sig_px <= 0:
            continue
        sgn = 1 if side == "BUY" else -1
        pnl = (now_px - sig_px) / sig_px * SIZE * sgn
        rows.append(dict(coin=coin, side=side, gate=gate, pnl=pnl))
    d = pd.DataFrame(rows)
    if d.empty:
        print("no priceable blocked trades."); return

    tot = d.pnl.sum()
    print(f"=== COUNTERFACTUAL: if we'd taken all {len(d)} priceable blocked trades (@ ${SIZE} each), marked to now ===")
    print(f"TOTAL hypothetical PnL: ${tot:+.2f}\n")
    print("by GATE:")
    for g, gd in d.groupby("gate"):
        print(f"  {g:<8} n={len(gd):>4} hypothetical ${gd.pnl.sum():+8.2f}  ({(gd.pnl>0).mean()*100:.0f}% would be winners)")
    print("\nby SIDE:")
    for s, sd in d.groupby("side"):
        print(f"  {s:<6} n={len(sd):>4} hypothetical ${sd.pnl.sum():+8.2f}")
    print("\nby GATE x SIDE:")
    for (g, s), gs in d.groupby(["gate", "side"]):
        print(f"  {g+'/'+s:<14} n={len(gs):>4} ${gs.pnl.sum():+8.2f}")
    print(f"\nREAD: TOTAL {tot:+.0f}. If POSITIVE -> the gates cost us (blocked trades would be up) -> Alberto's")
    print(f"trap worry. If NEGATIVE -> the gates SAVED us (blocked trades would have deepened the hole).")
    print(f"Watch knet/SELL specifically -- those are the de-risking shorts; if knet is strongly +, we're gating our best edge.")


if __name__ == "__main__":
    main()
