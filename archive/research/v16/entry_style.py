#!/usr/bin/env python
"""
entry_style.py -- Reverse-engineering: what setup do skilled leaders ENTER on? Momentum (enter after a move
in their direction) or mean-reversion (enter against a recent move)? And do WINNING entries differ from losers
in their entry context -> a predictive signal we could generate ourselves.

For each skill-cohort journey: trailing coin return over 1h/6h/24h BEFORE entry (HL 1h candles), signed by side
(positive = the move was IN the trade's direction before entry = momentum; negative = entered against the move =
mean-reversion). Split by side and by outcome (winner/loser). The win-vs-lose gap in entry context = the edge.

Memory-safe pyarrow scan. Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/entry_style.py
"""
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pymongo import MongoClient

JOURNEYS = "app/data/v15/m02_journeys.parquet"
MAJORS = {"ADA","AVAX","BNB","BTC","CRV","DOGE","ETH","HYPE","LINK","SOL"}


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc":1,"close":1,"_id":0}).sort("timestamp_utc",1))
        if len(rows) > 30:
            df = pd.DataFrame(rows)
            out[c] = (df["timestamp_utc"].to_numpy(), df["close"].to_numpy())
    return out


def trailing_ret(ts, close, t_entry, hours, sign):
    i = np.searchsorted(ts, t_entry)
    if i <= 0 or i >= len(ts):
        return None
    entry_px = close[i]
    j = np.searchsorted(ts, t_entry - hours*3600*1000)
    if j < 0 or j >= len(ts) or close[j] <= 0:
        return None
    return (entry_px - close[j]) / close[j] * sign * 1e4   # bps, signed by trade side


def main():
    sk = list(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j = ds.dataset(JOURNEYS, format="parquet").to_table(
        columns=["wallet","coin","side","entry_ts","max_position_notional","net_realized_pnl"],
        filter=pc.field("wallet").isin(sk)).to_pandas()
    j = j[(j.max_position_notional > 10) & (~j.coin.str.startswith("xyz:"))].dropna(subset=["entry_ts"]).copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["win"] = j.net_realized_pnl > 0
    j["t"] = j.entry_ts.astype("int64")
    vc = j.coin.value_counts(); coins = [c for c in vc.index if vc[c] >= 30]
    cand = load_candles(coins)
    j = j[j.coin.isin(cand)].copy()
    print(f"skill-cohort journeys with candles: {len(j)}\n")

    for h in [1, 6, 24]:
        j[f"tr{h}"] = [trailing_ret(*cand[r.coin], r.t, h, r.sgn) if r.coin in cand else None
                      for r in j.itertuples()]
    j = j.dropna(subset=["tr1","tr6","tr24"])

    def row(label, d):
        return (f"{label:<22} n={len(d):>5} | 1h {d.tr1.mean():+6.0f} | 6h {d.tr6.mean():+6.0f} | "
                f"24h {d.tr24.mean():+6.0f}  (signed bps: + = momentum, - = mean-reversion)")

    print("ENTRY trailing-return context (signed by trade side):")
    print(row("ALL", j))
    print(row("  LONG", j[j.sgn>0])); print(row("  SHORT", j[j.sgn<0]))
    print()
    print("WINNERS vs LOSERS (does entry context predict outcome?):")
    print(row("WIN", j[j.win])); print(row("LOSE", j[~j.win]))
    print(row("  LONG-win", j[(j.sgn>0)&j.win])); print(row("  LONG-lose", j[(j.sgn>0)&~j.win]))
    print(row("  SHORT-win", j[(j.sgn<0)&j.win])); print(row("  SHORT-lose", j[(j.sgn<0)&~j.win]))

    # verdict
    allm = j.tr6.mean(); wm = j[j.win].tr6.mean(); lm = j[~j.win].tr6.mean()
    print()
    style = "MOMENTUM (enter with the move)" if allm > 50 else ("MEAN-REVERSION (enter against the move)" if allm < -50 else "MIXED / neutral entry timing")
    print(f"ENTRY STYLE (6h): {style}  [mean {allm:+.0f}bps]")
    print(f"WIN-LOSE entry gap (6h): {wm-lm:+.0f}bps -- "
          f"{'winners enter on a DIFFERENT setup -> predictive signal' if abs(wm-lm)>40 else 'entry context weakly separates outcome'}")
    print("RE READ: if winners systematically enter on a distinct trailing-return setup, that setup IS the extractable")
    print("signal -- we can generate it from candles without watching the leader. Next: add funding/vol/OI features.")


if __name__ == "__main__":
    main()
