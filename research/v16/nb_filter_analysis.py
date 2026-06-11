#!/usr/bin/env python
"""nb_filter_analysis.py -- H-NB-FILTER card, Steps 3 + 5 (agent G, 2026-06-11).

Step 3 (--buckets): join each replay trade's entry_ts to the asof NB percentile
(last completed-hour pct STRICTLY before entry, identical asof + bucket rule as the
engine: imports engine_replay._nb_bucket). Tables per fold + pooled:
n, mean net bps, median, total, win% for ALIGNED / NEUTRAL / OPPOSED / no_coverage.

Step 5 (--verdict): mechanical kill-rule application vs baseline result parquets.

Usage:
  python research/v16/nb_filter_analysis.py --buckets /tmp/agentG_trades_base.parquet
  python research/v16/nb_filter_analysis.py --verdict
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

REPO = "/Users/hermes/quants-lab"
sys.path.insert(0, f"{REPO}/research/v16")

# Force-load the engine's NB machinery so bucket/asof semantics are THE SAME CODE.
os.environ.setdefault("V16_REPLAY_NB_FILTER", "block_opposed")
import engine_replay as ER  # noqa: E402

BUCKETS = ["ALIGNED", "NEUTRAL", "OPPOSED", "no_coverage"]


def bucket_trades(trades_pq: str) -> pd.DataFrame:
    ER._load_nb()
    t = pd.read_parquet(trades_pq)
    t["bucket"] = [
        ER._nb_bucket(f, c, int(ts), d > 0)[0]
        for f, c, ts, d in zip(t["fold"], t["coin"], t["entry_ts_ms"], t["dir"])
    ]
    t["pct"] = [
        ER._nb_bucket(f, c, int(ts), d > 0)[1]
        for f, c, ts, d in zip(t["fold"], t["coin"], t["entry_ts_ms"], t["dir"])
    ]
    return t


def table(t: pd.DataFrame, label: str) -> str:
    L = [f"### {label}",
         "| bucket | n | mean net bps | median | total bps | win% |",
         "|--------|---|--------------|--------|-----------|------|"]
    for b in BUCKETS:
        s = t[t["bucket"] == b]["net_bps"]
        if len(s) == 0:
            L.append(f"| {b} | 0 | -- | -- | -- | -- |")
            continue
        L.append(f"| {b} | {len(s)} | {s.mean():+.1f} | {s.median():+.1f} |"
                 f" {s.sum():+.0f} | {100 * (s > 0).mean():.0f} |")
    s = t["net_bps"]
    L.append(f"| ALL | {len(s)} | {s.mean():+.1f} | {s.median():+.1f} |"
             f" {s.sum():+.0f} | {100 * (s > 0).mean():.0f} |")
    return "\n".join(L)


def buckets_main(trades_pq: str):
    t = bucket_trades(trades_pq)
    out = []
    for fold in sorted(t["fold"].unique()):
        out.append(table(t[t["fold"] == fold], f"{fold} (n={int((t['fold'] == fold).sum())})"))
    out.append(table(t, "pooled"))
    # coverage boundary disclosure
    for fold in sorted(t["fold"].unique()):
        sub = t[(t["fold"] == fold) & (t["bucket"] == "no_coverage")]
        if len(sub):
            lo = pd.to_datetime(int(sub["entry_ts_ms"].min()), unit="ms")
            hi = pd.to_datetime(int(sub["entry_ts_ms"].max()), unit="ms")
            out.append(f"no_coverage {fold}: {len(sub)} trades, entries {lo} .. {hi}")
    print("\n\n".join(out))
    t.to_parquet("/tmp/agentG_trades_bucketed.parquet", index=False)
    print("\nwrote /tmp/agentG_trades_bucketed.parquet")


def res_row(pq: str, fold: str) -> dict:
    df = pd.read_parquet(pq)
    r = df[df["fold"] == fold].iloc[0]
    return dict(entries=int(r["entries"]), pnl=float(r["book_pnl"]),
                mean=float(r["trade_mean_bps"]), med=float(r["trade_med_bps"]),
                dd=float(r["max_dd"]), fund=float(r["funding_paid"]),
                stopped=bool(r["stopped"]),
                rejects=r["rejects"], exits=r["exits"],
                nb=(r["nb_counts"] if "nb_counts" in df.columns else None))


def verdict_main():
    base_pq = "/tmp/agentG_engine_replay_committed.parquet"
    runs = {
        "baseline": base_pq,
        "block_opposed": "/tmp/agentG_replay_nb_block_opposed.parquet",
        "block_opposed_neutral": "/tmp/agentG_replay_nb_block_opp_neutral.parquet",
    }
    rows = []
    for name, pq in runs.items():
        if not os.path.exists(pq):
            print(f"missing {pq} -- run the sims first")
            return
        for fold in ("fold1", "fold2"):
            r = res_row(pq, fold)
            r.update(run=name, fold=fold)
            rows.append(r)
    df = pd.DataFrame(rows)
    # pooled = sum PnL, entry-weighted mean bps, sum entries; DD has no pooled meaning
    # across folds (separate equity paths) -> report per fold + sum-of-fold DD for ref.
    print("| run | fold | entries | book_pnl $ | mean bps | med bps | maxDD $ | funding $ | stopped |")
    print("|-----|------|---------|------------|----------|---------|---------|-----------|---------|")
    for _, r in df.iterrows():
        print(f"| {r['run']} | {r['fold']} | {r['entries']} | {r['pnl']:+.2f} | {r['mean']:+.2f} |"
              f" {r['med']:+.2f} | {r['dd']:.2f} | {r['fund']:+.2f} | {r['stopped']} |")
    print()
    base = {f: res_row(base_pq, f) for f in ("fold1", "fold2")}
    bm = {f: base[f] for f in base}
    pooled_base_mean = (bm['fold1']['mean'] * bm['fold1']['entries'] + bm['fold2']['mean'] * bm['fold2']['entries']) / (bm['fold1']['entries'] + bm['fold2']['entries'])
    for name in ("block_opposed", "block_opposed_neutral"):
        print(f"-- {name} vs baseline --")
        tot_pnl_d = 0.0
        for fold in ("fold1", "fold2"):
            b, v = base[fold], res_row(runs[name], fold)
            d_mean = v["mean"] - b["mean"]
            d_dd = (v["dd"] - b["dd"]) / b["dd"] * 100 if b["dd"] else float("nan")
            d_pnl = v["pnl"] - b["pnl"]
            tot_pnl_d += d_pnl
            print(f"  {fold}: d_mean {d_mean:+.2f} bps/tr | d_maxDD {d_dd:+.1f}% | d_PnL ${d_pnl:+.2f}"
                  f" | entries {b['entries']} -> {v['entries']}"
                  f" | nb_rejects {dict(v['rejects']).get('nb_filter', 0)} | nb_counts {v['nb']}")
        v1, v2 = res_row(runs[name], "fold1"), res_row(runs[name], "fold2")
        pooled_mean = (v1['mean'] * v1['entries'] + v2['mean'] * v2['entries']) / max(v1['entries'] + v2['entries'], 1)
        print(f"  pooled: mean {pooled_base_mean:+.2f} -> {pooled_mean:+.2f} (d {pooled_mean - pooled_base_mean:+.2f} bps/tr)"
              f" | total PnL d ${tot_pnl_d:+.2f}")
        # mechanical kill rule (codex, binding): PASS iff mean improves >= +5 bps/tr
        # OR (maxDD improves >= 10% AND total PnL >= baseline) -- judged on POOLED mean
        # / both-fold DD + PnL.
        mean_pass = (pooled_mean - pooled_base_mean) >= 5.0
        dd_pass = all((res_row(runs[name], f)["dd"] <= 0.90 * base[f]["dd"]) for f in ("fold1", "fold2"))
        pnl_ok = tot_pnl_d >= 0.0
        print(f"  kill-rule: mean+5bps={mean_pass} | dd-10%(both folds)={dd_pass} & pnl>=base={pnl_ok}"
              f" -> {'PASS' if (mean_pass or (dd_pass and pnl_ok)) else 'KILL'}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--buckets", metavar="TRADES_PQ")
    ap.add_argument("--verdict", action="store_true")
    a = ap.parse_args()
    if a.buckets:
        buckets_main(a.buckets)
    if a.verdict:
        verdict_main()
