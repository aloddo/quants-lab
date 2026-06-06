#!/usr/bin/env python3
"""V15 two-regime verdict -- the deciding go/no-go after a clean leak-free re-run.

Regime 1 (in-sample, Dec-May): per-entity mean copy-return under OUR costs from the CLEAN M7 TEST
(holdout) summary (roe_engine).
Regime 2 (genuinely future + a different regime, the June crash): each candidate's REALIZED trading PnL
(flow-neutral: closedPnl net of fees, which excludes deposits/withdrawals) from the live HL API over a
post-pipeline window, divided by current equity.

The point (per codex + Alberto): does Regime-1 selection PREDICT Regime-2 performance? If Spearman ~ 0,
the selection does not generalize across regimes -> no deployable edge at our horizon. This is the honest
verdict the clean re-run feeds into.

Usage:
  python research/v15/v15_two_regime_test.py \
     --m07-test app/data/v15/m07_test_final/m07_summary.parquet \
     --m04-entities app/data/v15/m04_entities.parquet \
     --since 2026-05-24 [--min-fills 5] [--min-equity 500]

NOTE: --m04-entities accepts the global m04_entities.parquet OR (post fold-pure run) any single
m04_entities_f*.parquet -- it is used ONLY for entity_id -> primary_wallet mapping (regime-agnostic).
Reads the live HL API (read-only). Does NOT trade.
"""
from __future__ import annotations
import argparse
import time
import concurrent.futures as cf

import pandas as pd
import requests
import scipy.stats as ss

HL_URL = "https://api.hyperliquid.xyz/info"


def regime2_realized(wallet: str, since_ms: int, now_ms: int) -> dict:
    """Flow-neutral June realized PnL: sum(closedPnl) - sum(fee) over (since, now], + current equity +
    open uPnL + fill count. Paginated userFillsByTime, dedup on (time,oid,tid)."""
    try:
        fills, start, seen = [], since_ms, set()
        for _ in range(8):
            r = requests.post(HL_URL, json={"type": "userFillsByTime", "user": wallet,
                                            "startTime": start, "endTime": now_ms}, timeout=12).json()
            if not isinstance(r, list) or not r:
                break
            fills += r
            if len(r) < 2000:
                break
            start = max(f["time"] for f in r) + 1
        cp = fee = 0.0
        n = 0
        for f in fills:
            k = (f.get("time"), f.get("oid"), f.get("tid"))
            if k in seen:
                continue
            seen.add(k)
            cp += float(f.get("closedPnl", 0) or 0)
            fee += float(f.get("fee", 0) or 0)
            n += 1
        ch = requests.post(HL_URL, json={"type": "clearinghouseState", "user": wallet}, timeout=10).json()
        av = float(ch.get("marginSummary", {}).get("accountValue", 0) or 0)
        uupnl = sum(float(p["position"].get("unrealizedPnl", 0) or 0)
                    for p in ch.get("assetPositions", []) if abs(float(p["position"]["szi"])) > 0)
        return {"wallet": wallet, "r2_realized_net": cp - fee, "eq": av, "open_uPnL": uupnl, "r2_fills": n}
    except Exception as e:  # noqa: BLE001
        return {"wallet": wallet, "r2_realized_net": None, "eq": None, "open_uPnL": None,
                "r2_fills": 0, "err": str(e)[:40]}


def run(m07_test: str, m04_entities: str, since: str, min_fills: int, min_equity: float) -> dict:
    tst = pd.read_parquet(m07_test)[["entity_id", "fold_id", "roe_engine",
                                     "round_trip_win_rate", "n_round_trips", "max_dd"]]
    g = tst.groupby("entity_id").agg(
        nf=("fold_id", "count"), r1_test=("roe_engine", "mean"),
        wr=("round_trip_win_rate", "mean"), rt=("n_round_trips", "sum"), dd=("max_dd", "max")).reset_index()
    em = pd.read_parquet(m04_entities)[["entity_id", "primary_wallet"]].drop_duplicates("entity_id")
    g = g.merge(em, on="entity_id", how="left").dropna(subset=["primary_wallet"])

    since_ms = int(pd.Timestamp(since, tz="UTC").timestamp() * 1000)
    now_ms = int(time.time() * 1000)
    rows = {}
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        for r in ex.map(lambda w: regime2_realized(w, since_ms, now_ms), g["primary_wallet"].tolist()):
            rows[r["wallet"]] = r
    for col in ("r2_realized_net", "eq", "open_uPnL", "r2_fills"):
        g[col] = g["primary_wallet"].map(lambda w: rows[w][col])
    v = g.dropna(subset=["r1_test", "r2_realized_net", "eq"])
    v = v[(v["eq"] > min_equity) & (v["r2_fills"] >= min_fills)].copy()
    v["r2_clean_pct"] = 100.0 * v["r2_realized_net"] / v["eq"]

    out = {"n_candidates": int(len(g)), "n_regime2_valid": int(len(v))}
    if len(v) >= 5:
        sp, p = ss.spearmanr(v["r1_test"], v["r2_clean_pct"])
        spw, pw = ss.spearmanr(v["wr"], v["r2_clean_pct"])
        spd, pdd = ss.spearmanr(-v["dd"], v["r2_clean_pct"])
        out.update({
            "spearman_r1_vs_r2": round(float(sp), 3), "p_r1_vs_r2": round(float(p), 3),
            "spearman_winrate_vs_r2": round(float(spw), 3), "p_winrate": round(float(pw), 3),
            "spearman_negDD_vs_r2": round(float(spd), 3), "p_negDD": round(float(pdd), 3),
            "n_robust_both_pos": int(((v["r1_test"] > 0) & (v["r2_clean_pct"] > 0)).sum()),
        })
        out["VERDICT"] = ("NO cross-regime predictivity (Spearman n.s.) -> no deployable edge at horizon"
                          if p > 0.05 or sp <= 0.1 else
                          "Regime-1 selection DOES predict Regime-2 -> investigate the robust set further")
    else:
        out["VERDICT"] = "INSUFFICIENT regime-2-active candidates (<5) -- cannot conclude"
    return out, v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m07-test", default="app/data/v15/m07_test_final/m07_summary.parquet")
    ap.add_argument("--m04-entities", default="app/data/v15/m04_entities.parquet")
    ap.add_argument("--since", default="2026-05-24", help="Regime-2 start (post-pipeline future window)")
    ap.add_argument("--min-fills", type=int, default=5)
    ap.add_argument("--min-equity", type=float, default=500.0)
    ap.add_argument("--out", default=None, help="optional parquet for the per-candidate table")
    args = ap.parse_args()
    summary, table = run(args.m07_test, args.m04_entities, args.since, args.min_fills, args.min_equity)
    print("=== V15 TWO-REGIME VERDICT ===")
    for k, val in summary.items():
        print(f"  {k}: {val}")
    if args.out:
        table.to_parquet(args.out, index=False)
        print(f"  wrote per-candidate table -> {args.out}")


if __name__ == "__main__":
    main()
