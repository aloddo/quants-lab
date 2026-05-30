#!/usr/bin/env python3
"""V13 full-pool Module 04 ranker sweep with ProcessPoolExecutor.

For each eligible wallet (post proxy STAGE 3), run simulate_wallet_copy + compute_copy_score
in parallel workers. Output: ranked parquet for anti-corr greedy + Module 10 gates.

Usage:
    python scripts/v13_full_pool_sweep.py \
        --journeys-glob 'app/data/v13/journey_chunks/chunk_*.parquet' \
        --output app/data/v13/copy_scores_full_pool.parquet \
        --n-workers 6 \
        --max-wallets 5000 \
        --K-target 25

Pre-fetch optimization: ALL candle marks bulk-loaded ONCE shared across workers via
read-only dict (passed at fork time). Avoids per-poll Mongo find.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import numpy as np
import pymongo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_copy_ranker_v2 import simulate_wallet_copy, compute_copy_score, CopyParams

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_sweep] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v13_sweep")


# Module-global state — set by main() at startup. Threads share via Python GIL,
# no pickle cost. CPU-bound parts inside simulate_wallet_copy are released to GIL
# during numpy/pandas operations.
_MARK_CACHE: dict = {}
_COIN_VOLUMES: dict = {}
_DF_ALL: pd.DataFrame = None


def worker_init(mark_cache: dict, coin_volumes: dict, df_all: pd.DataFrame):
    """Threaded mode: just set the module-level vars; no per-thread init needed."""
    global _MARK_CACHE, _COIN_VOLUMES, _DF_ALL
    _MARK_CACHE = mark_cache
    _COIN_VOLUMES = coin_volumes
    _DF_ALL = df_all


def _candle_close(coin: str, ts_ms: int):
    minute = (ts_ms // 60_000) * 60_000
    return _MARK_CACHE.get((coin, minute))


def _funding_rate(coin: str, ts_ms: int):
    return 0.0


def process_wallet(wallet: str, params_dict: dict) -> dict:
    """Worker function: process one wallet, return summary dict."""
    try:
        wallet_j = _DF_ALL[_DF_ALL["wallet"] == wallet].copy()
        if len(wallet_j) < 5:
            return {"wallet": wallet, "n_src": len(wallet_j), "n_copy_j": 0,
                    "copy_score": 0.0, "reason": "<5_journeys", "fees": 0.0, "n_legs": 0}
        params = CopyParams(**params_dict)
        t0 = time.time()
        result = simulate_wallet_copy(
            wallet=wallet, journeys_for_wallet=wallet_j,
            candle_close_fn=_candle_close,
            hourly_funding_rate_fn=_funding_rate,
            coin_volume_lookup=_COIN_VOLUMES, params=params,
        )
        score, reason = compute_copy_score(
            returns=result["returns"], active_days=30, global_pool_median=0.0,
        )
        return {
            "wallet": wallet, "n_src": len(wallet_j),
            "n_copy_j": result["n_copy_journeys"],
            "copy_score": score, "reason": reason or "PASS",
            "fees": round(result["total_fees_usd"], 2),
            "n_legs": len(result["legs"]),
            "wall_s": round(time.time() - t0, 1),
        }
    except Exception as e:
        return {"wallet": wallet, "n_src": -1, "n_copy_j": 0, "copy_score": 0.0,
                "reason": f"ERROR:{type(e).__name__}", "fees": 0.0, "n_legs": 0}


def apply_proxy_stage3(df: pd.DataFrame) -> list[str]:
    """Proxy STAGE 3 from journey aggregates."""
    df["entry_date"] = pd.to_datetime(df["entry_ts"], unit="ms", utc=True).dt.date
    agg = df.groupby("wallet").agg(
        n_journeys=("journey_id", "count"),
        active_days=("entry_date", "nunique"),
        sum_net_pnl=("net_realized_pnl_usd", "sum"),
        median_duration_hours=("duration_hours", "median"),
        max_pos_pct=("max_position_pct_equity", "max"),
    ).reset_index()
    eligible = agg[
        (agg["active_days"] >= 15) &
        (agg["n_journeys"] >= 30) &
        (agg["sum_net_pnl"] > 0) &
        (agg["median_duration_hours"] > 5/60) &
        (agg["max_pos_pct"].notna()) &
        (agg["max_pos_pct"] < 100)
    ]
    return eligible["wallet"].tolist()


def bulk_load_marks(coins: list[str], start_ms: int, end_ms: int) -> dict:
    """Bulk-load all 1m candles for the coin/time range."""
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["quants_lab"]
    mark_cache = {}
    t0 = time.time()
    for coin in coins:
        for doc in db.hyperliquid_candles.find(
            {"coin": coin, "interval": "1m",
             "timestamp_utc": {"$gte": start_ms, "$lte": end_ms + 60_000}},
            {"timestamp_utc": 1, "close": 1, "_id": 0},
        ):
            mark_cache[(coin, doc["timestamp_utc"])] = float(doc["close"])
    logger.info(f"bulk-loaded {len(mark_cache):,} marks for {len(coins)} coins in {time.time()-t0:.1f}s")
    return mark_cache


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--output", required=True)
    ap.add_argument("--n-workers", type=int, default=6)
    ap.add_argument("--max-wallets", type=int, default=5000)
    ap.add_argument("--K-target", type=int, default=25)
    ap.add_argument("--poll-cadence-s", type=int, default=300)
    ap.add_argument("--latency-s", type=int, default=60)
    args = ap.parse_args()

    import glob
    chunks = sorted(glob.glob(args.journeys_glob))
    logger.info(f"Loading {len(chunks)} chunks...")
    df_all = pd.concat([pd.read_parquet(c) for c in chunks], ignore_index=True)
    logger.info(f"  {len(df_all):,} journeys, {df_all['wallet'].nunique():,} wallets")

    logger.info("Applying proxy STAGE 3 filter...")
    eligible = apply_proxy_stage3(df_all)
    logger.info(f"  STAGE 3 eligible: {len(eligible):,} wallets")

    if len(eligible) > args.max_wallets:
        logger.info(f"  capping to --max-wallets={args.max_wallets}")
        eligible = eligible[:args.max_wallets]

    # Bulk-load all needed candle marks
    coins_needed = sorted(set(df_all[df_all["wallet"].isin(eligible)]["coin"].unique().tolist()))
    window_start = int(df_all["entry_ts"].min())
    window_end = int(df_all["exit_ts"].max())
    mark_cache = bulk_load_marks(coins_needed, window_start, window_end)

    coin_volumes = df_all.groupby("coin")["max_position_notional_usd"].sum().to_dict()
    params_dict = dict(
        K_target=args.K_target,
        poll_cadence_s=args.poll_cadence_s,
        latency_s=args.latency_s,
        anti_corr_threshold=0.6,
    )

    # codex/fix: ThreadPoolExecutor (not Process) avoids GB-scale pickle for shared state.
    # Mark_cache + df_all + coin_volumes are read-only → no GIL contention on dict access.
    # Module 04 sim spends most time in numpy/pandas ops which release GIL.
    worker_init(mark_cache, coin_volumes, df_all)

    logger.info(f"Running Module 04 ranker on {len(eligible)} wallets with {args.n_workers} threads...")
    results = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
        futures = {ex.submit(process_wallet, w, params_dict): w for w in eligible}
        for i, f in enumerate(as_completed(futures), 1):
            res = f.result()
            results.append(res)
            if i % 50 == 0:
                elapsed = time.time() - t0
                rate = i / elapsed
                eta = (len(eligible) - i) / rate
                positive = sum(1 for r in results if r["copy_score"] > 0)
                logger.info(f"  [{i}/{len(eligible)}] rate={rate:.1f}/s eta={eta/60:.1f}min positive={positive}")

    res_df = pd.DataFrame(results).sort_values("copy_score", ascending=False)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    res_df.to_parquet(out_path, index=False)
    logger.info(f"\nWrote {out_path}: {len(res_df):,} rows")

    passers = res_df[res_df["reason"] == "PASS"]
    positives = res_df[res_df["copy_score"] > 0]
    logger.info(f"\n=== Summary ===")
    logger.info(f"  total processed: {len(res_df)}")
    logger.info(f"  passed gates: {len(passers)}")
    logger.info(f"  positive scores: {len(positives)}")
    logger.info(f"  median copy_score (all): {res_df['copy_score'].median():.5f}")
    logger.info(f"  max copy_score: {res_df['copy_score'].max():.5f}")
    logger.info(f"  total wall: {(time.time()-t0)/60:.1f}min")
    if len(positives) > 0:
        logger.info(f"\n=== Top 25 ===")
        for _, r in positives.head(25).iterrows():
            logger.info(f"  {r['wallet'][:14]}  score={r['copy_score']:+.5f}  n_src={int(r['n_src']):4d}  n_copy={int(r['n_copy_j']):4d}  reason={r['reason']}")


if __name__ == "__main__":
    main()
