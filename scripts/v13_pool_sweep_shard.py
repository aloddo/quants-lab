#!/usr/bin/env python3
"""V13 single-shard pool sweep — runs Module 04 ranker on a SUBSET of eligible wallets.

Designed to be spawned N times for true parallelism (sidesteps Python GIL).

Each shard:
  1. Loads its share of journey chunks (or all chunks if memory permits)
  2. Filters to its assigned wallets
  3. Bulk-loads marks only for coins those wallets touch
  4. Runs Module 04 simulate_wallet_copy + compute_copy_score
  5. Writes shard parquet

Orchestrator (see v13_pool_sweep_orchestrator.sh) spawns N shards and merges.

Usage:
    python scripts/v13_pool_sweep_shard.py \
        --journeys-glob 'app/data/v13/journey_chunks/chunk_*.parquet' \
        --wallets-file /tmp/v13_shard_0.txt \
        --output /tmp/v13_shard_0.parquet \
        --K-target 25
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import numpy as np
import pymongo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_copy_ranker_v2 import simulate_wallet_copy, compute_copy_score, CopyParams

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_shard] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v13_shard")


def bulk_load_marks(coins: list[str], start_ms: int, end_ms: int) -> dict:
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["quants_lab"]
    mark_cache: dict = {}
    t0 = time.time()
    for coin in coins:
        for doc in db.hyperliquid_candles.find(
            {"coin": coin, "interval": "1m",
             "timestamp_utc": {"$gte": start_ms, "$lte": end_ms + 60_000}},
            {"timestamp_utc": 1, "close": 1, "_id": 0},
        ):
            mark_cache[(coin, doc["timestamp_utc"])] = float(doc["close"])
    logger.info(f"bulk-loaded {len(mark_cache):,} marks for {len(coins)} coins in {time.time()-t0:.1f}s (mongo)")
    return mark_cache


def load_marks_from_parquet(parquet_path: str, coins: set[str]) -> dict:
    """Load marks from preloaded parquet. Filters to only the coins this shard needs."""
    t0 = time.time()
    df = pd.read_parquet(parquet_path, columns=["coin", "minute_ms", "close"])
    df = df[df["coin"].isin(coins)]
    mark_cache = {(row.coin, row.minute_ms): row.close for row in df.itertuples(index=False)}
    logger.info(f"bulk-loaded {len(mark_cache):,} marks for {len(coins)} coins in {time.time()-t0:.1f}s (parquet)")
    return mark_cache


def load_marks_from_npz(npz_dir: str, coins: set[str]) -> dict:
    """Memory-efficient mark loader using per-coin mmap'd numpy arrays.

    Returns: {coin: (timestamps_array_int64, closes_array_float32)} where both arrays
    are mmap'd from disk via np.load(mmap_mode='r'). OS page cache shares pages across
    processes — no per-shard memory duplication.
    """
    import bisect
    t0 = time.time()
    npz_path = Path(npz_dir)
    mark_arrays = {}
    total_marks = 0
    for coin in coins:
        # Sanitize coin name (mirrors v13_marks_to_npz.py)
        safe = str(coin).replace("/", "__").replace(":", "__")
        ts_file = npz_path / f"{safe}__ts.npy"
        close_file = npz_path / f"{safe}__close.npy"
        if not ts_file.exists() or not close_file.exists():
            continue
        ts_arr = np.load(ts_file, mmap_mode="r")
        close_arr = np.load(close_file, mmap_mode="r")
        mark_arrays[coin] = (ts_arr, close_arr)
        total_marks += len(ts_arr)
    logger.info(f"mmap-loaded {total_marks:,} marks for {len(mark_arrays)} coins in {time.time()-t0:.1f}s (npz)")
    return mark_arrays


def make_candle_close_fn_npz(mark_arrays: dict):
    """Build candle_close_fn that does binary search on mmap'd arrays."""
    import bisect

    def _candle_close(coin: str, ts_ms: int):
        arrays = mark_arrays.get(coin)
        if arrays is None:
            return None
        ts_arr, close_arr = arrays
        # Snap to minute boundary
        minute = (ts_ms // 60_000) * 60_000
        # Binary search for exact minute
        idx = bisect.bisect_left(ts_arr, minute)
        if idx < len(ts_arr) and ts_arr[idx] == minute:
            return float(close_arr[idx])
        return None

    return _candle_close


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--wallets-file", required=True, help="newline-delimited wallet addresses (this shard's slice)")
    ap.add_argument("--output", required=True)
    ap.add_argument("--marks-parquet", default=None,
                    help="Path to preloaded marks parquet. If set, load from parquet instead of Mongo (avoids I/O contention).")
    ap.add_argument("--marks-npz-dir", default=None,
                    help="Path to per-coin npz directory. If set, mmap-load (FASTEST, low mem).")
    ap.add_argument("--K-target", type=int, default=25)
    ap.add_argument("--poll-cadence-s", type=int, default=300)
    ap.add_argument("--latency-s", type=int, default=60)
    ap.add_argument("--checkpoint-every", type=int, default=20)
    args = ap.parse_args()

    import glob
    chunks = sorted(glob.glob(args.journeys_glob))
    wallets = [w.strip() for w in Path(args.wallets_file).read_text().splitlines() if w.strip()]
    logger.info(f"Shard with {len(wallets)} wallets, loading {len(chunks)} chunks...")

    # Load only what we need: filter journeys to assigned wallets during load
    wallet_set = set(wallets)
    dfs = []
    for c in chunks:
        df_chunk = pd.read_parquet(c, columns=["wallet", "coin", "entry_ts", "exit_ts",
                                                "side", "max_position_notional_usd",
                                                "max_position_pct_equity", "duration_hours",
                                                "net_realized_pnl_usd", "journey_id"])
        dfs.append(df_chunk[df_chunk["wallet"].isin(wallet_set)])
    df_all = pd.concat(dfs, ignore_index=True)
    logger.info(f"  filtered to {len(df_all):,} journeys for {df_all['wallet'].nunique()} wallets")

    if len(df_all) == 0:
        logger.warning("No journeys for assigned wallets; writing empty parquet")
        pd.DataFrame(columns=["wallet", "n_src", "n_copy_j", "copy_score", "reason", "fees", "n_legs"]).to_parquet(args.output, index=False)
        return

    # Bulk-load marks only for coins this shard touches
    coins_needed = sorted(df_all["coin"].unique().tolist())
    window_start = int(df_all["entry_ts"].min())
    window_end = int(df_all["exit_ts"].max())
    using_npz = bool(args.marks_npz_dir)
    if using_npz:
        mark_arrays = load_marks_from_npz(args.marks_npz_dir, set(coins_needed))
        _candle_close = make_candle_close_fn_npz(mark_arrays)
        mark_cache = None  # not used
    elif args.marks_parquet:
        mark_cache = load_marks_from_parquet(args.marks_parquet, set(coins_needed))
        def _candle_close(coin: str, ts_ms: int):
            minute = (ts_ms // 60_000) * 60_000
            return mark_cache.get((coin, minute))
    else:
        mark_cache = bulk_load_marks(coins_needed, window_start, window_end)
        def _candle_close(coin: str, ts_ms: int):
            minute = (ts_ms // 60_000) * 60_000
            return mark_cache.get((coin, minute))

    def _funding_rate(coin: str, ts_ms: int):
        return 0.0

    coin_volumes = df_all.groupby("coin")["max_position_notional_usd"].sum().to_dict()

    params = CopyParams(
        K_target=args.K_target,
        poll_cadence_s=args.poll_cadence_s,
        latency_s=args.latency_s,
        anti_corr_threshold=0.6,
    )

    results = []
    t0 = time.time()
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for i, wallet in enumerate(wallets, 1):
        wallet_j = df_all[df_all["wallet"] == wallet]
        if len(wallet_j) < 5:
            results.append({"wallet": wallet, "n_src": len(wallet_j), "n_copy_j": 0,
                            "copy_score": 0.0, "reason": "<5_journeys",
                            "fees": 0.0, "n_legs": 0, "wall_s": 0.0})
            continue
        try:
            t_w = time.time()
            result = simulate_wallet_copy(
                wallet=wallet, journeys_for_wallet=wallet_j,
                candle_close_fn=_candle_close,
                hourly_funding_rate_fn=_funding_rate,
                coin_volume_lookup=coin_volumes, params=params,
            )
            score, reason = compute_copy_score(
                returns=result["returns"], active_days=30, global_pool_median=0.0,
            )
            results.append({
                "wallet": wallet, "n_src": len(wallet_j),
                "n_copy_j": result["n_copy_journeys"],
                "copy_score": score, "reason": reason or "PASS",
                "fees": round(result["total_fees_usd"], 2),
                "n_legs": len(result["legs"]),
                "wall_s": round(time.time() - t_w, 1),
            })
        except Exception as e:
            results.append({"wallet": wallet, "n_src": -1, "n_copy_j": 0, "copy_score": 0.0,
                            "reason": f"ERROR:{type(e).__name__}", "fees": 0.0, "n_legs": 0,
                            "wall_s": 0.0})

        # Checkpoint
        if i % args.checkpoint_every == 0:
            pd.DataFrame(results).to_parquet(out_path, index=False)
            elapsed = time.time() - t0
            rate = i / elapsed
            eta = (len(wallets) - i) / rate
            positive = sum(1 for r in results if r["copy_score"] > 0)
            logger.info(f"  [{i}/{len(wallets)}] rate={rate:.2f}/s eta={eta/60:.1f}min positive={positive}")

    pd.DataFrame(results).to_parquet(out_path, index=False)
    positives = sum(1 for r in results if r["copy_score"] > 0)
    passers = sum(1 for r in results if r["reason"] == "PASS")
    logger.info(f"Shard done: {len(results)} wallets, {passers} passers, {positives} positive, wall={(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    main()
