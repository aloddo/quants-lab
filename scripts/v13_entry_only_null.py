#!/usr/bin/env python3
"""V13 entry-only random null benchmark.

For each fold, sample N=200 random pools of size K from the wallet universe
(default: positive sweep wallets) and run entry-only sim on each. Compute
per-fold p95 Sharpe + aggregate-Sharpe p95.

Used for G6 gate on V-H_WF_K3 / V-H_WF_K5 candidates.
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_entry_only_evaluate import build_coin_info, run_entry_only_fold
from v13_walk_forward_folds import build_folds
from v13_pool_sweep_shard import load_marks_from_npz, make_candle_close_fn_npz

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_null] %(message)s")
logger = logging.getLogger("v13_null")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep-results", required=True)
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--marks-npz-dir", required=True)
    ap.add_argument("--K", type=int, default=3)
    ap.add_argument("--n-trials", type=int, default=200)
    ap.add_argument("--universe", choices=["positives", "all_eligible", "top200"], default="top200",
                    help="Sampling universe for random pools")
    ap.add_argument("--exclude-coins-prefix", default="xyz:,flx:")
    ap.add_argument("--cooldown-s", type=int, default=1800)
    ap.add_argument("--latency-s", type=int, default=60)
    ap.add_argument("--starting-cash", type=float, default=10_000.0)
    ap.add_argument("--n-folds", type=int, default=8)
    ap.add_argument("--window-start", default="2025-12-01")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    sweep = pd.read_parquet(args.sweep_results)
    if args.universe == "positives":
        universe = sweep[sweep["copy_score"] > 0]["wallet"].tolist()
    elif args.universe == "top200":
        universe = sweep.sort_values("copy_score", ascending=False).head(200)["wallet"].tolist()
    else:
        universe = sweep["wallet"].tolist()
    logger.info(f"Universe={args.universe}: {len(universe)} wallets")
    if len(universe) < args.K:
        logger.error(f"Universe smaller than K={args.K}")
        sys.exit(1)

    chunks = sorted(glob.glob(args.journeys_glob))
    journeys_all = []
    for c in chunks:
        d = pd.read_parquet(c, filters=[("wallet", "in", universe)])
        if len(d):
            journeys_all.append(d)
    journeys_all = pd.concat(journeys_all, ignore_index=True)
    if args.exclude_coins_prefix:
        prefixes = [p.strip() for p in args.exclude_coins_prefix.split(",") if p.strip()]
        mask = pd.Series(False, index=journeys_all.index)
        for p in prefixes:
            mask = mask | journeys_all["coin"].str.startswith(p)
        journeys_all = journeys_all[~mask]
    logger.info(f"  {len(journeys_all):,} journeys post-filter")

    coins_needed = set(journeys_all["coin"].unique())
    coin_info = build_coin_info(list(coins_needed))
    mark_arrays = load_marks_from_npz(args.marks_npz_dir, coins_needed)
    candle_fn = make_candle_close_fn_npz(mark_arrays)

    window_start = date.fromisoformat(args.window_start)
    folds = build_folds(window_start, n_folds=args.n_folds)
    logger.info(f"Built {len(folds)} folds")

    rng = np.random.RandomState(args.seed)
    universe_array = np.array(universe)

    per_fold_p95 = {}
    per_fold_dist = {}
    per_trial_aggregates = []  # for each trial, store list of fold-sharpes → agg Sharpe

    for fold in folds:
        test_start_ms = int(datetime.combine(fold.test_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        test_end_ms = int(datetime.combine(fold.test_end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        sharpes = []
        per_fold_dist[fold.n] = []
        t0 = time.time()
        for trial in range(args.n_trials):
            seed_trial = args.seed + fold.n * 10_000 + trial
            rng_t = np.random.RandomState(seed_trial)
            sample = rng_t.choice(universe_array, size=args.K, replace=False).tolist()
            wj = journeys_all[journeys_all["wallet"].isin(sample)]
            try:
                fs = run_entry_only_fold(
                    fold_n=fold.n,
                    fold_start_ms=test_start_ms, fold_end_ms=test_end_ms,
                    selected_wallets=sample, journeys=wj,
                    candle_close_at_fn=candle_fn,
                    K_target=args.K, latency_s=args.latency_s, cooldown_s=args.cooldown_s,
                    starting_cash_usd=args.starting_cash, coin_info_by_coin=coin_info,
                    regime_tags={"trend": "UNK", "vol": "UNK"},
                )
                s = fs.summary["sharpe"]
                if np.isfinite(s):
                    sharpes.append(s)
                    per_fold_dist[fold.n].append(s)
            except Exception:
                continue
            if (trial + 1) % 50 == 0:
                logger.info(f"  fold {fold.n} trial {trial+1}/{args.n_trials}: sharpes so far {len(sharpes)}, "
                            f"median={np.median(sharpes) if sharpes else 0:.2f}, "
                            f"p95={np.percentile(sharpes, 95) if sharpes else 0:.2f}, "
                            f"wall={time.time()-t0:.0f}s")
        p95 = float(np.percentile(sharpes, 95)) if len(sharpes) >= 20 else float("-inf")
        per_fold_p95[fold.n] = p95
        logger.info(f"FOLD {fold.n}: N={len(sharpes)}, median={np.median(sharpes):.2f}, "
                    f"p95={p95:.2f}, max={max(sharpes):.2f}")

    out = {
        "K": args.K,
        "n_trials": args.n_trials,
        "universe": args.universe,
        "universe_size": len(universe),
        "exclude_coins_prefix": args.exclude_coins_prefix,
        "cooldown_s": args.cooldown_s,
        "per_fold_p95": per_fold_p95,
        "per_fold_distribution_summary": {
            str(k): {"n": len(v), "median": float(np.median(v)) if v else 0,
                     "p75": float(np.percentile(v, 75)) if v else 0,
                     "p90": float(np.percentile(v, 90)) if v else 0,
                     "p95": float(np.percentile(v, 95)) if v else 0,
                     "p99": float(np.percentile(v, 99)) if v else 0,
                     "max": float(max(v)) if v else 0}
            for k, v in per_fold_dist.items()
        },
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(out, indent=2, default=str))
    logger.info(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
