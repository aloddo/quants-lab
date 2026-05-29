#!/usr/bin/env python3
"""Per-wallet single-wallet entry-only evaluation across all top-N from sweep.

Returns a ranking of wallets by stable per-fold performance, used to identify
candidates for shadow-live before pooling. Reuses v13_entry_only_evaluate.
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
from v13_entry_only_evaluate import (
    build_coin_info, run_entry_only_fold, select_pool,
)
from v13_walk_forward_folds import build_folds, tag_fold_regime
from v13_pool_sweep_shard import load_marks_from_npz, make_candle_close_fn_npz

logging.basicConfig(level=logging.INFO, format="%(asctime)s [single_wallet] %(message)s")
logger = logging.getLogger("single_wallet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep-results", required=True)
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--marks-npz-dir", required=True)
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--exclude-coins-prefix", default="xyz:,flx:")
    ap.add_argument("--cooldown-s", type=int, default=1800)
    ap.add_argument("--latency-s", type=int, default=60)
    ap.add_argument("--starting-cash", type=float, default=10_000.0)
    ap.add_argument("--n-folds", type=int, default=8)
    ap.add_argument("--window-start", default="2025-12-01")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    sweep = pd.read_parquet(args.sweep_results)
    df_pos = sweep[sweep["copy_score"] > 0].sort_values("copy_score", ascending=False).head(args.top_n)
    wallets = df_pos["wallet"].tolist()
    logger.info(f"Top {len(wallets)} wallets to evaluate individually")

    chunks = sorted(glob.glob(args.journeys_glob))
    journeys_all = []
    for c in chunks:
        d = pd.read_parquet(c, filters=[("wallet", "in", wallets)])
        if len(d):
            journeys_all.append(d)
    journeys_all = pd.concat(journeys_all, ignore_index=True)

    # Exclude flx:/xyz:
    prefixes = [p.strip() for p in args.exclude_coins_prefix.split(",") if p.strip()]
    if prefixes:
        mask = pd.Series(False, index=journeys_all.index)
        for p in prefixes:
            mask = mask | journeys_all["coin"].str.startswith(p)
        journeys_all = journeys_all[~mask]
    logger.info(f"  {len(journeys_all):,} journeys (after prefix exclusion)")

    coins_needed = set(journeys_all["coin"].unique().tolist())
    coin_info = build_coin_info(list(coins_needed))
    mark_arrays = load_marks_from_npz(args.marks_npz_dir, coins_needed)
    candle_fn = make_candle_close_fn_npz(mark_arrays)

    window_start = date.fromisoformat(args.window_start)
    folds = build_folds(window_start, n_folds=args.n_folds)
    logger.info(f"Built {len(folds)} folds")

    per_wallet_results = {}
    for w in wallets:
        wj = journeys_all[journeys_all["wallet"] == w]
        if len(wj) == 0:
            per_wallet_results[w] = {"agg_sharpe": 0.0, "n_active_folds": 0, "n_entries": 0,
                                      "max_dd": 0.0, "total_pnl": 0.0, "per_fold": []}
            continue
        per_fold = []
        all_daily_returns = []
        total_entries = 0
        for fold in folds:
            test_start_ms = int(datetime.combine(fold.test_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            test_end_ms = int(datetime.combine(fold.test_end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            try:
                fs = run_entry_only_fold(
                    fold_n=fold.n,
                    fold_start_ms=test_start_ms, fold_end_ms=test_end_ms,
                    selected_wallets=[w], journeys=wj,
                    candle_close_at_fn=candle_fn,
                    K_target=1, latency_s=args.latency_s, cooldown_s=args.cooldown_s,
                    starting_cash_usd=args.starting_cash, coin_info_by_coin=coin_info,
                    regime_tags={"trend": "UNK", "vol": "UNK"},
                )
            except Exception as e:
                logger.warning(f"  {w[:14]} fold {fold.n} fail: {e}")
                continue
            per_fold.append({
                "fold_n": fold.n,
                "sharpe": fs.summary["sharpe"],
                "net_pnl": fs.summary["net_pnl"],
                "max_dd": fs.summary["max_dd_pct"],
                "n_entries": fs.summary["n_entries_executed"],
            })
            total_entries += fs.summary["n_entries_executed"]
            if len(fs.daily_returns):
                all_daily_returns.append(fs.daily_returns)
        active = sum(1 for x in per_fold if x["n_entries"] > 0)
        if all_daily_returns:
            agg = pd.concat(all_daily_returns)
            if agg.std() > 0:
                agg_sharpe = float(agg.mean() / agg.std() * np.sqrt(365))
            else:
                agg_sharpe = 0.0
            eq = (1 + agg).cumprod()
            peak = eq.cummax()
            dd = (eq - peak) / peak
            max_dd_overall = float(abs(dd.min())) if len(dd) > 0 else 0.0
        else:
            agg_sharpe = 0.0
            max_dd_overall = 0.0
        total_pnl = sum(x["net_pnl"] for x in per_fold)
        per_wallet_results[w] = {
            "copy_score": float(df_pos[df_pos["wallet"] == w]["copy_score"].iloc[0]),
            "n_src_total": int(df_pos[df_pos["wallet"] == w]["n_src"].iloc[0]),
            "n_copy_j": int(df_pos[df_pos["wallet"] == w]["n_copy_j"].iloc[0]),
            "agg_sharpe": agg_sharpe,
            "n_active_folds": active,
            "n_entries_total": total_entries,
            "max_dd_overall": max_dd_overall,
            "total_pnl_usd": total_pnl,
            "per_fold": per_fold,
        }
        logger.info(f"  {w[:18]}: sh={agg_sharpe:+.2f} active={active}/{args.n_folds} ent={total_entries} pnl=${total_pnl:.0f} dd={max_dd_overall:.1%}")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(per_wallet_results, indent=2, default=str))
    logger.info(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
