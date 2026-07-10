#!/usr/bin/env python3
"""V13 walk-forward pool evaluator.

Per fold:
  1. Compute per-wallet entry-only Sharpe in TRAIN+VAL window
  2. Select top-K wallets by train+val Sharpe (NO test data used)
  3. Run pool sim on TEST window with selected pool
  4. Aggregate test-fold results → final WF Sharpe + gates

This is the cleanest possible OOS check: pool composition for each test fold
is decided strictly from prior data.

Reuses v13_entry_only_evaluate.run_entry_only_fold for the simulator core.
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
    build_coin_info, run_entry_only_fold,
)
from v13_walk_forward_folds import build_folds, tag_fold_regime
from v13_pass_fail_gates import FoldResult, evaluate_gates
from v13_pool_sweep_shard import load_marks_from_npz, make_candle_close_fn_npz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_wf] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("v13_wf")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep-results", required=True)
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--marks-npz-dir", required=True)
    ap.add_argument("--top-n-universe", type=int, default=30,
                    help="Top-N candidates from sweep (by copy_score) to evaluate per fold")
    ap.add_argument("--K-select", type=int, default=5,
                    help="Pool size selected per fold by train+val Sharpe")
    ap.add_argument("--min-train-val-entries", type=int, default=3,
                    help="Wallet must have >= this many entries in train+val to be eligible")
    ap.add_argument("--max-train-val-dd", type=float, default=1.0,
                    help="Wallet max_dd in train+val must be <= this (e.g. 0.30 for 30%). "
                         "Filters wallets whose train+val period included a big drawdown.")
    ap.add_argument("--exclude-coins-prefix", default="xyz:,flx:")
    ap.add_argument("--cooldown-s", type=int, default=1800)
    ap.add_argument("--latency-s", type=int, default=60)
    ap.add_argument("--starting-cash", type=float, default=10_000.0)
    ap.add_argument("--n-folds", type=int, default=8)
    ap.add_argument("--window-start", default="2025-12-01")
    ap.add_argument("--source-proportional-sizing", action="store_true",
                    help="Use source's max_position_pct_equity (capped at 1/K) for sizing")
    ap.add_argument("--max-concurrent-per-wallet", type=int, default=0,
                    help="Cap concurrent open positions per wallet (0 = unlimited)")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    sweep = pd.read_parquet(args.sweep_results)
    sweep_pos = sweep[sweep["copy_score"] > 0].sort_values("copy_score", ascending=False).head(args.top_n_universe)
    universe = sweep_pos["wallet"].tolist()
    logger.info(f"Universe: top {len(universe)} wallets from sweep by copy_score")

    # Load journeys for universe
    chunks = sorted(glob.glob(args.journeys_glob))
    journeys_all = []
    for c in chunks:
        d = pd.read_parquet(c, filters=[("wallet", "in", universe)])
        if len(d):
            journeys_all.append(d)
    journeys_all = pd.concat(journeys_all, ignore_index=True)

    # Exclude prefix coins
    if args.exclude_coins_prefix:
        prefixes = [p.strip() for p in args.exclude_coins_prefix.split(",") if p.strip()]
        mask = pd.Series(False, index=journeys_all.index)
        for p in prefixes:
            mask = mask | journeys_all["coin"].str.startswith(p)
        journeys_all = journeys_all[~mask]
    logger.info(f"  {len(journeys_all):,} journeys (post-filter)")

    coins_needed = set(journeys_all["coin"].unique())
    coin_info = build_coin_info(list(coins_needed))
    mark_arrays = load_marks_from_npz(args.marks_npz_dir, coins_needed)
    candle_fn = make_candle_close_fn_npz(mark_arrays)

    window_start = date.fromisoformat(args.window_start)
    folds = build_folds(window_start, n_folds=args.n_folds)
    logger.info(f"Built {len(folds)} folds")

    fold_results: list[FoldResult] = []
    selections_per_fold = {}
    fold_summaries_full = []

    for fold in folds:
        train_start_ms = int(datetime.combine(fold.train_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        test_start_ms = int(datetime.combine(fold.test_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        test_end_ms = int(datetime.combine(fold.test_end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        # train+val window = [train_start, test_start)
        train_val_start = train_start_ms
        train_val_end = test_start_ms

        logger.info(f"=== Fold {fold.n}: train+val [{fold.train_start} → {fold.test_start}], test [{fold.test_start} → {fold.test_end}] ===")

        # Score each wallet on train+val window
        wallet_scores = []
        for w in universe:
            wj = journeys_all[journeys_all["wallet"] == w]
            if len(wj) == 0:
                continue
            try:
                fs = run_entry_only_fold(
                    fold_n=-fold.n,  # synthetic
                    fold_start_ms=train_val_start, fold_end_ms=train_val_end,
                    selected_wallets=[w], journeys=wj,
                    candle_close_at_fn=candle_fn,
                    K_target=1, latency_s=args.latency_s, cooldown_s=args.cooldown_s,
                    starting_cash_usd=args.starting_cash, coin_info_by_coin=coin_info,
                    regime_tags={"trend": "UNK", "vol": "UNK"},
                    source_proportional_sizing=args.source_proportional_sizing,
                    max_concurrent_per_wallet=args.max_concurrent_per_wallet,
                )
            except Exception as e:
                logger.warning(f"  trainval fail {w[:14]}: {e}")
                continue
            n_ent = fs.summary["n_entries_executed"]
            if n_ent < args.min_train_val_entries:
                continue
            if fs.summary["max_dd_pct"] > args.max_train_val_dd:
                continue
            wallet_scores.append({
                "wallet": w, "sharpe": fs.summary["sharpe"],
                "net_pnl": fs.summary["net_pnl"], "n_entries": n_ent,
                "max_dd": fs.summary["max_dd_pct"],
            })
        scores_df = pd.DataFrame(wallet_scores).sort_values("sharpe", ascending=False) if wallet_scores else pd.DataFrame()
        if len(scores_df) == 0:
            logger.warning(f"  no eligible wallets for fold {fold.n}")
            fold_results.append(FoldResult(
                fold_n=fold.n, daily_returns=pd.Series(dtype=float),
                summary={"net_pnl": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0,
                         "worst_day_pct": 0.0, "n_legs": 0, "fee_drag": 0.0, "slip_drag": 0.0},
                regime_tags={"trend": "UNK", "vol": "UNK"}, anti_corr_pruned=False,
            ))
            selections_per_fold[fold.n] = {"selected": [], "n_eligible": 0}
            continue
        selected = scores_df.head(args.K_select)["wallet"].tolist()
        logger.info(f"  eligible: {len(scores_df)}, selected top-{args.K_select}:")
        for _, r in scores_df.head(args.K_select).iterrows():
            logger.info(f"    {r['wallet'][:18]} trainval sharpe={r['sharpe']:+.2f} n_ent={r['n_entries']}")
        selections_per_fold[fold.n] = {
            "selected": selected,
            "n_eligible": len(scores_df),
            "scores": scores_df.to_dict(orient="records"),
        }

        # Regime tag
        def _market_data_at(d):
            ts_ms = int(datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            btc_close = candle_fn("BTC", ts_ms)
            return {"btc_price": btc_close, "hl_perp_price": btc_close, "btc_dvol": None}
        try:
            regime_tags = tag_fold_regime(fold, _market_data_at)
        except Exception as e:
            regime_tags = {"trend": "UNK", "vol": "UNK"}

        # Test fold sim
        K = max(1, len(selected))
        try:
            fs = run_entry_only_fold(
                fold_n=fold.n,
                fold_start_ms=test_start_ms, fold_end_ms=test_end_ms,
                selected_wallets=selected, journeys=journeys_all[journeys_all["wallet"].isin(selected)],
                candle_close_at_fn=candle_fn,
                K_target=K, latency_s=args.latency_s, cooldown_s=args.cooldown_s,
                starting_cash_usd=args.starting_cash, coin_info_by_coin=coin_info,
                regime_tags=regime_tags,
                source_proportional_sizing=args.source_proportional_sizing,
                max_concurrent_per_wallet=args.max_concurrent_per_wallet,
            )
        except Exception as e:
            logger.error(f"  fold {fold.n} test sim failed: {e}")
            continue
        s = fs.summary
        logger.info(f"  TEST fold {fold.n}: sharpe={s['sharpe']:+.3f} pnl=${s['net_pnl']:+.0f} dd={s['max_dd_pct']:.1%} ent={s['n_entries_executed']}")
        fold_results.append(FoldResult(
            fold_n=fold.n, daily_returns=fs.daily_returns,
            summary=s, regime_tags=regime_tags, anti_corr_pruned=False,
        ))
        fold_summaries_full.append({
            "fold_n": fold.n, "test_start": str(fold.test_start), "test_end": str(fold.test_end),
            "selected": selected, "summary": s, "regime_tags": regime_tags,
        })

    decision = evaluate_gates(fold_results=fold_results, random_null=None)
    logger.info(f"")
    logger.info(f"=== WF DECISION ===")
    logger.info(f"  agg_sharpe = {decision.summary['agg_sharpe']:.3f}")
    logger.info(f"  folds_positive = {decision.summary['folds_positive']}/{len(fold_results)}")
    logger.info(f"  max_dd_pct = {decision.summary['max_dd_pct']:.1%}")
    logger.info(f"  worst_day_pct = {decision.summary['worst_day_pct']:.1%}")
    logger.info(f"  go = {decision.go}, failures = {decision.failures}")

    out = {
        "universe_size": len(universe),
        "K_select": args.K_select,
        "min_train_val_entries": args.min_train_val_entries,
        "cooldown_s": args.cooldown_s,
        "latency_s": args.latency_s,
        "n_folds": args.n_folds,
        "selections_per_fold": selections_per_fold,
        "fold_summaries": fold_summaries_full,
        "decision": {
            "go": decision.go,
            "failures": list(decision.failures),
            "summary": decision.summary,
        },
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(out, indent=2, default=str))
    logger.info(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
