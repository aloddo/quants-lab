#!/usr/bin/env python3
"""V13 pool evaluator — Module 09 anti-corr + Module 10 6-gates on sweep results.

Pipeline:
  1. Load sharded sweep results (copy_score per wallet)
  2. Filter positive copy_score wallets
  3. Build journeys for top N candidates
  4. For each of 8 walk-forward folds:
       a. Compute per-wallet daily returns within the fold test window
       b. Module 09 anti_corr_greedy_fill → K=25 selected pool
       c. Module 08 portfolio_simulator(pool, fold_test_window) → fold summary
  5. Module 10 6-gates on 8 fold results

Usage:
    python scripts/v13_pool_evaluate.py \
        --sweep-results /tmp/v13_sharded_v3_1591.parquet \
        --journeys-glob 'app/data/v13/journey_chunks/chunk_*.parquet' \
        --marks-npz-dir /tmp/v13_marks_npz/ \
        --K-target 25 \
        --top-n 100 \
        --output /tmp/v13_pool_evaluation.json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_copy_ranker_v2 import CopyParams, _build_source_state_fn
from v13_portfolio_simulator import run_portfolio_simulator, SimParams
from v13_portfolio_ledger import CopyLedger
from v13_execution_realism import CoinInfo, classify_coin_tier


def build_default_coin_info(coins: list[str],
                             coin_volume_lookup: dict[str, float] = None) -> dict[str, CoinInfo]:
    """Build CoinInfo for each coin. Uses real 30d-median 1m volume when provided
    in coin_volume_lookup, else falls back to a permissive default that keeps
    most coins inside the {majors, liquid, mid} tiers (codex m08 r5 fix).

    codex review FIX: default 50k usd → most alts classified 'thin' and excluded
    from Module 08 sim. Bumping default to 200_000 USD keeps mid-cap alts in the
    simulation universe pending real volume lookups.
    """
    out = {}
    for c in coins:
        vol = (coin_volume_lookup or {}).get(c, 200_000.0)
        tier = classify_coin_tier(c, volume_30d_median_1m_usd=vol)
        out[c] = CoinInfo(
            coin=c, tier=tier,
            tick_size=0.01,       # generous
            qty_step=0.0001,      # fine
            min_order_usd=10.0,
        )
    return out
from v13_cold_start import ColdStartState
from v13_walk_forward_folds import build_folds, anti_corr_greedy_fill, tag_fold_regime
from v13_pass_fail_gates import FoldResult, evaluate_gates
from v13_pool_sweep_shard import load_marks_from_npz, make_candle_close_fn_npz

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_eval] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v13_eval")


def _build_pool_source_state_fn(candidates: list[str], journeys_per_wallet: dict[str, pd.DataFrame]):
    """Returns a source_state_fn(wallet, ts_ms) → {coin: {"size","signed_notional"}}.
    Wraps per-wallet _build_source_state_fn from copy_ranker_v2. Each per-wallet fn has
    signature (wallet, ts_ms) — passes wallet through (it's unused inside but matches API).
    """
    per_wallet_fns = {}
    for w in candidates:
        wj = journeys_per_wallet.get(w)
        if wj is None or len(wj) == 0:
            per_wallet_fns[w] = lambda w, t: {}
            continue
        per_wallet_fns[w] = _build_source_state_fn(wj, source_equity_approx=1000.0)

    def source_state_at_poll_fn(wallet, ts_ms):
        fn = per_wallet_fns.get(wallet)
        if fn is None:
            return {}
        return fn(wallet, ts_ms)

    return source_state_at_poll_fn


def per_wallet_daily_returns(wallet: str, wj: pd.DataFrame, fold_start_ms: int, fold_end_ms: int,
                              candle_close_fn, params: CopyParams) -> pd.Series:
    """Run single-wallet sim over fold window → daily returns Series.
    Matches v13_copy_ranker_v2.simulate_wallet_copy invocation pattern."""
    source_fn = _build_source_state_fn(wj, source_equity_approx=1000.0)
    coin_info = build_default_coin_info(list(wj["coin"].unique()))

    sim = run_portfolio_simulator(
        selected_pool=[wallet],
        params=SimParams(
            K_target=1,
            poll_interval_s=params.poll_cadence_s,
            latency_s=params.latency_s,
            anti_corr_threshold=params.anti_corr_threshold,
            cooldown_s=600,
        ),
        window_start_ms=fold_start_ms,
        window_end_ms=fold_end_ms,
        source_state_at_poll_fn=source_fn,
        source_equity_at_fn=lambda w, t: 1000.0,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=candle_close_fn,
        hourly_funding_rate_fn=lambda c, t: 0.0,
        starting_cash_usd=10_000.0,
    )
    return sim.daily_returns


def _run_pool_sim(pool, journeys_per_wallet, fold_start_ms, fold_end_ms, candle_close_fn, params,
                   coin_volume_lookup=None):
    """Internal helper: returns the full SimResult for a multi-wallet pool."""
    source_fn = _build_pool_source_state_fn(pool, journeys_per_wallet)
    all_coins = set()
    for w in pool:
        wj = journeys_per_wallet.get(w)
        if wj is not None:
            all_coins.update(wj["coin"].unique())
    coin_info = build_default_coin_info(list(all_coins), coin_volume_lookup=coin_volume_lookup)
    return run_portfolio_simulator(
        selected_pool=pool,
        params=SimParams(
            K_target=params.K_target,
            poll_interval_s=params.poll_cadence_s,
            latency_s=params.latency_s,
            anti_corr_threshold=params.anti_corr_threshold,
            cooldown_s=600,
        ),
        window_start_ms=fold_start_ms,
        window_end_ms=fold_end_ms,
        source_state_at_poll_fn=source_fn,
        source_equity_at_fn=lambda w, t: 1000.0,
        coin_info_by_coin=coin_info,
        candle_close_at_fn=candle_close_fn,
        hourly_funding_rate_fn=lambda c, t: 0.0,
        starting_cash_usd=10_000.0,
    )


def run_pool_simulation(pool, journeys_per_wallet, fold_start_ms, fold_end_ms,
                         candle_close_fn, params):
    return _run_pool_sim(pool, journeys_per_wallet, fold_start_ms, fold_end_ms,
                          candle_close_fn, params).summary


def run_pool_simulation_returns(pool, journeys_per_wallet, fold_start_ms, fold_end_ms,
                                 candle_close_fn, params):
    return _run_pool_sim(pool, journeys_per_wallet, fold_start_ms, fold_end_ms,
                          candle_close_fn, params).daily_returns


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep-results", required=True, help="merged sharded sweep parquet")
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--marks-npz-dir", required=True)
    ap.add_argument("--K-target", type=int, default=25)
    ap.add_argument("--top-n", type=int, default=100,
                    help="Top N positive wallets to consider for anti-corr fill")
    ap.add_argument("--output", required=True, help="JSON output of evaluation results")
    ap.add_argument("--window-start", default="2025-12-01")
    ap.add_argument("--n-folds", type=int, default=8)
    args = ap.parse_args()

    # Load sweep results
    sweep = pd.read_parquet(args.sweep_results).sort_values("copy_score", ascending=False)
    positives = sweep[(sweep["copy_score"] > 0) & (sweep["reason"] == "PASS")]
    logger.info(f"Sweep: {len(sweep)} wallets, {len(positives)} positive PASS")
    top_candidates = positives.head(args.top_n)["wallet"].tolist()
    logger.info(f"Top {len(top_candidates)} candidates for anti-corr selection")

    # Load all journeys + filter to top candidates
    import glob
    chunks = sorted(glob.glob(args.journeys_glob))
    if not chunks:
        logger.error(f"No journey chunks matched --journeys-glob={args.journeys_glob}")
        sys.exit(1)
    logger.info(f"Loading {len(chunks)} chunks...")
    df_all = pd.concat([pd.read_parquet(c) for c in chunks], ignore_index=True)
    cand_set = set(top_candidates)
    df_cand = df_all[df_all["wallet"].isin(cand_set)].copy()
    logger.info(f"  {len(df_cand):,} journeys for {df_cand['wallet'].nunique()} candidates")
    journeys_per_wallet = {w: g.reset_index(drop=True) for w, g in df_cand.groupby("wallet")}

    # Load marks (mmap)
    coins_needed = set(df_cand["coin"].unique())
    mark_arrays = load_marks_from_npz(args.marks_npz_dir, coins_needed)
    candle_fn = make_candle_close_fn_npz(mark_arrays)

    # Build 8 folds
    window_start = date.fromisoformat(args.window_start)
    folds = build_folds(window_start, n_folds=args.n_folds)
    logger.info(f"Built {len(folds)} folds")
    for f in folds:
        logger.info(f"  fold {f.n}: train {f.train_start}→{f.val_start}, "
                    f"val {f.val_start}→{f.test_start}, test {f.test_start}→{f.test_end}")

    params = CopyParams(K_target=args.K_target, poll_cadence_s=300, latency_s=60,
                        anti_corr_threshold=0.6)
    fold_results: list[FoldResult] = []
    selected_pools_by_fold: dict[int, list[str]] = {}

    for fold in folds:
        logger.info(f"=== Fold {fold.n} ===")
        # Test window in ms.
        # codex review FIX (off-by-one): build_folds() stores test_end as INCLUSIVE date,
        # but simulator windows are [start, end). Without +1 day the final test day is
        # excluded → 14-day folds instead of 15-day. Add 86_400_000 ms to test_end_ms.
        from datetime import timedelta
        test_start_ms = int(datetime.combine(fold.test_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        test_end_ms = int(datetime.combine(fold.test_end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        logger.info(f"  test window {test_start_ms} → {test_end_ms} (incl test_end day)")

        # Compute per-candidate daily returns within fold test window
        t_dr = time.time()
        daily_returns_per_wallet: dict[str, pd.Series] = {}
        for i, w in enumerate(top_candidates, 1):
            wj = journeys_per_wallet.get(w)
            if wj is None:
                continue
            try:
                dr = per_wallet_daily_returns(w, wj, test_start_ms, test_end_ms, candle_fn, params)
                if len(dr) > 0:
                    daily_returns_per_wallet[w] = dr
            except Exception as e:
                logger.warning(f"  daily returns failed for {w[:14]}: {e}")
            if i % 25 == 0:
                logger.info(f"    daily returns: {i}/{len(top_candidates)} in {time.time()-t_dr:.0f}s")
        logger.info(f"  daily returns done: {len(daily_returns_per_wallet)} wallets in {time.time()-t_dr:.0f}s")

        # Module 09 anti-corr greedy fill — pick K=25 from candidates
        candidates_for_fill = [w for w in top_candidates if w in daily_returns_per_wallet]
        candidate_scores = {
            w: float(positives[positives["wallet"] == w]["copy_score"].iloc[0])
            for w in candidates_for_fill
        }

        try:
            wallets_with_scores = {w: candidate_scores[w] for w in candidates_for_fill}
            daily_returns_np = {w: daily_returns_per_wallet[w].to_numpy() for w in candidates_for_fill}
            selected, anti_corr_pruned = anti_corr_greedy_fill(
                wallets_with_scores=wallets_with_scores,
                daily_returns_by_wallet=daily_returns_np,
                threshold=params.anti_corr_threshold,
                K_target=args.K_target,
                multiplier=3,
            )
            logger.info(f"  anti-corr selected: {len(selected)}/{args.K_target} (pruned={anti_corr_pruned})")
        except Exception as e:
            logger.error(f"  anti-corr fill failed: {e}; falling back to top by copy_score")
            selected = candidates_for_fill[: args.K_target]
            anti_corr_pruned = False

        selected_pools_by_fold[fold.n] = selected

        # Module 08 portfolio sim on selected pool — ONE call, take both summary + returns
        # codex review FIX: previously ran sim twice (once for summary once for returns),
        # doubled runtime + could produce inconsistent FoldResults on partial failure.
        t_sim = time.time()
        try:
            sim_result = _run_pool_sim(selected, journeys_per_wallet, test_start_ms, test_end_ms,
                                        candle_fn, params)
            summary = sim_result.summary
            pool_daily_returns = sim_result.daily_returns
            logger.info(f"  pool sim done in {time.time()-t_sim:.0f}s: {summary}")
        except Exception as e:
            logger.error(f"  pool sim failed: {e}")
            summary = {"net_pnl": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0, "worst_day_pct": 0.0,
                       "n_legs": 0, "fee_drag": 0.0, "slip_drag": 0.0}
            pool_daily_returns = pd.Series(dtype=float)

        # codex review FIX: tag real regime via Module 09 tag_fold_regime so G5 can count
        # distinct regime cells across folds. Use HL candle BTC + null DVol as MVP market
        # data (placeholder DVol → "UNKNOWN" vol tier per Module 09 fail-closed).
        def _market_data_at(d):
            ts_ms = int(datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            btc_close = candle_fn("BTC", ts_ms)
            return {
                "btc_price": btc_close,
                "hl_perp_price": btc_close,
                "btc_dvol": None,  # no DVol source in this pipeline → vol tier UNKNOWN
            }
        try:
            regime_tags = tag_fold_regime(fold, _market_data_at)
        except Exception as e:
            logger.warning(f"  regime tagging failed: {e}")
            regime_tags = {"trend": "UNKNOWN", "vol": "UNKNOWN"}

        fold_results.append(FoldResult(
            fold_n=fold.n,
            daily_returns=pool_daily_returns,
            summary=summary,
            regime_tags=regime_tags,
            anti_corr_pruned=anti_corr_pruned,
        ))

    # Module 10 evaluate gates
    decision = evaluate_gates(fold_results=fold_results, random_null=None)
    logger.info(f"")
    logger.info(f"=== Module 10 Decision ===")
    logger.info(f"  go: {decision.go}")
    logger.info(f"  failures: {decision.failures}")
    logger.info(f"  summary: {decision.summary}")

    out = {
        "sweep_results": args.sweep_results,
        "K_target": args.K_target,
        "top_n_candidates": args.top_n,
        "n_folds": args.n_folds,
        "fold_selections": {str(k): v for k, v in selected_pools_by_fold.items()},
        "fold_summaries": [
            {"fold_n": fr.fold_n, "summary": fr.summary,
             "regime_tags": fr.regime_tags, "anti_corr_pruned": fr.anti_corr_pruned,
             "n_wallets_selected": int(len(selected_pools_by_fold.get(fr.fold_n, [])))}
            for fr in fold_results
        ],
        "decision": {
            "go": decision.go,
            "failures": list(decision.failures),
            "summary": decision.summary,
        },
    }
    Path(args.output).write_text(json.dumps(out, indent=2, default=str))
    logger.info(f"Wrote {args.output}")


def tag_basic(fold) -> str:
    """Placeholder regime tag — full implementation in Module 09 tag_fold_regime."""
    return "neutral_mid"


if __name__ == "__main__":
    main()
