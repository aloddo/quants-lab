"""
E3 Optuna Parameter Optimization — Phase 1.4

Optimizes 4 parameters on train window only (first 8 months).
Validation window (last 4 months) is held out for Phase 1.5 one-shot.

Parameters:
  1. funding_streak_min  — signal quality filter
  2. funding_rate_threshold — minimum funding magnitude
  3. sl_pct — stop loss (actual field E3 reads, not parent stop_loss)
  4. time_limit_seconds — max hold duration

Objective: composite score (Sharpe + PF bonus - DD penalty) averaged across pairs.

Usage:
  python scripts/e3_optuna.py                    # 50 trials, 4 pairs
  python scripts/e3_optuna.py --n-trials 100     # more trials
  python scripts/e3_optuna.py --validate          # run one-shot validation with best params
"""
import argparse
import asyncio
import gc
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np
import optuna

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

# Top pairs from governance (DEPLOY, score >= 0.55)
OPTUNA_PAIRS = ["ADA-USDT", "SUI-USDT", "SOL-USDT", "XRP-USDT"]

# Train/validation split — hard-coded dates (research process requirement)
# Full data: 2025-04-12 to 2026-04-12 (365 days)
# Train: first 244 days (8 months) — Optuna tunes here
# Validation: last 121 days (4 months) — one-shot, never touched during tuning
TRAIN_DAYS = 244
VALIDATION_DAYS = 121


def get_parquet_end_time(connector, pairs, resolution="1m"):
    """Find earliest end time across parquet files."""
    from core.data_paths import data_paths
    import pandas as pd
    min_end = float("inf")
    for pair in pairs[:5]:
        f = data_paths.candles_dir / f"{connector}|{pair}|{resolution}.parquet"
        if f.exists():
            df = pd.read_parquet(f, columns=["timestamp"])
            end = df["timestamp"].max()
            if end < min_end:
                min_end = end
    if min_end == float("inf"):
        return int(datetime.now(timezone.utc).timestamp())
    return int(min_end)


async def load_shared_candles_with_derivatives(pairs, connector, engine_name):
    """Load parquet candles and merge derivatives from MongoDB."""
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy
    from app.tasks.backtesting.bulk_backtest_task import BulkBacktestTask

    _cache = BacktestingEngine(load_cached_data=True)
    shared_candles = _cache._bt_engine.backtesting_data_provider.candles_feeds.copy()
    del _cache
    gc.collect()

    engine_meta = get_strategy(engine_name)
    if "derivatives" in engine_meta.required_features:
        _merger = object.__new__(BulkBacktestTask)
        _merger.connector_name = connector
        _merger.engine_name = engine_name

        import motor.motor_asyncio
        mongo_uri = os.environ["MONGO_URI"]
        _merger.mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)

        n = await _merger._merge_derivatives_into_candles(shared_candles, pairs)
        print(f"  Merged derivatives into {n}/{len(pairs)} pairs")

    return shared_candles


async def run_backtest(pair, engine_name, connector, shared_candles,
                       trade_cost=0.000375, config_overrides=None,
                       start_ts=None, end_ts=None):
    """Run a single backtest using pre-loaded shared candles."""
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    meta = get_strategy(engine_name)

    config = build_backtest_config(
        engine_name=engine_name,
        connector=connector,
        pair=pair,
    )

    # Apply overrides (must use correct field names — sl_pct not stop_loss)
    if config_overrides:
        for key, val in config_overrides.items():
            setattr(config, key, val)

    bt_engine = BacktestingEngine(load_cached_data=False)
    bt_engine._bt_engine.backtesting_data_provider.candles_feeds = shared_candles

    try:
        result = await bt_engine.run_backtesting(
            config=config,
            start=start_ts,
            end=end_ts,
            backtesting_resolution=meta.backtesting_resolution,
            trade_cost=trade_cost,
        )

        if not isinstance(result.results.get("close_types"), dict):
            result.results["close_types"] = {}

        r = result.results
        return {
            "pf": r.get("profit_factor", 0) or 0,
            "trades": r.get("total_executors", 0) or 0,
            "sharpe": r.get("sharpe_ratio", 0) or 0,
            "wr": (r.get("accuracy_long", 0) or 0) * 100,
            "pnl": r.get("net_pnl_quote", 0) or 0,
            "max_dd": r.get("max_drawdown_pct", 0) or 0,
        }
    except Exception as e:
        logger.warning(f"Backtest failed for {pair}: {e}")
        return {"pf": 0, "trades": 0, "sharpe": 0, "wr": 0, "pnl": 0, "max_dd": -1, "error": str(e)}
    finally:
        del bt_engine
        gc.collect()


def compute_objective(pair_results: dict) -> float:
    """Composite objective: capped Sharpe + PF bonus - DD penalty.

    Designed to reward robust edge, not peak performance.
    Sharpe capped at 3.0 to prevent low-trade noise dominating.
    Hard reject if any pair has < 30 trades.
    """
    if not pair_results:
        return -10.0

    sharpes = []
    pf_bonuses = []

    for pair, r in pair_results.items():
        if r.get("error"):
            return -10.0

        sharpe = r["sharpe"]
        pf = r["pf"]
        trades = r["trades"]

        # Hard floor: insufficient trades = invalid trial
        if trades < 30:
            return -10.0

        # Cap Sharpe at 3.0 — prevents low-N noise from dominating
        sharpes.append(min(sharpe, 3.0))

        # PF bonus: reward PF > 1.2, extra for > 1.3
        if pf >= 1.3:
            pf_bonuses.append(0.3)
        elif pf >= 1.2:
            pf_bonuses.append(0.15)
        elif pf >= 1.1:
            pf_bonuses.append(0.0)
        else:
            pf_bonuses.append(-0.5)  # penalty for PF < 1.1

    avg_sharpe = np.mean(sharpes)
    avg_pf_bonus = np.mean(pf_bonuses)

    # Trade count identity guard: penalize if average deviates far from baseline
    avg_trades = np.mean([r["trades"] for r in pair_results.values()])
    if avg_trades > 600:
        trade_penalty = -0.3  # too many = different strategy
    elif avg_trades < 80:
        trade_penalty = -0.2  # borderline low
    else:
        trade_penalty = 0.0

    score = avg_sharpe + avg_pf_bonus + trade_penalty

    return score


# Duplicate suppression: hash param cache
_seen_hashes = set()


def param_hash(params: dict) -> str:
    """Hash param dict for dedup."""
    key = json.dumps(params, sort_keys=True)
    return hashlib.md5(key.encode()).hexdigest()[:12]


def create_study():
    """Create Optuna study with TPE sampler."""
    sampler = optuna.samplers.TPESampler(seed=42, n_startup_trials=10)
    study = optuna.create_study(
        direction="maximize",
        sampler=sampler,
        study_name="e3_optuna",
    )
    return study


def suggest_params(trial: optuna.Trial) -> dict:
    """Define search space — 4 params."""
    params = {
        "funding_streak_min": trial.suggest_int("funding_streak_min", 2, 6),
        "funding_rate_threshold": trial.suggest_float(
            "funding_rate_threshold", 0.00001, 0.0005, log=True
        ),
        "sl_pct": trial.suggest_float("sl_pct", 0.02, 0.10, step=0.005),
        "time_limit_seconds": trial.suggest_int(
            "time_limit_seconds", 86400, 604800, step=43200
        ),  # 1 day to 7 days, step = 12h
    }
    return params


async def objective(trial, shared_candles, connector, engine, pairs,
                    train_start, train_end):
    """Optuna objective: run backtests on train window, return composite score."""
    params = suggest_params(trial)

    # Dedup check
    h = param_hash(params)
    if h in _seen_hashes:
        raise optuna.TrialPruned(f"Duplicate params: {h}")
    _seen_hashes.add(h)

    # Build overrides dict (correct field names for E3)
    overrides = {
        "funding_streak_min": params["funding_streak_min"],
        "funding_rate_threshold": params["funding_rate_threshold"],
        "sl_pct": params["sl_pct"],
        "time_limit_seconds": params["time_limit_seconds"],
    }

    pair_results = {}
    for pair in pairs:
        r = await run_backtest(
            pair, engine, connector, shared_candles,
            config_overrides=overrides,
            start_ts=train_start, end_ts=train_end,
        )
        pair_results[pair] = r

        # Early pruning: if first pair fails badly, skip rest
        if r.get("error") or r["pf"] < 0.8:
            trial.set_user_attr("early_prune", True)
            return -10.0

    score = compute_objective(pair_results)

    # Store per-pair results for analysis
    trial.set_user_attr("pair_results", pair_results)
    trial.set_user_attr("avg_pf", np.mean([r["pf"] for r in pair_results.values()]))
    trial.set_user_attr("avg_trades", np.mean([r["trades"] for r in pair_results.values()]))

    return score


def analyze_study(study: optuna.Study, n_top=10):
    """Analyze completed study for boundary pinning, clustering, trade drift."""
    print(f"\n{'=' * 90}")
    print(f"  OPTUNA OPTIMIZATION RESULTS — {len(study.trials)} trials")
    print(f"{'=' * 90}")

    # Filter completed trials
    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    if not completed:
        print("  No completed trials!")
        return

    # Sort by value (objective)
    completed.sort(key=lambda t: t.value, reverse=True)
    top_n = completed[:n_top]

    # Best trial
    best = study.best_trial
    print(f"\n  Best trial #{best.number}: score={best.value:.4f}")
    print(f"  Parameters:")
    for k, v in best.params.items():
        print(f"    {k}: {v}")
    print(f"  avg_PF={best.user_attrs.get('avg_pf', 0):.3f}  "
          f"avg_trades={best.user_attrs.get('avg_trades', 0):.0f}")

    pair_results = best.user_attrs.get("pair_results", {})
    if pair_results:
        for pair, r in sorted(pair_results.items()):
            print(f"    {pair}: PF={r['pf']:.3f}  trades={r['trades']}  "
                  f"Sharpe={r['sharpe']:.2f}  WR={r['wr']:.1f}%")

    # Top-10 analysis
    print(f"\n  {'─' * 80}")
    print(f"  Top-{n_top} trials:")
    print(f"  {'─' * 80}")
    for t in top_n:
        print(f"    #{t.number:3d}  score={t.value:.4f}  "
              + "  ".join(f"{k}={v}" for k, v in t.params.items())
              + f"  avg_PF={t.user_attrs.get('avg_pf', 0):.3f}"
              + f"  trades={t.user_attrs.get('avg_trades', 0):.0f}")

    # Clustering analysis: check if top-10 params are stable
    print(f"\n  {'─' * 80}")
    print(f"  Top-{n_top} parameter clustering:")
    print(f"  {'─' * 80}")
    for param_name in best.params:
        values = [t.params[param_name] for t in top_n]
        mean_v = np.mean(values)
        std_v = np.std(values)
        cv = std_v / abs(mean_v) if mean_v != 0 else 0
        status = "TIGHT" if cv < 0.2 else ("MODERATE" if cv < 0.5 else "SCATTERED")
        print(f"    {param_name:<25} mean={mean_v:.6f}  std={std_v:.6f}  CV={cv:.3f}  [{status}]")

    # Boundary pinning check
    print(f"\n  {'─' * 80}")
    print(f"  Boundary pinning check:")
    print(f"  {'─' * 80}")
    bounds = {
        "funding_streak_min": (2, 6),
        "funding_rate_threshold": (0.00001, 0.0005),
        "sl_pct": (0.02, 0.10),
        "time_limit_seconds": (86400, 604800),
    }
    for param_name, (lo, hi) in bounds.items():
        best_val = best.params[param_name]
        at_boundary = (best_val == lo or best_val == hi)
        top_at_boundary = sum(1 for t in top_n
                              if t.params[param_name] == lo or t.params[param_name] == hi)
        status = "PINNED" if at_boundary else "OK"
        if top_at_boundary >= n_top * 0.5:
            status = "CLUSTER AT EDGE"
        print(f"    {param_name:<25} best={best_val}  boundary={status}  "
              f"({top_at_boundary}/{n_top} at edge)")

    # Trade count drift
    print(f"\n  {'─' * 80}")
    print(f"  Trade count drift (vs baseline ~250 trades in train window):")
    print(f"  {'─' * 80}")
    best_trades = best.user_attrs.get("avg_trades", 0)
    baseline_trades = 250  # approximate baseline for train window
    ratio = best_trades / baseline_trades if baseline_trades > 0 else 0
    status = "OK" if 0.5 <= ratio <= 2.0 else "IDENTITY DRIFT"
    print(f"    Best trial: {best_trades:.0f} trades (ratio={ratio:.2f}x baseline)  [{status}]")

    return best


async def run_validation(best_params, shared_candles, connector, engine, pairs,
                         val_start, val_end):
    """Phase 1.5: One-shot validation with locked params on held-out window."""
    print(f"\n{'=' * 90}")
    print(f"  Phase 1.5: ONE-SHOT VALIDATION — locked params on held-out window")
    print(f"  Window: {datetime.fromtimestamp(val_start, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(val_end, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"({(val_end - val_start) // 86400} days)")
    print(f"  Params: {best_params}")
    print(f"{'=' * 90}")

    overrides = {
        "funding_streak_min": best_params["funding_streak_min"],
        "funding_rate_threshold": best_params["funding_rate_threshold"],
        "sl_pct": best_params["sl_pct"],
        "time_limit_seconds": best_params["time_limit_seconds"],
    }

    pair_results = {}
    for pair in pairs:
        r = await run_backtest(
            pair, engine, connector, shared_candles,
            config_overrides=overrides,
            start_ts=val_start, end_ts=val_end,
        )
        pair_results[pair] = r
        print(f"    {pair:<16} PF={r['pf']:.3f}  trades={r['trades']}  "
              f"Sharpe={r['sharpe']:.2f}  WR={r['wr']:.1f}%")

    # Validation metrics
    avg_pf = np.mean([r["pf"] for r in pair_results.values()])
    avg_sharpe = np.mean([r["sharpe"] for r in pair_results.values()])
    avg_trades = np.mean([r["trades"] for r in pair_results.values()])

    print(f"\n  Validation averages: PF={avg_pf:.3f}  Sharpe={avg_sharpe:.2f}  trades={avg_trades:.0f}")

    return pair_results, avg_pf, avg_sharpe


async def main(n_trials, validate_only):
    engine = "E3"
    connector = "bybit_perpetual"

    # Compute time range from parquet
    all_pairs = OPTUNA_PAIRS + ["BTC-USDT"]
    end_ts = get_parquet_end_time(connector, all_pairs, resolution="1m")
    start_ts = end_ts - (TRAIN_DAYS + VALIDATION_DAYS) * 86400

    train_start = start_ts
    train_end = start_ts + TRAIN_DAYS * 86400
    val_start = train_end
    val_end = end_ts

    print("=" * 90)
    print(f"  Phase 1.4: OPTUNA PARAMETER OPTIMIZATION — {engine}")
    print(f"  Pairs: {', '.join(OPTUNA_PAIRS)}")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 90)
    print(f"\n  Full range: {datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')}")
    print(f"  Train:      {datetime.fromtimestamp(train_start, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(train_end, tz=timezone.utc).strftime('%Y-%m-%d')} ({TRAIN_DAYS}d)")
    print(f"  Validation: {datetime.fromtimestamp(val_start, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(val_end, tz=timezone.utc).strftime('%Y-%m-%d')} ({VALIDATION_DAYS}d)")

    # Load shared candles once
    print(f"\n  Loading candles and merging derivatives...")
    shared_candles = await load_shared_candles_with_derivatives(
        all_pairs, connector, engine
    )

    if validate_only:
        # Skip Optuna, just run validation with hardcoded best params
        # (update these after Optuna completes)
        best_params = {
            "funding_streak_min": 3,
            "funding_rate_threshold": 0.0001,
            "sl_pct": 0.05,
            "time_limit_seconds": 432000,
        }
        print(f"\n  Running validation only with params: {best_params}")
        await run_validation(best_params, shared_candles, connector, engine,
                             OPTUNA_PAIRS, val_start, val_end)
        return

    # Run baseline on train window first
    print(f"\n  Running baseline on train window...")
    baseline_results = {}
    for pair in OPTUNA_PAIRS:
        r = await run_backtest(pair, engine, connector, shared_candles,
                               start_ts=train_start, end_ts=train_end)
        baseline_results[pair] = r
        print(f"    {pair:<16} PF={r['pf']:.3f}  trades={r['trades']}  Sharpe={r['sharpe']:.2f}")

    baseline_score = compute_objective(baseline_results)
    baseline_avg_pf = np.mean([r["pf"] for r in baseline_results.values()])
    baseline_avg_trades = np.mean([r["trades"] for r in baseline_results.values()])
    print(f"  Baseline score: {baseline_score:.4f}  avg_PF={baseline_avg_pf:.3f}  "
          f"avg_trades={baseline_avg_trades:.0f}")

    # Create and run Optuna study
    print(f"\n  Starting Optuna optimization ({n_trials} trials)...")
    print(f"  Search space:")
    print(f"    funding_streak_min:    [2, 6] (int)")
    print(f"    funding_rate_threshold: [0.00001, 0.0005] (log)")
    print(f"    sl_pct:                [0.02, 0.10] (step=0.005)")
    print(f"    time_limit_seconds:    [86400, 604800] (step=43200)")

    study = create_study()

    for i in range(n_trials):
        trial = study.ask()
        try:
            value = await objective(trial, shared_candles, connector, engine,
                                    OPTUNA_PAIRS, train_start, train_end)
            study.tell(trial, value)
            avg_pf = trial.user_attrs.get("avg_pf", 0)
            avg_trades = trial.user_attrs.get("avg_trades", 0)
            print(f"  [{i+1:3d}/{n_trials}] #{trial.number}  score={value:.4f}  "
                  f"avg_PF={avg_pf:.3f}  trades={avg_trades:.0f}  "
                  + "  ".join(f"{k}={v}" for k, v in trial.params.items()))
        except optuna.TrialPruned as e:
            study.tell(trial, state=optuna.trial.TrialState.PRUNED)
            print(f"  [{i+1:3d}/{n_trials}] PRUNED: {e}")

    # Analyze results
    best = analyze_study(study)

    # Compare best vs baseline
    if best:
        print(f"\n  {'─' * 80}")
        print(f"  Baseline vs Best:")
        print(f"  {'─' * 80}")
        print(f"    Baseline: score={baseline_score:.4f}  avg_PF={baseline_avg_pf:.3f}  "
              f"trades={baseline_avg_trades:.0f}")
        best_avg_pf = best.user_attrs.get("avg_pf", 0)
        best_avg_trades = best.user_attrs.get("avg_trades", 0)
        print(f"    Best:     score={best.value:.4f}  avg_PF={best_avg_pf:.3f}  "
              f"trades={best_avg_trades:.0f}")

        pf_delta = best_avg_pf - baseline_avg_pf
        if pf_delta > 0.05:
            print(f"    Optuna improved PF by +{pf_delta:.3f}")
        elif pf_delta > -0.02:
            print(f"    Optuna ~= baseline (delta={pf_delta:+.3f})")
            print(f"    Baseline params are near-optimal — keep them for robustness")
        else:
            print(f"    *** Optuna WORSE than baseline (delta={pf_delta:+.3f}) — keep baseline ***")

        # Auto-run validation with best params
        print(f"\n  Auto-running one-shot validation with best params...")
        val_results, val_pf, val_sharpe = await run_validation(
            best.params, shared_candles, connector, engine,
            OPTUNA_PAIRS, val_start, val_end
        )

        # Train vs validation comparison
        train_sharpe = np.mean([r["sharpe"] for r in best.user_attrs.get("pair_results", {}).values()])
        sharpe_delta = abs(train_sharpe - val_sharpe)
        print(f"\n  {'─' * 80}")
        print(f"  Train vs Validation (Phase 1.5 check):")
        print(f"  {'─' * 80}")
        print(f"    Train Sharpe:  {train_sharpe:.2f}")
        print(f"    Val Sharpe:    {val_sharpe:.2f}")
        print(f"    Delta:         {sharpe_delta:.2f}  "
              f"{'SUSPICIOUS (>0.3)' if sharpe_delta > 0.3 else 'OK'}")
        print(f"    Train avg PF:  {best_avg_pf:.3f}")
        print(f"    Val avg PF:    {val_pf:.3f}")

        train_trades = best.user_attrs.get("avg_trades", 0)
        val_trades = np.mean([r["trades"] for r in val_results.values()])
        # Normalize by window length
        train_rate = train_trades / TRAIN_DAYS
        val_rate = val_trades / VALIDATION_DAYS
        rate_ratio = val_rate / train_rate if train_rate > 0 else 0
        print(f"    Trade rate:    train={train_rate:.1f}/day  val={val_rate:.1f}/day  "
              f"ratio={rate_ratio:.2f}  {'OK' if 0.5 <= rate_ratio <= 2.0 else 'REGIME MISMATCH'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-trials", type=int, default=50)
    parser.add_argument("--validate", action="store_true",
                        help="Run validation only with hardcoded best params")
    args = parser.parse_args()

    asyncio.run(main(args.n_trials, args.validate))
