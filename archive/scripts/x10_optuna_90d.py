#!/usr/bin/env python3
"""
X10 Optuna optimization on 90d REAL hourly liquidation data.

Train: Jan 20 - Mar 20, 2026 (60d)
Test:  Mar 20 - Apr 21, 2026 (30d) — ONE SHOT, locked after first use

Optimizes: z_total, z_imb, sl_atr, tp_atr, trailing_activation, trailing_delta, time_limit
Objective: composite (Sharpe + PF bonus - DD penalty), not raw PnL
"""
import asyncio
import gc
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017/quants_lab")
os.environ.setdefault("MONGO_DATABASE", "quants_lab")

import numpy as np
import optuna

# Silence optuna logging during trials
optuna.logging.set_verbosity(optuna.logging.WARNING)

# ── Date boundaries ──
TRAIN_START = int(datetime(2026, 1, 20, tzinfo=timezone.utc).timestamp())
TRAIN_END = int(datetime(2026, 3, 20, tzinfo=timezone.utc).timestamp())
TEST_START = TRAIN_END
TEST_END = int(datetime(2026, 4, 21, tzinfo=timezone.utc).timestamp())

# 3 diverse pairs for optimization speed (major, mid, alt)
OPTUNA_PAIRS = ["BTC-USDT", "SOL-USDT", "TAO-USDT"]


async def run_backtest(pairs, start_ts, end_ts, config_overrides):
    """Run backtest on given pairs/window, return pooled trade PnLs."""
    from core.backtesting import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config
    import pandas as pd

    meta = get_strategy("X10")
    all_pnls = []

    for pair in pairs:
        config = build_backtest_config(engine_name="X10", connector="bybit_perpetual", pair=pair)
        for k, v in config_overrides.items():
            if hasattr(config, k):
                setattr(config, k, v)

        engine = BacktestingEngine(load_cached_data=True)
        try:
            result = await engine.run_backtesting(
                config=config, start=start_ts, end=end_ts,
                backtesting_resolution=meta.backtesting_resolution,
                trade_cost=0.000375,
            )
            ct = result.results.get("close_types", {})
            if not isinstance(ct, dict):
                ct = {}
            n = sum(ct.values())
            if n > 0 and result.executors_df is not None:
                edf = result.executors_df
                edf["net_pnl_quote"] = pd.to_numeric(edf["net_pnl_quote"], errors="coerce")
                all_pnls.extend(edf["net_pnl_quote"].dropna().tolist())
        except Exception:
            pass
        del engine
        gc.collect()

    return np.array(all_pnls) if all_pnls else np.array([])


def composite_score(pnls: np.ndarray) -> float:
    """Composite objective: Sharpe + PF bonus - DD penalty."""
    if len(pnls) < 10:
        return -10.0

    mean = np.mean(pnls)
    std = np.std(pnls)
    sharpe = mean / std * np.sqrt(252 * 2) if std > 0 else 0  # ~2 trades/day

    pos = pnls[pnls > 0].sum()
    neg = -pnls[pnls < 0].sum()
    pf = pos / neg if neg > 0 else 10.0

    # Max drawdown
    cum = np.cumsum(pnls)
    peak = np.maximum.accumulate(cum)
    dd = (peak - cum).max()
    dd_pct = dd / (100000 * 0.003) if dd > 0 else 0  # relative to position size

    wr = (pnls > 0).mean()

    # Composite
    score = sharpe
    if pf > 1.5:
        score += 0.5
    if pf > 2.0:
        score += 0.5
    if wr > 0.55:
        score += 0.3
    if dd_pct > 0.5:
        score -= 1.0
    if len(pnls) < 20:
        score -= 1.0  # penalize low trade count

    return score


async def objective(trial):
    """Optuna objective: optimize on TRAIN window only."""
    overrides = {
        "z_total_threshold": trial.suggest_float("z_total", 1.0, 3.0, step=0.25),
        "z_imb_threshold": trial.suggest_float("z_imb", 0.25, 1.5, step=0.25),
        "z_window": trial.suggest_int("z_window", 36, 120, step=12),
        "sl_atr_mult": trial.suggest_float("sl_atr", 1.5, 4.0, step=0.5),
        "tp_atr_mult": trial.suggest_float("tp_atr", 1.0, 3.5, step=0.5),
        "trailing_activation_atr": trial.suggest_float("trail_act", 0.8, 2.5, step=0.25),
        "trailing_delta_atr": trial.suggest_float("trail_delta", 0.3, 1.2, step=0.1),
        "time_limit_seconds": trial.suggest_int("time_limit_h", 4, 24, step=2) * 3600,
    }

    pnls = await run_backtest(OPTUNA_PAIRS, TRAIN_START, TRAIN_END, overrides)
    return composite_score(pnls)


async def main():
    print("=" * 70)
    print("X10 OPTUNA OPTIMIZATION — 90d real hourly data")
    print("=" * 70)
    print(f"Train: {datetime.utcfromtimestamp(TRAIN_START).date()} to {datetime.utcfromtimestamp(TRAIN_END).date()} (60d)")
    print(f"Test:  {datetime.utcfromtimestamp(TEST_START).date()} to {datetime.utcfromtimestamp(TEST_END).date()} (30d)")
    print(f"Pairs: {OPTUNA_PAIRS}")
    print()

    # ── OPTUNA TRAIN ─────────────────────────────────────────
    study = optuna.create_study(direction="maximize", study_name="x10_90d")

    n_trials = 40
    print(f"Running {n_trials} Optuna trials on train window...")

    for i in range(n_trials):
        trial = study.ask()
        value = await objective(trial)
        study.tell(trial, value)
        if (i + 1) % 10 == 0:
            best = study.best_trial
            print(f"  Trial {i+1}/{n_trials}: best_score={best.value:.2f} params={best.params}")

    best = study.best_trial
    print(f"\n{'='*70}")
    print(f"BEST TRAIN PARAMS (score={best.value:.2f}):")
    print(f"{'='*70}")
    for k, v in sorted(best.params.items()):
        print(f"  {k}: {v}")

    # Build config overrides from best
    bp = best.params
    best_overrides = {
        "z_total_threshold": bp["z_total"],
        "z_imb_threshold": bp["z_imb"],
        "z_window": bp["z_window"],
        "sl_atr_mult": bp["sl_atr"],
        "tp_atr_mult": bp["tp_atr"],
        "trailing_activation_atr": bp["trail_act"],
        "trailing_delta_atr": bp["trail_delta"],
        "time_limit_seconds": bp["time_limit_h"] * 3600,
    }

    # ── TRAIN RESULTS ────────────────────────────────────────
    print(f"\n--- Train Results (best params) ---")
    train_pnls = await run_backtest(OPTUNA_PAIRS, TRAIN_START, TRAIN_END, best_overrides)
    if len(train_pnls) > 0:
        pos = train_pnls[train_pnls > 0].sum()
        neg = -train_pnls[train_pnls < 0].sum()
        pf = pos / neg if neg > 0 else float('inf')
        print(f"  n={len(train_pnls)}, mean=${np.mean(train_pnls):.4f}, "
              f"WR={(train_pnls > 0).mean():.1%}, PF={pf:.2f}")

    # ── TEST (ONE SHOT) ──────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"TEST VALIDATION (one-shot, locked)")
    print(f"{'='*70}")

    # Run on ALL 18 pairs for test
    all_pairs = ["ADA-USDT","APT-USDT","AVAX-USDT","BCH-USDT","BNB-USDT","BTC-USDT",
                 "DOGE-USDT","DOT-USDT","ETH-USDT","LINK-USDT","LTC-USDT","NEAR-USDT",
                 "SEI-USDT","SOL-USDT","SUI-USDT","TAO-USDT","WLD-USDT","XRP-USDT"]

    test_pnls = await run_backtest(all_pairs, TEST_START, TEST_END, best_overrides)
    if len(test_pnls) > 0:
        pos = test_pnls[test_pnls > 0].sum()
        neg = -test_pnls[test_pnls < 0].sum()
        pf = pos / neg if neg > 0 else float('inf')
        wr = (test_pnls > 0).mean()
        print(f"  n={len(test_pnls)}, mean=${np.mean(test_pnls):.4f}, "
              f"median=${np.median(test_pnls):.4f}, WR={wr:.1%}, PF={pf:.2f}")
        print(f"  Total PnL: ${test_pnls.sum():.2f}")

        # Check degradation
        if len(train_pnls) > 0:
            train_sharpe = np.mean(train_pnls) / np.std(train_pnls) if np.std(train_pnls) > 0 else 0
            test_sharpe = np.mean(test_pnls) / np.std(test_pnls) if np.std(test_pnls) > 0 else 0
            delta = abs(train_sharpe - test_sharpe)
            print(f"  Train Sharpe: {train_sharpe:.2f}, Test Sharpe: {test_sharpe:.2f}, Delta: {delta:.2f}")
            if delta > 0.3:
                print(f"  ⚠ SUSPICIOUS: Sharpe delta > 0.3 — potential overfit")
            else:
                print(f"  ✓ Sharpe delta acceptable")
    else:
        print("  ⚠ ZERO TEST TRADES")

    # ── COMPARE WITH DEFAULTS ────────────────────────────────
    print(f"\n--- Default params comparison (test window) ---")
    default_overrides = {
        "z_total_threshold": 2.0,
        "z_imb_threshold": 0.5,
        "z_window": 72,
        "sl_atr_mult": 2.5,
        "tp_atr_mult": 2.0,
        "trailing_activation_atr": 1.5,
        "trailing_delta_atr": 0.7,
        "time_limit_seconds": 43200,
    }
    default_pnls = await run_backtest(all_pairs, TEST_START, TEST_END, default_overrides)
    if len(default_pnls) > 0:
        pos = default_pnls[default_pnls > 0].sum()
        neg = -default_pnls[default_pnls < 0].sum()
        pf = pos / neg if neg > 0 else float('inf')
        print(f"  Default: n={len(default_pnls)}, mean=${np.mean(default_pnls):.4f}, "
              f"WR={(default_pnls > 0).mean():.1%}, PF={pf:.2f}")
    else:
        print("  Default: ZERO TRADES")

    # ── TOP-10 CLUSTERING CHECK ──────────────────────────────
    print(f"\n--- Anti-overfit: top-10 Optuna trials ---")
    top10 = sorted(study.trials, key=lambda t: t.value if t.value is not None else -999, reverse=True)[:10]
    for i, t in enumerate(top10):
        if t.value is not None:
            print(f"  #{i+1} score={t.value:.2f} z_total={t.params['z_total']:.2f} "
                  f"z_imb={t.params['z_imb']:.2f} sl={t.params['sl_atr']:.1f} "
                  f"tp={t.params['tp_atr']:.1f} tl={t.params['time_limit_h']}h")

    # ── BOUNDARY CHECK ───────────────────────────────────────
    print(f"\n--- Boundary check ---")
    bounds = {
        "z_total": (1.0, 3.0), "z_imb": (0.25, 1.5), "sl_atr": (1.5, 4.0),
        "tp_atr": (1.0, 3.5), "trail_act": (0.8, 2.5), "trail_delta": (0.3, 1.2),
        "time_limit_h": (4, 24),
    }
    for param, (lo, hi) in bounds.items():
        val = bp[param]
        at_boundary = val <= lo or val >= hi
        print(f"  {param}: {val} {'⚠ BOUNDARY' if at_boundary else '✓'}")

    print(f"\n{'='*70}")
    print("DONE — update controller defaults with best params if test passes")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
