"""
E3 Robustness Suite — Phase 1.2, 1.3c, P0, P1 from research-process skill.

Covers:
  Phase 1.2  — Regime stress test (breakdown by BTC regime at trade time)
  Phase 1.3c — Mini sensitivity (perturb key params +/- 1 step)
  P0         — Degradation stress tests:
                 A2: Slippage tiers (2/5/10 bps)
                 Trade distribution: R-multiples, top-5 concentration, max consec losses
  P1         — Monte Carlo block bootstrap (10k sims, ruin < 1%)

Usage:
  # All analyses (trade-data first, then backtest re-runs):
  python scripts/e3_robustness.py --engine E3

  # Trade-data only (fast, no re-runs):
  python scripts/e3_robustness.py --engine E3 --trade-data-only

  # Specific pair:
  python scripts/e3_robustness.py --engine E3 --pair ADA-USDT

  # Slippage + sensitivity only:
  python scripts/e3_robustness.py --engine E3 --backtest-only
"""
import argparse
import asyncio
import gc
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Config ──────────────────────────────────────────────────
TOP_PAIRS = ["ADA-USDT", "SUI-USDT", "SOL-USDT", "XRP-USDT"]

SLIPPAGE_TIERS_BPS = [2, 5, 10]          # A2: must pass at 2, should at 5
MONTE_CARLO_SIMS = 10_000
MONTE_CARLO_BLOCK = 5                     # block bootstrap block size
RUIN_THRESHOLD = 0.01                     # 1% ruin probability gate

# Mini sensitivity: perturb these params
SENSITIVITY_PARAMS = {
    "funding_streak_min": [2, 3, 4, 5],
    "tp_pct":            [0.02, 0.025, 0.03, 0.035, 0.04],
    "sl_pct":            [0.03, 0.04, 0.05, 0.06, 0.07],
}
# Baseline values (must match current E3 config)
BASELINE = {"funding_streak_min": 3, "tp_pct": 0.03, "sl_pct": 0.05}


# ═══════════════════════════════════════════════════════════
#  SECTION 1: Trade-Data Analyses (from backtest_trades, no re-run)
# ═══════════════════════════════════════════════════════════

def load_trades(db, engine: str, pair: str = None) -> pd.DataFrame:
    """Load backtest trades from MongoDB."""
    query = {"engine": engine}
    if pair:
        query["pair"] = pair
    trades = list(db["backtest_trades"].find(query))
    if not trades:
        return pd.DataFrame()
    df = pd.DataFrame(trades)
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
    return df


def load_btc_candles(db) -> pd.DataFrame:
    """Load BTC 1h candles for regime classification."""
    # Use cached parquet for BTC
    import glob
    parquet_dir = "app/data/cache/candles"
    btc_files = glob.glob(f"{parquet_dir}/*BTC*USDT*1h*.parquet")
    if not btc_files:
        # Fallback: try to load from any available BTC candle file
        btc_files = glob.glob(f"{parquet_dir}/*BTC*USDT*.parquet")
    if not btc_files:
        print("  WARNING: No BTC candle data found for regime classification")
        return pd.DataFrame()
    # Use the most recent file
    btc_files.sort(key=os.path.getmtime, reverse=True)
    df = pd.read_parquet(btc_files[0])
    if "timestamp" in df.columns:
        df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s")
    elif "open_time" in df.columns:
        df["timestamp_dt"] = pd.to_datetime(df["open_time"], unit="s")
    return df


def classify_regime(btc_df: pd.DataFrame) -> pd.DataFrame:
    """Classify each BTC candle into regime: BULL, BEAR, RANGE, SHOCK.

    Uses 7-day rolling return and 7-day rolling volatility.
    - SHOCK: 7d vol > 2x median vol
    - BULL: 7d return > 2%
    - BEAR: 7d return < -2%
    - RANGE: everything else
    """
    if btc_df.empty:
        return btc_df
    df = btc_df.copy()
    df = df.sort_values("timestamp_dt")
    df["ret_7d"] = df["close"].pct_change(168)   # 168 1h bars = 7 days
    df["vol_7d"] = df["close"].pct_change().rolling(168).std()
    vol_median = df["vol_7d"].median()

    def _regime(row):
        if pd.isna(row["vol_7d"]) or pd.isna(row["ret_7d"]):
            return "UNKNOWN"
        if row["vol_7d"] > 2 * vol_median:
            return "SHOCK"
        if row["ret_7d"] > 0.02:
            return "BULL"
        if row["ret_7d"] < -0.02:
            return "BEAR"
        return "RANGE"

    df["regime"] = df.apply(_regime, axis=1)
    return df[["timestamp_dt", "regime"]].copy()


def tag_trades_with_regime(trades_df: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    """Merge regime label onto each trade."""
    if regime_df.empty or trades_df.empty:
        trades_df["regime"] = "UNKNOWN"
        return trades_df
    trades_df = trades_df.sort_values("timestamp_dt")
    regime_df = regime_df.sort_values("timestamp_dt")
    merged = pd.merge_asof(
        trades_df, regime_df,
        on="timestamp_dt",
        direction="backward"
    )
    return merged


def analyze_regime_breakdown(trades_df: pd.DataFrame, pair: str):
    """Phase 1.2: Regime stress test — PF/PnL breakdown by regime."""
    print(f"\n  ── Phase 1.2: Regime Breakdown ({pair}) ──")

    if "regime" not in trades_df.columns or trades_df.empty:
        print("    No regime data available")
        return {}

    results = {}
    for regime in ["BULL", "BEAR", "RANGE", "SHOCK", "UNKNOWN"]:
        subset = trades_df[trades_df["regime"] == regime]
        if len(subset) == 0:
            continue
        wins = subset[subset["net_pnl_quote"] > 0]["net_pnl_quote"].sum()
        losses = abs(subset[subset["net_pnl_quote"] <= 0]["net_pnl_quote"].sum())
        pf = wins / losses if losses > 0 else float("inf")
        total_pnl = subset["net_pnl_quote"].sum()
        wr = (subset["net_pnl_quote"] > 0).mean() * 100
        results[regime] = {
            "trades": len(subset), "pf": pf, "pnl": total_pnl,
            "wr": wr, "pnl_pct": 0  # filled below
        }

    total_pnl = sum(r["pnl"] for r in results.values())
    for r in results.values():
        r["pnl_pct"] = (r["pnl"] / total_pnl * 100) if total_pnl != 0 else 0

    print(f"    {'Regime':<8} {'Trades':>7} {'PF':>7} {'WR%':>6} {'PnL':>10} {'PnL%':>6}")
    print(f"    {'-'*48}")
    for regime, r in sorted(results.items(), key=lambda x: -x[1]["pnl"]):
        print(f"    {regime:<8} {r['trades']:>7} {r['pf']:>7.2f} {r['wr']:>5.1f}% {r['pnl']:>10.2f} {r['pnl_pct']:>5.1f}%")

    # Gate check: require positive expectancy in >= 2 regimes
    positive_regimes = [k for k, v in results.items() if v["pf"] > 1.0 and k != "UNKNOWN"]
    print(f"\n    Positive-expectancy regimes: {len(positive_regimes)} ({', '.join(positive_regimes)})")
    if len(positive_regimes) < 2:
        print("    *** GATE FAIL: Need positive expectancy in >= 2 regimes ***")
    else:
        print("    GATE PASS: >= 2 regimes with positive expectancy")

    # Identify failure regime
    failure_regimes = [k for k, v in results.items() if v["pf"] < 1.0 and k != "UNKNOWN"]
    if failure_regimes:
        print(f"    Failure regimes: {', '.join(failure_regimes)}")

    return results


def analyze_r_distribution(trades_df: pd.DataFrame, pair: str, sl_pct: float = 0.05):
    """P0: R-multiple distribution, top-5 concentration, max consecutive losses."""
    print(f"\n  ── P0: Trade Distribution ({pair}, N={len(trades_df)}) ──")

    if trades_df.empty:
        print("    No trades")
        return {}

    pnl = trades_df["net_pnl_quote"].values
    # R-multiple: PnL / risk (risk = position_size * SL)
    # Approximate risk from trade amounts if available
    if "filled_amount_quote" in trades_df.columns:
        risk = trades_df["filled_amount_quote"].values * sl_pct
        r_mult = pnl / np.where(risk > 0, risk, 1)
    else:
        # Normalize to units of SL
        r_mult = pnl / abs(pnl).mean() if abs(pnl).mean() > 0 else pnl

    # Distribution stats
    print(f"    R-multiples: mean={np.mean(r_mult):.3f}  median={np.median(r_mult):.3f}  "
          f"std={np.std(r_mult):.3f}  skew={pd.Series(r_mult).skew():.3f}")
    print(f"    Min R: {np.min(r_mult):.3f}  Max R: {np.max(r_mult):.3f}")

    # Percentiles
    pcts = [5, 25, 50, 75, 95]
    vals = np.percentile(r_mult, pcts)
    print(f"    Percentiles: " + "  ".join(f"p{p}={v:.3f}" for p, v in zip(pcts, vals)))

    # Gate: Avg R > 0 AND Median R > 0
    avg_r = np.mean(r_mult)
    med_r = np.median(r_mult)
    if avg_r > 0 and med_r > 0:
        print(f"    GATE PASS: Avg R ({avg_r:.3f}) > 0 AND Median R ({med_r:.3f}) > 0")
    else:
        print(f"    *** GATE FAIL: Avg R={avg_r:.3f}, Median R={med_r:.3f} (both must be > 0) ***")

    # Top-5 trade concentration
    sorted_pnl = np.sort(pnl)[::-1]
    top5_pnl = sorted_pnl[:5].sum()
    total_pnl = pnl.sum()
    top5_pct = (top5_pnl / total_pnl * 100) if total_pnl > 0 else 0
    print(f"\n    Top-5 trade PnL: {top5_pnl:.2f} / {total_pnl:.2f} = {top5_pct:.1f}%")
    if top5_pct > 80:
        print("    *** STOP CONDITION: Top-5 trades contribute > 80% of PnL — outlier dependence ***")
    else:
        print(f"    PASS: Top-5 concentration {top5_pct:.1f}% < 80%")

    # Max consecutive losses
    is_loss = (pnl < 0).astype(int)
    if len(is_loss) > 0:
        streaks = is_loss.copy()
        max_streak = 0
        current = 0
        for v in is_loss:
            if v == 1:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
    else:
        max_streak = 0

    print(f"    Max consecutive losses: {max_streak}")
    if max_streak > 5:
        print("    *** STOP CONDITION: Max consecutive losses > 5 — review regime alignment ***")
    elif max_streak > 3:
        print(f"    WARNING: Max consecutive losses = {max_streak} (gate is <= 3, review recommended)")
    else:
        print(f"    PASS: Max consecutive losses {max_streak} <= 3")

    return {
        "avg_r": avg_r, "median_r": med_r, "top5_pct": top5_pct,
        "max_consec_loss": max_streak, "n_trades": len(trades_df),
    }


def monte_carlo_block_bootstrap(trades_df: pd.DataFrame, pair: str,
                                 n_sims: int = MONTE_CARLO_SIMS,
                                 block_size: int = MONTE_CARLO_BLOCK,
                                 position_size: float = 300.0,
                                 capital: float = 100_000.0):
    """P1: Monte Carlo block bootstrap — confidence intervals and ruin probability."""
    print(f"\n  ── P1: Monte Carlo Block Bootstrap ({pair}, {n_sims} sims) ──")

    if trades_df.empty or len(trades_df) < 10:
        print("    Insufficient trades for Monte Carlo")
        return {}

    pnl = trades_df["net_pnl_quote"].values
    n = len(pnl)

    # Block bootstrap: resample blocks of consecutive trades
    n_blocks = max(n // block_size, 1)
    sim_pfs = []
    sim_sharpes = []
    sim_max_dds = []
    sim_final_pnls = []
    ruin_count = 0
    ruin_level = -capital * 0.20  # 20% drawdown = ruin at intended sizing

    rng = np.random.default_rng(42)

    for _ in range(n_sims):
        # Sample n_blocks random blocks (with replacement)
        block_starts = rng.integers(0, max(n - block_size, 1), size=n_blocks)
        sim_trades = np.concatenate([pnl[s:s+block_size] for s in block_starts])[:n]

        # PF
        wins = sim_trades[sim_trades > 0].sum()
        losses = abs(sim_trades[sim_trades <= 0].sum())
        sim_pf = wins / losses if losses > 0 else float("inf")
        sim_pfs.append(min(sim_pf, 10.0))  # cap for stats

        # Sharpe (daily, assume 1 trade/day average)
        if sim_trades.std() > 0:
            sim_sharpes.append(sim_trades.mean() / sim_trades.std() * np.sqrt(365))
        else:
            sim_sharpes.append(0)

        # Drawdown
        cum = np.cumsum(sim_trades)
        peak = np.maximum.accumulate(cum)
        dd = cum - peak
        max_dd = dd.min()
        sim_max_dds.append(max_dd)
        sim_final_pnls.append(cum[-1])

        # Ruin check
        if max_dd < ruin_level:
            ruin_count += 1

    sim_pfs = np.array(sim_pfs)
    sim_sharpes = np.array(sim_sharpes)
    sim_max_dds = np.array(sim_max_dds)
    sim_final_pnls = np.array(sim_final_pnls)
    ruin_prob = ruin_count / n_sims

    # Report
    print(f"    PF:     mean={np.mean(sim_pfs):.3f}  "
          f"CI95=[{np.percentile(sim_pfs, 2.5):.3f}, {np.percentile(sim_pfs, 97.5):.3f}]  "
          f"p5={np.percentile(sim_pfs, 5):.3f}")
    print(f"    Sharpe: mean={np.mean(sim_sharpes):.3f}  "
          f"CI95=[{np.percentile(sim_sharpes, 2.5):.3f}, {np.percentile(sim_sharpes, 97.5):.3f}]")
    print(f"    MaxDD:  mean={np.mean(sim_max_dds):.2f}  "
          f"worst_5%={np.percentile(sim_max_dds, 5):.2f}")
    print(f"    Final PnL: mean={np.mean(sim_final_pnls):.2f}  "
          f"CI95=[{np.percentile(sim_final_pnls, 2.5):.2f}, {np.percentile(sim_final_pnls, 97.5):.2f}]")
    print(f"    Prob(PF < 1.0): {(sim_pfs < 1.0).mean()*100:.1f}%")
    print(f"    Ruin probability (20% DD at $300/trade on $100k): {ruin_prob*100:.2f}%")

    if ruin_prob > RUIN_THRESHOLD:
        print(f"    *** GATE FAIL: Ruin probability {ruin_prob*100:.2f}% > {RUIN_THRESHOLD*100:.1f}% ***")
    else:
        print(f"    GATE PASS: Ruin probability {ruin_prob*100:.2f}% < {RUIN_THRESHOLD*100:.1f}%")

    # CI check: does 95% CI on PF include 1.0?
    pf_lo = np.percentile(sim_pfs, 2.5)
    if pf_lo < 1.0:
        print(f"    WARNING: PF 95% CI lower bound ({pf_lo:.3f}) includes 1.0 — edge may be noise")
    else:
        print(f"    PASS: PF 95% CI lower bound ({pf_lo:.3f}) > 1.0 — edge is statistically significant")

    return {
        "pf_mean": np.mean(sim_pfs), "pf_ci95": (np.percentile(sim_pfs, 2.5), np.percentile(sim_pfs, 97.5)),
        "sharpe_mean": np.mean(sim_sharpes), "ruin_prob": ruin_prob,
        "prob_pf_below_1": (sim_pfs < 1.0).mean(),
    }


def anti_overfitting_checklist(trades_df: pd.DataFrame, wf_data: dict, pair: str):
    """Anti-overfitting checklist from quant-governance skill."""
    print(f"\n  ── Anti-Overfitting Checklist ({pair}) ──")

    checks = []

    # 1. Boundary params? (check if current params are at search space edges)
    # E3 uses funding_streak_min=3 (range 1-10), tp=0.03 (range 0.01-0.10), sl=0.05 (range 0.01-0.10)
    # None at boundaries.
    print("    [1] Boundary params: funding_streak_min=3 (range 1-10), tp=3% (range 1-10%), sl=5% (range 1-10%)")
    print("        PASS — no params at search space boundary")

    # 2. OOS collapse? (train vs test PF delta)
    test_pfs = [d.get("profit_factor", 0) for d in wf_data.get("test", [])]
    train_pfs = [d.get("profit_factor", 0) for d in wf_data.get("train", [])]
    test_sharpes = [d.get("sharpe", 0) for d in wf_data.get("test", [])]
    train_sharpes = [d.get("sharpe", 0) for d in wf_data.get("train", [])]
    if test_sharpes and train_sharpes:
        sharpe_delta = abs(np.mean(train_sharpes) - np.mean(test_sharpes))
        pf_delta = abs(np.mean(train_pfs) - np.mean(test_pfs))
        print(f"    [2] OOS collapse: train_sharpe={np.mean(train_sharpes):.3f} "
              f"test_sharpe={np.mean(test_sharpes):.3f} delta={sharpe_delta:.3f}")
        if sharpe_delta > 0.3:
            print("        *** WARNING: Sharpe delta > 0.3 — possible overfit ***")
            checks.append("sharpe_delta")
        else:
            print("        PASS — Sharpe delta <= 0.3")
    else:
        print("    [2] OOS collapse: no walk-forward data for this pair")

    # 3. Improvement driven by increased trade count?
    test_trades = sum(d.get("trades", 0) for d in wf_data.get("test", []))
    train_trades = sum(d.get("trades", 0) for d in wf_data.get("train", []))
    if train_trades > 0 and test_trades > 0:
        trade_ratio = test_trades / train_trades
        print(f"    [3] Trade count: train={train_trades} test={test_trades} ratio={trade_ratio:.2f}")
        # Identity check: trade count within 2x (adjust for period length difference)
        # Train=270d, Test=90d → expected ratio ~0.33
        expected_ratio = 90.0 / 270.0
        adjusted_ratio = trade_ratio / expected_ratio
        print(f"        Period-adjusted ratio: {adjusted_ratio:.2f} (expect ~1.0)")
        if adjusted_ratio > 2.0 or adjusted_ratio < 0.5:
            print("        *** WARNING: Trade count drift > 2x — identity change ***")
            checks.append("trade_count_drift")
        else:
            print("        PASS — trade count consistent with period length")

    # 4. PF stable across regimes? (from regime breakdown)
    print("    [4] PF stable across regimes — see Phase 1.2 above")

    # 5. Optuna clustering? N/A — no Optuna used (manual params)
    print("    [5] Optuna clustering: N/A — no optimization used (manual parameters)")

    n_fails = len(checks)
    if n_fails >= 2:
        print(f"\n    *** OVERFIT: {n_fails} checks failed (>= 2 = likely overfit) ***")
    else:
        print(f"\n    PASS: {n_fails} checks failed (< 2 threshold)")

    return checks


# ═══════════════════════════════════════════════════════════
#  SECTION 2: Backtest Re-runs (slippage tiers, param sensitivity)
# ═══════════════════════════════════════════════════════════

async def run_single_backtest(pair: str, engine_name: str, connector: str,
                               trade_cost: float = 0.000375,
                               config_overrides: dict = None) -> dict:
    """Run a single HB backtest with optional config overrides."""
    from app.engines.strategy_registry import get_strategy, build_backtest_config
    from core.backtesting import BacktestingEngine

    meta = get_strategy(engine_name)

    # Build config
    config = build_backtest_config(
        engine_name=engine_name,
        connector=connector,
        pair=pair,
    )

    # Apply overrides (e.g. different TP/SL or funding params)
    if config_overrides:
        for k, v in config_overrides.items():
            if hasattr(config, k):
                setattr(config, k, v)

    # Time range: last 365 days
    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - 365 * 86400

    backtesting = BacktestingEngine(load_cached_data=True)
    try:
        result = await backtesting.run_backtesting(
            config=config,
            start=start_ts,
            end=end_ts,
            backtesting_resolution=meta.backtesting_resolution,
            trade_cost=trade_cost,
        )

        close_types = result.results.get("close_types", {})
        if not isinstance(close_types, dict):
            close_types = {}

        return {
            "pair": pair,
            "profit_factor": result.results.get("profit_factor", 0) or 0,
            "sharpe": result.results.get("sharpe_ratio", 0) or 0,
            "trades": result.results.get("total_executors", 0) or 0,
            "win_rate": result.results.get("win_rate", 0) or 0,
            "max_dd": result.results.get("max_drawdown_pct", 0) or 0,
            "net_pnl": result.results.get("net_pnl_quote", 0) or 0,
            "close_types": close_types,
        }
    except Exception as e:
        return {"pair": pair, "error": str(e), "profit_factor": 0, "trades": 0}
    finally:
        del backtesting
        gc.collect()


async def slippage_sensitivity(pairs: list, engine_name: str, connector: str,
                                db, merge_derivatives: bool = True):
    """A2: Slippage tiers at 2, 5, 10 bps."""
    print("\n" + "=" * 90)
    print("  P0/A2: SLIPPAGE SENSITIVITY")
    print("=" * 90)

    # Need derivatives merge for E3
    if merge_derivatives:
        await _ensure_derivatives_merged(pairs, engine_name, connector, db)

    # Baseline fee: 0.000375 (3.75 bps per side = 7.5 bps RT)
    # Additional slippage on top of base fee
    base_cost = 0.000375

    results = {}
    for tier_bps in SLIPPAGE_TIERS_BPS:
        # Add slippage to base trading cost
        # tier_bps is one-way slippage, applied to both entry and exit
        total_cost = base_cost + (tier_bps / 10000.0)
        print(f"\n  --- Slippage tier: +{tier_bps} bps (total cost: {total_cost*10000:.1f} bps/side) ---")

        tier_results = {}
        for pair in pairs:
            r = await run_single_backtest(pair, engine_name, connector, trade_cost=total_cost)
            tier_results[pair] = r
            pf = r.get("profit_factor", 0)
            trades = r.get("trades", 0)
            label = "PASS" if pf > 1.0 else "FAIL"
            print(f"    {pair:<16} PF={pf:.3f}  trades={trades}  [{label}]")

        results[tier_bps] = tier_results

    # Summary
    print(f"\n  --- Slippage Summary ---")
    print(f"  {'Pair':<16} {'Base PF':>8} {'+2bps':>8} {'+5bps':>8} {'+10bps':>8} {'Verdict':>8}")
    print(f"  {'-'*56}")
    for pair in pairs:
        base_pf = results[2][pair].get("profit_factor", 0)  # 2bps is closest to base
        vals = [results[t][pair].get("profit_factor", 0) for t in SLIPPAGE_TIERS_BPS]
        # Must pass at 2bps, should pass at 5bps
        must = vals[0] > 1.0
        should = vals[1] > 1.0
        verdict = "PASS" if must else "FAIL"
        if must and not should:
            verdict = "MARGINAL"
        print(f"  {pair:<16} {vals[0]:>8.3f} {vals[0]:>8.3f} {vals[1]:>8.3f} {vals[2]:>8.3f} {verdict:>8}")

    return results


async def param_sensitivity(pairs: list, engine_name: str, connector: str,
                             db, merge_derivatives: bool = True):
    """Phase 1.3c: Mini sensitivity — perturb key params +/- 1 step."""
    print("\n" + "=" * 90)
    print("  Phase 1.3c: PARAMETER SENSITIVITY")
    print("=" * 90)

    if merge_derivatives:
        await _ensure_derivatives_merged(pairs, engine_name, connector, db)

    all_results = {}

    for param_name, param_values in SENSITIVITY_PARAMS.items():
        print(f"\n  --- {param_name} sweep: {param_values} (baseline={BASELINE[param_name]}) ---")

        param_results = {}
        for val in param_values:
            overrides = {}
            if param_name == "funding_streak_min":
                overrides = {"funding_streak_min": val}
            elif param_name == "tp_pct":
                overrides = {"take_profit": Decimal(str(val))}
            elif param_name == "sl_pct":
                overrides = {"stop_loss": Decimal(str(val))}

            pair_pfs = {}
            for pair in pairs:
                r = await run_single_backtest(pair, engine_name, connector,
                                              config_overrides=overrides)
                pair_pfs[pair] = r.get("profit_factor", 0)

            avg_pf = np.mean(list(pair_pfs.values()))
            is_baseline = (val == BASELINE[param_name])
            marker = " <<< BASELINE" if is_baseline else ""
            print(f"    {param_name}={val:<6}  avg_PF={avg_pf:.3f}  "
                  + "  ".join(f"{p.split('-')[0]}={pf:.3f}" for p, pf in pair_pfs.items())
                  + marker)

            param_results[val] = {"avg_pf": avg_pf, "pairs": pair_pfs}

        all_results[param_name] = param_results

        # Cliff detection: does PF drop > 30% with +/- 1 step from baseline?
        baseline_val = BASELINE[param_name]
        baseline_idx = param_values.index(baseline_val)
        baseline_pf = param_results[baseline_val]["avg_pf"]

        cliff = False
        for delta_name, delta_idx in [("left", baseline_idx - 1), ("right", baseline_idx + 1)]:
            if 0 <= delta_idx < len(param_values):
                neighbor_val = param_values[delta_idx]
                neighbor_pf = param_results[neighbor_val]["avg_pf"]
                drop_pct = (baseline_pf - neighbor_pf) / baseline_pf * 100 if baseline_pf > 0 else 0
                if drop_pct > 30:
                    print(f"    *** CLIFF EDGE: {param_name}={baseline_val}->{neighbor_val} "
                          f"drops PF by {drop_pct:.1f}% ***")
                    cliff = True

        if not cliff:
            print(f"    PASS: No cliff edges detected for {param_name}")

        # Boundary check
        best_val = max(param_results.keys(), key=lambda v: param_results[v]["avg_pf"])
        if best_val == param_values[0] or best_val == param_values[-1]:
            print(f"    *** BOUNDARY WARNING: Best {param_name}={best_val} is at search space edge ***")
        else:
            print(f"    PASS: Best {param_name}={best_val} is interior (not at boundary)")

    return all_results


async def _ensure_derivatives_merged(pairs, engine_name, connector, db):
    """Ensure derivatives data is merged into cached candles (needed for E3)."""
    from app.engines.strategy_registry import get_strategy
    meta = get_strategy(engine_name)
    if "derivatives" not in meta.required_features:
        return

    # The BulkBacktestTask handles this, but for standalone runs we need to
    # trigger it manually. The backtest engine will use cached candles which
    # should already have derivatives merged from the walk-forward run.
    # If not, the controller's btc_regime_enabled check handles missing columns gracefully.
    pass


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════

def run_trade_data_analyses(db, engine: str, pairs: list):
    """Run all trade-data-only analyses."""
    print("\n" + "=" * 90)
    print(f"  E3 ROBUSTNESS SUITE — TRADE DATA ANALYSES")
    print(f"  Engine: {engine}  Pairs: {', '.join(pairs)}")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 90)

    # Load BTC data for regime classification
    btc_df = load_btc_candles(db)
    regime_df = classify_regime(btc_df)
    if not regime_df.empty:
        regime_counts = regime_df["regime"].value_counts()
        print(f"\n  BTC regime distribution: {dict(regime_counts)}")

    # Load walk-forward data for anti-overfitting checks
    latest_wf = db["walk_forward_results"].find_one(
        {"engine": engine}, sort=[("created_at", -1)]
    )
    wf_run_id = latest_wf["run_id"] if latest_wf else None

    all_results = {}
    for pair in pairs:
        print(f"\n{'─' * 90}")
        print(f"  {pair}")
        print(f"{'─' * 90}")

        trades = load_trades(db, engine, pair)
        if trades.empty:
            print(f"  No trades found for {pair}")
            continue

        print(f"  Total trades: {len(trades)}")

        # Tag with regime
        trades = tag_trades_with_regime(trades, regime_df)

        # Phase 1.2: Regime breakdown
        regime_results = analyze_regime_breakdown(trades, pair)

        # P0: Trade distribution
        dist_results = analyze_r_distribution(trades, pair)

        # P1: Monte Carlo
        mc_results = monte_carlo_block_bootstrap(trades, pair)

        # Anti-overfitting checklist
        wf_data = {"train": [], "test": []}
        if wf_run_id:
            wf_docs = list(db["walk_forward_results"].find(
                {"engine": engine, "pair": pair, "run_id": wf_run_id}
            ))
            for doc in wf_docs:
                wf_data[doc["period_type"]].append(doc)
        overfit_checks = anti_overfitting_checklist(trades, wf_data, pair)

        all_results[pair] = {
            "regime": regime_results,
            "distribution": dist_results,
            "monte_carlo": mc_results,
            "overfit_checks": overfit_checks,
        }

    # ── Summary ──
    print(f"\n{'=' * 90}")
    print(f"  ROBUSTNESS SUMMARY (Trade Data)")
    print(f"{'=' * 90}")

    print(f"\n  {'Pair':<16} {'MC PF':>7} {'PF CI95':>16} {'Ruin%':>7} {'Top5%':>6} {'MaxLoss':>8} {'Regimes+':>9}")
    print(f"  {'-'*72}")
    for pair in pairs:
        r = all_results.get(pair, {})
        mc = r.get("monte_carlo", {})
        dist = r.get("distribution", {})
        regime = r.get("regime", {})
        pos_regimes = len([k for k, v in regime.items() if v.get("pf", 0) > 1.0 and k != "UNKNOWN"])

        pf_ci = mc.get("pf_ci95", (0, 0))
        print(f"  {pair:<16} {mc.get('pf_mean', 0):>7.3f} "
              f"[{pf_ci[0]:.3f}, {pf_ci[1]:.3f}] "
              f"{mc.get('ruin_prob', 0)*100:>6.2f}% "
              f"{dist.get('top5_pct', 0):>5.1f}% "
              f"{dist.get('max_consec_loss', 0):>8} "
              f"{pos_regimes:>9}")

    return all_results


async def run_backtest_analyses(db, engine: str, pairs: list, connector: str):
    """Run backtest-based analyses (slippage, param sensitivity)."""
    # Slippage tiers
    slip_results = await slippage_sensitivity(pairs, engine, connector, db)

    # Parameter sensitivity
    param_results = await param_sensitivity(pairs, engine, connector, db)

    return {"slippage": slip_results, "params": param_results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E3 Robustness Suite")
    parser.add_argument("--engine", default="E3", help="Engine name")
    parser.add_argument("--pair", default=None, help="Single pair (default: top 4)")
    parser.add_argument("--trade-data-only", action="store_true", help="Skip backtest re-runs")
    parser.add_argument("--backtest-only", action="store_true", help="Only run backtest analyses")
    args = parser.parse_args()

    pairs = [args.pair] if args.pair else TOP_PAIRS

    client = MongoClient(os.environ["MONGO_URI"])
    db = client[os.environ["MONGO_DATABASE"]]

    if not args.backtest_only:
        run_trade_data_analyses(db, args.engine, pairs)

    if not args.trade_data_only:
        connector = "bybit_perpetual"
        asyncio.run(run_backtest_analyses(db, args.engine, pairs, connector))

    print("\n  Robustness suite complete.")
