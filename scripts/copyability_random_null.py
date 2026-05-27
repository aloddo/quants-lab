#!/usr/bin/env python3
"""
copyability_random_null.py -- Step E random portfolio null test.

Per Alberto-locked spec 2026-05-26 + codex r9 #3 + r19 separation-from-D.

For each (capital_scale, latency, proxy) AND each fold:
  1. Load per-wallet copy_net_pnl_usd_sum from step D fold outputs
  2. Identify the survivor pool (from step D candidates_persistent.parquet)
  3. Generate 1000 random K-wallet portfolios where K = |survivors|
  4. Each random portfolio = equal-weighted sum of K random eligible wallets' fold PnL
  5. Compute random portfolio fold return distribution (95th percentile, mean, std)
  6. Compare survivor pool's fold return vs random 95th percentile per fold
  7. Verdict: survivor pool BEATS random p95 in N of 8 folds

This is the empirical FDR gate per codex r9 #3.

Inputs:
  --step-d-dir   directory containing per_fold F{N}_wallets_test.parquet + candidates_persistent.parquet + pass_matrix.parquet
  --capital-scale, --latency, --proxy (which slice to test; default primary 1000/120/conservative)
  --n-trials     default 1000

Outputs:
  random_null_{cap}_{lat}_{proxy}.parquet  -- per-fold random distribution stats + survivor-vs-random comparison

Memory: bounded; this script only does N_trials sampling on dataframes. Memory guards installed anyway for safety.
"""
from __future__ import annotations

import argparse
import logging
import os
import resource
import signal
import sys
import threading
import time as _time
from pathlib import Path

import numpy as np
import pandas as pd
import psutil


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [random_null] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def install_memory_guards(rlimit_data_gb: float = 4.0, rss_abort_gb: float = 3.0) -> None:
    """Install OOM prevention guards. Call at start of main()."""
    try:
        cap_bytes = int(rlimit_data_gb * 1024 ** 3)
        resource.setrlimit(resource.RLIMIT_DATA, (cap_bytes, cap_bytes))
    except (ValueError, OSError):
        pass
    pid = os.getpid()
    abort_bytes = int(rss_abort_gb * 1024 ** 3)
    def monitor():
        proc = psutil.Process(pid)
        while True:
            try:
                if proc.memory_info().rss > abort_bytes:
                    os.kill(pid, signal.SIGTERM)
                    return
                _time.sleep(10)
            except psutil.NoSuchProcess:
                return
            except Exception:
                pass
    threading.Thread(target=monitor, daemon=True, name="rss_monitor").start()


def compute_random_portfolio_returns(
    eligible_pool_pnl: np.ndarray,  # per-wallet copy_net_pnl_usd_sum for fold
    K: int,                          # size of survivor pool
    capital_scale: float,
    n_trials: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Generate n_trials random K-wallet portfolios from eligible pool.

    Each portfolio:
      portfolio_pnl = sum of K randomly-chosen wallets' fold PnL
      portfolio_return = portfolio_pnl / (K * capital_scale)

    Returns array of n_trials portfolio returns.
    """
    if len(eligible_pool_pnl) < K:
        return np.array([])  # not enough wallets for random sample of size K
    if K <= 0:
        return np.array([])

    returns = np.empty(n_trials, dtype=np.float64)
    n_eligible = len(eligible_pool_pnl)
    for i in range(n_trials):
        idx = rng.choice(n_eligible, size=K, replace=False)
        portfolio_pnl = eligible_pool_pnl[idx].sum()
        returns[i] = portfolio_pnl / (K * capital_scale)
    return returns


def compute_survivor_portfolio_return(
    survivor_pnl: np.ndarray,        # per-wallet copy_net_pnl_usd_sum for fold (survivors only)
    capital_scale: float,
) -> float:
    """Survivor portfolio fold return = sum survivor PnL / (K * capital)."""
    K = len(survivor_pnl)
    if K == 0:
        return float("nan")
    return float(survivor_pnl.sum() / (K * capital_scale))


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--step-d-dir", required=True,
                   help="Directory containing per-fold F{N}_wallets_test.parquet + candidates_persistent.parquet")
    p.add_argument("--capital-scale", type=float, default=1000.0)
    p.add_argument("--latency", type=int, default=120)
    p.add_argument("--proxy", default="conservative")
    p.add_argument("--n-trials", type=int, default=1000)
    p.add_argument("--out", required=True,
                   help="Output parquet path for random null distribution + verdict")
    p.add_argument("--rlimit-data-gb", type=float, default=4.0)
    p.add_argument("--rss-abort-gb", type=float, default=3.0)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main():
    args = parse_args()
    install_memory_guards(args.rlimit_data_gb, args.rss_abort_gb)

    step_d_dir = Path(args.step_d_dir)
    if not step_d_dir.exists():
        log.error(f"step-d-dir does not exist: {step_d_dir}")
        sys.exit(1)

    # Load candidates_persistent.parquet to identify SURVIVORS
    cand_path = step_d_dir / "candidates_persistent.parquet"
    if not cand_path.exists():
        log.error(f"candidates_persistent.parquet missing: {cand_path}")
        sys.exit(1)
    candidates = pd.read_parquet(cand_path)
    log.info("loaded candidates: %d rows", len(candidates))

    primary_filter = (
        (candidates["capital_scale"] == args.capital_scale)
        & (candidates["latency_seconds"] == args.latency)
        & (candidates["proxy"] == args.proxy)
    )
    if "survives" not in candidates.columns:
        log.error("candidates parquet missing 'survives' column")
        sys.exit(1)
    survivors_df = candidates[primary_filter & candidates["survives"]]
    survivor_wallets = set(survivors_df["wallet"].tolist())
    K = len(survivor_wallets)
    log.info(
        "survivors at cap=$%.0f lat=%ds proxy=%s: K=%d wallets",
        args.capital_scale, args.latency, args.proxy, K,
    )
    if K == 0:
        log.warning("Zero survivors at this slice; null test trivial")

    # Find all per-fold output files
    fold_files = sorted(step_d_dir.glob("F*_wallets_test.parquet"))
    log.info("found %d fold output files", len(fold_files))
    if not fold_files:
        log.error("no F*_wallets_test.parquet files in %s", step_d_dir)
        sys.exit(1)

    rng = np.random.default_rng(args.seed)

    fold_results = []
    survivor_beat_count = 0
    fold_count_with_data = 0

    for fold_file in fold_files:
        fold_name = fold_file.stem.split("_")[0]  # F0, F1, etc.
        df = pd.read_parquet(fold_file)
        # Filter to (capital_scale, latency, proxy)
        sub = df[
            (df["capital_scale"] == args.capital_scale)
            & (df["latency_seconds"] == args.latency)
            & (df["proxy"] == args.proxy)
        ]
        if sub.empty:
            log.info("fold %s: no rows at slice; skip", fold_name)
            continue

        eligible_pool = sub.copy()
        eligible_pnl = eligible_pool["copy_net_pnl_usd_sum"].to_numpy(dtype=np.float64)
        n_eligible = len(eligible_pnl)
        log.info("fold %s: eligible pool size = %d", fold_name, n_eligible)

        # Survivor pool fold PnL: filter eligible to survivor wallets
        survivor_pnl_arr = eligible_pool[
            eligible_pool["wallet"].isin(survivor_wallets)
        ]["copy_net_pnl_usd_sum"].to_numpy(dtype=np.float64)
        survivor_return = compute_survivor_portfolio_return(survivor_pnl_arr, args.capital_scale)

        # Random portfolio returns (K = survivor pool size; uses eligible pool minus survivor set, OR full eligible per spec)
        # Per codex r9 #3: random draws from FULL eligible pool (not "eligible minus survivors").
        # This is conservative: makes random null harder to beat (the "random" pool can include survivors by chance).
        if K > 0 and n_eligible >= K:
            random_returns = compute_random_portfolio_returns(
                eligible_pnl, K, args.capital_scale, args.n_trials, rng,
            )
            p95 = float(np.percentile(random_returns, 95))
            p50 = float(np.percentile(random_returns, 50))
            p05 = float(np.percentile(random_returns, 5))
            mean_random = float(random_returns.mean())
            std_random = float(random_returns.std())
            beats_p95 = survivor_return > p95
            beats_mean = survivor_return > mean_random
        else:
            p95 = p50 = p05 = mean_random = std_random = float("nan")
            beats_p95 = False
            beats_mean = False

        fold_results.append({
            "fold": fold_name,
            "n_eligible": n_eligible,
            "K_survivors": K,
            "survivor_portfolio_return": survivor_return,
            "random_p95_return": p95,
            "random_p50_return": p50,
            "random_p05_return": p05,
            "random_mean_return": mean_random,
            "random_std_return": std_random,
            "beats_random_p95": beats_p95,
            "beats_random_mean": beats_mean,
        })
        fold_count_with_data += 1
        if beats_p95:
            survivor_beat_count += 1

        log.info(
            "  %s: survivor_ret=%.4f%% random_p95=%.4f%% random_p50=%.4f%% beats_p95=%s",
            fold_name, survivor_return * 100, p95 * 100, p50 * 100, beats_p95,
        )

    # Final verdict
    out_df = pd.DataFrame(fold_results)
    out_df.to_parquet(args.out, index=False)
    log.info("wrote %s: %d fold rows", args.out, len(out_df))

    # Summary
    if fold_count_with_data > 0:
        log.info("=== RANDOM NULL SUMMARY ===")
        log.info("Slice: cap=$%.0f lat=%ds proxy=%s K=%d",
                 args.capital_scale, args.latency, args.proxy, K)
        log.info("Survivor pool beats random p95 in %d of %d folds",
                 survivor_beat_count, fold_count_with_data)
        log.info("Median survivor fold return: %.4f%%",
                 out_df["survivor_portfolio_return"].median() * 100)
        log.info("Median random p95: %.4f%%",
                 out_df["random_p95_return"].median() * 100)
        beat_ratio = survivor_beat_count / fold_count_with_data
        # codex r9 + r15 8-fold spec: pass = beat in >= 5/8 folds (62.5%)
        # For partial-fold smoke: scale the same ratio.
        if beat_ratio >= 0.625:
            log.info("VERDICT: PASS — survivor pool beats random p95 in %d/%d folds (%.0f%%)",
                     survivor_beat_count, fold_count_with_data, 100 * beat_ratio)
        elif beat_ratio >= 0.40:
            log.info("VERDICT: MARGINAL — beats random p95 in %d/%d folds (%.0f%%)",
                     survivor_beat_count, fold_count_with_data, 100 * beat_ratio)
        else:
            log.info("VERDICT: FAIL — survivor pool beats random p95 in only %d/%d folds (%.0f%%)",
                     survivor_beat_count, fold_count_with_data, 100 * beat_ratio)


if __name__ == "__main__":
    main()
