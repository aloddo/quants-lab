#!/usr/bin/env python3
"""
copyability_3split_runner.py -- V13 8-fold OOS walk-forward orchestrator with
PER-FOLD CAUSAL ELIGIBILITY + capital-scale sweep + LCB-of-median ranking.

Per Alberto-locked spec 2026-05-26 + codex r15/r17/r18 reviews.

PIPELINE (per fold N in 0..7):
  Window for fold N:
    train_start = WINDOW_START + N * 15d
    train_end   = train_start + 30d - 1
    val_start   = train_end + 1
    val_end     = train_end + 15d
    test_start  = val_end + 1
    test_end    = test_start + 15d - 1

  1. PRE-SCREEN window = [WINDOW_START, val_end].
     Compute wallet activity in this window only (no test-window data).
     Apply causal eligibility gates (codex r15):
       days_active >= 0.30 * W
       lifetime_fills >= 10 * W
       peak_fills_per_day < 5000  (HFT exclusion)
       median_fills_per_active_day <= 100
       distinct_coins >= 3
       last_active_day >= train_start - 15d  (recently active)

  2. For each eligible wallet, run copy_fill_replay on TEST window only.

  3. Per (wallet, capital_scale, latency, proxy) accumulate fold-level
     copy_net_return_pct_equity_shrunk_mean + n_our_journeys + score.

ACROSS FOLDS:
  Persistence intersect per (capital_scale, latency, proxy):
    Wallet passes fold N if copy_net_return_pct_equity_shrunk_mean > 0
       AND n_our_journeys >= --min-trades-per-fold (default 10).
    Wallet survives if pass_count >= --min-pass-folds (default 5 of 8).

  Conservative survivor ranking (codex r15 final):
    Primary: LCB(median_fold_copy_net_return_pct_equity) at 95% one-sided.
    Tie-breakers: positive in >=6/8 folds, max DD, lower turnover.

INPUTS:
  --fills-dir            S3 fills daily parquets (default app/data/hl_s3_fills)
  --journeys             wallet_journeys_costed.parquet from step B (for fallback pct)
  --equity-series        wallet_equity_series.parquet from step A (optional, but recommended)
  --window-start, --window-end  walk-forward window (default 2025-12-01 to 2026-05-26)
  --wallets              optional wallet allowlist (else use distribution)
  --latencies, --capital-scales (sweep dimensions)
  --min-pass-folds       default 5 (of 8)
  --min-trades-per-fold  default 10
  --out-dir              output directory

OUTPUTS:
  per_fold_F{N}_wallets.parquet           -- raw replay output per fold
  pass_matrix.parquet                     -- fold pass/fail matrix per (wallet,cap,lat,proxy)
  candidates_persistent.parquet           -- final survivor ranking
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [3split] %(levelname)s: %(message)s",
)
log = logging.getLogger("3split")

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR_DEFAULT = ROOT / "app" / "data" / "hl_s3_fills"

# V13 8-fold spec (per Alberto msg 7204):
#   train: 30 days, val: 15 days, test: 15 days, step: 15 days
FOLD_TRAIN_DAYS = 30
FOLD_VAL_DAYS = 15
FOLD_TEST_DAYS = 15
FOLD_STEP_DAYS = 15

# Per-fold causal eligibility gates (codex r15 spec)
DAYS_ACTIVE_PCT_OF_W = 0.30        # at least 30% of pre-screen window days active
FILLS_PER_W_DAY = 10.0              # at least 10 fills/day on average
PEAK_FILLS_PER_DAY_MAX = 5000.0     # HFT exclusion primary
MEDIAN_FILLS_PER_DAY_MAX = 100.0    # consistent churn limit
DISTINCT_COINS_MIN = 3              # no single-market lottery
RECENT_ACTIVITY_DAYS = 15           # last_active >= train_start - 15d

# Persistence + ranking defaults
MIN_PASS_FOLDS_DEFAULT = 5          # of 8
MIN_TRADES_PER_FOLD_DEFAULT = 10


def make_folds(window_start: _dt.date, window_end: _dt.date) -> list[dict]:
    """Enumerate 8-fold V13 spec across the window.

    Each fold returns dict with all date boundaries + window_W (pre-screen days).
    """
    folds = []
    cur = window_start
    while True:
        train_start = cur
        train_end = train_start + _dt.timedelta(days=FOLD_TRAIN_DAYS - 1)
        val_start = train_end + _dt.timedelta(days=1)
        val_end = train_end + _dt.timedelta(days=FOLD_VAL_DAYS)
        test_start = val_end + _dt.timedelta(days=1)
        test_end = test_start + _dt.timedelta(days=FOLD_TEST_DAYS - 1)
        if test_end > window_end:
            # Truncate last fold if at least 7 test days available
            if test_start + _dt.timedelta(days=6) <= window_end:
                test_end = window_end
                fold = {
                    "name": f"F{len(folds)}",
                    "train_start": train_start,
                    "train_end": train_end,
                    "val_start": val_start,
                    "val_end": val_end,
                    "test_start": test_start,
                    "test_end": test_end,
                    "window_W_days": (val_end - window_start).days + 1,
                }
                folds.append(fold)
            break
        folds.append({
            "name": f"F{len(folds)}",
            "train_start": train_start,
            "train_end": train_end,
            "val_start": val_start,
            "val_end": val_end,
            "test_start": test_start,
            "test_end": test_end,
            "window_W_days": (val_end - window_start).days + 1,
        })
        cur = cur + _dt.timedelta(days=FOLD_STEP_DAYS)
    return folds


def compute_prescreen_stats(
    fills_dir: Path,
    window_start: _dt.date,
    val_end: _dt.date,
    wallets_filter: set[str] | None = None,
) -> pd.DataFrame:
    """Compute per-wallet activity stats in [window_start, val_end] only.

    Returns DataFrame with columns:
        wallet, lifetime_fills, n_active_days, fills_per_active_day,
        fills_per_day_peak, median_fills_per_day, distinct_coins,
        last_active_date
    """
    cols = ["wallet", "coin", "time"]
    fills_count: dict[str, int] = defaultdict(int)
    active_days: dict[str, set] = defaultdict(set)
    per_day_fills_peak: dict[str, int] = defaultdict(int)
    distinct_coins: dict[str, set] = defaultdict(set)
    last_active: dict[str, _dt.date] = {}
    per_day_fills_log: dict[str, list[int]] = defaultdict(list)  # for median

    import gc
    cur = window_start
    while cur <= val_end:
        p = fills_dir / f"{cur.strftime('%Y%m%d')}.parquet"
        if not p.exists():
            cur += _dt.timedelta(days=1)
            continue
        wallets_lc = [w.lower() for w in wallets_filter] if wallets_filter else None
        filt = [("wallet", "in", wallets_lc)] if wallets_lc else None
        try:
            df = pd.read_parquet(p, columns=cols, filters=filt)
        except Exception as e:
            log.warning(f"prescreen skipped {p.name}: {e}")
            cur += _dt.timedelta(days=1)
            continue
        if not df.empty:
            df["wallet"] = df["wallet"].str.lower()
            day_counts = df.groupby("wallet").size()
            for wallet, count in day_counts.items():
                fills_count[wallet] += int(count)
                active_days[wallet].add(cur.isoformat())
                if count > per_day_fills_peak[wallet]:
                    per_day_fills_peak[wallet] = int(count)
                per_day_fills_log[wallet].append(int(count))
                if wallet not in last_active or cur > last_active[wallet]:
                    last_active[wallet] = cur
            coin_counts = df.groupby(["wallet", "coin"]).size().reset_index(name="n")
            for r in coin_counts.itertuples(index=False):
                distinct_coins[r.wallet].add(r.coin)
            del df
        gc.collect()
        cur += _dt.timedelta(days=1)

    rows = []
    for w in fills_count:
        n_days = len(active_days[w])
        per_day = per_day_fills_log[w]
        median_per_day = float(np.median(per_day)) if per_day else 0.0
        rows.append({
            "wallet": w,
            "lifetime_fills": fills_count[w],
            "n_active_days": n_days,
            "fills_per_active_day": fills_count[w] / max(n_days, 1),
            "fills_per_day_peak": per_day_fills_peak[w],
            "median_fills_per_day": median_per_day,
            "distinct_coins": len(distinct_coins[w]),
            "last_active_date": last_active[w].isoformat(),
        })

    # Codex r19 P1 fix: return empty DataFrame with schema to avoid KeyError downstream
    cols = ["wallet", "lifetime_fills", "n_active_days", "fills_per_active_day",
            "fills_per_day_peak", "median_fills_per_day", "distinct_coins",
            "last_active_date"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)


def apply_eligibility_gates(
    stats: pd.DataFrame,
    fold: dict,
) -> tuple[pd.DataFrame, dict]:
    """Apply per-fold causal eligibility gates. Returns (eligible_df, gate_stats)."""
    W = fold["window_W_days"]
    train_start = fold["train_start"]
    days_active_min = DAYS_ACTIVE_PCT_OF_W * W
    fills_min = FILLS_PER_W_DAY * W
    recent_cutoff = (train_start - _dt.timedelta(days=RECENT_ACTIVITY_DAYS)).isoformat()

    cond_days = stats["n_active_days"] >= days_active_min
    cond_fills = stats["lifetime_fills"] >= fills_min
    cond_peak = stats["fills_per_day_peak"] < PEAK_FILLS_PER_DAY_MAX
    cond_median = stats["median_fills_per_day"] <= MEDIAN_FILLS_PER_DAY_MAX
    cond_coins = stats["distinct_coins"] >= DISTINCT_COINS_MIN
    cond_recent = stats["last_active_date"] >= recent_cutoff

    all_cond = cond_days & cond_fills & cond_peak & cond_median & cond_coins & cond_recent
    eligible = stats[all_cond].copy()

    gate_stats = {
        "n_total": len(stats),
        "pass_days_active": int(cond_days.sum()),
        "pass_lifetime_fills": int(cond_fills.sum()),
        "pass_peak_fills": int(cond_peak.sum()),
        "pass_median_fills": int(cond_median.sum()),
        "pass_distinct_coins": int(cond_coins.sum()),
        "pass_recent_activity": int(cond_recent.sum()),
        "pass_all": int(all_cond.sum()),
        "threshold_days_active_min": days_active_min,
        "threshold_lifetime_fills_min": fills_min,
        "threshold_peak_max": PEAK_FILLS_PER_DAY_MAX,
        "threshold_recent_cutoff": recent_cutoff,
    }
    return eligible, gate_stats


def run_fill_replay_for_fold(
    fold: dict,
    eligible_wallets: list[str],
    journeys_path: Path,
    equity_path: Path | None,
    out_dir: Path,
    latencies: str,
    capital_scales: str,
    fee_rt_bps: float,
    max_copy_leverage: float,
    min_order_notional: float,
) -> tuple[Path | None, Path | None]:
    """Subprocess copy_fill_replay.py for one fold on test window only.

    Returns (journeys_parquet_path, wallets_parquet_path) or (None, None) on failure.
    """
    wallets_file = out_dir / f"{fold['name']}_wallets.txt"
    wallets_file.write_text("\n".join(eligible_wallets) + "\n")

    out_prefix = out_dir / fold["name"]
    cmd = [
        "/Users/hermes/miniforge3/envs/quants-lab/bin/python",
        str(ROOT / "scripts" / "copy_fill_replay.py"),
        "--journeys", str(journeys_path),
        "--wallets", str(wallets_file),
        "--train-start", fold["train_start"].isoformat(),
        "--train-end", fold["train_end"].isoformat(),
        "--test-start", fold["test_start"].isoformat(),
        "--test-end", fold["test_end"].isoformat(),
        "--out-prefix", str(out_prefix),
        "--latencies", latencies,
        "--capital-scales", capital_scales,
        "--fee-rt-bps", str(fee_rt_bps),
        "--max-copy-leverage", str(max_copy_leverage),
        "--min-order-notional", str(min_order_notional),
    ]
    if equity_path:
        cmd.extend(["--equity-series", str(equity_path)])

    log.info("running fill_replay for %s on %d wallets", fold["name"], len(eligible_wallets))
    log.info("cmd: %s", " ".join(cmd))
    res = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if res.returncode != 0:
        log.error("fill_replay failed %s: %s", fold["name"], res.stderr[-2000:])
        return None, None
    j_path = Path(f"{out_prefix}_journeys_test.parquet")
    w_path = Path(f"{out_prefix}_wallets_test.parquet")
    return j_path if j_path.exists() else None, w_path if w_path.exists() else None


def build_pass_matrix(
    folds: list[dict],
    fold_outputs: dict,
    min_trades: int,
) -> pd.DataFrame:
    """Build per (wallet, capital_scale, latency, proxy) pass-matrix across all folds.

    Codex r19 #5 fix: PRIMARY fold metric is `fold_return_on_capital = copy_net_pnl_usd_sum / capital_scale`
    (portfolio return per fold), NOT the per-journey shrunk mean.

    Pass condition for fold N:
        n_our_journeys >= min_trades AND
        fold_return_on_capital > 0
    """
    pieces = []
    for fold in folds:
        name = fold["name"]
        w_path = fold_outputs.get(name)
        if w_path is None or not Path(w_path).exists():
            log.warning("fold %s missing output; skipping in pass matrix", name)
            continue
        df = pd.read_parquet(w_path)
        if df.empty:
            continue
        df = df[["wallet", "capital_scale", "latency_seconds", "proxy",
                 "n_our_journeys", "copy_net_return_pct_equity_shrunk_mean",
                 "copy_net_return_pct_equity_median",
                 "copy_net_pnl_usd_sum", "copy_score", "jpd_valid",
                 "sizing_scope_dominant"]].copy()
        df["fold"] = name
        # Codex r19 #5: PRIMARY fold metric = sum PnL / capital scale (portfolio return)
        df["fold_return_on_capital"] = df["copy_net_pnl_usd_sum"] / df["capital_scale"]
        df["fold_pass"] = (
            (df["n_our_journeys"] >= min_trades) &
            (df["fold_return_on_capital"] > 0)
        ).astype(int)
        pieces.append(df)

    if not pieces:
        return pd.DataFrame()
    return pd.concat(pieces, ignore_index=True)


def aggregate_persistence(
    pass_matrix: pd.DataFrame,
    n_folds: int,
    min_pass_folds: int,
) -> pd.DataFrame:
    """Aggregate per (wallet, capital_scale, latency, proxy) across folds.

    Codex r19 #5 fix: PRIMARY metric is `fold_return_on_capital`
    (= copy_net_pnl_usd_sum / capital_scale, the portfolio return per fold).
    NOT the per-journey shrunk mean.

    Codex r19 #3 LCB: 10K bootstrap of median is the dominant cost; keep that.
    Also report deterministic nonparametric LCB (second-smallest of N) per
    codex r19 #3 as auditable check.

    Outputs:
        pass_count, pass_frac, eligible_count
        median_fold_return (PRIMARY)
        lcb_median_return (bootstrap 10K)
        lcb_median_nonparam (second-smallest of N)
        worst_fold_return, mean_fold_return
        survives = pass_count >= min_pass_folds
    """
    if pass_matrix.empty:
        return pd.DataFrame()

    out_rows = []
    grouped = pass_matrix.groupby(
        ["wallet", "capital_scale", "latency_seconds", "proxy"], sort=False
    )
    for (wallet, cap, lat, proxy), sub in grouped:
        if sub.empty:
            continue
        # PRIMARY: fold_return_on_capital (= sum PnL / capital)
        ret_arr = sub["fold_return_on_capital"].to_numpy(dtype=np.float64)
        ret_arr = ret_arr[~np.isnan(ret_arr)]
        if len(ret_arr) == 0:
            continue
        n_folds_observed = len(sub)
        pass_count = int(sub["fold_pass"].sum())
        median_ret = float(np.median(ret_arr))
        worst_fold = float(np.min(ret_arr))
        mean_ret = float(np.mean(ret_arr))

        # LCB on median: bootstrap (10K resamples per codex r19 #3) lower 5th percentile of medians
        if len(ret_arr) >= 3:
            rng = np.random.default_rng(42)
            bs = np.array([
                np.median(rng.choice(ret_arr, size=len(ret_arr), replace=True))
                for _ in range(10000)
            ])
            lcb_median_bootstrap = float(np.percentile(bs, 5))
        else:
            lcb_median_bootstrap = median_ret

        # Codex r19 #3 deterministic nonparametric LCB: second-smallest of N
        # gives ~96.5% one-sided coverage for N=8. For smaller N, use the smallest.
        if len(ret_arr) >= 4:
            sorted_arr = np.sort(ret_arr)
            lcb_median_nonparam = float(sorted_arr[1])  # second-smallest
        else:
            lcb_median_nonparam = float(np.min(ret_arr)) if len(ret_arr) else float("nan")

        out_rows.append({
            "wallet": wallet,
            "capital_scale": cap,
            "latency_seconds": lat,
            "proxy": proxy,
            "n_folds_observed": n_folds_observed,
            "pass_count": pass_count,
            "pass_frac": pass_count / max(n_folds_observed, 1),
            "median_fold_return": median_ret,
            "lcb_median_return": lcb_median_bootstrap,
            "lcb_median_nonparam": lcb_median_nonparam,
            "worst_fold_return": worst_fold,
            "mean_fold_return": mean_ret,
            "survives": pass_count >= min_pass_folds,
        })

    return pd.DataFrame(out_rows)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--fills-dir", default=str(FILLS_DIR_DEFAULT))
    p.add_argument("--journeys", required=True, help="wallet_journeys_costed.parquet")
    p.add_argument("--equity-series", default=None, help="wallet_equity_series.parquet (optional)")
    p.add_argument("--window-start", default="2025-12-01")
    p.add_argument("--window-end", default="2026-05-26")
    p.add_argument("--wallets", default=None, help="optional wallet allowlist file")
    p.add_argument("--latencies", default="120,300")
    p.add_argument("--capital-scales", default="500,1000,5000,10000,50000")
    p.add_argument("--fee-rt-bps", type=float, default=8.64)
    p.add_argument("--max-copy-leverage", type=float, default=10.0)
    p.add_argument("--min-order-notional", type=float, default=10.0)
    p.add_argument("--min-pass-folds", type=int, default=MIN_PASS_FOLDS_DEFAULT)
    p.add_argument("--min-trades-per-fold", type=int, default=MIN_TRADES_PER_FOLD_DEFAULT)
    p.add_argument("--primary-capital-scale", type=float, default=1000.0)
    p.add_argument("--primary-latency", type=int, default=120)
    p.add_argument("--out-dir", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    window_start = _dt.date.fromisoformat(args.window_start)
    window_end = _dt.date.fromisoformat(args.window_end)
    fills_dir = Path(args.fills_dir)
    journeys_path = Path(args.journeys)
    equity_path = Path(args.equity_series) if args.equity_series else None

    folds = make_folds(window_start, window_end)
    log.info("generated %d folds covering %s -> %s", len(folds), window_start, window_end)
    for fold in folds:
        log.info("  %s: train %s..%s val %s..%s test %s..%s (W=%d days)",
                 fold["name"],
                 fold["train_start"], fold["train_end"],
                 fold["val_start"], fold["val_end"],
                 fold["test_start"], fold["test_end"],
                 fold["window_W_days"])

    # Load optional wallet allowlist
    wallets_filter: set[str] | None = None
    if args.wallets:
        with open(args.wallets) as f:
            wallets_filter = {w.strip().lower() for w in f if w.strip()}
        log.info("wallet allowlist: %d wallets", len(wallets_filter))

    # Per-fold causal eligibility + replay
    fold_outputs = {}
    eligibility_log = []
    for fold in folds:
        log.info("=== FOLD %s ===", fold["name"])
        stats = compute_prescreen_stats(
            fills_dir, window_start, fold["val_end"], wallets_filter,
        )
        log.info("prescreen stats for %s: %d wallets", fold["name"], len(stats))
        eligible, gate_stats = apply_eligibility_gates(stats, fold)
        log.info("eligibility gates for %s: pass_all=%d (days=%d fills=%d peak=%d median=%d coins=%d recent=%d)",
                 fold["name"], gate_stats["pass_all"],
                 gate_stats["pass_days_active"], gate_stats["pass_lifetime_fills"],
                 gate_stats["pass_peak_fills"], gate_stats["pass_median_fills"],
                 gate_stats["pass_distinct_coins"], gate_stats["pass_recent_activity"])
        eligibility_log.append({**{"fold": fold["name"]}, **gate_stats})

        if eligible.empty:
            log.warning("fold %s has zero eligible wallets; skipping replay", fold["name"])
            fold_outputs[fold["name"]] = None
            continue

        eligible_wallets = sorted(eligible["wallet"].tolist())

        j_path, w_path = run_fill_replay_for_fold(
            fold, eligible_wallets,
            journeys_path, equity_path, out_dir,
            args.latencies, args.capital_scales,
            args.fee_rt_bps, args.max_copy_leverage, args.min_order_notional,
        )
        fold_outputs[fold["name"]] = w_path
        log.info("fold %s output: %s", fold["name"], w_path)

    # Save eligibility log
    pd.DataFrame(eligibility_log).to_parquet(out_dir / "eligibility_log.parquet", index=False)

    # Build pass matrix
    pass_matrix = build_pass_matrix(folds, fold_outputs, args.min_trades_per_fold)
    pass_matrix.to_parquet(out_dir / "pass_matrix.parquet", index=False)
    log.info("pass_matrix: %d rows", len(pass_matrix))

    # Persistence aggregation
    candidates = aggregate_persistence(pass_matrix, len(folds), args.min_pass_folds)
    candidates.to_parquet(out_dir / "candidates_persistent.parquet", index=False)
    log.info("candidates_persistent: %d rows; %d survive (>=%d folds)",
             len(candidates),
             int(candidates["survives"].sum()) if "survives" in candidates.columns else 0,
             args.min_pass_folds)

    # Top 20 survivors at primary (capital, latency, proxy=conservative)
    if not candidates.empty and "survives" in candidates.columns:
        survivors = candidates[
            candidates["survives"] &
            (candidates["capital_scale"] == args.primary_capital_scale) &
            (candidates["latency_seconds"] == args.primary_latency) &
            (candidates["proxy"] == "conservative")
        ].copy()
        survivors = survivors.sort_values("lcb_median_return", ascending=False)
        log.info("=== TOP 20 SURVIVORS at primary cap=$%.0f lat=%ds conservative ===",
                 args.primary_capital_scale, args.primary_latency)
        cols_show = ["wallet", "pass_count", "median_fold_return", "lcb_median_return",
                     "worst_fold_return", "mean_fold_return"]
        cols_show = [c for c in cols_show if c in survivors.columns]
        print(survivors.head(20)[cols_show].to_string())


if __name__ == "__main__":
    main()
