#!/usr/bin/env python3
"""V13 Universe rebuilder: forward-only candidate wallet pool per fold.

Per projects/quant/v13 Section 4.5 + 5.4. The strategy's edge claim depends
on selecting wallets without survivorship bias. The prior
`wallet_copyable_v13.parquet` (2,029 wallets, surviving period 2025-12-01 to
today) was built by filtering retroactively; any wallet that blew up between
2025-12-01 and 2026-05-23 was excluded, biasing every backtest in our favor.

This script discovers the candidate pool FRESH from the S3 fill archive with
forward-only logic:

  Step 1 (this script): full-window pre-screen across all 174 days
      Keep wallets that are NOT one-day gamblers and NOT dust accounts:
          total_fills_in_window     >= 30
          total_active_days_window  >= 3
          any 30-day window         meets the per-fold pre-screen
          self_liquidation_events   == 0 (lifetime in window)

      Self-liquidation counting uses the FIXED semantics confirmed by
      `v13_validate_liquidation_label_semantics.py`:
          dir.startswith("Liquidated") OR
          dir in {"Partial Borrow Liquidation", "Backstop Borrow Liquidation"}
          AND closedPnl < 0

      Output: a wallet list file used by downstream scripts.

  Step 2 (v13_equity_reconstruct + v13_journey_trace, separate runs):
      Run the equity series + journey tracing pipeline on this filtered list.

  Step 3 (v13_walk_forward, per-fold logic): the per-fold gates in Section
      5.4 apply WITHIN each fold's train window using ONLY data through
      train_end. A wallet may be eligible in fold N and not in fold N+1 (or
      vice versa); membership is recomputed per fold. This script's pre-screen
      is a LOOSE upper bound that does not affect per-fold semantics; it
      exists to bound the compute cost of the downstream pipeline.

Output: `<output>/wallet_universe.txt` -- one address per line. Plus
`<output>/wallet_universe_stats.parquet` with per-wallet counts and dates
for diagnostic / report use.

Usage:
    python scripts/v13_universe_rebuild.py \\
        --start 2025-12-01 --end 2026-05-24 \\
        --min-fills 30 --min-active-days 3 \\
        --output app/data/v13/wallet_universe
"""
from __future__ import annotations

import argparse
import glob
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_universe] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_universe"

# Trade fill `dir` values that count as a SELF-liquidation (the wallet's own
# position got liquidated). Counterparty rows (closedPnl >= 0) are filtered
# by the closedPnl < 0 check at the call site.
SELF_LIQ_PREFIXES = ("Liquidated",)
SELF_LIQ_EXACT = {"Partial Borrow Liquidation", "Backstop Borrow Liquidation"}


def _is_self_liq_row(dir_val: str, closed_pnl: float) -> bool:
    """Match the v13_wallet_metrics semantics."""
    if pd.isna(dir_val) or closed_pnl is None or closed_pnl >= 0:
        return False
    s = str(dir_val)
    return s.startswith(SELF_LIQ_PREFIXES) or s in SELF_LIQ_EXACT


def aggregate_day(path: Path) -> pd.DataFrame:
    """Read one day's fills, return per-wallet (date, n_fills, self_liq) summary.

    Reads only the columns we need. The summary is keyed by (wallet, date).
    """
    cols = ["wallet", "time", "dir", "closedPnl"]
    df = pd.read_parquet(path, columns=cols)
    if df.empty:
        return pd.DataFrame()
    df["wallet"] = df["wallet"].str.lower()
    df["date"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.floor("D").dt.date
    # Self-liquidation mask (vectorized; matches v13_wallet_metrics).
    dir_str = df["dir"].astype(str)
    cp = pd.to_numeric(df["closedPnl"], errors="coerce")
    self_liq_mask = (
        (dir_str.str.startswith("Liquidated", na=False)
         | dir_str.isin(SELF_LIQ_EXACT))
        & (cp < 0)
    )
    df["is_self_liq"] = self_liq_mask.astype(int)
    agg = df.groupby(["wallet", "date"], as_index=False).agg(
        n_fills=("time", "count"),
        self_liq=("is_self_liq", "sum"),
    )
    return agg


def _rolling_30d_max(per_day: pd.DataFrame, value_col: str, *, dtype: str = "uint32") -> pd.Series:
    """Compute the per-wallet maximum 30-day rolling sum of `value_col`.

    Generic helper that works for either `n_fills` (counts) or `active_day`
    (binary indicator). Returns Series indexed by wallet.
    """
    if per_day.empty:
        return pd.Series(dtype="int64")
    all_dates = pd.date_range(per_day["date"].min(), per_day["date"].max(), freq="D").date
    pivot = per_day.pivot_table(
        index="wallet", columns="date", values=value_col, aggfunc="sum", fill_value=0,
    ).astype(dtype)
    pivot = pivot.reindex(columns=all_dates, fill_value=0)
    arr = pivot.to_numpy(dtype="int64")
    if arr.shape[1] == 0:
        return pd.Series(0, index=pivot.index, dtype="int64")
    window = min(30, arr.shape[1])
    cs = np.cumsum(arr, axis=1)
    rolled30 = np.zeros_like(arr)
    for end_j in range(arr.shape[1]):
        start_j = max(0, end_j - window + 1)
        rolled30[:, end_j] = cs[:, end_j] - (cs[:, start_j - 1] if start_j > 0 else 0)
    max_rolled = rolled30.max(axis=1)
    return pd.Series(max_rolled, index=pivot.index)


def rolling_max_30d_fills(per_day: pd.DataFrame) -> pd.Series:
    """For each wallet, compute the MAXIMUM 30-day rolling sum of n_fills.

    A wallet that does NOT meet >=30 fills in any 30-day window will fail
    the per-fold pre-screen. This gives us the upper bound check.
    """
    if per_day.empty:
        return pd.Series(dtype="int64")
    wallets = per_day["wallet"].unique()
    logger.info(f"Computing rolling 30d max fills across {len(wallets):,} wallets x {(per_day['date'].max() - per_day['date'].min()).days + 1:,} days")
    return _rolling_30d_max(per_day, "n_fills", dtype="uint32")


def rolling_max_30d_active_days(per_day: pd.DataFrame) -> pd.Series:
    """For each wallet, compute the MAXIMUM 30-day rolling count of DISTINCT
    active days (days with >=1 fill).

    Per-fold gate requires active_days >= 15 within the fold's 30-day train
    window. If a wallet does NOT hit >=15 active days in ANY 30-day window
    over the full archive, it CANNOT pass the per-fold gate in any fold.
    Forward-only safe: this is a lifetime upper-bound check.
    """
    if per_day.empty:
        return pd.Series(dtype="int64")
    # Convert per-day n_fills to a binary active-day indicator (1 if any fill).
    pd_indicator = per_day[["wallet", "date"]].copy()
    pd_indicator["active_day"] = (per_day["n_fills"] > 0).astype("uint8")
    return _rolling_30d_max(pd_indicator, "active_day", dtype="uint8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--min-fills", type=int, default=30,
                    help="Minimum lifetime fills in window (default 30)")
    ap.add_argument("--min-active-days", type=int, default=3,
                    help="Minimum active days in window (default 3)")
    ap.add_argument("--min-30d-window-fills", type=int, default=30,
                    help="A wallet must hit at least this many fills in SOME 30-day rolling window (default 30 matches per-fold gate)")
    ap.add_argument("--min-30d-window-active-days", type=int, default=0,
                    help="A wallet must hit at least this many distinct active days in SOME 30-day rolling window (default 0 = disabled). Set to 15 to match the per-fold gate's lifetime upper-bound.")
    # NOTE: self-liquidation is NOT filtered in the global pre-screen because
    # doing so would use forward-looking information (a wallet self-liquidated
    # in fold 8 would be excluded from fold 1, where the bot had no way to
    # know that). The per-fold gate in v13_wallet_metrics.py:eligible enforces
    # `liquidation_events == 0` WITHIN that fold's train window only --
    # forward-only.
    ap.add_argument("--max-self-liqs-diagnostic", type=int, default=None,
                    help="DIAGNOSTIC ONLY: report wallets with lifetime self-liq <= this. Does NOT filter the output universe (per-fold gate handles forward-only self-liq).")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # 1) Scan all daily parquets, build per-(wallet, date) stats.
    logger.info(f"Scanning S3 fills {start.date()} -> {end.date()}")
    rows = []
    cur = start
    n_days = 0
    while cur <= end:
        path = FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet"
        if not path.exists():
            logger.warning(f"Missing {path.name}; skipping")
            cur += timedelta(days=1)
            continue
        agg = aggregate_day(path)
        if not agg.empty:
            rows.append(agg)
        n_days += 1
        if n_days % 20 == 0:
            logger.info(f"  processed {n_days} days; {len(rows):,} day-summaries accumulated")
        cur += timedelta(days=1)

    if not rows:
        logger.error("No S3 fill data found in window.")
        return

    logger.info(f"Concatenating {n_days} day-summaries...")
    per_day = pd.concat(rows, ignore_index=True)
    logger.info(f"Per-(wallet,date) rows: {len(per_day):,}")

    # 2) Per-wallet lifetime aggregates.
    logger.info("Aggregating per-wallet lifetime stats...")
    by_wallet = per_day.groupby("wallet", as_index=False).agg(
        total_fills=("n_fills", "sum"),
        total_active_days=("date", "nunique"),
        first_active_date=("date", "min"),
        last_active_date=("date", "max"),
        self_liq_lifetime=("self_liq", "sum"),
    )
    logger.info(f"Unique wallets in window: {len(by_wallet):,}")

    # 3) Rolling 30-day fill window max.
    max_30d = rolling_max_30d_fills(per_day)
    by_wallet["max_30d_fills"] = by_wallet["wallet"].map(max_30d).fillna(0).astype("uint32")
    # 3b) Rolling 30-day ACTIVE DAYS window max (only computed when filter is enabled).
    if args.min_30d_window_active_days > 0:
        logger.info("Computing rolling 30d max active-days...")
        max_30d_ad = rolling_max_30d_active_days(per_day)
        by_wallet["max_30d_active_days"] = by_wallet["wallet"].map(max_30d_ad).fillna(0).astype("uint8")
    else:
        by_wallet["max_30d_active_days"] = 0

    # 4) Apply pre-screen filters. Forward-only semantics: NO self-liq
    # filter at the global pre-screen level. Per-fold gate handles self-liq
    # within each fold's train window.
    before = len(by_wallet)
    keep = (
        (by_wallet["total_fills"] >= args.min_fills)
        & (by_wallet["total_active_days"] >= args.min_active_days)
        & (by_wallet["max_30d_fills"] >= args.min_30d_window_fills)
    )
    if args.min_30d_window_active_days > 0:
        keep = keep & (by_wallet["max_30d_active_days"] >= args.min_30d_window_active_days)
    filtered = by_wallet[keep].copy()
    logger.info(
        f"Pre-screen: {len(filtered):,} / {before:,} pass "
        f"({100*len(filtered)/before:.1f}%)"
    )
    drop_msg = (
        f"  Drop reasons: fills<{args.min_fills}: {(~(by_wallet['total_fills'] >= args.min_fills)).sum():,} | "
        f"active_days<{args.min_active_days}: {(~(by_wallet['total_active_days'] >= args.min_active_days)).sum():,} | "
        f"max_30d_fills<{args.min_30d_window_fills}: {(~(by_wallet['max_30d_fills'] >= args.min_30d_window_fills)).sum():,}"
    )
    if args.min_30d_window_active_days > 0:
        drop_msg += f" | max_30d_active_days<{args.min_30d_window_active_days}: {(~(by_wallet['max_30d_active_days'] >= args.min_30d_window_active_days)).sum():,}"
    logger.info(drop_msg)
    # Diagnostic only: how many wallets had self-liq events lifetime?
    n_self_liq = int((by_wallet["self_liq_lifetime"] > 0).sum())
    logger.info(
        f"  (diagnostic) wallets with >=1 lifetime self-liq event: {n_self_liq:,} "
        f"({100*n_self_liq/before:.2f}%). NOT filtered here; per-fold gate enforces this within each fold's train window only."
    )

    # 5) Output: wallet list + stats parquet.
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    wallet_list_path = out_dir / "wallet_universe.txt"
    stats_path = out_dir / "wallet_universe_stats.parquet"

    with open(wallet_list_path, "w") as f:
        for w in sorted(filtered["wallet"].tolist()):
            f.write(w + "\n")
    logger.info(f"Wrote {len(filtered):,} wallet addresses to {wallet_list_path}")

    filtered.to_parquet(stats_path, index=False, compression="snappy")
    logger.info(f"Wrote stats to {stats_path}")

    # 6) Diagnostic summary.
    logger.info("=== Filtered universe summary ===")
    logger.info(f"  total_fills     p50={int(filtered['total_fills'].median())} p95={int(filtered['total_fills'].quantile(0.95))} max={int(filtered['total_fills'].max())}")
    logger.info(f"  active_days     p50={int(filtered['total_active_days'].median())} p95={int(filtered['total_active_days'].quantile(0.95))} max={int(filtered['total_active_days'].max())}")
    logger.info(f"  max_30d_fills   p50={int(filtered['max_30d_fills'].median())} p95={int(filtered['max_30d_fills'].quantile(0.95))} max={int(filtered['max_30d_fills'].max())}")
    logger.info(f"  lifetime self-liq distribution among kept: p50={int(filtered['self_liq_lifetime'].median())} max={int(filtered['self_liq_lifetime'].max())}")
    logger.info(f"  first_active_date range: {filtered['first_active_date'].min()} -> {filtered['first_active_date'].max()}")
    logger.info(f"  last_active_date range: {filtered['last_active_date'].min()} -> {filtered['last_active_date'].max()}")


if __name__ == "__main__":
    main()
