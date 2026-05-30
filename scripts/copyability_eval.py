#!/usr/bin/env python3
"""
copyability_eval.py — empirical copyability test over HL S3 fills universe.

First principles (per round 8 converged methodology, codex+claude+Alberto):
  A wallet is COPYABLE iff:
    E_copy = E_source - edge_decay_over_L - execution_costs > 0 out of sample
  where:
    E_source = source wallet PnL per journey (in bps of entry notional)
    edge_decay_over_L = (entry_price_at_t+L - entry_price_at_t) / entry_price_at_t, signed by side
    execution_costs = 8.64 bps (HL taker RT) + modeled slippage
    L = follower latency (start with L=60s = 1m candle resolution)

Input universe: HL S3 fills, 175 daily parquets (Dec 1 2025 -> May 24 2026), 306K wallets.
Reference prices: hyperliquid_candles_1m in MongoDB (800 coins x 175 days, 1m resolution).

Stage 1 (this script): aggregate per-wallet journeys, compute E_source and E_copy@60s, output ranking.
Stage 2 (later): sub-minute latency 1s/5s/15s using fill-stream mid-price reconstruction.
Stage 3 (later): wallet correlation matrix + anti-corr greedy pick.

Run:
  python scripts/copyability_eval.py --start 2026-04-01 --end 2026-04-30 \
      --min-journeys 50 --min-coins 3 --out app/data/v13/copyability/eval_apr2026.parquet
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from glob import glob
from typing import Dict, Iterator, List, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pymongo import MongoClient

# Constants
FEE_RT_BPS = 8.64  # HL taker round-trip
FILLS_DIR = "/Users/hermes/quants-lab/app/data/hl_s3_fills"
LATENCY_SECONDS = [60, 300, 900]  # 1m, 5m, 15m initial latency horizons

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("copyability_eval")


# ----------------------------------------------------------------------------
# Data loaders
# ----------------------------------------------------------------------------


def list_fill_files(start: datetime, end: datetime) -> List[str]:
    """Return sorted list of HL S3 parquet files within [start, end] (inclusive)."""
    out = []
    cur = start.date()
    end_d = end.date()
    while cur <= end_d:
        path = os.path.join(FILLS_DIR, f"{cur:%Y%m%d}.parquet")
        if os.path.exists(path):
            out.append(path)
        cur = cur + timedelta(days=1)
    return out


def load_candles_1m(coins: List[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Load 1m candles for selected coins between start/end. Returns DataFrame
    indexed by (coin, ts_minute) with 'close' column.
    """
    log.info("loading 1m candles for %d coins from MongoDB...", len(coins))
    client = MongoClient("mongodb://localhost:27017")
    coll = client["quants_lab"]["hyperliquid_candles_1m"]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    cur = coll.find(
        {
            "coin": {"$in": coins},
            "interval": "1m",
            "timestamp_utc": {"$gte": start_ms, "$lt": end_ms},
        },
        {"_id": 0, "coin": 1, "timestamp_utc": 1, "close": 1, "open": 1},
    )
    rows = list(cur)
    client.close()
    if not rows:
        log.warning("no candles returned")
        return pd.DataFrame(columns=["coin", "ts_minute", "close", "open"])
    df = pd.DataFrame(rows)
    # ts_minute aligned to minute boundary
    df["ts_minute"] = (df["timestamp_utc"] // 60_000) * 60_000
    df = df.drop_duplicates(["coin", "ts_minute"]).sort_values(["coin", "ts_minute"])
    log.info("candles loaded: %d rows", len(df))
    return df


# ----------------------------------------------------------------------------
# Journey reconstruction (FIFO position tracking per wallet/coin)
# ----------------------------------------------------------------------------


@dataclass
class Journey:
    """One closed round-trip for a wallet on a coin."""
    wallet: str
    coin: str
    side: int  # +1 long, -1 short
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    size: float
    notional: float
    source_pnl_usd: float
    n_partial_closes: int = 0


@dataclass
class OpenLot:
    """One open lot within a wallet's position queue."""
    entry_time_ms: int
    entry_price: float
    size: float  # always positive


def iter_journeys(fill_files: List[str]) -> Iterator[Journey]:
    """Stream Journey objects from sorted daily parquet files.

    Maintains FIFO position queues per (wallet, coin) across days. Yields a
    Journey when a position lot is closed.

    Filters out:
    - spot fills (Buy/Sell)
    - net child vaults / dust conversions
    - extreme outlier sizes (TBD)
    """
    # Per (wallet, coin): two FIFO queues (long lots, short lots).
    # The "Long > Short" flip = close all longs + open short for the residual size.
    open_long: Dict[Tuple[str, str], deque[OpenLot]] = defaultdict(deque)
    open_short: Dict[Tuple[str, str], deque[OpenLot]] = defaultdict(deque)

    total_fills = 0
    total_journeys = 0

    for f_idx, path in enumerate(fill_files):
        log.info("[%d/%d] loading %s", f_idx + 1, len(fill_files), os.path.basename(path))
        df = pq.read_table(
            path,
            columns=["wallet", "coin", "size", "price", "time", "dir", "closedPnl", "notional"],
        ).to_pandas()
        # Drop spot + admin
        df = df[df["dir"].isin([
            "Open Long", "Close Long", "Open Short", "Close Short",
            "Long > Short", "Short > Long",
        ])]
        df = df.sort_values("time", kind="stable")
        total_fills += len(df)

        for row in df.itertuples(index=False):
            wallet = row.wallet
            coin = row.coin
            size = float(row.size)
            price = float(row.price)
            t_ms = int(row.time)
            dir_ = row.dir
            pnl = float(row.closedPnl or 0.0)
            notional = float(row.notional)

            key = (wallet, coin)

            if dir_ == "Open Long":
                open_long[key].append(OpenLot(t_ms, price, size))
            elif dir_ == "Open Short":
                open_short[key].append(OpenLot(t_ms, price, size))
            elif dir_ == "Close Long":
                # FIFO close longs
                remaining = size
                while remaining > 1e-12 and open_long[key]:
                    lot = open_long[key][0]
                    take = min(lot.size, remaining)
                    lot_pnl = take * (price - lot.entry_price)  # long pnl
                    yield Journey(
                        wallet=wallet, coin=coin, side=+1,
                        entry_time_ms=lot.entry_time_ms,
                        exit_time_ms=t_ms,
                        entry_price=lot.entry_price,
                        exit_price=price,
                        size=take,
                        notional=take * lot.entry_price,
                        source_pnl_usd=lot_pnl,
                    )
                    total_journeys += 1
                    lot.size -= take
                    remaining -= take
                    if lot.size <= 1e-12:
                        open_long[key].popleft()
                # else: residual close with no matching open (started before our window)
            elif dir_ == "Close Short":
                remaining = size
                while remaining > 1e-12 and open_short[key]:
                    lot = open_short[key][0]
                    take = min(lot.size, remaining)
                    lot_pnl = take * (lot.entry_price - price)  # short pnl
                    yield Journey(
                        wallet=wallet, coin=coin, side=-1,
                        entry_time_ms=lot.entry_time_ms,
                        exit_time_ms=t_ms,
                        entry_price=lot.entry_price,
                        exit_price=price,
                        size=take,
                        notional=take * lot.entry_price,
                        source_pnl_usd=lot_pnl,
                    )
                    total_journeys += 1
                    lot.size -= take
                    remaining -= take
                    if lot.size <= 1e-12:
                        open_short[key].popleft()
            elif dir_ == "Long > Short":
                # Close all longs, then open short for residual
                remaining = size
                while remaining > 1e-12 and open_long[key]:
                    lot = open_long[key][0]
                    take = min(lot.size, remaining)
                    lot_pnl = take * (price - lot.entry_price)
                    yield Journey(
                        wallet=wallet, coin=coin, side=+1,
                        entry_time_ms=lot.entry_time_ms,
                        exit_time_ms=t_ms,
                        entry_price=lot.entry_price,
                        exit_price=price,
                        size=take,
                        notional=take * lot.entry_price,
                        source_pnl_usd=lot_pnl,
                    )
                    total_journeys += 1
                    lot.size -= take
                    remaining -= take
                    if lot.size <= 1e-12:
                        open_long[key].popleft()
                if remaining > 1e-12:
                    open_short[key].append(OpenLot(t_ms, price, remaining))
            elif dir_ == "Short > Long":
                remaining = size
                while remaining > 1e-12 and open_short[key]:
                    lot = open_short[key][0]
                    take = min(lot.size, remaining)
                    lot_pnl = take * (lot.entry_price - price)
                    yield Journey(
                        wallet=wallet, coin=coin, side=-1,
                        entry_time_ms=lot.entry_time_ms,
                        exit_time_ms=t_ms,
                        entry_price=lot.entry_price,
                        exit_price=price,
                        size=take,
                        notional=take * lot.entry_price,
                        source_pnl_usd=lot_pnl,
                    )
                    total_journeys += 1
                    lot.size -= take
                    remaining -= take
                    if lot.size <= 1e-12:
                        open_short[key].popleft()
                if remaining > 1e-12:
                    open_long[key].append(OpenLot(t_ms, price, remaining))

    log.info("processed %d fills, emitted %d journeys", total_fills, total_journeys)


# ----------------------------------------------------------------------------
# Latency overlay: compute E_copy at each latency horizon
# ----------------------------------------------------------------------------


def attach_latency_prices(
    journeys_df: pd.DataFrame,
    candles_df: pd.DataFrame,
    latency_seconds: List[int],
) -> pd.DataFrame:
    """For each journey, attach copy_entry_price@L and copy_exit_price@L for each L.

    Uses 1m candle CLOSE price at the minute boundary >= (entry_time + L) and
    >= (exit_time + L). For L=60s, picks the minute >= entry+1m.
    """
    # Build coin -> sorted candle DataFrame for fast searchsorted
    log.info("indexing candles by coin...")
    coin_to_idx = {}
    for coin, sub in candles_df.groupby("coin", sort=False):
        sub = sub.sort_values("ts_minute").reset_index(drop=True)
        coin_to_idx[coin] = (
            sub["ts_minute"].to_numpy(dtype=np.int64),
            sub["close"].to_numpy(dtype=np.float64),
        )

    for L in latency_seconds:
        entry_px = np.full(len(journeys_df), np.nan)
        exit_px = np.full(len(journeys_df), np.nan)
        for i, row in enumerate(journeys_df.itertuples(index=False)):
            arr = coin_to_idx.get(row.coin)
            if arr is None:
                continue
            ts_arr, px_arr = arr
            # entry-side: smallest minute >= entry_time + L
            target_e = int(row.entry_time_ms) + L * 1000
            idx_e = np.searchsorted(ts_arr, target_e)
            if idx_e < len(ts_arr):
                entry_px[i] = px_arr[idx_e]
            target_x = int(row.exit_time_ms) + L * 1000
            idx_x = np.searchsorted(ts_arr, target_x)
            if idx_x < len(ts_arr):
                exit_px[i] = px_arr[idx_x]
        journeys_df[f"copy_entry_px_{L}s"] = entry_px
        journeys_df[f"copy_exit_px_{L}s"] = exit_px

    return journeys_df


def compute_copy_pnl(journeys_df: pd.DataFrame, latency_seconds: List[int]) -> pd.DataFrame:
    """Add E_source_bps and E_copy_bps_{L}s columns."""
    # E_source in bps of entry notional
    js = journeys_df
    js["source_pnl_bps"] = (
        js["source_pnl_usd"] / js["notional"].clip(lower=1e-9) * 10000.0
    )
    for L in latency_seconds:
        e_col = f"copy_entry_px_{L}s"
        x_col = f"copy_exit_px_{L}s"
        side = js["side"].astype(float)
        # copy_pnl in bps of entry price = side * (exit - entry) / entry * 10000
        copy_gross_bps = side * (js[x_col] - js[e_col]) / js[e_col].clip(lower=1e-12) * 10000.0
        js[f"copy_gross_bps_{L}s"] = copy_gross_bps
        js[f"copy_net_bps_{L}s"] = copy_gross_bps - FEE_RT_BPS
    return js


# ----------------------------------------------------------------------------
# Per-wallet aggregation
# ----------------------------------------------------------------------------


def aggregate_per_wallet(
    journeys_df: pd.DataFrame,
    latency_seconds: List[int],
    min_journeys: int,
    min_coins: int,
) -> pd.DataFrame:
    """Aggregate to per-wallet stats. Filter wallets with insufficient activity."""
    agg = journeys_df.groupby("wallet").agg(
        n_journeys=("coin", "count"),
        n_coins=("coin", "nunique"),
        total_notional=("notional", "sum"),
        median_hold_s=("hold_s", "median") if "hold_s" in journeys_df.columns else ("notional", "size"),
        source_pnl_usd_sum=("source_pnl_usd", "sum"),
        source_pnl_bps_median=("source_pnl_bps", "median"),
        source_pnl_bps_mean=("source_pnl_bps", "mean"),
        win_rate=("source_pnl_usd", lambda s: float((s > 0).mean())),
    )
    for L in latency_seconds:
        net_col = f"copy_net_bps_{L}s"
        agg[f"copy_net_bps_{L}s_median"] = journeys_df.groupby("wallet")[net_col].median()
        agg[f"copy_net_bps_{L}s_mean"] = journeys_df.groupby("wallet")[net_col].mean()
        agg[f"copy_win_rate_{L}s"] = journeys_df.groupby("wallet")[net_col].apply(
            lambda s: float((s > 0).mean())
        )
        # Copyable ratio = copy_net / source_pnl (per journey, then median)
        ratio = journeys_df[net_col] / journeys_df["source_pnl_bps"].abs().clip(lower=1e-9)
        ratio_by_w = ratio.groupby(journeys_df["wallet"]).median()
        agg[f"copyable_alpha_ratio_{L}s_median"] = ratio_by_w

    agg = agg[(agg["n_journeys"] >= min_journeys) & (agg["n_coins"] >= min_coins)]
    agg = agg.sort_values(f"copy_net_bps_{latency_seconds[0]}s_median", ascending=False)
    return agg


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--min-journeys", type=int, default=50)
    p.add_argument("--min-coins", type=int, default=3)
    p.add_argument("--out", required=True, help="output parquet path")
    p.add_argument("--journeys-out", help="optional: dump raw journeys to this path")
    return p.parse_args()


def main():
    args = parse_args()
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    log.info("range: %s -> %s", start.date(), end.date())

    files = list_fill_files(start, end)
    log.info("fill files: %d", len(files))
    if not files:
        log.error("no fill files in range, aborting")
        sys.exit(1)

    # Stream journeys
    journeys = list(iter_journeys(files))
    if not journeys:
        log.error("no journeys produced")
        sys.exit(1)

    log.info("converting %d journeys to dataframe...", len(journeys))
    journeys_df = pd.DataFrame([j.__dict__ for j in journeys])
    journeys_df["hold_s"] = (journeys_df["exit_time_ms"] - journeys_df["entry_time_ms"]) / 1000.0

    # Determine coins to fetch candles for
    coins = sorted(journeys_df["coin"].unique().tolist())
    log.info("unique coins in journeys: %d", len(coins))

    # Load candles for the same range (extend +1 day for trailing latency)
    candles = load_candles_1m(coins, start, end + timedelta(days=1))

    # Attach latency-shifted prices + compute copy pnl
    journeys_df = attach_latency_prices(journeys_df, candles, LATENCY_SECONDS)
    journeys_df = compute_copy_pnl(journeys_df, LATENCY_SECONDS)

    # Optionally dump raw journeys for stage 2 sub-minute latency
    if args.journeys_out:
        log.info("writing raw journeys: %s", args.journeys_out)
        journeys_df.to_parquet(args.journeys_out, index=False)

    # Aggregate per wallet
    agg = aggregate_per_wallet(
        journeys_df,
        LATENCY_SECONDS,
        min_journeys=args.min_journeys,
        min_coins=args.min_coins,
    )
    log.info("per-wallet results: %d wallets after filters", len(agg))

    # Write output
    agg.reset_index().to_parquet(args.out, index=False)
    log.info("wrote %s", args.out)

    # Summary
    print("\n=== TOP 20 WALLETS BY E_copy@60s median ===")
    top = agg.head(20)[
        [
            "n_journeys", "n_coins", "source_pnl_usd_sum",
            "source_pnl_bps_median", "win_rate",
            "copy_net_bps_60s_median", "copy_win_rate_60s",
            "copyable_alpha_ratio_60s_median",
        ]
    ]
    print(top.to_string())

    print("\n=== UNIVERSE STATS ===")
    print(f"total wallets passing filters: {len(agg):,}")
    print(f"wallets with copy_net_60s_median > 0:        {(agg['copy_net_bps_60s_median']>0).sum():,}")
    print(f"wallets with copy_net_60s_median > 5 bps:    {(agg['copy_net_bps_60s_median']>5).sum():,}")
    print(f"wallets with copy_net_60s_median > 20 bps:   {(agg['copy_net_bps_60s_median']>20).sum():,}")
    print(f"wallets with copy_net_300s_median > 0:       {(agg['copy_net_bps_300s_median']>0).sum():,}")
    print(f"wallets with copy_net_900s_median > 0:       {(agg['copy_net_bps_900s_median']>0).sum():,}")


if __name__ == "__main__":
    main()
