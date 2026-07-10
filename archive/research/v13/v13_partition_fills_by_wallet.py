#!/usr/bin/env python3
"""Partition S3 fills v2 by wallet for fast per-wallet loading — FULL SCHEMA.

Output: <out-dir>/{wallet}.parquet, one file per wallet, deterministically
ordered by (time, tid). This is the MOST IMPORTANT upstream dependency for the
V15 equity reconstruction (M01): the per-wallet fills ARE the source of truth for
positions, cash, fees, and liquidations.

2026-05-30 REWRITE (Alberto: "do things properly"): the previous version dropped
`dir`, `tid`, `closedPnl`, `crossed`. Consequences downstream:
  - no `tid` -> same-millisecond fill bursts could not be ordered -> position
    seeding corrupted (a flat wallet reconstructed as -90k of a coin).
  - no `dir` -> liquidation detection silently failed on the fast path.
  - no `closedPnl` -> realized-PnL reconciliation cross-check was a no-op.
This version carries the full accounting + ordering schema, ordered by (time, tid).

Memory-safe: processes the universe in 16 wallet-prefix buckets via DuckDB (filter
+ sort pushed into the engine; no in-memory global sort, no 20k-partition writer).
Writes to a NEW dir; verify, then swap. Heavy hash/oid columns are dropped (M01
does not use them; tid is the ordering key).

Usage:
    python v13_partition_fills_by_wallet.py \
        --start 2025-12-01 --end 2026-05-27 \
        --out-dir app/data/hl_s3_fills_v2_by_wallet_full
"""
from __future__ import annotations

import argparse
import glob
import logging
import shutil
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
logger = logging.getLogger("partition")

ROOT = Path(__file__).resolve().parent.parent.parent
S3_DIR = ROOT / "app" / "data" / "hl_s3_fills_v2"

SELECT_SQL = """
    lower(wallet)                                     AS wallet,
    coin, side, size, price,
    CAST(time AS BIGINT)                              AS time,
    COALESCE(CAST(tid AS BIGINT), 0)                  AS tid,
    COALESCE(dir, '')                                 AS dir,
    closedPnl, startPosition, fee, builderFee, deployerFee, crossed,
    CASE WHEN side = 'B' THEN CAST(size AS DOUBLE)
         ELSE -CAST(size AS DOUBLE) END               AS signed_sz
"""
HEX = "0123456789abcdef"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-27")
    ap.add_argument("--out-dir", default="app/data/hl_s3_fills_v2_by_wallet_full")
    ap.add_argument("--memory-limit", default="6GB")
    ap.add_argument("--threads", type=int, default=4)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir
    tmp_flat = out_dir.with_name(out_dir.name + "_new")
    if tmp_flat.exists():
        shutil.rmtree(tmp_flat)
    tmp_flat.mkdir(parents=True, exist_ok=True)

    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)
    n_files = len(sorted(glob.glob(str(S3_DIR / "*.parquet"))))
    logger.info(f"Source: {n_files} daily files | window {args.start}..{args.end} | out {tmp_flat}")

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{tmp_flat.parent / '.duckdb_tmp'}'")

    t0 = time.time()
    total_wallets = 0
    total_rows = 0
    src = f"read_parquet('{S3_DIR}/*.parquet', union_by_name=true)"
    for c in HEX:
        tb = time.time()
        q = f"""
            SELECT {SELECT_SQL}
            FROM {src}
            WHERE CAST(time AS BIGINT) BETWEEN {start_ms} AND {end_ms}
              AND substr(lower(wallet), 3, 1) = '{c}'
            ORDER BY wallet, time, tid
        """
        df = con.execute(q).df()
        if len(df):
            for w, sub in df.groupby("wallet", sort=False):
                sub.drop(columns=["wallet"]).to_parquet(
                    tmp_flat / f"{w}.parquet", index=False, compression="snappy"
                )
            total_wallets += df["wallet"].nunique()
            total_rows += len(df)
        logger.info(
            f"  prefix 0x{c}: {len(df):,} rows, {df['wallet'].nunique() if len(df) else 0:,} wallets "
            f"({time.time()-tb:.0f}s) | cumulative {total_wallets:,} wallets"
        )

    logger.info(f"\nDONE in {(time.time()-t0)/60:.1f} min | {total_rows:,} rows, {total_wallets:,} wallets")
    logger.info(f"New partition staged at: {tmp_flat}")
    logger.info(f"  -> VERIFY, then swap: mv {out_dir} {out_dir}_old ; mv {tmp_flat} {out_dir}")


if __name__ == "__main__":
    main()
