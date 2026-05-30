#!/usr/bin/env python3
"""Concat journey_chunks/chunk_NN.parquet into wallet_journeys_costed.parquet.

This is the canonical output expected by downstream scripts:
  - scripts/copyability_3split_runner.py (--journeys)
  - scripts/copyability_eval.py (reads it indirectly via journey loader)
  - scripts/copyability_overlay.py

Run after scripts/run_chunks_sequential.sh completes. Idempotent: re-running
just rebuilds the concat from current chunk files.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("v13_concat")

REPO = Path(__file__).resolve().parents[1]
DEFAULT_CHUNKS_DIR = REPO / "app" / "data" / "v13" / "journey_chunks"
DEFAULT_OUTPUT = REPO / "app" / "data" / "v13" / "wallet_journeys_costed.parquet"


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks-dir", default=str(DEFAULT_CHUNKS_DIR),
                    help=f"Directory with chunk_NN.parquet files (default {DEFAULT_CHUNKS_DIR})")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT),
                    help=f"Concat output (default {DEFAULT_OUTPUT})")
    ap.add_argument("--require-min-chunks", type=int, default=17,
                    help="Refuse to concat if fewer than N chunks exist (default 17)")
    args = ap.parse_args(argv)

    chunks_dir = Path(args.chunks_dir)
    chunk_files = sorted(chunks_dir.glob("chunk_*.parquet"))
    if len(chunk_files) < args.require_min_chunks:
        logger.error(
            f"Found {len(chunk_files)} chunks in {chunks_dir} but require "
            f"{args.require_min_chunks}. Refusing to concat partial set."
        )
        return 2

    logger.info(f"Concatenating {len(chunk_files)} chunks from {chunks_dir}")
    dfs = []
    total_rows = 0
    for cf in chunk_files:
        d = pd.read_parquet(cf)
        logger.info(f"  {cf.name}: {len(d):,} rows, {d['wallet'].nunique():,} wallets")
        dfs.append(d)
        total_rows += len(d)

    out = pd.concat(dfs, ignore_index=True)
    if len(out) != total_rows:
        logger.error(f"Concat row count mismatch: {len(out)} vs {total_rows}")
        return 3

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Write to .tmp then rename to keep downstream readers from seeing a partial file.
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    out.to_parquet(tmp_path, index=False, compression="snappy")
    tmp_path.replace(out_path)

    logger.info(f"Wrote {len(out):,} journeys to {out_path}")
    logger.info(f"  Wallets: {out['wallet'].nunique():,}")
    logger.info(f"  Coins:   {out['coin'].nunique():,}")
    logger.info(f"  Date range: {pd.to_datetime(out['entry_ts'], unit='ms').min()} "
                f"→ {pd.to_datetime(out['exit_ts'], unit='ms').max()}")
    if "journey_class" in out.columns:
        logger.info("  Journey class distribution:")
        for cls, n in out["journey_class"].value_counts().items():
            logger.info(f"    {cls:>15}: {n:>10,} ({100*n/len(out):5.1f}%)")
    logger.info(f"  Win rate (pnl > 0): {100 * (out['realized_pnl_usd'] > 0).mean():.1f}%")
    if "net_realized_pnl_usd" in out.columns:
        logger.info(f"  Sum realized: ${out['realized_pnl_usd'].sum():,.0f}")
        logger.info(f"  Sum NET realized (after fees+funding): "
                    f"${out['net_realized_pnl_usd'].sum():,.0f}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
