#!/usr/bin/env python3
"""Parallel wrapper for v13_equity_reconstruct.

Splits a wallet list into N chunks, runs v13_equity_reconstruct.py in
N parallel subprocesses (one chunk each), and concatenates the resulting
parquet shards into a single output parquet.

The underlying script's API call rate-limiting handles individual subprocess
backpressure. HL's info endpoint accepts ~10-30 concurrent connections from
a single IP without issue; the 429 retry/backoff loop in _hl_post protects
against transient throttling.

Usage:
    python scripts/v13_equity_reconstruct_parallel.py \\
        --wallets app/data/v13/wallet_universe/wallet_universe.txt \\
        --start 2025-12-01 --end 2026-05-24 \\
        --workers 10 \\
        --output app/data/v13/wallet_equity_series.parquet

Output is the SAME schema as v13_equity_reconstruct.py: one parquet at
--output containing the concatenated equity series across all chunks.

Resumability: each chunk writes to a per-shard parquet first. If a chunk
fails the wrapper logs which one and the user can re-run on the failed
chunk(s) via the --resume-from-shard-dir flag.
"""
from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_eq_par] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent


def run_chunk(chunk_idx: int, chunk_path: Path, start: str, end: str,
              out_path: Path, log_path: Path) -> tuple[int, int, str]:
    """Spawn one v13_equity_reconstruct subprocess for one chunk.

    Returns (chunk_idx, return_code, stderr_tail).
    """
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "v13_equity_reconstruct.py"),
        "--wallets", str(chunk_path),
        "--start", start, "--end", end,
        "--output", str(out_path),
    ]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as lf:
        res = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, timeout=86400)
    return chunk_idx, res.returncode, log_path.name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", required=True, help="Wallet list (one per line)")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--workers", type=int, default=10,
                    help="Number of parallel subprocesses (default 10)")
    ap.add_argument("--output", required=True,
                    help="Final concatenated parquet path")
    ap.add_argument("--shard-dir", default="app/data/v13/equity_shards_parallel",
                    help="Per-chunk shard directory (default app/data/v13/equity_shards_parallel; "
                         "moved off /tmp after 2026-05-29 OOM-2 lost 5h compute)")
    ap.add_argument("--chunk-list-dir", default="app/data/v13/equity_chunk_lists",
                    help="Per-chunk wallet list directory (default app/data/v13/equity_chunk_lists; "
                         "moved off /tmp after 2026-05-29 OOM-2)")
    ap.add_argument("--log-dir", default=None,
                    help="Per-chunk log directory (default <output_dir>/logs/parallel)")
    ap.add_argument("--resume", action="store_true",
                    help="Skip chunks whose shard parquet already exists")
    args = ap.parse_args()

    out_path = Path(args.output)
    shard_dir = Path(args.shard_dir)
    chunk_list_dir = Path(args.chunk_list_dir)
    log_dir = Path(args.log_dir) if args.log_dir else (out_path.parent / "logs" / "parallel")

    shard_dir.mkdir(parents=True, exist_ok=True)
    chunk_list_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    # 1) Load wallet list.
    wallets = []
    with open(args.wallets) as f:
        for line in f:
            w = line.strip().lower()
            if w:
                wallets.append(w)
    logger.info(f"Loaded {len(wallets):,} wallets from {args.wallets}")

    # 2) Split into N approximately-equal chunks.
    n_workers = args.workers
    chunk_size = (len(wallets) + n_workers - 1) // n_workers
    chunks = []
    for i in range(n_workers):
        wallet_chunk = wallets[i * chunk_size : (i + 1) * chunk_size]
        if not wallet_chunk:
            continue
        chunk_path = chunk_list_dir / f"chunk_{i:03d}.txt"
        with open(chunk_path, "w") as f:
            for w in wallet_chunk:
                f.write(w + "\n")
        shard_path = shard_dir / f"shard_{i:03d}.parquet"
        log_path = log_dir / f"chunk_{i:03d}.log"
        chunks.append((i, chunk_path, shard_path, log_path, len(wallet_chunk)))
    logger.info(f"Split into {len(chunks)} chunks of ~{chunk_size:,} wallets each")

    # 3) Optionally skip chunks whose shard already exists.
    pending = []
    skipped = 0
    for ch in chunks:
        i, chunk_path, shard_path, log_path, n = ch
        if args.resume and shard_path.exists():
            skipped += 1
            continue
        pending.append(ch)
    if skipped:
        logger.info(f"Resume: skipping {skipped} chunks with existing shards")
    logger.info(f"Submitting {len(pending)} chunks to {n_workers} workers")

    # 4) Submit. Use ProcessPoolExecutor with max_workers = n_workers.
    t0 = time.time()
    failures = []
    successes = []
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(run_chunk, i, chunk_path, args.start, args.end,
                            shard_path, log_path): i
            for (i, chunk_path, shard_path, log_path, n) in pending
        }
        completed = 0
        for fut in as_completed(futures):
            chunk_idx = futures[fut]
            try:
                idx, rc, log_name = fut.result()
                completed += 1
                if rc != 0:
                    failures.append(idx)
                    logger.error(f"Chunk {idx} FAILED rc={rc}; see {log_name}")
                else:
                    successes.append(idx)
                    elapsed = time.time() - t0
                    logger.info(f"Chunk {idx} OK [{completed}/{len(pending)}] elapsed={elapsed:.0f}s")
            except Exception as e:
                failures.append(chunk_idx)
                logger.exception(f"Chunk {chunk_idx} raised: {e}")

    logger.info(f"All chunks done. {len(successes)} OK, {len(failures)} failed")
    if failures:
        logger.error(f"Failed chunk indices: {failures}")
        logger.error(f"Re-run with --resume to retry failed chunks. Continuing to concat anyway.")

    # 5) Concat all shard parquets (and any already-existing ones from resume).
    shards = sorted(shard_dir.glob("shard_*.parquet"))
    if not shards:
        logger.error("No shard parquets to concatenate. Aborting.")
        sys.exit(1)
    logger.info(f"Concatenating {len(shards)} shards into {out_path}")
    frames = []
    for s in shards:
        try:
            df = pd.read_parquet(s)
            frames.append(df)
            logger.info(f"  {s.name}: {len(df):,} rows")
        except Exception as e:
            logger.exception(f"Failed to read {s.name}: {e}")
    if not frames:
        logger.error("No shard data readable.")
        sys.exit(1)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    final = pd.concat(frames, ignore_index=True)
    final.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(final):,} rows across {final['wallet'].nunique():,} wallets to {out_path}")

    elapsed = time.time() - t0
    logger.info(f"Total elapsed: {elapsed:.0f}s ({elapsed/3600:.2f}h)")


if __name__ == "__main__":
    main()
