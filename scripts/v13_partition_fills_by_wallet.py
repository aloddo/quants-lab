#!/usr/bin/env python3
"""Partition S3 fills v2 by wallet for fast per-wallet loading.

One-time cost. Output: app/data/hl_s3_fills_v2_by_wallet/{wallet}.parquet
Each per-wallet file has all in-window fills sorted by time.

Speeds equity_reconstruct_v7 from ~9s/wallet to ~0.5s/wallet.
"""
from __future__ import annotations
import argparse
import glob
import time
from pathlib import Path
import pandas as pd
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', stream=sys.stdout)
logger = logging.getLogger('partition')

S3_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
OUT_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2_by_wallet')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--wallets-file', help='Filter to these wallets only (one per line)')
    ap.add_argument('--start', default='2025-12-01')
    ap.add_argument('--end', default='2026-05-23')
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    wallets_filter = None
    if args.wallets_file:
        with open(args.wallets_file) as f:
            wallets_filter = set(line.strip().lower() for line in f if line.strip())
        logger.info(f'Filter to {len(wallets_filter):,} wallets')

    start_ms = int(pd.Timestamp(args.start, tz='UTC').timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz='UTC') + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    files = sorted(glob.glob(str(S3_DIR / '*.parquet')))
    logger.info(f'Scanning {len(files)} S3 fill files')

    # Accumulate fills per wallet in memory (use dict of lists, flush at end)
    # For 100M rows / 20K wallets = 5K rows/wallet avg, ~200 bytes/row = 1MB/wallet * 20K = 20GB in memory
    # That's too much. Better: bucket by wallet prefix, process buckets sequentially.

    COLS = ['wallet', 'coin', 'side', 'size', 'price', 'time', 'startPosition',
            'fee', 'builderFee', 'deployerFee']

    # Single-pass approach: load each file, filter by wallet+time, append to per-wallet parquet (write-once at end)
    # For memory efficiency: accumulate dict of {wallet: list of frames}, dump at end.
    per_wallet = {}  # wallet -> list of small dfs
    t0 = time.time()
    total_rows = 0
    for i, ff in enumerate(files):
        try:
            df = pd.read_parquet(ff, columns=COLS)
            df['time'] = df['time'].astype('int64')
            df = df[(df['time'] >= start_ms) & (df['time'] <= end_ms)]
            if wallets_filter:
                df = df[df['wallet'].isin(wallets_filter)]
            if df.empty:
                continue
            total_rows += len(df)
            for w, sub in df.groupby('wallet'):
                if w not in per_wallet:
                    per_wallet[w] = []
                per_wallet[w].append(sub)
        except Exception as e:
            logger.warning(f'  {ff}: {e}')
        if (i+1) % 20 == 0:
            elapsed = time.time() - t0
            logger.info(f'  [{i+1}/{len(files)}] rows={total_rows:,} wallets={len(per_wallet):,} elapsed={elapsed:.0f}s')

    logger.info(f'\nScan done in {time.time()-t0:.0f}s. Total rows: {total_rows:,}, wallets: {len(per_wallet):,}')
    logger.info('Concatenating and writing per-wallet parquets...')

    t1 = time.time()
    for j, (w, frames) in enumerate(per_wallet.items()):
        out_df = pd.concat(frames, ignore_index=True).sort_values('time').reset_index(drop=True)
        out_df['signed_sz'] = out_df.apply(lambda r: float(r['size']) if r['side']=='B' else -float(r['size']), axis=1)
        out_path = OUT_DIR / f'{w}.parquet'
        out_df.to_parquet(out_path, index=False, compression='snappy')
        if (j+1) % 500 == 0:
            logger.info(f'  written {j+1}/{len(per_wallet)} files ({(time.time()-t1):.0f}s)')

    logger.info(f'\nTotal wall: {(time.time()-t0)/60:.1f}min')
    logger.info(f'Output: {OUT_DIR}')


if __name__ == '__main__':
    main()
