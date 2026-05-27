"""V13 S3 ledger downloader (Alberto direction TG 7441, 2026-05-27).

Replaces the rate-limited userNonFundingLedgerUpdates + userFunding REST
loop with a one-time bulk download from hl-mainnet-node-data S3 bucket.

Source: hl-mainnet-node-data/misc_events_by_block/hourly/YYYYMMDD/H.lz4
  - Path: per-day, per-hour LZ4-compressed JSONL of block events
  - Block format: {block_number, block_time, local_time, events: [...]}
  - Event shape: {time, hash, inner: {EventType: {...payload...}}}

Event types we extract:
  - LedgerUpdate: inner.LedgerUpdate.delta has IDENTICAL shape to
    userNonFundingLedgerUpdates response. Wallets affected listed in
    inner.LedgerUpdate.users[].
  - Funding: inner.Funding.deltas[] has {user, coin, funding_amount, szi,
    funding_rate}. Reshape to match userFunding response.

Output:
  - /Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k/{wallet}_{start_ms}_{end_ms}.json
  - /Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k/{wallet}_{start_ms}_{end_ms}.json
  - Format matches what v13_equity_reconstruct.get_non_funding_ledger_updates +
    get_funding_updates cache readers expect.

Stream + discard: NEVER persist the raw lz4 to disk. Download, decompress in
memory, parse, accumulate per-wallet, flush per-day.

ETA: 175 days × 24 hours = 4,200 files × ~10MB compressed = ~42GB egress.
At 50MB/s = ~14min download. Plus parse time ≈ 1-2h total.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import lz4.frame
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s', stream=sys.stdout)
logger = logging.getLogger('s3_ledger_dl')

BUCKET = 'hl-mainnet-node-data'
PREFIX = 'misc_events_by_block/hourly'
LEDGER_OUT_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
FUNDING_OUT_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k')


def parse_block_time(s: str) -> int:
    """Parse '2026-05-24T00:00:00.038099382' → ms since epoch."""
    # Truncate nanoseconds to microseconds for fromisoformat
    if '.' in s:
        head, frac = s.split('.', 1)
        frac = (frac + '000000')[:6]
        s = f'{head}.{frac}'
    dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def stream_one_hour(s3, day: str, hour: int, wallets_lc: set, start_ms_filter: int, end_ms_filter: int):
    """Download + decompress one hour file in memory, yield (kind, user_lc, entry).

    kind in {'ledger', 'funding'}.
    """
    key = f'{PREFIX}/{day}/{hour}.lz4'
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer='requester')
        raw = resp['Body'].read()
    except ClientError as e:
        logger.warning(f'S3 fetch failed {key}: {e}')
        return
    try:
        data = lz4.frame.decompress(raw)
    except Exception as e:
        logger.warning(f'lz4 decompress failed {key}: {e}')
        return
    n_ledger = 0
    n_funding = 0
    for line in data.split(b'\n'):
        if not line:
            continue
        try:
            block = json.loads(line)
        except Exception:
            continue
        # Block time is the canonical event timestamp
        block_time_str = block.get('block_time')
        if not block_time_str:
            continue
        try:
            block_time_ms = parse_block_time(block_time_str)
        except Exception:
            continue
        # Window filter (cheap-skip non-window blocks)
        if block_time_ms < start_ms_filter or block_time_ms > end_ms_filter:
            continue
        for ev in block.get('events', []):
            inner = ev.get('inner') or {}
            ev_time = ev.get('time')
            ev_hash = ev.get('hash')
            # Prefer event-level time over block-level if present.
            try:
                event_time_ms = parse_block_time(ev_time) if ev_time else block_time_ms
            except Exception:
                event_time_ms = block_time_ms
            for kind_key, payload in inner.items():
                if kind_key == 'LedgerUpdate':
                    delta = payload.get('delta', {})
                    users = payload.get('users', [])
                    # Emit one entry per affected wallet IN OUR SET.
                    for u in users:
                        u_lc = u.lower() if isinstance(u, str) else ''
                        if u_lc in wallets_lc:
                            entry = {
                                'time': event_time_ms,
                                'hash': ev_hash or '',
                                'delta': delta,
                            }
                            yield ('ledger', u_lc, entry)
                            n_ledger += 1
                elif kind_key == 'Funding':
                    # inner.Funding.deltas: per-user funding deltas
                    deltas = payload.get('deltas', [])
                    for d in deltas:
                        u = d.get('user', '')
                        u_lc = u.lower() if isinstance(u, str) else ''
                        if u_lc in wallets_lc:
                            # Reshape to userFunding response format
                            entry = {
                                'time': event_time_ms,
                                'hash': ev_hash or '',
                                'delta': {
                                    'type': 'funding',
                                    'coin': d.get('coin', ''),
                                    'usdc': d.get('funding_amount', '0'),  # signed
                                    'szi': d.get('szi', '0'),
                                    'fundingRate': d.get('funding_rate', '0'),
                                    'nSamples': d.get('n_samples', 1),
                                },
                            }
                            yield ('funding', u_lc, entry)
                            n_funding += 1


def flush_per_wallet(buckets: dict, out_dir: Path, suffix_window: str):
    """Persist per-wallet entries to JSON files. Append-merge if file exists."""
    out_dir.mkdir(parents=True, exist_ok=True)
    n_files = 0
    n_entries = 0
    for wallet_lc, entries in buckets.items():
        path = out_dir / f'{wallet_lc}{suffix_window}.json'
        # Merge with existing (in case of restart with overlapping days)
        existing = []
        if path.exists():
            try:
                with open(path) as f:
                    existing = json.load(f)
            except Exception:
                existing = []
        merged = existing + entries
        # Dedup by (time, hash) — same event from overlapping fetch
        seen = set()
        deduped = []
        for e in merged:
            key = (e.get('time'), e.get('hash'), json.dumps(e.get('delta', {}), sort_keys=True))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(e)
        # Sort by time ascending
        deduped.sort(key=lambda e: e.get('time', 0))
        tmp = path.with_suffix('.json.tmp')
        with open(tmp, 'w') as f:
            json.dump(deduped, f)
        tmp.replace(path)
        n_files += 1
        n_entries += len(entries)
    return n_files, n_entries


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2025-12-01', help='YYYY-MM-DD inclusive')
    ap.add_argument('--end', default='2026-05-24', help='YYYY-MM-DD inclusive')
    ap.add_argument('--wallets-parquet', default='/Users/hermes/quants-lab/app/data/v13/equity_universe_20k.parquet',
                    help='Parquet with wallet column to filter to')
    ap.add_argument('--n-workers', type=int, default=4, help='Parallel S3 hour-file fetchers')
    ap.add_argument('--flush-every-day', action='store_true', default=True,
                    help='Flush per-wallet buckets to disk after each completed day')
    ap.add_argument('--days-limit', type=int, default=0, help='Smoke: only process N days. 0=all.')
    ap.add_argument('--ledger-only', action='store_true',
                    help='Skip funding parse/persist. Alberto path A 2026-05-27: funding stays in REST cache; '
                         'avoids ~49GB funding disk on the ledger-only S3 path.')
    args = ap.parse_args()

    start_d = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_d = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    start_ms_window = int(start_d.timestamp() * 1000)
    end_ms_window = int((end_d + timedelta(days=1)).timestamp() * 1000) - 1

    # Cache file suffix encodes the window (matches v4 cache reader's keying).
    suffix_window = f'_{start_ms_window}_{end_ms_window}'

    # Load wallet universe.
    df = pd.read_parquet(args.wallets_parquet)
    wallets_lc = set(df['wallet'].str.lower().unique())
    logger.info(f'Loaded {len(wallets_lc):,} wallets to filter')

    s3 = boto3.client('s3', region_name='us-east-1')

    days = []
    cur = start_d
    while cur <= end_d:
        days.append(cur.strftime('%Y%m%d'))
        cur += timedelta(days=1)
    if args.days_limit > 0:
        days = days[:args.days_limit]
    logger.info(f'Processing {len(days)} days × 24 hours = {len(days)*24} hour-files')

    ledger_buckets: dict[str, list] = defaultdict(list)
    funding_buckets: dict[str, list] = defaultdict(list)
    n_ledger_total = 0
    n_funding_total = 0
    bytes_dl = 0
    t_start = time.time()

    for day_idx, day in enumerate(days, 1):
        # Fetch all 24 hours of the day in parallel.
        with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
            futures = {ex.submit(_fetch_and_parse_hour, s3, day, h, wallets_lc, start_ms_window, end_ms_window, args.ledger_only): h for h in range(24)}
            for fut in as_completed(futures):
                ledger_entries, funding_entries, dl_bytes = fut.result()
                for u, e in ledger_entries:
                    ledger_buckets[u].append(e)
                    n_ledger_total += 1
                if not args.ledger_only:
                    for u, e in funding_entries:
                        funding_buckets[u].append(e)
                        n_funding_total += 1
                bytes_dl += dl_bytes
        if args.flush_every_day:
            nL, eL = flush_per_wallet(ledger_buckets, LEDGER_OUT_DIR, suffix_window)
            ledger_buckets.clear()
            if not args.ledger_only:
                nF, eF = flush_per_wallet(funding_buckets, FUNDING_OUT_DIR, suffix_window)
                funding_buckets.clear()
            else:
                nF, eF = 0, 0
            elapsed = time.time() - t_start
            logger.info(
                f'[{day_idx}/{len(days)}] day={day} | '
                f'ledger: +{eL:,} entries → {nL:,} wallets touched this day | '
                f'funding: +{eF:,} (skipped={args.ledger_only}) | '
                f'cum: ledger={n_ledger_total:,}, funding={n_funding_total:,} | '
                f'dl={bytes_dl/1e9:.1f}GB | elapsed={elapsed/60:.1f}min'
            )

    # Final flush
    if ledger_buckets or funding_buckets:
        flush_per_wallet(ledger_buckets, LEDGER_OUT_DIR, suffix_window)
        flush_per_wallet(funding_buckets, FUNDING_OUT_DIR, suffix_window)

    total = time.time() - t_start
    logger.info(
        f'\nDONE. Days: {len(days)}. Total ledger entries: {n_ledger_total:,}. '
        f'Total funding entries: {n_funding_total:,}. '
        f'Bytes downloaded: {bytes_dl/1e9:.1f}GB. Wall: {total/60:.1f}min.'
    )


def _fetch_and_parse_hour(s3, day, hour, wallets_lc, start_ms, end_ms, ledger_only=False):
    """Worker: fetch + parse one hour file. Returns (ledger_list, funding_list, bytes)."""
    ledger = []
    funding = []
    dl_bytes = 0
    key = f'{PREFIX}/{day}/{hour}.lz4'
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer='requester')
        raw = resp['Body'].read()
        dl_bytes = len(raw)
    except ClientError as e:
        return ledger, funding, 0
    try:
        data = lz4.frame.decompress(raw)
    except Exception as e:
        return ledger, funding, dl_bytes
    for line in data.split(b'\n'):
        if not line:
            continue
        try:
            block = json.loads(line)
        except Exception:
            continue
        block_time_str = block.get('block_time')
        if not block_time_str:
            continue
        try:
            block_time_ms = parse_block_time(block_time_str)
        except Exception:
            continue
        if block_time_ms < start_ms or block_time_ms > end_ms:
            continue
        for ev in block.get('events', []):
            inner = ev.get('inner') or {}
            ev_time = ev.get('time')
            ev_hash = ev.get('hash')
            try:
                event_time_ms = parse_block_time(ev_time) if ev_time else block_time_ms
            except Exception:
                event_time_ms = block_time_ms
            for kind_key, payload in inner.items():
                if kind_key == 'LedgerUpdate':
                    delta = payload.get('delta', {})
                    users = payload.get('users', [])
                    for u in users:
                        u_lc = u.lower() if isinstance(u, str) else ''
                        if u_lc in wallets_lc:
                            ledger.append((u_lc, {
                                'time': event_time_ms,
                                'hash': ev_hash or '',
                                'delta': delta,
                            }))
                elif kind_key == 'Funding' and not ledger_only:
                    deltas = payload.get('deltas', [])
                    for d in deltas:
                        u = d.get('user', '')
                        u_lc = u.lower() if isinstance(u, str) else ''
                        if u_lc in wallets_lc:
                            funding.append((u_lc, {
                                'time': event_time_ms,
                                'hash': ev_hash or '',
                                'delta': {
                                    'type': 'funding',
                                    'coin': d.get('coin', ''),
                                    'usdc': d.get('funding_amount', '0'),
                                    'szi': d.get('szi', '0'),
                                    'fundingRate': d.get('funding_rate', '0'),
                                    'nSamples': d.get('n_samples', 1),
                                },
                            }))
    return ledger, funding, dl_bytes


if __name__ == '__main__':
    main()
