"""V13 S3 enriched fills downloader (Alberto direction 2026-05-27).

Replaces existing app/data/hl_s3_fills/ (which strips fee/startPosition/etc)
with a complete-schema parquet that preserves HL's authoritative per-fill data
needed for professional-grade equity reconstruction.

Source: hl-mainnet-node-data/node_fills_by_block/hourly/YYYYMMDD/H.lz4
  - JSONL, one block per line
  - events: array of [wallet, fill_dict] pairs
  - fill_dict shape:
    {
      "coin": "...", "px": str, "sz": str, "side": "A"|"B",
      "time": int_ms, "startPosition": str,
      "dir": "Open Short" | "Close Short" | "Open Long" | "Close Long" | ...,
      "closedPnl": str,    # GROSS realized PnL (does not include fees)
      "fee": str,          # SIGNED. -value=maker rebate, +value=taker fee paid.
      "feeToken": "USDC" | other,
      "builderFee": str,   # routing fee paid to builder (positive=paid)
      "deployerFee": str,  # deployer fee (positive=paid)
      "crossed": bool,     # True=taker, False=maker
      "hash": str, "oid": int, "tid": int, "cloid": str | null,
      "twapId": int | null, "builder": "0x...",
    }

Output: app/data/hl_s3_fills_v2/{YYYYMMDD}.parquet
  Schema: wallet, coin, side, size, price, time, dir, closedPnl, hash,
          startPosition, fee, builderFee, deployerFee, feeToken, crossed,
          notional, source.

NET realized PnL per fill = closedPnl - abs(fee_if_positive) + abs(fee_if_negative_as_rebate) - builderFee - deployerFee
                          = closedPnl - fee - builderFee - deployerFee
  (fee is signed: negative for maker rebate = +profit; positive for taker fee = -profit)
  All four fields are in USDC (when feeToken == "USDC").

Stream + discard: filter to 20K wallet set on the fly, write enriched per-day
parquet. Never persist raw lz4.
"""
from __future__ import annotations

import argparse
import json
import logging
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
logger = logging.getLogger('s3_fills_dl')

BUCKET = 'hl-mainnet-node-data'
PREFIX = 'node_fills_by_block/hourly'
OUT_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')


def _fetch_and_parse_hour(s3, day, hour, wallets_lc):
    """Worker: fetch + parse one hour file. Returns (list_of_fill_dicts, bytes)."""
    key = f'{PREFIX}/{day}/{hour}.lz4'
    fills = []
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer='requester')
        raw = resp['Body'].read()
        dl_bytes = len(raw)
    except ClientError:
        return fills, 0
    try:
        data = lz4.frame.decompress(raw)
    except Exception:
        return fills, dl_bytes
    for line in data.split(b'\n'):
        if not line:
            continue
        try:
            block = json.loads(line)
        except Exception:
            continue
        for ev in block.get('events', []):
            # Schema: [wallet_addr, fill_dict]
            if not isinstance(ev, list) or len(ev) != 2:
                continue
            wallet, fd = ev
            if not isinstance(wallet, str):
                continue
            w_lc = wallet.lower()
            if w_lc not in wallets_lc:
                continue
            if not isinstance(fd, dict):
                continue
            try:
                fills.append({
                    'wallet': w_lc,
                    'coin': fd.get('coin', ''),
                    'side': fd.get('side', ''),
                    'size': fd.get('sz', '0'),
                    'price': fd.get('px', '0'),
                    'time': int(fd.get('time', 0)),
                    'dir': fd.get('dir', ''),
                    'closedPnl': fd.get('closedPnl', '0'),
                    'startPosition': fd.get('startPosition', '0'),
                    'fee': fd.get('fee', '0'),
                    'feeToken': fd.get('feeToken', ''),
                    'builderFee': fd.get('builderFee', '0'),
                    'deployerFee': fd.get('deployerFee', '0'),
                    'crossed': bool(fd.get('crossed', False)),
                    'hash': fd.get('hash', ''),
                    'oid': int(fd.get('oid', 0)),
                    'tid': int(fd.get('tid', 0)),
                    'cloid': fd.get('cloid', '') or '',
                    'twapId': fd.get('twapId') if fd.get('twapId') is not None else 0,
                    'builder': fd.get('builder', '') or '',
                })
            except Exception as e:
                logger.warning(f'parse err {key}: {e}')
                continue
    return fills, dl_bytes


def flush_day(fills_for_day: list, day: str):
    """Write a day's filtered fills to parquet."""
    if not fills_for_day:
        # write empty marker
        df = pd.DataFrame(columns=[
            'wallet', 'coin', 'side', 'size', 'price', 'time', 'dir',
            'closedPnl', 'startPosition', 'fee', 'feeToken', 'builderFee',
            'deployerFee', 'crossed', 'hash', 'oid', 'tid', 'cloid',
            'twapId', 'builder', 'notional', 'source',
        ])
    else:
        df = pd.DataFrame(fills_for_day)
        # Compute notional (size * price)
        df['notional'] = df['size'].astype(float) * df['price'].astype(float)
        df['source'] = 's3_node_fills_by_block_v2'
    out_path = OUT_DIR / f'{day}.parquet'
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix('.parquet.tmp')
    df.to_parquet(tmp, index=False, compression='snappy')
    tmp.replace(out_path)
    return len(df)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2025-12-01')
    ap.add_argument('--end', default='2026-05-24')
    ap.add_argument('--wallets-parquet',
                    default='/Users/hermes/quants-lab/app/data/v13/equity_universe_20k.parquet')
    ap.add_argument('--n-workers', type=int, default=4)
    ap.add_argument('--days-limit', type=int, default=0, help='Smoke: only N days. 0=all.')
    args = ap.parse_args()

    df = pd.read_parquet(args.wallets_parquet)
    wallets_lc = set(df['wallet'].str.lower().unique())
    logger.info(f'Loaded {len(wallets_lc):,} wallets to filter')

    s3 = boto3.client('s3', region_name='us-east-1')

    start_d = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_d = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    days = []
    cur = start_d
    while cur <= end_d:
        days.append(cur.strftime('%Y%m%d'))
        cur += timedelta(days=1)
    if args.days_limit > 0:
        days = days[:args.days_limit]
    logger.info(f'Processing {len(days)} days × 24 hours = {len(days)*24} hour-files')

    n_total = 0
    bytes_dl = 0
    t_start = time.time()

    for day_idx, day in enumerate(days, 1):
        day_fills = []
        with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
            futures = {ex.submit(_fetch_and_parse_hour, s3, day, h, wallets_lc): h for h in range(24)}
            for fut in as_completed(futures):
                fills, dl_bytes = fut.result()
                day_fills.extend(fills)
                bytes_dl += dl_bytes
        n_written = flush_day(day_fills, day)
        n_total += n_written
        elapsed = time.time() - t_start
        logger.info(
            f'[{day_idx}/{len(days)}] day={day} | '
            f'fills_written: {n_written:,} | '
            f'cum_fills: {n_total:,} | '
            f'dl={bytes_dl/1e9:.1f}GB | elapsed={elapsed/60:.1f}min'
        )

    total = time.time() - t_start
    logger.info(
        f'\nDONE. Days: {len(days)}. Total fills: {n_total:,}. '
        f'Bytes downloaded: {bytes_dl/1e9:.1f}GB. Wall: {total/60:.1f}min.'
    )


if __name__ == '__main__':
    main()
