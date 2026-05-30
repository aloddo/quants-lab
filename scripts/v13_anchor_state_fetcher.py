"""Fetch + cache per-wallet anchor state for v13 unified walker.

For each wallet, queries HL clearinghouseState for main + xyz + flx dexes.
Captures: totalRawUsd, accountValue per dex; per-coin signed_pos + entry_px.

Output: app/data/v13/wallet_anchor_state.parquet
Columns:
  wallet, dex, totalRawUsd, accountValue, n_positions, fetched_at_ts, ok
  positions_json (JSON string with per-coin szi + entryPx + positionValue)

Usage:
  python scripts/v13_anchor_state_fetcher.py --n-workers 10
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s', stream=sys.stdout)
logger = logging.getLogger('anchor_state_fetcher')

HL_URL = 'https://api.hyperliquid.xyz/info'
UNIVERSE_PARQUET = Path('/Users/hermes/quants-lab/app/data/v13/equity_universe_20k.parquet')
OUT_PATH = Path('/Users/hermes/quants-lab/app/data/v13/wallet_anchor_state.parquet')

DEXES = [None, 'xyz', 'flx']  # None = main


def query_dex_state(wallet: str, dex: str | None, sess: requests.Session, max_retries: int = 6) -> dict | None:
    body = {'type': 'clearinghouseState', 'user': wallet}
    if dex is not None:
        body['dex'] = dex
    backoff = 1.5
    for attempt in range(max_retries):
        try:
            r = sess.post(HL_URL, json=body, timeout=20)
            if r.status_code == 429:
                time.sleep(backoff); backoff = min(backoff * 1.5, 30); continue
            if r.status_code != 200:
                return None
            return r.json()
        except requests.exceptions.RequestException:
            time.sleep(backoff); backoff = min(backoff * 1.5, 30)
    return None


def fetch_one_wallet(wallet: str, sess: requests.Session) -> list[dict]:
    """Returns list of dicts, one per dex."""
    rows = []
    for dex in DEXES:
        data = query_dex_state(wallet, dex, sess)
        if data is None:
            rows.append({'wallet': wallet, 'dex': dex or 'main', 'ok': False,
                         'totalRawUsd': 0.0, 'accountValue': 0.0, 'n_positions': 0,
                         'positions_json': '[]', 'fetched_at_ts': time.time()})
            continue
        ms = data.get('marginSummary', {})
        positions = []
        for p in data.get('assetPositions', []):
            pos = p['position']
            sz = float(pos['szi'])
            if abs(sz) > 1e-9:
                positions.append({
                    'coin': pos['coin'],
                    'szi': sz,
                    'entryPx': float(pos.get('entryPx', 0) or 0),
                    'positionValue': float(pos.get('positionValue', 0)),
                    'unrealizedPnl': float(pos.get('unrealizedPnl', 0)),
                })
        rows.append({
            'wallet': wallet,
            'dex': dex or 'main',
            'ok': True,
            'totalRawUsd': float(ms.get('totalRawUsd', 0)),
            'accountValue': float(ms.get('accountValue', 0)),
            'n_positions': len(positions),
            'positions_json': json.dumps(positions),
            'fetched_at_ts': time.time(),
        })
        time.sleep(0.05)  # spacing between dex calls
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n-workers', type=int, default=10)
    ap.add_argument('--wallets-limit', type=int, default=0)
    args = ap.parse_args()

    # Load 20K wallet universe
    df = pd.read_parquet(UNIVERSE_PARQUET)
    wallets = sorted(df['wallet'].str.lower().unique())
    if args.wallets_limit > 0:
        wallets = wallets[:args.wallets_limit]
    logger.info(f'Fetching anchor state for {len(wallets):,} wallets × 3 dexes via {args.n_workers} workers')

    session = requests.Session()
    rows = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
        futs = {ex.submit(fetch_one_wallet, w, session): w for w in wallets}
        for j, fut in enumerate(as_completed(futs), 1):
            try:
                rows.extend(fut.result())
            except Exception as e:
                logger.warning(f'wallet {futs[fut][:10]} fail: {e}')
            if j % 500 == 0:
                elapsed = time.time() - t0
                rate = j / elapsed
                eta_min = (len(wallets) - j) / rate / 60
                logger.info(f'  [{j}/{len(wallets)}] rate={rate:.1f} wallets/s eta={eta_min:.1f}min')

    df_out = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(OUT_PATH, index=False, compression='snappy')
    elapsed = time.time() - t0
    logger.info(f'\nWrote {OUT_PATH}: {len(df_out):,} rows in {elapsed/60:.1f}min')
    ok = df_out[df_out['ok']]
    logger.info(f'  ok rows: {len(ok):,} ({len(ok)/len(df_out)*100:.1f}%)')
    logger.info(f'  unique wallets: {df_out["wallet"].nunique():,}')


if __name__ == '__main__':
    main()
