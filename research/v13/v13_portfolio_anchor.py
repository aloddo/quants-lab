"""V13 Dec 1 wallet equity anchor — direct query via HL portfolio API.

Per Alberto direction TG 7500 + 7501 (2026-05-27): forget the algebra. HL's
info.portfolio endpoint exposes accountValueHistory directly. Extract the
snapshot nearest Dec 1 2025 for each wallet — that IS the Dec 1 equity anchor
for backtest position sizing.

ENDPOINT
========
POST https://api.hyperliquid.xyz/info
Body: {"type": "portfolio", "user": "0x..."}
Returns: list of [timeframe, body]
  where body has accountValueHistory: list of [ms_ts, "value_str"]
  Timeframes: day, week, month, allTime, perpDay, perpWeek, perpMonth, perpAllTime

For Dec 1 (~6 months ago) we use 'perpAllTime' which spans ~210d with ~7d
median gap. The closest snapshot to Dec 1 is 0-3.5 days off (acceptable for
backtest sizing).

We use perpAllTime (NOT allTime) because:
  - allTime includes spot token MTM (e.g. wallets holding HYPE airdrop see
    spot value grow massively, contaminating the perp equity series).
  - perpAllTime is the perp account value only — what copy-trading sizing
    actually needs.

OUTPUT
======
app/data/v13/wallet_dec1_portfolio_anchor.parquet
Columns:
  wallet, dec1_anchor_usd, anchor_ts_ms, days_off_dec1,
  today_anchor_usd, today_anchor_ts_ms, n_history_pts,
  earliest_history_ts, ok, error_msg

RATE LIMITING
=============
HL info endpoint allows ~20 req/sec per IP (empirical). We use 10 workers
with no explicit sleep — should run ~50 req/sec aggregate, well under HL's
soft limits. 20K wallets / 50 = ~7 minutes wall time.

If we hit 429 we back off exponentially per worker.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger('portfolio_anchor')

HL_URL = 'https://api.hyperliquid.xyz/info'
HL_STATE_CACHE = Path('/Users/hermes/quants-lab/app/data/v13/today_hl_state_cache.parquet')
OUT_PATH = Path('/Users/hermes/quants-lab/app/data/v13/wallet_dec1_portfolio_anchor.parquet')

DEC1_TS_MS = int(pd.Timestamp('2025-12-01', tz='UTC').timestamp() * 1000)
TIMEFRAME = 'perpAllTime'


def query_one(wallet: str, session: requests.Session, max_retries: int = 8) -> dict:
    """Fetch portfolio for one wallet; return parsed anchor record."""
    backoff = 2.0
    for attempt in range(max_retries):
        try:
            r = session.post(HL_URL,
                             json={'type': 'portfolio', 'user': wallet},
                             timeout=30)
            if r.status_code == 429:
                time.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)
                continue
            if r.status_code != 200:
                return {'wallet': wallet, 'ok': False,
                        'error_msg': f'http {r.status_code}'}
            data = r.json()
            break
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                return {'wallet': wallet, 'ok': False,
                        'error_msg': f'request fail: {e}'}
            time.sleep(backoff)
            backoff *= 2
    else:
        return {'wallet': wallet, 'ok': False,
                'error_msg': 'max retries (429)'}

    # Extract perpAllTime: accountValueHistory AND pnlHistory at same timestamps.
    # Both are required for exact reconciliation (HL emits them together).
    avh = []
    pnl = []
    for tf, body in data:
        if tf == TIMEFRAME:
            avh = body.get('accountValueHistory', [])
            pnl = body.get('pnlHistory', [])
            break

    if not avh:
        return {'wallet': wallet, 'ok': False,
                'error_msg': f'no {TIMEFRAME} history'}
    if pnl and len(pnl) != len(avh):
        return {'wallet': wallet, 'ok': False,
                'error_msg': f'pnlHistory length mismatch'}

    # Closest snapshot to Dec 1 (use that exact timestamp as anchor).
    anchor_idx = min(range(len(avh)), key=lambda i: abs(avh[i][0] - DEC1_TS_MS))
    anchor = avh[anchor_idx]
    last = avh[-1]
    anchor_pnl = float(pnl[anchor_idx][1]) if pnl else float('nan')
    last_pnl = float(pnl[-1][1]) if pnl else float('nan')
    return {
        'wallet': wallet,
        'dec1_anchor_usd': float(anchor[1]),
        'anchor_ts_ms': int(anchor[0]),
        'days_off_dec1': (anchor[0] - DEC1_TS_MS) / 86400_000.0,
        'anchor_pnl_usd': anchor_pnl,
        'today_anchor_usd': float(last[1]),
        'today_anchor_ts_ms': int(last[0]),
        'today_pnl_usd': last_pnl,
        'pnl_delta_usd': last_pnl - anchor_pnl,
        'n_history_pts': len(avh),
        'earliest_history_ts': int(avh[0][0]),
        'ok': True,
        'error_msg': '',
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n-workers', type=int, default=10)
    ap.add_argument('--wallets-limit', type=int, default=0)
    args = ap.parse_args()

    state = pd.read_parquet(HL_STATE_CACHE)
    state['wallet'] = state['wallet'].str.lower()
    wallets = list(state.loc[state['ok'], 'wallet'].unique())
    if args.wallets_limit > 0:
        wallets = wallets[:args.wallets_limit]
    logger.info(f'Querying portfolio for {len(wallets):,} wallets via {args.n_workers} workers')

    session = requests.Session()
    rows = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
        futs = {ex.submit(query_one, w, session): w for w in wallets}
        for j, fut in enumerate(as_completed(futs), 1):
            rows.append(fut.result())
            if j % 1000 == 0:
                elapsed = time.time() - t0
                rate = j / elapsed
                eta = (len(wallets) - j) / rate / 60
                logger.info(f'  [{j}/{len(wallets)}] rate={rate:.1f}/s eta={eta:.1f}min')

    df = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix('.parquet.tmp')
    df.to_parquet(tmp, index=False, compression='snappy')
    tmp.replace(OUT_PATH)

    elapsed = time.time() - t0
    logger.info(f'\nWrote {OUT_PATH} ({len(df):,} rows) in {elapsed/60:.1f}min')
    logger.info(f'  ok={df["ok"].sum():,} | fail={(~df["ok"]).sum():,}')
    if 'dec1_anchor_usd' in df.columns:
        ok_df = df[df['ok']]
        logger.info(f'\n  Dec 1 anchor distribution ({len(ok_df):,} ok wallets):')
        a = ok_df['dec1_anchor_usd']
        logger.info(f'    median: ${a.median():>14,.2f}')
        logger.info(f'    mean:   ${a.mean():>14,.2f}')
        logger.info(f'    p25:    ${a.quantile(0.25):>14,.2f}')
        logger.info(f'    p75:    ${a.quantile(0.75):>14,.2f}')
        logger.info(f'    p99:    ${a.quantile(0.99):>14,.2f}')
        logger.info(f'    max:    ${a.max():>14,.2f}')
        logger.info(f'    zero:   {(a == 0).sum():,} ({(a == 0).mean() * 100:.1f}%)')
        # PnL delta (window)
        if 'pnl_delta_usd' in df.columns:
            d = ok_df['pnl_delta_usd'].dropna()
            logger.info(f'\n  PnL delta (anchor → last snapshot) on {len(d):,} wallets:')
            logger.info(f'    median: ${d.median():>14,.2f}')
            logger.info(f'    mean:   ${d.mean():>14,.2f}')
            logger.info(f'    p25:    ${d.quantile(0.25):>14,.2f}')
            logger.info(f'    p75:    ${d.quantile(0.75):>14,.2f}')
            logger.info(f'    p99:    ${d.quantile(0.99):>14,.2f}')
            logger.info(f'    max:    ${d.max():>14,.2f}')
            logger.info(f'    min:    ${d.min():>14,.2f}')
            logger.info(f'    positive: {(d > 0).sum():,} ({(d > 0).mean()*100:.1f}%)')
        # Days off distribution
        d = ok_df['days_off_dec1'].abs()
        logger.info(f'\n  |days off Dec 1| distribution:')
        logger.info(f'    median: {d.median():.2f}d')
        logger.info(f'    p99:    {d.quantile(0.99):.2f}d')
        logger.info(f'    max:    {d.max():.2f}d')


if __name__ == '__main__':
    main()
