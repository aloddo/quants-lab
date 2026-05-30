"""V13 Dec 1 wallet equity anchor solver (algebraic, cash-only v1).

Per Alberto direction TG 7494 + confirmation TG 7498 (2026-05-27): instead of
extending S3 download backward to 2025-07-27, algebraically solve for each
wallet's USDC balance at the window start (2025-12-01 00:00:00 UTC) from data
we ALREADY have on disk.

IDENTITY
========
For each HL wallet, the perp account value at any time t decomposes:

    accountValue(t) = cash(t) + Σ_coin unrealized_pnl_coin(t)

Where:
    cash(t)             = USDC free + USDC locked as margin
    unrealized_pnl(t)   = Σ_coin size_coin(t) × (mark_coin(t) − entryPx_coin(t))

cash changes only via discrete cash flows:
    cash(today) = cash(Dec1)
                + Σ window  closedPnl       (realized PnL flows into cash on close)
                − Σ window  fees            (taker + maker rebate net via signed fee)
                                            (+ builderFee + deployerFee, all positive=paid)
                + Σ window  funding         (signed: negative=paid, positive=received)
                + Σ window  ledger_flows    (signed per ledger type: deposit + / withdraw -)

Therefore:
    cash(Dec1) = cash(today)
               − cum_closedPnl + cum_fees − cum_funding − cum_ledger

And the equity anchor we ultimately want:
    accountValue(Dec1) = cash(Dec1) + unrealized_dec1

V1 OUTPUT
=========
This script computes the CASH component of Dec1 equity:
    dec1_cash_anchor = today_perp_acct_value − cum_closedPnl + cum_fees
                       − cum_funding − cum_ledger
                       − (sum_unrealized_today)
                       + (sum_unrealized_dec1)   [<-- v2; v1 omits, sets =0]

The unrealized correction is OMITTED in v1. For wallets with no positions
either today or on Dec 1, the formula is EXACT. For wallets with positions at
either snapshot, the omission introduces error bounded by the unrealized PnL
on those positions (typically a few % of accountValue).

VALIDATION
==========
Wallets whose first ledger / fill activity is AFTER 2025-12-01 must yield
dec1_cash_anchor ≈ 0 (they had no HL presence on Dec 1). The script tags such
wallets and reports the distribution of dec1_cash_anchor in that subset as the
empirical accuracy bound.

INPUTS (all already on disk)
============================
- today_hl_state_cache.parquet              (wallet, perp_acct_value)
- raw_ledger_cache_20k/{wallet}_*.json      (per-wallet ledger entries)
- raw_funding_cache_20k/{wallet}_*.json     (per-wallet funding entries)
- hl_s3_fills_v2/{YYYYMMDD}.parquet         (enriched fills, all wallets)

OUTPUT
======
- app/data/v13/wallet_dec1_anchor.parquet
  Columns:
    wallet, today_perp_acct, cum_closedPnl, cum_fees,
    cum_funding, cum_ledger, n_fills_window, n_ledger_window,
    n_funding_window, first_activity_ts, post_dec1_only,
    dec1_cash_anchor
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Import sign-convention machinery from v13_equity_reconstruct (same module).
sys.path.insert(0, '/Users/hermes/quants-lab/scripts')
from v13_equity_reconstruct import (  # noqa: E402
    accumulate_ledger_flow,
    accumulate_funding_flow,
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger('dec1_anchor')

WINDOW_START = pd.Timestamp('2025-12-01', tz='UTC')
WINDOW_END = pd.Timestamp('2026-05-24 23:59:59', tz='UTC')  # last S3 day

HL_STATE_CACHE = Path('/Users/hermes/quants-lab/app/data/v13/today_hl_state_cache.parquet')
LEDGER_CACHE_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
FUNDING_CACHE_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k')
FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
OUT_PATH = Path('/Users/hermes/quants-lab/app/data/v13/wallet_dec1_anchor.parquet')


def _load_wallet_ledger(wallet: str) -> list[dict]:
    """Load + concat all ledger cache JSON files for a wallet."""
    pattern = str(LEDGER_CACHE_DIR / f'{wallet}_*.json')
    files = sorted(glob.glob(pattern))
    entries: list[dict] = []
    for f in files:
        try:
            with open(f) as fh:
                entries.extend(json.load(fh))
        except Exception as e:
            logger.warning(f'ledger load fail {f}: {e}')
    return entries


def _load_wallet_funding(wallet: str) -> list[dict]:
    pattern = str(FUNDING_CACHE_DIR / f'{wallet}_*.json')
    files = sorted(glob.glob(pattern))
    entries: list[dict] = []
    for f in files:
        try:
            with open(f) as fh:
                entries.extend(json.load(fh))
        except Exception as e:
            logger.warning(f'funding load fail {f}: {e}')
    return entries


def _per_wallet_window_aggregates(args):
    """Worker: compute per-wallet window aggregates from ledger + funding.

    Returns dict with cum_ledger, cum_funding, n_ledger, n_funding, first_activity_ts.
    Fills aggregates are computed in a separate streaming pass over the parquet
    files (one parquet read scans many wallets, more efficient).
    """
    wallet = args
    wallet_lc = wallet.lower()

    # Ledger
    ledger_entries = _load_wallet_ledger(wallet_lc)
    earliest_ts = None
    if ledger_entries:
        try:
            ts_min = min(int(e.get('time', 0)) for e in ledger_entries if int(e.get('time', 0)) > 0)
            earliest_ts = ts_min
        except ValueError:
            pass
    if ledger_entries:
        try:
            ledger_df = accumulate_ledger_flow(ledger_entries, wallet=wallet_lc, lenient_unknown=True)
        except Exception as e:
            logger.warning(f'ledger classify fail {wallet_lc}: {e}')
            ledger_df = pd.DataFrame(columns=['date', 'signed_flow_usd'])
    else:
        ledger_df = pd.DataFrame(columns=['date', 'signed_flow_usd'])

    # Window-filter ledger
    if not ledger_df.empty:
        ledger_df = ledger_df.copy()
        ledger_df['ts'] = pd.to_datetime(ledger_df['date']).dt.tz_localize('UTC')
        in_window = (ledger_df['ts'] >= WINDOW_START) & (ledger_df['ts'] <= WINDOW_END)
        cum_ledger = float(ledger_df.loc[in_window, 'signed_flow_usd'].sum())
        n_ledger = int(in_window.sum())
    else:
        cum_ledger = 0.0
        n_ledger = 0

    # Funding
    funding_entries = _load_wallet_funding(wallet_lc)
    if funding_entries:
        funding_df = accumulate_funding_flow(funding_entries)
    else:
        funding_df = pd.DataFrame(columns=['date', 'signed_flow_usd'])
    if not funding_df.empty:
        funding_df = funding_df.copy()
        funding_df['ts'] = pd.to_datetime(funding_df['date']).dt.tz_localize('UTC')
        in_window = (funding_df['ts'] >= WINDOW_START) & (funding_df['ts'] <= WINDOW_END)
        cum_funding = float(funding_df.loc[in_window, 'signed_flow_usd'].sum())
        n_funding = int(in_window.sum())
    else:
        cum_funding = 0.0
        n_funding = 0

    return {
        'wallet': wallet_lc,
        'cum_ledger': cum_ledger,
        'cum_funding': cum_funding,
        'n_ledger_window': n_ledger,
        'n_funding_window': n_funding,
        'first_activity_ts': earliest_ts,
    }


def aggregate_fills_window(wallets_lc: set) -> pd.DataFrame:
    """Single-pass aggregation of enriched fills over the window.

    Returns DataFrame: wallet, cum_closedPnl, cum_fees, n_fills_window,
    earliest_fill_ts.
    """
    cols = ['wallet', 'time', 'closedPnl', 'fee', 'builderFee', 'deployerFee']
    files = sorted(glob.glob(str(FILLS_DIR / '2025*.parquet')) +
                   glob.glob(str(FILLS_DIR / '2026*.parquet')))
    # Window-filter by filename date
    start_str = WINDOW_START.strftime('%Y%m%d')
    end_str = WINDOW_END.strftime('%Y%m%d')
    files = [f for f in files if start_str <= Path(f).stem <= end_str]
    logger.info(f'Streaming {len(files)} fill files for {len(wallets_lc):,} wallets')

    sums = {}  # wallet -> [pnl, fees, n, earliest_ts]
    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f, columns=cols)
        except Exception as e:
            logger.warning(f'fills load fail {f}: {e}')
            continue
        # Filter to our wallet set
        df = df[df['wallet'].isin(wallets_lc)]
        if df.empty:
            continue
        # Cast
        for c in ['closedPnl', 'fee', 'builderFee', 'deployerFee']:
            df[c] = df[c].astype(float)
        df['time'] = df['time'].astype('int64')
        df['fees_total'] = df['fee'] + df['builderFee'] + df['deployerFee']
        for w, sub in df.groupby('wallet', sort=False):
            cur = sums.setdefault(w, [0.0, 0.0, 0, None])
            cur[0] += float(sub['closedPnl'].sum())
            cur[1] += float(sub['fees_total'].sum())
            cur[2] += int(len(sub))
            ts_min = int(sub['time'].min())
            if cur[3] is None or ts_min < cur[3]:
                cur[3] = ts_min
        if i % 20 == 0:
            logger.info(f'  [{i}/{len(files)}] {Path(f).name} | wallets-with-fills so far: {len(sums):,}')

    rows = [{'wallet': w, 'cum_closedPnl': v[0], 'cum_fees': v[1],
             'n_fills_window': v[2], 'earliest_fill_ts': v[3]}
            for w, v in sums.items()]
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n-workers', type=int, default=8)
    ap.add_argument('--wallets-limit', type=int, default=0,
                    help='Sample: only first N wallets. 0=all.')
    args = ap.parse_args()

    t_start = time.time()

    # 1) Today HL state cache
    today_state = pd.read_parquet(HL_STATE_CACHE)
    today_state['wallet'] = today_state['wallet'].str.lower()
    logger.info(f'Loaded today HL state: {len(today_state):,} wallets')

    # Use only ok=True wallets (failed-query wallets get NaN downstream).
    valid = today_state[today_state['ok']].copy()
    wallets = list(valid['wallet'].unique())
    if args.wallets_limit > 0:
        wallets = wallets[:args.wallets_limit]
    logger.info(f'Wallets to process: {len(wallets):,}')
    wallets_set = set(wallets)

    # 2) Aggregate fills (single pass over 175 parquets)
    t = time.time()
    fills_agg = aggregate_fills_window(wallets_set)
    logger.info(f'Fills aggregation done in {(time.time() - t) / 60:.1f}min. '
                f'Wallets with fills: {len(fills_agg):,}')

    # 3) Aggregate ledger + funding per wallet in parallel
    t = time.time()
    rows = []
    with ProcessPoolExecutor(max_workers=args.n_workers) as ex:
        futures = {ex.submit(_per_wallet_window_aggregates, w): w for w in wallets}
        for j, fut in enumerate(as_completed(futures), 1):
            try:
                rows.append(fut.result())
            except Exception as e:
                w = futures[fut]
                logger.warning(f'wallet worker fail {w}: {e}')
            if j % 1000 == 0:
                logger.info(f'  ledger/funding {j}/{len(wallets)}')
    ledger_agg = pd.DataFrame(rows)
    logger.info(f'Ledger+funding aggregation done in {(time.time() - t) / 60:.1f}min')

    # 4) Merge everything
    df = pd.merge(valid[['wallet', 'perp_acct_value', 'spot_usdc']],
                  fills_agg, on='wallet', how='left')
    df = pd.merge(df, ledger_agg, on='wallet', how='left')

    # NaN -> 0 for wallets with no fills / no ledger / no funding
    for c in ['cum_closedPnl', 'cum_fees', 'n_fills_window', 'earliest_fill_ts',
              'cum_ledger', 'cum_funding', 'n_ledger_window', 'n_funding_window',
              'first_activity_ts']:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    df = df.rename(columns={'perp_acct_value': 'today_perp_acct'})

    # First activity = min(first ledger ts, earliest fill ts)
    df['first_activity_ts'] = df[['first_activity_ts', 'earliest_fill_ts']].max(axis=1)
    # min, not max (we want earliest). max picks 0 when other is non-zero...
    def _first_ts(row):
        a = row['first_activity_ts']
        b = row['earliest_fill_ts']
        cands = [x for x in (a, b) if x is not None and x > 0]
        return min(cands) if cands else 0
    df['first_activity_ts'] = df.apply(_first_ts, axis=1)
    df['post_dec1_only'] = df['first_activity_ts'] >= WINDOW_START.timestamp() * 1000

    # 5) Apply identity
    # dec1_cash_anchor = today_perp_acct - cum_closedPnl + cum_fees
    #                    - cum_funding - cum_ledger
    # (v1: omits unrealized correction)
    df['dec1_cash_anchor'] = (
        df['today_perp_acct']
        - df['cum_closedPnl']
        + df['cum_fees']
        - df['cum_funding']
        - df['cum_ledger']
    )

    # 6) Write parquet
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix('.parquet.tmp')
    df.to_parquet(tmp, index=False, compression='snappy')
    tmp.replace(OUT_PATH)
    logger.info(f'\nWrote {OUT_PATH} ({len(df):,} rows)')

    # 7) Validation: post-Dec1-only wallets should have dec1_cash_anchor ≈ 0
    post = df[df['post_dec1_only']]
    logger.info(f'\n=== Validation on {len(post):,} post-Dec1-only wallets ===')
    if len(post):
        a = post['dec1_cash_anchor']
        logger.info(f'  median: ${a.median():>12,.2f}')
        logger.info(f'  mean:   ${a.mean():>12,.2f}')
        logger.info(f'  p25:    ${a.quantile(0.25):>12,.2f}')
        logger.info(f'  p75:    ${a.quantile(0.75):>12,.2f}')
        logger.info(f'  |a|<1:    {(a.abs() < 1).sum():>6,} / {len(a):,} '
                    f'({(a.abs() < 1).mean() * 100:.1f}%)')
        logger.info(f'  |a|<10:   {(a.abs() < 10).sum():>6,} / {len(a):,} '
                    f'({(a.abs() < 10).mean() * 100:.1f}%)')
        logger.info(f'  |a|<100:  {(a.abs() < 100).sum():>6,} / {len(a):,} '
                    f'({(a.abs() < 100).mean() * 100:.1f}%)')
        logger.info(f'  |a|<1000: {(a.abs() < 1000).sum():>6,} / {len(a):,} '
                    f'({(a.abs() < 1000).mean() * 100:.1f}%)')
        logger.info(f'  min:    ${a.min():>12,.2f}')
        logger.info(f'  max:    ${a.max():>12,.2f}')

    # Full-population distribution
    a = df['dec1_cash_anchor']
    logger.info(f'\n=== Full population: {len(df):,} wallets ===')
    logger.info(f'  median: ${a.median():>14,.2f}')
    logger.info(f'  mean:   ${a.mean():>14,.2f}')
    logger.info(f'  p25:    ${a.quantile(0.25):>14,.2f}')
    logger.info(f'  p75:    ${a.quantile(0.75):>14,.2f}')
    logger.info(f'  p99:    ${a.quantile(0.99):>14,.2f}')
    logger.info(f'  min:    ${a.min():>14,.2f}')
    logger.info(f'  max:    ${a.max():>14,.2f}')
    logger.info(f'  negatives: {(a < 0).sum():,} / {len(a):,} '
                f'({(a < 0).mean() * 100:.1f}%)')

    logger.info(f'\nDONE. Total wall: {(time.time() - t_start) / 60:.1f}min')


if __name__ == '__main__':
    main()
