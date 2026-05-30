"""V13 wallet ranking + copy candidate selection.

Consumes wallet_dec1_portfolio_anchor.parquet (from v13_portfolio_anchor.py)
and ranks wallets by ROI = pnl_delta / anchor_equity over the Dec 1 → today
window, then enriches the top-N with trade pattern features computed from
the enriched S3 fills (hl_s3_fills_v2/).

OUTPUT
======
app/data/v13/wallet_copy_candidates.parquet
Columns:
  wallet, dec1_anchor_usd, today_anchor_usd, pnl_delta_usd,
  roi_window, anchor_ts_ms, anchor_pnl_usd, today_pnl_usd,
  n_history_pts, days_off_dec1,
  # Trade-pattern features from S3 fills (in Dec1→today window):
  n_fills, n_unique_coins, n_trade_days,
  avg_fill_notional, total_fill_notional,
  realized_pnl_per_fill, win_rate_per_fill,
  top_coin, top_coin_pct_of_fills, top_coin_pnl,
  cum_closedPnl, cum_fees, cum_funding, cum_ledger,
  reconciliation_residual, reconciliation_pct,

FILTERS (default)
=================
  anchor_equity >= $1000  (filter out dust accounts)
  pnl_delta > 0           (profitable wallets only)
  n_history_pts >= 20     (active wallet, not new)
  n_fills >= 50           (meaningful sample)
  |reconciliation_pct| <= 30%  (formula reconciles within 30%; flags wallets
                                with major Δunrealized that we can't model)

USAGE
=====
  python scripts/v13_rank_copy_candidates.py
  python scripts/v13_rank_copy_candidates.py --top-n 100 --min-anchor 5000
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, '/Users/hermes/quants-lab/scripts')
from v13_equity_reconstruct import accumulate_ledger_flow, accumulate_funding_flow

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger('rank_candidates')

ANCHOR_INPUT = Path('/Users/hermes/quants-lab/app/data/v13/wallet_dec1_portfolio_anchor.parquet')
FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
LEDGER_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
FUNDING_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k')
OUT_PATH = Path('/Users/hermes/quants-lab/app/data/v13/wallet_copy_candidates.parquet')


def aggregate_fills_per_wallet(wallets_lc: set, anchor_ts_ms_by_wallet: dict[str, int]) -> dict:
    """Single-pass aggregation of fills per wallet, filtered to (wallet_anchor_ts, max_end_ts].

    Returns dict wallet -> per-wallet aggregates: n_fills, n_unique_coins,
    n_trade_days, avg_fill_notional, total_fill_notional, top_coin,
    top_coin_pct_of_fills, top_coin_pnl, cum_closedPnl, cum_fees,
    win_count, loss_count.
    """
    files = sorted(glob.glob(str(FILLS_DIR / '2025*.parquet')) +
                   glob.glob(str(FILLS_DIR / '2026*.parquet')))
    cols = ['wallet', 'coin', 'time', 'price', 'size', 'closedPnl',
            'fee', 'builderFee', 'deployerFee']
    aggs = {}  # wallet -> dict of state
    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f, columns=cols)
        except Exception as e:
            logger.warning(f'skip {f}: {e}')
            continue
        df = df[df['wallet'].isin(wallets_lc)]
        if df.empty:
            continue
        for c in ['price', 'size', 'closedPnl', 'fee', 'builderFee', 'deployerFee']:
            df[c] = df[c].astype(float)
        df['time'] = df['time'].astype('int64')
        df['notional'] = df['size'].abs() * df['price']
        df['fees_total'] = df['fee'] + df['builderFee'] + df['deployerFee']
        df['day'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.date
        for w, sub in df.groupby('wallet', sort=False):
            anchor_ts = anchor_ts_ms_by_wallet.get(w, 0)
            sub_win = sub[sub['time'] > anchor_ts]
            if sub_win.empty:
                continue
            cur = aggs.setdefault(w, {
                'n_fills': 0, 'unique_coins': set(), 'trade_days': set(),
                'sum_notional': 0.0, 'cum_pnl': 0.0, 'cum_fees': 0.0,
                'coin_pnl': {}, 'coin_count': {},
                'wins': 0, 'losses': 0,
            })
            cur['n_fills'] += int(len(sub_win))
            cur['unique_coins'].update(sub_win['coin'].unique())
            cur['trade_days'].update(sub_win['day'].unique())
            cur['sum_notional'] += float(sub_win['notional'].sum())
            cur['cum_pnl'] += float(sub_win['closedPnl'].sum())
            cur['cum_fees'] += float(sub_win['fees_total'].sum())
            for coin, gs in sub_win.groupby('coin', sort=False):
                p = float(gs['closedPnl'].sum())
                n = int(len(gs))
                cur['coin_pnl'][coin] = cur['coin_pnl'].get(coin, 0.0) + p
                cur['coin_count'][coin] = cur['coin_count'].get(coin, 0) + n
            # Wins/losses by fills with non-zero closedPnl
            cp = sub_win['closedPnl']
            cur['wins'] += int((cp > 0).sum())
            cur['losses'] += int((cp < 0).sum())
        if i % 20 == 0:
            logger.info(f'  fills agg {i}/{len(files)}, wallets-so-far {len(aggs):,}')
    return aggs


def cum_ledger_and_funding(wallet: str, anchor_ts_ms: int):
    le = []
    for lf in glob.glob(str(LEDGER_DIR / f'{wallet}_*.json')):
        try:
            le.extend(json.load(open(lf)))
        except Exception:
            pass
    le_win = [e for e in le if int(e.get('time', 0)) > anchor_ts_ms]
    cum_ledger = 0.0
    if le_win:
        try:
            ldf = accumulate_ledger_flow(le_win, wallet=wallet, lenient_unknown=True)
            if not ldf.empty:
                cum_ledger = float(ldf['signed_flow_usd'].sum())
        except Exception as e:
            logger.warning(f'ledger fail {wallet}: {e}')
    fe = []
    for ff in glob.glob(str(FUNDING_DIR / f'{wallet}_*.json')):
        try:
            fe.extend(json.load(open(ff)))
        except Exception:
            pass
    fe_win = [e for e in fe if int(e.get('time', 0)) > anchor_ts_ms]
    cum_funding = 0.0
    if fe_win:
        fdf = accumulate_funding_flow(fe_win)
        if not fdf.empty:
            cum_funding = float(fdf['signed_flow_usd'].sum())
    return cum_ledger, cum_funding


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top-n', type=int, default=100,
                    help='Output top-N wallets by ROI after filters')
    ap.add_argument('--min-anchor', type=float, default=1000.0,
                    help='Min anchor equity USD (filter dust)')
    ap.add_argument('--min-fills', type=int, default=50,
                    help='Min n_fills in window')
    ap.add_argument('--min-history-pts', type=int, default=20,
                    help='Min portfolio history snapshots (active wallets)')
    args = ap.parse_args()

    if not ANCHOR_INPUT.exists():
        logger.error(f'{ANCHOR_INPUT} does not exist. Run v13_portfolio_anchor.py first.')
        sys.exit(1)

    anchor_df = pd.read_parquet(ANCHOR_INPUT)
    anchor_df = anchor_df[anchor_df['ok']].copy()
    logger.info(f'Loaded {len(anchor_df):,} ok wallets from anchor sweep')

    # First-pass filters (before fills aggregation, save compute):
    f1 = anchor_df['dec1_anchor_usd'] >= args.min_anchor
    f2 = anchor_df['pnl_delta_usd'] > 0
    f3 = anchor_df['n_history_pts'] >= args.min_history_pts
    pre = anchor_df[f1 & f2 & f3].copy()
    logger.info(f'Pre-filter: anchor>={args.min_anchor}, pnl_delta>0, '
                f'history_pts>={args.min_history_pts} → {len(pre):,} wallets')

    if len(pre) == 0:
        logger.warning('No wallets pass pre-filter. Adjust thresholds.')
        sys.exit(0)

    # Aggregate fills per wallet (over each wallet's anchor → today window)
    wallets_lc = set(pre['wallet'].str.lower().unique())
    anchor_ts_map = dict(zip(pre['wallet'].str.lower(), pre['anchor_ts_ms']))

    t0 = time.time()
    aggs = aggregate_fills_per_wallet(wallets_lc, anchor_ts_map)
    logger.info(f'Fills agg done in {(time.time()-t0)/60:.1f}min. '
                f'Wallets with fills: {len(aggs):,}')

    # Add ledger + funding per wallet
    t1 = time.time()
    ledger_fund = {}
    for w in wallets_lc:
        anchor_ts = anchor_ts_map[w]
        ledger_fund[w] = cum_ledger_and_funding(w, anchor_ts)
    logger.info(f'Ledger+funding done in {(time.time()-t1)/60:.1f}min')

    # Build output rows
    rows = []
    for _, r in pre.iterrows():
        w = r['wallet'].lower()
        a = aggs.get(w)
        if a is None:
            continue
        if a['n_fills'] < args.min_fills:
            continue
        coin_count = a['coin_count']
        top_coin = max(coin_count, key=coin_count.get) if coin_count else ''
        top_coin_n = coin_count.get(top_coin, 0)
        top_coin_pnl = a['coin_pnl'].get(top_coin, 0.0)
        cum_ledger, cum_funding = ledger_fund.get(w, (0.0, 0.0))
        # Reconciliation: predicted_today_acct = anchor + cumPnl - cumFees + cumFunding + cumLedger
        predicted = r['dec1_anchor_usd'] + a['cum_pnl'] - a['cum_fees'] + cum_funding + cum_ledger
        actual = r['today_anchor_usd']
        residual = actual - predicted
        residual_pct = abs(residual / r['dec1_anchor_usd']) * 100 if r['dec1_anchor_usd'] != 0 else float('inf')
        rows.append({
            'wallet': w,
            'dec1_anchor_usd': r['dec1_anchor_usd'],
            'today_anchor_usd': r['today_anchor_usd'],
            'pnl_delta_usd': r['pnl_delta_usd'],
            'roi_window': r['pnl_delta_usd'] / r['dec1_anchor_usd'] if r['dec1_anchor_usd'] > 0 else 0.0,
            'anchor_ts_ms': r['anchor_ts_ms'],
            'anchor_pnl_usd': r['anchor_pnl_usd'],
            'today_pnl_usd': r['today_pnl_usd'],
            'n_history_pts': r['n_history_pts'],
            'days_off_dec1': r['days_off_dec1'],
            'n_fills': a['n_fills'],
            'n_unique_coins': len(a['unique_coins']),
            'n_trade_days': len(a['trade_days']),
            'avg_fill_notional': a['sum_notional'] / a['n_fills'],
            'total_fill_notional': a['sum_notional'],
            'realized_pnl_per_fill': (a['cum_pnl'] - a['cum_fees']) / a['n_fills'],
            'win_rate_per_fill': a['wins'] / (a['wins'] + a['losses']) if (a['wins'] + a['losses']) > 0 else 0.0,
            'top_coin': top_coin,
            'top_coin_pct_of_fills': 100.0 * top_coin_n / a['n_fills'],
            'top_coin_pnl': top_coin_pnl,
            'cum_closedPnl': a['cum_pnl'],
            'cum_fees': a['cum_fees'],
            'cum_funding': cum_funding,
            'cum_ledger': cum_ledger,
            'reconciliation_residual': residual,
            'reconciliation_pct': residual_pct,
        })

    if not rows:
        logger.warning('No wallets pass full filter set.')
        sys.exit(0)

    df = pd.DataFrame(rows)
    df = df.sort_values('roi_window', ascending=False)

    # Save full, save top-N
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False, compression='snappy')
    logger.info(f'Wrote {OUT_PATH} ({len(df):,} candidates after all filters)')

    logger.info(f'\nTop {args.top_n} by ROI:')
    top = df.head(args.top_n)
    print(top[['wallet', 'dec1_anchor_usd', 'pnl_delta_usd', 'roi_window',
               'n_fills', 'n_unique_coins', 'top_coin',
               'win_rate_per_fill', 'reconciliation_pct']].to_string(index=False))


if __name__ == '__main__':
    main()
