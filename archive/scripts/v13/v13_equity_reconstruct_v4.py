"""V13 equity reconstruct v4 — spot USDC model (cash-flow only).

Per Alberto direction TG 7577 + 7579 (2026-05-28): HL TOTAL EQUITY = spot USDC.
Forget perp accountValue, forget cost_basis, forget mark-to-market. Equity is
the wallet's spot USDC total, which evolves by these cash flows ONLY:

  spot_USDC(t) = spot_USDC(today)
               - cum_closedPnl_window(t..today)
               + cum_fees_window(t..today)
               - cum_funding_window(t..today)
               - cum_ledger_signed_window(t..today)

Where:
  cum_closedPnl: sum of HL's per-fill closedPnl (realized PnL flowed to spot)
  cum_fees:      sum of (fee + builderFee + deployerFee) per fill
  cum_funding:   sum of signed funding payments (negative=paid, positive=received)
  cum_ledger:    sum of signed external ledger flows (deposits +, withdrawals -)

VALIDATION (parent wallet 0x11ca20aeb7cd, 15.2 days back):
  walked $534.63 vs API $535.44, drift -$0.81 (0.15%)

INPUTS
======
- today_hl_state_cache.parquet (spot_usdc per wallet, populated daily)
- hl_s3_fills_v2/{YYYYMMDD}.parquet (enriched fills with fee + builderFee + deployerFee)
- raw_ledger_cache_20k/{wallet}_*.json
- raw_funding_cache_20k/{wallet}_*.json

OUTPUT
======
- wallet_equity_series_v4.parquet
  Columns: wallet, date, spot_usdc_equity, cum_closedPnl, cum_fees, cum_funding,
           cum_ledger, n_fills_window
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

sys.path.insert(0, '/Users/hermes/quants-lab/scripts')
from v13_equity_reconstruct import accumulate_ledger_flow, accumulate_funding_flow

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s', stream=sys.stdout)
logger = logging.getLogger('equity_v4')

FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
LEDGER_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
FUNDING_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k')
TODAY_HL_STATE = Path('/Users/hermes/quants-lab/app/data/v13/today_hl_state_cache.parquet')
DEFAULT_OUTPUT = Path('/Users/hermes/quants-lab/app/data/v13/wallet_equity_series_v4.parquet')

WINDOW_START_DEFAULT = '2025-12-01'
WINDOW_END_DEFAULT = '2026-05-27'


def aggregate_fills_per_wallet_day(wallets_lc: set, win_start_ms: int, win_end_ms: int) -> pd.DataFrame:
    """Single pass over S3 fills, aggregate per (wallet, date) for daily PnL + fees."""
    files = sorted(glob.glob(str(FILLS_DIR / '2025*.parquet')) + glob.glob(str(FILLS_DIR / '2026*.parquet')))
    cols = ['wallet', 'time', 'closedPnl', 'fee', 'builderFee', 'deployerFee']
    chunks = []
    for i, f in enumerate(files, 1):
        try:
            df = pd.read_parquet(f, columns=cols)
        except Exception as e:
            logger.warning(f'skip {f}: {e}')
            continue
        df = df[df['wallet'].isin(wallets_lc)]
        if df.empty: continue
        for c in ['closedPnl', 'fee', 'builderFee', 'deployerFee']:
            df[c] = df[c].astype(float)
        df['time'] = df['time'].astype('int64')
        df = df[(df['time'] >= win_start_ms) & (df['time'] <= win_end_ms)]
        if df.empty: continue
        df['fees_total'] = df['fee'] + df['builderFee'] + df['deployerFee']
        df['date'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.floor('D').dt.date
        agg = df.groupby(['wallet', 'date']).agg(
            cum_closedPnl_day=('closedPnl', 'sum'),
            cum_fees_day=('fees_total', 'sum'),
            n_fills_day=('time', 'count'),
        ).reset_index()
        chunks.append(agg)
        if i % 20 == 0:
            logger.info(f'  fills agg {i}/{len(files)} | partial rows: {sum(len(c) for c in chunks):,}')
    if not chunks:
        return pd.DataFrame(columns=['wallet', 'date', 'cum_closedPnl_day', 'cum_fees_day', 'n_fills_day'])
    out = pd.concat(chunks, ignore_index=True)
    # Re-aggregate (different files may have same wallet × date)
    out = out.groupby(['wallet', 'date']).agg(
        cum_closedPnl_day=('cum_closedPnl_day', 'sum'),
        cum_fees_day=('cum_fees_day', 'sum'),
        n_fills_day=('n_fills_day', 'sum'),
    ).reset_index()
    return out


def per_wallet_ledger_funding(wallet: str, win_start_ms: int, win_end_ms: int) -> tuple[dict, dict]:
    """Returns (daily_ledger_dict, daily_funding_dict) — date -> signed_flow_usd."""
    ledger_entries = []
    for lf in glob.glob(str(LEDGER_DIR / f'{wallet}_*.json')):
        try:
            ledger_entries.extend(json.load(open(lf)))
        except Exception:
            pass
    # Dedup by hash + time
    seen = set()
    le_uniq = []
    for e in ledger_entries:
        key = (e.get('time'), e.get('hash', ''))
        if key in seen: continue
        seen.add(key)
        le_uniq.append(e)
    le_win = [e for e in le_uniq if win_start_ms <= int(e.get('time', 0)) <= win_end_ms]

    daily_ledger = {}
    if le_win:
        try:
            ldf = accumulate_ledger_flow(le_win, wallet=wallet, lenient_unknown=True)
            if not ldf.empty:
                ldf['date_d'] = pd.to_datetime(ldf['date']).dt.date if not isinstance(ldf['date'].iloc[0], type(pd.Timestamp(0).date())) else ldf['date']
                for d, v in ldf.groupby('date_d')['signed_flow_usd'].sum().items():
                    daily_ledger[d] = float(v)
        except Exception as e:
            logger.warning(f'ledger classify fail {wallet[:10]}: {e}')

    funding_entries = []
    for ff in glob.glob(str(FUNDING_DIR / f'{wallet}_*.json')):
        try:
            funding_entries.extend(json.load(open(ff)))
        except Exception:
            pass
    seen = set()
    fe_uniq = []
    for e in funding_entries:
        key = (e.get('time'), e.get('hash', ''))
        if key in seen: continue
        seen.add(key)
        fe_uniq.append(e)
    fe_win = [e for e in fe_uniq if win_start_ms <= int(e.get('time', 0)) <= win_end_ms]

    daily_funding = {}
    if fe_win:
        try:
            fdf = accumulate_funding_flow(fe_win)
            if not fdf.empty:
                for d, v in fdf.groupby('date')['signed_flow_usd'].sum().items():
                    daily_funding[d] = float(v)
        except Exception as e:
            logger.warning(f'funding classify fail {wallet[:10]}: {e}')

    return daily_ledger, daily_funding


def reconstruct_one_wallet(args) -> pd.DataFrame | None:
    """Per-wallet worker. Returns daily series."""
    wallet, spot_today, win_start_ms, win_end_ms, fills_per_day_dict = args
    win_start = pd.Timestamp(win_start_ms, unit='ms', tz='UTC').floor('D')
    win_end = pd.Timestamp(win_end_ms, unit='ms', tz='UTC').floor('D')
    date_range = pd.date_range(win_start.date(), win_end.date(), freq='D').date

    # Build daily series
    daily = pd.DataFrame({'date': date_range})
    daily['cum_closedPnl_day'] = 0.0
    daily['cum_fees_day'] = 0.0
    daily['n_fills_day'] = 0
    daily['ledger_signed_day'] = 0.0
    daily['funding_signed_day'] = 0.0

    # Fills
    for d, vals in fills_per_day_dict.items():
        if d in date_range:
            idx = daily.index[daily['date'] == d]
            if len(idx) > 0:
                daily.loc[idx[0], 'cum_closedPnl_day'] = vals['pnl']
                daily.loc[idx[0], 'cum_fees_day'] = vals['fees']
                daily.loc[idx[0], 'n_fills_day'] = vals['n']

    # Ledger + funding (per-wallet)
    daily_ledger, daily_funding = per_wallet_ledger_funding(wallet, win_start_ms, win_end_ms)
    for d, v in daily_ledger.items():
        idx = daily.index[daily['date'] == d]
        if len(idx) > 0:
            daily.loc[idx[0], 'ledger_signed_day'] = v
    for d, v in daily_funding.items():
        idx = daily.index[daily['date'] == d]
        if len(idx) > 0:
            daily.loc[idx[0], 'funding_signed_day'] = v

    # Forward-cumsum of all components
    daily['cum_closedPnl'] = daily['cum_closedPnl_day'].cumsum()
    daily['cum_fees'] = daily['cum_fees_day'].cumsum()
    daily['cum_funding'] = daily['funding_signed_day'].cumsum()
    daily['cum_ledger'] = daily['ledger_signed_day'].cumsum()
    daily['n_fills_window'] = daily['n_fills_day'].cumsum()

    # Reverse formula: spot(t) = spot(today) - (cum_today - cum_t)_pnl + ...
    #                          = spot(today) - cum_pnl[today] + cum_pnl[t]
    #                            + cum_fees[today] - cum_fees[t]
    #                            - cum_funding[today] + cum_funding[t]
    #                            - cum_ledger[today] + cum_ledger[t]
    pnl_total = float(daily['cum_closedPnl'].iloc[-1])
    fees_total = float(daily['cum_fees'].iloc[-1])
    funding_total = float(daily['cum_funding'].iloc[-1])
    ledger_total = float(daily['cum_ledger'].iloc[-1])

    # equity(t) = spot_today - (pnl_total - pnl_t) + (fees_total - fees_t) - (funding_total - funding_t) - (ledger_total - ledger_t)
    daily['spot_usdc_equity'] = (
        spot_today
        - (pnl_total - daily['cum_closedPnl'])
        + (fees_total - daily['cum_fees'])
        - (funding_total - daily['cum_funding'])
        - (ledger_total - daily['cum_ledger'])
    )

    daily['wallet'] = wallet
    # Final columns
    return daily[['wallet', 'date', 'spot_usdc_equity', 'cum_closedPnl', 'cum_fees',
                  'cum_funding', 'cum_ledger', 'n_fills_window']]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--wallets', default=None, help='Optional wallet list (text, one per line). Default: all in today_hl_state_cache')
    ap.add_argument('--max-wallets', type=int, default=0, help='Cap. 0 = all.')
    ap.add_argument('--start', default=WINDOW_START_DEFAULT)
    ap.add_argument('--end', default=WINDOW_END_DEFAULT)
    ap.add_argument('--n-workers', type=int, default=4)
    ap.add_argument('--output', default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    # Today's spot USDC per wallet
    state = pd.read_parquet(TODAY_HL_STATE)
    state['wallet'] = state['wallet'].str.lower()
    state = state[state['ok']]
    today_spot_map = dict(zip(state['wallet'], state['spot_usdc']))
    logger.info(f'Loaded {len(today_spot_map):,} wallets from today_hl_state_cache')

    if args.wallets:
        with open(args.wallets) as f:
            wallets_req = [w.strip().lower() for w in f if w.strip()]
        wallets = [w for w in wallets_req if w in today_spot_map]
    else:
        wallets = sorted(today_spot_map.keys())
    if args.max_wallets > 0:
        wallets = wallets[:args.max_wallets]
    logger.info(f'Processing {len(wallets):,} wallets')

    win_start_ms = int(pd.Timestamp(args.start, tz='UTC').timestamp() * 1000)
    win_end_ms = int((pd.Timestamp(args.end, tz='UTC') + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    # Aggregate fills per (wallet, date) — single pass over all S3 files
    logger.info('Aggregating fills per (wallet, date)...')
    t0 = time.time()
    fills_agg = aggregate_fills_per_wallet_day(set(wallets), win_start_ms, win_end_ms)
    logger.info(f'Fills agg: {len(fills_agg):,} (wallet, date) rows in {(time.time()-t0)/60:.1f}min')

    # Index fills_agg by wallet for fast per-wallet lookup
    fills_by_wallet = {}
    for _, row in fills_agg.iterrows():
        fills_by_wallet.setdefault(row['wallet'], {})[row['date']] = {
            'pnl': row['cum_closedPnl_day'],
            'fees': row['cum_fees_day'],
            'n': row['n_fills_day'],
        }

    # Per-wallet reconstruction
    logger.info(f'Reconstructing per-wallet (n_workers={args.n_workers})...')
    t0 = time.time()
    results = []
    job_args = [
        (w, float(today_spot_map.get(w, 0.0)), win_start_ms, win_end_ms, fills_by_wallet.get(w, {}))
        for w in wallets
    ]
    if args.n_workers > 1:
        with ProcessPoolExecutor(max_workers=args.n_workers) as ex:
            futs = {ex.submit(reconstruct_one_wallet, a): a[0] for a in job_args}
            for j, fut in enumerate(as_completed(futs), 1):
                try:
                    df = fut.result()
                    if df is not None:
                        results.append(df)
                except Exception as e:
                    logger.warning(f'wallet fail {futs[fut][:10]}: {e}')
                if j % 200 == 0:
                    logger.info(f'  reconstructed {j}/{len(wallets)} in {(time.time()-t0)/60:.1f}min')
    else:
        for j, a in enumerate(job_args, 1):
            try:
                df = reconstruct_one_wallet(a)
                if df is not None:
                    results.append(df)
            except Exception as e:
                logger.warning(f'wallet fail {a[0][:10]}: {e}')
            if j % 200 == 0:
                logger.info(f'  reconstructed {j}/{len(wallets)} in {(time.time()-t0)/60:.1f}min')

    if not results:
        logger.error('No results.')
        return
    out = pd.concat(results, ignore_index=True)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False, compression='snappy')
    logger.info(f'Wrote {out_path}: {len(out):,} rows ({out["wallet"].nunique():,} wallets)')

    # Summary stats
    last_day = out.groupby('wallet').tail(1)
    logger.info(f'Last-day equity stats:')
    logger.info(f'  median: ${last_day["spot_usdc_equity"].median():,.2f}')
    logger.info(f'  p25: ${last_day["spot_usdc_equity"].quantile(0.25):,.2f}')
    logger.info(f'  p75: ${last_day["spot_usdc_equity"].quantile(0.75):,.2f}')
    logger.info(f'  max: ${last_day["spot_usdc_equity"].max():,.2f}')
    logger.info(f'  negatives: {(last_day["spot_usdc_equity"] < 0).sum():,} ({(last_day["spot_usdc_equity"] < 0).mean()*100:.1f}%)')


if __name__ == '__main__':
    main()
