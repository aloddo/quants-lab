"""V13 unified equity + position walker (v5).

Per Alberto's iterated direction (TG 7572-7587): one walker produces BOTH
equity series AND per-coin signed position state, used by:
- Wallet ranking (Sharpe, Sortino, etc.) — needs daily equity
- Backtest sizing — needs per-coin position notional AND wallet equity
- V13 spec Section 5.3 metrics

FORMULA (HL accountValue identity, verified empirically on 0xe3dff077 + parent):
  equity(t) = totalRawUsd(t) + Σ_coin signed_pos(t) × mark(coin, t)

Walking BACKWARD from today's clearinghouseState (which has exact totalRawUsd
+ positions + entry_px per coin per dex). Per-dex state-machines:
  - main perp: coins not starting with xyz:/flx:
  - xyz dex:   coins starting with xyz:
  - flx dex:   coins starting with flx:

Cash walk per fill (FORWARD effect on totalRawUsd):
  Δcash = -signed_size × price - fee_total
where signed_size = +size if side=='B' else -size.
REVERSING: cash += signed_size × price + fee_total.

Plus ledger flows (signed via accumulate_ledger_flow, per-dex routing handled in
the classifier) and funding flows (signed per coin, route by coin prefix).

INPUTS
======
- app/data/v13/wallet_anchor_state.parquet (from v13_anchor_state_fetcher.py)
- app/data/hl_s3_fills_v2/{YYYYMMDD}.parquet
- app/data/v13/raw_ledger_cache_20k/{wallet}_*.json
- app/data/v13/raw_funding_cache_20k/{wallet}_*.json
- MongoDB hyperliquid_candles (1m, source=s3_reconstructed)

OUTPUT
======
app/data/v13/wallet_equity_series_v5.parquet
Columns: wallet, dex, date, totalRawUsd, accountValue, n_positions,
         positions_json
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pymongo

sys.path.insert(0, '/Users/hermes/quants-lab/scripts')
from v13_equity_reconstruct import accumulate_ledger_flow, accumulate_funding_flow

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s', stream=sys.stdout)
logger = logging.getLogger('equity_v5')

FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
LEDGER_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
FUNDING_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k')
ANCHOR_PATH = Path('/Users/hermes/quants-lab/app/data/v13/wallet_anchor_state.parquet')
DEFAULT_OUTPUT = Path('/Users/hermes/quants-lab/app/data/v13/wallet_equity_series_v5.parquet')
EPS = 1e-9
MONGO_URI = 'mongodb://localhost:27017/'

# Worker-process-local Mongo client + mark cache
_mongo = None
_mark_cache: dict = {}


def get_mongo():
    global _mongo
    if _mongo is None:
        _mongo = pymongo.MongoClient(MONGO_URI)['quants_lab']
    return _mongo


def coin_dex(coin: str) -> str:
    if coin.startswith('xyz:'): return 'xyz'
    if coin.startswith('flx:'): return 'flx'
    return 'main'


def fetch_mark(coin: str, ts_ms: int) -> float:
    """Mark price at ts_ms from MongoDB. Forward-fill: nearest prior bar."""
    key = (coin, ts_ms // 60_000 * 60_000)
    if key in _mark_cache:
        return _mark_cache[key]
    db = get_mongo()
    doc = db.hyperliquid_candles.find_one(
        {'coin': coin, 'interval': '1m', 'timestamp_utc': {'$lte': key[1]}},
        sort=[('timestamp_utc', -1)],
    )
    px = float(doc['close']) if doc else 0.0
    _mark_cache[key] = px
    return px


def load_wallet_fills(wallet: str, win_start_ms: int, win_end_ms: int) -> list[dict]:
    """Load fills for one wallet in window."""
    files = sorted(glob.glob(str(FILLS_DIR / '2025*.parquet')) + glob.glob(str(FILLS_DIR / '2026*.parquet')))
    fills = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=['wallet','coin','side','size','price','time','closedPnl','fee','builderFee','deployerFee'])
        except Exception: continue
        df = df[df['wallet'] == wallet]
        if df.empty: continue
        for c in ['size','price','closedPnl','fee','builderFee','deployerFee']: df[c] = df[c].astype(float)
        df['time'] = df['time'].astype('int64')
        df = df[(df['time'] >= win_start_ms) & (df['time'] <= win_end_ms)]
        for _, r in df.iterrows():
            fills.append({
                'time': int(r['time']), 'coin': r['coin'], 'side': r['side'],
                'size': float(r['size']), 'price': float(r['price']),
                'fee_total': float(r['fee']) + float(r['builderFee']) + float(r['deployerFee']),
            })
    fills.sort(key=lambda x: x['time'])
    return fills


def load_wallet_ledger(wallet: str, win_start_ms: int, win_end_ms: int) -> list[dict]:
    """Load + dedup ledger entries for wallet in window."""
    entries = []
    seen = set()
    for lf in glob.glob(str(LEDGER_DIR / f'{wallet}_*.json')):
        try:
            for e in json.load(open(lf)):
                key = (e.get('time'), e.get('hash', ''))
                if key in seen: continue
                seen.add(key)
                if win_start_ms <= int(e.get('time', 0)) <= win_end_ms:
                    entries.append(e)
        except Exception: pass
    return entries


def load_wallet_funding(wallet: str, win_start_ms: int, win_end_ms: int) -> list[dict]:
    entries = []
    seen = set()
    for ff in glob.glob(str(FUNDING_DIR / f'{wallet}_*.json')):
        try:
            for e in json.load(open(ff)):
                key = (e.get('time'), e.get('hash', ''))
                if key in seen: continue
                seen.add(key)
                if (win_start_ms <= int(e.get('time', 0)) <= win_end_ms
                        and e.get('delta', {}).get('type') == 'funding'):
                    entries.append(e)
        except Exception: pass
    return entries


def reconstruct_one_wallet(args):
    """Per-wallet reconstruction. Returns DataFrame with daily snapshots."""
    wallet, anchor_per_dex, win_start_ms, win_end_ms, eod_ts_per_day = args

    # Per-dex state: cash, positions[coin]
    state = {}
    for dex_name in ['main', 'xyz', 'flx']:
        a = anchor_per_dex.get(dex_name, {'totalRawUsd': 0.0, 'positions': []})
        positions = {p['coin']: p['szi'] for p in a.get('positions', []) if abs(p['szi']) > EPS}
        state[dex_name] = {'cash': a.get('totalRawUsd', 0.0), 'positions': dict(positions)}

    # Load events
    fills = load_wallet_fills(wallet, win_start_ms, win_end_ms)
    ledger_entries = load_wallet_ledger(wallet, win_start_ms, win_end_ms)
    funding_entries = load_wallet_funding(wallet, win_start_ms, win_end_ms)

    # Build event stream
    events = []
    for f in fills:
        events.append({'time': f['time'], 'type': 'fill', 'dex': coin_dex(f['coin']),
                       'coin': f['coin'], 'side': f['side'],
                       'size': f['size'], 'price': f['price'], 'fee_total': f['fee_total']})
    for e in ledger_entries:
        try:
            one = accumulate_ledger_flow([e], wallet=wallet, lenient_unknown=True)
            if not one.empty:
                events.append({'time': int(e['time']), 'type': 'ledger',
                               'dex': 'main',  # accumulate_ledger_flow targets main perp
                               'signed_flow': float(one['signed_flow_usd'].iloc[0])})
        except Exception:
            pass
    for e in funding_entries:
        try:
            u = float(e['delta'].get('usdc', 0))
        except Exception:
            continue
        coin = e['delta'].get('coin', '')
        events.append({'time': int(e['time']), 'type': 'funding',
                       'dex': coin_dex(coin) if coin else 'main',
                       'signed_flow': u, 'coin': coin})

    # Group events by day; sort descending for backward walk
    events_by_day = defaultdict(list)
    for ev in events:
        d = pd.Timestamp(ev['time'], unit='ms', tz='UTC').floor('D').date()
        events_by_day[d].append(ev)
    for d in events_by_day:
        events_by_day[d].sort(key=lambda x: x['time'])

    # Walk backward day-by-day
    win_start_d = pd.Timestamp(win_start_ms, unit='ms', tz='UTC').floor('D').date()
    today_d = pd.Timestamp(win_end_ms, unit='ms', tz='UTC').floor('D').date()
    date_range = pd.date_range(win_start_d, today_d, freq='D').date

    # Walk backward from today
    rows = []
    # Snapshot END-of-today first (the anchor state is "now"; eod_today ≈ anchor)
    def snapshot(date):
        for dex_name, st in state.items():
            pos_serializable = []
            pos_value_sum = 0.0
            for coin, sz in st['positions'].items():
                ts_ms = eod_ts_per_day.get(date, int(pd.Timestamp(date, tz='UTC').timestamp() * 1000 + 86399000))
                mark = fetch_mark(coin, ts_ms)
                pv = sz * mark
                pos_value_sum += pv
                pos_serializable.append({'coin': coin, 'szi': sz, 'mark': mark, 'pv': pv})
            equity = st['cash'] + pos_value_sum
            rows.append({
                'wallet': wallet, 'dex': dex_name, 'date': date,
                'totalRawUsd': st['cash'], 'accountValue': equity,
                'n_positions': len(st['positions']),
                'positions_json': json.dumps(pos_serializable),
            })

    # Walk: start at end-of-window day, snapshot, then reverse events through prior days
    sorted_dates = sorted(date_range, reverse=True)
    # Snapshot the FIRST date (= today / end-of-window)
    snapshot(sorted_dates[0])
    # For each subsequent date going backward, reverse the events between this and prior
    for i in range(1, len(sorted_dates)):
        cur_day = sorted_dates[i]   # date we want state AT END OF
        prev_day = sorted_dates[i - 1]  # the day AFTER cur (already snapshotted)
        # Reverse all events that occurred BETWEEN end-of(cur_day) and end-of(prev_day) = events on prev_day
        for ev in reversed(events_by_day.get(prev_day, [])):
            dex = ev['dex']
            st = state.setdefault(dex, {'cash': 0.0, 'positions': {}})
            if ev['type'] == 'fill':
                sz_delta = ev['size'] if ev['side'] == 'B' else -ev['size']
                notional = ev['size'] * ev['price']
                # Reverse cash: forward was cash -= sz_delta × price - fee_total
                #   so cash += sz_delta × price + fee_total
                # But wait: BUY (B): forward cash -= size × price; reverse: cash += size × price
                #          SELL (A): forward cash += size × price; reverse: cash -= size × price
                # Generalized: cash += sz_delta × price (BUY: +; SELL: -)
                # Wait that doesn't match: BUY cash goes DOWN (cash -= notional), reverse adds it back (cash += notional)
                # sz_delta for BUY = +size. So cash += sz_delta × price = +size × price. ✓
                # SELL cash goes UP (cash += notional), reverse subtracts (cash -= notional)
                # sz_delta for SELL = -size. cash += sz_delta × price = -size × price. ✓
                st['cash'] += sz_delta * ev['price']
                # Fees: forward subtracted fee; reverse adds back
                st['cash'] += ev['fee_total']
                # Position: reverse size delta
                st['positions'][ev['coin']] = st['positions'].get(ev['coin'], 0.0) - sz_delta
                if abs(st['positions'][ev['coin']]) < EPS:
                    st['positions'].pop(ev['coin'], None)
            elif ev['type'] in ('ledger', 'funding'):
                # Forward: cash += signed_flow. Reverse: cash -= signed_flow
                st['cash'] -= ev['signed_flow']
        # Snapshot at end of cur_day (after reversing prev_day events)
        snapshot(cur_day)

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', default='2025-12-01')
    ap.add_argument('--end', default='2026-05-27')
    ap.add_argument('--wallets-limit', type=int, default=0)
    ap.add_argument('--n-workers', type=int, default=4)
    ap.add_argument('--output', default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    anchor_df = pd.read_parquet(ANCHOR_PATH)
    anchor_df = anchor_df[anchor_df['ok']]
    logger.info(f'Loaded {anchor_df["wallet"].nunique():,} wallet anchors')

    # Group anchor by wallet
    anchor_per_wallet = {}
    for w, group in anchor_df.groupby('wallet'):
        per_dex = {}
        for _, row in group.iterrows():
            per_dex[row['dex']] = {
                'totalRawUsd': float(row['totalRawUsd']),
                'positions': json.loads(row['positions_json']) if isinstance(row['positions_json'], str) else [],
            }
        anchor_per_wallet[w] = per_dex

    wallets = sorted(anchor_per_wallet.keys())
    if args.wallets_limit > 0:
        wallets = wallets[:args.wallets_limit]
    logger.info(f'Processing {len(wallets):,} wallets')

    win_start_ms = int(pd.Timestamp(args.start, tz='UTC').timestamp() * 1000)
    win_end_ms = int((pd.Timestamp(args.end, tz='UTC') + pd.Timedelta(days=1)).timestamp() * 1000 - 1)
    # EOD timestamps per day (end of UTC day in ms)
    eod_ts_per_day = {}
    for d in pd.date_range(args.start, args.end, freq='D').date:
        eod_ts_per_day[d] = int(pd.Timestamp(d, tz='UTC').timestamp() * 1000 + 86399000)

    job_args = [(w, anchor_per_wallet[w], win_start_ms, win_end_ms, eod_ts_per_day) for w in wallets]
    results = []
    t0 = time.time()
    if args.n_workers > 1:
        with ProcessPoolExecutor(max_workers=args.n_workers) as ex:
            futs = {ex.submit(reconstruct_one_wallet, a): a[0] for a in job_args}
            for j, fut in enumerate(as_completed(futs), 1):
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        results.append(df)
                except Exception as e:
                    logger.warning(f'wallet fail {futs[fut][:10]}: {e}')
                if j % 100 == 0:
                    elapsed = time.time() - t0
                    rate = j / elapsed
                    eta = (len(wallets) - j) / rate / 60
                    logger.info(f'  [{j}/{len(wallets)}] rate={rate:.1f}/s eta={eta:.1f}min')
    else:
        for j, a in enumerate(job_args, 1):
            try:
                df = reconstruct_one_wallet(a)
                if df is not None and not df.empty:
                    results.append(df)
            except Exception as e:
                logger.warning(f'wallet fail {a[0][:10]}: {e}')
            if j % 100 == 0:
                logger.info(f'  [{j}/{len(wallets)}]')

    if not results:
        logger.error('No results')
        return
    out = pd.concat(results, ignore_index=True)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False, compression='snappy')
    logger.info(f'Wrote {out_path}: {len(out):,} rows ({out["wallet"].nunique():,} wallets × {out["dex"].nunique()} dexes × ~178 days)')


if __name__ == '__main__':
    main()
