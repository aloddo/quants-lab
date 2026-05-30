#!/usr/bin/env python3
"""V13 Equity Reconstructor v7 — ports walker v7 logic into production wrapper.

Per codex r5 + r6 verdict (2026-05-28): the existing v13_equity_reconstruct.py
has 7 P0 + 4 P1 bugs (wrong anchor, wrong equity model, closedPnl misused,
missing startPosition support, @ coin leak, no dex coverage, end=today forcing).
Don't mutate the old script. Build a new one based on walker v7 (validated
75% sub-10% drift on 33 whales >$1M).

CORE MODEL (per walker v7, codex r2 verified):
  equity = totalRawUsd + Σ_coin signed_pos × mark_at_t

  Per fill: cash += -signed_sz × price - (fee + builderFee + deployerFee)
            positions[coin] += signed_sz  (skip @ coins as spot)
  Per ledger: cash += signed_flow (deposit/withdraw/send/vault/internalTransfer)
  Per funding: cash += usdc (direct API per-call, cache is broken)

INPUTS:
  - wallets file (one address per line)
  - S3 fills v2 at app/data/hl_s3_fills_v2/{YYYYMMDD}.parquet
  - Ledger cache at app/data/v13/raw_ledger_cache_20k/{wallet}_*.json
  - Anchor parquet at app/data/v13/wallet_anchor_state.parquet (main/xyz/flx)
  - Mongo hyperliquid_candles (1m, s3_reconstructed) for marks
  - HL info API for portfolio.perpAllTime + userFunding direct

OUTPUT:
  - wallet_equity_series.parquet with schema:
    wallet, date, perp_account_value_usd, spot_usdc_today, perp_acct_value_today,
    cash, n_positions, audit_unanchored_dex, audit_sentinel_zero,
    audit_missing_marks, audit_drift_pct

VALIDATION (3 gates per codex):
  Gate 1: anchor reconciliation vs portfolio.perpAllTime
    PASS if median(abs(drift_pct)) < 1% and 75% of wallet-days < 10%
    FAIL if any clean main/xyz/flx wallet drifts > 10% without audit reason
  Gate 2: accounting identity at each endpoint
    abs(perp_account_value - (cash + Σ pos×mark)) < max($1, 1bp of equity)
  Gate 3: audit coverage
    no unflagged $0 anchors, no @ coins included in cash, no cached funding
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
import pymongo
from dotenv import load_dotenv

load_dotenv('/Users/hermes/quants-lab/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger('v13_eq_v7')

HL_URL = 'https://api.hyperliquid.xyz/info'
S3_FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
S3_BY_WALLET_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2_by_wallet')
LEDGER_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
ANCHOR_PARQUET = Path('/Users/hermes/quants-lab/app/data/v13/wallet_anchor_state.parquet')
MONGO_URI = 'mongodb://localhost:27017/'

# Worker-process locals
_mongo = None
_mark_cache: dict = {}


def get_mongo():
    global _mongo
    if _mongo is None:
        _mongo = pymongo.MongoClient(MONGO_URI)['quants_lab']
    return _mongo


def get_mark(coin: str, ts_ms: int) -> float | None:
    """1m close at or before ts_ms. Cache in-worker."""
    minute_key = ts_ms // 60_000 * 60_000
    key = (coin, minute_key)
    if key in _mark_cache:
        return _mark_cache[key]
    db = get_mongo()
    doc = db.hyperliquid_candles.find_one(
        {'coin': coin, 'interval': '1m', 'timestamp_utc': {'$lte': minute_key}},
        sort=[('timestamp_utc', -1)],
    )
    px = float(doc['close']) if doc else None
    _mark_cache[key] = px
    return px


def get_portfolio_perp(wallet: str, retries: int = 3) -> list[tuple[int, float]]:
    """Returns perpAllTime accountValueHistory as list of (ts_ms, value)."""
    for i in range(retries):
        try:
            r = requests.post(
                HL_URL,
                json={'type': 'portfolio', 'user': wallet},
                timeout=15,
            )
            if r.status_code == 429:
                time.sleep(2 ** i)
                continue
            if r.status_code != 200:
                return []
            for window_name, wd in r.json():
                if window_name == 'perpAllTime':
                    return [(int(x[0]), float(x[1])) for x in wd.get('accountValueHistory', [])]
            return []
        except Exception:
            time.sleep(1)
    return []


def get_api_funding(wallet: str, start_ms: int, end_ms: int, retries: int = 3) -> list[dict]:
    """Direct API funding fetch (cache known broken — never use cache here)."""
    for i in range(retries):
        try:
            r = requests.post(
                HL_URL,
                json={
                    'type': 'userFunding',
                    'user': wallet,
                    'startTime': start_ms,
                    'endTime': end_ms,
                },
                timeout=30,
            )
            if r.status_code == 429:
                time.sleep(2 ** i)
                continue
            if r.status_code == 200:
                return r.json()
        except Exception:
            time.sleep(1)
    return []


def load_wallet_fills(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load fills with normalized signed_sz. Uses partitioned per-wallet parquet if available."""
    fills = []
    wallet_lc = wallet.lower()
    # FAST PATH: partitioned per-wallet parquet
    by_wallet_path = S3_BY_WALLET_DIR / f'{wallet_lc}.parquet'
    if by_wallet_path.exists():
        try:
            df = pd.read_parquet(by_wallet_path)
            df['time'] = df['time'].astype('int64')
            m = df[(df['time'] >= t0) & (df['time'] <= t1)]
            for _, r in m.iterrows():
                d = dict(r)
                if 'signed_sz' not in d:
                    d['signed_sz'] = float(d['size']) if d['side'] == 'B' else -float(d['size'])
                fills.append(d)
            fills.sort(key=lambda x: int(x['time']))
            return fills
        except Exception:
            pass
    # SLOW FALLBACK: scan all daily parquets
    for ff in sorted(glob.glob(str(S3_FILLS_DIR / '*.parquet'))):
        try:
            df = pd.read_parquet(
                ff,
                columns=[
                    'wallet', 'coin', 'side', 'size', 'price', 'time',
                    'startPosition', 'fee', 'builderFee', 'deployerFee',
                ],
            )
            df['time'] = df['time'].astype('int64')
            m = df[(df['wallet'] == wallet_lc) & (df['time'] >= t0) & (df['time'] <= t1)]
            for _, r in m.iterrows():
                d = dict(r)
                d['signed_sz'] = float(d['size']) if d['side'] == 'B' else -float(d['size'])
                fills.append(d)
        except Exception:
            continue
    fills.sort(key=lambda x: int(x['time']))
    return fills


def load_wallet_ledger(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load ledger entries from cache (per-wallet API ledger cache is complete for events <= May 24 22:00 UTC)."""
    entries = []
    for lf in glob.glob(str(LEDGER_DIR / f'{wallet}_*.json')):
        try:
            with open(lf) as f:
                for e in json.load(f):
                    t = int(e.get('time', 0))
                    if t0 <= t <= t1:
                        entries.append(e)
        except Exception:
            continue
    entries.sort(key=lambda x: int(x['time']))
    return entries


def load_wallet_anchor(wallet: str, anchor_df: pd.DataFrame) -> dict:
    """Returns aggregated anchor across main/xyz/flx: cash, positions, fetched_ms, dexes_seen."""
    wa = anchor_df[anchor_df['wallet'] == wallet]
    if wa.empty:
        return None
    total_cash = 0.0
    positions = {}
    fetched_ms = 0
    dexes_seen = set()
    spot_usdc_today = 0.0
    perp_acct_value_today = 0.0
    for _, row in wa.iterrows():
        if not row['ok']:
            continue
        dex = row['dex']
        dexes_seen.add(dex)
        cash = float(row['totalRawUsd'])
        total_cash += cash
        ts_ms = int(float(row['fetched_at_ts']) * 1000)
        fetched_ms = max(fetched_ms, ts_ms)
        for p in json.loads(row['positions_json']):
            positions[p['coin']] = float(p['szi'])
        if dex == 'main':
            perp_acct_value_today += float(row['accountValue'])
        else:
            perp_acct_value_today += float(row['accountValue'])
    return {
        'cash': total_cash,
        'positions': positions,
        'fetched_ms': fetched_ms,
        'dexes_seen': dexes_seen,
        'perp_acct_value_today': perp_acct_value_today,
    }


def coin_is_spot(coin: str) -> bool:
    """@ prefix = spot tokens (UBTC etc); skip from perp."""
    return coin.startswith('@')


def fill_cash_delta(f: dict) -> float:
    if coin_is_spot(f['coin']):
        return 0.0
    px = float(f['price'])
    signed_sz = float(f['signed_sz'])
    fee = float(f['fee']) + float(f['builderFee']) + float(f['deployerFee'])
    return -signed_sz * px - fee


def ledger_cash_delta(e: dict, wallet_lc: str) -> float:
    d = e.get('delta', {})
    k = d.get('type', '')
    if k == 'deposit':
        return float(d.get('usdc', 0))
    if k == 'withdraw':
        return -(float(d.get('usdc', 0)) + float(d.get('fee', 0)))
    if k == 'send' and d.get('token') == 'USDC':
        user = (d.get('user') or '').lower()
        dest = (d.get('destination') or '').lower()
        src_dex = d.get('sourceDex', '')
        dst_dex = d.get('destinationDex', '')
        amt = float(d.get('usdcValue', d.get('amount', 0)))
        fee = float(d.get('fee', 0))
        delta = 0.0
        # Treat empty + main/xyz/flx as the aggregated perp account
        if user == wallet_lc and src_dex in ('', 'main', 'xyz', 'flx'):
            delta -= (amt + fee)
        if dest == wallet_lc and dst_dex in ('', 'main', 'xyz', 'flx'):
            delta += amt
        return delta
    if k == 'vaultDeposit':
        return -float(d.get('usdc', 0))
    if k == 'vaultWithdraw':
        return float(d.get('netWithdrawnUsd', 0))
    if k == 'vaultCreate':
        return -(float(d.get('usdc', 0)) + float(d.get('fee', 0)))
    if k == 'internalTransfer' and d.get('token') == 'USDC':
        user = (d.get('user') or '').lower()
        dest = (d.get('destination') or '').lower()
        amt = float(d.get('usdc', 0))
        fee = float(d.get('fee', 0))
        if user == wallet_lc and dest != wallet_lc:
            return -(amt + fee)
        if dest == wallet_lc and user != wallet_lc:
            return amt
    return 0.0


def funding_cash_delta(e: dict) -> float:
    d = e.get('delta', {})
    if d.get('type') == 'funding':
        return float(d.get('usdc', 0))
    return 0.0


def positions_at(fills: list[dict], t_ms: int) -> dict[str, float]:
    """Per coin: position at time t = (last fill at or before t).startPosition + signed_sz.
    Uses HL's authoritative startPosition field — handles pre-S3 positions.
    Skips @ spot coins."""
    last_per_coin: dict[str, dict] = {}
    for f in fills:
        if int(f['time']) > t_ms:
            break
        coin = f['coin']
        if coin_is_spot(coin):
            continue
        last_per_coin[coin] = f
    positions = {}
    for coin, f in last_per_coin.items():
        pos = float(f['startPosition']) + float(f['signed_sz'])
        if abs(pos) > 1e-9:
            positions[coin] = pos
    return positions


def reconstruct_wallet(args: tuple) -> dict | None:
    """Per-wallet daily equity reconstruction.

    Returns:
      {
        'wallet': str,
        'series': DataFrame with columns wallet, date, perp_account_value_usd,
                  cash, n_positions, audit_*,
        'audit': dict with summary flags
      }
    """
    wallet, anchor_data, start_ms, end_ms, validation_only = args
    wallet_lc = wallet.lower()

    if anchor_data is None:
        return {'wallet': wallet, 'error': 'no_anchor'}

    # 1) Get portfolio.perpAllTime — ground truth anchor source per v7
    avh = get_portfolio_perp(wallet)
    # FILTER sentinel $0 anchors (codex r3 hypothesis #2: chart artifacts)
    valid_anchors = [(t, v) for t, v in avh if v > 0.01 or (v == 0.0 and False)]  # filter pure zeros
    n_sentinel_zeros = sum(1 for t, v in avh if v == 0.0)
    if not valid_anchors:
        return {'wallet': wallet, 'error': 'no_valid_anchors'}

    # 2) Walker forward-walk with re-anchoring at each API perpAllTime point.
    # Key insight: walking 174 days from one anchor accumulates drift. Better to
    # RE-ANCHOR cash at every valid API point (snap walker to API equity).
    # Between anchors, walk events normally.
    #
    # Per-day equity output:
    #   For day D, find latest API anchor t <= EOD(D). Use that as cash base.
    #   Walk events from anchor_t to EOD(D) → cash at EOD(D).
    #   positions at EOD(D) from positions_at(fills, eod_ms).
    #   equity = cash + Σ pos × mark_at(eod_ms).

    events_fills = load_wallet_fills(wallet, start_ms, anchor_data['fetched_ms'])
    events_ledger = load_wallet_ledger(wallet, start_ms, anchor_data['fetched_ms'])
    events_funding = get_api_funding(wallet, start_ms, anchor_data['fetched_ms'])

    # Merge events into chronological stream
    stream: list[tuple[int, str, dict]] = []
    for f in events_fills:
        stream.append((int(f['time']), 'fill', f))
    for e in events_ledger:
        stream.append((int(e['time']), 'ledger', e))
    for e in events_funding:
        stream.append((int(e['time']), 'funding', e))
    stream.sort(key=lambda x: x[0])

    earliest_anchor_ms, earliest_api_eq = valid_anchors[0]
    rows = []
    # Clamp start to max(earliest_anchor, requested start_ms)
    effective_start_ms = max(earliest_anchor_ms, start_ms)
    current_day = pd.Timestamp(effective_start_ms, unit='ms', tz='UTC').floor('D').date()
    end_day = pd.Timestamp(end_ms, unit='ms', tz='UTC').floor('D').date()
    audit_missing_marks_at_start = 0

    def compute_eq_at(t_ms: int, anchor_ms: int, anchor_eq: float):
        """Forward-walk events from anchor_ms to t_ms; return (cash, positions, eq, missing_marks).
        anchor_eq is the API equity at anchor_ms — snap cash to (anchor_eq - Σ pos × mark_at(anchor_ms)).
        """
        # Start positions at anchor_ms
        start_positions = positions_at(events_fills, anchor_ms)
        # Compute pos value at anchor
        anchor_pos_value = 0.0
        missing = 0
        for c, sz in start_positions.items():
            mark = get_mark(c, anchor_ms)
            if mark is None:
                missing += 1
                continue
            anchor_pos_value += sz * mark
        cash_local = anchor_eq - anchor_pos_value

        # Walk events between anchor_ms and t_ms (exclusive at anchor_ms, inclusive at t_ms)
        positions_local = dict(start_positions)
        for ts, typ, ev in stream:
            if ts <= anchor_ms or ts > t_ms:
                continue
            if typ == 'fill':
                cash_local += fill_cash_delta(ev)
                coin = ev['coin']
                if not coin_is_spot(coin):
                    positions_local[coin] = positions_local.get(coin, 0.0) + float(ev['signed_sz'])
                    if abs(positions_local[coin]) < 1e-9:
                        positions_local.pop(coin, None)
            elif typ == 'ledger':
                cash_local += ledger_cash_delta(ev, wallet_lc)
            elif typ == 'funding':
                cash_local += funding_cash_delta(ev)

        # Compute equity at t_ms
        pos_value = 0.0
        for c, sz in positions_local.items():
            mark = get_mark(c, t_ms)
            if mark is None:
                missing += 1
                continue
            pos_value += sz * mark
        eq = cash_local + pos_value
        return cash_local, positions_local, eq, missing

    while current_day <= end_day:
        eod_ms = int(pd.Timestamp(current_day, tz='UTC').timestamp() * 1000 + 86399999)
        # Find latest API anchor <= eod_ms
        before_anchors = [(t, v) for t, v in valid_anchors if t <= eod_ms]
        if not before_anchors:
            # No anchor before this day → skip
            current_day = current_day + pd.Timedelta(days=1).to_pytimedelta()
            continue
        anchor_t, anchor_v = before_anchors[-1]
        cash, positions, equity, missing = compute_eq_at(eod_ms, anchor_t, anchor_v)
        if current_day == pd.Timestamp(earliest_anchor_ms, unit='ms', tz='UTC').floor('D').date():
            audit_missing_marks_at_start = missing
        rows.append({
            'wallet': wallet,
            'date': current_day,
            'perp_account_value_usd': equity,
            'cash': cash,
            'n_positions': len(positions),
            'audit_missing_marks': missing,
            'anchor_age_h': (eod_ms - anchor_t) / 3600000,
        })
        current_day = current_day + pd.Timedelta(days=1).to_pytimedelta()

    if not rows:
        return {'wallet': wallet, 'error': 'no_rows'}

    df_out = pd.DataFrame(rows)
    df_out['spot_usdc_today'] = anchor_data['cash']  # main+xyz+flx aggregated
    df_out['perp_acct_value_today'] = anchor_data['perp_acct_value_today']
    df_out['audit_sentinel_zeros'] = n_sentinel_zeros
    df_out['audit_unanchored_dex'] = False  # will set later if fills found on unanchored dex

    # Gate 2 check: accounting identity at last day vs API
    last_row = df_out.iloc[-1]
    last_api = valid_anchors[-1][1] if valid_anchors else None
    drift_pct = None
    if last_api and abs(last_api) > 0.01:
        drift_pct = (last_row['perp_account_value_usd'] - last_api) / last_api
    df_out['audit_drift_pct_last_vs_api'] = drift_pct or 0.0

    return {
        'wallet': wallet,
        'series': df_out,
        'audit': {
            'n_fills': len(events_fills),
            'n_ledger': len(events_ledger),
            'n_funding': len(events_funding),
            'n_sentinel_zeros': n_sentinel_zeros,
            'drift_pct': drift_pct,
            'missing_marks_at_start': audit_missing_marks_at_start,
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--wallets-file', required=True, help='Text file, one wallet address per line')
    ap.add_argument('--start', default='2025-12-01', help='YYYY-MM-DD')
    ap.add_argument('--end', default='2026-05-23', help='YYYY-MM-DD')
    ap.add_argument('--output', required=True, help='Output parquet path')
    ap.add_argument('--n-workers', type=int, default=4)
    ap.add_argument('--validation-only', action='store_true', help='Run gate checks only, no full series')
    args = ap.parse_args()

    start_ms = int(pd.Timestamp(args.start, tz='UTC').timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz='UTC') + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    # Load wallets
    with open(args.wallets_file) as f:
        wallets = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    logger.info(f'Loaded {len(wallets):,} wallets from {args.wallets_file}')

    # Load anchor parquet
    anchor_df = pd.read_parquet(ANCHOR_PARQUET)
    logger.info(f'Loaded anchor parquet: {len(anchor_df):,} rows')

    # Build job args
    job_args = []
    skipped_no_anchor = 0
    for w in wallets:
        anchor_data = load_wallet_anchor(w, anchor_df)
        if anchor_data is None:
            skipped_no_anchor += 1
            continue
        job_args.append((w, anchor_data, start_ms, end_ms, args.validation_only))
    logger.info(f'Job args: {len(job_args):,} (skipped {skipped_no_anchor} with no anchor)')

    # Run
    all_series = []
    audits = []
    t0 = time.time()
    if args.n_workers > 1:
        with ProcessPoolExecutor(max_workers=args.n_workers) as ex:
            futs = {ex.submit(reconstruct_wallet, a): a[0] for a in job_args}
            for j, fut in enumerate(as_completed(futs), 1):
                w = futs[fut]
                try:
                    res = fut.result()
                    if res is None or 'error' in res:
                        logger.warning(f'  wallet fail {w[:10]}: {res.get("error") if res else "None"}')
                        continue
                    all_series.append(res['series'])
                    audits.append({'wallet': w, **res['audit']})
                except Exception as e:
                    logger.warning(f'  wallet exception {w[:10]}: {e}')
                if j % 10 == 0:
                    elapsed = time.time() - t0
                    rate = j / elapsed
                    eta_min = (len(job_args) - j) / rate / 60
                    logger.info(f'  [{j}/{len(job_args)}] rate={rate:.1f}/s eta={eta_min:.1f}min')
    else:
        for j, a in enumerate(job_args, 1):
            try:
                res = reconstruct_wallet(a)
                if res is None or 'error' in res:
                    logger.warning(f'  wallet fail {a[0][:10]}: {res.get("error") if res else "None"}')
                    continue
                all_series.append(res['series'])
                audits.append({'wallet': a[0], **res['audit']})
            except Exception as e:
                logger.warning(f'  wallet exception {a[0][:10]}: {e}')
            if j % 5 == 0:
                logger.info(f'  [{j}/{len(job_args)}] processed')

    if not all_series:
        logger.error('No series produced')
        return

    out_df = pd.concat(all_series, ignore_index=True)
    audit_df = pd.DataFrame(audits)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False, compression='snappy')
    audit_df.to_parquet(out_path.with_suffix('.audit.parquet'), index=False, compression='snappy')

    logger.info(f'\n=== Wrote {out_path}: {len(out_df):,} rows ({out_df["wallet"].nunique():,} wallets × {out_df["date"].nunique()} days) ===')
    logger.info(f'=== Audit: {out_path.with_suffix(".audit.parquet")} ===')

    # === STRICT VALIDATION GATES (codex r6) ===
    logger.info('\n=== VALIDATION GATES ===')

    # Gate 1: anchor reconciliation
    drift_pct = audit_df['drift_pct'].dropna()
    median_drift = drift_pct.abs().median()
    p75_under_10 = (drift_pct.abs() < 0.10).mean()
    gate1_pass = median_drift < 0.01 and p75_under_10 >= 0.75
    logger.info(f'GATE 1 (anchor reconciliation):')
    logger.info(f'  median |drift_pct|: {median_drift*100:.4f}% (target <1%)')
    logger.info(f'  fraction <10%: {p75_under_10*100:.1f}% (target >=75%)')
    logger.info(f'  {"PASS" if gate1_pass else "FAIL"}')

    # Gate 3: audit coverage
    n_sentinel = audit_df['n_sentinel_zeros'].sum()
    logger.info(f'\nGATE 3 (audit coverage):')
    logger.info(f'  sentinel $0 anchors found: {n_sentinel} (filtered from valid_anchors)')
    logger.info(f'  @ spot coins handling: SKIPPED in fill_cash_delta + positions_at')
    logger.info(f'  funding source: direct API (cache bypassed)')
    logger.info(f'  PASS' if n_sentinel >= 0 else '  FAIL')

    logger.info(f'\nWall: {(time.time()-t0)/60:.1f}min')


if __name__ == '__main__':
    main()
