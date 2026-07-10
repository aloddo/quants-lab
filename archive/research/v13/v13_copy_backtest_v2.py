"""V13 copy-trading backtest v2 — proportional-equity sizing.

Per Alberto TG 7530 + 7534 (2026-05-28): "copy SIZING in % of equity incl
leverage" + static base equity for backtest.

SIZING MODEL
============
For each lifecycle (wallet opens → flat on a coin):
  wallet_position_notional = entry_size × entry_px  (USD notional at open)
  wallet_equity_at_entry   = interpolated wallet equity from
                             portfolio.accountValueHistory at entry_ts
  leverage_pct             = wallet_position_notional / wallet_equity_at_entry
  our_copy_notional        = BASE_EQUITY × leverage_pct
  our_size                 = our_copy_notional / entry_px
  pnl_pct                  = (exit_px − entry_px) / entry_px × side − HL_RT_FEE
  pnl_usd                  = our_copy_notional × pnl_pct

EQUITY MODEL
============
Static base $BASE_EQUITY for every lifecycle (no compounding). Per Alberto
TG 7534. Cleaner ROI comparison across strategies.

WALLET EQUITY HISTORY
=====================
Source: HL portfolio API perpAllTime.accountValueHistory, fetched on demand
(we don't have it cached from sweep). For each wallet we cache after first
fetch to a local pickle.

OUTPUT
======
app/data/v13/copy_backtest_v2_results.parquet  (per-trade)
app/data/v13/copy_backtest_v2_summary.parquet  (per-wallet aggregate)

USAGE
=====
  python scripts/v13_copy_backtest_v2.py --top-n 50 --base-equity 1000
"""
from __future__ import annotations

import argparse
import bisect
import glob
import json
import logging
import pickle
import sys
import time
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger('copy_backtest_v2')

FEE_RT = 0.000864  # HL round-trip taker fee
FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
CANDIDATES = Path('/Users/hermes/quants-lab/app/data/v13/wallet_copy_candidates.parquet')
EQUITY_HISTORY_CACHE = Path('/Users/hermes/quants-lab/app/data/v13/wallet_equity_history_cache.pkl')
OUT_PATH = Path('/Users/hermes/quants-lab/app/data/v13/copy_backtest_v2_results.parquet')
SUMMARY_PATH = Path('/Users/hermes/quants-lab/app/data/v13/copy_backtest_v2_summary.parquet')

WINDOW_START_MS = int(pd.Timestamp('2025-12-01', tz='UTC').timestamp() * 1000)
WINDOW_END_MS = int(pd.Timestamp('2026-05-25', tz='UTC').timestamp() * 1000)


def load_equity_history_cache() -> dict:
    if EQUITY_HISTORY_CACHE.exists():
        with open(EQUITY_HISTORY_CACHE, 'rb') as f:
            return pickle.load(f)
    return {}


def save_equity_history_cache(cache: dict):
    EQUITY_HISTORY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(EQUITY_HISTORY_CACHE, 'wb') as f:
        pickle.dump(cache, f)


def fetch_wallet_equity_history(wallet: str, session: requests.Session,
                                cache: dict) -> list[tuple[int, float]]:
    """Returns list of (ts_ms, equity_usd) tuples for the wallet. Cached."""
    if wallet in cache:
        return cache[wallet]
    backoff = 2.0
    for attempt in range(5):
        try:
            r = session.post('https://api.hyperliquid.xyz/info',
                             json={'type': 'portfolio', 'user': wallet}, timeout=20)
            if r.status_code == 429:
                time.sleep(backoff); backoff = min(backoff * 1.5, 30.0); continue
            if r.status_code != 200:
                cache[wallet] = []; return []
            data = r.json()
            if data is None:
                time.sleep(backoff); continue
            for tf, body in data:
                if tf == 'perpAllTime':
                    avh = body.get('accountValueHistory', [])
                    hist = [(int(p[0]), float(p[1])) for p in avh]
                    cache[wallet] = hist
                    return hist
            cache[wallet] = []; return []
        except requests.exceptions.RequestException:
            time.sleep(backoff); backoff = min(backoff * 1.5, 30.0)
    cache[wallet] = []; return []


def interpolate_equity(hist: list[tuple[int, float]], target_ts: int) -> float:
    """Linear interp wallet equity at target_ts. Clamped to history bounds."""
    if not hist:
        return 0.0
    if target_ts <= hist[0][0]:
        return hist[0][1]
    if target_ts >= hist[-1][0]:
        return hist[-1][1]
    ts_list = [p[0] for p in hist]
    i = bisect.bisect_left(ts_list, target_ts)
    t0, e0 = hist[i - 1]
    t1, e1 = hist[i]
    if t1 == t0:
        return e0
    return e0 + (e1 - e0) * (target_ts - t0) / (t1 - t0)


def load_wallet_fills(wallet: str) -> pd.DataFrame:
    files = sorted(glob.glob(str(FILLS_DIR / '2025*.parquet')) +
                   glob.glob(str(FILLS_DIR / '2026*.parquet')))
    chunks = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=['wallet', 'coin', 'side', 'size', 'price',
                                             'time', 'closedPnl', 'fee'])
        except Exception:
            continue
        df = df[df['wallet'] == wallet]
        if df.empty: continue
        for c in ['size', 'price', 'closedPnl', 'fee']:
            df[c] = df[c].astype(float)
        df['time'] = df['time'].astype('int64')
        df = df[(df['time'] >= WINDOW_START_MS) & (df['time'] <= WINDOW_END_MS)]
        if not df.empty:
            chunks.append(df)
    if not chunks:
        return pd.DataFrame()
    out = pd.concat(chunks, ignore_index=True)
    return out.sort_values(['coin', 'time']).reset_index(drop=True)


def extract_lifecycles(fills: pd.DataFrame) -> list[dict]:
    """Walk per-coin fills, identify (open → flat) lifecycles.

    For sizing: track ACCUMULATED notional (sum of size×price for all opens
    in this lifecycle). entry_px = weighted avg entry. peak_notional = max
    position notional reached during the lifecycle (used for sizing reference).
    """
    out = []
    for coin, sub in fills.groupby('coin', sort=False):
        sub = sub.sort_values('time')
        pos = 0.0
        weighted_entry_total = 0.0  # sum of size × px for opens in this lifecycle
        opened_size_cum = 0.0
        entry_ts = None
        entry_side = None
        peak_pos_notional = 0.0
        n_in_lifecycle = 0
        first_entry_px = None
        for _, fill in sub.iterrows():
            sz_delta = fill['size'] if fill['side'] == 'B' else -fill['size']
            prev_pos = pos
            pos += sz_delta
            n_in_lifecycle += 1
            px = float(fill['price'])
            is_opening = (abs(prev_pos) < 1e-9) or (abs(pos) > abs(prev_pos))
            if abs(prev_pos) < 1e-9 and abs(pos) > 1e-9:
                # New lifecycle
                first_entry_px = px
                weighted_entry_total = abs(sz_delta) * px
                opened_size_cum = abs(sz_delta)
                entry_ts = int(fill['time'])
                entry_side = 'long' if pos > 0 else 'short'
                n_in_lifecycle = 1
                peak_pos_notional = abs(pos) * px
            elif entry_side is not None and is_opening:
                # Add to existing position
                weighted_entry_total += abs(sz_delta) * px
                opened_size_cum += abs(sz_delta)
                peak_pos_notional = max(peak_pos_notional, abs(pos) * px)
            elif abs(pos) < 1e-9 and entry_side is not None:
                # Lifecycle closes
                avg_entry_px = weighted_entry_total / opened_size_cum if opened_size_cum > 0 else first_entry_px
                out.append({
                    'coin': coin,
                    'entry_ts': entry_ts,
                    'exit_ts': int(fill['time']),
                    'hold_sec': (int(fill['time']) - entry_ts) / 1000.0,
                    'side': entry_side,
                    'entry_px': avg_entry_px,
                    'first_entry_px': first_entry_px,
                    'exit_px': px,
                    'opened_size_cum': opened_size_cum,
                    'peak_pos_notional': peak_pos_notional,
                    'n_fills_lifecycle': n_in_lifecycle,
                })
                weighted_entry_total = 0.0
                opened_size_cum = 0.0
                entry_ts = None
                entry_side = None
                peak_pos_notional = 0.0
                n_in_lifecycle = 0
                first_entry_px = None
            elif entry_side is not None and (prev_pos > 0) != (pos > 0):
                # Sign flip: close + open new
                avg_entry_px = weighted_entry_total / opened_size_cum if opened_size_cum > 0 else first_entry_px
                out.append({
                    'coin': coin,
                    'entry_ts': entry_ts,
                    'exit_ts': int(fill['time']),
                    'hold_sec': (int(fill['time']) - entry_ts) / 1000.0,
                    'side': entry_side,
                    'entry_px': avg_entry_px,
                    'first_entry_px': first_entry_px,
                    'exit_px': px,
                    'opened_size_cum': opened_size_cum,
                    'peak_pos_notional': peak_pos_notional,
                    'n_fills_lifecycle': n_in_lifecycle,
                })
                # Re-init for new lifecycle
                weighted_entry_total = abs(pos) * px
                opened_size_cum = abs(pos)
                first_entry_px = px
                entry_ts = int(fill['time'])
                entry_side = 'long' if pos > 0 else 'short'
                peak_pos_notional = abs(pos) * px
                n_in_lifecycle = 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--candidates', type=str, default=str(CANDIDATES))
    ap.add_argument('--top-n', type=int, default=50)
    ap.add_argument('--base-equity', type=float, default=1000.0)
    ap.add_argument('--max-reconciliation-pct', type=float, default=100.0)
    ap.add_argument('--max-leverage-cap', type=float, default=20.0,
                    help='Cap on leverage_pct (avoid moonshot wallets blowing up backtest)')
    args = ap.parse_args()

    cand = pd.read_parquet(args.candidates)
    if 'reconciliation_pct' in cand.columns:
        cand = cand[cand['reconciliation_pct'] <= args.max_reconciliation_pct].copy()
    cand = cand.sort_values('roi_window', ascending=False).head(args.top_n)
    logger.info(f'Backtesting {len(cand)} wallets (top-{args.top_n} by wallet ROI)')

    session = requests.Session()
    equity_cache = load_equity_history_cache()
    logger.info(f'Equity history cache: {len(equity_cache):,} wallets pre-cached')

    all_trades = []
    summary = []
    t0 = time.time()
    for i, (_, r) in enumerate(cand.iterrows(), 1):
        w = r['wallet']
        fills = load_wallet_fills(w)
        if fills.empty:
            continue
        lifecycles = extract_lifecycles(fills)
        if not lifecycles:
            continue
        hist = fetch_wallet_equity_history(w, session, equity_cache)
        if not hist:
            logger.warning(f'no equity history for {w[:10]} — skip')
            continue
        wallet_pnl_usd = 0.0
        wins = 0
        for lc in lifecycles:
            wallet_equity = interpolate_equity(hist, lc['entry_ts'])
            if wallet_equity <= 0:
                continue
            wallet_notional = lc['opened_size_cum'] * lc['first_entry_px']
            leverage_pct = wallet_notional / wallet_equity
            # Cap leverage to avoid blowups from wallets with $0 anchor or absurd ratios
            leverage_pct_capped = min(leverage_pct, args.max_leverage_cap)
            our_copy_notional = args.base_equity * leverage_pct_capped
            entry_px = lc['entry_px']
            exit_px = lc['exit_px']
            if entry_px <= 0:
                continue
            if lc['side'] == 'long':
                raw_pnl_pct = (exit_px - entry_px) / entry_px
            else:
                raw_pnl_pct = (entry_px - exit_px) / entry_px
            net_pnl_pct = raw_pnl_pct - FEE_RT
            pnl_usd = our_copy_notional * net_pnl_pct
            lc['wallet'] = w
            lc['wallet_equity_at_entry'] = wallet_equity
            lc['wallet_notional'] = wallet_notional
            lc['leverage_pct'] = leverage_pct
            lc['leverage_pct_capped'] = leverage_pct_capped
            lc['our_copy_notional'] = our_copy_notional
            lc['pnl_pct'] = net_pnl_pct
            lc['pnl_usd'] = pnl_usd
            all_trades.append(lc)
            wallet_pnl_usd += pnl_usd
            if pnl_usd > 0: wins += 1
        if not lifecycles:
            continue
        summary.append({
            'wallet': w,
            'wallet_dec1_anchor_usd': r['dec1_anchor_usd'],
            'wallet_pnl_delta_usd': r['pnl_delta_usd'],
            'wallet_roi_window': r['roi_window'],
            'reconciliation_pct': r.get('reconciliation_pct', float('nan')),
            'n_trades_simulated': len(lifecycles),
            'wins': wins,
            'win_rate': wins / len(lifecycles) if lifecycles else 0.0,
            'avg_hold_sec': sum(lc['hold_sec'] for lc in lifecycles) / len(lifecycles),
            'avg_leverage_pct': sum(lc['leverage_pct'] for lc in lifecycles) / len(lifecycles),
            'max_leverage_pct': max(lc['leverage_pct'] for lc in lifecycles),
            'copy_pnl_usd': wallet_pnl_usd,
            'copy_roi_on_base': wallet_pnl_usd / args.base_equity,
            'best_trade_pnl': max((lc['pnl_usd'] for lc in lifecycles), default=0.0),
            'worst_trade_pnl': min((lc['pnl_usd'] for lc in lifecycles), default=0.0),
        })
        if i % 10 == 0:
            logger.info(f'  [{i}/{len(cand)}] {time.time()-t0:.0f}s | '
                        f'cum trades: {len(all_trades):,}')
            save_equity_history_cache(equity_cache)

    save_equity_history_cache(equity_cache)
    trades_df = pd.DataFrame(all_trades)
    summary_df = pd.DataFrame(summary).sort_values('copy_roi_on_base', ascending=False)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    trades_df.to_parquet(OUT_PATH, index=False, compression='snappy')
    summary_df.to_parquet(SUMMARY_PATH, index=False, compression='snappy')
    logger.info(f'Wrote {OUT_PATH} ({len(trades_df):,} trades)')
    logger.info(f'Wrote {SUMMARY_PATH} ({len(summary_df):,} wallets)')

    n_show = min(25, len(summary_df))
    print(f'\n=== Top {n_show} by copy_roi_on_base (base=${args.base_equity}, %-of-equity sizing, leverage cap {args.max_leverage_cap}x) ===')
    show_cols = ['wallet', 'wallet_dec1_anchor_usd', 'wallet_roi_window',
                 'reconciliation_pct', 'n_trades_simulated', 'win_rate',
                 'avg_leverage_pct', 'max_leverage_pct',
                 'copy_pnl_usd', 'copy_roi_on_base',
                 'best_trade_pnl', 'worst_trade_pnl']
    print(summary_df.head(n_show)[show_cols].to_string(index=False))
    print(f'\nAGGREGATE on {len(summary_df)} wallets:')
    print(f'  total copy_pnl: ${summary_df["copy_pnl_usd"].sum():,.2f}')
    print(f'  median wallet copy_roi: {summary_df["copy_roi_on_base"].median():.2%}')
    print(f'  positive wallets: {(summary_df["copy_pnl_usd"] > 0).sum()} / {len(summary_df)}')
    print(f'  total trades simulated: {len(trades_df):,}')
    print(f'  trade win rate: {(trades_df["pnl_usd"] > 0).mean():.2%}')


if __name__ == '__main__':
    main()
