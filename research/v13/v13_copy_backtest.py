"""V13 copy-trading backtest.

Per Alberto direction TG 7527 (2026-05-28 04:37 CEST): "By the time I wake up
I want the full list of wallets and the whole backtest finished."

DESIGN
======
For each top-N copy candidate (from wallet_copy_candidates.parquet):
  1. Load wallet's enriched fills (hl_s3_fills_v2/) in Dec1→today window.
  2. Walk fills chronologically PER COIN, tracking signed position.
  3. Identify lifecycles (open → ... → flat).
  4. Simulate our copy: enter $COPY_NOTIONAL at the lifecycle's first open
     price (+entry slippage), exit at the final close price (-exit slippage),
     apply HL RT fees.
  5. Aggregate per-wallet equity curve assuming sequential lifecycles with
     base capital $BASE_CAPITAL.

ASSUMPTIONS (v1, simplistic — iterate later)
============================================
- Fixed $COPY_NOTIONAL per lifecycle (default $50).
- One open lifecycle per wallet per coin at a time (re-opens treated as new).
- No per-wallet risk overrides (no SL, trailing, max_hold).
- Slippage: 0 bps (use wallet's actual fill prices).
- HL RT fee: 8.64 bps (FEE_RT constant from research/).
- We sequentially process every lifecycle for every wallet; no concurrency limit.
- Position sizing: $COPY_NOTIONAL fixed regardless of wallet size.

OUTPUT
======
app/data/v13/copy_backtest_results.parquet
Columns: wallet, coin, entry_ts, exit_ts, hold_sec, side, entry_px,
         exit_px, pnl_pct, pnl_usd, n_fills_lifecycle.

Plus aggregate summary printed to stdout: per-wallet ROI, win rate, n_trades.
"""
from __future__ import annotations

import argparse
import glob
import logging
import sys
import time
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(name)s] %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger('copy_backtest')

FEE_RT = 0.000864  # HL round-trip taker fee
FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
CANDIDATES = Path('/Users/hermes/quants-lab/app/data/v13/wallet_copy_candidates.parquet')
OUT_PATH = Path('/Users/hermes/quants-lab/app/data/v13/copy_backtest_results.parquet')
SUMMARY_PATH = Path('/Users/hermes/quants-lab/app/data/v13/copy_backtest_summary.parquet')

WINDOW_START_MS = int(pd.Timestamp('2025-12-01', tz='UTC').timestamp() * 1000)
WINDOW_END_MS = int(pd.Timestamp('2026-05-25', tz='UTC').timestamp() * 1000)


def load_wallet_fills(wallet: str) -> pd.DataFrame:
    """Load all fills for one wallet in the backtest window."""
    files = sorted(glob.glob(str(FILLS_DIR / '2025*.parquet')) +
                   glob.glob(str(FILLS_DIR / '2026*.parquet')))
    chunks = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=['wallet', 'coin', 'side', 'size',
                                             'price', 'time', 'closedPnl',
                                             'fee', 'builderFee', 'deployerFee'])
        except Exception:
            continue
        df = df[df['wallet'] == wallet]
        if df.empty:
            continue
        for c in ['size', 'price', 'closedPnl', 'fee', 'builderFee', 'deployerFee']:
            df[c] = df[c].astype(float)
        df['time'] = df['time'].astype('int64')
        df = df[(df['time'] >= WINDOW_START_MS) & (df['time'] <= WINDOW_END_MS)]
        if not df.empty:
            chunks.append(df)
    if not chunks:
        return pd.DataFrame()
    out = pd.concat(chunks, ignore_index=True)
    out = out.sort_values(['coin', 'time']).reset_index(drop=True)
    return out


def extract_lifecycles(fills: pd.DataFrame) -> list[dict]:
    """Walk per-coin fills, identify each (open→flat) lifecycle.

    Returns list of dicts: coin, entry_ts, exit_ts, hold_sec, side,
    entry_px, exit_px, n_fills_lifecycle.

    Sign convention: HL fill side 'B' = buy (positive size delta),
    'A' = sell (negative size delta).
    """
    out = []
    for coin, sub in fills.groupby('coin', sort=False):
        sub = sub.sort_values('time')
        pos = 0.0
        entry_px = None
        entry_ts = None
        entry_side = None
        n_in_lifecycle = 0
        for _, fill in sub.iterrows():
            sz_delta = fill['size'] if fill['side'] == 'B' else -fill['size']
            prev_pos = pos
            pos += sz_delta
            n_in_lifecycle += 1
            # New lifecycle starts when we go from flat to nonzero
            if abs(prev_pos) < 1e-9 and abs(pos) > 1e-9:
                entry_px = float(fill['price'])
                entry_ts = int(fill['time'])
                entry_side = 'long' if pos > 0 else 'short'
                n_in_lifecycle = 1
            # Lifecycle closes when we go back to flat (or sign flips → treat as 2 lifecycles)
            elif abs(pos) < 1e-9 and entry_px is not None:
                exit_px = float(fill['price'])
                hold_sec = (int(fill['time']) - entry_ts) / 1000.0
                out.append({
                    'coin': coin,
                    'entry_ts': entry_ts,
                    'exit_ts': int(fill['time']),
                    'hold_sec': hold_sec,
                    'side': entry_side,
                    'entry_px': entry_px,
                    'exit_px': exit_px,
                    'n_fills_lifecycle': n_in_lifecycle,
                })
                entry_px = None
                entry_ts = None
                entry_side = None
                n_in_lifecycle = 0
            # Sign flip (rare): close prior + open new
            elif entry_px is not None and (prev_pos > 0) != (pos > 0):
                # Close at this fill's price
                exit_px = float(fill['price'])
                hold_sec = (int(fill['time']) - entry_ts) / 1000.0
                out.append({
                    'coin': coin,
                    'entry_ts': entry_ts,
                    'exit_ts': int(fill['time']),
                    'hold_sec': hold_sec,
                    'side': entry_side,
                    'entry_px': entry_px,
                    'exit_px': exit_px,
                    'n_fills_lifecycle': n_in_lifecycle,
                })
                entry_px = float(fill['price'])
                entry_ts = int(fill['time'])
                entry_side = 'long' if pos > 0 else 'short'
                n_in_lifecycle = 1
        # Open lifecycle at end of window — exclude (no exit yet)
    return out


def simulate_copy_pnl(lifecycle: dict, copy_notional: float) -> tuple[float, float]:
    """Returns (pnl_pct, pnl_usd) for copying this lifecycle with copy_notional."""
    entry_px = lifecycle['entry_px']
    exit_px = lifecycle['exit_px']
    if entry_px <= 0:
        return 0.0, 0.0
    if lifecycle['side'] == 'long':
        raw_pnl_pct = (exit_px - entry_px) / entry_px
    else:
        raw_pnl_pct = (entry_px - exit_px) / entry_px
    net_pnl_pct = raw_pnl_pct - FEE_RT
    pnl_usd = copy_notional * net_pnl_pct
    return net_pnl_pct, pnl_usd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--candidates', type=str, default=str(CANDIDATES))
    ap.add_argument('--top-n', type=int, default=50)
    ap.add_argument('--copy-notional', type=float, default=50.0)
    ap.add_argument('--base-capital', type=float, default=1000.0)
    ap.add_argument('--max-reconciliation-pct', type=float, default=50.0,
                    help='Skip wallets with worse reconciliation_pct than this')
    args = ap.parse_args()

    cand = pd.read_parquet(args.candidates)
    if 'reconciliation_pct' in cand.columns:
        cand = cand[cand['reconciliation_pct'] <= args.max_reconciliation_pct].copy()
    cand = cand.sort_values('roi_window', ascending=False).head(args.top_n)
    logger.info(f'Backtesting {len(cand)} wallets (top-{args.top_n} by ROI, '
                f'reconciliation_pct<={args.max_reconciliation_pct}%)')

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
        wallet_pnl_usd = 0.0
        wallet_pnl_pct = []
        wins = 0
        for lc in lifecycles:
            pnl_pct, pnl_usd = simulate_copy_pnl(lc, args.copy_notional)
            lc['wallet'] = w
            lc['pnl_pct'] = pnl_pct
            lc['pnl_usd'] = pnl_usd
            all_trades.append(lc)
            wallet_pnl_usd += pnl_usd
            wallet_pnl_pct.append(pnl_pct)
            if pnl_usd > 0:
                wins += 1
        if not lifecycles:
            continue
        roi_on_base = wallet_pnl_usd / args.base_capital
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
            'copy_pnl_usd': wallet_pnl_usd,
            'copy_roi_on_base': roi_on_base,
            'copy_pnl_per_trade': wallet_pnl_usd / len(lifecycles),
            'best_trade_pnl': max((lc['pnl_usd'] for lc in lifecycles), default=0.0),
            'worst_trade_pnl': min((lc['pnl_usd'] for lc in lifecycles), default=0.0),
        })
        if i % 10 == 0:
            logger.info(f'  [{i}/{len(cand)}] {time.time()-t0:.0f}s | '
                        f'lifecycles in cum: {len(all_trades):,}')

    trades_df = pd.DataFrame(all_trades)
    summary_df = pd.DataFrame(summary).sort_values('copy_roi_on_base', ascending=False)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    trades_df.to_parquet(OUT_PATH, index=False, compression='snappy')
    summary_df.to_parquet(SUMMARY_PATH, index=False, compression='snappy')
    logger.info(f'Wrote {OUT_PATH} ({len(trades_df):,} trades)')
    logger.info(f'Wrote {SUMMARY_PATH} ({len(summary_df):,} wallets)')

    # Print top-N summary
    n_show = min(20, len(summary_df))
    print(f'\n=== Top {n_show} candidates by copy_roi_on_base (copy_notional=${args.copy_notional}, base=${args.base_capital}) ===')
    show_cols = ['wallet', 'wallet_dec1_anchor_usd', 'wallet_roi_window',
                 'reconciliation_pct', 'n_trades_simulated', 'win_rate',
                 'avg_hold_sec', 'copy_pnl_usd', 'copy_roi_on_base',
                 'best_trade_pnl', 'worst_trade_pnl']
    print(summary_df.head(n_show)[show_cols].to_string(index=False))
    print(f'\nAggregate (all {len(summary_df)} wallets backtested):')
    print(f'  total copy_pnl: ${summary_df["copy_pnl_usd"].sum():,.2f}')
    print(f'  median wallet ROI: {summary_df["copy_roi_on_base"].median():.2%}')
    print(f'  positive wallets: {(summary_df["copy_pnl_usd"] > 0).sum()} / {len(summary_df)}')
    print(f'  total trades simulated: {len(trades_df):,}')
    print(f'  median trade PnL: ${trades_df["pnl_usd"].median():.4f}')
    print(f'  trade win rate: {(trades_df["pnl_usd"] > 0).mean():.2%}')


if __name__ == '__main__':
    main()
