"""
Event-driven backtest: copy-trade top HL wallets on Bybit.

Questions answered:
1. If we copy top-scored wallets with N-second delay, what's the net PnL?
2. Which holding period is optimal (30s, 60s, 120s, 300s, 600s)?
3. How much latency can we tolerate before edge decays?
4. Is the signal concentrated in 1 wallet or distributed?

Uses hl_wallet_trades + hyperliquid_candles_1h for price reference.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# Config
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.environ.get("MONGO_DATABASE", "quants_lab")

# Fee assumptions (Bybit perp)
TAKER_FEE_BPS = 5.5  # round-trip taker-taker
MAKER_TAKER_FEE_BPS = 3.5  # maker entry, taker exit
SLIPPAGE_BPS = 1.0  # 1 tick slippage on entry

# Parameters to sweep
HOLD_PERIODS_S = [30, 60, 120, 300, 600]
LATENCY_S = [0.5, 1.0, 2.0, 5.0]
MIN_MARKOUT_TSTAT = 2.0
MIN_FILLS = 20


def load_top_wallets(db, min_tstat=MIN_MARKOUT_TSTAT, min_fills=MIN_FILLS):
    """Load wallet profiles with significant positive markout at 120s."""
    profiles = list(db['hl_wallet_profiles'].find(
        {'markout_120s_tstat': {'$gt': min_tstat}, 'fills_with_markout': {'$gte': min_fills}},
        {'address': 1, 'markout_120s_shrunk': 1, 'markout_120s_tstat': 1,
         'markout_600s_shrunk': 1, 'fills_with_markout': 1, 'coins': 1}
    ).sort('markout_120s_shrunk', -1))
    return profiles


def load_trades(db, wallet_addresses):
    """Load all trades involving top wallets (both aggressive and passive).

    In hl_wallet_trades:
    - buyer/seller = wallet addresses on each side
    - side = 'B' means buyer was aggressor, 'A' means seller was aggressor
    - For copy-trading, we care about the wallet's DIRECTION, not aggression:
      - wallet is buyer → BUY direction
      - wallet is seller → SELL direction
    """
    trades = []
    for addr in wallet_addresses:
        # Wallet was the BUYER (regardless of who aggressed)
        buyer_trades = list(db['hl_wallet_trades'].find(
            {'buyer': addr},
            {'timestamp': 1, 'coin': 1, 'price': 1, 'size': 1, 'notional': 1, 'buyer': 1, 'side': 1}
        ).sort('timestamp', 1))
        for t in buyer_trades:
            t['wallet'] = addr
            t['direction'] = 'BUY'
            t['was_aggressor'] = (t.get('side') == 'B')
        trades.extend(buyer_trades)

        # Wallet was the SELLER (regardless of who aggressed)
        seller_trades = list(db['hl_wallet_trades'].find(
            {'seller': addr},
            {'timestamp': 1, 'coin': 1, 'price': 1, 'size': 1, 'notional': 1, 'seller': 1, 'side': 1}
        ).sort('timestamp', 1))
        for t in seller_trades:
            t['wallet'] = addr
            t['direction'] = 'SELL'
            t['was_aggressor'] = (t.get('side') == 'A')
        trades.extend(seller_trades)

    return sorted(trades, key=lambda x: x['timestamp'])


def build_price_index(db, coins):
    """Build high-resolution price index from recent trades data."""
    price_index = {}
    for coin in coins:
        # Get all trades for this coin, sorted by time
        raw = list(db['hl_wallet_trades'].find(
            {'coin': coin},
            {'timestamp': 1, 'price': 1}
        ).sort('timestamp', 1))

        if not raw:
            continue

        # Build a price series (last price at each second)
        prices = {}
        for r in raw:
            ts_sec = int(r['timestamp'])
            prices[ts_sec] = r['price']

        price_index[coin] = prices

    return price_index


def get_price_at(price_index, coin, timestamp):
    """Get closest price at or after timestamp."""
    if coin not in price_index:
        return None

    prices = price_index[coin]
    ts_sec = int(timestamp)

    # Look for price within 30 seconds
    for offset in range(31):
        if (ts_sec + offset) in prices:
            return prices[ts_sec + offset]
        if (ts_sec - offset) in prices:
            return prices[ts_sec - offset]

    return None


def run_backtest(trades, price_index, wallet_scores, hold_period_s, latency_s, fee_bps):
    """
    Simulate copy-trading: for each smart wallet trade,
    enter at trade_price + latency slippage, exit after hold_period.
    """
    results = []

    for trade in trades:
        coin = trade['coin']
        entry_time = trade['timestamp'] + latency_s
        exit_time = entry_time + hold_period_s
        direction = trade['direction']
        wallet = trade['wallet']

        entry_price = get_price_at(price_index, coin, entry_time)
        exit_price = get_price_at(price_index, coin, exit_time)

        if entry_price is None or exit_price is None:
            continue

        # Compute raw return
        if direction == 'BUY':
            raw_return_bps = (exit_price - entry_price) / entry_price * 10000
        else:
            raw_return_bps = (entry_price - exit_price) / entry_price * 10000

        # Subtract fees + slippage
        net_return_bps = raw_return_bps - fee_bps - SLIPPAGE_BPS

        results.append({
            'timestamp': trade['timestamp'],
            'coin': coin,
            'direction': direction,
            'wallet': wallet[:12],
            'wallet_score': wallet_scores.get(wallet, 0),
            'entry_price': entry_price,
            'exit_price': exit_price,
            'raw_return_bps': raw_return_bps,
            'net_return_bps': net_return_bps,
            'notional': trade.get('notional', 0),
        })

    return results


def analyze_results(results, label=""):
    """Print summary statistics for backtest results."""
    if not results:
        print(f"\n{label}: NO TRADES")
        return

    df = pd.DataFrame(results)

    n = len(df)
    win_rate = (df['net_return_bps'] > 0).mean() * 100
    avg_return = df['net_return_bps'].mean()
    median_return = df['net_return_bps'].median()
    total_return = df['net_return_bps'].sum()
    sharpe = df['net_return_bps'].mean() / df['net_return_bps'].std() * np.sqrt(252 * 24) if df['net_return_bps'].std() > 0 else 0

    # Per-wallet breakdown
    by_wallet = df.groupby('wallet')['net_return_bps'].agg(['mean', 'count', 'sum'])

    # Per-coin breakdown
    by_coin = df.groupby('coin')['net_return_bps'].agg(['mean', 'count', 'sum'])

    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"{'='*60}")
    print(f"Trades: {n} | WinRate: {win_rate:.1f}% | Avg: {avg_return:+.2f}bps | Med: {median_return:+.2f}bps")
    print(f"Total: {total_return:+.1f}bps | Sharpe(ann): {sharpe:.2f}")

    print(f"\n  By wallet:")
    for wallet, row in by_wallet.sort_values('mean', ascending=False).iterrows():
        print(f"    {wallet}  mean={row['mean']:+.2f}bps  n={int(row['count'])}  total={row['sum']:+.1f}bps")

    print(f"\n  By coin:")
    for coin, row in by_coin.sort_values('mean', ascending=False).iterrows():
        print(f"    {coin:8s}  mean={row['mean']:+.2f}bps  n={int(row['count'])}  total={row['sum']:+.1f}bps")

    # Direction breakdown
    by_dir = df.groupby('direction')['net_return_bps'].agg(['mean', 'count'])
    print(f"\n  By direction:")
    for d, row in by_dir.iterrows():
        print(f"    {d:6s}  mean={row['mean']:+.2f}bps  n={int(row['count'])}")

    return df


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # 1. Load top wallets
    wallets = load_top_wallets(db)
    print(f"Top wallets (t>2, n>=20): {len(wallets)}")

    wallet_scores = {}
    wallet_addrs = []
    for w in wallets:
        addr = w['address']
        score = w['markout_120s_shrunk']
        wallet_addrs.append(addr)
        wallet_scores[addr] = score
        print(f"  {addr[:12]}... score={score:+.2f}bps t={w['markout_120s_tstat']:.1f} coins={w.get('coins',[])} n={w['fills_with_markout']}")

    # 2. Load trades from these wallets
    print(f"\nLoading trades for {len(wallet_addrs)} wallets...")
    trades = load_trades(db, wallet_addrs)
    print(f"Total trades: {len(trades)}")

    # Get unique coins
    coins = list(set(t['coin'] for t in trades))
    print(f"Coins: {coins}")

    # 3. Build price index
    print("Building price index from all trades...")
    price_index = build_price_index(db, coins)
    for coin, prices in price_index.items():
        print(f"  {coin}: {len(prices)} price points")

    # 4. Run backtests: sweep hold period x latency
    print(f"\n{'='*60}")
    print("PARAMETER SWEEP: Hold Period x Latency x Fee Model")
    print(f"{'='*60}")

    summary = []

    for hold_s in HOLD_PERIODS_S:
        for lat_s in LATENCY_S:
            for fee_label, fee_bps in [("taker-taker", TAKER_FEE_BPS), ("maker-taker", MAKER_TAKER_FEE_BPS)]:
                results = run_backtest(trades, price_index, wallet_scores, hold_s, lat_s, fee_bps)

                if not results:
                    continue

                df = pd.DataFrame(results)
                n = len(df)
                avg = df['net_return_bps'].mean()
                wr = (df['net_return_bps'] > 0).mean() * 100
                total = df['net_return_bps'].sum()

                summary.append({
                    'hold_s': hold_s,
                    'latency_s': lat_s,
                    'fees': fee_label,
                    'n_trades': n,
                    'avg_bps': avg,
                    'win_rate': wr,
                    'total_bps': total,
                })

    # Print summary table
    print(f"\n{'Hold':>6s} {'Lat':>5s} {'Fees':>12s} {'N':>5s} {'Avg(bps)':>9s} {'WR%':>5s} {'Total':>8s}")
    print("-" * 60)
    for s in sorted(summary, key=lambda x: x['avg_bps'], reverse=True):
        print(f"{s['hold_s']:>5d}s {s['latency_s']:>4.1f}s {s['fees']:>12s} {s['n_trades']:>5d} {s['avg_bps']:>+8.2f} {s['win_rate']:>5.1f} {s['total_bps']:>+8.1f}")

    # 5. Deep-dive on best configuration
    if summary:
        best = max(summary, key=lambda x: x['avg_bps'])
        print(f"\n\nBest config: hold={best['hold_s']}s lat={best['latency_s']}s fees={best['fees']}")

        fee_bps = TAKER_FEE_BPS if best['fees'] == 'taker-taker' else MAKER_TAKER_FEE_BPS
        results = run_backtest(trades, price_index, wallet_scores, best['hold_s'], best['latency_s'], fee_bps)
        analyze_results(results, f"BEST: hold={best['hold_s']}s lat={best['latency_s']}s {best['fees']}")

    # 6. Top wallet only (concentration check)
    if wallets:
        top_wallet = wallets[0]['address']
        top_trades = [t for t in trades if t['wallet'] == top_wallet]
        print(f"\n\n=== CONCENTRATION CHECK: Top wallet only ({top_wallet[:12]}...) ===")
        for hold_s in [120, 300, 600]:
            results = run_backtest(top_trades, price_index, wallet_scores, hold_s, 1.0, MAKER_TAKER_FEE_BPS)
            if results:
                df = pd.DataFrame(results)
                avg = df['net_return_bps'].mean()
                wr = (df['net_return_bps'] > 0).mean() * 100
                print(f"  hold={hold_s}s: n={len(results)} avg={avg:+.2f}bps WR={wr:.1f}%")


if __name__ == "__main__":
    main()
