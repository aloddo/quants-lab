"""
Event-driven backtest: Smart Wallet Imbalance (SWI) signal.

Instead of copy-trading individual wallets, aggregate ALL smart wallet
activity into a directional signal and trade on threshold crossings.

SWI[coin, t] = sum(score[w] * sign[w,t] * notional[w,t]) over [t-L, t]

Key question: does aggregate smart money flow predict direction?
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
import pandas as pd
import numpy as np
from collections import defaultdict

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.environ.get("MONGO_DATABASE", "quants_lab")

# Fee models (Bybit perp)
TAKER_RT_BPS = 5.5
MAKER_TAKER_BPS = 3.5
SLIPPAGE_BPS = 1.0


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # 1. Load ALL wallet profiles (not just top)
    all_profiles = list(db['hl_wallet_profiles'].find(
        {'fills_with_markout': {'$gte': 10}},
        {'address': 1, 'markout_120s_shrunk': 1, 'markout_120s_tstat': 1}
    ))
    wallet_scores = {p['address']: p['markout_120s_shrunk'] for p in all_profiles}
    print(f"Loaded {len(wallet_scores)} wallet scores")
    print(f"  Score range: {min(wallet_scores.values()):.2f} to {max(wallet_scores.values()):.2f} bps")
    print(f"  Positive-score wallets: {sum(1 for v in wallet_scores.values() if v > 0)}")
    print(f"  Negative-score wallets: {sum(1 for v in wallet_scores.values() if v < 0)}")

    # 2. Load ALL trades for scored wallets (both sides)
    print("\nLoading trades...")
    trades_by_coin = defaultdict(list)
    scored_addrs = set(wallet_scores.keys())

    # Load all trades and check if buyer or seller is in scored wallets
    cursor = db['hl_wallet_trades'].find(
        {},
        {'timestamp': 1, 'coin': 1, 'price': 1, 'notional': 1, 'buyer': 1, 'seller': 1, 'side': 1}
    ).sort('timestamp', 1)

    total_scored = 0
    for doc in cursor:
        buyer = doc.get('buyer', '')
        seller = doc.get('seller', '')
        coin = doc['coin']
        ts = doc['timestamp']
        notional = doc.get('notional', 0)
        side = doc.get('side', '')  # B = buyer aggressor, A = seller aggressor

        # For SWI, use the aggressor's direction weighted by wallet score
        if side == 'B' and buyer in scored_addrs:
            # Buyer aggressed — buyer's direction is BUY
            score = wallet_scores[buyer]
            trades_by_coin[coin].append({
                'ts': ts, 'sign': +1, 'score': score,
                'notional': notional, 'price': doc['price'],
                'wallet': buyer[:12]
            })
            total_scored += 1
        elif side == 'A' and seller in scored_addrs:
            # Seller aggressed — seller's direction is SELL
            score = wallet_scores[seller]
            trades_by_coin[coin].append({
                'ts': ts, 'sign': -1, 'score': score,
                'notional': notional, 'price': doc['price'],
                'wallet': seller[:12]
            })
            total_scored += 1

    print(f"Total scored aggressive trades: {total_scored}")
    for coin, trades in sorted(trades_by_coin.items(), key=lambda x: -len(x[1])):
        print(f"  {coin:10s}: {len(trades)} trades")

    # 3. Build price index (all trades, not just scored)
    print("\nBuilding price index...")
    price_index = {}
    for coin in trades_by_coin:
        raw = list(db['hl_wallet_trades'].find(
            {'coin': coin}, {'timestamp': 1, 'price': 1}
        ).sort('timestamp', 1))
        prices = {}
        for r in raw:
            ts_sec = int(r['timestamp'])
            prices[ts_sec] = r['price']
        price_index[coin] = prices
        print(f"  {coin}: {len(prices)} price points")

    # 4. Compute rolling SWI and test threshold crossings
    print("\n" + "=" * 70)
    print("SWI SIGNAL ANALYSIS")
    print("=" * 70)

    # For each coin with enough data, compute SWI over rolling windows
    lookbacks = [30, 60, 120, 300]  # seconds
    hold_periods = [60, 120, 300, 600]

    for coin in sorted(trades_by_coin.keys()):
        trades = trades_by_coin[coin]
        if len(trades) < 20:
            continue

        prices = price_index.get(coin, {})
        if not prices:
            continue

        print(f"\n--- {coin} ({len(trades)} scored trades) ---")

        for lookback in lookbacks:
            # Compute SWI at each trade timestamp
            swi_values = []
            for i, trade in enumerate(trades):
                ts = trade['ts']
                # Sum score-weighted flow in [ts-lookback, ts]
                swi = 0.0
                for j in range(max(0, i - 200), i + 1):  # look back up to 200 trades
                    t = trades[j]
                    if t['ts'] >= ts - lookback and t['ts'] <= ts:
                        swi += t['score'] * t['sign'] * t['notional']

                swi_values.append({
                    'ts': ts,
                    'swi': swi,
                    'price': trade['price'],
                })

            if not swi_values:
                continue

            df = pd.DataFrame(swi_values)

            # Compute forward returns at each SWI reading
            for hold_s in hold_periods:
                fwd_returns = []
                for _, row in df.iterrows():
                    entry_ts = int(row['ts'])
                    exit_ts = entry_ts + hold_s
                    entry_px = row['price']

                    # Find exit price
                    exit_px = None
                    for offset in range(31):
                        if (exit_ts + offset) in prices:
                            exit_px = prices[exit_ts + offset]
                            break
                        if (exit_ts - offset) in prices:
                            exit_px = prices[exit_ts - offset]
                            break

                    if exit_px is None:
                        continue

                    # If SWI > 0 → smart money net buying → go LONG
                    if row['swi'] > 0:
                        ret_bps = (exit_px - entry_px) / entry_px * 10000
                    elif row['swi'] < 0:
                        ret_bps = (entry_px - exit_px) / entry_px * 10000
                    else:
                        continue

                    fwd_returns.append({
                        'swi': row['swi'],
                        'swi_abs': abs(row['swi']),
                        'ret_bps': ret_bps,
                        'net_bps': ret_bps - MAKER_TAKER_BPS - SLIPPAGE_BPS,
                    })

                if len(fwd_returns) < 5:
                    continue

                fdf = pd.DataFrame(fwd_returns)

                # Overall
                avg = fdf['net_bps'].mean()
                wr = (fdf['net_bps'] > 0).mean() * 100

                # Top quartile SWI (strongest signals only)
                q75 = fdf['swi_abs'].quantile(0.75)
                strong = fdf[fdf['swi_abs'] >= q75]
                strong_avg = strong['net_bps'].mean() if len(strong) > 0 else 0
                strong_wr = (strong['net_bps'] > 0).mean() * 100 if len(strong) > 0 else 0

                if lookback == 60 and hold_s in [120, 300]:
                    print(f"  L={lookback}s H={hold_s}s: n={len(fdf)} avg={avg:+.2f}bps WR={wr:.0f}% | STRONG(Q75) n={len(strong)} avg={strong_avg:+.2f}bps WR={strong_wr:.0f}%")

    # 5. Aggregate across coins — is there a universal SWI signal?
    print("\n" + "=" * 70)
    print("CROSS-COIN AGGREGATE RESULTS")
    print("=" * 70)

    for lookback in [30, 60, 120]:
        for hold_s in [60, 120, 300]:
            all_returns = []
            strong_returns = []

            for coin in trades_by_coin:
                trades = trades_by_coin[coin]
                prices = price_index.get(coin, {})
                if len(trades) < 10 or not prices:
                    continue

                for i, trade in enumerate(trades):
                    ts = trade['ts']
                    swi = 0.0
                    for j in range(max(0, i - 200), i + 1):
                        t = trades[j]
                        if t['ts'] >= ts - lookback and t['ts'] <= ts:
                            swi += t['score'] * t['sign'] * t['notional']

                    if abs(swi) < 0.01:
                        continue

                    entry_ts = int(ts)
                    exit_ts = entry_ts + hold_s
                    entry_px = trade['price']

                    exit_px = None
                    for offset in range(31):
                        if (exit_ts + offset) in prices:
                            exit_px = prices[exit_ts + offset]
                            break
                        if (exit_ts - offset) in prices:
                            exit_px = prices[exit_ts - offset]
                            break

                    if exit_px is None:
                        continue

                    if swi > 0:
                        ret = (exit_px - entry_px) / entry_px * 10000
                    else:
                        ret = (entry_px - exit_px) / entry_px * 10000

                    net = ret - MAKER_TAKER_BPS - SLIPPAGE_BPS
                    all_returns.append(net)
                    if abs(swi) > np.percentile([abs(swi)], 75):
                        strong_returns.append(net)

            if all_returns:
                arr = np.array(all_returns)
                avg = arr.mean()
                wr = (arr > 0).mean() * 100
                n = len(arr)
                tstat = avg / (arr.std() / np.sqrt(n)) if arr.std() > 0 else 0
                print(f"L={lookback:3d}s H={hold_s:3d}s: n={n:5d} avg={avg:+.2f}bps WR={wr:.1f}% t={tstat:+.2f}")


if __name__ == "__main__":
    main()
