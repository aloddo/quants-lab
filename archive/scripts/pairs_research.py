"""
Statistical Arbitrage / Pairs Trading Research on Hyperliquid Perps
===================================================================
Uses 1h candle data from MongoDB (200+ days) to evaluate mean-reversion
of log-price-ratio spreads for all pair combinations.

Key metrics:
- Rolling correlation (60-bar = 60 hours)
- Spread z-score (log price ratio, 60-bar rolling mean/std)
- Mean reversion rate: % of z>2 events that revert to z<0.5 within 2h (at 1h granularity)
  and within 120 bars (for the hourly proxy of what 120 min would look like)
- Net profit per trade after fees (2x taker RT = 17.28 bps)
"""

import numpy as np
import pandas as pd
from pymongo import MongoClient
from itertools import combinations
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# --- Config ---
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "quants_lab"
COLLECTION = "hyperliquid_candles_1h"

# Pairs with sufficient data (200+ days of 1h candles)
PAIRS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "SUI-USDT", "DOGE-USDT",
    "LINK-USDT", "AAVE-USDT", "XRP-USDT", "TAO-USDT", "NEAR-USDT",
    "DOT-USDT", "ADA-USDT",
]

# Parameters
ZSCORE_WINDOW = 60       # rolling window for z-score (60 hours)
ZSCORE_ENTRY = 2.0       # entry threshold
ZSCORE_EXIT = 0.5        # exit threshold (close to mean)
MAX_HOLD_BARS = 120      # max hold period in bars (120 hours ~ 5 days)
FEE_RT_TAKER = 0.000864  # HL round-trip taker fee per leg
TOTAL_FEE_BPS = 2 * FEE_RT_TAKER * 10000  # 17.28 bps for 2 legs


def load_data():
    """Load 1h close prices from MongoDB, return DataFrame with pair columns."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]

    frames = {}
    for pair in PAIRS:
        cursor = col.find(
            {"pair": pair, "interval": "1h"},
            {"_id": 0, "timestamp_utc": 1, "close": 1}
        ).sort("timestamp_utc", 1)
        df = pd.DataFrame(list(cursor))
        if len(df) == 0:
            print(f"  WARNING: No data for {pair}")
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp_utc"], unit="ms")
        df = df.set_index("timestamp")["close"]
        df = df[~df.index.duplicated(keep="first")]
        frames[pair] = df
        print(f"  {pair}: {len(df)} bars, {df.index.min()} to {df.index.max()}")

    client.close()
    prices = pd.DataFrame(frames)
    prices = prices.sort_index().ffill()
    # Trim to common date range (where all pairs have data)
    prices = prices.dropna()
    print(f"\nCommon range: {prices.index.min()} to {prices.index.max()}, {len(prices)} bars")
    return prices


def compute_spread_zscore(prices, pair_a, pair_b, window=ZSCORE_WINDOW):
    """Compute log price ratio spread and its z-score."""
    log_ratio = np.log(prices[pair_a] / prices[pair_b])
    roll_mean = log_ratio.rolling(window).mean()
    roll_std = log_ratio.rolling(window).std()
    zscore = (log_ratio - roll_mean) / roll_std
    return log_ratio, zscore


def analyze_pair(prices, pair_a, pair_b):
    """Analyze a single pair combination for mean reversion."""
    log_ratio, zscore = compute_spread_zscore(prices, pair_a, pair_b)

    # Rolling correlation
    ret_a = prices[pair_a].pct_change()
    ret_b = prices[pair_b].pct_change()
    rolling_corr = ret_a.rolling(ZSCORE_WINDOW).corr(ret_b)
    mean_corr = rolling_corr.dropna().mean()
    min_corr = rolling_corr.dropna().quantile(0.05)

    # Find z-score breach events (|z| > entry threshold)
    z_vals = zscore.dropna().values
    z_idx = zscore.dropna().index
    log_vals = log_ratio.reindex(z_idx).values

    events_long = []   # z < -ENTRY (spread too low, buy spread)
    events_short = []  # z > +ENTRY (spread too high, sell spread)

    i = 0
    while i < len(z_vals):
        if abs(z_vals[i]) >= ZSCORE_ENTRY:
            direction = 1 if z_vals[i] < -ZSCORE_ENTRY else -1  # 1=long spread, -1=short
            entry_log = log_vals[i]
            entry_idx = i

            # Look for reversion within MAX_HOLD_BARS
            reverted = False
            exit_log = None
            for j in range(i + 1, min(i + MAX_HOLD_BARS + 1, len(z_vals))):
                if abs(z_vals[j]) <= ZSCORE_EXIT:
                    reverted = True
                    exit_log = log_vals[j]
                    exit_bar = j
                    break

            if not reverted:
                # Use value at MAX_HOLD_BARS or end of data
                end_j = min(i + MAX_HOLD_BARS, len(z_vals) - 1)
                exit_log = log_vals[end_j]
                exit_bar = end_j

            # Profit = direction * (exit_log - entry_log) in log terms ~ bps
            raw_profit_bps = direction * (exit_log - entry_log) * 10000
            net_profit_bps = raw_profit_bps - TOTAL_FEE_BPS
            hold_bars = exit_bar - i

            event = {
                "reverted": reverted,
                "raw_profit_bps": raw_profit_bps,
                "net_profit_bps": net_profit_bps,
                "hold_bars": hold_bars,
                "direction": direction,
            }
            if direction == 1:
                events_long.append(event)
            else:
                events_short.append(event)

            # Skip past exit to avoid overlapping events
            i = exit_bar + 1
        else:
            i += 1

    all_events = events_long + events_short
    if len(all_events) == 0:
        return None

    n_events = len(all_events)
    n_reverted = sum(1 for e in all_events if e["reverted"])
    reversion_rate = n_reverted / n_events if n_events > 0 else 0

    raw_profits = [e["raw_profit_bps"] for e in all_events]
    net_profits = [e["net_profit_bps"] for e in all_events]
    hold_bars_list = [e["hold_bars"] for e in all_events]

    # Only count reverted trades for profit calc (those are the ones we'd take)
    reverted_nets = [e["net_profit_bps"] for e in all_events if e["reverted"]]
    mean_net_reverted = np.mean(reverted_nets) if reverted_nets else 0

    # Days in sample
    total_hours = len(zscore.dropna())
    total_days = total_hours / 24
    events_per_day = n_events / total_days if total_days > 0 else 0
    reverted_per_day = n_reverted / total_days if total_days > 0 else 0

    # Win rate on reverted trades
    wins = sum(1 for p in reverted_nets if p > 0)
    win_rate = wins / len(reverted_nets) if reverted_nets else 0

    return {
        "pair_a": pair_a.replace("-USDT", ""),
        "pair_b": pair_b.replace("-USDT", ""),
        "mean_corr": mean_corr,
        "min_corr_5pct": min_corr,
        "n_events": n_events,
        "n_reverted": n_reverted,
        "reversion_rate": reversion_rate,
        "mean_raw_bps": np.mean(raw_profits),
        "mean_net_bps": np.mean(net_profits),
        "mean_net_reverted_bps": mean_net_reverted,
        "median_net_reverted_bps": np.median(reverted_nets) if reverted_nets else 0,
        "events_per_day": events_per_day,
        "reverted_per_day": reverted_per_day,
        "mean_hold_bars": np.mean(hold_bars_list),
        "win_rate": win_rate,
        "total_days": total_days,
        "sharpe_est": (np.mean(reverted_nets) / np.std(reverted_nets)) if reverted_nets and np.std(reverted_nets) > 0 else 0,
    }


def main():
    print("=" * 80)
    print("STATISTICAL ARBITRAGE / PAIRS TRADING RESEARCH -- HYPERLIQUID PERPS")
    print("=" * 80)
    print(f"\nParameters:")
    print(f"  Z-score window: {ZSCORE_WINDOW} bars (hours)")
    print(f"  Entry threshold: z = +/- {ZSCORE_ENTRY}")
    print(f"  Exit threshold: z = +/- {ZSCORE_EXIT}")
    print(f"  Max hold: {MAX_HOLD_BARS} bars ({MAX_HOLD_BARS} hours)")
    print(f"  Fee per trade (2 legs taker): {TOTAL_FEE_BPS:.2f} bps")
    print(f"\nLoading data...")

    prices = load_data()

    combos = list(combinations(PAIRS, 2))
    print(f"\nAnalyzing {len(combos)} pair combinations...")

    results = []
    for pair_a, pair_b in combos:
        res = analyze_pair(prices, pair_a, pair_b)
        if res is not None:
            results.append(res)

    df = pd.DataFrame(results)

    # --- FULL RESULTS (sorted by net profit on reverted trades) ---
    print("\n" + "=" * 80)
    print("ALL PAIR COMBINATIONS (sorted by mean net profit on reverted trades)")
    print("=" * 80)
    df_sorted = df.sort_values("mean_net_reverted_bps", ascending=False)

    cols_display = [
        "pair_a", "pair_b", "mean_corr", "n_events", "n_reverted",
        "reversion_rate", "mean_net_reverted_bps", "median_net_reverted_bps",
        "events_per_day", "reverted_per_day", "mean_hold_bars", "win_rate", "sharpe_est"
    ]
    headers = {
        "pair_a": "Pair A",
        "pair_b": "Pair B",
        "mean_corr": "Corr",
        "n_events": "Events",
        "n_reverted": "Reverted",
        "reversion_rate": "Rev%",
        "mean_net_reverted_bps": "NetBps",
        "median_net_reverted_bps": "MedBps",
        "events_per_day": "Ev/Day",
        "reverted_per_day": "Rev/Day",
        "mean_hold_bars": "AvgHold",
        "win_rate": "Win%",
        "sharpe_est": "Sharpe",
    }
    print(df_sorted[cols_display].rename(columns=headers).to_string(
        index=False,
        float_format=lambda x: f"{x:.2f}"
    ))

    # --- FILTERED TOP PAIRS ---
    print("\n" + "=" * 80)
    print("FILTERED: Reversion > 60%, Net Profit > 10 bps, Frequency > 0.5/day")
    print("=" * 80)

    filtered = df[
        (df["reversion_rate"] > 0.60) &
        (df["mean_net_reverted_bps"] > 10) &
        (df["reverted_per_day"] > 0.5)
    ].sort_values("mean_net_reverted_bps", ascending=False).head(10)

    if len(filtered) == 0:
        print("\nNo pairs pass the strict filter. Relaxing to: Rev > 50%, Net > 5 bps, Freq > 0.3/day")
        filtered = df[
            (df["reversion_rate"] > 0.50) &
            (df["mean_net_reverted_bps"] > 5) &
            (df["reverted_per_day"] > 0.3)
        ].sort_values("mean_net_reverted_bps", ascending=False).head(10)

    if len(filtered) == 0:
        print("\nStill no pairs pass. Showing top 10 by net profit regardless:")
        filtered = df_sorted.head(10)

    print(filtered[cols_display].rename(columns=headers).to_string(
        index=False,
        float_format=lambda x: f"{x:.2f}"
    ))

    # --- SUMMARY STATISTICS ---
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total pair combinations analyzed: {len(df)}")
    print(f"Mean reversion rate across all pairs: {df['reversion_rate'].mean():.1%}")
    print(f"Median reversion rate: {df['reversion_rate'].median():.1%}")
    print(f"Mean net profit (reverted trades): {df['mean_net_reverted_bps'].mean():.1f} bps")
    print(f"Median net profit (reverted trades): {df['mean_net_reverted_bps'].median():.1f} bps")
    print(f"Mean events/day: {df['events_per_day'].mean():.2f}")
    print(f"Pairs with positive mean net profit: {(df['mean_net_reverted_bps'] > 0).sum()}/{len(df)}")
    print(f"Pairs with Rev>60% & Net>10bps: {((df['reversion_rate'] > 0.6) & (df['mean_net_reverted_bps'] > 10)).sum()}/{len(df)}")

    # --- DAILY PNL ESTIMATE for top pairs ---
    if len(filtered) > 0:
        print("\n" + "=" * 80)
        print("DAILY PNL ESTIMATE (per $10,000 notional per leg)")
        print("=" * 80)
        notional = 10000
        for _, row in filtered.iterrows():
            daily_trades = row["reverted_per_day"]
            net_bps = row["mean_net_reverted_bps"]
            daily_pnl = daily_trades * (net_bps / 10000) * notional
            monthly_pnl = daily_pnl * 30
            print(f"  {row['pair_a']}/{row['pair_b']}: "
                  f"{daily_trades:.1f} trades/day x {net_bps:.1f} bps = "
                  f"${daily_pnl:.2f}/day (${monthly_pnl:.0f}/month)")


if __name__ == "__main__":
    main()
