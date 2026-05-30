"""
Token Unlock Event Study
Validates price impact of historical cliff unlock events
using our 1h candle data from Hyperliquid.
"""
import sys
sys.path.insert(0, '/Users/hermes/quants-lab')

from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

# Historical cliff unlock events that overlap with our candle data (Oct 2025 - May 2026)
# Format: (pair, unlock_date, pct_circ_supply, usd_value_M, recipient, description)
UNLOCK_EVENTS = [
    # HYPE monthly unlocks (Core Contributors)
    ("HYPE-USDT", "2025-11-06", 3.66, 371.0, "core_contributors", "HYPE Nov 2025 cliff"),
    ("HYPE-USDT", "2025-12-29", 2.66, 309.0, "core_contributors", "HYPE Dec 2025 cliff"),
    ("HYPE-USDT", "2026-01-06", 0.18, 16.4, "core_contributors", "HYPE Jan 2026 cliff"),
    ("HYPE-USDT", "2026-02-06", 0.18, 16.4, "core_contributors", "HYPE Feb 2026 cliff"),
    ("HYPE-USDT", "2026-03-06", 0.18, 16.4, "core_contributors", "HYPE Mar 2026 cliff"),
    ("HYPE-USDT", "2026-04-06", 0.18, 16.4, "core_contributors", "HYPE Apr 2026 cliff"),
    ("HYPE-USDT", "2026-05-06", 0.18, 16.4, "core_contributors", "HYPE May 2026 cliff"),

    # WLD (Worldcoin) monthly unlocks
    ("WLD-USDT", "2025-11-01", 7.19, 145.5, "community", "WLD Nov 2025 cliff"),
    ("WLD-USDT", "2025-12-01", 7.19, 145.5, "community", "WLD Dec 2025 cliff"),
    ("WLD-USDT", "2026-01-01", 7.19, 145.5, "community", "WLD Jan 2026 cliff"),
    ("WLD-USDT", "2026-02-01", 7.19, 145.5, "community", "WLD Feb 2026 cliff"),
    ("WLD-USDT", "2026-03-01", 7.19, 145.5, "community", "WLD Mar 2026 cliff"),
    ("WLD-USDT", "2026-04-01", 7.19, 145.5, "community", "WLD Apr 2026 cliff"),

    # ZRO (LayerZero) monthly unlocks
    ("ZRO-USDT", "2025-11-01", 23.13, 43.7, "investors", "ZRO Nov 2025 cliff"),
    ("ZRO-USDT", "2025-12-01", 2.57, 43.7, "investors", "ZRO Dec 2025 cliff"),
    ("ZRO-USDT", "2026-01-01", 2.57, 43.7, "investors", "ZRO Jan 2026 cliff"),
    ("ZRO-USDT", "2026-02-01", 2.57, 43.7, "investors", "ZRO Feb 2026 cliff"),
    ("ZRO-USDT", "2026-03-01", 2.57, 43.7, "investors", "ZRO Mar 2026 cliff"),
    ("ZRO-USDT", "2026-04-01", 2.57, 43.7, "investors", "ZRO Apr 2026 cliff"),

    # JUP (Jupiter) monthly unlocks
    ("JUP-USDT", "2025-11-28", 1.69, 12.8, "ecosystem", "JUP Nov 2025 cliff"),
    ("JUP-USDT", "2025-12-28", 0.53, 12.8, "ecosystem", "JUP Dec 2025 cliff"),
    ("JUP-USDT", "2026-01-28", 0.53, 12.8, "ecosystem", "JUP Jan 2026 cliff"),
    ("JUP-USDT", "2026-02-28", 0.53, 12.8, "ecosystem", "JUP Feb 2026 cliff"),
    ("JUP-USDT", "2026-03-28", 0.53, 12.8, "ecosystem", "JUP Mar 2026 cliff"),

    # SUI monthly unlocks  
    ("SUI-USDT", "2025-11-01", 2.0, 50.0, "community", "SUI Nov 2025 cliff"),
    ("SUI-USDT", "2025-12-01", 2.0, 50.0, "community", "SUI Dec 2025 cliff"),
    ("SUI-USDT", "2026-01-01", 2.0, 50.0, "community", "SUI Jan 2026 cliff"),
    ("SUI-USDT", "2026-02-01", 2.0, 50.0, "community", "SUI Feb 2026 cliff"),
    ("SUI-USDT", "2026-03-01", 2.0, 50.0, "community", "SUI Mar 2026 cliff"),
    ("SUI-USDT", "2026-04-01", 2.0, 50.0, "community", "SUI Apr 2026 cliff"),
    ("SUI-USDT", "2026-05-01", 2.0, 42.6, "community", "SUI May 2026 cliff"),

    # ENA monthly unlocks
    ("ENA-USDT", "2025-11-01", 2.0, 31.9, "ecosystem", "ENA Nov 2025 cliff"),
    ("ENA-USDT", "2025-12-01", 2.0, 31.9, "ecosystem", "ENA Dec 2025 cliff"),
    ("ENA-USDT", "2026-01-01", 2.0, 31.9, "ecosystem", "ENA Jan 2026 cliff"),
    ("ENA-USDT", "2026-02-01", 2.0, 31.9, "ecosystem", "ENA Feb 2026 cliff"),
    ("ENA-USDT", "2026-03-01", 2.0, 31.9, "ecosystem", "ENA Mar 2026 cliff"),
    ("ENA-USDT", "2026-04-01", 2.0, 31.9, "ecosystem", "ENA Apr 2026 cliff"),

    # PENDLE unlocks
    ("PENDLE-USDT", "2025-11-01", 1.5, 15.0, "ecosystem", "PENDLE Nov 2025"),
    ("PENDLE-USDT", "2025-12-01", 1.5, 15.0, "ecosystem", "PENDLE Dec 2025"),
    ("PENDLE-USDT", "2026-01-01", 1.5, 15.0, "ecosystem", "PENDLE Jan 2026"),
    ("PENDLE-USDT", "2026-02-01", 1.5, 15.0, "ecosystem", "PENDLE Feb 2026"),
    ("PENDLE-USDT", "2026-03-01", 1.5, 15.0, "ecosystem", "PENDLE Mar 2026"),
    ("PENDLE-USDT", "2026-04-01", 1.5, 15.0, "ecosystem", "PENDLE Apr 2026"),

    # NEAR monthly unlocks
    ("NEAR-USDT", "2025-11-01", 0.5, 10.0, "ecosystem", "NEAR Nov 2025"),
    ("NEAR-USDT", "2025-12-01", 0.5, 10.0, "ecosystem", "NEAR Dec 2025"),
    ("NEAR-USDT", "2026-01-01", 0.5, 10.0, "ecosystem", "NEAR Jan 2026"),
    ("NEAR-USDT", "2026-02-01", 0.5, 10.0, "ecosystem", "NEAR Feb 2026"),
    ("NEAR-USDT", "2026-03-01", 0.5, 10.0, "ecosystem", "NEAR Mar 2026"),
    ("NEAR-USDT", "2026-04-01", 0.5, 10.0, "ecosystem", "NEAR Apr 2026"),

    # TAO unlocks
    ("TAO-USDT", "2025-11-01", 1.0, 20.0, "miners", "TAO Nov 2025"),
    ("TAO-USDT", "2025-12-01", 1.0, 20.0, "miners", "TAO Dec 2025"),
    ("TAO-USDT", "2026-01-01", 1.0, 20.0, "miners", "TAO Jan 2026"),
    ("TAO-USDT", "2026-02-01", 1.0, 20.0, "miners", "TAO Feb 2026"),
    ("TAO-USDT", "2026-03-01", 1.0, 20.0, "miners", "TAO Mar 2026"),
    ("TAO-USDT", "2026-04-01", 1.0, 20.0, "miners", "TAO Apr 2026"),
]


def get_daily_ohlcv(db, pair: str) -> pd.DataFrame:
    """Get 1h candles and resample to daily."""
    coll = db['hyperliquid_candles_1h']
    cursor = coll.find(
        {'pair': pair},
        {'timestamp_utc': 1, 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1}
    ).sort('timestamp_utc', 1)
    
    rows = list(cursor)
    if not rows:
        return pd.DataFrame()
    
    df = pd.DataFrame(rows)
    df['timestamp'] = pd.to_datetime(df['timestamp_utc'], unit='ms', utc=True)
    df = df.set_index('timestamp')
    
    # Resample to daily
    daily = df.resample('1D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    return daily


def compute_event_returns(daily: pd.DataFrame, event_date: str) -> dict:
    """Compute returns around an unlock event.
    
    Windows (in trading days):
    - T-7 to T-2: pre-event drift (anticipation)
    - T-2 to T+0: immediate pre-event 
    - T+0 to T+4: post-event impact
    - T+4 to T+14: recovery/continuation
    - T-7 to T+14: full window
    """
    ed = pd.Timestamp(event_date, tz='UTC')
    
    # Find nearest trading day
    if ed not in daily.index:
        mask = daily.index >= ed
        if not mask.any():
            return None
        ed = daily.index[mask][0]
    
    idx = daily.index.get_loc(ed)
    
    results = {}
    
    # T-7 to T-2
    if idx >= 7:
        p_t7 = daily.iloc[idx - 7]['close']
        p_t2 = daily.iloc[idx - 2]['close']
        results['pre_drift_T-7_T-2'] = (p_t2 / p_t7 - 1) * 100
    
    # T-2 to T+0
    if idx >= 2:
        p_t2 = daily.iloc[idx - 2]['close']
        p_t0 = daily.iloc[idx]['close']
        results['immediate_T-2_T0'] = (p_t0 / p_t2 - 1) * 100
    
    # T+0 to T+4
    if idx + 4 < len(daily):
        p_t0 = daily.iloc[idx]['close']
        p_t4 = daily.iloc[idx + 4]['close']
        results['post_T0_T+4'] = (p_t4 / p_t0 - 1) * 100
    
    # T+4 to T+14
    if idx + 14 < len(daily):
        p_t4 = daily.iloc[idx + 4]['close']
        p_t14 = daily.iloc[idx + 14]['close']
        results['recovery_T+4_T+14'] = (p_t14 / p_t4 - 1) * 100
    
    # T-7 to T+14 full window
    if idx >= 7 and idx + 14 < len(daily):
        p_t7 = daily.iloc[idx - 7]['close']
        p_t14 = daily.iloc[idx + 14]['close']
        results['full_T-7_T+14'] = (p_t14 / p_t7 - 1) * 100
    
    # Also compute BTC return for same window (market-adjusted)
    return results


def main():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['quants_lab']
    
    # Get BTC daily for market-adjusting returns
    btc_daily = get_daily_ohlcv(db, 'BTC-USDT')
    print(f"BTC daily candles: {len(btc_daily)} days ({btc_daily.index[0].date()} to {btc_daily.index[-1].date()})")
    
    # Cache daily data per pair
    pair_cache = {'BTC-USDT': btc_daily}
    
    results = []
    
    for pair, date_str, pct_supply, usd_val, recipient, desc in UNLOCK_EVENTS:
        if pair not in pair_cache:
            pair_cache[pair] = get_daily_ohlcv(db, pair)
        
        daily = pair_cache[pair]
        if daily.empty:
            print(f"  SKIP {desc}: no candle data for {pair}")
            continue
        
        event_returns = compute_event_returns(daily, date_str)
        if event_returns is None:
            print(f"  SKIP {desc}: event date {date_str} outside data range")
            continue
        
        # Compute BTC returns for same windows for market-adjustment
        btc_returns = compute_event_returns(btc_daily, date_str)
        
        row = {
            'pair': pair,
            'date': date_str,
            'pct_supply': pct_supply,
            'usd_M': usd_val,
            'recipient': recipient,
            'desc': desc,
        }
        row.update(event_returns)
        
        # Add market-adjusted returns
        if btc_returns:
            for key in event_returns:
                btc_key = f"btc_{key}"
                adj_key = f"adj_{key}"
                if key in btc_returns:
                    row[btc_key] = btc_returns[key]
                    row[adj_key] = event_returns[key] - btc_returns[key]
        
        results.append(row)
    
    if not results:
        print("No events could be analyzed!")
        return
    
    df = pd.DataFrame(results)
    
    print(f"\n{'='*80}")
    print(f"TOKEN UNLOCK EVENT STUDY - {len(df)} events analyzed")
    print(f"{'='*80}")
    
    # Summary stats by window
    windows = [
        ('pre_drift_T-7_T-2', 'Pre-drift (T-7 to T-2)'),
        ('immediate_T-2_T0', 'Immediate (T-2 to T+0)'),
        ('post_T0_T+4', 'Post-event (T+0 to T+4)'),
        ('recovery_T+4_T+14', 'Recovery (T+4 to T+14)'),
        ('full_T-7_T+14', 'Full window (T-7 to T+14)'),
    ]
    
    print(f"\n--- RAW RETURNS (%) ---")
    print(f"{'Window':<30} {'Mean':>8} {'Median':>8} {'Std':>8} {'Neg%':>6} {'N':>4}")
    print("-" * 70)
    for col, label in windows:
        if col in df.columns:
            vals = df[col].dropna()
            neg_pct = (vals < 0).mean() * 100
            print(f"{label:<30} {vals.mean():>8.2f} {vals.median():>8.2f} {vals.std():>8.2f} {neg_pct:>5.0f}% {len(vals):>4}")
    
    # Market-adjusted returns
    print(f"\n--- MARKET-ADJUSTED RETURNS (vs BTC, %) ---")
    print(f"{'Window':<30} {'Mean':>8} {'Median':>8} {'Std':>8} {'Neg%':>6} {'N':>4}")
    print("-" * 70)
    for col, label in windows:
        adj_col = f"adj_{col}"
        if adj_col in df.columns:
            vals = df[adj_col].dropna()
            neg_pct = (vals < 0).mean() * 100
            print(f"{label:<30} {vals.mean():>8.2f} {vals.median():>8.2f} {vals.std():>8.2f} {neg_pct:>5.0f}% {len(vals):>4}")
    
    # Filter for large unlocks (>1% supply)
    large = df[df['pct_supply'] > 1.0]
    print(f"\n--- LARGE UNLOCKS ONLY (>1% supply) - {len(large)} events ---")
    print(f"{'Window':<30} {'Mean':>8} {'Median':>8} {'Neg%':>6} {'N':>4}")
    print("-" * 60)
    for col, label in windows:
        adj_col = f"adj_{col}"
        if adj_col in large.columns:
            vals = large[adj_col].dropna()
            if len(vals) > 0:
                neg_pct = (vals < 0).mean() * 100
                print(f"{label:<30} {vals.mean():>8.2f} {vals.median():>8.2f} {neg_pct:>5.0f}% {len(vals):>4}")
    
    # By recipient type
    print(f"\n--- MARKET-ADJUSTED POST-EVENT (T+0 to T+4) BY RECIPIENT ---")
    adj_post = 'adj_post_T0_T+4'
    if adj_post in df.columns:
        for rec in df['recipient'].unique():
            subset = df[df['recipient'] == rec][adj_post].dropna()
            if len(subset) > 0:
                neg_pct = (subset < 0).mean() * 100
                print(f"  {rec:<25} mean={subset.mean():>7.2f}%  median={subset.median():>7.2f}%  neg={neg_pct:.0f}%  n={len(subset)}")
    
    # Print individual events sorted by adj post-event return
    print(f"\n--- INDIVIDUAL EVENTS (sorted by adj post-event return) ---")
    if adj_post in df.columns:
        sorted_df = df.dropna(subset=[adj_post]).sort_values(adj_post)
        for _, row in sorted_df.iterrows():
            pre = row.get('adj_pre_drift_T-7_T-2', float('nan'))
            imm = row.get('adj_immediate_T-2_T0', float('nan'))
            post = row.get(adj_post, float('nan'))
            print(f"  {row['desc']:<35} supply={row['pct_supply']:>5.1f}%  pre={pre:>7.2f}%  imm={imm:>7.2f}%  post={post:>7.2f}%")
    
    # Statistical significance
    print(f"\n--- STATISTICAL TESTS ---")
    from scipy import stats
    for col, label in windows:
        adj_col = f"adj_{col}"
        if adj_col in df.columns:
            vals = df[adj_col].dropna()
            if len(vals) >= 5:
                t_stat, p_val = stats.ttest_1samp(vals, 0)
                sig = "***" if p_val < 0.01 else "**" if p_val < 0.05 else "*" if p_val < 0.1 else ""
                print(f"  {label:<30} t={t_stat:>6.2f}  p={p_val:.4f} {sig}")


if __name__ == '__main__':
    main()
