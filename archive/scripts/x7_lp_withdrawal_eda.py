"""
X7 — LP Withdrawal Rate as Directional Signal for Crypto Perps
===============================================================
Thesis: Informed LPs pull liquidity before big moves. When LP withdrawals spike,
it predicts volatility. Direction can be inferred from whether burns are
concentrated above or below current tick (bid-side vs ask-side).

Data source: Uniswap V3 pool-level Mint/Burn events via Dune Analytics.
Pools:
  - 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640 (USDC/WETH 0.05% — highest volume)
  - 0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8 (USDC/WETH 0.30%)
  - 0x4585fe77225b41b697c938b018e2ac67ac5a20c0 (WBTC/WETH 0.05%)
  - 0xcbcdf9626bc03e24f779434178a73a0b4bad62ed (WBTC/USDC 0.30%)

Note: In USDC/WETH pools, token0=USDC, token1=WETH. Ticks increase = WETH price
decreases (more USDC per WETH). A burn below current tick removes bid-side liquidity
(LP withdrawing WETH). A burn above current tick removes ask-side liquidity (LP
withdrawing USDC).
"""

import os
import sys
import json
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Paths
CACHE_DIR = Path("/Users/hermes/quants-lab/app/data/cache/onchain_x7_lp_withdrawal")
CANDLE_DIR = Path("/Users/hermes/quants-lab/app/data/cache/candles")

# Dune setup
from dune_client.client import DuneClient
DUNE_API_KEY = "4lS0uYbOdht78ZzWK0R63NEccj9Z9JLD"
dune = DuneClient(DUNE_API_KEY)

# Pool definitions
POOLS = {
    "ETH_005": {
        "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        "token": "ETH",
        "pair": "ETH-USDT",  # Map to Bybit perp
        "fee": "0.05%",
    },
    "ETH_030": {
        "address": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        "token": "ETH",
        "pair": "ETH-USDT",
        "fee": "0.30%",
    },
    "BTC_005": {
        "address": "0x4585fe77225b41b697c938b018e2ac67ac5a20c0",
        "token": "BTC",
        "pair": "BTC-USDT",
        "fee": "0.05%",
    },
    "BTC_030": {
        "address": "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
        "token": "BTC",
        "pair": "BTC-USDT",
        "fee": "0.30%",
    },
}


def _run_sql_with_retry(query: str, max_retries: int = 3, base_delay: float = 30.0):
    """Execute Dune SQL with exponential backoff on rate limits."""
    for attempt in range(max_retries):
        try:
            return dune.run_sql(query)
        except Exception as e:
            if '429' in str(e) or 'rate' in str(e).lower() or 'too many' in str(e).lower():
                delay = base_delay * (2 ** attempt)
                print(f"    Rate limited (attempt {attempt+1}/{max_retries}), sleeping {delay}s...")
                time.sleep(delay)
            else:
                raise
    raise RuntimeError(f"Failed after {max_retries} retries")


def fetch_lp_events(pool_address: str, event_type: str, start_date: str) -> pd.DataFrame:
    """Fetch daily aggregated mint or burn events from Dune."""
    table = f"uniswap_v3_ethereum.uniswapv3pool_evt_{event_type}"

    # We aggregate daily: count of events, total liquidity amount, and sum of amount0/amount1
    # Also get tick positioning relative to pool
    query = f"""
    SELECT
        evt_block_date as dt,
        count(*) as n_events,
        sum(cast(amount as double)) as total_liquidity,
        sum(cast(amount0 as double)) as total_amount0,
        sum(cast(amount1 as double)) as total_amount1,
        -- Tick analysis: avg tick position of LP events
        avg(cast(tickLower as double)) as avg_tick_lower,
        avg(cast(tickUpper as double)) as avg_tick_upper,
        -- Large events (liquidity > 1e18 for ETH pools)
        sum(case when cast(amount as double) > 1e18 then 1 else 0 end) as n_large_events,
        sum(case when cast(amount as double) > 1e18 then cast(amount as double) else 0 end) as large_event_liquidity
    FROM {table}
    WHERE contract_address = {pool_address}
    AND evt_block_date >= date '{start_date}'
    GROUP BY 1
    ORDER BY 1
    """
    print(f"  Querying {event_type} events for {pool_address[:10]}...")
    result = _run_sql_with_retry(query)

    if not result.result.rows:
        print(f"  WARNING: No rows returned for {event_type} on {pool_address}")
        return pd.DataFrame()

    df = pd.DataFrame(result.result.rows)
    df['dt'] = pd.to_datetime(df['dt'])
    for col in df.columns:
        if col != 'dt':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.set_index('dt').sort_index()
    return df


def fetch_current_ticks(pool_addresses: list, start_date: str) -> pd.DataFrame:
    """Fetch daily average current tick from swap events (proxy for price)."""
    addr_list = ",".join(pool_addresses)
    query = f"""
    SELECT
        contract_address,
        evt_block_date as dt,
        -- sqrtPriceX96 from swap events gives current price
        avg(cast(tick as double)) as avg_tick
    FROM uniswap_v3_ethereum.uniswapv3pool_evt_swap
    WHERE contract_address IN ({addr_list})
    AND evt_block_date >= date '{start_date}'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    print("  Querying daily swap ticks...")
    result = _run_sql_with_retry(query)
    if not result.result.rows:
        return pd.DataFrame()
    df = pd.DataFrame(result.result.rows)
    df['dt'] = pd.to_datetime(df['dt'])
    df['avg_tick'] = pd.to_numeric(df['avg_tick'], errors='coerce')
    return df


def fetch_combined_lp_events(token: str, event_type: str, start_date: str) -> pd.DataFrame:
    """Fetch daily LP events combining both fee-tier pools for a token into one query."""
    table = f"uniswap_v3_ethereum.uniswapv3pool_evt_{event_type}"

    if token == "ETH":
        addresses = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640, 0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"
    else:
        addresses = "0x4585fe77225b41b697c938b018e2ac67ac5a20c0, 0xcbcdf9626bc03e24f779434178a73a0b4bad62ed"

    query = f"""
    SELECT
        evt_block_date as dt,
        count(*) as n_events,
        sum(cast(amount as double)) as total_liquidity,
        sum(cast(amount0 as double)) as total_amount0,
        sum(cast(amount1 as double)) as total_amount1,
        avg(cast(tickLower as double)) as avg_tick_lower,
        avg(cast(tickUpper as double)) as avg_tick_upper,
        sum(case when cast(amount as double) > 1e18 then 1 else 0 end) as n_large_events,
        sum(case when cast(amount as double) > 1e18 then cast(amount as double) else 0 end) as large_event_liquidity
    FROM {table}
    WHERE contract_address IN ({addresses})
    AND evt_block_date >= date '{start_date}'
    GROUP BY 1
    ORDER BY 1
    """
    print(f"  Querying {event_type} for {token} (both fee tiers combined)...")
    result = _run_sql_with_retry(query)

    if not result.result.rows:
        print(f"  WARNING: No rows returned")
        return pd.DataFrame()

    df = pd.DataFrame(result.result.rows)
    df['dt'] = pd.to_datetime(df['dt'])
    for col in df.columns:
        if col != 'dt':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.set_index('dt').sort_index()
    return df


def fetch_all_data():
    """Fetch and cache all LP event data from Dune. 4 queries total (2 tokens x 2 event types)."""
    start_date = "2025-04-20"  # 365 days back
    all_data = {}

    for token, pair in [("ETH", "ETH-USDT"), ("BTC", "BTC-USDT")]:
        cache_file = CACHE_DIR / f"{token}_combined_events.parquet"

        if cache_file.exists():
            print(f"\n  Loading {token} from cache: {cache_file}")
            all_data[token] = pd.read_parquet(cache_file)
            continue

        print(f"\n--- {token} (combined pools) ---")
        burns = fetch_combined_lp_events(token, 'burn', start_date)
        time.sleep(20)  # Generous delay for rate limits
        mints = fetch_combined_lp_events(token, 'mint', start_date)
        time.sleep(20)

        if burns.empty or mints.empty:
            print(f"  SKIP: insufficient data for {token}")
            continue

        # Combine into single DataFrame
        combined = pd.DataFrame(index=burns.index.union(mints.index))
        for col in burns.columns:
            combined[f'burn_{col}'] = burns[col]
        for col in mints.columns:
            combined[f'mint_{col}'] = mints[col]
        combined = combined.fillna(0)

        # Save to cache
        combined.to_parquet(cache_file)
        all_data[token] = combined
        print(f"  Saved {len(combined)} days to {cache_file}")

    # Map tokens to pairs for downstream
    token_to_pair = {"ETH": "ETH-USDT", "BTC": "BTC-USDT"}
    ticks = pd.DataFrame()  # Skip tick query to save API calls

    return all_data, ticks, token_to_pair


def compute_features(all_data: dict, token_to_pair: dict) -> dict:
    """Compute LP withdrawal features for each token."""
    features_by_token = {}

    for token, df in all_data.items():
        pair = token_to_pair[token]
        print(f"\n--- Computing features for {token} ({pair}) ---")

        # Core feature: LP withdrawal rate = burns / (burns + mints)
        df['withdrawal_rate'] = df['burn_n_events'] / (df['burn_n_events'] + df['mint_n_events'] + 1e-10)

        # Liquidity-weighted withdrawal rate
        df['liq_withdrawal_rate'] = df['burn_total_liquidity'] / (
            df['burn_total_liquidity'] + df['mint_total_liquidity'] + 1e-10
        )

        # Z-scores of withdrawal rate
        df['withdrawal_rate_z24'] = (
            df['withdrawal_rate'] - df['withdrawal_rate'].rolling(24).mean()
        ) / (df['withdrawal_rate'].rolling(24).std() + 1e-10)

        df['withdrawal_rate_z72'] = (
            df['withdrawal_rate'] - df['withdrawal_rate'].rolling(72).mean()
        ) / (df['withdrawal_rate'].rolling(72).std() + 1e-10)

        # TVL proxy: net liquidity change (mints - burns)
        df['net_liquidity'] = df['mint_total_liquidity'] - df['burn_total_liquidity']
        df['net_liquidity_cumsum'] = df['net_liquidity'].cumsum()
        df['tvl_change_z7'] = (
            df['net_liquidity'].rolling(7).sum() - df['net_liquidity'].rolling(30).mean() * 7
        ) / (df['net_liquidity'].rolling(30).std() * np.sqrt(7) + 1e-10)

        # Large LP exit flag (large burn events)
        df['large_lp_exit'] = (df['burn_n_large_events'] > 0).astype(int)
        df['large_lp_exit_24h'] = df['large_lp_exit'].rolling(1).max()  # daily data, so 1 = 24h

        # Tick positioning: are burns above or below average mint position?
        # If burn ticks are LOWER than mint ticks, LPs are pulling bid-side = bearish
        # If burn ticks are HIGHER than mint ticks, LPs are pulling ask-side = bullish
        df['tick_bias'] = df['burn_avg_tick_lower'] - df['mint_avg_tick_lower']
        df['tick_bias_z'] = (
            df['tick_bias'] - df['tick_bias'].rolling(30).mean()
        ) / (df['tick_bias'].rolling(30).std() + 1e-10)

        # Volume asymmetry: burn amount0 vs amount1 relative to mints
        # In USDC/WETH: amount0 = USDC, amount1 = WETH
        # More WETH being burned than minted = bearish (LPs pulling WETH from bid side)
        if token == "ETH":
            df['weth_burn_ratio'] = df['burn_total_amount1'] / (df['mint_total_amount1'] + 1e-10)
        else:
            # BTC pools: amount varies by pool design
            df['weth_burn_ratio'] = df['burn_total_amount0'] / (df['mint_total_amount0'] + 1e-10)

        features_by_token[pair] = [(token, df)]

    return features_by_token


def load_bybit_candles(pair: str) -> pd.DataFrame:
    """Load Bybit perp candle data from parquet cache or MongoDB."""
    # Try parquet first
    pattern = f"bybit_perpetual-{pair.replace('-', '_')}_USDT-1d-*.parquet"
    # Actually look for 1h candles and resample to daily
    import glob

    # Search for any matching candle files
    search_patterns = [
        f"bybit_perpetual*{pair.replace('-', '_')}*1h*.parquet",
        f"bybit_perpetual*{pair.split('-')[0]}*USDT*1h*.parquet",
        f"bybit_perpetual*{pair.split('-')[0]}*1h*.parquet",
    ]

    candle_files = []
    for pat in search_patterns:
        found = list(CANDLE_DIR.glob(pat))
        candle_files.extend(found)

    if not candle_files:
        # Try MongoDB
        print(f"  No parquet for {pair}, trying MongoDB...")
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/quants_lab")
        db = client['quants_lab']

        # Check if we have candle data in features
        # Actually let's just use the Dune swap data as price proxy, or fetch from coingecko
        # For simplicity, let's query Bybit REST API for daily klines
        return fetch_bybit_daily_klines(pair)

    # Load and resample
    dfs = []
    for f in candle_files:
        dfs.append(pd.read_parquet(f))
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs).sort_index()
    df = df[~df.index.duplicated(keep='last')]

    # Resample to daily
    daily = df.resample('1D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }).dropna()

    return daily


def fetch_bybit_daily_klines(pair: str, days: int = 400) -> pd.DataFrame:
    """Fetch daily klines from Bybit REST API."""
    import requests

    symbol = pair.replace("-", "")
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    all_klines = []
    current_start = start_time

    while current_start < end_time:
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": "D",
            "start": current_start,
            "end": min(current_start + 200 * 86400000, end_time),
            "limit": 200,
        }
        resp = requests.get(url, params=params)
        data = resp.json()

        if data['retCode'] != 0:
            print(f"  Bybit API error for {pair}: {data['retMsg']}")
            break

        klines = data['result']['list']
        if not klines:
            break

        for k in klines:
            all_klines.append({
                'timestamp': int(k[0]),
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
            })

        # Move forward (Bybit returns newest first)
        timestamps = [int(k[0]) for k in klines]
        current_start = max(timestamps) + 86400000
        time.sleep(0.1)

    if not all_klines:
        return pd.DataFrame()

    df = pd.DataFrame(all_klines)
    df['dt'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.set_index('dt').sort_index()
    df = df[~df.index.duplicated(keep='last')]
    return df[['open', 'high', 'low', 'close', 'volume']]


def merge_and_analyze(features_by_token: dict):
    """Merge LP features with price returns and test predictive power."""
    results = {}

    for pair, pool_features in features_by_token.items():
        print(f"\n{'='*60}")
        print(f"ANALYSIS: {pair}")
        print(f"{'='*60}")

        # Load price data
        candle_cache = CACHE_DIR / f"{pair}_candles.parquet"
        if candle_cache.exists():
            candles = pd.read_parquet(candle_cache)
        else:
            candles = fetch_bybit_daily_klines(pair)
            if not candles.empty:
                candles.to_parquet(candle_cache)

        if candles.empty:
            print(f"  SKIP: no candle data for {pair}")
            continue

        # Compute forward returns
        candles['ret_1d'] = candles['close'].pct_change(1).shift(-1)  # Next day return
        candles['ret_2d'] = candles['close'].pct_change(2).shift(-2)
        candles['ret_3d'] = candles['close'].pct_change(3).shift(-3)
        candles['abs_ret_1d'] = candles['ret_1d'].abs()
        candles['abs_ret_2d'] = candles['ret_2d'].abs()

        # Combine features from all pools for this token
        # Use the highest-volume pool as primary
        primary_pool_name = pool_features[0][0]
        primary_df = pool_features[0][1].copy()

        # If multiple pools, average the features
        if len(pool_features) > 1:
            secondary_df = pool_features[1][1]
            for col in ['withdrawal_rate_z24', 'withdrawal_rate_z72', 'tvl_change_z7',
                       'large_lp_exit', 'tick_bias_z', 'weth_burn_ratio']:
                if col in secondary_df.columns:
                    primary_df[f'{col}_avg'] = (primary_df[col] + secondary_df[col].reindex(primary_df.index, method='ffill').fillna(0)) / 2

        # Merge with candles on date
        merged = candles.join(primary_df, how='inner')
        print(f"  Merged rows: {len(merged)} (from {candles.index.min().date()} to {candles.index.max().date()})")

        if len(merged) < 60:
            print(f"  SKIP: insufficient merged data ({len(merged)} < 60)")
            continue

        # --- SIGNAL ANALYSIS ---
        feature_cols = [
            'withdrawal_rate_z24', 'withdrawal_rate_z72', 'tvl_change_z7',
            'large_lp_exit', 'tick_bias_z', 'weth_burn_ratio', 'liq_withdrawal_rate',
        ]
        available_features = [c for c in feature_cols if c in merged.columns]

        print(f"\n  Available features: {available_features}")
        print(f"  Date range: {merged.index.min().date()} to {merged.index.max().date()}")

        # 1. Correlation analysis
        print(f"\n  --- Correlations with forward returns ---")
        print(f"  {'Feature':<25} {'ret_1d':>8} {'ret_2d':>8} {'ret_3d':>8} {'|ret_1d|':>8} {'|ret_2d|':>8}")
        print(f"  {'-'*75}")

        pair_results = {'correlations': {}, 'quintile_analysis': {}}

        for feat in available_features:
            valid = merged[[feat, 'ret_1d', 'ret_2d', 'ret_3d', 'abs_ret_1d', 'abs_ret_2d']].dropna()
            if len(valid) < 30:
                continue

            corr_1d = valid[feat].corr(valid['ret_1d'])
            corr_2d = valid[feat].corr(valid['ret_2d'])
            corr_3d = valid[feat].corr(valid['ret_3d'])
            corr_abs1 = valid[feat].corr(valid['abs_ret_1d'])
            corr_abs2 = valid[feat].corr(valid['abs_ret_2d'])

            print(f"  {feat:<25} {corr_1d:>8.4f} {corr_2d:>8.4f} {corr_3d:>8.4f} {corr_abs1:>8.4f} {corr_abs2:>8.4f}")

            pair_results['correlations'][feat] = {
                'ret_1d': corr_1d, 'ret_2d': corr_2d, 'ret_3d': corr_3d,
                'abs_ret_1d': corr_abs1, 'abs_ret_2d': corr_abs2,
            }

        # 2. Quintile analysis for strongest features
        print(f"\n  --- Quintile analysis (mean forward return by feature quintile) ---")

        for feat in available_features:
            valid = merged[[feat, 'ret_1d', 'ret_2d', 'abs_ret_1d']].dropna()
            if len(valid) < 50:
                continue

            try:
                valid['quintile'] = pd.qcut(valid[feat], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
            except ValueError:
                continue

            quintile_stats = valid.groupby('quintile').agg(
                mean_ret_1d=('ret_1d', 'mean'),
                mean_ret_2d=('ret_2d', 'mean'),
                mean_abs_ret=('abs_ret_1d', 'mean'),
                count=('ret_1d', 'count'),
            )

            # Only print if there's a meaningful spread
            spread = quintile_stats['mean_ret_1d'].iloc[-1] - quintile_stats['mean_ret_1d'].iloc[0]
            vol_spread = quintile_stats['mean_abs_ret'].iloc[-1] - quintile_stats['mean_abs_ret'].iloc[0]

            if abs(spread) > 0.001 or abs(vol_spread) > 0.002:
                print(f"\n  Feature: {feat} (Q5-Q1 direction spread: {spread*100:.2f}%, vol spread: {vol_spread*100:.2f}%)")
                print(f"  {'Q':>4} {'ret_1d%':>8} {'ret_2d%':>8} {'|ret|%':>8} {'N':>5}")
                for idx, row in quintile_stats.iterrows():
                    print(f"  {idx:>4} {row['mean_ret_1d']*100:>8.3f} {row['mean_ret_2d']*100:>8.3f} {row['mean_abs_ret']*100:>8.3f} {int(row['count']):>5}")

                pair_results['quintile_analysis'][feat] = {
                    'direction_spread_pct': spread * 100,
                    'vol_spread_pct': vol_spread * 100,
                }

        # 3. Large LP exit event study
        print(f"\n  --- Event study: Large LP exits ---")
        exit_days = merged[merged['large_lp_exit'] == 1]
        normal_days = merged[merged['large_lp_exit'] == 0]

        if len(exit_days) > 5:
            exit_abs_ret = exit_days['abs_ret_1d'].mean()
            normal_abs_ret = normal_days['abs_ret_1d'].mean()
            exit_ret = exit_days['ret_1d'].mean()
            normal_ret = normal_days['ret_1d'].mean()

            print(f"  Large LP exit days: {len(exit_days)}")
            print(f"  Mean |return| on exit days:   {exit_abs_ret*100:.3f}%")
            print(f"  Mean |return| on normal days: {normal_abs_ret*100:.3f}%")
            print(f"  Volatility ratio: {exit_abs_ret/normal_abs_ret:.2f}x")
            print(f"  Mean direction on exit days:  {exit_ret*100:.3f}%")
            print(f"  Mean direction on normal days: {normal_ret*100:.3f}%")

            pair_results['event_study'] = {
                'n_exit_days': len(exit_days),
                'vol_ratio': exit_abs_ret / normal_abs_ret if normal_abs_ret > 0 else 0,
                'exit_day_abs_ret': exit_abs_ret,
                'normal_day_abs_ret': normal_abs_ret,
            }

        # 4. High withdrawal rate regime
        print(f"\n  --- Regime analysis: High withdrawal rate (z>1.5) ---")
        high_wr = merged[merged['withdrawal_rate_z24'] > 1.5] if 'withdrawal_rate_z24' in merged.columns else pd.DataFrame()
        if len(high_wr) > 10:
            print(f"  Days with z24 > 1.5: {len(high_wr)} ({len(high_wr)/len(merged)*100:.1f}%)")
            print(f"  Mean |ret_1d| in regime: {high_wr['abs_ret_1d'].mean()*100:.3f}%")
            print(f"  Mean |ret_1d| outside:   {merged[merged['withdrawal_rate_z24'] <= 1.5]['abs_ret_1d'].mean()*100:.3f}%")
            print(f"  Mean ret_1d in regime:   {high_wr['ret_1d'].mean()*100:.3f}%")
            print(f"  Mean ret_2d in regime:   {high_wr['ret_2d'].mean()*100:.3f}%")

        # 5. Tick bias directional signal
        print(f"\n  --- Tick bias as directional signal ---")
        if 'tick_bias_z' in merged.columns:
            # High tick_bias_z = burns above mints = ask-side removal = bullish
            # Low tick_bias_z = burns below mints = bid-side removal = bearish
            high_bias = merged[merged['tick_bias_z'] > 1.0]
            low_bias = merged[merged['tick_bias_z'] < -1.0]
            if len(high_bias) > 10 and len(low_bias) > 10:
                print(f"  High tick bias (z>1, ask removal, bullish signal): N={len(high_bias)}")
                print(f"    Mean ret_1d: {high_bias['ret_1d'].mean()*100:.3f}%")
                print(f"    Mean ret_2d: {high_bias['ret_2d'].mean()*100:.3f}%")
                print(f"  Low tick bias (z<-1, bid removal, bearish signal): N={len(low_bias)}")
                print(f"    Mean ret_1d: {low_bias['ret_1d'].mean()*100:.3f}%")
                print(f"    Mean ret_2d: {low_bias['ret_2d'].mean()*100:.3f}%")
                print(f"  Spread (high-low): {(high_bias['ret_1d'].mean() - low_bias['ret_1d'].mean())*100:.3f}%")

        results[pair] = pair_results

    return results


def walk_forward_test(features_by_token: dict):
    """Simple walk-forward: 60d train / 30d gap / 30d test, rolling."""
    print(f"\n{'='*60}")
    print("WALK-FORWARD TEST (60d/30d/30d)")
    print(f"{'='*60}")

    for pair, pool_features in features_by_token.items():
        # Load candles
        candle_cache = CACHE_DIR / f"{pair}_candles.parquet"
        if not candle_cache.exists():
            continue
        candles = pd.read_parquet(candle_cache)
        candles['ret_1d'] = candles['close'].pct_change(1).shift(-1)
        candles['abs_ret_1d'] = candles['ret_1d'].abs()

        primary_df = pool_features[0][1].copy()
        merged = candles.join(primary_df, how='inner').dropna(subset=['ret_1d', 'withdrawal_rate_z24'])

        if len(merged) < 150:
            print(f"\n  {pair}: insufficient data for walk-forward ({len(merged)} < 150 days)")
            continue

        print(f"\n  --- {pair} Walk-Forward ---")
        print(f"  Total days: {len(merged)}")

        # Simple strategy: go SHORT when withdrawal_rate_z24 > 1.5 (LPs fleeing = vol coming)
        # combined with tick_bias for direction
        train_days = 60
        gap_days = 30
        test_days = 30

        fold_results = []
        i = 0
        fold = 0

        while i + train_days + gap_days + test_days <= len(merged):
            train = merged.iloc[i:i+train_days]
            test = merged.iloc[i+train_days+gap_days:i+train_days+gap_days+test_days]

            # In-sample: find optimal z-threshold for vol prediction
            # Strategy: position = -sign(tick_bias_z) when withdrawal_rate_z24 > threshold
            # Simple threshold scan
            best_sharpe = -999
            best_thresh = 1.0

            for thresh in [0.5, 1.0, 1.5, 2.0]:
                signal_days = train[train['withdrawal_rate_z24'] > thresh]
                if len(signal_days) < 5:
                    continue
                if 'tick_bias_z' in signal_days.columns:
                    positions = -np.sign(signal_days['tick_bias_z'])
                    returns = positions * signal_days['ret_1d']
                    if returns.std() > 0:
                        sharpe = returns.mean() / returns.std() * np.sqrt(252)
                        if sharpe > best_sharpe:
                            best_sharpe = sharpe
                            best_thresh = thresh

            # Out-of-sample test
            test_signals = test[test['withdrawal_rate_z24'] > best_thresh]
            if len(test_signals) < 3:
                i += test_days
                fold += 1
                continue

            if 'tick_bias_z' in test_signals.columns:
                positions = -np.sign(test_signals['tick_bias_z'])
                oos_returns = positions * test_signals['ret_1d']
                oos_sharpe = oos_returns.mean() / (oos_returns.std() + 1e-10) * np.sqrt(252)
                oos_mean = oos_returns.mean() * 100
                n_trades = len(test_signals)
                win_rate = (oos_returns > 0).mean()

                fold_results.append({
                    'fold': fold,
                    'train_start': train.index[0].date(),
                    'test_start': test.index[0].date(),
                    'threshold': best_thresh,
                    'is_sharpe': best_sharpe,
                    'oos_sharpe': oos_sharpe,
                    'oos_mean_ret': oos_mean,
                    'n_trades': n_trades,
                    'win_rate': win_rate,
                })

            i += test_days
            fold += 1

        if fold_results:
            fr_df = pd.DataFrame(fold_results)
            print(f"  Folds completed: {len(fr_df)}")
            print(f"  Mean OOS Sharpe: {fr_df['oos_sharpe'].mean():.3f}")
            print(f"  Mean OOS daily ret: {fr_df['oos_mean_ret'].mean():.4f}%")
            print(f"  Mean win rate: {fr_df['win_rate'].mean()*100:.1f}%")
            print(f"  Mean N trades/fold: {fr_df['n_trades'].mean():.1f}")
            print(f"  IS/OOS Sharpe ratio: {fr_df['is_sharpe'].mean():.2f} / {fr_df['oos_sharpe'].mean():.2f}")
            print(f"\n  Per-fold breakdown:")
            print(f"  {'Fold':>4} {'Test Start':>12} {'OOS Sharpe':>11} {'OOS Ret%':>9} {'WR%':>5} {'N':>3}")
            for _, row in fr_df.iterrows():
                print(f"  {int(row['fold']):>4} {str(row['test_start']):>12} {row['oos_sharpe']:>11.3f} {row['oos_mean_ret']:>9.4f} {row['win_rate']*100:>5.1f} {int(row['n_trades']):>3}")


def main():
    print("=" * 60)
    print("X7 — LP WITHDRAWAL RATE SIGNAL ANALYSIS")
    print("=" * 60)
    print(f"Run time: {datetime.now().isoformat()}")

    # Step 1: Fetch data
    print("\n\n[1/4] FETCHING LP EVENT DATA FROM DUNE...")
    all_data, ticks, token_to_pair = fetch_all_data()

    if not all_data:
        print("FATAL: No data retrieved from Dune. Aborting.")
        sys.exit(1)

    # Step 2: Compute features
    print("\n\n[2/4] COMPUTING FEATURES...")
    features_by_token = compute_features(all_data, token_to_pair)

    # Step 3: Merge and analyze
    print("\n\n[3/4] SIGNAL ANALYSIS...")
    results = merge_and_analyze(features_by_token)

    # Step 4: Walk-forward
    print("\n\n[4/4] WALK-FORWARD VALIDATION...")
    walk_forward_test(features_by_token)

    # Summary
    print("\n\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("""
    Key features tested:
    1. withdrawal_rate_z24/z72 — z-score of LP burn/mint ratio (vol predictor)
    2. tvl_change_z7 — 7-day TVL change rate (trend signal)
    3. large_lp_exit — binary flag for large LP removals (event signal)
    4. tick_bias_z — burn tick position vs mint tick position (directional)
    5. weth_burn_ratio — ratio of token burns to mints (flow imbalance)

    Interpretation:
    - High withdrawal_rate_z → LPs fleeing → volatility expansion expected
    - Tick bias z > 0 → burns above mints → ask-side removal → bullish
    - Tick bias z < 0 → burns below mints → bid-side removal → bearish
    - Combined: trade direction of tick_bias when withdrawal_rate elevated
    """)


if __name__ == "__main__":
    main()
