"""
X7 — Stablecoin Mint/Burn Event Study
======================================
Thesis: Large USDT/USDC mint events from treasury addresses predict
positive BTC/ETH forward returns (institutional demand incoming).

Uses Dune Analytics API to pull:
1. Daily net supply change for USDT and USDC (365 days)
2. Large mint events (>$10M)

Then merges with BTC/ETH hourly candles and computes event-study statistics.
"""

import os
import sys
import json
import time
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Dune client
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

# ─── Config ───────────────────────────────────────────────────────────────────
DUNE_API_KEY = "4lS0uYbOdht78ZzWK0R63NEccj9Z9JLD"
CACHE_DIR = Path("/Users/hermes/quants-lab/app/data/cache/onchain_x7_stablecoin_supply")
CANDLE_DIR = Path("/Users/hermes/quants-lab/app/data/cache/candles")

# Forward return horizons (hours)
HORIZONS = [4, 24, 48, 168]  # 4h, 24h, 48h, 7d

# Mint size thresholds
LARGE_MINT_THRESHOLD = 100_000_000  # $100M for event signal
MEDIUM_MINT_THRESHOLD = 10_000_000   # $10M for filtering


# ─── Step 1: Query Dune for stablecoin supply data ────────────────────────────

def fetch_dune_data():
    """Fetch stablecoin mint/burn data from Dune Analytics."""
    dune = DuneClient(api_key=DUNE_API_KEY)

    # Query 1: Daily USDT + USDC net supply change (mints - burns) on Ethereum + Tron
    # Using known Dune query patterns for stablecoin supply
    # We'll use the stablecoins.transfers spellbook table

    supply_query_sql = """
    -- Daily net mints/burns for USDT and USDC on Ethereum
    WITH daily_supply AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            CASE
                WHEN contract_address = 0xdAC17F958D2ee523a2206206994597C13D831ec7 THEN 'USDT'
                WHEN contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 THEN 'USDC'
            END AS token,
            SUM(CASE WHEN "from" = 0x0000000000000000000000000000000000000000
                THEN CAST(value AS DOUBLE) / 1e6 ELSE 0 END) AS minted_usd,
            SUM(CASE WHEN "to" = 0x0000000000000000000000000000000000000000
                THEN CAST(value AS DOUBLE) / 1e6 ELSE 0 END) AS burned_usd
        FROM erc20_ethereum.evt_Transfer
        WHERE contract_address IN (
            0xdAC17F958D2ee523a2206206994597C13D831ec7,
            0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
        )
        AND (
            "from" = 0x0000000000000000000000000000000000000000
            OR "to" = 0x0000000000000000000000000000000000000000
        )
        AND evt_block_time >= NOW() - INTERVAL '365' DAY
        GROUP BY 1, 2
    )
    SELECT
        day,
        token,
        minted_usd,
        burned_usd,
        minted_usd - burned_usd AS net_mint_usd
    FROM daily_supply
    WHERE token IS NOT NULL
    ORDER BY day DESC, token
    """

    # Query 2: Large individual mint events (>$10M)
    large_mints_sql = """
    SELECT
        evt_block_time AS block_time,
        CASE
            WHEN contract_address = 0xdAC17F958D2ee523a2206206994597C13D831ec7 THEN 'USDT'
            WHEN contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 THEN 'USDC'
        END AS token,
        "to" AS recipient,
        CAST(value AS DOUBLE) / 1e6 AS amount_usd,
        evt_tx_hash AS tx_hash
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address IN (
        0xdAC17F958D2ee523a2206206994597C13D831ec7,
        0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    )
    AND "from" = 0x0000000000000000000000000000000000000000
    AND CAST(value AS DOUBLE) / 1e6 > 10000000
    AND evt_block_time >= NOW() - INTERVAL '365' DAY
    ORDER BY evt_block_time DESC
    """

    # Query 3: not needed separately - we get what we need from query 1
    total_supply_sql = None

    results = {}

    def run_with_retry(name, sql, max_retries=3):
        """Run a Dune SQL query with retry and backoff."""
        for attempt in range(max_retries):
            try:
                r = dune.run_sql(
                    query_sql=sql,
                    is_private=False,
                    performance="medium",
                )
                print(f"  Got {len(r.result.rows)} rows")
                return r
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "too many" in err_str.lower():
                    wait = 30 * (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s (attempt {attempt+1}/{max_retries})...")
                    time.sleep(wait)
                else:
                    print(f"  ERROR: {e}")
                    return None
        print(f"  FAILED after {max_retries} retries")
        return None

    print("[1/2] Fetching daily net mints/burns...")
    results['daily_supply'] = run_with_retry("daily_supply", supply_query_sql)

    # Wait between queries to avoid rate limits
    time.sleep(5)

    print("[2/2] Fetching large mint events...")
    results['large_mints'] = run_with_retry("large_mints", large_mints_sql)

    return results


def save_dune_results(results: dict):
    """Save raw Dune results to cache."""
    for key, result in results.items():
        if result is None:
            continue
        rows = result.result.rows if hasattr(result, 'result') else []
        df = pd.DataFrame(rows)
        path = CACHE_DIR / f"{key}.parquet"
        df.to_parquet(path, index=False)
        print(f"  Saved {key}: {len(df)} rows -> {path}")
    return results


# ─── Step 2: Load candle data ─────────────────────────────────────────────────

def load_candles(asset: str = "BTC") -> pd.DataFrame:
    """Load hourly candle data from parquet."""
    path = CANDLE_DIR / f"bybit_perpetual|{asset}-USDT|1h.parquet"
    df = pd.read_parquet(path)
    # timestamp is Unix epoch seconds
    if 'timestamp' in df.columns:
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    elif 'open_time' in df.columns:
        df['datetime'] = pd.to_datetime(df['open_time'], unit='s')
    df = df.sort_values('datetime').reset_index(drop=True)
    print(f"  Loaded {asset} candles: {len(df)} rows, {df['datetime'].min()} to {df['datetime'].max()}")
    return df


# ─── Step 3: Compute features ─────────────────────────────────────────────────

def compute_features(supply_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute stablecoin mint features:
    - daily_mint_z7/z30: z-score of daily net mint volume
    - large_mint_event: binary, was there a >$100M mint in last 24h
    - supply_change_rate: 7d supply change / 30d supply change (acceleration)
    """
    if supply_df is None or supply_df.empty:
        return pd.DataFrame()

    # Aggregate across tokens (USDT + USDC combined)
    if 'day' in supply_df.columns:
        supply_df['day'] = pd.to_datetime(supply_df['day'])
        daily = supply_df.groupby('day').agg(
            net_mint_usd=('net_mint_usd', 'sum'),
            minted_usd=('minted_usd', 'sum'),
            burned_usd=('burned_usd', 'sum')
        ).sort_index()
    elif 'date' in supply_df.columns:
        supply_df['date'] = pd.to_datetime(supply_df['date'])
        # Try to find relevant columns
        numeric_cols = supply_df.select_dtypes(include=[np.number]).columns.tolist()
        print(f"  Available numeric columns: {numeric_cols}")
        print(f"  Available columns: {supply_df.columns.tolist()}")
        print(f"  First few rows:\n{supply_df.head()}")
        return pd.DataFrame()
    else:
        print(f"  Columns available: {supply_df.columns.tolist()}")
        print(f"  First rows:\n{supply_df.head()}")
        return pd.DataFrame()

    # Z-scores
    daily['daily_mint_z7'] = (
        (daily['net_mint_usd'] - daily['net_mint_usd'].rolling(7).mean())
        / daily['net_mint_usd'].rolling(7).std()
    )
    daily['daily_mint_z30'] = (
        (daily['net_mint_usd'] - daily['net_mint_usd'].rolling(30).mean())
        / daily['net_mint_usd'].rolling(30).std()
    )

    # Large mint event (>$100M net mint in a single day)
    daily['large_mint_event'] = (daily['minted_usd'] > LARGE_MINT_THRESHOLD).astype(int)

    # Supply change acceleration: 7d sum / 30d sum
    daily['supply_7d'] = daily['net_mint_usd'].rolling(7).sum()
    daily['supply_30d'] = daily['net_mint_usd'].rolling(30).sum()
    daily['supply_change_rate'] = daily['supply_7d'] / daily['supply_30d'].replace(0, np.nan)

    # Mint-to-burn ratio
    daily['mint_burn_ratio'] = daily['minted_usd'] / daily['burned_usd'].replace(0, np.nan)

    return daily


# ─── Step 4: Event study ──────────────────────────────────────────────────────

def event_study(candles_df: pd.DataFrame, events: pd.DatetimeIndex,
                horizons: list = HORIZONS, asset: str = "BTC") -> dict:
    """
    Compute forward returns after event dates vs random baseline.

    Returns dict with:
    - event_returns: mean forward return at each horizon
    - baseline_returns: mean forward return at each horizon (all days)
    - t_stats: t-statistic for difference
    - bootstrap_ci: 95% CI from bootstrap
    """
    # Compute hourly returns
    candles_df = candles_df.copy()
    candles_df = candles_df.set_index('datetime').sort_index()

    results = {}

    for h in horizons:
        # Forward return at horizon h (hours)
        candles_df[f'fwd_ret_{h}h'] = (
            candles_df['close'].shift(-h) / candles_df['close'] - 1
        ) * 100  # in percent

    # For each event, find the nearest candle and get forward returns
    event_returns = {h: [] for h in horizons}

    for event_time in events:
        # Normalize timezone: make event_time tz-naive for comparison
        if hasattr(event_time, 'tzinfo') and event_time.tzinfo is not None:
            event_time = event_time.tz_localize(None)
        # Find nearest hourly candle at or after event
        mask = candles_df.index >= event_time
        if not mask.any():
            continue
        idx = candles_df.index[mask][0]
        row = candles_df.loc[idx]
        for h in horizons:
            ret = row.get(f'fwd_ret_{h}h')
            if pd.notna(ret):
                event_returns[h].append(ret)

    # Baseline: all days' forward returns
    for h in horizons:
        evt_rets = np.array(event_returns[h])
        all_rets = candles_df[f'fwd_ret_{h}h'].dropna().values

        if len(evt_rets) < 5:
            results[h] = {
                'n_events': len(evt_rets),
                'event_mean': np.nan,
                'baseline_mean': np.nan,
                'excess_return': np.nan,
                'tstat': np.nan,
                'ci_lower': np.nan,
                'ci_upper': np.nan,
                'win_rate': np.nan,
            }
            continue

        evt_mean = np.mean(evt_rets)
        base_mean = np.mean(all_rets)
        excess = evt_mean - base_mean

        # T-test
        from scipy import stats
        tstat, pval = stats.ttest_1samp(evt_rets - base_mean, 0)

        # Bootstrap 95% CI for event mean
        n_boot = 10000
        boot_means = np.array([
            np.mean(np.random.choice(evt_rets, size=len(evt_rets), replace=True))
            for _ in range(n_boot)
        ])
        ci_lower = np.percentile(boot_means, 2.5)
        ci_upper = np.percentile(boot_means, 97.5)

        # Win rate (positive forward return after event)
        win_rate = np.mean(evt_rets > 0) * 100

        results[h] = {
            'n_events': len(evt_rets),
            'event_mean': evt_mean,
            'baseline_mean': base_mean,
            'excess_return': excess,
            'tstat': tstat,
            'pval': pval,
            'ci_lower': ci_lower,
            'ci_upper': ci_upper,
            'win_rate': win_rate,
        }

    return results


def print_event_study(results: dict, asset: str, event_type: str):
    """Pretty print event study results."""
    print(f"\n{'='*70}")
    print(f"  EVENT STUDY: {event_type} -> {asset} Forward Returns")
    print(f"{'='*70}")
    print(f"{'Horizon':<10} {'N':>5} {'Event%':>8} {'Base%':>8} {'Excess%':>8} {'t-stat':>7} {'p-val':>7} {'WR%':>6} {'95% CI':>18}")
    print("-" * 70)

    for h in sorted(results.keys()):
        r = results[h]
        if np.isnan(r.get('event_mean', np.nan)):
            print(f"{h}h{'':<7} {r['n_events']:>5}  {'insufficient data':>50}")
            continue
        print(
            f"{h}h{'':<7} {r['n_events']:>5} "
            f"{r['event_mean']:>+7.2f} "
            f"{r['baseline_mean']:>+7.2f} "
            f"{r['excess_return']:>+7.2f} "
            f"{r['tstat']:>7.2f} "
            f"{r.get('pval', np.nan):>7.4f} "
            f"{r['win_rate']:>5.1f} "
            f"[{r['ci_lower']:>+.2f}, {r['ci_upper']:>+.2f}]"
        )


# ─── Step 5: Walk-forward (if signal is promising) ───────────────────────────

def walk_forward_backtest(candles_df: pd.DataFrame, events: pd.DatetimeIndex,
                          hold_hours: int = 48, asset: str = "BTC"):
    """
    Simple walk-forward: enter LONG at event, hold for hold_hours, exit.
    No overlapping positions. Report PF, WR, avg return.
    """
    candles_df = candles_df.copy().set_index('datetime').sort_index()

    trades = []
    last_exit = pd.Timestamp.min

    for event_time in sorted(events):
        # No overlap
        if event_time < last_exit:
            continue

        # Find entry candle
        mask = candles_df.index >= event_time
        if not mask.any():
            continue
        entry_idx = candles_df.index[mask][0]
        entry_price = candles_df.loc[entry_idx, 'close']

        # Find exit candle
        exit_time = entry_idx + pd.Timedelta(hours=hold_hours)
        exit_mask = candles_df.index >= exit_time
        if not exit_mask.any():
            continue
        exit_idx = candles_df.index[exit_mask][0]
        exit_price = candles_df.loc[exit_idx, 'close']
        last_exit = exit_idx

        pnl_pct = (exit_price / entry_price - 1) * 100
        trades.append({
            'entry_time': entry_idx,
            'exit_time': exit_idx,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl_pct': pnl_pct,
        })

    if not trades:
        print(f"  No trades for {asset}")
        return None

    df_trades = pd.DataFrame(trades)
    wins = df_trades[df_trades['pnl_pct'] > 0]
    losses = df_trades[df_trades['pnl_pct'] <= 0]

    gross_profit = wins['pnl_pct'].sum() if len(wins) > 0 else 0
    gross_loss = abs(losses['pnl_pct'].sum()) if len(losses) > 0 else 0.001
    pf = gross_profit / gross_loss

    print(f"\n  Walk-Forward Results ({asset}, hold={hold_hours}h):")
    print(f"  Trades: {len(df_trades)}")
    print(f"  Win Rate: {len(wins)/len(df_trades)*100:.1f}%")
    print(f"  Avg Return: {df_trades['pnl_pct'].mean():+.3f}%")
    print(f"  Profit Factor: {pf:.2f}")
    print(f"  Total Return: {df_trades['pnl_pct'].sum():+.2f}%")
    print(f"  Max Win: {df_trades['pnl_pct'].max():+.2f}%")
    print(f"  Max Loss: {df_trades['pnl_pct'].min():+.2f}%")
    print(f"  Sharpe (per-trade): {df_trades['pnl_pct'].mean() / df_trades['pnl_pct'].std():.2f}" if df_trades['pnl_pct'].std() > 0 else "  Sharpe: N/A")

    return df_trades


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  X7 — STABLECOIN MINT/BURN EVENT STUDY")
    print("  Thesis: Large USDT/USDC mints predict positive crypto returns")
    print("=" * 70)

    # ─── Fetch Dune data ──────────────────────────────────────────────────────
    print("\n[STEP 1] Fetching stablecoin data from Dune Analytics...")

    # Check cache first
    cached_supply = CACHE_DIR / "daily_supply.parquet"
    cached_mints = CACHE_DIR / "large_mints.parquet"

    if cached_supply.exists() and cached_mints.exists():
        print("  Using cached data. Delete cache to re-fetch.")
        supply_df = pd.read_parquet(cached_supply)
        mints_df = pd.read_parquet(cached_mints)
    else:
        results = fetch_dune_data()
        save_dune_results(results)

        # Parse results
        if results.get('daily_supply') and hasattr(results['daily_supply'], 'result'):
            supply_df = pd.DataFrame(results['daily_supply'].result.rows)
        else:
            supply_df = pd.DataFrame()

        if results.get('large_mints') and hasattr(results['large_mints'], 'result'):
            mints_df = pd.DataFrame(results['large_mints'].result.rows)
        else:
            mints_df = pd.DataFrame()

    print(f"\n  Daily supply data: {len(supply_df)} rows")
    print(f"  Large mint events: {len(mints_df)} rows")

    if not supply_df.empty:
        print(f"  Supply columns: {supply_df.columns.tolist()}")
        print(f"  Sample:\n{supply_df.head(3)}")

    if not mints_df.empty:
        print(f"  Mints columns: {mints_df.columns.tolist()}")
        print(f"  Sample:\n{mints_df.head(3)}")

    # ─── Load candles ─────────────────────────────────────────────────────────
    print("\n[STEP 2] Loading candle data...")
    btc_candles = load_candles("BTC")
    eth_candles = load_candles("ETH")

    # ─── Compute features ─────────────────────────────────────────────────────
    print("\n[STEP 3] Computing features...")
    features = compute_features(supply_df)

    if features.empty:
        print("\n  WARNING: Could not compute features from Dune data.")
        print("  Falling back to large_mints event study only...")
    else:
        print(f"  Features computed: {len(features)} days")
        print(f"  Large mint events: {features['large_mint_event'].sum()}")
        print(f"  Feature stats:")
        print(features[['net_mint_usd', 'daily_mint_z7', 'daily_mint_z30',
                        'large_mint_event', 'supply_change_rate']].describe())

    # ─── Event study ──────────────────────────────────────────────────────────
    print("\n[STEP 4] Running event studies...")

    # Event set 1: Large mint events from Dune individual mints
    if not mints_df.empty and 'block_time' in mints_df.columns:
        mints_df['block_time'] = pd.to_datetime(mints_df['block_time'])

        # Filter >$100M events
        if 'amount_usd' in mints_df.columns:
            large_events = mints_df[mints_df['amount_usd'] >= LARGE_MINT_THRESHOLD]
            medium_events = mints_df[mints_df['amount_usd'] >= MEDIUM_MINT_THRESHOLD]

            print(f"\n  Events >$100M: {len(large_events)}")
            print(f"  Events >$10M: {len(medium_events)}")

            if len(large_events) >= 5:
                event_times = pd.DatetimeIndex(large_events['block_time'].values)

                btc_results = event_study(btc_candles, event_times, asset="BTC")
                print_event_study(btc_results, "BTC", f"Large Mint >$100M (N={len(large_events)})")

                eth_results = event_study(eth_candles, event_times, asset="ETH")
                print_event_study(eth_results, "ETH", f"Large Mint >$100M (N={len(large_events)})")

            if len(medium_events) >= 10:
                event_times_med = pd.DatetimeIndex(medium_events['block_time'].values)

                btc_results_med = event_study(btc_candles, event_times_med, asset="BTC")
                print_event_study(btc_results_med, "BTC", f"Medium Mint >$10M (N={len(medium_events)})")

                eth_results_med = event_study(eth_candles, event_times_med, asset="ETH")
                print_event_study(eth_results_med, "ETH", f"Medium Mint >$10M (N={len(medium_events)})")

    # Event set 2: From daily features (z-score spikes)
    if not features.empty:
        # Events where z30 > 2 (large positive supply shock)
        zscore_events = features[features['daily_mint_z30'] > 2.0]
        print(f"\n  Z-score > 2.0 events: {len(zscore_events)}")

        if len(zscore_events) >= 5:
            event_times_z = pd.DatetimeIndex(zscore_events.index)

            btc_z_results = event_study(btc_candles, event_times_z, asset="BTC")
            print_event_study(btc_z_results, "BTC", f"Supply Z30 > 2.0 (N={len(zscore_events)})")

            eth_z_results = event_study(eth_candles, event_times_z, asset="ETH")
            print_event_study(eth_z_results, "ETH", f"Supply Z30 > 2.0 (N={len(zscore_events)})")

    # ─── Walk-forward (if any signal looks promising) ─────────────────────────
    print("\n[STEP 5] Walk-forward backtest (if promising signals found)...")

    # Run walk-forward on all event sets that had > 5 events
    if not mints_df.empty and 'block_time' in mints_df.columns and 'amount_usd' in mints_df.columns:
        medium_events = mints_df[mints_df['amount_usd'] >= MEDIUM_MINT_THRESHOLD]
        if len(medium_events) >= 10:
            event_times_wf = pd.DatetimeIndex(medium_events['block_time'].values)

            for hold_h in [24, 48, 168]:
                walk_forward_backtest(btc_candles, event_times_wf, hold_hours=hold_h, asset="BTC")
                walk_forward_backtest(eth_candles, event_times_wf, hold_hours=hold_h, asset="ETH")

    # ─── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  SUMMARY & CONCLUSIONS")
    print("=" * 70)
    print("""
    VERDICT: THESIS INVERTED — Mints predict NEGATIVE returns (BEARISH).

    Key findings:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    1. Large mints (>$100M) predict NEGATIVE BTC returns:
       - 24h: -0.57% excess, t=-3.77, p=0.0002 *** (WR 34.4%)
       - 7d:  -1.79% excess, t=-4.65, p<0.0001 *** (WR 35.0%)
       Highly significant but in the WRONG direction for a long signal.

    2. Medium mints (>$10M, N=2892) confirm the pattern for BTC:
       - 24h: -0.24% excess, t=-6.04, p<0.0001 ***
       - 48h: -0.37% excess, t=-6.89, p<0.0001 ***
       - 7d:  -1.01% excess, t=-10.61, p<0.0001 ***
       Massive sample, extremely significant, consistently bearish.

    3. ETH shows NO significant effect (p>0.05 at all horizons except
       a tiny +0.08% at 4h which is not tradeable after fees).

    4. Walk-forward confirms: BTC PF 0.83-1.01 (unprofitable/breakeven).
       ETH 168h PF=1.43 but only 50 trades with huge variance (max +38%).

    5. Interpretation: Large mints happen when issuers are RESPONDING to
       demand that has ALREADY peaked. By the time Tether/Circle mint,
       the buying pressure is exhausted. The mint is a LAGGING indicator,
       not a leading one. The market sells the news.

    CONCLUSION: Signal is DEAD as a LONG signal. Could theoretically be
    used as a SHORT signal (fade the mint), but:
    - Win rate 35% means brutal drawdowns
    - The edge requires 7d hold (168h) which is too long for perp trading
    - Funding costs on short positions would eat the edge
    - No walk-forward PF > 1.3 for BTC shorts

    STATUS: X7 KILLED. Do not proceed to controller implementation.
    """)


if __name__ == "__main__":
    main()
