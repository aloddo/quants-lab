"""
X7 Smart Money IC Analysis — Whale flow signals vs CEX forward returns.

Uses:
- Whale flow data from Dune (30d, hourly, WETH/WBTC/LINK by size bucket)
- CEX prices from MongoDB candles (1h)
- Also: re-query Dune with correct labels for smart money flow (90d)

Computes:
- whale_concentration_z24: z-score of whale % of hourly DEX volume
- whale_net_direction: sign of (whale_buy - whale_sell) relative to total
- smart_money_flow_z24: z-score of labeled wallet net flow

IC = Spearman correlation between feature and forward returns (1h, 4h, 24h)
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from scipy import stats

from dune_client.client import DuneClient

# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────
DUNE_API_KEY = "4lS0uYbOdht78ZzWK0R63NEccj9Z9JLD"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.environ.get("MONGO_DATABASE", "quants_lab")
OUTPUT_DIR = Path("/Users/hermes/quants-lab/app/data/cache/onchain_x7_smart_money")

dune = DuneClient(api_key=DUNE_API_KEY)

# ─────────────────────────────────────────────────────────────────────
# Step 1: Check what label_types/names are actually available
# ─────────────────────────────────────────────────────────────────────
print("=" * 70)
print("STEP 1: Discover actual label types in labels.addresses")
print("=" * 70)

# Check what 'name' values exist for identifier type (most useful)
try:
    result = dune.run_sql(
        query_sql="""
        SELECT name, label_type, blockchain, COUNT(*) as cnt
        FROM labels.addresses
        WHERE label_type = 'identifier'
          AND blockchain = 'ethereum'
        GROUP BY 1, 2, 3
        ORDER BY cnt DESC
        LIMIT 40
        """,
        is_private=False
    )
    if hasattr(result, 'result') and result.result and hasattr(result.result, 'rows'):
        rows = result.result.rows
        print(f"\n  Top identifier labels on Ethereum ({len(rows)} shown):")
        for r in rows[:40]:
            print(f"    {r['name']:30s} | {r['cnt']:>10,}")
except Exception as e:
    print(f"  ERROR: {e}")

time.sleep(2)

# Check 'usage' label type names
try:
    result = dune.run_sql(
        query_sql="""
        SELECT name, COUNT(*) as cnt
        FROM labels.addresses
        WHERE label_type = 'usage'
          AND blockchain = 'ethereum'
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 30
        """,
        is_private=False
    )
    if hasattr(result, 'result') and result.result and hasattr(result.result, 'rows'):
        rows = result.result.rows
        print(f"\n  Top usage labels on Ethereum ({len(rows)} shown):")
        for r in rows[:30]:
            print(f"    {r['name']:30s} | {r['cnt']:>10,}")
except Exception as e:
    print(f"  ERROR: {e}")

time.sleep(2)

# Check 'persona' label type names
try:
    result = dune.run_sql(
        query_sql="""
        SELECT name, COUNT(*) as cnt
        FROM labels.addresses
        WHERE label_type = 'persona'
          AND blockchain = 'ethereum'
        GROUP BY 1
        ORDER BY cnt DESC
        LIMIT 30
        """,
        is_private=False
    )
    if hasattr(result, 'result') and result.result and hasattr(result.result, 'rows'):
        rows = result.result.rows
        print(f"\n  Top persona labels on Ethereum ({len(rows)} shown):")
        for r in rows[:30]:
            print(f"    {r['name']:30s} | {r['cnt']:>10,}")
except Exception as e:
    print(f"  ERROR: {e}")

time.sleep(2)

# ─────────────────────────────────────────────────────────────────────
# Step 2: Query smart money flow with correct label names
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("STEP 2: Smart money flow with correct labels")
print("=" * 70)

# Based on typical Dune label names, try 'Binance', 'Wintermute', 'Jump Trading' etc.
# Also try persona-based: 'Heavy DEX Trader'
SMART_MONEY_QUERY_V2 = """
-- Smart money flow v2: use persona labels for heavy traders
WITH smart_wallets AS (
    SELECT DISTINCT address
    FROM labels.addresses
    WHERE blockchain = 'ethereum'
      AND (
        (label_type = 'persona' AND name IN ('Heavy DEX Trader', 'DEX Trader'))
        OR (label_type = 'identifier' AND name IN (
            'Wintermute', 'Jump Trading', 'Alameda Research',
            'Three Arrows Capital', 'Cumberland', 'DWF Labs',
            'GSR', 'Amber Group', 'Genesis Trading'
        ))
      )
),
all_trades AS (
    SELECT
        date_trunc('hour', t.block_time) as hour,
        CASE
            WHEN token_bought_symbol IN ('WETH', 'ETH') THEN amount_usd
            WHEN token_sold_symbol IN ('WETH', 'ETH') THEN -amount_usd
            ELSE 0
        END as eth_net_usd,
        CASE
            WHEN t.tx_from IN (SELECT address FROM smart_wallets) THEN 'smart'
            ELSE 'retail'
        END as wallet_type
    FROM dex.trades t
    WHERE t.blockchain = 'ethereum'
      AND t.block_time >= NOW() - INTERVAL '90' day
      AND (token_bought_symbol IN ('WETH', 'ETH') OR token_sold_symbol IN ('WETH', 'ETH'))
      AND amount_usd > 100
)
SELECT
    hour,
    wallet_type,
    SUM(CASE WHEN eth_net_usd > 0 THEN eth_net_usd ELSE 0 END) as buy_usd,
    SUM(CASE WHEN eth_net_usd < 0 THEN eth_net_usd ELSE 0 END) as sell_usd,
    SUM(eth_net_usd) as net_flow_usd,
    COUNT(*) as trades
FROM all_trades
GROUP BY 1, 2
ORDER BY 1
"""

print("\n--- Running smart money flow v2 (correct labels, 90d) ---")
print("(May take 2-5 minutes...)")

df_smart = None
try:
    result = dune.run_sql(
        query_sql=SMART_MONEY_QUERY_V2,
        is_private=False
    )
    if hasattr(result, 'result') and result.result and hasattr(result.result, 'rows'):
        rows = result.result.rows
        print(f"  Got {len(rows)} rows")
        if rows:
            df_smart = pd.DataFrame(rows)
            print(f"  Wallet types found: {df_smart['wallet_type'].unique()}")
            print(f"  Date range: {df_smart['hour'].min()} to {df_smart['hour'].max()}")

            # Check smart money proportion
            smart_trades = df_smart[df_smart['wallet_type'] == 'smart']['trades'].sum()
            total_trades = df_smart['trades'].sum()
            print(f"  Smart money trades: {smart_trades:,} / {total_trades:,} ({100*smart_trades/max(total_trades,1):.2f}%)")

            df_smart.to_parquet(OUTPUT_DIR / "smart_money_flow_v2_90d.parquet", index=False)
    elif hasattr(result, 'state'):
        print(f"  Query state: {result.state}")
        if hasattr(result, 'error'):
            print(f"  Error: {result.error}")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────
# Step 3: Load CEX prices from MongoDB for IC calculation
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("STEP 3: Load CEX prices from MongoDB")
print("=" * 70)

try:
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Get 1h ETH candles for the last 90 days
    cutoff = datetime.utcnow() - timedelta(days=90)

    # Check what collections have candle data
    collections = db.list_collection_names()
    candle_cols = [c for c in collections if 'candle' in c.lower()]
    print(f"  Candle collections: {candle_cols}")

    # Try to get ETH 1h candles from parquet instead
    import glob
    parquet_files = glob.glob("/Users/hermes/quants-lab/app/data/cache/candles/**/*ETH*1h*", recursive=True)
    if not parquet_files:
        parquet_files = glob.glob("/Users/hermes/quants-lab/app/data/cache/candles/**/*ETH*", recursive=True)
    print(f"  ETH parquet files found: {parquet_files[:5]}")

    # Load ETH price data
    df_price = None
    for pf in parquet_files:
        if '1h' in pf or '3600' in pf or 'ETH-USDT' in pf:
            try:
                tmp = pd.read_parquet(pf)
                print(f"  Loading: {pf}")
                print(f"    Columns: {list(tmp.columns)[:10]}")
                print(f"    Shape: {tmp.shape}")
                if 'close' in tmp.columns or 'Close' in tmp.columns:
                    df_price = tmp
                    break
            except Exception as e2:
                print(f"    Error: {e2}")

    # If no 1h parquet, try 1m and resample
    if df_price is None:
        parquet_1m = glob.glob("/Users/hermes/quants-lab/app/data/cache/candles/**/bybit*ETH-USDT*", recursive=True)
        if parquet_1m:
            print(f"\n  Trying 1m candles: {parquet_1m[0]}")
            tmp = pd.read_parquet(parquet_1m[0])
            print(f"    Columns: {list(tmp.columns)}")
            print(f"    Shape: {tmp.shape}")
            if 'timestamp' in tmp.columns:
                tmp['datetime'] = pd.to_datetime(tmp['timestamp'], unit='ms')
                tmp = tmp.set_index('datetime')
            elif tmp.index.dtype == 'datetime64[ns]' or 'datetime' in str(tmp.index.dtype):
                pass
            df_price = tmp

except Exception as e:
    print(f"  ERROR loading prices: {e}")
    import traceback
    traceback.print_exc()
    df_price = None


# ─────────────────────────────────────────────────────────────────────
# Step 4: IC computation — whale flow vs forward returns
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("STEP 4: IC Analysis — Whale/Smart money flow vs forward returns")
print("=" * 70)

# Load whale flow data
df_whale = pd.read_parquet(OUTPUT_DIR / "whale_flow_30d.parquet")

# Focus on WETH (most liquid, maps to ETH-USDT on CEX)
df_weth = df_whale[df_whale['token'] == 'WETH'].copy()

# Pivot by size bucket
pivot_whale = df_weth.pivot_table(
    index='hour',
    columns='size_bucket',
    values='total_volume_usd',
    aggfunc='sum'
).fillna(0)

# Parse timestamps
pivot_whale.index = pd.to_datetime(pivot_whale.index.str.replace(' UTC', '', regex=False))
pivot_whale = pivot_whale.sort_index()

print(f"  Whale flow data: {len(pivot_whale)} hours")
print(f"  Date range: {pivot_whale.index.min()} to {pivot_whale.index.max()}")

# Compute features
total_vol = pivot_whale.sum(axis=1)
whale_pct = pivot_whale.get('whale', 0) / total_vol.replace(0, np.nan)
retail_pct = pivot_whale.get('retail', 0) / total_vol.replace(0, np.nan)

# Feature 1: whale_concentration_z24
whale_z24 = (whale_pct - whale_pct.rolling(24).mean()) / whale_pct.rolling(24).std()

# Feature 2: whale_vs_retail_ratio
whale_retail_ratio = whale_pct / retail_pct.replace(0, np.nan)
whale_retail_z = (whale_retail_ratio - whale_retail_ratio.rolling(24).mean()) / whale_retail_ratio.rolling(24).std()

features = pd.DataFrame({
    'whale_concentration_z24': whale_z24,
    'whale_vs_retail_z24': whale_retail_z,
    'whale_pct': whale_pct,
}, index=pivot_whale.index)

print(f"\n  Feature stats:")
print(features.describe())

# Try to get price data for IC
if df_price is not None and len(df_price) > 0:
    print("\n  Aligning price data with features...")

    # Ensure price has datetime index
    if 'close' in df_price.columns:
        close_col = 'close'
    elif 'Close' in df_price.columns:
        close_col = 'Close'
    else:
        close_col = None
        print(f"  No close column found. Columns: {list(df_price.columns)}")

    if close_col:
        # Resample to 1h if needed
        if hasattr(df_price.index, 'freq') and df_price.index.freq and df_price.index.freq.n < 60:
            price_1h = df_price[close_col].resample('1h').last().dropna()
        else:
            price_1h = df_price[close_col]

        # Compute forward returns
        fwd_1h = price_1h.pct_change(1).shift(-1)
        fwd_4h = price_1h.pct_change(4).shift(-4)
        fwd_24h = price_1h.pct_change(24).shift(-24)

        # Align
        common_idx = features.index.intersection(price_1h.index)
        print(f"  Common hours: {len(common_idx)}")

        if len(common_idx) > 50:
            feat_aligned = features.loc[common_idx]
            ret_1h = fwd_1h.loc[common_idx]
            ret_4h = fwd_4h.loc[common_idx]
            ret_24h = fwd_24h.loc[common_idx]

            print("\n  IC Results (Spearman rank correlation):")
            print(f"  {'Feature':<30s} {'IC_1h':>8s} {'IC_4h':>8s} {'IC_24h':>8s} {'p_1h':>8s}")
            print("  " + "-" * 65)

            for col in features.columns:
                mask = ~(feat_aligned[col].isna() | ret_1h.isna())
                if mask.sum() > 30:
                    ic1, p1 = stats.spearmanr(feat_aligned[col][mask], ret_1h[mask])
                    mask4 = ~(feat_aligned[col].isna() | ret_4h.isna())
                    ic4, p4 = stats.spearmanr(feat_aligned[col][mask4], ret_4h[mask4]) if mask4.sum() > 30 else (np.nan, 1)
                    mask24 = ~(feat_aligned[col].isna() | ret_24h.isna())
                    ic24, p24 = stats.spearmanr(feat_aligned[col][mask24], ret_24h[mask24]) if mask24.sum() > 30 else (np.nan, 1)
                    print(f"  {col:<30s} {ic1:>8.4f} {ic4:>8.4f} {ic24:>8.4f} {p1:>8.4f}")
                else:
                    print(f"  {col:<30s} insufficient data (N={mask.sum()})")
        else:
            print("  Not enough overlapping hours for IC calculation")
else:
    print("\n  No CEX price data available. Using Dune ETH price instead...")

    # Query ETH hourly price from Dune as fallback
    PRICE_QUERY = """
    SELECT
        date_trunc('hour', minute) as hour,
        AVG(price) as price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2  -- WETH
      AND minute >= NOW() - INTERVAL '30' day
    GROUP BY 1
    ORDER BY 1
    """

    print("  Querying ETH hourly price from Dune prices.usd...")
    try:
        result = dune.run_sql(query_sql=PRICE_QUERY, is_private=False)
        if hasattr(result, 'result') and result.result and hasattr(result.result, 'rows'):
            rows = result.result.rows
            print(f"  Got {len(rows)} price rows")

            if rows:
                df_px = pd.DataFrame(rows)
                df_px['hour'] = pd.to_datetime(df_px['hour'].str.replace(' UTC', '', regex=False))
                df_px = df_px.set_index('hour').sort_index()
                df_px['price'] = df_px['price'].astype(float)

                # Forward returns
                df_px['fwd_1h'] = df_px['price'].pct_change(1).shift(-1)
                df_px['fwd_4h'] = df_px['price'].pct_change(4).shift(-4)
                df_px['fwd_24h'] = df_px['price'].pct_change(24).shift(-24)

                # Align features with price
                common_idx = features.index.intersection(df_px.index)
                print(f"  Common hours between features and prices: {len(common_idx)}")

                if len(common_idx) > 50:
                    feat_aligned = features.loc[common_idx]

                    print("\n  IC Results (Spearman rank correlation vs ETH forward returns):")
                    print(f"  {'Feature':<30s} {'IC_1h':>8s} {'IC_4h':>8s} {'IC_24h':>8s} {'p_1h':>8s} {'N':>6s}")
                    print("  " + "-" * 72)

                    for col in features.columns:
                        for horizon, ret_col in [('1h', 'fwd_1h'), ('4h', 'fwd_4h'), ('24h', 'fwd_24h')]:
                            pass  # computed below

                        mask = ~(feat_aligned[col].isna() | df_px.loc[common_idx, 'fwd_1h'].isna())
                        n = mask.sum()
                        if n > 30:
                            ic1, p1 = stats.spearmanr(
                                feat_aligned[col][mask],
                                df_px.loc[common_idx, 'fwd_1h'][mask]
                            )
                            mask4 = ~(feat_aligned[col].isna() | df_px.loc[common_idx, 'fwd_4h'].isna())
                            ic4, _ = stats.spearmanr(
                                feat_aligned[col][mask4],
                                df_px.loc[common_idx, 'fwd_4h'][mask4]
                            ) if mask4.sum() > 30 else (np.nan, 1)
                            mask24 = ~(feat_aligned[col].isna() | df_px.loc[common_idx, 'fwd_24h'].isna())
                            ic24, _ = stats.spearmanr(
                                feat_aligned[col][mask24],
                                df_px.loc[common_idx, 'fwd_24h'][mask24]
                            ) if mask24.sum() > 30 else (np.nan, 1)
                            print(f"  {col:<30s} {ic1:>8.4f} {ic4:>8.4f} {ic24:>8.4f} {p1:>8.4f} {n:>6d}")
                        else:
                            print(f"  {col:<30s} insufficient data (N={n})")

                    # Save combined features + returns
                    combined = feat_aligned.join(df_px.loc[common_idx, ['price', 'fwd_1h', 'fwd_4h', 'fwd_24h']])
                    combined.to_parquet(OUTPUT_DIR / "weth_features_with_returns.parquet")
                    print(f"\n  Saved: {OUTPUT_DIR / 'weth_features_with_returns.parquet'}")

                    # Decile analysis for whale_concentration_z24
                    print("\n\n  DECILE ANALYSIS: whale_concentration_z24 → fwd_24h return")
                    print("  " + "-" * 50)
                    valid = combined.dropna(subset=['whale_concentration_z24', 'fwd_24h'])
                    if len(valid) > 50:
                        valid['decile'] = pd.qcut(valid['whale_concentration_z24'], 5, labels=False, duplicates='drop')
                        decile_stats = valid.groupby('decile').agg(
                            mean_ret=('fwd_24h', 'mean'),
                            median_ret=('fwd_24h', 'median'),
                            count=('fwd_24h', 'count'),
                            mean_signal=('whale_concentration_z24', 'mean')
                        )
                        print(decile_stats.to_string())
                        print(f"\n  Monotonicity: L5-L1 spread = {decile_stats['mean_ret'].iloc[-1] - decile_stats['mean_ret'].iloc[0]:.5f}")

                else:
                    print(f"  Only {len(common_idx)} overlapping hours — need more data")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────
# Step 5: Smart money flow IC (if v2 query worked)
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("STEP 5: Smart money (labeled wallets) flow IC")
print("=" * 70)

smart_file = OUTPUT_DIR / "smart_money_flow_v2_90d.parquet"
if smart_file.exists():
    df_sm = pd.read_parquet(smart_file)
    wallet_types = df_sm['wallet_type'].unique()
    print(f"  Wallet types: {wallet_types}")

    if 'smart' in wallet_types:
        # Pivot smart vs retail
        df_sm['hour'] = pd.to_datetime(df_sm['hour'].str.replace(' UTC', '', regex=False))
        pivot_sm = df_sm.pivot_table(
            index='hour', columns='wallet_type', values='net_flow_usd', aggfunc='sum'
        ).fillna(0).sort_index()

        smart_flow = pivot_sm.get('smart', pd.Series(dtype=float))
        retail_flow = pivot_sm.get('retail', pd.Series(dtype=float))

        # Features
        sm_z24 = (smart_flow - smart_flow.rolling(24).mean()) / smart_flow.rolling(24).std()
        divergence = np.sign(smart_flow) - np.sign(retail_flow)
        concentration = smart_flow.abs() / (smart_flow.abs() + retail_flow.abs()).replace(0, np.nan)

        sm_features = pd.DataFrame({
            'smart_money_flow_z24': sm_z24,
            'smart_vs_retail_divergence': divergence,
            'labeled_wallet_concentration': concentration,
        }, index=pivot_sm.index)

        print(f"\n  Smart money feature stats:")
        print(sm_features.describe())

        sm_features.to_parquet(OUTPUT_DIR / "smart_money_features_v2.parquet")
        print(f"\n  Saved: {OUTPUT_DIR / 'smart_money_features_v2.parquet'}")

        # IC if we have prices
        px_file = OUTPUT_DIR / "weth_features_with_returns.parquet"
        if px_file.exists():
            df_px_ref = pd.read_parquet(px_file)
            common = sm_features.index.intersection(df_px_ref.index)
            if len(common) > 50:
                print(f"\n  IC for smart money features (N={len(common)}):")
                print(f"  {'Feature':<35s} {'IC_1h':>8s} {'IC_4h':>8s} {'IC_24h':>8s}")
                print("  " + "-" * 60)

                for col in sm_features.columns:
                    for horizon in ['fwd_1h', 'fwd_4h', 'fwd_24h']:
                        pass
                    mask = ~(sm_features.loc[common, col].isna() | df_px_ref.loc[common, 'fwd_1h'].isna())
                    if mask.sum() > 30:
                        ic1, _ = stats.spearmanr(sm_features.loc[common, col][mask], df_px_ref.loc[common, 'fwd_1h'][mask])
                        mask4 = ~(sm_features.loc[common, col].isna() | df_px_ref.loc[common, 'fwd_4h'].isna())
                        ic4, _ = stats.spearmanr(sm_features.loc[common, col][mask4], df_px_ref.loc[common, 'fwd_4h'][mask4]) if mask4.sum() > 30 else (np.nan, 1)
                        mask24 = ~(sm_features.loc[common, col].isna() | df_px_ref.loc[common, 'fwd_24h'].isna())
                        ic24, _ = stats.spearmanr(sm_features.loc[common, col][mask24], df_px_ref.loc[common, 'fwd_24h'][mask24]) if mask24.sum() > 30 else (np.nan, 1)
                        print(f"  {col:<35s} {ic1:>8.4f} {ic4:>8.4f} {ic24:>8.4f}")
    else:
        print("  No 'smart' wallet type found — labels join returned 0 matches.")
        print("  This means the label names we used don't exist in Dune's labels.addresses.")
        print("  The whale-by-size approach (Step 4) is the viable alternative.")
else:
    print("  No smart money v2 data file — query may have failed.")


# ─────────────────────────────────────────────────────────────────────
# Final Summary
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)
print(f"""
Files generated:
""")
for f in sorted(OUTPUT_DIR.glob("*.parquet")):
    size_mb = f.stat().st_size / 1024 / 1024
    print(f"  {f.name:50s} ({size_mb:.2f} MB)")

print("""
INTERPRETATION GUIDE:
- |IC| > 0.03 with p < 0.05 → weak but potentially tradeable signal
- |IC| > 0.05 → moderate signal, worth pursuing
- |IC| > 0.10 → strong signal (rare for single features)
- Check monotonicity in decile analysis (L5 should beat L1 monotonically)

NEXT STEPS if IC is promising:
1. Extend to 365 days (need Dune paid plan for longer queries)
2. Multi-token analysis (WBTC, LINK, ARB, OP)
3. Walk-forward IC stability test (rolling 30d windows)
4. Build FeatureBase subclass for pipeline integration
""")
