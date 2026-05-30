#!/usr/bin/env python3
"""
X7 — Token Unlock / Vesting Event Study
=========================================
Thesis: Large token unlocks from team/foundation wallets create predictable
sell pressure. We can SHORT before unlocks and cover after.

Approach:
1. Search Dune for token unlock/vesting data
2. Query large transfers from known foundation/team wallets to exchanges
3. Cross-reference with Bybit perp returns
4. Event study: pre/post unlock returns vs random baseline
5. Bootstrap confidence intervals

Tokens of interest (Bybit-tradeable with active vesting):
  ARB, OP, SUI, APT, SEI, NEAR
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

# Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DUNE_API_KEY = "4lS0uYbOdht78ZzWK0R63NEccj9Z9JLD"
CACHE_DIR = Path("/Users/hermes/quants-lab/app/data/cache/onchain_x7_token_unlocks")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Bybit-tradeable tokens with known vesting schedules
TOKENS = {
    "ARB": {"chain": "arbitrum", "contract": "0x912CE59144191C1204E64559FE8253a0e49E6548"},
    "OP": {"chain": "optimism", "contract": "0x4200000000000000000000000000000000000042"},
    "SUI": {"chain": "sui", "contract": None},  # Sui is its own chain
    "APT": {"chain": "aptos", "contract": None},  # Aptos is its own chain
    "SEI": {"chain": "sei", "contract": None},  # Sei is its own chain
    "NEAR": {"chain": "near", "contract": None},  # NEAR is its own chain
}

# Known foundation/team wallet labels (from Dune labels)
# We'll search for these in the data
FOUNDATION_LABELS = [
    "foundation", "team", "treasury", "vesting", "investor",
    "ecosystem", "community", "airdrop", "token_unlock"
]


def init_dune_client():
    """Initialize Dune client."""
    return DuneClient(api_key=DUNE_API_KEY)


# ============================================================
# STEP 1: Search Dune for relevant tables
# ============================================================

def search_dune_tables(client: DuneClient):
    """Search Dune catalog for token unlock / vesting related tables."""
    log.info("=" * 60)
    log.info("STEP 1: Searching Dune for token unlock/vesting tables")
    log.info("=" * 60)

    search_terms = [
        "token unlock",
        "vesting",
        "token release",
        "cliff vesting",
        "token emission",
        "foundation transfer",
        "treasury outflow",
    ]

    all_results = []
    for term in search_terms:
        log.info(f"Searching for: '{term}'")
        try:
            # Use the Dune API catalog search
            results = client.search_tables(term)
            if results:
                for r in results:
                    all_results.append({
                        "search_term": term,
                        "namespace": getattr(r, 'namespace', ''),
                        "table_name": getattr(r, 'table_name', getattr(r, 'name', str(r))),
                        "full_name": getattr(r, 'full_name', ''),
                        "description": getattr(r, 'description', ''),
                    })
                log.info(f"  Found {len(results)} results")
            else:
                log.info(f"  No results")
        except Exception as e:
            log.warning(f"  Search failed: {e}")
        time.sleep(1)  # Rate limit

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(CACHE_DIR / "dune_table_search_results.csv", index=False)
        log.info(f"\nTotal tables found: {len(df)}")
        log.info(f"Unique tables: {df['table_name'].nunique()}")
        print("\n--- TOP RESULTS ---")
        print(df[['search_term', 'namespace', 'table_name']].drop_duplicates().to_string())
    else:
        log.info("No curated vesting tables found. Proceeding with transfer-based approach.")

    return all_results


# ============================================================
# STEP 2: Query large transfers from foundation wallets
# ============================================================

def query_arb_foundation_transfers(client: DuneClient) -> pd.DataFrame:
    """
    Query ARB token transfers from known foundation/team wallets.
    ARB is the best candidate — EVM chain, well-labeled on Dune.
    """
    log.info("\n" + "=" * 60)
    log.info("STEP 2: Querying ARB foundation transfers (Dune SQL)")
    log.info("=" * 60)

    # Query 1: ARB large transfers from labeled wallets
    # Uses dune labels to identify foundation/team wallets
    query_sql = """
    SELECT
        block_time,
        "from" as sender,
        "to" as receiver,
        CAST(value AS DOUBLE) / 1e18 as amount_arb,
        CAST(value AS DOUBLE) / 1e18 * p.price as amount_usd,
        tx_hash
    FROM arbitrum.erc20_arbitrum.evt_Transfer t
    LEFT JOIN prices.usd p
        ON p.contract_address = 0x912CE59144191C1204E64559FE8253a0e49E6548
        AND p.blockchain = 'arbitrum'
        AND p.minute = date_trunc('minute', t.block_time)
    WHERE t.contract_address = 0x912CE59144191C1204E64559FE8253a0e49E6548
        AND t.block_time >= NOW() - INTERVAL '365' DAY
        AND CAST(value AS DOUBLE) / 1e18 > 1000000
    ORDER BY block_time DESC
    """

    cache_file = CACHE_DIR / "arb_large_transfers.csv"
    if cache_file.exists():
        log.info(f"Loading from cache: {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['block_time'])

    log.info("Executing Dune query for ARB large transfers...")
    try:
        # Use run_sql for direct SQL execution
        result = client.run_sql(query_sql)
        if result and hasattr(result, 'result') and result.result:
            rows = result.result.rows
            df = pd.DataFrame(rows)
            df.to_csv(cache_file, index=False)
            log.info(f"Got {len(df)} large ARB transfers")
            return df
        else:
            log.warning("No results from ARB transfer query")
            return pd.DataFrame()
    except Exception as e:
        log.error(f"Dune query failed: {e}")
        return pd.DataFrame()


def query_op_foundation_transfers(client: DuneClient) -> pd.DataFrame:
    """Query OP token transfers from foundation wallets."""
    query_sql = """
    SELECT
        block_time,
        "from" as sender,
        "to" as receiver,
        CAST(value AS DOUBLE) / 1e18 as amount_op,
        CAST(value AS DOUBLE) / 1e18 * p.price as amount_usd,
        tx_hash
    FROM optimism.erc20_optimism.evt_Transfer t
    LEFT JOIN prices.usd p
        ON p.contract_address = 0x4200000000000000000000000000000000000042
        AND p.blockchain = 'optimism'
        AND p.minute = date_trunc('minute', t.block_time)
    WHERE t.contract_address = 0x4200000000000000000000000000000000000042
        AND t.block_time >= NOW() - INTERVAL '365' DAY
        AND CAST(value AS DOUBLE) / 1e18 > 1000000
    ORDER BY block_time DESC
    """

    cache_file = CACHE_DIR / "op_large_transfers.csv"
    if cache_file.exists():
        log.info(f"Loading from cache: {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['block_time'])

    log.info("Executing Dune query for OP large transfers...")
    try:
        result = client.run_sql(query_sql)
        if result and hasattr(result, 'result') and result.result:
            rows = result.result.rows
            df = pd.DataFrame(rows)
            df.to_csv(cache_file, index=False)
            log.info(f"Got {len(df)} large OP transfers")
            return df
        else:
            log.warning("No results from OP transfer query")
            return pd.DataFrame()
    except Exception as e:
        log.error(f"Dune query failed: {e}")
        return pd.DataFrame()


def query_multi_token_unlocks(client: DuneClient) -> pd.DataFrame:
    """
    Alternative approach: use a known Dune query that tracks token unlocks.
    Several community queries exist (e.g., query_id 3303077, 2461782).
    """
    log.info("\n" + "=" * 60)
    log.info("STEP 2b: Trying known Dune community queries for token unlocks")
    log.info("=" * 60)

    # Try known query IDs for token unlock tracking
    known_query_ids = [
        3303077,  # Token unlocks tracker
        2461782,  # Vesting schedule overview
        3582640,  # Token unlock events
    ]

    cache_file = CACHE_DIR / "dune_community_unlock_queries.csv"
    if cache_file.exists():
        log.info(f"Loading from cache: {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['block_time'] if 'block_time' in pd.read_csv(cache_file, nrows=0).columns else None)

    all_rows = []
    for qid in known_query_ids:
        log.info(f"Trying query ID {qid}...")
        try:
            query = QueryBase(query_id=qid)
            result = client.get_latest_result(query)
            if result and hasattr(result, 'result') and result.result and result.result.rows:
                rows = result.result.rows
                for r in rows:
                    r['source_query_id'] = qid
                all_rows.extend(rows)
                log.info(f"  Got {len(rows)} rows from query {qid}")
            else:
                log.info(f"  No data from query {qid}")
        except Exception as e:
            log.warning(f"  Query {qid} failed: {e}")
        time.sleep(2)

    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv(cache_file, index=False)
        log.info(f"Total rows from community queries: {len(df)}")
        return df

    log.info("No community query data available")
    return pd.DataFrame()


def query_cex_inflows(client: DuneClient, token: str) -> pd.DataFrame:
    """
    Query CEX deposit inflows for a specific token.
    Large inflows to exchanges often precede sell pressure.
    """
    log.info(f"\nQuerying CEX inflows for {token}...")

    # For EVM tokens, use the cex_evms schema if available
    if token == "ARB":
        query_sql = """
        SELECT
            block_time,
            "from" as sender,
            "to" as cex_address,
            CAST(value AS DOUBLE) / 1e18 as amount,
            tx_hash
        FROM arbitrum.erc20_arbitrum.evt_Transfer
        WHERE contract_address = 0x912CE59144191C1204E64559FE8253a0e49E6548
            AND block_time >= NOW() - INTERVAL '365' DAY
            AND CAST(value AS DOUBLE) / 1e18 > 500000
            AND "to" IN (
                -- Known exchange deposit addresses (Binance, OKX, Bybit, etc.)
                SELECT address FROM labels.addresses
                WHERE category = 'cex'
                    AND blockchain = 'arbitrum'
            )
        ORDER BY block_time DESC
        """
    elif token == "OP":
        query_sql = """
        SELECT
            block_time,
            "from" as sender,
            "to" as cex_address,
            CAST(value AS DOUBLE) / 1e18 as amount,
            tx_hash
        FROM optimism.erc20_optimism.evt_Transfer
        WHERE contract_address = 0x4200000000000000000000000000000000000042
            AND block_time >= NOW() - INTERVAL '365' DAY
            AND CAST(value AS DOUBLE) / 1e18 > 500000
            AND "to" IN (
                SELECT address FROM labels.addresses
                WHERE category = 'cex'
                    AND blockchain = 'optimism'
            )
        ORDER BY block_time DESC
        """
    else:
        return pd.DataFrame()

    cache_file = CACHE_DIR / f"{token.lower()}_cex_inflows.csv"
    if cache_file.exists():
        log.info(f"Loading from cache: {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['block_time'])

    try:
        result = client.run_sql(query_sql)
        if result and hasattr(result, 'result') and result.result:
            rows = result.result.rows
            df = pd.DataFrame(rows)
            df.to_csv(cache_file, index=False)
            log.info(f"Got {len(df)} CEX inflows for {token}")
            return df
    except Exception as e:
        log.error(f"CEX inflow query failed for {token}: {e}")

    return pd.DataFrame()


# ============================================================
# STEP 3: Alternative — Use TokenUnlocks.app public data
# ============================================================

def fetch_known_unlock_schedule() -> pd.DataFrame:
    """
    Hardcoded known unlock events from public sources (TokenUnlocks.app, etc.)
    These are the SCHEDULED unlocks that are known in advance.
    This is our ground truth for the event study.
    """
    log.info("\n" + "=" * 60)
    log.info("STEP 3: Loading known unlock schedule (public data)")
    log.info("=" * 60)

    # Known major unlock events (from TokenUnlocks.app, manually compiled)
    # Format: token, date, amount_usd (approximate), category
    events = [
        # ARB unlocks (monthly investor/team unlocks since Mar 2024)
        ("ARB", "2025-04-16", 100_000_000, "investor"),
        ("ARB", "2025-05-16", 100_000_000, "investor"),
        ("ARB", "2025-06-16", 100_000_000, "investor"),
        ("ARB", "2025-07-16", 100_000_000, "investor"),
        ("ARB", "2025-08-16", 100_000_000, "investor"),
        ("ARB", "2025-09-16", 100_000_000, "investor"),
        ("ARB", "2025-10-16", 100_000_000, "investor"),
        ("ARB", "2025-11-16", 100_000_000, "investor"),
        ("ARB", "2025-12-16", 100_000_000, "investor"),
        ("ARB", "2026-01-16", 100_000_000, "investor"),
        ("ARB", "2026-02-16", 100_000_000, "investor"),
        ("ARB", "2026-03-16", 100_000_000, "investor"),
        # OP unlocks (team + investor, monthly)
        ("OP", "2025-04-30", 30_000_000, "team"),
        ("OP", "2025-05-31", 30_000_000, "team"),
        ("OP", "2025-06-30", 30_000_000, "team"),
        ("OP", "2025-07-31", 30_000_000, "team"),
        ("OP", "2025-08-31", 30_000_000, "team"),
        ("OP", "2025-09-30", 30_000_000, "team"),
        ("OP", "2025-10-31", 30_000_000, "team"),
        ("OP", "2025-11-30", 30_000_000, "team"),
        ("OP", "2025-12-31", 30_000_000, "team"),
        ("OP", "2026-01-31", 30_000_000, "team"),
        ("OP", "2026-02-28", 30_000_000, "team"),
        ("OP", "2026-03-31", 30_000_000, "team"),
        # SUI unlocks (large ecosystem unlocks)
        ("SUI", "2025-05-01", 250_000_000, "ecosystem"),
        ("SUI", "2025-06-01", 60_000_000, "investor"),
        ("SUI", "2025-07-01", 60_000_000, "investor"),
        ("SUI", "2025-08-01", 60_000_000, "investor"),
        ("SUI", "2025-09-01", 60_000_000, "investor"),
        ("SUI", "2025-10-01", 60_000_000, "investor"),
        ("SUI", "2025-11-01", 60_000_000, "investor"),
        ("SUI", "2025-12-01", 60_000_000, "investor"),
        ("SUI", "2026-01-01", 60_000_000, "investor"),
        ("SUI", "2026-02-01", 60_000_000, "investor"),
        ("SUI", "2026-03-01", 60_000_000, "investor"),
        # APT unlocks (monthly)
        ("APT", "2025-04-12", 80_000_000, "investor"),
        ("APT", "2025-05-12", 80_000_000, "investor"),
        ("APT", "2025-06-12", 80_000_000, "investor"),
        ("APT", "2025-07-12", 80_000_000, "investor"),
        ("APT", "2025-08-12", 80_000_000, "investor"),
        ("APT", "2025-09-12", 80_000_000, "investor"),
        ("APT", "2025-10-12", 80_000_000, "investor"),
        ("APT", "2025-11-12", 80_000_000, "investor"),
        ("APT", "2025-12-12", 80_000_000, "investor"),
        ("APT", "2026-01-12", 80_000_000, "investor"),
        ("APT", "2026-02-12", 80_000_000, "investor"),
        ("APT", "2026-03-12", 80_000_000, "investor"),
        # SEI unlocks
        ("SEI", "2025-04-15", 40_000_000, "investor"),
        ("SEI", "2025-05-15", 40_000_000, "investor"),
        ("SEI", "2025-06-15", 40_000_000, "investor"),
        ("SEI", "2025-07-15", 40_000_000, "investor"),
        ("SEI", "2025-08-15", 40_000_000, "investor"),
        ("SEI", "2025-09-15", 40_000_000, "investor"),
        ("SEI", "2025-10-15", 40_000_000, "investor"),
        ("SEI", "2025-11-15", 40_000_000, "investor"),
        ("SEI", "2025-12-15", 40_000_000, "investor"),
        ("SEI", "2026-01-15", 40_000_000, "investor"),
        ("SEI", "2026-02-15", 40_000_000, "investor"),
        ("SEI", "2026-03-15", 40_000_000, "investor"),
        # NEAR unlocks
        ("NEAR", "2025-04-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-05-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-06-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-07-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-08-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-09-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-10-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-11-20", 20_000_000, "ecosystem"),
        ("NEAR", "2025-12-20", 20_000_000, "ecosystem"),
        ("NEAR", "2026-01-20", 20_000_000, "ecosystem"),
        ("NEAR", "2026-02-20", 20_000_000, "ecosystem"),
        ("NEAR", "2026-03-20", 20_000_000, "ecosystem"),
    ]

    df = pd.DataFrame(events, columns=["token", "date", "amount_usd", "category"])
    df["date"] = pd.to_datetime(df["date"])
    df.to_csv(CACHE_DIR / "known_unlock_schedule.csv", index=False)
    log.info(f"Loaded {len(df)} known unlock events across {df['token'].nunique()} tokens")
    log.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    log.info(f"\nPer token:\n{df.groupby('token').size()}")
    return df


# ============================================================
# STEP 4: Get Bybit perp candle data
# ============================================================

def load_bybit_candles(token: str) -> pd.DataFrame:
    """Load 1h candles from parquet cache or MongoDB."""
    pair = f"{token}-USDT"
    # Try parquet first
    parquet_dir = Path("/Users/hermes/quants-lab/app/data/cache/candles")

    # Search for matching parquet files
    matching = list(parquet_dir.glob(f"*bybit*{token}*USDT*1h*")) + \
               list(parquet_dir.glob(f"*bybit*{token}*USDT*60*"))

    if matching:
        log.info(f"Loading candles from parquet: {matching[0].name}")
        df = pd.read_parquet(matching[0])
        # The parquet has datetime index + 'timestamp' col as epoch seconds
        # Use the index as the real timestamp
        if df.index.dtype == 'datetime64[ns]' or str(df.index.dtype).startswith('datetime'):
            df = df.reset_index()
            # The reset index column might be named 'index' or the original index name
            if 'index' in df.columns and 'timestamp' in df.columns:
                # 'timestamp' col is epoch float, 'index' is datetime
                df['timestamp'] = df['index']
                df.drop(columns=['index'], inplace=True)
            elif df.columns[0] != 'timestamp':
                # First col after reset is the datetime index
                df.rename(columns={df.columns[0]: 'timestamp'}, inplace=True)
        # If timestamp is still numeric (epoch seconds), convert
        if 'timestamp' in df.columns and df['timestamp'].dtype in ['float64', 'int64']:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        return df

    # Try 1-minute candles and resample
    matching_1m = list(parquet_dir.glob(f"*bybit*{token}*USDT*1m*")) + \
                  list(parquet_dir.glob(f"*bybit*{token}*USDT*1*"))

    if matching_1m:
        log.info(f"Loading 1m candles from parquet: {matching_1m[0].name}")
        df = pd.read_parquet(matching_1m[0])
        # Same index handling
        if df.index.dtype == 'datetime64[ns]' or str(df.index.dtype).startswith('datetime'):
            df = df.reset_index()
            if 'index' in df.columns:
                df['timestamp'] = df['index']
                df.drop(columns=['index'], inplace=True)
            elif df.columns[0] != 'timestamp':
                df.rename(columns={df.columns[0]: 'timestamp'}, inplace=True)
        if 'timestamp' in df.columns and df['timestamp'].dtype in ['float64', 'int64']:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df = df.set_index('timestamp')
        # Resample to 1h
        ohlcv = df.resample('1h').agg({
            'open': 'first', 'high': 'max', 'low': 'min',
            'close': 'last', 'volume': 'sum'
        }).dropna()
        return ohlcv.reset_index()

    log.warning(f"No candle data found for {pair}")
    return pd.DataFrame()


def fetch_candles_from_bybit(token: str) -> pd.DataFrame:
    """Fetch candles directly from Bybit API if not in cache."""
    import requests

    pair = f"{token}USDT"
    url = "https://api.bybit.com/v5/market/kline"

    all_candles = []
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now() - timedelta(days=365)).timestamp() * 1000)

    log.info(f"Fetching {token}-USDT 1h candles from Bybit API...")

    current_end = end_time
    while current_end > start_time:
        params = {
            "category": "linear",
            "symbol": pair,
            "interval": "60",
            "end": current_end,
            "limit": 1000,
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            if data["retCode"] != 0:
                log.error(f"Bybit API error: {data['retMsg']}")
                break
            rows = data["result"]["list"]
            if not rows:
                break
            for r in rows:
                all_candles.append({
                    "timestamp": pd.to_datetime(int(r[0]), unit='ms'),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "volume": float(r[5]),
                })
            # Move window back
            current_end = int(rows[-1][0]) - 1
            time.sleep(0.1)
        except Exception as e:
            log.error(f"Bybit API error: {e}")
            break

    if all_candles:
        df = pd.DataFrame(all_candles).sort_values("timestamp").reset_index(drop=True)
        # Cache it
        cache_file = CACHE_DIR / f"{token}_USDT_1h_candles.parquet"
        df.to_parquet(cache_file)
        log.info(f"Fetched {len(df)} candles for {token}-USDT")
        return df

    return pd.DataFrame()


def get_candles(token: str) -> pd.DataFrame:
    """Get candles from cache or Bybit API."""
    df = load_bybit_candles(token)
    if df.empty:
        df = fetch_candles_from_bybit(token)
    return df


# ============================================================
# STEP 5: Event Study
# ============================================================

def compute_event_returns(candles: pd.DataFrame, event_date: pd.Timestamp) -> dict:
    """
    Compute returns around an unlock event.
    Returns dict with pre/post event returns at various horizons.
    """
    if candles.empty:
        return None

    # Ensure timestamp column
    if 'timestamp' not in candles.columns and candles.index.name == 'timestamp':
        candles = candles.reset_index()

    candles = candles.sort_values('timestamp')

    # Find closest candle to event date
    event_ts = event_date
    mask_before = candles['timestamp'] <= event_ts
    mask_after = candles['timestamp'] >= event_ts

    if mask_before.sum() < 24 or mask_after.sum() < 168:  # Need 24h before, 7d after
        return None

    # Get prices at key timestamps
    idx_event = mask_before.sum() - 1  # Last candle before/at event

    if idx_event < 24 or idx_event + 168 >= len(candles):
        return None

    price_event = candles.iloc[idx_event]['close']
    price_24h_before = candles.iloc[idx_event - 24]['close']
    price_24h_after = candles.iloc[min(idx_event + 24, len(candles) - 1)]['close']
    price_48h_after = candles.iloc[min(idx_event + 48, len(candles) - 1)]['close']
    price_7d_after = candles.iloc[min(idx_event + 168, len(candles) - 1)]['close']

    return {
        "ret_24h_pre": (price_event - price_24h_before) / price_24h_before,
        "ret_24h_post": (price_24h_after - price_event) / price_event,
        "ret_48h_post": (price_48h_after - price_event) / price_event,
        "ret_7d_post": (price_7d_after - price_event) / price_event,
        "price_at_event": price_event,
    }


def compute_random_baseline(candles: pd.DataFrame, n_samples: int = 1000, seed: int = 42) -> pd.DataFrame:
    """
    Compute returns at random dates for baseline comparison.
    Same methodology as event returns but on random dates.
    """
    if candles.empty or len(candles) < 200:
        return pd.DataFrame()

    rng = np.random.default_rng(seed)

    # Pick random indices (avoiding edges)
    valid_range = range(168, len(candles) - 168)
    if len(valid_range) < n_samples:
        n_samples = len(valid_range)

    random_indices = rng.choice(list(valid_range), size=n_samples, replace=False)

    results = []
    for idx in random_indices:
        price_event = candles.iloc[idx]['close']
        price_24h_before = candles.iloc[idx - 24]['close']
        price_24h_after = candles.iloc[idx + 24]['close']
        price_48h_after = candles.iloc[idx + 48]['close']
        price_7d_after = candles.iloc[idx + 168]['close']

        results.append({
            "ret_24h_pre": (price_event - price_24h_before) / price_24h_before,
            "ret_24h_post": (price_24h_after - price_event) / price_event,
            "ret_48h_post": (price_48h_after - price_event) / price_event,
            "ret_7d_post": (price_7d_after - price_event) / price_event,
        })

    return pd.DataFrame(results)


def bootstrap_ci(data: np.ndarray, n_bootstrap: int = 10000, ci: float = 0.95) -> tuple:
    """Bootstrap confidence interval for the mean."""
    rng = np.random.default_rng(42)
    means = np.array([
        rng.choice(data, size=len(data), replace=True).mean()
        for _ in range(n_bootstrap)
    ])
    alpha = (1 - ci) / 2
    return np.percentile(means, alpha * 100), np.percentile(means, (1 - alpha) * 100)


def run_event_study(unlock_schedule: pd.DataFrame) -> pd.DataFrame:
    """
    Main event study: compute returns around unlock events vs random baseline.
    """
    log.info("\n" + "=" * 60)
    log.info("STEP 5: Running Event Study")
    log.info("=" * 60)

    all_event_returns = []
    all_baselines = {}

    for token in unlock_schedule['token'].unique():
        log.info(f"\nProcessing {token}...")

        # Get candles
        candles = get_candles(token)
        if candles.empty:
            log.warning(f"  No candle data for {token}, skipping")
            continue

        # Ensure datetime
        if 'timestamp' in candles.columns:
            candles['timestamp'] = pd.to_datetime(candles['timestamp'])
        elif candles.index.name == 'timestamp':
            candles = candles.reset_index()
            candles['timestamp'] = pd.to_datetime(candles['timestamp'])

        log.info(f"  Candles: {len(candles)} rows, {candles['timestamp'].min()} to {candles['timestamp'].max()}")

        # Filter unlock events to candle date range
        token_events = unlock_schedule[unlock_schedule['token'] == token].copy()
        candle_start = candles['timestamp'].min()
        candle_end = candles['timestamp'].max()
        token_events = token_events[
            (token_events['date'] >= candle_start) &
            (token_events['date'] <= candle_end - timedelta(days=7))
        ]

        log.info(f"  Events in range: {len(token_events)}")

        # Compute event returns
        for _, event in token_events.iterrows():
            result = compute_event_returns(candles, event['date'])
            if result:
                result['token'] = token
                result['event_date'] = event['date']
                result['amount_usd'] = event['amount_usd']
                result['category'] = event['category']
                all_event_returns.append(result)

        # Compute random baseline for this token
        baseline = compute_random_baseline(candles)
        if not baseline.empty:
            all_baselines[token] = baseline
            log.info(f"  Baseline samples: {len(baseline)}")

    # Compile results
    if not all_event_returns:
        log.error("No event returns computed! Check candle data availability.")
        return pd.DataFrame()

    event_df = pd.DataFrame(all_event_returns)
    event_df.to_csv(CACHE_DIR / "event_returns.csv", index=False)

    # Aggregate baseline
    if all_baselines:
        baseline_df = pd.concat(all_baselines.values(), ignore_index=True)
        baseline_df.to_csv(CACHE_DIR / "baseline_returns.csv", index=False)
    else:
        baseline_df = pd.DataFrame()

    return event_df


# ============================================================
# STEP 6: Statistical Analysis
# ============================================================

def analyze_results(event_df: pd.DataFrame):
    """Analyze event study results with bootstrap CIs."""
    log.info("\n" + "=" * 60)
    log.info("STEP 6: Statistical Analysis")
    log.info("=" * 60)

    if event_df.empty:
        log.error("No event data to analyze")
        return

    # Load baseline
    baseline_file = CACHE_DIR / "baseline_returns.csv"
    if baseline_file.exists():
        baseline_df = pd.read_csv(baseline_file)
    else:
        baseline_df = pd.DataFrame()

    # Overall event statistics
    log.info(f"\n{'='*50}")
    log.info(f"OVERALL EVENT STUDY RESULTS (N={len(event_df)} events)")
    log.info(f"{'='*50}")

    horizons = ['ret_24h_pre', 'ret_24h_post', 'ret_48h_post', 'ret_7d_post']

    print(f"\n{'Horizon':<15} {'Mean':>8} {'Median':>8} {'Std':>8} {'95% CI':>20} {'N':>4}")
    print("-" * 70)

    for h in horizons:
        data = event_df[h].dropna().values
        if len(data) == 0:
            continue
        mean = data.mean()
        median = np.median(data)
        std = data.std()
        ci_low, ci_high = bootstrap_ci(data)
        print(f"{h:<15} {mean:>8.4f} {median:>8.4f} {std:>8.4f} [{ci_low:>7.4f}, {ci_high:>7.4f}] {len(data):>4}")

    # Compare to baseline
    if not baseline_df.empty:
        print(f"\n{'='*50}")
        print(f"BASELINE (Random Dates) (N={len(baseline_df)})")
        print(f"{'='*50}")
        print(f"\n{'Horizon':<15} {'Mean':>8} {'Median':>8} {'Std':>8} {'95% CI':>20}")
        print("-" * 60)

        for h in horizons:
            if h not in baseline_df.columns:
                continue
            data = baseline_df[h].dropna().values
            mean = data.mean()
            median = np.median(data)
            std = data.std()
            ci_low, ci_high = bootstrap_ci(data)
            print(f"{h:<15} {mean:>8.4f} {median:>8.4f} {std:>8.4f} [{ci_low:>7.4f}, {ci_high:>7.4f}]")

        # Difference (event - baseline)
        print(f"\n{'='*50}")
        print(f"DIFFERENCE (Event - Baseline)")
        print(f"{'='*50}")
        print(f"\n{'Horizon':<15} {'Diff Mean':>10} {'95% CI':>20} {'Significant':>12}")
        print("-" * 60)

        for h in horizons:
            if h not in baseline_df.columns:
                continue
            event_data = event_df[h].dropna().values
            base_data = baseline_df[h].dropna().values
            diff = event_data.mean() - base_data.mean()
            # Bootstrap CI on the difference
            rng = np.random.default_rng(42)
            diffs = []
            for _ in range(10000):
                e_sample = rng.choice(event_data, size=len(event_data), replace=True).mean()
                b_sample = rng.choice(base_data, size=len(base_data), replace=True).mean()
                diffs.append(e_sample - b_sample)
            diffs = np.array(diffs)
            ci_low, ci_high = np.percentile(diffs, [2.5, 97.5])
            sig = "YES" if (ci_low > 0 or ci_high < 0) else "NO"
            print(f"{h:<15} {diff:>10.4f} [{ci_low:>7.4f}, {ci_high:>7.4f}] {sig:>12}")

    # Per-token breakdown
    print(f"\n{'='*50}")
    print(f"PER-TOKEN BREAKDOWN")
    print(f"{'='*50}")

    for token in event_df['token'].unique():
        tdf = event_df[event_df['token'] == token]
        print(f"\n  {token} (N={len(tdf)} events):")
        print(f"    {'Horizon':<15} {'Mean':>8} {'Median':>8}")
        print(f"    {'-'*35}")
        for h in horizons:
            data = tdf[h].dropna().values
            if len(data) > 0:
                print(f"    {h:<15} {data.mean():>8.4f} {np.median(data):>8.4f}")

    # Win rate analysis (for strategy viability)
    print(f"\n{'='*50}")
    print(f"STRATEGY VIABILITY: SHORT on unlock day")
    print(f"{'='*50}")

    for horizon in ['ret_24h_post', 'ret_48h_post', 'ret_7d_post']:
        data = event_df[horizon].dropna().values
        # For SHORT strategy: negative return = win
        win_rate = (data < 0).mean()
        avg_win = data[data < 0].mean() if (data < 0).any() else 0
        avg_loss = data[data >= 0].mean() if (data >= 0).any() else 0
        # Profit factor
        gross_profit = abs(data[data < 0].sum()) if (data < 0).any() else 0
        gross_loss = abs(data[data >= 0].sum()) if (data >= 0).any() else 0
        pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        print(f"\n  {horizon}:")
        print(f"    Win Rate (SHORT): {win_rate:.1%}")
        print(f"    Avg Win:  {avg_win:.4f} ({avg_win*100:.2f}%)")
        print(f"    Avg Loss: {avg_loss:.4f} ({avg_loss*100:.2f}%)")
        print(f"    Profit Factor: {pf:.2f}")
        print(f"    Expected Value: {data.mean():.4f} ({data.mean()*100:.2f}%)")

    # Save summary
    summary = {
        "n_events": len(event_df),
        "tokens": list(event_df['token'].unique()),
        "date_range": f"{event_df['event_date'].min()} to {event_df['event_date'].max()}",
        "mean_ret_24h_post": float(event_df['ret_24h_post'].mean()),
        "mean_ret_48h_post": float(event_df['ret_48h_post'].mean()),
        "mean_ret_7d_post": float(event_df['ret_7d_post'].mean()),
        "short_win_rate_24h": float((event_df['ret_24h_post'] < 0).mean()),
        "short_win_rate_48h": float((event_df['ret_48h_post'] < 0).mean()),
        "short_win_rate_7d": float((event_df['ret_7d_post'] < 0).mean()),
    }

    with open(CACHE_DIR / "event_study_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    log.info(f"\nResults saved to {CACHE_DIR}")
    return summary


# ============================================================
# STEP 7: Walk-Forward Backtest (if signal is viable)
# ============================================================

def walk_forward_backtest(event_df: pd.DataFrame):
    """
    Simple walk-forward: SHORT on unlock day, cover after 48h.
    Track cumulative PnL over the 365-day period.
    """
    log.info("\n" + "=" * 60)
    log.info("STEP 7: Walk-Forward Backtest")
    log.info("=" * 60)

    if event_df.empty:
        log.error("No events for backtest")
        return

    # Sort by date
    event_df = event_df.sort_values('event_date').copy()

    # Simple strategy: SHORT at event, cover 48h later
    trades = []
    capital = 100000
    position_size = 0.003 * capital  # 0.3% per trade ($300)

    for _, row in event_df.iterrows():
        entry_price = row['price_at_event']
        ret_48h = row['ret_48h_post']

        # SHORT: profit = -return * position_size
        pnl = -ret_48h * position_size
        trades.append({
            'token': row['token'],
            'date': row['event_date'],
            'entry_price': entry_price,
            'return_48h': ret_48h,
            'pnl_usd': pnl,
            'cumulative_pnl': 0,  # Will compute below
        })

    trades_df = pd.DataFrame(trades)
    trades_df['cumulative_pnl'] = trades_df['pnl_usd'].cumsum()

    print(f"\n{'='*50}")
    print(f"WALK-FORWARD RESULTS")
    print(f"{'='*50}")
    print(f"  Total trades: {len(trades_df)}")
    print(f"  Win rate: {(trades_df['pnl_usd'] > 0).mean():.1%}")
    print(f"  Total PnL: ${trades_df['pnl_usd'].sum():.2f}")
    print(f"  Avg PnL per trade: ${trades_df['pnl_usd'].mean():.2f}")
    print(f"  Max drawdown: ${trades_df['cumulative_pnl'].min():.2f}")
    print(f"  Final cumulative PnL: ${trades_df['cumulative_pnl'].iloc[-1]:.2f}")
    print(f"  Sharpe (annualized): {trades_df['pnl_usd'].mean() / trades_df['pnl_usd'].std() * np.sqrt(12):.2f}" if trades_df['pnl_usd'].std() > 0 else "  Sharpe: N/A")

    # Per-token breakdown
    print(f"\n  Per-token:")
    for token in trades_df['token'].unique():
        tdf = trades_df[trades_df['token'] == token]
        print(f"    {token}: {len(tdf)} trades, WR={( tdf['pnl_usd'] > 0).mean():.0%}, PnL=${tdf['pnl_usd'].sum():.2f}")

    trades_df.to_csv(CACHE_DIR / "walkforward_trades.csv", index=False)
    log.info(f"Walk-forward trades saved to {CACHE_DIR / 'walkforward_trades.csv'}")

    return trades_df


# ============================================================
# MAIN
# ============================================================

def main():
    log.info("=" * 60)
    log.info("X7 TOKEN UNLOCK EVENT STUDY")
    log.info("=" * 60)
    log.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"Tokens: {list(TOKENS.keys())}")
    log.info(f"Cache: {CACHE_DIR}")

    # Initialize Dune client
    client = init_dune_client()

    # Step 1: Search Dune for tables
    table_results = search_dune_tables(client)

    # Step 2: Query on-chain data
    # Try direct SQL queries for EVM tokens
    arb_transfers = query_arb_foundation_transfers(client)
    op_transfers = query_op_foundation_transfers(client)

    # Try community queries
    community_data = query_multi_token_unlocks(client)

    # Step 2b: CEX inflows
    arb_cex = query_cex_inflows(client, "ARB")
    op_cex = query_cex_inflows(client, "OP")

    # Step 3: Load known unlock schedule (fallback / ground truth)
    unlock_schedule = fetch_known_unlock_schedule()

    # If we got on-chain data, merge it with known schedule
    if not arb_transfers.empty:
        log.info(f"\nOn-chain ARB transfers: {len(arb_transfers)} large transfers found")
    if not op_transfers.empty:
        log.info(f"On-chain OP transfers: {len(op_transfers)} large transfers found")

    # Step 5: Event study using known schedule + candle data
    event_df = run_event_study(unlock_schedule)

    if event_df.empty:
        log.error("\nFATAL: No event returns computed. Cannot proceed with analysis.")
        log.error("Likely cause: no candle data available for target tokens.")
        log.error("Solution: ensure parquet files exist or Bybit API is accessible.")
        return

    # Step 6: Statistical analysis
    summary = analyze_results(event_df)

    # Step 7: Walk-forward (if signal looks viable)
    # Proceed regardless to show full picture
    walk_forward_backtest(event_df)

    # Final verdict
    print(f"\n{'='*60}")
    print(f"FINAL VERDICT")
    print(f"{'='*60}")

    if summary:
        wr_48h = summary.get('short_win_rate_48h', 0)
        mean_ret = summary.get('mean_ret_48h_post', 0)

        if wr_48h > 0.55 and mean_ret < -0.005:
            print("  SIGNAL: VIABLE — consistent negative post-unlock returns")
            print("  NEXT: Implement as HB-native controller with on-chain trigger")
        elif wr_48h > 0.50 and mean_ret < 0:
            print("  SIGNAL: MARGINAL — slight edge but may not survive fees/slippage")
            print("  NEXT: Need larger sample or more precise event timing")
        else:
            print("  SIGNAL: NOT VIABLE — no consistent post-unlock sell pressure")
            print("  HYPOTHESIS REJECTED: unlocks are already priced in")

        print(f"\n  Key metrics:")
        print(f"    48h post-unlock mean return: {mean_ret*100:.3f}%")
        print(f"    48h SHORT win rate: {wr_48h:.1%}")
        print(f"    N events: {summary['n_events']}")


if __name__ == "__main__":
    main()
