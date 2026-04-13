"""
E3 Filter A/B Test — measure lift from each filter independently.

Runs E3 bulk backtest on target pairs with different filter configs:
  - Baseline (no filters)
  - OI filter only
  - BTC regime filter only
  - Volatility filter only
  - Time-of-day filter (Asia session 0-8 UTC excluded)
  - All winners stacked

Reports PF, WR, trades per pair per config.
"""
import asyncio
import gc
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

# Setup path
sys.path.insert(0, "/Users/hermes/quants-lab")

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from app.engines.strategy_registry import get_strategy, get_config_class

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("e3_ab_test")
logger.setLevel(logging.INFO)

# Target pairs (best E3 candidates from prior run)
TARGET_PAIRS = ["XRP-USDT", "SUI-USDT", "ADA-USDT"]
CONNECTOR = "bybit_perpetual"
BACKTEST_DAYS = 365
TRADE_COST = 0.000375

# Filter configs to test
FILTER_CONFIGS = {
    "baseline": {},
    "oi_only": {
        "oi_filter_enabled": True,
        "oi_filter_min_pct": 0.20,
        "oi_filter_window": 168,
    },
    "btc_regime": {
        "btc_regime_enabled": True,
        "btc_regime_threshold": 0.0,  # any opposite BTC move blocks
    },
    "btc_regime_0.5": {
        "btc_regime_enabled": True,
        "btc_regime_threshold": 0.5,  # only strong opposite BTC move blocks
    },
    "vol_filter": {
        "vol_filter_enabled": True,
        "vol_filter_max_pct": 0.95,
        "vol_filter_window": 168,
    },
    "tod_no_asia": {
        "tod_filter_enabled": True,
        "tod_allowed_hours": list(range(8, 24)),  # skip 0-7 UTC (Asia quiet hours)
    },
    "tod_london_ny": {
        "tod_filter_enabled": True,
        "tod_allowed_hours": list(range(7, 22)),  # London open to NY close
    },
}


async def run_single_backtest(shared_candles, pair, config_overrides, start_ts, end_ts):
    """Run a single E3 backtest with given config overrides."""
    from hummingbot.core.data_type.common import OrderType

    meta = get_strategy("E3")
    ConfigClass = get_config_class("E3")

    config_kwargs = {
        "id": f"e3_ab_{pair}",
        "connector_name": CONNECTOR,
        "trading_pair": pair,
        "total_amount_quote": Decimal("100"),
        "max_executors_per_side": 1,
        "cooldown_time": 60,
        "leverage": 1,
    }
    config_kwargs.update(meta.exit_params)
    config_kwargs.update(config_overrides)

    config = ConfigClass(**config_kwargs)

    bt_engine = BacktestingEngine(load_cached_data=False)
    bt_engine._bt_engine.backtesting_data_provider.candles_feeds = shared_candles

    bt_result = await bt_engine.run_backtesting(
        config=config,
        start=start_ts,
        end=end_ts,
        backtesting_resolution="1m",
        trade_cost=TRADE_COST,
    )

    if not isinstance(bt_result.results.get("close_types"), dict):
        bt_result.results["close_types"] = {}

    r = bt_result.results
    result = {
        "trades": r.get("total_executors", 0),
        "pf": r.get("profit_factor", 0) or 0,
        "wr": (r.get("accuracy_long", 0) or 0) * 100,
        "sharpe": r.get("sharpe_ratio", 0) or 0,
        "max_dd": r.get("max_drawdown_pct", 0) or 0,
        "pnl": r.get("net_pnl_quote", 0) or 0,
    }

    del bt_engine, bt_result
    gc.collect()
    return result


async def merge_derivatives(shared_candles, pairs, db):
    """Merge derivatives + sentiment data into candles (replicates BulkBacktestTask logic)."""
    for pair in pairs:
        feed_key = f"{CONNECTOR}_{pair}_1h"
        if feed_key not in shared_candles:
            continue
        df = shared_candles[feed_key]
        if df is None or len(df) == 0:
            continue

        candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

        # Funding rates
        funding_docs = list(db["bybit_funding_rates"].find({"pair": pair}).sort("timestamp_utc", 1))
        if funding_docs:
            fdf = pd.DataFrame(funding_docs)
            if "timestamp_utc" in fdf.columns and "funding_rate" in fdf.columns:
                fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
                fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()
                fdf = fdf[~fdf.index.duplicated(keep="last")]
                fdf_r = fdf.reindex(candle_idx, method="ffill")
                df["funding_rate"] = fdf_r["funding_rate"].fillna(0.0).values
        else:
            df["funding_rate"] = 0.0

        # OI
        oi_docs = list(db["bybit_open_interest"].find({"pair": pair}).sort("timestamp_utc", 1))
        if oi_docs:
            odf = pd.DataFrame(oi_docs)
            if "timestamp_utc" in odf.columns and "oi_value" in odf.columns:
                odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
                odf = odf.set_index("ts")[["oi_value"]].sort_index()
                odf = odf[~odf.index.duplicated(keep="last")]
                odf_r = odf.reindex(candle_idx, method="ffill")
                df["oi_value"] = odf_r["oi_value"].bfill().fillna(0.0).values
        else:
            df["oi_value"] = 0.0

        # LS ratio
        ls_docs = list(db["bybit_ls_ratio"].find({"pair": pair}).sort("timestamp_utc", 1))
        if ls_docs:
            ldf = pd.DataFrame(ls_docs)
            if "timestamp_utc" in ldf.columns and "buy_ratio" in ldf.columns:
                ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
                ldf = ldf.set_index("ts")[["buy_ratio"]].sort_index()
                ldf = ldf[~ldf.index.duplicated(keep="last")]
                ldf_r = ldf.reindex(candle_idx, method="ffill")
                df["buy_ratio"] = ldf_r["buy_ratio"].fillna(0.5).values
        else:
            df["buy_ratio"] = 0.5

        shared_candles[feed_key] = df
        logger.info(f"  Derivatives merged for {pair}")

    # BTC regime
    btc_key = f"{CONNECTOR}_BTC-USDT_1h"
    btc_df = shared_candles.get(btc_key)
    regime_df = None
    if btc_df is not None and len(btc_df) > 200:
        btc_idx = pd.to_datetime(btc_df["timestamp"], unit="s", utc=True)
        close_s = pd.Series(btc_df["close"].values, index=btc_idx)
        ret_4h = (close_s / close_s.shift(4) - 1) * 100
        regime_df = pd.DataFrame({"btc_return_4h": ret_4h}).dropna()

    # Fear & Greed
    fg_docs = list(db["fear_greed_index"].find({}, {"timestamp_utc": 1, "value": 1, "_id": 0}).sort("timestamp_utc", 1))
    fg_df = None
    if fg_docs:
        fgd = pd.DataFrame(fg_docs)
        if "timestamp_utc" in fgd.columns and "value" in fgd.columns:
            fgd["ts"] = pd.to_datetime(fgd["timestamp_utc"], unit="ms", utc=True)
            fg_df = fgd.set_index("ts")[["value"]].rename(columns={"value": "fear_greed_value"}).sort_index()
            fg_df = fg_df[~fg_df.index.duplicated(keep="last")]

    for pair in pairs:
        feed_key = f"{CONNECTOR}_{pair}_1h"
        df = shared_candles.get(feed_key)
        if df is None or len(df) == 0:
            continue
        candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

        if fg_df is not None:
            fg_r = fg_df.reindex(candle_idx, method="ffill")
            df["fear_greed_value"] = fg_r["fear_greed_value"].fillna(50.0).values
        else:
            df["fear_greed_value"] = 50.0

        if regime_df is not None:
            reg_r = regime_df.reindex(candle_idx, method="ffill")
            df["btc_return_4h"] = reg_r["btc_return_4h"].fillna(0.0).values
        else:
            df["btc_return_4h"] = 0.0

        shared_candles[feed_key] = df


async def main():
    import os
    from pymongo import MongoClient

    db = MongoClient(os.environ["MONGO_URI"])[os.environ["MONGO_DATABASE"]]

    logger.info("Loading parquet cache...")
    _cache = BacktestingEngine(load_cached_data=True)
    shared_candles = _cache._bt_engine.backtesting_data_provider.candles_feeds.copy()
    del _cache
    gc.collect()

    # Determine time window
    end_ts = 0
    for pair in TARGET_PAIRS[:3]:
        f = data_paths.candles_dir / f"{CONNECTOR}|{pair}|1m.parquet"
        if f.exists():
            df = pd.read_parquet(f, columns=["timestamp"])
            end_ts = max(end_ts, int(df["timestamp"].max()))
    start_ts = end_ts - BACKTEST_DAYS * 86400
    logger.info(f"Window: {datetime.fromtimestamp(start_ts, tz=timezone.utc)} to {datetime.fromtimestamp(end_ts, tz=timezone.utc)}")

    # Merge derivatives
    logger.info("Merging derivatives data...")
    await merge_derivatives(shared_candles, TARGET_PAIRS, db)

    # Run A/B tests
    all_results = {}
    for config_name, overrides in FILTER_CONFIGS.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"Config: {config_name}")
        logger.info(f"{'='*60}")
        config_results = {}
        for pair in TARGET_PAIRS:
            feed_key = f"{CONNECTOR}_{pair}_1h"
            if feed_key not in shared_candles:
                logger.warning(f"  {pair}: no data, skipping")
                continue
            try:
                result = await run_single_backtest(shared_candles, pair, overrides, start_ts, end_ts)
                config_results[pair] = result
                logger.info(f"  {pair}: PF={result['pf']:.2f} WR={result['wr']:.1f}% trades={result['trades']} sharpe={result['sharpe']:.2f}")
            except Exception as e:
                logger.error(f"  {pair}: FAILED -- {e}")
        all_results[config_name] = config_results

    # Print comparison table
    print("\n" + "=" * 100)
    print("E3 FILTER A/B TEST RESULTS")
    print("=" * 100)

    header = f"{'Pair':<15}"
    for config_name in FILTER_CONFIGS:
        header += f" | {config_name:>15}"
    print(header)
    print("-" * len(header))

    for pair in TARGET_PAIRS:
        row_pf = f"{pair:<15}"
        row_trades = f"{'  trades':<15}"
        for config_name in FILTER_CONFIGS:
            r = all_results.get(config_name, {}).get(pair)
            if r:
                pf = r["pf"]
                trades = r["trades"]
                # Highlight improvements over baseline
                base_pf = all_results.get("baseline", {}).get(pair, {}).get("pf", 0)
                delta = pf - base_pf if base_pf else 0
                marker = "+" if delta > 0.02 else ("-" if delta < -0.02 else " ")
                row_pf += f" | {pf:>6.2f}{marker}({delta:+.2f})"
                row_trades += f" | {trades:>15}"
            else:
                row_pf += f" | {'N/A':>15}"
                row_trades += f" | {'N/A':>15}"
        print(row_pf)
        print(row_trades)
        print()

    # Summary: best config per pair
    print("\nBEST CONFIG PER PAIR (highest PF with >= 30 trades):")
    print("-" * 60)
    for pair in TARGET_PAIRS:
        best_name = None
        best_pf = 0
        for config_name in FILTER_CONFIGS:
            r = all_results.get(config_name, {}).get(pair)
            if r and r["trades"] >= 30 and r["pf"] > best_pf:
                best_pf = r["pf"]
                best_name = config_name
        if best_name:
            base_pf = all_results.get("baseline", {}).get(pair, {}).get("pf", 0)
            print(f"  {pair:<15} -> {best_name:<20} PF={best_pf:.2f} (base: {base_pf:.2f}, delta: {best_pf-base_pf:+.2f})")


if __name__ == "__main__":
    asyncio.run(main())
