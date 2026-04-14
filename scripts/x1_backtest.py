"""
X1 standalone backtest script.
Runs the X1 bulk backtest with manual derivatives merge, bypassing BulkBacktestTask.
This avoids the known issue where run_backtesting() strips derivative columns.

Usage:
    MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/x1_backtest.py
"""
import asyncio
import gc
import sys
from datetime import datetime, timezone
from decimal import Decimal

def log(msg):
    """Print with flush for reliable output in background/redirect."""
    print(msg, flush=True)

log(">>> imports starting")

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient

from core.backtesting.engine import BacktestingEngine
from app.engines.strategy_registry import build_backtest_config

log(">>> imports done")

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "quants_lab"
CONNECTOR = "bybit_perpetual"
ENGINE = "X1"
BACKTEST_DAYS = 365
TRADE_COST = 0.000375


async def merge_derivatives(db, df: pd.DataFrame, pair: str) -> pd.DataFrame:
    """Merge funding, OI, and sentiment data into 1h candle DataFrame."""
    candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    # Bybit funding
    cursor = db["bybit_funding_rates"].find({"pair": pair}).sort("timestamp_utc", 1)
    docs = await cursor.to_list(length=100000)
    if docs:
        fdf = pd.DataFrame(docs)
        fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
        fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()
        fdf = fdf[~fdf.index.duplicated(keep="last")]
        df["funding_rate"] = fdf.reindex(candle_idx, method="ffill")["funding_rate"].fillna(0.0).values
    else:
        df["funding_rate"] = 0.0

    # Binance funding
    cursor = db["binance_funding_rates"].find({"pair": pair}).sort("timestamp_utc", 1)
    docs = await cursor.to_list(length=100000)
    if docs:
        bdf = pd.DataFrame(docs)
        bdf["ts"] = pd.to_datetime(bdf["timestamp_utc"], unit="ms", utc=True)
        bdf = bdf.set_index("ts")[["funding_rate"]].sort_index()
        bdf = bdf.rename(columns={"funding_rate": "binance_funding_rate"})
        bdf = bdf[~bdf.index.duplicated(keep="last")]
        df["binance_funding_rate"] = bdf.reindex(candle_idx, method="ffill")["binance_funding_rate"].fillna(0.0).values
    else:
        df["binance_funding_rate"] = 0.0

    # OI
    cursor = db["bybit_open_interest"].find({"pair": pair}).sort("timestamp_utc", 1)
    docs = await cursor.to_list(length=100000)
    if docs:
        odf = pd.DataFrame(docs)
        odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
        odf = odf.set_index("ts")[["oi_value"]].sort_index()
        odf = odf[~odf.index.duplicated(keep="last")]
        df["oi_value"] = odf.reindex(candle_idx, method="ffill")["oi_value"].bfill().fillna(0.0).values
    else:
        df["oi_value"] = 0.0

    # LS ratio
    cursor = db["bybit_ls_ratio"].find({"pair": pair}).sort("timestamp_utc", 1)
    docs = await cursor.to_list(length=100000)
    if docs:
        ldf = pd.DataFrame(docs)
        ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
        ldf = ldf.set_index("ts")[["buy_ratio"]].sort_index()
        ldf = ldf[~ldf.index.duplicated(keep="last")]
        df["buy_ratio"] = ldf.reindex(candle_idx, method="ffill")["buy_ratio"].fillna(0.5).values
    else:
        df["buy_ratio"] = 0.5

    # BTC regime
    df["btc_return_4h"] = 0.0

    # Fear & Greed
    cursor = db["fear_greed_index"].find().sort("timestamp", 1)
    docs = await cursor.to_list(length=10000)
    if docs:
        fgdf = pd.DataFrame(docs)
        if "timestamp" in fgdf.columns and "value" in fgdf.columns:
            fgdf["ts"] = pd.to_datetime(fgdf["timestamp"], unit="s", utc=True)
            fgdf = fgdf.set_index("ts")[["value"]].rename(columns={"value": "fear_greed_value"}).sort_index()
            fgdf = fgdf[~fgdf.index.duplicated(keep="last")]
            df["fear_greed_value"] = fgdf.reindex(candle_idx, method="ffill")["fear_greed_value"].fillna(50.0).values
        else:
            df["fear_greed_value"] = 50.0
    else:
        df["fear_greed_value"] = 50.0

    return df


async def run_backtest(db, shared_candles, pair, start_ts, end_ts):
    """Run backtest for a single pair with derivatives merged."""
    feed_key_1h = f"{CONNECTOR}_{pair}_1h"

    if feed_key_1h not in shared_candles:
        return None

    # Deep copy the 1h candles for this pair (run_backtesting mutates them)
    df_orig = shared_candles[feed_key_1h]
    df = await merge_derivatives(db, df_orig.copy(), pair)

    # Create isolated candles dict for this pair
    pair_candles = dict(shared_candles)  # shallow copy of dict
    pair_candles[feed_key_1h] = df  # put our merged copy

    bt_engine = BacktestingEngine(load_cached_data=False)
    bt_engine._bt_engine.backtesting_data_provider.candles_feeds = pair_candles

    config = build_backtest_config(ENGINE, CONNECTOR, pair)

    try:
        result = await bt_engine.run_backtesting(
            config=config,
            start=start_ts,
            end=end_ts,
            backtesting_resolution="1m",
            trade_cost=TRADE_COST,
        )
        return result.results
    except Exception as e:
        log(f"  {pair}: backtest failed: {e}")
        import traceback
        traceback.print_exc()
        return None


async def store_results(db, pair, results, period_label, run_id):
    """Store results in pair_historical and compute verdict."""
    trades = results.get("total_executors", 0)
    pf = results.get("profit_factor", 0) or 0
    wr = results.get("accuracy", 0) * 100
    sharpe = results.get("sharpe_ratio", 0) or 0
    max_dd = results.get("max_drawdown_pct", 0) or 0
    pnl = results.get("net_pnl_quote", 0) or 0
    close_types = results.get("close_types", {})

    # Verdict logic — aligned with BulkBacktestTask governance gates
    # NOTE: prefer running via BulkBacktestTask for full quality metrics.
    # This is a simplified version for quick standalone runs.
    MIN_TRADES = 30
    if trades < MIN_TRADES:
        verdict = "BLOCK"
    elif pf >= 1.3 and sharpe >= 1.0 and max_dd >= -0.15:
        verdict = "ALLOW"
    elif pf >= 1.0 and sharpe >= 0.0 and max_dd >= -0.20:
        verdict = "WATCH"
    else:
        verdict = "BLOCK"

    doc = {
        "engine": ENGINE,
        "pair": pair,
        "period": period_label,
        "run_id": run_id,
        "total_trades": trades,
        "profit_factor": round(pf, 4),
        "win_rate": round(wr, 1),
        "sharpe_ratio": round(sharpe, 4) if sharpe else 0,
        "max_drawdown_pct": round(max_dd, 4) if max_dd else 0,
        "net_pnl_quote": round(pnl, 2),
        "close_types": close_types,
        "verdict": verdict,
        "backtesting_resolution": "1m",
        "trade_cost": TRADE_COST,
        "updated_at": datetime.now(timezone.utc),
    }

    await db["pair_historical"].update_one(
        {"engine": ENGINE, "pair": pair},
        {"$set": doc},
        upsert=True,
    )
    return verdict


async def main():
    log(f"X1 Standalone Backtest -- {ENGINE}, {BACKTEST_DAYS}d, 1m resolution")

    # Connect to MongoDB
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    # Clean old results
    await db["pair_historical"].delete_many({"engine": ENGINE})
    await db["backtest_trades"].delete_many({"engine": ENGINE})

    # Load parquet cache
    log("Loading parquet cache...")
    _shared = BacktestingEngine(load_cached_data=True)
    shared_candles = _shared._bt_engine.backtesting_data_provider.candles_feeds.copy()
    del _shared
    gc.collect()
    log(f"Loaded {len(shared_candles)} candle feeds")

    # Discover pairs with 1h data
    pairs_1h = sorted(set(
        k.split("_")[2] for k in shared_candles.keys()
        if k.endswith("_1h") and CONNECTOR in k
    ))

    # Filter to pairs that also have Binance funding data
    binance_pairs = set(await db["binance_funding_rates"].distinct("pair"))
    pairs = [p for p in pairs_1h if p in binance_pairs]
    log(f"Found {len(pairs)} pairs with both 1h candles and Binance funding")

    # Compute backtest period
    end_ts = max(
        int(shared_candles[f"{CONNECTOR}_{p}_1h"]["timestamp"].max())
        for p in pairs if f"{CONNECTOR}_{p}_1h" in shared_candles
    )
    start_ts = end_ts - BACKTEST_DAYS * 86400
    period_label = (
        f"{datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d')}"
        f"_{datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')}"
    )
    run_id = f"{ENGINE}_{period_label}_{end_ts}"
    log(f"Period: {period_label}, {len(pairs)} pairs")

    # Run backtests
    stats = {"allow": 0, "watch": 0, "block": 0, "errors": 0}
    results_summary = []

    for i, pair in enumerate(pairs):
        log(f"  [{i+1}/{len(pairs)}] {pair} starting...")
        r = await run_backtest(db, shared_candles, pair, start_ts, end_ts)
        if r is None:
            stats["errors"] += 1
            log(f"  {pair:15s}: ERROR")
            continue

        verdict = await store_results(db, pair, r, period_label, run_id)
        stats[verdict.lower()] += 1

        trades = r.get("total_executors", 0)
        pf = r.get("profit_factor", 0) or 0
        wr = r.get("accuracy", 0) * 100
        pnl = r.get("net_pnl_quote", 0) or 0
        ct = r.get("close_types", {})

        results_summary.append({
            "pair": pair, "trades": trades, "pf": pf, "wr": wr, "pnl": pnl, "verdict": verdict
        })
        log(f"  [{i+1}/{len(pairs)}] {pair:15s} {trades:3d}t PF={pf:.2f} WR={wr:.0f}% ${pnl:>7.2f} -> {verdict}")

    # Summary
    log(f"\nX1 Backtest Complete: {len(pairs)} pairs")
    log(f"  ALLOW={stats['allow']} WATCH={stats['watch']} BLOCK={stats['block']} ERRORS={stats['errors']}")

    # Top performers
    profitable = [r for r in results_summary if r["pf"] > 1.0 and r["trades"] >= 10]
    if profitable:
        log(f"\nTop performers (PF > 1.0, trades >= 10):")
        for r in sorted(profitable, key=lambda x: -x["pf"]):
            log(f"  {r['pair']:15s} PF={r['pf']:.2f} WR={r['wr']:.0f}% {r['trades']}t ${r['pnl']:.2f}")
    else:
        log("\nNo pairs with PF > 1.0 and >= 10 trades")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
