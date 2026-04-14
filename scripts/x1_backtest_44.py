"""
X1 equalized-exit backtest (4% SL / 4% TP).
Re-runs the X1 backtest with symmetric exits on pairs that had trades in the baseline.
Stores results in pair_historical with engine="X1_44" for comparison.

Usage:
    /Users/hermes/miniforge3/envs/quants-lab/bin/python -u scripts/x1_backtest_eq.py
"""
import asyncio
import gc
import sys
from datetime import datetime, timezone
from decimal import Decimal


def log(msg):
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
ENGINE_TAG = "X1_44"  # Tag for equalized results
BACKTEST_DAYS = 365
TRADE_COST = 0.000375

# Equalized exits
SL_PCT = 0.04
TP_PCT = 0.04


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
    """Run backtest for a single pair with equalized exits."""
    feed_key_1h = f"{CONNECTOR}_{pair}_1h"

    if feed_key_1h not in shared_candles:
        return None

    df_orig = shared_candles[feed_key_1h]
    df = await merge_derivatives(db, df_orig.copy(), pair)

    pair_candles = dict(shared_candles)
    pair_candles[feed_key_1h] = df

    bt_engine = BacktestingEngine(load_cached_data=False)
    bt_engine._bt_engine.backtesting_data_provider.candles_feeds = pair_candles

    # Override exits to equalized 3%/3%
    config = build_backtest_config(ENGINE, CONNECTOR, pair, sl_pct=SL_PCT, tp_pct=TP_PCT)

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
    """Store results in pair_historical with ENGINE_TAG."""
    trades = results.get("total_executors", 0)
    pf = results.get("profit_factor", 0) or 0
    wr = results.get("accuracy", 0) * 100
    sharpe = results.get("sharpe_ratio", 0) or 0
    max_dd = results.get("max_drawdown_pct", 0) or 0
    pnl = results.get("net_pnl_quote", 0) or 0
    close_types = results.get("close_types", {})

    if trades < 10:
        verdict = "BLOCK"
    elif pf >= 1.2 and sharpe > 0:
        verdict = "ALLOW"
    elif pf >= 1.0:
        verdict = "WATCH"
    else:
        verdict = "BLOCK"

    doc = {
        "engine": ENGINE_TAG,
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
        "exit_params": {"sl_pct": SL_PCT, "tp_pct": TP_PCT},
        "updated_at": datetime.now(timezone.utc),
    }

    await db["pair_historical"].update_one(
        {"engine": ENGINE_TAG, "pair": pair},
        {"$set": doc},
        upsert=True,
    )
    return verdict


async def main():
    log(f"X1 Equalized-Exit Backtest -- {ENGINE_TAG}, SL={SL_PCT*100:.0f}%/TP={TP_PCT*100:.0f}%, {BACKTEST_DAYS}d, 1m resolution")

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    # Get pairs that had >= 10 trades in the baseline X1 run
    baseline_docs = []
    async for d in db["pair_historical"].find({"engine": "X1", "total_trades": {"$gte": 10}}):
        baseline_docs.append(d)
    pairs = sorted([d["pair"] for d in baseline_docs])
    log(f"Found {len(pairs)} pairs with >= 10 trades in X1 baseline")

    if not pairs:
        log("ERROR: No baseline X1 results found. Run x1_backtest.py first.")
        return

    # Clean old equalized results
    await db["pair_historical"].delete_many({"engine": ENGINE_TAG})

    # Load parquet cache
    log("Loading parquet cache...")
    _shared = BacktestingEngine(load_cached_data=True)
    shared_candles = _shared._bt_engine.backtesting_data_provider.candles_feeds.copy()
    del _shared
    gc.collect()
    log(f"Loaded {len(shared_candles)} candle feeds")

    # Compute backtest period (same as baseline)
    end_ts = max(
        int(shared_candles[f"{CONNECTOR}_{p}_1h"]["timestamp"].max())
        for p in pairs if f"{CONNECTOR}_{p}_1h" in shared_candles
    )
    start_ts = end_ts - BACKTEST_DAYS * 86400
    period_label = (
        f"{datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d')}"
        f"_{datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')}"
    )
    run_id = f"{ENGINE_TAG}_{period_label}_{end_ts}"
    log(f"Period: {period_label}, {len(pairs)} pairs")

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

        results_summary.append({
            "pair": pair, "trades": trades, "pf": pf, "wr": wr, "pnl": pnl, "verdict": verdict
        })
        log(f"  [{i+1}/{len(pairs)}] {pair:15s} {trades:3d}t PF={pf:.2f} WR={wr:.0f}% ${pnl:>7.2f} -> {verdict}")

    log(f"\nX1_44 Backtest Complete: {len(pairs)} pairs (SL={SL_PCT*100:.0f}%/TP={TP_PCT*100:.0f}%)")
    log(f"  ALLOW={stats['allow']} WATCH={stats['watch']} BLOCK={stats['block']} ERRORS={stats['errors']}")

    profitable = [r for r in results_summary if r["pf"] > 1.0 and r["trades"] >= 10]
    if profitable:
        log(f"\nTop performers (PF > 1.0, trades >= 10):")
        for r in sorted(profitable, key=lambda x: -x["pf"]):
            log(f"  {r['pair']:15s} PF={r['pf']:.2f} WR={r['wr']:.0f}% {r['trades']}t ${r['pnl']:.2f}")

    # Side-by-side comparison with baseline
    log(f"\n{'Pair':15s} {'BL_PF':>6s} {'EQ_PF':>6s} {'Delta':>6s} {'BL_v':>6s} {'EQ_v':>6s}")
    log("-" * 55)
    for r in sorted(results_summary, key=lambda x: -x["pf"]):
        bl = next((d for d in baseline_docs if d["pair"] == r["pair"]), None)
        if bl:
            bl_pf = bl.get("profit_factor", 0)
            delta = r["pf"] - bl_pf
            log(f"  {r['pair']:15s} {bl_pf:6.2f} {r['pf']:6.2f} {delta:+6.2f} {bl['verdict']:>6s} {r['verdict']:>6s}")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
