"""
Backtest exit strategy comparison for E1 Compression Breakout.

Compares three exit modes side-by-side:
  1. range  — TP = 1× range_width, SL = breakout level (R:R ≥ 1.0 gate)
  2. atr    — TP = 1.5× ATR, SL = 1.0× ATR (legacy)
  3. fixed  — TP = 3%, SL = 1.5% (registry defaults)

Usage:
  MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
    python scripts/backtest_exit_comparison.py [--pairs BTC-USDT,ETH-USDT] [--days 365]
"""
import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.backtesting.engine import BacktestingEngine
from app.engines.strategy_registry import get_strategy, get_config_class, _build_candles_config
from decimal import Decimal


def build_config(exit_mode: str, connector: str, pair: str, min_rr: float = 1.0):
    """Build E1 backtest config with specified exit_mode."""
    meta = get_strategy("E1")
    ConfigClass = get_config_class("E1")
    candles = _build_candles_config(connector, pair, meta.intervals)

    config_kwargs = {
        "id": f"e1_{exit_mode}_{pair}",
        "connector_name": connector,
        "trading_pair": pair,
        "total_amount_quote": Decimal("100"),
        "max_executors_per_side": 1,
        "cooldown_time": 60,
        "leverage": 1,
        "candles_config": candles,
        "exit_mode": exit_mode,
        "min_rr": min_rr,
    }

    config_kwargs.update(meta.exit_params)

    from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop
    from hummingbot.core.data_type.common import OrderType
    if meta.trailing_stop:
        config_kwargs["trailing_stop"] = TrailingStop(**meta.trailing_stop)
        config_kwargs["take_profit_order_type"] = OrderType.MARKET

    return ConfigClass(**config_kwargs)


async def run_comparison(pairs: list, days: int, connector: str):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = now_ts - days * 86400

    modes = ["range", "atr", "fixed"]
    results = {}

    for pair in pairs:
        results[pair] = {}
        for mode in modes:
            print(f"  Running {pair} / {mode}...", end=" ", flush=True)
            try:
                config = build_config(mode, connector, pair)
                engine = BacktestingEngine(load_cached_data=True)
                bt_result = await engine.run_backtesting(
                    config=config,
                    start=start_ts,
                    end=now_ts,
                    backtesting_resolution="1m",
                    trade_cost=0.000375,
                )
                r = bt_result.results
                results[pair][mode] = {
                    "trades": r.get("total_executors", 0),
                    "pf": r.get("profit_factor", 0) or 0,
                    "wr": (r.get("accuracy_long", 0) or 0) * 100,
                    "sharpe": r.get("sharpe_ratio", 0) or 0,
                    "max_dd": (r.get("max_drawdown_pct", 0) or 0) * 100,
                    "pnl": r.get("net_pnl_quote", 0) or 0,
                    "close_types": {str(k): v for k, v in r.get("close_types", {}).items()},
                }
                print(f"OK ({results[pair][mode]['trades']} trades)")
                del engine, bt_result
            except Exception as e:
                print(f"FAILED: {e}")
                results[pair][mode] = {"error": str(e)}

    # Print comparison table
    print("\n" + "=" * 90)
    print(f"{'Pair':<14} {'Mode':<8} {'Trades':>7} {'PF':>7} {'WR%':>7} {'Sharpe':>8} {'MaxDD%':>8} {'PnL':>10}")
    print("-" * 90)

    for pair in pairs:
        for mode in modes:
            r = results[pair].get(mode, {})
            if "error" in r:
                print(f"{pair:<14} {mode:<8} {'ERROR':>7} {r['error'][:50]}")
            else:
                print(
                    f"{pair:<14} {mode:<8} {r['trades']:>7} {r['pf']:>7.2f} "
                    f"{r['wr']:>6.1f}% {r['sharpe']:>8.2f} {r['max_dd']:>7.1f}% "
                    f"${r['pnl']:>9.2f}"
                )
                ct = r.get("close_types", {})
                if ct:
                    parts = [f"{k}={v}" for k, v in ct.items()]
                    print(f"{'':>22} Close types: {', '.join(parts)}")
        print()

    print("=" * 90)


def main():
    parser = argparse.ArgumentParser(description="E1 exit strategy comparison")
    parser.add_argument("--pairs", default="BTC-USDT,ETH-USDT,SOL-USDT,BCH-USDT",
                        help="Comma-separated pairs")
    parser.add_argument("--days", type=int, default=365, help="Lookback days")
    parser.add_argument("--connector", default="bybit_perpetual", help="Connector name")
    args = parser.parse_args()

    pairs = [p.strip() for p in args.pairs.split(",")]
    print(f"E1 Exit Strategy Comparison — {len(pairs)} pairs, {args.days}d lookback\n")
    asyncio.run(run_comparison(pairs, args.days, args.connector))


if __name__ == "__main__":
    main()
