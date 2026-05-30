#!/usr/bin/env python3
"""
E1 Phase 1.3 — Long/short asymmetry analysis.
Per research-process skill: always split by side before anything else.
"""
import asyncio
import gc
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.backtesting.engine import BacktestingEngine
from app.engines.registry import get_engine, build_backtest_config

CONNECTOR = "bybit_perpetual"
DAYS = 365
TRADE_COST = 0.000375
# Use pairs with enough trades from the bulk run
PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "ADA-USDT", "NEAR-USDT",
         "DOGE-USDT", "DOT-USDT", "LTC-USDT", "LINK-USDT", "XRP-USDT",
         "AVAX-USDT", "SUI-USDT", "APT-USDT", "GALA-USDT", "BLUR-USDT"]


async def main():
    engine_meta = get_engine("E1")
    resolution = engine_meta["backtesting_resolution"]

    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = now_ts - DAYS * 86400

    print(f"E1 Long/Short Asymmetry Analysis")
    print(f"Pairs: {len(PAIRS)} | Days: {DAYS}\n")

    all_executors = []

    for pair in PAIRS:
        try:
            bt = BacktestingEngine(load_cached_data=True)
            config = build_backtest_config(engine_name="E1", connector=CONNECTOR, pair=pair)
            result = await bt.run_backtesting(
                config=config, start=start_ts, end=now_ts,
                backtesting_resolution=resolution, trade_cost=TRADE_COST,
            )
            if not isinstance(result.results.get("close_types"), dict):
                result.results["close_types"] = {}

            edf = result.executors_df
            if len(edf) > 0:
                edf["pair"] = pair
                all_executors.append(edf)
                n_long = result.results.get("total_long", 0)
                n_short = result.results.get("total_short", 0)
                print(f"  {pair:>12s}  L={n_long:>3d}  S={n_short:>3d}  total={len(edf)}")

            del bt, result
            gc.collect()
        except Exception as e:
            print(f"  {pair:>12s}  ERROR: {e}")

    if not all_executors:
        print("No executor data collected!")
        return

    df = pd.concat(all_executors, ignore_index=True)
    print(f"\nTotal executors: {len(df)}")

    # Determine side from the 'side' column
    # In HB: side can be TradeType.BUY (1) or TradeType.SELL (2), or string
    if "side" in df.columns:
        df["side_label"] = df["side"].apply(
            lambda x: "LONG" if str(x) in ["TradeType.BUY", "1", "BUY"] else "SHORT"
        )
    else:
        print("WARNING: no 'side' column found, trying net_pnl_quote sign heuristic")
        df["side_label"] = "UNKNOWN"

    # Close type as string
    df["close_type_str"] = df["close_type"].apply(
        lambda x: x.name if hasattr(x, "name") else str(x)
    )

    # Profitable flag
    df["profitable"] = df["net_pnl_quote"] > 0

    # ── Overall split ──
    print(f"\n{'='*70}")
    print(f"  LONG vs SHORT — Overall")
    print(f"{'='*70}")

    for side in ["LONG", "SHORT"]:
        sub = df[df["side_label"] == side]
        if len(sub) == 0:
            print(f"\n  {side}: 0 trades")
            continue

        n = len(sub)
        wins = sub["profitable"].sum()
        wr = wins / n * 100
        total_pnl = sub["net_pnl_quote"].sum()
        avg_pnl = sub["net_pnl_quote"].mean()
        median_pnl = sub["net_pnl_quote"].median()
        gross_win = sub[sub["profitable"]]["net_pnl_quote"].sum()
        gross_loss = abs(sub[~sub["profitable"]]["net_pnl_quote"].sum())
        pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

        print(f"\n  {side}:")
        print(f"    Trades:     {n}")
        print(f"    Win rate:   {wr:.1f}%")
        print(f"    PF:         {pf:.2f}")
        print(f"    Total PnL:  {total_pnl:+.2f}")
        print(f"    Avg PnL:    {avg_pnl:+.4f}")
        print(f"    Median PnL: {median_pnl:+.4f}")
        print(f"    PnL share:  {total_pnl / max(df['net_pnl_quote'].sum(), 0.01) * 100:.1f}%")

        # Exit type breakdown
        exits = sub["close_type_str"].value_counts()
        print(f"    Exits:      {dict(exits)}")

    # ── Per-pair long/short ──
    print(f"\n{'='*70}")
    print(f"  PER-PAIR Long vs Short")
    print(f"{'='*70}")

    print(f"\n  {'Pair':>12s}  {'L_N':>4s}  {'L_WR':>5s}  {'L_PF':>6s}  {'L_PnL':>8s}  {'S_N':>4s}  {'S_WR':>5s}  {'S_PF':>6s}  {'S_PnL':>8s}")
    print(f"  {'-'*12}  {'-'*4}  {'-'*5}  {'-'*6}  {'-'*8}  {'-'*4}  {'-'*5}  {'-'*6}  {'-'*8}")

    for pair in PAIRS:
        pair_df = df[df["pair"] == pair]
        if len(pair_df) == 0:
            continue

        row_parts = [f"  {pair:>12s}"]
        for side in ["LONG", "SHORT"]:
            sub = pair_df[pair_df["side_label"] == side]
            n = len(sub)
            if n == 0:
                row_parts.append(f"  {0:>4d}  {'—':>5s}  {'—':>6s}  {'—':>8s}")
                continue
            wins = sub["profitable"].sum()
            wr = wins / n * 100
            gw = sub[sub["profitable"]]["net_pnl_quote"].sum()
            gl = abs(sub[~sub["profitable"]]["net_pnl_quote"].sum())
            pf = gw / gl if gl > 0 else float("inf")
            pnl = sub["net_pnl_quote"].sum()
            row_parts.append(f"  {n:>4d}  {wr:>5.1f}  {pf:>6.2f}  {pnl:>+8.2f}")

        print("".join(row_parts))

    # ── Distribution analysis ──
    print(f"\n{'='*70}")
    print(f"  PnL DISTRIBUTION")
    print(f"{'='*70}")

    for side in ["LONG", "SHORT"]:
        sub = df[df["side_label"] == side]
        if len(sub) == 0:
            continue
        pnl = sub["net_pnl_quote"]
        print(f"\n  {side} ({len(sub)} trades):")
        print(f"    Mean:   {pnl.mean():+.4f}")
        print(f"    Median: {pnl.median():+.4f}")
        print(f"    Std:    {pnl.std():.4f}")
        print(f"    Skew:   {pnl.skew():.2f}")
        print(f"    Min:    {pnl.min():+.4f}")
        print(f"    Max:    {pnl.max():+.4f}")
        print(f"    Q25:    {pnl.quantile(0.25):+.4f}")
        print(f"    Q75:    {pnl.quantile(0.75):+.4f}")

    # ── Recommendation ──
    print(f"\n{'='*70}")
    print(f"  RECOMMENDATION")
    print(f"{'='*70}")

    longs = df[df["side_label"] == "LONG"]
    shorts = df[df["side_label"] == "SHORT"]

    l_pnl = longs["net_pnl_quote"].sum() if len(longs) > 0 else 0
    s_pnl = shorts["net_pnl_quote"].sum() if len(shorts) > 0 else 0
    l_pf = (longs[longs["profitable"]]["net_pnl_quote"].sum() /
            abs(longs[~longs["profitable"]]["net_pnl_quote"].sum())) if len(longs) > 0 and (longs["net_pnl_quote"] < 0).any() else float("inf")
    s_pf = (shorts[shorts["profitable"]]["net_pnl_quote"].sum() /
            abs(shorts[~shorts["profitable"]]["net_pnl_quote"].sum())) if len(shorts) > 0 and (shorts["net_pnl_quote"] < 0).any() else float("inf")

    if len(shorts) > 0 and s_pf < 1.0:
        print(f"\n  WARNING: Short side PF < 1.0 ({s_pf:.2f})")
        print(f"  Consider: LONG_ONLY or EMA gate for shorts")
    elif len(shorts) > 0 and s_pf < l_pf * 0.5:
        print(f"\n  NOTE: Short PF ({s_pf:.2f}) is less than half of Long PF ({l_pf:.2f})")
        print(f"  Shorts are dragging — consider EMA gate before Optuna")
    else:
        print(f"\n  Both sides contributing positively.")
        print(f"  Long PF={l_pf:.2f} ({l_pnl:+.2f}), Short PF={s_pf:.2f} ({s_pnl:+.2f})")
        print(f"  No directional filter needed.")


if __name__ == "__main__":
    asyncio.run(main())
