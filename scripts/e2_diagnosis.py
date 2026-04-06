#!/usr/bin/env python3
"""
E2 Range Fade — Backtest Diagnosis

Runs E2 backtests on representative pairs (ALLOW, WATCH, BLOCK) and
outputs trade-level analysis: exit type breakdown, hold times, PnL
distribution, regime performance, and funnel analysis.

Usage:
    MONGO_URI=... python scripts/e2_diagnosis.py
"""
import asyncio
import gc
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np
import pandas as pd

from app.engines.registry import get_engine, build_backtest_config
from core.backtesting.engine import BacktestingEngine


ENGINE = "E2"
CONNECTOR = "bybit_perpetual"
BACKTEST_DAYS = 365
TRADE_COST = 0.000375

# Representative pairs: mix of ALLOW, WATCH, and BLOCK
# Pulled from pair_historical
DIAGNOSIS_PAIRS = [
    # ALLOW (PF > 1.3)
    "PAXG-USDT", "PUMPFUN-USDT", "XAUT-USDT",
    # WATCH (1.0 <= PF < 1.3) — where the edge is thin
    "LTC-USDT", "LINK-USDT", "APT-USDT", "HYPE-USDT", "CRV-USDT",
    "ADA-USDT", "WLD-USDT", "SUI-USDT",
    # BLOCK (PF < 1.0) — where it's losing
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT", "XRP-USDT",
    "AVAX-USDT", "TRUMP-USDT", "BNB-USDT",
]


async def run_diagnosis():
    meta = get_engine(ENGINE)
    resolution = meta["backtesting_resolution"]

    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = now_ts - BACKTEST_DAYS * 86400

    print(f"E2 Diagnosis — {len(DIAGNOSIS_PAIRS)} pairs")
    print(f"Period: {datetime.utcfromtimestamp(start_ts).date()} → {datetime.utcfromtimestamp(now_ts).date()}")
    print(f"Resolution: {resolution}, Trade cost: {TRADE_COST}")
    print("=" * 80)

    all_trades = []
    pair_summaries = []

    for pair in DIAGNOSIS_PAIRS:
        try:
            bt = BacktestingEngine(load_cached_data=True)
            config = build_backtest_config(
                engine_name=ENGINE, connector=CONNECTOR, pair=pair,
            )
            result = await bt.run_backtesting(
                config=config, start=start_ts, end=now_ts,
                backtesting_resolution=resolution, trade_cost=TRADE_COST,
            )

            if not isinstance(result.results.get("close_types"), dict):
                result.results["close_types"] = {}

            r = result.results
            trades = r.get("total_executors", 0)
            pf = r.get("profit_factor", 0) or 0
            wr = (r.get("accuracy_long", 0) or 0) * 100
            pnl = r.get("net_pnl_quote", 0) or 0
            close_types = r.get("close_types", {})

            summary = {
                "pair": pair, "trades": trades, "pf": round(pf, 3),
                "wr": round(wr, 1), "pnl": round(pnl, 2),
                "close_types": close_types,
                "max_dd": r.get("max_drawdown_pct"),
                "sharpe": r.get("sharpe_ratio"),
                "n_long": r.get("total_long", 0),
                "n_short": r.get("total_short", 0),
            }
            pair_summaries.append(summary)

            # Extract trade-level data
            if hasattr(result, "executors_df") and result.executors_df is not None:
                edf = result.executors_df.copy()
                if len(edf) > 0:
                    edf["pair"] = pair
                    all_trades.append(edf)

            print(f"  {pair:20s} trades={trades:3d} PF={pf:.2f} WR={wr:.1f}% "
                  f"PnL={pnl:+.2f} close={close_types}")

            del bt, result
            gc.collect()

        except Exception as e:
            print(f"  {pair:20s} ERROR: {e}")

    print("\n" + "=" * 80)

    # ── Aggregate analysis ───────────────────────────────
    if not all_trades:
        print("No trades found across all pairs. E2 may need structural changes.")
        return

    trades_df = pd.concat(all_trades, ignore_index=True)
    # Convert Decimal columns to float
    for col in trades_df.columns:
        if trades_df[col].dtype == object:
            try:
                trades_df[col] = trades_df[col].astype(float)
            except (ValueError, TypeError):
                pass
    print(f"\nTotal trades across {len(DIAGNOSIS_PAIRS)} pairs: {len(trades_df)}")

    # Available columns
    print(f"\nTrade DataFrame columns: {list(trades_df.columns)}")

    # ── 1. Exit type breakdown ───────────────────────────
    print("\n── 1. EXIT TYPE BREAKDOWN ──")
    if "close_type" in trades_df.columns:
        ct = trades_df["close_type"].value_counts()
        for k, v in ct.items():
            subset = trades_df[trades_df["close_type"] == k]
            if "net_pnl_quote" in subset.columns:
                avg_pnl = subset["net_pnl_quote"].mean()
                win_rate = (subset["net_pnl_quote"] > 0).mean() * 100
                print(f"  {str(k):25s} count={v:4d} ({v/len(trades_df)*100:.1f}%) "
                      f"avg_pnl={avg_pnl:+.3f} WR={win_rate:.1f}%")
            else:
                print(f"  {str(k):25s} count={v:4d} ({v/len(trades_df)*100:.1f}%)")

    # ── 2. PnL distribution ──────────────────────────────
    print("\n── 2. PnL DISTRIBUTION ──")
    if "net_pnl_quote" in trades_df.columns:
        pnl_col = trades_df["net_pnl_quote"]
        print(f"  Mean:   {pnl_col.mean():+.4f}")
        print(f"  Median: {pnl_col.median():+.4f}")
        print(f"  Std:    {pnl_col.std():.4f}")
        print(f"  Min:    {pnl_col.min():+.4f}")
        print(f"  Max:    {pnl_col.max():+.4f}")
        print(f"  Skew:   {pnl_col.skew():.3f}")
        # Win/loss stats
        winners = pnl_col[pnl_col > 0]
        losers = pnl_col[pnl_col < 0]
        if len(winners) > 0 and len(losers) > 0:
            print(f"  Avg win:  {winners.mean():+.4f} ({len(winners)} trades)")
            print(f"  Avg loss: {losers.mean():+.4f} ({len(losers)} trades)")
            print(f"  Win/Loss ratio: {abs(winners.mean() / losers.mean()):.2f}")
    elif "net_pnl_pct" in trades_df.columns:
        pnl_col = trades_df["net_pnl_pct"]
        print(f"  Mean:   {pnl_col.mean():+.4%}")
        print(f"  Median: {pnl_col.median():+.4%}")
        winners = pnl_col[pnl_col > 0]
        losers = pnl_col[pnl_col < 0]
        if len(winners) > 0 and len(losers) > 0:
            print(f"  Avg win:  {winners.mean():+.4%} ({len(winners)} trades)")
            print(f"  Avg loss: {losers.mean():+.4%} ({len(losers)} trades)")

    # ── 3. Hold time analysis ────────────────────────────
    print("\n── 3. HOLD TIME ANALYSIS ──")
    time_cols = [c for c in trades_df.columns if "duration" in c.lower() or "hold" in c.lower() or "time" in c.lower()]
    if time_cols:
        print(f"  Time-related columns: {time_cols}")
    if "close_timestamp" in trades_df.columns and "timestamp" in trades_df.columns:
        trades_df["hold_seconds"] = trades_df["close_timestamp"] - trades_df["timestamp"]
        trades_df["hold_hours"] = trades_df["hold_seconds"] / 3600
        print(f"  Mean hold:   {trades_df['hold_hours'].mean():.1f}h")
        print(f"  Median hold: {trades_df['hold_hours'].median():.1f}h")
        print(f"  Max hold:    {trades_df['hold_hours'].max():.1f}h")
        # Hold time by exit type
        if "close_type" in trades_df.columns:
            for ct_val in trades_df["close_type"].unique():
                subset = trades_df[trades_df["close_type"] == ct_val]
                print(f"    {str(ct_val):25s} avg_hold={subset['hold_hours'].mean():.1f}h")

    # ── 4. Per-pair performance ──────────────────────────
    print("\n── 4. PER-PAIR PERFORMANCE ──")
    pnl_field = "net_pnl_quote" if "net_pnl_quote" in trades_df.columns else "net_pnl_pct"
    if pnl_field in trades_df.columns:
        by_pair = trades_df.groupby("pair").agg(
            trades=(pnl_field, "count"),
            total_pnl=(pnl_field, "sum"),
            avg_pnl=(pnl_field, "mean"),
            wr=(pnl_field, lambda x: (x > 0).mean() * 100),
        ).sort_values("total_pnl", ascending=False)
        for _, row in by_pair.iterrows():
            marker = "+" if row["total_pnl"] > 0 else " "
            print(f"  {row.name:20s} trades={int(row['trades']):3d} "
                  f"PnL={marker}{row['total_pnl']:.3f} avg={row['avg_pnl']:+.4f} "
                  f"WR={row['wr']:.1f}%")

    # ── 5. Monthly breakdown ─────────────────────────────
    print("\n── 5. MONTHLY PnL ──")
    if "timestamp" in trades_df.columns and pnl_field in trades_df.columns:
        trades_df["month"] = pd.to_datetime(trades_df["timestamp"], unit="s").dt.to_period("M")
        monthly = trades_df.groupby("month").agg(
            trades=(pnl_field, "count"),
            pnl=(pnl_field, "sum"),
            wr=(pnl_field, lambda x: (x > 0).mean() * 100),
        )
        for idx, row in monthly.iterrows():
            bar = "+" * int(max(0, row["pnl"] * 10)) or "-" * int(max(0, -row["pnl"] * 10))
            print(f"  {idx}  trades={int(row['trades']):3d} PnL={row['pnl']:+.3f} "
                  f"WR={row['wr']:.1f}%  {bar}")

    # ── 6. Top 5 winners and losers ──────────────────────
    print("\n── 6. TOP 5 WINNERS / LOSERS ──")
    if pnl_field in trades_df.columns:
        top_win = trades_df.nlargest(5, pnl_field)
        top_loss = trades_df.nsmallest(5, pnl_field)
        ct_col = "close_type" if "close_type" in trades_df.columns else None
        print("  WINNERS:")
        for _, t in top_win.iterrows():
            ct = t[ct_col] if ct_col else "?"
            print(f"    {t['pair']:20s} PnL={t[pnl_field]:+.4f} exit={ct}")
        print("  LOSERS:")
        for _, t in top_loss.iterrows():
            ct = t[ct_col] if ct_col else "?"
            print(f"    {t['pair']:20s} PnL={t[pnl_field]:+.4f} exit={ct}")

    # ── 7. Top-5 concentration check ────────────────────
    print("\n── 7. CONCENTRATION CHECK ──")
    if pnl_field in trades_df.columns:
        total_pnl = trades_df[pnl_field].sum()
        top5_pnl = trades_df.nlargest(5, pnl_field)[pnl_field].sum()
        if total_pnl > 0:
            pct = top5_pnl / total_pnl * 100
            print(f"  Total PnL: {total_pnl:+.4f}")
            print(f"  Top-5 trades PnL: {top5_pnl:+.4f} ({pct:.1f}% of total)")
            if pct > 80:
                print("  ⚠ STOP CONDITION: Top-5 trades > 80% of PnL — outlier dependence")
        else:
            print(f"  Total PnL: {total_pnl:+.4f} (negative — no concentration concern)")

    print("\n" + "=" * 80)
    print("Diagnosis complete.")


if __name__ == "__main__":
    asyncio.run(run_diagnosis())
