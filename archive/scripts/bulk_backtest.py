#!/usr/bin/env python3
"""
Bulk backtest script — runs E1 or E2 across all pairs, prints funnel + summary.

Usage:
  set -a && source /Users/hermes/quants-lab/.env && set +a
  /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/bulk_backtest.py \
      --engine E1 --days 365 --output app/data/processed/backtest_results

Features:
  - Fresh BacktestingEngine per pair (avoids HB state corruption)
  - Retry on intermittent API errors
  - Funnel analysis (setup → trigger → entry quality → signal)
  - Summary table + verdict distribution
  - Saves results JSON + Plotly HTML charts
  - Optionally pushes verdicts to MongoDB
"""
import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from app.engines.registry import get_engine, build_backtest_config

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger(__name__)

# Verdict thresholds
PF_ALLOW = 1.3
PF_WATCH = 1.0
MIN_TRADES = 10
MAX_RETRIES = 2


def discover_pairs(connector: str) -> list[str]:
    """Find pairs with 1h parquet data."""
    candles_dir = data_paths.candles_dir
    pairs = []
    for f in candles_dir.glob(f"{connector}|*|1h.parquet"):
        parts = f.stem.split("|")
        if len(parts) == 3:
            pairs.append(parts[1])
    return sorted(pairs)


def compute_verdict(pf: float, trades: int) -> str:
    if trades < MIN_TRADES:
        return "BLOCK"
    if pf >= PF_ALLOW:
        return "ALLOW"
    if pf >= PF_WATCH:
        return "WATCH"
    return "BLOCK"


async def backtest_pair(engine_name: str, connector: str, pair: str,
                        start_ts: int, end_ts: int, trade_cost: float,
                        overrides: dict = None) -> dict:
    """Run backtest for a single pair with retry logic."""
    import gc
    engine_meta = get_engine(engine_name)
    resolution = engine_meta["backtesting_resolution"]
    override_kwargs = overrides or {}

    for attempt in range(MAX_RETRIES + 1):
        try:
            bt = BacktestingEngine(load_cached_data=True)
            config = build_backtest_config(
                engine_name=engine_name, connector=connector, pair=pair,
                **override_kwargs,
            )
            result_obj = await bt.run_backtesting(
                config=config, start=start_ts, end=end_ts,
                backtesting_resolution=resolution, trade_cost=trade_cost,
            )
            if not isinstance(result_obj.results.get("close_types"), dict):
                result_obj.results["close_types"] = {}

            r = result_obj.results
            pdf = result_obj.processed_data
            trades = r.get("total_executors", 0)
            pf = r.get("profit_factor", 0) or 0
            wr = (r.get("accuracy_long", 0) or 0) * 100
            sharpe = r.get("sharpe_ratio", None)
            max_dd = r.get("max_drawdown_pct", None)
            pnl = r.get("net_pnl_quote", 0) or 0

            # Funnel stats
            setup_active = int((pdf["setup_active"] != 0).sum()) if "setup_active" in pdf.columns else 0
            trigger_pass = int((pdf["trigger_5m_pass"] != 0).sum()) if "trigger_5m_pass" in pdf.columns else 0
            eq_pass = int((pdf["entry_quality_pass"] != 0).sum()) if "entry_quality_pass" in pdf.columns else 0
            signals = int((pdf["signal"] != 0).sum()) if "signal" in pdf.columns else 0

            result = {
                "pair": pair, "trades": trades, "pf": round(pf, 2),
                "win_rate": round(wr, 1), "sharpe": round(sharpe, 2) if sharpe else None,
                "max_dd_pct": round(max_dd * 100, 1) if max_dd else None,
                "pnl_quote": round(pnl, 2),
                "n_long": r.get("total_long", 0), "n_short": r.get("total_short", 0),
                "verdict": compute_verdict(pf, trades),
                "setup_active": setup_active, "trigger_5m": trigger_pass,
                "eq_pass": eq_pass, "signals": signals,
                "close_types": {(k.name if hasattr(k, "name") else str(k)): v
                                for k, v in r.get("close_types", {}).items()},
            }
            del bt, result_obj, pdf
            gc.collect()
            return result
        except Exception as e:
            gc.collect()
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1)
                continue
            return {
                "pair": pair, "trades": 0, "pf": 0, "verdict": "ERROR",
                "error": str(e), "setup_active": 0, "trigger_5m": 0,
                "eq_pass": 0, "signals": 0,
            }


def print_summary(results: list[dict], engine_name: str, period: str):
    """Print formatted summary table and funnel."""
    df = pd.DataFrame(results)
    ok = df[df["verdict"] != "ERROR"]
    err = df[df["verdict"] == "ERROR"]

    print(f"\n{'='*75}")
    print(f"  {engine_name} BULK BACKTEST — {period}")
    print(f"  {len(ok)} pairs tested, {len(err)} errors")
    print(f"{'='*75}")

    # Verdict counts
    counts = ok["verdict"].value_counts()
    print(f"\n  ALLOW: {counts.get('ALLOW', 0)}  |  WATCH: {counts.get('WATCH', 0)}  |  BLOCK: {counts.get('BLOCK', 0)}")

    if len(ok) > 0:
        total_pnl = ok["pnl_quote"].sum()
        allow_pnl = ok[ok["verdict"] == "ALLOW"]["pnl_quote"].sum()
        print(f"  Total PnL: {total_pnl:+.2f}  |  ALLOW PnL: {allow_pnl:+.2f}")

    # Funnel
    total_setups = ok["setup_active"].sum()
    total_triggers = ok["trigger_5m"].sum()
    total_eq = ok["eq_pass"].sum()
    total_signals = ok["signals"].sum()
    total_trades = ok["trades"].sum()

    print(f"\n  FUNNEL:")
    print(f"    Setup active:     {total_setups:>6}")
    print(f"    5m trigger pass:  {total_triggers:>6}  ({total_triggers/max(total_setups,1)*100:.1f}%)")
    print(f"    Entry quality:    {total_eq:>6}  ({total_eq/max(total_triggers,1)*100:.1f}%)")
    print(f"    Final signals:    {total_signals:>6}")
    print(f"    Executed trades:  {total_trades:>6}")

    # Table sorted by PF
    print(f"\n  {'Pair':>14s}  {'Verdict':>7s}  {'PF':>6s}  {'WR%':>5s}  {'Trades':>6s}  {'PnL':>8s}  {'Sharpe':>6s}  {'Setups':>6s}  {'Trig':>5s}  {'EQ':>4s}")
    print(f"  {'-'*14}  {'-'*7}  {'-'*6}  {'-'*5}  {'-'*6}  {'-'*8}  {'-'*6}  {'-'*6}  {'-'*5}  {'-'*4}")
    for _, r in ok.sort_values("pf", ascending=False).iterrows():
        print(f"  {r['pair']:>14s}  {r['verdict']:>7s}  {r['pf']:>6.2f}  {r.get('win_rate',0):>5.1f}  {r['trades']:>6d}  {r.get('pnl_quote',0):>+8.2f}  {str(r.get('sharpe','—')):>6s}  {r['setup_active']:>6d}  {r['trigger_5m']:>5d}  {r['eq_pass']:>4d}")

    if len(err) > 0:
        print(f"\n  ERRORS ({len(err)}):")
        for _, r in err.iterrows():
            print(f"    {r['pair']}: {r.get('error', 'unknown')}")

    return df


def save_results(df: pd.DataFrame, output_dir: Path, engine_name: str, period: str):
    """Save results to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{engine_name.lower()}_bulk_{period.replace(' ', '_')}.json"
    fpath = output_dir / fname
    df.to_json(fpath, orient="records", indent=2)
    print(f"\n  Results saved: {fpath}")
    return fpath


def save_charts(df: pd.DataFrame, output_dir: Path, engine_name: str, period: str):
    """Generate and save Plotly HTML charts."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print("  (plotly not available, skipping charts)")
        return

    ok = df[df["verdict"] != "ERROR"].copy()
    if len(ok) == 0:
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    # Chart 1: PF vs Win Rate scatter
    color_map = {"ALLOW": "#00ff88", "WATCH": "#ffd700", "BLOCK": "#ff6666"}
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Verdict Distribution", "Profit Factor vs Win Rate"),
                        column_widths=[0.3, 0.7])

    vc = ok["verdict"].value_counts().reindex(["ALLOW", "WATCH", "BLOCK"]).fillna(0)
    fig.add_trace(go.Bar(
        x=vc.index, y=vc.values,
        marker_color=[color_map[v] for v in vc.index],
        text=vc.values, textposition="auto",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=ok["win_rate"], y=ok["pf"],
        mode="markers+text",
        text=ok["pair"].str.replace("-USDT", ""),
        textposition="top center", textfont=dict(size=9, color="white"),
        marker=dict(
            size=np.clip(ok["trades"], 5, 50),
            color=[color_map.get(v, "#999") for v in ok["verdict"]],
            opacity=0.8, line=dict(width=1, color="white"),
        ),
        hovertext=[f"{r['pair']}<br>PF={r['pf']}<br>WR={r['win_rate']}%<br>Trades={r['trades']}"
                   for _, r in ok.iterrows()],
        hoverinfo="text", showlegend=False,
    ), row=1, col=2)

    fig.add_hline(y=PF_ALLOW, line_dash="dash", line_color="#00ff88", row=1, col=2)
    fig.add_hline(y=PF_WATCH, line_dash="dash", line_color="#ffd700", row=1, col=2)
    fig.update_layout(
        title=f"{engine_name} Bulk Backtest — {period}",
        height=500, showlegend=False,
        plot_bgcolor='rgba(0,0,0,0.85)', paper_bgcolor='rgba(0,0,0,0.85)',
        font=dict(color="white"),
    )
    fig.update_xaxes(gridcolor="gray")
    fig.update_yaxes(gridcolor="gray")

    chart_path = output_dir / f"{engine_name.lower()}_bulk_{period.replace(' ', '_')}.html"
    fig.write_html(str(chart_path))
    print(f"  Chart saved: {chart_path}")


async def push_to_mongo(results: list[dict], engine_name: str, period: str):
    """Push verdicts to MongoDB pair_historical."""
    import os
    from motor.motor_asyncio import AsyncIOMotorClient

    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    db_name = os.environ.get("MONGO_DATABASE", "quants_lab")
    client = AsyncIOMotorClient(uri)
    db = client[db_name]
    collection = db["pair_historical"]

    pushed = 0
    for row in results:
        if row.get("verdict") == "ERROR":
            continue
        doc = {
            "engine": engine_name, "pair": row["pair"], "period": period,
            "trades": row["trades"], "profit_factor": row["pf"],
            "win_rate": row.get("win_rate", 0), "pnl_quote": row.get("pnl_quote", 0),
            "max_dd_pct": row.get("max_dd_pct"), "sharpe": row.get("sharpe"),
            "n_long": row.get("n_long", 0), "n_short": row.get("n_short", 0),
            "verdict": row["verdict"],
            "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        await collection.update_one(
            {"engine": engine_name, "pair": row["pair"]},
            {"$set": doc}, upsert=True,
        )
        pushed += 1

    print(f"\n  Pushed {pushed} verdicts to MongoDB pair_historical")
    client.close()


async def run_sensitivity(engine_name: str, connector: str, pairs: list[str],
                          start_ts: int, end_ts: int, trade_cost: float):
    """Run entry quality sensitivity test across 4 variants."""
    variants = {
        "locked":   {},
        "relaxed":  {"entry_distance_atr_mult": 0.5, "entry_body_atr_mult": 0.8, "entry_gap_bps_floor": 20.0},
        "wide":     {"entry_distance_atr_mult": 1.0, "entry_body_atr_mult": 1.0, "entry_gap_bps_floor": 50.0},
        "disabled": {"entry_quality_filter": False},
    }

    all_rows = []
    for vname, overrides in variants.items():
        print(f"\n{'='*60}")
        print(f"  VARIANT: {vname}  {overrides or '(defaults)'}")
        print(f"{'='*60}")

        for pair in pairs:
            row = await backtest_pair(engine_name, connector, pair, start_ts, end_ts,
                                      trade_cost, overrides=overrides)
            row["variant"] = vname
            all_rows.append(row)

            if row["verdict"] == "ERROR":
                print(f"    [!] {pair:>12s}  ERROR")
            else:
                marker = "+" if row["pf"] >= 1.3 and row["trades"] >= 5 else ("~" if row["trades"] > 0 else "x")
                print(f"    [{marker}] {pair:>12s}  sig={row['signals']:3d}  trades={row['trades']:3d}  PF={row['pf']:6.2f}  WR={row.get('win_rate',0):5.1f}%  PnL={row.get('pnl_quote',0):+8.2f}")

    # Summary comparison
    comp_df = pd.DataFrame(all_rows)
    ok = comp_df[comp_df["verdict"] != "ERROR"]

    print(f"\n\n{'='*75}")
    print(f"  VARIANT COMPARISON — {engine_name}")
    print(f"{'='*75}")

    summary = ok.groupby("variant").agg(
        pairs=("pair", "count"),
        total_signals=("signals", "sum"),
        total_trades=("trades", "sum"),
        total_pnl=("pnl_quote", "sum"),
        avg_pf=("pf", "mean"),
        avg_wr=("win_rate", "mean"),
    ).reindex(["locked", "relaxed", "wide", "disabled"])

    print(f"\n  {'Variant':>10s}  {'Pairs':>5s}  {'Signals':>7s}  {'Trades':>6s}  {'PnL':>8s}  {'AvgPF':>6s}  {'AvgWR':>6s}")
    print(f"  {'-'*10}  {'-'*5}  {'-'*7}  {'-'*6}  {'-'*8}  {'-'*6}  {'-'*6}")
    for v in ["locked", "relaxed", "wide", "disabled"]:
        if v in summary.index:
            r = summary.loc[v]
            print(f"  {v:>10s}  {int(r['pairs']):>5d}  {int(r['total_signals']):>7d}  {int(r['total_trades']):>6d}  {r['total_pnl']:>+8.2f}  {r['avg_pf']:>6.2f}  {r['avg_wr']:>6.1f}")

    locked_sig = summary.loc["locked", "total_signals"] if "locked" in summary.index else 1
    print(f"\n  Signal multiplier vs locked:")
    for v in ["relaxed", "wide", "disabled"]:
        if v in summary.index:
            mult = summary.loc[v, "total_signals"] / max(locked_sig, 1)
            print(f"    {v}: {mult:.1f}x signals")

    return comp_df


async def main():
    parser = argparse.ArgumentParser(description="Bulk backtest across all pairs")
    parser.add_argument("--engine", default="E1", help="Engine name (E1, E2)")
    parser.add_argument("--days", type=int, default=365, help="Backtest window in days")
    parser.add_argument("--cost", type=float, default=0.000375, help="Trade cost per side")
    parser.add_argument("--connector", default="bybit_perpetual")
    parser.add_argument("--output", default="app/data/processed", help="Output directory")
    parser.add_argument("--push-mongo", action="store_true", help="Push verdicts to MongoDB")
    parser.add_argument("--sensitivity", action="store_true", help="Run entry quality sensitivity test")
    parser.add_argument("--pairs", nargs="*", help="Specific pairs (default: all)")
    args = parser.parse_args()

    engine_meta = get_engine(args.engine)
    print(f"Engine: {args.engine} ({engine_meta['name']})")
    print(f"Resolution: {engine_meta['backtesting_resolution']} | Days: {args.days} | Cost: {args.cost}")

    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = now_ts - args.days * 86400
    period = f"{datetime.utcfromtimestamp(start_ts).strftime('%Y-%m-%d')}_{datetime.utcfromtimestamp(now_ts).strftime('%Y-%m-%d')}"

    pairs = args.pairs or discover_pairs(args.connector)
    print(f"Pairs: {len(pairs)}")

    if args.sensitivity:
        comp_df = await run_sensitivity(args.engine, args.connector, pairs,
                                        start_ts, now_ts, args.cost)
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        comp_df.to_json(output_dir / f"{args.engine.lower()}_sensitivity.json", orient="records", indent=2)
        print(f"\n  Sensitivity results saved: {output_dir / f'{args.engine.lower()}_sensitivity.json'}")
        return

    # Full bulk backtest
    t0 = time.time()
    results = []
    for i, pair in enumerate(pairs):
        result = await backtest_pair(args.engine, args.connector, pair,
                                     start_ts, now_ts, args.cost)
        results.append(result)
        # Progress
        marker = {"ALLOW": "+", "WATCH": "~", "BLOCK": "x", "ERROR": "!"}[result["verdict"]]
        pf_str = f"PF={result['pf']:.2f}" if result["verdict"] != "ERROR" else result.get("error", "")[:40]
        print(f"  [{marker}] {i+1:>3d}/{len(pairs)}  {pair:>14s}  {pf_str}")

    elapsed = time.time() - t0
    print(f"\n  Completed in {elapsed:.0f}s ({elapsed/len(pairs):.1f}s/pair)")

    df = print_summary(results, args.engine, period)
    output_dir = Path(args.output)
    save_results(df, output_dir, args.engine, period)
    save_charts(df, output_dir, args.engine, period)

    if args.push_mongo:
        await push_to_mongo(results, args.engine, period)


if __name__ == "__main__":
    asyncio.run(main())
