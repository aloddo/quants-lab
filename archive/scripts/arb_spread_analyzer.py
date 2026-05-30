"""
R1 Spread Analyzer — Characterize cross-exchange spread dynamics from trade-level data.

Runs on the merged 1s/5s parquet files produced by download_arb_trades.py.
Computes all R1 metrics: spread distribution, autocorrelation, OU half-life,
opportunity density, time-of-day patterns, direction analysis, and regime detection.

Usage:
    python scripts/arb_spread_analyzer.py                    # all downloaded pairs
    python scripts/arb_spread_analyzer.py --pairs BANDUSDT   # specific pair
    python scripts/arb_spread_analyzer.py --tier majors      # by tier

Outputs:
    docs/arb_spread_analysis.md          — human-readable report
    app/data/cache/arb_analysis.csv      — per-pair metrics table
"""
import argparse
import glob
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parents[1] / "app/data/cache/arb_trades"
DOCS_DIR = Path(__file__).resolve().parents[1] / "docs"


def find_merged_files(pairs: list[str] | None = None) -> dict[str, Path]:
    """Find all merged parquet files, optionally filtered to specific pairs."""
    files = {}
    for d in CACHE_DIR.iterdir():
        if not d.is_dir():
            continue
        symbol = d.name
        if pairs and symbol not in pairs:
            continue
        # Find merged file (could be 1s or 5s)
        for f in d.glob("*_merged_*.parquet"):
            files[symbol] = f
            break
    return files


def compute_ou_halflife(series: pd.Series) -> float:
    """Ornstein-Uhlenbeck half-life from AR(1) regression on spread."""
    s = series.dropna()
    if len(s) < 100:
        return np.nan
    x = s.values
    dx = np.diff(x)
    x_lag = x[:-1]
    # Regress dx on x_lag: dx = beta * x_lag + epsilon
    # Half-life = -ln(2) / beta
    x_dm = x_lag - x_lag.mean()
    dx_dm = dx - dx.mean()
    denom = (x_dm ** 2).sum()
    if denom == 0:
        return np.nan
    beta = (x_dm * dx_dm).sum() / denom
    if beta >= 0:
        return np.nan  # not mean-reverting
    # Adjust for bar interval (infer from index)
    return -np.log(2) / beta  # in bars


def compute_hurst(series: pd.Series, max_lags: int = 100) -> float:
    """Hurst exponent via rescaled range (R/S) method.
    H < 0.5 = mean-reverting, H = 0.5 = random walk, H > 0.5 = trending.
    """
    s = series.dropna().values
    if len(s) < 200:
        return np.nan

    lags = range(10, min(max_lags, len(s) // 4))
    rs_values = []
    lag_values = []

    for lag in lags:
        n_chunks = len(s) // lag
        if n_chunks < 2:
            continue
        rs_list = []
        for i in range(n_chunks):
            chunk = s[i * lag:(i + 1) * lag]
            mean = chunk.mean()
            devs = np.cumsum(chunk - mean)
            r = devs.max() - devs.min()
            std = chunk.std()
            if std > 0:
                rs_list.append(r / std)
        if rs_list:
            rs_values.append(np.mean(rs_list))
            lag_values.append(lag)

    if len(lag_values) < 5:
        return np.nan

    log_lags = np.log(lag_values)
    log_rs = np.log(rs_values)
    slope, _, _, _, _ = stats.linregress(log_lags, log_rs)
    return slope


def analyze_pair(symbol: str, df: pd.DataFrame) -> dict:
    """Compute all R1 metrics for one pair."""
    result = {"symbol": symbol, "bars": len(df)}

    # Detect interval from index
    if len(df) > 2:
        dt = (df.index[1] - df.index[0]).total_seconds()
        result["interval_s"] = dt
    else:
        result["interval_s"] = 1

    days = (df.index[-1] - df.index[0]).total_seconds() / 86400
    result["days"] = round(days, 1)

    spread = df["spread_vwap_bps"]
    abs_spread = df["abs_spread_vwap"]

    # ── Basic spread statistics ──────────────────────────
    result["spread_mean"] = round(spread.mean(), 2)
    result["spread_median"] = round(spread.median(), 2)
    result["spread_std"] = round(spread.std(), 2)
    result["abs_spread_mean"] = round(abs_spread.mean(), 2)
    for pct in [25, 50, 75, 90, 95, 99]:
        result[f"abs_p{pct}"] = round(abs_spread.quantile(pct / 100), 2)

    # ── Direction analysis ───────────────────────────────
    # Positive spread = Bybit more expensive = BUY_BN_SELL_BB
    # Negative spread = Binance more expensive = BUY_BB_SELL_BN
    positive_frac = (spread > 0).mean()
    result["frac_buy_bn"] = round(positive_frac, 3)  # BUY_BN_SELL_BB
    result["frac_buy_bb"] = round(1 - positive_frac, 3)  # BUY_BB_SELL_BN

    # Direction at high spreads (>30bp)
    high = abs_spread >= 30
    if high.sum() > 0:
        result["high_spread_frac_buy_bn"] = round((spread[high] > 0).mean(), 3)
        result["high_spread_frac_buy_bb"] = round((spread[high] < 0).mean(), 3)
    else:
        result["high_spread_frac_buy_bn"] = np.nan
        result["high_spread_frac_buy_bb"] = np.nan

    # ── Opportunity density ──────────────────────────────
    # What fraction of time is there a tradeable spread?
    for threshold in [15, 20, 24, 30, 40, 60, 100]:
        frac = (abs_spread >= threshold).mean()
        result[f"density_{threshold}bp"] = round(frac, 4)

    # ── Autocorrelation ──────────────────────────────────
    for lag in [1, 5, 12, 60, 288]:
        ac = spread.autocorr(lag=lag)
        result[f"ac_{lag}"] = round(ac, 4) if not np.isnan(ac) else np.nan

    # ── OU half-life (in bars, then convert to hours) ────
    hl_bars = compute_ou_halflife(spread)
    interval_s = result["interval_s"]
    result["ou_halflife_bars"] = round(hl_bars, 1) if not np.isnan(hl_bars) else np.nan
    result["ou_halflife_hours"] = round(hl_bars * interval_s / 3600, 3) if not np.isnan(hl_bars) else np.nan

    # ── Hurst exponent ───────────────────────────────────
    result["hurst"] = round(compute_hurst(spread), 3)

    # ── Time-of-day pattern ──────────────────────────────
    df_copy = df.copy()
    df_copy["hour"] = df_copy.index.hour
    hourly = df_copy.groupby("hour")["abs_spread_vwap"].mean()
    result["best_hour_utc"] = int(hourly.idxmax())
    result["worst_hour_utc"] = int(hourly.idxmin())
    result["best_hour_spread"] = round(hourly.max(), 2)
    result["worst_hour_spread"] = round(hourly.min(), 2)
    result["hour_range_bps"] = round(hourly.max() - hourly.min(), 2)

    # ── Volume characteristics ───────────────────────────
    if "bn_volume_usd" in df.columns:
        result["bn_daily_vol_usd"] = round(df["bn_volume_usd"].sum() / max(days, 1), 0)
    if "bb_volume_usd" in df.columns:
        result["bb_daily_vol_usd"] = round(df["bb_volume_usd"].sum() / max(days, 1), 0)

    # ── Fee breakeven analysis ───────────────────────────
    # At different fee levels, what fraction of spreads are tradeable?
    for fee_rt_bps in [24, 19, 31]:  # current, BNB discount, fee increase
        tradeable = (abs_spread >= fee_rt_bps).mean()
        result[f"tradeable_at_{fee_rt_bps}bp_fee"] = round(tradeable, 4)

    return result


def generate_report(results: list[dict], output_path: Path):
    """Generate human-readable analysis report."""
    df = pd.DataFrame(results).sort_values("abs_spread_mean", ascending=False)

    with open(output_path, "w") as f:
        f.write("# Cross-Exchange Arb — Spread Analysis Report\n\n")
        f.write(f"**Generated:** {pd.Timestamp.now(tz='UTC').isoformat()}\n")
        f.write(f"**Data:** Trade-level (Binance SPOT aggTrades + Bybit PERP trades)\n")
        f.write(f"**Pairs analyzed:** {len(df)}\n\n")

        # Summary table
        f.write("## Summary (sorted by mean absolute spread)\n\n")
        f.write(f"| {'Symbol':<12} | {'Days':>4} | {'Mean':>6} | {'P50':>5} | {'P95':>5} | "
                f"{'HL(h)':>6} | {'Hurst':>5} | {'AC(1)':>5} | "
                f"{'Dens>30':>7} | {'Dens>60':>7} | {'Dir(BN)':>7} |\n")
        f.write(f"|{'-'*14}|{'-'*6}|{'-'*8}|{'-'*7}|{'-'*7}|"
                f"{'-'*8}|{'-'*7}|{'-'*7}|"
                f"{'-'*9}|{'-'*9}|{'-'*9}|\n")

        for _, r in df.iterrows():
            f.write(
                f"| {r['symbol']:<12} | {r['days']:>4.0f} | {r['abs_spread_mean']:>5.1f}bp | "
                f"{r['abs_p50']:>4.1f} | {r['abs_p95']:>4.1f} | "
                f"{r.get('ou_halflife_hours', np.nan):>5.3f}h | "
                f"{r.get('hurst', np.nan):>5.3f} | "
                f"{r.get('ac_1', np.nan):>5.3f} | "
                f"{r.get('density_30bp', 0):>6.1%} | "
                f"{r.get('density_60bp', 0):>6.1%} | "
                f"{r.get('frac_buy_bn', 0):>6.1%} |\n"
            )

        # Tier comparison
        f.write("\n## Tier Comparison\n\n")
        # Classify by volume
        for tier, vol_min, vol_max in [
            ("T1 Majors", 100e6, 1e15),
            ("T2 Mid-cap", 10e6, 100e6),
            ("T3 Small-cap", 1e6, 10e6),
            ("T4 Micro", 0, 1e6),
        ]:
            tier_pairs = df[
                (df.get("bn_daily_vol_usd", 0) >= vol_min) &
                (df.get("bn_daily_vol_usd", 0) < vol_max)
            ] if "bn_daily_vol_usd" in df.columns else pd.DataFrame()

            if len(tier_pairs) > 0:
                f.write(f"**{tier}** ({len(tier_pairs)} pairs): "
                        f"mean spread {tier_pairs['abs_spread_mean'].mean():.1f}bp, "
                        f"density >30bp {tier_pairs['density_30bp'].mean():.1%}, "
                        f"density >60bp {tier_pairs['density_60bp'].mean():.1%}\n\n")

        # Kill gate assessment
        f.write("\n## R1 Kill Gate Assessment\n\n")
        pass_30bp = (df["density_30bp"] >= 0.02).sum()
        f.write(f"- Pairs with >2% opportunity density at 30bp: **{pass_30bp}** (need >= 10)\n")

        half_life_ok = df[df["ou_halflife_hours"] < 1.0] if "ou_halflife_hours" in df.columns else pd.DataFrame()
        f.write(f"- Pairs with OU half-life < 1h: **{len(half_life_ok)}**\n")

        mean_reverting = df[df["hurst"] < 0.5] if "hurst" in df.columns else pd.DataFrame()
        f.write(f"- Pairs with Hurst < 0.5 (mean-reverting): **{len(mean_reverting)}**\n")

        if pass_30bp >= 10:
            f.write(f"\n**KILL GATE: PASS** ({pass_30bp} >= 10)\n")
        else:
            f.write(f"\n**KILL GATE: FAIL** ({pass_30bp} < 10)\n")

        # Direction analysis
        f.write("\n## Direction Analysis\n\n")
        f.write("Positive spread = Bybit expensive = BUY_BN_SELL_BB\n")
        f.write("Negative spread = Binance expensive = BUY_BB_SELL_BN\n\n")
        for _, r in df.iterrows():
            dominant = "BUY_BN" if r["frac_buy_bn"] > 0.5 else "BUY_BB"
            f.write(f"- {r['symbol']}: {dominant} dominant ({max(r['frac_buy_bn'], r['frac_buy_bb']):.0%})\n")

        # Time-of-day
        f.write("\n## Time-of-Day Patterns\n\n")
        f.write("| Symbol | Best Hour (UTC) | Spread | Worst Hour | Spread | Range |\n")
        f.write("|--------|----------------|--------|------------|--------|-------|\n")
        for _, r in df.iterrows():
            f.write(f"| {r['symbol']} | {r['best_hour_utc']:02d}:00 | {r['best_hour_spread']:.1f}bp | "
                    f"{r['worst_hour_utc']:02d}:00 | {r['worst_hour_spread']:.1f}bp | "
                    f"{r['hour_range_bps']:.1f}bp |\n")

    logger.info(f"Report written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="R1 Spread Analyzer")
    parser.add_argument("--pairs", type=str, help="Comma-separated symbols")
    parser.add_argument("--tier", type=str, choices=["majors", "midcap", "shitcoins", "all"])
    args = parser.parse_args()

    if args.pairs:
        pair_filter = [p.strip() for p in args.pairs.split(",")]
    elif args.tier:
        from download_arb_trades import TIER_MAJORS, TIER_MIDCAP
        if args.tier == "majors":
            pair_filter = TIER_MAJORS
        elif args.tier == "midcap":
            pair_filter = TIER_MIDCAP
        else:
            pair_filter = None
    else:
        pair_filter = None

    files = find_merged_files(pair_filter)
    if not files:
        logger.error("No merged parquet files found. Run download_arb_trades.py first.")
        sys.exit(1)

    logger.info(f"Analyzing {len(files)} pairs...")

    results = []
    for symbol, path in sorted(files.items()):
        logger.info(f"  {symbol}...")
        try:
            df = pd.read_parquet(path)
            r = analyze_pair(symbol, df)
            results.append(r)

            # Print key metrics inline
            logger.info(
                f"    mean={r['abs_spread_mean']:.1f}bp  "
                f"dens>30={r['density_30bp']:.1%}  "
                f"HL={r.get('ou_halflife_hours', 'N/A')}h  "
                f"H={r.get('hurst', 'N/A')}"
            )
        except Exception as e:
            logger.error(f"    FAILED: {e}")

    if not results:
        logger.error("No results")
        sys.exit(1)

    # Save CSV
    csv_path = Path(__file__).resolve().parents[1] / "app/data/cache/arb_analysis.csv"
    pd.DataFrame(results).to_csv(csv_path, index=False)
    logger.info(f"Metrics saved to {csv_path}")

    # Generate report
    report_path = DOCS_DIR / "arb_spread_analysis.md"
    generate_report(results, report_path)

    # Print summary
    df = pd.DataFrame(results)
    print("\n" + "=" * 70)
    print("R1 SPREAD ANALYSIS SUMMARY")
    print("=" * 70)
    print(f"Pairs analyzed: {len(df)}")
    print(f"Mean absolute spread: {df['abs_spread_mean'].mean():.1f}bp")
    print(f"Pairs with >2% density at 30bp: {(df['density_30bp'] >= 0.02).sum()}")
    print(f"Pairs with >2% density at 60bp: {(df['density_60bp'] >= 0.02).sum()}")
    print(f"Mean Hurst exponent: {df['hurst'].mean():.3f}")
    print(f"Mean OU half-life: {df['ou_halflife_hours'].mean():.3f}h")
    print("=" * 70)


if __name__ == "__main__":
    main()
