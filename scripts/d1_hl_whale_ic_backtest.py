"""
D1: Whale Positioning IC Backtest

Correlate HL whale net_bias and bias_change with Bybit forward returns.

Approach:
1. Load all whale consensus snapshots from MongoDB
2. Load Bybit 1h candles from parquet for the same period
3. For each snapshot, compute:
   - bias_t: net_bias at time t
   - bias_change: net_bias(t) - net_bias(t-1)
   - fwd_ret_1h: (close_t+1h - close_t) / close_t
   - fwd_ret_4h: (close_t+4h - close_t) / close_t
4. Compute rank IC (Spearman correlation) between signal and forward return
5. Compute per-quintile forward return spread
6. Check if extreme bias predicts direction

IC > 0.05 with consistent sign = proceed to controller.
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient
from pathlib import Path
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

MONGO_URI = "mongodb://localhost:27017/quants_lab"
CANDLE_DIR = Path("/Users/hermes/quants-lab/app/data/cache/candles")

# Map HL coin names to Bybit parquet file names
COIN_TO_PAIR = {
    "BTC": "BTC-USDT", "ETH": "ETH-USDT", "SOL": "SOL-USDT",
    "XRP": "XRP-USDT", "DOGE": "DOGE-USDT", "ADA": "ADA-USDT",
    "AVAX": "AVAX-USDT", "LINK": "LINK-USDT", "DOT": "DOT-USDT",
    "UNI": "UNI-USDT", "NEAR": "NEAR-USDT", "APT": "APT-USDT",
    "ARB": "ARB-USDT", "OP": "OP-USDT", "SUI": "SUI-USDT",
    "SEI": "SEI-USDT", "WLD": "WLD-USDT", "LTC": "LTC-USDT",
    "BCH": "BCH-USDT", "BNB": "BNB-USDT", "TAO": "TAO-USDT",
    "ENA": "ENA-USDT", "WIF": "WIF-USDT", "ONDO": "ONDO-USDT",
    "RENDER": "RENDER-USDT", "INJ": "INJ-USDT", "TIA": "TIA-USDT",
    "FET": "FET-USDT", "HYPE": "HYPE-USDT",
}


def load_whale_consensus() -> pd.DataFrame:
    """Load all whale consensus data from MongoDB."""
    db = MongoClient(MONGO_URI)["quants_lab"]
    docs = list(db.hl_whale_consensus.find({}, {"_id": 0}).sort("timestamp_utc", 1))
    df = pd.DataFrame(docs)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    print(f"Loaded {len(df)} consensus docs, {df['coin'].nunique()} coins, "
          f"{df['timestamp_utc'].min()} to {df['timestamp_utc'].max()}")
    return df


def load_candles_1h(pair: str) -> pd.DataFrame:
    """Load 1h candles from parquet."""
    path = CANDLE_DIR / f"bybit_perpetual|{pair}|1h.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)

    # The index is datetime, the 'timestamp' column is epoch seconds
    if isinstance(df.index, pd.DatetimeIndex):
        df["timestamp"] = df.index.tz_localize("UTC") if df.index.tz is None else df.index
        df = df.reset_index(drop=True)
    elif "timestamp" in df.columns:
        # Epoch seconds
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def compute_forward_returns(candles: pd.DataFrame, horizons: list[int] = [1, 4]) -> pd.DataFrame:
    """Compute forward returns at given hour horizons."""
    df = candles[["timestamp", "close"]].copy()
    df = df.set_index("timestamp").sort_index()

    for h in horizons:
        df[f"fwd_ret_{h}h"] = df["close"].shift(-h) / df["close"] - 1

    return df.reset_index()


def align_signals_with_returns(
    consensus: pd.DataFrame,
    candles_with_returns: pd.DataFrame,
    coin: str,
) -> pd.DataFrame:
    """Align whale signals with forward returns using nearest hour."""
    # Filter consensus to this coin
    sig = consensus[consensus["coin"] == coin][["timestamp_utc", "net_bias", "total_notional",
                                                  "n_long", "n_short", "hhi", "long_pct"]].copy()
    sig = sig.sort_values("timestamp_utc")

    # Round whale snapshot time to nearest hour for joining
    sig["hour"] = sig["timestamp_utc"].dt.floor("h")

    # Aggregate multiple snapshots within the same hour (take last)
    sig = sig.groupby("hour").last().reset_index()

    # Compute bias change
    sig["bias_change"] = sig["net_bias"].diff()
    sig["bias_abs"] = sig["net_bias"].abs()

    # Join with candle forward returns
    candles_with_returns["hour"] = candles_with_returns["timestamp"].dt.floor("h")
    merged = sig.merge(candles_with_returns[["hour", "close", "fwd_ret_1h", "fwd_ret_4h"]],
                       on="hour", how="inner")

    return merged


def compute_ic(signal: pd.Series, returns: pd.Series) -> tuple:
    """Compute Spearman rank IC and p-value."""
    mask = signal.notna() & returns.notna()
    if mask.sum() < 10:
        return np.nan, np.nan
    corr, pval = stats.spearmanr(signal[mask], returns[mask])
    return corr, pval


def quintile_analysis(merged: pd.DataFrame, signal_col: str, return_col: str) -> pd.DataFrame:
    """Compute mean forward return by signal quintile."""
    df = merged[[signal_col, return_col]].dropna()
    if len(df) < 20:
        return pd.DataFrame()

    df["quintile"] = pd.qcut(df[signal_col], 5, labels=["Q1(low)", "Q2", "Q3", "Q4", "Q5(high)"],
                              duplicates="drop")
    result = df.groupby("quintile", observed=True)[return_col].agg(["mean", "count", "std"])
    result["mean_bps"] = result["mean"] * 10000
    return result


def extreme_analysis(merged: pd.DataFrame, bias_col: str = "net_bias",
                     threshold: float = 0.3) -> dict:
    """Check if extreme bias predicts reversals."""
    df = merged.dropna(subset=[bias_col, "fwd_ret_1h", "fwd_ret_4h"])

    results = {}
    for label, cond, expected_sign in [
        ("strong_long (bias>+t)", df[bias_col] > threshold, -1),   # fade = expect negative return
        ("strong_short (bias<-t)", df[bias_col] < -threshold, +1),  # fade = expect positive return
    ]:
        subset = df[cond]
        if len(subset) < 5:
            results[label] = {"n": len(subset), "note": "too few"}
            continue

        mean_1h = subset["fwd_ret_1h"].mean()
        mean_4h = subset["fwd_ret_4h"].mean()

        # Does the return go in the expected (fade) direction?
        fade_correct_1h = (mean_1h * expected_sign) > 0
        fade_correct_4h = (mean_4h * expected_sign) > 0

        results[label] = {
            "n": len(subset),
            "avg_bias": subset[bias_col].mean(),
            "fwd_1h_bps": round(mean_1h * 10000, 1),
            "fwd_4h_bps": round(mean_4h * 10000, 1),
            "fade_correct_1h": fade_correct_1h,
            "fade_correct_4h": fade_correct_4h,
        }

    return results


def main():
    print("=" * 70)
    print("D1: Hyperliquid Whale Positioning — IC Backtest")
    print("=" * 70)

    consensus = load_whale_consensus()

    # Per-coin IC analysis
    all_ic_results = []
    all_merged = []

    for coin, pair in sorted(COIN_TO_PAIR.items()):
        candles = load_candles_1h(pair)
        if candles.empty or "close" not in candles.columns:
            continue

        candles_ret = compute_forward_returns(candles)
        merged = align_signals_with_returns(consensus, candles_ret, coin)

        if len(merged) < 15:
            continue

        # IC for net_bias and bias_change
        ic_bias_1h, p_bias_1h = compute_ic(merged["net_bias"], merged["fwd_ret_1h"])
        ic_bias_4h, p_bias_4h = compute_ic(merged["net_bias"], merged["fwd_ret_4h"])
        ic_change_1h, p_change_1h = compute_ic(merged["bias_change"], merged["fwd_ret_1h"])
        ic_change_4h, p_change_4h = compute_ic(merged["bias_change"], merged["fwd_ret_4h"])

        all_ic_results.append({
            "coin": coin,
            "n": len(merged),
            "ic_bias_1h": ic_bias_1h,
            "p_bias_1h": p_bias_1h,
            "ic_bias_4h": ic_bias_4h,
            "p_bias_4h": p_bias_4h,
            "ic_change_1h": ic_change_1h,
            "p_change_1h": p_change_1h,
            "ic_change_4h": ic_change_4h,
            "p_change_4h": p_change_4h,
            "bias_std": merged["net_bias"].std(),
            "bias_range": merged["net_bias"].max() - merged["net_bias"].min(),
        })

        merged["coin"] = coin
        all_merged.append(merged)

    ic_df = pd.DataFrame(all_ic_results)
    pooled = pd.concat(all_merged, ignore_index=True) if all_merged else pd.DataFrame()

    # ── Per-Coin IC Table ──
    print(f"\n{'─'*70}")
    print(f"PER-COIN INFORMATION COEFFICIENT (Spearman Rank IC)")
    print(f"{'─'*70}")
    print(f"{'Coin':<6} {'N':>4} {'IC_bias_1h':>10} {'p':>6} {'IC_bias_4h':>10} {'p':>6} "
          f"{'IC_chg_1h':>10} {'p':>6} {'IC_chg_4h':>10} {'p':>6} {'bias_std':>8}")
    print(f"{'─'*6} {'─'*4} {'─'*10} {'─'*6} {'─'*10} {'─'*6} {'─'*10} {'─'*6} {'─'*10} {'─'*6} {'─'*8}")

    for _, r in ic_df.sort_values("ic_bias_4h", key=abs, ascending=False).iterrows():
        def fmt_ic(ic, p):
            if pd.isna(ic):
                return "    N/A", "  N/A"
            marker = "*" if p < 0.1 else " "
            marker = "**" if p < 0.05 else marker
            return f"{ic:+.4f}{marker}", f"{p:.3f}"

        ic1, p1 = fmt_ic(r["ic_bias_1h"], r["p_bias_1h"])
        ic4, p4 = fmt_ic(r["ic_bias_4h"], r["p_bias_4h"])
        icc1, pc1 = fmt_ic(r["ic_change_1h"], r["p_change_1h"])
        icc4, pc4 = fmt_ic(r["ic_change_4h"], r["p_change_4h"])

        print(f"{r['coin']:<6} {r['n']:>4} {ic1:>10} {p1:>6} {ic4:>10} {p4:>6} "
              f"{icc1:>10} {pc1:>6} {icc4:>10} {pc4:>6} {r['bias_std']:>8.4f}")

    # ── Pooled IC ──
    if not pooled.empty:
        print(f"\n{'─'*70}")
        print(f"POOLED IC (all coins, all timestamps)")
        print(f"{'─'*70}")
        for sig_col, label in [("net_bias", "Level (net_bias)"),
                                ("bias_change", "Change (delta bias)"),
                                ("bias_abs", "Absolute (|bias|)"),
                                ("long_pct", "Long % (count ratio)")]:
            if sig_col not in pooled.columns:
                continue
            for ret_col in ["fwd_ret_1h", "fwd_ret_4h"]:
                ic, p = compute_ic(pooled[sig_col], pooled[ret_col])
                sig = "**" if p < 0.05 else ("*" if p < 0.1 else "")
                print(f"  {label:<25} vs {ret_col}: IC={ic:+.4f} p={p:.4f} {sig} (n={pooled[sig_col].notna().sum()})")

        # ── Quintile Analysis (pooled) ──
        print(f"\n{'─'*70}")
        print(f"QUINTILE ANALYSIS — net_bias vs fwd_ret_4h (pooled)")
        print(f"{'─'*70}")
        qa = quintile_analysis(pooled, "net_bias", "fwd_ret_4h")
        if not qa.empty:
            for idx, row in qa.iterrows():
                bar = "█" * max(1, int(abs(row["mean_bps"]) / 2))
                sign = "+" if row["mean_bps"] > 0 else ""
                print(f"  {idx}: {sign}{row['mean_bps']:>6.1f} bps  n={row['count']:>4}  "
                      f"{'▸' if row['mean_bps'] > 0 else '◂'}{bar}")

            spread = qa.iloc[-1]["mean_bps"] - qa.iloc[0]["mean_bps"]
            print(f"\n  Q5-Q1 spread: {spread:+.1f} bps")
            print(f"  {'MONOTONIC' if all(qa['mean_bps'].diff().dropna() > 0) or all(qa['mean_bps'].diff().dropna() < 0) else 'NON-MONOTONIC'}")

        # ── Extreme Bias Analysis ──
        print(f"\n{'─'*70}")
        print(f"EXTREME BIAS ANALYSIS (|bias| > 0.3)")
        print(f"{'─'*70}")
        extreme = extreme_analysis(pooled, threshold=0.3)
        for label, res in extreme.items():
            if "note" in res:
                print(f"  {label}: n={res['n']} ({res['note']})")
            else:
                fade_1h = "FADE WORKS" if res["fade_correct_1h"] else "FOLLOW WORKS"
                fade_4h = "FADE WORKS" if res["fade_correct_4h"] else "FOLLOW WORKS"
                print(f"  {label}: n={res['n']}, avg_bias={res['avg_bias']:+.2f}")
                print(f"    1h: {res['fwd_1h_bps']:+.1f} bps → {fade_1h}")
                print(f"    4h: {res['fwd_4h_bps']:+.1f} bps → {fade_4h}")

        # ── Bias Change Extreme Analysis ──
        print(f"\n{'─'*70}")
        print(f"BIAS CHANGE ANALYSIS (|change| > 0.05)")
        print(f"{'─'*70}")
        change_ext = extreme_analysis(pooled, bias_col="bias_change", threshold=0.05)
        for label, res in change_ext.items():
            if "note" in res:
                print(f"  {label}: n={res['n']} ({res['note']})")
            else:
                print(f"  {label}: n={res['n']}, avg_change={res['avg_bias']:+.3f}")
                print(f"    1h: {res['fwd_1h_bps']:+.1f} bps")
                print(f"    4h: {res['fwd_4h_bps']:+.1f} bps")

    # ── Summary ──
    print(f"\n{'='*70}")
    print(f"VERDICT")
    print(f"{'='*70}")
    sig_coins_1h = ic_df[ic_df["p_bias_1h"] < 0.1]
    sig_coins_4h = ic_df[ic_df["p_bias_4h"] < 0.1]
    print(f"  Coins with significant IC at 1h (p<0.1): {len(sig_coins_1h)}/{len(ic_df)}")
    if not sig_coins_1h.empty:
        for _, r in sig_coins_1h.iterrows():
            print(f"    {r['coin']}: IC={r['ic_bias_1h']:+.4f} (p={r['p_bias_1h']:.3f})")
    print(f"  Coins with significant IC at 4h (p<0.1): {len(sig_coins_4h)}/{len(ic_df)}")
    if not sig_coins_4h.empty:
        for _, r in sig_coins_4h.iterrows():
            print(f"    {r['coin']}: IC={r['ic_bias_4h']:+.4f} (p={r['p_bias_4h']:.3f})")

    if not pooled.empty:
        pooled_ic_4h, pooled_p_4h = compute_ic(pooled["net_bias"], pooled["fwd_ret_4h"])
        print(f"\n  Pooled IC (bias→4h): {pooled_ic_4h:+.4f} (p={pooled_p_4h:.4f})")
        if abs(pooled_ic_4h) > 0.05 and pooled_p_4h < 0.05:
            print(f"  → SIGNAL DETECTED. Proceed to controller design.")
        elif abs(pooled_ic_4h) > 0.03:
            print(f"  → WEAK SIGNAL. Accumulate more data (need 200+ snapshots).")
        else:
            print(f"  → NO SIGNAL at current sample size. Check per-coin and extreme analysis.")

    # Save
    output_dir = Path("app/data/cache/d1_hl_whale")
    output_dir.mkdir(parents=True, exist_ok=True)
    ic_df.to_csv(output_dir / "ic_results.csv", index=False)
    if not pooled.empty:
        pooled.to_csv(output_dir / "pooled_signals.csv", index=False)
    print(f"\n  Results saved to {output_dir}/")


if __name__ == "__main__":
    main()
