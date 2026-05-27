"""
Cross-Sectional Funding Crowding EDA — Hyperliquid
====================================================
Hypothesis: At each hourly timestamp, SHORT the coin(s) with the highest
72h funding z-score (z > 2.5). Pool ALL HL coins instead of betting on
a single coin (FARTCOIN). Hold 8h, non-overlapping per coin.

Phase 0 EDA per research-process guidelines.
"""

import os
import sys
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from scipy import stats
from dotenv import load_dotenv
from pymongo import MongoClient

warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
load_dotenv("/Users/hermes/quants-lab/.env")
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB  = os.environ["MONGO_DATABASE"]

ZSCORE_WINDOW   = 72          # hours for rolling z-score
HOLD_HOURS      = 8           # holding period
COOLDOWN_HOURS  = 8           # per-coin non-overlap cooldown
Z_THRESHOLD     = 2.5         # minimum z-score to trigger signal
TOP_N_LIST      = [1, 3, 5]   # portfolios to test
FEE_RT_BPS      = 8.64        # 4.32bp taker x2 round-trip (conservative; live uses limit orders at 2.88bp)
MIN_DAYS        = 300         # minimum coverage for coin to qualify
MIN_OBS_PER_DAY = 20          # minimum hourly observations per day

print("=" * 72)
print("CROSS-SECTIONAL FUNDING CROWDING EDA — HYPERLIQUID")
print(f"Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 72)

# ── 0. Connect & Load Funding Rates ─────────────────────────────────────────
print("\n[Phase 0] Loading data from MongoDB …")
client = MongoClient(MONGO_URI)
db     = client[MONGO_DB]

funding_coll = db["hyperliquid_funding_rates"]
candles_coll = db["hyperliquid_candles_1h"]

# Per-coin coverage audit
pipeline = [
    {"$group": {
        "_id": "$coin",
        "count": {"$sum": 1},
        "min_ts": {"$min": "$timestamp_utc"},
        "max_ts": {"$max": "$timestamp_utc"},
    }}
]
coverage = pd.DataFrame(list(funding_coll.aggregate(pipeline)))
coverage.rename(columns={"_id": "coin"}, inplace=True)
coverage["days"] = (coverage["max_ts"] - coverage["min_ts"]) / (1000 * 86400)
coverage["obs_per_day"] = coverage["count"] / coverage["days"].clip(lower=1)

print(f"\n[Phase 0 — Data Coverage Audit]")
print(f"  Total coins with any funding data : {len(coverage)}")
print(f"  Obs range  : {coverage['count'].min()} – {coverage['count'].max()}")
print(f"  Days range : {coverage['days'].min():.0f} – {coverage['days'].max():.0f}")

# Apply filters
qualifying = coverage[
    (coverage["days"] >= MIN_DAYS) &
    (coverage["obs_per_day"] >= MIN_OBS_PER_DAY)
].copy()
print(f"\n  Qualifying coins (>= {MIN_DAYS}d, >= {MIN_OBS_PER_DAY} obs/day): {len(qualifying)}")
print(f"  Excluded coins: {len(coverage) - len(qualifying)}")

qual_coins = sorted(qualifying["coin"].tolist())
print(f"\n  Qualifying coins: {qual_coins}")

# ── 1. Load Funding Rates for Qualifying Coins ──────────────────────────────
print("\n[Phase 1] Loading funding rates …")
docs = list(funding_coll.find(
    {"coin": {"$in": qual_coins}},
    {"_id": 0, "coin": 1, "timestamp_utc": 1, "funding_rate": 1}
))
print(f"  Loaded {len(docs):,} funding rate docs")

fr = pd.DataFrame(docs)
fr["ts"] = pd.to_datetime(fr["timestamp_utc"], unit="ms", utc=True)
fr = fr.drop(columns=["timestamp_utc"])

# Round to nearest hour to align
fr["ts"] = fr["ts"].dt.round("1h")
# Remove duplicates (keep last per coin/ts)
fr = fr.sort_values("ts").drop_duplicates(subset=["coin", "ts"], keep="last")
fr = fr.set_index(["ts", "coin"])["funding_rate"].unstack("coin")

# Reindex to full hourly grid
ts_start = fr.index.min()
ts_end   = fr.index.max()
full_idx  = pd.date_range(ts_start, ts_end, freq="1h", tz="UTC")
fr = fr.reindex(full_idx)

print(f"  Date range : {ts_start.date()} → {ts_end.date()}")
print(f"  Grid hours : {len(full_idx):,}")
print(f"  Coins      : {fr.shape[1]}")
print(f"  Avg NaN%   : {fr.isna().mean().mean() * 100:.1f}%")

# ── 2. Compute 72h Rolling Z-Score ──────────────────────────────────────────
print("\n[Phase 2] Computing rolling 72h z-scores …")

# Forward-fill short gaps (up to 3h), then compute rolling stats
fr_ffill = fr.ffill(limit=3)

# CRITICAL: shift(1) so current bar's funding is NOT self-included in z-score.
# Without this, extreme values attenuate their own z-score (Codex challenge finding 2026-05-06).
fr_shifted = fr_ffill.shift(1)
roll_mean = fr_shifted.rolling(ZSCORE_WINDOW, min_periods=48).mean()
roll_std  = fr_shifted.rolling(ZSCORE_WINDOW, min_periods=48).std()

# Avoid division by zero: if std < 1e-10, set to 1e-10
roll_std = roll_std.clip(lower=1e-10)

# Z-score uses CURRENT value vs PRIOR window stats (proper out-of-sample)
zscore_df = (fr_ffill - roll_mean) / roll_std

print(f"  Z-score matrix shape: {zscore_df.shape}")
print(f"  Z-score range: {zscore_df.stack().min():.2f} – {zscore_df.stack().max():.2f}")
print(f"  Rows with any z > {Z_THRESHOLD}: {(zscore_df > Z_THRESHOLD).any(axis=1).sum():,}")

# Drop the first ZSCORE_WINDOW hours (warm-up)
zscore_df = zscore_df.iloc[ZSCORE_WINDOW:]
fr_ffill  = fr_ffill.iloc[ZSCORE_WINDOW:]

# ── 3. Load Candle Data for Forward Returns ──────────────────────────────────
print("\n[Phase 3] Loading 1h candle data for forward returns …")

candle_coins = candles_coll.distinct("coin")
overlap_coins = sorted(set(qual_coins) & set(candle_coins))
missing_candles = sorted(set(qual_coins) - set(candle_coins))
print(f"  Qualifying coins with candle data : {len(overlap_coins)}")
print(f"  Missing candles (will be excluded): {len(missing_candles)}")
if missing_candles:
    print(f"    {missing_candles}")

candle_docs = list(candles_coll.find(
    {"coin": {"$in": overlap_coins}, "interval": "1h"},
    {"_id": 0, "coin": 1, "timestamp_utc": 1, "close": 1, "open": 1}
))
print(f"  Loaded {len(candle_docs):,} candle docs")

candles = pd.DataFrame(candle_docs)
candles["ts"] = pd.to_datetime(candles["timestamp_utc"], unit="ms", utc=True)
candles = candles.drop(columns=["timestamp_utc"])
candles = candles.sort_values("ts").drop_duplicates(subset=["coin", "ts"], keep="last")

close_px = candles.pivot(index="ts", columns="coin", values="close")
close_px = close_px.reindex(full_idx[ZSCORE_WINDOW:])
close_px = close_px.ffill(limit=2)

print(f"  Candle date range: {close_px.index.min().date()} → {close_px.index.max().date()}")
print(f"  Candle coverage % per coin (sample):")
coverage_pct = close_px[overlap_coins].notna().mean().sort_values(ascending=False)
for coin, pct in coverage_pct.head(10).items():
    print(f"    {coin:12s}: {pct*100:.1f}%")

# Align zscore_df to coins with candle data only
zscore_aligned = zscore_df[overlap_coins]

# Compute 8h forward log-return: log(close[t+8] / close[t])
# Entry at close of signal bar t, exit at close of t+HOLD_HOURS
# SHORT => return = -(forward_return)
fwd_close = close_px.shift(-HOLD_HOURS)
log_ret   = np.log(fwd_close / close_px)  # long return
short_ret = -log_ret                        # short return

# ── 4. Signal Generation — Non-Overlapping ──────────────────────────────────
print(f"\n[Phase 4] Generating signals (z > {Z_THRESHOLD}, top-N, {HOLD_HOURS}h hold, {COOLDOWN_HOURS}h cooldown) …")

def generate_trades(zscore_mat: pd.DataFrame,
                    short_ret_mat: pd.DataFrame,
                    top_n: int,
                    z_thresh: float,
                    cooldown_h: int,
                    hold_h: int) -> pd.DataFrame:
    """
    Generate non-overlapping SHORT trades.
    At each hour:
      1. Rank coins by z-score descending.
      2. Take top-N that exceed z_thresh AND are not in cooldown.
      3. Record trade; apply cooldown for cooldown_h after entry.
    Returns DataFrame of trades.
    """
    trades = []
    # Track when each coin is free to trade again
    next_free = {coin: pd.Timestamp.min.tz_localize("UTC") for coin in zscore_mat.columns}

    for ts, row in zscore_mat.iterrows():
        # Only consider coins above threshold
        candidates = row[row > z_thresh].sort_values(ascending=False)
        if candidates.empty:
            continue

        selected = 0
        for coin, z in candidates.items():
            if selected >= top_n:
                break
            # Skip if in cooldown
            if ts < next_free[coin]:
                continue
            # Get forward return
            if ts not in short_ret_mat.index or pd.isna(short_ret_mat.loc[ts, coin]):
                continue
            ret = short_ret_mat.loc[ts, coin]

            trades.append({
                "ts_entry": ts,
                "ts_exit":  ts + pd.Timedelta(hours=hold_h),
                "coin": coin,
                "z_score": z,
                "raw_ret": ret,
            })
            next_free[coin] = ts + pd.Timedelta(hours=cooldown_h)
            selected += 1

    return pd.DataFrame(trades)


def bootstrap_ci(returns: np.ndarray, n_boot: int = 10_000, ci: float = 0.95) -> tuple:
    """Bootstrap confidence interval for mean return."""
    if len(returns) < 5:
        return (np.nan, np.nan)
    rng = np.random.default_rng(42)
    boot_means = [rng.choice(returns, size=len(returns), replace=True).mean()
                  for _ in range(n_boot)]
    lo = np.percentile(boot_means, (1 - ci) / 2 * 100)
    hi = np.percentile(boot_means, (1 + ci) / 2 * 100)
    return (lo, hi)


FEE_RT = FEE_RT_BPS / 10_000

results_summary = []

for top_n in TOP_N_LIST:
    print(f"\n  --- Top-{top_n} portfolio ---")
    trades = generate_trades(
        zscore_aligned, short_ret_mat=short_ret,
        top_n=top_n, z_thresh=Z_THRESHOLD,
        cooldown_h=COOLDOWN_HOURS, hold_h=HOLD_HOURS
    )

    if trades.empty:
        print(f"    No trades generated.")
        results_summary.append({"top_n": top_n, "n_trades": 0})
        continue

    trades["net_ret"] = trades["raw_ret"] - FEE_RT

    n = len(trades)
    mean_r   = trades["net_ret"].mean()
    median_r = trades["net_ret"].median()
    win_rate = (trades["net_ret"] > 0).mean()
    t_stat, p_val = stats.ttest_1samp(trades["net_ret"], 0)
    ci_lo, ci_hi = bootstrap_ci(trades["net_ret"].values)

    print(f"    Trades         : {n:,}")
    print(f"    Date range     : {trades['ts_entry'].min().date()} → {trades['ts_entry'].max().date()}")
    print(f"    Mean net ret   : {mean_r*100:+.4f}%")
    print(f"    Median net ret : {median_r*100:+.4f}%")
    print(f"    Win rate       : {win_rate*100:.1f}%")
    print(f"    t-stat         : {t_stat:.3f}")
    print(f"    p-value        : {p_val:.4f}")
    print(f"    95% CI (boot)  : [{ci_lo*100:+.4f}%, {ci_hi*100:+.4f}%]")
    print(f"    Annualised ret : {mean_r * (365*24/HOLD_HOURS)*100:+.2f}%  (naive, no compounding)")

    # Coin distribution
    coin_counts = trades["coin"].value_counts()
    print(f"\n    Top-10 coins selected:")
    for coin, cnt in coin_counts.head(10).items():
        pct = cnt / n * 100
        print(f"      {coin:12s}: {cnt:4d} times ({pct:.1f}%)")

    is_fartcoin_dominant = coin_counts.iloc[0] == coin_counts.get("FARTCOIN", 0)
    fartcoin_share = coin_counts.get("FARTCOIN", 0) / n * 100
    print(f"\n    FARTCOIN share  : {fartcoin_share:.1f}%")

    results_summary.append({
        "top_n":        top_n,
        "n_trades":     n,
        "mean_net_ret": mean_r,
        "median_ret":   median_r,
        "win_rate":     win_rate,
        "t_stat":       t_stat,
        "p_val":        p_val,
        "ci_lo":        ci_lo,
        "ci_hi":        ci_hi,
        "fartcoin_pct": fartcoin_share,
    })

    # ── Regime Breakdown ──
    print(f"\n    Regime breakdown (BTC 8h forward return quintiles):")
    if "BTC" in short_ret.columns:
        btc_ret = short_ret["BTC"].reindex(trades["ts_entry"])
        trades["btc_fwd_ret"] = btc_ret.values
        trades_reg = trades.dropna(subset=["btc_fwd_ret"])
        if len(trades_reg) > 50:
            trades_reg = trades_reg.copy()
            trades_reg["regime"] = pd.qcut(trades_reg["btc_fwd_ret"], 5,
                                           labels=["Q1(bear)", "Q2", "Q3", "Q4", "Q5(bull)"])
            reg_stats = trades_reg.groupby("regime")["net_ret"].agg(
                ["mean", "count", lambda x: (x > 0).mean()]
            ).rename(columns={"mean": "mean_ret", "count": "n", "<lambda_0>": "win_rate"})
            for regime, row2 in reg_stats.iterrows():
                print(f"      {str(regime):12s}: n={row2['n']:4.0f}, "
                      f"mean={row2['mean_ret']*100:+.4f}%, "
                      f"wr={row2['win_rate']*100:.1f}%")
        else:
            print("      (not enough data for regime breakdown)")
    else:
        print("      (BTC not in candle data)")

# ── 5. Multiple Hypothesis Correction ───────────────────────────────────────
print("\n[Phase 5] Multiple Hypothesis Correction (Benjamini-Hochberg) …")
valid_results = [r for r in results_summary if "p_val" in r and not np.isnan(r.get("p_val", np.nan))]
if len(valid_results) >= 2:
    p_vals = np.array([r["p_val"] for r in valid_results])
    n_tests = len(p_vals)
    # BH procedure
    sorted_idx = np.argsort(p_vals)
    sorted_p   = p_vals[sorted_idx]
    bh_threshold = np.arange(1, n_tests + 1) / n_tests * 0.05  # FDR = 5%
    reject = sorted_p <= bh_threshold

    print(f"  Tests: {n_tests} (Top-N portfolios)")
    for i, idx in enumerate(sorted_idx):
        r = valid_results[idx]
        status = "REJECT H0 ✓" if reject[i] else "fail to reject"
        print(f"  Top-{r['top_n']}: p={r['p_val']:.4f}, BH-threshold={bh_threshold[i]:.4f} → {status}")
else:
    print("  (Not enough valid tests for BH correction)")

# ── 6. Baseline Comparison — FARTCOIN-Only ──────────────────────────────────
print("\n[Phase 6] Baseline: FARTCOIN-only strategy (single-coin) …")
if "FARTCOIN" in zscore_aligned.columns:
    fart_z    = zscore_aligned[["FARTCOIN"]]
    fart_ret  = short_ret[["FARTCOIN"]] if "FARTCOIN" in short_ret.columns else None

    if fart_ret is not None:
        fart_trades = generate_trades(
            fart_z, short_ret_mat=fart_ret,
            top_n=1, z_thresh=Z_THRESHOLD,
            cooldown_h=COOLDOWN_HOURS, hold_h=HOLD_HOURS
        )
        if not fart_trades.empty:
            fart_trades["net_ret"] = fart_trades["raw_ret"] - FEE_RT
            fn  = len(fart_trades)
            fmr = fart_trades["net_ret"].mean()
            fwr = (fart_trades["net_ret"] > 0).mean()
            ft, fp = stats.ttest_1samp(fart_trades["net_ret"], 0)
            fci = bootstrap_ci(fart_trades["net_ret"].values)
            print(f"  FARTCOIN-only:")
            print(f"    Trades     : {fn:,}")
            print(f"    Mean net   : {fmr*100:+.4f}%")
            print(f"    Win rate   : {fwr*100:.1f}%")
            print(f"    t-stat     : {ft:.3f}")
            print(f"    p-value    : {fp:.4f}")
            print(f"    95% CI     : [{fci[0]*100:+.4f}%, {fci[1]*100:+.4f}%]")
        else:
            print("  FARTCOIN: no trades generated.")
    else:
        print("  FARTCOIN: no candle data.")
else:
    print("  FARTCOIN not in qualifying coins.")

# ── 7. Summary Table ──────────────────────────────────────────────────────────
print("\n" + "=" * 72)
print("SUMMARY TABLE")
print("=" * 72)
print(f"{'Portfolio':<12} {'Trades':>7} {'Mean%':>8} {'WinRate':>8} {'t-stat':>8} {'p-val':>7} {'95% CI':>20} {'FART%':>7}")
print("-" * 72)
for r in results_summary:
    if r.get("n_trades", 0) == 0:
        print(f"Top-{r['top_n']:<8} {'0':>7} {'—':>8} {'—':>8} {'—':>8} {'—':>7} {'—':>20} {'—':>7}")
        continue
    ci_str = f"[{r['ci_lo']*100:+.3f}%, {r['ci_hi']*100:+.3f}%]"
    print(f"Top-{r['top_n']:<8} {r['n_trades']:>7,} "
          f"{r['mean_net_ret']*100:>+8.4f} "
          f"{r['win_rate']*100:>8.1f} "
          f"{r['t_stat']:>8.3f} "
          f"{r['p_val']:>7.4f} "
          f"{ci_str:>20} "
          f"{r['fartcoin_pct']:>7.1f}")

# ── 8. Verdict ───────────────────────────────────────────────────────────────
print("\n" + "=" * 72)
print("VERDICT")
print("=" * 72)

any_significant = any(r.get("p_val", 1.0) < 0.05 and r.get("mean_net_ret", 0) > 0
                      for r in results_summary if r.get("n_trades", 0) > 0)

if any_significant:
    print("SIGNAL CANDIDATE: At least one portfolio shows positive mean return with p < 0.05.")
    print("Proceed to Phase 1 (parameter sensitivity) and Phase 2 (walk-forward) before deployment.")
else:
    print("SIGNAL DEAD or MARGINAL: No portfolio clears both positive mean return AND p < 0.05.")
    print("Do NOT proceed to deployment. Consider:")
    print("  - Alternative thresholds or holding periods")
    print("  - Filtering by market regime (BTC trend)")
    print("  - Using funding absolute level instead of z-score")
    print("  - Cross-sectional rank normalization instead of absolute z-score")

print("\nDone.")
