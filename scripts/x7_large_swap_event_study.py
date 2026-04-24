#!/usr/bin/env python3
"""
X7 Large DEX Swap Event Study
==============================
Tests whether large DEX swap hours predict short-term CEX perp price moves.

Methodology:
1. Identify "large swap hours" from hourly DEX data (z-score > 3 on buy/sell USD,
   plus top 5% by |dex_flow_imbalance|).
2. For each event, load 5s Bybit data for that hour + next hour.
3. Compute intra-hour return profile: first 10 min, 10-30 min, 30-60 min.
4. Test if signal propagates slowly (tradeable window exists).

Assets: ETH, SOL, BTC
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

# ── Paths ──────────────────────────────────────────────────────────────────
BASE = Path("/Users/hermes/quants-lab")
DEX_PATH = BASE / "app/data/cache/onchain_x7_extended/dex_365d_raw.csv"
ARB_DIR = BASE / "app/data/cache/arb_trades"

ASSETS = {
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BTC": "BTCUSDT",
}

# ── Load DEX data ─────────────────────────────────────────────────────────
print("=" * 80)
print("X7 LARGE DEX SWAP EVENT STUDY")
print("=" * 80)

dex = pd.read_csv(DEX_PATH, parse_dates=["hour"])
dex["hour"] = pd.to_datetime(dex["hour"], utc=True)
print(f"\nDEX data: {dex.shape[0]} rows, {dex['asset'].nunique()} assets")
print(f"Date range: {dex['hour'].min()} to {dex['hour'].max()}")

# ── Load 5s CEX data ──────────────────────────────────────────────────────
cex_data = {}
for asset, symbol in ASSETS.items():
    fpath = ARB_DIR / symbol / f"{symbol}_5s_merged_14d.parquet"
    if fpath.exists():
        df = pd.read_parquet(fpath)
        cex_data[asset] = df
        print(f"{asset} 5s data: {df.shape[0]} rows, {df.index.min()} to {df.index.max()}")

# ── Identify large swap events ────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 1: IDENTIFY LARGE SWAP EVENTS")
print("=" * 80)

all_events = []

for asset in ASSETS:
    ad = dex[dex["asset"] == asset].copy().sort_values("hour").reset_index(drop=True)

    # Z-scores over 72h rolling window
    for col in ["buy_usd", "sell_usd"]:
        roll_mean = ad[col].rolling(72, min_periods=24).mean()
        roll_std = ad[col].rolling(72, min_periods=24).std()
        ad[f"{col}_zscore"] = (ad[col] - roll_mean) / roll_std.replace(0, np.nan)

    # Absolute flow imbalance z-score
    ad["abs_imbalance"] = ad["dex_flow_imbalance"].abs()
    imb_mean = ad["abs_imbalance"].rolling(72, min_periods=24).mean()
    imb_std = ad["abs_imbalance"].rolling(72, min_periods=24).std()
    ad["imbalance_zscore"] = (ad["abs_imbalance"] - imb_mean) / imb_std.replace(0, np.nan)

    # Events: z-score > 3 on buy OR sell, OR top 5% by |imbalance|
    threshold_buy = ad["buy_usd_zscore"] > 3
    threshold_sell = ad["sell_usd_zscore"] > 3
    threshold_imb = ad["imbalance_zscore"] > 2  # ~top 2-3%

    events = ad[threshold_buy | threshold_sell | threshold_imb].copy()
    events["asset"] = asset

    # Determine direction: positive imbalance = net buy pressure
    events["dex_direction"] = np.where(events["dex_flow_imbalance"] > 0, "BUY", "SELL")

    # Strength metric: max z-score across all signals
    events["signal_strength"] = events[
        ["buy_usd_zscore", "sell_usd_zscore", "imbalance_zscore"]
    ].max(axis=1)

    all_events.append(events)
    print(f"\n{asset}: {len(events)} large swap events out of {len(ad)} hours")
    print(f"  Buy spikes (z>3): {threshold_buy.sum()}")
    print(f"  Sell spikes (z>3): {threshold_sell.sum()}")
    print(f"  Imbalance spikes (z>2): {threshold_imb.sum()}")

events_df = pd.concat(all_events, ignore_index=True)
print(f"\nTotal events across all assets: {len(events_df)}")

# ── Filter to CEX data overlap period ─────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 2: MATCH EVENTS TO 5s CEX DATA")
print("=" * 80)

# CEX data covers Apr 4-17 2026
cex_start = pd.Timestamp("2026-04-04", tz="UTC")
cex_end = pd.Timestamp("2026-04-17 23:59:59", tz="UTC")

overlap_events = events_df[
    (events_df["hour"] >= cex_start) & (events_df["hour"] <= cex_end)
].copy()

print(f"\nEvents in CEX overlap window ({cex_start.date()} to {cex_end.date()}): {len(overlap_events)}")
for asset in ASSETS:
    n = len(overlap_events[overlap_events["asset"] == asset])
    print(f"  {asset}: {n} events")

# ── Intra-hour return analysis ────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 3: INTRA-HOUR RETURN PROFILES")
print("=" * 80)

results = []

for _, event in overlap_events.iterrows():
    asset = event["asset"]
    if asset not in cex_data:
        continue

    cex = cex_data[asset]
    hour_start = event["hour"]
    hour_end = hour_start + pd.Timedelta(hours=1)
    next_hour_end = hour_start + pd.Timedelta(hours=2)

    # Get 5s data for the event hour
    mask_hour = (cex.index >= hour_start) & (cex.index < hour_end)
    hour_data = cex.loc[mask_hour]

    if len(hour_data) < 10:
        continue

    # Get 5s data for next hour
    mask_next = (cex.index >= hour_end) & (cex.index < next_hour_end)
    next_hour_data = cex.loc[mask_next]

    # Use Bybit close prices
    price_col = "bb_close"
    open_price = hour_data[price_col].iloc[0]

    # Return at different intervals within the event hour
    intervals = {
        "0_10min": (0, 10),
        "10_20min": (10, 20),
        "20_30min": (20, 30),
        "30_60min": (30, 60),
    }

    row = {
        "asset": asset,
        "hour": hour_start,
        "dex_direction": event["dex_direction"],
        "signal_strength": event["signal_strength"],
        "dex_flow_imbalance": event["dex_flow_imbalance"],
        "buy_usd": event["buy_usd"],
        "sell_usd": event["sell_usd"],
    }

    for label, (start_min, end_min) in intervals.items():
        t_start = hour_start + pd.Timedelta(minutes=start_min)
        t_end = hour_start + pd.Timedelta(minutes=end_min)
        mask = (cex.index >= t_start) & (cex.index < t_end)
        segment = cex.loc[mask]

        if len(segment) > 0:
            seg_open = segment[price_col].iloc[0]
            seg_close = segment[price_col].iloc[-1]
            seg_return_bps = (seg_close / seg_open - 1) * 10000
            seg_high = segment[price_col].max()
            seg_low = segment[price_col].min()
            seg_range_bps = (seg_high / seg_low - 1) * 10000
        else:
            seg_return_bps = np.nan
            seg_range_bps = np.nan

        row[f"return_{label}_bps"] = seg_return_bps
        row[f"range_{label}_bps"] = seg_range_bps

    # Full hour return
    close_price = hour_data[price_col].iloc[-1]
    row["full_hour_return_bps"] = (close_price / open_price - 1) * 10000

    # Next hour return
    if len(next_hour_data) > 0:
        next_open = next_hour_data[price_col].iloc[0]
        next_close = next_hour_data[price_col].iloc[-1]
        row["next_hour_return_bps"] = (next_close / next_open - 1) * 10000
    else:
        row["next_hour_return_bps"] = np.nan

    # "Remaining return" if we detect the swap in first 10 min and enter
    t_10 = hour_start + pd.Timedelta(minutes=10)
    after_10 = cex.loc[(cex.index >= t_10) & (cex.index < hour_end)]
    if len(after_10) > 0:
        entry_price = after_10[price_col].iloc[0]
        exit_price = after_10[price_col].iloc[-1]
        row["remaining_50min_return_bps"] = (exit_price / entry_price - 1) * 10000
    else:
        row["remaining_50min_return_bps"] = np.nan

    # Directional alignment: does CEX move match DEX direction?
    if event["dex_direction"] == "BUY":
        row["aligned_full_hour"] = row["full_hour_return_bps"] > 0
        row["aligned_remaining"] = (row.get("remaining_50min_return_bps") or 0) > 0
        row["signed_remaining_bps"] = row.get("remaining_50min_return_bps", 0)
    else:
        row["aligned_full_hour"] = row["full_hour_return_bps"] < 0
        row["aligned_remaining"] = (row.get("remaining_50min_return_bps") or 0) < 0
        row["signed_remaining_bps"] = -(row.get("remaining_50min_return_bps", 0) or 0)

    results.append(row)

results_df = pd.DataFrame(results)
print(f"\nAnalyzable events: {len(results_df)}")

if len(results_df) == 0:
    print("NO EVENTS FOUND IN OVERLAP WINDOW. Cannot proceed.")
    exit(0)

# ── Results ───────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4: AGGREGATE RESULTS")
print("=" * 80)

# Per-asset summary
for asset in ASSETS:
    adf = results_df[results_df["asset"] == asset]
    if len(adf) == 0:
        print(f"\n{asset}: No events")
        continue

    print(f"\n{'─' * 60}")
    print(f"{asset}: {len(adf)} events")
    print(f"{'─' * 60}")

    # Direction breakdown
    buys = adf[adf["dex_direction"] == "BUY"]
    sells = adf[adf["dex_direction"] == "SELL"]
    print(f"  BUY events: {len(buys)}, SELL events: {len(sells)}")

    # Full hour alignment
    align_pct = adf["aligned_full_hour"].mean() * 100
    print(f"\n  Full hour direction alignment: {align_pct:.1f}% (50% = random)")

    # Remaining 50-min alignment (the tradeable window)
    remain_align = adf["aligned_remaining"].mean() * 100
    print(f"  Remaining 50-min alignment:    {remain_align:.1f}%")

    # Mean signed return (positive = profitable if trading the signal)
    mean_signed = adf["signed_remaining_bps"].mean()
    print(f"  Mean signed remaining return:  {mean_signed:.2f} bps")

    # Intra-hour return profile
    print(f"\n  Intra-hour return profile (mean |return| in bps):")
    for label in ["0_10min", "10_20min", "20_30min", "30_60min"]:
        col = f"return_{label}_bps"
        if col in adf.columns:
            mean_abs = adf[col].abs().mean()
            mean_raw = adf[col].mean()
            print(f"    {label:>10s}: mean={mean_raw:+.2f}, |mean|={mean_abs:.2f}")

    # Range (volatility) profile
    print(f"\n  Intra-hour range profile (mean range in bps):")
    for label in ["0_10min", "10_20min", "20_30min", "30_60min"]:
        col = f"range_{label}_bps"
        if col in adf.columns:
            mean_range = adf[col].mean()
            print(f"    {label:>10s}: {mean_range:.2f} bps")

    # Next hour continuation
    next_mean = adf["next_hour_return_bps"].mean()
    print(f"\n  Next hour mean return: {next_mean:+.2f} bps")

# ── Overall summary ───────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("OVERALL SUMMARY")
print("=" * 80)

align_full = results_df["aligned_full_hour"].mean() * 100
align_remain = results_df["aligned_remaining"].mean() * 100
mean_signed = results_df["signed_remaining_bps"].mean()
median_signed = results_df["signed_remaining_bps"].median()

print(f"\n  Total events analyzed: {len(results_df)}")
print(f"  Full hour alignment:      {align_full:.1f}% (50% = random)")
print(f"  50-min remaining align:   {align_remain:.1f}%")
print(f"  Mean signed return (50m): {mean_signed:+.2f} bps")
print(f"  Median signed return:     {median_signed:+.2f} bps")

# Statistical significance
from scipy import stats

if len(results_df) >= 5:
    t_stat, p_val = stats.ttest_1samp(results_df["signed_remaining_bps"].dropna(), 0)
    print(f"\n  t-stat: {t_stat:.3f}, p-value: {p_val:.4f}")
    if p_val < 0.05:
        print("  >>> STATISTICALLY SIGNIFICANT at 5% level")
    else:
        print("  >>> NOT statistically significant")

# ── Return distribution ───────────────────────────────────────────────────
print("\n" + "=" * 80)
print("RETURN DISTRIBUTION (signed remaining 50-min returns)")
print("=" * 80)

sr = results_df["signed_remaining_bps"].dropna()
print(f"\n  Count:  {len(sr)}")
print(f"  Mean:   {sr.mean():+.2f} bps")
print(f"  Std:    {sr.std():.2f} bps")
print(f"  Min:    {sr.min():+.2f} bps")
print(f"  25%:    {sr.quantile(0.25):+.2f} bps")
print(f"  Median: {sr.median():+.2f} bps")
print(f"  75%:    {sr.quantile(0.75):+.2f} bps")
print(f"  Max:    {sr.max():+.2f} bps")
print(f"  >0:     {(sr > 0).sum()}/{len(sr)} ({(sr > 0).mean()*100:.1f}%)")

# ── Signal strength buckets ──────────────────────────────────────────────
print("\n" + "=" * 80)
print("SIGNAL STRENGTH ANALYSIS")
print("=" * 80)

results_df["strength_bucket"] = pd.qcut(
    results_df["signal_strength"], q=3, labels=["weak", "medium", "strong"],
    duplicates="drop"
)

for bucket in ["weak", "medium", "strong"]:
    bdf = results_df[results_df["strength_bucket"] == bucket]
    if len(bdf) == 0:
        continue
    align = bdf["aligned_remaining"].mean() * 100
    mean_ret = bdf["signed_remaining_bps"].mean()
    print(f"\n  {bucket.upper()} signals ({len(bdf)} events):")
    print(f"    Alignment: {align:.1f}%, Mean return: {mean_ret:+.2f} bps")

# ── Conclusion ────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)

if align_remain > 55 and mean_signed > 2:
    print("\n  PROMISING: DEX large swaps predict CEX direction with tradeable lag.")
    print("  Next step: build real-time DEX swap detection via WebSocket/RPC.")
elif align_remain > 52:
    print("\n  WEAK SIGNAL: Slight edge but likely not tradeable after fees.")
    print("  The hourly aggregation destroys timing precision.")
elif align_remain < 48:
    print("\n  CONTRARIAN SIGNAL: CEX moves AGAINST DEX flow direction.")
    print("  Large DEX swaps may be retail noise that gets faded by MM.")
else:
    print("\n  NO SIGNAL: DEX large swap hours show no predictive power on CEX.")
    print("  Hourly aggregation is too coarse, or the signal doesn't exist.")

# ── CONTROL TEST: Random non-event hours ─────────────────────────────────
print("\n" + "=" * 80)
print("CONTROL TEST: RANDOM NON-EVENT HOURS")
print("=" * 80)
print("Running the same analysis on random hours (no DEX spike) as a sanity check.")

np.random.seed(42)
control_results = []

for asset in ASSETS:
    if asset not in cex_data:
        continue
    cex = cex_data[asset]

    # Get all hours in CEX range
    all_hours = pd.date_range(cex_start, cex_end, freq="h", tz="UTC")

    # Exclude event hours for this asset
    event_hours = set(overlap_events[overlap_events["asset"] == asset]["hour"].values)
    non_event_hours = [h for h in all_hours if h not in event_hours and h < cex_end - pd.Timedelta(hours=1)]

    # Sample same number as events
    n_events = len(results_df[results_df["asset"] == asset])
    if n_events == 0 or len(non_event_hours) < n_events:
        continue

    # Run 100 bootstrap samples
    for _ in range(100):
        sampled = np.random.choice(non_event_hours, size=n_events, replace=False)
        for hour_start in sampled:
            hour_start = pd.Timestamp(hour_start)
            hour_end = hour_start + pd.Timedelta(hours=1)

            mask = (cex.index >= hour_start) & (cex.index < hour_end)
            hour_data = cex.loc[mask]
            if len(hour_data) < 10:
                continue

            price_col = "bb_close"
            open_price = hour_data[price_col].iloc[0]
            close_price = hour_data[price_col].iloc[-1]
            full_ret = (close_price / open_price - 1) * 10000

            # Randomly assign direction (simulating random "signal")
            fake_dir = np.random.choice(["BUY", "SELL"])
            if fake_dir == "BUY":
                signed = full_ret
            else:
                signed = -full_ret

            # Remaining 50 min
            t_10 = hour_start + pd.Timedelta(minutes=10)
            after_10 = cex.loc[(cex.index >= t_10) & (cex.index < hour_end)]
            if len(after_10) > 0:
                entry = after_10[price_col].iloc[0]
                exit_ = after_10[price_col].iloc[-1]
                remain_ret = (exit_ / entry - 1) * 10000
                if fake_dir == "BUY":
                    signed_remain = remain_ret
                else:
                    signed_remain = -remain_ret
            else:
                signed_remain = 0

            control_results.append({
                "asset": asset,
                "signed_remaining_bps": signed_remain,
            })

ctrl_df = pd.DataFrame(control_results)
ctrl_mean = ctrl_df["signed_remaining_bps"].mean()
ctrl_align = (ctrl_df["signed_remaining_bps"] > 0).mean() * 100
ctrl_median = ctrl_df["signed_remaining_bps"].median()

print(f"\n  Control events: {len(ctrl_df)}")
print(f"  Control alignment: {ctrl_align:.1f}% (should be ~50%)")
print(f"  Control mean signed return: {ctrl_mean:+.2f} bps (should be ~0)")
print(f"  Control median signed return: {ctrl_median:+.2f} bps")

print(f"\n  SIGNAL vs CONTROL:")
print(f"    Signal alignment:  {align_remain:.1f}% vs Control: {ctrl_align:.1f}%")
print(f"    Signal mean:       {mean_signed:+.2f} bps vs Control: {ctrl_mean:+.2f} bps")
print(f"    Lift:              {mean_signed - ctrl_mean:+.2f} bps")

# ── Look-ahead bias check ────────────────────────────────────────────────
print("\n" + "=" * 80)
print("LOOK-AHEAD BIAS CHECK")
print("=" * 80)
print("The DEX hourly data aggregates the FULL hour. We don't know WHEN in the")
print("hour the large swap occurred. If the swap happened at minute 55, the")
print("'remaining 50 min' metric is meaningless.")
print()
print("Key question: does the signal come from the swap CAUSING the move,")
print("or from the move CAUSING the swap (reflexive)? This study cannot")
print("distinguish the two with hourly data.")
print()
print("To resolve: need real-time DEX swap data (mempool/block-level timestamps).")

# ── Alternative: full-hour-ahead test ─────────────────────────────────────
print("\n" + "=" * 80)
print("FORWARD-LOOKING TEST: NEXT HOUR RETURNS ONLY")
print("=" * 80)
print("This eliminates look-ahead bias entirely. Does a large-swap hour")
print("predict the NEXT hour's direction?")

for asset in ASSETS:
    adf = results_df[results_df["asset"] == asset]
    if len(adf) == 0:
        continue

    next_hr = adf["next_hour_return_bps"].dropna()
    if len(next_hr) == 0:
        continue

    # Signed by DEX direction
    signed_next = []
    for _, row in adf.iterrows():
        nhr = row["next_hour_return_bps"]
        if pd.isna(nhr):
            continue
        if row["dex_direction"] == "BUY":
            signed_next.append(nhr)
        else:
            signed_next.append(-nhr)

    signed_next = np.array(signed_next)
    align = (signed_next > 0).mean() * 100
    mean_ret = signed_next.mean()

    print(f"\n  {asset} ({len(signed_next)} events):")
    print(f"    Next-hour alignment: {align:.1f}%")
    print(f"    Mean signed return:  {mean_ret:+.2f} bps")

    if len(signed_next) >= 5:
        t, p = stats.ttest_1samp(signed_next, 0)
        print(f"    t-stat: {t:.3f}, p-value: {p:.4f}")

# All assets combined
all_signed_next = []
for _, row in results_df.iterrows():
    nhr = row["next_hour_return_bps"]
    if pd.isna(nhr):
        continue
    if row["dex_direction"] == "BUY":
        all_signed_next.append(nhr)
    else:
        all_signed_next.append(-nhr)

all_signed_next = np.array(all_signed_next)
print(f"\n  ALL ASSETS ({len(all_signed_next)} events):")
print(f"    Next-hour alignment: {(all_signed_next > 0).mean()*100:.1f}%")
print(f"    Mean signed return:  {all_signed_next.mean():+.2f} bps")
if len(all_signed_next) >= 5:
    t, p = stats.ttest_1samp(all_signed_next, 0)
    print(f"    t-stat: {t:.3f}, p-value: {p:.4f}")
    if p < 0.05:
        print("    >>> SIGNIFICANT: Large DEX swaps predict NEXT HOUR direction")
    else:
        print("    >>> NOT significant: No forward-looking edge")

print()
