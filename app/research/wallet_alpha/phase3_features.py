#!/usr/bin/env python3
"""
Phase 3: Feature Engineering for Wallet Alpha Research

Computes Copy Markout (CopyMO), edge decomposition, and per-wallet aggregate
features. Uses L2 mid-price data from MongoDB for markout computation.

CopyMO(h, L) = side * (mid(t+h) / mid(t+L) - 1)
Where L = 1s (our detection/execution latency).

Usage:
    python -m app.research.wallet_alpha.phase3_features
"""
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [phase3] %(levelname)s: %(message)s",
)
logger = logging.getLogger("phase3")

OUTPUT_DIR = Path("app/data/wallet_alpha")
EVENTS_PATH = OUTPUT_DIR / "events.parquet"
ROUND_TRIPS_PATH = OUTPUT_DIR / "round_trips.parquet"
FEATURES_PATH = OUTPUT_DIR / "wallet_features.parquet"
MARKOUT_EVENTS_PATH = OUTPUT_DIR / "events_with_markout.parquet"

# Copy trading parameters
LATENCY_MS = 1000  # 1s detection/execution latency
FEE_RT_BPS = 8.64  # Round-trip taker fees in bps
MARKOUT_HORIZONS_S = [5, 15, 30, 60, 120, 300, 600, 1800, 3600]

# MongoDB
MONGO_URI = "mongodb://localhost:27017/quants_lab"
L2_COLLECTION = "hyperliquid_l2_snapshots_1s"

# Fill-derived mid-prices (fallback)
FILL_MID_DIR = OUTPUT_DIR / "fill_midprices"


def load_mid_prices(coins: list[str]) -> dict[str, pd.DataFrame]:
    """Load 1-second mid-price series from MongoDB L2 snapshots.

    Returns dict: coin -> DataFrame with columns [timestamp_ms, mid_price].

    NOTE: L2 data only covers May 6-13 2026 (8 days). Events outside this range
    will not have markout data. This is documented and handled in feature computation.
    """
    logger.info(f"Loading L2 mid-prices for {len(coins)} coins from MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]
    col = db[L2_COLLECTION]

    mid_prices = {}
    for coin in coins:
        cursor = col.find(
            {"coin": coin},
            {"_id": 0, "timestamp_utc": 1, "best_bid": 1, "best_ask": 1, "mid_px": 1, "spread_bps": 1},
        ).sort("timestamp_utc", 1)

        records = list(cursor)
        if not records:
            continue

        df = pd.DataFrame(records)
        # Use pre-computed mid_px if available, otherwise compute
        if "mid_px" in df.columns and df["mid_px"].notna().any():
            df["mid_price"] = df["mid_px"]
        else:
            df["mid_price"] = (df["best_bid"] + df["best_ask"]) / 2

        if "spread_bps" not in df.columns or df["spread_bps"].isna().all():
            df["spread_bps"] = (df["best_ask"] - df["best_bid"]) / df["mid_price"] * 10000

        df["timestamp_ms"] = df["timestamp_utc"].astype(int)

        # Deduplicate by timestamp (keep last)
        df = df.drop_duplicates(subset=["timestamp_ms"], keep="last")
        df = df.sort_values("timestamp_ms").reset_index(drop=True)

        mid_prices[coin] = df[["timestamp_ms", "mid_price", "spread_bps"]]

    loaded = len(mid_prices)
    logger.info(f"Loaded L2 data for {loaded}/{len(coins)} coins")
    if loaded > 0:
        # Report coverage window
        all_ts = pd.concat([mp["timestamp_ms"] for mp in mid_prices.values()])
        from datetime import datetime
        t_min = datetime.fromtimestamp(all_ts.min() / 1000)
        t_max = datetime.fromtimestamp(all_ts.max() / 1000)
        logger.info(f"L2 coverage: {t_min} to {t_max}")

    client.close()
    return mid_prices


def load_fill_derived_midprices(coins: list[str]) -> dict[str, pd.DataFrame]:
    """Load fill-derived mid-prices as fallback for dates without L2 data.

    Returns dict: coin -> DataFrame[timestamp_ms, mid_price, spread_bps].
    spread_bps is estimated as 2x the median spread for that coin from L2 data,
    or a conservative default of 5 bps.
    """
    if not FILL_MID_DIR.exists():
        logger.info("No fill-derived mid-prices found. Run build_fill_midprice first.")
        return {}

    logger.info("Loading fill-derived mid-prices...")
    fill_files = sorted(FILL_MID_DIR.glob("*.parquet"))
    if not fill_files:
        return {}

    mid_prices = {}
    for ff in fill_files:
        df = pd.read_parquet(ff)
        for coin in df["coin"].unique():
            if coin not in coins:
                continue
            cdf = df[df["coin"] == coin][["timestamp_ms", "mid_price"]].copy()
            cdf["spread_bps"] = 5.0  # conservative default
            if coin not in mid_prices:
                mid_prices[coin] = cdf
            else:
                mid_prices[coin] = pd.concat([mid_prices[coin], cdf], ignore_index=True)

    # Sort and deduplicate
    for coin in mid_prices:
        mid_prices[coin] = (
            mid_prices[coin]
            .sort_values("timestamp_ms")
            .drop_duplicates(subset=["timestamp_ms"], keep="last")
            .reset_index(drop=True)
        )

    logger.info(f"Loaded fill-derived data for {len(mid_prices)} coins")
    return mid_prices


def merge_midprice_sources(
    l2_prices: dict[str, pd.DataFrame],
    fill_prices: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    """Merge L2 and fill-derived mid-prices, preferring L2 where available.

    Returns:
    - merged dict: coin -> DataFrame[timestamp_ms, mid_price, spread_bps, source]
    - source_map: coin -> "l2" | "fill" | "mixed" (for QA tracking)
    """
    merged = {}
    source_map = {}
    all_coins = set(l2_prices.keys()) | set(fill_prices.keys())

    for coin in all_coins:
        has_l2 = coin in l2_prices
        has_fill = coin in fill_prices

        if has_l2 and has_fill:
            # Use L2 where available, fill-derived for gaps
            l2 = l2_prices[coin].copy()
            l2["source"] = "l2"
            fill = fill_prices[coin].copy()
            fill["source"] = "fill"

            # L2 timestamps
            l2_ts = set(l2["timestamp_ms"].values)
            # Only use fill data for timestamps NOT in L2
            fill_gap = fill[~fill["timestamp_ms"].isin(l2_ts)]

            combined = pd.concat([l2, fill_gap], ignore_index=True)
            combined = combined.sort_values("timestamp_ms").reset_index(drop=True)
            merged[coin] = combined
            source_map[coin] = "mixed"
        elif has_l2:
            l2 = l2_prices[coin].copy()
            l2["source"] = "l2"
            merged[coin] = l2
            source_map[coin] = "l2"
        elif has_fill:
            fill = fill_prices[coin].copy()
            fill["source"] = "fill"
            merged[coin] = fill
            source_map[coin] = "fill"

    return merged, source_map


def compute_markout_for_events(
    events: pd.DataFrame,
    mid_prices: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Compute raw and copy markout at multiple horizons for each event.

    For each event at time t:
    - raw_markout_Xs = side * (mid(t+X) - mid(t)) / mid(t)
    - copy_markout_Xs = side * (mid(t+X) - mid(t+L)) / mid(t+L)  where L=1s
    - latency_decay = raw_markout_Xs - copy_markout_Xs (alpha lost in first second)

    Uses merge_asof for efficient time-series lookup.
    """
    if len(events) == 0:
        return events

    events = events.copy()

    # Initialize markout columns
    for h in MARKOUT_HORIZONS_S:
        events[f"raw_mo_{h}s"] = np.nan
        events[f"copy_mo_{h}s"] = np.nan
    events["mid_at_fill"] = np.nan
    events["mid_at_1s"] = np.nan
    events["spread_at_fill_bps"] = np.nan
    events["has_l2"] = False

    # Process per coin
    coins_with_data = 0
    coins_without = 0

    for coin in events["coin"].unique():
        if coin not in mid_prices:
            coins_without += 1
            continue

        mid_df = mid_prices[coin]
        coin_mask = events["coin"] == coin
        coin_events = events.loc[coin_mask].copy()

        if len(coin_events) == 0:
            continue

        coins_with_data += 1

        # merge_asof to find mid price at event time
        coin_events = coin_events.sort_values("start_ts")
        mid_df = mid_df.sort_values("timestamp_ms")

        # Mid at fill time (t+0)
        merged = pd.merge_asof(
            coin_events[["start_ts"]].rename(columns={"start_ts": "timestamp_ms"}),
            mid_df,
            on="timestamp_ms",
            direction="nearest",
            tolerance=2000,  # 2s tolerance
        )
        events.loc[coin_mask, "mid_at_fill"] = merged["mid_price"].values
        events.loc[coin_mask, "spread_at_fill_bps"] = merged["spread_bps"].values
        events.loc[coin_mask, "has_l2"] = merged["mid_price"].notna().values

        # Mid at t+L (1s latency)
        lookup_ts = coin_events["start_ts"].values + LATENCY_MS
        lookup_df = pd.DataFrame({"timestamp_ms": lookup_ts})
        merged_1s = pd.merge_asof(
            lookup_df,
            mid_df,
            on="timestamp_ms",
            direction="nearest",
            tolerance=2000,
        )
        events.loc[coin_mask, "mid_at_1s"] = merged_1s["mid_price"].values

        # Markout at each horizon
        for h in MARKOUT_HORIZONS_S:
            lookup_ts_h = coin_events["start_ts"].values + (h * 1000)
            lookup_df_h = pd.DataFrame({"timestamp_ms": lookup_ts_h})
            merged_h = pd.merge_asof(
                lookup_df_h,
                mid_df,
                on="timestamp_ms",
                direction="nearest",
                tolerance=2000,
            )
            mid_h = merged_h["mid_price"].values
            mid_0 = events.loc[coin_mask, "mid_at_fill"].values
            mid_1s = events.loc[coin_mask, "mid_at_1s"].values

            # Side sign: Buy=+1, Sell=-1
            side_sign = np.where(coin_events["side"].values == "Buy", 1.0, -1.0)

            # Raw markout: from fill time
            with np.errstate(divide="ignore", invalid="ignore"):
                raw_mo = side_sign * (mid_h / mid_0 - 1) * 10000  # in bps
                events.loc[coin_mask, f"raw_mo_{h}s"] = raw_mo

                # Copy markout: from 1s after fill (our entry point)
                copy_mo = side_sign * (mid_h / mid_1s - 1) * 10000  # in bps
                events.loc[coin_mask, f"copy_mo_{h}s"] = copy_mo

    logger.info(f"Markout computed: {coins_with_data} coins with L2 data, {coins_without} without")

    # Latency decay: alpha lost in first second
    events["latency_decay_bps"] = events["raw_mo_5s"] - events["copy_mo_5s"]

    return events


def compute_wallet_features(events: pd.DataFrame, round_trips: pd.DataFrame) -> pd.DataFrame:
    """Compute per-wallet aggregate features for scoring.

    Features computed on OPENING events only for alpha metrics.
    All events used for activity and copyability metrics.
    """
    if len(events) == 0:
        return pd.DataFrame()

    # Separate opens for alpha metrics
    opens = events[events["event_type"] == "OPEN"].copy()
    opens_with_l2 = opens[opens["has_l2"]].copy()

    logger.info(f"Computing features: {events['wallet'].nunique():,} wallets, "
                f"{len(opens):,} open events ({len(opens_with_l2):,} with L2)")

    # --- Alpha metrics (on opens with L2 data) ---
    alpha_features = []
    for wallet, wdf in opens_with_l2.groupby("wallet"):
        feats = {"wallet": wallet}

        # Copy markout at each horizon
        for h in MARKOUT_HORIZONS_S:
            col = f"copy_mo_{h}s"
            vals = wdf[col].dropna()
            if len(vals) > 0:
                feats[f"copy_mo_{h}s_mean"] = vals.mean()
                feats[f"copy_mo_{h}s_median"] = vals.median()
                feats[f"copy_mo_{h}s_winrate"] = (vals > 0).mean()
            else:
                feats[f"copy_mo_{h}s_mean"] = np.nan
                feats[f"copy_mo_{h}s_median"] = np.nan
                feats[f"copy_mo_{h}s_winrate"] = np.nan

        # Net edge (60s copy markout minus RT fees)
        mo_60s = wdf["copy_mo_60s"].dropna()
        feats["net_edge_bps"] = mo_60s.mean() - FEE_RT_BPS if len(mo_60s) > 0 else np.nan

        # Markout curve slope (alpha persistence)
        horizons_log = np.log([5, 15, 30, 60, 120, 300, 600, 1800, 3600])
        means = [wdf[f"copy_mo_{h}s"].dropna().mean() for h in MARKOUT_HORIZONS_S]
        valid = [(h, m) for h, m in zip(horizons_log, means) if not np.isnan(m)]
        if len(valid) >= 3:
            x = np.array([v[0] for v in valid])
            y = np.array([v[1] for v in valid])
            feats["markout_curve_slope"] = np.polyfit(x, y, 1)[0]
        else:
            feats["markout_curve_slope"] = np.nan

        # T-statistics
        # Event-level t-stat (secondary)
        if len(mo_60s) > 1:
            feats["event_t_stat"] = mo_60s.mean() / (mo_60s.std() / np.sqrt(len(mo_60s)))
        else:
            feats["event_t_stat"] = np.nan

        # Daily t-stat (primary, more honest)
        wdf_daily = wdf.copy()
        wdf_daily["date"] = pd.to_datetime(wdf_daily["start_ts"], unit="ms").dt.date
        daily_pnl = wdf_daily.groupby("date")["copy_mo_60s"].mean().dropna()
        if len(daily_pnl) > 1:
            feats["daily_t_stat"] = daily_pnl.mean() / (daily_pnl.std() / np.sqrt(len(daily_pnl)))
        else:
            feats["daily_t_stat"] = np.nan

        # Latency decay
        ld = wdf["latency_decay_bps"].dropna()
        feats["latency_decay_bps"] = ld.mean() if len(ld) > 0 else np.nan

        # Copyability factor
        raw_60 = wdf["raw_mo_60s"].dropna().mean()
        copy_60 = wdf["copy_mo_60s"].dropna().mean()
        if raw_60 and abs(raw_60) > 0.01:
            feats["copyability_factor"] = copy_60 / raw_60
        else:
            feats["copyability_factor"] = np.nan

        # Spread at fill
        feats["avg_spread_bps"] = wdf["spread_at_fill_bps"].dropna().mean()

        alpha_features.append(feats)

    alpha_df = pd.DataFrame(alpha_features)

    # --- Activity metrics (all events) ---
    activity = events.groupby("wallet").agg(
        event_count=("wallet", "count"),
        open_event_count=("event_type", lambda x: (x == "OPEN").sum()),
        total_notional=("total_notional", "sum"),
        total_fees=("total_fee", "sum"),
        coins_traded=("coin", "nunique"),
        active_days=("date", "nunique"),
        first_ts=("start_ts", "min"),
        last_ts=("start_ts", "max"),
    ).reset_index()
    activity["events_per_day"] = activity["event_count"] / activity["active_days"].clip(lower=1)
    activity["avg_event_notional"] = activity["total_notional"] / activity["event_count"].clip(lower=1)

    # --- Copyability metrics (all events) ---
    copy_metrics = events.groupby("wallet").agg(
        burst_ratio=("is_burst", "mean"),
        single_fill_pct=("n_fills", lambda x: (x == 1).mean()),
        avg_fills_per_event=("n_fills", "mean"),
    ).reset_index()

    # --- Risk and round trip metrics ---
    rt_features = []
    if len(round_trips) > 0:
        for wallet, wrt in round_trips.groupby("wallet"):
            feats = {"wallet": wallet}
            feats["round_trips"] = len(wrt)
            feats["rt_pnl_mean_bps"] = wrt["pnl_bps"].mean()
            feats["rt_pnl_std_bps"] = wrt["pnl_bps"].std()
            feats["rt_pnl_sharpe"] = (
                wrt["pnl_bps"].mean() / wrt["pnl_bps"].std()
                if wrt["pnl_bps"].std() > 0 else 0
            )
            feats["median_hold_s"] = wrt["hold_duration_s"].median()
            feats["p90_hold_s"] = wrt["hold_duration_s"].quantile(0.9)

            # Risk
            cum_pnl = wrt["pnl_usd"].cumsum()
            running_max = cum_pnl.cummax()
            drawdown = cum_pnl - running_max
            feats["max_drawdown_usd"] = drawdown.min()
            total_pnl = cum_pnl.iloc[-1] if len(cum_pnl) > 0 else 0
            feats["max_drawdown_pct"] = (
                abs(drawdown.min()) / max(abs(total_pnl), abs(running_max.max()), 1) * 100
            )
            feats["max_single_loss_bps"] = wrt["pnl_bps"].min()

            # Losing streak
            is_loss = (wrt["pnl_bps"] < 0).values
            streak = 0
            max_streak = 0
            for loss in is_loss:
                if loss:
                    streak += 1
                    max_streak = max(max_streak, streak)
                else:
                    streak = 0
            feats["loss_streak_max"] = max_streak

            # PnL skew and tail ratio
            if len(wrt) > 3:
                feats["pnl_skew"] = wrt["pnl_bps"].skew()
                p95 = wrt["pnl_bps"].quantile(0.95)
                p05 = wrt["pnl_bps"].quantile(0.05)
                feats["tail_ratio"] = abs(p95 / p05) if abs(p05) > 0.01 else np.nan
            else:
                feats["pnl_skew"] = np.nan
                feats["tail_ratio"] = np.nan

            rt_features.append(feats)

    rt_df = pd.DataFrame(rt_features) if rt_features else pd.DataFrame(columns=["wallet"])

    # --- PnL concentration (per day) ---
    if len(opens_with_l2) > 0:
        daily_mo = opens_with_l2.copy()
        daily_mo["date"] = pd.to_datetime(daily_mo["start_ts"], unit="ms").dt.date
        daily_sum = daily_mo.groupby(["wallet", "date"])["copy_mo_60s"].sum().reset_index()

        conc = []
        for wallet, wdf in daily_sum.groupby("wallet"):
            total = wdf["copy_mo_60s"].sum()
            if abs(total) > 0.01:
                max_day_pct = wdf["copy_mo_60s"].abs().max() / abs(total) * 100
            else:
                max_day_pct = 100.0
            profitable_days = (wdf["copy_mo_60s"] > 0).sum()
            conc.append({
                "wallet": wallet,
                "pnl_concentration_pct": max_day_pct,
                "profitable_days": profitable_days,
                "total_active_days": len(wdf),
                "consistency": profitable_days / max(1, len(wdf)),
            })
        conc_df = pd.DataFrame(conc)
    else:
        conc_df = pd.DataFrame(columns=["wallet"])

    # --- Merge all features ---
    features = activity.copy()
    for df in [alpha_df, copy_metrics, rt_df, conc_df]:
        if len(df) > 0:
            features = features.merge(df, on="wallet", how="left")

    # --- Composite score components ---
    if "net_edge_bps" in features.columns:
        features["sample_confidence"] = np.minimum(
            1.0, np.sqrt(features["open_event_count"].fillna(0) / 300)
        )
        features["decay_penalty"] = 1.0  # Will be computed in Phase 5 with train/test split

    logger.info(f"Feature matrix: {len(features):,} wallets, {len(features.columns)} features")

    return features


def main():
    t0 = time.time()

    EVENTS_DIR = OUTPUT_DIR / "events_daily"
    MARKOUT_DIR = OUTPUT_DIR / "events_markout_daily"
    MARKOUT_DIR.mkdir(parents=True, exist_ok=True)

    event_files = sorted(EVENTS_DIR.glob("*.parquet"))
    if not event_files:
        logger.error("No event files found. Run phase2 first.")
        return

    logger.info(f"Found {len(event_files)} daily event files")

    # Load universe filter early (needed for memory-safe round trip loading)
    universe_path = OUTPUT_DIR / "universe_filtered.csv"
    filtered_wallets = None
    if universe_path.exists():
        universe = pd.read_csv(universe_path)
        filtered_wallets = set(universe["wallet"].values)
        logger.info(f"Universe filter: {len(filtered_wallets):,} wallets")
    else:
        logger.warning("No universe filter found, using all wallets (high memory!)")

    # Load round trips (chunked + filtered to avoid OOM on 50M+ rows)
    round_trips = pd.DataFrame()
    if ROUND_TRIPS_PATH.exists():
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(ROUND_TRIPS_PATH)
        total_rt_rows = pf.metadata.num_rows
        logger.info(f"Round trips file: {total_rt_rows:,} rows, {pf.metadata.num_row_groups} row groups")

        if total_rt_rows > 5_000_000 and filtered_wallets:
            # Stream row groups, keep only filtered wallets
            logger.info("Streaming round trips with wallet filter...")
            rt_chunks = []
            for i in range(pf.metadata.num_row_groups):
                chunk = pf.read_row_group(i).to_pandas()
                chunk = chunk[chunk["wallet"].isin(filtered_wallets)]
                if len(chunk) > 0:
                    rt_chunks.append(chunk)
                if (i + 1) % 20 == 0:
                    logger.info(f"  Row group {i+1}/{pf.metadata.num_row_groups}, "
                                f"kept {sum(len(c) for c in rt_chunks):,} rows")
            round_trips = pd.concat(rt_chunks, ignore_index=True) if rt_chunks else pd.DataFrame()
            del rt_chunks
            logger.info(f"Filtered round trips: {len(round_trips):,} rows "
                        f"(from {total_rt_rows:,}, {len(round_trips)/max(1,total_rt_rows)*100:.1f}%)")
        else:
            round_trips = pd.read_parquet(ROUND_TRIPS_PATH)
            logger.info(f"Loaded {len(round_trips):,} round trips")

    # Collect all coins across all days (sample a few files)
    all_coins = set()
    for ef in event_files:
        df = pd.read_parquet(ef, columns=["coin"])
        all_coins.update(df["coin"].unique())
    coins = list(all_coins)
    logger.info(f"Total coins across all days: {len(coins)}")

    # Load mid prices: L2 (preferred) + fill-derived (fallback)
    l2_prices = load_mid_prices(coins)
    fill_prices = load_fill_derived_midprices(coins)
    mid_prices, source_map = merge_midprice_sources(l2_prices, fill_prices)

    l2_count = sum(1 for v in source_map.values() if v == "l2")
    fill_count = sum(1 for v in source_map.values() if v == "fill")
    mixed_count = sum(1 for v in source_map.values() if v == "mixed")
    logger.info(f"Mid-price sources: {l2_count} L2-only, {fill_count} fill-only, {mixed_count} mixed")

    # Compute markout per day (memory efficient)
    total_events = 0
    total_with_l2 = 0
    all_events_with_markout = []

    for i, ef in enumerate(event_files):
        date_str = ef.stem
        out_path = MARKOUT_DIR / f"{date_str}.parquet"

        if out_path.exists():
            logger.info(f"[{i+1}/{len(event_files)}] {date_str}: markout exists, loading")
            day_events = pd.read_parquet(out_path)
        else:
            logger.info(f"[{i+1}/{len(event_files)}] Computing markout for {date_str}...")
            t_day = time.time()
            day_events = pd.read_parquet(ef)
            day_events = compute_markout_for_events(day_events, mid_prices)
            day_events.to_parquet(out_path, index=False)
            logger.info(f"  {len(day_events):,} events, {day_events['has_l2'].sum():,} with L2, {time.time()-t_day:.1f}s")

        total_events += len(day_events)
        total_with_l2 += day_events["has_l2"].sum()
        all_events_with_markout.append(day_events)

    logger.info(f"\nTotal: {total_events:,} events, {total_with_l2:,} with mid-price ({total_with_l2/max(1,total_events)*100:.1f}%)")

    # Combine events for filtered wallets only
    logger.info("Combining events for filtered wallets...")
    filtered_events = []
    for day_events in all_events_with_markout:
        if filtered_wallets:
            day_filtered = day_events[day_events["wallet"].isin(filtered_wallets)]
        else:
            day_filtered = day_events
        if len(day_filtered) > 0:
            filtered_events.append(day_filtered)
    del all_events_with_markout

    if not filtered_events:
        logger.error("No events for filtered wallets")
        return

    events = pd.concat(filtered_events, ignore_index=True)
    del filtered_events
    logger.info(f"Filtered events: {len(events):,} ({events['wallet'].nunique():,} wallets)")

    # Compute wallet features
    logger.info("Computing wallet features...")
    t_feat = time.time()
    features = compute_wallet_features(events, round_trips)
    logger.info(f"Features done in {time.time() - t_feat:.0f}s")

    # Save
    features.to_parquet(FEATURES_PATH, index=False)
    logger.info(f"Features saved to {FEATURES_PATH}")

    # Quick summary
    if "net_edge_bps" in features.columns:
        pos_edge = features[features["net_edge_bps"] > 0]
        logger.info(f"\nWallets with positive net edge: {len(pos_edge):,} / {len(features):,}")
        if len(pos_edge) > 0:
            logger.info(f"Top 10 by net edge:")
            top10 = pos_edge.nlargest(10, "net_edge_bps")
            for _, row in top10.iterrows():
                logger.info(
                    f"  {row['wallet'][:10]}... edge={row['net_edge_bps']:.2f}bps "
                    f"t={row.get('daily_t_stat', 0):.2f} "
                    f"events={row.get('open_event_count', 0):.0f} "
                    f"days={row.get('active_days', 0):.0f}"
                )

    elapsed = time.time() - t0
    logger.info(f"\nPhase 3 complete in {elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    main()
