#!/usr/bin/env python3
"""
BTC Regime Tagger for Wallet Alpha Research

Tags each day with BTC market regime:
- trending_up: BTC daily return > +1%
- trending_down: BTC daily return < -1%
- range: abs(BTC daily return) < 1%
- high_vol: BTC 24h realized vol > 60% annualized
- low_vol: BTC 24h realized vol < 30% annualized

Used by Phase 3 for per-regime feature computation and Phase 6 for regime consistency.
"""
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [regime] %(levelname)s: %(message)s",
)
logger = logging.getLogger("regime")

OUTPUT_DIR = Path("app/data/wallet_alpha")
REGIME_PATH = OUTPUT_DIR / "btc_regimes.csv"
MONGO_URI = "mongodb://localhost:27017/quants_lab"


def build_btc_regimes() -> pd.DataFrame:
    """Build daily BTC regime tags from candle data in MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]

    col = db["hyperliquid_candles_1h"]
    cursor = col.find(
        {"coin": "BTC"},
        {"_id": 0, "timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1},
    ).sort("timestamp_utc", 1)

    records = list(cursor)
    client.close()

    if not records:
        logger.warning("No BTC candle data found in MongoDB")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # timestamp_utc is in milliseconds
    ts_col = "timestamp_utc"
    if df[ts_col].max() > 1e12:
        df["date"] = pd.to_datetime(df[ts_col], unit="ms").dt.date
    else:
        df["date"] = pd.to_datetime(df[ts_col], unit="s").dt.date

    # Daily OHLC
    daily = df.groupby("date").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
    ).reset_index()

    # Daily return
    daily["return_pct"] = (daily["close"] / daily["open"] - 1) * 100

    # Realized volatility (annualized from hourly returns)
    # Rolling 24h vol
    daily["vol_ann"] = np.nan
    for i, row in daily.iterrows():
        day = row["date"]
        if df[ts_col].max() > 1e12:
            day_hours = df[pd.to_datetime(df[ts_col], unit="ms").dt.date == day]
        else:
            day_hours = df[pd.to_datetime(df[ts_col], unit="s").dt.date == day]
        if len(day_hours) >= 12:
            hr = day_hours["close"].pct_change().dropna()
            daily.loc[i, "vol_ann"] = hr.std() * np.sqrt(24 * 365) * 100

    # Regime classification
    daily["trend_regime"] = "range"
    daily.loc[daily["return_pct"] > 1, "trend_regime"] = "trending_up"
    daily.loc[daily["return_pct"] < -1, "trend_regime"] = "trending_down"

    daily["vol_regime"] = "normal_vol"
    daily.loc[daily["vol_ann"] > 60, "vol_regime"] = "high_vol"
    daily.loc[daily["vol_ann"] < 30, "vol_regime"] = "low_vol"

    # Convert date to string for joining
    daily["date_str"] = daily["date"].astype(str).str.replace("-", "")

    logger.info(f"Regime summary:")
    logger.info(f"  Trend: {daily['trend_regime'].value_counts().to_dict()}")
    logger.info(f"  Vol: {daily['vol_regime'].value_counts().to_dict()}")

    return daily


def main():
    regimes = build_btc_regimes()
    if len(regimes) > 0:
        regimes.to_csv(REGIME_PATH, index=False)
        logger.info(f"Saved {len(regimes)} daily regimes to {REGIME_PATH}")
    else:
        logger.warning("No regime data produced")


if __name__ == "__main__":
    main()
