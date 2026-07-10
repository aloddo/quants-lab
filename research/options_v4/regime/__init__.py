"""
Options V4 Regime Engine -- Daily classification with intraday kill rules.
7 regimes, each maps to a specific options structure.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/quants_lab"


class Regime(Enum):
    CRASH_MOMENTUM = "crash_momentum"    # Buy put debit spread
    CRASH_EXTREME = "crash_extreme"      # FLAT + kill open credits
    HIGH_IV = "high_iv"                  # Sell wide IC (8%/11% OTM)
    NEUTRAL = "neutral"                  # Sell IC (8%/11% OTM)
    BULL = "bull"                        # Buy call debit spread (3%/6% OTM)
    STRESS = "stress"                    # Sell call credit spread (5%/8% OTM)
    UNCLEAR = "unclear"                  # FLAT


@dataclass
class RegimeFeatures:
    timestamp: datetime
    spot: float
    dvol: float           # Deribit DVOL (annualized IV %)
    rv_7d: float          # 7-day realized vol (annualized %)
    vrp: float            # DVOL - RV_7d
    trend_7d: float       # 7-day price change %
    trend_24h: float      # 24h price change %
    drawdown_7d: float    # Max drawdown from 7d high (negative %)


def compute_features() -> Optional[RegimeFeatures]:
    """Compute regime features from MongoDB data."""
    import numpy as np
    import pandas as pd

    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]

    # Latest DVOL. Repointed 2026-07-04 from deribit_dvol_full (collector died 2026-05-16,
    # caused stale-implied-vol false NEUTRAL signals) to deribit_dvol (fresh, maintained; the
    # backtest load_data uses this same series; the two are byte-identical where they overlap).
    dvol_rec = db["deribit_dvol"].find_one(
        {"currency": "BTC"}, sort=[("timestamp_utc", -1)]
    )
    if not dvol_rec:
        return None
    dvol = float(dvol_rec["dvol_close"])

    # BTC candles for RV + trend
    candles = list(db["hyperliquid_candles_1h"].find(
        {"coin": "BTC"}, {"_id": 0, "timestamp_utc": 1, "close": 1}
    ).sort("timestamp_utc", -1).limit(24 * 8))

    if len(candles) < 24 * 7:
        return None

    df = pd.DataFrame(candles).sort_values("timestamp_utc")
    df["close"] = df["close"].astype(float)
    log_rets = np.log(df["close"] / df["close"].shift(1)).dropna()

    spot = df["close"].iloc[-1]
    rv_7d = log_rets.tail(24 * 7).std() * np.sqrt(24 * 365) * 100
    vrp = dvol - rv_7d

    price_7d = df["close"].iloc[-24 * 7] if len(df) >= 24 * 7 else df["close"].iloc[0]
    price_24h = df["close"].iloc[-24] if len(df) >= 24 else df["close"].iloc[0]
    trend_7d = (spot / price_7d - 1) * 100
    trend_24h = (spot / price_24h - 1) * 100

    recent_7d = df["close"].tail(24 * 7)
    drawdown_7d = (spot / recent_7d.max() - 1) * 100

    return RegimeFeatures(
        timestamp=datetime.now(timezone.utc),
        spot=spot, dvol=dvol, rv_7d=rv_7d, vrp=vrp,
        trend_7d=trend_7d, trend_24h=trend_24h, drawdown_7d=drawdown_7d,
    )


def classify(f: RegimeFeatures) -> Regime:
    """Classify regime from features. Thresholds validated over 2yr backtest (324/324 robust)."""
    # FIX: CRASH_EXTREME must be checked FIRST (supercedes CRASH_MOMENTUM)
    if f.trend_7d < -8 or f.drawdown_7d < -10:
        return Regime.CRASH_EXTREME
    if f.trend_7d <= -6 and f.vrp <= 4 and abs(f.trend_24h) <= 7:
        return Regime.CRASH_MOMENTUM
    if f.dvol > 50 and f.vrp > 5:
        return Regime.HIGH_IV
    if abs(f.trend_7d) < 5 and f.vrp > 0:
        return Regime.NEUTRAL
    if f.trend_7d >= 5:
        return Regime.BULL
    if f.dvol > 55 and f.vrp < 0:
        return Regime.STRESS
    return Regime.UNCLEAR


def should_kill(f: RegimeFeatures, open_credits: bool) -> bool:
    """Intraday kill check. Returns True if open credit positions should be closed."""
    if not open_credits:
        return False
    # Kill if regime transitions to extreme crash
    if f.trend_7d < -8 or f.drawdown_7d < -10:
        return True
    # Kill if VRP flips deeply negative while holding IC
    if f.vrp < -5:
        return True
    return False
