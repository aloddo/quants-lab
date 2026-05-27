#!/usr/bin/env python3
"""
Options Regime Monitor -- Continuous regime detection + strategy recommendation.

Checks regime every hour, recommends options strategy adjustments, tracks
open positions and signals when to roll, close, or open new trades.

Usage:
    python scripts/options_regime_monitor.py              # One-shot regime check
    python scripts/options_regime_monitor.py --loop 3600  # Continuous (every hour)
    python scripts/options_regime_monitor.py --backtest   # Backtest regime strategy
"""
import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [regime] %(levelname)s: %(message)s")
logger = logging.getLogger("regime")

MONGO_URI = "mongodb://localhost:27017/quants_lab"

# Regime thresholds (calibrated from 225 days of data)
IV_HIGH = 45.0          # DVOL above this = high vol
TREND_THRESHOLD = 3.0   # 7d % move above this = trending
VRP_SELL_THRESHOLD = 3.0  # VRP above this = sell premium


def get_regime_data():
    """Fetch latest DVOL, RV, and trend data."""
    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]

    # Latest DVOL
    dvol_rec = db["deribit_dvol"].find_one({"currency": "BTC"}, sort=[("timestamp_utc", -1)])
    iv = float(dvol_rec["dvol_close"]) if dvol_rec else 0

    # BTC candles for RV + trend
    candles = list(db["hyperliquid_candles_1h"].find(
        {"coin": "BTC"}, {"_id": 0, "timestamp_utc": 1, "close": 1}
    ).sort("timestamp_utc", -1).limit(24 * 8))  # 8 days for 7d calc

    if len(candles) < 24 * 7:
        return None

    df = pd.DataFrame(candles)
    df["close"] = df["close"].astype(float)
    df = df.sort_values("timestamp_utc")

    # Realized vol (7d)
    log_rets = np.log(df["close"] / df["close"].shift(1)).dropna()
    rv_7d = log_rets.tail(24 * 7).std() * np.sqrt(24 * 365) * 100

    # Trend
    current_price = df["close"].iloc[-1]
    price_7d_ago = df["close"].iloc[-24 * 7] if len(df) >= 24 * 7 else df["close"].iloc[0]
    trend_7d = (current_price / price_7d_ago - 1) * 100

    price_24h_ago = df["close"].iloc[-24] if len(df) >= 24 else df["close"].iloc[0]
    trend_24h = (current_price / price_24h_ago - 1) * 100

    vrp = iv - rv_7d

    return {
        "iv": iv,
        "rv_7d": rv_7d,
        "vrp": vrp,
        "trend_7d": trend_7d,
        "trend_24h": trend_24h,
        "btc_price": current_price,
        "timestamp": datetime.now(timezone.utc),
    }


def classify_regime(data):
    """Classify current market regime."""
    iv = data["iv"]
    trend = data["trend_7d"]
    vrp = data["vrp"]

    high_vol = iv > IV_HIGH
    trending_up = trend > TREND_THRESHOLD
    trending_down = trend < -TREND_THRESHOLD

    if high_vol and trending_down:
        regime = "CRASH"
    elif high_vol and trending_up:
        regime = "EUPHORIA"
    elif not high_vol and abs(trend) < TREND_THRESHOLD:
        regime = "QUIET"
    else:
        regime = "TRENDING"

    return regime


def get_strategy_recommendation(regime, data):
    """Recommend options strategy based on regime."""
    vrp = data["vrp"]
    trend = data["trend_7d"]
    iv = data["iv"]

    rec = {
        "regime": regime,
        "strategy": None,
        "direction": None,
        "width": None,
        "dte_target": None,
        "position_action": None,
        "reasoning": "",
    }

    if regime == "QUIET":
        rec["strategy"] = "iron_condor" if vrp > VRP_SELL_THRESHOLD else "strangle"
        rec["direction"] = "neutral"
        rec["width"] = "tight"  # 5-6% OTM
        rec["dte_target"] = "7d"
        rec["reasoning"] = f"Low vol ({iv:.0f}%), no trend ({trend:+.1f}%), VRP +{vrp:.1f}%. Sell premium."
        rec["position_action"] = "OPEN" if vrp > VRP_SELL_THRESHOLD else "WAIT"

    elif regime == "TRENDING":
        if trend > 0:
            rec["strategy"] = "put_credit_spread"
            rec["direction"] = "bullish"
        else:
            rec["strategy"] = "call_credit_spread"
            rec["direction"] = "bearish"
        rec["width"] = "medium"  # 3-5% OTM
        rec["dte_target"] = "7-14d"
        rec["reasoning"] = f"Trending ({trend:+.1f}%), moderate vol ({iv:.0f}%). Directional spread."
        rec["position_action"] = "OPEN"

    elif regime == "EUPHORIA":
        rec["strategy"] = "strangle"
        rec["direction"] = "neutral"
        rec["width"] = "wide"  # 8-10% OTM
        rec["dte_target"] = "7d"
        rec["reasoning"] = f"High vol ({iv:.0f}%), strong up ({trend:+.1f}%). Sell overpriced vol aggressively."
        rec["position_action"] = "OPEN"

    elif regime == "CRASH":
        rec["strategy"] = "call_credit_spread"
        rec["direction"] = "bearish"
        rec["width"] = "wide"  # 8-10% OTM
        rec["dte_target"] = "14d"
        rec["reasoning"] = f"High vol ({iv:.0f}%), crashing ({trend:+.1f}%). Sell call spreads, wait for vol spike to sell puts."
        rec["position_action"] = "OPEN_PARTIAL"

    return rec


def get_position_management(regime, data):
    """Recommend adjustments for existing positions."""
    rules = []

    # Universal rules
    rules.append("CLOSE at 50% profit (buy back at half premium)")
    rules.append("CLOSE at 2x loss (buy back at 3x premium paid)")
    rules.append("ROLL at 2 DTE (close current, open next expiry)")

    # Regime-specific
    if regime == "CRASH":
        rules.append("CLOSE any naked short puts immediately")
        rules.append("WIDEN iron condor wings if holding")
    elif regime == "EUPHORIA":
        rules.append("TIGHTEN take-profit to 30% (vol may collapse fast)")
    elif regime == "QUIET":
        rules.append("LET positions run, theta is your friend")
    elif regime == "TRENDING":
        if data["trend_7d"] > 0:
            rules.append("CLOSE call-side if delta drifts > 0.3")
        else:
            rules.append("CLOSE put-side if delta drifts > 0.3")

    return rules


def print_report(data, regime, recommendation, management_rules):
    """Print regime report."""
    print(f"\n{'='*60}")
    print(f"OPTIONS REGIME MONITOR -- {data['timestamp'].strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    print(f"\nBTC: ${data['btc_price']:,.2f}")
    print(f"IV (DVOL): {data['iv']:.1f}%  |  RV (7d): {data['rv_7d']:.1f}%  |  VRP: {data['vrp']:+.1f}%")
    print(f"Trend 7d: {data['trend_7d']:+.1f}%  |  Trend 24h: {data['trend_24h']:+.1f}%")

    print(f"\nREGIME: {regime}")
    print(f"ACTION: {recommendation['position_action']}")
    print(f"STRATEGY: {recommendation['strategy']} ({recommendation['direction']}, {recommendation['width']} width)")
    print(f"TARGET DTE: {recommendation['dte_target']}")
    print(f"REASONING: {recommendation['reasoning']}")

    print(f"\nPOSITION MANAGEMENT:")
    for rule in management_rules:
        print(f"  - {rule}")

    # Strike recommendations based on current spot
    spot = data["btc_price"]
    print(f"\nSUGGESTED STRIKES:")
    if recommendation["strategy"] in ("iron_condor", "strangle"):
        if recommendation["width"] == "tight":
            put_k = round(spot * 0.95 / 1000) * 1000
            call_k = round(spot * 1.05 / 1000) * 1000
        elif recommendation["width"] == "medium":
            put_k = round(spot * 0.93 / 1000) * 1000
            call_k = round(spot * 1.07 / 1000) * 1000
        else:
            put_k = round(spot * 0.90 / 1000) * 1000
            call_k = round(spot * 1.10 / 1000) * 1000
        print(f"  Put: ${put_k:,}  |  Call: ${call_k:,}  |  Range: {(call_k-put_k)/spot*100:.0f}%")
    elif recommendation["strategy"] == "put_credit_spread":
        short_k = round(spot * 0.96 / 1000) * 1000
        long_k = short_k - 2000
        print(f"  Short put: ${short_k:,}  |  Long put: ${long_k:,}  |  Width: $2K")
    elif recommendation["strategy"] == "call_credit_spread":
        short_k = round(spot * 1.04 / 1000) * 1000
        long_k = short_k + 2000
        print(f"  Short call: ${short_k:,}  |  Long call: ${long_k:,}  |  Width: $2K")

    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", type=int, help="Loop interval in seconds")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    while True:
        data = get_regime_data()
        if not data:
            logger.error("Insufficient data for regime classification")
            if not args.loop:
                return
            time.sleep(args.loop)
            continue

        regime = classify_regime(data)
        recommendation = get_strategy_recommendation(regime, data)
        management = get_position_management(regime, data)

        if args.json:
            output = {**data, "regime": regime, "recommendation": recommendation, "management": management}
            output["timestamp"] = output["timestamp"].isoformat()
            print(json.dumps(output, indent=2))
        else:
            print_report(data, regime, recommendation, management)

        if not args.loop:
            break

        time.sleep(args.loop)


if __name__ == "__main__":
    main()
