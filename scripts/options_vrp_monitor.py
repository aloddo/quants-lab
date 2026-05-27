#!/usr/bin/env python3
"""
Options VRP + Skew Monitor

Real-time dashboard for options trading decisions.
Shows: VRP level, skew, term structure, iron condor candidates.

Usage:
    python scripts/options_vrp_monitor.py
    python scripts/options_vrp_monitor.py --once  # single snapshot
"""
import argparse
import logging
import time
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pybit.unified_trading import HTTP

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("vrp_monitor")

MONGO_URI = "mongodb://localhost:27017/quants_lab"


def compute_realized_vol(db, lookback_days=30):
    """Compute realized vol from BTC 1h candles."""
    candles = list(db.hyperliquid_candles_1h.find(
        {"coin": "BTC"},
        {"_id": 0, "timestamp_utc": 1, "close": 1}
    ).sort("timestamp_utc", -1).limit(lookback_days * 24 + 24))

    if len(candles) < 48:
        return {}

    df = pd.DataFrame(candles)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    df = df.set_index("dt").sort_index()
    df["close"] = df["close"].astype(float)

    daily = df["close"].resample("1D").last().dropna()
    returns = np.log(daily / daily.shift(1)).dropna()

    return {
        "rv_7d": float(returns.tail(7).std() * np.sqrt(365) * 100),
        "rv_14d": float(returns.tail(14).std() * np.sqrt(365) * 100),
        "rv_30d": float(returns.tail(30).std() * np.sqrt(365) * 100) if len(returns) >= 30 else None,
        "btc_price": float(daily.iloc[-1]),
        "daily_return": float(returns.iloc[-1] * 100),
    }


def get_dvol(db):
    """Get latest DVOL from Deribit."""
    record = db.deribit_dvol.find_one(
        {"currency": "BTC"},
        sort=[("timestamp_utc", -1)]
    )
    if record:
        return float(record["dvol_close"])
    return None


def get_bybit_surface(db):
    """Get latest Bybit options snapshot."""
    latest_ts = db.bybit_options_surface.find_one(
        {"base_coin": "BTC"},
        sort=[("collected_at", -1)]
    )
    if not latest_ts:
        return None

    ts = latest_ts["collected_at"]
    docs = list(db.bybit_options_surface.find(
        {"base_coin": "BTC", "collected_at": ts}
    ))
    return pd.DataFrame(docs)


def get_deribit_skew(db, lookback_days=1):
    """Compute current skew from Deribit surface."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - lookback_days * 86400 * 1000

    records = list(db.deribit_options_surface.find(
        {
            "currency": "BTC",
            "collected_at": {"$gte": start_ms},
            "mark_iv": {"$gt": 0},
        },
        {"_id": 0, "strike": 1, "type": 1, "mark_iv": 1, "underlying_price": 1}
    ).sort("collected_at", -1).limit(5000))

    if not records:
        return {}

    df = pd.DataFrame(records)
    df["moneyness"] = df["strike"] / df["underlying_price"]

    atm = df[(df["moneyness"] > 0.95) & (df["moneyness"] < 1.05)]
    otm_puts = df[(df["moneyness"] > 0.85) & (df["moneyness"] < 0.95) & (df["type"] == "put")]
    otm_calls = df[(df["moneyness"] > 1.05) & (df["moneyness"] < 1.15) & (df["type"] == "call")]

    result = {}
    if len(atm) > 0:
        result["atm_iv"] = float(atm["mark_iv"].mean() * 100)
    if len(otm_puts) > 0:
        result["put_iv"] = float(otm_puts["mark_iv"].mean() * 100)
    if len(otm_calls) > 0:
        result["call_iv"] = float(otm_calls["mark_iv"].mean() * 100)
    if "put_iv" in result and "call_iv" in result:
        result["skew"] = result["put_iv"] - result["call_iv"]

    return result


def find_iron_condor_candidates(session):
    """Find best iron condor strikes on Bybit."""
    result = session.get_tickers(category="option", baseCoin="BTC")
    if result["retCode"] != 0:
        return []

    tickers = result["result"]["list"]

    # Group by expiry
    by_expiry = {}
    for t in tickers:
        parts = t["symbol"].split("-")
        if len(parts) < 4:
            continue
        expiry = parts[1]
        if expiry not in by_expiry:
            by_expiry[expiry] = []
        by_expiry[expiry].append(t)

    # Sort expiries chronologically (nearest first)
    # Simple heuristic: shorter name = nearer
    sorted_expiries = sorted(by_expiry.keys(), key=lambda x: (len(x), x))

    candidates = []
    for expiry in sorted_expiries[:3]:  # Check nearest 3 expiries
        options = by_expiry[expiry]

        # Find options with delta near target
        put_candidates = []
        call_candidates = []

        for t in options:
            delta = float(t.get("delta") or 0)
            bid = float(t.get("bid1Price") or 0)
            ask = float(t.get("ask1Price") or 0)
            oi = float(t.get("openInterest") or 0)
            iv = float(t.get("markIv") or 0)
            strike = float(t["symbol"].split("-")[2])

            if bid <= 0:
                continue

            if "-P-" in t["symbol"] and -0.25 <= delta <= -0.10:
                put_candidates.append({
                    "symbol": t["symbol"], "strike": strike,
                    "delta": delta, "bid": bid, "ask": ask,
                    "iv": iv, "oi": oi,
                })
            elif "-C-" in t["symbol"] and 0.10 <= delta <= 0.25:
                call_candidates.append({
                    "symbol": t["symbol"], "strike": strike,
                    "delta": delta, "bid": bid, "ask": ask,
                    "iv": iv, "oi": oi,
                })

        if put_candidates and call_candidates:
            # Best put: highest premium with delta in range
            best_put = max(put_candidates, key=lambda x: x["bid"])
            best_call = max(call_candidates, key=lambda x: x["bid"])

            premium = (best_put["bid"] + best_call["bid"]) * 0.01
            candidates.append({
                "expiry": expiry,
                "put": best_put,
                "call": best_call,
                "total_premium_001btc": premium,
            })

    return candidates


def print_dashboard(rv, dvol, skew, candidates):
    """Print formatted dashboard."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'=' * 65}")
    print(f"  OPTIONS VRP MONITOR  |  {now}")
    print(f"{'=' * 65}")

    if rv:
        print(f"\n  BTC: ${rv['btc_price']:,.0f}  |  Daily return: {rv['daily_return']:+.2f}%")
        print(f"\n  REALIZED VOL")
        print(f"    7-day:  {rv['rv_7d']:.1f}%")
        print(f"    14-day: {rv['rv_14d']:.1f}%")
        if rv.get("rv_30d"):
            print(f"    30-day: {rv['rv_30d']:.1f}%")

    if dvol:
        print(f"\n  IMPLIED VOL (Deribit DVOL)")
        print(f"    DVOL: {dvol:.1f}%")

        if rv:
            vrp_7 = dvol - rv["rv_7d"]
            vrp_14 = dvol - rv["rv_14d"]
            print(f"\n  VARIANCE RISK PREMIUM")
            print(f"    VRP (vs 7d RV):  {vrp_7:+.1f}%  {'[SELL VOL]' if vrp_7 > 5 else '[NEUTRAL]' if vrp_7 > 0 else '[BUY VOL]'}")
            print(f"    VRP (vs 14d RV): {vrp_14:+.1f}%  {'[SELL VOL]' if vrp_14 > 5 else '[NEUTRAL]' if vrp_14 > 0 else '[BUY VOL]'}")

    if skew:
        print(f"\n  SKEW (Deribit)")
        if "atm_iv" in skew:
            print(f"    ATM IV:  {skew['atm_iv']:.1f}%")
        if "put_iv" in skew:
            print(f"    OTM Put IV:  {skew['put_iv']:.1f}%")
        if "call_iv" in skew:
            print(f"    OTM Call IV: {skew['call_iv']:.1f}%")
        if "skew" in skew:
            print(f"    Skew (put-call): {skew['skew']:+.1f}%  (avg: +6.2%)")

    if candidates:
        print(f"\n  IRON CONDOR CANDIDATES (Bybit, 0.01 BTC)")
        print(f"  {'Expiry':<10} {'Put Strike':<12} {'Call Strike':<12} {'Premium':<10} {'Put d':<8} {'Call d':<8}")
        print(f"  {'-'*62}")
        for c in candidates:
            p = c["put"]
            cl = c["call"]
            print(f"  {c['expiry']:<10} {p['strike']:<12,.0f} {cl['strike']:<12,.0f} "
                  f"${c['total_premium_001btc']:<9.2f} {p['delta']:<8.3f} {cl['delta']:<8.3f}")

    print(f"\n{'=' * 65}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Single snapshot")
    parser.add_argument("--interval", type=int, default=900, help="Refresh interval (seconds)")
    args = parser.parse_args()

    db = MongoClient(MONGO_URI).quants_lab
    session = HTTP(testnet=False)

    while True:
        try:
            rv = compute_realized_vol(db)
            dvol = get_dvol(db)
            skew = get_deribit_skew(db)
            candidates = find_iron_condor_candidates(session)
            print_dashboard(rv, dvol, skew, candidates)
        except Exception as e:
            logger.error(f"Monitor error: {e}")

        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
