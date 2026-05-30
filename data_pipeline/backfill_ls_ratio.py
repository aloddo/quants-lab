"""
Backfill historical LS ratio data from Bybit REST API into MongoDB.

Fetches 4h and 1d periods for all pairs currently in bybit_ls_ratio.
Existing 1h docs are left untouched. New docs get a `period` field.

Usage:
    MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/backfill_ls_ratio.py
"""

import time
import requests
from pymongo import MongoClient, UpdateOne

MONGO_URI = "mongodb://localhost:27017/quants_lab"
DB_NAME = "quants_lab"
COLLECTION = "bybit_ls_ratio"
API_URL = "https://api.bybit.com/v5/market/account-ratio"
PERIODS = ["4h", "1d"]
LIMIT = 500
DELAY = 0.2  # seconds between API calls


def pair_to_symbol(pair: str) -> str:
    """BTC-USDT -> BTCUSDT"""
    return pair.replace("-", "")


def fetch_ls_ratio(symbol: str, period: str) -> list[dict]:
    """Fetch LS ratio from Bybit API. Returns list of records."""
    params = {
        "category": "linear",
        "symbol": symbol,
        "period": period,
        "limit": LIMIT,
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("retCode") != 0:
            return []
        return data.get("result", {}).get("list", [])
    except Exception as e:
        print(f"  ERROR fetching {symbol} {period}: {e}")
        return []


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]

    pairs = sorted(col.distinct("pair"))
    print(f"Found {len(pairs)} pairs in {COLLECTION}")

    total_upserted = 0
    total_api_calls = 0

    for period in PERIODS:
        print(f"\n=== Backfilling period={period} ===")
        period_upserted = 0

        for i, pair in enumerate(pairs):
            symbol = pair_to_symbol(pair)
            records = fetch_ls_ratio(symbol, period)
            total_api_calls += 1

            if not records:
                if (i + 1) % 10 == 0:
                    print(f"  [{i+1}/{len(pairs)}] {pair}: no data")
                time.sleep(DELAY)
                continue

            ops = []
            for r in records:
                ts = int(r["timestamp"])
                buy_ratio = float(r["buyRatio"])
                sell_ratio = float(r["sellRatio"])
                ops.append(UpdateOne(
                    {"pair": pair, "timestamp_utc": ts, "period": period},
                    {"$set": {
                        "pair": pair,
                        "timestamp_utc": ts,
                        "buy_ratio": buy_ratio,
                        "sell_ratio": sell_ratio,
                        "period": period,
                    }},
                    upsert=True,
                ))

            if ops:
                result = col.bulk_write(ops, ordered=False)
                count = result.upserted_count + result.modified_count
                period_upserted += count

            if (i + 1) % 10 == 0:
                print(f"  [{i+1}/{len(pairs)}] Last: {pair} ({len(records)} records) | Period total so far: {period_upserted}")

            time.sleep(DELAY)

        total_upserted += period_upserted
        print(f"  Period {period} done: {period_upserted} docs upserted/updated")

    # Also tag existing 1h docs that don't have a period field
    result = col.update_many(
        {"period": {"$exists": False}},
        {"$set": {"period": "1h"}},
    )
    print(f"\nTagged {result.modified_count} existing docs with period='1h'")

    # Ensure index for upsert performance
    col.create_index([("pair", 1), ("timestamp_utc", 1), ("period", 1)], unique=True, background=True)
    print("Created compound index on (pair, timestamp_utc, period)")

    print(f"\n=== DONE ===")
    print(f"Total API calls: {total_api_calls}")
    print(f"Total docs upserted/updated: {total_upserted}")
    print(f"Collection count: {col.count_documents({})}")

    client.close()


if __name__ == "__main__":
    main()
