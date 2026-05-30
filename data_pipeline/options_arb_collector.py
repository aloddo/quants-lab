#!/usr/bin/env python3
"""
Dual-venue options arb collector: Bybit + Deribit.

Fast mode: 4 API calls per snapshot (~2s), compares on PRICES not IVs.
Flags tradeable windows where bid > ask cross-venue.

Usage:
    python scripts/options_arb_collector.py              # 1s interval (default)
    python scripts/options_arb_collector.py --interval 5   # 5s interval
"""
import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from pymongo import MongoClient
from pybit.unified_trading import HTTP

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [arb_collector] %(levelname)s: %(message)s",
)
logger = logging.getLogger("arb_collector")

MONGO_URI = "mongodb://localhost:27017/quants_lab"
COLLECTION = "options_arb_snapshots"
COINS = ["BTC", "ETH"]
MAX_MONEYNESS = 0.20
MIN_DTE = 1
MAX_DTE = 45


def _sf(val) -> float:
    """Safe float."""
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _compute_dte(expiry_str: str) -> int:
    try:
        exp_dt = datetime.strptime(expiry_str, "%d%b%y").replace(tzinfo=timezone.utc)
        return (exp_dt - datetime.now(timezone.utc)).days
    except ValueError:
        return -1


def fetch_all_bybit(session) -> dict:
    """Fetch all Bybit options in 2 API calls. Returns {coin: {(expiry, strike, type): data}}."""
    out = {}
    for coin in COINS:
        try:
            result = session.get_tickers(category="option", baseCoin=coin)
            if result["retCode"] != 0:
                continue
            coin_data = {}
            for t in result["result"]["list"]:
                parts = t["symbol"].split("-")
                if len(parts) < 4:
                    continue
                try:
                    expiry = parts[1]
                    strike = float(parts[2])
                    opt_type = parts[3][0]
                except (ValueError, IndexError):
                    continue
                coin_data[(expiry, strike, opt_type)] = {
                    "symbol": t["symbol"],
                    "bid_px": _sf(t.get("bid1Price")),
                    "ask_px": _sf(t.get("ask1Price")),
                    "bid_iv": _sf(t.get("bid1Iv")),
                    "ask_iv": _sf(t.get("ask1Iv")),
                    "mark_iv": _sf(t.get("markIv")),
                    "mark_px": _sf(t.get("markPrice")),
                    "underlying": _sf(t.get("underlyingPrice")),
                }
            out[coin] = coin_data
        except Exception as e:
            logger.error(f"Bybit {coin}: {e}")
    return out


def fetch_all_deribit() -> dict:
    """Fetch Deribit USDC-settled options. Returns {coin: {(expiry, strike, type): data}}.
    Uses USDC settlement so prices are directly comparable to Bybit USDT prices.
    No crypto-to-USD conversion needed."""
    out = {}
    for coin in COINS:
        try:
            # Use USDC currency for stablecoin-settled options
            r = requests.get(
                "https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
                params={"currency": "USDC", "kind": "option"},
                timeout=10,
            )
            items = r.json().get("result", [])
            coin_data = {}
            for s in items:
                name = s.get("instrument_name", "")
                # USDC instruments: BTC_USDC-29MAY26-80000-C
                parts = name.split("-")
                if len(parts) < 4:
                    continue

                # Filter to matching base coin (BTC_USDC -> BTC, ETH_USDC -> ETH)
                base = parts[0].split("_")[0]
                if base != coin:
                    continue

                try:
                    expiry = parts[1]
                    strike = float(parts[2])
                    opt_type = parts[3][0]
                except (ValueError, IndexError):
                    continue

                # USDC-settled: prices are already in USDC (stablecoin)
                bid_usdc = _sf(s.get("bid_price"))
                ask_usdc = _sf(s.get("ask_price"))
                mark_usdc = _sf(s.get("mark_price"))
                underlying = _sf(s.get("underlying_price"))

                coin_data[(expiry, strike, opt_type)] = {
                    "instrument": name,
                    "bid_px_usd": bid_usdc,
                    "ask_px_usd": ask_usdc,
                    "mark_px_usd": mark_usdc,
                    "mark_iv": _sf(s.get("mark_iv", 0)) / 100,
                    "underlying": underlying,
                    "oi": _sf(s.get("open_interest")),
                    "volume": _sf(s.get("volume")),
                    "settlement": "USDC",
                }
            out[coin] = coin_data
        except Exception as e:
            logger.error(f"Deribit USDC {coin}: {e}")
    return out


def collect_snapshot(session, db) -> dict:
    """One snapshot: 4 API calls, match, store, flag."""
    t0 = time.time()

    # Fetch both venues (near-simultaneously)
    deribit_all = fetch_all_deribit()    # ~0.15s
    bybit_all = fetch_all_bybit(session)  # ~1.6s

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    collection = db[COLLECTION]
    total_matched = 0
    total_flagged = 0
    docs = []

    for coin in COINS:
        bb_data = bybit_all.get(coin, {})
        dd_data = deribit_all.get(coin, {})
        if not bb_data or not dd_data:
            continue

        # Spot price from Bybit
        spots = [v["underlying"] for v in bb_data.values() if v["underlying"] > 0]
        spot = spots[0] if spots else 0
        if spot <= 0:
            continue

        for key, bb in bb_data.items():
            expiry, strike, opt_type = key

            moneyness = abs(strike - spot) / spot
            if moneyness > MAX_MONEYNESS:
                continue
            dte = _compute_dte(expiry)
            if dte < MIN_DTE or dte > MAX_DTE:
                continue

            dd = dd_data.get(key)
            if not dd:
                continue

            # Both in stablecoins now (Bybit USDT, Deribit USDC)
            b_bid = bb["bid_px"]
            b_ask = bb["ask_px"]
            d_bid = dd["bid_px_usd"]
            d_ask = dd["ask_px_usd"]

            # Arb edges (positive = profitable)
            # Sell Bybit (at bid), buy Deribit (at ask)
            edge_sell_bybit = b_bid - d_ask if b_bid > 0 and d_ask > 0 else 0
            # Sell Deribit (at bid), buy Bybit (at ask)
            edge_sell_deribit = d_bid - b_ask if d_bid > 0 and b_ask > 0 else 0

            is_tradeable = edge_sell_bybit > 0 or edge_sell_deribit > 0

            # Mark IV spread for analysis
            mark_spread_pts = 0
            if bb["mark_iv"] > 0 and dd["mark_iv"] > 0:
                mark_spread_pts = (bb["mark_iv"] - dd["mark_iv"]) * 100

            if is_tradeable:
                total_flagged += 1

            doc = {
                "coin": coin,
                "expiry": expiry,
                "strike": strike,
                "type": opt_type,
                "dte": dte,
                "moneyness": round(moneyness, 4),
                "spot": spot,
                "ts": now,
                "ts_ms": now_ms,
                # Bybit (USD prices)
                "b_bid": b_bid,
                "b_ask": b_ask,
                "b_mark": bb["mark_px"],
                "b_bid_iv": bb["bid_iv"],
                "b_ask_iv": bb["ask_iv"],
                "b_mark_iv": bb["mark_iv"],
                # Deribit (USD prices)
                "d_bid": round(d_bid, 4),
                "d_ask": round(d_ask, 4),
                "d_mark": round(dd["mark_px_usd"], 4),
                "d_mark_iv": dd["mark_iv"],
                # Arb
                "edge_sell_b": round(edge_sell_bybit, 4),
                "edge_sell_d": round(edge_sell_deribit, 4),
                "arb": is_tradeable,
            }
            docs.append(doc)

        total_matched += len([d for d in docs if d["coin"] == coin])

    if docs:
        collection.insert_many(docs, ordered=False)

    elapsed = time.time() - t0
    return {
        "matched": total_matched,
        "flagged": total_flagged,
        "elapsed": round(elapsed, 2),
        "time": now.strftime("%H:%M:%S"),
    }


def ensure_indexes(db):
    """Create indexes once on startup."""
    coll = db[COLLECTION]
    coll.create_index([("ts_ms", -1)], background=True)
    coll.create_index([("coin", 1), ("strike", 1), ("expiry", 1), ("ts_ms", -1)], background=True)
    coll.create_index([("arb", 1), ("ts_ms", -1)], background=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=1, help="Seconds between snapshots")
    args = parser.parse_args()

    session = HTTP(testnet=False)
    db = MongoClient(MONGO_URI)["quants_lab"]
    ensure_indexes(db)

    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(f"Starting dual-venue options collector. Interval: {args.interval}s")
    snapshot_count = 0

    while running:
        try:
            stats = collect_snapshot(session, db)
            snapshot_count += 1
            # Log every 60th snapshot (1 per minute at 1s interval) or if tradeable
            if stats["flagged"] > 0 or snapshot_count % 60 == 0:
                logger.info(
                    f"#{snapshot_count} {stats['matched']} pairs, "
                    f"{stats['flagged']} arb, {stats['elapsed']}s "
                    f"@ {stats['time']}"
                )
        except Exception as e:
            logger.error(f"Snapshot error: {e}")

        # Wait remainder of interval
        # Snapshot takes ~2s, so if interval=1, we run back-to-back
        # That's fine, it just means effective interval = snapshot_duration
        remaining = max(0, args.interval - 2)
        for _ in range(remaining):
            if not running:
                break
            time.sleep(1)

    logger.info(f"Shutdown after {snapshot_count} snapshots")


if __name__ == "__main__":
    main()
