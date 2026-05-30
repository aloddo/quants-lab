#!/usr/bin/env python3
"""Close a Bybit put credit spread by intent_id.

USE CASE: pre-expiry de-risking. Alberto approval REQUIRED before --execute.
Created 2026-05-29 06:50 CEST because bybit_options_trader.py 'close' is a stub
and we have 2 spreads expiring in ~3h with one (ETH 1900/2000) only $7 above
short strike.

Default is DRY-RUN: prints the orders that would be sent, returns 0.
With --execute: sends the orders. Logs full request/response.

Usage:
  # Dry-run (always run this first)
  python scripts/close_options_spread.py --intent-id v4-4565c5e7

  # Execute (Alberto approval required)
  python scripts/close_options_spread.py --intent-id v4-4565c5e7 --execute

Mechanics:
  - To close a put credit spread:
    - Long leg (we BOUGHT a put) -> place a SELL order to close it
    - Short leg (we SOLD a put)  -> place a BUY order to close it (= buy-back)
  - Uses LIMIT orders at mid-price by default (small qty, low impact).
    Add --market for MARKET orders if speed > price.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [close_spread] %(levelname)s: %(message)s",
)
logger = logging.getLogger("close_spread")


def load_env() -> None:
    env_path = Path("/Users/hermes/quants-lab/.env")
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


def get_session():
    from pybit.unified_trading import HTTP
    return HTTP(
        api_key=os.environ["BYBIT_API_KEY"],
        api_secret=os.environ["BYBIT_API_SECRET"],
    )


def get_intent(intent_id: str) -> dict:
    from pymongo import MongoClient
    db = MongoClient("mongodb://localhost:27017").quants_lab
    intent = db.options_v4_intents.find_one({"intent_id": intent_id})
    if intent is None:
        raise SystemExit(f"intent_id {intent_id} not found in options_v4_intents")
    return intent


def get_mid(session, symbol: str) -> tuple[float, float, float]:
    """Return (bid, ask, mid) for a symbol."""
    r = session.get_tickers(category="option", symbol=symbol)
    rec = r["result"]["list"][0]
    bid = float(rec["bid1Price"]) if rec.get("bid1Price") else None
    ask = float(rec["ask1Price"]) if rec.get("ask1Price") else None
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        # Fallback to mark
        mark = float(rec["markPrice"])
        return mark, mark, mark
    mid = (bid + ask) / 2
    return bid, ask, mid


def place(session, symbol: str, side: str, qty: float, price: float | None,
          order_link_id: str, dry: bool) -> dict:
    """Place an options order. Returns dict (dry-run = stub request only)."""
    req = {
        "category": "option",
        "symbol": symbol,
        "side": side,            # "Buy" or "Sell"
        "orderType": "Limit" if price is not None else "Market",
        "qty": str(qty),
        "orderLinkId": order_link_id,
        "reduceOnly": True,      # closing only
    }
    if price is not None:
        req["price"] = str(price)
        req["timeInForce"] = "IOC"  # immediate-or-cancel to avoid sitting; market-like behavior at limit price
    logger.info(f"REQUEST: {json.dumps(req)}")
    if dry:
        return {"dry_run": True, "request": req}
    resp = session.place_order(**req)
    logger.info(f"RESPONSE: {json.dumps(resp)}")
    return resp


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--intent-id", required=True, help="v4-XXXX intent id from options_v4_intents")
    ap.add_argument("--execute", action="store_true",
                    help="Actually send orders. Without this, just prints what would be sent.")
    ap.add_argument("--market", action="store_true",
                    help="Use MARKET orders. Default is LIMIT at mid-price (IOC).")
    ap.add_argument("--slippage-bps", type=float, default=50.0,
                    help="For LIMIT orders, willing-to-pay slippage from mid (default 50bps = 0.5%).")
    args = ap.parse_args(argv)

    load_env()
    intent = get_intent(args.intent_id)
    logger.info(f"Intent: {intent['intent_id']} {intent['strategy']} {intent['underlying']} "
                f"long={intent['long_symbol']} short={intent['short_symbol']} qty={intent['qty']}")
    if intent.get("status") != "both_filled":
        logger.warning(f"Intent status is {intent.get('status')}, expected 'both_filled'. Continue? Aborting for safety.")
        return 2

    session = get_session()

    # Compute close prices
    # CLOSE LONG = SELL the long-put (we own it). Want HIGH price.
    long_bid, long_ask, long_mid = get_mid(session, intent["long_symbol"])
    # CLOSE SHORT = BUY the short-put (we are short it). Want LOW price.
    short_bid, short_ask, short_mid = get_mid(session, intent["short_symbol"])
    logger.info(f"LONG  {intent['long_symbol']}: bid={long_bid} ask={long_ask} mid={long_mid:.2f}")
    logger.info(f"SHORT {intent['short_symbol']}: bid={short_bid} ask={short_ask} mid={short_mid:.2f}")

    # Limit prices: sell long at mid-slippage (accept lower), buy short at mid+slippage (accept higher)
    slip = args.slippage_bps / 10000.0
    sell_long_px = None if args.market else max(0.01, round(long_mid * (1 - slip), 2))
    buy_short_px = None if args.market else round(short_mid * (1 + slip), 2)

    qty = intent["qty"]
    suffix = intent["intent_id"][-8:]

    # Net debit/credit estimate at limit prices
    if not args.market:
        # We RECEIVE sell_long_px (closing long) and PAY buy_short_px (closing short).
        net_credit_per_unit = sell_long_px - buy_short_px
        total_net = qty * net_credit_per_unit
        logger.info(f"Limit close estimate: receive {sell_long_px} - pay {buy_short_px} = "
                    f"net {net_credit_per_unit:+.2f}/unit × {qty} qty = {total_net:+.2f} total")

    if not args.execute:
        logger.info("=== DRY-RUN — not sending orders. Pass --execute to fire. ===")

    # 1) Close long: SELL the long put
    r1 = place(session, intent["long_symbol"], "Sell", qty, sell_long_px,
               f"ov4-CL-{suffix}", dry=not args.execute)
    # 2) Close short: BUY back the short put
    r2 = place(session, intent["short_symbol"], "Buy", qty, buy_short_px,
               f"ov4-CS-{suffix}", dry=not args.execute)

    if args.execute:
        logger.info(f"long_close_resp={json.dumps(r1)}")
        logger.info(f"short_close_resp={json.dumps(r2)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
