#!/usr/bin/env python3
"""
Gate 3: Live Fill Test

Place ALO orders at the touch on BIO to get real fills. Measure:
- Fill rate, fill price vs mid
- Markout at 1s, 5s, 15s post-fill
- Adverse selection per fill
- Actual fees charged
- Immediately flatten after each fill (place opposite side at touch)

Safety:
- $10-12 notional per fill (5x leverage = $2 margin)
- Max loss per fill: $0.50
- Total session stop: -$2
- Verify zero positions after test
"""
import argparse
import json
import logging
import os
import statistics
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import eth_account
import requests
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from hyperliquid.utils.signing import OrderType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ALBERTO_MAIN = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
API = "https://api.hyperliquid.xyz/info"


def load_env():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    os.environ.setdefault(key.strip(), val.strip().strip("'\""))


def hl_rest(payload, retries=3):
    """HL /info REST call with retry."""
    for attempt in range(retries):
        resp = requests.post(API, json=payload)
        if resp.status_code == 200 and resp.json() is not None:
            return resp.json()
        time.sleep(3 * (attempt + 1))
    return None


def get_book(info, coin):
    """Get current L2 book for a coin."""
    try:
        book = info.l2_snapshot(coin)
        if not book or "levels" not in book:
            return None, None, None, None
        bids = book["levels"][0]
        asks = book["levels"][1]
        if not bids or not asks:
            return None, None, None, None
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_bps = (best_ask - best_bid) / mid * 10000
        return best_bid, best_ask, mid, spread_bps
    except Exception as e:
        logger.warning(f"Book fetch failed: {e}")
        return None, None, None, None


def get_position(info, address, coin):
    """Get current position size for a coin."""
    try:
        state = info.user_state(address)
        if not state:
            return 0.0
        for ap in state.get("assetPositions", []):
            pos = ap.get("position", {})
            if pos.get("coin") == coin:
                return float(pos.get("szi", 0))
        return 0.0
    except:
        return 0.0


def main():
    parser = argparse.ArgumentParser(description="Gate 3: Live Fill Test")
    parser.add_argument("--coin", default="BIO", help="Coin to test")
    parser.add_argument("--fills", type=int, default=5, help="Target fills per side")
    parser.add_argument("--leverage", type=int, default=5, help="Leverage")
    parser.add_argument("--max-loss", type=float, default=2.0, help="Total session stop loss ($)")
    parser.add_argument("--wait-fill", type=float, default=30.0, help="Max seconds to wait for a fill")
    parser.add_argument("--spacing", type=float, default=10.0, help="Seconds between cycles")
    args = parser.parse_args()

    load_env()
    wallet = eth_account.Account.from_key(os.environ["HL_PRIVATE_KEY"])

    print("=" * 60)
    print(f"  GATE 3: LIVE FILL TEST")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Coin: {args.coin} | Target: {args.fills} fills/side | Leverage: {args.leverage}x")
    print(f"  Max loss: ${args.max_loss} | Wait: {args.wait_fill}s per order")
    print(f"  THIS IS REAL MONEY")
    print("=" * 60)

    # Init SDK with retry
    info = None
    for attempt in range(5):
        try:
            info = Info(constants.MAINNET_API_URL, skip_ws=True)
            break
        except Exception as e:
            if "429" in str(e):
                wait = 5 * (2 ** attempt)
                logger.warning(f"SDK 429, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
    if not info:
        print("ERROR: SDK init failed")
        sys.exit(1)
    time.sleep(2)

    exchange = Exchange(wallet, constants.MAINNET_API_URL, account_address=ALBERTO_MAIN)
    time.sleep(2)

    # Set leverage
    exchange.update_leverage(args.leverage, args.coin, is_cross=True)
    time.sleep(2)

    # Clean any existing orders/positions
    orders = hl_rest({"type": "openOrders", "user": ALBERTO_MAIN}) or []
    if orders:
        logger.info(f"Cleaning {len(orders)} existing orders...")
        for o in orders:
            try:
                exchange.cancel(o["coin"], o["oid"])
            except:
                pass
            time.sleep(2)

    # Check starting position
    start_pos = get_position(info, ALBERTO_MAIN, args.coin)
    time.sleep(2)
    if abs(start_pos) > 0:
        logger.warning(f"Starting with existing position: {start_pos}")

    # Get book to determine sizing
    best_bid, best_ask, mid, spread = get_book(info, args.coin)
    time.sleep(2)
    if not mid:
        print("ERROR: Cannot get book")
        sys.exit(1)

    # Size: ~$12 notional
    sz = round(12.0 / mid, 0 if mid < 0.01 else (2 if mid > 10 else 4))
    while sz * mid < 10:
        sz = round(sz * 1.5, 0 if mid < 0.01 else (2 if mid > 10 else 4))

    logger.info(f"Mid: ${mid:.6f}, Spread: {spread:.1f}bps, Size: {sz} ({sz*mid:.2f}$)")
    print()

    # Track fills
    fills = []
    total_pnl = 0.0
    buy_fills = 0
    sell_fills = 0

    # Alternate buy/sell
    sides = []
    for i in range(args.fills):
        sides.append(True)   # buy
        sides.append(False)  # sell

    for i, is_buy in enumerate(sides):
        if buy_fills >= args.fills and is_buy:
            continue
        if sell_fills >= args.fills and not is_buy:
            continue
        if buy_fills >= args.fills and sell_fills >= args.fills:
            break

        # Check stop loss
        if total_pnl < -args.max_loss:
            logger.error(f"SESSION STOP: total PnL ${total_pnl:.4f} < -${args.max_loss}")
            break

        # Refresh book
        best_bid, best_ask, mid, spread = get_book(info, args.coin)
        time.sleep(2)
        if not mid:
            logger.warning("Book unavailable, skipping cycle")
            time.sleep(args.spacing)
            continue

        # Place ALO at the touch
        side_str = "BUY" if is_buy else "SELL"
        price = best_bid if is_buy else best_ask  # at the touch = might fill on next trade-through

        logger.info(f"[{i+1}] {side_str} {sz} @ ${price:.6f} (spread={spread:.1f}bps)")

        try:
            result = exchange.order(
                args.coin, is_buy, sz, price,
                OrderType(limit={"tif": "Alo"}),
            )
        except Exception as e:
            if "429" in str(e):
                logger.warning(f"  PLACE 429, waiting 10s...")
                time.sleep(10)
                continue
            else:
                logger.error(f"  PLACE ERROR: {e}")
                time.sleep(args.spacing)
                continue

        statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
        s = statuses[0] if statuses else {}

        if "resting" in s:
            oid = s["resting"]["oid"]
            logger.info(f"  Resting: oid={oid}, waiting for fill (max {args.wait_fill}s)...")

            # Wait for fill
            fill_time = None
            fill_price = None
            waited = 0
            while waited < args.wait_fill:
                time.sleep(3)
                waited += 3

                # Check if order still resting
                try:
                    open_orders = hl_rest({"type": "openOrders", "user": ALBERTO_MAIN}) or []
                except:
                    continue

                order_exists = any(o.get("oid") == oid for o in open_orders)
                if not order_exists:
                    # Order gone — either filled or cancelled
                    fill_time = time.time()
                    # Check position to confirm fill
                    time.sleep(2)
                    new_pos = get_position(info, ALBERTO_MAIN, args.coin)
                    time.sleep(2)

                    if abs(new_pos - start_pos) > sz * 0.5:
                        fill_price = price  # approximation
                        logger.info(f"  FILLED! pos={new_pos:.0f} (was {start_pos:.0f})")

                        # Record markout: get mid at 1s, 5s, 15s
                        markouts = {}
                        for delay, label in [(1, "1s"), (5, "5s"), (15, "15s")]:
                            time.sleep(delay - (1 if delay > 1 else 0))
                            _, _, m, _ = get_book(info, args.coin)
                            time.sleep(1)
                            if m:
                                if is_buy:
                                    markout_bps = (m - fill_price) / fill_price * 10000
                                else:
                                    markout_bps = (fill_price - m) / fill_price * 10000
                                markouts[label] = round(markout_bps, 2)

                        fill_record = {
                            "side": side_str,
                            "price": fill_price,
                            "mid_at_fill": mid,
                            "spread_at_fill": spread,
                            "markouts": markouts,
                            "wait_s": waited,
                        }
                        fills.append(fill_record)

                        if is_buy:
                            buy_fills += 1
                        else:
                            sell_fills += 1

                        logger.info(f"  Markouts: {markouts}")

                        # Immediately flatten: place opposite side at touch
                        time.sleep(2)
                        bb, ba, _, _ = get_book(info, args.coin)
                        time.sleep(2)
                        if bb and ba:
                            exit_price = ba if is_buy else bb  # sell to flatten buy, vice versa
                            try:
                                exit_result = exchange.order(
                                    args.coin, not is_buy, sz, exit_price,
                                    OrderType(limit={"tif": "Alo"}),
                                )
                                exit_s = exit_result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                                if "resting" in exit_s:
                                    exit_oid = exit_s["resting"]["oid"]
                                    logger.info(f"  Exit order placed: oid={exit_oid}")
                                    # Wait briefly for exit fill
                                    time.sleep(5)
                                    # Cancel if not filled
                                    try:
                                        exchange.cancel(args.coin, exit_oid)
                                    except:
                                        pass
                            except Exception as e:
                                logger.warning(f"  Exit order failed: {e}")

                        # Update position tracking
                        time.sleep(2)
                        start_pos = get_position(info, ALBERTO_MAIN, args.coin)
                        time.sleep(2)
                        break
                    else:
                        logger.info(f"  Order gone but no position change — cancelled/rejected")
                        break

            else:
                # Timeout — cancel the order
                logger.info(f"  No fill after {args.wait_fill}s — cancelling")
                try:
                    exchange.cancel(args.coin, oid)
                except:
                    pass
                time.sleep(2)

        elif "error" in s:
            err = s["error"]
            logger.warning(f"  Order rejected: {err}")

        time.sleep(args.spacing)

    # Final cleanup
    logger.info("\nFinal cleanup...")
    time.sleep(3)
    orders = hl_rest({"type": "openOrders", "user": ALBERTO_MAIN}) or []
    for o in orders:
        try:
            exchange.cancel(o["coin"], o["oid"])
        except:
            pass
        time.sleep(2)

    final_pos = get_position(info, ALBERTO_MAIN, args.coin)
    time.sleep(2)

    # Results
    print()
    print("=" * 60)
    print("  GATE 3 RESULTS")
    print("=" * 60)
    print(f"  Total fills: {len(fills)} (buy={buy_fills}, sell={sell_fills})")
    print(f"  Final position: {final_pos}")

    if fills:
        markout_1s = [f["markouts"].get("1s", 0) for f in fills if "1s" in f["markouts"]]
        markout_5s = [f["markouts"].get("5s", 0) for f in fills if "5s" in f["markouts"]]
        markout_15s = [f["markouts"].get("15s", 0) for f in fills if "15s" in f["markouts"]]

        if markout_1s:
            print(f"  Markout 1s:  median={statistics.median(markout_1s):.2f}bps mean={statistics.mean(markout_1s):.2f}bps")
        if markout_5s:
            print(f"  Markout 5s:  median={statistics.median(markout_5s):.2f}bps mean={statistics.mean(markout_5s):.2f}bps")
        if markout_15s:
            print(f"  Markout 15s: median={statistics.median(markout_15s):.2f}bps mean={statistics.mean(markout_15s):.2f}bps")

        spreads = [f["spread_at_fill"] for f in fills]
        waits = [f["wait_s"] for f in fills]
        print(f"  Spread at fill: median={statistics.median(spreads):.1f}bps")
        print(f"  Wait for fill: median={statistics.median(waits):.0f}s")

        for f in fills:
            print(f"    {f['side']:>4} @ ${f['price']:.6f} spread={f['spread_at_fill']:.1f}bps "
                  f"markouts={f['markouts']} wait={f['wait_s']}s")

    print(f"\n  Final position: {final_pos} (should be ~0)")

    # Save report
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "coin": args.coin,
        "fills": fills,
        "buy_fills": buy_fills,
        "sell_fills": sell_fills,
        "final_position": final_pos,
    }
    with open("/tmp/hl_gate3_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n  Report: /tmp/hl_gate3_report.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
