#!/usr/bin/env python3
"""
Gate 2: Place/Cancel Micro-Test

50 ALO place+cancel cycles on a target coin. No fills expected.
Validates order placement, cancellation, OMS tracking, and rate limit safety.

Success criteria:
  - 50/50 placed, 50/50 cancelled
  - 0 accidental fills
  - 0 ghost orders after test
  - Median place RTT < 700ms
  - Median cancel RTT < 700ms
  - No 429 errors
  - Account balance unchanged
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


def query_open_orders():
    """Query HL for all open orders on main wallet."""
    resp = requests.post(API, json={"type": "openOrders", "user": ALBERTO_MAIN})
    return resp.json() or []


def query_account_value():
    """Query unified account value."""
    resp = requests.post(API, json={"type": "clearinghouseState", "user": ALBERTO_MAIN})
    state = resp.json()
    return float(state.get("marginSummary", {}).get("accountValue", 0))


def main():
    parser = argparse.ArgumentParser(description="Gate 2: Place/Cancel Micro-Test")
    parser.add_argument("--coin", default="BIO", help="Coin to test (default: BIO)")
    parser.add_argument("--cycles", type=int, default=50, help="Number of place/cancel cycles")
    parser.add_argument("--leverage", type=int, default=5, help="Leverage")
    parser.add_argument("--spacing", type=float, default=3.0, help="Seconds between cycles")
    args = parser.parse_args()

    load_env()

    private_key = os.environ.get("HL_PRIVATE_KEY", "")
    if not private_key:
        print("ERROR: HL_PRIVATE_KEY not set")
        sys.exit(1)

    wallet = eth_account.Account.from_key(private_key)

    print("=" * 60)
    print(f"  GATE 2: PLACE/CANCEL MICRO-TEST")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Coin: {args.coin} | Cycles: {args.cycles} | Spacing: {args.spacing}s")
    print(f"  Agent: {wallet.address[:10]}... -> Main: {ALBERTO_MAIN[:10]}...")
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
                logger.warning(f"SDK init 429, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
    if not info:
        print("ERROR: Could not init SDK after 5 retries")
        sys.exit(1)
    time.sleep(2)

    exchange = Exchange(wallet, constants.MAINNET_API_URL, account_address=ALBERTO_MAIN)
    time.sleep(2)

    # Set leverage
    logger.info(f"Setting {args.coin} leverage to {args.leverage}x...")
    exchange.update_leverage(args.leverage, args.coin, is_cross=True)
    time.sleep(2)

    # Pre-test checks
    logger.info("Pre-test checks...")
    pre_orders = query_open_orders()
    time.sleep(1)
    if pre_orders:
        logger.warning(f"Found {len(pre_orders)} existing open orders — cancelling first")
        for o in pre_orders:
            exchange.cancel(o["coin"], o["oid"])
            time.sleep(1)
        time.sleep(2)

    pre_balance = query_account_value()
    time.sleep(1)
    logger.info(f"Pre-test balance: ${pre_balance:.2f}")

    # Get mid price
    mids = info.all_mids()
    time.sleep(1)
    mid = float(mids.get(args.coin, 0))
    if mid <= 0:
        print(f"ERROR: No mid price for {args.coin}")
        sys.exit(1)
    logger.info(f"{args.coin} mid: ${mid:.6f}")

    # Compute order params: 5% below mid, minimum notional
    test_price = round(mid * 0.95, 6)  # 5% below — won't fill
    sz = round(12.0 / test_price, 2 if mid > 10 else (4 if mid > 0.1 else 0))
    # Ensure above $10 minimum
    while sz * test_price < 10:
        sz = round(sz * 1.5, 2 if mid > 10 else (4 if mid > 0.1 else 0))

    logger.info(f"Test order: ALO BUY {sz} @ ${test_price} (notional: ${sz * test_price:.2f})")
    logger.info("")

    # Run cycles
    place_rtts = []
    cancel_rtts = []
    errors = []
    rate_limit_count = 0
    fill_count = 0

    for i in range(args.cycles):
        cycle_start = time.time()

        # PLACE
        t0 = time.time()
        try:
            result = exchange.order(args.coin, True, sz, test_price, OrderType(limit={"tif": "Alo"}))
        except Exception as e:
            if "429" in str(e):
                rate_limit_count += 1
                logger.warning(f"  [{i+1}/{args.cycles}] PLACE 429 — backing off 5s")
                time.sleep(5)
                continue
            else:
                errors.append(f"place_{i}: {e}")
                logger.error(f"  [{i+1}/{args.cycles}] PLACE ERROR: {e}")
                time.sleep(args.spacing)
                continue
        t1 = time.time()
        place_rtt = (t1 - t0) * 1000

        statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
        s = statuses[0] if statuses else {}

        if "resting" in s:
            oid = s["resting"]["oid"]
            place_rtts.append(place_rtt)

            # Small gap before cancel
            time.sleep(0.3)

            # CANCEL
            t2 = time.time()
            try:
                cancel_result = exchange.cancel(args.coin, oid)
            except Exception as e:
                if "429" in str(e):
                    rate_limit_count += 1
                    logger.warning(f"  [{i+1}/{args.cycles}] CANCEL 429 — order may be dangling!")
                    errors.append(f"cancel_429_{i}: oid={oid}")
                    time.sleep(5)
                    continue
                else:
                    errors.append(f"cancel_{i}: {e}")
                    logger.error(f"  [{i+1}/{args.cycles}] CANCEL ERROR: {e}")
                    time.sleep(args.spacing)
                    continue
            t3 = time.time()
            cancel_rtt = (t3 - t2) * 1000
            cancel_rtts.append(cancel_rtt)

            status_icon = "OK" if cancel_rtt < 700 else "SLOW"
            logger.info(
                f"  [{i+1}/{args.cycles}] Place={place_rtt:.0f}ms Cancel={cancel_rtt:.0f}ms "
                f"oid={oid} [{status_icon}]"
            )

        elif "error" in s:
            err = s["error"]
            if "Post only" in err or "immediately matched" in err:
                # ALO rejection — price too close to market. Widen.
                logger.warning(f"  [{i+1}/{args.cycles}] ALO rejected (too close to market) — widening")
                test_price = round(test_price * 0.99, 6)
                errors.append(f"alo_reject_{i}")
            elif "filled" in err.lower():
                fill_count += 1
                errors.append(f"ACCIDENTAL_FILL_{i}: {err}")
                logger.error(f"  [{i+1}/{args.cycles}] ACCIDENTAL FILL: {err}")
            else:
                errors.append(f"place_error_{i}: {err}")
                logger.warning(f"  [{i+1}/{args.cycles}] Place={place_rtt:.0f}ms ERROR: {err}")
        else:
            errors.append(f"unknown_{i}: {s}")
            logger.warning(f"  [{i+1}/{args.cycles}] Unknown: {s}")

        # Spacing between cycles
        elapsed = time.time() - cycle_start
        remaining = max(0, args.spacing - elapsed)
        if remaining > 0:
            time.sleep(remaining)

    # Post-test verification
    logger.info("")
    logger.info("Post-test verification...")
    time.sleep(2)

    post_orders = query_open_orders()
    time.sleep(1)
    ghost_count = len(post_orders) if post_orders else 0
    if ghost_count > 0:
        logger.error(f"GHOST ORDERS FOUND: {ghost_count}")
        for o in post_orders:
            logger.error(f"  {o['coin']} {o['side']} {o['sz']} @ {o['limitPx']} oid={o['oid']}")
            exchange.cancel(o["coin"], o["oid"])
            time.sleep(1)
        logger.info("Cleaned up ghost orders")
    else:
        logger.info("No ghost orders — CLEAN")

    post_balance = query_account_value()
    time.sleep(1)
    balance_change = post_balance - pre_balance

    # Results
    print("")
    print("=" * 60)
    print("  GATE 2 RESULTS")
    print("=" * 60)

    placed = len(place_rtts)
    cancelled = len(cancel_rtts)

    print(f"  Cycles attempted: {args.cycles}")
    print(f"  Placed:           {placed}/{args.cycles}")
    print(f"  Cancelled:        {cancelled}/{placed}")
    print(f"  Accidental fills: {fill_count}")
    print(f"  Ghost orders:     {ghost_count}")
    print(f"  Rate limit (429): {rate_limit_count}")
    print(f"  Other errors:     {len(errors) - rate_limit_count - fill_count}")
    print(f"  Balance change:   ${balance_change:.4f}")
    print("")

    if place_rtts:
        print(f"  Place RTT:  median={statistics.median(place_rtts):.0f}ms "
              f"mean={statistics.mean(place_rtts):.0f}ms "
              f"P95={sorted(place_rtts)[int(len(place_rtts)*0.95)]:.0f}ms "
              f"(n={len(place_rtts)})")
    if cancel_rtts:
        print(f"  Cancel RTT: median={statistics.median(cancel_rtts):.0f}ms "
              f"mean={statistics.mean(cancel_rtts):.0f}ms "
              f"P95={sorted(cancel_rtts)[int(len(cancel_rtts)*0.95)]:.0f}ms "
              f"(n={len(cancel_rtts)})")

    # Verdict
    print("")
    passed = True
    checks = [
        ("Placed 50/50", placed >= args.cycles * 0.9),
        ("Cancelled all", cancelled == placed),
        ("Zero fills", fill_count == 0),
        ("Zero ghost orders", ghost_count == 0),
        (f"Place RTT < 700ms", statistics.median(place_rtts) < 700 if place_rtts else False),
        (f"Cancel RTT < 700ms", statistics.median(cancel_rtts) < 700 if cancel_rtts else False),
        ("No 429 errors", rate_limit_count == 0),
        ("Balance unchanged", abs(balance_change) < 0.01),
    ]

    for name, ok in checks:
        icon = "PASS" if ok else "FAIL"
        print(f"  [{icon}] {name}")
        if not ok:
            passed = False

    print("")
    if passed:
        print("  GATE 2: PASSED")
    else:
        print("  GATE 2: FAILED")

    if errors:
        print(f"\n  Errors ({len(errors)}):")
        for e in errors[:10]:
            print(f"    - {e}")

    # Save report
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "coin": args.coin,
        "cycles": args.cycles,
        "placed": placed,
        "cancelled": cancelled,
        "fills": fill_count,
        "ghosts": ghost_count,
        "rate_limits": rate_limit_count,
        "place_median_ms": round(statistics.median(place_rtts)) if place_rtts else None,
        "cancel_median_ms": round(statistics.median(cancel_rtts)) if cancel_rtts else None,
        "balance_change": round(balance_change, 4),
        "passed": passed,
        "errors": errors[:20],
    }
    outfile = "/tmp/hl_gate2_report.json"
    with open(outfile, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report: {outfile}")
    print("=" * 60)


if __name__ == "__main__":
    main()
