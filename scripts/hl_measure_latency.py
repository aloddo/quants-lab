#!/usr/bin/env python3
"""
Measure REAL Hyperliquid latency metrics. Run post-maintenance.

Measures:
1. WebSocket L2 book update latency (time between updates)
2. Order place round-trip (send → ack/reject)
3. Order cancel round-trip
4. ALO rejection rate at various depths
5. Fill notification latency (UserFills WS vs polling)

Output: JSON report with all measured values.
"""
import os
import sys
import time
import json
import statistics
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from hyperliquid.utils.signing import OrderType


def measure_ws_latency(info: Info, coin: str = "SOL", duration_s: float = 30.0):
    """Measure WebSocket book update interval and latency."""
    print(f"\n=== WS L2 Book Latency ({coin}, {duration_s}s) ===")

    update_times = []
    last_update = [0.0]

    def on_l2_update(data):
        now = time.time()
        if last_update[0] > 0:
            update_times.append(now - last_update[0])
        last_update[0] = now

    sub_id = info.subscribe({"type": "l2Book", "coin": coin}, on_l2_update)
    time.sleep(duration_s)
    info.unsubscribe({"type": "l2Book", "coin": coin}, sub_id)

    if update_times:
        print(f"  Updates received: {len(update_times)}")
        print(f"  Mean interval: {statistics.mean(update_times)*1000:.1f}ms")
        print(f"  Median interval: {statistics.median(update_times)*1000:.1f}ms")
        print(f"  P95 interval: {sorted(update_times)[int(len(update_times)*0.95)]*1000:.1f}ms")
        print(f"  Min/Max: {min(update_times)*1000:.1f}ms / {max(update_times)*1000:.1f}ms")
        return {
            "n_updates": len(update_times),
            "mean_ms": statistics.mean(update_times) * 1000,
            "median_ms": statistics.median(update_times) * 1000,
            "p95_ms": sorted(update_times)[int(len(update_times) * 0.95)] * 1000,
        }
    else:
        print("  No updates received!")
        return None


def measure_order_roundtrip(exchange: Exchange, info: Info, coin: str = "SOL"):
    """Measure order place and cancel round-trip times."""
    print(f"\n=== Order Round-Trip ({coin}) ===")

    # Get current price
    mids = info.all_mids()
    mid = float(mids.get(coin, 0))
    if mid <= 0:
        print("  ERROR: Could not get mid price")
        return None

    # Place 5 orders far from market and measure RTT
    place_times = []
    cancel_times = []
    reject_count = 0

    for i in range(5):
        # Place at 10% below mid (won't fill, won't reject ALO)
        test_price = round(mid * 0.90, 2)
        order_type = OrderType(limit={"tif": "Alo"})

        t0 = time.time()
        result = exchange.order(coin, True, 0.1, test_price, order_type)
        t1 = time.time()

        rtt = (t1 - t0) * 1000
        place_times.append(rtt)

        # Check if placed or rejected
        if result and result.get("status") == "ok":
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if statuses and "resting" in statuses[0]:
                oid = statuses[0]["resting"]["oid"]
                # Cancel it
                t2 = time.time()
                exchange.cancel(coin, oid)
                t3 = time.time()
                cancel_times.append((t3 - t2) * 1000)
            elif statuses and "error" in statuses[0]:
                reject_count += 1
        else:
            reject_count += 1

        time.sleep(0.5)

    print(f"  Place RTT: mean={statistics.mean(place_times):.0f}ms, "
          f"median={statistics.median(place_times):.0f}ms, "
          f"min={min(place_times):.0f}ms, max={max(place_times):.0f}ms")
    if cancel_times:
        print(f"  Cancel RTT: mean={statistics.mean(cancel_times):.0f}ms, "
              f"median={statistics.median(cancel_times):.0f}ms")
    print(f"  Rejects: {reject_count}/5")

    return {
        "place_mean_ms": statistics.mean(place_times),
        "place_median_ms": statistics.median(place_times),
        "cancel_mean_ms": statistics.mean(cancel_times) if cancel_times else None,
        "reject_rate": reject_count / 5,
    }


def measure_alo_reject_rate(exchange: Exchange, info: Info, coin: str = "SOL"):
    """Measure ALO rejection rate at various depths from mid."""
    print(f"\n=== ALO Reject Rate by Depth ({coin}) ===")

    mids = info.all_mids()
    mid = float(mids.get(coin, 0))
    if mid <= 0:
        return None

    results = {}
    for depth_bps in [1, 2, 5, 10, 50]:
        placed = 0
        rejected = 0

        for _ in range(3):
            # Bid at mid - depth
            test_price = round(mid * (1 - depth_bps / 10000), 4)
            order_type = OrderType(limit={"tif": "Alo"})

            result = exchange.order(coin, True, 0.1, test_price, order_type)
            if result and result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses and "resting" in statuses[0]:
                    placed += 1
                    oid = statuses[0]["resting"]["oid"]
                    exchange.cancel(coin, oid)
                else:
                    rejected += 1
            else:
                rejected += 1
            time.sleep(0.3)

        total = placed + rejected
        reject_pct = rejected / total * 100 if total > 0 else 0
        print(f"  {depth_bps}bps from mid: {reject_pct:.0f}% rejected ({rejected}/{total})")
        results[f"{depth_bps}bps"] = {"placed": placed, "rejected": rejected, "reject_pct": reject_pct}

    return results


def main():
    private_key = os.environ.get("HL_PRIVATE_KEY")
    address = os.environ.get("HL_ADDRESS")
    if not private_key or not address:
        print("ERROR: Set HL_PRIVATE_KEY and HL_ADDRESS in .env")
        sys.exit(1)

    wallet = eth_account.Account.from_key(private_key)
    info = Info(constants.MAINNET_API_URL, skip_ws=False)  # WS ENABLED
    exchange = Exchange(wallet, constants.MAINNET_API_URL)

    print(f"=== HL Latency Measurement ===")
    print(f"Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"Address: {address[:12]}...")

    results = {}

    # 1. WS latency
    ws_result = measure_ws_latency(info, "SOL", duration_s=20)
    if ws_result:
        results["ws_l2_latency"] = ws_result

    # 2. Order round-trip
    order_result = measure_order_roundtrip(exchange, info, "SOL")
    if order_result:
        results["order_roundtrip"] = order_result

    # 3. ALO reject rate
    alo_result = measure_alo_reject_rate(exchange, info, "SOL")
    if alo_result:
        results["alo_reject_rate"] = alo_result

    # Save results
    print(f"\n=== SUMMARY ===")
    print(json.dumps(results, indent=2))

    outfile = "/tmp/hl_latency_report.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {outfile}")

    info.disconnect_websocket()


if __name__ == "__main__":
    main()
