#!/usr/bin/env python3
"""
Measure REAL Hyperliquid latency metrics — rate-limit-safe version.

Measures:
1. WebSocket L2 book update interval (multiple coins, WS is free of rate limits)
2. WebSocket trade feed interval
3. Order place round-trip (send → ack/reject) with 2s spacing
4. Order cancel round-trip
5. ALO rejection rate at various depths from mid
6. Account state (balances, margin, sub-account check)

Output: JSON report + human-readable summary.

RATE LIMIT NOTES:
- HL REST rate limit is IP-level, ~5-6 rapid calls before 429
- WS subscriptions are NOT rate-limited
- This script spaces REST calls by 2s minimum
- Run when other HL tasks are idle (not during funding collection)
"""
import os
import sys
import time
import json
import statistics
import asyncio
import threading
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from hyperliquid.utils.signing import OrderType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_percentile(data: list, pct: float) -> float:
    """Percentile without numpy."""
    s = sorted(data)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


def _rest_call_with_retry(fn, *args, max_retries=3, spacing_s=2.0, **kwargs):
    """Call a REST function with rate-limit-safe spacing and retry on 429."""
    for attempt in range(max_retries):
        try:
            result = fn(*args, **kwargs)
            time.sleep(spacing_s)  # Always space calls
            return result
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                wait = spacing_s * (2 ** attempt)
                print(f"  429 rate limited, waiting {wait:.0f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise
    return None


# ---------------------------------------------------------------------------
# WS Measurements (no rate limits)
# ---------------------------------------------------------------------------

def measure_ws_l2_latency(info: Info, coins: list, duration_s: float = 30.0) -> dict:
    """Measure WebSocket book update interval for multiple coins simultaneously."""
    print(f"\n{'='*60}")
    print(f"  WS L2 Book Update Interval ({', '.join(coins)}, {duration_s}s)")
    print(f"{'='*60}")

    update_data = {coin: {"times": [], "last": 0.0} for coin in coins}

    def make_handler(coin):
        def on_update(data):
            now = time.time()
            if update_data[coin]["last"] > 0:
                update_data[coin]["times"].append(now - update_data[coin]["last"])
            update_data[coin]["last"] = now
        return on_update

    # Subscribe all coins
    sub_ids = {}
    for coin in coins:
        sub_ids[coin] = info.subscribe(
            {"type": "l2Book", "coin": coin}, make_handler(coin)
        )

    time.sleep(duration_s)

    # Unsubscribe
    for coin in coins:
        try:
            info.unsubscribe({"type": "l2Book", "coin": coin}, sub_ids[coin])
        except Exception:
            pass

    results = {}
    for coin in coins:
        times = update_data[coin]["times"]
        if not times:
            print(f"  {coin}: no updates received")
            continue

        times_ms = [t * 1000 for t in times]
        stats = {
            "n_updates": len(times),
            "updates_per_sec": round(len(times) / duration_s, 2),
            "mean_ms": round(statistics.mean(times_ms), 1),
            "median_ms": round(statistics.median(times_ms), 1),
            "p95_ms": round(_safe_percentile(times_ms, 95), 1),
            "p99_ms": round(_safe_percentile(times_ms, 99), 1),
            "min_ms": round(min(times_ms), 1),
            "max_ms": round(max(times_ms), 1),
        }
        results[coin] = stats
        print(f"  {coin}: {stats['updates_per_sec']}/sec, "
              f"median={stats['median_ms']:.0f}ms, "
              f"P95={stats['p95_ms']:.0f}ms, "
              f"P99={stats['p99_ms']:.0f}ms")

    return results


def measure_ws_trades(info: Info, coins: list, duration_s: float = 20.0) -> dict:
    """Measure trade feed message interval."""
    print(f"\n{'='*60}")
    print(f"  WS Trade Feed ({', '.join(coins)}, {duration_s}s)")
    print(f"{'='*60}")

    trade_data = {coin: {"msg_times": [], "trade_count": 0, "last_msg": 0.0} for coin in coins}

    def make_handler(coin):
        def on_trade(data):
            now = time.time()
            if trade_data[coin]["last_msg"] > 0:
                trade_data[coin]["msg_times"].append(now - trade_data[coin]["last_msg"])
            trade_data[coin]["last_msg"] = now
            if isinstance(data, list):
                trade_data[coin]["trade_count"] += len(data)
            elif isinstance(data, dict) and "data" in data:
                trade_data[coin]["trade_count"] += len(data["data"])
            else:
                trade_data[coin]["trade_count"] += 1
        return on_trade

    sub_ids = {}
    for coin in coins:
        sub_ids[coin] = info.subscribe(
            {"type": "trades", "coin": coin}, make_handler(coin)
        )

    time.sleep(duration_s)

    for coin in coins:
        try:
            info.unsubscribe({"type": "trades", "coin": coin}, sub_ids[coin])
        except Exception:
            pass

    results = {}
    for coin in coins:
        msg_times = trade_data[coin]["msg_times"]
        tc = trade_data[coin]["trade_count"]
        if not msg_times:
            print(f"  {coin}: no trade messages")
            continue

        msg_times_ms = [t * 1000 for t in msg_times]
        stats = {
            "n_messages": len(msg_times),
            "total_trades": tc,
            "trades_per_sec": round(tc / duration_s, 2),
            "msg_interval_median_ms": round(statistics.median(msg_times_ms), 1),
            "msg_interval_p95_ms": round(_safe_percentile(msg_times_ms, 95), 1),
        }
        results[coin] = stats
        print(f"  {coin}: {stats['trades_per_sec']} trades/sec, "
              f"msg interval median={stats['msg_interval_median_ms']:.0f}ms")

    return results


# ---------------------------------------------------------------------------
# Account & Fund Check (minimal REST calls)
# ---------------------------------------------------------------------------

def check_account_state(info: Info, address: str) -> dict:
    """Check perps margin and spot balance. Single REST call."""
    print(f"\n{'='*60}")
    print(f"  Account State")
    print(f"{'='*60}")

    try:
        user_state = info.user_state(address)
        time.sleep(2)  # Rate limit spacing
    except Exception as e:
        print(f"  ERROR fetching user state: {e}")
        return {"error": str(e)}

    margin = user_state.get("marginSummary", {})
    account_value = float(margin.get("accountValue", 0))
    total_margin = float(margin.get("totalMarginUsed", 0))
    free_margin = account_value - total_margin

    positions = user_state.get("assetPositions", [])
    open_positions = [p for p in positions if float(p.get("position", {}).get("szi", 0)) != 0]

    result = {
        "account_value_usd": round(account_value, 2),
        "total_margin_used": round(total_margin, 2),
        "free_margin": round(free_margin, 2),
        "open_positions": len(open_positions),
    }

    print(f"  Account value: ${account_value:.2f}")
    print(f"  Free margin: ${free_margin:.2f}")
    print(f"  Open positions: {len(open_positions)}")

    if free_margin < 5:
        print(f"  ⚠️ INSUFFICIENT MARGIN — need to transfer funds to perps")
        result["can_trade"] = False
    else:
        result["can_trade"] = True

    return result


# ---------------------------------------------------------------------------
# Order RTT (rate-limit-aware)
# ---------------------------------------------------------------------------

def measure_order_roundtrip(
    exchange: Exchange, info: Info, coin: str = "SOL",
    n_orders: int = 10, spacing_s: float = 2.0
) -> dict:
    """Measure order place and cancel RTT with proper spacing."""
    print(f"\n{'='*60}")
    print(f"  Order Round-Trip ({coin}, {n_orders} orders, {spacing_s}s spacing)")
    print(f"{'='*60}")

    # Get mid price (1 REST call)
    try:
        mids = info.all_mids()
        mid = float(mids.get(coin, 0))
        time.sleep(spacing_s)
    except Exception as e:
        print(f"  ERROR getting mid price: {e}")
        return {"error": str(e)}

    if mid <= 0:
        print(f"  ERROR: no mid price for {coin}")
        return {"error": "no mid price"}

    print(f"  Mid price: ${mid:.4f}")

    place_rtts = []
    cancel_rtts = []
    errors = []

    for i in range(n_orders):
        # Place ALO bid at 5% below mid (guaranteed won't fill, likely won't reject)
        test_price = round(mid * 0.95, 2 if mid > 10 else 4)
        order_type = OrderType(limit={"tif": "Alo"})
        # HL minimum order value is $10. Use 1.0 SOL (~$84) to be safe.
        sz = 1.0 if mid > 20 else round(15 / mid, 2)

        t0 = time.time()
        try:
            result = exchange.order(coin, True, sz, test_price, order_type)
        except Exception as e:
            errors.append(f"order_{i}: {e}")
            time.sleep(spacing_s * 2)  # Extra wait on error
            continue
        t1 = time.time()

        place_rtt = (t1 - t0) * 1000
        place_rtts.append(place_rtt)

        # Parse result
        if result and result.get("status") == "ok":
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if statuses and "resting" in statuses[0]:
                oid = statuses[0]["resting"]["oid"]
                # Cancel
                time.sleep(0.3)  # Small gap before cancel
                t2 = time.time()
                try:
                    exchange.cancel(coin, oid)
                except Exception as e:
                    errors.append(f"cancel_{i}: {e}")
                t3 = time.time()
                cancel_rtts.append((t3 - t2) * 1000)
                status_str = f"PLACED (oid={oid}) → CANCELLED"
            elif statuses and "error" in statuses[0]:
                err = statuses[0]["error"]
                errors.append(f"order_{i}_rejected: {err}")
                status_str = f"REJECTED: {err}"
            else:
                status_str = f"UNKNOWN: {statuses}"
        elif result and result.get("status") == "err":
            err = result.get("response", "unknown")
            errors.append(f"order_{i}_err: {err}")
            status_str = f"ERR: {err}"
        else:
            status_str = "NO RESPONSE"
            errors.append(f"order_{i}: no response")

        print(f"  [{i+1}/{n_orders}] {place_rtt:.0f}ms — {status_str}")
        time.sleep(spacing_s)

    result = {}
    if place_rtts:
        result["place"] = {
            "n": len(place_rtts),
            "mean_ms": round(statistics.mean(place_rtts), 0),
            "median_ms": round(statistics.median(place_rtts), 0),
            "min_ms": round(min(place_rtts), 0),
            "max_ms": round(max(place_rtts), 0),
            "p95_ms": round(_safe_percentile(place_rtts, 95), 0),
        }
        print(f"\n  Place RTT: n={len(place_rtts)}, "
              f"median={result['place']['median_ms']:.0f}ms, "
              f"mean={result['place']['mean_ms']:.0f}ms, "
              f"P95={result['place']['p95_ms']:.0f}ms")

    if cancel_rtts:
        result["cancel"] = {
            "n": len(cancel_rtts),
            "mean_ms": round(statistics.mean(cancel_rtts), 0),
            "median_ms": round(statistics.median(cancel_rtts), 0),
            "min_ms": round(min(cancel_rtts), 0),
            "max_ms": round(max(cancel_rtts), 0),
        }
        print(f"  Cancel RTT: n={len(cancel_rtts)}, "
              f"median={result['cancel']['median_ms']:.0f}ms, "
              f"mean={result['cancel']['mean_ms']:.0f}ms")

    if errors:
        result["errors"] = errors
        print(f"  Errors: {len(errors)}")
        for e in errors:
            print(f"    - {e}")

    return result


# ---------------------------------------------------------------------------
# ALO Reject Rate (rate-limit-aware)
# ---------------------------------------------------------------------------

def measure_alo_reject_rate(
    exchange: Exchange, info: Info, coin: str = "SOL",
    trials_per_depth: int = 5, spacing_s: float = 2.0
) -> dict:
    """Measure ALO rejection rate at various depths from mid."""
    print(f"\n{'='*60}")
    print(f"  ALO Reject Rate by Depth ({coin})")
    print(f"{'='*60}")

    try:
        mids = info.all_mids()
        mid = float(mids.get(coin, 0))
        time.sleep(spacing_s)
    except Exception as e:
        print(f"  ERROR: {e}")
        return {"error": str(e)}

    if mid <= 0:
        return {"error": "no mid price"}

    results = {}
    for depth_bps in [1, 2, 5, 10, 20, 50]:
        placed = 0
        rejected = 0
        reject_reasons = []

        for trial in range(trials_per_depth):
            test_price = round(mid * (1 - depth_bps / 10000), 2 if mid > 10 else 4)
            order_type = OrderType(limit={"tif": "Alo"})

            try:
                # HL minimum order value is $10. Use 1.0 SOL (~$84) to be safe.
                sz = 1.0 if mid > 20 else round(15 / mid, 2)
                result = exchange.order(coin, True, sz, test_price, order_type)
            except Exception as e:
                rejected += 1
                reject_reasons.append(str(e))
                time.sleep(spacing_s * 2)
                continue

            if result and result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses and "resting" in statuses[0]:
                    placed += 1
                    oid = statuses[0]["resting"]["oid"]
                    time.sleep(0.3)
                    try:
                        exchange.cancel(coin, oid)
                    except Exception:
                        pass
                elif statuses and "error" in statuses[0]:
                    rejected += 1
                    reject_reasons.append(statuses[0]["error"])
                else:
                    rejected += 1
                    reject_reasons.append(f"unknown: {statuses}")
            else:
                rejected += 1
                reject_reasons.append(str(result))

            time.sleep(spacing_s)

        total = placed + rejected
        reject_pct = rejected / total * 100 if total > 0 else 0
        results[f"{depth_bps}bps"] = {
            "placed": placed,
            "rejected": rejected,
            "reject_pct": round(reject_pct, 1),
        }
        symbol = "✅" if reject_pct == 0 else "⚠️" if reject_pct < 50 else "❌"
        print(f"  {symbol} {depth_bps}bps: {reject_pct:.0f}% rejected ({rejected}/{total})")
        if reject_reasons:
            print(f"      Reasons: {', '.join(set(reject_reasons))}")

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Load credentials
    # Try .env first, then environment
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    val = val.strip().strip("'\"")
                    if key.strip() in ("HL_PRIVATE_KEY", "HL_ADDRESS"):
                        os.environ[key.strip()] = val

    private_key = os.environ.get("HL_PRIVATE_KEY", "").strip()
    address = os.environ.get("HL_ADDRESS", "").strip()

    if not private_key or not address:
        print("ERROR: HL_PRIVATE_KEY and HL_ADDRESS not found in .env or environment")
        print("Add them to ~/quants-lab/.env:")
        print("  HL_PRIVATE_KEY=0x...")
        print("  HL_ADDRESS=0x...")
        sys.exit(1)

    wallet = eth_account.Account.from_key(private_key)
    print(f"{'='*60}")
    print(f"  HYPERLIQUID LATENCY MEASUREMENT")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Address: {address[:8]}...{address[-4:]}")
    print(f"  Wallet OK: {wallet.address.lower() == address.lower()}")
    print(f"{'='*60}")

    # Create SDK instances — Info() makes 2 REST calls on init (meta + spotMeta).
    # These can 429 if other HL tasks recently consumed the rate budget.
    print("\nInitializing SDK (may retry on 429)...")
    info = None
    for attempt in range(5):
        try:
            info = Info(constants.MAINNET_API_URL, skip_ws=False)
            break
        except Exception as e:
            if "429" in str(e):
                wait = 5 * (2 ** attempt)
                print(f"  SDK init 429'd, waiting {wait}s (attempt {attempt+1}/5)")
                time.sleep(wait)
            else:
                raise
    if info is None:
        print("ERROR: Could not initialize HL SDK after 5 retries. Rate limit exhausted.")
        print("Wait 60s and try again, or pause the HL pipeline tasks first.")
        sys.exit(1)
    time.sleep(3)

    exchange = None
    for attempt in range(3):
        try:
            exchange = Exchange(wallet, constants.MAINNET_API_URL)
            break
        except Exception as e:
            if "429" in str(e):
                time.sleep(5 * (2 ** attempt))
            else:
                raise
    if exchange is None:
        print("ERROR: Could not initialize Exchange after retries.")
        sys.exit(1)
    time.sleep(3)

    all_results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "address": f"{address[:8]}...{address[-4:]}",
    }

    # Phase 1: WS measurements (no rate limits, safe to run anytime)
    print("\n" + "=" * 60)
    print("  PHASE 1: WebSocket Measurements (no rate limit impact)")
    print("=" * 60)

    coins = ["SOL", "BTC", "ETH"]
    all_results["ws_l2"] = measure_ws_l2_latency(info, coins, duration_s=30)
    all_results["ws_trades"] = measure_ws_trades(info, coins, duration_s=20)

    # Phase 2: Account check (1 REST call)
    print("\n" + "=" * 60)
    print("  PHASE 2: Account State (1 REST call)")
    print("=" * 60)

    acct = check_account_state(info, address)
    all_results["account"] = acct

    if not acct.get("can_trade", False):
        print("\n  ⚠️ Cannot run order tests — insufficient margin.")
        print("  Transfer funds to perps sub-account on app.hyperliquid.xyz")
        print("  Then re-run this script.")
        all_results["order_tests"] = "SKIPPED — insufficient margin"
    else:
        # Phase 3: Order tests (rate-limit-aware, 2s spacing)
        print("\n" + "=" * 60)
        print("  PHASE 3: Order Tests (2s spacing, ~50s total)")
        print("=" * 60)

        all_results["order_rtt"] = measure_order_roundtrip(
            exchange, info, "SOL", n_orders=10, spacing_s=2.0
        )

        # Phase 4: ALO reject rate (rate-limit-aware)
        print("\n" + "=" * 60)
        print("  PHASE 4: ALO Reject Rate (2s spacing, ~60s total)")
        print("=" * 60)

        all_results["alo_reject"] = measure_alo_reject_rate(
            exchange, info, "SOL", trials_per_depth=5, spacing_s=3.0
        )

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)

    ws = all_results.get("ws_l2", {})
    for coin, data in ws.items():
        print(f"  WS L2 {coin}: {data['updates_per_sec']}/sec, "
              f"median={data['median_ms']:.0f}ms, P95={data['p95_ms']:.0f}ms")

    if isinstance(all_results.get("order_rtt"), dict) and "place" in all_results["order_rtt"]:
        p = all_results["order_rtt"]["place"]
        print(f"  Order Place: median={p['median_ms']:.0f}ms, P95={p['p95_ms']:.0f}ms (n={p['n']})")
    if isinstance(all_results.get("order_rtt"), dict) and "cancel" in all_results["order_rtt"]:
        c = all_results["order_rtt"]["cancel"]
        print(f"  Order Cancel: median={c['median_ms']:.0f}ms (n={c['n']})")

    if isinstance(all_results.get("alo_reject"), dict):
        for depth, data in all_results["alo_reject"].items():
            if isinstance(data, dict):
                print(f"  ALO {depth}: {data['reject_pct']}% rejected")

    # Effective latency estimate
    ws_median = None
    for coin_data in ws.values():
        ws_median = coin_data.get("median_ms")
        break
    order_median = None
    if isinstance(all_results.get("order_rtt"), dict) and "place" in all_results["order_rtt"]:
        order_median = all_results["order_rtt"]["place"]["median_ms"]

    if ws_median and order_median:
        effective = ws_median + order_median
        print(f"\n  EFFECTIVE LATENCY: {effective:.0f}ms "
              f"(WS {ws_median:.0f}ms + order {order_median:.0f}ms)")
        all_results["effective_latency_ms"] = round(effective, 0)

    # Save
    outfile = "/tmp/hl_latency_full_report.json"
    with open(outfile, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  Report saved: {outfile}")

    info.disconnect_websocket()


if __name__ == "__main__":
    main()
