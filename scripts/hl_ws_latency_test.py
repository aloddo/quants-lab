#!/usr/bin/env python3
"""
HL WebSocket Order Submission Latency Test

Measures the TRUE minimum latency by submitting orders via WebSocket
instead of REST. The HL WS protocol supports:
  {"method": "post", "type": "action", "payload": {signed order...}}

This eliminates HTTP overhead, TLS handshake, and connection setup
that adds ~100-200ms to the REST path.

Compares:
1. WS order place RTT (send → ack on same WS)
2. REST order place RTT (HTTP POST → response)
3. WS L2 update interval (baseline)
"""
import os
import sys
import time
import json
import threading
import statistics
import websocket
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from hyperliquid.utils.signing import (
    OrderType,
    sign_l1_action,
    order_type_to_wire,
    float_to_wire,
    order_request_to_order_wire,
    order_wires_to_order_action,
    get_timestamp_ms,
)


def pct(data, p):
    s = sorted(data)
    return s[min(int(len(s) * p / 100), len(s) - 1)]


def load_env():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    val = val.strip().strip("'\"")
                    os.environ.setdefault(key.strip(), val)


def build_order_action(coin: str, is_buy: bool, sz: float, price: float, tif: str = "Alo"):
    """Build the order action dict matching HL's wire format."""
    # Asset index lookup — we need the index, not the name
    # For simplicity, we'll use the Exchange class to build and sign the payload
    return {
        "type": "order",
        "orders": [{
            "a": None,  # Will be filled by the signing code
            "b": is_buy,
            "p": float_to_wire(price),
            "s": float_to_wire(sz),
            "r": False,
            "t": order_type_to_wire(OrderType(limit={"tif": tif})),
        }],
        "grouping": "na",
    }


def main():
    load_env()

    ALBERTO_MAIN = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
    private_key = os.environ["HL_PRIVATE_KEY"]
    wallet = eth_account.Account.from_key(private_key)

    print("=" * 60)
    print("  HL WEBSOCKET vs REST LATENCY COMPARISON")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Agent: {wallet.address[:10]}... → Main: {ALBERTO_MAIN[:10]}...")
    print("=" * 60)

    # Initialize SDK (REST) for comparison and for building signed payloads
    print("\nInitializing SDK...")
    for attempt in range(5):
        try:
            info = Info(constants.MAINNET_API_URL, skip_ws=True)
            break
        except Exception as e:
            if "429" in str(e):
                time.sleep(5 * (2 ** attempt))
            else:
                raise
    time.sleep(2)

    exchange = Exchange(wallet, constants.MAINNET_API_URL, account_address=ALBERTO_MAIN)
    time.sleep(2)

    # Set leverage
    exchange.update_leverage(5, "SOL", is_cross=True)
    time.sleep(2)

    mids = info.all_mids()
    time.sleep(2)
    sol_mid = float(mids["SOL"])
    test_price = round(sol_mid * 0.95, 3)
    sz = round(12.0 / test_price, 2)

    print(f"SOL mid: ${sol_mid:.3f}, test price: ${test_price}, size: {sz}")

    # ═══════════════════════════════════════════════════════════
    # PHASE 1: REST Order RTT (baseline, 5 orders)
    # ═══════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("  PHASE 1: REST Order Place+Cancel RTT (5 orders, 3s spacing)")
    print("=" * 60)

    rest_place_rtts = []
    rest_cancel_rtts = []

    for i in range(5):
        t0 = time.time()
        try:
            result = exchange.order("SOL", True, sz, test_price, OrderType(limit={"tif": "Alo"}))
        except Exception as e:
            print(f"  [{i+1}/5] ERROR: {e}")
            time.sleep(5)
            continue
        t1 = time.time()

        rtt = (t1 - t0) * 1000
        rest_place_rtts.append(rtt)

        statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
        s = statuses[0] if statuses else {}

        if "resting" in s:
            oid = s["resting"]["oid"]
            time.sleep(0.2)
            t2 = time.time()
            exchange.cancel("SOL", oid)
            t3 = time.time()
            rest_cancel_rtts.append((t3 - t2) * 1000)
            print(f"  [{i+1}/5] REST Place={rtt:.0f}ms, Cancel={(t3-t2)*1000:.0f}ms ✅")
        elif "error" in s:
            print(f"  [{i+1}/5] REST Place={rtt:.0f}ms ❌ {s['error']}")

        time.sleep(3)

    if rest_place_rtts:
        print(f"\n  REST Place: median={statistics.median(rest_place_rtts):.0f}ms, "
              f"mean={statistics.mean(rest_place_rtts):.0f}ms (n={len(rest_place_rtts)})")
    if rest_cancel_rtts:
        print(f"  REST Cancel: median={statistics.median(rest_cancel_rtts):.0f}ms (n={len(rest_cancel_rtts)})")

    # ═══════════════════════════════════════════════════════════
    # PHASE 2: WebSocket Order RTT
    # ═══════════════════════════════════════════════════════════
    print("\n  Cooling 10s...")
    time.sleep(10)
    print("\n" + "=" * 60)
    print("  PHASE 2: WebSocket Order Place+Cancel RTT (5 orders)")
    print("=" * 60)

    # We need to:
    # 1. Open a raw WS connection
    # 2. Build signed order payloads (using Exchange internals)
    # 3. Send via WS and measure ack time

    ws_url = "wss://api.hyperliquid.xyz/ws"
    ws_place_rtts = []
    ws_cancel_rtts = []
    ws_responses = {}
    ws_ready = threading.Event()
    ws_conn = [None]
    msg_id = [1000]

    def on_ws_message(ws, message):
        data = json.loads(message)
        if data.get("channel") == "post":
            resp_id = data.get("data", {}).get("id")
            if resp_id is not None:
                ws_responses[resp_id] = (time.time(), data)

    def on_ws_open(ws):
        ws_ready.set()

    def on_ws_error(ws, error):
        print(f"  WS error: {error}")

    # Connect
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_ws_message,
        on_open=on_ws_open,
        on_error=on_ws_error,
    )
    ws_thread = threading.Thread(target=ws.run_forever, daemon=True)
    ws_thread.start()
    ws_ready.wait(timeout=10)
    ws_conn[0] = ws
    print("  WS connected")
    time.sleep(1)

    # Build signed order payloads using Exchange internals
    # The Exchange class handles all the signing complexity
    for i in range(5):
        # Build the signed payload the same way Exchange.order() does,
        # but instead of POSTing via REST, we send via WS

        # Use the exchange to build the action + signature
        order_request = {
            "coin": "SOL",
            "is_buy": True,
            "sz": sz,
            "limit_px": test_price,
            "order_type": OrderType(limit={"tif": "Alo"}),
            "reduce_only": False,
        }

        # Replicate Exchange.bulk_orders signing, then send via WS
        timestamp = get_timestamp_ms()
        order_wire = order_request_to_order_wire(order_request, exchange.info.name_to_asset("SOL"))
        action = order_wires_to_order_action([order_wire], None, "na")

        signature = sign_l1_action(
            wallet, action, None, timestamp,
            exchange.expires_after, True
        )

        # Build WS message
        mid = msg_id[0]
        msg_id[0] += 1

        ws_payload = {
            "method": "post",
            "id": mid,
            "request": {
                "type": "action",
                "payload": {
                    "action": action,
                    "nonce": timestamp,
                    "signature": signature,
                    "vaultAddress": exchange.vault_address,
                    "expiresAfter": exchange.expires_after,
                },
            },
        }

        # Send and measure
        t0 = time.time()
        ws.send(json.dumps(ws_payload))

        # Wait for response
        deadline = time.time() + 10
        while mid not in ws_responses and time.time() < deadline:
            time.sleep(0.001)

        if mid in ws_responses:
            t_recv, resp_data = ws_responses[mid]
            rtt = (t_recv - t0) * 1000
            ws_place_rtts.append(rtt)

            # Check if order placed
            resp_payload = resp_data.get("data", {}).get("response", {})
            resp_type = resp_payload.get("type")
            payload_data = resp_payload.get("payload", resp_payload.get("data", {}))

            if resp_type == "action":
                statuses = payload_data.get("statuses", [])
                if statuses and isinstance(statuses[0], dict) and "resting" in statuses[0]:
                    oid = statuses[0]["resting"]["oid"]
                    print(f"  [{i+1}/5] WS Place={rtt:.0f}ms ✅ (oid={oid})")

                    # Cancel via WS too
                    time.sleep(0.2)
                    cancel_ts = get_timestamp_ms()
                    cancel_action = {
                        "type": "cancel",
                        "cancels": [{"a": exchange.info.name_to_asset["SOL"], "o": oid}],
                    }
                    cancel_sig = sign_l1_action(wallet, cancel_action, None, cancel_ts, None, True)

                    cmid = msg_id[0]
                    msg_id[0] += 1
                    cancel_payload = {
                        "method": "post",
                        "id": cmid,
                        "request": {
                            "type": "action",
                            "payload": {
                                "action": cancel_action,
                                "nonce": cancel_ts,
                                "signature": cancel_sig,
                                "vaultAddress": None,
                            },
                        },
                    }

                    ct0 = time.time()
                    ws.send(json.dumps(cancel_payload))
                    while cmid not in ws_responses and time.time() < ct0 + 10:
                        time.sleep(0.001)
                    if cmid in ws_responses:
                        ct_recv, _ = ws_responses[cmid]
                        ws_cancel_rtts.append((ct_recv - ct0) * 1000)

                elif statuses and isinstance(statuses[0], dict) and "error" in statuses[0]:
                    print(f"  [{i+1}/5] WS Place={rtt:.0f}ms ❌ {statuses[0]['error']}")
                else:
                    print(f"  [{i+1}/5] WS Place={rtt:.0f}ms ? {statuses}")
            elif resp_type == "error":
                print(f"  [{i+1}/5] WS Place={rtt:.0f}ms ❌ {payload_data}")
            else:
                print(f"  [{i+1}/5] WS Place={rtt:.0f}ms ? type={resp_type}")
        else:
            print(f"  [{i+1}/5] WS TIMEOUT (no response in 10s)")

        time.sleep(3)

    ws.close()

    if ws_place_rtts:
        print(f"\n  WS Place: median={statistics.median(ws_place_rtts):.0f}ms, "
              f"mean={statistics.mean(ws_place_rtts):.0f}ms (n={len(ws_place_rtts)})")
    if ws_cancel_rtts:
        print(f"  WS Cancel: median={statistics.median(ws_cancel_rtts):.0f}ms (n={len(ws_cancel_rtts)})")

    # ═══════════════════════════════════════════════════════════
    # COMPARISON
    # ═══════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("  REST vs WebSocket COMPARISON")
    print("=" * 60)

    results = {}
    if rest_place_rtts:
        rm = statistics.median(rest_place_rtts)
        results["rest_place_median_ms"] = round(rm)
        print(f"  REST Place:  median={rm:.0f}ms (n={len(rest_place_rtts)})")
    if ws_place_rtts:
        wm = statistics.median(ws_place_rtts)
        results["ws_place_median_ms"] = round(wm)
        print(f"  WS Place:    median={wm:.0f}ms (n={len(ws_place_rtts)})")
    if rest_place_rtts and ws_place_rtts:
        improvement = statistics.median(rest_place_rtts) - statistics.median(ws_place_rtts)
        pct_improvement = improvement / statistics.median(rest_place_rtts) * 100
        print(f"  Improvement: {improvement:.0f}ms ({pct_improvement:.0f}%)")
        results["improvement_ms"] = round(improvement)
        results["improvement_pct"] = round(pct_improvement)

    if rest_cancel_rtts:
        results["rest_cancel_median_ms"] = round(statistics.median(rest_cancel_rtts))
        print(f"  REST Cancel: median={statistics.median(rest_cancel_rtts):.0f}ms")
    if ws_cancel_rtts:
        results["ws_cancel_median_ms"] = round(statistics.median(ws_cancel_rtts))
        print(f"  WS Cancel:   median={statistics.median(ws_cancel_rtts):.0f}ms")

    # Effective latency = WS L2 interval + order RTT
    ws_l2_median = 530  # From prior measurement
    best_order = min(
        statistics.median(ws_place_rtts) if ws_place_rtts else 9999,
        statistics.median(rest_place_rtts) if rest_place_rtts else 9999,
    )
    print(f"\n  WS L2 interval: {ws_l2_median}ms (prior measurement)")
    print(f"  Best order RTT: {best_order:.0f}ms")
    print(f"  EFFECTIVE LATENCY: {ws_l2_median + best_order:.0f}ms")
    results["effective_latency_ms"] = round(ws_l2_median + best_order)

    # Verify clean state
    import requests
    resp = requests.post("https://api.hyperliquid.xyz/info",
                         json={"type": "openOrders", "user": ALBERTO_MAIN})
    orders = resp.json() or []
    if orders:
        print(f"\n  ⚠️ {len(orders)} dangling orders — cleaning up")
        for o in orders:
            try:
                exchange.cancel(o["coin"], o["oid"])
                time.sleep(1)
            except:
                pass
    else:
        print(f"\n  ✅ No dangling orders")

    # Save
    with open("/tmp/hl_ws_vs_rest_latency.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Report: /tmp/hl_ws_vs_rest_latency.json")


if __name__ == "__main__":
    main()
