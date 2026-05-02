"""
WebSocket Order Client for Hyperliquid.

Sends orders via the WS post protocol instead of REST, avoiding REST rate limits.
The WS connection is shared with the L2/trade data subscriptions (via the SDK's
Info object), but order submissions use a separate raw WS connection to avoid
interfering with the SDK's subscription management.

Protocol:
    Send: {"method": "post", "id": N, "request": {"type": "action", "payload": {...}}}
    Recv: {"channel": "post", "data": {"id": N, "response": {"type": "action"|"error", ...}}}

The signed payload is identical to REST -- same EIP-712 signature, same action format.
Only the transport changes.
"""
import asyncio
import json
import logging
import threading
import time
from typing import Any, Optional

import websocket

from hyperliquid.utils.signing import (
    OrderType,
    sign_l1_action,
    order_request_to_order_wire,
    order_wires_to_order_action,
    get_timestamp_ms,
)

logger = logging.getLogger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"


class WSOrderClient:
    """Places and cancels orders via HL WebSocket, bypassing REST rate limits.

    Thread-safe: the WS runs in a background thread, responses are collected
    in a dict keyed by message ID. Callers await responses via asyncio events.
    """

    def __init__(self, wallet, exchange, is_mainnet: bool = True):
        """
        Args:
            wallet: eth_account LocalAccount (for signing)
            exchange: hyperliquid Exchange instance (for name_to_asset, vault_address)
            is_mainnet: True for mainnet, False for testnet
        """
        self.wallet = wallet
        self.exchange = exchange
        self.is_mainnet = is_mainnet
        self._msg_id = 1000
        self._responses: dict[int, tuple[float, dict]] = {}
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._lock = threading.Lock()

    def start(self):
        """Connect WS in background thread."""
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_message=self._on_message,
            on_open=self._on_open,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws_thread = threading.Thread(target=self._ws.run_forever, daemon=True)
        self._ws_thread.start()
        if not self._connected.wait(timeout=10):
            logger.error("WS order client: connection timeout")
            return False
        logger.info("WS order client connected")
        return True

    def stop(self):
        """Close WS connection."""
        if self._ws:
            self._ws.close()
        self._connected.clear()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def _on_open(self, ws):
        self._connected.set()

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("channel") == "post":
                resp_id = data.get("data", {}).get("id")
                if resp_id is not None:
                    self._responses[resp_id] = (time.time(), data)
        except Exception as e:
            logger.debug(f"WS order client parse error: {e}")

    def _on_error(self, ws, error):
        logger.warning(f"WS order client error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self._connected.clear()
        logger.warning(f"WS order client disconnected: {close_status_code}")

    def _next_id(self) -> int:
        with self._lock:
            self._msg_id += 1
            return self._msg_id

    def _build_payload(self, action: dict, msg_id: int) -> str:
        """Build a signed WS post message."""
        timestamp = get_timestamp_ms()
        signature = sign_l1_action(
            self.wallet, action, self.exchange.vault_address,
            timestamp, self.exchange.expires_after, self.is_mainnet,
        )
        payload = {
            "method": "post",
            "id": msg_id,
            "request": {
                "type": "action",
                "payload": {
                    "action": action,
                    "nonce": timestamp,
                    "signature": signature,
                    "vaultAddress": self.exchange.vault_address,
                    "expiresAfter": self.exchange.expires_after,
                },
            },
        }
        return json.dumps(payload)

    async def _send_and_wait(self, action: dict, timeout_s: float = 10.0) -> Optional[dict]:
        """Send action via WS and wait for response."""
        if not self.is_connected:
            logger.error("WS order client not connected")
            return None

        msg_id = self._next_id()
        payload = self._build_payload(action, msg_id)

        # Send
        t0 = time.time()
        try:
            self._ws.send(payload)
        except Exception as e:
            logger.error(f"WS send failed: {e}")
            return None

        # Wait for response (poll in asyncio-friendly way)
        deadline = t0 + timeout_s
        while time.time() < deadline:
            if msg_id in self._responses:
                t_recv, resp_data = self._responses.pop(msg_id)
                rtt = (t_recv - t0) * 1000
                logger.debug(f"WS order RTT: {rtt:.0f}ms")
                return resp_data.get("data", {}).get("response", {})
            await asyncio.sleep(0.005)

        logger.warning(f"WS order timeout (msg_id={msg_id})")
        return None

    async def place_order(
        self, coin: str, is_buy: bool, sz: float, price: float,
        tif: str = "Alo", reduce_only: bool = False,
    ) -> Optional[dict]:
        """Place an order via WS. Returns the response payload or None on failure.

        Returns dict with 'statuses' list on success, e.g.:
            {"type": "action", "payload": {"statuses": [{"resting": {"oid": 123}}]}}
        """
        order_request = {
            "coin": coin,
            "is_buy": is_buy,
            "sz": sz,
            "limit_px": price,
            "order_type": OrderType(limit={"tif": tif}),
            "reduce_only": reduce_only,
        }
        asset_id = self.exchange.info.name_to_asset(coin)
        order_wire = order_request_to_order_wire(order_request, asset_id)
        action = order_wires_to_order_action([order_wire], None, "na")

        resp = await self._send_and_wait(action)
        if not resp:
            return None

        if resp.get("type") == "action":
            return resp.get("payload", resp)
        elif resp.get("type") == "error":
            logger.warning(f"WS order error: {resp.get('payload', resp)}")
            return resp
        return resp

    async def cancel_order(self, coin: str, oid: int) -> Optional[dict]:
        """Cancel an order via WS."""
        asset_id = self.exchange.info.name_to_asset(coin)
        action = {
            "type": "cancel",
            "cancels": [{"a": asset_id, "o": oid}],
        }
        resp = await self._send_and_wait(action)
        return resp

    async def cancel_all(self, coin: str) -> Optional[dict]:
        """Cancel all orders for a coin via WS.

        Note: HL doesn't have a native cancel-all. We query open orders (REST)
        then cancel each via WS. The query is REST but cancels are WS.
        """
        # This still needs a REST call to get open orders
        # The caller should provide the OIDs if possible
        logger.warning("cancel_all via WS requires REST query for OIDs — use cancel_order with known OIDs")
        return None

    async def place_and_wait(
        self, coin: str, is_buy: bool, sz: float, price: float,
        tif: str = "Alo",
    ) -> tuple[Optional[int], float]:
        """Place order and return (oid, rtt_ms) or (None, rtt_ms) on failure."""
        t0 = time.time()
        result = await self.place_order(coin, is_buy, sz, price, tif)
        rtt = (time.time() - t0) * 1000

        if not result:
            return None, rtt

        statuses = result.get("statuses", [])
        if statuses and isinstance(statuses[0], dict) and "resting" in statuses[0]:
            oid = statuses[0]["resting"]["oid"]
            return oid, rtt
        elif statuses and isinstance(statuses[0], dict) and "error" in statuses[0]:
            logger.warning(f"WS place rejected: {statuses[0]['error']}")
            return None, rtt

        return None, rtt

    async def cancel_and_wait(self, coin: str, oid: int) -> tuple[bool, float]:
        """Cancel order and return (success, rtt_ms)."""
        t0 = time.time()
        result = await self.cancel_order(coin, oid)
        rtt = (time.time() - t0) * 1000

        if not result:
            return False, rtt

        # Check cancel success
        if result.get("type") == "action":
            payload = result.get("payload", result)
            statuses = payload.get("statuses", [])
            if statuses and statuses[0] == "success":
                return True, rtt
        return False, rtt
