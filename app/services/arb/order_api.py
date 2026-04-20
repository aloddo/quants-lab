"""
H2 Order API — HMAC-signed REST order submission for Bybit perp + Binance spot.

Handles: submit, cancel, query orders, query positions, query balances.
All methods return normalized responses.
"""
import asyncio
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import json

import aiohttp

logger = logging.getLogger(__name__)

# Standard timeout for all REST calls
_REQ_TIMEOUT = aiohttp.ClientTimeout(total=5)


# ── Bybit REST API (linear perps, mainnet) ──────────────────────

class BybitOrderAPI:
    """Bybit V5 API with HMAC-SHA256 authentication."""

    BASE_URL = "https://api.bybit.com"
    RECV_WINDOW = "5000"

    def __init__(self, api_key: str, api_secret: str, session: aiohttp.ClientSession):
        self._key = api_key
        self._secret = api_secret
        self._session = session

    def _sign(self, timestamp: str, params_str: str) -> str:
        sign_str = f"{timestamp}{self._key}{self.RECV_WINDOW}{params_str}"
        return hmac.new(self._secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()

    def _headers(self, timestamp: str, signature: str) -> dict:
        return {
            "X-BAPI-API-KEY": self._key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.RECV_WINDOW,
            "Content-Type": "application/json",
        }

    async def submit_order(
        self, symbol: str, side: str, qty: float, price: float, order_type: str = "limit",
        client_order_id: str = "",
    ) -> str:
        """
        Submit a linear perp order.
        Returns order_id. If client_order_id provided, Bybit tracks it as orderLinkId.
        """
        ts = str(int(time.time() * 1000))
        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side.capitalize(),
            "orderType": "Market" if order_type == "market" else "Limit",
            "qty": f"{qty:.8f}".rstrip("0").rstrip("."),
            "timeInForce": "IOC",
        }
        if client_order_id:
            params["orderLinkId"] = client_order_id
        if order_type != "market" and price > 0:
            params["price"] = f"{price:.8f}".rstrip("0").rstrip(".")

        body = json.dumps(params)
        sig = self._sign(ts, body)

        async with self._session.post(
            f"{self.BASE_URL}/v5/order/create",
            headers=self._headers(ts, sig),
            data=body,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Bybit order HTTP {resp.status}: {text[:200]}")
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError) as e:
                raise RuntimeError(f"Bybit order non-JSON response: {e}")
            if data.get("retCode") != 0:
                raise RuntimeError(f"Bybit order failed: {data.get('retMsg')} ({data.get('retCode')})")
            order_id = data["result"]["orderId"]
            logger.info(f"Bybit order submitted: {symbol} {side} {qty}@{price} -> {order_id}")
            return order_id

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel a linear perp order."""
        ts = str(int(time.time() * 1000))
        params = {
            "category": "linear",
            "symbol": symbol,
            "orderId": order_id,
        }
        body = json.dumps(params)
        sig = self._sign(ts, body)

        async with self._session.post(
            f"{self.BASE_URL}/v5/order/cancel",
            headers=self._headers(ts, sig),
            data=body,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.warning(f"Bybit cancel HTTP {resp.status} order={order_id}")
                return False
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.warning(f"Bybit cancel non-JSON response order={order_id}")
                return False
            if data.get("retCode") != 0:
                logger.warning(f"Bybit cancel failed: {data.get('retMsg')} order={order_id}")
                return False
            logger.info(f"Bybit order cancelled: {order_id}")
            return True

    async def get_positions(self, symbol: str = "") -> list[dict] | None:
        """Query open perp positions. Returns None on API failure (not empty list).
        Callers MUST distinguish None (check failed) from [] (confirmed no positions)."""
        ts = str(int(time.time() * 1000))
        params = {"category": "linear"}
        if symbol:
            params["symbol"] = symbol
        qs = urlencode(params)
        sig = self._sign(ts, qs)

        async with self._session.get(
            f"{self.BASE_URL}/v5/position/list?{qs}",
            headers=self._headers(ts, sig),
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Bybit positions query HTTP {resp.status}")
                return None  # API failure — not "no positions"
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.error("Bybit positions query non-JSON response")
                return None
            if data.get("retCode") != 0:
                logger.error(f"Bybit positions query failed: {data.get('retMsg')}")
                return None
            return data.get("result", {}).get("list", [])

    async def get_open_orders(self, symbol: str = "") -> list[dict]:
        """Query open orders."""
        ts = str(int(time.time() * 1000))
        params = {"category": "linear"}
        if symbol:
            params["symbol"] = symbol
        qs = urlencode(params)
        sig = self._sign(ts, qs)

        async with self._session.get(
            f"{self.BASE_URL}/v5/order/realtime?{qs}",
            headers=self._headers(ts, sig),
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Bybit open orders query HTTP {resp.status}")
                return []
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.error("Bybit open orders query non-JSON response")
                return []
            if data.get("retCode") != 0:
                logger.error(f"Bybit open orders query failed: {data.get('retMsg')}")
                return []
            return data.get("result", {}).get("list", [])

    async def get_executions(self, limit: int = 50) -> list[dict]:
        """Query recent executions (for gap recovery)."""
        ts = str(int(time.time() * 1000))
        params = {"category": "linear", "limit": str(limit)}
        qs = urlencode(params)
        sig = self._sign(ts, qs)

        async with self._session.get(
            f"{self.BASE_URL}/v5/execution/list?{qs}",
            headers=self._headers(ts, sig),
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                return []
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                return []
            if data.get("retCode") != 0:
                return []
            return data.get("result", {}).get("list", [])

    async def get_order(self, symbol: str, order_id: str = "", client_order_id: str = "") -> dict:
        """
        Query single order status — definitive answer for fill detection.

        Uses GET /v5/order/history (works for terminal + active orders, unlike
        /v5/order/realtime which is open-only).

        Returns: {
            "order_id": str,
            "client_order_id": str,
            "status": str,  # Filled, PartiallyFilled, Cancelled, New, Rejected
            "filled_qty": float,
            "avg_price": float,
            "orig_qty": float,
            "leaves_qty": float,
            "fill_result": "FILLED" | "PARTIAL" | "NOT_FILLED" | "NOT_FOUND" | "UNKNOWN",
        }

        Returns fill_result=NOT_FOUND when order doesn't exist on exchange.
        Returns fill_result=UNKNOWN on transient errors (network, rate limit).
        """
        ts = str(int(time.time() * 1000))
        params = {"category": "linear", "symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["orderLinkId"] = client_order_id
        else:
            raise ValueError("Must provide order_id or client_order_id")

        qs = urlencode(params)
        sig = self._sign(ts, qs)

        try:
            async with self._session.get(
                f"{self.BASE_URL}/v5/order/history?{qs}",
                headers=self._headers(ts, sig),
                timeout=_REQ_TIMEOUT,
            ) as resp:
                if resp.status >= 400:
                    logger.warning(f"Bybit get_order HTTP {resp.status}")
                    return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": f"http_{resp.status}"}
                try:
                    data = await resp.json()
                except (aiohttp.ContentTypeError, ValueError):
                    logger.warning("Bybit get_order non-JSON response (Cloudflare?)")
                    return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "non_json"}

                if data.get("retCode") != 0:
                    logger.warning(f"Bybit get_order error: {data.get('retMsg')}")
                    return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "error"}

                items = data.get("result", {}).get("list", [])
                if not items:
                    # Order not found -- never submitted or too old for history
                    return {"fill_result": "NOT_FOUND", "filled_qty": 0, "avg_price": 0, "status": "not_found"}

                item = items[0]
                cum_qty = float(item.get("cumExecQty", 0))
                avg_price = float(item.get("avgPrice", 0))
                orig_qty = float(item.get("qty", 0))
                status = item.get("orderStatus", "")

                # Determine fill result (4-state: FILLED, PARTIAL, NOT_FILLED, UNKNOWN)
                if status == "Filled" or (cum_qty > 0 and cum_qty >= orig_qty * 0.99):
                    fill_result = "FILLED"
                elif status == "PartiallyFilled":
                    fill_result = "PARTIAL"  # Actively partially filled, not yet complete
                elif status in ("Cancelled", "Rejected") and cum_qty > 0:
                    fill_result = "PARTIAL"  # Partial fill then cancelled
                elif status in ("Cancelled", "Rejected", "Deactivated") and cum_qty == 0:
                    fill_result = "NOT_FILLED"
                elif status == "New":
                    fill_result = "NOT_FILLED"  # Still open, not filled yet
                else:
                    fill_result = "UNKNOWN"

                return {
                    "order_id": item.get("orderId", ""),
                    "client_order_id": item.get("orderLinkId", ""),
                    "status": status,
                    "filled_qty": cum_qty,
                    "avg_price": avg_price,
                    "orig_qty": orig_qty,
                    "leaves_qty": float(item.get("leavesQty", 0)),
                    "fill_result": fill_result,
                }
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Bybit get_order transient error: {e}")
            return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "transport_error"}

    async def get_instrument_info(self, symbol: str) -> dict:
        """Query instrument info (min qty, lot step, min notional)."""
        async with self._session.get(
            f"{self.BASE_URL}/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol},
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Bybit instrument info HTTP {resp.status}")
                return {}
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                return {}
            items = data.get("result", {}).get("list", [])
            return items[0] if items else {}


# ── Binance REST API (spot) ─────────────────────────────────────

class BinanceOrderAPI:
    """Binance Spot API with HMAC-SHA256 authentication."""

    BASE_URL = "https://api.binance.com"

    def __init__(self, api_key: str, api_secret: str, session: aiohttp.ClientSession):
        self._key = api_key
        self._secret = api_secret
        self._session = session

    def _sign(self, params: dict) -> str:
        qs = urlencode(params)
        return hmac.new(self._secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _headers(self) -> dict:
        return {"X-MBX-APIKEY": self._key}

    async def submit_order(
        self, symbol: str, side: str, qty: float, price: float, order_type: str = "limit",
        client_order_id: str = "",
    ) -> str:
        """
        Submit a spot order.
        Returns order_id (as string). If client_order_id provided, Binance tracks it as newClientOrderId.
        """
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET" if order_type == "market" else "LIMIT",
            "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        if order_type != "market" and price > 0:
            params["price"] = f"{price:.8f}".rstrip("0").rstrip(".")
            params["timeInForce"] = "GTC"  # GTC: rest on book until filled or cancelled
            # On thin USDC books, IOC expires instantly with no fill.
            # GTC rests for a moment and catches the next trade.
            # The coordinator cancels unfilled GTC orders after 2-3 seconds.

        params["signature"] = self._sign(params)

        async with self._session.post(
            f"{self.BASE_URL}/api/v3/order",
            headers=self._headers(),
            params=params,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Binance order HTTP {resp.status}: {text[:200]}")
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError) as e:
                raise RuntimeError(f"Binance order non-JSON response: {e}")
            if "orderId" not in data:
                raise RuntimeError(f"Binance order failed: {data.get('msg', data)}")
            order_id = str(data["orderId"])
            logger.info(f"Binance order submitted: {symbol} {side} {qty}@{price} -> {order_id}")
            return order_id

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel a spot order."""
        params = {
            "symbol": symbol,
            "orderId": order_id,
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        params["signature"] = self._sign(params)

        async with self._session.delete(
            f"{self.BASE_URL}/api/v3/order",
            headers=self._headers(),
            params=params,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.warning(f"Binance cancel HTTP {resp.status} order={order_id}")
                return False
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.warning(f"Binance cancel non-JSON response order={order_id}")
                return False
            if "orderId" not in data:
                logger.warning(f"Binance cancel failed: {data.get('msg', data)} order={order_id}")
                return False
            logger.info(f"Binance order cancelled: {order_id}")
            return True

    async def get_account(self) -> dict:
        """Query account info (balances)."""
        params = {
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        params["signature"] = self._sign(params)

        async with self._session.get(
            f"{self.BASE_URL}/api/v3/account",
            headers=self._headers(),
            params=params,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Binance account query HTTP {resp.status}")
                return {}
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.error("Binance account query non-JSON response")
                return {}
            if "balances" not in data:
                logger.error(f"Binance account query failed: {data.get('msg', data)}")
                return {}
            return data

    async def get_balance(self, asset: str) -> float:
        """Query free balance for a specific asset."""
        account = await self.get_account()
        for bal in account.get("balances", []):
            if bal["asset"] == asset:
                return float(bal["free"])
        return 0.0

    async def get_open_orders(self, symbol: str = "") -> list[dict]:
        """Query open orders."""
        params = {
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        if symbol:
            params["symbol"] = symbol
        params["signature"] = self._sign(params)

        async with self._session.get(
            f"{self.BASE_URL}/api/v3/openOrders",
            headers=self._headers(),
            params=params,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Binance open orders HTTP {resp.status}")
                return []
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.error("Binance open orders non-JSON response")
                return []
            if isinstance(data, list):
                return data
            logger.error(f"Binance open orders failed: {data}")
            return []

    async def get_my_trades(self, symbol: str, limit: int = 50) -> list[dict]:
        """Query recent trades for a symbol (for gap recovery)."""
        params = {
            "symbol": symbol,
            "limit": str(limit),
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        params["signature"] = self._sign(params)

        async with self._session.get(
            f"{self.BASE_URL}/api/v3/myTrades",
            headers=self._headers(),
            params=params,
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Binance trades query HTTP {resp.status}")
                return []
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                logger.error("Binance trades query non-JSON response")
                return []
            if isinstance(data, list):
                return data
            logger.error(f"Binance trades query failed: {data}")
            return []

    async def get_order(self, symbol: str, order_id: str = "", client_order_id: str = "") -> dict:
        """
        Query single order status — definitive answer for fill detection.

        Uses GET /api/v3/order (single order lookup, not myTrades scan).
        THIS IS THE CRITICAL MISSING METHOD that caused 91% false-unfilled verdicts.

        Returns: {
            "order_id": str,
            "client_order_id": str,
            "status": str,  # NEW, PARTIALLY_FILLED, FILLED, CANCELED, EXPIRED, REJECTED
            "filled_qty": float,
            "avg_price": float,
            "orig_qty": float,
            "leaves_qty": float,
            "fill_result": "FILLED" | "PARTIAL" | "NOT_FILLED" | "NOT_FOUND" | "UNKNOWN",
        }

        Returns fill_result=NOT_FOUND when order doesn't exist on exchange.
        Returns fill_result=UNKNOWN on transient errors (network, rate limit).
        """
        params = {
            "symbol": symbol,
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        else:
            raise ValueError("Must provide order_id or client_order_id")

        params["signature"] = self._sign(params)

        try:
            async with self._session.get(
                f"{self.BASE_URL}/api/v3/order",
                headers=self._headers(),
                params=params,
                timeout=_REQ_TIMEOUT,
            ) as resp:
                # Track rate limit consumption
                used_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M", "0")
                if int(used_weight) > 960:  # 80% of 1200 limit
                    logger.warning(f"Binance rate limit high: {used_weight}/1200 weight used")

                if resp.status >= 400:
                    # Try to parse error JSON, but handle non-JSON (Cloudflare HTML)
                    try:
                        data = await resp.json()
                        msg = data.get("msg", str(data))
                        if "Order does not exist" in msg or data.get("code") == -2013:
                            return {"fill_result": "NOT_FOUND", "filled_qty": 0, "avg_price": 0, "status": "not_found"}
                    except (aiohttp.ContentTypeError, ValueError):
                        pass
                    logger.warning(f"Binance get_order HTTP {resp.status}")
                    return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": f"http_{resp.status}"}

                try:
                    data = await resp.json()
                except (aiohttp.ContentTypeError, ValueError):
                    logger.warning("Binance get_order non-JSON response (Cloudflare?)")
                    return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "non_json"}

                if "orderId" not in data:
                    msg = data.get("msg", str(data))
                    if "Order does not exist" in msg or data.get("code") == -2013:
                        return {"fill_result": "NOT_FOUND", "filled_qty": 0, "avg_price": 0, "status": "not_found"}
                    logger.warning(f"Binance get_order error: {msg}")
                    return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "error"}

                executed_qty = float(data.get("executedQty", 0))
                cum_quote = float(data.get("cummulativeQuoteQty", 0))
                avg_price = cum_quote / executed_qty if executed_qty > 0 else 0
                orig_qty = float(data.get("origQty", 0))
                status = data.get("status", "")

                # Determine fill result (4-state: FILLED, PARTIAL, NOT_FILLED, UNKNOWN)
                if status == "FILLED" or (executed_qty > 0 and executed_qty >= orig_qty * 0.99):
                    fill_result = "FILLED"
                elif status == "PARTIALLY_FILLED" and executed_qty > 0:
                    fill_result = "PARTIAL"  # Actively partially filled, not yet complete
                elif status in ("CANCELED", "EXPIRED") and executed_qty > 0:
                    fill_result = "PARTIAL"  # Partial fill then cancelled
                elif status in ("CANCELED", "EXPIRED", "REJECTED") and executed_qty == 0:
                    fill_result = "NOT_FILLED"
                elif status == "NEW":
                    fill_result = "NOT_FILLED"  # Still open, not filled yet
                else:
                    fill_result = "UNKNOWN"

                return {
                    "order_id": str(data.get("orderId", "")),
                    "client_order_id": data.get("clientOrderId", ""),
                    "status": status,
                    "filled_qty": executed_qty,
                    "avg_price": avg_price,
                    "orig_qty": orig_qty,
                    "leaves_qty": orig_qty - executed_qty,
                    "fill_result": fill_result,
                }
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Binance get_order transient error: {e}")
            return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "transport_error"}

    async def get_exchange_info(self, symbol: str) -> dict:
        """Query symbol info (min notional, lot size, price filter)."""
        async with self._session.get(
            f"{self.BASE_URL}/api/v3/exchangeInfo",
            params={"symbol": symbol},
            timeout=_REQ_TIMEOUT,
        ) as resp:
            if resp.status >= 400:
                logger.error(f"Binance exchange info HTTP {resp.status}")
                return {}
            try:
                data = await resp.json()
            except (aiohttp.ContentTypeError, ValueError):
                return {}
            symbols = data.get("symbols", [])
            return symbols[0] if symbols else {}
