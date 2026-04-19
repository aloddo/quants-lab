"""
H2 Order API — HMAC-signed REST order submission for Bybit perp + Binance spot.

Handles: submit, cancel, query orders, query positions, query balances.
All methods return normalized responses.
"""
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger(__name__)


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
        self, symbol: str, side: str, qty: float, price: float, order_type: str = "limit"
    ) -> str:
        """
        Submit a linear perp order.
        Returns order_id.
        """
        ts = str(int(time.time() * 1000))
        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side.capitalize(),  # "Buy" or "Sell"
            "orderType": "Market" if order_type == "market" else "Limit",
            "qty": str(qty),
            "timeInForce": "IOC",  # IOC for all orders: fill immediately or cancel
        }
        if order_type != "market" and price > 0:
            params["price"] = str(price)

        import json
        body = json.dumps(params)
        sig = self._sign(ts, body)

        async with self._session.post(
            f"{self.BASE_URL}/v5/order/create",
            headers=self._headers(ts, sig),
            data=body,
        ) as resp:
            data = await resp.json()
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
        import json
        body = json.dumps(params)
        sig = self._sign(ts, body)

        async with self._session.post(
            f"{self.BASE_URL}/v5/order/cancel",
            headers=self._headers(ts, sig),
            data=body,
        ) as resp:
            data = await resp.json()
            if data.get("retCode") != 0:
                logger.warning(f"Bybit cancel failed: {data.get('retMsg')} order={order_id}")
                return False
            logger.info(f"Bybit order cancelled: {order_id}")
            return True

    async def get_positions(self, symbol: str = "") -> list[dict]:
        """Query open perp positions."""
        ts = str(int(time.time() * 1000))
        params = {"category": "linear"}
        if symbol:
            params["symbol"] = symbol
        qs = urlencode(params)
        sig = self._sign(ts, qs)

        async with self._session.get(
            f"{self.BASE_URL}/v5/position/list?{qs}",
            headers=self._headers(ts, sig),
        ) as resp:
            data = await resp.json()
            if data.get("retCode") != 0:
                logger.error(f"Bybit positions query failed: {data.get('retMsg')}")
                return []
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
        ) as resp:
            data = await resp.json()
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
        ) as resp:
            data = await resp.json()
            if data.get("retCode") != 0:
                return []
            return data.get("result", {}).get("list", [])

    async def get_instrument_info(self, symbol: str) -> dict:
        """Query instrument info (min qty, lot step, min notional)."""
        async with self._session.get(
            f"{self.BASE_URL}/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol},
        ) as resp:
            data = await resp.json()
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
        self, symbol: str, side: str, qty: float, price: float, order_type: str = "limit"
    ) -> str:
        """
        Submit a spot order.
        Returns order_id (as string).
        """
        params = {
            "symbol": symbol,
            "side": side.upper(),  # "BUY" or "SELL"
            "type": "MARKET" if order_type == "market" else "LIMIT",
            "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
            "timestamp": str(int(time.time() * 1000)),
            "recvWindow": "5000",
        }
        if order_type != "market" and price > 0:
            params["price"] = f"{price:.8f}".rstrip("0").rstrip(".")
            params["timeInForce"] = "IOC"  # IOC: fill immediately or cancel

        params["signature"] = self._sign(params)

        async with self._session.post(
            f"{self.BASE_URL}/api/v3/order",
            headers=self._headers(),
            params=params,
        ) as resp:
            data = await resp.json()
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
        ) as resp:
            data = await resp.json()
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
        ) as resp:
            data = await resp.json()
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
        ) as resp:
            data = await resp.json()
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
        ) as resp:
            data = await resp.json()
            if isinstance(data, list):
                return data
            logger.error(f"Binance trades query failed: {data}")
            return []

    async def get_exchange_info(self, symbol: str) -> dict:
        """Query symbol info (min notional, lot size, price filter)."""
        async with self._session.get(
            f"{self.BASE_URL}/api/v3/exchangeInfo",
            params={"symbol": symbol},
        ) as resp:
            data = await resp.json()
            symbols = data.get("symbols", [])
            return symbols[0] if symbols else {}
