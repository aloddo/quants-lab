"""
Authenticated Binance REST client for spot trading.

Covers: account info, spot orders (market/limit), balances, ticker prices.
Uses HMAC-SHA256 signing against the Binance API.

Configuration via env vars:
    BINANCE_API_KEY / BINANCE_API_SECRET — credentials
"""
import hashlib
import hmac
import logging
import os
import time
from typing import Optional
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger(__name__)

BASE_URL = "https://api.binance.com"


class BinanceAPIError(Exception):
    def __init__(self, code: int, msg: str, path: str):
        self.code = code
        self.msg = msg
        self.path = path
        super().__init__(f"Binance {path}: [{code}] {msg}")


class BinanceClient:
    """Authenticated Binance REST client for spot trading."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("BINANCE_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BINANCE_API_SECRET", "")
        self.base_url = base_url or BASE_URL
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-MBX-APIKEY": self.api_key}
            )

    def _sign(self, params: dict) -> str:
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return f"{query}&signature={sig}"

    async def _get(self, path: str, params: dict | None = None, signed: bool = True) -> dict:
        await self._ensure_session()
        params = params or {}
        if signed:
            qs = self._sign(params)
            url = f"{self.base_url}{path}?{qs}"
        else:
            url = f"{self.base_url}{path}"
            if params:
                url += f"?{urlencode(params)}"
        async with self._session.get(url) as resp:
            data = await resp.json()
            if isinstance(data, dict) and "code" in data and data["code"] != 200:
                raise BinanceAPIError(data["code"], data.get("msg", ""), path)
            return data

    async def _post(self, path: str, params: dict | None = None) -> dict:
        await self._ensure_session()
        params = params or {}
        qs = self._sign(params)
        url = f"{self.base_url}{path}?{qs}"
        async with self._session.post(url) as resp:
            data = await resp.json()
            if isinstance(data, dict) and "code" in data and data["code"] != 200:
                raise BinanceAPIError(data.get("code", -1), data.get("msg", ""), path)
            return data

    async def _delete(self, path: str, params: dict | None = None) -> dict:
        await self._ensure_session()
        params = params or {}
        qs = self._sign(params)
        url = f"{self.base_url}{path}?{qs}"
        async with self._session.delete(url) as resp:
            data = await resp.json()
            if isinstance(data, dict) and "code" in data and data["code"] != 200:
                raise BinanceAPIError(data.get("code", -1), data.get("msg", ""), path)
            return data

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Account ──────────────────────────────────────────────

    async def get_account(self) -> dict:
        return await self._get("/api/v3/account")

    async def get_balances(self) -> dict[str, dict]:
        """Return {asset: {free, locked}} for non-zero balances."""
        data = await self.get_account()
        return {
            a["asset"]: {"free": float(a["free"]), "locked": float(a["locked"])}
            for a in data.get("balances", [])
            if float(a["free"]) > 0 or float(a["locked"]) > 0
        }

    # ── Market data (public, no auth) ────────────────────────

    async def get_ticker_price(self, symbol: str | None = None) -> dict | list:
        """Get latest price. If symbol=None, returns all."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/api/v3/ticker/price", params, signed=False)

    async def get_book_ticker(self, symbol: str | None = None) -> dict | list:
        """Get best bid/ask. If symbol=None, returns all."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/api/v3/ticker/bookTicker", params, signed=False)

    async def get_exchange_info(self, symbol: str | None = None) -> dict:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/api/v3/exchangeInfo", params, signed=False)

    # ── Spot orders ──────────────────────────────────────────

    async def place_market_order(
        self, symbol: str, side: str, quantity: float | None = None,
        quote_quantity: float | None = None,
    ) -> dict:
        """Place a MARKET order. Use quantity for base asset, quote_quantity for quote."""
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
        }
        if quantity is not None:
            params["quantity"] = f"{quantity:.8f}".rstrip("0").rstrip(".")
        elif quote_quantity is not None:
            params["quoteOrderQty"] = f"{quote_quantity:.2f}"
        else:
            raise ValueError("Must provide quantity or quote_quantity")
        return await self._post("/api/v3/order", params)

    async def place_limit_order(
        self, symbol: str, side: str, quantity: float, price: float,
        time_in_force: str = "GTC",
    ) -> dict:
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": time_in_force,
            "quantity": f"{quantity:.8f}".rstrip("0").rstrip("."),
            "price": f"{price:.8f}".rstrip("0").rstrip("."),
        }
        return await self._post("/api/v3/order", params)

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        return await self._delete("/api/v3/order", {"symbol": symbol, "orderId": order_id})

    async def get_open_orders(self, symbol: str | None = None) -> list:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/api/v3/openOrders", params)

    async def get_order(self, symbol: str, order_id: int) -> dict:
        return await self._get("/api/v3/order", {"symbol": symbol, "orderId": order_id})
