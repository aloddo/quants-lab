"""
Direct Bybit demo trading client — bypasses HB executor for order placement.

Uses the same REST API as the manual test that confirmed working.
Falls back to this when HB executor has connector issues.
"""
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

DEMO_BASE_URL = "https://api-demo.bybit.com"


class BybitDemoTrader:
    """Direct async Bybit demo API client for order management."""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _sign(self, ts: str, recv_window: str, payload: str) -> str:
        sign_str = ts + self.api_key + recv_window + payload
        return hmac.new(
            self.api_secret.encode(), sign_str.encode(), hashlib.sha256
        ).hexdigest()

    async def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._get_session()
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        payload = json.dumps(data)
        signature = self._sign(ts, recv_window, payload)

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

        async with session.post(
            f"{DEMO_BASE_URL}{path}", data=payload, headers=headers
        ) as resp:
            result = await resp.json(content_type=None)
            if result is None:
                raise RuntimeError(f"Bybit demo API returned null response for {path}")
            if result.get("retCode") != 0:
                raise RuntimeError(
                    f"Bybit demo API error: {result.get('retMsg')} (code {result.get('retCode')})"
                )
            return result

    async def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._get_session()
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        from urllib.parse import urlencode
        param_str = urlencode(params)
        signature = self._sign(ts, recv_window, param_str)

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": recv_window,
        }

        async with session.get(
            f"{DEMO_BASE_URL}{path}?{param_str}", headers=headers
        ) as resp:
            result = await resp.json()
            if result.get("retCode") != 0:
                raise RuntimeError(
                    f"Bybit demo API error: {result.get('retMsg')} (code {result.get('retCode')})"
                )
            return result

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: str,
        take_profit: Optional[str] = None,
        stop_loss: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a market order with optional TP/SL. side='Buy' or 'Sell'."""
        data = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": qty,
        }
        if take_profit:
            data["takeProfit"] = take_profit
        if stop_loss:
            data["stopLoss"] = stop_loss
        return await self._post("/v5/order/create", data)

    async def set_tp_sl(
        self,
        symbol: str,
        take_profit: Optional[str] = None,
        stop_loss: Optional[str] = None,
        position_idx: int = 0,
    ) -> Dict[str, Any]:
        """Set TP/SL on an existing position."""
        data = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx,
        }
        if take_profit:
            data["takeProfit"] = take_profit
        if stop_loss:
            data["stopLoss"] = stop_loss
        return await self._post("/v5/position/set-trading-stop", data)

    async def get_positions(self, symbol: Optional[str] = None) -> list:
        """Get open positions."""
        params = {"category": "linear", "settleCoin": "USDT"}
        if symbol:
            params["symbol"] = symbol
        result = await self._get("/v5/position/list", params)
        return result.get("result", {}).get("list", [])

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order by ID."""
        result = await self._get(
            "/v5/order/realtime",
            {"category": "linear", "orderId": order_id},
        )
        orders = result.get("result", {}).get("list", [])
        return orders[0] if orders else {}

    async def get_wallet_balance(self) -> Dict[str, Any]:
        """Get USDT balance."""
        result = await self._get(
            "/v5/account/wallet-balance", {"accountType": "UNIFIED"}
        )
        for acct in result.get("result", {}).get("list", []):
            for coin in acct.get("coin", []):
                if coin["coin"] == "USDT":
                    return {
                        "equity": float(coin.get("equity") or 0),
                        "available": float(coin.get("availableToWithdraw") or 0),
                    }
        return {"equity": 0, "available": 0}
