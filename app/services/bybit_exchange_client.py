"""
Authenticated Bybit REST client — single source of truth for exchange state.

Covers: positions, order history, execution/fill history, closed PnL, wallet balance.
All endpoints use HMAC-SHA256 signing against the Bybit V5 API.

Configuration via env vars:
    BYBIT_DEMO_API_KEY / BYBIT_DEMO_API_SECRET — credentials
    BYBIT_API_BASE_URL — defaults to https://api-demo.bybit.com
                          set to https://api.bybit.com for mainnet
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

CATEGORY = "linear"  # USDT perpetuals


def _to_pair(symbol: str) -> str:
    """Convert Bybit symbol to standard pair format: BTCUSDT -> BTC-USDT."""
    if "-" in symbol:
        return symbol
    base = symbol.replace("USDT", "")
    return f"{base}-USDT"


def _to_symbol(pair: str) -> str:
    """Convert standard pair to Bybit symbol: BTC-USDT -> BTCUSDT."""
    return pair.replace("-", "")


class BybitExchangeClient:
    """Authenticated Bybit V5 REST client for account and position data."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("BYBIT_DEMO_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BYBIT_DEMO_API_SECRET", "")
        self.base_url = (
            base_url
            or os.getenv("BYBIT_API_BASE_URL", "https://api-demo.bybit.com")
        ).rstrip("/")

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    # ── Core HTTP ────────────────────────────────────────────

    def _sign(self, ts: str, recv_window: str, payload: str) -> str:
        sign_str = ts + self.api_key + recv_window + payload
        return hmac.new(
            self.api_secret.encode(), sign_str.encode(), hashlib.sha256
        ).hexdigest()

    def _headers(self, ts: str, recv_window: str, signature: str) -> dict:
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
        }

    async def _get(
        self, session: aiohttp.ClientSession, path: str, params: dict
    ) -> dict:
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        param_str = urlencode(params)
        signature = self._sign(ts, recv_window, param_str)
        headers = self._headers(ts, recv_window, signature)

        url = f"{self.base_url}{path}"
        if param_str:
            url = f"{url}?{param_str}"

        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()

        if data.get("retCode") != 0:
            raise RuntimeError(
                f"Bybit API error: {data.get('retMsg')} "
                f"(code {data.get('retCode')}) path={path}"
            )
        return data.get("result", {})

    # ── Positions ────────────────────────────────────────────

    async def fetch_positions(
        self, session: aiohttp.ClientSession
    ) -> list[dict]:
        """Fetch all open USDT perpetual positions.

        Returns list of dicts with keys: pair, side, qty, entry_price,
        mark_price, unrealised_pnl, realised_pnl, liq_price, leverage,
        position_value, created_time.
        """
        if not self.is_configured():
            logger.debug("BybitExchangeClient: no credentials configured")
            return []

        result = await self._get(session, "/v5/position/list", {
            "category": CATEGORY,
            "settleCoin": "USDT",
        })

        positions = []
        for pos in result.get("list", []):
            size = float(pos.get("size", 0))
            if size == 0:
                continue
            positions.append({
                "pair": _to_pair(pos["symbol"]),
                "side": pos["side"].upper(),
                "qty": size,
                "entry_price": float(pos.get("avgPrice", 0)),
                "mark_price": float(pos.get("markPrice", 0)),
                "unrealised_pnl": float(pos.get("unrealisedPnl", 0)),
                "realised_pnl": float(pos.get("cumRealisedPnl", 0)),
                "liq_price": pos.get("liqPrice", ""),
                "leverage": pos.get("leverage", "1"),
                "position_value": float(pos.get("positionValue", 0)),
                "created_time": int(pos.get("createdTime", 0)),
            })
        return positions

    # ── Order history ────────────────────────────────────────

    async def fetch_order_history(
        self,
        session: aiohttp.ClientSession,
        symbol: Optional[str] = None,
        limit: int = 50,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> list[dict]:
        """Fetch order history (filled, cancelled, etc).

        Returns list of dicts with keys: order_id, pair, side, order_type,
        avg_price, qty, filled_qty, filled_value, filled_fee, status,
        order_link_id, created_time, updated_time.
        """
        params: dict = {"category": CATEGORY, "limit": limit}
        if symbol:
            params["symbol"] = _to_symbol(symbol)
        if start_ms:
            params["startTime"] = start_ms
        if end_ms:
            params["endTime"] = end_ms

        result = await self._get(session, "/v5/order/history", params)

        orders = []
        for o in result.get("list", []):
            orders.append({
                "order_id": o.get("orderId", ""),
                "pair": _to_pair(o.get("symbol", "")),
                "side": o.get("side", ""),
                "order_type": o.get("orderType", ""),
                "avg_price": float(o.get("avgPrice", 0)),
                "qty": float(o.get("qty", 0)),
                "filled_qty": float(o.get("cumExecQty", 0)),
                "filled_value": float(o.get("cumExecValue", 0)),
                "filled_fee": float(o.get("cumExecFee", 0)),
                "status": o.get("orderStatus", ""),
                "order_link_id": o.get("orderLinkId", ""),
                "created_time": int(o.get("createdTime", 0)),
                "updated_time": int(o.get("updatedTime", 0)),
            })
        return orders

    # ── Executions (individual fills) ────────────────────────

    async def fetch_executions(
        self,
        session: aiohttp.ClientSession,
        symbol: Optional[str] = None,
        limit: int = 100,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> list[dict]:
        """Fetch execution/trade history (actual fills).

        Returns list of dicts with keys: exec_id, order_id, pair, side,
        exec_price, mark_price, exec_qty, exec_value, exec_fee, fee_rate,
        is_maker, order_link_id, closed_size, exec_time.
        """
        params: dict = {"category": CATEGORY, "limit": limit}
        if symbol:
            params["symbol"] = _to_symbol(symbol)
        if start_ms:
            params["startTime"] = start_ms
        if end_ms:
            params["endTime"] = end_ms

        result = await self._get(session, "/v5/execution/list", params)

        executions = []
        for e in result.get("list", []):
            executions.append({
                "exec_id": e.get("execId", ""),
                "order_id": e.get("orderId", ""),
                "pair": _to_pair(e.get("symbol", "")),
                "side": e.get("side", ""),
                "exec_price": float(e.get("execPrice", 0)),
                "mark_price": float(e.get("markPrice", 0)),
                "exec_qty": float(e.get("execQty", 0)),
                "exec_value": float(e.get("execValue", 0)),
                "exec_fee": float(e.get("execFee", 0)),
                "fee_rate": float(e.get("feeRate", 0)),
                "is_maker": e.get("isMaker", False),
                "order_link_id": e.get("orderLinkId", ""),
                "order_type": e.get("orderType", ""),
                "closed_size": float(e.get("closedSize", 0)),
                "exec_time": int(e.get("execTime", 0)),
            })
        return executions

    # ── Closed PnL ───────────────────────────────────────────

    async def fetch_closed_pnl(
        self,
        session: aiohttp.ClientSession,
        symbol: Optional[str] = None,
        limit: int = 50,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> list[dict]:
        """Fetch closed position PnL (complete round-trips).

        Returns list of dicts with keys: order_id, pair, side, avg_entry_price,
        avg_exit_price, qty, closed_pnl, open_fee, close_fee, leverage,
        fill_count, created_time.
        """
        params: dict = {"category": CATEGORY, "limit": limit}
        if symbol:
            params["symbol"] = _to_symbol(symbol)
        if start_ms:
            params["startTime"] = start_ms
        if end_ms:
            params["endTime"] = end_ms

        result = await self._get(session, "/v5/position/closed-pnl", params)

        closed = []
        for c in result.get("list", []):
            closed.append({
                "order_id": c.get("orderId", ""),
                "pair": _to_pair(c.get("symbol", "")),
                "side": c.get("side", ""),
                "avg_entry_price": float(c.get("avgEntryPrice", 0)),
                "avg_exit_price": float(c.get("avgExitPrice", 0)),
                "qty": float(c.get("qty", 0)),
                "closed_pnl": float(c.get("closedPnl", 0)),
                "cum_entry_value": float(c.get("cumEntryValue", 0)),
                "cum_exit_value": float(c.get("cumExitValue", 0)),
                "open_fee": float(c.get("openFee", 0)),
                "close_fee": float(c.get("closeFee", 0)),
                "leverage": c.get("leverage", "1"),
                "fill_count": int(c.get("fillCount", 0)),
                "created_time": int(c.get("createdTime", 0)),
                "updated_time": int(c.get("updatedTime", 0)),
            })
        return closed

    # ── Wallet balance ───────────────────────────────────────

    async def fetch_wallet_balance(
        self, session: aiohttp.ClientSession
    ) -> dict:
        """Fetch USDT wallet balance.

        Returns dict with keys: equity, available.
        """
        result = await self._get(
            session, "/v5/account/wallet-balance", {"accountType": "UNIFIED"}
        )
        for acct in result.get("list", []):
            for coin in acct.get("coin", []):
                if coin["coin"] == "USDT":
                    return {
                        "equity": float(coin.get("equity") or 0),
                        "available": float(coin.get("availableToWithdraw") or 0),
                    }
        return {"equity": 0, "available": 0}
