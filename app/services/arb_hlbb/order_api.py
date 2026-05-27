"""
Dual-venue order API: Hyperliquid + Bybit.

HL: Uses WSOrderClient for ~100ms RTT (proven in HL MM).
Bybit: REST via pybit for order placement.

Both support IOC (Immediate-or-Cancel) for taker execution.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """Result of an order submission."""
    success: bool
    venue: str                  # "HL" or "BB"
    order_id: str = ""
    filled_qty: float = 0.0
    avg_price: float = 0.0
    status: str = ""            # "FILLED", "PARTIAL", "REJECTED", "TIMEOUT"
    error: str = ""
    latency_ms: float = 0.0
    raw: dict = None

    def __post_init__(self):
        if self.raw is None:
            self.raw = {}


class HLOrderAPI:
    """Hyperliquid order placement via WS (fast) or REST (fallback)."""

    def __init__(self, ws_client=None, exchange=None, info=None):
        """
        Args:
            ws_client: WSOrderClient instance (preferred, ~100ms)
            exchange: hyperliquid Exchange instance (REST fallback, ~500ms)
            info: hyperliquid Info instance (for position queries)
        """
        self.ws_client = ws_client
        self.exchange = exchange
        self.info = info
        self._use_ws = ws_client is not None

    async def place_ioc(self, coin: str, is_buy: bool, sz: float,
                        price: float, reduce_only: bool = False,
                        cloid: str = "") -> OrderResult:
        """Place an IOC order. Returns fill result.

        Args:
            cloid: Client order ID for idempotency/reconciliation. If empty, none sent.
        """
        t0 = time.time()

        if self._use_ws and self.ws_client and self.ws_client.is_connected:
            return await self._place_via_ws(coin, is_buy, sz, price, reduce_only, t0, cloid)
        elif self.exchange:
            return await self._place_via_rest(coin, is_buy, sz, price, reduce_only, t0, cloid)
        else:
            return OrderResult(
                success=False, venue="HL",
                status="REJECTED", error="No HL client available",
                latency_ms=(time.time() - t0) * 1000,
            )

    async def _place_via_ws(self, coin: str, is_buy: bool, sz: float,
                            price: float, reduce_only: bool, t0: float,
                            cloid: str = "") -> OrderResult:
        try:
            # NOTE: WSOrderClient does NOT support cloid parameter.
            # cloid is only usable with the REST exchange.order() API.
            result = await self.ws_client.place_order(
                coin=coin, is_buy=is_buy, sz=sz, price=price,
                tif="Ioc", reduce_only=reduce_only,
            )
            latency = (time.time() - t0) * 1000

            if not result:
                return OrderResult(
                    success=False, venue="HL",
                    status="TIMEOUT", latency_ms=latency,
                )

            # Parse response
            statuses = result.get("statuses", [])
            if not statuses:
                return OrderResult(
                    success=False, venue="HL",
                    status="REJECTED", error="Empty response",
                    latency_ms=latency, raw=result,
                )

            status = statuses[0]
            if isinstance(status, dict):
                if "filled" in status:
                    fill = status["filled"]
                    return OrderResult(
                        success=True, venue="HL",
                        order_id=str(fill.get("oid", "")),
                        filled_qty=float(fill.get("totalSz", sz)),
                        avg_price=float(fill.get("avgPx", price)),
                        status="FILLED",
                        latency_ms=latency, raw=result,
                    )
                elif "resting" in status:
                    # IOC should not rest — this means partial/no fill
                    rest = status["resting"]
                    return OrderResult(
                        success=False, venue="HL",
                        order_id=str(rest.get("oid", "")),
                        status="RESTING",
                        error="IOC order resting (should not happen)",
                        latency_ms=latency, raw=result,
                    )
                elif "error" in status:
                    return OrderResult(
                        success=False, venue="HL",
                        status="REJECTED",
                        error=str(status["error"]),
                        latency_ms=latency, raw=result,
                    )

            return OrderResult(
                success=False, venue="HL",
                status="UNKNOWN", error=f"Unexpected: {status}",
                latency_ms=latency, raw=result,
            )

        except Exception as e:
            return OrderResult(
                success=False, venue="HL",
                status="ERROR", error=str(e),
                latency_ms=(time.time() - t0) * 1000,
            )

    async def _place_via_rest(self, coin: str, is_buy: bool, sz: float,
                              price: float, reduce_only: bool, t0: float,
                              cloid: str = "") -> OrderResult:
        """REST order via exchange.order(). Primary path (WS disabled for reliability)."""
        try:
            from hyperliquid.utils.types import Cloid
            hl_cloid = Cloid.from_str(cloid) if cloid else None
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.exchange.order(
                    coin, is_buy, sz, price,
                    {"limit": {"tif": "Ioc"}},
                    reduce_only=reduce_only,
                    cloid=hl_cloid,
                ),
            )
            latency = (time.time() - t0) * 1000

            if result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses and isinstance(statuses[0], dict) and "filled" in statuses[0]:
                    fill = statuses[0]["filled"]
                    return OrderResult(
                        success=True, venue="HL",
                        order_id=str(fill.get("oid", "")),
                        filled_qty=float(fill.get("totalSz", sz)),
                        avg_price=float(fill.get("avgPx", price)),
                        status="FILLED",
                        latency_ms=latency, raw=result,
                    )

            return OrderResult(
                success=False, venue="HL",
                status="REJECTED", error=str(result),
                latency_ms=latency, raw=result,
            )

        except Exception as e:
            return OrderResult(
                success=False, venue="HL",
                status="ERROR", error=str(e),
                latency_ms=(time.time() - t0) * 1000,
            )

    def get_positions(self, address: str) -> list[dict]:
        """Query all open positions on HL."""
        if not self.info:
            return []
        try:
            state = self.info.user_state(address)
            positions = []
            for ap in state.get("assetPositions", []):
                pos = ap.get("position", {})
                sz = float(pos.get("szi", 0))
                if sz != 0:
                    positions.append({
                        "coin": pos.get("coin", ""),
                        "size": sz,
                        "entry_price": float(pos.get("entryPx", 0)),
                        "unrealized_pnl": float(pos.get("unrealizedPnl", 0)),
                        "leverage": float(pos.get("leverage", {}).get("value", 1)),
                    })
            return positions
        except Exception as e:
            logger.error(f"HL position query failed: {e}")
            return []

    def get_balance(self, address: str) -> float:
        """Get total available balance (perps equity + spot USDC)."""
        if not self.info:
            return 0.0
        try:
            state = self.info.user_state(address)
            perps_equity = float(
                state.get("marginSummary", {}).get("accountValue", 0)
            )
            spot_state = self.info.spot_user_state(address)
            spot_usdc = 0.0
            for bal in spot_state.get("balances", []):
                if bal.get("coin") == "USDC":
                    spot_usdc = float(bal.get("total", 0))
            return perps_equity + spot_usdc
        except Exception as e:
            logger.error(f"HL balance query failed: {e}")
            return 0.0


class BybitOrderAPI:
    """Bybit linear perpetual order placement via REST."""

    def __init__(self, fill_poll_attempts: int = 8, fill_poll_delay_s: float = 0.15):
        self.api_key = os.getenv("BYBIT_API_KEY", "")
        self.api_secret = os.getenv("BYBIT_API_SECRET", "")
        self.base_url = "https://api.bybit.com"
        self._session = requests.Session()
        self.fill_poll_attempts = fill_poll_attempts
        self.fill_poll_delay_s = fill_poll_delay_s

    def _sign(self, params: dict) -> dict:
        """Generate Bybit V5 HMAC signature."""
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"

        param_str = json.dumps(params, separators=(",", ":"))
        sign_str = f"{timestamp}{self.api_key}{recv_window}{param_str}"

        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

    def _sign_get(self, params: dict) -> tuple[dict, list]:
        """Generate Bybit V5 HMAC signature for GET query params.

        Returns (headers, sorted_params) — MUST use sorted_params in
        requests.get(params=sorted_params) so the actual URL query string
        matches the signed string. Using params=dict can reorder keys.
        """
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        sorted_items = sorted(params.items())
        query_str = "&".join(f"{k}={v}" for k, v in sorted_items)
        sign_str = f"{timestamp}{self.api_key}{recv_window}{query_str}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        return headers, sorted_items

    async def place_ioc(self, symbol: str, side: str, qty: str,
                        price: str, reduce_only: bool = False,
                        order_link_id: str = "") -> OrderResult:
        """Place a Bybit linear IOC order.

        Args:
            order_link_id: Client order ID for idempotency/reconciliation.
        """
        t0 = time.time()

        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side,          # "Buy" or "Sell"
            "orderType": "Limit",
            "qty": qty,
            "price": price,
            "timeInForce": "IOC",
            "reduceOnly": reduce_only,
        }
        if order_link_id:
            params["orderLinkId"] = order_link_id

        try:
            headers = self._sign(params)
            # CRITICAL: Use data= with the EXACT same serialization as _sign()
            # to ensure the signed body matches what Bybit receives.
            # Using json= lets requests re-serialize, which can differ.
            body_str = json.dumps(params, separators=(",", ":"))
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._session.post(
                    f"{self.base_url}/v5/order/create",
                    headers=headers,
                    data=body_str,
                    timeout=5,
                ),
            )
            latency = (time.time() - t0) * 1000
            data = resp.json()

            if data.get("retCode") == 0:
                result_data = data.get("result", {})
                order_id = result_data.get("orderId", "")

                fill = await self._wait_for_fill(
                    order_id=order_id,
                    symbol=symbol,
                    requested_qty=float(qty),
                )

                return OrderResult(
                    success=fill.get("qty", 0) > 0,
                    venue="BB",
                    order_id=order_id,
                    filled_qty=fill.get("qty", 0),
                    avg_price=fill.get("price", 0),
                    status=fill.get("status", "NOT_FILLED"),
                    latency_ms=latency,
                    raw=data,
                )
            else:
                return OrderResult(
                    success=False, venue="BB",
                    status="REJECTED",
                    error=f"retCode={data.get('retCode')}: {data.get('retMsg', '')}",
                    latency_ms=latency, raw=data,
                )

        except Exception as e:
            return OrderResult(
                success=False, venue="BB",
                status="ERROR", error=str(e),
                latency_ms=(time.time() - t0) * 1000,
            )

    async def _wait_for_fill(self, order_id: str, symbol: str, requested_qty: float) -> dict:
        """Poll Bybit for IOC fills with short backoff.

        Bybit's order/realtime endpoint can lag order creation by several
        hundred milliseconds. A single 100ms check creates false NOT_FILLED
        results and unnecessary naked-leg unwinds.
        """
        best = {"filled": False, "qty": 0.0, "price": 0.0, "status": "NOT_FILLED"}
        for attempt in range(max(1, self.fill_poll_attempts)):
            await asyncio.sleep(self.fill_poll_delay_s * (1.5 ** min(attempt, 4)))
            fill = await self._check_fill(order_id, symbol, requested_qty)
            if fill.get("qty", 0) > best.get("qty", 0):
                best = fill
            if fill.get("status") == "FILLED":
                return fill
        return best

    async def _check_fill(self, order_id: str, symbol: str, requested_qty: float) -> dict:
        """Check if an IOC order has any executed quantity.

        Checks /v5/order/realtime first, then falls back to /v5/execution/list
        for fills that have already moved out of the active order book.
        """
        result = await self._check_fill_realtime(order_id, symbol, requested_qty)
        if result.get("qty", 0) > 0:
            return result

        # Fallback: check execution history (IOC fills can vanish from realtime quickly)
        return await self._check_fill_execution_history(order_id, symbol, requested_qty)

    async def _check_fill_realtime(self, order_id: str, symbol: str, requested_qty: float) -> dict:
        """Check /v5/order/realtime for fill info."""
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "orderId": order_id,
            }
            headers, sorted_params = self._sign_get(params)

            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._session.get(
                    f"{self.base_url}/v5/order/realtime",
                    headers=headers,
                    params=sorted_params,
                    timeout=5,
                ),
            )
            data = resp.json()

            orders = data.get("result", {}).get("list", [])
            if orders:
                order = orders[0]
                cum_qty = float(order.get("cumExecQty", 0) or 0)
                avg_px = float(order.get("avgPrice", 0) or 0)
                status = order.get("orderStatus", "")
                filled = cum_qty > 0
                full = requested_qty > 0 and cum_qty >= requested_qty * 0.999
                return {
                    "filled": filled,
                    "qty": cum_qty,
                    "price": avg_px,
                    "status": "FILLED" if full else ("PARTIAL" if filled else status or "NOT_FILLED"),
                }
        except Exception as e:
            logger.error(f"Bybit realtime fill check failed: {e}")

        return {"filled": False, "qty": 0, "price": 0}

    async def _check_fill_execution_history(self, order_id: str, symbol: str, requested_qty: float) -> dict:
        """Fallback: check /v5/execution/list for IOC fills that left realtime."""
        try:
            params = {
                "category": "linear",
                "symbol": symbol,
                "orderId": order_id,
            }
            headers, sorted_params = self._sign_get(params)

            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: self._session.get(
                    f"{self.base_url}/v5/execution/list",
                    headers=headers,
                    params=sorted_params,
                    timeout=5,
                ),
            )
            data = resp.json()

            executions = data.get("result", {}).get("list", [])
            if executions:
                total_qty = 0.0
                total_notional = 0.0
                for ex in executions:
                    qty = float(ex.get("execQty", 0))
                    px = float(ex.get("execPrice", 0))
                    total_qty += qty
                    total_notional += qty * px

                avg_px = total_notional / total_qty if total_qty > 0 else 0
                full = requested_qty > 0 and total_qty >= requested_qty * 0.999
                return {
                    "filled": total_qty > 0,
                    "qty": total_qty,
                    "price": avg_px,
                    "status": "FILLED" if full else ("PARTIAL" if total_qty > 0 else "NOT_FILLED"),
                }
        except Exception as e:
            logger.error(f"Bybit execution history check failed: {e}")

        return {"filled": False, "qty": 0, "price": 0}

    def get_positions(self) -> list[dict]:
        """Query all open Bybit linear positions."""
        try:
            params = {"category": "linear", "settleCoin": "USDT"}
            headers, sorted_params = self._sign_get(params)
            resp = self._session.get(
                f"{self.base_url}/v5/position/list",
                headers=headers,
                params=sorted_params,
                timeout=5,
            )
            data = resp.json()

            positions = []
            for pos in data.get("result", {}).get("list", []):
                sz = float(pos.get("size", 0))
                if sz != 0:
                    positions.append({
                        "symbol": pos.get("symbol", ""),
                        "side": pos.get("side", ""),
                        "size": sz,
                        "entry_price": float(pos.get("avgPrice", 0)),
                        "unrealized_pnl": float(pos.get("unrealisedPnl", 0)),
                        "leverage": pos.get("leverage", "1"),
                    })
            return positions
        except Exception as e:
            logger.error(f"Bybit position query failed: {e}")
            return []

    def preflight_check(self, pairs: list[str], leverage: int = 3) -> tuple[bool, list[str]]:
        """Validate Bybit account mode and set leverage for all pairs.

        Returns (ok, errors). Checks:
        1. Account is in one-way position mode (not hedge mode)
        2. Leverage is set correctly for each pair

        Call this once during initialization, before any live trading.
        """
        errors = []

        # Check position mode for first pair to detect hedge mode
        if pairs:
            test_sym = pairs[0].replace("-", "")
            try:
                params = {"category": "linear", "symbol": test_sym}
                headers, sorted_params = self._sign_get(params)
                resp = self._session.get(
                    f"{self.base_url}/v5/position/list",
                    headers=headers,
                    params=sorted_params,
                    timeout=5,
                ).json()
                pos_list = resp.get("result", {}).get("list", [])
                if pos_list:
                    mode = pos_list[0].get("positionIdx", 0)
                    if mode != 0:
                        errors.append(
                            f"Bybit account is in HEDGE mode (positionIdx={mode}). "
                            f"HLBB requires one-way mode. Set via Bybit app/API."
                        )
            except Exception as e:
                errors.append(f"Position mode check failed: {e}")

        # Set leverage for all pairs
        for pair in pairs:
            symbol = pair.replace("-", "")
            try:
                params = {
                    "category": "linear",
                    "symbol": symbol,
                    "buyLeverage": str(leverage),
                    "sellLeverage": str(leverage),
                }
                headers = self._sign(params)
                body_str = json.dumps(params, separators=(",", ":"))
                resp = self._session.post(
                    f"{self.base_url}/v5/position/set-leverage",
                    headers=headers,
                    data=body_str,
                    timeout=5,
                ).json()
                ret_code = resp.get("retCode", -1)
                # retCode 110043 = "Set leverage not modified" — this is OK
                if ret_code not in (0, 110043):
                    errors.append(
                        f"Leverage set failed for {symbol}: "
                        f"retCode={ret_code} msg={resp.get('retMsg', '')}"
                    )
                else:
                    logger.info(f"  BB leverage {symbol}: {leverage}x OK")
            except Exception as e:
                errors.append(f"Leverage set failed for {symbol}: {e}")

        return len(errors) == 0, errors

    def get_balance(self) -> float:
        """Get Bybit USDT wallet balance."""
        try:
            params = {"accountType": "UNIFIED", "coin": "USDT"}
            headers, sorted_params = self._sign_get(params)
            resp = self._session.get(
                f"{self.base_url}/v5/account/wallet-balance",
                headers=headers,
                params=sorted_params,
                timeout=5,
            )
            data = resp.json()

            coins = data.get("result", {}).get("list", [{}])[0].get("coin", [])
            for c in coins:
                if c.get("coin") == "USDT":
                    return float(c.get("walletBalance", 0))
        except Exception as e:
            logger.error(f"Bybit balance query failed: {e}")
        return 0.0
