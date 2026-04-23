"""
V2 OrderGateway — Unified order submission, cancellation, and status queries.

Single point for all exchange interactions during trading. Handles:
- Symbol mapping (internal NOMUSDT -> Binance NOMUSDC)
- Shadow mode (simulated fills, no real orders)
- Client order ID generation and persistence
- Tri-state get_order() with retry logic

Designed for reuse across any cross-venue strategy.
"""
import asyncio
import logging
import time
import uuid
from typing import Optional

import aiohttp

from app.services.arb.order_api import BybitOrderAPI, BinanceOrderAPI
from app.services.arb.order_feed import FillEvent, FillSource

logger = logging.getLogger(__name__)


class OrderGateway:
    """
    Unified order gateway for both exchanges.

    All order operations go through here. No direct API access elsewhere during trading.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        bybit_key: str,
        bybit_secret: str,
        binance_key: str,
        binance_secret: str,
        bn_symbol_map: dict[str, str] | None = None,
        shadow: bool = False,
        on_shadow_fill=None,
    ):
        self._session = session
        self._shadow = shadow
        self._on_shadow_fill = on_shadow_fill
        self._order_counter = 0

        # Symbol mapping: internal -> Binance actual (e.g., NOMUSDT -> NOMUSDC)
        self._bn_symbol_map = bn_symbol_map or {}

        # Real API clients
        self.bybit_api = BybitOrderAPI(bybit_key, bybit_secret, session)
        self.binance_api = BinanceOrderAPI(binance_key, binance_secret, session)

        # Shadow fill tracking
        self._shadow_fills: dict[str, dict] = {}

    def generate_client_id(self, prefix: str = "h2v2") -> str:
        """Generate a unique client order ID."""
        return f"{prefix}_{uuid.uuid4().hex[:12]}"

    def map_bn_symbol(self, internal_symbol: str) -> str:
        """Map internal symbol to actual Binance symbol."""
        return self._bn_symbol_map.get(internal_symbol, internal_symbol)

    async def submit(
        self,
        venue: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        order_type: str,
        client_order_id: str = "",
        order_mode: str = "aggressive",
    ) -> str:
        """
        Submit an order. Returns exchange order_id.

        Symbol is the internal key (e.g., NOMUSDT). For Binance, it is
        automatically mapped to the actual symbol (e.g., NOMUSDC).

        order_mode: "aggressive" (IOC/taker) or "passive" (PostOnly/maker).
        Only affects Bybit. Binance maker=taker so mode is ignored.
        """
        self._order_counter += 1

        if self._shadow:
            return await self._shadow_submit(venue, symbol, side, qty, price, order_type, client_order_id)

        if venue == "bybit":
            return await self.bybit_api.submit_order(symbol, side, qty, price, order_type, client_order_id, order_mode=order_mode)
        elif venue == "binance":
            bn_symbol = self.map_bn_symbol(symbol)
            return await self.binance_api.submit_order(bn_symbol, side, qty, price, order_type, client_order_id)
        else:
            raise ValueError(f"Unknown venue: {venue}")

    async def cancel(self, venue: str, symbol: str, order_id: str) -> bool:
        """Cancel an order. Returns True if successful."""
        if self._shadow:
            logger.info(f"[SHADOW] Would cancel: {venue} {symbol} {order_id}")
            return True

        if venue == "bybit":
            return await self.bybit_api.cancel_order(symbol, order_id)
        elif venue == "binance":
            bn_symbol = self.map_bn_symbol(symbol)
            return await self.binance_api.cancel_order(bn_symbol, order_id)
        return False

    async def get_order(
        self,
        venue: str,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
    ) -> dict:
        """
        Query single order status. Returns tri-state fill_result.

        CRITICAL: This is the fix for the 91% false-unfilled bug.
        Uses GET /api/v3/order (Binance) or GET /v5/order/history (Bybit)
        instead of scanning myTrades.

        Returns: dict with fill_result ("FILLED" | "NOT_FILLED" | "UNKNOWN")
        """
        if self._shadow:
            oid = order_id or client_order_id
            shadow_fill = self._shadow_fills.get(oid)
            if shadow_fill:
                return {
                    "fill_result": "FILLED",
                    "filled_qty": shadow_fill["filled_qty"],
                    "avg_price": shadow_fill["avg_price"],
                    "status": "Filled",
                }
            return {"fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0, "status": "shadow_unknown"}

        if venue == "bybit":
            return await self.bybit_api.get_order(symbol, order_id=order_id, client_order_id=client_order_id)
        elif venue == "binance":
            bn_symbol = self.map_bn_symbol(symbol)
            return await self.binance_api.get_order(bn_symbol, order_id=order_id, client_order_id=client_order_id)

        return {"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0, "status": "unknown_venue"}

    async def get_order_with_retry(
        self,
        venue: str,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
        max_retries: int = 3,
        backoff_base: float = 0.5,
    ) -> dict:
        """
        Query order status with exponential backoff retry on UNKNOWN results.

        This is the hardened version that handles transient REST failures
        without conflating "check failed" with "not filled."
        """
        for attempt in range(max_retries):
            result = await self.get_order(venue, symbol, order_id=order_id, client_order_id=client_order_id)
            if result.get("fill_result") != "UNKNOWN":
                return result
            delay = backoff_base * (2 ** attempt)
            logger.warning(
                f"get_order UNKNOWN (attempt {attempt + 1}/{max_retries}): "
                f"{venue} {symbol} oid={order_id or client_order_id}. Retry in {delay:.1f}s"
            )
            await asyncio.sleep(delay)

        logger.error(
            f"get_order UNKNOWN after {max_retries} retries: "
            f"{venue} {symbol} oid={order_id or client_order_id}"
        )
        return result  # Return last UNKNOWN result

    async def get_trades_for_order(self, venue: str, symbol: str, order_id: str = "") -> list[dict]:
        """
        Query trades for a specific order — returns commission/fee data.
        Only implemented for Binance (Bybit WS already provides fees).
        """
        if self._shadow:
            return []
        if venue == "binance":
            bn_symbol = self.map_bn_symbol(symbol)
            return await self.binance_api.get_trades_for_order(bn_symbol, order_id)
        return []

    async def check_position(self, venue: str, symbol: str) -> float | None:
        """
        Query actual exchange position size.
        Bybit: directional perp position. Binance: returns None (spot has no position concept).

        CRITICAL FIX (Codex review): Returns None on error instead of 0.0.
        Callers MUST distinguish "confirmed flat" (0.0) from "check failed" (None).
        Returning 0.0 on error caused recovery to close live positions.
        """
        if self._shadow:
            return 0.0
        try:
            if venue == "bybit":
                positions = await self.bybit_api.get_positions(symbol)
                if positions is None:
                    return None  # API failure — cannot confirm position state
                for p in positions:
                    if float(p.get("size", 0)) != 0:
                        return float(p["size"])
                return 0.0  # Confirmed: no position on exchange
            # Binance spot: no position concept
            return None
        except Exception as e:
            logger.warning(f"check_position failed {venue} {symbol}: {e}")
            return None  # UNKNOWN — caller must NOT assume flat

    # ── Shadow mode ────────────────────────────────────────────

    async def _shadow_submit(
        self, venue, symbol, side, qty, price, order_type, client_order_id,
    ) -> str:
        """Simulated order submission for shadow/test mode."""
        import random
        oid = f"shadow_{venue}_{self._order_counter}"
        bn_symbol = self.map_bn_symbol(symbol) if venue == "binance" else symbol
        logger.info(f"[SHADOW] {venue} {bn_symbol} {side} {qty:.6f} @ {price:.6f} ({order_type}) -> {oid}")

        # Simulate fill with small random slippage (1-5bp)
        slippage_bps = random.uniform(1, 5)
        slippage_mult = 1 + (slippage_bps / 10000) if side in ("Buy", "BUY") else 1 - (slippage_bps / 10000)
        fill_price = price * slippage_mult if price > 0 else price

        # Simulate execution latency (50-200ms)
        await asyncio.sleep(random.uniform(0.05, 0.2))

        self._shadow_fills[oid] = {"filled_qty": qty, "avg_price": fill_price}
        if client_order_id:
            self._shadow_fills[client_order_id] = {"filled_qty": qty, "avg_price": fill_price}

        # Emit simulated fill via callback
        if self._on_shadow_fill:
            fill = FillEvent(
                venue=venue,
                symbol=symbol,
                order_id=oid,
                exec_id=f"sim_{oid}",
                side=side,
                price=fill_price,
                qty=qty,
                fee=qty * fill_price * 0.00055 if venue == "bybit" else qty * fill_price * 0.001,
                fee_asset="USDT" if venue == "bybit" else "USDC",
                timestamp_ms=int(time.time() * 1000),
                local_ts=time.time(),
                source=FillSource.WS,
                is_maker=False,
                order_status="Filled",
                leaves_qty=0,
                client_order_id=client_order_id,
            )
            self._on_shadow_fill(fill)

        return oid
