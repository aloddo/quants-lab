"""
H2 LiquidityVeto — Coarse pair-level book check.

NOT a precise pre-trade depth guarantee (REST L2 is stale by order time).
This is a VETO: catches structurally empty books, ghost liquidity, and
market maker withdrawal. Actual fill protection comes from IOC orders.

Refreshes every 5 minutes per pair, not per trade.
"""
import logging
import time

import aiohttp

logger = logging.getLogger(__name__)


class LiquidityVeto:
    """Coarse liquidity filter — veto structurally thin books."""

    # Veto thresholds
    MIN_DEPTH_USD = 5.0         # must have $5+ at best bid/ask
    MAX_VENUE_SPREAD_BPS = 50   # venue's own bid-ask spread can't be > 50bp
    REFRESH_INTERVAL = 300      # check every 5 minutes

    def __init__(self):
        self._cache: dict[str, dict] = {}  # "venue:symbol" -> {liquid, checked_at, reason}

    async def check_pair(
        self, session: aiohttp.ClientSession, venue: str, symbol: str
    ) -> tuple[bool, str]:
        """
        Returns (is_liquid, reason).
        Caches result for REFRESH_INTERVAL seconds.
        """
        key = f"{venue}:{symbol}"
        cached = self._cache.get(key)
        if cached and time.time() - cached["checked_at"] < self.REFRESH_INTERVAL:
            return cached["liquid"], cached["reason"]

        try:
            if venue == "bybit":
                return await self._check_bybit(session, symbol, key)
            elif venue == "binance":
                return await self._check_binance(session, symbol, key)
            else:
                return True, "unknown_venue"
        except Exception as e:
            logger.warning(f"Liquidity check failed {key}: {e}")
            # On error: if we have a cached result, use it; otherwise BLOCK (fail-closed)
            cached = self._cache.get(key)
            if cached:
                logger.info(f"Using cached liquidity result for {key}: liquid={cached['liquid']}")
                return cached["liquid"], f"cached_after_error:{e}"
            return False, f"check_failed_no_cache:{e}"

    async def _check_bybit(self, session, symbol, key) -> tuple[bool, str]:
        async with session.get(
            "https://api.bybit.com/v5/market/orderbook",
            params={"category": "linear", "symbol": symbol, "limit": "5"},
        ) as resp:
            data = await resp.json()

        book = data.get("result", {})
        bids = book.get("b", [])
        asks = book.get("a", [])

        if not bids or not asks:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": "empty_book"}
            return False, "empty_book"

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        bid_qty_usd = float(bids[0][1]) * best_bid
        ask_qty_usd = float(asks[0][1]) * best_ask

        if best_bid <= 0 or best_ask <= 0:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": "zero_price"}
            return False, "zero_price"

        venue_spread = (best_ask - best_bid) / best_bid * 10000
        if venue_spread > self.MAX_VENUE_SPREAD_BPS:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": f"wide_spread_{venue_spread:.0f}bp"}
            return False, f"wide_spread_{venue_spread:.0f}bp"

        if bid_qty_usd < self.MIN_DEPTH_USD or ask_qty_usd < self.MIN_DEPTH_USD:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": f"thin_book_bid${bid_qty_usd:.0f}_ask${ask_qty_usd:.0f}"}
            return False, f"thin_book"

        self._cache[key] = {"liquid": True, "checked_at": time.time(), "reason": "ok"}
        return True, "ok"

    async def _check_binance(self, session, symbol, key) -> tuple[bool, str]:
        async with session.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": symbol, "limit": "5"},
        ) as resp:
            data = await resp.json()

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        if not bids or not asks:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": "empty_book"}
            return False, "empty_book"

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        bid_qty_usd = float(bids[0][1]) * best_bid
        ask_qty_usd = float(asks[0][1]) * best_ask

        if best_bid <= 0 or best_ask <= 0:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": "zero_price"}
            return False, "zero_price"

        venue_spread = (best_ask - best_bid) / best_bid * 10000
        if venue_spread > self.MAX_VENUE_SPREAD_BPS:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": f"wide_spread_{venue_spread:.0f}bp"}
            return False, f"wide_spread_{venue_spread:.0f}bp"

        if bid_qty_usd < self.MIN_DEPTH_USD or ask_qty_usd < self.MIN_DEPTH_USD:
            self._cache[key] = {"liquid": False, "checked_at": time.time(), "reason": "thin_book"}
            return False, "thin_book"

        self._cache[key] = {"liquid": True, "checked_at": time.time(), "reason": "ok"}
        return True, "ok"

    def is_cached_liquid(self, venue: str, symbol: str) -> bool:
        """Quick check from cache (no API call). Returns True if no data (conservative)."""
        cached = self._cache.get(f"{venue}:{symbol}")
        if not cached:
            return True  # no data = don't block
        return cached["liquid"]
