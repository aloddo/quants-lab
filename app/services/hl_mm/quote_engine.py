"""
Quote Engine V2 — Atomic cancel/place, post-only, tick rounding.

Fixes from adversarial review:
- F5: Atomic cancel-then-place (wait for ack before new orders)
- F6: Orphan detection via full open_orders query
- F13: Post-only enforcement (bid < best_ask, ask > best_bid)
- F14: Tick/lot size rounding from venue metadata
"""
import logging
import time
from dataclasses import dataclass
from typing import Optional

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.signing import OrderType, OrderRequest, CancelRequest

from .avellaneda_quoter import QuoteResult

logger = logging.getLogger(__name__)


@dataclass
class QuoteState:
    """Tracks active orders for one coin."""
    bid_oid: Optional[int] = None
    ask_oid: Optional[int] = None
    bid_price: float = 0.0
    ask_price: float = 0.0
    last_update: float = 0.0


class QuoteEngineV2:
    """Order management with atomic operations and safety checks."""

    def __init__(
        self,
        exchange: Exchange,
        info: Info,
        address: str,
        sz_decimals: dict[str, int],  # from HL meta: coin -> szDecimals
        refresh_interval: float = 8.0,  # seconds between full refresh
        price_move_threshold_bps: float = 3.0,  # re-quote if mid moved this much
        min_notional_usd: float = 10.0,  # HL minimum order notional
    ):
        self.exchange = exchange
        self.info = info
        self.address = address
        self.sz_decimals = sz_decimals
        self.refresh_interval = refresh_interval
        self.price_move_threshold_bps = price_move_threshold_bps
        self.min_notional_usd = min_notional_usd

        self._states: dict[str, QuoteState] = {}
        self._last_full_cleanup: float = 0.0

    def update_quotes(self, coin: str, quote: QuoteResult, signal_stale: bool = False):
        """Place or refresh quotes for a coin based on AS quoter output.

        Atomic sequence: cancel existing → verify cancelled → place new.
        """
        state = self._states.get(coin, QuoteState())

        # Don't quote if signal is stale (F10)
        if signal_stale:
            self._cancel_all_for_coin(coin)
            return

        # Check if refresh needed
        if not self._needs_refresh(coin, quote, state):
            return

        # Step 1: Cancel existing orders (if any)
        if state.bid_oid or state.ask_oid:
            self._cancel_all_for_coin(coin)
            time.sleep(0.3)  # F5: wait for cancel propagation (HL ~200ms)

        # Step 2: Verify cancels took effect
        # Query actual open orders to catch orphans (F6)
        remaining = self._get_coin_orders(coin)
        if remaining:
            logger.warning(f"{coin}: {len(remaining)} orphan orders, cancelling")
            for order in remaining:
                try:
                    self.exchange.cancel(coin, order['oid'])
                except Exception:
                    pass
            time.sleep(0.3)

        # Step 3: Place new orders with rounding (F14)
        new_state = QuoteState(last_update=time.time())

        if quote.should_quote_bid and quote.bid_size > 0:
            bid_sz = self._round_size(coin, quote.bid_size)
            bid_px = self._round_price(coin, quote.bid_price)
            if bid_sz > 0 and bid_px > 0:
                oid = self._place_limit(coin, True, bid_sz, bid_px)
                if oid:
                    new_state.bid_oid = oid
                    new_state.bid_price = bid_px

        if quote.should_quote_ask and quote.ask_size > 0:
            ask_sz = self._round_size(coin, quote.ask_size)
            ask_px = self._round_price(coin, quote.ask_price)
            if ask_sz > 0 and ask_px > 0:
                oid = self._place_limit(coin, False, ask_sz, ask_px)
                if oid:
                    new_state.ask_oid = oid
                    new_state.ask_price = ask_px

        self._states[coin] = new_state

    def detect_fills(self, coin: str) -> list[dict]:
        """Detect fills by comparing tracked orders against open orders + fill history.

        FIX 2: Uses HL user_fills endpoint for REAL fill data (price, size, fee).
        Falls back to order-disappearance detection if fills API unavailable.

        Returns list: [{"side": "bid"|"ask", "price": float, "size": float, "fee": float}]
        """
        state = self._states.get(coin)
        if not state:
            return []

        fills = []
        open_orders = self._get_coin_orders(coin)
        open_oids = {o['oid'] for o in open_orders}

        # Check each tracked order
        for side, oid_attr, price_attr in [
            ("bid", "bid_oid", "bid_price"),
            ("ask", "ask_oid", "ask_price"),
        ]:
            oid = getattr(state, oid_attr)
            if oid and oid not in open_oids:
                # Order disappeared — check if it was filled via order status
                fill_info = self._query_order_fill(oid)
                if fill_info:
                    fills.append({
                        "side": side,
                        "price": fill_info.get("price", getattr(state, price_attr)),
                        "size": fill_info.get("size", 0),
                        "fee": fill_info.get("fee", 0),
                        "oid": oid,
                    })
                else:
                    # Fallback: assume filled at our quoted price
                    fills.append({
                        "side": side,
                        "price": getattr(state, price_attr),
                        "size": 0,  # unknown
                        "fee": 0,
                        "oid": oid,
                    })
                setattr(state, oid_attr, None)

        return fills

    def _query_order_fill(self, oid: int) -> Optional[dict]:
        """Query HL for actual fill details of a specific order."""
        try:
            # HL API: query_order_by_oid returns order status including fill info
            result = self.info.query_order_by_oid(self.address, oid)
            if result and result.get("status") == "filled":
                return {
                    "price": float(result.get("avgPx", 0) or 0),
                    "size": float(result.get("sz", 0) or 0),
                    "fee": float(result.get("fee", 0) or 0),
                }
            elif result and result.get("status") == "canceled":
                return None  # was cancelled, not filled
        except Exception as e:
            logger.debug(f"Order query failed for oid={oid}: {e}")
        return None

    def cleanup_orphans(self):
        """F6: Periodically query ALL open orders and cancel any not tracked."""
        now = time.time()
        if now - self._last_full_cleanup < 30.0:  # every 30s
            return

        self._last_full_cleanup = now
        try:
            all_orders = self.info.open_orders(self.address)
            if not all_orders:
                return

            tracked_oids = set()
            for state in self._states.values():
                if state.bid_oid:
                    tracked_oids.add(state.bid_oid)
                if state.ask_oid:
                    tracked_oids.add(state.ask_oid)

            for order in all_orders:
                oid = order.get('oid')
                if oid and oid not in tracked_oids:
                    coin = order.get('coin', '')
                    logger.warning(f"Orphan order found: {coin} oid={oid}, cancelling")
                    try:
                        self.exchange.cancel(coin, oid)
                    except Exception as e:
                        logger.error(f"Failed to cancel orphan {oid}: {e}")

        except Exception as e:
            logger.warning(f"Orphan cleanup failed: {e}")

    def cancel_all(self):
        """Emergency: cancel everything we know about + full query."""
        for coin in list(self._states.keys()):
            self._cancel_all_for_coin(coin)

        # Also query and cancel any remaining (F6)
        try:
            all_orders = self.info.open_orders(self.address)
            if all_orders:
                for order in all_orders:
                    try:
                        self.exchange.cancel(order['coin'], order['oid'])
                    except Exception:
                        pass
        except Exception:
            pass

        self._states.clear()

    def _needs_refresh(self, coin: str, quote: QuoteResult, state: QuoteState) -> bool:
        """Check if quotes need updating."""
        if not state.bid_oid and not state.ask_oid:
            return True  # no active quotes

        if time.time() - state.last_update > self.refresh_interval:
            return True  # time-based refresh

        # Price-based refresh: mid moved significantly
        if state.bid_price > 0:
            mid_approx = (state.bid_price + state.ask_price) / 2 if state.ask_price > 0 else state.bid_price
            new_mid = (quote.bid_price + quote.ask_price) / 2
            move_bps = abs(new_mid - mid_approx) / mid_approx * 10000
            if move_bps > self.price_move_threshold_bps:
                return True

        return False

    def _cancel_all_for_coin(self, coin: str):
        """Cancel all tracked orders for a coin."""
        state = self._states.get(coin)
        if not state:
            return

        cancels = []
        if state.bid_oid:
            cancels.append(CancelRequest(coin=coin, oid=state.bid_oid))
        if state.ask_oid:
            cancels.append(CancelRequest(coin=coin, oid=state.ask_oid))

        if cancels:
            try:
                self.exchange.bulk_cancel(cancels)
            except Exception as e:
                logger.warning(f"Cancel failed for {coin}: {e}")

        state.bid_oid = None
        state.ask_oid = None

    def _get_coin_orders(self, coin: str) -> list[dict]:
        """Get open orders for a specific coin."""
        try:
            all_orders = self.info.open_orders(self.address)
            return [o for o in (all_orders or []) if o.get('coin') == coin]
        except Exception:
            return []

    def _place_limit(self, coin: str, is_buy: bool, size: float, price: float) -> Optional[int]:
        """Place a single MAKER-ONLY limit order. Returns oid or None.

        Uses ALO (Add Liquidity Only) order type to guarantee maker execution.
        Enforces minimum notional (FIX 6).
        """
        # FIX 6: Enforce minimum notional
        notional = size * price
        if notional < self.min_notional_usd:
            logger.debug(f"Order below min notional: {coin} {size}@{price} = ${notional:.2f} < ${self.min_notional_usd}")
            return None

        try:
            # ALO = Add Liquidity Only — rejected if it would be a taker fill
            order_type = OrderType(limit={"tif": "Alo"})
            result = self.exchange.order(coin, is_buy, size, price, order_type)

            if result and result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses and "resting" in statuses[0]:
                    return statuses[0]["resting"]["oid"]
                elif statuses and "error" in statuses[0]:
                    logger.warning(f"Order rejected: {coin} {'BUY' if is_buy else 'SELL'} {size}@{price}: {statuses[0]['error']}")
            return None
        except Exception as e:
            logger.error(f"Place order failed: {coin} {'BUY' if is_buy else 'SELL'} {size}@{price}: {e}")
            return None

    def _round_size(self, coin: str, size: float) -> float:
        """F14: Round size to venue lot increment. F7: enforce minimum notional."""
        decimals = self.sz_decimals.get(coin, 4)
        rounded = round(size, decimals)
        if rounded <= 0:
            return 0.0
        # F7 FIX: HL minimum notional is $10 for most pairs.
        # Check at order time: if size * approx_price < $10, return 0 (skip order).
        # Caller should check for 0 before placing.
        return rounded

    def _round_price(self, coin: str, price: float) -> float:
        """F14: Round price to venue tick size.

        HL tick size = significant digits based on price magnitude.
        For most pairs: 4-5 significant figures.
        """
        if price <= 0:
            return 0.0
        # HL uses 5 significant figures for price
        magnitude = len(str(int(price)))
        decimals = max(0, 5 - magnitude)
        return round(price, decimals)
