"""
Quote Engine — Inside-improvement + contrarian quoting (Spec Section 3).

Responsibilities:
  - Compute bid/ask prices from fair value + AS reservation
  - Inside-spread improvement with contrarian logic
  - Child order sizing with depth/equity/inventory constraints
  - Requote gating (>1.2s since last action + price/spread/age triggers)
  - Hard refresh every 5s
  - ALO order submission and tracking
  - Cancel/replace with single batch in flight per pair

This module does NOT own the WS connection or the event loop. It exposes
synchronous methods called by the orchestrator.
"""
import logging
import time
from dataclasses import dataclass
from typing import Optional

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.signing import OrderType, CancelRequest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class QuoteDecision:
    """A single quote decision for one side."""
    price: float
    size: float           # base asset units
    notional_usd: float
    is_improvement: bool  # True if inside the current best


@dataclass
class QuotePair:
    """Bid + ask quote pair for one coin."""
    coin: str
    bid: Optional[QuoteDecision]
    ask: Optional[QuoteDecision]
    fair_value: float
    reservation_price: float
    spread_bps: float
    timestamp: float


@dataclass
class ActiveOrder:
    """Tracks a resting order on HL."""
    oid: int
    coin: str
    is_buy: bool
    price: float
    size: float
    placed_at: float


@dataclass
class QuoteState:
    """Per-pair order tracking."""
    bid_order: Optional[ActiveOrder] = None
    ask_order: Optional[ActiveOrder] = None
    last_action_time: float = 0.0
    last_fv: float = 0.0
    last_spread_bucket: int = 0       # quantized spread for change detection
    batch_in_flight: bool = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class QuoteConfig:
    """Quoting parameters from spec."""
    maker_fee_bps: float = 1.44
    min_edge_target_bps: float = 1.0    # minimum realized edge per side after fees
    requote_min_interval_s: float = 1.2  # minimum time between requotes
    hard_refresh_s: float = 5.0          # unconditional refresh interval
    fv_move_threshold_bps: float = 0.8   # requote if FV moved this much
    max_quote_age_s: float = 4.0         # requote if quote older than this
    min_notional_usd: float = 10.0       # HL minimum order notional
    default_leverage: int = 5


# ---------------------------------------------------------------------------
# Toxicity buffer defaults (from spec)
# ---------------------------------------------------------------------------

DEFAULT_TOX_BUFFERS: dict[str, float] = {
    "ORDI": 0.8,
    "BIO": 0.9,
    "DASH": 1.2,
    "AXS": 1.1,
    "PNUT": 1.4,
    "APE": 1.0,
    "PENDLE": 1.0,
    "MEGA": 1.2,
}

# Default starting notional per pair (spec Section 3)
DEFAULT_NOTIONAL: dict[str, float] = {
    "ORDI": 50.0,
    "BIO": 50.0,
    "DASH": 40.0,
    "AXS": 40.0,
    "PNUT": 40.0,
    "APE": 50.0,
    "PENDLE": 40.0,
}


class QuoteEngine:
    """Compute and manage quotes for HL market making."""

    def __init__(
        self,
        exchange: Exchange,
        info: Info,
        address: str,
        sz_decimals: dict[str, int],
        config: Optional[QuoteConfig] = None,
        tox_buffers: Optional[dict[str, float]] = None,
        dry_run: bool = False,
    ):
        self.exchange = exchange
        self.info = info
        self.address = address
        self.sz_decimals = sz_decimals
        self.config = config or QuoteConfig()
        self.tox_buffers = tox_buffers or DEFAULT_TOX_BUFFERS
        self.dry_run = dry_run

        self._states: dict[str, QuoteState] = {}
        self._last_orphan_check: float = 0.0

        # Bug #1: Track orders that failed to cancel so reconciliation can clean them up
        self._stale_orders: set[int] = set()

        # Bug #6/#7: Pending fill checks — OID -> (timestamp, original_size)
        self._pending_fill_check: dict[int, tuple[float, str, float]] = {}
        # {oid: (disappeared_at, coin, original_size)}

    # ------------------------------------------------------------------
    # Quote computation (pure, no side effects)
    # ------------------------------------------------------------------

    def compute_quotes(
        self,
        coin: str,
        fair_value: float,
        reservation_price: float,
        hl_bid: float,
        hl_ask: float,
        depth20_bid_usd: float,
        depth20_ask_usd: float,
        free_equity_usd: float,
        q_soft: float,
        inventory_usd: float,
        sigma_1s: float,
        vol_scale: float = 1.0,
        anchor_scale: float = 1.0,
        quote_bid: bool = True,
        quote_ask: bool = True,
        exit_mode: bool = False,
    ) -> Optional[QuotePair]:
        """Compute bid/ask quotes using inside-improvement + contrarian logic.

        Args:
            coin: HL coin name
            fair_value: blended FV from FairValueEngine
            reservation_price: AS reservation price (inventory-skewed FV)
            hl_bid/hl_ask: current HL best bid/ask
            depth20_bid/ask_usd: top-20 depth on each side (USD)
            free_equity_usd: available margin
            q_soft: soft inventory limit (notional USD)
            inventory_usd: current signed inventory
            sigma_1s: per-second volatility
            vol_scale: volatility-based size multiplier (default 1.0)
            anchor_scale: anchor-quality-based size multiplier
            quote_bid/quote_ask: from state machine
            exit_mode: if True, improve exit side more aggressively
        """
        cfg = self.config
        if fair_value <= 0 or hl_bid <= 0 or hl_ask <= 0:
            return None

        native_spread_bps = (hl_ask - hl_bid) / hl_bid * 10000
        native_half_spread = native_spread_bps / 2.0
        tox_buffer = self.tox_buffers.get(coin, 1.0)

        # Edge room per side
        edge_room = native_half_spread - cfg.maker_fee_bps - tox_buffer

        # Inside improvement (Spec Section 3)
        # improve = min(0.35 * edge_room, edge_room - tox_buffer - 1.0bps)
        if edge_room > 0:
            improve = min(0.35 * edge_room, edge_room - tox_buffer - cfg.min_edge_target_bps)
            improve = max(0.0, improve)
        else:
            improve = 0.0

        improve_price = fair_value * (improve / 10000.0)

        # Extra improvement in exit mode
        exit_extra = 0.0
        if exit_mode:
            exit_extra = fair_value * (1.0 / 10000.0)  # 1 extra tick

        # Build bid quote
        bid_decision = None
        if quote_bid:
            # Reservation price is already inventory-skewed (AS)
            bid_px = reservation_price - fair_value * (cfg.maker_fee_bps / 10000.0)

            # Bug #10: Inside improvement should quote INSIDE the spread (better than touch).
            # Removed cap at hl_bid that pinned us at touch. Now cap at hl_ask - 1 tick
            # to prevent crossing.
            if improve > 0 and not exit_mode:
                bid_px = min(bid_px + improve_price, hl_ask - fair_value * 0.0001)
            elif exit_mode and inventory_usd < 0:
                # We are short, buying to exit -> more aggressive bid
                bid_px = min(bid_px + improve_price + exit_extra, hl_ask - fair_value * 0.0001)

            # Never cross the ask
            bid_px = min(bid_px, hl_ask * 0.9999)

            bid_sz_usd = self._compute_child_size(
                coin, depth20_bid_usd, free_equity_usd, q_soft, vol_scale, anchor_scale
            )
            bid_sz = bid_sz_usd / fair_value if fair_value > 0 else 0
            bid_sz = self._round_size(coin, bid_sz)
            bid_px = self._round_price(coin, bid_px)

            if bid_sz > 0 and bid_px > 0 and bid_sz * bid_px >= cfg.min_notional_usd:
                bid_decision = QuoteDecision(
                    price=bid_px, size=bid_sz,
                    notional_usd=bid_sz * bid_px,
                    is_improvement=bid_px > hl_bid,
                )

        # Build ask quote
        ask_decision = None
        if quote_ask:
            ask_px = reservation_price + fair_value * (cfg.maker_fee_bps / 10000.0)

            # Bug #10: Inside improvement should quote INSIDE the spread (better than touch).
            # Removed cap at hl_ask. Now cap at hl_bid + 1 tick to prevent crossing.
            if improve > 0 and not exit_mode:
                ask_px = max(ask_px - improve_price, hl_bid + fair_value * 0.0001)
            elif exit_mode and inventory_usd > 0:
                # We are long, selling to exit -> more aggressive ask
                ask_px = max(ask_px - improve_price - exit_extra, hl_bid + fair_value * 0.0001)

            # Never cross the bid
            ask_px = max(ask_px, hl_bid * 1.0001)

            ask_sz_usd = self._compute_child_size(
                coin, depth20_ask_usd, free_equity_usd, q_soft, vol_scale, anchor_scale
            )
            ask_sz = ask_sz_usd / fair_value if fair_value > 0 else 0
            ask_sz = self._round_size(coin, ask_sz)
            ask_px = self._round_price(coin, ask_px)

            if ask_sz > 0 and ask_px > 0 and ask_sz * ask_px >= cfg.min_notional_usd:
                ask_decision = QuoteDecision(
                    price=ask_px, size=ask_sz,
                    notional_usd=ask_sz * ask_px,
                    is_improvement=ask_px < hl_ask,
                )

        if not bid_decision and not ask_decision:
            return None

        spread_bps = 0.0
        if bid_decision and ask_decision:
            spread_bps = (ask_decision.price - bid_decision.price) / fair_value * 10000

        return QuotePair(
            coin=coin,
            bid=bid_decision,
            ask=ask_decision,
            fair_value=fair_value,
            reservation_price=reservation_price,
            spread_bps=spread_bps,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Requote gating
    # ------------------------------------------------------------------

    def should_requote(self, coin: str, current_fv: float) -> bool:
        """Check if we should requote based on time/price/spread conditions.

        Requote if >= 1.2s since last action AND one of:
          - FV moved >= 0.8bps
          - Spread bucket changed (not tracked here, caller passes)
          - Inventory changed (tracked externally)
          - Quote age > 4s

        Hard refresh every 5s regardless.
        """
        state = self._states.get(coin)
        if not state:
            return True  # no existing quotes

        now = time.time()
        cfg = self.config

        time_since_last = now - state.last_action_time
        if time_since_last < cfg.requote_min_interval_s:
            return False

        # Hard refresh
        if time_since_last >= cfg.hard_refresh_s:
            return True

        # FV moved
        if state.last_fv > 0 and current_fv > 0:
            fv_move = abs(current_fv - state.last_fv) / state.last_fv * 10000
            if fv_move >= cfg.fv_move_threshold_bps:
                return True

        # Quote age
        oldest_order_age = 0.0
        if state.bid_order:
            oldest_order_age = max(oldest_order_age, now - state.bid_order.placed_at)
        if state.ask_order:
            oldest_order_age = max(oldest_order_age, now - state.ask_order.placed_at)
        if oldest_order_age > cfg.max_quote_age_s:
            return True

        return False

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------

    def execute_quotes(self, quotes: QuotePair) -> None:
        """Place/replace quotes on HL. Cancel existing first, then place new.

        One batch in flight per pair. If dry_run, just log.
        """
        coin = quotes.coin
        state = self._states.get(coin, QuoteState())

        if state.batch_in_flight:
            logger.debug(f"{coin}: batch in flight, skipping")
            return

        # Cancel existing orders — if cancel fails, don't place new orders
        # to avoid ghost order accumulation (Bug #1)
        cancel_ok = self._cancel_coin_orders(coin, state)
        if not cancel_ok:
            logger.warning(f"{coin}: cancel failed, skipping new quote placement — retry next tick")
            return

        # Place new orders
        now = time.time()
        new_state = QuoteState(last_action_time=now, last_fv=quotes.fair_value)

        if quotes.bid and quotes.bid.size > 0:
            if self.dry_run:
                logger.info(
                    f"[DRY] {coin} BID {quotes.bid.size:.6f} @ {quotes.bid.price:.6f} "
                    f"(${quotes.bid.notional_usd:.2f})"
                )
            else:
                oid = self._place_alo(coin, True, quotes.bid.size, quotes.bid.price)
                if oid:
                    new_state.bid_order = ActiveOrder(
                        oid=oid, coin=coin, is_buy=True,
                        price=quotes.bid.price, size=quotes.bid.size,
                        placed_at=now,
                    )

        if quotes.ask and quotes.ask.size > 0:
            if self.dry_run:
                logger.info(
                    f"[DRY] {coin} ASK {quotes.ask.size:.6f} @ {quotes.ask.price:.6f} "
                    f"(${quotes.ask.notional_usd:.2f})"
                )
            else:
                oid = self._place_alo(coin, False, quotes.ask.size, quotes.ask.price)
                if oid:
                    new_state.ask_order = ActiveOrder(
                        oid=oid, coin=coin, is_buy=False,
                        price=quotes.ask.price, size=quotes.ask.size,
                        placed_at=now,
                    )

        self._states[coin] = new_state

    def cancel_all(self) -> None:
        """Emergency: cancel all tracked orders + query for orphans."""
        for coin in list(self._states.keys()):
            state = self._states[coin]
            self._cancel_coin_orders(coin, state)

        if not self.dry_run:
            try:
                all_orders = self.info.open_orders(self.address)
                if all_orders:
                    for order in all_orders:
                        try:
                            self.exchange.cancel(order["coin"], order["oid"])
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"Orphan cleanup failed: {e}")

        self._states.clear()

    def cancel_coin(self, coin: str) -> None:
        """Cancel all orders for a specific coin."""
        state = self._states.get(coin, QuoteState())
        cancel_ok = self._cancel_coin_orders(coin, state)
        if cancel_ok:
            self._states[coin] = QuoteState()
        # If cancel failed, state is preserved with stale orders tracked

    def detect_fills(self, coin: str) -> list[dict]:
        """Detect fills by checking order status changes.

        Bug #6 fix: When an order disappears, query userFills for that OID
        before recording a fill. If no fill found, treat as cancel.

        Bug #7 fix: Check remaining size vs original for partial fills on
        still-resting orders.

        Returns list of fill dicts: {side, price, size, fee, oid}
        """
        state = self._states.get(coin)
        if not state:
            return []

        if self.dry_run:
            return []

        fills = []
        now = time.time()
        try:
            open_orders = self.info.open_orders(self.address)
            open_oids = {}
            for o in (open_orders or []):
                open_oids[o["oid"]] = o
        except Exception:
            return []

        for attr, side in [("bid_order", "bid"), ("ask_order", "ask")]:
            order = getattr(state, attr)
            if not order:
                continue

            if order.oid in open_oids:
                # Bug #7: Order still resting — check for partial fills
                resting_order = open_oids[order.oid]
                remaining_sz = float(resting_order.get("sz", order.size) or order.size)
                if remaining_sz < order.size - 1e-10:
                    # Partial fill detected
                    filled_sz = order.size - remaining_sz
                    fill_info = self._query_fill(order.oid)
                    fills.append({
                        "side": side,
                        "price": fill_info.get("price", order.price) if fill_info else order.price,
                        "size": filled_sz,
                        "fee": fill_info.get("fee", 0) if fill_info else 0,
                        "oid": order.oid,
                        "partial": True,
                    })
                    # Update tracked order with new remaining size
                    order.size = remaining_sz
                    logger.debug(
                        f"{coin} partial fill: {side} {filled_sz:.6f}, "
                        f"remaining={remaining_sz:.6f}"
                    )
            else:
                # Order disappeared — could be fill OR cancel (Bug #6)
                # First check pending fill check buffer
                if order.oid in self._pending_fill_check:
                    disappeared_at, _, _ = self._pending_fill_check[order.oid]
                    if now - disappeared_at < 5.0:
                        # Still within grace period — query for fill confirmation
                        fill_info = self._query_fill(order.oid)
                        if fill_info and fill_info.get("size", 0) > 0:
                            fills.append({
                                "side": side,
                                "price": fill_info.get("price", order.price),
                                "size": fill_info.get("size", order.size),
                                "fee": fill_info.get("fee", 0),
                                "oid": order.oid,
                            })
                            setattr(state, attr, None)
                            del self._pending_fill_check[order.oid]
                        # else: still waiting, don't clear
                    else:
                        # 5s passed with no fill confirmation — assume cancel
                        logger.info(
                            f"{coin} order {order.oid} ({side}) disappeared with no fill "
                            f"after 5s — treating as cancel"
                        )
                        setattr(state, attr, None)
                        del self._pending_fill_check[order.oid]
                else:
                    # First time we notice it disappeared — query immediately
                    fill_info = self._query_fill(order.oid)
                    if fill_info and fill_info.get("size", 0) > 0:
                        # Confirmed fill
                        fills.append({
                            "side": side,
                            "price": fill_info.get("price", order.price),
                            "size": fill_info.get("size", order.size),
                            "fee": fill_info.get("fee", 0),
                            "oid": order.oid,
                        })
                        setattr(state, attr, None)
                    else:
                        # No fill found yet — add to pending check buffer
                        self._pending_fill_check[order.oid] = (now, coin, order.size)
                        logger.debug(
                            f"{coin} order {order.oid} ({side}) disappeared, "
                            f"queued for fill confirmation"
                        )

        # Clean up old pending fill checks (for orders from deactivated coins etc.)
        stale_pending = [
            oid for oid, (ts, _, _) in self._pending_fill_check.items()
            if now - ts > 10.0
        ]
        for oid in stale_pending:
            del self._pending_fill_check[oid]

        return fills

    def cleanup_orphans(self) -> None:
        """Reconcile: cancel any open orders not tracked by us. Every 30s.
        Also retries cancellation of stale orders from failed cancels (Bug #1).
        """
        now = time.time()
        if now - self._last_orphan_check < 30.0:
            return
        self._last_orphan_check = now

        if self.dry_run:
            return

        try:
            all_orders = self.info.open_orders(self.address)
            if not all_orders:
                # No open orders — clear all stale tracking
                self._stale_orders.clear()
                return

            tracked_oids = set()
            for state in self._states.values():
                if state.bid_order:
                    tracked_oids.add(state.bid_order.oid)
                if state.ask_order:
                    tracked_oids.add(state.ask_order.oid)

            open_oids = {o.get("oid") for o in all_orders}
            cancelled_stale = set()

            for order in all_orders:
                oid = order.get("oid")
                if not oid:
                    continue

                # Cancel orphans (not tracked) AND stale orders (failed prior cancel)
                if oid not in tracked_oids or oid in self._stale_orders:
                    label = "stale" if oid in self._stale_orders else "orphan"
                    logger.warning(f"{label.title()}: {order.get('coin')} oid={oid}, cancelling")
                    try:
                        self.exchange.cancel(order["coin"], oid)
                        cancelled_stale.add(oid)
                    except Exception as e:
                        logger.error(f"Cancel {label} failed: {e}")

            # Clean up stale orders that are no longer open (cancelled elsewhere or filled)
            self._stale_orders -= cancelled_stale
            self._stale_orders -= (self._stale_orders - open_oids)

            # Also clean up local state for stale orders that were successfully cancelled
            for coin, state in self._states.items():
                if state.bid_order and state.bid_order.oid in cancelled_stale:
                    state.bid_order = None
                if state.ask_order and state.ask_order.oid in cancelled_stale:
                    state.ask_order = None

        except Exception as e:
            logger.warning(f"Orphan check failed: {e}")

    def get_active_orders(self, coin: str) -> tuple[Optional[ActiveOrder], Optional[ActiveOrder]]:
        """Return (bid_order, ask_order) for a coin."""
        state = self._states.get(coin)
        if not state:
            return None, None
        return state.bid_order, state.ask_order

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_child_size(
        self,
        coin: str,
        depth20_usd: float,
        free_equity_usd: float,
        q_soft: float,
        vol_scale: float,
        anchor_scale: float,
    ) -> float:
        """Child order size (Spec Section 3).

        size = min(notional_target, 0.20*free_equity, 0.003*depth20, 0.75*Q_soft)
               * vol_scale * anchor_scale

        Bug #5 fix: removed $10 floor. If constraints say size < HL minimum,
        return 0 (don't quote) instead of overriding to $10.
        """
        notional_target = DEFAULT_NOTIONAL.get(coin, 50.0)
        size = min(
            notional_target,
            0.20 * free_equity_usd,
            0.003 * depth20_usd if depth20_usd > 0 else notional_target,
            0.75 * q_soft,
        )
        size *= vol_scale * anchor_scale
        # Return 0 if below HL minimum — caller will skip quoting this side
        if size < self.config.min_notional_usd:
            return 0.0
        return size

    def _cancel_coin_orders(self, coin: str, state: QuoteState) -> bool:
        """Cancel all tracked orders for a coin.

        Returns True if cancel succeeded (safe to clear local state).
        Returns False if cancel failed (orders kept in tracking + stale set).
        """
        if self.dry_run:
            if state.bid_order or state.ask_order:
                logger.info(f"[DRY] Cancel {coin} orders")
            state.bid_order = None
            state.ask_order = None
            return True

        cancels = []
        if state.bid_order:
            cancels.append(CancelRequest(coin=coin, oid=state.bid_order.oid))
        if state.ask_order:
            cancels.append(CancelRequest(coin=coin, oid=state.ask_order.oid))

        if not cancels:
            state.bid_order = None
            state.ask_order = None
            return True

        try:
            self.exchange.bulk_cancel(cancels)
            # Cancel confirmed — safe to clear local state
            state.bid_order = None
            state.ask_order = None
            return True
        except Exception as e:
            logger.warning(f"Cancel {coin} failed: {e} — orders kept in stale tracking")
            # Bug #1: Do NOT clear local state. Add to stale set for reconciliation.
            if state.bid_order:
                self._stale_orders.add(state.bid_order.oid)
            if state.ask_order:
                self._stale_orders.add(state.ask_order.oid)
            return False

    def _place_alo(self, coin: str, is_buy: bool, size: float, price: float) -> Optional[int]:
        """Place ALO (Add Liquidity Only) limit order. Returns oid or None."""
        notional = size * price
        if notional < self.config.min_notional_usd:
            return None

        try:
            order_type = OrderType(limit={"tif": "Alo"})
            result = self.exchange.order(coin, is_buy, size, price, order_type)

            if result and result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses and "resting" in statuses[0]:
                    oid = statuses[0]["resting"]["oid"]
                    logger.debug(
                        f"{coin} {'BID' if is_buy else 'ASK'} placed: "
                        f"{size:.6f} @ {price:.6f} oid={oid}"
                    )
                    return oid
                elif statuses and "error" in statuses[0]:
                    logger.debug(f"ALO rejected: {coin} {statuses[0]['error']}")
            return None
        except Exception as e:
            logger.error(f"Order failed: {coin} {'BUY' if is_buy else 'SELL'} {size}@{price}: {e}")
            return None

    def _query_fill(self, oid: int) -> Optional[dict]:
        """Query HL for fill details of a specific order."""
        try:
            result = self.info.query_order_by_oid(self.address, oid)
            if result and result.get("order", {}).get("status") == "filled":
                order = result.get("order", {})
                return {
                    "price": float(order.get("avgPx", 0) or 0),
                    "size": float(order.get("sz", 0) or 0),
                    "fee": float(order.get("fee", 0) or 0),
                }
        except Exception as e:
            logger.debug(f"Order query failed for oid={oid}: {e}")
        return None

    def _round_size(self, coin: str, size: float) -> float:
        """Round size to venue lot increment."""
        decimals = self.sz_decimals.get(coin, 4)
        return round(size, decimals)

    def _round_price(self, coin: str, price: float) -> float:
        """Round price to venue tick size (5 significant figures)."""
        if price <= 0:
            return 0.0
        magnitude = len(str(int(price)))
        decimals = max(0, 5 - magnitude)
        return round(price, decimals)
