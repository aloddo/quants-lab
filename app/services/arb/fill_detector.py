"""
V2 FillDetector — WS-primary, REST-fallback fill detection with tri-state returns.

Handles the asymmetry: Bybit WS works, Binance WS returns 410 (EU restriction).
Designed to work with ANY combination of WS availability.

Key invariant: NEVER conflate "check failed" (UNKNOWN) with "not filled" (NOT_FILLED).
On UNKNOWN, the caller must retry or pause -- never unwind/escalate.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Callable

from app.services.arb.order_feed import FillEvent, OrderUpdate
from app.services.arb.position_store import LegState as LegStateEnum, FillResult

logger = logging.getLogger(__name__)


# ── Leg Tracker ────────────────────────────────────────────────

@dataclass
class TrackedLeg:
    """In-memory state for a single order leg being tracked for fills."""
    venue: str
    symbol: str
    side: str
    order_id: str = ""
    client_order_id: str = ""
    target_qty: float = 0.0
    target_price: float = 0.0
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0
    fee: float = 0.0
    fee_asset: str = ""
    is_maker: bool = False
    exec_ids: list = field(default_factory=list)
    state: LegStateEnum = LegStateEnum.IDLE
    fill_event: asyncio.Event = field(default_factory=asyncio.Event)
    submitted_at: float = 0.0
    filled_at: float = 0.0

    @property
    def is_filled(self) -> bool:
        return self.state == LegStateEnum.FILLED

    @property
    def is_terminal(self) -> bool:
        return self.state in (LegStateEnum.FILLED, LegStateEnum.CANCELLED, LegStateEnum.REJECTED)

    @property
    def has_any_fill(self) -> bool:
        return self.filled_qty > 0

    def apply_fill(self, fill: FillEvent) -> bool:
        """Apply a fill event. Returns False if duplicate (already seen exec_id)."""
        # MEDIUM FIX (Codex #10): Only dedupe if exec_id is non-empty
        # Empty exec_ids from REST fallback are legitimate fills, not duplicates
        if fill.exec_id and fill.exec_id in self.exec_ids:
            return False  # Duplicate

        # Weighted average price
        prev_value = self.avg_fill_price * self.filled_qty
        self.filled_qty += fill.qty

        # Normalize fee to USD. Bybit returns fees in USDT (quote), but Binance
        # returns fees in the received asset: base token for buys, USDC for sells.
        # HIGH FIX (Codex #5): BNB discount fees use BNB as fee_asset, not the traded
        # pair's base token. Using fill.price (traded pair) for BNB conversion is WRONG.
        # We can only safely convert when fee_asset matches the traded pair's base.
        _QUOTE_ASSETS = ("USDT", "USDC", "USD", "BUSD", "FDUSD")
        if fill.fee_asset and fill.fee_asset.upper() not in _QUOTE_ASSETS:
            # Derive traded pair's base from symbol (e.g., RAREUSDC -> RARE)
            _base = self.symbol.replace("USDT", "").replace("USDC", "") if self.symbol else ""
            if fill.fee_asset.upper() == _base.upper() and fill.price > 0:
                # Fee is in the traded base token — safe to use fill price
                self.fee += fill.fee * fill.price
            elif fill.price > 0 and not _base:
                # No symbol info, best effort with fill price
                self.fee += fill.fee * fill.price
            else:
                # Fee is in a DIFFERENT asset (BNB discount, etc.) — can't convert accurately
                # Estimate: assume fee is ~0.1% of notional as fallback
                notional = fill.price * fill.qty if fill.price > 0 and fill.qty > 0 else 0
                estimated_fee_usd = notional * 0.001 if notional > 0 else 0
                if estimated_fee_usd > 0:
                    self.fee += estimated_fee_usd
                    logger.warning(f"Fee in {fill.fee_asset} (not base token), estimated ${estimated_fee_usd:.6f} from 0.1% notional")
                else:
                    logger.warning(f"Fee normalization: unknown asset {fill.fee_asset}, fee={fill.fee} — cannot convert")
                    self.fee += fill.fee  # Store raw
        else:
            self.fee += fill.fee
        self.fee_asset = "USD"  # Always normalized to USD
        self.is_maker = fill.is_maker
        self.filled_at = fill.local_ts
        self.exec_ids.append(fill.exec_id)

        if self.filled_qty > 0:
            self.avg_fill_price = (prev_value + fill.price * fill.qty) / self.filled_qty

        # Check if fully filled
        if self.filled_qty >= self.target_qty * 0.99 or fill.order_status == "Filled":
            self.state = LegStateEnum.FILLED
        elif self.filled_qty > 0:
            self.state = LegStateEnum.PARTIAL

        if self.is_filled:
            self.fill_event.set()

        return True

    def apply_rest_result(self, result: dict):
        """Apply a REST get_order() result to update state.

        CRITICAL FIX (Codex review): Partial fills must remain PARTIAL, not be
        promoted to FILLED. Only mark FILLED when filled_qty >= target_qty.
        """
        fill_result = result.get("fill_result", "UNKNOWN")
        if fill_result == "UNKNOWN":
            return  # Don't update on unknown — caller must retry or pause

        filled_qty = float(result.get("filled_qty", 0))
        avg_price = float(result.get("avg_price", 0))

        if fill_result in ("FILLED", "PARTIAL") and filled_qty > self.filled_qty:
            self.filled_qty = filled_qty
            self.avg_fill_price = avg_price
            # Only mark FILLED if qty meets target (within 1% tolerance)
            if self.target_qty > 0 and filled_qty >= self.target_qty * 0.99:
                self.state = LegStateEnum.FILLED
                self.fill_event.set()  # Only signal completion on FULL fill
            elif filled_qty > 0:
                self.state = LegStateEnum.PARTIAL
                # FIX #4: Do NOT set fill_event for PARTIAL — premature wake causes
                # callers to cancel/escalate while order is still filling.
                # The timeout in wait_for_fills will handle the PARTIAL case.
        elif fill_result == "NOT_FILLED":
            status = result.get("status", "")
            if status in ("Cancelled", "CANCELED", "Rejected", "REJECTED", "EXPIRED"):
                if not self.has_any_fill:
                    self.state = LegStateEnum.CANCELLED
                    self.fill_event.set()  # Signal terminal state

    def enrich_fee_from_trades(self, trades: list[dict]):
        """
        Enrich fee data from exchange trade records (e.g. Binance /myTrades).
        Called after REST fill confirmation to capture commission data that
        get_order() doesn't return.

        Each trade dict should have: commission (float), commissionAsset (str), price (float).
        """
        _QUOTE_ASSETS = ("USDT", "USDC", "USD", "BUSD", "FDUSD")
        # Derive base asset from symbol for fee asset matching
        _base = self.symbol.replace("USDT", "").replace("USDC", "") if self.symbol else ""
        total_fee_usd = 0.0
        for t in trades:
            commission = float(t.get("commission", 0))
            asset = t.get("commissionAsset", "")
            price = float(t.get("price", 0))
            if asset.upper() in _QUOTE_ASSETS:
                total_fee_usd += commission
            elif asset.upper() == _base.upper() and price > 0:
                # Fee in traded base token — safe to use fill price
                total_fee_usd += commission * price
            elif price > 0:
                # MEDIUM FIX (Opus C1 #4): Fee in unknown asset (BNB discount etc.)
                # Estimate from notional since we can't convert accurately
                qty = float(t.get("qty", 0))
                notional = price * qty
                total_fee_usd += notional * 0.001  # ~0.1% estimate
                logger.warning(f"Fee in {asset} (not {_base}), estimated from 0.1% notional")
            # else: can't convert, skip (shouldn't happen)
        # MEDIUM FIX (Opus C3 #5): Only overwrite fee if REST data is more authoritative
        # than existing WS-accumulated fee. If WS fee is already set, use the higher of the two
        # (REST is more complete since it captures all fills, WS may miss some)
        if total_fee_usd > 0:
            self.fee = max(self.fee, total_fee_usd)
        self.fee_asset = "USD"
        # Set is_maker from first trade (all fills in an arb order should be same type)
        if trades:
            self.is_maker = bool(trades[0].get("isMaker", False))

    def mark_cancelled(self):
        """Mark leg as cancelled (only if no fills received)."""
        if not self.has_any_fill:
            self.state = LegStateEnum.CANCELLED
        self.fill_event.set()


# ── Fill Detector ──────────────────────────────────────────────

class FillDetector:
    """
    Routes fill events from WS and REST to tracked legs.

    Three-path detection:
    A) WS primary — Bybit always, Binance if Ed25519 key available
    B) REST poll — GET /api/v3/order with exponential backoff
    C) Hybrid — Bybit via WS, Binance via REST

    Uses asyncio.Event so callers can wait_for() regardless of fill source.
    """

    UNMATCHED_BUFFER_MAX = 50
    UNMATCHED_TTL_S = 60.0

    def __init__(self, get_order_fn: Callable, get_trades_fn: Callable = None):
        """
        get_order_fn(venue, symbol, order_id, client_order_id) -> dict with fill_result
        get_trades_fn(venue, symbol, order_id) -> list[dict] with commission data
        """
        self._get_order = get_order_fn
        self._get_trades = get_trades_fn

        # Tracked legs: keyed by order_id AND client_order_id
        self._legs: dict[str, TrackedLeg] = {}
        self._unmatched_fills: list[FillEvent] = []
        # Fail-closed flag: set when unmatched buffer overflows.
        # Callers should check this and trigger RISK_PAUSE.
        self.unmatched_buffer_overflow: bool = False

    def register_leg(self, leg: TrackedLeg):
        """Register a leg for fill tracking. Call BEFORE submitting the order."""
        if leg.client_order_id:
            self._legs[leg.client_order_id] = leg
        if leg.order_id and leg.order_id != leg.client_order_id:
            self._legs[leg.order_id] = leg

    def register_exchange_id(self, client_order_id: str, exchange_order_id: str):
        """Map exchange order ID to an existing tracked leg (after submit returns)."""
        leg = self._legs.get(client_order_id)
        if leg and exchange_order_id:
            leg.order_id = exchange_order_id
            self._legs[exchange_order_id] = leg
            # Share the same fill event
            logger.debug(f"Registered exchange ID: {client_order_id} -> {exchange_order_id}")

    def on_fill(self, fill: FillEvent):
        """
        Called by OrderFeed when a WS fill arrives.
        Matches by exchange order_id first, then client_order_id.
        Buffers unmatched fills for later replay.
        """
        leg = self._legs.get(fill.order_id)

        # Fallback: try client order ID (handles WS fill during submit race)
        if not leg:
            client_id = getattr(fill, 'client_order_id', '')
            if client_id:
                leg = self._legs.get(client_id)
                if leg:
                    # Also register exchange order_id for future lookups
                    self._legs[fill.order_id] = leg
                    logger.debug(f"Fill matched via client_order_id: {client_id} -> {fill.order_id}")

        if not leg:
            # Buffer unmatched fills for later replay
            now = time.time()
            self._unmatched_fills = [
                f for f in self._unmatched_fills
                if now - f.local_ts < self.UNMATCHED_TTL_S
            ]
            if len(self._unmatched_fills) < self.UNMATCHED_BUFFER_MAX:
                self._unmatched_fills.append(fill)
                logger.warning(f"Unmatched fill buffered: {fill.venue} {fill.order_id} qty={fill.qty}")
            else:
                # CRITICAL: buffer overflow is a fail-closed condition.
                # Fills are being dropped — position state may be inconsistent.
                self.unmatched_buffer_overflow = True
                logger.critical(
                    f"Unmatched fill buffer FULL ({self.UNMATCHED_BUFFER_MAX}), "
                    f"DROPPING fill: {fill.venue} {fill.order_id} qty={fill.qty}. "
                    f"RISK_PAUSE required — fill tracking integrity compromised."
                )
            return

        if leg.apply_fill(fill):
            logger.info(
                f"FILL: {fill.venue} {fill.symbol} {fill.side} "
                f"{fill.qty}@{fill.price} (total={leg.filled_qty}/{leg.target_qty})"
            )

    def on_order_update(self, update: OrderUpdate):
        """Called by OrderFeed when an order status changes."""
        leg = self._legs.get(update.order_id)
        if not leg:
            return

        if update.status in ("Cancelled", "CANCELED", "Rejected", "REJECTED"):
            leg.mark_cancelled()
        elif update.status in ("Filled", "FILLED"):
            if update.filled_qty > leg.filled_qty:
                leg.filled_qty = update.filled_qty
                leg.avg_fill_price = update.avg_price
                leg.state = LegStateEnum.FILLED
                leg.fill_event.set()

    def replay_buffered(self):
        """Replay any unmatched fills that can now be matched. Call after registering exchange IDs."""
        if not self._unmatched_fills:
            return
        buffered = self._unmatched_fills[:]
        self._unmatched_fills.clear()
        for fill in buffered:
            logger.info(f"Replaying buffered fill: {fill.venue} {fill.order_id} qty={fill.qty}")
            self.on_fill(fill)

    async def wait_for_fills(
        self,
        legs: list[TrackedLeg],
        timeout: float = 3.0,
        ws_available: dict[str, bool] | None = None,
    ) -> None:
        """
        Wait for all legs to reach terminal state, using WS + REST as appropriate.

        ws_available: {"bybit": True, "binance": False} -- per-venue WS status
        """
        ws_status = ws_available or {"bybit": True, "binance": True}

        # Start REST pollers for venues without WS
        # Also start pollers when only client_order_id is known (no exchange order_id yet)
        poller_tasks = []
        for leg in legs:
            if not ws_status.get(leg.venue, True) and (leg.order_id or leg.client_order_id):
                task = asyncio.create_task(
                    self._poll_fill_rest(leg, timeout)
                )
                poller_tasks.append(task)

        # Wait for all fill events (WS or REST will set them)
        events = [leg.fill_event.wait() for leg in legs if not leg.is_terminal]
        if events:
            try:
                await asyncio.wait_for(asyncio.gather(*events), timeout=timeout)
            except asyncio.TimeoutError:
                pass  # Caller will check leg states

        # Cancel any remaining pollers
        for task in poller_tasks:
            if not task.done():
                task.cancel()

    async def _poll_fill_rest(self, leg: TrackedLeg, total_timeout: float):
        """Background REST poller for venues without WS. Exponential backoff."""
        delays = [0.2, 0.5, 1.0, 2.0]
        for i, delay in enumerate(delays):
            await asyncio.sleep(delay)
            if leg.is_terminal:
                return

            try:
                result = await self._get_order(
                    leg.venue, leg.symbol,
                    order_id=leg.order_id,
                    client_order_id=leg.client_order_id,
                )
                leg.apply_rest_result(result)
                if leg.is_terminal:
                    logger.info(
                        f"REST poll {i + 1}/{len(delays)}: {leg.venue} "
                        f"{leg.order_id} -> {leg.state} qty={leg.filled_qty}"
                    )
                    # Enrich fee data for Binance (REST get_order doesn't return fees)
                    if leg.venue == "binance" and leg.has_any_fill and self._get_trades:
                        await self._enrich_fee(leg)
                    return
            except Exception as e:
                logger.warning(f"REST poll {i + 1} failed: {leg.venue} {e}")

    async def check_fill_definitive(
        self,
        venue: str,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
        max_retries: int = 3,
    ) -> dict:
        """
        Definitive fill check with retry. For use in crash recovery and pre-escalation checks.

        Returns dict with fill_result tri-state. Retries on UNKNOWN.
        """
        for attempt in range(max_retries):
            result = await self._get_order(venue, symbol, order_id=order_id, client_order_id=client_order_id)
            if result.get("fill_result") != "UNKNOWN":
                return result
            await asyncio.sleep(0.5 * (2 ** attempt))

        return result  # Last UNKNOWN result

    async def enrich_leg_fee(self, leg: TrackedLeg):
        """Public method to enrich fee data for a filled leg. Called by entry/exit flows."""
        if leg.venue == "binance" and leg.has_any_fill and self._get_trades:
            await self._enrich_fee(leg)

    async def _enrich_fee(self, leg: TrackedLeg):
        """
        Fetch commission data from exchange trade records for a filled Binance leg.
        REST get_order() doesn't return fees — we need /myTrades for that.
        """
        if not self._get_trades:
            return
        try:
            trades = await self._get_trades(leg.venue, leg.symbol, order_id=leg.order_id)
            if trades:
                leg.enrich_fee_from_trades(trades)
                logger.info(
                    f"Fee enriched: {leg.venue} {leg.symbol} "
                    f"fee=${leg.fee:.6f} from {len(trades)} trade(s)"
                )
        except Exception as e:
            logger.warning(f"Fee enrichment failed for {leg.venue} {leg.symbol}: {e}")

    def cleanup_leg(self, leg: TrackedLeg):
        """Remove a leg from tracking. Call after position transition is complete."""
        for key in list(self._legs.keys()):
            if self._legs.get(key) is leg:
                del self._legs[key]
