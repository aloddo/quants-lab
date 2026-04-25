"""
InventoryRiskGuard — Per-pair SL/TP for Binance spot inventory.

SL: Sells inventory and sets INACTIVE (pair leaves arb rotation).
TP: Sells inventory, realizes gain, immediately rebuys at market price.
    New cost basis = rebuy price. SL now protects from the new level.
    Pair stays ACTIVE. ~$0.04 round-trip fee on $20 position.

Does NOT sell if the pair has a locked (active arb) position.

Thresholds are provisional (N=23 retired pairs + 14-pair backtest):
- SL: -8% (worst historical loss was BB at -11.8%)
- TP: +25% (TP too tight kills winners — KAT went +51%)

Engineering fixes from adversarial review:
- Per-symbol asyncio.Lock prevents race with EntryFlow
- Fresh REST price when within 2% of threshold (stale WS = false triggers)
- SELLING state prevents double-sells
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from pymongo import MongoClient

logger = logging.getLogger(__name__)

# Rebuy pricing: buy at ask + surcharge (matches _seed_inventory pattern)
SEED_SURCHARGE = 0.0010


@dataclass
class RiskEvent:
    """Recorded when SL or TP triggers."""
    symbol: str
    event_type: str  # "SL", "TP", "TP_REBUY"
    cost_basis_usd: float
    current_price: float
    avg_cost_per_unit: float
    pnl_pct: float
    pnl_usd: float
    qty_sold: float
    fill_price: float
    rebuy_price: float  # 0 if no rebuy (SL or rebuy failed)
    rebuy_qty: float    # 0 if no rebuy
    timestamp: float


class InventoryRiskGuard:
    """
    Per-pair stop-loss and take-profit guard for spot inventory.

    Runs every check cycle (default 30s). For each ACTIVE pair:
    1. Get current price (WS preferred, REST fallback near thresholds)
    2. Compute unrealized PnL % vs avg_cost_per_unit
    3. If PnL <= SL → sell, set INACTIVE
    4. If PnL >= TP → sell, rebuy immediately, stay ACTIVE with new cost basis

    Does NOT sell locked pairs (active arb position using that inventory).
    """

    def __init__(
        self,
        sl_pct: float = -8.0,
        tp_pct: float = 25.0,
        db_uri: str = "mongodb://localhost:27017/quants_lab",
        check_interval: float = 30.0,
        fresh_price_margin_pct: float = 2.0,
    ):
        self.sl_pct = sl_pct
        self.tp_pct = tp_pct
        self.check_interval = check_interval
        self.fresh_price_margin_pct = fresh_price_margin_pct

        # Per-symbol locks shared with entry flow
        self._locks: dict[str, asyncio.Lock] = {}

        # Track pairs currently being sold (prevent double-sells)
        self._selling: set[str] = set()

        # MongoDB for event logging
        client = MongoClient(db_uri)
        db_name = db_uri.rsplit("/", 1)[-1]
        self._events_coll = client[db_name]["arb_h2_inventory_risk_events"]

        self._last_check: float = 0.0
        self._stats = {"sl_triggered": 0, "tp_triggered": 0, "tp_rebuys": 0, "checks": 0}

    def get_lock(self, symbol: str) -> asyncio.Lock:
        """Get or create per-symbol lock. Share this with entry flow."""
        if symbol not in self._locks:
            self._locks[symbol] = asyncio.Lock()
        return self._locks[symbol]

    def needs_check(self) -> bool:
        return time.time() - self._last_check >= self.check_interval

    async def check_all(
        self,
        inventory,  # InventoryLedger
        price_feed,  # PriceFeed (WS prices)
        session,  # aiohttp.ClientSession
        instrument_rules,  # InstrumentRules
        gateway,  # OrderGateway
        pair_map: dict,  # symbol -> (bn_symbol, ...)
        tg_send,  # async telegram sender
        tier_engine=None,  # TierEngine (for TP rebuy check)
    ) -> list[RiskEvent]:
        """
        Check all ACTIVE pairs for SL/TP triggers.

        Returns list of triggered events.
        """
        self._last_check = time.time()
        self._stats["checks"] += 1
        events = []

        for sym, inv in list(inventory.inventories.items()):
            # Only check ACTIVE pairs with inventory and cost basis
            if inv.lifecycle_state != "ACTIVE":
                continue
            if inv.expected_qty <= 0 or inv.avg_cost_per_unit <= 0:
                continue
            if sym in self._selling:
                continue

            # Skip locked pairs (active arb position)
            if inv.locked_qty > 0:
                continue

            # Get price — prefer WS, check if we need fresh REST
            price = self._get_ws_price(sym, price_feed)
            pnl_pct = self._compute_pnl_pct(price, inv.avg_cost_per_unit) if price else None

            # If near threshold, get fresh REST price to avoid false triggers
            needs_fresh = (
                price is None
                or (pnl_pct is not None and self._near_threshold(pnl_pct))
            )
            if needs_fresh:
                fresh = await self._fetch_fresh_price(sym, pair_map, session)
                if fresh and fresh > 0:
                    price = fresh
                    pnl_pct = self._compute_pnl_pct(price, inv.avg_cost_per_unit)

            if price is None or price <= 0 or pnl_pct is None:
                continue

            # Check SL — sell and go INACTIVE
            if pnl_pct <= self.sl_pct:
                event = await self._trigger(
                    sym, inv, price, pnl_pct, "SL",
                    inventory, session, instrument_rules, gateway,
                    pair_map, tg_send, tier_engine,
                )
                if event:
                    events.append(event)
                    self._stats["sl_triggered"] += 1

            # Check TP — sell, rebuy, stay ACTIVE
            elif pnl_pct >= self.tp_pct:
                event = await self._trigger(
                    sym, inv, price, pnl_pct, "TP",
                    inventory, session, instrument_rules, gateway,
                    pair_map, tg_send, tier_engine,
                )
                if event:
                    events.append(event)
                    self._stats["tp_triggered"] += 1

        return events

    async def _trigger(
        self,
        symbol: str,
        inv,
        current_price: float,
        pnl_pct: float,
        event_type: str,
        inventory,
        session,
        instrument_rules,
        gateway,
        pair_map: dict,
        tg_send,
        tier_engine,
    ) -> Optional[RiskEvent]:
        """Execute sell (and rebuy for TP) under the per-symbol lock."""
        lock = self.get_lock(symbol)

        # Non-blocking check — if lock is held (entry in progress), skip this cycle
        if lock.locked():
            logger.info(f"RiskGuard: {symbol} lock held (arb entry in progress), skipping {event_type}")
            return None

        async with lock:
            # Re-check after acquiring lock (state may have changed)
            inv = inventory.inventories.get(symbol)
            if not inv or inv.locked_qty > 0 or inv.lifecycle_state != "ACTIVE":
                return None

            self._selling.add(symbol)
            try:
                return await self._execute(
                    symbol, inv, current_price, pnl_pct, event_type,
                    inventory, session, instrument_rules, gateway,
                    pair_map, tg_send, tier_engine,
                )
            finally:
                self._selling.discard(symbol)

    async def _execute(
        self,
        symbol: str,
        inv,
        current_price: float,
        pnl_pct: float,
        event_type: str,
        inventory,
        session,
        instrument_rules,
        gateway,
        pair_map: dict,
        tg_send,
        tier_engine,
    ) -> Optional[RiskEvent]:
        """Execute the sell order, and rebuy on TP."""
        available = max(0.0, inv.expected_qty - inv.locked_qty)
        if available <= 0:
            return None

        bn_sym = pair_map.get(symbol, (symbol.replace("USDT", "USDC"),))[0]
        bn_rules = instrument_rules.get("binance", bn_sym)
        if not bn_rules:
            await instrument_rules.load_binance(session, [bn_sym])
            bn_rules = instrument_rules.get("binance", bn_sym)
            if not bn_rules:
                logger.error(f"RiskGuard: no instrument rules for {bn_sym}")
                return None

        sell_qty = bn_rules.round_qty(available)
        if sell_qty <= 0:
            return None

        # Get actual bid for sell price
        bid_price = await self._fetch_bid_price(bn_sym, session)
        if bid_price <= 0:
            bid_price = current_price * 0.998
        sell_price = bn_rules.round_price_for_side(bid_price * 0.9985, "Sell")

        if not bn_rules.check_notional(sell_qty, sell_price):
            logger.warning(f"RiskGuard: {symbol} notional too small for {event_type}")
            return None

        pnl_usd = available * (current_price - inv.avg_cost_per_unit)
        emoji = "\U0001f6d1" if event_type == "SL" else "\U0001f4b0"

        logger.info(f"RiskGuard {event_type}: {symbol} PnL={pnl_pct:+.1f}% (${pnl_usd:+.2f}) — selling {sell_qty}")

        try:
            # Set RETIRING before sell to prevent new arb entries
            inventory.set_lifecycle_state(symbol, "RETIRING")

            oid = await gateway.submit("binance", symbol, "Sell", sell_qty, sell_price, "limit")
            result = await gateway.get_order_with_retry("binance", symbol, order_id=oid)
            fill_state = result.get("fill_result")

            if fill_state not in ("FILLED", "PARTIAL"):
                await gateway.cancel("binance", symbol, oid)
                inventory.set_lifecycle_state(symbol, "ACTIVE")
                logger.warning(f"RiskGuard: {event_type} sell didn't fill for {symbol}")
                return None

            filled = float(result.get("filled_qty", 0))
            fill_px = float(result.get("avg_price", sell_price))
            if filled <= 0:
                inventory.set_lifecycle_state(symbol, "ACTIVE")
                return None

            inventory.sold(symbol, filled, fill_price=fill_px, fee_in_base=False)
            realized_pnl = filled * (fill_px - inv.avg_cost_per_unit)

            event = RiskEvent(
                symbol=symbol,
                event_type=event_type,
                cost_basis_usd=inv.cost_basis_usd,
                current_price=current_price,
                avg_cost_per_unit=inv.avg_cost_per_unit,
                pnl_pct=pnl_pct,
                pnl_usd=realized_pnl,
                qty_sold=filled,
                fill_price=fill_px,
                rebuy_price=0.0,
                rebuy_qty=0.0,
                timestamp=time.time(),
            )

            # ── SL: go INACTIVE, done ──
            if event_type == "SL":
                inventory.set_lifecycle_state(symbol, "INACTIVE")
                self._log_event(event)

                base = symbol.replace("USDT", "")
                await tg_send(
                    f"{emoji} <b>H2 INVENTORY SL</b> <code>{base}</code>\n"
                    f"PnL: <b>{pnl_pct:+.1f}%</b> (${realized_pnl:+.2f})\n"
                    f"Sold {filled:.6f} @ {fill_px:.6f}\n"
                    f"Pair retired.",
                    session,
                )
                return event

            # ── TP: rebuy immediately if still in tier set ──
            still_tradable = self._is_in_tier_set(symbol, tier_engine)

            if not still_tradable:
                # Pair left the tier set — just take profit, go INACTIVE
                inventory.set_lifecycle_state(symbol, "INACTIVE")
                self._log_event(event)

                base = symbol.replace("USDT", "")
                await tg_send(
                    f"{emoji} <b>H2 INVENTORY TP</b> <code>{base}</code>\n"
                    f"PnL: <b>{pnl_pct:+.1f}%</b> (${realized_pnl:+.2f})\n"
                    f"Sold {filled:.6f} @ {fill_px:.6f}\n"
                    f"Not in tier set — pair retired.",
                    session,
                )
                return event

            # Rebuy: same qty at current ask + surcharge
            rebuy_price, rebuy_qty = await self._rebuy(
                symbol, bn_sym, filled, bn_rules,
                inventory, session, gateway,
            )

            if rebuy_qty > 0:
                event.event_type = "TP_REBUY"
                event.rebuy_price = rebuy_price
                event.rebuy_qty = rebuy_qty
                self._stats["tp_rebuys"] += 1

                # Pair stays ACTIVE with new cost basis
                inventory.set_lifecycle_state(symbol, "ACTIVE")
                self._log_event(event)

                base = symbol.replace("USDT", "")
                new_cost = inventory.inventories[symbol].avg_cost_per_unit
                await tg_send(
                    f"{emoji} <b>H2 INVENTORY TP+REBUY</b> <code>{base}</code>\n"
                    f"Realized: <b>{pnl_pct:+.1f}%</b> (${realized_pnl:+.2f})\n"
                    f"Sold {filled:.6f} @ {fill_px:.6f}\n"
                    f"Rebought {rebuy_qty:.6f} @ {rebuy_price:.6f}\n"
                    f"New cost basis: ${new_cost:.6f} | SL now at ${new_cost * (1 + self.sl_pct/100):.6f}",
                    session,
                )
            else:
                # Rebuy failed — pair goes INACTIVE, gain is still realized
                inventory.set_lifecycle_state(symbol, "INACTIVE")
                self._log_event(event)

                base = symbol.replace("USDT", "")
                await tg_send(
                    f"{emoji} <b>H2 INVENTORY TP</b> <code>{base}</code>\n"
                    f"Realized: <b>{pnl_pct:+.1f}%</b> (${realized_pnl:+.2f})\n"
                    f"Sold {filled:.6f} @ {fill_px:.6f}\n"
                    f"\u26a0\ufe0f Rebuy FAILED — pair retired. Will re-seed next cycle if still in tiers.",
                    session,
                )

            return event

        except Exception as e:
            logger.error(f"RiskGuard: {event_type} failed for {symbol}: {e}", exc_info=True)
            inventory.set_lifecycle_state(symbol, "ACTIVE")
            return None

    async def _rebuy(
        self,
        symbol: str,
        bn_sym: str,
        target_qty: float,
        bn_rules,
        inventory,
        session,
        gateway,
    ) -> tuple[float, float]:
        """
        Rebuy inventory after TP sell.

        Returns (fill_price, filled_qty). Both 0 on failure.
        """
        import aiohttp

        # Get current ask
        try:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/bookTicker",
                params={"symbol": bn_sym},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                data = await resp.json()
            ask_price = float(data.get("askPrice", 0))
        except Exception:
            ask_price = 0

        if ask_price <= 0:
            logger.error(f"RiskGuard rebuy: no ask price for {bn_sym}")
            return 0.0, 0.0

        buy_price = bn_rules.round_price_for_side(ask_price * (1 + SEED_SURCHARGE), "Buy")
        buy_qty = bn_rules.round_qty(target_qty)

        if buy_qty <= 0 or not bn_rules.check_notional(buy_qty, buy_price):
            logger.error(f"RiskGuard rebuy: qty/notional check failed for {symbol}")
            return 0.0, 0.0

        logger.info(f"RiskGuard rebuy: {symbol} buy {buy_qty} @ {buy_price}")

        try:
            oid = await gateway.submit("binance", symbol, "Buy", buy_qty, buy_price, "limit")
            result = await gateway.get_order_with_retry("binance", symbol, order_id=oid)
            fill_state = result.get("fill_result")

            if fill_state in ("FILLED", "PARTIAL"):
                filled = float(result.get("filled_qty", 0))
                fill_price = float(result.get("avg_price", buy_price))
                if filled > 0:
                    inventory.bought(symbol, filled, fill_price=fill_price)
                    logger.info(f"RiskGuard rebuy OK: {symbol} +{filled:.6f} @ {fill_price:.6f}")
                    return fill_price, filled

            # Didn't fill — cancel
            await gateway.cancel("binance", symbol, oid)
            logger.warning(f"RiskGuard rebuy: order didn't fill for {symbol}")
            return 0.0, 0.0

        except Exception as e:
            logger.error(f"RiskGuard rebuy failed for {symbol}: {e}")
            return 0.0, 0.0

    def _is_in_tier_set(self, symbol: str, tier_engine) -> bool:
        """Check if symbol is still in the tradable tier set."""
        if tier_engine is None:
            return True  # No tier engine = assume tradable
        try:
            tradable = {info.symbol_bb for info in tier_engine.tradable_pairs()}
            return symbol in tradable
        except Exception:
            return True  # On error, assume tradable (don't punish)

    def _get_ws_price(self, symbol: str, price_feed) -> Optional[float]:
        """Get mid price from WS feed."""
        if not price_feed:
            return None
        snap = price_feed.get_spread_for_exit(symbol)
        if snap and snap.bn_bid > 0 and snap.bn_ask > 0:
            return (snap.bn_bid + snap.bn_ask) / 2
        return None

    def _compute_pnl_pct(self, price: float, cost: float) -> float:
        if cost <= 0:
            return 0.0
        return (price / cost - 1) * 100

    def _near_threshold(self, pnl_pct: float) -> bool:
        """Is PnL within fresh_price_margin_pct of either threshold?"""
        margin = self.fresh_price_margin_pct
        near_sl = abs(pnl_pct - self.sl_pct) <= margin
        near_tp = abs(pnl_pct - self.tp_pct) <= margin
        return near_sl or near_tp

    async def _fetch_fresh_price(
        self, symbol: str, pair_map: dict, session
    ) -> Optional[float]:
        """Fetch fresh REST mid price from Binance."""
        import aiohttp
        bn_sym = pair_map.get(symbol, (symbol.replace("USDT", "USDC"),))[0]
        try:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/bookTicker",
                params={"symbol": bn_sym},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                data = await resp.json()
            bid = float(data.get("bidPrice", 0))
            ask = float(data.get("askPrice", 0))
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
        except Exception as e:
            logger.debug(f"RiskGuard: REST price failed for {bn_sym}: {e}")
        return None

    async def _fetch_bid_price(self, bn_sym: str, session) -> float:
        """Fetch actual bid from Binance for sell pricing."""
        import aiohttp
        try:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/bookTicker",
                params={"symbol": bn_sym},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                data = await resp.json()
            return float(data.get("bidPrice", 0))
        except Exception:
            return 0.0

    def _log_event(self, event: RiskEvent):
        """Persist event to MongoDB."""
        self._events_coll.insert_one({
            "symbol": event.symbol,
            "event_type": event.event_type,
            "cost_basis_usd": event.cost_basis_usd,
            "current_price": event.current_price,
            "avg_cost_per_unit": event.avg_cost_per_unit,
            "pnl_pct": event.pnl_pct,
            "pnl_usd": event.pnl_usd,
            "qty_sold": event.qty_sold,
            "fill_price": event.fill_price,
            "rebuy_price": event.rebuy_price,
            "rebuy_qty": event.rebuy_qty,
            "timestamp": event.timestamp,
        })

    def status(self) -> dict:
        return {
            "sl_pct": self.sl_pct,
            "tp_pct": self.tp_pct,
            "sl_triggered": self._stats["sl_triggered"],
            "tp_triggered": self._stats["tp_triggered"],
            "tp_rebuys": self._stats["tp_rebuys"],
            "checks": self._stats["checks"],
            "selling": list(self._selling),
        }
