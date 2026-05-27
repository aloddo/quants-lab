"""
Maker Arb Engine V2 — rests ALO orders on HL, hedges on BB taker.

Architecture:
  1. For each pair, rest an ALO sell (HL premium pairs) or buy (BB premium pairs)
     on HL at the adaptive P90 spread offset from current mid.
  2. WS orderUpdates subscription detects fills in real-time (<100ms).
  3. On fill → immediately fire BB taker IOC to hedge (180ms).
  4. Exit via taker IOC on both venues when spread reverts to P25.
  5. Requote resting orders on mid drift > requote_drift_bps or age > requote_interval_s.

Fee structure:
  Entry: HL maker 1.44bp + BB taker 5.5bp = 6.94bp/side
  Exit:  HL taker 4.32bp + BB taker 5.5bp = 9.82bp/side
  RT total: 16.76bp (vs 19.64bp taker-taker)

V2 fixes (all 10 from code review):
  #1  WS format: OID at update["order"]["oid"], not update["oid"]
  #2  Direction: fixed per pair from historical bias, not per-tick noise
  #3  Exit logic: exit monitoring loop closes both legs on spread revert
  #4  HL unwind: proper aggressive price with 5-sig-fig rounding
  #5  Shutdown: closes all open positions before exiting
  #6  ALO "filled" case: removed (ALO cannot cross by definition)
  #7  Margin awareness: limit quoting to max_resting_orders pairs
  #8  Requote race: _requoting lock set prevents double-placement
  #9  WS keepalive: periodic ping every 30s
  #10 Unused hl_info: removed from launch script
"""
import asyncio
import json
import logging
import math
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import os

import requests as req_lib
import websockets

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.instrument_rules import InstrumentManager, PairRules
from app.services.arb_hlbb.order_api import BybitOrderAPI, OrderResult
from app.services.arb_hlbb.price_feed import DualPriceFeed, SpreadSnapshot
from app.services.arb_hlbb.signal_engine import SignalEngine, SignalEvent, TrackedPosition

logger = logging.getLogger(__name__)

# FIX #2: Historical direction bias per pair (from 12-day spread analysis).
# HL_PREMIUM = HL overshoots → rest SELL on HL.
# BB_PREMIUM = BB overshoots → rest BUY on HL.
PAIR_DIRECTION_BIAS: dict[str, str] = {
    "NEAR-USDT": "HL_PREMIUM",
    "DYDX-USDT": "HL_PREMIUM",
    "FARTCOIN-USDT": "HL_PREMIUM",
    "APE-USDT": "HL_PREMIUM",
    "HYPE-USDT": "HL_PREMIUM",
    "IP-USDT": "HL_PREMIUM",
    "AAVE-USDT": "HL_PREMIUM",
    "ORDI-USDT": "HL_PREMIUM",
    "PENGU-USDT": "HL_PREMIUM",
    "CHIP-USDT": "BB_PREMIUM",
    "AXS-USDT": "BB_PREMIUM",
    "OP-USDT": "BB_PREMIUM",
    "ARB-USDT": "BB_PREMIUM",
    "DOT-USDT": "HL_PREMIUM",
    "LINK-USDT": "HL_PREMIUM",
    "UNI-USDT": "HL_PREMIUM",
    "ADA-USDT": "HL_PREMIUM",
    "SOL-USDT": "HL_PREMIUM",
    "WLD-USDT": "HL_PREMIUM",
    "SUI-USDT": "HL_PREMIUM",
}


class MakerState(Enum):
    QUOTING = "quoting"
    HEDGING = "hedging"
    OPEN = "open"
    EXITING = "exiting"
    FLAT = "flat"


@dataclass
class RestingOrder:
    """Tracks an ALO order resting on HL."""
    oid: int
    coin: str
    pair: str
    is_buy: bool
    price: float
    qty: float
    placed_at: float
    mid_at_place: float


@dataclass
class MakerPosition:
    """Tracks a maker arb position."""
    position_id: str
    pair: str
    coin: str
    direction: str
    state: MakerState = MakerState.FLAT
    target_qty: float = 0.0

    # Entry fills
    hl_fill_price: float = 0.0
    hl_fill_qty: float = 0.0
    hl_fill_time: float = 0.0

    bb_fill_price: float = 0.0
    bb_fill_qty: float = 0.0
    bb_fill_time: float = 0.0

    # Spread at fill
    signal_spread_bps: float = 0.0
    captured_spread_bps: float = 0.0

    # Exit
    entry_time: float = 0.0
    exit_time: float = 0.0
    exit_reason: str = ""
    net_pnl_bps: float = 0.0
    net_pnl_usd: float = 0.0
    hl_exited: bool = False  # C2: track per-leg exit status
    bb_exited: bool = False
    hl_exit_price: float = 0.0
    bb_exit_price: float = 0.0
    exit_retries: int = 0


def _round_hl_price_aggressive(price: float, is_buy: bool) -> float:
    """Round HL price to 5 significant figures, direction-aware. FIX #4."""
    if price <= 0 or math.isnan(price):
        return price
    magnitude = math.floor(math.log10(price))
    factor = 10 ** (4 - magnitude)  # 5 sig figs
    if is_buy:
        return math.ceil(price * factor) / factor
    else:
        return math.floor(price * factor) / factor


class MakerArbEngine:
    """Rests ALO orders on HL at adaptive spread offsets. On fill, hedges with BB taker."""

    def __init__(
        self,
        config: ArbConfig,
        price_feed: DualPriceFeed,
        signal_engine: SignalEngine,
        instrument_mgr: InstrumentManager,
        bb_api: BybitOrderAPI,
        hl_exchange,
        hl_address: str,
    ):
        self.config = config
        self.price_feed = price_feed
        self.signal_engine = signal_engine
        self.instrument_mgr = instrument_mgr
        self.bb_api = bb_api
        self.hl_exchange = hl_exchange
        self.hl_address = hl_address

        # State
        self._resting_orders: dict[str, RestingOrder] = {}
        self._positions: dict[str, MakerPosition] = {}
        self._pair_cooldowns: dict[str, float] = {}
        self._requoting: set[str] = set()  # FIX #8: pairs currently being requoted

        # Config — FIX #7: limit resting orders to margin capacity
        self.requote_drift_bps: float = 10.0
        self.requote_interval_s: float = 30.0
        self.max_concurrent: int = 3
        self.max_resting_orders: int = 7  # ~$47 / $7 margin per order
        self.max_hold_s: float = 300.0     # max position hold time
        self.exit_check_interval_s: float = 1.0

        # WS
        self._fill_queue: asyncio.Queue = asyncio.Queue()
        self._shutdown = False

        # C5 FIX: daily loss tracking
        self._daily_pnl_usd: float = 0.0
        self._daily_reset_date: str = ""

        # Metrics
        self.total_hl_fills: int = 0
        self.total_bb_hedges: int = 0
        self.total_bb_hedge_failures: int = 0
        self.total_exits: int = 0

    # ── WS Fill Monitor ─────────────────────────────────────────

    async def _start_fill_monitor(self):
        """Subscribe to HL orderUpdates for real-time fill detection. FIX #9: keepalive."""
        uri = self.config.hl_ws_url
        while not self._shutdown:
            try:
                async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
                    sub = {
                        "method": "subscribe",
                        "subscription": {
                            "type": "orderUpdates",
                            "user": self.hl_address,
                        }
                    }
                    await ws.send(json.dumps(sub))
                    logger.info("Fill monitor: subscribed to orderUpdates")

                    async for msg in ws:
                        if self._shutdown:
                            break
                        try:
                            data = json.loads(msg)
                            if data.get("channel") == "orderUpdates":
                                await self._handle_order_update(data["data"])
                        except Exception as e:
                            logger.error(f"Fill monitor parse error: {e}", exc_info=True)

            except websockets.ConnectionClosed:
                logger.warning("Fill monitor WS closed. Reconnecting...")
            except Exception as e:
                logger.warning(f"Fill monitor error: {e}. Reconnecting...")
            if not self._shutdown:
                # C3 FIX: check for missed fills during WS gap
                await self._reconcile_missed_fills()
                await asyncio.sleep(2)

    async def _handle_order_update(self, updates: list):
        """Process HL order update events. FIX #1: correct nested format."""
        for update in updates:
            order = update.get("order", {})
            # H1 FIX: normalize OID to int for consistent comparison
            raw_oid = order.get("oid")
            oid = int(raw_oid) if raw_oid is not None else None
            status = update.get("status")
            remaining_sz = order.get("sz", "0")
            orig_sz = order.get("origSz", "0")
            coin = order.get("coin")

            logger.info(
                f"WS orderUpdate: {coin} oid={oid} status={status} "
                f"remaining={remaining_sz} orig={orig_sz}"
            )

            if status not in ("filled",):
                continue  # only act on complete fills for now

            # Check if this is one of our resting orders
            matched = False
            for pair, resting in list(self._resting_orders.items()):
                if int(resting.oid) == oid:
                    fill_qty = float(orig_sz) if float(orig_sz) > 0 else resting.qty
                    fill_px = float(order.get("limitPx", resting.price))

                    fill_msg = (
                        f"MAKER FILL {pair}: HL filled "
                        f"qty={fill_qty} @ {fill_px} "
                        f"(resting at {resting.price})"
                    )
                    logger.info(fill_msg)
                    self._notify_telegram(fill_msg)

                    await self._fill_queue.put({
                        "pair": pair,
                        "coin": coin or resting.coin,
                        "oid": oid,
                        "fill_qty": fill_qty,
                        "fill_price": fill_px,
                        "is_buy": resting.is_buy,
                        "timestamp": time.time(),
                    })

                    del self._resting_orders[pair]
                    self.total_hl_fills += 1
                    matched = True
                    break

            if not matched and status == "filled":
                logger.warning(
                    f"WS fill for UNKNOWN order: {coin} oid={oid} "
                    f"sz={orig_sz} — not in our resting orders dict"
                )

    async def _reconcile_missed_fills(self):
        """C3 FIX: After WS reconnect, check for fills that happened during the gap."""
        try:
            # Query HL for current positions — any position not in our _positions is unhedged
            state = await asyncio.to_thread(
                self.hl_exchange.info.user_state, self.hl_address
            )
            for ap in state.get("assetPositions", []):
                pos = ap.get("position", {})
                sz = float(pos.get("szi", 0))
                if sz == 0:
                    continue
                coin = pos.get("coin", "")
                pair = f"{coin}-USDT"

                # C3 FIX: only touch positions for OUR configured pairs
                if pair not in self.config.default_pairs:
                    logger.info(f"Reconcile: ignoring {coin} position (not in our pair list)")
                    continue

                if pair not in self._positions:
                    logger.error(
                        f"UNTRACKED POSITION DETECTED: {coin} size={sz}. "
                        f"Likely filled during WS gap. UNWINDING..."
                    )
                    is_buy = sz < 0  # reverse direction
                    await self._unwind_hl({
                        "pair": pair, "coin": coin, "oid": 0,
                        "fill_qty": abs(sz), "fill_price": float(pos.get("entryPx", 0)),
                        "is_buy": not is_buy,  # original direction
                    })

            # Also reconcile resting orders — remove any that HL no longer shows
            hl_orders = await asyncio.to_thread(
                self.hl_exchange.info.open_orders, self.hl_address
            )
            hl_oids = {int(o["oid"]) for o in hl_orders}
            for pair, resting in list(self._resting_orders.items()):
                if int(resting.oid) not in hl_oids:
                    logger.warning(f"Resting order {pair} oid={resting.oid} no longer on HL — removing")
                    del self._resting_orders[pair]

        except Exception as e:
            logger.error(f"Reconciliation failed: {e}")

    # ── Hedge Execution ─────────────────────────────────────────

    async def _hedge_loop(self):
        """Process fill events and hedge on BB."""
        while not self._shutdown:
            try:
                fill = await asyncio.wait_for(self._fill_queue.get(), timeout=5)
                await self._execute_bb_hedge(fill)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Hedge loop error: {e}", exc_info=True)

    async def _execute_bb_hedge(self, fill: dict):
        """Fire BB taker IOC to hedge an HL maker fill."""
        pair = fill["pair"]
        rules = self.instrument_mgr.get_rules(pair)
        if not rules:
            logger.error(f"No rules for {pair}, cannot hedge! UNWINDING HL...")
            await self._unwind_hl(fill)
            return

        bb_side = "Buy" if not fill["is_buy"] else "Sell"

        snap = self.price_feed.get_spread(pair)
        if not snap:
            logger.error(f"No price snapshot for {pair}, cannot hedge! UNWINDING HL...")
            await self._unwind_hl(fill)
            return

        aggression_mult = self.config.order_aggression_bps / 10000
        if bb_side == "Buy":
            bb_price = snap.bb_ask * (1 + aggression_mult)
            direction = "SHORT_HL_LONG_BB"
        else:
            bb_price = snap.bb_bid * (1 - aggression_mult)
            direction = "SHORT_BB_LONG_HL"

        qty = rules.round_bb_qty(fill["fill_qty"])
        bb_link_id = f"mkr_{pair.replace('-','')[:6]}_{int(time.time()) % 100000}"

        logger.info(
            f"HEDGING {pair}: BB {bb_side} {qty} @ "
            f"{rules.format_bb_price(rules.round_bb_price_buy(bb_price) if bb_side == 'Buy' else rules.round_bb_price_sell(bb_price))} "
            f"(HL filled {fill['fill_qty']} @ {fill['fill_price']})"
        )

        t0 = time.time()
        bb_result = await self.bb_api.place_ioc(
            symbol=rules.bb_symbol,
            side=bb_side,
            qty=rules.format_bb_qty(qty),
            price=rules.format_bb_price(
                rules.round_bb_price_buy(bb_price) if bb_side == "Buy"
                else rules.round_bb_price_sell(bb_price)
            ),
            order_link_id=bb_link_id,
        )
        hedge_ms = (time.time() - t0) * 1000

        if bb_result.success and bb_result.filled_qty > 0:
            self.total_bb_hedges += 1
            actual_qty = min(fill["fill_qty"], bb_result.filled_qty)

            # C1 FIX: unwind excess HL if BB partially filled
            hl_excess = fill["fill_qty"] - bb_result.filled_qty
            if hl_excess > 0.001 * fill["fill_qty"]:  # >0.1% mismatch
                logger.warning(
                    f"PARTIAL HEDGE {pair}: BB filled {bb_result.filled_qty} "
                    f"of {fill['fill_qty']}. Unwinding {hl_excess:.4f} excess on HL..."
                )
                excess_fill = dict(fill)
                excess_fill["fill_qty"] = hl_excess
                await self._unwind_hl(excess_fill)

            # Compute captured spread
            if direction == "SHORT_HL_LONG_BB":
                captured = (fill["fill_price"] - bb_result.avg_price) / bb_result.avg_price * 10000
            else:
                captured = (bb_result.avg_price - fill["fill_price"]) / fill["fill_price"] * 10000

            pos = MakerPosition(
                position_id=f"mkr_{uuid.uuid4().hex[:8]}",
                pair=pair,
                coin=fill["coin"],
                direction=direction,
                state=MakerState.OPEN,
                target_qty=actual_qty,
                hl_fill_price=fill["fill_price"],
                hl_fill_qty=fill["fill_qty"],
                hl_fill_time=fill["timestamp"],
                bb_fill_price=bb_result.avg_price,
                bb_fill_qty=bb_result.filled_qty,
                bb_fill_time=time.time(),
                signal_spread_bps=snap.best_spread_bps,
                captured_spread_bps=captured,
                entry_time=time.time(),
            )
            self._positions[pair] = pos

            open_msg = (
                f"POSITION OPEN {pair}: captured={captured:.1f}bp "
                f"HL@{fill['fill_price']} BB@{bb_result.avg_price} "
                f"qty={actual_qty} hedge={hedge_ms:.0f}ms"
            )
            logger.info(open_msg)
            self._notify_telegram(open_msg)
        else:
            fail_msg = (
                f"HEDGE FAILED {pair}: BB {bb_side} rejected "
                f"({bb_result.error}). Unwinding HL..."
            )
            logger.error(fail_msg)
            self._notify_telegram(f"ALERT: {fail_msg}")
            self.total_bb_hedge_failures += 1
            await self._unwind_hl(fill)

    async def _unwind_hl(self, fill: dict):
        """Unwind an HL position after hedge failure. FIX #4: proper price."""
        coin = fill["coin"]
        unwind_is_buy = not fill["is_buy"]
        qty = fill["fill_qty"]

        # Use aggressive price with proper rounding
        snap = self.price_feed.get_spread(fill["pair"])
        if snap:
            mid = (snap.hl_bid + snap.hl_ask) / 2
        else:
            mid = fill["fill_price"]

        price = _round_hl_price_aggressive(
            mid * (1.02 if unwind_is_buy else 0.98),
            is_buy=unwind_is_buy,
        )

        try:
            result = await asyncio.to_thread(
                self.hl_exchange.order,
                coin, unwind_is_buy, qty, price,
                {"limit": {"tif": "Ioc"}},
                True,  # reduce_only
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if statuses and "filled" in statuses[0]:
                logger.info(f"HL unwind OK: {coin} {statuses[0]['filled']}")
            elif statuses and "error" in statuses[0]:
                logger.error(f"HL UNWIND REJECTED {coin}: {statuses[0]['error']}. MANUAL INTERVENTION!")
            else:
                logger.warning(f"HL unwind unclear: {result}")
        except Exception as e:
            logger.error(f"HL UNWIND FAILED {coin}: {e}. MANUAL INTERVENTION NEEDED!")

    # ── Exit Logic ──────────────────────────────────────────────

    async def _exit_loop(self):
        """FIX #3: Monitor open positions and close on spread revert or timeout."""
        while not self._shutdown:
            try:
                for pair, pos in list(self._positions.items()):
                    if pos.state != MakerState.OPEN:
                        continue

                    rules = self.instrument_mgr.get_rules(pair)
                    if not rules:
                        continue

                    hold_s = time.time() - pos.entry_time

                    snap = self.price_feed.get_spread(pair)
                    if not snap or (time.time() - snap.ts) > 5.0:
                        # N4 FIX: don't exit on stale prices (unless timeout)
                        if snap and hold_s > self.max_hold_s:
                            logger.warning(f"EXIT {pair}: stale snap but timeout — forcing exit")
                        else:
                            continue

                    # M2 FIX: use direction-specific spread, not best_spread
                    # If we entered on HL_PREMIUM (sold HL), the relevant exit spread
                    # is spread_hl_over_bb_bps. When this drops, the arb has reverted.
                    if pos.direction == "SHORT_HL_LONG_BB":
                        dir_spread = snap.spread_hl_over_bb_bps
                    else:
                        dir_spread = snap.spread_bb_over_hl_bps

                    should_exit = False
                    exit_reason = ""

                    # Exit when direction spread collapses below 30% of captured
                    if dir_spread < pos.captured_spread_bps * 0.3:
                        should_exit = True
                        exit_reason = "SPREAD_REVERT"
                    # Stop loss: spread WIDENED beyond 2.5x entry (wrong direction)
                    elif dir_spread > pos.captured_spread_bps * 2.5 and pos.captured_spread_bps > 0:
                        should_exit = True
                        exit_reason = "STOP_LOSS"
                    elif hold_s > self.max_hold_s:
                        should_exit = True
                        exit_reason = "TIMEOUT"

                    if should_exit:
                        await self._execute_exit(pair, pos, rules, snap, exit_reason)

                await asyncio.sleep(self.exit_check_interval_s)
            except Exception as e:
                logger.error(f"Exit loop error: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _execute_exit(
        self, pair: str, pos: MakerPosition, rules: PairRules,
        snap: SpreadSnapshot, reason: str,
    ):
        """Close legs via taker IOC. C2 FIX: only close legs not yet exited."""
        pos.state = MakerState.EXITING
        pos.exit_retries += 1

        if pos.exit_retries > 10:
            logger.error(f"EXIT STUCK {pair}: {pos.exit_retries} retries. MANUAL CLOSE NEEDED!")
            self._notify_telegram(f"ALERT: EXIT STUCK {pair} after {pos.exit_retries} retries! ENGINE PAUSING.")
            # Stop retrying — leave in EXITING so exit_loop skips it and quote loop blocks
            pos.state = MakerState.EXITING
            self._shutdown = True  # stop all activity until manual intervention
            return

        logger.info(
            f"EXITING {pair}: {reason} (hold={time.time()-pos.entry_time:.0f}s, "
            f"retry={pos.exit_retries}, hl_done={pos.hl_exited}, bb_done={pos.bb_exited})"
        )

        # Determine exit sides
        if pos.direction == "SHORT_HL_LONG_BB":
            hl_buy = True
            bb_side = "Sell"
        else:
            hl_buy = False
            bb_side = "Buy"

        aggression_mult = self.config.order_aggression_bps / 10000

        # C2 FIX: Only close legs that haven't been closed yet
        hl_ok = pos.hl_exited  # already done from prior attempt
        bb_ok = pos.bb_exited

        if not pos.hl_exited:
            hl_price = snap.hl_ask if hl_buy else snap.hl_bid
            hl_price = hl_price * (1 + aggression_mult) if hl_buy else hl_price * (1 - aggression_mult)
            hl_price = _round_hl_price_aggressive(hl_price, is_buy=hl_buy)
            try:
                hl_result = await asyncio.to_thread(
                    self.hl_exchange.order,
                    pos.coin, hl_buy, pos.target_qty, hl_price,
                    {"limit": {"tif": "Ioc"}},
                    True,  # reduce_only
                )
                statuses = hl_result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses and "filled" in statuses[0]:
                    hl_ok = True
                    pos.hl_exited = True
                    pos.hl_exit_price = float(statuses[0]["filled"].get("avgPx", 0))
                    logger.info(f"EXIT HL OK {pair}: {statuses[0]['filled']}")
                else:
                    logger.warning(f"EXIT HL ISSUE {pair}: {statuses}")
            except Exception as e:
                logger.error(f"EXIT HL ERROR {pair}: {e}")

        if not pos.bb_exited:
            bb_price = snap.bb_bid if bb_side == "Sell" else snap.bb_ask
            bb_price = bb_price * (1 - aggression_mult) if bb_side == "Sell" else bb_price * (1 + aggression_mult)
            try:
                bb_result = await self.bb_api.place_ioc(
                    symbol=rules.bb_symbol,
                    side=bb_side,
                    qty=rules.format_bb_qty(rules.round_bb_qty(pos.target_qty)),
                    price=rules.format_bb_price(
                        rules.round_bb_price_buy(bb_price) if bb_side == "Buy"
                        else rules.round_bb_price_sell(bb_price)
                    ),
                    reduce_only=True,
                )
                if bb_result.success:
                    bb_ok = True
                    pos.bb_exited = True
                    pos.bb_exit_price = bb_result.avg_price
                    logger.info(f"EXIT BB OK {pair}: {bb_result.filled_qty} @ {bb_result.avg_price}")
                else:
                    logger.warning(f"EXIT BB ISSUE {pair}: {bb_result.error}")
            except Exception as e:
                logger.error(f"EXIT BB ERROR {pair}: {e}")

        pos.exit_time = time.time()
        pos.exit_reason = reason

        if hl_ok and bb_ok:
            pos.state = MakerState.FLAT
            hold_s = pos.exit_time - pos.entry_time

            # C5 FIX: compute PnL from ACTUAL entry AND exit fill prices
            hl_entry = pos.hl_fill_price
            hl_exit = pos.hl_exit_price or hl_entry  # fallback if not recorded
            bb_entry = pos.bb_fill_price
            bb_exit = pos.bb_exit_price or bb_entry

            if hl_entry > 0 and bb_entry > 0:
                if pos.direction == "SHORT_HL_LONG_BB":
                    # Entry: sold HL high, bought BB low
                    # Exit: bought HL low, sold BB high
                    entry_pnl = (hl_entry - bb_entry) * pos.target_qty
                    exit_pnl = (bb_exit - hl_exit) * pos.target_qty
                else:
                    entry_pnl = (bb_entry - hl_entry) * pos.target_qty
                    exit_pnl = (hl_exit - bb_exit) * pos.target_qty
                gross_pnl = entry_pnl + exit_pnl
                # Fee cost from actual fills
                fee_cost = self.config.position_usd * 16.76 / 10000
                pos.net_pnl_usd = gross_pnl - fee_cost
                self._daily_pnl_usd += pos.net_pnl_usd

            exit_msg = (
                f"EXIT COMPLETE {pair}: {reason} hold={hold_s:.0f}s "
                f"captured={pos.captured_spread_bps:.1f}bp "
                f"pnl=${pos.net_pnl_usd:+.4f} daily=${self._daily_pnl_usd:+.3f}"
            )
            logger.info(exit_msg)
            self._notify_telegram(exit_msg)
            self.total_exits += 1
            del self._positions[pair]
            self._pair_cooldowns[pair] = time.time() + self.config.reentry_cooldown_s
        else:
            pos.state = MakerState.OPEN  # back to OPEN for retry
            fail_msg = (
                f"EXIT PARTIAL {pair}: HL={'OK' if hl_ok else 'FAIL'} "
                f"BB={'OK' if bb_ok else 'FAIL'} — retry {pos.exit_retries}/10"
            )
            logger.error(fail_msg)
            self._notify_telegram(f"ALERT: {fail_msg}")

    # ── Quote Management ─────────────────────────────────────────

    async def _quote_loop(self):
        """Main quoting loop — place/update resting orders on HL."""
        cycle = 0
        while not self._shutdown:
            try:
                quoted = 0
                skipped_reasons: dict[str, int] = {}
                for pair in self.config.default_pairs:
                    result = await self._manage_quote(pair)
                    if result == "quoted":
                        quoted += 1
                        await asyncio.sleep(0.5)  # rate limit between placements
                    elif result:
                        skipped_reasons[result] = skipped_reasons.get(result, 0) + 1

                cycle += 1
                if cycle % 6 == 1:
                    # Exchange source of truth: query actual HL equity periodically
                    equity_str = ""
                    if cycle % 30 == 1:  # every ~2.5 min
                        equity = await self._check_exchange_equity()
                        equity_str = f" HL_equity=${equity:.2f}"
                    logger.info(
                        f"[cycle {cycle}] resting={len(self._resting_orders)} "
                        f"positions={len(self._positions)} exits={self.total_exits} "
                        f"fills={self.total_hl_fills} hedges={self.total_bb_hedges} "
                        f"daily_pnl=${self._daily_pnl_usd:+.2f} "
                        f"quoted={quoted} skips={dict(skipped_reasons)}{equity_str}"
                    )
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Quote loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _manage_quote(self, pair: str) -> Optional[str]:
        """Place or update resting ALO order for a pair."""
        # C5 FIX: check daily loss limit
        today = time.strftime("%Y-%m-%d")
        if today != self._daily_reset_date:
            self._daily_pnl_usd = 0.0
            self._daily_reset_date = today
        if self._daily_pnl_usd < -self.config.max_daily_loss_usd:
            return "daily_loss_limit"

        if pair in self._positions:
            return "has_position"
        if pair in self._pair_cooldowns and time.time() < self._pair_cooldowns[pair]:
            return "cooldown"
        if len(self._positions) >= self.max_concurrent:
            return "max_concurrent"
        if pair in self._requoting:  # FIX #8
            return "requoting"
        # FIX #7: limit total resting orders
        if pair not in self._resting_orders and len(self._resting_orders) >= self.max_resting_orders:
            return "max_resting"

        rules = self.instrument_mgr.get_rules(pair)
        if not rules:
            return "no_rules"

        snap = self.price_feed.get_spread(pair)
        if not snap or (time.time() - snap.ts) > 2.0:
            return "no_snap"

        thresholds = self.signal_engine.thresholds.get_thresholds(pair)
        if not thresholds:
            return "no_threshold"
        if not thresholds.get("viable", False):
            return "not_viable"

        p90 = thresholds["effective_entry"]
        hl_mid = (snap.hl_bid + snap.hl_ask) / 2

        # FIX #2: Use fixed historical direction bias, not current tick
        bias = PAIR_DIRECTION_BIAS.get(pair, "HL_PREMIUM")
        if bias == "HL_PREMIUM":
            is_buy = False  # rest sell on HL (sell high when HL spikes)
            quote_price = hl_mid * (1 + p90 / 10000)
        else:
            is_buy = True   # rest buy on HL (buy low when HL drops)
            quote_price = hl_mid * (1 - p90 / 10000)

        quote_price = _round_hl_price_aggressive(quote_price, is_buy=is_buy)

        # Check existing resting order
        existing = self._resting_orders.get(pair)
        if existing:
            mid_drift = abs(hl_mid - existing.mid_at_place) / existing.mid_at_place * 10000
            age = time.time() - existing.placed_at

            if mid_drift < self.requote_drift_bps and age < self.requote_interval_s:
                return "already_resting"

            # Cancel existing — FIX #8: mark as requoting
            self._requoting.add(pair)
            try:
                await asyncio.to_thread(self.hl_exchange.cancel, existing.coin, existing.oid)
                logger.info(f"Cancelled stale order {pair} (drift={mid_drift:.1f}bp, age={age:.0f}s)")
                del self._resting_orders[pair]
            except Exception as e:
                logger.warning(f"Cancel failed {pair}: {e} — keeping existing order")
                return "cancel_failed"
            finally:
                self._requoting.discard(pair)

        # Place new ALO order
        qty = rules.round_hl_qty(self.config.position_usd / hl_mid)
        if not rules.is_tradeable(qty, hl_mid):
            return "qty_too_small"

        try:
            result = await asyncio.to_thread(
                self.hl_exchange.order,
                rules.coin, is_buy, qty, quote_price,
                {"limit": {"tif": "Alo"}},
            )

            if result.get("status") == "ok":
                statuses = result["response"]["data"]["statuses"]
                if statuses and "resting" in statuses[0]:
                    oid = statuses[0]["resting"]["oid"]
                    self._resting_orders[pair] = RestingOrder(
                        oid=oid,
                        coin=rules.coin,
                        pair=pair,
                        is_buy=is_buy,
                        price=quote_price,
                        qty=qty,
                        placed_at=time.time(),
                        mid_at_place=hl_mid,
                    )
                    logger.info(
                        f"RESTING {pair}: {'BUY' if is_buy else 'SELL'} "
                        f"{qty} @ {quote_price} (mid={hl_mid:.5f}, offset={p90:.0f}bp)"
                    )
                    return "quoted"
                # FIX #6: ALO cannot cross — "filled" should not happen.
                # If it does, log as anomaly.
                elif statuses and "filled" in statuses[0]:
                    logger.error(
                        f"ALO CROSSED (should not happen) {pair}: {statuses[0]['filled']}. "
                        f"This indicates a pricing bug — our ALO price crossed the book."
                    )
                elif statuses and "error" in statuses[0]:
                    err = statuses[0]["error"]
                    if "Would cross" not in err and "Insufficient margin" not in err:
                        logger.info(f"ALO rejected {pair}: {err}")
        except Exception as e:
            logger.warning(f"ALO place failed {pair}: {e}")
        return None

    # ── Telegram + Exchange Source of Truth ────────────────────

    def _notify_telegram(self, text: str):
        """M7 FIX: Send Telegram notification (fire-and-forget)."""
        if not self.config.telegram_enabled:
            return
        try:
            token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            chat_id = self.config.telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
            if not token or not chat_id:
                return
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.run_in_executor(None, lambda: req_lib.post(
                    url, json={"chat_id": chat_id, "text": f"[HLBB Maker] {text}"},
                    timeout=5,
                ))
        except Exception as e:
            logger.warning(f"Telegram failed: {e}")

    async def _check_exchange_equity(self) -> float:
        """Query ACTUAL exchange equity (source of truth, not modeled PnL)."""
        try:
            # HL balance
            state = await asyncio.to_thread(
                self.hl_exchange.info.user_state, self.hl_address
            )
            hl_equity = float(state.get("marginSummary", {}).get("accountValue", 0))
            spot = await asyncio.to_thread(
                self.hl_exchange.info.spot_user_state, self.hl_address
            )
            hl_usdc = 0.0
            for b in spot.get("balances", []):
                if b.get("coin") == "USDC":
                    hl_usdc = float(b.get("total", 0))

            return hl_equity + hl_usdc
        except Exception as e:
            logger.error(f"Exchange equity check failed: {e}")
            return 0.0

    async def _startup_reconcile(self):
        """C6 FIX: Check for orphaned positions from prior crash. Refuse to start if found."""
        try:
            state = await asyncio.to_thread(
                self.hl_exchange.info.user_state, self.hl_address
            )
            open_positions = []
            for ap in state.get("assetPositions", []):
                pos = ap.get("position", {})
                sz = float(pos.get("szi", 0))
                if sz != 0:
                    open_positions.append(f"{pos.get('coin')}: {sz}")

            # C6 FIX: also check BB for orphaned positions
            bb_positions = await asyncio.to_thread(self.bb_api.get_positions)
            bb_orphans = []
            for p in bb_positions:
                sym = p.get("symbol", "")
                pair = sym[:len(sym)-4] + "-" + sym[len(sym)-4:]  # DYDXUSDT → DYDX-USDT
                if pair in self.config.default_pairs:
                    bb_orphans.append(f"{sym}: {p.get('side')} {p.get('size')}")

            all_orphans = open_positions + bb_orphans
            if all_orphans:
                logger.error(
                    f"STARTUP BLOCKED: orphaned positions found: "
                    f"HL={open_positions}, BB={bb_orphans}. Close manually before restarting."
                )
                raise RuntimeError(f"Orphaned positions: {all_orphans}")
            else:
                logger.info("Startup check: no orphaned positions on HL or BB. Clean start.")

            # Also cancel any stale resting orders from prior session
            hl_orders = await asyncio.to_thread(
                self.hl_exchange.info.open_orders, self.hl_address
            )
            if hl_orders:
                logger.warning(f"Found {len(hl_orders)} stale HL orders from prior session. Cancelling...")
                for o in hl_orders:
                    try:
                        await asyncio.to_thread(self.hl_exchange.cancel, o["coin"], o["oid"])
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        logger.warning(f"Cancel stale order failed: {e}")
                logger.info("Stale orders cleared.")

        except RuntimeError:
            raise  # re-raise the startup block
        except Exception as e:
            logger.error(f"Startup reconciliation FAILED: {e}. BLOCKING START — cannot verify clean state.")
            raise RuntimeError(f"Startup reconcile network failure: {e}")

    # ── Lifecycle ────────────────────────────────────────────────

    async def run(self):
        """Main entry point — starts all async loops."""
        logger.info("MakerArbEngine V2 starting...")
        logger.info(f"  Pairs: {len(self.config.default_pairs)}")
        logger.info(f"  Position size: ${self.config.position_usd}/side")
        logger.info(f"  Max resting orders: {self.max_resting_orders}")
        logger.info(f"  Max concurrent positions: {self.max_concurrent}")
        logger.info(f"  Requote drift: {self.requote_drift_bps}bp / interval: {self.requote_interval_s}s")
        logger.info(f"  Fee RT: 16.76bp (HL maker entry + BB taker exit)")

        # C6 FIX: check for existing positions from prior crash
        await self._startup_reconcile()

        # H5 FIX: wire price feed to signal engine for live threshold updates
        def _on_spread(snap: SpreadSnapshot):
            self.signal_engine.thresholds.update(snap.pair, snap.best_spread_bps)
        self.price_feed._on_spread = _on_spread

        await asyncio.gather(
            self._start_fill_monitor(),
            self._hedge_loop(),
            self._quote_loop(),
            self._exit_loop(),  # FIX #3: exit monitoring
        )

    async def shutdown(self):
        """FIX #5: Cancel all resting orders AND close open positions."""
        logger.info("MakerArbEngine shutting down...")
        self._shutdown = True

        # Cancel all resting orders FIRST (prevent new fills)
        for pair, resting in list(self._resting_orders.items()):
            try:
                await asyncio.to_thread(self.hl_exchange.cancel, resting.coin, resting.oid)
                logger.info(f"Cancelled resting order {pair}")
            except Exception as e:
                logger.warning(f"Cancel failed {pair}: {e}")
        self._resting_orders.clear()

        # C4 FIX: wait briefly for in-flight WS fill messages, then drain queue
        await asyncio.sleep(2)
        while not self._fill_queue.empty():
            try:
                fill = self._fill_queue.get_nowait()
                logger.info(f"Shutdown: processing queued fill for {fill['pair']}")
                await self._execute_bb_hedge(fill)
            except Exception as e:
                logger.error(f"Shutdown: failed to process queued fill: {e}")

        # C3 FIX: final reconciliation — detect any untracked positions
        await self._reconcile_missed_fills()

        # N12 FIX: close ALL positions (OPEN or EXITING state)
        for pair, pos in list(self._positions.items()):
            if pos.state in (MakerState.OPEN, MakerState.EXITING):
                rules = self.instrument_mgr.get_rules(pair)
                snap = self.price_feed.get_spread(pair)
                if rules and snap:
                    logger.info(f"Shutdown: closing position {pair} (state={pos.state.value})")
                    await self._execute_exit(pair, pos, rules, snap, "SHUTDOWN")
                else:
                    logger.error(f"Shutdown: cannot close {pair} — no rules/snap. MANUAL CLOSE NEEDED!")
                    self._notify_telegram(f"ALERT: ORPHANED POSITION {pair} on shutdown!")

        logger.info(
            f"MakerArbEngine shutdown complete. "
            f"Fills={self.total_hl_fills} Hedges={self.total_bb_hedges} "
            f"Exits={self.total_exits} HedgeFails={self.total_bb_hedge_failures}"
        )
