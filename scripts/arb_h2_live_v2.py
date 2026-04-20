"""
H2 Spike Fade V2 — Live Trader (thin orchestrator).

Wires together V2 components with ZERO business logic in this file.
All execution logic lives in EntryFlow/ExitFlow.
All state lives in PositionStore (MongoDB).
All fill detection lives in FillDetector.

Usage:
    set -a && source .env && set +a
    python scripts/arb_h2_live_v2.py [--shadow]
"""
import asyncio
import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.arb.position_store import PositionStore, PositionState
from app.services.arb.fill_detector import FillDetector
from app.services.arb.order_gateway import OrderGateway
from app.services.arb.entry_flow import EntryFlow, EntryResult
from app.services.arb.exit_flow import ExitFlow, ExitResult
from app.services.arb.price_feed import PriceFeed
from app.services.arb.order_feed import OrderFeed, FillEvent, OrderUpdate
from app.services.arb.signal_engine import SignalEngine, OpenPosition
from app.services.arb.inventory_ledger import InventoryLedger
from app.services.arb.risk_manager import RiskManager, RiskAction
from app.services.arb.crash_recovery_v2 import CrashRecoveryV2
from app.services.arb.instrument_rules import InstrumentRules
from app.services.arb.inventory_guard import InventoryImpairmentGuard

# Load .env
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key.strip(), val)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/arb-h2-v2.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────

TRADING_MODE = "usdc"

USDC_PAIR_MAP = {
    "NOMUSDT": ("NOMUSDC", "NOMUSDT"),
    "ENJUSDT": ("ENJUSDC", "ENJUSDT"),
    "MOVEUSDT": ("MOVEUSDC", "MOVEUSDT"),
    "KERNELUSDT": ("KERNELUSDC", "KERNELUSDT"),
}

PAIRS = list(USDC_PAIR_MAP.keys())
POSITION_USD = 10.0
MAX_CONCURRENT = 3
POLL_INTERVAL = 0.5

BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BINANCE_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET = os.getenv("BINANCE_API_SECRET", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")


# ── Telegram ────────────────────────────────────────────────────

async def tg_send(text: str, session: aiohttp.ClientSession):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        await session.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=5),
        )
    except Exception as e:
        logger.warning(f"TG send failed: {e}")


# ── Main Trader ─────────────────────────────────────────────────

class H2LiveTraderV2:
    """V2 thin orchestrator. Zero business logic."""

    def __init__(self, shadow: bool = False):
        self.shadow = shadow
        self.running = True
        self._poll_count = 0
        self._start_time = time.time()

        # Config
        bn_symbol_map = {k: v[0] for k, v in USDC_PAIR_MAP.items()}

        # Components
        self.price_feed = PriceFeed(PAIRS, bn_symbol_map=bn_symbol_map)
        self.signal_engine = SignalEngine(PAIRS, MONGO_URI)
        self.inventory = InventoryLedger(MONGO_URI)
        self.risk_manager = RiskManager()
        self.instrument_rules = InstrumentRules()
        self.position_store = PositionStore(MONGO_URI)
        self.inventory_guard = InventoryImpairmentGuard(impairment_threshold=3.0)

    async def run(self):
        mode = "SHADOW" if self.shadow else "LIVE"
        logger.info("=" * 60)
        logger.info(f"H2 V2 SPIKE FADE — {mode} TRADER")
        logger.info(f"Pairs: {PAIRS} | Size: ${POSITION_USD} | Max: {MAX_CONCURRENT}")
        logger.info("=" * 60)

        self.signal_engine.load_history(MONGO_URI)
        self.inventory.load(PAIRS)

        async with aiohttp.ClientSession() as session:
            # Build gateway
            gateway = OrderGateway(
                session, BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, BINANCE_SECRET,
                bn_symbol_map={k: v[0] for k, v in USDC_PAIR_MAP.items()},
                shadow=self.shadow,
            )

            # Build fill detector
            detector = FillDetector(
                get_order_fn=lambda venue, symbol, order_id="", client_order_id="":
                    gateway.get_order(venue, symbol, order_id=order_id, client_order_id=client_order_id)
            )

            # Wire WS fill callbacks to detector
            def on_fill(fill: FillEvent):
                detector.on_fill(fill)

            def on_order(update: OrderUpdate):
                detector.on_order_update(update)

            # Track WS availability per venue
            ws_available = {"bybit": True, "binance": False}

            def on_disconnect(venue: str):
                logger.warning(f"WS DISCONNECT: {venue}")
                ws_available[venue] = False
                self.risk_manager.record_disconnect()

            # Monitor WS reconnection — restore ws_available on reconnect
            async def _ws_health_monitor():
                while self.running:
                    await asyncio.sleep(10)
                    if hasattr(order_feed, 'bybit') and order_feed.bybit.is_connected:
                        if not ws_available.get("bybit"):
                            ws_available["bybit"] = True
                            logger.info("Bybit WS reconnected — fill detection restored")
                    if hasattr(order_feed, 'binance') and order_feed.binance.is_connected:
                        if not ws_available.get("binance"):
                            ws_available["binance"] = True
                            logger.info("Binance WS connected — switching from REST to WS fill detection")

            # Order feed (WS fills)
            order_feed = OrderFeed(
                BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, BINANCE_SECRET,
                on_fill=on_fill, on_order=on_order, on_disconnect=on_disconnect,
            )

            # Build flows
            entry_flow = EntryFlow(self.position_store, detector, gateway, ws_available)
            exit_flow = ExitFlow(self.position_store, detector, gateway, ws_available)

            # Crash recovery (V2: state-machine-aware)
            recovery = CrashRecoveryV2(self.position_store, gateway, self.inventory)
            if not self.shadow:
                report = await recovery.recover(PAIRS)
                if not report.clean:
                    await tg_send(f"H2 V2 {mode}: Recovery issues: {report.errors}", session)

                # Resume OPEN positions into signal engine
                for pos in await self.position_store.get_open():
                    sym = pos["symbol"]
                    entry = pos.get("entry", {})
                    opos = OpenPosition(
                        symbol=sym,
                        direction=pos.get("direction", "BUY_BB_SELL_BN"),
                        entry_spread=entry.get("actual_spread_bps", pos.get("signal_spread_bps", 0)),
                        entry_time=pos.get("entry_time", time.time()),
                        entry_threshold_p90=pos.get("threshold_p90", 0),
                        exit_threshold_p25=pos.get("threshold_p25", 0),
                        position_id=pos["position_id"],
                    )
                    self.signal_engine.register_position(opos)
                    logger.info(f"Position RESUMED: {sym} (from PositionStore)")

                # FIX (Round 4 #3): Also resume PARTIAL_EXIT positions into signal engine.
                # These block new entries but weren't being retried because signal_engine
                # didn't know about them. The main loop drives their retry via check_exit.
                for pos in await self.position_store.get_by_symbol_states([PositionState.PARTIAL_EXIT]):
                    sym = pos["symbol"]
                    if sym in self.signal_engine.open_positions:
                        continue  # Already registered (shouldn't happen, but guard)
                    entry = pos.get("entry", {})
                    opos = OpenPosition(
                        symbol=sym,
                        direction=pos.get("direction", "BUY_BB_SELL_BN"),
                        entry_spread=entry.get("actual_spread_bps", pos.get("signal_spread_bps", 0)),
                        entry_time=pos.get("entry_time", time.time()),
                        entry_threshold_p90=pos.get("threshold_p90", 0),
                        exit_threshold_p25=pos.get("threshold_p25", 0),
                        position_id=pos["position_id"],
                    )
                    self.signal_engine.register_position(opos)
                    logger.info(f"PARTIAL_EXIT RESUMED: {sym} (will retry in main loop)")

            # Start feeds
            await self.price_feed.start(session)
            if not self.shadow:
                await order_feed.start(session)

            # Load instrument rules
            bb_symbols = [USDC_PAIR_MAP[p][1] for p in PAIRS]
            bn_symbols = [USDC_PAIR_MAP[p][0] for p in PAIRS]
            await self.instrument_rules.load_all(session, bb_symbols, bn_symbols)

            # Execute deferred actions from recovery (orphan buybacks — Codex #13)
            # FIX: was checking hasattr(recovery, '_last_report') which is never set
            if not self.shadow and report.deferred_actions:
                for action in report.deferred_actions:
                    if action.get("action") == "orphan_buyback":
                        sym = action["symbol"]
                        qty = action["qty"]
                        bn_sym = USDC_PAIR_MAP.get(sym, (sym,))[0]
                        bn_rules = self.instrument_rules.get("binance", bn_sym)
                        snap = self.price_feed.get_spread_for_exit(sym)
                        side = action.get("side", "Buy")
                        if snap and bn_rules:
                            if side == "Buy":
                                price = bn_rules.round_price(snap.bn_ask * 1.001)
                            else:
                                price = bn_rules.round_price(snap.bn_bid * 0.999)
                            rounded_qty = bn_rules.round_qty(qty)
                            oid = await gateway.submit("binance", sym, side, rounded_qty, price, "limit")
                            # FIX (Round 4 #4): Wait for fill confirmation before updating inventory.
                            # Previous code called inventory.bought() immediately after submit,
                            # corrupting inventory if the order never filled.
                            try:
                                fill_result = await gateway.get_order_with_retry("binance", sym, order_id=oid)
                                if fill_result.get("fill_result") == "FILLED":
                                    fill_qty = float(fill_result.get("filled_qty", rounded_qty))
                                    fill_price = float(fill_result.get("avg_price", price))
                                    if side == "Buy":
                                        self.inventory.bought(sym, fill_qty, fill_price=fill_price)
                                    else:
                                        self.inventory.sold(sym, fill_qty, fill_price=fill_price)
                                    logger.info(f"Orphan {side.lower()}back CONFIRMED: {sym} {fill_qty} @ {fill_price}")
                                else:
                                    logger.warning(
                                        f"Orphan {side.lower()}back NOT FILLED: {sym} {rounded_qty} @ {price} "
                                        f"result={fill_result.get('fill_result')} — inventory NOT updated"
                                    )
                            except Exception as e:
                                logger.error(f"Orphan {side.lower()}back fill check failed: {sym}: {e} — inventory NOT updated")
                    elif action.get("action") == "reconcile_binance_exposure":
                        # FIX (Round 4 #5): Log deferred reconciliation actions
                        logger.critical(
                            f"DEFERRED RECONCILIATION: {action['symbol']} pid={action['position_id']} "
                            f"bn_qty={action['bn_qty']} — {action['reason']}"
                        )
                        await tg_send(
                            f"MANUAL ACTION NEEDED: {action['symbol']} has Binance exposure "
                            f"({action['bn_qty']} units) but Bybit is flat. Reconcile manually.",
                            session,
                        )

            await tg_send(
                f"H2 V2 {mode} STARTED\n"
                f"Pairs: {len(PAIRS)} | Size: ${POSITION_USD}/side | Max: {MAX_CONCURRENT}\n"
                f"Components: PositionStore + FillDetector + EntryFlow/ExitFlow",
                session,
            )

            # Start WS health monitor (restores ws_available after reconnect)
            ws_monitor_task = asyncio.create_task(_ws_health_monitor())

            # Seed inventory guard from inventory ledger
            for sym in PAIRS:
                inv = self.inventory.inventories.get(sym)
                if inv:
                    self.inventory_guard.register_pair(sym, inv.expected_qty, inv.cost_basis_usd)
                    logger.info(f"Inventory guard seeded: {sym} qty={inv.expected_qty:.1f} cost=${inv.cost_basis_usd:.2f}")

            logger.info("Waiting 3s for feeds to warm up...")
            await asyncio.sleep(3)

            # ── Main loop ──
            try:
                while self.running:
                    t0 = time.time()
                    self._poll_count += 1

                    self.risk_manager.check_daily_reset()
                    action, reason = self.risk_manager.evaluate()

                    if action == RiskAction.KILL:
                        logger.critical(f"KILL: {reason}")
                        await tg_send(f"H2 V2 KILLED: {reason}", session)
                        break

                    # Inventory impairment check every 5 minutes
                    if self._poll_count % 600 == 0:
                        await self._check_inventory_guard(session)

                    # Update thresholds
                    for sym in PAIRS:
                        snap = self.price_feed.get_spread(sym)
                        if snap:
                            self.signal_engine.update_thresholds(snap)

                    # Check exits on OPEN and PARTIAL_EXIT positions
                    # FIX #1: PARTIAL_EXIT must also be retried, not dropped
                    for sym in list(self.signal_engine.open_positions.keys()):
                        pos_doc = await self.position_store.get_by_symbol(sym, [PositionState.OPEN, PositionState.PARTIAL_EXIT])
                        if not pos_doc:
                            self.signal_engine.unregister_position(sym)
                            continue

                        direction = pos_doc[0].get("direction", "BUY_BB_SELL_BN")
                        # Use direction-aware spread for exit (Codex #6 fix)
                        snap = self.price_feed.get_spread_for_direction(sym, direction, for_exit=True)
                        if not snap:
                            snap = self.price_feed.get_spread_for_exit(sym)
                        # PARTIAL_EXIT: auto-retry without waiting for exit signal
                        if pos_doc[0].get("state") == PositionState.PARTIAL_EXIT:
                            if snap:
                                # Create a synthetic exit signal for the retry
                                from app.services.arb.signal_engine import SignalEvent
                                retry_signal = SignalEvent(
                                    symbol=sym, signal_type="EXIT_PARTIAL_RETRY",
                                    spread_snapshot=snap,
                                    threshold_p90=0, threshold_p25=0,
                                    threshold_median=0, excess_bps=0,
                                    timestamp=time.time(),
                                )
                                await self._handle_exit(
                                    retry_signal, pos_doc[0], session, gateway, exit_flow,
                                )
                        elif snap:
                            exit_signal = self.signal_engine.check_exit(snap)
                            if exit_signal:
                                await self._handle_exit(
                                    exit_signal, pos_doc[0], session, gateway, exit_flow,
                                )

                    # Check fill detector integrity (buffer overflow = RISK_PAUSE)
                    if detector.unmatched_buffer_overflow:
                        logger.critical("FILL DETECTOR BUFFER OVERFLOW — pausing all trading")
                        await tg_send("H2 V2 CRITICAL: Fill detector buffer overflow. Trading paused.", session)
                        self.risk_manager._paused = True  # Force pause

                    # Check entries
                    feeds_ready = self.shadow or ws_available.get("bybit", False)
                    if self.risk_manager.can_trade and feeds_ready and action != RiskAction.PAUSE:
                        active_count = await self.position_store.count_active()
                        if active_count < MAX_CONCURRENT:
                            for sym in PAIRS:
                                if sym in self.signal_engine.open_positions:
                                    continue
                                # Also check PositionStore (more authoritative)
                                # Block on ALL non-terminal states (including UNWINDING)
                                if await self.position_store.get_by_symbol(sym, [
                                    PositionState.OPEN, PositionState.ENTERING,
                                    PositionState.EXITING, PositionState.PARTIAL_EXIT,
                                    PositionState.UNWINDING, PositionState.PENDING,
                                ]):
                                    continue

                                snap = self.price_feed.get_spread(sym)
                                if snap:
                                    entry_signal = self.signal_engine.check_entry(snap)
                                    if entry_signal:
                                        fresh_snap = self.price_feed.get_spread(sym)
                                        if fresh_snap and self.signal_engine.verify_spread_at_execution(fresh_snap, entry_signal):
                                            await self._handle_entry(
                                                entry_signal, fresh_snap, session, gateway, entry_flow,
                                            )

                    # Periodic status
                    if self._poll_count % 600 == 0:
                        await self._log_status(session)

                    elapsed = time.time() - t0
                    await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

            except asyncio.CancelledError:
                logger.info("Trading loop cancelled")
            except Exception as e:
                logger.critical(f"UNHANDLED: {e}", exc_info=True)
                await tg_send(f"H2 V2 CRASH: {e}", session)

            await self.price_feed.stop()
            if not self.shadow:
                await order_feed.stop()

    async def _handle_entry(self, signal, snap, session, gateway, entry_flow: EntryFlow):
        """Process entry signal through EntryFlow."""
        sym = signal.symbol
        direction = snap.direction
        bb_sym = USDC_PAIR_MAP[sym][1]
        bn_sym = USDC_PAIR_MAP[sym][0]
        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        if not bb_rules or not bn_rules:
            return

        avg_price = (snap.bb_ask + snap.bn_bid) / 2
        if avg_price <= 0:
            return

        canonical_qty = POSITION_USD / avg_price
        qty_bb = bb_rules.round_qty(canonical_qty)
        qty_bn = bn_rules.round_qty(canonical_qty)
        hedge_qty = min(qty_bb, qty_bn)

        # Inventory check for sell-side
        if direction == "BUY_BB_SELL_BN":
            if not self.inventory.can_sell(sym, hedge_qty):
                return
            self.inventory.lock(sym, hedge_qty)
        elif direction == "BUY_BN_SELL_BB":
            # USDC balance check: need enough to buy on Binance
            needed_usdc = hedge_qty * (snap.bn_ask if snap.bn_ask > 0 else avg_price) * 1.002  # 0.2% buffer for fees
            try:
                usdc_balance = await gateway.binance_api.get_balance("USDC")
                if usdc_balance < needed_usdc:
                    logger.debug(f"{sym} skip entry: insufficient USDC for BUY_BN_SELL_BB (have ${usdc_balance:.2f}, need ${needed_usdc:.2f})")
                    return
            except Exception as e:
                logger.warning(f"{sym} USDC balance check failed: {e} — skipping entry")

        bb_side = "Buy" if direction == "BUY_BB_SELL_BN" else "Sell"
        bn_side = "Sell" if direction == "BUY_BB_SELL_BN" else "Buy"
        # FIX #7: Use side-aware rounding (buy=ceil, sell=floor) for better fill rates
        price_bb = bb_rules.round_price_for_side(
            snap.bb_ask if direction == "BUY_BB_SELL_BN" else snap.bb_bid, bb_side)
        price_bn = bn_rules.round_price_for_side(
            snap.bn_bid if direction == "BUY_BB_SELL_BN" else snap.bn_ask, bn_side)

        # Acquire per-symbol lock (covers entire entry flow)
        async with self.position_store.lock_for(sym):
            result = await entry_flow.execute(
                symbol=sym, bn_symbol=bn_sym, direction=direction,
                bb_side=bb_side, bn_side=bn_side,
                qty_bb=hedge_qty, qty_bn=hedge_qty,
                price_bb=price_bb, price_bn=price_bn,
                signal_spread_bps=snap.spread_bps,
                threshold_p90=signal.threshold_p90,
                threshold_p25=signal.threshold_p25,
            )

        if result.success:
            # Verify the position actually exists in OPEN state in PositionStore
            # before registering with signal engine (guard against phantom success)
            pos_check = await self.position_store.get_by_symbol(sym, [PositionState.OPEN])
            if not pos_check:
                logger.error(f"{sym} entry reported success but position not OPEN in DB — skipping registration")
                if direction == "BUY_BB_SELL_BN":
                    self.inventory.release(sym, hedge_qty)
                return

            # Register with signal engine for exit monitoring
            pos = OpenPosition(
                symbol=sym, direction=direction,
                entry_spread=result.actual_spread_bps,
                entry_time=time.time(),
                entry_threshold_p90=signal.threshold_p90,
                exit_threshold_p25=signal.threshold_p25,
                position_id=result.position_id,
            )
            self.signal_engine.register_position(pos)

            # Update inventory
            if bn_side == "Sell":
                fee_in_base = True  # Binance spot sell: fee in base asset
                self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price, fee_in_base=fee_in_base)
            else:
                self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)

            await tg_send(
                f"H2 V2 OPEN {sym.replace('USDT', '')}\n"
                f"Spread: {result.actual_spread_bps:.0f}bp | Slip: {result.slippage_bps:.1f}bp\n"
                f"Lat: {result.latency_ms:.0f}ms",
                session,
            )
        else:
            # Release inventory lock on failure
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, hedge_qty)

            if result.outcome == "LEG_FAILURE":
                self.risk_manager.record_trade(result.unwind_pnl_usd, 0, 0, leg_failure=True)

    async def _handle_exit(self, signal, pos_doc, session, gateway, exit_flow: ExitFlow):
        """Process exit signal through ExitFlow."""
        sym = signal.symbol
        pid = pos_doc["position_id"]
        direction = pos_doc.get("direction", "BUY_BB_SELL_BN")
        entry = pos_doc.get("entry", {})
        snap = signal.spread_snapshot

        bb_side = "Sell" if direction == "BUY_BB_SELL_BN" else "Buy"
        bn_side = "Buy" if direction == "BUY_BB_SELL_BN" else "Sell"
        is_stop_loss = signal.signal_type == "EXIT_STOP_LOSS"

        qty_bb = float(entry.get("bb", {}).get("filled_qty", 0))
        qty_bn = float(entry.get("bn", {}).get("filled_qty", 0))

        # Check if prior partial exit already closed some legs — use REMAINING qty
        # FIX #6: For PARTIAL legs, subtract already-filled exit qty from target
        exit_data = pos_doc.get("exit", {})
        bb_exit_state = exit_data.get("bb", {}).get("state", "")
        bn_exit_state = exit_data.get("bn", {}).get("state", "")

        if bb_exit_state == "FILLED":
            qty_bb = 0  # Fully closed
        elif bb_exit_state == "PARTIAL":
            # Subtract already-exited qty from the remaining target
            already_exited = float(exit_data.get("bb", {}).get("filled_qty", 0))
            qty_bb = max(0, qty_bb - already_exited)
        # else: no prior exit fills, use full entry qty

        if bn_exit_state == "FILLED":
            qty_bn = 0
        elif bn_exit_state == "PARTIAL":
            already_exited = float(exit_data.get("bn", {}).get("filled_qty", 0))
            qty_bn = max(0, qty_bn - already_exited)

        bb_sym = USDC_PAIR_MAP[sym][1]
        bn_sym = USDC_PAIR_MAP[sym][0]
        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        if bb_rules and qty_bb > 0:
            qty_bb = bb_rules.round_qty(qty_bb)
        if bn_rules and qty_bn > 0:
            qty_bn = bn_rules.round_qty(qty_bn)

        # Side-aware rounding for exits too
        price_bb = bb_rules.round_price_for_side(
            snap.bb_bid if bb_side == "Sell" else snap.bb_ask, bb_side) if bb_rules else snap.bb_bid
        price_bn = bn_rules.round_price_for_side(
            snap.bn_ask if bn_side == "Buy" else snap.bn_bid, bn_side) if bn_rules else snap.bn_ask

        # Acquire per-symbol lock
        async with self.position_store.lock_for(sym):
            result = await exit_flow.execute(
                position_id=pid, symbol=sym, direction=direction,
                bb_side=bb_side, bn_side=bn_side,
                qty_bb=qty_bb, qty_bn=qty_bn,
                price_bb=price_bb, price_bn=price_bn,
                exit_reason=signal.signal_type,
                is_stop_loss=is_stop_loss,
                position_usd=POSITION_USD,
            )

        if result.outcome == "SUCCESS":
            self.signal_engine.unregister_position(sym)
            # Update inventory (buy-back)
            if bn_side == "Buy" and result.bn_filled_qty > 0:
                self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
            elif bn_side == "Sell" and result.bn_filled_qty > 0:
                self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)

            self.risk_manager.record_trade(result.pnl_net_usd, 0, 0)
            self.inventory_guard.record_trade_capture(sym, result.pnl_net_usd)

            emoji = "+" if result.pnl_net_bps > 0 else ""
            await tg_send(
                f"H2 V2 CLOSE {sym.replace('USDT', '')}\n"
                f"Net: {emoji}{result.pnl_net_bps:.0f}bp (${result.pnl_net_usd:.3f})\n"
                f"{signal.signal_type} | Lat: {result.latency_ms:.0f}ms",
                session,
            )

        elif result.outcome == "PARTIAL":
            # Update inventory immediately for whichever leg filled (don't wait for full close)
            if hasattr(result, 'bn_filled_qty') and result.bn_filled_qty > 0:
                if bn_side == "Buy":
                    self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
                elif bn_side == "Sell":
                    self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
            await tg_send(f"H2 V2 PARTIAL EXIT {sym.replace('USDT', '')} — retrying next cycle", session)

        elif result.outcome == "FAILED":
            logger.warning(f"Exit failed for {sym}, will retry next cycle")

    async def _check_inventory_guard(self, session):
        """Mark to market and check if inventory impairment exceeds alpha."""
        prices = {}
        for sym in PAIRS:
            snap = self.price_feed.get_spread_for_exit(sym)
            if snap:
                prices[sym] = (snap.bn_bid + snap.bn_ask) / 2

        self.inventory_guard.mark_to_market(prices)
        should_pause, reason = self.inventory_guard.should_pause()
        if should_pause:
            logger.warning(f"INVENTORY GUARD: {reason}")
            await tg_send(f"H2 V2 INVENTORY GUARD: {reason}", session)
            # Don't auto-pause yet -- just alert. Alberto decides.

    async def _log_status(self, session):
        """Periodic status log."""
        uptime_h = (time.time() - self._start_time) / 3600
        active = await self.position_store.count_active()
        risk = self.risk_manager.status()
        guard = self.inventory_guard.status()

        logger.info(
            f"H2 V2 ({uptime_h:.1f}h) | Active: {active} | "
            f"Trades: {risk['total_trades']} | PnL: ${risk['total_pnl_usd']:.2f} | "
            f"Inv impairment: ${guard['total_impairment_usd']:.2f} | "
            f"Capture: ${guard['total_capture_usd']:.2f}"
        )


# ── Entry point ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="H2 V2 Live Trader")
    parser.add_argument("--shadow", action="store_true", help="Shadow mode: no real orders")
    args = parser.parse_args()

    trader = H2LiveTraderV2(shadow=args.shadow)

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig} received, stopping...")
        trader.running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(trader.run())


if __name__ == "__main__":
    main()
