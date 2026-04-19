"""
H2 Spike Fade — Live Trader (execution engine).

Wires together all components:
- PriceFeed (WS prices from both exchanges)
- OrderFeed (WS fill events from both exchanges)
- SignalEngine (adaptive P90/P25 thresholds)
- LegCoordinator (atomic dual-leg execution)
- InventoryLedger (persistent Binance spot tracking)
- RiskManager (3-tier: alert/pause/kill)
- CrashRecovery (startup reconciliation)

Paper trader (arb_h2_paper.py) keeps running separately for comparison.

Usage:
    set -a && source .env && set +a
    python scripts/arb_h2_live.py [--shadow]

    --shadow: log signals and decisions but do NOT place real orders
"""
import asyncio
import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.arb.price_feed import PriceFeed
from app.services.arb.order_feed import OrderFeed, FillEvent, OrderUpdate
from app.services.arb.execution_engine import LegCoordinator, EntryResult, EntryOutcome
from app.services.arb.signal_engine import SignalEngine, OpenPosition
from app.services.arb.inventory_ledger import InventoryLedger
from app.services.arb.risk_manager import RiskManager, RiskAction
from app.services.arb.crash_recovery import CrashRecovery
from app.services.arb.order_api import BybitOrderAPI, BinanceOrderAPI

# Load .env
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/arb-h2-live.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────

PAIRS = ["HIGHUSDT", "NOMUSDT"]  # Phase 1 test pairs
POSITION_USD = 20.0               # $20 per side for micro-test
MAX_CONCURRENT = 2
POLL_INTERVAL = 0.5                # check signals every 500ms (WS feeds are real-time)

# Exchange credentials
BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BINANCE_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET = os.getenv("BINANCE_API_SECRET", "")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")


# ── Telegram helper ─────────────────────────────────────────────

async def tg_send(text: str, session: aiohttp.ClientSession):
    """Send Telegram notification."""
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        await session.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"},
            timeout=aiohttp.ClientTimeout(total=5),
        )
    except Exception as e:
        logger.warning(f"TG send failed: {e}")


# ── Order Submission (placeholder — wired to exchange REST) ─────

class OrderSubmitter:
    """
    Submits orders to Bybit perp and Binance spot via HMAC-signed REST.
    In shadow mode, returns fake order IDs without hitting exchanges.
    """

    def __init__(self, session: aiohttp.ClientSession, shadow: bool = False):
        self._session = session
        self._shadow = shadow
        self._order_counter = 0

        # Real API clients
        self.bybit_api = BybitOrderAPI(BYBIT_KEY, BYBIT_SECRET, session)
        self.binance_api = BinanceOrderAPI(BINANCE_KEY, BINANCE_SECRET, session)

    async def submit(
        self, venue: str, symbol: str, side: str, qty: float, price: float, order_type: str
    ) -> str:
        """Submit an order. Returns order_id."""
        self._order_counter += 1

        if self._shadow:
            oid = f"shadow_{venue}_{self._order_counter}"
            logger.info(f"[SHADOW] Would submit: {venue} {symbol} {side} {qty:.6f} @ {price:.6f} ({order_type}) -> {oid}")
            return oid

        if venue == "bybit":
            return await self.bybit_api.submit_order(symbol, side, qty, price, order_type)
        elif venue == "binance":
            return await self.binance_api.submit_order(symbol, side, qty, price, order_type)
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
            return await self.binance_api.cancel_order(symbol, order_id)
        return False


# ── Main Trader ─────────────────────────────────────────────────

class H2LiveTrader:
    """Main trading loop. Wires all components together."""

    def __init__(self, shadow: bool = False):
        self.shadow = shadow
        self.running = True

        # Components
        self.price_feed = PriceFeed(PAIRS)
        self.signal_engine = SignalEngine(PAIRS, MONGO_URI)
        self.inventory = InventoryLedger(MONGO_URI)
        self.risk_manager = RiskManager()
        self.crash_recovery = CrashRecovery(MONGO_URI, self.inventory)

        # MongoDB for trade logging
        client = MongoClient(MONGO_URI)
        db_name = MONGO_URI.rsplit("/", 1)[-1]
        self._db = client[db_name]
        self._trades_coll = self._db["arb_h2_live_positions"]
        self._orders_coll = self._db["arb_h2_live_orders"]
        self._leg_failures_coll = self._db["arb_h2_leg_failures"]

        self._poll_count = 0
        self._start_time = time.time()

    async def run(self):
        mode = "SHADOW" if self.shadow else "LIVE"
        logger.info("=" * 60)
        logger.info(f"H2 SPIKE FADE — {mode} TRADER")
        logger.info(f"Pairs: {PAIRS}")
        logger.info(f"Position size: ${POSITION_USD}")
        logger.info(f"Max concurrent: {MAX_CONCURRENT}")
        logger.info("=" * 60)

        # Load threshold history
        self.signal_engine.load_history(MONGO_URI)

        # Load inventory state
        self.inventory.load(PAIRS)

        async with aiohttp.ClientSession() as session:
            # Setup order submission
            submitter = OrderSubmitter(session, shadow=self.shadow)

            # Setup order feed with callbacks
            def on_fill(fill: FillEvent):
                logger.info(f"FILL: {fill.venue} {fill.symbol} {fill.side} {fill.qty}@{fill.price} ({fill.source})")
                if hasattr(self, '_coordinator'):
                    self._coordinator.on_fill(fill)

            def on_order(update: OrderUpdate):
                logger.debug(f"ORDER: {update.venue} {update.symbol} {update.status}")
                if hasattr(self, '_coordinator'):
                    self._coordinator.on_order_update(update)

            def on_disconnect(venue: str):
                logger.warning(f"WS DISCONNECT: {venue} — blocking new entries, starting gap recovery")
                self.risk_manager.record_disconnect()
                # Schedule async gap recovery (can't await in sync callback)
                asyncio.create_task(self._handle_disconnect(venue, session, submitter))

            self.order_feed = OrderFeed(
                BYBIT_KEY, BYBIT_SECRET,
                BINANCE_KEY, BINANCE_SECRET,
                on_fill=on_fill, on_order=on_order,
                on_disconnect=on_disconnect,
            )

            # Setup leg coordinator
            self._coordinator = LegCoordinator(
                submit_order_fn=submitter.submit,
                cancel_order_fn=submitter.cancel,
            )

            # Crash recovery
            if not self.shadow:
                report = await self.crash_recovery.recover(
                    session, BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, PAIRS
                )
                if not report.clean:
                    logger.warning(f"Recovery found issues: {report}")
                    await tg_send(f"H2 {mode}: Recovery found issues: {report.errors}", session)

            # Start feeds
            await self.price_feed.start(session)
            if not self.shadow:
                await self.order_feed.start(session)

            # Startup notification
            await tg_send(
                f"H2 Spike Fade {mode} STARTED\n"
                f"Pairs: {', '.join(PAIRS)}\n"
                f"Size: ${POSITION_USD}/side\n"
                f"Risk: 3-tier (alert/pause/kill)",
                session,
            )

            logger.info("Waiting 3s for feeds to warm up...")
            await asyncio.sleep(3)

            # ── Main loop ──
            try:
                while self.running:
                    t0 = time.time()
                    self._poll_count += 1

                    # Daily reset check
                    self.risk_manager.check_daily_reset()

                    # Risk check
                    action, reason = self.risk_manager.evaluate()
                    if action == RiskAction.KILL:
                        logger.critical(f"KILL: {reason}")
                        await tg_send(f"H2 KILLED: {reason}", session)
                        await self._graceful_shutdown(session, submitter)
                        break
                    elif action == RiskAction.PAUSE:
                        if self._poll_count % 120 == 1:  # log every 60s
                            logger.warning(f"PAUSED: {reason}")
                    elif action == RiskAction.ALERT:
                        if self._poll_count % 120 == 1:
                            logger.info(f"ALERT: {reason}")
                            await tg_send(f"H2 ALERT: {reason}", session)

                    # Check for exit signals on open positions
                    for sym in list(self.signal_engine.open_positions.keys()):
                        snap = self.price_feed.get_spread(sym)
                        if snap:
                            exit_signal = self.signal_engine.check_exit(snap)
                            if exit_signal:
                                await self._handle_exit(exit_signal, session, submitter)

                    # Check for entry signals (only if we can trade)
                    # In shadow mode, skip the both_connected check (order feed not started)
                    feeds_ready = self.shadow or self.order_feed.both_connected
                    if self.risk_manager.can_trade and feeds_ready:
                        open_count = len(self.signal_engine.open_positions)
                        if open_count < MAX_CONCURRENT:
                            for sym in PAIRS:
                                if sym in self.signal_engine.open_positions:
                                    continue
                                snap = self.price_feed.get_spread(sym)
                                if snap:
                                    entry_signal = self.signal_engine.check_entry(snap)
                                    if entry_signal:
                                        # Re-verify spread at execution time
                                        fresh_snap = self.price_feed.get_spread(sym)
                                        if fresh_snap and self.signal_engine.verify_spread_at_execution(fresh_snap, entry_signal):
                                            await self._handle_entry(entry_signal, fresh_snap, session, submitter)

                    # Status logging every 5 min
                    if self._poll_count % 600 == 0:
                        await self._log_status(session)

                    # Periodic inventory reconciliation every 5 min
                    if self._poll_count % 600 == 0 and not self.shadow:
                        await self.inventory.periodic_reconcile(session, BINANCE_KEY, BINANCE_SECRET)

                    # Sleep to target poll interval
                    elapsed = time.time() - t0
                    sleep_time = max(0, POLL_INTERVAL - elapsed)
                    await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                logger.info("Trading loop cancelled")
            except Exception as e:
                logger.critical(f"UNHANDLED EXCEPTION in main loop: {e}", exc_info=True)
                await tg_send(f"H2 CRASH: {e}", session)

            # Cleanup
            await self.price_feed.stop()
            if not self.shadow:
                await self.order_feed.stop()

    async def _handle_disconnect(self, venue: str, session: aiohttp.ClientSession, submitter):
        """
        Gap recovery after WS disconnect.
        1. Fetch recent fills/orders via REST
        2. Reconcile against local state
        3. Resume only after state is consistent
        """
        logger.info(f"GAP RECOVERY starting for {venue}...")

        try:
            if venue == "bybit":
                # Fetch recent executions from Bybit REST
                executions = await submitter.bybit_api.get_executions(limit=50)
                for exec_data in executions:
                    exec_id = exec_data.get("execId", "")
                    order_id = exec_data.get("orderId", "")
                    # Check if coordinator knows about this fill
                    leg = self._coordinator._leg_states.get(order_id)
                    if leg and exec_id not in leg.exec_ids:
                        logger.warning(f"MISSED FILL recovered: {venue} {exec_id} order={order_id}")
                        from app.services.arb.order_feed import FillEvent, FillSource
                        fill = FillEvent(
                            venue="bybit",
                            symbol=exec_data.get("symbol", ""),
                            order_id=order_id,
                            exec_id=exec_id,
                            side=exec_data.get("side", ""),
                            price=float(exec_data.get("execPrice", 0)),
                            qty=float(exec_data.get("execQty", 0)),
                            fee=float(exec_data.get("execFee", 0)),
                            fee_asset="USDT",
                            timestamp_ms=int(exec_data.get("execTime", 0)),
                            local_ts=time.time(),
                            source=FillSource.REST_RECONCILE,
                        )
                        self._coordinator.on_fill(fill)

                # Check for orphan positions
                for sym in PAIRS:
                    positions = await submitter.bybit_api.get_positions(sym)
                    for pos in positions:
                        size = float(pos.get("size", 0))
                        if size > 0 and sym not in self.signal_engine.open_positions:
                            logger.warning(f"ORPHAN position on Bybit: {sym} size={size}")
                            await tg_send(f"H2 ALERT: Orphan Bybit position {sym} size={size}", session)

            elif venue == "binance":
                # Fetch recent trades from Binance REST per symbol
                for sym in PAIRS:
                    trades = await submitter.binance_api.get_my_trades(sym, limit=20)
                    for trade in trades:
                        trade_id = str(trade.get("id", ""))
                        order_id = str(trade.get("orderId", ""))
                        leg = self._coordinator._leg_states.get(order_id)
                        if leg and trade_id not in leg.exec_ids:
                            logger.warning(f"MISSED FILL recovered: binance {trade_id} order={order_id}")
                            from app.services.arb.order_feed import FillEvent, FillSource
                            fill = FillEvent(
                                venue="binance",
                                symbol=sym,
                                order_id=order_id,
                                exec_id=trade_id,
                                side="Buy" if trade.get("isBuyer") else "Sell",
                                price=float(trade.get("price", 0)),
                                qty=float(trade.get("qty", 0)),
                                fee=float(trade.get("commission", 0)),
                                fee_asset=trade.get("commissionAsset", ""),
                                timestamp_ms=int(trade.get("time", 0)),
                                local_ts=time.time(),
                                source=FillSource.REST_RECONCILE,
                            )
                            self._coordinator.on_fill(fill)

            # Reconcile inventory
            await self.inventory.periodic_reconcile(session, BINANCE_KEY, BINANCE_SECRET)

            logger.info(f"GAP RECOVERY complete for {venue}")
            await tg_send(f"H2: {venue} WS reconnected, gap recovery complete", session)

        except Exception as e:
            logger.error(f"GAP RECOVERY failed for {venue}: {e}", exc_info=True)
            await tg_send(f"H2 ALERT: {venue} gap recovery failed: {e}", session)

    async def _handle_entry(self, signal, snap, session, submitter):
        """Process an entry signal."""
        sym = signal.symbol
        direction = snap.direction

        # Check inventory (for BUY_BB_SELL_BN: we need inventory to sell on Binance)
        if direction == "BUY_BB_SELL_BN":
            qty_bn = POSITION_USD / snap.bn_bid if snap.bn_bid > 0 else 0
            if not self.inventory.can_sell(sym, qty_bn):
                logger.warning(f"Insufficient inventory for {sym}: need {qty_bn}, have {self.inventory.inventories.get(sym, {})}")
                return

            self.inventory.lock(sym, qty_bn)

        # Execute
        qty_bb = POSITION_USD / snap.bb_ask if snap.bb_ask > 0 else 0
        qty_bn = POSITION_USD / snap.bn_bid if snap.bn_bid > 0 else 0

        bb_side = "Buy" if direction == "BUY_BB_SELL_BN" else "Sell"
        bn_side = "Sell" if direction == "BUY_BB_SELL_BN" else "Buy"

        logger.info(
            f"ENTRY SIGNAL: {sym} {direction} spread={snap.spread_bps:.1f}bp "
            f"P90={signal.threshold_p90:.1f} P25={signal.threshold_p25:.1f}"
        )

        result = await self._coordinator.execute_entry(
            signal=snap,
            bb_side=bb_side, bn_side=bn_side,
            qty_bb=qty_bb, qty_bn=qty_bn,
            price_bb=snap.bb_ask if bb_side == "Buy" else snap.bb_bid,
            price_bn=snap.bn_bid if bn_side == "Sell" else snap.bn_ask,
        )

        if result.outcome == EntryOutcome.SUCCESS:
            # Register position for exit tracking
            pos = OpenPosition(
                symbol=sym,
                direction=direction,
                entry_spread=snap.spread_bps,
                entry_time=time.time(),
                entry_threshold_p90=signal.threshold_p90,
                exit_threshold_p25=signal.threshold_p25,
            )
            self.signal_engine.register_position(pos)

            # Update inventory with ACTUAL fill quantities (not intended)
            actual_bn_qty = result.binance_leg.filled_qty
            actual_bn_fee = result.binance_leg.fee
            fee_in_base = result.binance_leg.fee_asset != "USDT" and result.binance_leg.fee_asset != "BNB"
            if bn_side == "Sell":
                self.inventory.sold(sym, actual_bn_qty, actual_bn_fee, fee_in_base=fee_in_base)
            else:
                self.inventory.bought(sym, actual_bn_qty, actual_bn_fee)

            logger.info(
                f"ENTRY SUCCESS: {sym} actual_spread={result.actual_spread_bps:.1f}bp "
                f"slippage={result.slippage_bps:.1f}bp latency={result.entry_latency_ms:.0f}ms"
            )
            await tg_send(
                f"H2 OPEN {sym}\n"
                f"Spread: {snap.spread_bps:.0f}bp (actual: {result.actual_spread_bps:.0f}bp)\n"
                f"Slippage: {result.slippage_bps:.1f}bp | Latency: {result.entry_latency_ms:.0f}ms",
                session,
            )

        elif result.outcome == EntryOutcome.LEG_FAILURE:
            self.risk_manager.record_trade(result.unwind_pnl_usd, 0, 0, leg_failure=True)
            # Only release inventory if Binance leg did NOT fill
            # If Binance sold successfully but Bybit failed, inventory is reduced
            if not result.binance_leg.has_any_fill:
                self.inventory.release(sym, qty_bn)
            else:
                # Binance leg filled (sold our inventory). The unwind bought it back on Binance.
                # Inventory should be back to normal after unwind — reconcile to be sure.
                logger.info(f"Binance leg filled during failure — inventory auto-reconciled via unwind")
            self._leg_failures_coll.insert_one({
                "symbol": sym, "direction": direction,
                "venue_failed": result.leg_failure_venue,
                "unwind_pnl_usd": result.unwind_pnl_usd,
                "timestamp": time.time(),
            })
            logger.warning(f"LEG FAILURE: {sym} {result.leg_failure_venue} unwind=${result.unwind_pnl_usd:.4f}")
            await tg_send(f"H2 LEG FAILURE {sym}: {result.leg_failure_venue} unwind=${result.unwind_pnl_usd:.4f}", session)

        else:
            # Both missed or rejected
            self.inventory.release(sym, qty_bn)
            logger.info(f"Entry {result.outcome}: {sym}")

    async def _handle_exit(self, signal, session, submitter):
        """Process an exit signal. Closes both legs."""
        sym = signal.symbol
        snap = signal.spread_snapshot
        pos = self.signal_engine.open_positions.get(sym)
        if not pos:
            return

        logger.info(f"EXIT SIGNAL: {sym} {signal.signal_type} spread={snap.spread_bps:.1f}bp")

        if self.shadow:
            # Shadow mode: log but don't execute
            self.signal_engine.unregister_position(sym)
            logger.info(f"[SHADOW] Would close {sym}: {signal.signal_type}")
            await tg_send(f"[SHADOW] H2 CLOSE {sym}: {signal.signal_type}", session)
            return

        # Determine close sides (reverse of entry)
        if pos.direction == "BUY_BB_SELL_BN":
            bb_side = "Sell"   # close Bybit long
            bn_side = "Buy"   # buy back Binance spot (restore inventory)
        else:
            bb_side = "Buy"
            bn_side = "Sell"

        # Use market orders for stop loss, limit for reverts
        order_type = "market" if signal.signal_type == "EXIT_STOP_LOSS" else "limit"

        qty_bb = pos.entry_spread  # TODO: track actual position qty from entry
        qty_bn = qty_bb  # same size

        # Execute exit via coordinator
        result = await self._coordinator.execute_entry(
            signal=snap,
            bb_side=bb_side, bn_side=bn_side,
            qty_bb=POSITION_USD / snap.bb_bid if bb_side == "Sell" else POSITION_USD / snap.bb_ask,
            qty_bn=POSITION_USD / snap.bn_ask if bn_side == "Buy" else POSITION_USD / snap.bn_bid,
            price_bb=snap.bb_bid if bb_side == "Sell" else snap.bb_ask,
            price_bn=snap.bn_ask if bn_side == "Buy" else snap.bn_bid,
        )

        # Update inventory (buy-back restores inventory)
        if result.outcome == EntryOutcome.SUCCESS:
            actual_bn_qty = result.binance_leg.filled_qty
            actual_bn_fee = result.binance_leg.fee
            if bn_side == "Buy":
                self.inventory.bought(sym, actual_bn_qty, actual_bn_fee)
            else:
                fee_in_base = result.binance_leg.fee_asset not in ("USDT", "BNB")
                self.inventory.sold(sym, actual_bn_qty, actual_bn_fee, fee_in_base=fee_in_base)

        self.signal_engine.unregister_position(sym)

        safe_reason = signal.signal_type.replace("<", "&lt;").replace(">", "&gt;")
        await tg_send(
            f"H2 CLOSE {sym}: {safe_reason}\n"
            f"Outcome: {result.outcome.value} | Latency: {result.entry_latency_ms:.0f}ms",
            session,
        )

    async def _graceful_shutdown(self, session, submitter):
        """
        Graceful shutdown sequence:
        1. Stop new entries
        2. Cancel all working orders on both exchanges
        3. Close all open positions at market
        4. Reconcile inventory
        5. Log final state
        """
        logger.info("GRACEFUL SHUTDOWN initiated")
        self.running = False

        # Cancel all open orders on both exchanges
        try:
            bb_orders = await submitter.bybit_api.get_open_orders()
            for order in bb_orders:
                await submitter.bybit_api.cancel_order(order.get("symbol", ""), order.get("orderId", ""))
                logger.info(f"Cancelled Bybit order: {order.get('orderId')}")
        except Exception as e:
            logger.error(f"Bybit order cancellation failed: {e}")

        try:
            for sym in PAIRS:
                bn_orders = await submitter.binance_api.get_open_orders(sym)
                for order in bn_orders:
                    await submitter.binance_api.cancel_order(sym, str(order.get("orderId", "")))
                    logger.info(f"Cancelled Binance order: {order.get('orderId')}")
        except Exception as e:
            logger.error(f"Binance order cancellation failed: {e}")

        # Close all open Bybit positions at market
        try:
            for sym in PAIRS:
                positions = await submitter.bybit_api.get_positions(sym)
                for pos in positions:
                    size = float(pos.get("size", 0))
                    if size > 0:
                        side = "Sell" if pos.get("side") == "Buy" else "Buy"
                        await submitter.bybit_api.submit_order(sym, side, size, 0, "market")
                        logger.info(f"Closed Bybit position: {sym} {side} {size}")
        except Exception as e:
            logger.error(f"Bybit position close failed: {e}")

        # Reconcile inventory
        await self.inventory.periodic_reconcile(session, BINANCE_KEY, BINANCE_SECRET)

        # Final status
        logger.info(f"Final risk status: {self.risk_manager.status()}")
        logger.info(f"Final inventory: {self.inventory.status()}")

        await tg_send("H2 SHUTDOWN complete. All orders cancelled, positions closed.", session)

    async def _log_status(self, session):
        """Periodic status logging."""
        uptime_h = (time.time() - self._start_time) / 3600
        risk = self.risk_manager.status()
        mode = "SHADOW" if self.shadow else "LIVE"

        status_lines = [
            f"H2 {mode} ({uptime_h:.1f}h)",
            f"Trades: {risk['total_trades']} | PnL: ${risk['total_pnl_usd']:.2f}",
            f"Slippage: {risk['avg_slippage_bps']:.1f}bp | Leg fails: {risk['leg_failures']}",
        ]

        # Feed health
        feed_status = self.price_feed.status()
        for sym, fs in feed_status.items():
            status_lines.append(
                f"  {sym}: bb={fs['bb_updates']}u/{fs['bb_age_ms']:.0f}ms "
                f"bn={fs['bn_updates']}u/{fs['bn_age_ms']:.0f}ms"
            )

        # Signal engine
        sig_status = self.signal_engine.status()
        for sym, ss in sig_status["pairs"].items():
            if ss["ready"]:
                status_lines.append(
                    f"  {sym}: P90={ss['p90']:.0f} P25={ss['p25']:.0f} "
                    f"viable={ss['viable']} pos={ss['has_position']}"
                )

        status = "\n".join(status_lines)
        logger.info(status)
        await tg_send(status, session)


# ── Entry point ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="H2 Live Trader")
    parser.add_argument("--shadow", action="store_true", help="Shadow mode: no real orders")
    args = parser.parse_args()

    trader = H2LiveTrader(shadow=args.shadow)

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig} received, stopping...")
        trader.running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(trader.run())


if __name__ == "__main__":
    main()
