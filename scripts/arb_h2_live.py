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
from app.services.arb.instrument_rules import InstrumentRules

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

# ── Pair configuration ───────────────────────────────────────
# Mode: "usdc" uses Binance USDC pairs (MiCA-compliant for EU/NL)
#        "usdt" uses Binance USDT pairs (original H2, blocked for EU)
TRADING_MODE = "usdc"

# USDC mode: Binance USDC symbol -> Bybit USDT perp symbol
USDC_PAIR_MAP = {
    "NOMUSDT":    ("NOMUSDC",    "NOMUSDT"),
    "ENJUSDT":    ("ENJUSDC",    "ENJUSDT"),
    "MOVEUSDT":   ("MOVEUSDC",   "MOVEUSDT"),
    "KERNELUSDT": ("KERNELUSDC", "KERNELUSDT"),
    "ACXUSDT":    ("ACXUSDC",    "ACXUSDT"),
}

# USDT mode (original, for non-EU accounts)
USDT_PAIRS = ["HIGHUSDT", "NOMUSDT"]

PAIRS = list(USDC_PAIR_MAP.keys()) if TRADING_MODE == "usdc" else USDT_PAIRS
POSITION_USD = 10.0               # $10 per side — first live test
MAX_CONCURRENT = 3                # up to 3 concurrent positions
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

    def __init__(self, session: aiohttp.ClientSession, shadow: bool = False, on_shadow_fill=None):
        self._session = session
        self._shadow = shadow
        self._order_counter = 0
        self._on_shadow_fill = on_shadow_fill  # callback for simulated fills in shadow mode

        # Real API clients (only used in live mode)
        self.bybit_api = BybitOrderAPI(BYBIT_KEY, BYBIT_SECRET, session)
        self.binance_api = BinanceOrderAPI(BINANCE_KEY, BINANCE_SECRET, session)

    async def submit(
        self, venue: str, symbol: str, side: str, qty: float, price: float, order_type: str,
        client_order_id: str = "",
    ) -> str:
        """Submit an order. Returns order_id. Maps symbols for USDC mode."""
        self._order_counter += 1

        # Map symbol for Binance in USDC mode
        bn_symbol = symbol
        if venue == "binance" and TRADING_MODE == "usdc" and symbol in USDC_PAIR_MAP:
            bn_symbol = USDC_PAIR_MAP[symbol][0]  # e.g., "NOMUSDC"

        if self._shadow:
            actual_sym = bn_symbol if venue == "binance" else symbol
            oid = f"shadow_{venue}_{self._order_counter}"
            logger.info(f"[SHADOW] Simulated: {venue} {actual_sym} {side} {qty:.6f} @ {price:.6f} ({order_type}) -> {oid}")

            # Simulate realistic fill with small random slippage (1-5bp)
            import random
            slippage_bps = random.uniform(1, 5)
            slippage_mult = 1 + (slippage_bps / 10000) if side in ("Buy", "BUY") else 1 - (slippage_bps / 10000)
            sim_price = price * slippage_mult if price > 0 else 0

            # Simulate execution latency (50-200ms)
            latency_ms = random.uniform(50, 200)
            await asyncio.sleep(latency_ms / 1000)

            # Emit simulated fill event through the coordinator
            if self._on_shadow_fill:
                from app.services.arb.order_feed import FillEvent, FillSource
                fill = FillEvent(
                    venue=venue,
                    symbol=symbol,
                    order_id=oid,
                    exec_id=f"sim_{oid}",
                    side=side,
                    price=sim_price if sim_price > 0 else price,
                    qty=qty,
                    fee=qty * sim_price * 0.00055 if venue == "bybit" else qty * sim_price * 0.001,
                    fee_asset="USDT" if venue == "bybit" else "USDC",
                    timestamp_ms=int(time.time() * 1000),
                    local_ts=time.time(),
                    source=FillSource.WS,
                    is_maker=False,
                    order_status="Filled",
                    leaves_qty=0,
                )
                self._on_shadow_fill(fill)

            return oid

        if venue == "bybit":
            return await self.bybit_api.submit_order(symbol, side, qty, price, order_type, client_order_id)
        elif venue == "binance":
            return await self.binance_api.submit_order(bn_symbol, side, qty, price, order_type, client_order_id)
        else:
            raise ValueError(f"Unknown venue: {venue}")

    async def check_order(self, venue: str, symbol: str, order_id: str) -> dict:
        """REST fallback: query order status for fill confirmation when WS unavailable."""
        if self._shadow:
            return {"filled_qty": 0, "avg_price": 0, "status": "shadow"}

        bn_symbol = symbol
        if venue == "binance" and TRADING_MODE == "usdc" and symbol in USDC_PAIR_MAP:
            bn_symbol = USDC_PAIR_MAP[symbol][0]

        try:
            if venue == "bybit":
                orders = await self.bybit_api.get_open_orders(symbol)
                # If order is NOT in open orders, it's filled or cancelled
                for o in orders:
                    if o.get("orderId") == order_id:
                        return {"filled_qty": float(o.get("cumExecQty", 0)), "avg_price": float(o.get("avgPrice", 0)), "status": o.get("orderStatus", "")}
                # Not in open orders — check executions
                execs = await self.bybit_api.get_executions(limit=10)
                for e in execs:
                    if e.get("orderId") == order_id:
                        return {"filled_qty": float(e.get("execQty", 0)), "avg_price": float(e.get("execPrice", 0)), "status": "Filled"}
            elif venue == "binance":
                trades = await self.binance_api.get_my_trades(bn_symbol, limit=5)
                for t in trades:
                    if str(t.get("orderId")) == order_id:
                        return {"filled_qty": float(t.get("qty", 0)), "avg_price": float(t.get("price", 0)), "status": "Filled"}
        except Exception as e:
            logger.warning(f"check_order failed {venue} {order_id}: {e}")
        return {"filled_qty": 0, "avg_price": 0, "status": "unknown"}

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

        # Components — build symbol map for USDC mode
        bn_symbol_map = None
        if TRADING_MODE == "usdc":
            bn_symbol_map = {k: v[0] for k, v in USDC_PAIR_MAP.items()}  # {"NOMUSDT": "NOMUSDC", ...}
        self.price_feed = PriceFeed(PAIRS, bn_symbol_map=bn_symbol_map)
        self.signal_engine = SignalEngine(PAIRS, MONGO_URI)
        self.inventory = InventoryLedger(MONGO_URI)
        self.risk_manager = RiskManager()
        self.crash_recovery = CrashRecovery(MONGO_URI, self.inventory)
        self.instrument_rules = InstrumentRules()

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
            # Setup leg coordinator first (needed for shadow fill callback)
            self._coordinator = LegCoordinator(
                submit_order_fn=lambda *a, **kw: None,  # placeholder, replaced below
                cancel_order_fn=lambda *a, **kw: None,
            )

            # Shadow fill callback — routes simulated fills to the coordinator
            def shadow_fill_cb(fill):
                self._coordinator.on_fill(fill)

            # Setup order submission
            submitter = OrderSubmitter(
                session, shadow=self.shadow,
                on_shadow_fill=shadow_fill_cb if self.shadow else None,
            )

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

            # Wire real submit/cancel/check functions into coordinator
            self._coordinator._submit = submitter.submit
            self._coordinator._cancel = submitter.cancel
            self._coordinator._check_order = submitter.check_order

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

            # Load instrument rules (qty step sizes, tick sizes, min notional)
            bb_symbols = [USDC_PAIR_MAP[p][1] for p in PAIRS] if TRADING_MODE == "usdc" else PAIRS
            bn_symbols = [USDC_PAIR_MAP[p][0] for p in PAIRS] if TRADING_MODE == "usdc" else PAIRS
            await self.instrument_rules.load_all(session, bb_symbols, bn_symbols)

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
                    # If Binance WS is unavailable (EU/NL 410), allow trading with Bybit WS only
                    # — Binance fills will be confirmed via REST polling in LegCoordinator
                    feeds_ready = self.shadow or self.order_feed.bybit.is_connected
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
                # BUG FIX #7: Map internal symbols to actual Binance symbols (USDC mode)
                for sym in PAIRS:
                    bn_sym = USDC_PAIR_MAP[sym][0] if TRADING_MODE == "usdc" and sym in USDC_PAIR_MAP else sym
                    trades = await submitter.binance_api.get_my_trades(bn_sym, limit=20)
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

        # Calculate and round quantities to exchange step sizes
        bb_sym = USDC_PAIR_MAP[sym][1] if TRADING_MODE == "usdc" else sym
        bn_sym = USDC_PAIR_MAP[sym][0] if TRADING_MODE == "usdc" else sym

        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        # BUG FIX #2: Size both legs from ONE canonical base quantity
        # Use the average mid price to determine base qty, then round for each venue
        avg_price = (snap.bb_ask + snap.bn_bid) / 2 if snap.bb_ask > 0 and snap.bn_bid > 0 else snap.bb_ask or snap.bn_bid
        if avg_price <= 0:
            logger.warning(f"Zero price for {sym}, skipping")
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, 0)
            return
        canonical_qty = POSITION_USD / avg_price

        # Round to the MORE restrictive step size (so both venues can fill)
        qty_bb = bb_rules.round_qty(canonical_qty) if bb_rules else canonical_qty
        qty_bn = bn_rules.round_qty(canonical_qty) if bn_rules else canonical_qty
        # Use the SMALLER of the two as the hedge qty (ensures delta neutral)
        hedge_qty = min(qty_bb, qty_bn)
        qty_bb = hedge_qty
        qty_bn = hedge_qty

        # Check min qty and min notional
        if bb_rules and (not bb_rules.check_min_qty(qty_bb) or not bb_rules.check_notional(qty_bb, snap.bb_ask)):
            logger.warning(f"Bybit qty/notional check failed: {sym} qty={qty_bb} price={snap.bb_ask}")
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, raw_qty_bn)
            return
        if bn_rules and (not bn_rules.check_min_qty(qty_bn) or not bn_rules.check_notional(qty_bn, snap.bn_bid)):
            logger.warning(f"Binance qty/notional check failed: {sym} qty={qty_bn} price={snap.bn_bid}")
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, raw_qty_bn)
            return

        # Round prices to tick size
        price_bb = bb_rules.round_price(snap.bb_ask if direction == "BUY_BB_SELL_BN" else snap.bb_bid) if bb_rules else snap.bb_ask
        price_bn = bn_rules.round_price(snap.bn_bid if direction == "BUY_BB_SELL_BN" else snap.bn_ask) if bn_rules else snap.bn_bid

        bb_side = "Buy" if direction == "BUY_BB_SELL_BN" else "Sell"
        bn_side = "Sell" if direction == "BUY_BB_SELL_BN" else "Buy"

        logger.info(
            f"ENTRY SIGNAL: {sym} {direction} spread={snap.spread_bps:.1f}bp "
            f"P90={signal.threshold_p90:.1f} P25={signal.threshold_p25:.1f} "
            f"qty_bb={qty_bb} qty_bn={qty_bn}"
        )

        result = await self._coordinator.execute_entry(
            signal=snap,
            bb_side=bb_side, bn_side=bn_side,
            qty_bb=qty_bb, qty_bn=qty_bn,
            price_bb=price_bb,
            price_bn=price_bn,
        )

        if result.outcome == EntryOutcome.SUCCESS:
            # Register position for exit tracking — store ACTUAL fill quantities
            pos = OpenPosition(
                symbol=sym,
                direction=direction,
                entry_spread=snap.spread_bps,
                entry_time=time.time(),
                entry_threshold_p90=signal.threshold_p90,
                exit_threshold_p25=signal.threshold_p25,
                position_id=f"live_{sym}_{int(time.time()*1000)}",
            )
            # Store actual fill quantities on the position for correct exit sizing
            pos._bb_filled_qty = result.bybit_leg.filled_qty
            pos._bn_filled_qty = result.binance_leg.filled_qty
            pos._bb_order_id = result.bybit_leg.order_id
            pos._bn_order_id = result.binance_leg.order_id
            self.signal_engine.register_position(pos)

            # BUG FIX #6: Persist open position to MongoDB
            self._trades_coll.insert_one({
                "position_id": pos.position_id,
                "symbol": sym,
                "direction": direction,
                "status": "OPEN",
                "entry_time": pos.entry_time,
                "entry_spread": pos.entry_spread,
                "bb_order_id": result.bybit_leg.order_id,
                "bn_order_id": result.binance_leg.order_id,
                "bb_filled_qty": result.bybit_leg.filled_qty,
                "bn_filled_qty": result.binance_leg.filled_qty,
                "bb_fill_price": result.bybit_leg.avg_fill_price,
                "bn_fill_price": result.binance_leg.avg_fill_price,
                "actual_spread_bps": result.actual_spread_bps,
                "slippage_bps": result.slippage_bps,
            })

            # Update inventory with ACTUAL fill quantities (not intended)
            actual_bn_qty = result.binance_leg.filled_qty
            actual_bn_fee = result.binance_leg.fee
            fee_in_base = result.binance_leg.fee_asset not in ("USDT", "USDC", "BNB")
            if bn_side == "Sell":
                self.inventory.sold(sym, actual_bn_qty, actual_bn_fee, fee_in_base=fee_in_base)
            else:
                self.inventory.bought(sym, actual_bn_qty, actual_bn_fee)

            logger.info(
                f"ENTRY SUCCESS: {sym} actual_spread={result.actual_spread_bps:.1f}bp "
                f"slippage={result.slippage_bps:.1f}bp latency={result.entry_latency_ms:.0f}ms "
                f"bb_qty={result.bybit_leg.filled_qty} bn_qty={result.binance_leg.filled_qty}"
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

        is_stop_loss = signal.signal_type == "EXIT_STOP_LOSS"

        # BUG FIX #3: Use STORED fill quantities from entry, not recomputed from current price
        qty_bb = getattr(pos, '_bb_filled_qty', 0)
        qty_bn = getattr(pos, '_bn_filled_qty', 0)

        if qty_bb <= 0 or qty_bn <= 0:
            # Fallback: if stored qty not available, compute from current price (legacy)
            logger.warning(f"No stored fill qty for {sym}, falling back to price-based sizing")
            avg_price = (snap.bb_bid + snap.bn_ask) / 2 if snap.bb_bid > 0 else snap.bb_bid or snap.bn_ask
            qty_bb = POSITION_USD / avg_price if avg_price > 0 else 0
            qty_bn = qty_bb

        # Get symbols for price rounding
        bb_sym = USDC_PAIR_MAP[sym][1] if TRADING_MODE == "usdc" else sym
        bn_sym = USDC_PAIR_MAP[sym][0] if TRADING_MODE == "usdc" else sym
        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        price_bb = bb_rules.round_price(snap.bb_bid if bb_side == "Sell" else snap.bb_ask) if bb_rules else snap.bb_bid
        price_bn = bn_rules.round_price(snap.bn_ask if bn_side == "Buy" else snap.bn_bid) if bn_rules else snap.bn_ask

        # Execute exit via dedicated exit method
        result = await self._coordinator.execute_exit(
            symbol=sym,
            bb_side=bb_side, bn_side=bn_side,
            qty_bb=qty_bb, qty_bn=qty_bn,
            price_bb=price_bb, price_bn=price_bn,
            is_stop_loss=is_stop_loss,
        )

        # Update inventory (buy-back restores inventory)
        if result.outcome in ("success", "partial"):
            actual_bn_qty = result.binance_leg.filled_qty
            actual_bn_fee = result.binance_leg.fee
            if bn_side == "Buy" and actual_bn_qty > 0:
                self.inventory.bought(sym, actual_bn_qty, actual_bn_fee)
            elif bn_side == "Sell" and actual_bn_qty > 0:
                fee_in_base = result.binance_leg.fee_asset not in ("USDT", "USDC", "BNB")
                self.inventory.sold(sym, actual_bn_qty, actual_bn_fee, fee_in_base=fee_in_base)

            # Record trade in risk manager
            pnl = 0.0  # TODO: compute PnL from entry vs exit fill prices
            self.risk_manager.record_trade(pnl, 0, 0, leg_failure=(result.outcome == "partial"))

            # Update MongoDB position record
            pos_id = getattr(pos, 'position_id', '')
            if pos_id:
                self._trades_coll.update_one(
                    {"position_id": pos_id},
                    {"$set": {
                        "status": "CLOSED",
                        "close_reason": signal.signal_type,
                        "exit_time": time.time(),
                        "exit_bb_qty": result.bybit_leg.filled_qty,
                        "exit_bn_qty": result.binance_leg.filled_qty,
                        "exit_bb_price": result.bybit_leg.avg_fill_price,
                        "exit_bn_price": result.binance_leg.avg_fill_price,
                        "exit_spread_bps": result.actual_exit_spread_bps,
                        "exit_latency_ms": result.exit_latency_ms,
                    }},
                )

        # BUG FIX #10: Only unregister if BOTH legs confirmed closed
        if result.outcome == "success":
            self.signal_engine.unregister_position(sym)
        elif result.outcome == "failed":
            # Exit completely failed — keep position active, will retry next cycle
            logger.critical(f"EXIT FAILED for {sym} — position stays active, will retry")
            await tg_send(f"H2 CRITICAL: Exit failed for {sym}, retrying next cycle", session)
        else:
            # Partial — one leg closed, other may still be open
            # Unregister to prevent duplicate exit attempts, but log the concern
            logger.warning(f"EXIT PARTIAL for {sym} — unregistering but exchange may have residual")
            self.signal_engine.unregister_position(sym)
            await tg_send(f"H2 WARNING: Partial exit for {sym}, check exchange manually", session)

        safe_reason = signal.signal_type.replace("<", "&lt;").replace(">", "&gt;")
        await tg_send(
            f"H2 CLOSE {sym}: {safe_reason}\n"
            f"Outcome: {result.outcome} | Latency: {result.exit_latency_ms:.0f}ms",
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
        """Periodic status logging with clear, actionable format."""
        uptime_h = (time.time() - self._start_time) / 3600
        risk = self.risk_manager.status()
        mode = "SHADOW" if self.shadow else "LIVE"

        # Header
        status_lines = [
            f"H2 {mode} ({uptime_h:.1f}h)",
            f"Trades: {risk['total_trades']} | PnL: ${risk['total_pnl_usd']:.2f} | Daily: ${risk['daily_pnl_usd']:.2f}",
        ]
        if risk['total_trades'] > 0:
            status_lines.append(f"Slippage: {risk['avg_slippage_bps']:.1f}bp | Leg fails: {risk['leg_failures']}")

        # Per-pair status: feed health + signal + current spread
        feed_status = self.price_feed.status()
        sig_status = self.signal_engine.status()

        status_lines.append("")
        for sym in PAIRS:
            fs = feed_status.get(sym, {})
            ss = sig_status["pairs"].get(sym, {})

            # Display name: show base asset + venues, not internal USDT key
            base = sym.replace("USDT", "")
            if TRADING_MODE == "usdc" and sym in USDC_PAIR_MAP:
                display = f"{base} (BB perp + BN USDC)"
            else:
                display = f"{base} (BB perp + BN USDT)"

            # Feed health label
            bb_age = fs.get("bb_age_ms", -1)
            bn_age = fs.get("bn_age_ms", -1)
            if bb_age < 0 or bn_age < 0:
                health = "NO DATA"
            elif bb_age > 5000 or bn_age > 5000:
                health = "STALE"
            elif bb_age > 2000 or bn_age > 2000:
                health = "SLOW"
            else:
                health = "OK"

            # Current spread
            snap = self.price_feed.get_spread(sym)
            spread_str = f"{snap.spread_bps:.0f}bp" if snap else "n/a"

            # Threshold info
            if ss.get("ready"):
                p90 = ss.get("p90", 0)
                p25 = ss.get("p25", 0)
                viable = ss.get("viable", False)
                has_pos = ss.get("has_position", False)

                # Status emoji
                if has_pos:
                    icon = "OPEN"
                elif viable and health == "OK":
                    icon = "READY"
                elif viable:
                    icon = "VIABLE"
                else:
                    icon = "QUIET"

                status_lines.append(
                    f"  {display}: [{icon}] spread={spread_str} P90={p90:.0f} P25={p25:.0f} feed={health}"
                )
            else:
                status_lines.append(f"  {display}: [WARMING] feed={health} ({fs.get('bb_updates', 0)}+{fs.get('bn_updates', 0)} updates)")

        # Inventory summary
        inv = self.inventory.status()
        low_inv = [s for s, v in inv.items() if not v["healthy"]]
        if low_inv:
            status_lines.append(f"\nInventory warning: {', '.join(low_inv)}")

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
