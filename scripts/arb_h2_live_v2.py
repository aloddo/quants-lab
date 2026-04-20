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
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import numpy as np

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

TG_URL = f"https://api.telegram.org/bot{TG_TOKEN}"
_tg_last_update_id = 0


async def tg_send(text: str, session: aiohttp.ClientSession):
    """Send Telegram notification with HTML formatting."""
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        await session.post(
            f"{TG_URL}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=5),
        )
    except Exception as e:
        logger.warning(f"TG send failed: {e}")


async def tg_send_photo(path: str, caption: str, session: aiohttp.ClientSession):
    """Send image to Telegram."""
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        with open(path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("chat_id", TG_CHAT)
            data.add_field("caption", caption, content_type="text/plain")
            data.add_field("parse_mode", "HTML")
            data.add_field("photo", f, filename="equity_v2.png")
            await session.post(f"{TG_URL}/sendPhoto", data=data,
                               timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.debug(f"TG photo failed: {e}")


async def tg_poll_commands(session: aiohttp.ClientSession) -> str | None:
    """Check for /report, /status, /h2, /h2live commands."""
    global _tg_last_update_id
    if not TG_TOKEN:
        return None
    try:
        async with session.get(
            f"{TG_URL}/getUpdates",
            params={"offset": _tg_last_update_id + 1, "timeout": 0, "limit": 5},
            timeout=aiohttp.ClientTimeout(total=3),
        ) as resp:
            data = await resp.json()
        if not data.get("ok"):
            return None
        for update in data.get("result", []):
            _tg_last_update_id = update["update_id"]
            msg = update.get("message", {})
            text = msg.get("text", "").strip().lower()
            if text in ("/report", "/status", "/h2", "/h2live"):
                return text
        return None
    except Exception:
        return None


def render_equity_curve_live(closed_positions: list, path: str = "/tmp/h2_equity_v2.png"):
    """Render equity curve as PNG for V2 live trades."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        if not closed_positions:
            return None

        # Sort by close_time
        sorted_pos = sorted(closed_positions, key=lambda p: p.get("close_time", p.get("exit_time", 0)))
        times = []
        pnls = []
        for p in sorted_pos:
            ct = p.get("close_time", p.get("exit_time"))
            if not ct:
                continue
            if isinstance(ct, (int, float)):
                times.append(datetime.fromtimestamp(ct, tz=timezone.utc))
            else:
                times.append(ct.replace(tzinfo=timezone.utc) if ct.tzinfo is None else ct)
            # PnL from exit data or spread capture
            exit_data = p.get("exit", {})
            pnl_net = exit_data.get("pnl_net_usd", 0)
            if pnl_net == 0:
                # Fallback: compute from spread capture
                entry_s = p.get("entry", {}).get("actual_spread_bps", p.get("signal_spread_bps", 0)) or 0
                exit_s = exit_data.get("actual_spread_bps", 0) or 0
                capture_bps = entry_s - exit_s
                pnl_net = (capture_bps - 31) / 10000 * POSITION_USD * 2
            pnls.append(pnl_net)

        if not times or not pnls:
            return None

        cum_pnl = np.cumsum(pnls)

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.step(times, cum_pnl, where="post", color="#2196F3", linewidth=1.5)
        ax.fill_between(times, cum_pnl, step="post", alpha=0.15, color="#2196F3")
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")

        for i, (t, p) in enumerate(zip(times, pnls)):
            c = "#4CAF50" if p > 0 else "#F44336"
            ax.scatter([t], [cum_pnl[i]], color=c, s=20, zorder=5)

        ax.set_title("H2 Spike Fade V2 -- LIVE Trading Equity", fontsize=12)
        ax.set_ylabel("Cumulative PnL ($)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
        fig.autofmt_xdate()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(path, dpi=120)
        plt.close(fig)
        return path
    except Exception as e:
        logger.warning(f"Equity chart failed: {e}")
        return None


# ── Main Trader ─────────────────────────────────────────────────

class H2LiveTraderV2:
    """V2 thin orchestrator. Zero business logic."""

    def __init__(self, shadow: bool = False):
        self.shadow = shadow
        self.running = True
        self._poll_count = 0
        self._start_time = time.time()
        self._entry_reject_cooldown: dict[str, float] = {}  # sym -> next_allowed_log_time

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

        # MongoDB for V2 positions (used by report/equity chart)
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        db_name = MONGO_URI.rsplit("/", 1)[-1]
        self._db = client[db_name]
        self._positions_coll = self._db["arb_h2_positions_v2"]

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
            entry_flow = EntryFlow(self.position_store, detector, gateway, ws_available, shadow=self.shadow)
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

            # Startup notification with threshold info (like V1)
            sig_status = self.signal_engine.status()
            viable_lines = []
            for sym in PAIRS:
                ss = sig_status["pairs"].get(sym, {})
                if ss.get("ready"):
                    base = sym.replace("USDT", "")
                    v = "\u2705" if ss.get("viable") else "\u274c"
                    viable_lines.append(
                        f"  {v} {base}: P90={ss.get('p90',0):.0f} exit={ss.get('p25',0):.0f} "
                        f"ex={ss.get('p90',0)-ss.get('p25',0):.0f}bp"
                    )
            await tg_send(
                f"\U0001f680 <b>H2 V2 Spike Fade {mode} STARTED</b>\n"
                f"Pairs: {len(PAIRS)} monitored\n"
                f"Entry: P90 | Exit: P25 | Fees: ~31bp RT\n"
                f"Size: ${POSITION_USD}/side | Max: {MAX_CONCURRENT}\n"
                + ("\n".join(viable_lines) if viable_lines else "(warming up thresholds...)"),
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
                                # Skip pairs on entry rejection cooldown (avoid log spam)
                                if time.time() < self._entry_reject_cooldown.get(sym, 0):
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

                    # Telegram report every 30 min (3600 polls at 0.5s)
                    if self._poll_count % 3600 == 0:
                        await self._send_telegram_report(session)

                    # Poll for /report command every 30s (60 polls at 0.5s)
                    if self._poll_count % 60 == 0:
                        cmd = await tg_poll_commands(session)
                        if cmd in ("/report", "/status", "/h2", "/h2live"):
                            await self._send_telegram_report(session)

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
            now = time.time()
            if now >= self._entry_reject_cooldown.get(sym, 0):
                logger.critical(f"Missing instrument rules for {sym} (bb={bb_rules is not None}, bn={bn_rules is not None}) -- BLOCKED")
                self._entry_reject_cooldown[sym] = now + 600
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
                now = time.time()
                if now >= self._entry_reject_cooldown.get(sym, 0):
                    logger.warning(f"Insufficient inventory for {sym}: need {hedge_qty}")
                    self._entry_reject_cooldown[sym] = now + 300
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

            open_count = len(self.signal_engine.open_positions)
            base = sym.replace("USDT", "")
            excess = signal.threshold_p90 - signal.threshold_p25
            await tg_send(
                f"\U0001f4dd <b>H2 V2 OPEN</b> <code>{base}</code> [{open_count}/{MAX_CONCURRENT}]\n"
                f"Spread: <b>{result.actual_spread_bps:.0f}bp</b> (P90={signal.threshold_p90:.0f})\n"
                f"Exit@{signal.threshold_p25:.0f}bp | Excess: {excess:.0f}bp (fees~31bp)\n"
                f"Slip: {result.slippage_bps:.1f}bp | Lat: {result.latency_ms:.0f}ms\n"
                f"Size: ${POSITION_USD:.0f}/side | Dir: {direction}",
                session,
            )
        else:
            # Release inventory lock on failure
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, hedge_qty)

            if result.outcome == "LEG_FAILURE":
                self.risk_manager.record_trade(result.unwind_pnl_usd, 0, 0, leg_failure=True)
                base = sym.replace("USDT", "")
                consec = self.risk_manager.status().get('consecutive_leg_failures', '?')
                await tg_send(
                    f"\u26a0\ufe0f <b>H2 V2 LEG FAIL</b> <code>{base}</code>\n"
                    f"Unwind: ${result.unwind_pnl_usd:.4f}\n"
                    f"Consecutive: {consec}/3 (circuit breaker at 3)",
                    session,
                )

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

            base = sym.replace("USDT", "")
            emoji = "\u2705" if result.pnl_net_bps > 0 else "\u274c"
            # Get entry spread from pos_doc
            entry_spread = entry.get("actual_spread_bps", pos_doc.get("signal_spread_bps", 0)) or 0
            exit_spread = result.actual_exit_spread_bps if hasattr(result, 'actual_exit_spread_bps') else 0
            hold_s = time.time() - pos_doc.get("entry_time", time.time())
            safe_reason = signal.signal_type.replace("<", "&lt;").replace(">", "&gt;")
            await tg_send(
                f"\U0001f4dd <b>H2 V2 CLOSE</b> {emoji} <code>{base}</code>\n"
                f"Entry: {entry_spread:.0f}bp \u2192 Exit: {exit_spread:.0f}bp\n"
                f"Net: <b>{result.pnl_net_bps:+.0f}bp (${result.pnl_net_usd:.3f})</b>\n"
                f"Hold: {hold_s/60:.0f}min | {safe_reason}\n"
                f"Lat: {result.latency_ms:.0f}ms",
                session,
            )

        elif result.outcome == "PARTIAL":
            # Update inventory immediately for whichever leg filled (don't wait for full close)
            if hasattr(result, 'bn_filled_qty') and result.bn_filled_qty > 0:
                if bn_side == "Buy":
                    self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
                elif bn_side == "Sell":
                    self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
            await tg_send(
                f"\u26a0\ufe0f <b>H2 V2 PARTIAL EXIT</b> <code>{sym.replace('USDT', '')}</code> -- retrying next cycle",
                session,
            )

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

    def _get_feed_health(self, sym: str) -> str:
        """Get feed health label for a symbol."""
        fs = self.price_feed.status().get(sym, {})
        bb_age = fs.get("bb_age_ms", -1)
        bn_age = fs.get("bn_age_ms", -1)
        if bb_age < 0 or bn_age < 0:
            return "NO DATA"
        elif bb_age > 5000 or bn_age > 5000:
            return "STALE"
        elif bb_age > 2000 or bn_age > 2000:
            return "SLOW"
        return "OK"

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

    def _build_report_text(self) -> str:
        """Build full Telegram report text with V2 data sources."""
        uptime_h = (time.time() - self._start_time) / 3600
        risk = self.risk_manager.status()
        mode = "SHADOW" if self.shadow else "LIVE"
        sig_status = self.signal_engine.status()
        guard = self.inventory_guard.status()

        # Load closed trades from arb_h2_positions_v2
        closed = list(self._positions_coll.find({"state": "CLOSED"}).sort("close_time", -1))
        open_pos = list(self.signal_engine.open_positions.values())

        # Compute stats from closed trades
        total_trades = len(closed)
        wins = 0
        total_pnl = 0.0
        for t in closed:
            exit_data = t.get("exit", {})
            pnl_net = exit_data.get("pnl_net_usd", 0)
            if pnl_net == 0:
                # Fallback: compute from spread capture
                entry_s = t.get("entry", {}).get("actual_spread_bps", t.get("signal_spread_bps", 0)) or 0
                exit_s = exit_data.get("actual_spread_bps", 0) or 0
                cap = entry_s - exit_s
                net = cap - 31
                pnl_net = net / 10000 * POSITION_USD * 2
            total_pnl += pnl_net
            if pnl_net > 0:
                wins += 1
        wr = wins / total_trades if total_trades else 0

        can_trade = self.risk_manager.can_trade
        state_emoji = "\U0001f7e2" if can_trade else "\U0001f534"

        lines = [
            f"<b>H2 V2 Spike Fade {mode}</b> ({uptime_h:.1f}h) {state_emoji}",
            f"Closed: {total_trades} | WR: {wr:.0%} | PnL: <b>${total_pnl:.2f}</b>",
            f"Leg fails: {risk['leg_failures']} | Trading: {'YES' if can_trade else 'PAUSED'}",
        ]

        if closed:
            # Per-pair breakdown
            by_pair: dict[str, list[float]] = defaultdict(list)
            for t in closed:
                sym = t.get("symbol", "?")
                exit_data = t.get("exit", {})
                pnl_net = exit_data.get("pnl_net_usd", 0)
                if pnl_net == 0:
                    entry_s = t.get("entry", {}).get("actual_spread_bps", t.get("signal_spread_bps", 0)) or 0
                    exit_s = exit_data.get("actual_spread_bps", 0) or 0
                    net_bps = (entry_s - exit_s) - 31
                    pnl_net = net_bps / 10000 * POSITION_USD * 2
                by_pair[sym].append(pnl_net)
            lines.append("")
            for sym in sorted(by_pair):
                pp = by_pair[sym]
                base = sym.replace("USDT", "")
                w = sum(1 for x in pp if x > 0)
                pnl = sum(pp)
                lines.append(f"  {base}: {len(pp)} trades ${pnl:.3f} WR={w}/{len(pp)}")

        # Open positions with uPnL estimate
        if open_pos:
            lines.append("")
            lines.append("<b>Open:</b>")
            for pos in open_pos:
                sym = pos.symbol
                base = sym.replace("USDT", "")
                hold_m = (time.time() - pos.entry_time) / 60
                snap = self.price_feed.get_spread(sym)
                if not snap:
                    snap = self.price_feed.get_spread_for_exit(sym)
                if snap:
                    current_spread = snap.spread_bps
                    upnl_bps = pos.entry_spread - current_spread - 31
                    upnl_usd = upnl_bps / 10000 * POSITION_USD * 2
                    emoji = "\U0001f7e2" if upnl_bps > 0 else "\U0001f534"
                    stale_tag = "" if snap.fresh else " \u23f3"
                    bb_age_s = snap.bb_age_ms / 1000
                    bn_age_s = snap.bn_age_ms / 1000
                    age_str = f"age={max(bb_age_s, bn_age_s):.0f}s"
                    lines.append(
                        f"  {emoji} {base} {pos.entry_spread:.0f}bp now={current_spread:.0f}bp "
                        f"exit@{pos.exit_threshold_p25:.0f}bp "
                        f"uPnL={upnl_bps:+.0f}bp (${upnl_usd:+.3f}) {hold_m:.0f}m "
                        f"{age_str}{stale_tag}"
                    )
                else:
                    health = self._get_feed_health(sym)
                    lines.append(
                        f"  \u23f3 {base} {pos.entry_spread:.0f}bp now=? "
                        f"exit@{pos.exit_threshold_p25:.0f}bp "
                        f"feed={health} {hold_m:.0f}m (>30s stale)"
                    )

        # Current thresholds per pair
        lines.append("")
        lines.append("<b>Thresholds:</b>")
        for sym in PAIRS:
            ss = sig_status["pairs"].get(sym, {})
            if ss.get("ready"):
                base = sym.replace("USDT", "")
                p90 = ss.get("p90", 0)
                p25 = ss.get("p25", 0)
                excess = p90 - p25
                viable = ss.get("viable", False)
                health = self._get_feed_health(sym)
                v = "\u2705" if viable else "\u274c"
                h = "\U0001f7e2" if health == "OK" else ("\U0001f7e1" if health == "SLOW" else "\U0001f534")
                lines.append(
                    f"  {base}: P90={p90:.0f} P25={p25:.0f} ex={excess:.0f}bp [{v}] {h}"
                )

        # Inventory + cost basis + mark-to-market
        inv_status = self.inventory.status()
        pnl_summary = self.inventory.total_pnl_summary()
        lines.append("")
        lines.append("<b>Inventory:</b>")
        for sym in PAIRS:
            iv = inv_status.get(sym, {})
            base = sym.replace("USDT", "")
            avail = iv.get("available", 0)
            cost = iv.get("cost_basis_usd", 0)
            rpnl = iv.get("realized_pnl", 0)
            snap = self.price_feed.get_spread(sym)
            if snap and avail > 0:
                mid = (snap.bn_bid + snap.bn_ask) / 2
                mkt_val = avail * mid
                upnl = mkt_val - cost if cost > 0 else 0
                lines.append(
                    f"  {base}: {avail:.0f} (${mkt_val:.2f}) cost=${cost:.2f} "
                    f"uPnL=${upnl:+.2f} rPnL=${rpnl:+.4f}"
                )
            else:
                lines.append(f"  {base}: {avail:.0f} cost=${cost:.2f} rPnL=${rpnl:+.4f}")

        if pnl_summary["realized_pnl_usd"] != 0:
            lines.append(f"\nTotal realized PnL: <b>${pnl_summary['realized_pnl_usd']:+.4f}</b>")

        low_inv = [s.replace("USDT", "") for s, v in inv_status.items() if not v["healthy"]]
        if low_inv:
            lines.append(f"\u26a0\ufe0f Low inventory: {', '.join(low_inv)}")

        # Inventory guard status (impairment vs capture)
        lines.append("")
        lines.append("<b>Inventory Guard:</b>")
        imp = guard.get("total_impairment_usd", 0)
        cap = guard.get("total_capture_usd", 0)
        net_guard = cap - abs(imp)
        emoji_guard = "\U0001f7e2" if net_guard >= 0 else "\U0001f534"
        lines.append(
            f"  {emoji_guard} Impairment: ${imp:+.2f} | Capture: ${cap:+.2f} | Net: ${net_guard:+.2f}"
        )
        paused, pause_reason = self.inventory_guard.should_pause()
        if paused:
            lines.append(f"  \u26a0\ufe0f {pause_reason}")

        return "\n".join(lines)

    async def _send_telegram_report(self, session):
        """Send full report + equity curve chart to Telegram."""
        report = self._build_report_text()

        # Generate equity curve from closed trades in V2 collection
        closed = list(self._positions_coll.find({"state": "CLOSED"}).sort("close_time", 1))
        chart_path = render_equity_curve_live(closed) if closed else None

        if chart_path:
            await tg_send_photo(chart_path, f"\U0001f4ca {report}", session)
        else:
            await tg_send(f"\U0001f4ca {report}", session)


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
