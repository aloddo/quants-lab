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
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import numpy as np
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
    # ACX removed: 190 trades/24h, $4K volume = dead pair
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

_tg_last_update_id = 0

TG_URL = f"https://api.telegram.org/bot{TG_TOKEN}"


async def tg_send(text: str, session: aiohttp.ClientSession):
    """Send Telegram notification with HTML formatting."""
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        await session.post(
            f"{TG_URL}/sendMessage",
            json={
                "chat_id": TG_CHAT, "text": text,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            },
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
            data.add_field("photo", f, filename="equity_live.png")
            await session.post(f"{TG_URL}/sendPhoto", data=data,
                               timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.debug(f"TG photo failed: {e}")


async def tg_poll_commands(session: aiohttp.ClientSession) -> str | None:
    """Check for /report, /status, /h2live commands."""
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


def render_equity_curve_live(closed_positions: list, path: str = "/tmp/h2_equity_live.png"):
    """Render equity curve as PNG for live trades."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        if not closed_positions:
            return None

        # Sort by exit time
        sorted_pos = sorted(closed_positions, key=lambda p: p.get("exit_time", 0))
        times = [datetime.fromtimestamp(p["exit_time"], tz=timezone.utc) for p in sorted_pos if p.get("exit_time")]
        pnls = []
        for p in sorted_pos:
            if not p.get("exit_time"):
                continue
            # Compute PnL from spread capture: entry_spread - exit_spread - fees
            entry_s = p.get("entry_spread", p.get("actual_spread_bps", 0)) or 0
            exit_s = p.get("exit_spread_bps", 0) or 0
            capture_bps = entry_s - exit_s
            pnl = capture_bps / 10000 * POSITION_USD * 2  # both legs
            pnls.append(pnl)

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

        ax.set_title("H2 Spike Fade \u2014 LIVE Trading Equity", fontsize=12)
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
        self._shadow_fills: dict[str, dict] = {}  # order_id -> {filled_qty, avg_price} for shadow mode

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
            fill_price = sim_price if sim_price > 0 else price
            # Track shadow fills so check_order returns correct data
            self._shadow_fills[oid] = {"filled_qty": qty, "avg_price": fill_price}

            if self._on_shadow_fill:
                from app.services.arb.order_feed import FillEvent, FillSource
                fill = FillEvent(
                    venue=venue,
                    symbol=symbol,
                    order_id=oid,
                    exec_id=f"sim_{oid}",
                    side=side,
                    price=fill_price,
                    qty=qty,
                    fee=qty * fill_price * 0.00055 if venue == "bybit" else qty * fill_price * 0.001,
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
            shadow_fill = self._shadow_fills.get(order_id)
            if shadow_fill:
                return {"filled_qty": shadow_fill["filled_qty"], "avg_price": shadow_fill["avg_price"], "status": "Filled"}
            return {"filled_qty": 0, "avg_price": 0, "status": "shadow"}

        bn_symbol = symbol
        if venue == "binance" and TRADING_MODE == "usdc" and symbol in USDC_PAIR_MAP:
            bn_symbol = USDC_PAIR_MAP[symbol][0]

        try:
            if venue == "bybit":
                orders = await self.bybit_api.get_open_orders(symbol)
                # If order IS in open orders, return its cumulative fill state
                for o in orders:
                    if o.get("orderId") == order_id:
                        return {"filled_qty": float(o.get("cumExecQty", 0)), "avg_price": float(o.get("avgPrice", 0)), "status": o.get("orderStatus", "")}
                # Not in open orders — sum all executions for this order
                execs = await self.bybit_api.get_executions(limit=50)
                total_qty = 0.0
                total_value = 0.0
                for e in execs:
                    if e.get("orderId") == order_id:
                        eq = float(e.get("execQty", 0))
                        ep = float(e.get("execPrice", 0))
                        total_qty += eq
                        total_value += eq * ep
                if total_qty > 0:
                    return {"filled_qty": total_qty, "avg_price": total_value / total_qty, "status": "Filled"}
            elif venue == "binance":
                trades = await self.binance_api.get_my_trades(bn_symbol, limit=20)
                total_qty = 0.0
                total_value = 0.0
                for t in trades:
                    if str(t.get("orderId")) == order_id:
                        tq = float(t.get("qty", 0))
                        tp = float(t.get("price", 0))
                        total_qty += tq
                        total_value += tq * tp
                if total_qty > 0:
                    return {"filled_qty": total_qty, "avg_price": total_value / total_qty, "status": "Filled"}
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
            # Map symbol for USDC mode (NOMUSDT -> NOMUSDC)
            bn_symbol = symbol
            if TRADING_MODE == "usdc" and symbol in USDC_PAIR_MAP:
                bn_symbol = USDC_PAIR_MAP[symbol][0]
            return await self.binance_api.cancel_order(bn_symbol, order_id)
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
        self._entry_reject_cooldown: dict[str, float] = {}  # sym -> next_allowed_log_time
        self._background_tasks: set = set()  # H6: keep strong references to background tasks

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
                # H6 FIX: store task reference to prevent GC, and attach error logger
                # so exceptions in the background recovery are not silently swallowed.
                task = asyncio.create_task(self._handle_disconnect(venue, session, submitter))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                task.add_done_callback(
                    lambda t: logger.error(
                        f"GAP RECOVERY task raised: {t.exception()}", exc_info=t.exception()
                    ) if not t.cancelled() and t.exception() else None
                )

            self.order_feed = OrderFeed(
                BYBIT_KEY, BYBIT_SECRET,
                BINANCE_KEY, BINANCE_SECRET,
                on_fill=on_fill, on_order=on_order,
                on_disconnect=on_disconnect,
            )

            # Wire real submit/cancel/check/position functions into coordinator
            self._coordinator._submit = submitter.submit
            self._coordinator._cancel = submitter.cancel
            self._coordinator._check_order = submitter.check_order

            # Position verification function — queries actual exchange state
            async def check_position(venue: str, symbol: str) -> float:
                """Returns net position size on exchange. Used to catch phantom leg failures."""
                try:
                    if venue == "bybit":
                        positions = await submitter.bybit_api.get_positions(symbol)
                        for p in positions:
                            if float(p.get("size", 0)) != 0:
                                return float(p["size"])
                    elif venue == "binance":
                        # For spot, check balance of base asset
                        base = symbol.replace("USDT", "").replace("USDC", "")
                        bal = await submitter.binance_api.get_balance(base)
                        return bal
                except Exception as e:
                    logger.warning(f"check_position failed {venue} {symbol}: {e}")
                return 0.0

            self._coordinator._check_position = check_position

            # Crash recovery
            if not self.shadow:
                report = await self.crash_recovery.recover(
                    session, BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, PAIRS,
                    binance_secret=BINANCE_SECRET,
                )
                if not report.clean:
                    logger.warning(f"Recovery found issues: {report}")
                    await tg_send(f"H2 {mode}: Recovery found issues: {report.errors}", session)

                # Resume tracked positions into signal engine
                from app.services.arb.signal_engine import OpenPosition
                for doc in getattr(self.crash_recovery, 'resumed_positions', []):
                    sym = doc["symbol"]
                    pos = OpenPosition(
                        symbol=sym,
                        direction=doc.get("direction", "BUY_BB_SELL_BN"),
                        entry_spread=doc.get("actual_spread_bps", doc.get("entry_spread", 0)),
                        entry_time=doc.get("entry_time", time.time()),
                        entry_threshold_p90=0,  # not stored, use 0
                        exit_threshold_p25=self.signal_engine.status()["pairs"].get(sym, {}).get("p25", 0),
                        position_id=doc.get("position_id", f"recovered_{sym}"),
                    )
                    pos._bb_filled_qty = doc.get("bb_filled_qty", 0)
                    pos._bn_filled_qty = doc.get("bn_filled_qty", 0)
                    pos._bb_order_id = doc.get("bb_order_id", "")
                    pos._bn_order_id = doc.get("bn_order_id", "")
                    self.signal_engine.register_position(pos)
                    logger.info(f"Position RESUMED: {sym} entry={pos.entry_spread:.0f}bp exit@{pos.exit_threshold_p25:.0f}bp")

                # Execute orphan inventory buybacks on Binance
                orphan_buybacks = getattr(self.crash_recovery, '_orphan_buybacks', [])
                if orphan_buybacks:
                    submitter_tmp = OrderSubmitter(session, shadow=self.shadow)
                    for buyback in orphan_buybacks:
                        sym = buyback["symbol"]
                        qty = buyback["qty"]
                        try:
                            # Get current price for the buy
                            bn_sym = USDC_PAIR_MAP[sym][0] if TRADING_MODE == "usdc" and sym in USDC_PAIR_MAP else sym
                            bb_sym = USDC_PAIR_MAP[sym][1] if TRADING_MODE == "usdc" and sym in USDC_PAIR_MAP else sym
                            bn_rules = self.instrument_rules.get("binance", bn_sym)
                            # Use a market-crossing limit price (5bp above mid)
                            snap = self.price_feed.get_spread_for_exit(sym, max_age_s=60)
                            if snap:
                                buy_price = snap.bn_ask * 1.001  # slight overpay to ensure fill
                                buy_price = bn_rules.round_price(buy_price) if bn_rules else buy_price
                                rounded_qty = bn_rules.round_qty(qty) if bn_rules else qty
                                logger.info(f"ORPHAN BUYBACK: {sym} buying {rounded_qty} @ {buy_price:.6f} on Binance")
                                oid = await submitter_tmp.submit("binance", sym, "Buy", rounded_qty, buy_price, "limit")
                                # Record in inventory with cost tracking
                                self.inventory.bought(sym, rounded_qty, fill_price=buy_price)
                                await tg_send(
                                    f"\U0001f504 <b>Inventory replenished</b> {sym.replace('USDT','')}\n"
                                    f"Bought {rounded_qty:.0f} @ {buy_price:.6f} (${rounded_qty*buy_price:.2f})",
                                    session,
                                )
                            else:
                                logger.warning(f"ORPHAN BUYBACK skipped {sym}: no price data available")
                                await tg_send(f"\u26a0\ufe0f Orphan buyback skipped {sym}: no price. Manual buy needed.", session)
                        except Exception as e:
                            logger.error(f"ORPHAN BUYBACK failed {sym}: {e}")
                            await tg_send(f"\u26a0\ufe0f Orphan buyback failed {sym}: {e}", session)

            # Start feeds
            await self.price_feed.start(session)
            if not self.shadow:
                await self.order_feed.start(session)

            # Startup notification
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
                f"\U0001f680 <b>H2 Spike Fade {mode} STARTED</b>\n"
                f"Pairs: {len(PAIRS)} monitored\n"
                f"Entry: P90 | Exit: P25 | Fees: ~31bp RT\n"
                f"Size: ${POSITION_USD}/side | Max: {MAX_CONCURRENT}\n"
                + ("\n".join(viable_lines) if viable_lines else "(warming up thresholds...)"),
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
                        if self._poll_count % 3600 == 1:  # notify via TG every 30 min while paused
                            await tg_send(f"\u23f8\ufe0f <b>H2 PAUSED</b>: {reason}", session)
                    elif action == RiskAction.ALERT:
                        if self._poll_count % 120 == 1:
                            logger.info(f"ALERT: {reason}")
                            await tg_send(f"H2 ALERT: {reason}", session)

                    # Update thresholds for ALL pairs regardless of entry/exit state
                    for sym in PAIRS:
                        snap = self.price_feed.get_spread(sym)
                        if snap:
                            self.signal_engine.update_thresholds(snap)

                    # Check for exit signals on open positions
                    # Use relaxed freshness (30s) for exits -- better to exit with
                    # slightly stale data than be trapped in a position
                    for sym in list(self.signal_engine.open_positions.keys()):
                        snap = self.price_feed.get_spread(sym)
                        if not snap:
                            snap = self.price_feed.get_spread_for_exit(sym)
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
                                # Skip pairs on entry rejection cooldown (avoid log spam)
                                if time.time() < self._entry_reject_cooldown.get(sym, 0):
                                    continue
                                snap = self.price_feed.get_spread(sym)
                                if snap:
                                    entry_signal = self.signal_engine.check_entry(snap)
                                    if entry_signal:
                                        # Re-verify spread at execution time
                                        fresh_snap = self.price_feed.get_spread(sym)
                                        if fresh_snap and self.signal_engine.verify_spread_at_execution(fresh_snap, entry_signal):
                                            await self._handle_entry(entry_signal, fresh_snap, session, submitter)
                                        elif fresh_snap:
                                            logger.info(f"Entry rejected at verify: {sym} spread dropped to {fresh_snap.spread_bps:.1f}bp")
                                        else:
                                            logger.info(f"Entry rejected: {sym} fresh_snap=None (stale at verify time)")

                    # Status logging every 5 min
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

                    # Periodic inventory reconciliation every 5 min
                    if self._poll_count % 600 == 0 and not self.shadow:
                        await self.inventory.periodic_reconcile(
                            session, BINANCE_KEY, BINANCE_SECRET,
                            open_positions=set(self.signal_engine.open_positions.keys()),
                        )

                    # Sleep to target poll interval
                    elapsed = time.time() - t0
                    sleep_time = max(0, POLL_INTERVAL - elapsed)
                    await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                logger.info("Trading loop cancelled — running graceful shutdown")
                await self._graceful_shutdown(session, submitter)
            except Exception as e:
                logger.critical(f"UNHANDLED EXCEPTION in main loop: {e}", exc_info=True)
                await tg_send(f"H2 CRASH: {e}", session)
                await self._graceful_shutdown(session, submitter)

            # If loop exited via self.running = False (SIGTERM), run shutdown
            if not self.running and self.signal_engine.open_positions:
                logger.info("Signal-triggered exit — running graceful shutdown")
                await self._graceful_shutdown(session, submitter)

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
            await self.inventory.periodic_reconcile(
                            session, BINANCE_KEY, BINANCE_SECRET,
                            open_positions=set(self.signal_engine.open_positions.keys()),
                        )

            logger.info(f"GAP RECOVERY complete for {venue}")
            await tg_send(f"H2: {venue} WS reconnected, gap recovery complete", session)

        except Exception as e:
            logger.error(f"GAP RECOVERY failed for {venue}: {e}", exc_info=True)
            await tg_send(f"H2 ALERT: {venue} gap recovery failed: {e}", session)

    async def _handle_entry(self, signal, snap, session, submitter):
        """Process an entry signal."""
        sym = signal.symbol
        direction = snap.direction

        # Calculate canonical hedge qty FIRST (before inventory check)
        # so lock and release use the same amount
        bb_sym = USDC_PAIR_MAP[sym][1] if TRADING_MODE == "usdc" else sym
        bn_sym = USDC_PAIR_MAP[sym][0] if TRADING_MODE == "usdc" else sym

        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        # M3: Block trading if instrument rules are missing for this pair
        if not bb_rules or not bn_rules:
            now = time.time()
            if now >= self._entry_reject_cooldown.get(sym, 0):
                logger.critical(f"Missing instrument rules for {sym} (bb={bb_rules is not None}, bn={bn_rules is not None}) — BLOCKED")
                self._entry_reject_cooldown[sym] = now + 600  # log every 10 min
            return

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

        # Cap to available inventory (use what we have, don't require full $10)
        if direction == "BUY_BB_SELL_BN":
            inv = self.inventory.inventories.get(sym)
            if inv:
                available = inv.available_qty
                if available <= 0:
                    now = time.time()
                    if now >= self._entry_reject_cooldown.get(sym, 0):
                        logger.warning(f"No inventory for {sym}")
                        self._entry_reject_cooldown[sym] = now + 300
                    return
                if hedge_qty > available:
                    # Cap to available, re-round to step size
                    hedge_qty = bb_rules.round_qty(available) if bb_rules else available
                    hedge_qty = min(hedge_qty, bn_rules.round_qty(available) if bn_rules else available)
                    logger.info(f"Position capped to available inventory: {sym} {hedge_qty} (wanted {canonical_qty:.1f})")

        qty_bb = hedge_qty
        qty_bn = hedge_qty

        # Check inventory and lock
        if direction == "BUY_BB_SELL_BN":
            if not self.inventory.can_sell(sym, hedge_qty):
                now = time.time()
                if now >= self._entry_reject_cooldown.get(sym, 0):
                    logger.warning(f"Insufficient inventory for {sym}: need {hedge_qty}, have {self.inventory.inventories.get(sym)}")
                    self._entry_reject_cooldown[sym] = now + 300
                return
            self.inventory.lock(sym, hedge_qty)

        # Check min qty and min notional
        if bb_rules and (not bb_rules.check_min_qty(qty_bb) or not bb_rules.check_notional(qty_bb, snap.bb_ask)):
            logger.warning(f"Bybit qty/notional check failed: {sym} qty={qty_bb} price={snap.bb_ask}")
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, hedge_qty)
            return
        if bn_rules and (not bn_rules.check_min_qty(qty_bn) or not bn_rules.check_notional(qty_bn, snap.bn_bid)):
            logger.warning(f"Binance qty/notional check failed: {sym} qty={qty_bn} price={snap.bn_bid}")
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, hedge_qty)
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

            # Update inventory with ACTUAL fill quantities and prices
            actual_bn_qty = result.binance_leg.filled_qty
            actual_bn_fee = result.binance_leg.fee
            actual_bn_price = result.binance_leg.avg_fill_price
            fee_in_base = result.binance_leg.fee_asset not in ("USDT", "USDC", "BNB")
            if bn_side == "Sell":
                self.inventory.sold(sym, actual_bn_qty, actual_bn_fee, fee_in_base=fee_in_base, fill_price=actual_bn_price)
            else:
                self.inventory.bought(sym, actual_bn_qty, actual_bn_fee, fill_price=actual_bn_price)

            logger.info(
                f"ENTRY SUCCESS: {sym} actual_spread={result.actual_spread_bps:.1f}bp "
                f"slippage={result.slippage_bps:.1f}bp latency={result.entry_latency_ms:.0f}ms "
                f"bb_qty={result.bybit_leg.filled_qty} bn_qty={result.binance_leg.filled_qty}"
            )
            base = sym.replace("USDT", "")
            open_count = len(self.signal_engine.open_positions)
            excess = signal.threshold_p90 - signal.threshold_p25
            await tg_send(
                f"\U0001f4dd <b>H2 OPEN</b> <code>{base}</code> [{open_count}/{MAX_CONCURRENT}]\n"
                f"Spread: <b>{result.actual_spread_bps:.0f}bp</b> (P90={signal.threshold_p90:.0f})\n"
                f"Exit@{signal.threshold_p25:.0f}bp | Excess: {excess:.0f}bp (fees~31bp)\n"
                f"Slip: {result.slippage_bps:.1f}bp | Lat: {result.entry_latency_ms:.0f}ms\n"
                f"Size: ${POSITION_USD:.0f}/side | BB={result.bybit_leg.filled_qty:.1f} BN={result.binance_leg.filled_qty:.1f}",
                session,
            )

        elif result.outcome == EntryOutcome.LEG_FAILURE:
            self.risk_manager.record_trade(result.unwind_pnl_usd, 0, 0, leg_failure=True)
            # ALWAYS release the lock on leg failure — the unwind closes everything
            # Whether Binance filled or not, the position is unwound and lock must clear
            self.inventory.release(sym, hedge_qty)
            logger.info(f"Inventory lock released after leg failure (was locked {hedge_qty})")
            self._leg_failures_coll.insert_one({
                "symbol": sym, "direction": direction,
                "venue_failed": result.leg_failure_venue,
                "unwind_pnl_usd": result.unwind_pnl_usd,
                "timestamp": time.time(),
            })
            base = sym.replace("USDT", "")
            consec = self.risk_manager.status().get('consecutive_leg_failures', '?')
            logger.warning(f"LEG FAILURE: {sym} {result.leg_failure_venue} unwind=${result.unwind_pnl_usd:.4f}")
            await tg_send(
                f"\u26a0\ufe0f <b>H2 LEG FAIL</b> <code>{base}</code>\n"
                f"Venue: {result.leg_failure_venue} | Unwind: ${result.unwind_pnl_usd:.4f}\n"
                f"Consecutive: {consec}/3 (circuit breaker at 3)",
                session,
            )

        else:
            # Both missed or rejected — release lock
            if direction == "BUY_BB_SELL_BN":
                self.inventory.release(sym, hedge_qty)
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

        # Use STORED fill quantities from entry, not recomputed from current price
        # CRITICAL: If a prior partial exit already closed one leg, skip that leg
        bb_already_closed = getattr(pos, '_bb_exit_closed', False)
        bn_already_closed = getattr(pos, '_bn_exit_closed', False)

        qty_bb = 0 if bb_already_closed else getattr(pos, '_bb_filled_qty', 0)
        qty_bn = 0 if bn_already_closed else getattr(pos, '_bn_filled_qty', 0)

        if qty_bb <= 0 and qty_bn <= 0:
            # Both legs already closed by prior partials -- just unregister
            logger.info(f"Both exit legs already closed for {sym}, unregistering")
            self.signal_engine.unregister_position(sym)
            self._trades_coll.update_one(
                {"position_id": getattr(pos, 'position_id', '')},
                {"$set": {"status": "CLOSED", "close_reason": "partial_complete"}},
            )
            return

        if qty_bb <= 0 and not bb_already_closed and qty_bn <= 0 and not bn_already_closed:
            # No stored qty at all -- fallback
            logger.warning(f"No stored fill qty for {sym}, falling back to price-based sizing")
            avg_price = (snap.bb_bid + snap.bn_ask) / 2 if snap.bb_bid > 0 else snap.bb_bid or snap.bn_ask
            qty_bb = POSITION_USD / avg_price if avg_price > 0 else 0
            qty_bn = qty_bb

        # Get symbols for price rounding
        bb_sym = USDC_PAIR_MAP[sym][1] if TRADING_MODE == "usdc" else sym
        bn_sym = USDC_PAIR_MAP[sym][0] if TRADING_MODE == "usdc" else sym
        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        # Round exit quantities to exchange step sizes (prevents LOT_SIZE errors)
        if bb_rules and qty_bb > 0:
            qty_bb = bb_rules.round_qty(qty_bb)
        if bn_rules and qty_bn > 0:
            qty_bn = bn_rules.round_qty(qty_bn)

        price_bb = bb_rules.round_price(snap.bb_bid if bb_side == "Sell" else snap.bb_ask) if bb_rules else snap.bb_bid
        price_bn = bn_rules.round_price(snap.bn_ask if bn_side == "Buy" else snap.bn_bid) if bn_rules else snap.bn_ask

        # Execute exit -- skip legs that are already closed
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
            actual_bn_price = result.binance_leg.avg_fill_price
            if bn_side == "Buy" and actual_bn_qty > 0:
                self.inventory.bought(sym, actual_bn_qty, actual_bn_fee, fill_price=actual_bn_price)
            elif bn_side == "Sell" and actual_bn_qty > 0:
                fee_in_base = result.binance_leg.fee_asset not in ("USDT", "USDC", "BNB")
                self.inventory.sold(sym, actual_bn_qty, actual_bn_fee, fee_in_base=fee_in_base, fill_price=actual_bn_price)

            # Compute round-trip PnL from spread capture
            entry_spread = pos.entry_spread if pos else 0
            exit_spread = result.actual_exit_spread_bps if hasattr(result, 'actual_exit_spread_bps') else 0
            capture_bps = entry_spread - exit_spread
            net_bps = capture_bps - 31  # ~31bp RT fees
            rt_pnl_usd = net_bps / 10000 * POSITION_USD * 2
            pnl = rt_pnl_usd
            self.inventory.record_round_trip_pnl(sym, rt_pnl_usd)
            self.risk_manager.record_trade(pnl, 0, 0, leg_failure=(result.outcome == "partial"))

            # Update MongoDB position record
            # Use PARTIAL_EXIT for partial outcomes so position stays trackable
            mongo_status = "CLOSED" if result.outcome == "success" else "PARTIAL_EXIT"
            pos_id = getattr(pos, 'position_id', '')
            if pos_id:
                self._trades_coll.update_one(
                    {"position_id": pos_id},
                    {"$set": {
                        "status": mongo_status,
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

        # Only unregister if BOTH legs confirmed closed
        if result.outcome == "success":
            self.signal_engine.unregister_position(sym)
        elif result.outcome == "failed":
            # Exit completely failed — keep position active, will retry next cycle
            logger.critical(f"EXIT FAILED for {sym} — position stays active, will retry")
            await tg_send(f"H2 CRITICAL: Exit failed for {sym}, retrying next cycle", session)
        else:
            # Partial — record WHICH leg closed so retry only tries the other
            bb_closed = result.bybit_leg.is_filled
            bn_closed = result.binance_leg.is_filled
            if bb_closed:
                pos._bb_exit_closed = True
                logger.warning(f"EXIT PARTIAL {sym}: Bybit CLOSED, Binance still open — will retry Binance only")
            if bn_closed:
                pos._bn_exit_closed = True
                logger.warning(f"EXIT PARTIAL {sym}: Binance CLOSED, Bybit still open — will retry Bybit only")
            if bb_closed and bn_closed:
                # Actually both closed -- treat as success
                self.signal_engine.unregister_position(sym)
                logger.info(f"EXIT PARTIAL {sym}: both legs actually closed, unregistering")
            else:
                await tg_send(
                    f"\u26a0\ufe0f H2 PARTIAL EXIT {sym.replace('USDT','')}: "
                    f"BB={'closed' if bb_closed else 'OPEN'} BN={'closed' if bn_closed else 'OPEN'} "
                    f"— retrying unfilled leg next cycle",
                    session,
                )

        safe_reason = signal.signal_type.replace("<", "&lt;").replace(">", "&gt;")
        base = sym.replace("USDT", "")
        # Compute PnL from spread capture
        entry_spread = pos.entry_spread if pos else 0
        exit_spread = result.actual_exit_spread_bps if hasattr(result, 'actual_exit_spread_bps') else 0
        capture_bps = entry_spread - exit_spread
        net_bps = capture_bps - 31  # ~31bp RT fees
        pnl_usd = net_bps / 10000 * POSITION_USD * 2
        hold_s = (time.time() - pos.entry_time) if pos else 0
        emoji = "\u2705" if net_bps > 0 else "\u274c"
        await tg_send(
            f"\U0001f4dd <b>H2 CLOSE</b> {emoji} <code>{base}</code>\n"
            f"Entry: {entry_spread:.0f}bp \u2192 Exit: {exit_spread:.0f}bp\n"
            f"Capture: {capture_bps:.0f}bp | Net: <b>{net_bps:.0f}bp (${pnl_usd:.3f})</b>\n"
            f"Hold: {hold_s/60:.0f}min | {safe_reason}\n"
            f"Outcome: {result.outcome} | Lat: {result.exit_latency_ms:.0f}ms",
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
        await self.inventory.periodic_reconcile(
                            session, BINANCE_KEY, BINANCE_SECRET,
                            open_positions=set(self.signal_engine.open_positions.keys()),
                        )

        # Final status
        logger.info(f"Final risk status: {self.risk_manager.status()}")
        logger.info(f"Final inventory: {self.inventory.status()}")

        await tg_send(
            "\U0001f6d1 <b>H2 SHUTDOWN complete</b>\n"
            "All orders cancelled, positions closed.\n"
            f"Final: {self.risk_manager.status().get('total_trades', 0)} trades, "
            f"${self.risk_manager.status().get('total_pnl_usd', 0):.2f} PnL",
            session,
        )

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
        """Console + brief Telegram status every 5 min."""
        uptime_h = (time.time() - self._start_time) / 3600
        risk = self.risk_manager.status()
        mode = "SHADOW" if self.shadow else "LIVE"
        sig_status = self.signal_engine.status()

        # Console status (verbose)
        status_lines = [
            f"H2 {mode} ({uptime_h:.1f}h)",
            f"Trades: {risk['total_trades']} | PnL: ${risk['total_pnl_usd']:.2f} | Daily: ${risk['daily_pnl_usd']:.2f}",
        ]
        if risk['total_trades'] > 0:
            status_lines.append(f"Slippage: {risk['avg_slippage_bps']:.1f}bp | Leg fails: {risk['leg_failures']}")

        status_lines.append("")
        for sym in PAIRS:
            ss = sig_status["pairs"].get(sym, {})
            base = sym.replace("USDT", "")
            health = self._get_feed_health(sym)
            snap = self.price_feed.get_spread(sym)
            spread_str = f"{snap.spread_bps:.0f}bp" if snap else "n/a"

            if ss.get("ready"):
                p90 = ss.get("p90", 0)
                p25 = ss.get("p25", 0)
                has_pos = ss.get("has_position", False)
                viable = ss.get("viable", False)
                if has_pos:
                    icon = "OPEN"
                elif viable and health == "OK":
                    icon = "READY"
                elif viable:
                    icon = "VIABLE"
                else:
                    icon = "QUIET"
                status_lines.append(
                    f"  {base}: [{icon}] spread={spread_str} P90={p90:.0f} P25={p25:.0f} feed={health}"
                )
            else:
                fs = self.price_feed.status().get(sym, {})
                status_lines.append(f"  {base}: [WARMING] feed={health} ({fs.get('bb_updates', 0)}+{fs.get('bn_updates', 0)} updates)")

        inv = self.inventory.status()
        low_inv = [s for s, v in inv.items() if not v["healthy"]]
        if low_inv:
            status_lines.append(f"\nInventory warning: {', '.join(low_inv)}")

        status = "\n".join(status_lines)
        logger.info(status)

    def _build_report_text(self) -> str:
        """Build full Telegram report text (matches paper trader quality)."""
        uptime_h = (time.time() - self._start_time) / 3600
        risk = self.risk_manager.status()
        mode = "SHADOW" if self.shadow else "LIVE"
        sig_status = self.signal_engine.status()

        # Load closed trades from MongoDB
        closed = list(self._trades_coll.find({"status": "CLOSED"}).sort("exit_time", -1))
        open_pos = list(self.signal_engine.open_positions.values())

        # Compute stats
        total_trades = len(closed)
        wins = 0
        total_pnl = 0.0
        for t in closed:
            entry_s = t.get("entry_spread", t.get("actual_spread_bps", 0)) or 0
            exit_s = t.get("exit_spread_bps", 0) or 0
            cap = entry_s - exit_s
            net = cap - 31
            pnl = net / 10000 * POSITION_USD * 2
            total_pnl += pnl
            if net > 0:
                wins += 1
        wr = wins / total_trades if total_trades else 0

        can_trade = self.risk_manager.can_trade
        state_emoji = "\U0001f7e2" if can_trade else "\U0001f534"

        lines = [
            f"<b>H2 Spike Fade {mode}</b> ({uptime_h:.1f}h) {state_emoji}",
            f"Closed: {total_trades} | WR: {wr:.0%} | PnL: <b>${total_pnl:.2f}</b>",
            f"Leg fails: {risk['leg_failures']} | Trading: {'YES' if can_trade else 'PAUSED'}",
        ]

        if closed:
            # Per-pair breakdown
            by_pair = defaultdict(list)
            for t in closed:
                sym = t.get("symbol", "?")
                entry_s = t.get("entry_spread", t.get("actual_spread_bps", 0)) or 0
                exit_s = t.get("exit_spread_bps", 0) or 0
                net = (entry_s - exit_s) - 31
                by_pair[sym].append(net)
            lines.append("")
            for sym in sorted(by_pair):
                pp = by_pair[sym]
                base = sym.replace("USDT", "")
                w = sum(1 for x in pp if x > 0)
                pnl = sum(x / 10000 * POSITION_USD * 2 for x in pp)
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

        # Current thresholds
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

        # Inventory + cost basis
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
            # Mark to market using Binance mid price
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

        return "\n".join(lines)

    async def _send_telegram_report(self, session):
        """Send full report + equity curve chart to Telegram."""
        report = self._build_report_text()

        # Generate equity curve from closed trades
        closed = list(self._trades_coll.find({"status": "CLOSED"}).sort("exit_time", 1))
        chart_path = render_equity_curve_live(closed) if closed else None

        if chart_path:
            await tg_send_photo(chart_path, f"\U0001f4ca {report}", session)
        else:
            await tg_send(f"\U0001f4ca {report}", session)


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
