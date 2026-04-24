"""
H2 Spike Fade V3 — Live Trader with Dynamic Pair Screener.

Extends V2 with:
- TierEngine: dynamic pair tiering from collector MongoDB (every 5min)
- UniverseManager: WS connection lifecycle (refresh every 4h)
- Per-tier quantile gates (P87 for Tier A, P90 for Tier B)
- Per-tier position sizing (full for A, half for B)

Usage:
    set -a && source .env && set +a
    python scripts/arb_h2_live_v3.py [--shadow]
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
from app.services.arb.tier_engine import TierEngine
from app.services.arb.universe_manager import UniverseManager

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
        logging.FileHandler("/tmp/arb-h2-v3.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────

TRADING_MODE = "usdc"

# V3: No static USDC_PAIR_MAP. TierEngine builds the pair list dynamically.
# Legacy map kept for backward compat with inventory/recovery code.
USDC_PAIR_MAP: dict[str, tuple[str, str]] = {}  # populated at startup from TierEngine

POSITION_USD = 10.0
MAX_CONCURRENT = 6
BYBIT_ENTRY_MODE = "aggressive"  # "aggressive" = IOC (taker 0.055%), "passive" = PostOnly (maker 0.02% but 90bp price slippage on spikes)
# Dynamic fee estimate based on entry mode:
# Passive (PostOnly): BB maker 0.02% * 2 sides + BN taker 0.095% * 2 sides = 4bp + 19bp = 23bp
# Aggressive (IOC):   BB taker 0.055% * 2 sides + BN taker 0.095% * 2 sides = 11bp + 19bp = 30bp
FEE_RT_BPS = 23.0 if BYBIT_ENTRY_MODE == "passive" else 31.0
POSTONLY_SPREAD_VERIFY_THRESHOLD = 0.7  # After BB fill, abort if spread < signal * this

# Naked-leg SLO — hard limits on unhedged exposure
MAX_NAKED_MS = 3000           # Max time with one leg filled, other not submitted (ms)
MAX_UNHEDGED_USD = 2000.0     # Max unhedged notional before forced market close
NAKED_LEG_ALERT_MS = 1500     # Alert threshold (half of hard limit)

POLL_INTERVAL = 0.5
TIER_RECOMPUTE_INTERVAL = 300  # 5 minutes
UNIVERSE_REFRESH_INTERVAL = 4 * 3600  # 4 hours
V3_EPOCH = 1776940200.0  # Apr 23 2026 10:30 UTC — first V3 deploy. Used to filter V3-only trades.

# Inventory lifecycle / rotation controls
MIN_RETAIN_USD = 2.0  # below this, treat remaining balance as dust
AUTOSEED_BUFFER_TRADES = 2.0  # keep enough token inventory for N entries
LIQUIDATE_DISCOUNT = 0.0015  # sell at bid * (1-discount) for fast liquidation
SEED_SURCHARGE = 0.0010  # buy at ask * (1+surcharge) for fast seeding
REBALANCE_INTERVAL_POLL = 600  # every 5 minutes

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
    """Check for /report, /status, /h2, /h2live, /h2stats, /h2pause, /h2resume, /h2mode, /h2size commands."""
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
            text = msg.get("text", "").strip().lower().split("@")[0]
            if text.startswith(("/report", "/status", "/h2")):
                return text
        return None
    except Exception:
        return None


def render_equity_curve_live(closed_positions: list, path: str = "/tmp/h2_equity_v2.png"):
    """Render equity curve as PNG for V2 live trades. Graceful degradation on any error."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        if not closed_positions:
            return None

        # Sort by close_time, filtering out positions with no timestamp
        def _sort_key(p):
            ct = p.get("close_time", p.get("exit_time", 0))
            if isinstance(ct, (int, float)):
                return ct
            if hasattr(ct, "timestamp"):
                return ct.timestamp()
            return 0

        sorted_pos = sorted(closed_positions, key=_sort_key)
        times = []
        pnls = []
        for p in sorted_pos:
            ct = p.get("close_time", p.get("exit_time"))
            if not ct:
                continue
            try:
                if isinstance(ct, (int, float)):
                    if ct > 1e12:  # milliseconds
                        ct = ct / 1000
                    times.append(datetime.fromtimestamp(ct, tz=timezone.utc))
                elif hasattr(ct, "replace"):
                    times.append(ct.replace(tzinfo=timezone.utc) if ct.tzinfo is None else ct)
                else:
                    continue
            except (ValueError, OSError, OverflowError):
                continue
            pnl_net = p.get("pnl", {}).get("net_usd", 0) or 0
            pnls.append(float(pnl_net))

        if len(times) < 1 or len(pnls) < 1:
            return None

        cum_pnl = np.cumsum(pnls)

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.step(times, cum_pnl, where="post", color="#2196F3", linewidth=1.5)
        ax.fill_between(times, cum_pnl, step="post", alpha=0.15, color="#2196F3")
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")

        for i, (t, p) in enumerate(zip(times, pnls)):
            c = "#4CAF50" if p > 0 else "#F44336"
            ax.scatter([t], [cum_pnl[i]], color=c, s=20, zorder=5)

        ax.set_title("H2 V3 Spike Fade -- LIVE Equity", fontsize=12)
        ax.set_ylabel("Cumulative PnL ($)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
        fig.autofmt_xdate()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(path, dpi=120)
        plt.close(fig)
        return path
    except Exception as e:
        logger.warning(f"Equity chart render failed (graceful skip): {e}")
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
        self._last_tier_recompute: float = 0.0
        # Real-time effective cost tracker (Item #2)
        self._realized_costs: list[float] = []  # last N trades' effective RT cost in bps
        self._realized_cost_window = 20  # rolling window
        self._retire_queue: set[str] = set()

        # V3: TierEngine drives pair selection
        self.tier_engine = TierEngine(mongo_uri=MONGO_URI)
        self.universe_manager: UniverseManager | None = None  # init in run()

        # Components (PriceFeed created by UniverseManager)
        self.price_feed: PriceFeed | None = None
        self.signal_engine = SignalEngine([], MONGO_URI, fee_rt_bps=FEE_RT_BPS)  # empty, populated dynamically
        self.inventory = InventoryLedger(MONGO_URI)
        self.risk_manager = RiskManager()
        self.instrument_rules = InstrumentRules()
        self.position_store = PositionStore(MONGO_URI)
        self.inventory_guard = InventoryImpairmentGuard(
            impairment_threshold=3.0,
            absolute_impairment_limit=20.0,
        )

        # MongoDB for positions (used by report/equity chart)
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        db_name = MONGO_URI.rsplit("/", 1)[-1]
        self._db = client[db_name]
        self._positions_coll = self._db["arb_h2_positions_v2"]

    async def _has_live_position(self, symbol: str) -> bool:
        """True if symbol has any non-terminal position in PositionStore."""
        live_states = [
            PositionState.PENDING,
            PositionState.ENTERING,
            PositionState.OPEN,
            PositionState.EXITING,
            PositionState.PARTIAL_EXIT,
            PositionState.UNWINDING,
        ]
        docs = await self.position_store.get_by_symbol(symbol, live_states)
        return bool(docs)

    async def _fetch_binance_mid_price(self, symbol: str, session: aiohttp.ClientSession) -> float:
        """Best-effort mid price from WS, then Binance REST ticker."""
        snap = self.price_feed.get_spread_for_exit(symbol) if self.price_feed else None
        if snap:
            mid = (snap.bn_bid + snap.bn_ask) / 2
            if mid > 0:
                return mid
        bn_sym = self._pair_map.get(symbol, (symbol.replace("USDT", "USDC"),))[0]
        try:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": bn_sym},
                timeout=aiohttp.ClientTimeout(total=4),
            ) as resp:
                data = await resp.json()
            return float(data.get("price", 0))
        except Exception:
            return 0.0

    def _sync_inventory_guard_pair(self, symbol: str):
        inv = self.inventory.inventories.get(symbol)
        if not inv:
            return
        derived_cost = (
            inv.expected_qty * inv.avg_cost_per_unit
            if inv.avg_cost_per_unit > 0
            else inv.cost_basis_usd
        )
        self.inventory_guard.register_pair(symbol, inv.expected_qty, derived_cost)

    async def _seed_inventory_for_symbols(
        self,
        symbols: list[str],
        session: aiohttp.ClientSession,
        gateway: OrderGateway,
        reason: str = "autoseed",
    ):
        """Ensure active symbols can place SELL_BN entries (inventory preloaded)."""
        if self.shadow:
            return
        targets = [s for s in dict.fromkeys(symbols) if s in self._pair_map]
        if not targets:
            return

        open_positions = await self.position_store.get_active()
        active_symbols = {p.get("symbol", "") for p in open_positions}
        usdc_reserved = sum(
            POSITION_USD * 1.01
            for p in open_positions
            if p.get("direction") == "BUY_BB_SELL_BN"
            and p.get("state") in (
                PositionState.OPEN.value,
                PositionState.EXITING.value,
                PositionState.PARTIAL_EXIT.value,
            )
        )
        try:
            usdc_balance = await gateway.binance_api.get_balance("USDC")
        except Exception:
            usdc_balance = 0.0
        usdc_available = max(0.0, usdc_balance - usdc_reserved)

        for sym in targets:
            if sym in active_symbols:
                continue
            inv = self.inventory.inventories.get(sym)
            if not inv:
                continue

            bn_sym = self._pair_map[sym][0]
            bn_rules = self.instrument_rules.get("binance", bn_sym)
            if not bn_rules:
                continue

            mid_price = await self._fetch_binance_mid_price(sym, session)
            if mid_price <= 0:
                continue

            min_qty = POSITION_USD / mid_price
            desired_qty = max(inv.target_qty, min_qty * AUTOSEED_BUFFER_TRADES)
            available = max(0.0, inv.expected_qty - inv.locked_qty)
            if available >= min_qty:
                if inv.target_qty < desired_qty:
                    self.inventory.set_target(sym, desired_qty)
                self.inventory.set_lifecycle_state(sym, "ACTIVE")
                self._sync_inventory_guard_pair(sym)
                continue

            buy_qty = max(0.0, desired_qty - available)
            if buy_qty <= 0:
                continue
            buy_cost = buy_qty * mid_price * 1.002
            if usdc_available < buy_cost:
                logger.warning(
                    f"AUTO-SEED blocked {sym}: need ${buy_cost:.2f}, "
                    f"available ${usdc_available:.2f} (reserved ${usdc_reserved:.2f})"
                )
                continue

            snap = self.price_feed.get_spread_for_exit(sym) if self.price_feed else None
            ask_price = snap.bn_ask if snap and snap.bn_ask > 0 else mid_price
            buy_price = bn_rules.round_price_for_side(ask_price * (1 + SEED_SURCHARGE), "Buy")
            rounded_qty = bn_rules.round_qty(buy_qty)
            if rounded_qty <= 0 or not bn_rules.check_notional(rounded_qty, buy_price):
                continue

            logger.info(
                f"AUTO-SEED[{reason}] {sym}: buy {rounded_qty} @ {buy_price} "
                f"(avail={available:.4f}, target={desired_qty:.4f})"
            )
            try:
                oid = await gateway.submit("binance", sym, "Buy", rounded_qty, buy_price, "limit")
                result = await gateway.get_order_with_retry("binance", sym, order_id=oid)
                fill_state = result.get("fill_result")
                if fill_state in ("FILLED", "PARTIAL"):
                    filled = float(result.get("filled_qty", 0))
                    fill_price = float(result.get("avg_price", buy_price))
                    if filled > 0:
                        self.inventory.bought(sym, filled, fill_price=fill_price)
                        self.inventory.set_target(sym, desired_qty)
                        self.inventory.set_lifecycle_state(sym, "ACTIVE")
                        self._sync_inventory_guard_pair(sym)
                        usdc_available = max(0.0, usdc_available - filled * fill_price * 1.002)
                        logger.info(f"AUTO-SEED success {sym}: +{filled:.6f} @ {fill_price:.6f}")
                else:
                    await gateway.cancel("binance", sym, oid)
            except Exception as e:
                logger.error(f"AUTO-SEED failed {sym}: {e}")

    async def _liquidate_inventory_for_symbol(
        self,
        symbol: str,
        session: aiohttp.ClientSession,
        gateway: OrderGateway,
        reason: str = "rotation",
    ) -> bool:
        """Attempt to liquidate non-live symbol inventory on Binance.

        Returns True when symbol can be removed from retire queue.
        """
        inv = self.inventory.inventories.get(symbol)
        if not inv:
            return True
        if inv.locked_qty > 0:
            return False

        self.inventory.set_target(symbol, 0.0)
        self.inventory.set_lifecycle_state(symbol, "RETIRING")

        available = max(0.0, inv.expected_qty - inv.locked_qty)
        if available <= 0:
            self.inventory.set_lifecycle_state(symbol, "INACTIVE")
            self._sync_inventory_guard_pair(symbol)
            return True

        bn_sym = self._pair_map.get(symbol, (symbol.replace("USDT", "USDC"),))[0]
        bn_rules = self.instrument_rules.get("binance", bn_sym)
        if not bn_rules:
            await self.instrument_rules.load_binance(session, [bn_sym])
            bn_rules = self.instrument_rules.get("binance", bn_sym)
            if not bn_rules:
                return False

        mid_price = await self._fetch_binance_mid_price(symbol, session)
        if mid_price <= 0:
            return False

        est_notional = available * mid_price
        if est_notional < MIN_RETAIN_USD:
            logger.info(f"Retire {symbol}: dust ${est_notional:.2f}, keeping residual")
            self.inventory.set_lifecycle_state(symbol, "INACTIVE")
            self._sync_inventory_guard_pair(symbol)
            return True

        sell_qty = bn_rules.round_qty(available)
        if sell_qty <= 0:
            return False

        snap = self.price_feed.get_spread_for_exit(symbol) if self.price_feed else None
        if snap and snap.bn_bid > 0:
            bid_price = snap.bn_bid
        else:
            # No WS feed — query actual bid from Binance REST
            try:
                async with session.get(
                    "https://api.binance.com/api/v3/ticker/bookTicker",
                    params={"symbol": bn_sym},
                    timeout=aiohttp.ClientTimeout(total=4),
                ) as resp:
                    ticker = await resp.json()
                bid_price = float(ticker.get("bidPrice", 0))
            except Exception:
                bid_price = mid_price * 0.995  # conservative fallback
            if bid_price <= 0:
                bid_price = mid_price * 0.995
        sell_price = bn_rules.round_price_for_side(bid_price * (1 - LIQUIDATE_DISCOUNT), "Sell")
        if not bn_rules.check_notional(sell_qty, sell_price):
            if sell_qty * sell_price < MIN_RETAIN_USD:
                self.inventory.set_lifecycle_state(symbol, "INACTIVE")
                self._sync_inventory_guard_pair(symbol)
                return True
            return False

        logger.info(f"RETIRE[{reason}] {symbol}: sell {sell_qty} @ {sell_price}")
        try:
            oid = await gateway.submit("binance", symbol, "Sell", sell_qty, sell_price, "limit")
            result = await gateway.get_order_with_retry("binance", symbol, order_id=oid)
            fill_state = result.get("fill_result")
            if fill_state in ("FILLED", "PARTIAL"):
                filled = float(result.get("filled_qty", 0))
                fill_price = float(result.get("avg_price", sell_price))
                if filled > 0:
                    self.inventory.sold(symbol, filled, fill_price=fill_price, fee_in_base=False)
                inv_now = self.inventory.inventories.get(symbol)
                remaining = max(0.0, inv_now.expected_qty - inv_now.locked_qty) if inv_now else 0.0
                self._sync_inventory_guard_pair(symbol)
                if remaining * max(fill_price, mid_price) < MIN_RETAIN_USD:
                    self.inventory.set_lifecycle_state(symbol, "INACTIVE")
                    logger.info(f"RETIRE done {symbol}: residual ${remaining * max(fill_price, mid_price):.2f}")
                    return True
                logger.warning(f"RETIRE partial {symbol}: remaining={remaining:.6f}")
                return False
            await gateway.cancel("binance", symbol, oid)
            return False
        except Exception as e:
            logger.error(f"RETIRE failed {symbol}: {e}")
            return False

    async def _process_retire_queue(self, session: aiohttp.ClientSession, gateway: OrderGateway):
        """Drain pending retire queue for removed pairs with leftover inventory."""
        if self.shadow or not self._retire_queue:
            return
        for sym in list(self._retire_queue):
            if await self._has_live_position(sym):
                continue
            if await self._liquidate_inventory_for_symbol(sym, session, gateway, reason="queue"):
                self._retire_queue.discard(sym)

    async def _apply_universe_changes(
        self,
        changes: list[str],
        session: aiohttp.ClientSession,
        gateway: OrderGateway,
    ):
        """Apply universe add/remove changes and keep inventory lifecycle aligned."""
        if not changes:
            return

        new_bb: list[str] = []
        for change in changes:
            if change.startswith("+"):
                sym_bb = change[1:]
                self.signal_engine.add_pair(sym_bb, MONGO_URI)
                tier_info = self.tier_engine.get_tier_by_bb(sym_bb)
                bn_sym = tier_info.symbol_bn if tier_info else sym_bb.replace("USDT", "USDC")
                self._pair_map[sym_bb] = (bn_sym, sym_bb)
                self._gateway_bn_map[sym_bb] = bn_sym
                if sym_bb not in self._active_pairs:
                    self._active_pairs.append(sym_bb)
                self.inventory.load([sym_bb])
                self.inventory.set_lifecycle_state(sym_bb, "ACTIVE")
                self._retire_queue.discard(sym_bb)
                self._sync_inventory_guard_pair(sym_bb)
                new_bb.append(sym_bb)
            elif change.startswith("-"):
                sym_bb = change[1:]
                self.signal_engine.remove_pair(sym_bb)
                has_live_pos = await self._has_live_position(sym_bb)
                if has_live_pos:
                    logger.info(f"Universe remove deferred for {sym_bb}: live position still open")
                    continue
                if sym_bb in self._active_pairs:
                    self._active_pairs.remove(sym_bb)
                inv = self.inventory.inventories.get(sym_bb)
                if inv and inv.expected_qty - inv.locked_qty > 0:
                    self._retire_queue.add(sym_bb)
                    self.inventory.set_lifecycle_state(sym_bb, "RETIRING")
                    self.inventory.set_target(sym_bb, 0.0)
                else:
                    self.inventory.set_target(sym_bb, 0.0)
                    self.inventory.set_lifecycle_state(sym_bb, "INACTIVE")

        if new_bb:
            new_bn = [self._pair_map[s][0] for s in new_bb]
            await self.instrument_rules.load_all(session, new_bb, new_bn)
            tradable_now = {info.symbol_bb for info in self.tier_engine.tradable_pairs()}
            seed_now = [s for s in new_bb if s in tradable_now]
            if seed_now:
                await self._seed_inventory_for_symbols(seed_now, session, gateway, reason="rotation_add")

        await self._process_retire_queue(session, gateway)

    async def _sync_inventory_policy(self, session: aiohttp.ClientSession, gateway: OrderGateway):
        """Inventory policy:
        - Seed only tradable (Tier A/B) symbols.
        - Retire inventory for non-tradable symbols with no live position.
        - Ensure gateway has BN symbol mapping for all retiring pairs.
        """
        tradable_symbols = {info.symbol_bb for info in self.tier_engine.tradable_pairs()}
        for sym, inv in list(self.inventory.inventories.items()):
            if await self._has_live_position(sym):
                continue
            if sym in tradable_symbols:
                self.inventory.set_lifecycle_state(sym, "ACTIVE")
                continue
            self.inventory.set_target(sym, 0.0)
            if inv.expected_qty - inv.locked_qty > 0:
                self.inventory.set_lifecycle_state(sym, "RETIRING")
                self._retire_queue.add(sym)
                # Ensure gateway knows the BN symbol for liquidation
                bn_sym = sym.replace("USDT", "USDC")
                if sym not in self._pair_map:
                    self._pair_map[sym] = (bn_sym, sym)
                if sym not in self._gateway_bn_map:
                    self._gateway_bn_map[sym] = bn_sym
                    gateway._bn_symbol_map[sym] = bn_sym
            else:
                self.inventory.set_lifecycle_state(sym, "INACTIVE")
        await self._process_retire_queue(session, gateway)

    async def run(self):
        global BYBIT_ENTRY_MODE, FEE_RT_BPS, POSITION_USD  # Runtime-modifiable via Telegram
        mode = "SHADOW" if self.shadow else "LIVE"
        logger.info("=" * 60)
        logger.info(f"H2 V3 SPIKE FADE — {mode} TRADER (dynamic screener)")
        logger.info(f"Size: ${POSITION_USD} | Max: {MAX_CONCURRENT}")
        logger.info("=" * 60)

        # V3: Compute initial tiers
        tiers = self.tier_engine.recompute()
        connected = self.tier_engine.connected_pairs()
        tradable = self.tier_engine.tradable_pairs()

        logger.info(f"Initial tiers:\n{self.tier_engine.summary()}")
        logger.info(f"Tradable: {len(tradable)}, Connected: {len(connected)}")

        # V3: Instance-level pair maps, kept in sync with universe
        self._pair_map = {
            info.symbol_bb: (info.symbol_bn, info.symbol_bb)
            for info in connected
        }
        self._active_pairs = list(self._pair_map.keys())

        # Initialize signal engine with all connected pairs
        self.signal_engine = SignalEngine(self._active_pairs, MONGO_URI, fee_rt_bps=FEE_RT_BPS)
        for info in connected:
            self.signal_engine.add_pair(info.symbol_bb, MONGO_URI)

        self.inventory.load(self._active_pairs)
        self.inventory.load_all()  # discover stale pairs for liquidation
        for sym in self._active_pairs:
            self.inventory.set_lifecycle_state(sym, "ACTIVE")

        async with aiohttp.ClientSession() as session:
            # Build gateway (V3: bn_symbol_map is a reference to self._pair_map derived dict)
            self._gateway_bn_map = {k: v[0] for k, v in self._pair_map.items()}
            gateway = OrderGateway(
                session, BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, BINANCE_SECRET,
                bn_symbol_map=self._gateway_bn_map,
                shadow=self.shadow,
            )

            # Build fill detector (with trade query for Binance fee enrichment)
            detector = FillDetector(
                get_order_fn=lambda venue, symbol, order_id="", client_order_id="":
                    gateway.get_order(venue, symbol, order_id=order_id, client_order_id=client_order_id),
                get_trades_fn=lambda venue, symbol, order_id="":
                    gateway.get_trades_for_order(venue, symbol, order_id=order_id),
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
                    try:
                        await asyncio.sleep(10)
                        if hasattr(order_feed, 'bybit') and order_feed.bybit and order_feed.bybit.is_connected:
                            if not ws_available.get("bybit"):
                                ws_available["bybit"] = True
                                logger.info("Bybit WS reconnected — fill detection restored")
                        if hasattr(order_feed, 'binance') and order_feed.binance and order_feed.binance.is_connected:
                            if not ws_available.get("binance"):
                                ws_available["binance"] = True
                                logger.info("Binance WS connected — switching from REST to WS fill detection")
                    except Exception as e:
                        logger.error(f"WS health monitor error (continuing): {e}")

            # Order feed (WS fills)
            order_feed = OrderFeed(
                BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, BINANCE_SECRET,
                on_fill=on_fill, on_order=on_order, on_disconnect=on_disconnect,
            )

            # Build flows
            entry_flow = EntryFlow(self.position_store, detector, gateway, ws_available, shadow=self.shadow, mongo_uri=MONGO_URI, max_naked_ms=MAX_NAKED_MS)
            exit_flow = ExitFlow(self.position_store, detector, gateway, ws_available, mongo_uri=MONGO_URI)

            # V3: Start feeds via UniverseManager FIRST (before crash recovery needs price_feed)
            self.universe_manager = UniverseManager(
                self.tier_engine, session,
                position_checker=self._has_live_position,
            )
            self.price_feed = await self.universe_manager.initialize(tiers)
            if not self.shadow:
                await order_feed.start(session)

            # Crash recovery (V2: state-machine-aware) — AFTER price_feed is initialized
            recovery = CrashRecoveryV2(self.position_store, gateway, self.inventory)
            if not self.shadow:
                report = await recovery.recover(self._active_pairs)
                if not report.clean:
                    await tg_send(f"H2 V3 {mode}: Recovery issues: {report.errors}", session)

                # V3 C3 fix: ensure ALL active position symbols have WS coverage
                # HIGH FIX (Codex C4 #4): Include PARTIAL_EXIT, not just OPEN
                from app.services.arb.price_feed import VenuePrice
                for pos in await self.position_store.get_active():
                    sym = pos["symbol"]
                    if sym not in self.price_feed.symbols:
                        logger.warning(f"Position {sym} outside current tier set — force-adding to WS")
                        bn_sym = sym.replace("USDT", "USDC")
                        self._pair_map[sym] = (bn_sym, sym)
                        self._gateway_bn_map[sym] = bn_sym
                        if sym not in self._active_pairs:
                            self._active_pairs.append(sym)
                        self.signal_engine.add_pair(sym, MONGO_URI)
                        self.inventory.set_lifecycle_state(sym, "ACTIVE")
                        # Dynamically subscribe on Bybit
                        if self.price_feed.bybit._ws and not self.price_feed.bybit._ws.closed:
                            await self.price_feed.bybit._ws.send_json(
                                {"op": "subscribe", "args": [f"tickers.{sym}"]}
                            )
                            self.price_feed.bybit.prices[sym] = VenuePrice(symbol=sym)
                            self.price_feed.bybit.symbols.append(sym)
                            self.price_feed.symbols.append(sym)
                            self.price_feed._bn_map[sym] = bn_sym
                            self.price_feed._bn_reverse[bn_sym] = sym
                        # Dynamically subscribe on Binance (reconnects with new stream URL)
                        if bn_sym not in self.price_feed.binance.prices:
                            await self.price_feed.binance.add_symbol_and_reconnect(bn_sym)
                        self.universe_manager.mark_position_opened(sym)

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

                # Also resume PARTIAL_EXIT positions into signal engine.
                for pos in await self.position_store.get_by_symbol_states([PositionState.PARTIAL_EXIT]):
                    sym = pos["symbol"]
                    if sym in self.signal_engine.open_positions:
                        continue
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

            # Load instrument rules for connected pairs
            bb_symbols = [info.symbol_bb for info in connected]
            bn_symbols = [info.symbol_bn for info in connected]
            await self.instrument_rules.load_all(session, bb_symbols, bn_symbols)

            # Execute deferred actions from recovery (orphan buybacks — Codex #13)
            # FIX: was checking hasattr(recovery, '_last_report') which is never set
            if not self.shadow and report.deferred_actions:
                for action in report.deferred_actions:
                    if action.get("action") == "orphan_buyback":
                        sym = action["symbol"]
                        qty = action["qty"]
                        bn_sym = self._pair_map.get(sym, (sym,))[0]
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
            for sym in self._active_pairs:
                ss = sig_status["pairs"].get(sym, {})
                if ss.get("ready"):
                    base = sym.replace("USDT", "")
                    v = "\u2705" if ss.get("viable") else "\u274c"
                    viable_lines.append(
                        f"  {v} {base}: P90={ss.get('p90',0):.0f} exit={ss.get('p25',0):.0f} "
                        f"ex={ss.get('p90',0)-ss.get('p25',0):.0f}bp"
                    )
            await tg_send(
                f"\U0001f680 <b>H2 V3 Spike Fade {mode} STARTED</b>\n"
                f"Pairs: {len(self._active_pairs)} monitored\n"
                f"Entry: P90 | Exit: P25 | Fees: ~{FEE_RT_BPS:.0f}bp RT\n"
                f"Size: ${POSITION_USD}/side | Max: {MAX_CONCURRENT}\n"
                + ("\n".join(viable_lines) if viable_lines else "(warming up thresholds...)"),
                session,
            )

            # Start WS health monitor (restores ws_available after reconnect)
            ws_monitor_task = asyncio.create_task(_ws_health_monitor())

            # Seed inventory guard from inventory ledger
            # Use qty * avg_cost_per_unit as cost basis (not cost_basis_usd which can
            # drift out of sync when reconciliation changes qty without updating cost).
            for sym in self._active_pairs:
                inv = self.inventory.inventories.get(sym)
                if inv:
                    derived_cost = inv.expected_qty * inv.avg_cost_per_unit if inv.avg_cost_per_unit > 0 else inv.cost_basis_usd
                    self.inventory_guard.register_pair(sym, inv.expected_qty, derived_cost)
                    logger.info(f"Inventory guard seeded: {sym} qty={inv.expected_qty:.1f} cost=${derived_cost:.2f} (avg=${inv.avg_cost_per_unit:.6f})")

            # ── Startup inventory reconciliation ──
            # CRITICAL: reconcile ledger against actual Binance balances BEFORE trading.
            # Fixes stale ledger from prior sessions (e.g., KERNEL 3.7 vs actual 149.67).
            if not self.shadow:
                logger.info("Reconciling inventory against Binance balances...")
                startup_active = await self.position_store.get_active()
                await self.inventory.periodic_reconcile(
                    session, BINANCE_KEY, BINANCE_SECRET,
                    open_positions={p.get("symbol", "") for p in startup_active},
                )

                # Fix cost_basis consistency: ensure cost_basis = qty * avg_cost always.
                # This catches drift from reconciliation changing qty without updating cost.
                for sym in self._active_pairs:
                    inv_item = self.inventory.inventories.get(sym)
                    if inv_item and inv_item.expected_qty > 0 and inv_item.avg_cost_per_unit > 0:
                        derived = inv_item.expected_qty * inv_item.avg_cost_per_unit
                        if abs(inv_item.cost_basis_usd - derived) > 0.10:
                            logger.warning(
                                f"Cost basis drift detected for {sym}: "
                                f"cost_basis=${inv_item.cost_basis_usd:.2f} vs derived=${derived:.2f}. Fixing."
                            )
                            inv_item.cost_basis_usd = derived
                            self.inventory._save(sym)

                # Backfill cost_basis for pre-existing inventory with $0 cost.
                # We don't know the original purchase price, so use current market
                # price as baseline. Impairment tracking starts from zero (honest).
                for sym in self._active_pairs:
                    inv_item = self.inventory.inventories.get(sym)
                    if inv_item and inv_item.expected_qty > 0 and inv_item.cost_basis_usd <= 0:
                        bn_sym = self._pair_map[sym][0]
                        try:
                            async with session.get(
                                f"https://api.binance.com/api/v3/ticker/price?symbol={bn_sym}"
                            ) as resp:
                                ticker = await resp.json()
                                current_price = float(ticker.get("price", 0))
                            if current_price > 0:
                                backfill_cost = inv_item.expected_qty * current_price
                                inv_item.cost_basis_usd = backfill_cost
                                inv_item.avg_cost_per_unit = current_price
                                self.inventory._save(sym)
                                logger.info(
                                    f"Cost basis backfilled: {sym} {inv_item.expected_qty:.1f} "
                                    f"@ ${current_price:.6f} = ${backfill_cost:.2f}"
                                )
                        except Exception as e:
                            logger.warning(f"Cost basis backfill failed for {sym}: {e}")

                # Re-seed inventory guard after reconciliation + backfill
                # Use derived cost (qty * avg_cost) for consistency
                for sym in self._active_pairs:
                    inv_item = self.inventory.inventories.get(sym)
                    if inv_item:
                        derived_cost = inv_item.expected_qty * inv_item.avg_cost_per_unit if inv_item.avg_cost_per_unit > 0 else inv_item.cost_basis_usd
                        self.inventory_guard.register_pair(sym, inv_item.expected_qty, derived_cost)
                        logger.info(f"Post-reconcile inventory: {sym} qty={inv_item.expected_qty:.1f} cost=${derived_cost:.2f} (avg=${inv_item.avg_cost_per_unit:.6f})")

                # Bootstrap guard capture history from MongoDB closed trades
                # so it doesn't reset to zero on every restart
                for pos in self._positions_coll.find({"state": "CLOSED"}):
                    sym = pos.get("symbol", "")
                    pnl_usd = pos.get("pnl", {}).get("net_usd", 0)
                    if pnl_usd != 0:
                        self.inventory_guard.record_trade_capture(sym, pnl_usd)
                total_capture = self.inventory_guard._total_capture_usd
                logger.info(f"Guard bootstrapped: ${total_capture:.4f} capture from {self._positions_coll.count_documents({'state': 'CLOSED'})} closed trades")

            logger.info("Waiting 3s for feeds to warm up...")
            await asyncio.sleep(3)

            # Startup auto-seed (and reconcile target quantities) for active pairs
            if not self.shadow:
                await self._sync_inventory_policy(session, gateway)
                startup_seed = [info.symbol_bb for info in self.tier_engine.tradable_pairs()]
                await self._seed_inventory_for_symbols(startup_seed, session, gateway, reason="startup")

            # ── Main loop ──
            try:
                while self.running:
                    t0 = time.time()
                    self._poll_count += 1

                    self.risk_manager.check_daily_reset()
                    action, reason = self.risk_manager.evaluate()

                    if action == RiskAction.KILL:
                        logger.critical(f"KILL: {reason}")
                        await tg_send(f"H2 V3 KILLED: {reason}", session)
                        break

                    # Inventory impairment + slippage + degraded-mode checks every 5 minutes
                    if self._poll_count % 600 == 0:
                        await self._check_inventory_guard(session)
                        await self._check_slippage_alerts(session)
                        await self._check_degraded_mode(session, ws_available)

                    # V3: Periodic tier recompute (every 5min)
                    if time.time() - self._last_tier_recompute >= TIER_RECOMPUTE_INTERVAL:
                        self.tier_engine.recompute()
                        self._last_tier_recompute = time.time()
                        if not self.shadow:
                            await self._sync_inventory_policy(session, gateway)
                        if self._poll_count % 600 == 0:
                            logger.info(f"Tiers:\n{self.tier_engine.summary()}")

                    # V3: Periodic universe refresh (every 4h)
                    if self.universe_manager and self.universe_manager.needs_refresh():
                        changes = await self.universe_manager.refresh_universe()
                        if changes:
                            await self._apply_universe_changes(changes, session, gateway)

                    # Update thresholds for all connected pairs
                    for sym in list(self.price_feed.symbols):
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
                        await tg_send("H2 V3 CRITICAL: Fill detector buffer overflow. Trading paused.", session)
                        self.risk_manager._paused = True  # Force pause

                    # Check entries (V3: per-tier quantile + sizing)
                    feeds_ready = self.shadow or ws_available.get("bybit", False)
                    if self.risk_manager.can_trade and feeds_ready and action != RiskAction.PAUSE:
                        active_count = await self.position_store.count_active()
                        if active_count < MAX_CONCURRENT:
                            # V3: iterate tradable pairs from TierEngine
                            for tier_info in self.tier_engine.tradable_pairs():
                                sym = tier_info.symbol_bb
                                if sym in self.signal_engine.open_positions:
                                    continue
                                # Skip pairs on entry rejection cooldown (avoid log spam)
                                if time.time() < self._entry_reject_cooldown.get(sym, 0):
                                    continue
                                # Also check PositionStore (more authoritative)
                                if await self.position_store.get_by_symbol(sym, [
                                    PositionState.OPEN, PositionState.ENTERING,
                                    PositionState.EXITING, PositionState.PARTIAL_EXIT,
                                    PositionState.UNWINDING, PositionState.PENDING,
                                ]):
                                    continue

                                snap = self.price_feed.get_spread(sym)
                                if snap:
                                    # V3: use per-tier quantile gate
                                    entry_signal = self.signal_engine.check_entry(
                                        snap, entry_quantile=tier_info.quantile_gate
                                    )
                                    if entry_signal:
                                        fresh_snap = self.price_feed.get_spread(sym)
                                        if fresh_snap and self.signal_engine.verify_spread_at_execution(
                                            fresh_snap, entry_signal, entry_quantile=tier_info.quantile_gate
                                        ):
                                            # V3: store tier info for position sizing
                                            entry_signal._tier_info = tier_info
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
                        elif cmd == "/h2stats":
                            await self._send_stats_report(session)
                        elif cmd == "/h2pause":
                            self.risk_manager._paused = True
                            await tg_send("H2 V3 PAUSED. Send /h2resume to resume.", session)
                        elif cmd == "/h2resume":
                            self.risk_manager._paused = False
                            await tg_send("H2 V3 RESUMED.", session)
                        elif cmd and cmd.startswith("/h2mode"):
                            parts = cmd.split()
                            if len(parts) >= 2 and parts[1] in ("passive", "aggressive"):
                                BYBIT_ENTRY_MODE = parts[1]
                                FEE_RT_BPS = 23.0 if BYBIT_ENTRY_MODE == "passive" else 31.0
                                self.signal_engine.fee_rt_bps = FEE_RT_BPS
                                await tg_send(f"Mode: {BYBIT_ENTRY_MODE}, FEE_RT_BPS: {FEE_RT_BPS}", session)
                            else:
                                await tg_send(f"Current mode: {BYBIT_ENTRY_MODE}. Usage: /h2mode passive|aggressive", session)
                        elif cmd and cmd.startswith("/h2size"):
                            parts = cmd.split()
                            if len(parts) >= 2:
                                try:
                                    new_size = float(parts[1])
                                    if 1 <= new_size <= 10000:
                                        POSITION_USD = new_size
                                        await tg_send(f"Position size: ${POSITION_USD}/side", session)
                                    else:
                                        await tg_send("Size must be $1-$10000", session)
                                except ValueError:
                                    await tg_send(f"Current: ${POSITION_USD}/side. Usage: /h2size 100", session)
                            else:
                                await tg_send(f"Current: ${POSITION_USD}/side. Usage: /h2size 100", session)

                    # Periodic inventory reconciliation every 5 min (600 polls)
                    if self._poll_count % REBALANCE_INTERVAL_POLL == 0 and not self.shadow:
                        active_docs = await self.position_store.get_active()
                        await self.inventory.periodic_reconcile(
                            session, BINANCE_KEY, BINANCE_SECRET,
                            open_positions={p.get("symbol", "") for p in active_docs},
                        )
                        await self._sync_inventory_policy(session, gateway)
                        periodic_seed = [info.symbol_bb for info in self.tier_engine.tradable_pairs()]
                        await self._seed_inventory_for_symbols(periodic_seed, session, gateway, reason="periodic")

                    # V3: Mark position opens on universe manager (prevents WS disconnect)
                    if self.universe_manager:
                        for sym in self.signal_engine.open_positions:
                            self.universe_manager.mark_position_opened(sym)

                    elapsed = time.time() - t0
                    await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

            except asyncio.CancelledError:
                logger.info("Trading loop cancelled")
            except Exception as e:
                logger.critical(f"UNHANDLED: {e}", exc_info=True)
                await tg_send(f"H2 V3 CRASH: {e}", session)

            await self.price_feed.stop()
            if not self.shadow:
                await order_feed.stop()

    async def _handle_entry(self, signal, snap, session, gateway, entry_flow: EntryFlow):
        """Process entry signal through EntryFlow."""
        sym = signal.symbol
        direction = snap.direction
        # V3: derive symbols from tier engine or price feed map
        pair_info = self._pair_map.get(sym)
        if pair_info:
            bn_sym, bb_sym = pair_info[0], pair_info[1]
        else:
            bb_sym = sym
            bn_sym = self.price_feed._bn_map.get(sym, sym.replace("USDT", "USDC"))
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

        # V3: per-tier position sizing
        tier_info = getattr(signal, '_tier_info', None)
        size_mult = tier_info.position_size_mult if tier_info else 1.0
        position_usd = POSITION_USD * size_mult
        canonical_qty = position_usd / avg_price
        qty_bb = bb_rules.round_qty(canonical_qty)
        qty_bn = bn_rules.round_qty(canonical_qty)
        hedge_qty = min(qty_bb, qty_bn)

        # HIGH FIX (Codex #6): Validate min-notional and min-qty BEFORE submit
        # If either venue would reject, skip entirely (no one-leg-fills → unwind)
        if hedge_qty <= 0:
            return
        if not bb_rules.check_notional(hedge_qty, avg_price):
            logger.debug(f"{sym} skip: BB notional too low ({hedge_qty * avg_price:.2f})")
            return
        if not bn_rules.check_notional(hedge_qty, avg_price):
            logger.debug(f"{sym} skip: BN notional too low ({hedge_qty * avg_price:.2f})")
            return

        # Inventory check for sell-side (pre-check only, lock happens inside asyncio Lock)
        if direction == "BUY_BB_SELL_BN":
            if not self.inventory.can_sell(sym, hedge_qty):
                now = time.time()
                if now >= self._entry_reject_cooldown.get(sym, 0):
                    logger.warning(f"Insufficient inventory for {sym}: need {hedge_qty}")
                    self._entry_reject_cooldown[sym] = now + 300
                return
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

        # BB pricing: PostOnly joins the book (maker), IOC crosses the spread (taker)
        if BYBIT_ENTRY_MODE == "passive":
            # PostOnly: join the book — bid for buys, ask for sells
            # This ensures the order rests and earns maker fee (0.02%)
            if bb_side == "Buy":
                price_bb = bb_rules.round_price_for_side(snap.bb_bid, bb_side)
            else:
                price_bb = bb_rules.round_price_for_side(snap.bb_ask, bb_side)
        else:
            # IOC: cross the spread — ask for buys, bid for sells
            price_bb = bb_rules.round_price_for_side(
                snap.bb_ask if direction == "BUY_BB_SELL_BN" else snap.bb_bid, bb_side)

        # BN pricing: always cross (maker=taker on Binance, no benefit to posting)
        price_bn = bn_rules.round_price_for_side(
            snap.bn_bid if direction == "BUY_BB_SELL_BN" else snap.bn_ask, bn_side)

        # Spread verification callback for PostOnly sequential entry
        def _verify_spread(symbol, signal_spread, threshold_p90):
            """Check if spread is still alive after BB fill, before BN submit."""
            current_snap = self.price_feed.get_spread(symbol) if self.price_feed else None
            if not current_snap:
                return True  # Can't verify, proceed optimistically
            min_spread = signal_spread * POSTONLY_SPREAD_VERIFY_THRESHOLD
            if current_snap.spread_bps < min_spread:
                logger.warning(
                    f"Spread verification FAILED: {symbol} "
                    f"current={current_snap.spread_bps:.0f}bp < min={min_spread:.0f}bp "
                    f"(signal was {signal_spread:.0f}bp)"
                )
                return False
            return True

        # Acquire per-symbol lock (covers entire entry flow)
        # FIX E2: inventory lock INSIDE asyncio Lock to prevent double-lock race
        async with self.position_store.lock_for(sym):
            if direction == "BUY_BB_SELL_BN":
                if not self.inventory.can_sell(sym, hedge_qty):
                    return  # Re-check under lock — another coroutine may have taken it
                self.inventory.lock(sym, hedge_qty)
            result = await entry_flow.execute(
                symbol=sym, bn_symbol=bn_sym, direction=direction,
                bb_side=bb_side, bn_side=bn_side,
                qty_bb=hedge_qty, qty_bn=hedge_qty,
                price_bb=price_bb, price_bn=price_bn,
                signal_spread_bps=snap.spread_bps,
                threshold_p90=signal.threshold_p90,
                threshold_p25=signal.threshold_p25,
                bb_order_mode=BYBIT_ENTRY_MODE,
                spread_verify_fn=_verify_spread if BYBIT_ENTRY_MODE == "passive" else None,
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

            # Update inventory — HIGH FIX (Codex #7): pass fee data to prevent phantom inventory
            if bn_side == "Sell":
                self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price, fee_in_base=True)
            else:
                # BN buy: fee is in base token (deducted from received qty)
                self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price, fee_in_base=True)
            self.inventory.set_lifecycle_state(sym, "ACTIVE")

            open_count = len(self.signal_engine.open_positions)
            base = sym.replace("USDT", "")
            excess = signal.threshold_p90 - signal.threshold_p25
            await tg_send(
                f"\U0001f4dd <b>H2 V3 OPEN</b> <code>{base}</code> [{open_count}/{MAX_CONCURRENT}]\n"
                f"Spread: <b>{result.actual_spread_bps:.0f}bp</b> (P90={signal.threshold_p90:.0f})\n"
                f"Exit@{signal.threshold_p25:.0f}bp | Excess: {excess:.0f}bp (fees~{FEE_RT_BPS:.0f}bp)\n"
                f"Slip: {result.slippage_bps:.1f}bp | Lat: {result.latency_ms:.0f}ms\n"
                f"Size: ${POSITION_USD:.0f}/side | Dir: {direction}",
                session,
            )
        else:
            # Release inventory lock on failure — but NOT for RISK_PAUSE
            # MEDIUM FIX (Codex C4 #6): If unwind failed, inventory may still be in use
            if direction == "BUY_BB_SELL_BN" and result.outcome != "RISK_PAUSE":
                self.inventory.release(sym, hedge_qty)

            if result.outcome == "RISK_PAUSE":
                # CRITICAL FIX (Codex #3): RISK_PAUSE = catastrophic state, must stop trading
                logger.critical(f"RISK_PAUSE on entry for {sym}: unwind may have failed, naked leg possible")
                self.risk_manager._paused = True
                await tg_send(
                    f"\U0001f6d1 <b>RISK_PAUSE</b> <code>{sym.replace('USDT','')}</code>\n"
                    f"Entry unwind FAILED — naked leg may be open.\n"
                    f"Trading PAUSED. Manual intervention required.",
                    session,
                )
            elif result.outcome == "LEG_FAILURE":
                self.risk_manager.record_trade(
                    result.unwind_pnl_usd,
                    0.0,
                    abs(result.slippage_bps),
                    leg_failure=True,
                )
                base = sym.replace("USDT", "")
                consec = self.risk_manager.status().get('consecutive_leg_failures', '?')
                await tg_send(
                    f"\u26a0\ufe0f <b>H2 V3 LEG FAIL</b> <code>{base}</code>\n"
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

        bb_sym = self._pair_map[sym][1]
        bn_sym = self._pair_map[sym][0]
        bb_rules = self.instrument_rules.get("bybit", bb_sym)
        bn_rules = self.instrument_rules.get("binance", bn_sym)

        if bb_rules and qty_bb > 0:
            qty_bb = bb_rules.round_qty(qty_bb)
        if bn_rules and qty_bn > 0:
            qty_bn = bn_rules.round_qty(qty_bn)

        # BB exit pricing: PostOnly joins book (attempt 1), IOC crosses (attempt 2+)
        if BYBIT_ENTRY_MODE == "passive" and bb_rules:
            # PostOnly: join the book — ask for sells, bid for buys
            if bb_side == "Sell":
                price_bb = bb_rules.round_price_for_side(snap.bb_ask, bb_side)
            else:
                price_bb = bb_rules.round_price_for_side(snap.bb_bid, bb_side)
        elif bb_rules:
            price_bb = bb_rules.round_price_for_side(
                snap.bb_bid if bb_side == "Sell" else snap.bb_ask, bb_side)
        else:
            price_bb = snap.bb_bid
        # BN exit pricing: always cross (maker=taker)
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
                bb_order_mode=BYBIT_ENTRY_MODE,
            )

        if result.outcome == "SUCCESS":
            self.signal_engine.unregister_position(sym)
            # Update inventory (buy-back)
            if bn_side == "Buy" and result.bn_filled_qty > 0:
                self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
            elif bn_side == "Sell" and result.bn_filled_qty > 0:
                self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price)
            self.inventory.set_lifecycle_state(sym, "ACTIVE")

            self.risk_manager.record_trade(
                result.pnl_net_usd,
                getattr(result, "fees_usd", 0.0),
                abs(getattr(result, "slippage_bps", 0.0)),
                leg_failure=False,
            )
            self.inventory_guard.record_trade_capture(sym, result.pnl_net_usd)
            # Real-time effective cost tracking
            if hasattr(result, 'pnl_net_bps'):
                # Effective cost = gross - net (in bps)
                gross_bps = result.pnl_net_bps + (FEE_RT_BPS if result.pnl_net_bps != 0 else 0)
                effective_cost = gross_bps - result.pnl_net_bps if gross_bps != 0 else FEE_RT_BPS
                self._realized_costs.append(effective_cost)
                if len(self._realized_costs) > self._realized_cost_window:
                    self._realized_costs = self._realized_costs[-self._realized_cost_window:]

            base = sym.replace("USDT", "")
            emoji = "\u2705" if result.pnl_net_bps > 0 else "\u274c"
            # Get entry spread from pos_doc
            entry_spread = entry.get("actual_spread_bps", pos_doc.get("signal_spread_bps", 0)) or 0
            exit_spread = result.actual_exit_spread_bps if hasattr(result, 'actual_exit_spread_bps') else 0
            hold_s = time.time() - pos_doc.get("entry_time", time.time())
            safe_reason = signal.signal_type.replace("<", "&lt;").replace(">", "&gt;")
            await tg_send(
                f"\U0001f4dd <b>H2 V3 CLOSE</b> {emoji} <code>{base}</code>\n"
                f"Entry: {entry_spread:.0f}bp \u2192 Exit: {exit_spread:.0f}bp\n"
                f"Net: <b>{result.pnl_net_bps:+.0f}bp (${result.pnl_net_usd:.3f})</b>\n"
                f"Hold: {hold_s/60:.0f}min | {safe_reason}\n"
                f"Lat: {result.latency_ms:.0f}ms",
                session,
            )

        elif result.outcome == "PARTIAL":
            # Update inventory immediately for whichever leg filled (don't wait for full close)
            # MEDIUM FIX (Codex #9): Guard against fill_price=0 corrupting cost basis
            if hasattr(result, 'bn_filled_qty') and result.bn_filled_qty > 0 and result.bn_fill_price > 0:
                if bn_side == "Buy":
                    self.inventory.bought(sym, result.bn_filled_qty, fill_price=result.bn_fill_price, fee_in_base=True)
                elif bn_side == "Sell":
                    self.inventory.sold(sym, result.bn_filled_qty, fill_price=result.bn_fill_price, fee_in_base=True)
            self.inventory.set_lifecycle_state(sym, "ACTIVE")
            await tg_send(
                f"\u26a0\ufe0f <b>H2 V3 PARTIAL EXIT</b> <code>{sym.replace('USDT', '')}</code> -- retrying next cycle",
                session,
            )

        elif result.outcome == "RISK_PAUSE":
            # CRITICAL FIX (Codex #3): Exit RISK_PAUSE = naked leg, must stop
            logger.critical(f"RISK_PAUSE on exit for {sym}: naked leg may be open")
            self.risk_manager._paused = True
            await tg_send(
                f"\U0001f6d1 <b>EXIT RISK_PAUSE</b> <code>{sym.replace('USDT','')}</code>\n"
                f"Exit failed catastrophically. Naked leg possible.\n"
                f"Trading PAUSED. Manual intervention required.",
                session,
            )

        elif result.outcome == "FAILED":
            logger.warning(f"Exit failed for {sym}, will retry next cycle")

    async def _check_inventory_guard(self, session):
        """Mark to market and check if inventory impairment exceeds alpha."""
        prices: dict[str, float] = {}
        missing_price_symbols: dict[str, str] = {}
        for sym, inv in self.inventory.inventories.items():
            self._sync_inventory_guard_pair(sym)
            if inv.expected_qty <= 0:
                continue
            snap = self.price_feed.get_spread_for_exit(sym) if self.price_feed else None
            if snap and snap.bn_bid > 0 and snap.bn_ask > 0:
                prices[sym] = (snap.bn_bid + snap.bn_ask) / 2
                continue
            bn_sym = self._pair_map.get(sym, (sym.replace("USDT", "USDC"),))[0]
            missing_price_symbols[sym] = bn_sym

        if missing_price_symbols:
            try:
                async with session.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    tickers = await resp.json()
                px_map = {t.get("symbol"): float(t.get("price", 0)) for t in tickers if "symbol" in t}
                for sym, bn_sym in missing_price_symbols.items():
                    px = px_map.get(bn_sym, 0)
                    if px > 0:
                        prices[sym] = px
            except Exception as e:
                logger.debug(f"Inventory guard REST price fallback failed: {e}")

        self.inventory_guard.mark_to_market(prices)
        should_pause, reason = self.inventory_guard.should_pause()
        if should_pause:
            logger.warning(f"INVENTORY GUARD: {reason}")
            await tg_send(f"H2 V3 INVENTORY GUARD: {reason}", session)
            # Don't auto-pause yet -- just alert. Alberto decides.

    async def _check_slippage_alerts(self, session):
        """S3+S4: Check per-pair rolling slippage from closed trades. Alert if avg > 5bp."""
        try:
            pipeline = [
                {"$match": {"state": "CLOSED", "created_at": {"$gte": V3_EPOCH}}},
                {"$group": {
                    "_id": "$symbol",
                    "count": {"$sum": 1},
                    "avg_entry_slip": {"$avg": {"$ifNull": ["$entry.slippage_bps", 0]}},
                }},
                {"$match": {"count": {"$gte": 5}}},  # Only alert after 5+ trades
            ]
            results = list(self._positions_coll.aggregate(pipeline))
            for r in results:
                sym = r["_id"]
                avg_slip = abs(r.get("avg_entry_slip", 0))
                if avg_slip > 5.0:
                    logger.warning(f"SLIPPAGE ALERT: {sym} avg entry slippage {avg_slip:.1f}bp over {r['count']} trades")
                    await tg_send(
                        f"\u26a0\ufe0f Slippage alert: <b>{sym.replace('USDT','')}</b> "
                        f"avg {avg_slip:.1f}bp over {r['count']} trades",
                        session,
                    )
        except Exception as e:
            logger.debug(f"Slippage alert check failed: {e}")

    async def _check_degraded_mode(self, session, ws_available):
        """Degraded-mode playbook: deterministic actions for failure conditions."""
        try:
            issues = []
            # Check Bybit WS
            if not ws_available.get("bybit", False):
                issues.append("BB_WS_DOWN")
            # Check price feed staleness — only alert if MAJORITY of feeds are truly dead (>30s)
            # Low-vol USDC pairs naturally tick every 5-15s, so 5s threshold causes false alarms
            stale_count = 0
            total_checked = len(self._active_pairs)
            for sym in self._active_pairs:
                fs = self.price_feed.status().get(sym, {})
                bb_age = fs.get("bb_age_ms", -1)
                bn_age = fs.get("bn_age_ms", -1)
                # 30s = truly dead, not just slow-ticking
                if bb_age > 30000 or bn_age > 30000 or (bb_age < 0 and bn_age < 0):
                    stale_count += 1
            if total_checked > 0 and stale_count > total_checked * 0.5:
                issues.append(f"FEEDS_STALE({stale_count}/{total_checked})")
            # Check MongoDB
            try:
                self._positions_coll.find_one({}, {"_id": 1})
            except Exception:
                issues.append("MONGO_DOWN")
            # Check for idle system — no trades in last 6 hours
            last_trade = self._positions_coll.find_one(
                {"state": "CLOSED"}, sort=[("exit_time", -1)],
                projection={"exit_time": 1},
            )
            if last_trade:
                last_exit = last_trade.get("exit_time", 0)
                if isinstance(last_exit, (int, float)) and time.time() - last_exit > 6 * 3600:
                    hours_idle = (time.time() - last_exit) / 3600
                    issues.append(f"IDLE({hours_idle:.0f}h)")

            # Check for stuck positions (ENTERING/EXITING for > 5 min)
            stuck = list(self._positions_coll.find({
                "state": {"$in": ["ENTERING", "EXITING", "UNWINDING"]},
                "entry_time": {"$lt": time.time() - 300},
            }))
            if stuck:
                issues.append(f"STUCK_POSITIONS({len(stuck)})")

            if issues:
                severity = "CRITICAL" if "MONGO_DOWN" in issues or "STUCK_POSITIONS" in str(issues) else "WARNING"
                msg = f"DEGRADED: {', '.join(issues)}"
                logger.warning(msg)
                if severity == "CRITICAL":
                    self.risk_manager._paused = True
                    await tg_send(f"\U0001f6d1 {msg}\nTrading PAUSED.", session)
                else:
                    await tg_send(f"\u26a0\ufe0f {msg}", session)
        except Exception as e:
            logger.debug(f"Degraded mode check failed: {e}")

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
            f"H2 V3 ({uptime_h:.1f}h) | Active: {active} | "
            f"Trades: {risk['total_trades']} | PnL: ${risk['total_pnl_usd']:.2f} | "
            f"Inv M2M: ${guard['total_impairment_usd']:+.2f} | "
            f"Capture: ${guard['total_capture_usd']:.2f} | "
            f"RetireQ: {len(self._retire_queue)}"
        )

    def _build_report_text(self) -> str:
        """Build hypercare report for V3 dynamic screener."""
        uptime_h = (time.time() - self._start_time) / 3600
        risk = self.risk_manager.status()
        mode = "SHADOW" if self.shadow else "LIVE"
        guard = self.inventory_guard.status()

        # V3: filter trades by V3 epoch (exclude V2 history, survive restarts)
        v3_filter = {"state": "CLOSED", "created_at": {"$gte": V3_EPOCH}}
        closed = list(self._positions_coll.find(v3_filter).sort("close_time", -1))
        all_closed = list(self._positions_coll.find({"state": "CLOSED"}).sort("close_time", -1))
        open_docs = list(self._positions_coll.find({"state": "OPEN"}).sort("created_at", -1))

        total_trades = len(closed)
        wins = sum(1 for t in closed if t.get("pnl", {}).get("net_usd", 0) > 0)
        total_pnl = sum(t.get("pnl", {}).get("net_usd", 0) for t in closed)
        wr = wins / total_trades if total_trades else 0

        can_trade = self.risk_manager.can_trade
        se = "\U0001f7e2" if can_trade else "\U0001f534"

        lines = [
            f"<b>H2 V3 Dynamic Screener {mode}</b> ({uptime_h:.1f}h) {se}",
            f"Session: {total_trades} trades | WR: {wr:.0%} | PnL: <b>${total_pnl:.4f}</b>",
            f"All-time: {len(all_closed)} trades | ${sum(t.get('pnl',{}).get('net_usd',0) for t in all_closed):.4f}",
            f"Trading: {'YES' if can_trade else 'PAUSED'} | Leg fails: {risk['leg_failures']}",
        ]

        # Tiers
        lines.append("")
        lines.append("<b>Tiers:</b>")
        lines.append(self.tier_engine.summary())

        # Open positions with full details
        if open_docs:
            lines.append("")
            lines.append("<b>Open Positions:</b>")
            for doc in open_docs:
                sym = doc.get("symbol", "?")
                base = sym.replace("USDT", "")
                entry = doc.get("entry", {})
                bb_entry = entry.get("bb", {}).get("avg_fill_price", 0)
                bn_entry = entry.get("bn", {}).get("avg_fill_price", 0)
                entry_spread = entry.get("actual_spread_bps", doc.get("signal_spread_bps", 0))
                direction = doc.get("direction", "?")
                hold_s = time.time() - doc.get("entry_time", time.time())
                hold_m = hold_s / 60

                # Current price + uPnL
                snap = self.price_feed.get_spread(sym) if self.price_feed else None
                if not snap and self.price_feed:
                    snap = self.price_feed.get_spread_for_exit(sym)

                # Exit targets from signal engine
                opos = self.signal_engine.open_positions.get(sym)
                exit_p25 = opos.exit_threshold_p25 if opos else 0

                if snap:
                    now_spread = snap.spread_bps
                    upnl_bps = entry_spread - now_spread - FEE_RT_BPS
                    upnl_usd = upnl_bps / 10000 * POSITION_USD * 2
                    e = "\U0001f7e2" if upnl_bps > 0 else "\U0001f534"
                    lines.append(
                        f"  {e} <b>{base}</b> {direction[:7]}"
                    )
                    lines.append(
                        f"    Entry: BB={bb_entry:.6f} BN={bn_entry:.6f} ({entry_spread:.0f}bp)"
                    )
                    lines.append(
                        f"    Now: {now_spread:.0f}bp | Exit@{exit_p25:.0f}bp"
                    )
                    lines.append(
                        f"    uPnL: {upnl_bps:+.0f}bp (${upnl_usd:+.4f}) | Hold: {hold_m:.0f}min"
                    )
                else:
                    lines.append(f"  \u23f3 <b>{base}</b> entry={entry_spread:.0f}bp hold={hold_m:.0f}min (no price)")

        # Closed positions (this session)
        if closed:
            lines.append("")
            lines.append("<b>Closed (this session):</b>")
            for t in closed[:10]:  # last 10
                sym = t.get("symbol", "?").replace("USDT", "")
                pnl_net = t.get("pnl", {}).get("net_usd", 0)
                pnl_bps = t.get("pnl", {}).get("net_bps", 0)
                reason = t.get("exit", {}).get("reason", "?")
                hold = t.get("hold_seconds", 0) / 60
                e = "\U0001f7e2" if pnl_net > 0 else "\U0001f534"
                lines.append(f"  {e} {sym}: ${pnl_net:+.4f} ({pnl_bps:+.0f}bp) {reason} {hold:.0f}min")

        # Inventory M2M
        inv_status = self.inventory.status()
        total_cost = 0
        total_mkt = 0
        lines.append("")
        lines.append("<b>Inventory M2M:</b>")
        # Use REST prices for M2M (WS single-venue fallback was returning wrong prices)
        m2m_prices = self._rest_prices_cache if hasattr(self, '_rest_prices_cache') else {}
        for sym in sorted(inv_status.keys()):
            iv = inv_status.get(sym, {})
            avail = iv.get("available", 0)
            cost = iv.get("cost_basis_usd", 0)
            if avail <= 0:
                continue
            lifecycle = self.inventory.inventories.get(sym).lifecycle_state if self.inventory.inventories.get(sym) else "ACTIVE"
            bn_sym = self._pair_map.get(sym, (sym.replace("USDT", "USDC"),))[0]
            mid = m2m_prices.get(bn_sym, 0)
            # Fallback to spread if REST cache empty
            if mid <= 0 and self.price_feed:
                snap = self.price_feed.get_spread(sym)
                if snap:
                    mid = (snap.bn_bid + snap.bn_ask) / 2
            if mid > 0:
                mkt = avail * mid
                upnl = mkt - cost if cost > 0 else 0
                total_cost += cost
                total_mkt += mkt
                base = sym.replace("USDT", "")
                e = "\U0001f7e2" if upnl >= 0 else "\U0001f534"
                lines.append(f"  {e} {base} [{lifecycle}]: ${mkt:.2f} (cost ${cost:.2f}) uPnL ${upnl:+.2f}")
            else:
                total_cost += cost
                total_mkt += cost
                base = sym.replace("USDT", "")
                lines.append(f"  \u23f3 {base} [{lifecycle}]: cost ${cost:.2f} (no price)")

        total_inv_upnl = total_mkt - total_cost
        e = "\U0001f7e2" if total_inv_upnl >= 0 else "\U0001f534"
        lines.append(f"  {e} <b>Total: ${total_mkt:.2f} (cost ${total_cost:.2f}) uPnL ${total_inv_upnl:+.2f}</b>")

        # Bottom line
        cap = guard.get("total_capture_usd", 0)
        total_return = total_pnl + total_inv_upnl
        e = "\U0001f7e2" if total_return >= 0 else "\U0001f534"
        lines.append("")
        lines.append(f"{e} <b>Net Return: ${total_return:+.2f}</b> (trades ${total_pnl:+.4f} + inv ${total_inv_upnl:+.2f})")

        return "\n".join(lines)

    async def _send_telegram_report(self, session):
        """Send equity chart + full report as 2 separate messages."""
        # Fetch REST prices for accurate M2M (WS prices unreliable for low-vol pairs)
        try:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/price",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                tickers = await resp.json()
            self._rest_prices_cache = {t["symbol"]: float(t["price"]) for t in tickers}
        except Exception as e:
            logger.warning(f"REST price fetch for M2M failed: {e}")
            self._rest_prices_cache = {}

        report = self._build_report_text()

        # 1. Chart (photo with minimal caption) — V3 trades only
        closed = list(self._positions_coll.find({"state": "CLOSED", "created_at": {"$gte": V3_EPOCH}}).sort("close_time", 1))
        chart_path = render_equity_curve_live(closed) if closed else None
        if chart_path:
            await tg_send_photo(chart_path, "\U0001f4ca H2 V3 Equity Curve", session)

        # 2. Full report (text message, no caption limit)
        await tg_send(report, session)

    def _build_stats_text(self) -> str:
        """Build per-pair analytics text from closed trades and fill analytics."""
        lines = ["\U0001f4ca <b>H2 V3 Per-Pair Stats</b>"]

        # Aggregate closed trades by symbol
        pipeline = [
            {"$match": {"state": "CLOSED", "created_at": {"$gte": V3_EPOCH}}},
            {"$group": {
                "_id": "$symbol",
                "trades": {"$sum": 1},
                "wins": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$pnl.net_usd", 0]}, 0]}, 1, 0]}},
                "total_pnl": {"$sum": {"$ifNull": ["$pnl.net_usd", 0]}},
                "avg_net_bps": {"$avg": {"$ifNull": ["$pnl.net_bps", 0]}},
                "avg_hold_s": {"$avg": {"$ifNull": ["$hold_seconds", 0]}},
                "avg_entry_slip": {"$avg": {"$ifNull": ["$entry.slippage_bps", 0]}},
            }},
            {"$sort": {"total_pnl": -1}},
        ]

        try:
            results = list(self._positions_coll.aggregate(pipeline))
        except Exception as e:
            return f"Stats query failed: {e}"

        if not results:
            lines.append("No V3 trades yet.")
            return "\n".join(lines)

        # Fill analytics summary
        analytics_coll = self._positions_coll.database["arb_h2_fill_analytics"]
        try:
            fill_pipeline = [
                {"$match": {"type": "entry"}},
                {"$group": {
                    "_id": "$outcome",
                    "count": {"$sum": 1},
                }},
            ]
            fill_results = {r["_id"]: r["count"] for r in analytics_coll.aggregate(fill_pipeline)}
            total_attempts = sum(fill_results.values())
            successes = fill_results.get("SUCCESS", 0)
            fill_rate = (successes / total_attempts * 100) if total_attempts > 0 else 0
            lines.append(f"Fill rate: {successes}/{total_attempts} ({fill_rate:.0f}%)")
            if fill_results.get("LEG_FAILURE", 0):
                lines.append(f"Leg failures: {fill_results['LEG_FAILURE']}")
            if fill_results.get("BOTH_MISSED", 0):
                lines.append(f"Both missed: {fill_results['BOTH_MISSED']}")
            # PostOnly-specific metrics
            po_rejected = fill_results.get("POSTONLY_REJECTED", 0)
            po_timeout = fill_results.get("POSTONLY_TIMEOUT", 0)
            po_partial = fill_results.get("POSTONLY_PARTIAL", 0)
            po_collapsed = fill_results.get("SPREAD_COLLAPSED", 0)
            po_slo = fill_results.get("NAKED_LEG_SLO", 0)
            po_total = po_rejected + po_timeout + po_partial + po_collapsed + po_slo
            if po_total > 0 or successes > 0:
                po_success_rate = (successes / (successes + po_total) * 100) if (successes + po_total) > 0 else 0
                lines.append(f"PostOnly: {po_success_rate:.0f}% success | rej:{po_rejected} timeout:{po_timeout} partial:{po_partial} collapse:{po_collapsed} slo:{po_slo}")
        except Exception:
            pass  # Analytics collection may not exist yet

        lines.append("")
        total_trades = 0
        total_pnl = 0.0

        for r in results:
            sym = r["_id"].replace("USDT", "")
            trades = r["trades"]
            wins = r["wins"]
            wr = wins / trades * 100 if trades > 0 else 0
            pnl = r["total_pnl"]
            avg_bps = r["avg_net_bps"]
            avg_hold = r["avg_hold_s"]
            avg_slip = r["avg_entry_slip"]
            hold_str = f"{avg_hold / 60:.0f}m" if avg_hold < 3600 else f"{avg_hold / 3600:.1f}h"

            e = "\U0001f7e2" if pnl > 0 else "\U0001f534"
            lines.append(
                f"{e} <b>{sym}</b>: {trades}T {wr:.0f}%WR "
                f"${pnl:+.4f} ({avg_bps:+.0f}bp avg) "
                f"slip:{avg_slip:.0f}bp hold:{hold_str}"
            )
            total_trades += trades
            total_pnl += pnl

        lines.append("")
        lines.append(f"<b>Total: {total_trades}T ${total_pnl:+.4f}</b>")

        return "\n".join(lines)

    async def _send_stats_report(self, session):
        """Send per-pair stats report via Telegram."""
        try:
            report = self._build_stats_text()
            await tg_send(report, session)
        except Exception as e:
            logger.warning(f"Stats report failed: {e}")
            await tg_send(f"Stats report error: {e}", session)


# ── Entry point ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="H2 V3 Live Trader")
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
