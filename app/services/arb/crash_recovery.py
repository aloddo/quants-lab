"""
H2 CrashRecovery — Startup state reconciliation.

On startup or after a crash, reconcile local state (MongoDB) with
exchange state (Bybit positions + Binance balances).

Key semantics:
- Bybit perp: position = live directional exposure. Orphans get closed.
- Binance spot: balance = inventory bucket. Reconcile against inventory ledger targets.
"""
import logging
import time
from dataclasses import dataclass

import aiohttp
from pymongo import MongoClient

from app.services.arb.inventory_ledger import InventoryLedger

logger = logging.getLogger(__name__)


@dataclass
class RecoveryReport:
    """Results of crash recovery."""
    bybit_orphans_closed: int = 0
    bybit_positions_resumed: int = 0
    bybit_stale_removed: int = 0
    binance_inventory_ok: int = 0
    binance_inventory_short: int = 0
    binance_inventory_excess: int = 0
    orders_cancelled: int = 0
    locks_released: int = 0
    errors: list = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    @property
    def clean(self) -> bool:
        return (
            self.bybit_orphans_closed == 0
            and self.binance_inventory_short == 0
            and len(self.errors) == 0
        )


class CrashRecovery:
    """
    Startup reconciliation between MongoDB state and exchange state.

    Must run BEFORE the main trading loop starts.
    """

    POSITIONS_COLLECTION = "arb_h2_live_positions"

    def __init__(
        self,
        db_uri: str = "mongodb://localhost:27017/quants_lab",
        inventory: InventoryLedger = None,
    ):
        client = MongoClient(db_uri)
        db_name = db_uri.rsplit("/", 1)[-1]
        self._db = client[db_name]
        self._positions = self._db[self.POSITIONS_COLLECTION]
        self._inventory = inventory
        self._orphan_buybacks: list = []

    async def recover(
        self,
        session: aiohttp.ClientSession,
        bybit_key: str,
        bybit_secret: str,
        binance_key: str,
        symbols: list[str],
        binance_secret: str = "",
    ) -> RecoveryReport:
        """
        Full recovery sequence. Call on startup.
        """
        report = RecoveryReport()
        logger.info("=" * 40)
        logger.info("CRASH RECOVERY starting...")

        # ── Step 1: Bybit perp positions ──
        try:
            await self._recover_bybit(session, bybit_key, bybit_secret, symbols, report)
        except Exception as e:
            logger.error(f"Bybit recovery failed: {e}")
            report.errors.append(f"bybit: {e}")

        # ── Step 2: Binance inventory ──
        try:
            await self._recover_binance(session, binance_key, binance_secret, symbols, report)
        except Exception as e:
            logger.error(f"Binance recovery failed: {e}")
            report.errors.append(f"binance: {e}")

        # ── Step 3: Cancel stranded orders ──
        try:
            await self._cancel_stranded_orders(session, bybit_key, bybit_secret, binance_key, binance_secret, symbols, report)
        except Exception as e:
            logger.error(f"Order cleanup failed: {e}")
            report.errors.append(f"orders: {e}")

        # ── Step 4: Release stale inventory locks ──
        if self._inventory:
            self._inventory.release_stale_locks(set())  # no active orders at startup
            report.locks_released += 1

        logger.info(f"CRASH RECOVERY complete: {report}")
        logger.info("=" * 40)
        return report

    async def _recover_bybit(
        self, session, api_key, api_secret, symbols, report: RecoveryReport
    ):
        """
        Compare MongoDB positions with Bybit exchange positions.
        - In MongoDB but not on exchange = already closed, mark as such
        - On exchange but not in MongoDB = orphan, close immediately
        - In both = resume tracking
        """
        # Load our tracked positions
        # Note: position docs use 'bb_order_id' not 'bybit_order_id'
        mongo_open = list(self._positions.find({"status": "OPEN"}))
        mongo_symbols = {doc["symbol"] for doc in mongo_open if doc.get("symbol")}
        logger.info(f"MongoDB has {len(mongo_open)} open positions: {list(mongo_symbols)}")

        # Query Bybit for actual positions using BybitOrderAPI
        exchange_positions = {}
        try:
            from app.services.arb.order_api import BybitOrderAPI
            bb_api = BybitOrderAPI(api_key, api_secret, session)
            for sym in symbols:
                positions = await bb_api.get_positions(sym)
                for pos in positions:
                    size = float(pos.get("size", 0))
                    if size > 0:
                        exchange_positions[sym] = pos
                        logger.info(f"Bybit position found: {sym} {pos.get('side')} size={size}")
        except Exception as e:
            logger.error(f"Bybit position query failed: {e}")
            report.errors.append(f"bybit_positions: {e}")

        # Compare and build resume list
        self.resumed_positions = []  # caller reads this to re-register with signal engine
        for doc in mongo_open:
            sym = doc["symbol"]
            if sym in exchange_positions:
                report.bybit_positions_resumed += 1
                self.resumed_positions.append(doc)
                logger.info(f"Resuming tracked position: {sym} (entry_spread={doc.get('actual_spread_bps', doc.get('entry_spread', 0)):.0f}bp)")
            else:
                # Position in MongoDB but not on exchange — already closed
                self._positions.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": "CLOSED_BY_RECOVERY", "recovered_at": time.time()}},
                )
                report.bybit_stale_removed += 1
                logger.warning(f"Stale position removed: {sym} (not on exchange)")

        # Check for exchange positions NOT in MongoDB (orphans) — CLOSE THEM
        # and replenish Binance inventory if this was a BUY position (we sold spot on entry)
        for sym, pos_data in exchange_positions.items():
            if sym not in mongo_symbols:
                logger.warning(f"ORPHAN detected on Bybit: {sym} — closing immediately")
                try:
                    size = float(pos_data.get("size", 0))
                    bb_side = pos_data.get("side", "")
                    close_side = "Sell" if bb_side == "Buy" else "Buy"
                    await bb_api.submit_order(sym, close_side, size, 0, "market")
                    report.bybit_orphans_closed += 1
                    logger.info(f"Orphan closed: {sym} {close_side} {size}")

                    # Replenish Binance inventory: if orphan was long Bybit,
                    # we must have sold on Binance to enter. Buy it back.
                    if bb_side == "Buy" and self._inventory:
                        inv = self._inventory.inventories.get(sym)
                        if inv:
                            logger.info(
                                f"Orphan replenish: {sym} needs buy-back of ~{size} "
                                f"on Binance to restore inventory"
                            )
                            # Record that inventory was depleted by orphan close
                            # The auto-heal reconcile will snap to actual exchange balance,
                            # but we need to explicitly buy back on Binance
                            self._orphan_buybacks.append({
                                "symbol": sym,
                                "qty": size,
                                "side": "Buy",  # buy back what was sold
                                "reason": "orphan_replenish",
                            })
                except Exception as e:
                    logger.critical(f"FAILED to close orphan {sym}: {e}")
                    report.errors.append(f"orphan_close_{sym}: {e}")

    async def _recover_binance(
        self, session, api_key, api_secret, symbols, report: RecoveryReport
    ):
        """
        Reconcile Binance spot balances against inventory ledger.
        Binance has no "positions" — just balances.
        Use inventory ledger targets as the source of truth.
        """
        if not self._inventory:
            logger.warning("No inventory ledger — skipping Binance recovery")
            return

        # Build Binance API client with proper credentials
        bn_api = None
        if api_key and api_secret:
            try:
                from app.services.arb.order_api import BinanceOrderAPI
                bn_api = BinanceOrderAPI(api_key, api_secret, session)
            except Exception as e:
                logger.warning(f"Binance API client init failed: {e}")

        for sym in symbols:
            inv = self._inventory.inventories.get(sym)
            if not inv:
                continue

            # Query actual balance via Binance REST
            actual_qty = inv.expected_qty  # default fallback
            if bn_api:
                try:
                    base = sym.replace("USDT", "").replace("USDC", "")
                    actual_qty = await bn_api.get_balance(base)
                    logger.info(f"Binance balance for {sym}: {actual_qty}")
                except Exception as e:
                    logger.warning(f"Binance balance query failed for {sym}: {e}")
                    actual_qty = inv.expected_qty  # fallback on error
            else:
                logger.warning(f"Binance balance query skipped for {sym}: no credentials")

            expected = inv.expected_qty - inv.locked_qty

            if abs(actual_qty - expected) < expected * 0.01:
                report.binance_inventory_ok += 1
            elif actual_qty < expected:
                report.binance_inventory_short += 1
                logger.warning(
                    f"Inventory SHORT: {sym} expected={expected} actual={actual_qty} "
                    f"(missing {expected - actual_qty})"
                )
                # Update ledger to match reality
                inv.expected_qty = actual_qty + inv.locked_qty
                inv.discrepancy = inv.target_qty - actual_qty
                self._inventory._save(sym)
            else:
                report.binance_inventory_excess += 1
                logger.info(
                    f"Inventory excess: {sym} expected={expected} actual={actual_qty} "
                    f"(surplus {actual_qty - expected})"
                )
                inv.expected_qty = actual_qty + inv.locked_qty
                self._inventory._save(sym)

    async def _cancel_stranded_orders(
        self, session, bybit_key, bybit_secret, binance_key, binance_secret, symbols, report: RecoveryReport
    ):
        """Cancel any open orders on both exchanges."""
        from app.services.arb.order_api import BybitOrderAPI, BinanceOrderAPI

        # Bybit: cancel all open linear orders
        try:
            bb_api = BybitOrderAPI(bybit_key, bybit_secret, session)
            for sym in symbols:
                orders = await bb_api.get_open_orders(sym)
                for order in orders:
                    oid = order.get("orderId", "")
                    if oid:
                        await bb_api.cancel_order(sym, oid)
                        report.orders_cancelled += 1
                        logger.info(f"Cancelled stranded Bybit order: {sym} {oid}")
        except Exception as e:
            logger.error(f"Bybit order cleanup failed: {e}")
            report.errors.append(f"bybit_orders: {e}")

        # Binance: cancel all open spot orders
        if binance_key:
            try:
                bn_api = BinanceOrderAPI(binance_key, binance_secret, session)
                for sym in symbols:
                    # Map to Binance symbol if needed
                    orders = await bn_api.get_open_orders(sym)
                    for order in orders:
                        oid = str(order.get("orderId", ""))
                        if oid:
                            await bn_api.cancel_order(sym, oid)
                            report.orders_cancelled += 1
                            logger.info(f"Cancelled stranded Binance order: {sym} {oid}")
            except Exception as e:
                logger.error(f"Binance order cleanup failed: {e}")
                report.errors.append(f"binance_orders: {e}")
