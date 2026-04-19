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

    async def recover(
        self,
        session: aiohttp.ClientSession,
        bybit_key: str,
        bybit_secret: str,
        binance_key: str,
        symbols: list[str],
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
            await self._recover_binance(session, binance_key, symbols, report)
        except Exception as e:
            logger.error(f"Binance recovery failed: {e}")
            report.errors.append(f"binance: {e}")

        # ── Step 3: Cancel stranded orders ──
        try:
            await self._cancel_stranded_orders(session, bybit_key, bybit_secret, binance_key, symbols, report)
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
        mongo_open = list(self._positions.find({"status": "OPEN", "bybit_order_id": {"$exists": True}}))
        mongo_symbols = {doc["symbol"] for doc in mongo_open}
        logger.info(f"MongoDB has {len(mongo_open)} open positions")

        # Query Bybit for actual positions
        # (simplified — real implementation needs HMAC signing)
        exchange_positions = {}  # symbol -> position data
        # TODO: implement Bybit REST GET /v5/position/list with auth
        # For now, log the intent
        logger.info("Bybit position query: TODO (needs mainnet API keys)")

        # Compare
        for doc in mongo_open:
            sym = doc["symbol"]
            if sym in exchange_positions:
                report.bybit_positions_resumed += 1
                logger.info(f"Resuming tracked position: {sym}")
            else:
                # Position in MongoDB but not on exchange — already closed
                self._positions.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": "CLOSED_BY_RECOVERY", "recovered_at": time.time()}},
                )
                report.bybit_stale_removed += 1
                logger.warning(f"Stale position removed: {sym} (not on exchange)")

        # Check for exchange positions NOT in MongoDB (orphans)
        for sym, pos_data in exchange_positions.items():
            if sym not in mongo_symbols:
                logger.warning(f"ORPHAN detected on Bybit: {sym} — closing immediately")
                # TODO: close via market order
                report.bybit_orphans_closed += 1

    async def _recover_binance(
        self, session, api_key, symbols, report: RecoveryReport
    ):
        """
        Reconcile Binance spot balances against inventory ledger.
        Binance has no "positions" — just balances.
        Use inventory ledger targets as the source of truth.
        """
        if not self._inventory:
            logger.warning("No inventory ledger — skipping Binance recovery")
            return

        for sym in symbols:
            inv = self._inventory.inventories.get(sym)
            if not inv:
                continue

            # Query actual balance
            # TODO: implement Binance REST /api/v3/account with HMAC signing
            actual_qty = inv.expected_qty  # placeholder until keys configured

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
        self, session, bybit_key, bybit_secret, binance_key, symbols, report: RecoveryReport
    ):
        """Cancel any open orders on both exchanges."""
        # TODO: implement order cancellation with auth
        # Bybit: GET /v5/order/realtime, then DELETE for each open order
        # Binance: GET /api/v3/openOrders, then DELETE for each
        logger.info("Stranded order cleanup: TODO (needs mainnet API keys)")
