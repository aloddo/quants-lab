"""
H2 InventoryLedger — Persistent per-pair inventory tracking on Binance spot.

Tracks expected vs actual balances, handles dust, auto-rebalances on drift.
Reconciles against Binance REST every 5 minutes.

Key semantics (from autoplan review):
- Binance spot has no "positions" — only balances
- Pre-bought inventory is the BASE STATE, not evidence of open trades
- Inventory target = what we should hold when no trade is active
- locked = reserved for a pending sell order
- Recovery checks against inventory ledger, not raw balances
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from pymongo import MongoClient
from pymongo.collection import Collection

logger = logging.getLogger(__name__)


@dataclass
class PairInventory:
    """Inventory state for a single pair on Binance spot."""
    symbol: str             # e.g., "HIGHUSDT"
    base_asset: str         # e.g., "HIGH"
    target_qty: float       # what we should hold when idle
    expected_qty: float     # what we think we have (accounting)
    locked_qty: float = 0.0      # reserved for pending sell orders
    dust_qty: float = 0.0        # accumulated fee deductions
    last_reconciled: float = 0.0
    last_exchange_qty: float = 0.0
    discrepancy: float = 0.0     # expected - actual (positive = we're short)
    # USD cost tracking
    cost_basis_usd: float = 0.0  # total USD spent to acquire current inventory
    avg_cost_per_unit: float = 0.0  # cost_basis / qty
    realized_pnl_usd: float = 0.0  # cumulative PnL from round-trips (spread capture - fees)
    unrealized_pnl_usd: float = 0.0  # mark-to-market vs cost basis

    @property
    def available_qty(self) -> float:
        """Quantity available to sell (not locked)."""
        return max(0, self.expected_qty - self.locked_qty)

    @property
    def is_healthy(self) -> bool:
        """No significant discrepancy."""
        if self.target_qty <= 0:
            return True
        return abs(self.discrepancy) / self.target_qty < 0.05  # <5% drift


# Symbol to base asset mapping
SYMBOL_TO_BASE = {
    "HIGHUSDT": "HIGH",
    "NOMUSDT": "NOM",
    "ALICEUSDT": "ALICE",
    "AXLUSDT": "AXL",
    "PORTALUSDT": "PORTAL",
    "THETAUSDT": "THETA",
    "ILVUSDT": "ILV",
    "ARPAUSDT": "ARPA",
}


class InventoryLedger:
    """
    Persistent inventory tracking with auto-rebalance.

    MongoDB collection: arb_h2_inventory
    One document per pair with full accounting state.
    """

    COLLECTION = "arb_h2_inventory"
    RECONCILE_INTERVAL = 300  # 5 minutes
    DISCREPANCY_ALERT_PCT = 0.05  # 5%
    DISCREPANCY_REBALANCE_PCT = 0.10  # 10%

    def __init__(self, db_uri: str = "mongodb://localhost:27017/quants_lab"):
        client = MongoClient(db_uri)
        db_name = db_uri.rsplit("/", 1)[-1]
        self._coll: Collection = client[db_name][self.COLLECTION]
        self._coll.create_index("symbol", unique=True)
        self.inventories: dict[str, PairInventory] = {}

    def load(self, symbols: list[str], target_qty_usd: float = 20.0):
        """Load inventory state from MongoDB, create if missing."""
        for sym in symbols:
            doc = self._coll.find_one({"symbol": sym})
            base = SYMBOL_TO_BASE.get(sym, sym.replace("USDT", ""))

            if doc:
                self.inventories[sym] = PairInventory(
                    symbol=sym,
                    base_asset=base,
                    target_qty=doc.get("target_qty", 0),
                    expected_qty=doc.get("expected_qty", 0),
                    locked_qty=doc.get("locked_qty", 0),
                    dust_qty=doc.get("dust_qty", 0),
                    last_reconciled=doc.get("last_reconciled", 0),
                    last_exchange_qty=doc.get("last_exchange_qty", 0),
                    discrepancy=doc.get("discrepancy", 0),
                    cost_basis_usd=doc.get("cost_basis_usd", 0),
                    avg_cost_per_unit=doc.get("avg_cost_per_unit", 0),
                    realized_pnl_usd=doc.get("realized_pnl_usd", 0),
                    unrealized_pnl_usd=doc.get("unrealized_pnl_usd", 0),
                )
            else:
                # New pair — target will be set when inventory is pre-bought
                self.inventories[sym] = PairInventory(
                    symbol=sym,
                    base_asset=base,
                    target_qty=0,  # set via initialize()
                    expected_qty=0,
                )

    def _save(self, sym: str):
        """Persist inventory state to MongoDB."""
        inv = self.inventories[sym]
        self._coll.update_one(
            {"symbol": sym},
            {"$set": {
                "symbol": sym,
                "base_asset": inv.base_asset,
                "target_qty": inv.target_qty,
                "expected_qty": inv.expected_qty,
                "locked_qty": inv.locked_qty,
                "dust_qty": inv.dust_qty,
                "last_reconciled": inv.last_reconciled,
                "last_exchange_qty": inv.last_exchange_qty,
                "discrepancy": inv.discrepancy,
                "cost_basis_usd": inv.cost_basis_usd,
                "avg_cost_per_unit": inv.avg_cost_per_unit,
                "realized_pnl_usd": inv.realized_pnl_usd,
                "unrealized_pnl_usd": inv.unrealized_pnl_usd,
                "updated_at": time.time(),
            }},
            upsert=True,
        )

    def initialize(self, symbol: str, qty: float):
        """Set initial inventory target after pre-buying on Binance."""
        inv = self.inventories.get(symbol)
        if inv:
            inv.target_qty = qty
            inv.expected_qty = qty
            inv.last_exchange_qty = qty
            inv.discrepancy = 0
            self._save(symbol)
            logger.info(f"Inventory initialized: {symbol} target={qty} {inv.base_asset}")

    def can_sell(self, symbol: str, qty: float) -> bool:
        """Check if we have enough unlocked inventory to sell."""
        inv = self.inventories.get(symbol)
        if not inv:
            return False
        return inv.available_qty >= qty

    def lock(self, symbol: str, qty: float):
        """Lock inventory for a pending sell order."""
        inv = self.inventories.get(symbol)
        if inv:
            inv.locked_qty += qty
            self._save(symbol)
            logger.debug(f"Locked {qty} {inv.base_asset} (total locked: {inv.locked_qty})")

    def release(self, symbol: str, qty: float):
        """Release locked inventory (order cancelled or failed)."""
        inv = self.inventories.get(symbol)
        if inv:
            inv.locked_qty = max(0, inv.locked_qty - qty)
            self._save(symbol)
            logger.debug(f"Released {qty} {inv.base_asset} (total locked: {inv.locked_qty})")

    def sold(self, symbol: str, qty: float, fee_qty: float = 0, fee_in_base: bool = False,
             fill_price: float = 0.0):
        """
        Record a completed sell (entry leg on Binance spot).
        If fee_in_base=True, the fee was deducted from the token balance (not USDT).
        fill_price: actual fill price for USD cost tracking.
        """
        inv = self.inventories.get(symbol)
        if inv:
            inv.expected_qty -= qty
            if fee_in_base:
                inv.expected_qty -= fee_qty  # fee ate into our balance
            inv.locked_qty = max(0, inv.locked_qty - qty)
            inv.dust_qty += fee_qty
            # USD tracking: reduce cost basis proportionally
            if inv.expected_qty > 0 and inv.cost_basis_usd > 0:
                sold_fraction = qty / (inv.expected_qty + qty)
                inv.cost_basis_usd *= (1 - sold_fraction)
            self._save(symbol)
            logger.info(f"SOLD {qty} {inv.base_asset} @{fill_price:.6f} (fee={fee_qty} base={fee_in_base}) (expected: {inv.expected_qty:.2f})")

    def bought(self, symbol: str, qty: float, fee_qty: float = 0, fill_price: float = 0.0):
        """
        Record a completed buy (exit restoring inventory, or initial purchase).
        fill_price: actual fill price for USD cost tracking.
        """
        inv = self.inventories.get(symbol)
        if inv:
            net_qty = qty - fee_qty  # fee may be deducted from received qty
            inv.expected_qty += net_qty
            # USD tracking: add cost of this purchase
            if fill_price > 0:
                inv.cost_basis_usd += net_qty * fill_price
                if inv.expected_qty > 0:
                    inv.avg_cost_per_unit = inv.cost_basis_usd / inv.expected_qty
            self._save(symbol)
            logger.info(f"BOUGHT {qty} {inv.base_asset} @{fill_price:.6f} (expected: {inv.expected_qty:.2f}, cost_basis: ${inv.cost_basis_usd:.2f})")

    async def reconcile(
        self, symbol: str, session: aiohttp.ClientSession, api_key: str,
        api_secret: str = "",
    ) -> Optional[float]:
        """
        Query actual Binance balance via signed REST and compare to expected.
        Returns discrepancy (positive = we're short, negative = surplus).
        """
        inv = self.inventories.get(symbol)
        if not inv:
            return None

        if not api_key or not api_secret:
            logger.debug(f"Reconcile {symbol}: no Binance credentials, skipping")
            inv.last_reconciled = time.time()
            self._save(symbol)
            return 0.0

        try:
            import hashlib, hmac, time as _time
            from urllib.parse import urlencode

            ts = str(int(_time.time() * 1000))
            params = {"timestamp": ts, "recvWindow": "5000"}
            qs = urlencode(params)
            sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
            params["signature"] = sig

            headers = {"X-MBX-APIKEY": api_key}
            async with session.get(
                "https://api.binance.com/api/v3/account",
                headers=headers,
                params=params,
            ) as resp:
                data = await resp.json()

            if "balances" not in data:
                logger.error(f"Binance account query failed: {data.get('msg', data)}")
                return None

            # Find our base asset balance
            actual_qty = 0.0
            for bal in data["balances"]:
                if bal["asset"] == inv.base_asset:
                    actual_qty = float(bal["free"]) + float(bal["locked"])
                    break

            # Compare to expected
            expected = inv.expected_qty
            disc = expected - actual_qty  # positive = we're short

            inv.last_reconciled = time.time()
            inv.last_exchange_qty = actual_qty
            inv.discrepancy = disc

            # Mark-to-market: compute unrealized PnL vs cost basis
            # We need a current price — use the spread midpoint if available
            if inv.cost_basis_usd > 0 and actual_qty > 0 and inv.avg_cost_per_unit > 0:
                # Approximate current value from last exchange qty * avg cost
                # (real mark-to-market needs a live price feed — this is just cost-basis tracking)
                inv.unrealized_pnl_usd = 0  # Will be computed with live prices in reporting

            self._save(symbol)

            if abs(disc) > 0.001:
                logger.info(
                    f"Reconcile {symbol}: expected={expected:.6f} actual={actual_qty:.6f} "
                    f"disc={disc:.6f} ({abs(disc)/max(inv.target_qty, 1e-10)*100:.1f}%)"
                )

            return disc

        except Exception as e:
            logger.error(f"Reconciliation failed for {symbol}: {e}")
            return None

    async def periodic_reconcile(
        self, session: aiohttp.ClientSession, api_key: str, api_secret: str = "",
        open_positions: set[str] | None = None,
    ):
        """
        Reconcile all pairs. Run every 5 minutes.

        Key behavior: if a pair has NO open position, snap expected_qty to
        the actual exchange balance. The exchange IS the source of truth.
        This handles fee erosion, rounding dust, and failed unwind residuals
        automatically.
        """
        if open_positions is None:
            open_positions = set()

        for sym in self.inventories:
            disc = await self.reconcile(sym, session, api_key, api_secret)
            if disc is None:
                continue

            inv = self.inventories[sym]

            # AUTO-HEAL: If no position is open for this pair, snap to exchange balance.
            # This is the core fix: the ledger tracks the exchange, not the other way around.
            if sym not in open_positions and inv.locked_qty == 0 and abs(disc) > 0.001:
                old_expected = inv.expected_qty
                inv.expected_qty = inv.last_exchange_qty
                inv.discrepancy = 0
                self._save(sym)
                if abs(old_expected - inv.expected_qty) > 0.01:
                    logger.info(
                        f"Inventory auto-healed {sym}: {old_expected:.4f} -> "
                        f"{inv.expected_qty:.4f} (exchange={inv.last_exchange_qty:.4f})"
                    )
            elif abs(disc) > 0:
                pct = abs(disc) / max(inv.expected_qty, 1e-10)
                if pct > self.DISCREPANCY_ALERT_PCT:
                    logger.warning(
                        f"Inventory discrepancy {sym} (position open): "
                        f"expected={inv.expected_qty:.4f} actual={inv.last_exchange_qty:.4f} "
                        f"disc={disc:.4f} ({pct*100:.1f}%)"
                    )

    def release_stale_locks(self, active_order_ids: set[str]):
        """
        Release locks for orders that no longer exist.
        Called during crash recovery.
        """
        # In production, each lock would be tagged with an order_id
        # For now, if no orders are active, release all locks
        for sym, inv in self.inventories.items():
            if inv.locked_qty > 0 and not active_order_ids:
                logger.warning(f"Releasing stale lock: {sym} locked={inv.locked_qty}")
                inv.locked_qty = 0
                self._save(sym)

    def record_round_trip_pnl(self, symbol: str, pnl_usd: float):
        """Record realized PnL from a completed arb round-trip."""
        inv = self.inventories.get(symbol)
        if inv:
            inv.realized_pnl_usd += pnl_usd
            self._save(symbol)
            logger.info(f"Round-trip PnL {inv.base_asset}: ${pnl_usd:.4f} (cumulative: ${inv.realized_pnl_usd:.4f})")

    def mark_to_market(self, symbol: str, current_price: float):
        """Update unrealized PnL based on current market price."""
        inv = self.inventories.get(symbol)
        if inv and inv.cost_basis_usd > 0 and inv.expected_qty > 0:
            current_value = inv.expected_qty * current_price
            inv.unrealized_pnl_usd = current_value - inv.cost_basis_usd
            self._save(symbol)

    def total_pnl_summary(self) -> dict:
        """Return total realized + unrealized PnL across all pairs."""
        total_realized = sum(inv.realized_pnl_usd for inv in self.inventories.values())
        total_unrealized = sum(inv.unrealized_pnl_usd for inv in self.inventories.values())
        total_cost_basis = sum(inv.cost_basis_usd for inv in self.inventories.values())
        return {
            "realized_pnl_usd": round(total_realized, 4),
            "unrealized_pnl_usd": round(total_unrealized, 4),
            "total_pnl_usd": round(total_realized + total_unrealized, 4),
            "total_cost_basis_usd": round(total_cost_basis, 2),
        }

    def status(self) -> dict:
        """Return status of all inventories for monitoring."""
        return {
            sym: {
                "base": inv.base_asset,
                "target": inv.target_qty,
                "expected": inv.expected_qty,
                "locked": inv.locked_qty,
                "available": inv.available_qty,
                "healthy": inv.is_healthy,
                "discrepancy": inv.discrepancy,
                "cost_basis_usd": round(inv.cost_basis_usd, 2),
                "avg_cost": round(inv.avg_cost_per_unit, 6),
                "realized_pnl": round(inv.realized_pnl_usd, 4),
                "unrealized_pnl": round(inv.unrealized_pnl_usd, 4),
            }
            for sym, inv in self.inventories.items()
        }
