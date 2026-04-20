"""
V2 PositionStore — MongoDB-backed position state machine.

SINGLE SOURCE OF TRUTH for all position state. No in-memory copies.
Every transition is atomic with state precondition.
Per-symbol asyncio.Lock prevents concurrent mutations.

Designed for reuse across any cross-venue dual-leg strategy.
"""
import asyncio
import logging
import time
from enum import Enum
from typing import Optional

import pymongo.errors
from pymongo import MongoClient
from pymongo.collection import Collection

logger = logging.getLogger(__name__)


# ── State Enums ────────────────────────────────────────────────

class PositionState(str, Enum):
    PENDING = "PENDING"              # Pre-checks passed, about to submit
    ENTERING = "ENTERING"            # Both legs submitted, waiting for fills
    OPEN = "OPEN"                    # Both legs filled, position active
    EXITING = "EXITING"              # Exit orders submitted
    PARTIAL_EXIT = "PARTIAL_EXIT"    # One exit leg filled, other pending
    UNWINDING = "UNWINDING"          # Emergency: unwinding a naked leg
    CLOSED = "CLOSED"                # Fully closed (terminal)
    FAILED = "FAILED"                # Entry failed or unwind complete (terminal)


class LegState(str, Enum):
    IDLE = "IDLE"
    PENDING_SUBMIT = "PENDING_SUBMIT"   # Client ID generated, not yet submitted
    SUBMITTED = "SUBMITTED"             # REST call sent, exchange order ID received
    PARTIAL = "PARTIAL"                 # Some fills received
    FILLED = "FILLED"                   # Fully filled
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"                 # REST check failed, true state unknown


class FillResult(str, Enum):
    """Four-state return from fill detection. NEVER conflate UNKNOWN with NOT_FILLED."""
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"          # Some fills received, order not fully complete
    NOT_FILLED = "NOT_FILLED"
    NOT_FOUND = "NOT_FOUND"      # Order never existed on exchange (distinct from unfilled)
    UNKNOWN = "UNKNOWN"


# Valid state transitions
VALID_TRANSITIONS = {
    None: {PositionState.PENDING},
    PositionState.PENDING: {PositionState.ENTERING, PositionState.FAILED},
    PositionState.ENTERING: {PositionState.OPEN, PositionState.UNWINDING, PositionState.FAILED},
    PositionState.OPEN: {PositionState.EXITING},
    PositionState.EXITING: {PositionState.CLOSED, PositionState.PARTIAL_EXIT, PositionState.OPEN},
    PositionState.PARTIAL_EXIT: {PositionState.CLOSED, PositionState.EXITING},
    PositionState.UNWINDING: {PositionState.FAILED},
    # Terminal states have no valid transitions
    PositionState.CLOSED: set(),
    PositionState.FAILED: set(),
}


def _empty_leg() -> dict:
    """Template for a leg subdocument."""
    return {
        "order_id": "",
        "client_order_id": "",
        "side": "",
        "target_qty": 0.0,
        "target_price": 0.0,
        "submitted_at": 0.0,
        "filled_qty": 0.0,
        "avg_fill_price": 0.0,
        "fee": 0.0,
        "fee_asset": "",
        "exec_ids": [],
        "state": LegState.IDLE,
        "filled_at": 0.0,
    }


def new_position_doc(
    position_id: str,
    symbol: str,
    bn_symbol: str,
    direction: str,
    signal_spread_bps: float,
    threshold_p90: float,
    threshold_p25: float,
) -> dict:
    """Create a fully-initialized position document."""
    now = time.time()
    return {
        "position_id": position_id,
        "symbol": symbol,
        "bn_symbol": bn_symbol,
        "state": PositionState.PENDING,
        "direction": direction,
        "signal_spread_bps": signal_spread_bps,
        "threshold_p90": threshold_p90,
        "threshold_p25": threshold_p25,
        "entry": {
            "bb": _empty_leg(),
            "bn": _empty_leg(),
            "actual_spread_bps": 0.0,
            "slippage_bps": 0.0,
            "latency_ms": 0.0,
        },
        "exit": {
            "reason": "",
            "bb": _empty_leg(),
            "bn": _empty_leg(),
            "actual_spread_bps": 0.0,
            "latency_ms": 0.0,
            "attempt_count": 0,
        },
        "unwind": {
            "order_id": "",
            "client_order_id": "",
            "side": "",
            "filled_qty": 0.0,
            "avg_fill_price": 0.0,
            "pnl_usd": 0.0,
        },
        "pnl": {
            "gross_bps": 0.0,
            "fees_bps": 0.0,
            "net_bps": 0.0,
            "net_usd": 0.0,
        },
        "created_at": now,
        "updated_at": now,
        "entry_time": 0.0,
        "exit_time": 0.0,
        "hold_seconds": 0.0,
    }


# ── PositionStore ──────────────────────────────────────────────

class PositionStore:
    """
    MongoDB-backed position state machine with per-symbol locking.

    Core invariants:
    1. MongoDB is the ONLY source of truth. No in-memory position cache.
    2. Every state transition is atomic (update_one with state precondition).
    3. Per-symbol asyncio.Lock prevents concurrent entry/exit/recovery.
    4. Global recovery barrier blocks all flows during disconnect recovery.
    """

    def __init__(self, mongo_uri: str, collection_name: str = "arb_h2_positions_v2"):
        client = MongoClient(mongo_uri)
        db_name = mongo_uri.rsplit("/", 1)[-1]
        self._db = client[db_name]
        self._coll: Collection = self._db[collection_name]

        # Per-symbol locks prevent concurrent mutations
        self._locks: dict[str, asyncio.Lock] = {}
        # Global recovery barrier
        self._recovery_lock = asyncio.Lock()

        # Ensure indexes
        self._coll.create_index("position_id", unique=True)
        self._coll.create_index("state")
        self._coll.create_index([("symbol", 1), ("state", 1)])

    def lock_for(self, symbol: str) -> asyncio.Lock:
        """Get or create per-symbol lock. Thread-safe via setdefault."""
        return self._locks.setdefault(symbol, asyncio.Lock())

    @property
    def recovery_lock(self) -> asyncio.Lock:
        """Global lock held during disconnect recovery."""
        return self._recovery_lock

    # ── Create ─────────────────────────────────────────────────

    def _create_sync(self, doc: dict) -> bool:
        """Synchronous create — called via asyncio.to_thread()."""
        try:
            self._coll.insert_one(doc)
            logger.info(f"Position created: {doc['position_id']} state=PENDING")
            return True
        except pymongo.errors.DuplicateKeyError:
            logger.warning(f"Position already exists: {doc.get('position_id')}")
            return False
        except Exception as e:
            logger.critical(f"Position create DB FAILURE: {e} — THIS IS AN OPERATIONAL ERROR")
            raise  # Propagate operational DB failures — caller must handle

    async def create(self, doc: dict) -> bool:
        """
        Insert a new position in PENDING state.
        Returns True on success, False if position_id already exists.
        Raises on operational DB failures (caller must handle).
        """
        return await asyncio.to_thread(self._create_sync, doc)

    # ── Transition ─────────────────────────────────────────────

    def _transition_sync(
        self,
        position_id: str,
        from_state: PositionState,
        to_state: PositionState,
        updates: dict,
    ) -> bool:
        """Synchronous transition — called via asyncio.to_thread()."""
        # Validate transition
        valid = VALID_TRANSITIONS.get(from_state, set())
        if to_state not in valid:
            logger.error(
                f"INVALID transition: {from_state} -> {to_state} "
                f"(valid: {valid}) for {position_id}"
            )
            return False

        updates["state"] = to_state
        updates["updated_at"] = time.time()

        try:
            result = self._coll.update_one(
                {"position_id": position_id, "state": from_state},
                {"$set": updates},
            )
        except Exception as e:
            logger.critical(
                f"Transition DB FAILURE: {position_id} {from_state}->{to_state}: {e} "
                f"— state may be inconsistent, trigger risk pause"
            )
            raise  # Propagate so callers can trigger risk pause

        if result.modified_count == 0:
            # Check if position exists with different state
            try:
                current = self._coll.find_one({"position_id": position_id}, {"state": 1})
            except Exception as e:
                logger.critical(f"Transition verify DB FAILURE: {position_id}: {e}")
                raise
            if current:
                logger.warning(
                    f"Transition REJECTED: {position_id} expected {from_state}, "
                    f"actual {current.get('state')}"
                )
            else:
                logger.error(f"Transition REJECTED: {position_id} not found")
            return False

        logger.info(f"Position {position_id}: {from_state} -> {to_state}")
        return True

    async def transition(
        self,
        position_id: str,
        from_state: PositionState,
        to_state: PositionState,
        updates: dict,
    ) -> bool:
        """
        Atomic state transition with precondition.

        Returns True if transition succeeded, False if precondition failed
        (position was in a different state, or didn't exist).
        """
        return await asyncio.to_thread(self._transition_sync, position_id, from_state, to_state, updates)

    # ── Query ──────────────────────────────────────────────────

    def _get_sync(self, position_id: str) -> Optional[dict]:
        return self._coll.find_one({"position_id": position_id})

    async def get(self, position_id: str) -> Optional[dict]:
        """Get a position by ID."""
        return await asyncio.to_thread(self._get_sync, position_id)

    def _get_by_symbol_sync(self, symbol: str, states: list[PositionState] | None = None) -> list[dict]:
        query = {"symbol": symbol}
        if states:
            query["state"] = {"$in": [s.value for s in states]}
        return list(self._coll.find(query))

    async def get_by_symbol(self, symbol: str, states: list[PositionState] | None = None) -> list[dict]:
        """Get all positions for a symbol, optionally filtered by state."""
        return await asyncio.to_thread(self._get_by_symbol_sync, symbol, states)

    def _get_active_sync(self) -> list[dict]:
        terminal = [PositionState.CLOSED, PositionState.FAILED]
        return list(self._coll.find(
            {"state": {"$nin": [s.value for s in terminal]}}
        ))

    async def get_active(self) -> list[dict]:
        """Get all non-terminal positions."""
        return await asyncio.to_thread(self._get_active_sync)

    def _get_open_sync(self) -> list[dict]:
        return list(self._coll.find({"state": PositionState.OPEN}))

    async def get_open(self) -> list[dict]:
        """Get all OPEN positions (ready for exit signals)."""
        return await asyncio.to_thread(self._get_open_sync)

    def _get_by_symbol_states_sync(self, states: list[PositionState]) -> list[dict]:
        """Get all positions matching any of the given states."""
        return list(self._coll.find({"state": {"$in": [s.value for s in states]}}))

    async def get_by_symbol_states(self, states: list[PositionState]) -> list[dict]:
        """Get all positions matching any of the given states."""
        return await asyncio.to_thread(self._get_by_symbol_states_sync, states)

    def _get_open_symbols_sync(self) -> set[str]:
        active = self._get_active_sync()
        return {doc["symbol"] for doc in active}

    async def get_open_symbols(self) -> set[str]:
        """Get symbols with active (non-terminal) positions."""
        return await asyncio.to_thread(self._get_open_symbols_sync)

    def _count_active_sync(self) -> int:
        terminal = [PositionState.CLOSED, PositionState.FAILED]
        return self._coll.count_documents(
            {"state": {"$nin": [s.value for s in terminal]}}
        )

    async def count_active(self) -> int:
        """Count non-terminal positions."""
        return await asyncio.to_thread(self._count_active_sync)

    # ── Convenience Updates (use transition() internally) ──────

    def _update_leg_sync(
        self,
        position_id: str,
        phase: str,
        venue: str,
        updates: dict,
        expected_state: Optional[PositionState] = None,
    ) -> bool:
        """Synchronous update_leg — called via asyncio.to_thread()."""
        if venue:
            set_dict = {f"{phase}.{venue}.{k}": v for k, v in updates.items()}
        else:
            set_dict = {f"{phase}.{k}": v for k, v in updates.items()}
        set_dict["updated_at"] = time.time()

        query: dict = {"position_id": position_id}
        if expected_state is not None:
            query["state"] = expected_state

        result = self._coll.update_one(
            query,
            {"$set": set_dict},
        )
        return result.modified_count > 0

    async def update_leg(
        self,
        position_id: str,
        phase: str,     # "entry" or "exit"
        venue: str,     # "bb" or "bn"
        updates: dict,
        expected_state: Optional[PositionState] = None,
    ) -> bool:
        """
        Update a specific leg's fields without changing position state.
        Used for recording fills, order IDs, etc.

        If expected_state is provided, the update only applies if the position
        is in that state (prevents stale updates on already-transitioned positions).
        """
        return await asyncio.to_thread(self._update_leg_sync, position_id, phase, venue, updates, expected_state)

    def _add_exec_id_sync(
        self,
        position_id: str,
        phase: str,
        venue: str,
        exec_id: str,
        fill_qty: Optional[float] = None,
        avg_price: Optional[float] = None,
    ) -> bool:
        """Synchronous add_exec_id — called via asyncio.to_thread()."""
        set_fields: dict = {"updated_at": time.time()}
        if fill_qty is not None:
            set_fields[f"{phase}.{venue}.filled_qty"] = fill_qty
        if avg_price is not None:
            set_fields[f"{phase}.{venue}.avg_fill_price"] = avg_price

        result = self._coll.update_one(
            {
                "position_id": position_id,
                f"{phase}.{venue}.exec_ids": {"$ne": exec_id},
            },
            {
                "$addToSet": {f"{phase}.{venue}.exec_ids": exec_id},
                "$set": set_fields,
            },
        )
        return result.modified_count > 0

    async def add_exec_id(
        self,
        position_id: str,
        phase: str,
        venue: str,
        exec_id: str,
        fill_qty: Optional[float] = None,
        avg_price: Optional[float] = None,
    ) -> bool:
        """
        Atomically add an exec_id to a leg's exec_ids array (dedup).
        Optionally updates filled_qty and avg_fill_price atomically with the exec_id.
        Returns False if exec_id already exists (duplicate fill).
        """
        return await asyncio.to_thread(self._add_exec_id_sync, position_id, phase, venue, exec_id, fill_qty, avg_price)
