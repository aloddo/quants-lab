"""
Options V4 Execution Engine -- Protection-first, MongoDB-persisted.

Core principle: NEVER sell short options before owning the long hedge.
Every order gets a unique orderLinkId persisted to MongoDB BEFORE submission.
"""
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pymongo import MongoClient

logger = logging.getLogger("options_v4.execution")

MONGO_URI = "mongodb://localhost:27017/quants_lab"
DB_NAME = "quants_lab"
INTENTS_COLL = "options_v4_intents"
ORDERS_COLL = "options_v4_orders"
FILL_TIMEOUT_S = 60


class IntentStatus(Enum):
    CREATED = "created"
    LONG_PENDING = "long_pending"
    LONG_FILLED = "long_filled"
    SHORT_PENDING = "short_pending"
    BOTH_FILLED = "both_filled"
    FAILED_LONG = "failed_long"
    FAILED_SHORT = "failed_short"
    CLOSED = "closed"
    KILLED = "killed"


@dataclass
class SpreadIntent:
    intent_id: str
    strategy: str           # "put_credit", "call_credit", "iron_condor", "put_debit", "call_debit"
    regime: str
    underlying: str         # "BTC" or "ETH"
    long_symbol: str
    short_symbol: str
    qty: float
    max_loss: float
    expected_credit: float  # negative for debit spreads
    status: str = "created"


def get_db():
    return MongoClient(MONGO_URI)[DB_NAME]


def create_intent(intent: SpreadIntent) -> str:
    """Persist intent to MongoDB BEFORE any order. Returns intent_id."""
    db = get_db()
    now = datetime.now(timezone.utc)
    doc = {
        "intent_id": intent.intent_id,
        "strategy": intent.strategy,
        "regime": intent.regime,
        "underlying": intent.underlying,
        "long_symbol": intent.long_symbol,
        "short_symbol": intent.short_symbol,
        "qty": intent.qty,
        "max_loss": intent.max_loss,
        "expected_credit": intent.expected_credit,
        "status": IntentStatus.CREATED.value,
        "created_at": now,
        "updated_at": now,
        "long_order_link_id": f"ov4-L-{uuid.uuid4().hex[:12]}",
        "short_order_link_id": f"ov4-S-{uuid.uuid4().hex[:12]}",
        "long_fill_px": None,
        "short_fill_px": None,
        "realized_pnl": None,
        "close_reason": None,
    }
    db[INTENTS_COLL].insert_one(doc)
    logger.info(f"Intent {intent.intent_id}: {intent.strategy} {intent.underlying} "
                f"long={intent.long_symbol} short={intent.short_symbol}")
    return intent.intent_id


def update_intent(intent_id: str, **fields):
    """Update intent fields."""
    db = get_db()
    fields["updated_at"] = datetime.now(timezone.utc)
    db[INTENTS_COLL].update_one({"intent_id": intent_id}, {"$set": fields})


def get_open_intents() -> list:
    """Get all non-terminal intents. Includes partial fills that need attention."""
    db = get_db()
    return list(db[INTENTS_COLL].find({
        "status": {"$nin": ["closed", "failed_long", "killed", "failed_crash"]}
    }))


def execute_spread(session, intent: SpreadIntent, dry_run: bool = True) -> bool:
    """Execute a spread with protection-first ordering.

    For CREDIT spreads: buy long (protection) first, then sell short (income).
    For DEBIT spreads: buy long (profit leg) first, then sell short (hedge).
    Either way: long leg first, short leg second.

    Returns True if both legs filled.
    """
    db = get_db()
    intent_doc = db[INTENTS_COLL].find_one({"intent_id": intent.intent_id})
    if not intent_doc:
        logger.error(f"Intent {intent.intent_id} not found in DB")
        return False

    if dry_run:
        logger.info(f"[DRY RUN] Would execute: BUY {intent.long_symbol} then SELL {intent.short_symbol} x{intent.qty}")
        update_intent(intent.intent_id, status=IntentStatus.BOTH_FILLED.value)
        return True

    long_oid = intent_doc["long_order_link_id"]
    short_oid = intent_doc["short_order_link_id"]

    # === STEP 1: Buy long leg ===
    update_intent(intent.intent_id, status=IntentStatus.LONG_PENDING.value)

    try:
        ticker = session.get_tickers(category="option", symbol=intent.long_symbol)
        long_ask = float(ticker["result"]["list"][0].get("ask1Price", 0))
        if long_ask <= 0:
            logger.error(f"No ask for {intent.long_symbol}")
            update_intent(intent.intent_id, status=IntentStatus.FAILED_LONG.value)
            return False
    except Exception as e:
        logger.error(f"Ticker failed {intent.long_symbol}: {e}")
        update_intent(intent.intent_id, status=IntentStatus.FAILED_LONG.value)
        return False

    # Persist order BEFORE submission
    db[ORDERS_COLL].insert_one({
        "order_link_id": long_oid, "intent_id": intent.intent_id,
        "symbol": intent.long_symbol, "side": "Buy", "qty": intent.qty,
        "price": long_ask, "status": "pending",
        "created_at": datetime.now(timezone.utc),
    })

    try:
        result = session.place_order(
            category="option", symbol=intent.long_symbol,
            side="Buy", orderType="Limit", qty=str(intent.qty),
            price=str(long_ask), timeInForce="GTC", orderLinkId=long_oid,
        )
        if result["retCode"] != 0:
            logger.error(f"Long leg rejected: {result['retMsg']}")
            update_intent(intent.intent_id, status=IntentStatus.FAILED_LONG.value)
            db[ORDERS_COLL].update_one({"order_link_id": long_oid}, {"$set": {"status": "rejected", "error": result["retMsg"]}})
            return False
        db[ORDERS_COLL].update_one({"order_link_id": long_oid}, {"$set": {"status": "submitted", "exchange_oid": result["result"].get("orderId")}})
    except Exception as e:
        logger.error(f"Long leg failed: {e}")
        update_intent(intent.intent_id, status=IntentStatus.FAILED_LONG.value)
        return False

    # Wait for fill. FIX #4: handle partial fills explicitly
    fill_status = _wait_fill(session, long_oid, intent.long_symbol)
    if not fill_status:
        logger.warning(f"Long leg not filled in {FILL_TIMEOUT_S}s, cancelling")
        _cancel(session, long_oid, intent.long_symbol)
        # Check if partially filled (FIX #4)
        partial_qty = _get_fill_qty(long_oid)
        if partial_qty > 0:
            logger.warning(f"PARTIAL LONG: {partial_qty} filled, marking for manual review")
            update_intent(intent.intent_id, status="partial_long",
                          long_fill_px=_get_fill_price(long_oid))
        else:
            update_intent(intent.intent_id, status=IntentStatus.FAILED_LONG.value)
        return False

    long_fill = _get_fill_price(long_oid)
    update_intent(intent.intent_id, status=IntentStatus.LONG_FILLED.value, long_fill_px=long_fill)
    logger.info(f"Long leg filled: {intent.long_symbol} @ {long_fill}")

    # === STEP 2: Sell short leg ===
    update_intent(intent.intent_id, status=IntentStatus.SHORT_PENDING.value)

    try:
        ticker = session.get_tickers(category="option", symbol=intent.short_symbol)
        short_bid = float(ticker["result"]["list"][0].get("bid1Price", 0))
        if short_bid <= 0:
            logger.error(f"No bid for {intent.short_symbol}")
            # FIX #5: Note that long leg is still open (holding as cheap protection)
            update_intent(intent.intent_id, status=IntentStatus.FAILED_SHORT.value,
                          close_reason="no_bid_short_leg_long_still_open")
            logger.warning(f"Long leg {intent.long_symbol} remains open as protection")
            return False
    except Exception as e:
        logger.error(f"Ticker failed {intent.short_symbol}: {e}")
        update_intent(intent.intent_id, status=IntentStatus.FAILED_SHORT.value, close_reason="long_still_open")
        return False

    # Net credit/debit check (Codex condition #3: no naked short if economics bad)
    if intent.strategy in ("put_credit", "call_credit", "iron_condor"):
        net = (short_bid - long_fill) * intent.qty
        if net <= 0:
            logger.warning(f"Net credit negative ({net:.2f}), aborting short leg")
            update_intent(intent.intent_id, status=IntentStatus.FAILED_SHORT.value, close_reason="long_still_open")
            return False

    db[ORDERS_COLL].insert_one({
        "order_link_id": short_oid, "intent_id": intent.intent_id,
        "symbol": intent.short_symbol, "side": "Sell", "qty": intent.qty,
        "price": short_bid, "status": "pending",
        "created_at": datetime.now(timezone.utc),
    })

    try:
        result = session.place_order(
            category="option", symbol=intent.short_symbol,
            side="Sell", orderType="Limit", qty=str(intent.qty),
            price=str(short_bid), timeInForce="GTC", orderLinkId=short_oid,
        )
        if result["retCode"] != 0:
            logger.error(f"Short leg rejected: {result['retMsg']}")
            update_intent(intent.intent_id, status=IntentStatus.FAILED_SHORT.value, close_reason="long_still_open")
            db[ORDERS_COLL].update_one({"order_link_id": short_oid}, {"$set": {"status": "rejected", "error": result["retMsg"]}})
            return False
        db[ORDERS_COLL].update_one({"order_link_id": short_oid}, {"$set": {"status": "submitted", "exchange_oid": result["result"].get("orderId")}})
    except Exception as e:
        logger.error(f"Short leg failed: {e}")
        update_intent(intent.intent_id, status=IntentStatus.FAILED_SHORT.value, close_reason="long_still_open")
        return False

    if not _wait_fill(session, short_oid, intent.short_symbol):
        logger.warning(f"Short leg not filled, cancelling")
        _cancel(session, short_oid, intent.short_symbol)
        update_intent(intent.intent_id, status=IntentStatus.FAILED_SHORT.value, close_reason="long_still_open")
        return False

    short_fill = _get_fill_price(short_oid)
    update_intent(intent.intent_id, status=IntentStatus.BOTH_FILLED.value, short_fill_px=short_fill)
    logger.info(f"SPREAD FILLED: long@{long_fill} short@{short_fill}")
    return True


def _wait_fill(session, order_link_id: str, symbol: str) -> bool:
    """Poll for fill. Returns True if filled within timeout."""
    start = time.time()
    while time.time() - start < FILL_TIMEOUT_S:
        try:
            result = session.get_order_history(
                category="option", symbol=symbol, orderLinkId=order_link_id
            )
            orders = result["result"]["list"]
            if orders and orders[0].get("orderStatus") == "Filled":
                avg_px = float(orders[0].get("avgPrice", 0))
                filled_qty = float(orders[0].get("cumExecQty", 0))
                db = get_db()
                db[ORDERS_COLL].update_one(
                    {"order_link_id": order_link_id},
                    {"$set": {"status": "filled", "avg_price": avg_px, "filled_qty": filled_qty}}
                )
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _cancel(session, order_link_id: str, symbol: str):
    """Cancel an open order. Always attempts cancel, then records final state."""
    try:
        # ALWAYS attempt cancel first (FIX R4#1: don't return before cancelling)
        try:
            session.cancel_order(category="option", symbol=symbol, orderLinkId=order_link_id)
        except Exception as ce:
            logger.warning(f"Cancel request failed for {order_link_id}: {ce}")

        # Then fetch final state (may have partially filled before/during cancel)
        try:
            hist = session.get_order_history(category="option", symbol=symbol, orderLinkId=order_link_id)
            orders = hist["result"]["list"]
            if orders:
                cum_qty = float(orders[0].get("cumExecQty", 0))
                avg_px = float(orders[0].get("avgPrice", 0))
                final_status = orders[0].get("orderStatus", "unknown")
                if cum_qty > 0 and final_status != "Filled":
                    get_db()[ORDERS_COLL].update_one(
                        {"order_link_id": order_link_id},
                        {"$set": {"filled_qty": cum_qty, "avg_price": avg_px, "status": "partial_cancelled"}}
                    )
                    logger.warning(f"Cancel {order_link_id}: partial fill {cum_qty} detected. GTC cancelled.")
                    return
                elif final_status == "Filled":
                    get_db()[ORDERS_COLL].update_one(
                        {"order_link_id": order_link_id},
                        {"$set": {"filled_qty": cum_qty, "avg_price": avg_px, "status": "filled"}}
                    )
                    logger.warning(f"Cancel {order_link_id}: order FILLED before cancel took effect")
                    return
        except Exception:
            pass

        get_db()[ORDERS_COLL].update_one({"order_link_id": order_link_id}, {"$set": {"status": "cancelled"}})
    except Exception as e:
        logger.error(f"Cancel failed {order_link_id}: {e}")


def _get_fill_price(order_link_id: str) -> float:
    db = get_db()
    doc = db[ORDERS_COLL].find_one({"order_link_id": order_link_id})
    return doc.get("avg_price", 0) if doc else 0


def _get_fill_qty(order_link_id: str) -> float:
    """Get actual filled quantity (for partial fill detection)."""
    db = get_db()
    doc = db[ORDERS_COLL].find_one({"order_link_id": order_link_id})
    return doc.get("filled_qty", 0) if doc else 0
