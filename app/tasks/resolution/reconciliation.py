"""
Exchange-level position reconciliation — exchange is the source of truth.

Three-way comparison: MongoDB candidates vs HB executors vs exchange positions.

Key principle: NEVER trust HB's filled_amount=0 after a crash. HB stores
executor↔order linkage in memory only. On any crash, it reports 0 filled
even when the exchange has an open position.

Used by:
- WatchdogTask: periodic reconciliation (every 5 min)
- cli.py startup: exchange audit before pipeline starts
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from app.services.hb_api_client import HBApiClient

logger = logging.getLogger(__name__)


# ── Public API ──────────────────────────────────────────────


async def startup_exchange_audit(
    db, connector: str = "bybit_perpetual_testnet",
    account: str = "master_account",
) -> dict:
    """Run on pipeline startup BEFORE any trading tasks.

    Queries the exchange for ALL open positions, cross-references with MongoDB.
    Detects orphans left by a previous crash.
    """
    hb = HBApiClient()
    stats = {"exchange_positions": 0, "orphan_positions": 0, "stale_candidates": 0}

    try:
        if not await hb.health_check():
            logger.warning("Startup audit: HB API unreachable, skipping")
            return {**stats, "skipped": True}

        # Ensure common pairs are registered so we can see positions
        await _register_active_pairs(hb, db, connector, account)

        exchange_positions = await _get_exchange_positions(hb, connector, account)
        stats["exchange_positions"] = len(exchange_positions)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Check each exchange position against MongoDB
        for pos in exchange_positions:
            pair = pos["pair"]
            side = pos["side"]
            amount = pos["amount"]

            # Look for a matching TESTNET_ACTIVE candidate
            matching = db.candidates.find_one({
                "pair": pair,
                "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
            })

            if not matching:
                stats["orphan_positions"] += 1
                logger.error(
                    f"Startup audit: ORPHAN EXCHANGE POSITION {pair} {side} "
                    f"amount={amount} — open on exchange but not tracked in MongoDB"
                )
                # Create a tracking record so the watchdog can alert
                db.candidates.insert_one({
                    "candidate_id": f"orphan_{pair}_{now_ms}",
                    "pair": pair,
                    "direction": side,
                    "engine": "UNKNOWN",
                    "disposition": "TESTNET_ACTIVE",
                    "timestamp_utc": now_ms,
                    "trigger_fired": True,
                    "trigger_reason": "Orphan detected by startup audit",
                    "hard_filters_passed": True,
                    "decision_price": pos.get("entry_price"),
                    "testnet_placed_at": now_ms,
                    "testnet_amount": abs(amount),
                    "reconciliation_note": (
                        f"Orphan position found on exchange at startup. "
                        f"Entry: {pos.get('entry_price')}, "
                        f"Unrealized PnL: {pos.get('unrealized_pnl')}"
                    ),
                    "reconciliation_needs_manual": True,
                })
            else:
                logger.info(
                    f"Startup audit: {pair} {side} — matched to candidate "
                    f"{matching.get('candidate_id')}"
                )

        # Check for stale TESTNET_ACTIVE candidates with no exchange position
        mongo_active = list(db.candidates.find({
            "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
        }))
        exchange_pairs = {p["pair"] for p in exchange_positions}

        for doc in mongo_active:
            pair = doc.get("pair", "?")
            if pair not in exchange_pairs:
                stats["stale_candidates"] += 1
                cid = doc.get("candidate_id", "?")
                engine = doc.get("engine", "?")
                logger.warning(
                    f"Startup audit: {engine}/{pair} is TESTNET_ACTIVE in MongoDB "
                    f"but has no exchange position — attempting recovery"
                )
                eid = doc.get("executor_id")
                if eid:
                    await _recover_from_executor_history(
                        hb, db, doc, eid, now_ms, connector, account
                    )

        if stats["orphan_positions"] == 0 and stats["stale_candidates"] == 0:
            logger.info(
                f"Startup audit: clean — {stats['exchange_positions']} exchange "
                f"position(s), all matched"
            )

    except Exception as e:
        logger.error(f"Startup audit failed: {e}")
        stats["error"] = str(e)
    finally:
        await hb.close()

    return stats


async def reconcile_positions(
    db, connector: str = "bybit_perpetual_testnet",
    account: str = "master_account",
) -> dict:
    """Three-way reconciliation: MongoDB vs HB executors vs exchange.

    Runs periodically from the watchdog task.
    """
    stats = {
        "mongo_active": 0, "hb_active": 0, "exchange_active": 0,
        "ghost_positions": 0, "ghost_recovered": 0, "ghost_unresolved": 0,
        "orphan_executors": 0, "exchange_mismatches": 0,
    }

    hb = HBApiClient()
    try:
        if not await hb.health_check():
            logger.warning("Reconciliation: HB API unreachable")
            await hb.close()
            return {**stats, "error": "HB API unreachable"}

        # ── Gather all three sources ──
        mongo_active = list(db.candidates.find({
            "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
        }))
        stats["mongo_active"] = len(mongo_active)

        mongo_by_executor = {
            doc["executor_id"]: doc
            for doc in mongo_active
            if doc.get("executor_id")
        }
        mongo_pairs = {doc.get("pair") for doc in mongo_active}

        hb_executors = await hb.get_active_executors()
        stats["hb_active"] = len(hb_executors)
        hb_active_ids = set()
        for ex in hb_executors:
            eid = ex.get("executor_id") or ex.get("id")
            if eid:
                hb_active_ids.add(eid)

        exchange_positions = await _get_exchange_positions(hb, connector, account)
        stats["exchange_active"] = len(exchange_positions)
        exchange_pairs = {p["pair"] for p in exchange_positions}

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # ── Check 1: Ghost positions (MongoDB active, HB executor gone) ──
        for eid, doc in mongo_by_executor.items():
            if eid in hb_active_ids:
                continue

            stats["ghost_positions"] += 1
            pair = doc.get("pair", "?")
            engine = doc.get("engine", "?")
            direction = doc.get("direction", "?")
            cid = doc.get("candidate_id", "?")

            logger.warning(
                f"Reconciliation: ghost {engine}/{pair} {direction} "
                f"(executor {eid}) — attempting recovery"
            )

            recovered = await _recover_from_executor_history(
                hb, db, doc, eid, now_ms, connector, account
            )
            if recovered:
                stats["ghost_recovered"] += 1
            else:
                stats["ghost_unresolved"] += 1

        # ── Check 2: Exchange positions not tracked in MongoDB ──
        for pos in exchange_positions:
            pair = pos["pair"]
            if pair not in mongo_pairs:
                stats["exchange_mismatches"] += 1
                logger.error(
                    f"Reconciliation: UNTRACKED EXCHANGE POSITION {pair} "
                    f"{pos['side']} amount={pos['amount']} — not in MongoDB"
                )

        # ── Check 3: Orphan executors (HB active, not in MongoDB) ──
        for eid in hb_active_ids:
            if eid not in mongo_by_executor:
                stats["orphan_executors"] += 1
                logger.warning(
                    f"Reconciliation: orphan executor {eid} — "
                    "active in HB but not tracked in MongoDB"
                )

    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        stats["error"] = str(e)
    finally:
        await hb.close()

    clean = (
        stats["ghost_positions"] == 0
        and stats["orphan_executors"] == 0
        and stats["exchange_mismatches"] == 0
    )
    if clean:
        logger.info(
            f"Reconciliation: clean — {stats['mongo_active']} MongoDB, "
            f"{stats['hb_active']} HB, {stats['exchange_active']} exchange"
        )

    return stats


# ── Recovery logic ──────────────────────────────────────────


async def _recover_from_executor_history(
    hb: HBApiClient, db, doc: dict, executor_id: str, now_ms: int,
    connector: str = "bybit_perpetual_testnet",
    account: str = "master_account",
) -> bool:
    """Recover a ghost position from HB executor history.

    CRITICAL: If executor reports filled_amount=0 after a crash
    (SYSTEM_CLEANUP/FAILED), we cross-check with the exchange before
    accepting. HB loses executor↔order state on crash.
    """
    cid = doc.get("candidate_id", "?")
    pair = doc.get("pair", "?")
    engine = doc.get("engine", "?")

    try:
        status = await hb.get_executor_status(executor_id)
    except Exception as e:
        logger.warning(f"Reconciliation: cannot query executor {executor_id}: {e}")
        return False

    exec_status = (status.get("status") or "").lower()
    close_type = status.get("close_type")

    if exec_status not in ("completed", "failed", "stopped", "closed", "terminated"):
        return False

    filled_amount = status.get("filled_amount_quote")
    filled_zero = filled_amount is not None and float(filled_amount) == 0
    is_cleanup = close_type in ("SYSTEM_CLEANUP", "FAILED")

    # THE KEY CHECK: never trust filled_amount=0 from a crashed executor
    if filled_zero and is_cleanup:
        has_exchange_pos = await _check_exchange_position(hb, pair, connector, account)
        if has_exchange_pos:
            # Exchange has a position but executor says unfilled — MISMATCH
            logger.error(
                f"Reconciliation: EXCHANGE MISMATCH {engine}/{pair} — "
                f"executor reports filled=0 but exchange has open position. "
                f"DO NOT resolve. Manual intervention required."
            )
            db.candidates.update_one(
                {"candidate_id": cid},
                {"$set": {
                    "reconciliation_divergent": True,
                    "reconciliation_checked_at": now_ms,
                    "reconciliation_reason": "exchange_mismatch_filled_zero",
                    "reconciliation_needs_manual": True,
                }},
            )
            return False  # NOT recovered — needs manual handling

        # No exchange position either → genuinely never filled
        logger.info(
            f"Reconciliation: {engine}/{pair} — executor says unfilled, "
            f"exchange confirms no position. Marking NEVER_FILLED."
        )
        resolved_close_type = "NEVER_FILLED"
    else:
        resolved_close_type = close_type or "RECOVERED"

    # Extract outcome data
    custom_info = status.get("custom_info") or {}
    fill_price = (
        status.get("entry_price")
        or status.get("fill_price")
        or custom_info.get("current_position_average_price")
    )
    exit_price = (
        status.get("close_price")
        or status.get("exit_price")
        or custom_info.get("close_price")
    )
    pnl = status.get("pnl") or status.get("net_pnl_quote")

    # Compute slippage
    decision_price = doc.get("decision_price")
    slippage_bps = None
    slippage_bucket = None
    if decision_price and fill_price:
        try:
            dp = float(decision_price)
            fp_val = float(fill_price)
            if dp > 0:
                direction = doc.get("direction", "LONG")
                raw_slip = (fp_val - dp) / dp * 10000
                slippage_bps = raw_slip if direction == "LONG" else -raw_slip
                abs_slip = abs(slippage_bps)
                slippage_bucket = (
                    "safe" if abs_slip < 10
                    else "borderline" if abs_slip < 20
                    else "danger"
                )
        except (ValueError, TypeError):
            pass

    db.candidates.update_one(
        {"candidate_id": cid},
        {"$set": {
            "disposition": "RESOLVED_TESTNET",
            "testnet_resolved_at": now_ms,
            "testnet_fill_price": fill_price,
            "testnet_exit_price": exit_price,
            "testnet_pnl": pnl,
            "testnet_close_type": resolved_close_type,
            "testnet_filled_amount_quote": filled_amount,
            "testnet_status": exec_status,
            "testnet_raw_result": status,
            "testnet_slippage_bps": slippage_bps,
            "testnet_slippage_bucket": slippage_bucket,
            "reconciliation_recovered": True,
            "reconciliation_recovered_at": now_ms,
        }},
    )

    logger.info(
        f"Reconciliation: recovered {engine}/{pair} — "
        f"close_type={resolved_close_type}, pnl={pnl}, filled={filled_amount}"
    )
    return True


# ── Exchange helpers ────────────────────────────────────────


async def _get_exchange_positions(
    hb: HBApiClient, connector: str, account: str,
) -> list[dict]:
    """Query exchange for all open positions, normalized to a simple format."""
    try:
        raw = await hb.get_exchange_positions(connector, account)
        positions = []
        for pos in raw:
            amount = float(pos.get("amount", 0) or 0)
            if abs(amount) > 0:
                positions.append({
                    "pair": pos.get("trading_pair") or pos.get("pair", ""),
                    "side": pos.get("side", "UNKNOWN"),
                    "amount": amount,
                    "entry_price": pos.get("entry_price"),
                    "unrealized_pnl": pos.get("unrealized_pnl"),
                })
        return positions
    except Exception as e:
        logger.warning(f"Reconciliation: cannot query exchange positions: {e}")
        return []


async def _check_exchange_position(
    hb: HBApiClient, pair: str, connector: str, account: str,
) -> bool:
    """Check if a specific pair has an open position on the exchange."""
    positions = await _get_exchange_positions(hb, connector, account)
    return any(p["pair"] == pair for p in positions)


async def _register_active_pairs(
    hb: HBApiClient, db, connector: str, account: str,
) -> None:
    """Register all TESTNET_ACTIVE pairs with HB so exchange queries work.

    HB can only see positions for pairs that have been registered via
    /market-data/trading-pair/add. Without this, /trading/positions
    returns empty even when positions exist.
    """
    active = db.candidates.find({
        "disposition": {"$in": ["TESTNET_ACTIVE", "PLACING"]},
    })
    pairs_registered = set()
    for doc in active:
        pair = doc.get("pair")
        if pair and pair not in pairs_registered:
            await hb.ensure_trading_pair(connector, pair, account)
            pairs_registered.add(pair)

    # Also register common pairs that might have orphan positions
    common_pairs = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT"]
    for pair in common_pairs:
        if pair not in pairs_registered:
            await hb.ensure_trading_pair(connector, pair, account)
